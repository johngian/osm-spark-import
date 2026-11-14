"""osm-hadoop-loader: applies outbox batches to PostgreSQL.

A standalone process (no Spark, no JVM): it reads the outbox over WebHDFS
(fsspec + pyarrow) and writes with COPY (psycopg). The database connection
comes from libpq's PG* environment variables. The SQL comes from
mapping.postgis.

    osm-hadoop-loader [apply] [--loop]   apply new batches:
        snapshot -> schema import: create the tables, COPY, generalized
                    tables, indexes. Not deployed.
        delta    -> production schema, in one transaction: delete the
                    affected elements (and from the generalized tables), COPY
                    the new rows, update the generalized tables. Expired
                    tiles are written to OSM_EXPIRE_DIR first. Deltas wait
                    until their snapshot is deployed.
    osm-hadoop-loader deploy         import -> production -> backup
    osm-hadoop-loader revert-deploy  backup -> production -> import
    osm-hadoop-loader remove-backup  drop the backup tables
    osm-hadoop-loader status

State: osm_hadoop.loader_state, one row per namespace: which batch is in the
import, production and backup schemas, written in the same transaction as
the data.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import io
import json
import os
import time
from urllib.parse import urlparse

import fsspec
import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from . import outbox
from .mapping import postgis as P
from .mapping.writer import REL_ID_OFFSET
from .settings import Settings

COPY_BATCH = 50_000
STATE = "osm_hadoop.loader_state"


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


class Outbox:
    def __init__(self, s: Settings):
        u = urlparse(s.webhdfs_url)
        kw = {"host": u.hostname, "port": u.port or 9870}
        if s.webhdfs_user:
            kw["user"] = s.webhdfs_user
        self.fs = fsspec.filesystem("webhdfs", **kw)
        self.root = s.outbox_path

    def batches(self) -> list[int]:
        if not self.fs.exists(self.root):
            return []
        out = []
        for p in self.fs.ls(self.root, detail=False):
            name = p.rstrip("/").rsplit("/", 1)[-1]
            if name.isdigit():
                out.append(int(name))
        return sorted(out)

    def manifest(self, batch: int) -> dict | None:
        path = f"{outbox.batch_dir(self.root, batch)}/{outbox.MANIFEST}"
        if not self.fs.exists(path):
            return None  # not complete yet
        return json.loads(self.fs.cat(path))

    def files(self, batch: int, part: str) -> list[str]:
        d = f"{outbox.batch_dir(self.root, batch)}/{part}"
        if not self.fs.exists(d):
            return []
        return sorted(
            p
            for p in self.fs.find(d)
            if p.endswith(".parquet") and not p.rsplit("/", 1)[-1].startswith((".", "_"))
        )

    def read(self, path: str, columns: list[str]):
        with self.fs.open(path, "rb") as f:
            yield from pq.ParquetFile(f).iter_batches(batch_size=COPY_BATCH, columns=columns)


def _csv(batch: pa.RecordBatch, geometry: set[str]) -> bytes:
    """A record batch as CSV for COPY: geometries as hex EWKB."""
    for name in geometry & set(batch.schema.names):
        i = batch.schema.get_field_index(name)
        hexes = pa.array(
            [g.hex() if g is not None else None for g in batch.column(i).to_pylist()], pa.string()
        )
        batch = batch.set_column(i, name, hexes)
    buf = io.BytesIO()
    # quote every value: unquoted empty fields are NULL, "" is the empty string
    pacsv.write_csv(batch, buf, pacsv.WriteOptions(include_header=False, quoting_style="all_valid"))
    return buf.getvalue()


def copy_file(
    cur, ob: Outbox, path: str, schema: str, table: str, columns: list[str], geometry: set[str]
) -> int:
    stmt = (
        f"COPY {P.q(schema)}.{P.q(table)} ({', '.join(P.q(c) for c in columns)}) FROM STDIN WITH (FORMAT csv)"
    )
    n = 0
    with cur.copy(stmt) as cp:
        for b in ob.read(path, columns):
            cp.write(_csv(b, geometry))
            n += b.num_rows
    return n


def table_names(m: dict) -> list[str]:
    """tableNames(): mapping tables and generalized tables."""
    return list(m["tables"]) + list(m.get("generalized") or {})


def id_expr(kind: str, single_id_space: bool) -> str:
    """The ID written for an element of this kind (on osm_delete.id)."""
    if kind == "w" and single_id_space:
        return "-id"
    if kind == "r":
        return f"({REL_ID_OFFSET} - id)" if single_id_space else "-id"
    return "id"


def write_expire_file(expire_dir: str, tiles: list[str]) -> str | None:
    """DIR/YYYYMMDD/HHMMSS.mmm.tiles, written via .tiles~ and renamed."""
    if not tiles:
        return None
    now = dt.datetime.now(dt.UTC)
    day = os.path.join(expire_dir, now.strftime("%Y%m%d"))
    os.makedirs(day, exist_ok=True)
    path = os.path.join(day, now.strftime("%H%M%S.") + f"{now.microsecond // 1000:03d}.tiles")
    with open(path + "~", "w") as f:
        f.write("".join(t + "\n" for t in tiles))
    os.rename(path + "~", path)
    return path


class Loader:
    def __init__(self, s: Settings):
        self.s = s
        self.ob = Outbox(s)
        self.conn = psycopg.connect(autocommit=True)
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS osm_hadoop")
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {STATE} (
                    namespace text PRIMARY KEY,
                    import_batch bigint, import_manifest jsonb,
                    production_batch bigint, production_manifest jsonb,
                    backup_batch bigint, backup_manifest jsonb,
                    sequence bigint, osm_time timestamptz,
                    updated timestamptz NOT NULL DEFAULT now())"""
            )
            cur.execute(f"INSERT INTO {STATE} (namespace) VALUES (%s) ON CONFLICT DO NOTHING", (s.namespace,))

    def exclusive(self) -> None:
        """One applying loader per namespace (held for the process lifetime)."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext('osm-hadoop-loader:' || %s))", (self.s.namespace,)
            )
            if not cur.fetchone()[0]:
                raise SystemExit(f"another loader is applying batches for namespace {self.s.namespace}")

    def work_lock(self):
        """Serializes applying a batch with deploy/revert/remove-backup."""
        loader = self

        class _Lock:
            def __enter__(self):
                with loader.conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_lock(hashtext('osm-hadoop-work:' || %s))", (loader.s.namespace,)
                    )

            def __exit__(self, *exc):
                with loader.conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_unlock(hashtext('osm-hadoop-work:' || %s))", (loader.s.namespace,)
                    )

        return _Lock()

    def state(self, cur=None) -> dict:
        cur = cur or self.conn.cursor()
        cur.execute(
            f"""SELECT import_batch, import_manifest, production_batch, production_manifest,
                       backup_batch, backup_manifest, sequence, osm_time FROM {STATE} WHERE namespace = %s""",
            (self.s.namespace,),
        )
        keys = (
            "import_batch",
            "import_manifest",
            "production_batch",
            "production_manifest",
            "backup_batch",
            "backup_manifest",
            "sequence",
            "osm_time",
        )
        return dict(zip(keys, cur.fetchone(), strict=False))

    def _set(self, cur, **values) -> None:
        cols = ", ".join(f"{k} = %s" for k in values)
        params = [
            json.dumps(v) if k.endswith("_manifest") and v is not None else v for k, v in values.items()
        ]
        cur.execute(
            f"UPDATE {STATE} SET {cols}, updated = now() WHERE namespace = %s", (*params, self.s.namespace)
        )

    # --- applying batches --------------------------------------------------------

    def step(self) -> bool:
        with self.work_lock():
            return self._step()

    def _step(self) -> bool:
        st = self.state()
        seen = max(st["import_batch"] or 0, st["production_batch"] or 0)
        manifests = []
        for b in self.ob.batches():
            if b <= (st["production_batch"] or 0):
                continue
            m = self.ob.manifest(b)
            if m is None:
                break  # being written: later batches wait for it
            manifests.append(m)
        applied = False
        snapshots = [m for m in manifests if m["kind"] == "snapshot" and m["batch"] > seen]
        if snapshots:
            self.load_snapshot(snapshots[-1])
            applied = True
            st = self.state()
        for m in manifests:
            if (
                m["kind"] == "delta"
                and st["production_batch"] is not None
                and m["base"] == st["production_batch"]
            ):
                self.apply_delta(m, st)
                applied = True
                st = self.state()
        return applied

    def load_snapshot(self, m: dict) -> None:
        s, b = self.s, m["batch"]
        t0 = time.time()
        imp = s.schema_import
        log(f"batch {b}: snapshot (sequence {m['sequence']}) into schema {imp}")
        gen = P.Generalized(m["tables"], m.get("generalized") or {})
        with self.conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {P.q(imp)}")
            for name, spec in m["tables"].items():  # Init
                cur.execute(f"DROP TABLE IF EXISTS {P.q(imp)}.{P.q(name)}")
                cur.execute(P.create_table_sql(imp, name, spec))
                add = P.add_geometry_column_sql(imp, name, spec, m["srid"])
                if add:
                    cur.execute(add)

        def load(task):
            name, path = task
            spec = m["tables"][name]
            geometry = {c["name"] for c in spec["columns"] if P.is_geometry(c)}
            with psycopg.connect(autocommit=True) as conn, conn.cursor() as cur:
                return name, copy_file(cur, self.ob, path, imp, name, P.copy_columns(spec), geometry)

        def run(stmts):
            with psycopg.connect(autocommit=True) as conn, conn.cursor() as cur:
                for stmt in stmts:
                    cur.execute(stmt)

        tasks = [(name, p) for name in m["tables"] for p in self.ob.files(b, f"tables/{name}")]
        counts: dict[str, int] = {}
        with cf.ThreadPoolExecutor(s.loader_workers) as pool:
            for name, n in pool.map(load, tasks):
                counts[name] = counts.get(name, 0) + n
            for name in m["tables"]:
                if counts.get(name, 0) != m["rows"][name]:
                    raise SystemExit(
                        f"{name}: loaded {counts.get(name, 0)} rows, the batch has {m['rows'][name]}"
                    )
            # Generalize: level by level, sources first
            for name in gen.order():
                run([f"DROP TABLE IF EXISTS {P.q(imp)}.{P.q(name)}", gen.create_sql(imp, name)])
            # Finish: indexes
            jobs = [P.index_sqls(imp, n, spec["columns"], False) for n, spec in m["tables"].items()]
            jobs += [P.index_sqls(imp, n, m["tables"][gen.root(n)]["columns"], True) for n in gen.order()]
            list(pool.map(run, jobs))
        with self.conn.transaction(), self.conn.cursor() as cur:
            self._set(cur, import_batch=b, import_manifest=m)
        log(f"batch {b}: {sum(counts.values())} rows in {time.time() - t0:.1f}s; deploy to make it live")

    def apply_delta(self, m: dict, st: dict) -> None:
        s, b = self.s, m["batch"]
        prod = s.schema
        current = st["production_manifest"] or {}
        if current.get("tables") != m["tables"] or current.get("generalized") != m.get("generalized"):
            raise SystemExit(
                f"batch {b} has other tables than schema {prod} (mapping changed?): "
                "run `osm-hadoop rebuild` and deploy the new snapshot"
            )
        t0 = time.time()
        gen = P.Generalized(m["tables"], m.get("generalized") or {})
        single = m.get("single_id_space", False)

        tiles = []
        for path in self.ob.files(b, "tiles"):
            for batch in self.ob.read(path, ["tile"]):
                tiles += batch.column(0).to_pylist()
        if tiles and s.expire_dir:  # before the commit: tiles are never lost
            log(f"expired tiles: {write_expire_file(s.expire_dir, sorted(tiles))}")

        inserted = 0
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE osm_delete (kind char(1), id bigint) ON COMMIT DROP")
            for path in self.ob.files(b, "deletes"):
                copy_file(cur, self.ob, path, "pg_temp", "osm_delete", ["kind", "id"], set())
            cur.execute("ANALYZE osm_delete")
            deleted = 0
            for name, spec in m["tables"].items():
                idc = P.id_column(spec["columns"])
                for kind in P.KINDS[spec["type"]]:
                    ids = f"SELECT {id_expr(kind, single)} AS i FROM osm_delete WHERE kind = '{kind}'"
                    for t in [name, *gen.of_table(name)]:
                        cur.execute(
                            f"DELETE FROM {P.q(prod)}.{P.q(t)} x USING ({ids}) d WHERE x.{P.q(idc)} = d.i"
                        )
                        deleted += cur.rowcount if t == name else 0
            gen_ids: dict[str, list[int]] = {}
            for name, spec in m["tables"].items():
                geometry = {c["name"] for c in spec["columns"] if P.is_geometry(c)}
                gens = gen.of_table(name)
                for path in self.ob.files(b, f"tables/{name}"):
                    inserted += copy_file(cur, self.ob, path, prod, name, P.copy_columns(spec), geometry)
                    if gens:
                        for batch in self.ob.read(path, ["__id", "__gen"]):
                            for id_, g in zip(
                                batch.column(0).to_pylist(), batch.column(1).to_pylist(), strict=False
                            ):
                                if g:
                                    for gn in gens:
                                        gen_ids.setdefault(gn, []).append(id_)
            for gn in gen.order():  # GeneralizeUpdates, one insert per recorded ID
                ids = gen_ids.get(gn)
                if ids:
                    cur.executemany(gen.insert_sql(prod, gn), [(i,) for i in ids])
            self._set(
                cur, production_batch=b, production_manifest=m, sequence=m["sequence"], osm_time=m["osm_time"]
            )
        log(
            f"batch {b}: sequence {m['sequence']}, deleted {deleted}, inserted {inserted} rows in {time.time() - t0:.1f}s"
        )

    # --- deploy / revert-deploy / remove-backup ------------------------------------

    def _rotate(self, source: str, dest: str, backup: str, names: list[str]) -> None:
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {P.q(dest)}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {P.q(backup)}")
            for name in names:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{P.q(source)}.{P.q(name)}",))
                if not cur.fetchone()[0]:
                    log(f"skipping rotate of {name}, table does not exist in {source}")
                    continue
                cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{P.q(dest)}.{P.q(name)}",))
                if cur.fetchone()[0]:
                    cur.execute(f"DROP TABLE IF EXISTS {P.q(backup)}.{P.q(name)}")
                    cur.execute(f"ALTER TABLE {P.q(dest)}.{P.q(name)} SET SCHEMA {P.q(backup)}")
                cur.execute(f"ALTER TABLE {P.q(source)}.{P.q(name)} SET SCHEMA {P.q(dest)}")
                log(f"rotated {name}: {source} -> {dest} -> {backup}")
            self._after_rotate(cur, self.state(cur), source, dest)

    def _after_rotate(self, cur, st, source, dest):
        s = self.s
        if source == s.schema_import:  # deploy
            self._set(
                cur,
                backup_batch=st["production_batch"],
                backup_manifest=st["production_manifest"],
                production_batch=st["import_batch"],
                production_manifest=st["import_manifest"],
                import_batch=None,
                import_manifest=None,
                sequence=(st["import_manifest"] or {}).get("sequence"),
                osm_time=(st["import_manifest"] or {}).get("osm_time"),
            )
        else:  # revert
            self._set(
                cur,
                import_batch=st["production_batch"],
                import_manifest=st["production_manifest"],
                production_batch=st["backup_batch"],
                production_manifest=st["backup_manifest"],
                backup_batch=None,
                backup_manifest=None,
                sequence=(st["backup_manifest"] or {}).get("sequence"),
                osm_time=(st["backup_manifest"] or {}).get("osm_time"),
            )

    def deploy(self) -> None:
        st = self.state()
        if st["import_manifest"] is None:
            raise SystemExit(f"nothing in schema {self.s.schema_import} to deploy")
        self._rotate(
            self.s.schema_import, self.s.schema, self.s.schema_backup, table_names(st["import_manifest"])
        )

    def revert_deploy(self) -> None:
        st = self.state()
        m = st["backup_manifest"] or st["production_manifest"]
        if m is None:
            raise SystemExit("nothing deployed")
        self._rotate(self.s.schema_backup, self.s.schema, self.s.schema_import, table_names(m))

    def remove_backup(self) -> None:
        st = self.state()
        m = st["production_manifest"] or st["backup_manifest"]
        if m is None:
            return
        with self.conn.transaction(), self.conn.cursor() as cur:
            for name in table_names(m):
                cur.execute(f"DROP TABLE IF EXISTS {P.q(self.s.schema_backup)}.{P.q(name)}")
            self._set(cur, backup_batch=None, backup_manifest=None)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="osm-hadoop-loader", description=__doc__.split("\n\n")[0])
    p.add_argument(
        "command",
        nargs="?",
        default="apply",
        choices=["apply", "deploy", "revert-deploy", "remove-backup", "status"],
    )
    p.add_argument("--loop", action="store_true", help="apply: keep polling for new batches")
    p.add_argument("--interval", type=int, default=30, help="seconds between polls (--loop)")
    args = p.parse_args(argv)
    s = Settings.from_env()
    loader = Loader(s)
    if args.command in ("deploy", "revert-deploy", "remove-backup"):
        with loader.work_lock():
            return {
                "deploy": loader.deploy,
                "revert-deploy": loader.revert_deploy,
                "remove-backup": loader.remove_backup,
            }[args.command]()
    if args.command == "status":
        st = loader.state()
        for k in ("import", "production", "backup"):
            m = st[f"{k}_manifest"]
            st[f"{k}_manifest"] = m and {"kind": m["kind"], "sequence": m["sequence"], "rows": m.get("rows")}
        print(json.dumps(st, indent=2, default=str))
        return
    loader.exclusive()
    log(f"namespace {s.namespace}, outbox {s.webhdfs_url}{s.outbox_path}")
    while True:
        try:
            applied = loader.step()
        except (OSError, psycopg.OperationalError) as e:
            if not args.loop:
                raise
            log(f"error: {e}; retrying")
            applied = False
        if not args.loop:
            if not applied:
                log("nothing to apply")
            return
        if not applied:
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
