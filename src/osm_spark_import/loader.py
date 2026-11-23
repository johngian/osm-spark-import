"""osm-spark-import-loader: applies outbox batches to PostgreSQL.

A standalone process (no Spark, no JVM): it reads the outbox with fsspec
(WebHDFS, or the filesystem of OSM_STORAGE_ROOT; see files.py) and pyarrow,
and writes with COPY (psycopg). The database connection
comes from libpq's PG* environment variables. The SQL comes from
mapping.postgis.

    osm-spark-import-loader [apply] [--loop]   apply new batches:
        snapshot -> schema import: create the tables, COPY, generalized
                    tables, indexes. Not deployed.
        delta    -> production schema, in one transaction: delete the
                    affected elements (and from the generalized tables), COPY
                    the new rows, update the generalized tables. Expired
                    tiles are written to OSM_EXPIRE_DIR first. Deltas wait
                    until their snapshot is deployed.
    osm-spark-import-loader deploy         import -> production -> backup
    osm-spark-import-loader revert-deploy  backup -> production -> import
    osm-spark-import-loader remove-backup  drop the backup tables
    osm-spark-import-loader clean          delete outbox batches no schema needs
    osm-spark-import-loader status

Batches below the oldest batch in a schema are deleted after each change
(the loader needs write access to the outbox).

State: osm_spark_import.loader_state, one row per namespace: which batch is in the
import, production and backup schemas, written in the same transaction as
the data.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import contextlib
import datetime as dt
import io
import json
import logging
import os
import posixpath
import time
from collections.abc import Iterator

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from osm_spark_import import files, outbox
from osm_spark_import.mapping import postgis as P
from osm_spark_import.mapping.writer import REL_ID_OFFSET
from osm_spark_import.settings import Settings, setup_logging

COPY_BATCH = 50_000
STATE = "osm_spark_import.loader_state"

log = logging.getLogger(__name__)


class Outbox:
    """Access to the namespace's outbox."""

    def __init__(self, s: Settings):
        self.fs, self.root = files.url_fs(s, s.outbox)

    def delete(self, batch: int) -> None:
        """Delete a batch."""
        self.fs.rm(outbox.batch_dir(self.root, batch), recursive=True)

    def batches(self) -> list[int]:
        """Numbers of the batches in the outbox, complete or not, ascending."""
        if not self.fs.exists(self.root):
            return []
        names = (posixpath.basename(p.rstrip("/")) for p in self.fs.ls(self.root, detail=False))
        return sorted(int(n) for n in names if n.isdigit())

    def manifest(self, batch: int) -> dict | None:
        """A batch's manifest, or None while it is incomplete."""
        path = f"{outbox.batch_dir(self.root, batch)}/{outbox.MANIFEST}"
        if not self.fs.exists(path):
            return None  # not complete yet
        return json.loads(self.fs.cat(path))

    def files(self, batch: int, part: str) -> list[str]:
        """Parquet data files under a part (e.g. tables/roads) of a batch."""
        d = f"{outbox.batch_dir(self.root, batch)}/{part}"
        if not self.fs.exists(d):
            return []
        return sorted(
            p
            for p in self.fs.find(d)
            if p.endswith(".parquet") and not posixpath.basename(p).startswith((".", "_"))
        )

    def read(self, path: str, columns: list[str]) -> Iterator[pa.RecordBatch]:
        """Record batches of COPY_BATCH rows of a Parquet file."""
        with self.fs.open(path, "rb") as f:
            yield from pq.ParquetFile(f).iter_batches(batch_size=COPY_BATCH, columns=columns)


def to_csv(batch: pa.RecordBatch, geometry: set[str]) -> bytes:
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
    """COPY a Parquet file into a table; returns the number of rows."""
    stmt = (
        f"COPY {P.q(schema)}.{P.q(table)} ({', '.join(P.q(c) for c in columns)}) FROM STDIN WITH (FORMAT csv)"
    )
    n = 0
    with cur.copy(stmt) as cp:
        for b in ob.read(path, columns):
            cp.write(to_csv(b, geometry))
            n += b.num_rows
    return n


def table_names(m: dict) -> list[str]:
    """Names of a manifest's tables, generalized tables included."""
    return list(m["tables"]) + list(m.get("generalized") or {})


def id_expr(kind: str, single_id_space: bool) -> str:
    """The ID written for an element of this kind (on osm_delete.id)."""
    if kind == "w" and single_id_space:
        return "-id"
    if kind == "r":
        return f"({REL_ID_OFFSET} - id)" if single_id_space else "-id"
    return "id"


def obsolete_batches(batches: list[int], st: dict) -> list[int]:
    """Batches older than every batch a schema holds (none while no schema holds one)."""
    held = [st[k] for k in ("import_batch", "production_batch", "backup_batch") if st[k] is not None]
    return [b for b in batches if held and b < min(held)]


def write_expire_file(expire_dir: str, tiles: list[str]) -> str | None:
    """DIR/YYYYMMDD/HHMMSS.mmm.tiles, written via .tiles~ and renamed."""
    if not tiles:
        return None
    now = dt.datetime.now(dt.timezone.utc)
    day = os.path.join(expire_dir, now.strftime("%Y%m%d"))
    os.makedirs(day, exist_ok=True)
    path = os.path.join(day, now.strftime("%H%M%S.") + f"{now.microsecond // 1000:03d}.tiles")
    with open(path + "~", "w") as f:
        f.write("".join(t + "\n" for t in tiles))
    os.rename(path + "~", path)
    return path


class Loader:
    """Applies batches and rotates schemas; keeps its state in STATE."""

    def __init__(self, s: Settings):
        self.s = s
        self.ob = Outbox(s)
        self.conn: psycopg.Connection | None = None
        self.connect()

    def connect(self) -> None:
        """(Re)connect to PostgreSQL and create the state table if needed."""
        if self.conn is not None:
            with contextlib.suppress(psycopg.Error):
                self.conn.close()
        self.conn = psycopg.connect(autocommit=True)
        s = self.s
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS osm_spark_import")
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
                "SELECT pg_try_advisory_lock(hashtext('osm-spark-import-loader:' || %s))", (self.s.namespace,)
            )
            if not cur.fetchone()[0]:
                raise SystemExit(f"another loader is applying batches for namespace {self.s.namespace}")

    @contextlib.contextmanager
    def work_lock(self) -> Iterator[None]:
        """Serializes applying a batch with deploy/revert/remove-backup."""
        key = f"osm-spark-import-work:{self.s.namespace}"
        self.conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (key,))
        try:
            yield
        finally:
            self.conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))

    def state(self) -> dict:
        """The namespace's row of STATE (in the current transaction, if any)."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""SELECT import_batch, import_manifest, production_batch, production_manifest,
                           backup_batch, backup_manifest, sequence, osm_time FROM {STATE} WHERE namespace = %s""",
                (self.s.namespace,),
            )
            return cur.fetchone()

    def save_state(self, cur, **values) -> None:
        """Update columns of the namespace's row of STATE."""
        cols = ", ".join(f"{k} = %s" for k in values)
        params = [Jsonb(v) if k.endswith("_manifest") and v is not None else v for k, v in values.items()]
        cur.execute(
            f"UPDATE {STATE} SET {cols}, updated = now() WHERE namespace = %s", (*params, self.s.namespace)
        )

    # --- applying batches --------------------------------------------------------

    def step(self) -> bool:
        """Apply what is ready, under the work lock; True if anything was applied."""
        with self.work_lock():
            return self.apply_next()

    def apply_next(self) -> bool:
        """Load the newest complete snapshot, then the deltas based on production."""
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
        """Load a snapshot batch into the import schema (tables, COPY, generalized, indexes)."""
        s, b = self.s, m["batch"]
        t0 = time.time()
        imp = s.schema_import
        log.info(f"batch {b}: snapshot (sequence {m['sequence']}) into schema {imp}")
        gen = P.Generalized(m["tables"], m.get("generalized") or {})
        with self.conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {P.q(imp)}")
            for name, spec in m["tables"].items():  # empty tables
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
            # generalized tables, sources first
            for name in gen.order():
                run([f"DROP TABLE IF EXISTS {P.q(imp)}.{P.q(name)}", gen.create_sql(imp, name)])
            # indexes
            jobs = [P.index_sqls(imp, n, spec["columns"], False) for n, spec in m["tables"].items()]
            jobs += [P.index_sqls(imp, n, m["tables"][gen.root(n)]["columns"], True) for n in gen.order()]
            list(pool.map(run, jobs))
        with self.conn.transaction(), self.conn.cursor() as cur:
            self.save_state(cur, import_batch=b, import_manifest=m)
        log.info(f"batch {b}: {sum(counts.values())} rows in {time.time() - t0:.1f}s; deploy to make it live")

    def apply_delta(self, m: dict, st: dict) -> None:
        """Apply a delta batch to the production schema in one transaction."""
        s, b = self.s, m["batch"]
        prod = s.schema
        current = st["production_manifest"] or {}
        if current.get("tables") != m["tables"] or current.get("generalized") != m.get("generalized"):
            raise SystemExit(
                f"batch {b} has other tables than schema {prod} (mapping changed?): "
                "run `osm-spark-import rebuild` and deploy the new snapshot"
            )
        t0 = time.time()
        gen = P.Generalized(m["tables"], m.get("generalized") or {})
        single = m.get("single_id_space", False)

        tiles = []
        for path in self.ob.files(b, "tiles"):
            for batch in self.ob.read(path, ["tile"]):
                tiles += batch.column(0).to_pylist()
        if tiles and s.expire_dir:  # before the commit: tiles are never lost
            log.info(f"expired tiles: {write_expire_file(s.expire_dir, sorted(tiles))}")

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
            for gn in gen.order():  # one insert per recorded ID, sources first
                ids = gen_ids.get(gn)
                if ids:
                    cur.executemany(gen.insert_sql(prod, gn), [(i,) for i in ids])
            self.save_state(
                cur, production_batch=b, production_manifest=m, sequence=m["sequence"], osm_time=m["osm_time"]
            )
        log.info(
            f"batch {b}: sequence {m['sequence']}, deleted {deleted}, inserted {inserted} rows in {time.time() - t0:.1f}s"
        )

    # --- deploy / revert-deploy / remove-backup ------------------------------------

    def rotate_tables(self, source: str, dest: str, backup: str, names: list[str]) -> None:
        """Move tables source -> dest -> backup in one transaction and update the state."""
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {P.q(dest)}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {P.q(backup)}")
            for name in names:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{P.q(source)}.{P.q(name)}",))
                if not cur.fetchone()[0]:
                    log.info(f"skipping rotate of {name}, table does not exist in {source}")
                    continue
                cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{P.q(dest)}.{P.q(name)}",))
                if cur.fetchone()[0]:
                    cur.execute(f"DROP TABLE IF EXISTS {P.q(backup)}.{P.q(name)}")
                    cur.execute(f"ALTER TABLE {P.q(dest)}.{P.q(name)} SET SCHEMA {P.q(backup)}")
                cur.execute(f"ALTER TABLE {P.q(source)}.{P.q(name)} SET SCHEMA {P.q(dest)}")
                log.info(f"rotated {name}: {source} -> {dest} -> {backup}")
            self.after_rotate(cur, self.state(), source, dest)

    def after_rotate(self, cur, st: dict, source: str, dest: str) -> None:
        """Shift the batches recorded per schema after a deploy or revert."""
        s = self.s
        if source == s.schema_import:  # deploy
            self.save_state(
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
            self.save_state(
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
        """Make the import schema live: import -> production -> backup."""
        st = self.state()
        if st["import_manifest"] is None:
            raise SystemExit(f"nothing in schema {self.s.schema_import} to deploy")
        self.rotate_tables(
            self.s.schema_import, self.s.schema, self.s.schema_backup, table_names(st["import_manifest"])
        )

    def revert_deploy(self) -> None:
        """Undo a deploy: backup -> production -> import."""
        st = self.state()
        m = st["backup_manifest"] or st["production_manifest"]
        if m is None:
            raise SystemExit("nothing deployed")
        self.rotate_tables(self.s.schema_backup, self.s.schema, self.s.schema_import, table_names(m))

    def remove_backup(self) -> None:
        """Drop the backup schema's tables."""
        st = self.state()
        m = st["production_manifest"] or st["backup_manifest"]
        if m is None:
            return
        with self.conn.transaction(), self.conn.cursor() as cur:
            for name in table_names(m):
                cur.execute(f"DROP TABLE IF EXISTS {P.q(self.s.schema_backup)}.{P.q(name)}")
            self.save_state(cur, backup_batch=None, backup_manifest=None)

    def clean(self) -> None:
        """Delete the outbox batches no schema needs any more."""
        for b in obsolete_batches(self.ob.batches(), self.state()):
            log.info(f"deleting batch {b}")
            self.ob.delete(b)


def main(argv: list[str] | None = None) -> None:
    """Entry point of osm-spark-import-loader."""
    p = argparse.ArgumentParser(prog="osm-spark-import-loader", description=__doc__.split("\n\n")[0])
    p.add_argument(
        "command",
        nargs="?",
        default="apply",
        choices=["apply", "deploy", "revert-deploy", "remove-backup", "clean", "status"],
    )
    p.add_argument("--loop", action="store_true", help="apply: keep polling for new batches")
    p.add_argument("--interval", type=int, default=30, help="seconds between polls (--loop)")
    args = p.parse_args(argv)
    setup_logging()
    s = Settings.from_env()
    loader = Loader(s)
    if args.command in ("deploy", "revert-deploy", "remove-backup", "clean"):
        with loader.work_lock():
            {
                "deploy": loader.deploy,
                "revert-deploy": loader.revert_deploy,
                "remove-backup": loader.remove_backup,
                "clean": lambda: None,
            }[args.command]()
            loader.clean()
        return
    if args.command == "status":
        print(json.dumps(status(loader.state()), indent=2, default=str))
        return
    loader.exclusive()
    log.info(f"namespace {s.namespace}, outbox {s.outbox}")
    failures = 0
    while True:
        try:
            if failures:
                loader.connect()
                loader.exclusive()
            applied = loader.step()
            if applied:
                with loader.work_lock():
                    loader.clean()
            failures = 0
        except (OSError, psycopg.OperationalError) as e:
            if not args.loop:
                raise
            failures += 1
            delay = min(args.interval * 2 ** (failures - 1), 600)
            log.warning(f"error: {e}; reconnecting in {delay}s")
            time.sleep(delay)
            continue
        if not args.loop:
            if not applied:
                log.info("nothing to apply")
            return
        if not applied:
            time.sleep(args.interval)


def status(st: dict) -> dict:
    """The loader state for printing: manifests summarized, with the production lag."""
    out = dict(st)
    for k in ("import", "production", "backup"):
        m = out[f"{k}_manifest"]
        out[f"{k}_manifest"] = m and {"kind": m["kind"], "sequence": m["sequence"], "rows": m.get("rows")}
    if st["osm_time"] is not None:
        out["seconds_behind"] = int((dt.datetime.now(dt.timezone.utc) - st["osm_time"]).total_seconds())
    return out


if __name__ == "__main__":
    main()
