"""run: replication diffs -> delta batches.

One run handles a batch of consecutive sequences:

1. fetch: download the .osc.gz files (pyosmium's ReplicationServer) and
   decode them with pyosmium.
2. collapse: keep the last version of each element. Every affected element
   is rebuilt from the cache, so the final state of a batch gives the same
   rows as applying its files one by one.
3. affected: changed elements, ways using changed nodes, relations using
   changed nodes or affected ways (the indexes before the update).
4. merge: update the cache and index tables (MERGE INTO, one Iceberg commit
   each).
5. build: the affected elements from the cache before the update (tiles to
   expire) and after it (rows to insert, tiles to expire).
6. write a delta batch: delete every affected element, insert the new rows.

Recovery: the run is recorded with the cache snapshots before step 4. If
its batch manifest exists, the run is complete and only the state is left
to update; otherwise the cache tables are rolled back to the snapshots and
the partial batch is removed.
"""

from __future__ import annotations

import os
import time

import osmium
import pyarrow as pa
import pyarrow.parquet as pq
from osmium.replication.server import ReplicationServer
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import outbox, pipeline, state, tables
from .imports import download, log
from .mapping import geom as igeom
from .settings import Settings
from .spark import HDFS, session

CHANGE = pa.schema(
    [
        ("a", pa.string()),  # d: deleted, m: created or modified
        ("seq", pa.int64()),
        ("pos", pa.int64()),
        ("version", pa.int64()),
        ("k", pa.string()),
        ("id", pa.int64()),
        ("qlon", pa.int64()),
        ("qlat", pa.int64()),
        ("tags", pa.map_(pa.string(), pa.string())),
        ("refs", pa.list_(pa.int64())),
        ("members", pa.list_(pa.struct([("type", pa.string()), ("ref", pa.int64()), ("role", pa.string())]))),
    ]
)


class _Changes(osmium.SimpleHandler):
    def __init__(self, seq: int):
        super().__init__()
        self.seq = seq
        self.rows: dict[str, list] = {n: [] for n in CHANGE.names}

    def _add(self, o, k, qlon=None, qlat=None, refs=None, members=None):
        r = self.rows
        r["a"].append("d" if o.deleted else "m")
        r["seq"].append(self.seq)
        r["pos"].append(len(r["seq"]))
        r["version"].append(o.version)
        r["k"].append(k)
        r["id"].append(o.id)
        r["qlon"].append(qlon)
        r["qlat"].append(qlat)
        r["tags"].append([(t.k, t.v) for t in o.tags] if len(o.tags) else None)
        r["refs"].append(refs)
        r["members"].append(members)

    def node(self, n):
        loc = n.location
        if n.deleted or not loc.valid():
            self._add(n, "n")
        else:
            self._add(n, "n", igeom.q_osc(loc.x), igeom.q_osc(loc.y))

    def way(self, w):
        self._add(w, "w", refs=None if w.deleted else [r.ref for r in w.nodes])

    def relation(self, r):
        self._add(r, "r", members=None if r.deleted else [(m.type, m.ref, m.role) for m in r.members])


def decode_osc(path: str, seq: int, out: str) -> None:
    h = _Changes(seq)
    h.apply_file(path)
    pq.write_table(pa.Table.from_pydict(h.rows, schema=CHANGE), out)


def run(s: Settings, loop: bool = False, max_diffs: int | None = None, until: int | None = None) -> None:
    s.check_mapping()
    spark = session(f"osm-hadoop run {s.namespace}")
    # Dynamic partition pruning fails on some of these plans over Iceberg
    # snapshots ("dynamicpruning ... has not finished", Spark 3.5).
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
    hdfs = HDFS(spark)
    server = ReplicationServer(s.replication_url)
    max_diffs = max_diffs or s.max_diffs

    while True:
        st = state.load(hdfs, s)
        if st.get("current_run"):
            recover(spark, hdfs, s, st)
            st = state.load(hdfs, s)
        if not st.get("batch"):
            raise SystemExit("nothing imported yet: run `osm-hadoop import` first")
        info = server.get_state_info()
        if info is None:
            raise SystemExit(f"cannot read the replication state of {s.replication_url}")
        latest = info.sequence if until is None else min(info.sequence, until)
        if latest <= st["sequence"]:
            if not loop:
                log(f"up to date (sequence {st['sequence']})")
                return
            time.sleep(60)
            continue
        seqs = list(range(st["sequence"] + 1, min(latest, st["sequence"] + max_diffs) + 1))
        apply_batch(spark, hdfs, s, st, server, seqs)
        if not loop and seqs[-1] >= latest:
            return


def apply_batch(spark: SparkSession, hdfs: HDFS, s: Settings, st: dict, server, seqs: list[int]) -> None:
    mp, mapping_text = s.load_mapping()
    batch = st["batch"] + 1
    stage = f"{s.hdfs_root}/staging/{s.namespace}-{batch}"
    log(f"batch {batch}: sequences {seqs[0]}..{seqs[-1]}")

    # 1. fetch
    local = []
    osm_time = None
    for seq in seqs:
        url = server.get_diff_url(seq)
        rel = (
            url[len(s.replication_url.rstrip("/")) + 1 :]
            if url.startswith(s.replication_url.rstrip("/"))
            else str(seq)
        )
        osc = download(url, os.path.join(s.work_dir, "diffs", s.namespace, os.path.dirname(rel)))
        info = server.get_state_info(seq)
        osm_time = info.timestamp.isoformat() if info else osm_time
        out = osc[: -len(".osc.gz")] + ".changes.parquet"
        decode_osc(osc, seq, out)
        local.append(out)
    hdfs.put_files(local, stage + "/changes")

    # 2. collapse
    last = Window.partitionBy("k", "id").orderBy(F.desc("version"), F.desc("seq"), F.desc("pos"))
    (
        spark.read.parquet(stage + "/changes")
        .withColumn("rn", F.row_number().over(last))
        .filter("rn = 1")
        .drop("rn")
        .write.mode("overwrite")
        .parquet(stage + "/final")
    )
    final = spark.read.parquet(stage + "/final")

    snaps = tables.snapshots(spark, s)
    st["current_run"] = {
        "kind": "delta",
        "batch": batch,
        "base": st["batch"],
        "stage": stage,
        "snapshots": snaps,
        "seqs": seqs,
        "time": osm_time,
    }
    state.save(hdfs, s, st)
    old = {name: tables.at_snapshot(spark, s, name, snaps[name]) for name in tables.CACHE_TABLES}

    # 3. affected
    nodes_changed = final.filter("k = 'n'").select("id")
    ways_changed = final.filter("k = 'w'").select("id")
    rels_changed = final.filter("k = 'r'").select("id")
    ways_dep = (
        old["way_nodes"]
        .join(nodes_changed.withColumnRenamed("id", "node_id"), "node_id", "left_semi")
        .select(F.col("way_id").alias("id"))
    )
    ways_aff = ways_changed.unionByName(ways_dep).distinct()
    rm = old["rel_members"]
    rels_dep = (
        rm.filter("member_type = 'n'")
        .join(nodes_changed.withColumnRenamed("id", "member_id"), "member_id", "left_semi")
        .unionByName(
            rm.filter("member_type = 'w'").join(
                ways_aff.withColumnRenamed("id", "member_id"), "member_id", "left_semi"
            )
        )
        .select(F.col("rel_id").alias("id"))
    )
    rels_aff = rels_changed.unionByName(rels_dep).distinct()
    for name, df in (("nodes", nodes_changed), ("ways", ways_aff), ("rels", rels_aff)):
        df.write.mode("overwrite").parquet(f"{stage}/aff_{name}")
    aff = {k: spark.read.parquet(f"{stage}/aff_{k}") for k in ("nodes", "ways", "rels")}

    def units(t, changed=None):
        return pipeline.work_units(
            mp,
            t["nodes"],
            t["ways"],
            t["relations"],
            node_ids=aff["nodes"],
            way_ids=aff["ways"],
            rel_ids=aff["rels"],
            changed=changed,
        )

    # 5a. old versions: tiles to expire
    changed = {"n": nodes_changed, "w": ways_changed, "r": rels_changed}
    (
        pipeline.build(spark, s, units(old, changed), mapping_text, s.expire_zoom, deleter_expire=True)
        .write.mode("overwrite")
        .parquet(stage + "/old")
    )

    # 4. merge
    merge_cache(spark, s, final, old, stage)

    # 5b. new versions: rows and tiles to expire
    new = {name: spark.table(s.table(name)) for name in ("nodes", "ways", "relations")}
    pipeline.build(spark, s, units(new), mapping_text, s.expire_zoom).write.mode("overwrite").parquet(
        stage + "/rows"
    )

    # 6. delta batch
    deletes = (
        aff["nodes"]
        .select(F.lit("n").alias("kind"), "id")
        .unionByName(aff["ways"].select(F.lit("w").alias("kind"), "id"))
        .unionByName(aff["rels"].select(F.lit("r").alias("kind"), "id"))
    )
    rows = spark.read.parquet(stage + "/rows").unionByName(spark.read.parquet(stage + "/old"))
    manifest = pipeline.write_batch(
        spark, hdfs, s, mp, batch, "delta", st["batch"], rows, deletes, seqs[-1], osm_time
    )
    log(
        f"batch {batch}: {manifest['deletes']} elements to delete, {sum(manifest['rows'].values())} rows "
        f"to insert, {manifest['tiles']} tiles to expire"
    )
    finish(hdfs, s, st)


def finish(hdfs: HDFS, s: Settings, st: dict) -> None:
    cr = st["current_run"]
    st.update(batch=cr["batch"], sequence=cr["seqs"][-1], osm_time=cr["time"], current_run=None)
    state.save(hdfs, s, st)
    hdfs.delete(cr["stage"])


def recover(spark: SparkSession, hdfs: HDFS, s: Settings, st: dict) -> None:
    cr = st["current_run"]
    if cr["kind"] != "delta":
        raise SystemExit("an import was interrupted: run `osm-hadoop import` (or `rebuild`) again")
    if hdfs.exists(f"{outbox.batch_dir(s.outbox, cr['batch'])}/{outbox.MANIFEST}"):
        log(f"batch {cr['batch']} was complete; finishing it")
        finish(hdfs, s, st)
        return
    log(f"batch {cr['batch']} was interrupted; rolling back the cache")
    tables.rollback(spark, s, cr["snapshots"])
    hdfs.delete(outbox.batch_dir(s.outbox, cr["batch"]))
    hdfs.delete(cr["stage"])
    st["current_run"] = None
    state.save(hdfs, s, st)


def _pair_delta(old_pairs: DataFrame, new_pairs: DataFrame, keys: list[str]) -> DataFrame:
    """Index rows to delete (op d) and insert (op i); pairs in both stay."""
    d = old_pairs.distinct().withColumn("w", F.lit(-1))
    i = new_pairs.distinct().withColumn("w", F.lit(1))
    return (
        d.unionByName(i)
        .groupBy(*keys)
        .agg(F.sum("w").alias("w"))
        .filter("w != 0")
        .select(*keys, F.when(F.col("w") < 0, "d").otherwise("i").alias("op"))
    )


def _merge(spark: SparkSession, s: Settings, src: DataFrame, table: str, sql: str) -> None:
    view = f"src_{table}"
    src.createOrReplaceTempView(view)
    spark.sql(sql.format(t=s.table(table), s=view))


def merge_cache(spark: SparkSession, s: Settings, final: DataFrame, old: dict, stage: str) -> None:
    ways = final.filter("k = 'w'")
    rels = final.filter("k = 'r'")
    # read the old tables before changing them
    src = {
        "nodes": final.filter("k = 'n'").select("id", "a", "qlon", "qlat", "tags"),
        "ways": ways.select("id", "a", "tags", "refs"),
        "relations": rels.select("id", "a", "tags", "members"),
        "way_nodes": _pair_delta(
            tables.way_nodes(old["ways"].join(ways.select("id"), "id", "left_semi")),
            tables.way_nodes(ways.filter("a != 'd'")),
            ["node_id", "way_id"],
        ),
        "rel_members": _pair_delta(
            tables.rel_members(old["relations"].join(rels.select("id"), "id", "left_semi")),
            tables.rel_members(rels.filter("a != 'd'")),
            ["member_type", "member_id", "rel_id"],
        ),
    }
    for name, df in src.items():
        df.write.mode("overwrite").parquet(f"{stage}/merge_{name}")
    src = {name: spark.read.parquet(f"{stage}/merge_{name}") for name in src}

    _merge(
        spark,
        s,
        src["nodes"],
        "nodes",
        """
        MERGE INTO {t} t USING {s} s ON t.id = s.id
        WHEN MATCHED AND s.a = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.qlon = s.qlon, t.qlat = s.qlat, t.tags = s.tags
        WHEN NOT MATCHED AND s.a != 'd' THEN INSERT (id, qlon, qlat, tags) VALUES (s.id, s.qlon, s.qlat, s.tags)""",
    )
    _merge(
        spark,
        s,
        src["ways"],
        "ways",
        """
        MERGE INTO {t} t USING {s} s ON t.id = s.id
        WHEN MATCHED AND s.a = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.tags = s.tags, t.refs = s.refs
        WHEN NOT MATCHED AND s.a != 'd' THEN INSERT (id, tags, refs) VALUES (s.id, s.tags, s.refs)""",
    )
    _merge(
        spark,
        s,
        src["relations"],
        "relations",
        """
        MERGE INTO {t} t USING {s} s ON t.id = s.id
        WHEN MATCHED AND s.a = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.tags = s.tags, t.members = s.members
        WHEN NOT MATCHED AND s.a != 'd' THEN INSERT (id, tags, members) VALUES (s.id, s.tags, s.members)""",
    )
    _merge(
        spark,
        s,
        src["way_nodes"],
        "way_nodes",
        """
        MERGE INTO {t} t USING {s} s ON t.node_id = s.node_id AND t.way_id = s.way_id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN NOT MATCHED AND s.op = 'i' THEN INSERT (node_id, way_id) VALUES (s.node_id, s.way_id)""",
    )
    _merge(
        spark,
        s,
        src["rel_members"],
        "rel_members",
        """
        MERGE INTO {t} t USING {s} s
          ON t.member_type = s.member_type AND t.member_id = s.member_id AND t.rel_id = s.rel_id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN NOT MATCHED AND s.op = 'i' THEN INSERT (member_type, member_id, rel_id)
          VALUES (s.member_type, s.member_id, s.rel_id)""",
    )
