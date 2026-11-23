"""run: replication diffs -> delta batches.

One run handles a batch of consecutive sequences:

1. fetch: download the .osc.gz files (pyosmium's ReplicationServer, which
   retries) and decode them with pyosmium.
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

import logging
import os
import tempfile
import time

import osmium
import pyarrow as pa
import pyarrow.parquet as pq
from osmium.replication.server import ReplicationServer
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from osm_spark_import import osm, outbox, pipeline, state, tables
from osm_spark_import.settings import Settings
from osm_spark_import.spark import Storage, conf_scope, session

log = logging.getLogger(__name__)

# Dynamic partition pruning fails on some of these plans over Iceberg
# snapshots ("dynamicpruning ... has not finished", Spark 3.5).
RUN_CONF = {"spark.sql.optimizer.dynamicPartitionPruning.enabled": "false"}

CHANGE = pa.schema(
    [
        ("a", pa.string()),  # d: deleted, m: created or modified
        ("seq", pa.int64()),
        ("pos", pa.int64()),
        ("version", pa.int64()),
        ("k", pa.string()),
        ("id", pa.int64()),
        ("lon", pa.int32()),
        ("lat", pa.int32()),
        ("tags", osm.TAGS),
        ("refs", pa.list_(pa.int64())),
        ("members", pa.list_(osm.MEMBER)),
    ]
)


class Changes(osmium.SimpleHandler):
    """Collects the changes of one diff as CHANGE columns."""

    def __init__(self, seq: int):
        super().__init__()
        self.seq = seq
        self.rows: dict[str, list] = {n: [] for n in CHANGE.names}

    def add(self, o, k, lon=None, lat=None, refs=None, members=None):
        r = self.rows
        r["a"].append("d" if o.deleted else "m")
        r["seq"].append(self.seq)
        r["pos"].append(len(r["seq"]))
        r["version"].append(o.version)
        r["k"].append(k)
        r["id"].append(o.id)
        r["lon"].append(lon)
        r["lat"].append(lat)
        r["tags"].append([(t.k, t.v) for t in o.tags] if len(o.tags) else None)
        r["refs"].append(refs)
        r["members"].append(members)

    def node(self, n):
        loc = n.location
        if n.deleted or not loc.valid():
            self.add(n, "n")
        else:
            self.add(n, "n", loc.x, loc.y)

    def way(self, w):
        self.add(w, "w", refs=None if w.deleted else [r.ref for r in w.nodes])

    def relation(self, r):
        self.add(r, "r", members=None if r.deleted else [(m.type, m.ref, m.role) for m in r.members])


def decode_osc(data: bytes, seq: int, out: str) -> None:
    """Decode a diff (.osc.gz bytes) into a CHANGE Parquet file."""
    h = Changes(seq)
    h.apply_buffer(data, "osc.gz")
    pq.write_table(pa.Table.from_pydict(h.rows, schema=CHANGE), out)


def run(s: Settings, loop: bool = False, max_diffs: int | None = None, until: int | None = None) -> None:
    """Apply replication diffs in batches of max_diffs, up to until (or the latest).

    --loop keeps the Spark application alive between batches: for the docker
    setup; on a shared cluster, schedule single runs instead.
    """
    s.load_mapping()  # fail early on a bad mapping
    spark = session(f"osm-spark-import run {s.namespace}")
    fs = Storage(spark)
    with state.locked(spark, fs, s, "run"), ReplicationServer(s.replication_url) as server:
        with conf_scope(spark, RUN_CONF):
            apply_diffs(spark, fs, s, server, loop, max_diffs or s.max_diffs, until)


def apply_diffs(
    spark: SparkSession,
    fs: Storage,
    s: Settings,
    server: ReplicationServer,
    loop: bool,
    max_diffs: int,
    until: int | None,
) -> None:
    """The batches of run(), under its lock."""
    while True:
        st = state.load(fs, s)
        if st.get("current_run"):
            recover(spark, fs, s, st)
            st = state.load(fs, s)
        if not st.get("batch"):
            raise SystemExit("nothing imported yet: run `osm-spark-import import` first")
        info = server.get_state_info()
        if info is None:
            raise SystemExit(f"cannot read the replication state of {s.replication_url}")
        latest = info.sequence if until is None else min(info.sequence, until)
        if latest <= st["sequence"]:
            if not loop:
                log.info(f"up to date (sequence {st['sequence']})")
                return
            time.sleep(60)
            continue
        seqs = list(range(st["sequence"] + 1, min(latest, st["sequence"] + max_diffs) + 1))
        apply_batch(spark, fs, s, st, server, seqs)
        if not loop and seqs[-1] >= latest:
            return


def apply_batch(
    spark: SparkSession, fs: Storage, s: Settings, st: dict, server: ReplicationServer, seqs: list[int]
) -> None:
    """Steps 1-6 of the module docstring for consecutive sequences seqs."""
    mp, mapping_text = s.load_mapping()
    batch = st["batch"] + 1
    stage = f"{s.staging}/{outbox.batch_name(batch)}"
    log.info(f"batch {batch}: sequences {seqs[0]}..{seqs[-1]}")

    # 1. fetch
    osm_time = None
    with tempfile.TemporaryDirectory() as tmp:
        local = []
        for seq in seqs:
            log.info(f"GET {server.get_diff_url(seq)}")
            decode_osc(server.get_diff_block(seq), seq, os.path.join(tmp, f"{seq:012d}.parquet"))
            info = server.get_state_info(seq)
            osm_time = info.timestamp.isoformat() if info else osm_time
            local.append(os.path.join(tmp, f"{seq:012d}.parquet"))
        fs.put_files(local, stage + "/changes")

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
    state.save(fs, s, st)
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
    pipeline.write_rows(
        pipeline.build(spark, s, units(old, changed), mapping_text, s.expire_zoom, deleter_expire=True),
        stage + "/old",
    )

    # 4. merge
    merge_cache(spark, s, final, old, stage)

    # 5b. new versions: rows and tiles to expire
    new = {name: spark.table(s.table(name)) for name in ("nodes", "ways", "relations")}
    pipeline.write_rows(pipeline.build(spark, s, units(new), mapping_text, s.expire_zoom), stage + "/rows")

    # 6. delta batch
    deletes = (
        aff["nodes"]
        .select(F.lit("n").alias("kind"), "id")
        .unionByName(aff["ways"].select(F.lit("w").alias("kind"), "id"))
        .unionByName(aff["rels"].select(F.lit("r").alias("kind"), "id"))
    )
    rows = pipeline.read_rows(spark, stage + "/rows").unionByName(pipeline.read_rows(spark, stage + "/old"))
    manifest = pipeline.write_batch(
        spark, fs, s, mp, batch, "delta", st["batch"], rows, deletes, seqs[-1], osm_time
    )
    log.info(
        f"batch {batch}: {manifest['deletes']} elements to delete, {sum(manifest['rows'].values())} rows "
        f"to insert, {manifest['tiles']} tiles to expire"
    )
    finish(fs, s, st)


def finish(fs: Storage, s: Settings, st: dict) -> None:
    """Record the current run as done and remove its staging directory."""
    cr = st["current_run"]
    st.update(batch=cr["batch"], sequence=cr["seqs"][-1], osm_time=cr["time"], current_run=None)
    state.save(fs, s, st)
    fs.delete(cr["stage"])


def recover(spark: SparkSession, fs: Storage, s: Settings, st: dict) -> None:
    """Finish or roll back an interrupted run (see the module docstring)."""
    cr = st["current_run"]
    if cr["kind"] != "delta":
        raise SystemExit("an import was interrupted: run `osm-spark-import import` (or `rebuild`) again")
    if fs.exists(f"{outbox.batch_dir(s.outbox, cr['batch'])}/{outbox.MANIFEST}"):
        log.info(f"batch {cr['batch']} was complete; finishing it")
        finish(fs, s, st)
        return
    log.info(f"batch {cr['batch']} was interrupted; rolling back the cache")
    tables.rollback(spark, s, cr["snapshots"])
    fs.delete(outbox.batch_dir(s.outbox, cr["batch"]))
    fs.delete(cr["stage"])
    st["current_run"] = None
    state.save(fs, s, st)


def pair_delta(old_pairs: DataFrame, new_pairs: DataFrame, keys: list[str]) -> DataFrame:
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


INDEX_KEYS = {
    "way_nodes": ["node_id", "way_id"],
    "rel_members": ["member_type", "member_id", "rel_id"],
}


def merge_elements(
    spark: SparkSession, s: Settings, src: DataFrame, table: str, cols: tuple[str, ...]
) -> None:
    """MERGE collapsed changes into an element table: a = 'd' deletes, anything else upserts."""
    src.createOrReplaceTempView(f"src_{table}")
    sets = ", ".join(f"t.{c} = s.{c}" for c in cols)
    names = ", ".join(cols)
    values = ", ".join(f"s.{c}" for c in cols)
    spark.sql(
        f"""
        MERGE INTO {s.table(table)} t USING src_{table} s ON t.id = s.id
        WHEN MATCHED AND s.a = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET {sets}
        WHEN NOT MATCHED AND s.a != 'd' THEN INSERT (id, {names}) VALUES (s.id, {values})"""
    )


def merge_pairs(spark: SparkSession, s: Settings, src: DataFrame, table: str, keys: list[str]) -> None:
    """MERGE pair_delta output into an index table."""
    src.createOrReplaceTempView(f"src_{table}")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    names = ", ".join(keys)
    values = ", ".join(f"s.{k}" for k in keys)
    spark.sql(
        f"""
        MERGE INTO {s.table(table)} t USING src_{table} s ON {on}
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN NOT MATCHED AND s.op = 'i' THEN INSERT ({names}) VALUES ({values})"""
    )


def merge_cache(spark: SparkSession, s: Settings, final: DataFrame, old: dict, stage: str) -> None:
    """Apply collapsed changes to the cache and index tables (one Iceberg commit each)."""
    ways = final.filter("k = 'w'")
    rels = final.filter("k = 'r'")
    # staged first: the index deltas read the old tables before they change
    src = {
        name: final.filter(F.col("k") == k).select("id", "a", *cols)
        for name, (k, cols) in tables.ELEMENTS.items()
    }
    src["way_nodes"] = pair_delta(
        tables.way_nodes(old["ways"].join(ways.select("id"), "id", "left_semi")),
        tables.way_nodes(ways.filter("a != 'd'")),
        INDEX_KEYS["way_nodes"],
    )
    src["rel_members"] = pair_delta(
        tables.rel_members(old["relations"].join(rels.select("id"), "id", "left_semi")),
        tables.rel_members(rels.filter("a != 'd'")),
        INDEX_KEYS["rel_members"],
    )
    for name, df in src.items():
        df.write.mode("overwrite").parquet(f"{stage}/merge_{name}")
    src = {name: spark.read.parquet(f"{stage}/merge_{name}") for name in src}

    for name, (_, cols) in tables.ELEMENTS.items():
        merge_elements(spark, s, src[name], name, cols)
    for name, keys in INDEX_KEYS.items():
        merge_pairs(spark, s, src[name], name, keys)
