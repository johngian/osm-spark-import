"""import and rebuild: snapshot batches."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from osm_spark_import import chunk, diffs, outbox, pipeline, state, tables
from osm_spark_import.settings import Settings
from osm_spark_import.spark import Storage, session

log = logging.getLogger(__name__)


def stage_dir(s: Settings, batch: int) -> str:
    """Staging directory of a run."""
    return f"{s.staging}/{outbox.batch_name(batch)}"


def begin_run(fs: Storage, s: Settings, st: dict, kind: str) -> tuple[int, str]:
    """Record a new run in the state, discarding an interrupted one: (batch, stage dir)."""
    if st.get("current_run"):
        # a snapshot replaces everything an interrupted run left
        cr = st["current_run"]
        log.info(f"discarding the interrupted run {cr['kind']} batch {cr['batch']}")
        fs.delete(cr["stage"])
        fs.delete(outbox.batch_dir(s.outbox, cr["batch"]))
    batch = st.get("batch", 0) + 1
    stage = stage_dir(s, batch)
    st["current_run"] = {"kind": kind, "batch": batch, "stage": stage}
    state.save(fs, s, st)
    return batch, stage


def write_snapshot(
    spark: SparkSession,
    fs: Storage,
    s: Settings,
    mp,
    mapping_text: str,
    batch: int,
    stage: str,
    seq: int,
    osm_time: str | None,
) -> dict:
    """Build every row from the cache and write them as a snapshot batch; returns its manifest."""
    log.info("building rows")
    nodes, ways, rels = (spark.table(s.table(name)) for name in ("nodes", "ways", "relations"))
    units = pipeline.work_units(mp, nodes, ways, rels)
    pipeline.write_rows(pipeline.build(spark, s, units, mapping_text), stage + "/rows")
    log.info(f"writing batch {batch}")
    manifest = pipeline.write_batch(
        spark,
        fs,
        s,
        mp,
        batch,
        "snapshot",
        None,
        pipeline.read_rows(spark, stage + "/rows"),
        None,
        seq,
        osm_time,
    )
    for name, n in manifest["rows"].items():
        log.info(f"  {name}: {n} rows")
    return manifest


def run_import(s: Settings, chunks: str, sequence: int | None = None) -> None:
    """Import PBF chunks (written by `chunk`): decode them into the cache, write a snapshot batch."""
    mp, mapping_text = s.load_mapping()
    spark = session(f"osm-spark-import import {s.namespace}")
    fs = Storage(spark)
    chunks = chunks.rstrip("/")
    info = fs.read_json(f"{chunks}/{chunk.MANIFEST}")
    if info is None:
        raise SystemExit(f"{chunks}: no {chunk.MANIFEST} (missing or incomplete chunks; run `chunk`)")
    seq = sequence if sequence is not None else info["sequence"]
    osm_time = info.get("osm_time")
    log.info(f"{info['chunks']} chunks of {info['source']}: data till {osm_time}, diffs after sequence {seq}")

    tables.ensure_namespace(spark, s)
    with state.locked(spark, fs, s, "import"):
        st = state.load(fs, s)
        batch, stage = begin_run(fs, s, st, "snapshot")
        log.info("reading the chunks into the cache")
        pipeline.decode_chunks_to(spark, chunks, info["max_chunk_bytes"], stage + "/elements")
        tables.write_cache(s, spark.read.parquet(stage + "/elements"))
        write_snapshot(spark, fs, s, mp, mapping_text, batch, stage, seq, osm_time)
        st.update(
            batch=batch,
            sequence=seq,
            osm_time=osm_time,
            current_run=None,
            **{"import": {"batch": batch, "chunks": chunks, "sequence": seq, "time": osm_time}},
        )
        state.save(fs, s, st)
        fs.delete(stage)
    log.info("done")


def rebuild(s: Settings) -> None:
    """A snapshot batch from the cache, e.g. after a mapping change."""
    mp, mapping_text = s.load_mapping()
    spark = session(f"osm-spark-import rebuild {s.namespace}")
    fs = Storage(spark)
    with state.locked(spark, fs, s, "rebuild"):
        st = state.load(fs, s)
        if not st.get("batch"):
            raise SystemExit("nothing imported yet")
        if (st.get("current_run") or {}).get("kind") == "delta":
            diffs.recover(spark, fs, s, st)  # the cache must match the state's sequence
            st = state.load(fs, s)
        seq, osm_time = st["sequence"], st.get("osm_time")
        batch, stage = begin_run(fs, s, st, "snapshot")
        write_snapshot(spark, fs, s, mp, mapping_text, batch, stage, seq, osm_time)
        st.update(batch=batch, current_run=None)
        state.save(fs, s, st)
        fs.delete(stage)
    log.info("done")
