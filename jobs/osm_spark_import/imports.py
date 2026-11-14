"""import and rebuild: snapshot batches."""

from __future__ import annotations

import datetime as dt
import os
import shutil
import urllib.request

import osmium
from osmium.replication.server import ReplicationServer
from osmium.replication.utils import get_replication_header

from . import osm, outbox, pipeline, state, tables
from .settings import Settings
from .spark import Storage, session


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def download(url: str, dest_dir: str) -> str:
    """Download url into dest_dir, unless it's already there."""
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.join(dest_dir, url.rstrip("/").rsplit("/", 1)[-1])
    if os.path.exists(path):
        log(f"using {path}")
        return path
    log(f"GET {url}")
    tmp = path + ".part"
    with urllib.request.urlopen(url, timeout=60) as resp, open(tmp, "wb") as f:
        shutil.copyfileobj(resp, f, 1 << 20)
    os.rename(tmp, path)
    return path


def pbf_state(s: Settings, pbf: str, sequence: int | None) -> tuple[int, str]:
    """The replication sequence and time of a PBF, to start diffs from."""
    _, header_seq, ts = get_replication_header(pbf)
    if ts is None:
        log("no timestamp in the PBF header: looking for the newest change (reads the file)")
        ts = osmium.replication.newest_change_from_file(pbf)
    if sequence is None:
        sequence = ReplicationServer(s.replication_url).timestamp_to_sequence(ts)
        if sequence is None:
            raise SystemExit(f"no replication sequence for {ts} at {s.replication_url}; use --sequence")
    return sequence, ts.isoformat()


def _begin(fs: Storage, s: Settings, kind: str) -> tuple[dict, int, str]:
    st = state.load(fs, s)
    if st.get("current_run"):
        # a snapshot replaces everything an interrupted run left
        cr = st["current_run"]
        log(f"discarding the interrupted run {cr['kind']} batch {cr['batch']}")
        fs.delete(cr["stage"])
        fs.delete(outbox.batch_dir(s.outbox, cr["batch"]))
    batch = st.get("batch", 0) + 1
    stage = f"{s.storage_root}/staging/{s.namespace}-{batch}"
    st["current_run"] = {"kind": kind, "batch": batch, "stage": stage}
    state.save(fs, s, st)
    return st, batch, stage


def _snapshot(
    spark, fs: Storage, s: Settings, mp, mapping_text: str, batch: int, stage: str, seq: int, osm_time: str
):
    t = lambda name: spark.table(s.table(name))  # noqa: E731
    log("building rows")
    units = pipeline.work_units(mp, t("nodes"), t("ways"), t("relations"))
    pipeline.build(spark, s, units, mapping_text).write.mode("overwrite").parquet(stage + "/rows")
    log(f"writing batch {batch}")
    manifest = pipeline.write_batch(
        spark, fs, s, mp, batch, "snapshot", None, spark.read.parquet(stage + "/rows"), None, seq, osm_time
    )
    for name, n in manifest["rows"].items():
        log(f"  {name}: {n} rows")
    return manifest


def run_import(s: Settings, source: str, sequence: int | None = None) -> None:
    mp, mapping_text = s.load_mapping()
    pbf = source if os.path.exists(source) else download(source, os.path.join(s.work_dir, "downloads"))
    seq, osm_time = pbf_state(s, pbf, sequence)
    log(f"PBF data till {osm_time}, replication continues after sequence {seq}")

    spark = session(f"osm-spark-import import {s.namespace}")
    fs = Storage(spark)
    tables.ensure_namespace(spark, s)
    st, batch, stage = _begin(fs, s, "snapshot")

    log("splitting the PBF")
    local = os.path.join(s.work_dir, f"chunks-{s.namespace}-{batch}")
    shutil.rmtree(local, ignore_errors=True)
    chunks = osm.split_pbf(pbf, local, s.chunk_mb << 20)
    fs.put_files(chunks, stage + "/chunks")
    shutil.rmtree(local)
    log(f"{len(chunks)} chunks")

    log("reading the PBF into the cache")
    pipeline.decode_pbf_chunks(spark, stage + "/chunks", s.chunk_mb).write.mode("overwrite").parquet(
        stage + "/elements"
    )
    tables.write_cache(s, spark.read.parquet(stage + "/elements"))

    _snapshot(spark, fs, s, mp, mapping_text, batch, stage, seq, osm_time)
    st.update(
        batch=batch,
        sequence=seq,
        osm_time=osm_time,
        current_run=None,
        **{"import": {"batch": batch, "pbf": source, "sequence": seq, "time": osm_time}},
    )
    state.save(fs, s, st)
    fs.delete(stage)
    log("done")


def rebuild(s: Settings) -> None:
    """A snapshot batch from the cache, e.g. after a mapping change."""
    mp, mapping_text = s.load_mapping()
    spark = session(f"osm-spark-import rebuild {s.namespace}")
    fs = Storage(spark)
    st = state.load(fs, s)
    if not st.get("batch"):
        raise SystemExit("nothing imported yet")
    seq, osm_time = st["sequence"], st.get("osm_time")
    st, batch, stage = _begin(fs, s, "snapshot")
    _snapshot(spark, fs, s, mp, mapping_text, batch, stage, seq, osm_time)
    st.update(batch=batch, current_run=None)
    state.save(fs, s, st)
    fs.delete(stage)
    log("done")
