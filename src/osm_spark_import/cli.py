"""osm-spark-import: the writer (Spark). Settings come from the environment."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging

from osmium.replication.server import ReplicationServer

from osm_spark_import import outbox, state, tables
from osm_spark_import.chunk import chunk
from osm_spark_import.diffs import run as run_diffs
from osm_spark_import.imports import rebuild, run_import
from osm_spark_import.settings import Settings, setup_logging
from osm_spark_import.spark import Storage, session

log = logging.getLogger(__name__)


def lag(s: Settings, sequence: int | None, osm_time: str | None) -> dict:
    """How far sequence/osm_time are behind the replication server and now."""
    out: dict = {"applied": sequence, "osm_time": osm_time}
    if osm_time:
        age = dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(osm_time)
        out["seconds_behind"] = int(age.total_seconds())
    with ReplicationServer(s.replication_url) as server:
        info = server.get_state_info()
    if info is not None:
        out["latest"] = info.sequence
        out["sequences_behind"] = info.sequence - sequence if sequence is not None else None
    return out


def status(s: Settings) -> None:
    """Print the state, the lock, replication lag, the last batch's manifest and the cache sizes as JSON."""
    spark = session("osm-spark-import status")
    fs = Storage(spark)
    st = state.load(fs, s)
    batch = st.get("batch")
    out = {
        "state": st,
        "lock": fs.read_json(state.lock_path(s)),
        "replication": lag(s, st.get("sequence"), st.get("osm_time")),
        "last_batch": (
            fs.read_json(f"{outbox.batch_dir(s.outbox, batch)}/{outbox.MANIFEST}") if batch else None
        ),
        "cache_rows_approx": tables.row_counts(spark, s),
    }
    print(json.dumps(out, indent=2, default=str))


def maintain(s: Settings) -> None:
    """Compact the cache tables and expire old snapshots, under the writer lock."""
    spark = session(f"osm-spark-import maintain {s.namespace}")
    fs = Storage(spark)
    with state.locked(spark, fs, s, "maintain"):
        if state.load(fs, s).get("current_run"):
            raise SystemExit("a run was interrupted: finish it (`run` or `import`) before maintenance")
        tables.maintain(spark, s)
    log.info("done")


def unlock(s: Settings) -> None:
    """Remove the writer lock (after checking that its run is gone)."""
    spark = session("osm-spark-import unlock")
    fs = Storage(spark)
    log.info(f"removing lock {fs.read_json(state.lock_path(s))}")
    fs.delete(state.lock_path(s))


def main(argv: list[str] | None = None) -> None:
    """Entry point of osm-spark-import."""
    p = argparse.ArgumentParser(prog="osm-spark-import", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    ch = sub.add_parser("chunk", help="PBF (path or URL) -> PBF chunks in storage (no Spark)")
    ch.add_argument("source", help="PBF path or URL (https://, hdfs://, file://, ...)")
    ch.add_argument("dest", help="directory for the chunks, e.g. hdfs:///osm/pbf/planet-261109")
    size = ch.add_mutually_exclusive_group()
    size.add_argument("--chunk-mb", type=int, help="chunk size in MB (default OSM_CHUNK_MB)")
    size.add_argument("--chunks", type=int, help="number of chunks (e.g. 2-4 per executor core)")
    ch.add_argument("--md5", help="expected MD5 of the PBF (default: from SOURCE.md5, if any)")
    ch.add_argument("--sequence", type=int, help="replication sequence (default: from the PBF timestamp)")
    ch.add_argument("--force", action="store_true", help="replace existing chunks")
    imp = sub.add_parser("import", help="PBF chunks -> cache -> snapshot batch")
    imp.add_argument("chunks", help="directory written by `chunk`")
    imp.add_argument("--sequence", type=int, help="replication sequence (default: from the chunks)")
    sub.add_parser("rebuild", help="snapshot batch from the cache, e.g. after changing the mapping")
    run = sub.add_parser("run", help="replication diffs -> delta batches")
    run.add_argument(
        "--loop", action="store_true", help="keep running, waiting for new diffs (not on clusters)"
    )
    run.add_argument("--max-diffs", type=int, help="diffs per batch (OSM_MAX_DIFFS)")
    run.add_argument("--until", type=int, help="stop after this sequence")
    sub.add_parser("maintain", help="compact the cache, expire old snapshots")
    sub.add_parser("unlock", help="remove the writer lock left by a crashed run")
    sub.add_parser("status", help="state, lock, lag, last batch, cache table sizes")
    args = p.parse_args(argv)
    setup_logging()
    s = Settings.from_env()

    if args.cmd == "chunk":
        chunk(s, args.source, args.dest, args.chunk_mb, args.chunks, args.md5, args.sequence, args.force)
    elif args.cmd == "import":
        run_import(s, args.chunks, sequence=args.sequence)
    elif args.cmd == "rebuild":
        rebuild(s)
    elif args.cmd == "run":
        run_diffs(s, loop=args.loop, max_diffs=args.max_diffs, until=args.until)
    elif args.cmd == "maintain":
        maintain(s)
    elif args.cmd == "unlock":
        unlock(s)
    elif args.cmd == "status":
        status(s)


if __name__ == "__main__":
    main()
