"""osm-spark-import: the writer (Spark). Settings come from the environment."""

from __future__ import annotations

import argparse
import json

from osm_spark_import import outbox, state, tables
from osm_spark_import.diffs import run as run_diffs
from osm_spark_import.imports import rebuild, run_import
from osm_spark_import.settings import Settings
from osm_spark_import.spark import Storage, session


def status(s: Settings) -> None:
    spark = session("osm-spark-import status")
    fs = Storage(spark)
    st = state.load(fs, s)
    batch = st.get("batch")
    out = {
        "state": st,
        "last_batch": (
            fs.read_json(f"{outbox.batch_dir(s.outbox, batch)}/{outbox.MANIFEST}") if batch else None
        ),
        "cache": {
            name: spark.table(s.table(name)).count()
            for name in tables.CACHE_TABLES
            if spark.catalog.tableExists(s.table(name))
        },
    }
    print(json.dumps(out, indent=2, default=str))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="osm-spark-import", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    imp = sub.add_parser("import", help="PBF (path or URL) -> cache -> snapshot batch")
    imp.add_argument("pbf")
    imp.add_argument(
        "--sequence", type=int, help="replication sequence of the PBF (default: from its timestamp)"
    )
    sub.add_parser("rebuild", help="snapshot batch from the cache, e.g. after changing the mapping")
    run = sub.add_parser("run", help="replication diffs -> delta batches")
    run.add_argument("--loop", action="store_true", help="keep running, waiting for new diffs")
    run.add_argument("--max-diffs", type=int, help="diffs per batch (OSM_MAX_DIFFS)")
    run.add_argument("--until", type=int, help="stop after this sequence")
    sub.add_parser("status", help="state, last batch, cache table sizes")
    args = p.parse_args(argv)
    s = Settings.from_env()

    if args.cmd == "import":
        run_import(s, args.pbf, sequence=args.sequence)
    elif args.cmd == "rebuild":
        rebuild(s)
    elif args.cmd == "run":
        run_diffs(s, loop=args.loop, max_diffs=args.max_diffs, until=args.until)
    elif args.cmd == "status":
        status(s)


if __name__ == "__main__":
    main()
