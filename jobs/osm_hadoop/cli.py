"""osm-hadoop: the writer (Spark). Settings come from the environment."""

from __future__ import annotations

import argparse
import json

from .settings import Settings


def status(s: Settings) -> None:
    from . import outbox, state, tables
    from .spark import HDFS, session

    spark = session("osm-hadoop status")
    hdfs = HDFS(spark)
    st = state.load(hdfs, s)
    batch = st.get("batch")
    out = {
        "state": st,
        "last_batch": (
            hdfs.read_json(f"{outbox.batch_dir(s.outbox, batch)}/{outbox.MANIFEST}") if batch else None
        ),
        "cache": {
            name: spark.table(s.table(name)).count()
            for name in tables.CACHE_TABLES
            if spark.catalog.tableExists(s.table(name))
        },
    }
    print(json.dumps(out, indent=2, default=str))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="osm-hadoop", description=__doc__)
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
        from .imports import run_import

        run_import(s, args.pbf, sequence=args.sequence)
    elif args.cmd == "rebuild":
        from .imports import rebuild

        rebuild(s)
    elif args.cmd == "run":
        from .diffs import run as run_diffs

        run_diffs(s, loop=args.loop, max_diffs=args.max_diffs, until=args.until)
    elif args.cmd == "status":
        status(s)


if __name__ == "__main__":
    main()
