"""Writer state on HDFS, one JSON file per namespace.

    batch         the last complete outbox batch; the cache matches it
    sequence      the replication sequence of the cache
    osm_time      its timestamp
    import        the last import: batch, pbf, sequence
    current_run   the run in progress (kind, batch, stage, snapshots), for recovery
"""

from __future__ import annotations

from .settings import Settings
from .spark import HDFS


def path(s: Settings) -> str:
    return f"{s.hdfs_root}/state/{s.namespace}.json"


def load(hdfs: HDFS, s: Settings) -> dict:
    return hdfs.read_json(path(s)) or {}


def save(hdfs: HDFS, s: Settings, st: dict) -> None:
    hdfs.write_json(path(s), st)
