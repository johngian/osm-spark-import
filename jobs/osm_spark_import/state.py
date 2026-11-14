"""Writer state in storage, one JSON file per namespace.

    batch         the last complete outbox batch; the cache matches it
    sequence      the replication sequence of the cache
    osm_time      its timestamp
    import        the last import: batch, pbf, sequence
    current_run   the run in progress (kind, batch, stage, snapshots), for recovery
"""

from __future__ import annotations

from .settings import Settings
from .spark import Storage


def path(s: Settings) -> str:
    return f"{s.storage_root}/state/{s.namespace}.json"


def load(fs: Storage, s: Settings) -> dict:
    return fs.read_json(path(s)) or {}


def save(fs: Storage, s: Settings, st: dict) -> None:
    fs.write_json(path(s), st)
