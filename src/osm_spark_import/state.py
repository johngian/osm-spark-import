"""Writer state and lock in storage, one of each per namespace.

State (<root>/state/<namespace>.json):

    batch         the last complete outbox batch; the cache matches it
    sequence      the replication sequence of the cache
    osm_time      its timestamp
    import        the last import: batch, chunks, sequence
    current_run   the run in progress (kind, batch, stage, snapshots), for recovery

Lock (<root>/state/<namespace>.lock): held by the command changing the cache
or the outbox. A later attempt of the same Spark application takes it over;
anything else fails until `osm-spark-import unlock`.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import logging
import os
import socket
from collections.abc import Iterator

from pyspark.sql import SparkSession

from osm_spark_import.settings import Settings
from osm_spark_import.spark import Storage

log = logging.getLogger(__name__)


def path(s: Settings) -> str:
    """Location of the namespace's state file."""
    return f"{s.storage_root}/state/{s.namespace}.json"


def lock_path(s: Settings) -> str:
    """Location of the namespace's lock file."""
    return f"{s.storage_root}/state/{s.namespace}.lock"


def load(fs: Storage, s: Settings) -> dict:
    """The state, or {} before the first import."""
    return fs.read_json(path(s)) or {}


def save(fs: Storage, s: Settings, st: dict) -> None:
    """Replace the state file atomically."""
    fs.write_json(path(s), st)


@contextlib.contextmanager
def locked(spark: SparkSession, fs: Storage, s: Settings, command: str) -> Iterator[None]:
    """Hold the namespace's writer lock for the duration of a block."""
    app = spark.sparkContext.applicationId
    owner = {
        "application": app,
        "command": command,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "started": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    if not fs.create_exclusive(lock_path(s), json.dumps(owner)):
        held = fs.read_json(lock_path(s)) or {}
        if held.get("application") != app:
            raise SystemExit(
                f"namespace {s.namespace} is locked by {json.dumps(held)}; if that run is gone, "
                "run `osm-spark-import unlock`"
            )
        log.info(f"taking over the lock of an earlier attempt of {app}")
        fs.write_json(lock_path(s), owner)
    try:
        yield
    finally:
        fs.delete(lock_path(s))
