"""The cache: Iceberg tables, one namespace per import.

    nodes(id, lon, lat, tags)            every node; coordinates in 1e-7 degrees
    ways(id, tags, refs)                 every way
    relations(id, tags, members)         every relation; member type n/w/r
    way_nodes(node_id, way_id)           index: node -> ways
    rel_members(member_type, member_id, rel_id)   index: member -> relations

Tags are kept complete; the mapping is applied when rows are built, so a
mapping change only needs the rows built again.
"""

from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from osm_spark_import.settings import Settings

CACHE_TABLES = {
    "nodes": "id",
    "ways": "id",
    "relations": "id",
    "way_nodes": "node_id",
    "rel_members": "member_id",
}

# diffs change few rows of every bucket: write delete files instead of rewriting
# data files, and let `maintain` compact them
MERGE_ON_READ = {f"write.{op}.mode": "merge-on-read" for op in ("merge", "update", "delete")}
ORPHAN_AGE = dt.timedelta(days=3)

log = logging.getLogger(__name__)

# element table -> (element kind, columns besides id)
ELEMENTS = {
    "nodes": ("n", ("lon", "lat", "tags")),
    "ways": ("w", ("tags", "refs")),
    "relations": ("r", ("tags", "members")),
}


def ensure_namespace(spark: SparkSession, s: Settings) -> None:
    """Create the namespace's Iceberg namespace."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {s.catalog}.{s.namespace}")


def replace(df: DataFrame, s: Settings, name: str, bucket_col: str) -> None:
    """Create or replace a cache table, bucketed on bucket_col."""
    writer = (
        df.writeTo(s.table(name))
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.distribution-mode", "hash")
    )
    for key, value in MERGE_ON_READ.items():
        writer = writer.tableProperty(key, value)
    writer.partitionedBy(F.bucket(s.buckets, F.col(bucket_col))).createOrReplace()


def way_nodes(ways: DataFrame) -> DataFrame:
    """The node -> way index rows of ways."""
    return ways.select(F.explode("refs").alias("node_id"), F.col("id").alias("way_id")).distinct()


def rel_members(rels: DataFrame) -> DataFrame:
    """The member -> relation index rows of relations."""
    return (
        rels.select(F.col("id").alias("rel_id"), F.explode("members").alias("m"))
        .select(F.col("m.type").alias("member_type"), F.col("m.ref").alias("member_id"), "rel_id")
        .distinct()
    )


def write_cache(s: Settings, elems: DataFrame) -> None:
    """Replace the cache and index tables with decoded elements."""
    dfs = {name: elems.filter(F.col("k") == k).select("id", *cols) for name, (k, cols) in ELEMENTS.items()}
    dfs["way_nodes"] = way_nodes(dfs["ways"])
    dfs["rel_members"] = rel_members(dfs["relations"])
    for name, bucket_col in CACHE_TABLES.items():
        replace(dfs[name], s, name, bucket_col)


def current_snapshot(spark: SparkSession, s: Settings, name: str) -> int | None:
    """The table's current snapshot ID (None while empty)."""
    rows = spark.sql(f"SELECT snapshot_id FROM {s.table(name)}.refs WHERE name = 'main'").collect()
    return rows[0].snapshot_id if rows else None


def snapshots(spark: SparkSession, s: Settings) -> dict[str, int | None]:
    """Current snapshot ID of every cache table."""
    return {name: current_snapshot(spark, s, name) for name in CACHE_TABLES}


def at_snapshot(spark: SparkSession, s: Settings, name: str, snapshot: int | None) -> DataFrame:
    """A table as of a snapshot (empty for None)."""
    if snapshot is None:
        return spark.table(s.table(name)).limit(0)
    return spark.sql(f"SELECT * FROM {s.table(name)} VERSION AS OF {int(snapshot)}")


def rollback(spark: SparkSession, s: Settings, snaps: dict[str, int | None]) -> None:
    """Roll the cache tables back to snapshots (from snapshots())."""
    for name, snapshot in snaps.items():
        current = current_snapshot(spark, s, name)
        if current == snapshot:
            continue
        if snapshot is None:  # empty before the run
            spark.sql(f"DELETE FROM {s.table(name)}")
        else:
            spark.sql(
                f"CALL {s.catalog}.system.rollback_to_snapshot('{s.namespace}.{name}', {int(snapshot)})"
            )


def row_counts(spark: SparkSession, s: Settings) -> dict[str, int]:
    """Rows of each cache table from its current snapshot's summary, without a scan.

    Approximate: records minus position deletes, and compaction can leave deletes
    counted that no longer apply.
    """
    out = {}
    for name in CACHE_TABLES:
        if not spark.catalog.tableExists(s.table(name)):
            continue
        snapshot = current_snapshot(spark, s, name)
        if snapshot is None:
            out[name] = 0
            continue
        (summary,) = spark.sql(
            f"SELECT summary FROM {s.table(name)}.snapshots WHERE snapshot_id = {int(snapshot)}"
        ).first()
        out[name] = int(summary.get("total-records", 0)) - int(summary.get("total-position-deletes", 0))
    return out


def maintain(spark: SparkSession, s: Settings) -> None:
    """Compact data and delete files, expire old snapshots and remove orphan files."""
    now = dt.datetime.now(dt.timezone.utc)
    expire_before = (now - dt.timedelta(hours=s.snapshot_retention_hours)).strftime("%Y-%m-%d %H:%M:%SZ")
    orphan_before = (now - ORPHAN_AGE).strftime("%Y-%m-%d %H:%M:%SZ")
    props = ", ".join(f"'{k}' = '{v}'" for k, v in MERGE_ON_READ.items())
    call = f"CALL {s.catalog}.system"
    for name in CACHE_TABLES:
        table = f"'{s.namespace}.{name}'"
        log.info(f"maintaining {name}")
        spark.sql(f"ALTER TABLE {s.table(name)} SET TBLPROPERTIES ({props})")
        spark.sql(
            f"{call}.rewrite_data_files(table => {table}, options => map('delete-file-threshold', '1'))"
        )
        spark.sql(f"{call}.rewrite_position_delete_files(table => {table})")
        spark.sql(
            f"{call}.expire_snapshots(table => {table}, older_than => TIMESTAMP '{expire_before}', "
            "retain_last => 1)"
        )
        spark.sql(f"{call}.remove_orphan_files(table => {table}, older_than => TIMESTAMP '{orphan_before}')")
