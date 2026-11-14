"""The cache: Iceberg tables on HDFS, one namespace per import.

    nodes(id, qlon, qlat, tags)          every node; coordinates as uint32 (mapping.geom)
    ways(id, tags, refs)                 every way
    relations(id, tags, members)         every relation; member type n/w/r
    way_nodes(node_id, way_id)           index: node -> ways
    rel_members(member_type, member_id, rel_id)   index: member -> relations

Tags are kept complete; the mapping is applied when rows are built, so a
mapping change only needs the rows built again.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .settings import Settings

CACHE_TABLES = {
    "nodes": "id",
    "ways": "id",
    "relations": "id",
    "way_nodes": "node_id",
    "rel_members": "member_id",
}


def ensure_namespace(spark: SparkSession, s: Settings) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {s.catalog}.{s.namespace}")


def replace(df: DataFrame, s: Settings, name: str, bucket_col: str) -> None:
    (
        df.writeTo(s.table(name))
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.distribution-mode", "hash")
        .partitionedBy(F.bucket(s.buckets, F.col(bucket_col)))
        .createOrReplace()
    )


def way_nodes(ways: DataFrame) -> DataFrame:
    return ways.select(F.explode("refs").alias("node_id"), F.col("id").alias("way_id")).distinct()


def rel_members(rels: DataFrame) -> DataFrame:
    return (
        rels.select(F.col("id").alias("rel_id"), F.explode("members").alias("m"))
        .select(F.col("m.type").alias("member_type"), F.col("m.ref").alias("member_id"), "rel_id")
        .distinct()
    )


def write_cache(s: Settings, elems: DataFrame) -> None:
    """Replace the cache and index tables with decoded elements."""
    nodes = elems.filter("k = 'n'").select("id", "qlon", "qlat", "tags")
    ways = elems.filter("k = 'w'").select("id", "tags", "refs")
    rels = elems.filter("k = 'r'").select("id", "tags", "members")
    replace(nodes, s, "nodes", "id")
    replace(ways, s, "ways", "id")
    replace(rels, s, "relations", "id")
    replace(way_nodes(ways), s, "way_nodes", "node_id")
    replace(rel_members(rels), s, "rel_members", "member_id")


def current_snapshot(spark: SparkSession, s: Settings, name: str) -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {s.table(name)}.refs WHERE name = 'main'").collect()
    return rows[0].snapshot_id if rows else None


def snapshots(spark: SparkSession, s: Settings) -> dict[str, int | None]:
    return {name: current_snapshot(spark, s, name) for name in CACHE_TABLES}


def at_snapshot(spark: SparkSession, s: Settings, name: str, snapshot: int | None) -> DataFrame:
    if snapshot is None:
        return spark.table(s.table(name)).limit(0)
    return spark.sql(f"SELECT * FROM {s.table(name)} VERSION AS OF {int(snapshot)}")


def rollback(spark: SparkSession, s: Settings, snaps: dict[str, int | None]) -> None:
    for name, snapshot in snaps.items():
        if snapshot is None or current_snapshot(spark, s, name) == snapshot:
            continue
        spark.sql(f"CALL {s.catalog}.system.rollback_to_snapshot('{s.namespace}.{name}', {int(snapshot)})")
