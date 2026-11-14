"""The steps shared by imports and diffs: decode, assemble, build, write batches."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, _parse_datatype_string

from . import osm, outbox
from .settings import Settings
from .spark import HDFS

CHUNK_MB = 32  # default; OSM_CHUNK_MB
ELEMENT_SCHEMA = (
    "k string, id bigint, qlon bigint, qlat bigint, tags map<string,string>, refs array<bigint>, "
    "members array<struct<type:string,ref:bigint,role:string>>"
)
ROW_SCHEMA = "tbl string, id bigint, g boolean, vals string, geom binary, tile string"


def partitions(spark: SparkSession, s: Settings) -> int:
    return s.partitions or spark.sparkContext.defaultParallelism


def decode_pbf_chunks(spark: SparkSession, chunks_dir: str, chunk_mb: int = CHUNK_MB) -> DataFrame:
    """Decode the PBF chunks in an HDFS directory with pyosmium, one chunk per task."""
    size = str((chunk_mb + 8) << 20)
    spark.conf.set("spark.sql.files.maxPartitionBytes", size)
    spark.conf.set("spark.sql.files.openCostInBytes", size)
    raw = spark.read.format("binaryFile").load(chunks_dir).select("content")
    return raw.mapInArrow(osm.decode_chunks, ELEMENT_SCHEMA)


def _has_key(keys: list[str] | None):
    """Raw tags contain one of keys (None: any tagged element)."""
    tagged = F.size("tags") > 0
    if keys is None:
        return tagged
    return tagged & F.arrays_overlap(F.map_keys("tags"), F.array(*[F.lit(k) for k in keys]))


def _targets(df: DataFrame, ids: DataFrame | None) -> DataFrame:
    return df if ids is None else df.join(ids.select("id").distinct(), "id", "left_semi")


def work_units(
    mp,
    nodes: DataFrame,
    ways: DataFrame,
    rels: DataFrame,
    node_ids: DataFrame | None = None,
    way_ids: DataFrame | None = None,
    rel_ids: DataFrame | None = None,
    changed: dict[str, DataFrame] | None = None,
) -> DataFrame:
    """Each target element with what its rows need.

    changed: per kind (n, w, r), the IDs changed by a diff; adds a boolean
    column "changed" (the others are dependents).

    Columns: k, id, tags, qlon, qlat (nodes), refs (ways), members
    (relations), nodes (coordinates: of a way's nodes; of a relation's
    member ways' nodes and member nodes), ways (member ways: id, tags, refs),
    mnodes (tagged member nodes), rels (member relations).
    Targets: tagged elements with a key the mapping matches on (all of them,
    or those in the *_ids DataFrames).
    """
    coord = F.struct(F.col("nid").alias("id"), "qlon", "qlat")
    coords = nodes.select(F.col("id").alias("nid"), "qlon", "qlat")

    tn = _targets(nodes.filter(_has_key(mp.target_keys("n"))), node_ids)
    node_units = tn.select(F.lit("n").alias("k"), "id", "tags", "qlon", "qlat")

    tw = _targets(ways.filter(_has_key(mp.target_keys("w"))), way_ids)
    wcoords = (
        tw.select(F.col("id").alias("wid"), F.explode("refs").alias("nid"))
        .distinct()
        .join(coords, "nid")
        .groupBy("wid")
        .agg(F.collect_list(coord).alias("nodes"))
    )
    way_units = (
        tw.withColumnRenamed("id", "wid")
        .join(wcoords, "wid", "left")
        .select(F.lit("w").alias("k"), F.col("wid").alias("id"), "tags", "refs", "nodes")
    )

    tr = _targets(rels.filter(_has_key(mp.target_keys("r"))), rel_ids)
    m = tr.select(F.col("id").alias("rid"), F.explode("members").alias("m")).select(
        "rid", F.col("m.type").alias("mtype"), F.col("m.ref").alias("mref")
    )
    member_ways = (
        m.filter("mtype = 'w'")
        .select("rid", F.col("mref").alias("wid"))
        .distinct()
        .join(
            ways.select(F.col("id").alias("wid"), F.col("tags").alias("wtags"), F.col("refs").alias("wrefs")),
            "wid",
        )
    )
    rways = member_ways.groupBy("rid").agg(
        F.collect_list(
            F.struct(F.col("wid").alias("id"), F.col("wtags").alias("tags"), F.col("wrefs").alias("refs"))
        ).alias("ways")
    )
    node_refs = m.filter("mtype = 'n'").select("rid", F.col("mref").alias("nid")).distinct()
    rcoords = (
        member_ways.select("rid", F.explode("wrefs").alias("nid"))
        .unionByName(node_refs)
        .distinct()
        .join(coords, "nid")
        .groupBy("rid")
        .agg(F.collect_list(coord).alias("nodes"))
    )
    rmnodes = (
        node_refs.join(
            nodes.filter(F.size("tags") > 0).select(
                F.col("id").alias("nid"), F.col("tags").alias("ntags"), "qlon", "qlat"
            ),
            "nid",
        )
        .groupBy("rid")
        .agg(
            F.collect_list(
                F.struct(F.col("nid").alias("id"), F.col("ntags").alias("tags"), "qlon", "qlat")
            ).alias("mnodes")
        )
    )
    rrels = (
        m.filter("mtype = 'r'")
        .select("rid", F.col("mref").alias("mrid"))
        .distinct()
        .join(
            rels.select(
                F.col("id").alias("mrid"), F.col("tags").alias("mtags"), F.col("members").alias("mmembers")
            ),
            "mrid",
        )
        .groupBy("rid")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("mrid").alias("id"),
                    F.col("mtags").alias("tags"),
                    F.col("mmembers").alias("members"),
                )
            ).alias("rels")
        )
    )
    rel_units = (
        tr.withColumnRenamed("id", "rid")
        .join(rways, "rid", "left")
        .join(rcoords, "rid", "left")
        .join(rmnodes, "rid", "left")
        .join(rrels, "rid", "left")
        .select(
            F.lit("r").alias("k"),
            F.col("rid").alias("id"),
            "tags",
            "members",
            "ways",
            "nodes",
            "mnodes",
            "rels",
        )
    )
    if changed is not None:

        def flag(units, kind):
            ids = changed[kind].select("id").distinct().withColumn("changed", F.lit(True))
            return units.join(ids, "id", "left").withColumn("changed", F.coalesce("changed", F.lit(False)))

        node_units, way_units, rel_units = flag(node_units, "n"), flag(way_units, "w"), flag(rel_units, "r")
    return node_units.unionByName(way_units, allowMissingColumns=True).unionByName(
        rel_units, allowMissingColumns=True
    )


def build(
    spark: SparkSession,
    s: Settings,
    units: DataFrame,
    mapping_text: str,
    expire_zoom: int = 0,
    deleter_expire: bool = False,
) -> DataFrame:
    """Rows (tbl, id, g, vals, geom) and expired tiles (tile) of the work units."""
    fn = osm.make_build_fn(mapping_text, s.srid, expire_zoom, deleter_expire)
    return units.repartition(partitions(spark, s)).mapInArrow(fn, ROW_SCHEMA)


def table_schema(spec: dict) -> StructType:
    """Spark types of the non-geometry columns of a table spec."""
    from .mapping.columns import VALUE_TYPES

    return StructType(
        [
            StructField(c["name"], _parse_datatype_string(VALUE_TYPES[c["value_type"]][1]))
            for c in spec["columns"]
            if c["pg"] != "GEOMETRY"
        ]
    )


def write_batch(
    spark: SparkSession,
    hdfs: HDFS,
    s: Settings,
    mp,
    batch: int,
    kind: str,
    base: int | None,
    rows: DataFrame,
    deletes: DataFrame | None,
    sequence: int,
    osm_time: str | None,
) -> dict:
    """Write an outbox batch: Parquet per table (columns in mapping order,
    plus __id and __gen for the generalized tables), deletes, expired tiles,
    then the manifest. rows should be materialized: it is read per table."""
    path = outbox.batch_dir(s.outbox, batch)
    hdfs.delete(path)
    spec = mp.manifest()
    counts = {}
    for name, t in spec["tables"].items():
        cols = [
            (
                F.col("geom").alias(c["name"])
                if c["pg"] == "GEOMETRY"
                else F.col("v")[c["name"]].alias(c["name"])
            )
            for c in t["columns"]
        ]
        df = (
            rows.filter((F.col("tbl") == name) & F.col("tile").isNull())
            .select(F.from_json("vals", table_schema(t)).alias("v"), "geom", "id", "g")
            .select(*cols, F.col("id").alias("__id"), F.col("g").alias("__gen"))
        )
        df.write.mode("overwrite").parquet(f"{path}/tables/{name}")
        counts[name] = spark.read.parquet(f"{path}/tables/{name}").count()
    tiles = rows.filter(F.col("tile").isNotNull()).select("tile").distinct()
    tiles.write.mode("overwrite").parquet(f"{path}/tiles")
    n_tiles = spark.read.parquet(f"{path}/tiles").count()
    n_deletes = 0
    if deletes is not None:
        deletes.select("kind", "id").distinct().write.mode("overwrite").parquet(f"{path}/deletes")
        n_deletes = spark.read.parquet(f"{path}/deletes").count()
    manifest = {
        "batch": batch,
        "kind": kind,
        "base": base,
        "namespace": s.namespace,
        "sequence": sequence,
        "osm_time": osm_time,
        "created": dt.datetime.now(dt.UTC).isoformat(),
        "deletes": n_deletes,
        "tiles": n_tiles,
        "rows": counts,
        **spec,
    }
    hdfs.write_json(f"{path}/{outbox.MANIFEST}", manifest)
    return manifest
