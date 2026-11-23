"""The steps shared by imports and diffs: decode, assemble, build, write batches."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from osm_spark_import import osm, outbox
from osm_spark_import.mapping.columns import VALUE_TYPES
from osm_spark_import.settings import Settings
from osm_spark_import.spark import Storage, conf_scope

# Spark versions of osm.ELEMENT and osm.ROW
ELEMENT_SCHEMA = (
    "k string, id bigint, lon int, lat int, tags map<string,string>, refs array<bigint>, "
    "members array<struct<type:string,ref:bigint,role:string>>"
)
ROW_SCHEMA = "tbl string, id bigint, g boolean, vals string, geom binary, tile string"


def partitions(spark: SparkSession, s: Settings) -> int:
    """OSM_PARTITIONS, or spark.sql.shuffle.partitions (not the executors present right now,
    which dynamic allocation makes arbitrary)."""
    return s.partitions or int(spark.conf.get("spark.sql.shuffle.partitions"))


def decode_chunks_to(spark: SparkSession, chunks_dir: str, max_chunk_bytes: int, out: str) -> None:
    """Decode the PBF chunks of a directory with pyosmium, one chunk per task, into Parquet."""
    size = str(max_chunk_bytes + (8 << 20))
    with conf_scope(
        spark, {"spark.sql.files.maxPartitionBytes": size, "spark.sql.files.openCostInBytes": size}
    ):
        (
            spark.read.format("binaryFile")
            .option("pathGlobFilter", "*.osm.pbf")
            .load(chunks_dir)
            .select("content")
            .mapInArrow(osm.decode_chunks, ELEMENT_SCHEMA)
            .write.mode("overwrite")
            .parquet(out)
        )


def has_key(keys: list[str] | None):
    """Raw tags contain one of keys (None: any tagged element)."""
    tagged = F.size("tags") > 0
    if keys is None:
        return tagged
    return tagged & F.arrays_overlap(F.map_keys("tags"), F.array(*[F.lit(k) for k in keys]))


def select_targets(df: DataFrame, ids: DataFrame | None) -> DataFrame:
    """The rows of df whose id is in ids (all of them for None)."""
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

    Columns: k, id, tags, lon, lat (nodes), refs (ways), members
    (relations), nodes (coordinates: of a way's nodes; of a relation's
    member ways' nodes and member nodes), ways (member ways: id, tags, refs),
    mnodes (tagged member nodes), rels (member relations).
    Targets: tagged elements with a key the mapping matches on (all of them,
    or those in the *_ids DataFrames).
    """
    coord = F.struct(F.col("nid").alias("id"), "lon", "lat")
    coords = nodes.select(F.col("id").alias("nid"), "lon", "lat")

    tn = select_targets(nodes.filter(has_key(mp.target_keys("n"))), node_ids)
    node_units = tn.select(F.lit("n").alias("k"), "id", "tags", "lon", "lat")

    tw = select_targets(ways.filter(has_key(mp.target_keys("w"))), way_ids)
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

    tr = select_targets(rels.filter(has_key(mp.target_keys("r"))), rel_ids)
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
                F.col("id").alias("nid"), F.col("tags").alias("ntags"), "lon", "lat"
            ),
            "nid",
        )
        .groupBy("rid")
        .agg(
            F.collect_list(
                F.struct(F.col("nid").alias("id"), F.col("ntags").alias("tags"), "lon", "lat")
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
    """Rows (tbl, id, g, vals, geom) and expired tiles (tile) of the work units.

    Units with more than OSM_BIG_UNIT_NODES coordinates (large relations) are
    spread over their own partitions and built one at a time, apart from the
    others; both branches share the work units' shuffles (exchange reuse).
    """
    n = partitions(spark, s)
    big = F.greatest(F.size("nodes"), F.lit(0)) > s.big_unit_nodes
    small_fn = osm.make_build_fn(mapping_text, s.srid, expire_zoom, deleter_expire)
    big_fn = osm.make_build_fn(
        mapping_text, s.srid, expire_zoom, deleter_expire, batch_units=1, big_unit_nodes=s.big_unit_nodes
    )
    small_rows = units.filter(~big).repartition(n).mapInArrow(small_fn, ROW_SCHEMA)
    big_rows = units.filter(big).repartition(n).mapInArrow(big_fn, ROW_SCHEMA)
    return small_rows.unionByName(big_rows)


def write_rows(rows: DataFrame, path: str) -> None:
    """Stage built rows, partitioned by table so each table is read apart."""
    rows.write.mode("overwrite").partitionBy("tbl").parquet(path)


def read_rows(spark: SparkSession, path: str) -> DataFrame:
    """Rows staged by write_rows (the schema is given: an empty stage has no files)."""
    return spark.read.schema(ROW_SCHEMA).parquet(path)


def table_schema(spec: dict) -> str:
    """DDL schema of the non-geometry columns of a table spec."""
    return ", ".join(
        f"`{c['name']}` {VALUE_TYPES[c['value_type']][1]}" for c in spec["columns"] if c["pg"] != "GEOMETRY"
    )


def write_batch(
    spark: SparkSession,
    fs: Storage,
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
    then the manifest. rows should be staged with write_rows: each table reads its partition."""
    path = outbox.batch_dir(s.outbox, batch)
    fs.delete(path)
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
        "created": dt.datetime.now(dt.timezone.utc).isoformat(),
        "deletes": n_deletes,
        "tiles": n_tiles,
        "rows": counts,
        **spec,
    }
    fs.write_json(f"{path}/{outbox.MANIFEST}", manifest)
    return manifest
