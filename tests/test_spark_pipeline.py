"""The Spark side end to end: chunk -> import -> diffs -> recovery, lock and maintenance.

Local Spark with an Iceberg hadoop catalog under a temporary directory. Needs
Java and the Iceberg runtime JAR (OSM_TEST_ICEBERG_JAR), else skipped.
"""

import datetime as dt
import gzip
import json
import os
import shutil
import sys
from types import SimpleNamespace

import osmium
import pyarrow.parquet as pq
import pytest
from test_osm import DEFAULT_MAPPING

from osm_spark_import import chunk, cli, diffs, imports, outbox, state, tables
from osm_spark_import.settings import Settings
from osm_spark_import.spark import Storage

JAR = os.environ.get("OSM_TEST_ICEBERG_JAR", "")
pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(
        not shutil.which("java") or not os.path.isfile(JAR), reason="needs Java and OSM_TEST_ICEBERG_JAR"
    ),
]

TIME = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)

# the data before the diff; the diff below; the data after it
NODES = {
    i: ((i % 10) / 1000, (i // 10) / 1000, {"amenity": "bench"} if i % 7 == 0 else {}) for i in range(1, 51)
}
WAYS = {1: ([1, 2, 3, 4], {"highway": "residential"}), 2: ([11, 12, 22, 21, 11], {"building": "yes"})}
RELS = {1: ([("w", 2, "outer")], {"type": "multipolygon", "landuse": "grass"})}
OSC = """<osmChange version="0.6">
<modify><node id="3" version="2" lat="0.0005" lon="0.0035"><tag k="amenity" v="cafe"/></node></modify>
<create><node id="100" version="1" lat="0.01" lon="0.01"><tag k="amenity" v="bench"/></node></create>
<delete><node id="49" version="2" lat="0.004" lon="0.009"/></delete>
<modify><way id="1" version="2"><nd ref="1"/><nd ref="2"/><nd ref="3"/><tag k="highway" v="primary"/></way></modify>
</osmChange>"""


def after_diff():
    nodes = dict(NODES)
    nodes[3] = (0.0035, 0.0005, {"amenity": "cafe"})
    nodes[100] = (0.01, 0.01, {"amenity": "bench"})
    del nodes[49]
    ways = dict(WAYS)
    ways[1] = ([1, 2, 3], {"highway": "primary"})
    return nodes, ways, RELS


def write_pbf(path, nodes, ways, rels):
    with osmium.SimpleWriter(str(path)) as w:
        for i, (lon, lat, tags) in sorted(nodes.items()):
            w.add_node(osmium.osm.mutable.Node(id=i, location=(lon, lat), tags=tags))
        for i, (refs, tags) in sorted(ways.items()):
            w.add_way(osmium.osm.mutable.Way(id=i, nodes=refs, tags=tags))
        for i, (members, tags) in sorted(rels.items()):
            w.add_relation(osmium.osm.mutable.Relation(id=i, members=members, tags=tags))


class StubServer:
    """ReplicationServer with one diff (sequence 11) after the import's sequence 10."""

    def __init__(self, url):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def get_state_info(self, seq=None):
        return SimpleNamespace(sequence=seq or 11, timestamp=TIME + dt.timedelta(minutes=seq or 11))

    def get_diff_url(self, seq):
        return f"stub/{seq}.osc.gz"

    def get_diff_block(self, seq):
        return gzip.compress(OSC.encode())


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    warehouse = tmp_path_factory.mktemp("warehouse")
    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", JAR)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.osm", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.osm.type", "hadoop")
        .config("spark.sql.catalog.osm.warehouse", f"file://{warehouse}")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setattr(diffs, "ReplicationServer", StubServer)
    return Settings(
        mapping=DEFAULT_MAPPING,
        storage_root=f"file://{tmp_path}/root",
        buckets=2,
        partitions=2,
        expire_zoom=14,
    )


def import_data(s, tmp_path, data, namespace):
    s.namespace = namespace
    write_pbf(tmp_path / f"{namespace}.osm.pbf", *data)
    chunks = f"file://{tmp_path}/chunks-{namespace}"
    chunk.chunk(s, str(tmp_path / f"{namespace}.osm.pbf"), chunks, chunk_mb=1, sequence=10)
    imports.run_import(s, chunks)


def cache(spark, s):
    out = {}
    for name in tables.CACHE_TABLES:
        rows = spark.table(s.table(name)).collect()
        out[name] = sorted(json.dumps(r.asDict(recursive=True), sort_keys=True, default=str) for r in rows)
    return out


def batch_table(s, batch, part):
    path = f"{outbox.batch_dir(s.outbox, batch)}/{part}".removeprefix("file://")
    return pq.read_table(path).to_pylist() if os.path.isdir(path) else []


def rows_after(s, base, delta, table, kind):
    """A snapshot table with a delta applied: the deleted elements' rows out, the delta's rows in."""
    deleted = {r["id"] for r in batch_table(s, delta, "deletes") if r["kind"] == kind}
    rows = [r for r in batch_table(s, base, f"tables/{table}") if r["osm_id"] not in deleted]
    return rows + batch_table(s, delta, f"tables/{table}")


def key(rows):
    return sorted(
        json.dumps({k: v for k, v in r.items() if k != "__gen"}, sort_keys=True, default=str) for r in rows
    )


def test_import_and_diff_match_a_fresh_import(spark, settings, tmp_path):
    s = settings
    s.big_unit_nodes = 3  # most units take the big-unit path; the fresh import none
    import_data(s, tmp_path, (NODES, WAYS, RELS), "a")
    fs = Storage(spark)
    st = state.load(fs, s)
    assert st["batch"] == 1 and st["sequence"] == 10 and st["current_run"] is None
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {s.table('nodes')}").collect()}
    assert props["write.merge.mode"] == "merge-on-read"
    assert tables.row_counts(spark, s)["nodes"] == 50

    diffs.run(s, until=11)
    st = state.load(fs, s)
    assert st["batch"] == 2 and st["sequence"] == 11
    delta = fs.read_json(f"{outbox.batch_dir(s.outbox, 2)}/{outbox.MANIFEST}")
    assert delta["kind"] == "delta" and delta["base"] == 1 and delta["deletes"] > 0 and delta["tiles"] > 0
    assert tables.row_counts(spark, s)["nodes"] == 50
    assert not fs.exists(state.lock_path(s))

    fresh = Settings(**{**vars(s), "big_unit_nodes": 100_000})
    import_data(fresh, tmp_path, after_diff(), "b")
    assert cache(spark, s) == cache(spark, fresh)
    for table, kind in (("planet_osm_point", "n"), ("planet_osm_line", "w")):
        assert key(rows_after(s, 1, 2, table, kind)) == key(batch_table(fresh, 1, f"tables/{table}"))


def test_crashed_run_rolls_back(spark, settings, tmp_path, monkeypatch):
    s = settings
    import_data(s, tmp_path, (NODES, WAYS, RELS), "c")
    before = cache(spark, s)

    def crash(*args, **kwargs):
        raise RuntimeError("crash after the merge")

    monkeypatch.setattr(diffs.pipeline, "write_batch", crash)
    with pytest.raises(RuntimeError):
        diffs.run(s, until=11)
    fs = Storage(spark)
    assert state.load(fs, s)["current_run"]["batch"] == 2
    assert cache(spark, s) != before  # merged
    diffs.recover(spark, fs, s, state.load(fs, s))
    assert cache(spark, s) == before and state.load(fs, s)["current_run"] is None


def test_lock(spark, settings, tmp_path):
    s = settings
    import_data(s, tmp_path, (NODES, WAYS, RELS), "d")
    fs = Storage(spark)
    fs.write_json(state.lock_path(s), {"application": "app-other", "command": "run"})
    with pytest.raises(SystemExit, match="locked"):
        diffs.run(s, until=11)
    # a later attempt of the same application takes the lock over
    fs.write_json(state.lock_path(s), {"application": spark.sparkContext.applicationId})
    diffs.run(s, until=11)
    assert state.load(fs, s)["sequence"] == 11 and not fs.exists(state.lock_path(s))
    fs.write_json(state.lock_path(s), {"application": "app-other"})
    cli.unlock(s)
    assert not fs.exists(state.lock_path(s))


def test_maintain_and_atomic_state(spark, settings, tmp_path):
    s = settings
    import_data(s, tmp_path, (NODES, WAYS, RELS), "e")
    diffs.run(s, until=11)
    cli.maintain(s)
    assert spark.table(s.table("nodes")).count() == 50
    assert set(tables.row_counts(spark, s)) == set(tables.CACHE_TABLES)
    fs = Storage(spark)
    fs.write_json(state.path(s) + ".x", {"a": 1})
    fs.write_json(state.path(s) + ".x", {"a": 2})
    assert fs.read_json(state.path(s) + ".x") == {"a": 2} and not fs.exists(state.path(s) + ".x.tmp")
