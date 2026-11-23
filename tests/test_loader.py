import csv
import io

import psycopg
import pyarrow as pa
import pytest
from test_osm import DEFAULT_MAPPING

from osm_spark_import import loader
from osm_spark_import.loader import id_expr, obsolete_batches, table_names, to_csv
from osm_spark_import.mapping import config, postgis
from osm_spark_import.mapping.writer import Mapping


def test_csv_for_copy():
    b = pa.RecordBatch.from_pydict(
        {
            "osm_id": [1, 2],
            "name": ["", None],  # empty string vs NULL
            "note": ['a "quoted",\nvalue', "x"],
            "ok": [True, None],
            "area": pa.array([38549.098, None], pa.float32()),
            "way": [b"\x01\x02", b"\xff"],
        }
    )
    text = to_csv(b, {"way"}).decode()
    # PostgreSQL CSV: unquoted empty is NULL, "" is the empty string
    rows = list(csv.reader(io.StringIO(text)))
    assert rows[0] == ["1", "", 'a "quoted",\nvalue', "true", "38549.098", "0102"]
    assert ',,"x",,,"ff"' in text


def test_id_expr():
    assert id_expr("n", False) == "id" and id_expr("w", False) == "id" and id_expr("r", False) == "-id"
    assert id_expr("w", True) == "-id" and id_expr("r", True) == "(-100000000000000000 - id)"


def test_default_mapping_ddl():
    m = Mapping(config.load(DEFAULT_MAPPING)).manifest()
    point = m["tables"]["planet_osm_point"]
    sql = postgis.create_table_sql("import", "planet_osm_point", point)
    assert sql.startswith(
        'CREATE TABLE IF NOT EXISTS "import"."planet_osm_point" (\nid BIGSERIAL,\n"osm_id" BIGINT,'
    )
    assert '"tags" HSTORE' in sql and '"way"' not in sql and sql.endswith('PRIMARY KEY ("osm_id", "id")\n);')
    assert postgis.add_geometry_column_sql("import", "planet_osm_point", point, 3857) == (
        "SELECT AddGeometryColumn('import', 'planet_osm_point', 'way', '3857', 'POINT', 2);"
    )
    poly = m["tables"]["planet_osm_polygon"]
    assert "'GEOMETRY', 2)" in postgis.add_geometry_column_sql("import", "planet_osm_polygon", poly, 3857)
    admin = postgis.create_table_sql("import", "admin", m["tables"]["admin"])
    assert 'PRIMARY KEY ("relation_id", "osm_id", "id")' in admin and '"maritime" BOOL' in admin
    gen = postgis.Generalized(m["tables"], m["generalized"])
    assert gen.order()[0] == "planet_osm_polygon_landuse_gen_z13"
    assert gen.root("planet_osm_polygon_landuse_gen_z6") == "planet_osm_polygon_landuse"
    z13 = gen.create_sql("import", "planet_osm_polygon_landuse_gen_z13")
    assert (
        'ST_MakeValid(ST_SimplifyPreserveTopology("way", 19.109257), \'method=structure\')::Geometry as "way"'
        in z13
    )
    assert z13.endswith(
        'FROM "import"."planet_osm_polygon_landuse" WHERE area>power(76.4370282852,2) AND ST_IsValid(way))'
    )
    assert postgis.index_sqls("import", "planet_osm_point", point["columns"], False) == [
        'CREATE INDEX "planet_osm_point_geom" ON "import"."planet_osm_point" USING GIST ("way")'
    ]
    assert len(table_names(m)) == 8 + 8


def test_obsolete_batches():
    st = {"import_batch": None, "production_batch": 5, "backup_batch": 3}
    assert obsolete_batches([1, 2, 3, 4, 5, 6], st) == [1, 2]
    assert (
        obsolete_batches([1, 2], {"import_batch": None, "production_batch": None, "backup_batch": None}) == []
    )


def test_loop_reconnects(monkeypatch):
    events = []

    class FakeLoader:
        def __init__(self, s):
            pass

        def connect(self):
            events.append("connect")

        def exclusive(self):
            events.append("lock")

        def step(self):
            events.append("step")
            if events.count("step") == 1:
                raise psycopg.OperationalError("server closed the connection")
            raise SystemExit(0)  # stop the loop

    monkeypatch.setattr(loader, "Loader", FakeLoader)
    monkeypatch.setattr(loader.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit):
        loader.main(["apply", "--loop"])
    assert events == ["lock", "step", "connect", "lock", "step"]
