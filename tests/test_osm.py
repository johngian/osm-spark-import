import json
import os

import numpy as np
import osmium
import pyarrow as pa
import shapely

from osm_spark_import import osm
from osm_spark_import.mapping import config, geom
from osm_spark_import.mapping.writer import Builder, Mapping

DEFAULT_MAPPING = os.path.join(os.path.dirname(__file__), "..", "mappings", "default.yml")


def mp():
    return Mapping(config.load(DEFAULT_MAPPING), 3857)


def q(lon, lat):
    return round(lon * 1e7), round(lat * 1e7)


def coords(*pts):
    out = []
    for i, lon, lat in pts:
        x, y = q(lon, lat)
        out.append({"id": i, "lon": x, "lat": y})
    return out


SQUARE = coords((1, 10.0, 50.0), (2, 10.01, 50.0), (3, 10.01, 50.01), (4, 10.0, 50.01))


def node(i, lon, lat, tags):
    x, y = q(lon, lat)
    return {"k": "n", "id": i, "tags": list(tags.items()), "lon": x, "lat": y}


def way(i, tags, refs, nodes=SQUARE):
    return {"k": "w", "id": i, "tags": list(tags.items()), "refs": refs, "nodes": nodes}


def rel(i, tags, members, ways, nodes=SQUARE, mnodes=None, rels=None):
    return {
        "k": "r",
        "id": i,
        "tags": list(tags.items()),
        "members": members,
        "ways": ways,
        "nodes": nodes,
        "mnodes": mnodes or [],
        "rels": rels or [],
    }


def rows(units):
    m = mp()
    b = Builder(m)
    b.process(units)
    out = {}
    for r in b.rows:
        names = [c.name for c in m.columns[r.table]]
        out.setdefault((r.table, r.id), []).append(dict(zip(names, r.values, strict=False)))
    return out


def test_coordinates():
    x, y = geom.project(np.array([8.0]), np.array([53.0]), 3857)
    assert abs(x[0] - 890555.9263461898) < 1e-6 and abs(y[0] - 6982997.920389788) < 1e-6


def test_point_with_tags_filtered():
    r = rows([node(5, 10.0, 50.0, {"amenity": "cafe", "name": "C", "source": "survey", "note": "x"})])
    (row,) = r[("planet_osm_point", 5)]
    assert row["osm_id"] == 5 and row["amenity"] == "cafe" and row["name"] == "C" and row["highway"] == ""
    # tags.exclude removes source and note before matching and hstore
    assert row["tags"] == '"amenity"=>"cafe", "name"=>"C"' or set(row["tags"].split(", ")) == {
        '"amenity"=>"cafe"',
        '"name"=>"C"',
    }


def test_closed_building_is_line_and_polygon():
    # mapping __any__: the area_tags filter sees key __any__, so closed
    # buildings are written to planet_osm_line too
    r = rows([way(20, {"building": "yes"}, [1, 2, 3, 4, 1])])
    assert ("planet_osm_line", 20) in r and ("planet_osm_polygon", 20) in r
    poly = r[("planet_osm_polygon", 20)][0]
    assert poly["building"] == "yes" and poly["way_area"] > 0
    assert shapely.get_srid(poly["way"]) in (0, 3857)
    assert ("planet_osm_polygon_landuse", 20) not in r


def test_area_no_and_linear_tags():
    r = rows(
        [
            way(21, {"highway": "pedestrian"}, [1, 2, 3, 4, 1]),
            way(22, {"landuse": "forest", "area": "no"}, [1, 2, 3, 4, 1]),
        ]
    )
    # __any__ again: linear_tags are checked against the matched key (__any__),
    # so a closed highway=pedestrian is a polygon too; only area=no/yes count
    assert ("planet_osm_line", 21) in r and ("planet_osm_polygon", 21) in r
    assert ("planet_osm_polygon", 22) not in r and ("planet_osm_polygon_landuse", 22) not in r


def test_multipolygon_and_wayzorder():
    r = rows(
        [
            rel(
                30,
                {"type": "multipolygon", "landuse": "forest", "wikidata": "Q1"},
                [{"type": "w", "ref": 31, "role": "outer"}, {"type": "w", "ref": 32, "role": "outer"}],
                [{"id": 31, "tags": [], "refs": [1, 2, 3]}, {"id": 32, "tags": [], "refs": [3, 4, 1]}],
            ),
            way(40, {"highway": "primary", "bridge": "yes", "layer": "1"}, [1, 2]),
        ]
    )
    poly = r[("planet_osm_polygon_landuse", -30)][0]
    assert poly["osm_id"] == -30 and poly["landuse"] == "forest" and poly["z_order"] == 43
    assert ("wikidata_relation_polygon", -30) in r and ("water", -30) not in r
    # the line table maps __any__: the matched value is "__any__", so the
    # highway rank is 0 and only layer/bridge/tunnel count
    assert r[("planet_osm_line", 40)][0]["z_order"] == 10 + 10


def test_admin_relation_members():
    r = rows(
        [
            rel(
                50,
                {"type": "boundary", "boundary": "administrative", "admin_level": "2", "name": "X"},
                [{"type": "w", "ref": 51, "role": "outer"}, {"type": "n", "ref": 5, "role": "label"}],
                [{"id": 51, "tags": [("maritime", "yes")], "refs": [1, 2, 3, 4, 1]}],
                nodes=SQUARE + coords((5, 10.005, 50.005)),
            ),
        ]
    )
    members = r[("admin", -50)]
    assert [m["osm_id"] for m in members] == [51, 5]  # from_member: the member's ID
    assert [m["relation_id"] for m in members] == [-50, -50]
    assert [m["maritime"] for m in members] == [True, False]
    assert members[0]["way"].geom_type == "LineString" and members[1]["way"].geom_type == "Point"


def test_relation_skipped_if_member_way_missing():
    r = rows(
        [
            rel(
                60,
                {"type": "multipolygon", "landuse": "forest"},
                [{"type": "w", "ref": 61, "role": "outer"}, {"type": "w", "ref": 62, "role": "outer"}],
                [{"id": 61, "tags": [], "refs": [1, 2, 3, 4, 1]}],
            )
        ]
    )
    assert not r


def test_build_rows_json_and_tiles():
    b = osm.build_rows([way(20, {"building": "yes"}, [1, 2, 3, 4, 1])], mp(), expire_zoom=15)
    out = b.to_pylist()
    tiles = [x["tile"] for x in out if x["tile"]]
    data = [x for x in out if x["tbl"] == "planet_osm_polygon"]
    assert tiles and all(t.startswith("15/") for t in tiles)
    vals = json.loads(data[0]["vals"])
    assert "way" not in vals and isinstance(vals["way_area"], float)
    assert shapely.get_srid(shapely.from_wkb(data[0]["geom"])) == 3857


def write_pbf(path, n_nodes=5000):
    with osmium.SimpleWriter(str(path)) as w:
        for i in range(1, n_nodes + 1):
            tags = {"amenity": "bench"} if i % 100 == 0 else {}
            w.add_node(osmium.osm.mutable.Node(id=i, location=(i / 10000, 1.0), tags=tags))
        w.add_way(osmium.osm.mutable.Way(id=1, nodes=[1, 2, 3], tags={"highway": "path"}))
        w.add_relation(
            osmium.osm.mutable.Relation(id=1, members=[("w", 1, "outer")], tags={"type": "multipolygon"})
        )


def test_split_and_decode(tmp_path):
    pbf = tmp_path / "t.osm.pbf"
    write_pbf(pbf)
    whole = pa.Table.from_batches(osm.decode_pbf(pbf.read_bytes()))
    chunks = []

    def open_chunk(i):
        chunks.append(tmp_path / f"chunk-{i}.osm.pbf")
        return open(chunks[-1], "wb")

    with open(pbf, "rb") as f:
        header, sizes = osm.split(f, 1, open_chunk)  # one blob per chunk
    assert len(chunks) == len(sizes) > 1 and [c.stat().st_size for c in chunks] == sizes
    assert all(c.read_bytes().startswith(header) for c in chunks)
    parts = pa.concat_tables(pa.Table.from_batches(osm.decode_pbf(c.read_bytes())) for c in chunks)
    assert whole.num_rows == 5002

    def key(t):
        return sorted(zip(t.column("k").to_pylist(), t.column("id").to_pylist(), strict=False))

    assert key(parts) == key(whole)
    nodes = {r["id"]: r for r in whole.to_pylist() if r["k"] == "n"}
    assert nodes[100]["tags"] == [("amenity", "bench")]
    assert nodes[100]["lon"] == 100 * 1000 and nodes[1]["tags"] is None
