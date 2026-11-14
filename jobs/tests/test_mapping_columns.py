# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Column value functions."""

import numpy as np
import pytest
import shapely

from osm_spark_import.mapping.columns import Ctx, column_spec
from osm_spark_import.mapping.config import Column


def ctx(tags=None, match_value="", geom=None):
    return Ctx(tags=dict(tags or {}), elem_id=0, geom=geom, match_key="", match_value=match_value)


def func(type_, key="", args=None):
    """The value function of a column, called with (tag value, context)."""
    return column_spec(Column(name="c", type=type_, key=key, args=args or {})).func


def test_bool():
    # TestBool
    f = func("bool")
    for v in ("", "false", "no", "0"):
        assert f(v, ctx()) is False
    for v in ("yes", "1", "true"):
        assert f(v, ctx()) is True
    # Bool defaults to true
    assert f("other", ctx()) is True


def test_integer():
    # TestInteger
    f = func("integer")
    assert f("", ctx()) is None
    assert f("bar", ctx()) is None
    assert f("1e6", ctx()) is None
    assert f("0", ctx()) == 0
    assert f("123456", ctx()) == 123456
    assert f("-123456", ctx()) == -123456
    # >2^32, but <2^64, Integer type defaults to int32
    assert f("1000000000000000000", ctx()) is None
    # >2^64
    assert f("19082139812039812093908123", ctx()) is None


def test_zorder():
    # TestZOrder
    f = func("zorder", args={"key": "fips", "ranks": ["AA", "CC", "FF", "ZZ"]})
    assert f("", ctx({})) == 0  # missing
    assert f("", ctx({"fips": "ABCD"})) == 0  # unknown
    assert f("", ctx({"fips": "AA"})) == 4
    assert f("", ctx({"fips": "CC"})) == 3
    assert f("", ctx({"fips": "ZZ"})) == 1


ENUM_CASES = [("", 0), ("ABCD", 0), ("AA", 1), ("CC", 2), ("ZZ", 4)]


@pytest.mark.parametrize("key,expected", ENUM_CASES)
def test_enumerate_match(key, expected):
    # TestEnumerate_Match: enumerate by matched mapping value
    f = func("enumerate", args={"values": ["AA", "CC", "FF", "ZZ"]})
    assert f("", ctx(match_value=key)) == expected


@pytest.mark.parametrize("key,expected", ENUM_CASES)
def test_enumerate_key(key, expected):
    # TestEnumerate_Key: enumerate by key (the value is passed as val)
    f = func("enumerate", key="fips", args={"values": ["AA", "CC", "FF", "ZZ"]})
    assert f(key, ctx()) == expected


NIL = 999  # marker value


@pytest.mark.parametrize(
    "key,tags,expected",
    [
        ("unknown", None, 5),
        ("path", None, 1),
        ("residential", None, 4),
        ("residential", None, 4),
        ("motorway", None, 11),
        ("path", {"bridge": "yes"}, 12),
        ("path", {"layer": "1"}, 12),
        ("path", {"tunnel": "yes"}, -10),
        ("unknown", {"tunnel": "yes"}, -6),
        ("unknown", {"tunnel": "yes", "layer": "1"}, 5),
        ("unknown", {"tunnel": "yes", "layer": "123456789123456789"}, NIL),
    ],
)
def test_way_zorder(key, tags, expected):
    # TestWayZOrder
    f = func(
        "wayzorder",
        args={
            "default": float(5),
            "ranks": [
                "path",
                "footway",
                "pedestrian",
                "residential",
                "light_rail",
                "primary",
                "tram",
                "rail",
                "trunk",
                "motorway_link",
                "motorway",
            ],
        },
    )
    v = f("", ctx(tags, match_value=key))
    if expected == NIL:
        assert v is None
    else:
        assert v == expected


@pytest.mark.parametrize(
    "wkt,expected,type_",
    [
        ("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 100.0, "area"),
        ("POLYGON((-10 0, 10 0, 10 10, -10 10, -10 0))", 200.0, "area"),
        ("POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))", 400.0, "webmerc_area"),
        (
            "POLYGON((1000000  2000000, 1001000  2000000, 1001000  2001000, 1000000  2001000, 1000000  2000000))",
            1000000.0,
            "area",
        ),
        (
            "POLYGON((1000000  2000000, 1001000  2000000, 1001000  2001000, 1000000  2001000, 1000000  2000000))",
            907733.750000,
            "webmerc_area",
        ),
        (
            "POLYGON((1000000  5000000, 1001000  5000000, 1001000  5001000, 1000000  5001000, 1000000  5000000))",
            570974.687500,
            "webmerc_area",
        ),
        (
            "POLYGON((1000000 10000000, 1001000 10000000, 1001000 10001000, 1000000 10001000, 1000000 10000000))",
            159667.406250,
            "webmerc_area",
        ),
        (
            "POLYGON((1284931 6129149,1284931 6129153,1284931 6129174,1285008 6129171,1285008 6129155,1285008 6129146,1284931 6129149))",
            1925.000000,
            "area",
        ),
        (
            "POLYGON((1284931 6129149,1284931 6129153,1284931 6129174,1285008 6129171,1285008 6129155,1285008 6129146,1284931 6129149))",
            857.418396,
            "webmerc_area",
        ),
        # 100x100m square between ~20N and ~70N transformed from UTM to Webmerc
        (
            "POLYGON ((1212900 2099809, 1212900 2099916, 1212794 2099916, 1212794 2099809, 1212900 2099809))",
            10196.298828,
            "webmerc_area",
        ),
        (
            "POLYGON ((1227489 3193498, 1227489 3193613, 1227374 3193613, 1227374 3193498, 1227489 3193498))",
            10394.006836,
            "webmerc_area",
        ),
        (
            "POLYGON ((1250827 4379962, 1250827 4380090, 1250700 4380090, 1250700 4379962, 1250827 4379962))",
            10484.050781,
            "webmerc_area",
        ),
        (
            "POLYGON ((1287373 5712461, 1287373 5712609, 1287226 5712609, 1287226 5712461, 1287373 5712461))",
            10659.601562,
            "webmerc_area",
        ),
        (
            "POLYGON ((1346379 7276530, 1346379 7276709, 1346199 7276709, 1346199 7276530, 1346379 7276530))",
            10834.080078,
            "webmerc_area",
        ),
        (
            "POLYGON ((1449880 9229305, 1449880 9229543, 1449643 9229543, 1449643 9229305, 1449880 9229305))",
            11212.663086,
            "webmerc_area",
        ),
        (
            "POLYGON ((1665035 11920408, 1665035 11920770, 1664673 11920770, 1664673 11920408, 1665035 11920408))",
            11903.427734,
            "webmerc_area",
        ),
    ],
)
def test_area_column(wkt, expected, type_):
    # TestAreaColumn: float32 equality
    v = func(type_)("", ctx(geom=shapely.from_wkt(wkt)))
    assert isinstance(v, np.float32)
    assert v == np.float32(expected)


def test_make_suffix_replace():
    # TestMakeSuffixReplace
    f = func("string_suffixreplace", key="name", args={"suffixes": {"Straße": "Str.", "straße": "str."}})
    assert f("Hauptstraße", None) == "Hauptstr."
    assert f("", None) == ""
    assert f("Foostraßeee", None) == "Foostraßeee"


@pytest.mark.parametrize(
    "include,tags,expected",
    [
        (None, {}, ""),
        (None, {"key": "value"}, '"key"=>"value"'),
        (None, {'"key"': "'\"value\"'"}, r'''"\"key\""=>"'\"value\"'"'''),
        (None, {"\\": "\\\\\\\\"}, r'"\\"=>"\\\\\\\\"'),
        (None, {"Ümlåütê=>": ""}, '"Ümlåütê=>"=>""'),
        (["key1", "key2"], {"key": "value"}, ""),
        (["key1", "key2"], {"key1": "value"}, '"key1"=>"value"'),
        (["key1", "key2"], {"key": "value", "key2": "value"}, '"key2"=>"value"'),
    ],
)
def test_hstore_string(include, tags, expected):
    # TestHstoreString
    f = func("hstore_tags", args=None if include is None else {"include": include})
    assert f("", ctx(tags)) == expected


def test_hstore_string_multiple():
    # TestHstoreString: multiple tags, can be in any order
    f = func("hstore_tags")
    assert f("", ctx({"key1": "value", "key2": "value"})) in (
        '"key1"=>"value", "key2"=>"value"',
        '"key2"=>"value", "key1"=>"value"',
    )
