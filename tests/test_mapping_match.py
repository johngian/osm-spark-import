# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Tag filters, matchers and table filters."""

from pathlib import Path

import pytest

from osm_spark_import.mapping import config
from osm_spark_import.mapping.match import matchers
from osm_spark_import.mapping.tags import ExcludeFilter, node_filter, relation_filter, way_filter

DATA = Path(__file__).parent / "data"

CLOSED = [1, 2, 3, 4, 1]  # fake closed way for area matching


def matches_equal(expected, actual):
    """Same tables with same key/value, any order."""
    if len(expected) != len(actual):
        return False
    exp = {(t, sub): (k, v) for k, v, t, sub in expected}
    act = {(m.table, m.sub): (m.key, m.value) for m in actual}
    return exp == act


# --- TestTagFilterNodes ------------------------------------------------------

POINT_MAPPING = """
    tables:
      places:
        type: point
        columns:
        - key: name
          name: name
          type: string
        - args:
            values:
            - village
            - town
            - city
            - county
          name: z_order
          type: enumerate
        - key: population
          name: population
          type: integer
        mapping:
          place:
          - city
          - town
          - village
      transport_points:
        type: point
        columns:
        mapping:
          highway: [bus_stop]
      highways:
        type: linestring
        mapping:
          highway: [__any__] # ignored as we test node filters
"""


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({}, {}),
        ({"name": "foo"}, {"name": "foo"}),
        ({"name": "foo", "unknown": "foo"}, {"name": "foo"}),
        ({"place": "unknown"}, {}),
        ({"place": "village"}, {"place": "village"}),
        ({"population": "1000"}, {"population": "1000"}),
        ({"highway": "bus_stop"}, {"highway": "bus_stop"}),
        ({"highway": "residential"}, {}),
    ],
)
def test_tag_filter_nodes(tags, expected):
    # TestTagFilterNodes
    f = node_filter(config.from_text(POINT_MAPPING))
    assert f.filter(tags) == expected


# --- TestTagFilterWays -------------------------------------------------------

WAY_FILTER_MAPPING = """
    tables:
      buildings:
        type: polygon
        mapping:
          building: [__any__]
      highways:
        type: linestring
        columns:
            - key: name
              name: name
              type: string
            - key: tunnel
              name: tunnel
              type: boolint
            - key: oneway
              name: oneway
              type: direction
        mapping:
          highway:
            - track
      places:
        type: point
        mapping:
          place:  # ignored as we test ways
          - city
          - town
          - village
"""


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({}, {}),
        ({"name": "foo"}, {"name": "foo"}),
        ({"name": "foo", "unknown": "foo"}, {"name": "foo"}),
        ({"highway": "unknown"}, {}),
        ({"highway": "track"}, {"highway": "track"}),
        ({"building": "whatever"}, {"building": "whatever"}),
        ({"place": "village"}, {}),
        ({"oneway": "yes", "tunnel": "1"}, {"oneway": "yes", "tunnel": "1"}),
    ],
)
def test_tag_filter_ways(tags, expected):
    # TestTagFilterWays
    f = way_filter(config.from_text(WAY_FILTER_MAPPING))
    assert f.filter(tags) == expected


# --- TestTagFilterRelations --------------------------------------------------

REL_FILTER_MAPPING = """
    tags:
        include: [source]
    tables:
      landuse:
        type: polygon
        mapping:
          landuse: [farm]
      buildings:
        type: polygon
        columns:
          - key: name
            type: string
            name: name
        mapping:
          building: [__any__]
      highways:
        type: linestring
        mapping:
          highway: # imported for relations
            - track
      places:
        type: point
        mapping:
          place: # ignored as we test ways
          - city
          - town
          - village
"""


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({}, {}),
        ({"name": "foo"}, {"name": "foo"}),
        ({"unknown": "foo"}, {}),
        ({"landuse": "unknown"}, {}),
        ({"highway": "track"}, {"highway": "track"}),
        ({"place": "town"}, {}),
        ({"landuse": "farm"}, {"landuse": "farm"}),
        ({"landuse": "farm", "type": "multipolygon"}, {"landuse": "farm", "type": "multipolygon"}),
        ({"type": "multipolygon"}, {"type": "multipolygon"}),
        ({"type": "boundary"}, {"type": "boundary"}),
        ({"building": "yes"}, {"building": "yes"}),
        ({"source": "JOSM"}, {"source": "JOSM"}),
    ],
)
def test_tag_filter_relations(tags, expected):
    # TestTagFilterRelations
    f = relation_filter(config.from_text(REL_FILTER_MAPPING))
    assert f.filter(tags) == expected


# --- TestPointMatcher --------------------------------------------------------


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({"unknown": "baz"}, []),
        ({"place": "unknown"}, []),
        ({"place": "city"}, [("place", "city", "places", "")]),
        ({"place": "city", "highway": "residential"}, [("place", "city", "places", "")]),
        (
            {"place": "city", "highway": "bus_stop"},
            [("place", "city", "places", ""), ("highway", "bus_stop", "transport_points", "")],
        ),
    ],
)
def test_point_matcher(tags, expected):
    # TestPointMatcher
    m = matchers(config.from_text(POINT_MAPPING)).point
    actual = m.match_node(tags)
    assert matches_equal(expected, actual), actual


# --- TestLineStringMatcher ---------------------------------------------------

LINE_MAPPING = """
    areas:
      area_tags: [buildings, landuse, leisure, natural, aeroway]
      linear_tags: [highway, barrier]
    tables:
      landuse:
        type: polygon
        mapping:
          landuse: [park]
      aeroways:
        type: linestring
        mapping:
          aeroway: [runway]
      barrierways:
        type: linestring
        mapping:
          barrier: [hedge]
      roads:
        type: linestring
        mappings:
          roads:
            mapping:
              highway:
                - track
                - pedestrian
                - footway
                - secondary
          railway:
            mapping:
              railway:
                - tram
      places:
        type: point
        mapping:
          place:  # ignored as we test ways
          - city
          - town
          - village
"""


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({"unknown": "baz"}, []),
        ({"highway": "unknown"}, []),
        ({"place": "city"}, []),
        ({"highway": "pedestrian"}, [("highway", "pedestrian", "roads", "roads")]),
        # exclude_tags area=yes
        ({"highway": "pedestrian", "area": "yes"}, []),
        ({"barrier": "hedge"}, [("barrier", "hedge", "barrierways", "")]),
        ({"barrier": "hedge", "area": "yes"}, []),
        ({"aeroway": "runway"}, []),
        ({"aeroway": "runway", "area": "no"}, [("aeroway", "runway", "aeroways", "")]),
        (
            {"highway": "secondary", "railway": "tram"},
            [("highway", "secondary", "roads", "roads"), ("railway", "tram", "roads", "railway")],
        ),
        # landusages not a linestring table
        (
            {"highway": "footway", "landuse": "park", "barrier": "hedge"},
            [("highway", "footway", "roads", "roads"), ("barrier", "hedge", "barrierways", "")],
        ),
    ],
)
def test_linestring_matcher(tags, expected):
    # TestLineStringMatcher
    m = matchers(config.from_text(LINE_MAPPING)).linestring
    actual = m.match_way(tags, CLOSED)
    assert matches_equal(expected, actual), actual


# --- TestPolygonMatcher_MatchWay / _MatchRelation ----------------------------

POLYGON_MAPPING = """
    areas:
      area_tags: [buildings, landuse, leisure, natural, aeroway]
      linear_tags: [highway, barrier]
    tables:
      landusages:
        type: polygon
        mapping:
            amenity: [university]
            landuse: [farm, forest]
            leisure: [park]
            landuse: [park]
            highway: [footway]
            barrier: [hedge]
      transport_areas:
        type: polygon
        mapping:
          aeroway: [runway, apron]
      barrierways:
        type: linestring
        mapping:
          barrier: [hedge]
      buildings:
        type: polygon
        mapping:
          building: [__any__]
      amenity_areas:
        type: polygon
        mapping:
          building: [shop]
      highways:
        type: linestring
        mapping:
          highway: # imported for relations
            - track
      admin:
        type: polygon
        mapping:
          boundary: [administrative]
"""


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({}, []),
        ({"unknown": "baz"}, []),
        ({"landuse": "unknown"}, []),
        ({"landuse": "unknown", "type": "multipolygon"}, []),
        ({"building": "yes"}, [("building", "yes", "buildings", "")]),
        ({"building": "residential"}, [("building", "residential", "buildings", "")]),
        # line type requires area=yes
        ({"barrier": "hedge"}, []),
        ({"barrier": "hedge", "area": "yes"}, [("barrier", "hedge", "landusages", "")]),
        (
            {"building": "shop"},
            [("building", "shop", "buildings", ""), ("building", "shop", "amenity_areas", "")],
        ),
        (
            {"aeroway": "apron", "landuse": "farm"},
            [("aeroway", "apron", "transport_areas", ""), ("landuse", "farm", "landusages", "")],
        ),
        ({"landuse": "farm", "highway": "secondary"}, [("landuse", "farm", "landusages", "")]),
        ({"highway": "footway"}, []),
        ({"highway": "footway", "area": "yes"}, [("highway", "footway", "landusages", "")]),
        ({"boundary": "administrative", "admin_level": "8"}, [("boundary", "administrative", "admin", "")]),
        # landusages mapping order (amenity, landuse, leisure, landuse): the first wins
        ({"landuse": "forest", "leisure": "park"}, [("landuse", "forest", "landusages", "")]),
        ({"landuse": "park", "leisure": "park"}, [("leisure", "park", "landusages", "")]),
        (
            {"landuse": "park", "leisure": "park", "amenity": "university"},
            [("amenity", "university", "landusages", "")],
        ),
    ],
)
def test_polygon_matcher_match_way(tags, expected):
    # TestPolygonMatcher_MatchWay
    m = matchers(config.from_text(POLYGON_MAPPING)).polygon
    actual = m.match_way(tags, CLOSED)
    assert matches_equal(expected, actual), actual


def test_polygon_matcher_match_way_open():
    # TestPolygonMatcher_MatchWay: "open way matched as polygon"
    m = matchers(config.from_text(POLYGON_MAPPING)).polygon
    assert m.match_way({"building": "yes"}, []) == []


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({}, []),
        ({"unknown": "baz"}, []),
        ({"landuse": "unknown"}, []),
        ({"landuse": "unknown", "type": "multipolygon"}, []),
        ({"building": "yes"}, []),
        ({"building": "yes", "type": "multipolygon"}, [("building", "yes", "buildings", "")]),
        ({"building": "residential", "type": "multipolygon"}, [("building", "residential", "buildings", "")]),
        # line type requires area=yes
        ({"barrier": "hedge", "type": "multipolygon"}, []),
        (
            {"barrier": "hedge", "area": "yes", "type": "multipolygon"},
            [("barrier", "hedge", "landusages", "")],
        ),
        (
            {"building": "shop", "type": "multipolygon"},
            [("building", "shop", "buildings", ""), ("building", "shop", "amenity_areas", "")],
        ),
        (
            {"aeroway": "apron", "landuse": "farm", "type": "multipolygon"},
            [("aeroway", "apron", "transport_areas", ""), ("landuse", "farm", "landusages", "")],
        ),
        (
            {"landuse": "farm", "highway": "secondary", "type": "multipolygon"},
            [("landuse", "farm", "landusages", "")],
        ),
        ({"highway": "footway", "type": "multipolygon"}, []),
        (
            {"highway": "footway", "area": "yes", "type": "multipolygon"},
            [("highway", "footway", "landusages", "")],
        ),
        ({"boundary": "administrative", "admin_level": "8"}, []),
        (
            {"boundary": "administrative", "admin_level": "8", "type": "boundary"},
            [("boundary", "administrative", "admin", "")],
        ),
    ],
)
def test_polygon_matcher_match_relation(tags, expected):
    # TestPolygonMatcher_MatchRelation: only type=multipolygon/boundary relations match
    m = matchers(config.from_text(POLYGON_MAPPING)).polygon
    actual = m.match_relation(tags)
    assert matches_equal(expected, actual), actual


# --- TestExcludeFilter -------------------------------------------------------


def test_exclude_filter():
    tags = {"source": "1", "tiger:foo": "1", "source:foo": "1"}
    # no matches
    assert ExcludeFilter([]).filter(tags) == {"source": "1", "tiger:foo": "1", "source:foo": "1"}
    # match all
    assert ExcludeFilter(["*"]).filter(tags) == {}
    # fixed string and wildcard match
    assert ExcludeFilter(["source", "tiger:*"]).filter(tags) == {"source:foo": "1"}


def test_filter_nodes_test_mapping():
    # BenchmarkFilterNodes (as a test): test_mapping.yml node filter
    f = node_filter(config.load(str(DATA / "test_mapping.yml")))
    tags = f.filter({"population": "0", "name": "foo", "boring": "true"})
    assert tags == {"population": "0", "name": "foo"}


# --- table filters -------------------------------------------------------------


def filter_test(mapping, accept, reject):
    """Open ways through the linestring matcher."""
    ls = matchers(config.from_text(mapping)).linestring
    for tags in accept:
        assert ls.match_way(tags, []), f"Not Accepted: {tags}"
    for tags in reject:
        assert not ls.match_way(tags, []), f"Not Rejected: {tags}"


ADMIN_REJECT = [
    {"admin_level": "0", "boundary": "administrative"},
    {"admin_level": "1", "boundary": "administrative"},
    {"admin_level": "2", "boundary": "postal_code"},
    {"admin_level": "2", "boundary": ""},
    {"admin_level": "2", "boundary": "__nil__"},
    {"admin_level": "4", "boundary": "census"},
    {"admin_level": "3", "boundary": "administrative", "name": "NX"},
    {"admin_level": "2"},
    {"admin_level": "4"},
    {"admin_level": "❤"},
    {"admin_level": "__any__", "boundary": "__any__"},
    {"boundary": "administrative"},
    {"boundary": "maritime"},
    {"name": "maritime"},
]
ADMIN_ACCEPT = [
    {"admin_level": "2", "boundary": "administrative"},
    {"admin_level": "2", "boundary": "maritime"},
    {"admin_level": "4", "boundary": "administrative", "name": "N4"},
    {"admin_level": "4", "boundary": "maritime", "name": "N4"},
]


def test_filters_require():
    # TestFilters_require
    filter_test(
        """
tables:
  admin:
    fields:
    - name: id
      type: id
    - key: admin_level
      name: admin_level
      type: integer
    filters:
      require:
        boundary: ["administrative","maritime"]
    mapping:
      admin_level: ['2','4']
    type: linestring
""",
        ADMIN_ACCEPT,
        ADMIN_REJECT,
    )


def test_filters_require2():
    # TestFilters_require2: same as above, but mapping and filters are swapped
    filter_test(
        """
tables:
  admin:
    fields:
    - name: id
      type: id
    - key: admin_level
      name: admin_level
      type: integer
    filters:
      require:
        admin_level: ["2","4"]
    mapping:
      boundary:
      - administrative
      - maritime
    type: linestring
""",
        ADMIN_ACCEPT,
        ADMIN_REJECT,
    )


def test_filters_building():
    # TestFilters_building
    filter_test(
        r"""
tables:
  buildings:
    fields:
    - name: id
      type: id
    - key: building
      name: building
      type: string
    filters:
      reject:
        building: ["no","none"]
      require_regexp:
        'addr:housenumber': '^\d+[a-zA-Z,]*$'
        building: '^[a-z_]+$'
    mapping:
      building:
      - __any__
    type: linestring
""",
        [
            {"building": "yes", "addr:housenumber": "1a"},
            {"building": "house", "addr:housenumber": "131"},
            {"building": "residential", "addr:housenumber": "21"},
            {"building": "garage", "addr:housenumber": "0"},
            {"building": "hut", "addr:housenumber": "99999999"},
            {"building": "_", "addr:housenumber": "333"},
            {"building": "__any__", "addr:housenumber": "333"},
            {"building": "__nil__", "addr:housenumber": "333"},
            {"building": "y", "addr:housenumber": "1abcdefg"},
            {"building": "tower_block", "addr:housenumber": "1A"},
            {"building": "shed", "name": "N4", "addr:housenumber": "1AAA"},
            {"building": "office", "name": "N4", "addr:housenumber": "0XYAB,"},
        ],
        [
            {"building": "yes", "addr:housenumber": "aaaaa-number"},
            {"building": "house", "addr:housenumber": "1-3a"},
            {"building": "house", "addr:housenumber": "❤"},
            {"building": "house", "addr:housenumber": "two"},
            {"building": "residential", "addr:housenumber": "x21"},
            {"building": "", "addr:housenumber": "111"},
            {"building": "no"},
            {"building": "no", "addr:housenumber": "1a"},
            {"building": "No", "addr:housenumber": "1a"},
            {"building": "NO", "addr:housenumber": "1a"},
            {"building": "none"},
            {"building": "none", "addr:housenumber": "0"},
            {"building": "nONe", "addr:housenumber": "0"},
            {"building": "No"},
            {"building": "NO"},
            {"building": "NONe"},
            {"building": "Garage"},
            {"building": "Hut"},
            {"building": "Farm"},
            {"building": "tower-block"},
            {"building": "❤"},
            {"building": "Ümlåütê"},
            {"building": "木"},
            {"building": "SheD", "name": "N4"},
            {"building": "oFFice", "name": "N4"},
            {"admin_level": "2"},
            {"admin_level": "4"},
            {"boundary": "administrative"},
            {"boundary": "maritime"},
            {"name": "maritime"},
        ],
    )


def test_filters_highway_with_name():
    # TestFilters_highway_with_name
    filter_test(
        """
tables:
  highway:
    fields:
    - name: id
      type: id
    - key: highway
      name: highway
      type: string
    - key: name
      name: name
      type: string
    filters:
      require:
        name: ["__any__"]
      reject:
        highway: ["no","none"]
    mapping:
      highway:
      - __any__
    type: linestring
""",
        [
            {"highway": "residential", "name": "N1"},
            {"highway": "service", "name": "N2"},
            {"highway": "track", "name": "N3"},
            {"highway": "unclassified", "name": "N4"},
            {"highway": "path", "name": "N5"},
            {"highway": "", "name": "🌍🌎🌏"},
            {"highway": "_", "name": "N6"},
            {"highway": "y", "name": "N7"},
            {"highway": "tower_block", "name": "N8"},
            {"highway": "shed", "name": "N9"},
            {"highway": "office", "name": "N10"},
            {"highway": "SheD", "name": "N11"},
            {"highway": "oFFice", "name": "N12"},
            {"highway": "❤", "name": "❤"},
            {"highway": "Ümlåütê", "name": "Ümlåütê"},
            {"highway": "木", "name": "木"},
        ],
        [
            {"highway": "no", "name": "N1"},
            {"highway": "none", "name": "N2"},
            {"highway": "yes"},
            {"highway": "no"},
            {"highway": "none"},
            {"highway": "No"},
            {"highway": "NO"},
            {"highway": "NONe"},
            {"highway": "Garage"},
            {"highway": "residential"},
            {"highway": "path"},
            {"highway": "tower-block"},
            {"highway": "❤"},
            {"highway": "Ümlåütê"},
            {"highway": "木"},
            {"admin_level": "2"},
            {"admin_level": "4"},
            {"boundary": "administrative"},
            {"boundary": "maritime"},
            {"name": "maritime"},
        ],
    )


WATERWAY_REJECT_COMMON = [
    {"waterway": "path", "name": "N5"},
    {"waterway": "_", "name": "N6"},
    {"waterway": "y", "name": "N7"},
    {"waterway": "tower_block", "name": "N8"},
    {"waterway": "shed", "name": "N9"},
    {"waterway": "office", "name": "N10"},
    {"waterway": "SheD", "name": "N11"},
    {"waterway": "oFFice", "name": "N12"},
    {"waterway": "❤", "name": "❤"},
    {"waterway": "Ümlåütê", "name": "Ümlåütê"},
    {"waterway": "木", "name": "木"},
]


def test_filters_waterway_with_name():
    # TestFilters_waterway_with_name
    filter_test(
        r"""
tables:
  waterway:
    fields:
    - name: id
      type: id
    - key: waterway
      name: waterway
      type: string
    - key: name
      name: name
      type: string
    filters:
      require:
        name: ["__any__"]
        waterway:
        - stream
        - river
        - canal
        - drain
        - ditch
      reject:
        fixme: ['__any__']
        amenity: ['__any__']
        shop: ['__any__']
        building: ['__any__']
        tunnel: ['yes']
      reject_regexp:
        level: '^\D+.*$'
    mapping:
      waterway:
      - __any__
    type: linestring
""",
        [
            {"waterway": "stream", "name": "N1"},
            {"waterway": "river", "name": "N2"},
            {"waterway": "canal", "name": "N3"},
            {"waterway": "drain", "name": "N4"},
            {"waterway": "ditch", "name": "N5"},
            {"waterway": "stream", "name": "N1", "tunnel": "no"},
            {"waterway": "river", "name": "N2", "boat": "no"},
            {"waterway": "canal", "name": "N3"},
            {"waterway": "ditch", "name": "N4", "level": "3"},
            {"waterway": "stream", "name": "__any__"},
            {"waterway": "stream", "name": "__nil__"},
            {"waterway": "stream", "name": "❤"},
            {"waterway": "stream", "name": "木"},
            {"waterway": "stream", "name": "Ümlåütê"},
        ],
        [
            {"waterway": "ditch", "name": "N1", "fixme": "incomplete"},
            {"waterway": "stream", "name": "N1", "amenity": "parking"},
            {"waterway": "river", "name": "N2", "shop": "hairdresser"},
            {"waterway": "canal", "name": "N3", "building": "house"},
            {"waterway": "drain", "name": "N1 tunnel", "tunnel": "yes"},
            {"waterway": "river", "name": "N4", "level": "unknown"},
            {"waterway": "ditch", "name": "N4", "level": "primary"},
            *WATERWAY_REJECT_COMMON,
            {"waterway": "no", "name": "N1"},
            {"waterway": "none", "name": "N2"},
            {"waterway": "yes"},
            {"waterway": "no"},
            {"waterway": "none"},
            {"waterway": "tower-block"},
            {"waterway": "❤"},
            {"waterway": "Ümlåütê"},
            {"waterway": "木"},
            {"waterway": "__nil__", "name": "__nil__"},
            {"waterway": "__any__", "name": "__nil__"},
            {"waterway": "stream", "name": "__any__", "shop": "__any__"},
            {"waterway": "stream", "name": "__nil__", "shop": "__any__"},
            {"waterway": "stream", "name": "__any__", "shop": "__nil__"},
            {"waterway": "stream", "name": "__nil__", "shop": "__nil__"},
            {"waterway": "stream", "name": "__any__", "shop": ""},
            {"waterway": "stream", "name": "__nil__", "shop": ""},
            {"admin_level": "2"},
            {"admin_level": "4"},
            {"boundary": "administrative"},
            {"boundary": "maritime"},
            {"name": "maritime"},
        ],
    )


def test_filters_exclude_tags():
    # TestFilters_exclude_tags
    filter_test(
        """
tables:
  exclude_tags:
    _comment:  Allways Empty !
    fields:
    - name: id
      type: id
    - key: waterway
      name: waterway
      type: string
    - key: name
      name: name
      type: string
    filters:
      require:
        waterway:
         - stream
      exclude_tags:
      - ['waterway', 'river']
      - ['waterway', 'canal']
      - ['waterway', 'drain']
      - ['waterway', 'ditch']
    mapping:
      waterway:
      - __any__
    type: linestring
""",
        [
            {"waterway": "stream", "name": "N1"},
            {"waterway": "stream", "name": "N1", "tunnel": "no"},
            {"waterway": "stream", "name": "N1", "amenity": "parking"},
        ],
        [
            {"waterway": "river", "name": "N2"},
            {"waterway": "canal", "name": "N3"},
            {"waterway": "drain", "name": "N4"},
            {"waterway": "ditch", "name": "N5"},
            {"waterway": "river", "name": "N2", "boat": "no"},
            {"waterway": "canal", "name": "N3"},
            {"waterway": "ditch", "name": "N4", "level": "3"},
            {"waterway": "ditch", "name": "N1", "fixme": "incomplete"},
            {"waterway": "river", "name": "N2", "shop": "hairdresser"},
            {"waterway": "canal", "name": "N3", "building": "house"},
            {"waterway": "drain", "name": "N1 tunnel", "tunnel": "yes"},
            {"waterway": "river", "name": "N4", "level": "unknown"},
            {"waterway": "ditch", "name": "N4", "level": "primary"},
            *WATERWAY_REJECT_COMMON,
            {"waterway": "no", "name": "N1"},
            {"waterway": "none", "name": "N2"},
            {"waterway": "yes"},
            {"waterway": "no"},
            {"waterway": "none"},
            {"waterway": "tower-block"},
            {"waterway": "❤"},
            {"waterway": "Ümlåütê"},
            {"waterway": "木"},
            {"admin_level": "2"},
            {"admin_level": "4"},
            {"boundary": "administrative"},
            {"boundary": "maritime"},
            {"name": "maritime"},
        ],
    )
