# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Lines, polygons and multipolygons.

Ways are (way id, [(node id, x, y)]); coordinates are used as they are (no
projection); relations use a ring gap of 0.1.
"""

import pytest
import shapely

from osm_spark_import.mapping.geom import GeomError, linestring, multipolygon, polygon

MAX_GAP = 0.1


def way(wid, coords):
    return [(float(c[1]), float(c[2])) for c in coords]


def build(*ways):
    return multipolygon(list(ways), MAX_GAP)


def check(g, area):
    assert shapely.is_valid(g), shapely.to_wkt(g)
    assert g.area == area


SQUARE_10 = [(1, 0, 0), (2, 10, 0), (3, 10, 10), (4, 0, 10), (1, 0, 0)]


def test_simple_polygon_with_hole():
    # TestSimplePolygonWithHole
    w1 = way(1, SQUARE_10)
    w2 = way(2, [(5, 2, 2), (6, 8, 2), (7, 8, 8), (8, 2, 8), (5, 2, 2)])
    check(build(w1, w2), 100 - 36)


def test_multipolygon_with_hole_and_rel_name():
    # TestMultiPolygonWithHoleAndRelName (both ways have ID 1)
    w1 = way(1, SQUARE_10)
    w2 = way(1, [(5, 2, 2), (6, 8, 2), (7, 8, 8), (8, 2, 8), (5, 2, 2)])
    check(build(w1, w2), 64)


def test_multipolygon_with_multiple_holes():
    # TestMultiPolygonWithMultipleHoles
    w1 = way(1, SQUARE_10)
    w2 = way(1, [(1, 1, 1), (2, 2, 1), (3, 2, 2), (4, 1, 2), (1, 1, 1)])
    w3 = way(3, [(1, 3, 3), (2, 4, 3), (3, 4, 4), (4, 3, 4), (1, 3, 3)])
    check(build(w1, w2, w3), 100 - 1 - 1)


def test_multipolygon_with_neasted_holes():
    # TestMultiPolygonWithNeastedHoles
    w1 = way(1, SQUARE_10)
    w2 = way(2, [(1, 1, 1), (2, 9, 1), (3, 9, 9), (4, 1, 9), (1, 1, 1)])
    w3 = way(3, [(1, 2, 2), (2, 8, 2), (3, 8, 8), (4, 2, 8), (1, 2, 2)])
    w4 = way(4, [(1, 3, 3), (2, 7, 3), (3, 7, 7), (4, 3, 7), (1, 3, 3)])
    w5 = way(5, [(1, 4, 4), (2, 6, 4), (3, 6, 6), (4, 4, 6), (1, 4, 4)])
    check(build(w1, w2, w3, w4, w5), 100 - 64 + 36 - 16 + 4)


def test_hole_touching_outer():
    w1 = way(1, SQUARE_10)
    w2 = way(2, [(5, 0, 2), (6, 5, 2), (7, 5, 8), (8, 0, 8), (5, 0, 2)])
    check(build(w1, w2), 100 - 30)


def test_polygon_from_three_ways():
    # TestPolygonFromThreeWays
    w1 = way(1, [(1, 0, 0), (2, 10, 0), (3, 10, 10)])
    w2 = way(2, [(3, 10, 10), (4, 0, 10)])
    w3 = way(3, [(4, 0, 10), (1, 0, 0)])
    check(build(w1, w2, w3), 100)


def test_touching_polygons_with_hole():
    # TestTouchingPolygonsWithHole
    w1 = way(1, SQUARE_10)
    w2 = way(2, [(2, 10, 0), (5, 30, 0), (6, 30, 10), (3, 10, 10), (2, 10, 0)])
    w3 = way(3, [(7, 2, 2), (8, 8, 2), (9, 8, 8), (10, 2, 8), (7, 2, 2)])
    check(build(w1, w2, w3), 100 + 200 - 36)


def test_inserted_ways_different_tags():
    # TestInsertedWaysDifferentTags
    w1 = way(1, [(1, 0, 0), (2, 10, 0), (3, 10, 10)])
    w2 = way(2, [(3, 10, 10), (4, 0, 10), (1, 0, 0)])
    check(build(w1, w2), 100)


def test_insert_multiple_tags():
    # TestInsertMultipleTags
    w1 = way(1, [(1, 0, 0), (2, 10, 0), (3, 10, 10)])
    w2 = way(2, [(3, 10, 10), (4, 0, 10), (1, 0, 0)])
    check(build(w1, w2), 100)


def test_broken_polygon_self_intersect():
    # TestBrokenPolygonSelfIntersect
    #  2##3    6##7
    #  #  #    ####
    #  1##4____5##8
    w1 = way(
        1,
        [
            (1, 0, 0),
            (2, 0, 10),
            (3, 10, 10),
            (4, 10, 0),
            (5, 20, 0),
            (6, 20, 10),
            (7, 30, 10),
            (8, 30, 0),
            (1, 0, 0),
        ],
    )
    w2 = way(2, [(15, 2, 2), (16, 8, 2), (17, 8, 8), (18, 2, 8), (15, 2, 2)])
    check(build(w1, w2), 200 - 36)

    w3 = way(
        1,
        [
            (4, 10, 0),
            (1, 0, 0),
            (2, 0, 10),
            (3, 10, 10),
            (4, 10, 0),
            (5, 20, 0),
            (6, 20, 10),
            (7, 30, 10),
            (8, 30, 0),
            (4, 10, 0),
        ],
    )
    check(build(w3, w2), 200 - 36)


def test_broken_polygon_self_intersect_triangle():
    # TestBrokenPolygonSelfIntersectTriangle: triangle with four points, minor overlapping
    w1 = way(1, [(1, 0, 0), (2, 0, 100), (3, 100, 50 - 0.00001), (4, 100, 50 + 0.00001), (1, 0, 0)])
    w2 = way(2, [(15, 10, 45), (16, 10, 55), (17, 20, 55), (18, 20, 45), (15, 10, 45)])
    g = build(w1, w2)
    assert shapely.is_valid(g), shapely.to_wkt(g)
    assert abs(g.area - (100 * 100 / 2 - 100)) <= 0.01

    # larger overlap
    w3 = way(1, [(1, 0, 0), (2, 0, 100), (3, 100, 50 - 1), (4, 100, 50 + 1), (1, 0, 0)])
    w4 = way(2, [(15, 10, 45), (16, 10, 55), (17, 20, 55), (18, 20, 45), (15, 10, 45)])
    g = build(w3, w4)
    assert shapely.is_valid(g), shapely.to_wkt(g)
    assert abs(g.area - (100 * 98 / 2 - 100)) <= 10


def test_open_ring():
    # TestOpenRing
    w1 = way(1, [(1, 0, 0), (2, 10, 0), (3, 10, 10), (4, 0, 10)])
    with pytest.raises(GeomError):
        build(w1)


def test_closed_and_open_ring():
    # TestClosedAndOpenRing
    w1 = way(1, SQUARE_10)
    w2 = way(2, [(5, 0, 0), (6, -5, -2)])
    check(build(w1, w2), 100)  # the open way is left out


def test_gap_closed():
    w1 = way(1, [(1, 0, 0), (2, 10, 0), (3, 10, 10)])
    w2 = way(2, [(4, 10, 10.05), (5, 0, 10), (1, 0, 0)])
    g = build(w1, w2)
    assert shapely.is_valid(g) and abs(g.area - 100) < 1


def test_linestring():
    assert shapely.get_num_coordinates(linestring([(0, 0), (0, 0), (1, 1)])) == 2
    with pytest.raises(GeomError):
        linestring([(0, 0), (0, 0)])
    with pytest.raises(GeomError):
        linestring([(0, 0)])


def test_polygon():
    g = polygon([(0, 0), (0, 1), (1, 1), (1, 1), (1, 0), (0, 0)])
    assert shapely.get_num_coordinates(g) == 5
    assert g.exterior.coords[0] == (0, 0)  # normalized
    with pytest.raises(GeomError):
        polygon([(0, 0), (1, 1), (0, 0), (0, 0)])
    with pytest.raises(GeomError):
        polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
