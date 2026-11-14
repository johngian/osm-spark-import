# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Multipolygons and ring merging.

Ways are (way id, refs, nodes) with node coordinates used as they are (no
projection); relations use a ring gap of 0.1.
"""

import itertools

import pytest
import shapely

from osm_spark_import.mapping.geom import GeomError, Ring, build_rings, merge_rings, multipolygon

MAX_GAP = 0.1


def way(wid, coords):
    return (wid, [c[0] for c in coords], [(float(c[1]), float(c[2])) for c in coords])


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
    rings = build_rings([w1, w2], MAX_GAP)
    assert len(rings) == 1  # open ring is excluded
    g = multipolygon([w1, w2], MAX_GAP)
    assert shapely.is_valid(g), shapely.to_wkt(g)


# --- ring merging -------------------------------------------------------------------


def ring(wid, refs):
    return Ring(wid, refs, [(0.0, 0.0)] * len(refs))


def test_ring_merge():
    # TestRingMerge
    result = merge_rings([ring(1, [1, 2, 3]), ring(2, [3, 4, 1])])
    assert len(result) == 1
    assert result[0].refs == [1, 2, 3, 4, 1]


def test_ring_merge_missing_refs():
    # TestRingMergeMissingRefs: way without refs should not panic
    r1 = ring(1, [1, 2, 3])
    result = merge_rings([r1, ring(2, [])])
    assert len(result) == 1
    assert result[0] is r1


def test_ring_merge_reverse_endpoints():
    # TestRingMergeReverseEndpoints
    result = merge_rings([ring(1, [1, 2, 3, 4]), ring(2, [6, 5, 4]), ring(3, [1, 7, 6])])
    assert len(result) == 1
    assert result[0].refs == [6, 5, 4, 3, 2, 1, 7, 6]


def _permutations():
    # the 23 orderings other than the sorted one
    return list(itertools.permutations(range(4)))[1:]


@pytest.mark.parametrize("i", range(16))
def test_ring_merge_permutations(i):
    # TestRingMergePermutations: all permutations of 4 ring segments, each in both directions
    f1, f2, f3, f4 = (i & 1 == 0), (i & 2 == 0), (i & 4 == 0), (i & 8 == 0)
    for indices in _permutations():
        ways = [
            [1, 2, 3, 4] if f1 else [4, 3, 2, 1],
            [4, 5, 6, 7] if f2 else [7, 6, 5, 4],
            [7, 8, 9, 10] if f3 else [10, 9, 8, 7],
            [10, 11, 12, 1] if f4 else [1, 12, 11, 10],
        ]
        rings = [ring(n + 1, ways[idx]) for n, idx in enumerate(indices)]
        result = merge_rings(rings)
        assert len(result) == 1, f"not a single ring: {[r.refs for r in result]}"
        r = result[0].refs
        assert r[0] == r[-1], f"ring not closed: {r}"
        asc = all(r[k] == 1 or r[k - 1] < r[k] for k in range(1, len(r)))
        desc = all(r[k] == 12 or r[k - 1] > r[k] for k in range(1, len(r)))
        assert asc or desc, f"ring not ascending/descending: {r}"
