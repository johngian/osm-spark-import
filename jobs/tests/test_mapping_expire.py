# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Tile lists: adaptive zoom for lines and boxes."""

import re

import pytest

from osm_spark_import.mapping.expire import TileList


@pytest.mark.parametrize(
    "nodes,expected_num,expected_level,closed",
    [
        # point
        ([(8.30, 53.26)], 1, 14, False),
        # point + paddings
        ([(0, 0)], 4, 14, False),
        ([(0.01, 0)], 2, 14, False),
        ([(0, 0.01)], 2, 14, False),
        ([(0.01, 0.01)], 1, 14, False),
        # line
        ([(8.30, 53.25), (8.30, 53.30)], 5, 14, False),
        # same line, but split into multiple segments
        ([(8.30, 53.25), (8.30, 53.27), (8.30, 53.29), (8.30, 53.30)], 5, 14, False),
        # L-shape
        ([(8.30, 53.25), (8.30, 53.30), (8.35, 53.30)], 8, 14, False),
        # line (triangle)
        ([(8.30, 53.25), (8.30, 53.30), (8.35, 53.30), (8.30, 53.25)], 11, 14, False),
        # same line but closed/polygon (triangle), whole bbox (4x5 tiles) is expired
        ([(8.30, 53.25), (8.30, 53.30), (8.35, 53.30), (8.30, 53.25)], 20, 14, True),
        # large triangle, moved zoom level up
        ([(8.30, 53.25), (8.30, 53.90), (8.85, 53.90), (8.30, 53.25)], 28, 11, True),
        # same large triangle but as line, moved just one zoom level up
        ([(8.30, 53.25), (8.30, 53.90), (8.85, 53.90), (8.30, 53.25)], 63, 13, False),
        # long line, accross world
        ([(-170, -80), (170, 80)], 17, 4, False),
        # large polygon, accross world
        ([(-160, -70), (160, -70), (160, 70), (-160, 70)], 48, 3, True),
        # expire tiles use Webmercator, ignore nodes at the poles
        ([(0, 90), (0, 89)], 0, 0, True),
        ([(0, -90), (0, -89)], 0, 0, True),
        ([(-170, 89), (170, 70)], 0, 0, True),
    ],
)
def test_tile_list_expire_nodes_adaptive(nodes, expected_num, expected_level, closed):
    # TestTileList_ExpireNodesAdaptive
    tl = TileList(14)
    tl.expire_nodes([(float(x), float(y)) for x, y in nodes], closed)
    for z in range(tl.max_zoom + 1):
        expected = expected_num if z == expected_level else 0
        assert len(tl.tiles[z]) == expected, (z, sorted(tl.tiles[z]))
    lines = tl.as_strings()
    for line in lines:
        assert re.fullmatch(r"\d+/\d+/\d+", line), line
    assert len(lines) == expected_num
