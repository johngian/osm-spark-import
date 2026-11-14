# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Expired tiles.

Tiles are "z/x/y" strings; the loader writes them to files
(DIR/YYYYMMDD/HHMMSS.mmm.tiles).
"""

from __future__ import annotations

import math

from .geom import merc_to_wgs, wgs_to_merc

_MERC = 20037508.342789244
_RES = []
_r = 2 * 20037508.342789244 / 256
for _ in range(20):
    _RES.append(_r)
    _r /= 2


def _u32(v: float) -> int:
    """uint32 truncation; negative values (only at the antimeridian edge)
    clamp to 0."""
    return int(v) if v >= 0 else 0


def tile_coord(lon: float, lat: float, zoom: int) -> tuple[float, float]:
    try:
        x, y = wgs_to_merc(lon, lat)
    except (OverflowError, ValueError):
        return -1.0, -1.0
    if x < -_MERC or x > _MERC or y < -_MERC or y > _MERC or x != x or y != y:
        return -1.0, -1.0
    res = _RES[zoom]
    x = x - (-_MERC)
    y = _MERC - y
    return float(x / (res * 256)), float(y / (res * 256))


def _bbox(nodes):
    minx = miny = math.inf
    maxx = maxy = -math.inf
    for lon, lat in nodes:
        if lat == 0 and lon == 0:
            continue
        if maxx < lon:
            maxx = lon
        if maxy < lat:
            maxy = lat
        if minx > lon:
            minx = lon
        if miny > lat:
            miny = lat
    # empty boxes: +-max float, which no tile covers
    big = 1.7976931348623157e308
    return (
        minx if minx != math.inf else big,
        miny if miny != math.inf else big,
        maxx if maxx != -math.inf else -big,
        maxy if maxy != -math.inf else -big,
    )


def _num_bbox_tiles(b, zoom: int) -> int:
    x1, y1 = tile_coord(b[0], b[3], zoom)
    x2, y2 = tile_coord(b[2], b[1], zoom)
    if x1 < 0 or x2 < 0:
        return 0
    return int(abs((x2 - x1 + 1) * (y2 - y1 + 1)))


def _bresenham(x1, y1, x2, y2) -> list[tuple[int, int]]:
    tiles = []
    steep = False
    dx = abs(x2 - x1)
    sx = 1.0 if (x2 - x1) > 0 else -1.0
    dy = abs(y2 - y1)
    sy = 1.0 if (y2 - y1) > 0 else -1.0
    if dy > dx:
        steep = True
        x1, y1 = y1, x1
        dx, dy = dy, dx
        sx, sy = sy, sx
    e = 2 * dy - dx
    i = 0.0
    while i < dx:
        tiles.append((_u32(y1), _u32(x1)) if steep else (_u32(x1), _u32(y1)))
        while e >= 0:
            y1 += sy
            e -= 2 * dx
        x1 += sx
        e += 2 * dy
        i += 1
    tiles.append((_u32(x2), _u32(y2)))
    return tiles


class TileList:
    def __init__(self, max_zoom: int):
        self.max_zoom = max_zoom
        self.tiles: list[set[tuple[int, int]]] = [set() for _ in range(max_zoom + 1)]

    def expire(self, lon: float, lat: float) -> None:
        self._add_coord(lon, lat)

    def expire_nodes(self, nodes: list[tuple[float, float]], closed: bool) -> None:
        """nodes in WGS84; (0, 0) marks a missing node."""
        if not nodes:
            return
        box = _bbox(nodes)
        for zoom in range(self.max_zoom, 0, -1):
            n = _num_bbox_tiles(box, zoom)
            if closed:
                if n < 64:
                    self._expire_box(box, zoom)
                    return
            elif n < 500:
                self._expire_line(nodes, zoom)
                return

    def _add_coord(self, lon: float, lat: float) -> None:
        pad = 0.2
        tx, ty = tile_coord(lon, lat, self.max_zoom)
        if tx < 0:
            return
        for x in range(_u32(tx - pad), _u32(tx + pad) + 1):
            for y in range(_u32(ty - pad), _u32(ty + pad) + 1):
                self.tiles[self.max_zoom].add((x, y))

    def _expire_line(self, nodes, zoom: int) -> None:
        if len(nodes) == 1:
            self._add_coord(*nodes[0])
            return
        for i in range(len(nodes) - 1):
            (ax, ay), (bx, by) = nodes[i], nodes[i + 1]
            if (ax == 0 and ay == 0) or (bx == 0 and by == 0):
                continue
            x1, y1 = tile_coord(ax, ay, zoom)
            x2, y2 = tile_coord(bx, by, zoom)
            if x1 < 0 or x2 < 0:
                return
            if int(x1) == int(x2) and int(y1) == int(y2):
                self.tiles[zoom].add((_u32(x1), _u32(y1)))
            else:
                self.tiles[zoom].update(_bresenham(x1, y1, x2, y2))

    def _expire_box(self, b, zoom: int) -> None:
        x1, y1 = tile_coord(b[0], b[3], zoom)
        x2, y2 = tile_coord(b[2], b[1], zoom)
        if x1 < 0 or x2 < 0:
            return
        for x in range(_u32(x1), _u32(x2) + 1):
            for y in range(_u32(y1), _u32(y2) + 1):
                self.tiles[zoom].add((x, y))

    def as_strings(self) -> list[str]:
        return [f"{z}/{x}/{y}" for z, tiles in enumerate(self.tiles) for x, y in tiles]


def expire_projected_nodes(tl: TileList, nodes: list[tuple[float, float]], srid: int, closed: bool) -> None:
    if srid == 3857:
        nodes = [merc_to_wgs(x, y) if (x, y) != (0.0, 0.0) else (0.0, 0.0) for x, y in nodes]
    tl.expire_nodes(nodes, closed)


def expire_projected_node(tl: TileList, x: float, y: float, srid: int) -> None:
    if srid == 3857:
        x, y = merc_to_wgs(x, y)
    tl.expire(x, y)
