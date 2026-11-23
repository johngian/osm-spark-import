# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Expired tiles, with mercantile and shapely.

Points expire their tile and neighbours within 0.2 tiles at the maximum zoom.
Lines and polygons expire at the highest zoom where their bounding box is
small enough: polygons every tile of the box, lines the tiles the line
crosses. Tiles are "z/x/y" strings; the loader writes them to files
(DIR/YYYYMMDD/HHMMSS.mmm.tiles).
"""

from __future__ import annotations

import mercantile
import numpy as np
import shapely

PADDING = 0.2  # tiles, around points
MAX_BOX_TILES = 64
MAX_LINE_TILES = 500
MAX_LAT = 85.0511287798066


def in_range(lon: float, lat: float) -> bool:
    """Whether a point is within the Web Mercator range."""
    return -180 <= lon <= 180 and -MAX_LAT <= lat <= MAX_LAT


def box_count(w: float, s: float, e: float, n: float, zoom: int) -> int:
    """Number of tiles covering a bounding box at zoom."""
    nw, se = mercantile.tile(w, n, zoom), mercantile.tile(e, s, zoom)
    return (se.x - nw.x + 1) * (se.y - nw.y + 1)


class TileList:
    """A set of expired tiles."""

    def __init__(self, max_zoom: int):
        self.max_zoom = max_zoom
        self.tiles: set[tuple[int, int, int]] = set()

    def expire(self, lon: float, lat: float) -> None:
        """Expire the tiles around a point at the maximum zoom."""
        if not in_range(lon, lat):
            return
        z = self.max_zoom
        x, y = mercantile.xy(lon, lat)
        pad = PADDING * mercantile.CE / 2**z
        w, s = mercantile.lnglat(x - pad, y - pad)
        e, n = mercantile.lnglat(x + pad, y + pad)
        self.tiles.update((t.x, t.y, t.z) for t in mercantile.tiles(w, s, e, n, [z]))

    def expire_nodes(self, nodes: list[tuple[float, float]], closed: bool) -> None:
        """nodes in WGS84; nothing if one is beyond the Web Mercator range."""
        if not nodes or not all(in_range(*p) for p in nodes):
            return
        lons, lats = zip(*nodes, strict=True)
        w, s, e, n = min(lons), min(lats), max(lons), max(lats)
        for zoom in range(self.max_zoom, 0, -1):
            count = box_count(w, s, e, n, zoom)
            if closed and count < MAX_BOX_TILES:
                self.tiles.update((t.x, t.y, t.z) for t in mercantile.tiles(w, s, e, n, [zoom]))
                return
            if not closed and count < MAX_LINE_TILES:
                self.expire_line(nodes, (w, s, e, n), zoom)
                return

    def expire_line(self, nodes, bbox, zoom: int) -> None:
        """Expire the tiles at zoom that the line crosses."""
        if len(nodes) == 1:
            self.expire(*nodes[0])
            return
        candidates = list(mercantile.tiles(*bbox, [zoom]))
        if not candidates:  # a flat box on a tile edge: the tiles of its nodes
            self.tiles.update(
                (t.x, t.y, t.z) for t in (mercantile.tile(lon, lat, zoom) for lon, lat in nodes)
            )
            return
        bounds = np.array([mercantile.bounds(t) for t in candidates])
        hit = shapely.intersects(shapely.box(*bounds.T), shapely.LineString(nodes))
        self.tiles.update((t.x, t.y, t.z) for t, h in zip(candidates, hit, strict=True) if h)

    def as_strings(self) -> list[str]:
        """The tiles as z/x/y strings."""
        return [f"{z}/{x}/{y}" for x, y, z in self.tiles]
