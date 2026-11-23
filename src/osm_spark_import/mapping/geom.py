# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Coordinates and geometries, with pyproj and shapely.

Coordinates are pyosmium's fixed-point values (1e-7 degrees), projected with
pyproj. Lines and polygons drop repeated points; polygons are normalized and
repaired with buffer(0) if invalid; multipolygons are the member ways merged
into rings (after snapping gaps) and combined with the even-odd rule. Errors make
the writer skip the element.
"""

from __future__ import annotations

from functools import reduce

import numpy as np
import shapely
import shapely.errors
from pyproj import Transformer

TOLERANCE = 1e-9  # repeated points
TO_MERC = Transformer.from_crs(4326, 3857, always_xy=True)
TO_WGS = Transformer.from_crs(3857, 4326, always_xy=True)


class GeomError(Exception):
    """A geometry that cannot be built."""


# --- coordinates ----------------------------------------------------------------


def project(lons: np.ndarray, lats: np.ndarray, srid: int) -> tuple[np.ndarray, np.ndarray]:
    """WGS84 degrees to srid (3857 or 4326)."""
    if srid == 4326:
        return lons, lats
    return TO_MERC.transform(lons, lats)


def to_wgs84(geom, srid: int):
    """A geometry in srid as WGS84 degrees."""
    if srid == 4326:
        return geom
    return shapely.transform(geom, lambda c: np.column_stack(TO_WGS.transform(c[:, 0], c[:, 1])))


# --- geometries -------------------------------------------------------------------


def point(x: float, y: float):
    """A point."""
    return shapely.Point(x, y)


def linestring(pts):
    """A line without repeated points; GeomError if it has no length."""
    if len(pts) < 2:
        raise GeomError("need at least two nodes")
    g = shapely.remove_repeated_points(shapely.LineString(pts), TOLERANCE)
    if shapely.length(g) == 0:
        raise GeomError("need at least two separate nodes")
    return g


def polygon(pts):
    """A normalized polygon from a closed ring; GeomError if it is not one."""
    if len(pts) < 4 or pts[0] != pts[-1]:
        raise GeomError("not a closed ring")
    try:
        g = shapely.remove_repeated_points(shapely.Polygon(pts), TOLERANCE)
    except (ValueError, shapely.errors.GEOSException) as e:
        raise GeomError(str(e)) from e
    if shapely.get_num_coordinates(g) < 4:
        raise GeomError("not a closed ring")
    return shapely.normalize(g)


EMPTY_POLYGON = shapely.from_wkt("POLYGON EMPTY")


def make_valid(g):
    """Repair invalid polygons with buffer(0) (50 quadrant segments)."""
    try:
        if shapely.is_valid(g):
            return g
        return shapely.buffer(g, 0, quad_segs=50)
    except shapely.errors.GEOSException as e:
        raise GeomError(str(e)) from e


def multipolygon(member_ways: list[list[tuple[float, float]]], max_gap: float):
    """The area formed by the member ways: gaps below max_gap closed, ways
    merged into rings, rings combined with the even-odd rule (an inner ring
    touching the outer ring leaves a notch)."""
    lines = [shapely.LineString(pts) for pts in member_ways if len(pts) >= 2]
    if not lines:
        raise GeomError("no member ways")
    try:
        work = shapely.MultiLineString(lines)
        if max_gap > 0:
            work = shapely.snap(work, work, max_gap)
        merged = shapely.get_parts(shapely.line_merge(work))
        rings = [r for r in merged if shapely.is_closed(r) and shapely.get_num_coordinates(r) >= 4]
        if not rings:
            raise GeomError("member ways do not form a ring")
        area = reduce(shapely.symmetric_difference, (make_valid(shapely.Polygon(r)) for r in rings))
    except shapely.errors.GEOSException as e:
        raise GeomError(str(e)) from e
    if shapely.is_empty(area):
        raise GeomError("empty area")
    return shapely.normalize(make_valid(area))
