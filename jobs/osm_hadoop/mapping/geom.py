"""Coordinates and geometries.

Coordinates: the cache keeps coordinates as uint32 values (truncated from
the parsed float), and every geometry is built from these values. The float
depends on the source: 1e-9 * nanodegrees for PBF, the decimal text for OSC.
The cache tables keep the uint32 values ("q" coordinates).

Geometries: lines and polygons after removing consecutive duplicate nodes;
every polygon is normalized; invalid polygons are repaired with buffer(0);
multipolygons are assembled from member ways (ring merging, gap closing,
containment). Errors make the writer skip the element.
"""

from __future__ import annotations

import math

import shapely
import shapely.errors

COORD_FACTOR = 11930464.7083  # ((2<<31)-1)/360.0
POLE = 6378137 * math.pi  # 20037508.342789244


class GeomError(Exception):
    pass


# --- coordinates ---------------------------------------------------------------


def coord_to_int(f: float) -> int:
    return int((f + 180.0) * COORD_FACTOR)


def int_to_coord(i: int) -> float:
    return i / COORD_FACTOR - 180.0


def pbf_float(e7: int) -> float:
    """The float of a PBF coordinate: 1e-9 * (offset + granularity * v)
    (granularity 100, offset 0: nanodegrees = e7 * 100)."""
    return 0.000000001 * float(e7 * 100)


def osc_float(e7: int) -> float:
    """The float of an OSC (XML) coordinate: the decimal text, parsed."""
    sign = "-" if e7 < 0 else ""
    a = abs(e7)
    return float(f"{sign}{a // 10_000_000}.{a % 10_000_000:07d}")


def q_pbf(e7: int) -> int:
    return coord_to_int(pbf_float(e7))


def q_osc(e7: int) -> int:
    return coord_to_int(osc_float(e7))


def log_or_nan(v: float) -> float:
    """log without exceptions: -Inf for 0, NaN for negative values."""
    if v > 0:
        return math.log(v)
    return -math.inf if v == 0 else math.nan


def wgs_to_merc(lon: float, lat: float) -> tuple[float, float]:
    return lon * POLE / 180.0, log_or_nan(math.tan((90.0 + lat) * math.pi / 360.0)) / math.pi * POLE


def merc_to_wgs(x: float, y: float) -> tuple[float, float]:
    return 180.0 * x / POLE, 180.0 / math.pi * (2 * math.atan(math.exp((y / POLE) * math.pi)) - math.pi / 2)


def to_srid(qlon: int, qlat: int, srid: int) -> tuple[float, float]:
    lon, lat = int_to_coord(qlon), int_to_coord(qlat)
    return wgs_to_merc(lon, lat) if srid == 3857 else (lon, lat)


# --- simple geometries ---------------------------------------------------------


def unduplicate(pts: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if len(pts) < 2:
        return pts
    out = [pts[0]]
    for i in range(1, len(pts)):
        a, b = pts[i - 1], pts[i]
        if abs(a[0] - b[0]) < 1e-9 and abs(a[1] - b[1]) < 1e-9:
            continue
        out.append(b)
    return out


def point(x: float, y: float):
    return shapely.Point(x, y)


def linestring(pts):
    pts = unduplicate(pts)
    if len(pts) < 2:
        raise GeomError("need at least two separate nodes for way")
    return shapely.LineString(pts)


def polygon(pts):
    pts = unduplicate(pts)
    if len(pts) < 4:
        raise GeomError("linestrings do not form ring")
    if pts[0] != pts[-1]:
        # GEOS refuses unclosed linear rings (shapely would close them)
        raise GeomError("ring not closed")
    return new_polygon(pts)


def new_polygon(shell, holes=None):
    """Every polygon is normalized (GEOSNormalize: shell clockwise from its
    lowest point, holes ordered)."""
    return shapely.normalize(shapely.Polygon(shell, holes))


def make_valid(g):
    """Repair invalid polygons with buffer(0) (50 quadrant segments)."""
    try:
        if shapely.is_valid(g):
            return g
        return shapely.buffer(g, 0, quad_segs=50)
    except shapely.errors.GEOSException as e:  # the element is skipped
        raise GeomError(str(e)) from e


def _contains(a, b) -> bool:
    """GEOSPreparedContains_r(...) == 1: exceptions (2) count as false."""
    try:
        return bool(shapely.contains(a, b))
    except shapely.errors.GEOSException:
        return False


EMPTY_POLYGON = shapely.from_wkt("POLYGON EMPTY")


# --- multipolygons ----------------------------------------------------------------


class Ring:
    __slots__ = ("ways", "refs", "nodes", "geom", "holes", "contained_by", "area", "outer")

    def __init__(self, way_id: int, refs: list[int], nodes: list[tuple[float, float]]):
        self.ways = [way_id]
        self.refs = list(refs)
        self.nodes = list(nodes)
        self.geom = None
        self.holes: dict[Ring, bool] = {}
        self.contained_by = -1
        self.area = 0.0
        self.outer = False

    def is_closed(self) -> bool:
        return len(self.refs) >= 4 and self.refs[0] == self.refs[-1]

    def try_close(self, max_gap: float) -> bool:
        if len(self.refs) < 4:
            return False
        (sx, sy), (ex, ey) = self.nodes[0], self.nodes[-1]
        if math.hypot(sy - ey, sx - ex) < max_gap:
            self.refs[-1] = self.refs[0]
            self.nodes[-1] = self.nodes[0]
            return True
        return False


def merge_rings(rings: list[Ring]) -> list[Ring]:
    endpoints: dict[int, Ring] = {}
    for ring in rings:
        if len(ring.refs) < 2:
            continue
        left, right = ring.refs[0], ring.refs[-1]
        orig = endpoints.get(left)
        if orig is not None:
            del endpoints[left]
            if left == orig.refs[-1]:
                orig.refs += ring.refs[1:]
                orig.nodes += ring.nodes[1:]
            else:
                orig.refs.reverse()
                orig.refs += ring.refs[1:]
                orig.nodes.reverse()
                orig.nodes += ring.nodes[1:]
            orig.ways += ring.ways
            right_ring = endpoints.get(right)
            if right_ring is not None and right_ring is not orig:
                del endpoints[right]
                if right == right_ring.refs[0]:
                    orig.refs += right_ring.refs[1:]
                    orig.nodes += right_ring.nodes[1:]
                else:
                    right_ring.refs.reverse()
                    orig.refs = orig.refs[:-1] + right_ring.refs
                    right_ring.nodes.reverse()
                    orig.nodes = orig.nodes[:-1] + right_ring.nodes
                orig.ways += right_ring.ways
                endpoints[orig.refs[-1]] = orig
            else:
                endpoints[right] = orig
        elif (orig := endpoints.get(right)) is not None:
            del endpoints[right]
            if right == orig.refs[0]:
                orig.refs = ring.refs[:-1] + orig.refs
                orig.nodes = ring.nodes[:-1] + orig.nodes
            else:
                ring.refs.reverse()
                orig.refs = orig.refs[:-1] + ring.refs
                ring.nodes.reverse()
                orig.nodes = orig.nodes[:-1] + ring.nodes
            orig.ways += ring.ways
            endpoints[left] = orig
        else:
            endpoints[left] = ring
            endpoints[right] = ring
    return list(dict.fromkeys(endpoints.values()))


def build_rings(
    member_ways: list[tuple[int, list[int], list[tuple[float, float]]]], max_gap: float
) -> list[Ring]:
    """member_ways: (way id, refs, projected nodes) for every way member, in order."""
    rings = [Ring(wid, refs, nodes) for wid, refs, nodes in member_ways]
    complete, incomplete = [], []
    for r in rings:
        if r.is_closed():
            r.geom = polygon(r.nodes)  # errors fail the relation
            complete.append(r)
        else:
            incomplete.append(r)
    for r in merge_rings(incomplete):
        if not r.is_closed() and not r.try_close(max_gap):
            continue
        r.geom = polygon(r.nodes)
        complete.append(r)
    if not complete:
        raise GeomError("linestrings do not form ring")
    for r in complete:
        r.area = float(shapely.area(r.geom))
    complete.sort(key=lambda r: r.area, reverse=True)
    return complete


def _ring_is_hole(rings: list[Ring], idx: int) -> bool:
    n = 0
    while True:
        idx = rings[idx].contained_by
        if idx == -1:
            break
        n += 1
    return n % 2 == 1


def build_rel_geometry(rings: list[Ring]):
    """rings sorted by area, large to small."""
    total = len(rings)
    shells: dict[Ring, bool] = {rings[0]: True}
    for i in range(total):
        shapely.prepare(rings[i].geom)
        for j in range(i + 1, total):
            if _contains(rings[i].geom, rings[j].geom):
                if rings[j].contained_by != -1:
                    rings[rings[j].contained_by].holes.pop(rings[j], None)
                    shells.pop(rings[j], None)
                rings[j].contained_by = i
                if _ring_is_hole(rings, j):
                    rings[i].holes[rings[j]] = True
                    rings[i].outer = False
                else:
                    shells[rings[j]] = True
                    rings[i].outer = True
        if rings[i].contained_by == -1:
            shells[rings[i]] = True
            rings[i].outer = True
        shapely.destroy_prepared(rings[i].geom)

    polygons = []
    for shell in shells:
        interiors = [shapely.get_exterior_ring(h.geom) for h in shell.holes]
        polygons.append(new_polygon(shapely.get_exterior_ring(shell.geom), interiors))
    # MultiPolygon is a plain collection (not normalized)
    result = polygons[0] if len(polygons) == 1 else shapely.MultiPolygon(polygons)
    return make_valid(result)


def multipolygon(member_ways, max_gap: float):
    return build_rel_geometry(build_rings(member_ways, max_gap))
