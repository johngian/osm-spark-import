# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Rows of nodes, ways and relations, and expired tiles.

A Builder gets a batch of work units: target elements and everything their
rows need (coordinates, member ways, member nodes and relations). It fills a
small cache the way the import does (tags filtered; nodes and relations only
with tags left) and builds the rows, including the cases where an element is
skipped or a relation's member rows stop at a failing member.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import shapely

from osm_spark_import.mapping import geom as g
from osm_spark_import.mapping.columns import ColumnSpec, Ctx, MemberCtx, column_spec
from osm_spark_import.mapping.config import Config
from osm_spark_import.mapping.expire import TileList
from osm_spark_import.mapping.match import Match, is_closed, matchers
from osm_spark_import.mapping.tags import node_filter, relation_filter, way_filter

REL_ID_OFFSET = -100000000000000000  # relation IDs with use_single_id_space
MEMBER_TYPES = {"n": 0, "w": 1, "r": 2}


@dataclass
class Row:
    """A row of a table."""

    table: str
    id: int  # the element's ID as written (for generalized tables)
    gen: bool  # whether diffs update the generalized tables for it
    values: list  # in column order; geometries as shapely objects


class Mapping:
    """The parsed mapping with everything the builders need."""

    def __init__(self, conf: Config, srid: int = 3857):
        self.conf = conf
        self.srid = srid
        self.m = matchers(conf)
        self.columns: dict[str, list[ColumnSpec]] = {
            name: [column_spec(c) for c in t.columns] for name, t in conf.tables.items()
        }
        self.node_filter = node_filter(conf)
        self.way_filter = way_filter(conf)
        self.relation_filter = relation_filter(conf)
        self.max_gap = 1e-6 if srid == 4326 else 1e-1

    KIND_TABLES = {
        "n": (("point", "geometry"), ("points",)),
        "w": (("linestring", "polygon", "geometry"), ("linestrings", "polygons")),
        "r": (("polygon", "relation", "relation_member", "geometry"), ("polygons",)),
    }

    def target_keys(self, kind: str) -> list[str] | None:
        """Tag keys an element of this kind needs to match any table (to
        pre-select targets on raw tags), or None for "any tagged element"
        (a mapping with __any__)."""
        types, kinds = self.KIND_TABLES[kind]
        keys = {k for t in self.conf.tables.values() if t.type in types for k, _, _ in t.key_values(kinds)}
        if "__any__" in keys:
            return None
        return sorted(keys)

    def manifest(self) -> dict:
        """Table specs for the batch manifest (all the loader needs)."""
        return {
            "srid": self.srid,
            "single_id_space": self.conf.single_id_space,
            "tables": {
                name: {
                    "type": t.type,
                    "columns": [
                        {"name": c.name, "type": c.type, "value_type": c.value_type, "pg": c.pg_type}
                        for c in self.columns[name]
                    ],
                }
                for name, t in self.conf.tables.items()
            },
            "generalized": {
                name: {"source": gt.source, "tolerance": gt.tolerance, "sql_filter": gt.sql_filter}
                for name, gt in self.conf.generalized_tables.items()
            },
        }

    def way_id(self, id_: int) -> int:
        """The ID written for a way."""
        return -id_ if self.conf.single_id_space else id_

    def rel_id(self, id_: int) -> int:
        """The ID written for a relation."""
        return REL_ID_OFFSET - id_ if self.conf.single_id_space else -id_


class Builder:
    """Builds the rows (and expired tiles) of batches of work units."""

    def __init__(self, mapping: Mapping, expire_zoom: int = 0, deleter_expire: bool = False):
        self.mp = mapping
        self.srid = mapping.srid
        self.rows: list[Row] = []
        self.tiles = TileList(expire_zoom) if expire_zoom > 0 else None
        self.deleter_expire = deleter_expire

    # --- cache -----------------------------------------------------------------

    def fill_cache(self, units: list[dict]) -> dict[str, list[int]]:
        """Load the units' elements, tags filtered, and project coordinates; returns target IDs per kind."""
        mp = self.mp
        coords: dict[int, tuple[int, int]] = {}  # 1e-7 degrees
        nodes: dict[int, dict] = {}  # tags (non-empty after filtering)
        ways: dict[int, tuple[dict, list[int]]] = {}
        rels: dict[int, tuple[dict, list]] = {}
        targets = {"n": [], "w": [], "r": []}

        def put_node(id_, tags, lon, lat):
            coords[id_] = (lon, lat)
            t = mp.node_filter.filter(dict(tags or ()))
            if t:
                nodes[id_] = t

        def put_way(id_, tags, refs):
            ways[id_] = (mp.way_filter.filter(dict(tags or ())), list(refs or []))

        def put_rel(id_, tags, members):
            t = mp.relation_filter.filter(dict(tags or ()))
            if t:  # relations without tags are not cached
                rels[id_] = (t, list(members or []))

        self.changed: dict[str, set[int]] = {"n": set(), "w": set(), "r": set()}
        for u in units:
            k = u["k"]
            targets[k].append(u["id"])
            if u.get("changed", True) is not False:
                self.changed[k].add(u["id"])
            for c in u.get("nodes") or []:
                coords[c["id"]] = (c["lon"], c["lat"])
            if k == "n":
                put_node(u["id"], u["tags"], u["lon"], u["lat"])
            elif k == "w":
                put_way(u["id"], u["tags"], u["refs"])
            else:
                for w in u.get("ways") or []:
                    put_way(w["id"], w["tags"], w["refs"])
                for n in u.get("mnodes") or []:
                    put_node(n["id"], n["tags"], n["lon"], n["lat"])
                for r in u.get("rels") or []:
                    put_rel(r["id"], r["tags"], r["members"])
                put_rel(u["id"], u["tags"], u["members"])
        self.nodes, self.ways, self.rels = nodes, ways, rels
        ids = list(coords)
        e7 = np.array([coords[i] for i in ids], dtype=np.float64).reshape(-1, 2)
        lons, lats = e7[:, 0] / 1e7, e7[:, 1] / 1e7
        xs, ys = g.project(lons, lats, self.srid)
        self.wgs = dict(zip(ids, zip(lons.tolist(), lats.tolist(), strict=True), strict=True))
        self.coords = dict(
            zip(ids, zip(np.asarray(xs).tolist(), np.asarray(ys).tolist(), strict=True), strict=True)
        )
        return targets

    def fill(self, refs: list[int]) -> list[tuple[float, float]] | None:
        """Projected coordinates of refs; None if one is missing."""
        out = []
        for r in refs:
            c = self.coords.get(r)
            if c is None:
                return None
            out.append(c)
        return out

    def wgs_coords(self, refs: list[int]) -> list[tuple[float, float]]:
        """WGS84 coordinates for expiring; missing nodes left out."""
        return [self.wgs[r] for r in refs if r in self.wgs]

    # --- rows ------------------------------------------------------------------

    def insert(self, matches: list[Match], tags: dict, elem_id: int, geom, gen: bool) -> None:
        """Add a row per match."""
        for m in matches:
            ctx = Ctx(tags, elem_id, geom, m.key, m.value, self.srid)
            self.rows.append(Row(m.table, elem_id, gen, [c.value(ctx) for c in self.mp.columns[m.table]]))

    def process(self, units: list[dict]) -> None:
        """Build the rows of the targets, or with deleter_expire only expire their tiles."""
        targets = self.fill_cache(units)
        if self.deleter_expire:
            if self.tiles is not None:
                self.expire_old(targets)
            return
        for id_ in targets["r"]:
            self.build_relation(id_)
        for id_ in targets["w"]:
            self.build_way(id_)
        for id_ in targets["n"]:
            self.build_node(id_)

    def build_node(self, id_: int) -> None:
        """Point rows of a node."""
        tags = self.nodes.get(id_)
        if tags is None:
            return
        matches = self.mp.m.point.match_node(tags)
        if not matches:
            return
        self.insert(matches, tags, id_, g.point(*self.coords[id_]), False)
        if self.tiles is not None:
            self.tiles.expire(*self.wgs[id_])

    def build_way(self, id_: int) -> None:
        """Linestring and polygon rows of a way."""
        w = self.ways.get(id_)
        if w is None:
            return
        tags, refs = w
        if not tags:
            return
        m = self.mp.m
        nodes = None
        inserted = inserted_polygon = False
        line_matches = m.linestring.match_way(tags, refs)
        if line_matches:
            nodes = self.fill(refs)
            if nodes is None:
                return
            try:
                geom = g.linestring(nodes)
            except g.GeomError:
                return
            self.insert(line_matches, tags, self.mp.way_id(id_), geom, True)
            inserted = True
        poly_matches = m.polygon.match_way(tags, refs)
        if poly_matches:
            if nodes is None:
                nodes = self.fill(refs)
            if nodes is None:
                return
            if is_closed(refs):
                try:
                    geom = g.make_valid(g.polygon(nodes))
                except g.GeomError:
                    return
                self.insert(poly_matches, tags, self.mp.way_id(id_), geom, True)
                inserted_polygon = True
        if (inserted or inserted_polygon) and self.tiles is not None:
            self.tiles.expire_nodes(self.wgs_coords(refs), inserted_polygon)

    def build_relation(self, id_: int) -> None:
        """Relation member, relation and multipolygon rows of a relation."""
        r = self.rels.get(id_)
        if r is None:
            return
        tags, members = r
        # a missing member way or coordinate skips the relation
        member_ways: dict[int, tuple[dict, list[int], list]] = {}  # tags, refs, coordinates
        for mem in members:
            if mem["type"] != "w":
                continue
            w = self.ways.get(mem["ref"])
            if w is None:
                return
        for mem in members:
            if mem["type"] != "w":
                continue
            wtags, wrefs = self.ways[mem["ref"]]
            nodes = self.fill(wrefs)
            if nodes is None:
                return
            member_ways[mem["ref"]] = (wtags, wrefs, nodes)

        rel_id = self.mp.rel_id(id_)
        inserted = False
        if self.build_relation_members(id_, rel_id, tags, members, member_ways):
            inserted = True
        rel_matches = self.mp.m.relation.match_relation(tags)
        if rel_matches:  # relation tables: no geometry
            self.insert(rel_matches, tags, rel_id, None, True)
            inserted = True
        poly_matches = self.mp.m.polygon.match_relation(tags)
        if poly_matches:
            try:
                geom = g.multipolygon(
                    [member_ways[m["ref"]][2] for m in members if m["type"] == "w"],
                    self.mp.max_gap,
                )
                self.insert(poly_matches, tags, rel_id, geom, True)
                inserted = True
            except g.GeomError:
                pass
        if inserted and self.tiles is not None:
            for mem in members:
                if mem["type"] == "w":
                    self.tiles.expire_nodes(self.wgs_coords(member_ways[mem["ref"]][1]), True)

    def build_relation_members(self, id_, rel_id, tags, members, member_ways) -> bool:
        """Rows of relation_member tables, one per member; False on failure (rows may be partial)."""
        matches = self.mp.m.relation_member.match_relation(tags)
        if not matches:
            return False
        elements: list[Ctx | None] = []
        geoms = []
        for mem in members:
            t, ref = mem["type"], mem["ref"]
            if t == "r":
                r = self.rels.get(ref)
                if r is None:
                    return False
                elements.append(Ctx(r[0], ref, None, "", ""))
                geoms.append(None)
            elif t == "n":
                c = self.coords.get(ref)
                if c is None:
                    return False
                ntags = self.nodes.get(ref, {})
                elements.append(Ctx(ntags, ref, None, "", ""))
                geoms.append(c)
            else:
                elements.append(Ctx(member_ways[ref][0], ref, None, "", ""))
                geoms.append(None)
        cols = self.mp.columns
        for mi, mem in enumerate(members):
            t = mem["type"]
            if t == "n":
                geom = g.point(*geoms[mi])
            elif t == "w":
                try:
                    geom = g.linestring(member_ways[mem["ref"]][2])
                except g.GeomError:
                    return False
            else:
                geom = g.EMPTY_POLYGON
            el = elements[mi]
            for m in matches:
                rel_ctx = Ctx(tags, rel_id, geom, m.key, m.value, self.srid)
                member = MemberCtx(
                    mem["ref"],
                    MEMBER_TYPES[t],
                    mem["role"] or "",
                    mi,
                    Ctx(el.tags, el.elem_id, geom, m.key, m.value, self.srid),
                )
                self.rows.append(
                    Row(m.table, rel_id, True, [c.member_value(rel_ctx, member) for c in cols[m.table]])
                )
        return True

    # --- expiring old versions ---------------------------------------------------

    def expire_old(self, targets) -> None:
        """Tiles of the old versions of a diff.

        The changed elements are visited, and the elements that depend on
        them if they could be built: a dependent way is visited if its
        coordinates are complete, a dependent relation if all its member ways
        and their coordinates are there (relations cut off by an extract are
        not). Changed elements are always visited. Visited elements are
        expired if they match a table.
        """
        m, tl = self.mp.m, self.tiles
        for id_ in targets["r"]:
            r = self.rels.get(id_)
            if r is None:
                continue
            tags, members = r
            polygon = bool(m.polygon.match_relation(tags))
            if not (polygon or m.relation.match_relation(tags) or m.relation_member.match_relation(tags)):
                continue
            if id_ not in self.changed["r"] and not self.relation_complete(members):
                continue
            # stop at the first missing way
            for mem in members:
                if mem["type"] != "w":
                    continue
                w = self.ways.get(mem["ref"])
                if w is None:
                    break
                tl.expire_nodes(self.wgs_coords(w[1]), polygon)
        for id_ in targets["w"]:
            w = self.ways.get(id_)
            if w is None or not w[0]:
                continue
            tags, refs = w
            polygon = bool(m.polygon.match_way(tags, refs))
            if not polygon and not m.linestring.match_way(tags, refs):
                continue
            if id_ not in self.changed["w"] and any(r not in self.coords for r in refs):
                continue
            tl.expire_nodes(self.wgs_coords(refs), polygon)
        for id_ in targets["n"]:
            tags = self.nodes.get(id_)
            if tags and m.point.match_node(tags):
                tl.expire(*self.wgs[id_])

    def relation_complete(self, members) -> bool:
        """Whether every member way and its coordinates are there."""
        for mem in members:
            if mem["type"] != "w":
                continue
            w = self.ways.get(mem["ref"])
            if w is None or any(r not in self.coords for r in w[1]):
                return False
        return True


def ewkb(geom, srid: int) -> bytes | None:
    """EWKB of a geometry, with its SRID."""
    if geom is None:
        return None
    return shapely.to_wkb(shapely.set_srid(geom, srid), include_srid=True)
