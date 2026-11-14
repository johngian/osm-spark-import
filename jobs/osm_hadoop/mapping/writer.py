"""Rows of nodes, ways and relations, and expired tiles.

A Builder gets a batch of work units: target elements and everything their
rows need (coordinates, member ways, member nodes and relations). It fills a
small cache the way the import does (tags filtered; nodes and relations only
with tags left) and builds the rows, including the cases where an element is
skipped or a relation's member rows stop at a failing member.
"""

from __future__ import annotations

from dataclasses import dataclass

import shapely

from . import geom as g
from .columns import ColumnSpec, Ctx, MemberCtx, column_spec
from .config import Config
from .expire import TileList, expire_projected_node, expire_projected_nodes
from .match import Match, is_closed, matchers
from .tags import node_filter, relation_filter, way_filter

REL_ID_OFFSET = -100000000000000000  # element.RelIDOffset
MEMBER_TYPES = {"n": 0, "w": 1, "r": 2}


@dataclass
class Row:
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

    _KIND_TABLES = {
        "n": (("point", "geometry"), ("points",)),
        "w": (("linestring", "polygon", "geometry"), ("linestrings", "polygons")),
        "r": (("polygon", "relation", "relation_member", "geometry"), ("polygons",)),
    }

    def target_keys(self, kind: str) -> list[str] | None:
        """Tag keys an element of this kind needs to match any table (to
        pre-select targets on raw tags), or None for "any tagged element"
        (a mapping with __any__)."""
        types, kinds = self._KIND_TABLES[kind]
        keys: set[str] = set()
        for t in self.conf.tables.values():
            if t.type not in types:
                continue
            kvs = list(t.mapping)
            for sub in t.mappings.values():
                kvs += sub
            for k in kinds:
                kvs += t.type_mappings[k]
            keys.update(k for k, _, _ in kvs)
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
        return -id_ if self.conf.single_id_space else id_

    def rel_id(self, id_: int) -> int:
        return REL_ID_OFFSET - id_ if self.conf.single_id_space else -id_


def _tags(raw) -> dict[str, str]:
    if not raw:
        return {}
    return dict(raw)


class Builder:
    def __init__(self, mapping: Mapping, expire_zoom: int = 0, deleter_expire: bool = False):
        self.mp = mapping
        self.srid = mapping.srid
        self.rows: list[Row] = []
        self.tiles = TileList(expire_zoom) if expire_zoom > 0 else None
        self.deleter_expire = deleter_expire

    # --- cache -----------------------------------------------------------------

    def _fill_cache(self, units: list[dict]):
        mp = self.mp
        coords: dict[int, tuple[int, int]] = {}
        nodes: dict[int, dict] = {}  # tags (non-empty after filtering)
        ways: dict[int, tuple[dict, list[int]]] = {}
        rels: dict[int, tuple[dict, list]] = {}
        targets = {"n": [], "w": [], "r": []}

        def put_node(id_, tags, qlon, qlat):
            coords[id_] = (qlon, qlat)
            t = mp.node_filter.filter(_tags(tags))
            if t:
                nodes[id_] = t

        def put_way(id_, tags, refs):
            ways[id_] = (mp.way_filter.filter(_tags(tags)), list(refs or []))

        def put_rel(id_, tags, members):
            t = mp.relation_filter.filter(_tags(tags))
            if t:  # PutRelations skips relations without tags
                rels[id_] = (t, list(members or []))

        self.changed: dict[str, set[int]] = {"n": set(), "w": set(), "r": set()}
        for u in units:
            k = u["k"]
            targets[k].append(u["id"])
            if u.get("changed", True) is not False:
                self.changed[k].add(u["id"])
            for c in u.get("nodes") or []:
                coords[c["id"]] = (c["qlon"], c["qlat"])
            if k == "n":
                put_node(u["id"], u["tags"], u["qlon"], u["qlat"])
            elif k == "w":
                put_way(u["id"], u["tags"], u["refs"])
            else:
                for w in u.get("ways") or []:
                    put_way(w["id"], w["tags"], w["refs"])
                for n in u.get("mnodes") or []:
                    put_node(n["id"], n["tags"], n["qlon"], n["qlat"])
                for r in u.get("rels") or []:
                    put_rel(r["id"], r["tags"], r["members"])
                put_rel(u["id"], u["tags"], u["members"])
        self.coords, self.nodes, self.ways, self.rels = coords, nodes, ways, rels
        return targets

    def _fill(self, refs: list[int]) -> list[tuple[float, float]] | None:
        """FillWay + NodesToSrid; None if a coordinate is missing."""
        out = []
        for r in refs:
            c = self.coords.get(r)
            if c is None:
                return None
            out.append(g.to_srid(c[0], c[1], self.srid))
        return out

    def _fill_wgs(self, refs: list[int]) -> list[tuple[float, float]]:
        """FillWay in WGS84 for the deleter: stops at the first missing node,
        the rest stays (0, 0)."""
        out = []
        for r in refs:
            c = self.coords.get(r)
            if c is None:
                break
            out.append((g.int_to_coord(c[0]), g.int_to_coord(c[1])))
        return out + [(0.0, 0.0)] * (len(refs) - len(out))

    # --- rows ------------------------------------------------------------------

    def _insert(self, matches: list[Match], tags: dict, elem_id: int, geom, gen: bool) -> None:
        for m in matches:
            ctx = Ctx(tags, elem_id, geom, m.key, m.value)
            self.rows.append(Row(m.table, elem_id, gen, [c.value(ctx) for c in self.mp.columns[m.table]]))

    def process(self, units: list[dict]) -> None:
        targets = self._fill_cache(units)
        if self.deleter_expire:
            if self.tiles is not None:
                self._deleter_expire(targets)
            return
        for id_ in targets["r"]:
            self._relation(id_)
        for id_ in targets["w"]:
            self._way(id_)
        for id_ in targets["n"]:
            self._node(id_)

    def _node(self, id_: int) -> None:
        tags = self.nodes.get(id_)
        if tags is None:
            return
        matches = self.mp.m.point.match_node(tags)
        if not matches:
            return
        x, y = g.to_srid(*self.coords[id_], self.srid)
        self._insert(matches, tags, id_, g.point(x, y), False)
        if self.tiles is not None:
            expire_projected_node(self.tiles, x, y, self.srid)

    def _way(self, id_: int) -> None:
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
            nodes = self._fill(refs)
            if nodes is None:
                return
            try:
                geom = g.linestring(nodes)
            except g.GeomError:
                return
            self._insert(line_matches, tags, self.mp.way_id(id_), geom, True)
            inserted = True
        poly_matches = m.polygon.match_way(tags, refs)
        if poly_matches:
            if nodes is None:
                nodes = self._fill(refs)
            if nodes is None:
                return
            if is_closed(refs):
                try:
                    geom = g.make_valid(g.polygon(nodes))
                except g.GeomError:
                    return
                self._insert(poly_matches, tags, self.mp.way_id(id_), geom, True)
                inserted_polygon = True
        if (inserted or inserted_polygon) and self.tiles is not None:
            expire_projected_nodes(self.tiles, nodes, self.srid, inserted_polygon)

    def _relation(self, id_: int) -> None:
        r = self.rels.get(id_)
        if r is None:
            return
        tags, members = r
        # FillMembers, then FillWay for every way member: a missing way or
        # coordinate skips the relation
        member_ways: dict[int, tuple[dict, list[int], list]] = {}
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
            nodes = self._fill(wrefs)
            if nodes is None:
                return
            member_ways[mem["ref"]] = (wtags, wrefs, nodes)

        rel_id = self.mp.rel_id(id_)
        inserted = False
        if self._relation_members(id_, rel_id, tags, members, member_ways):
            inserted = True
        rel_matches = self.mp.m.relation.match_relation(tags)
        if rel_matches:  # relation tables: no geometry
            self._insert(rel_matches, tags, rel_id, None, True)
            inserted = True
        poly_matches = self.mp.m.polygon.match_relation(tags)
        if poly_matches:
            try:
                geom = g.multipolygon(
                    [
                        (m["ref"], member_ways[m["ref"]][1], member_ways[m["ref"]][2])
                        for m in members
                        if m["type"] == "w"
                    ],
                    self.mp.max_gap,
                )
                self._insert(poly_matches, tags, rel_id, geom, True)
                inserted = True
            except g.GeomError:
                pass
        if inserted and self.tiles is not None:
            for mem in members:
                if mem["type"] == "w":
                    expire_projected_nodes(self.tiles, member_ways[mem["ref"]][2], self.srid, True)

    def _relation_members(self, id_, rel_id, tags, members, member_ways) -> bool:
        """handleRelationMembers: True if succeeded (rows may be partial on failure)."""
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
                geoms.append(g.to_srid(c[0], c[1], self.srid))
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
                rel_ctx = Ctx(tags, rel_id, geom, m.key, m.value)
                member = MemberCtx(
                    mem["ref"],
                    MEMBER_TYPES[t],
                    mem["role"] or "",
                    mi,
                    Ctx(el.tags, el.elem_id, geom, m.key, m.value),
                )
                self.rows.append(
                    Row(m.table, rel_id, True, [c.member_value(rel_ctx, member) for c in cols[m.table]])
                )
        return True

    # --- expiring old versions ---------------------------------------------------

    def _deleter_expire(self, targets) -> None:
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
            if id_ not in self.changed["r"] and not self._relation_complete(members):
                continue
            # FillMembers stops at the first missing way
            for mem in members:
                if mem["type"] != "w":
                    continue
                w = self.ways.get(mem["ref"])
                if w is None:
                    break
                tl.expire_nodes(self._fill_wgs(w[1]), polygon)
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
            tl.expire_nodes(self._fill_wgs(refs), polygon)
        for id_ in targets["n"]:
            tags = self.nodes.get(id_)
            if tags and m.point.match_node(tags):
                c = self.coords[id_]
                tl.expire(g.int_to_coord(c[0]), g.int_to_coord(c[1]))

    def _relation_complete(self, members) -> bool:
        for mem in members:
            if mem["type"] != "w":
                continue
            w = self.ways.get(mem["ref"])
            if w is None or any(r not in self.coords for r in w[1]):
                return False
        return True


def ewkb(geom, srid: int) -> bytes | None:
    if geom is None:
        return None
    return shapely.to_wkb(shapely.set_srid(geom, srid), include_srid=True)
