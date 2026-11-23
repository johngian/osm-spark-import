# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Which tables an element goes into.

A match is (key, value, table, sub-mapping): one per table and sub-mapping,
the value that comes first in the mapping file. Filters (require, reject,
regexp, exclude_tags), the areas settings (area_tags, linear_tags) and
relation types narrow the matches.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

from osm_spark_import.mapping.config import TYPE_MAPPING_KINDS, Config, KeyValues

Filter = Callable[[dict, str, bool], bool]  # (tags, matched key, closed) -> keep


@dataclass(frozen=True)
class Match:
    """The tag that matched a table (and sub-mapping)."""

    key: str
    value: str
    table: str
    sub: str = ""


def add_mapping(into: dict, kvs: KeyValues, table: str, sub: str) -> None:
    """Index kvs as key -> value -> [(table, sub, order)]."""
    for k, v, order in kvs:
        into.setdefault(k, {}).setdefault(v, []).append((table, sub, order))


def value_filter(keep_if: bool, key: str, values: list[str]) -> Filter:
    """Filter on key having one of values: require (keep_if=True) or reject (False)."""
    if "__any__" in values:
        return lambda tags, k, closed: keep_if if key in tags else not keep_if
    vals = set(values)
    return lambda tags, k, closed: keep_if if tags.get(key) in vals and key in tags else not keep_if


def regexp_filter(keep_if: bool, key: str, pattern: str) -> Filter:
    """Filter on the value of key matching pattern: require (keep_if=True) or reject (False)."""
    r = re.compile(pattern)  # unanchored search
    return lambda tags, k, closed: keep_if if key in tags and r.search(tags[key]) else not keep_if


class Matcher:
    """Matches the tags of one kind of geometry against the tables of a type."""

    def __init__(
        self,
        conf: Config,
        table_type: str,
        typed_filter_type: str | None,
        relation_filters: bool,
        match_areas: bool,
    ):
        self.match_areas = match_areas
        self.mappings: dict[str, dict[str, list]] = {}
        kind = TYPE_MAPPING_KINDS.get(table_type)
        for name, t in conf.tables.items():
            if t.type != "geometry" and t.type != table_type:
                continue
            add_mapping(self.mappings, t.mapping, name, "")
            for sub, kvs in t.mappings.items():
                add_mapping(self.mappings, kvs, name, sub)
            if kind:
                add_mapping(self.mappings, t.type_mappings[kind], name, "")
        self.filters: dict[str, list[Filter]] = {}
        self.add_filters(conf)
        if typed_filter_type:
            self.add_typed_filters(conf, typed_filter_type)
        self.rel_filters: dict[str, list[Filter]] = {}
        if relation_filters:
            self.add_relation_filters(conf)

    def add_filters(self, conf: Config) -> None:
        """The tables' filters: exclude_tags, require, reject and their regexp variants."""
        for name, t in conf.tables.items():
            f = t.filters
            if f is None:
                continue
            fl = self.filters.setdefault(name, [])
            for kv in f.exclude_tags or []:
                fl.append(value_filter(False, str(kv[0]), [str(kv[1])]))
            for key in dict.fromkeys(k for k, _, _ in f.require):
                fl.append(value_filter(True, key, [v for k, v, _ in f.require if k == key]))
            for key in dict.fromkeys(k for k, _, _ in f.reject):
                fl.append(value_filter(False, key, [v for k, v, _ in f.reject if k == key]))
            for key, pattern in f.require_regexp.items():
                fl.append(regexp_filter(True, key, pattern))
            for key, pattern in f.reject_regexp.items():
                fl.append(regexp_filter(False, key, pattern))

    def add_typed_filters(self, conf: Config, table_type: str) -> None:
        """area_tags / linear_tags filters for closed ways and relations."""
        area_tags = set(conf.area_tags) if conf.area_tags is not None else None
        linear_tags = set(conf.linear_tags) if conf.linear_tags is not None else None
        for name, t in conf.tables.items():
            if t.type != "geometry" and t.type != table_type:
                continue
            if t.type == "linestring" and area_tags is not None:

                def line(tags, key, closed, area_tags=area_tags):
                    if closed:
                        if tags.get("area") == "yes":
                            return False
                        if tags.get("area") != "no" and key in area_tags:
                            return False
                    return True

                self.filters.setdefault(name, []).append(line)
            if t.type == "polygon" and linear_tags is not None:

                def poly(tags, key, closed, linear_tags=linear_tags):
                    if closed and tags.get("area") == "no":
                        return False
                    if tags.get("area") != "yes" and key in linear_tags:
                        return False
                    return True

                self.filters.setdefault(name, []).append(poly)

    def add_relation_filters(self, conf: Config) -> None:
        """relation_types filters (polygon tables default to multipolygon, boundary, land_area)."""
        for name, t in conf.tables.items():
            if t.relation_types is not None:
                types = set(t.relation_types)
                self.rel_filters.setdefault(name, []).append(
                    lambda tags, key, closed, types=types: tags.get("type") in types and "type" in tags
                )
            elif t.type == "polygon":
                std = {"multipolygon", "boundary", "land_area"}
                self.rel_filters.setdefault(name, []).append(
                    lambda tags, key, closed, std=std: tags.get("type") in std and "type" in tags
                )

    def match(self, tags: dict[str, str], closed: bool, relation: bool) -> list[Match]:
        """The matches of tags, one per table and sub-mapping, filters applied."""
        best: dict[tuple[str, str], tuple[int, str, str]] = {}

        def add(k, v, tables):
            for name, sub, order in tables:
                other = best.get((name, sub))
                if other is None or other[0] >= order:
                    best[(name, sub)] = (order, k, v)

        anyv = self.mappings.get("__any__")
        if anyv is not None and "__any__" in anyv:
            add("__any__", "__any__", anyv["__any__"])
        for k, v in tags.items():
            values = self.mappings.get(k)
            if values is None:
                continue
            if "__any__" in values:
                add(k, v, values["__any__"])
            if v in values:
                add(k, v, values[v])

        out = []
        for (name, sub), (_, k, v) in best.items():
            if any(not f(tags, k, closed) for f in self.filters.get(name, ())):
                continue
            if relation and any(not f(tags, k, closed) for f in self.rel_filters.get(name, ())):
                continue
            out.append(Match(k, v, name, sub))
        return out

    def match_node(self, tags: dict) -> list[Match]:
        return self.match(tags, False, False)

    def match_way(self, tags: dict, refs: list[int]) -> list[Match]:
        """Matches of a way; polygon matchers only match closed ways."""
        closed = is_closed(refs)
        if self.match_areas:  # as polygon
            if closed:
                if tags.get("area") == "no":
                    return []
                return self.match(tags, True, False)
            return []
        if closed:  # as linestring
            if tags.get("area") == "yes":
                return []
            return self.match(tags, True, False)
        return self.match(tags, False, False)

    def match_relation(self, tags: dict) -> list[Match]:
        return self.match(tags, True, True)


def is_closed(refs: list[int]) -> bool:
    """A way is closed if it has at least 4 nodes and ends where it starts."""
    return len(refs) >= 4 and refs[0] == refs[-1]


@dataclass
class Matchers:
    """One Matcher per table type."""

    point: Matcher
    linestring: Matcher
    polygon: Matcher
    relation: Matcher
    relation_member: Matcher


def matchers(conf: Config) -> Matchers:
    """The matchers of a mapping."""
    return Matchers(
        point=Matcher(conf, "point", "point", False, False),
        linestring=Matcher(conf, "linestring", "linestring", False, False),
        polygon=Matcher(conf, "polygon", "polygon", True, True),
        # with the typed filters of polygon and relation tables
        relation=relation_matcher(conf),
        relation_member=Matcher(conf, "relation_member", "relation_member", True, True),
    )


def relation_matcher(conf: Config) -> Matcher:
    """Matcher for relation tables, with the typed filters of polygon and relation tables."""
    m = Matcher(conf, "relation", "polygon", True, True)
    m.add_typed_filters(conf, "relation")
    return m
