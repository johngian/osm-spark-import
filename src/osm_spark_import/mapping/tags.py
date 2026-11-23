# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Tag filters applied when elements are cached.

With tags.load_all, every tag is kept except those in tags.exclude (keys or
fnmatch globs). Otherwise only the tags the mapping can use are kept.
"""

from __future__ import annotations

import re
from fnmatch import translate

from osm_spark_import.mapping.config import TYPE_MAPPING_KINDS, Config


class ExcludeFilter:
    """Keeps every tag except those whose key matches an exclude entry (key or glob)."""

    def __init__(self, exclude: list[str]):
        self.pattern = re.compile("|".join(translate(k) for k in exclude)) if exclude else None

    def filter(self, tags: dict[str, str]) -> dict[str, str]:
        """The tags that are not excluded."""
        if self.pattern is None:
            return dict(tags)
        return {k: v for k, v in tags.items() if not self.pattern.match(k)}


class TagFilter:
    """Keeps the tags a mapping value matches, and the tags of the extra keys."""

    def __init__(self, mappings: dict[str, set[str]], extra: set[str]):
        self.mappings, self.extra = mappings, extra

    def filter(self, tags: dict[str, str]) -> dict[str, str]:
        """The tags that are kept."""
        out = {}
        for k, v in tags.items():
            values = self.mappings.get(k)
            if values is not None and ("__any__" in values or v in values):
                out[k] = v
            elif k in self.extra:
                out[k] = v
        return out


def add_mapping_tags(conf: Config, table_type: str, into: dict[str, set[str]]) -> None:
    """The mapping values of a table type, as key -> values."""
    kind = TYPE_MAPPING_KINDS.get(table_type)
    for t in conf.tables.values():
        if t.type != "geometry" and t.type != table_type:
            continue
        for k, v, _ in t.key_values([kind] if kind else []):
            into.setdefault(k, set()).add(v)


def add_extra_tags(conf: Config, table_type: str, tags: set[str]) -> None:
    """Keys used by columns and filters of a table type."""
    for t in conf.tables.values():
        if t.type != table_type and t.type != "geometry":
            continue
        for c in t.columns:
            if c.key:
                tags.add(c.key)
            tags.update(c.keys)
        if t.filters and t.filters.exclude_tags:
            for kv in t.filters.exclude_tags:
                tags.add(str(kv[0]))
        if table_type in ("polygon", "relation", "relation_member") and t.relation_types is not None:
            tags.add("type")
    tags.update(conf.include)
    tags.add("area")


def node_filter(conf: Config) -> ExcludeFilter | TagFilter:
    """The tag filter for nodes."""
    if conf.load_all:
        return ExcludeFilter(conf.exclude)
    m: dict[str, set[str]] = {}
    add_mapping_tags(conf, "point", m)
    extra: set[str] = set()
    add_extra_tags(conf, "point", extra)
    add_extra_tags(conf, "relation_member", extra)
    return TagFilter(m, extra)


def way_filter(conf: Config) -> ExcludeFilter | TagFilter:
    """The tag filter for ways."""
    if conf.load_all:
        return ExcludeFilter(conf.exclude)
    m: dict[str, set[str]] = {}
    add_mapping_tags(conf, "linestring", m)
    add_mapping_tags(conf, "polygon", m)
    extra: set[str] = set()
    for tt in ("linestring", "polygon", "relation_member"):
        add_extra_tags(conf, tt, extra)
    return TagFilter(m, extra)


def relation_filter(conf: Config) -> ExcludeFilter | TagFilter:
    """The tag filter for relations."""
    if conf.load_all:
        return ExcludeFilter(conf.exclude)
    # the type tag of common relations is kept
    m: dict[str, set[str]] = {"type": {"multipolygon", "boundary", "land_area"}}
    for tt in ("linestring", "polygon", "relation", "relation_member"):
        add_mapping_tags(conf, tt, m)
    extra: set[str] = set()
    for tt in ("linestring", "polygon", "relation", "relation_member"):
        add_extra_tags(conf, tt, extra)
    return TagFilter(m, extra)
