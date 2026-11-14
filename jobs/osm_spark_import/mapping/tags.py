# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""Tag filters applied when elements are cached.

With tags.load_all, every tag is kept except those in tags.exclude (keys or
fnmatch globs). Otherwise only the tags the mapping can use are kept.
"""

from __future__ import annotations

from fnmatch import fnmatchcase

from .config import Config


class ExcludeFilter:
    def __init__(self, exclude: list[str]):
        self.keys = {k for k in exclude if not any(c in k for c in "?*[")}
        self.patterns = [k for k in exclude if any(c in k for c in "?*[")]
        self._cache: dict[str, bool] = {}

    def _excluded(self, k: str) -> bool:
        r = self._cache.get(k)
        if r is None:
            r = k in self.keys or any(fnmatchcase(k, p) for p in self.patterns)
            if len(self._cache) < 100_000:
                self._cache[k] = r
        return r

    def filter(self, tags: dict[str, str]) -> dict[str, str]:
        return {k: v for k, v in tags.items() if not self._excluded(k)}


class TagFilter:
    def __init__(self, mappings: dict[str, set[str]], extra: set[str]):
        self.mappings, self.extra = mappings, extra

    def filter(self, tags: dict[str, str]) -> dict[str, str]:
        out = {}
        for k, v in tags.items():
            values = self.mappings.get(k)
            if values is not None and ("__any__" in values or v in values):
                out[k] = v
            elif k in self.extra:
                out[k] = v
        return out


def _mappings(conf: Config, table_type: str, into: dict[str, set[str]]) -> None:
    """The mapping values of a table type, as key -> values."""
    kind = {"point": "points", "linestring": "linestrings", "polygon": "polygons"}.get(table_type)
    for t in conf.tables.values():
        if t.type != "geometry" and t.type != table_type:
            continue
        kvs = list(t.mapping)
        for sub in t.mappings.values():
            kvs += sub
        if kind:
            kvs += t.type_mappings[kind]
        for k, v, _ in kvs:
            into.setdefault(k, set()).add(v)


def _extra_tags(conf: Config, table_type: str, tags: set[str]) -> None:
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


def node_filter(conf: Config):
    if conf.load_all:
        return ExcludeFilter(conf.exclude)
    m: dict[str, set[str]] = {}
    _mappings(conf, "point", m)
    extra: set[str] = set()
    _extra_tags(conf, "point", extra)
    _extra_tags(conf, "relation_member", extra)
    return TagFilter(m, extra)


def way_filter(conf: Config):
    if conf.load_all:
        return ExcludeFilter(conf.exclude)
    m: dict[str, set[str]] = {}
    _mappings(conf, "linestring", m)
    _mappings(conf, "polygon", m)
    extra: set[str] = set()
    for tt in ("linestring", "polygon", "relation_member"):
        _extra_tags(conf, tt, extra)
    return TagFilter(m, extra)


def relation_filter(conf: Config):
    if conf.load_all:
        return ExcludeFilter(conf.exclude)
    # the type tag of common relations is kept
    m: dict[str, set[str]] = {"type": {"multipolygon", "boundary", "land_area"}}
    for tt in ("linestring", "polygon", "relation", "relation_member"):
        _mappings(conf, tt, m)
    extra: set[str] = set()
    for tt in ("linestring", "polygon", "relation", "relation_member"):
        _extra_tags(conf, tt, extra)
    return TagFilter(m, extra)
