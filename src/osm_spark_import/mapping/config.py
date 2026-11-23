# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""The mapping file (YAML or JSON).

Mapping values keep their order: when an element matches a table more than
once, the first value in the file wins. Keys repeated inside one mapping
are all kept.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import yaml


class MappingError(ValueError):
    """An invalid mapping file."""


# KeyValues: [(key, value, order)], order counted over all values of the map
KeyValues = list[tuple[str, str, int]]

TABLE_TYPES = ("point", "linestring", "polygon", "geometry", "relation", "relation_member")
# table type -> its type_mappings key (geometry tables)
TYPE_MAPPING_KINDS = {"point": "points", "linestring": "linestrings", "polygon": "polygons"}


class PairsMap(dict):
    """A YAML mapping that also keeps all its (key, value) pairs, duplicates
    included: mapping values (KeyValues) keep every duplicate key, other maps
    keep the last value of a duplicate key, as a dict does."""

    pairs: list


class YamlLoader(yaml.SafeLoader):
    """SafeLoader building PairsMap mappings."""


def construct_map(loader: YamlLoader, node: yaml.MappingNode) -> PairsMap:
    """Construct a YAML mapping as a PairsMap."""
    loader.flatten_mapping(node)
    pairs = [
        (loader.construct_object(k, deep=True), loader.construct_object(v, deep=True)) for k, v in node.value
    ]
    m = PairsMap(pairs)
    m.pairs = pairs
    return m


YamlLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_map)


def parse_key_values(raw: Any, where: str) -> KeyValues:
    """A mapping {key: [values]} as KeyValues, duplicate keys included."""
    if raw is None:
        return []
    if not isinstance(raw, dict):
        raise MappingError(f"{where}: not a mapping")
    out: KeyValues = []
    order = 0
    for k, values in getattr(raw, "pairs", raw.items()):
        if not isinstance(k, str):
            raise MappingError(f"{where}: mapping key '{k}' not a string")
        if not isinstance(values, list):
            raise MappingError(f"{where}: values of mapping key '{k}' not a list")
        for v in values:
            if not isinstance(v, str):
                raise MappingError(f"{where}: mapping value '{v}' for mapping key '{k}' not a string")
            out.append((k, v, order))
            order += 1
    return out


@dataclass
class Column:
    """A table column as written in the mapping file."""

    name: str
    type: str
    key: str = ""
    keys: list[str] = field(default_factory=list)
    args: dict = field(default_factory=dict)
    from_member: bool = False


@dataclass
class Filters:
    """The filters of a table."""

    require: KeyValues = field(default_factory=list)
    reject: KeyValues = field(default_factory=list)
    require_regexp: dict[str, str] = field(default_factory=dict)
    reject_regexp: dict[str, str] = field(default_factory=dict)
    exclude_tags: list[list[str]] | None = None


@dataclass
class Table:
    """A table of the mapping file."""

    name: str
    type: str
    mapping: KeyValues
    mappings: dict[str, KeyValues]  # sub-mapping name -> mapping
    type_mappings: dict[str, KeyValues]  # points / linestrings / polygons
    columns: list[Column]
    filters: Filters | None
    relation_types: list[str] | None

    def key_values(self, kinds: Iterable[str] = ()) -> KeyValues:
        """mapping, every sub-mapping, and the type_mappings of kinds (points, ...)."""
        return list(
            itertools.chain(self.mapping, *self.mappings.values(), *(self.type_mappings[k] for k in kinds))
        )


@dataclass
class GeneralizedTable:
    """A simplified copy of a table (or of another generalized table)."""

    name: str
    source: str
    tolerance: float
    sql_filter: str


@dataclass
class Config:
    """A parsed mapping file."""

    tables: dict[str, Table]
    generalized_tables: dict[str, GeneralizedTable]
    load_all: bool
    exclude: list[str]
    include: list[str]
    area_tags: list[str] | None  # None: not configured (differs from [])
    linear_tags: list[str] | None
    single_id_space: bool


def parse_column(raw: dict, where: str) -> Column:
    """A column of a table."""
    if not isinstance(raw, dict):
        raise MappingError(f"{where}: column is not a mapping")
    return Column(
        name=str(raw.get("name") or ""),
        type=str(raw.get("type") or ""),
        key=str(raw.get("key") or ""),
        keys=[str(k) for k in raw.get("keys") or []],
        args=raw.get("args") or {},
        from_member=bool(raw.get("from_member", False)),
    )


def parse_filters(raw: Any, where: str) -> Filters | None:
    """The filters of a table (None if it has none)."""
    if raw is None:
        return None
    return Filters(
        require=parse_key_values(raw.get("require"), f"{where} require"),
        reject=parse_key_values(raw.get("reject"), f"{where} reject"),
        require_regexp={str(k): str(v) for k, v in (raw.get("require_regexp") or {}).items()},
        reject_regexp={str(k): str(v) for k, v in (raw.get("reject_regexp") or {}).items()},
        exclude_tags=raw.get("exclude_tags"),
    )


def parse(raw: dict) -> Config:
    """A parsed mapping file; raises MappingError if it is invalid."""
    if not isinstance(raw, dict):
        raise MappingError("mapping is not a YAML/JSON object")
    tables = {}
    for name, t in (raw.get("tables") or {}).items():
        where = f"table {name}"
        if not t.get("type"):
            raise MappingError(f"missing type for table {name}")
        if t["type"] not in TABLE_TYPES:
            raise MappingError(f"{where}: unknown type {t['type']}")
        if t["type"] == "geometry" and (t.get("mapping") is not None or t.get("mappings") is not None):
            raise MappingError(f"table with type:geometry requires type_mappings for table {name}")
        cols = t.get("fields") if t.get("fields") is not None else t.get("columns")
        tm = t.get("type_mappings") or {}
        tables[name] = Table(
            name=name,
            type=t["type"],
            mapping=parse_key_values(t.get("mapping"), where),
            mappings={
                sub: parse_key_values((m or {}).get("mapping"), f"{where} mappings {sub}")
                for sub, m in (t.get("mappings") or {}).items()
            },
            type_mappings={
                kind: parse_key_values(tm.get(kind), f"{where} type_mappings {kind}")
                for kind in TYPE_MAPPING_KINDS.values()
            },
            columns=[parse_column(c, f"{where} column {i}") for i, c in enumerate(cols or [])],
            filters=parse_filters(t.get("filters"), where),
            relation_types=(
                [str(v) for v in t["relation_types"]] if t.get("relation_types") is not None else None
            ),
        )
    gens = {}
    for name, g in (raw.get("generalized_tables") or {}).items():
        gens[name] = GeneralizedTable(
            name=name,
            source=str(g.get("source") or ""),
            tolerance=float(g.get("tolerance") or 0.0),
            sql_filter=str(g.get("sql_filter") or ""),
        )
    tags = raw.get("tags") or {}
    areas = raw.get("areas") or {}
    return Config(
        tables=tables,
        generalized_tables=gens,
        load_all=bool(tags.get("load_all", False)),
        exclude=[str(k) for k in tags.get("exclude") or []],
        include=[str(k) for k in tags.get("include") or []],
        area_tags=[str(k) for k in areas["area_tags"]] if areas.get("area_tags") is not None else None,
        linear_tags=[str(k) for k in areas["linear_tags"]] if areas.get("linear_tags") is not None else None,
        single_id_space=bool(raw.get("use_single_id_space", False)),
    )


def load(path: str) -> Config:
    """Parse a mapping file."""
    with open(path) as f:
        return from_text(f.read())


def from_text(text: str) -> Config:
    """Parse mapping file text (YAML or JSON)."""
    return parse(yaml.load(text, Loader=YamlLoader))
