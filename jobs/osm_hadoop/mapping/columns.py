"""Column values and types.

Each column type has a value type, which decides the PostgreSQL type; values
are returned as Python values of that type (float32 values as
numpy.float32).
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np
import shapely

from .config import Column, MappingError

# value type -> (PostgreSQL type, Arrow/Spark type)
VALUE_TYPES = {
    "bool": ("BOOL", "boolean"),
    "int8": ("SMALLINT", "smallint"),
    "int32": ("INT", "int"),
    "int64": ("BIGINT", "bigint"),
    "float32": ("REAL", "float"),
    "string": ("VARCHAR", "string"),
    "hstore_string": ("HSTORE", "string"),
    "geometry": ("GEOMETRY", "binary"),
    "validated_geometry": ("GEOMETRY", "binary"),
}

DEFAULT_RANKS = {
    "minor": 3,
    "road": 3,
    "unclassified": 3,
    "residential": 3,
    "tertiary_link": 3,
    "tertiary": 4,
    "secondary_link": 3,
    "secondary": 5,
    "primary_link": 3,
    "primary": 6,
    "trunk_link": 3,
    "trunk": 8,
    "motorway_link": 3,
    "motorway": 9,
}

INT32 = (-(2**31), 2**31 - 1)
INT64 = (-(2**63), 2**63 - 1)
_INT_RE = re.compile(r"[+-]?[0-9]+\Z")


def parse_int(s: str | None, bits: int) -> int | None:
    """strconv.ParseInt(s, 10, bits): None on error (empty, syntax, range)."""
    if not s or not _INT_RE.match(s):
        return None
    v = int(s)
    lo, hi = INT32 if bits == 32 else INT64
    return v if lo <= v <= hi else None


def parse_int_ignoring_error(s: str | None, bits: int) -> int:
    """`v, _ := strconv.ParseInt(s, 10, bits)`: 0 on syntax errors, the
    clamped value on range errors."""
    if not s or not _INT_RE.match(s):
        return 0
    lo, hi = INT32 if bits == 32 else INT64
    return min(max(int(s), lo), hi)


def wrap64(v: int) -> int:
    """64 bit integer overflow."""
    return (v + 2**63) % 2**64 - 2**63


@dataclass
class Ctx:
    """What a value function sees: the element and the match."""

    tags: dict[str, str]
    elem_id: int  # the ID as written (ways/relations transformed)
    geom: Any  # shapely geometry or None
    match_key: str
    match_value: str


@dataclass
class MemberCtx:
    member_id: int
    member_type: int  # 0 node, 1 way, 2 relation
    role: str
    index: int
    element: Ctx | None  # the member element, if cached


def _truthy(v: str) -> bool:
    return not (v == "" or v == "0" or v == "false" or v == "no")


def _bool(val, c):
    return _truthy(val)


def _boolint(val, c):
    return 1 if _truthy(val) else 0


def _string(val, c):
    return val


def _integer(val, c):
    return parse_int(val, 32)


def _id(val, c):
    return c.elem_id


def _key(val, c):
    return c.match_key


def _value(val, c):
    return c.match_value


def _geometry(val, c):
    return c.geom


def _direction(val, c):
    if val in ("1", "yes", "true"):
        return 1
    if val == "-1":
        return -1
    return 0


def _area(val, c):
    if c.geom is None:
        return None
    a = float(shapely.area(c.geom))
    if a == 0.0:
        return None
    return np.float32(a)


def _webmerc_area(val, c):
    if c.geom is None:
        return None
    a = float(shapely.area(c.geom))
    if a == 0.0:
        return None
    minx, miny, maxx, maxy = shapely.bounds(c.geom)
    mid_y = miny + (maxy - miny) / 2
    pole = 6378137 * math.pi
    mid_lat = 2 * math.atan(math.exp((mid_y / pole) * math.pi)) - math.pi / 2
    return np.float32(a * math.pow(math.cos(mid_lat), 2))


def _hstore_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _enum_arg(column: Column, key: str) -> dict[str, int]:
    values = column.args.get(key)
    if values is None:
        raise MappingError(f"missing '{key}' in args for {column.type}")
    if not isinstance(values, list):
        raise MappingError(f"'{key}' in args for {column.type} not a list")
    out = {}
    for i, v in enumerate(values):
        if not isinstance(v, str):
            raise MappingError(f"value in '{key}' not a string")
        out[v] = i + 1
    return out


def _make_hstore(column: Column):
    include = None if "include" not in column.args else _enum_arg(column, "include")

    def hstore(val, c):
        return ", ".join(
            f'"{_hstore_escape(k)}"=>"{_hstore_escape(v)}"'
            for k, v in c.tags.items()
            if include is None or include.get(k, 0) != 0
        )

    return hstore


def _way_zorder_default(val, c):
    z = wrap64(parse_int_ignoring_error(c.tags.get("layer"), 64) * 10)
    rank = DEFAULT_RANKS.get(c.match_value, 0)
    if rank == 0 and "railway" in c.tags:
        rank = 7
    z = wrap64(z + rank)
    if c.tags.get("tunnel") in ("true", "yes", "1"):
        z = wrap64(z - 10)
    if c.tags.get("bridge") in ("true", "yes", "1"):
        z = wrap64(z + 10)
    return z if INT32[0] <= z <= INT32[1] else None


def _make_wayzorder(column: Column):
    if "ranks" not in column.args:
        return _way_zorder_default
    ranks = _enum_arg(column, "ranks")
    offset = len(ranks)
    default = column.args.get("default")
    default_rank = int(default) if isinstance(default, float) else 0  # only float values count

    def wayzorder(val, c):
        z = parse_int_ignoring_error(c.tags.get("layer"), 32) * offset
        rank = ranks.get(c.match_value)
        if rank is None:
            z += default_rank
            rank = 0
        z += rank
        if c.tags.get("tunnel") in ("true", "yes", "1"):
            z -= offset
        if c.tags.get("bridge") in ("true", "yes", "1"):
            z += offset
        return z if INT32[0] <= z <= INT32[1] else None

    return wayzorder


def _make_zorder(column: Column):
    ranks_list = column.args.get("ranks")
    if ranks_list is None:
        raise MappingError("missing ranks in args for zorder")
    if not isinstance(ranks_list, list):
        raise MappingError("ranks in args for zorder not a list")
    key = column.args.get("key", "")
    if not isinstance(key, str):
        raise MappingError("key in args for zorder not a string")
    ranks = {}
    for i, r in enumerate(ranks_list):
        if not isinstance(r, str):
            raise MappingError("rank in ranks not a string")
        ranks[r] = len(ranks_list) - i

    def zorder(val, c):
        if key:
            return ranks.get(c.tags.get(key, ""), 0)
        return ranks.get(c.match_value, 0)

    return zorder


def _make_enumerate(column: Column):
    values = _enum_arg(column, "values")

    def enumerate_(val, c):
        if column.key:
            return values.get(val, 0)
        return values.get(c.match_value, 0)

    return enumerate_


def _make_suffix_replace(column: Column):
    changes = column.args.get("suffixes")
    if changes is None:
        raise MappingError("missing suffixes in args for string_suffixreplace")
    if not isinstance(changes, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in changes.items()
    ):
        raise MappingError("suffixes in args for string_suffixreplace not strings")
    r = re.compile("(" + "|".join(changes) + r")\b", re.ASCII)  # word boundaries in ASCII

    def suffix(val, c):
        return r.sub(lambda m: changes[m.group(0)], val) if val else val

    return suffix


def _make_categorize_int(column: Column):
    values = column.args.get("values")
    if values is None:
        raise MappingError("missing 'values' in 'args' for categorize_int")
    if not isinstance(values, dict):
        raise MappingError("'values' in 'args' for categorize_int not a dictionary")
    for k, v in values.items():
        if not isinstance(k, str) or not isinstance(v, int) or isinstance(v, bool):
            raise MappingError("categorize_int values must map strings to ints")
    if "default" not in column.args:
        raise MappingError("missing 'default' in categorize_int")
    default = column.args["default"]
    if not isinstance(default, int) or isinstance(default, bool):
        raise MappingError("'default' in 'args' for categorize_int not an int")

    def categorize(val, c):
        if val and val in values:
            return values[val]
        for k in column.keys:
            v = c.tags.get(k)
            if v is not None and v in values:
                return values[v]
        return default

    return categorize


def _member_id(m: MemberCtx):
    return m.member_id


def _member_role(m: MemberCtx):
    return m.role


def _member_type(m: MemberCtx):
    return m.member_type


def _member_index(m: MemberCtx):
    return m.index


# name -> (value type, value function or None, factory or None, member function or None)
TYPES: dict[str, tuple[str, Callable | None, Callable | None, Callable | None]] = {
    "bool": ("bool", _bool, None, None),
    "boolint": ("int8", _boolint, None, None),
    "id": ("int64", _id, None, None),
    "string": ("string", _string, None, None),
    "direction": ("int8", _direction, None, None),
    "integer": ("int32", _integer, None, None),
    "mapping_key": ("string", _key, None, None),
    "mapping_value": ("string", _value, None, None),
    "member_id": ("int64", None, None, _member_id),
    "member_role": ("string", None, None, _member_role),
    "member_type": ("int8", None, None, _member_type),
    "member_index": ("int32", None, None, _member_index),
    "geometry": ("geometry", _geometry, None, None),
    "validated_geometry": ("validated_geometry", _geometry, None, None),
    "hstore_tags": ("hstore_string", None, _make_hstore, None),
    "wayzorder": ("int32", None, _make_wayzorder, None),
    "pseudoarea": ("float32", _area, None, None),
    "area": ("float32", _area, None, None),
    "webmerc_area": ("float32", _webmerc_area, None, None),
    "zorder": ("int32", None, _make_zorder, None),
    "enumerate": ("int32", None, _make_enumerate, None),
    "string_suffixreplace": ("string", None, _make_suffix_replace, None),
    "categorize_int": ("int32", None, _make_categorize_int, None),
}
UNSUPPORTED = {"geojson_intersects", "geojson_intersects_feature"}


@dataclass
class ColumnSpec:
    name: str
    type: str
    value_type: str
    key: str
    from_member: bool
    func: Callable | None
    member_func: Callable | None

    @property
    def pg_type(self) -> str:
        return VALUE_TYPES[self.value_type][0]

    @property
    def spark_type(self) -> str:
        return VALUE_TYPES[self.value_type][1]

    @property
    def is_geometry(self) -> bool:
        return self.value_type in ("geometry", "validated_geometry")

    def value(self, c: Ctx) -> Any:
        """valueBuilder.Value."""
        if self.func is None:
            return None
        return self.func(c.tags.get(self.key, ""), c)

    def member_value(self, rel: Ctx, m: MemberCtx) -> Any:
        """valueBuilder.MemberValue."""
        if self.func is not None:
            if self.from_member:
                if m.element is None:
                    return None
                return self.func(m.element.tags.get(self.key, ""), m.element)
            return self.func(rel.tags.get(self.key, ""), rel)
        if self.member_func is not None:
            return self.member_func(m)
        return None


def column_spec(column: Column) -> ColumnSpec:
    if column.type in UNSUPPORTED:
        raise MappingError(f"column type {column.type} is not supported")
    t = TYPES.get(column.type)
    if t is None:
        raise MappingError(f"unhandled type {column.type}")
    value_type, func, factory, member_func = t
    if factory is not None:
        func = factory(column)
    return ColumnSpec(column.name, column.type, value_type, column.key, column.from_member, func, member_func)
