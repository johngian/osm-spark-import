# Derived from imposm3 (Apache License 2.0), translated to Python and modified. See NOTICE.
"""The SQL.

Everything is generated from the batch manifest's table specs, so the loader
needs no mapping file:

    tables: {name: {"type": "polygon", "columns": [{"name", "type", "value_type", "pg"}, ...]}}
    generalized: {name: {"source", "tolerance", "sql_filter"}}

Table names have no prefix.
"""

from __future__ import annotations

import graphlib

from psycopg import sql


def q(name: str) -> str:
    """A quoted SQL identifier."""
    return sql.Identifier(name).as_string()


def is_geometry(col: dict) -> bool:
    """Whether a manifest column is a geometry."""
    return col["pg"] == "GEOMETRY"


def id_column(cols: list[dict]) -> str:
    """Name of the column holding the element ID."""
    for c in cols:
        if c["type"] == "id":
            return c["name"]
    raise ValueError("missing id column")


def create_table_sql(schema: str, name: str, spec: dict) -> str:
    """CREATE TABLE for a table spec, without its geometry column (see add_geometry_column_sql).

    Tables without an "id" column get a BIGSERIAL id, part of the primary key.
    """
    cols_spec = spec["columns"]
    found_id = any(c["name"] == "id" for c in cols_spec)
    pk = [c["name"] for c in cols_spec if c["type"] == "id"]
    cols = []
    if not found_id:
        cols.append("id BIGSERIAL")
        pk.append("id")
    for c in cols_spec:
        if is_geometry(c):
            continue
        cols.append(f'{q(c["name"])} {c["pg"]}')
    if not found_id:
        cols.append("PRIMARY KEY (" + ", ".join(q(p) for p in pk) + ")")
    return f"CREATE TABLE IF NOT EXISTS {q(schema)}.{q(name)} (\n" + ",\n".join(cols) + "\n);"


def add_geometry_column_sql(schema: str, name: str, spec: dict, srid: int) -> str | None:
    """AddGeometryColumn for the first geometry column (None if there is none)."""
    col = next((c["name"] for c in spec["columns"] if is_geometry(c)), None)
    if col is None:
        return None
    gtype = "geometry" if spec["type"] == "relation_member" else spec["type"]
    gtype = gtype.upper()
    if gtype == "POLYGON":
        gtype = "GEOMETRY"  # for multipolygon support
    args = ", ".join(sql.Literal(str(v)).as_string() for v in (schema, name, col, srid, gtype))
    return f"SELECT AddGeometryColumn({args}, 2);"


def copy_columns(spec: dict) -> list[str]:
    """Columns of a table spec, in COPY order."""
    return [c["name"] for c in spec["columns"]]


class Generalized:
    """Generalized tables of a manifest, with their sources checked."""

    def __init__(self, tables: dict, generalized: dict):
        self.tables, self.gen = tables, generalized
        for name, g in generalized.items():
            if g["source"] not in tables and g["source"] not in generalized:
                raise ValueError(f"missing source {g['source']!r} for generalized table {name!r}")

    def root(self, name: str) -> str:
        """The original (non-generalized) source table."""
        src = self.gen[name]["source"]
        while src in self.gen:
            src = self.gen[src]["source"]
        return src

    def order(self) -> list[str]:
        """Generalized tables, each after its source; sorted by name within a level."""
        ts = graphlib.TopologicalSorter(
            {n: [g["source"]] if g["source"] in self.gen else [] for n, g in self.gen.items()}
        )
        ts.prepare()  # raises graphlib.CycleError (a ValueError) on a cycle
        out: list[str] = []
        while ts.is_active():
            ready = sorted(ts.get_ready())
            out += ready
            ts.done(*ready)
        return out

    def of_table(self, table: str) -> list[str]:
        """Every generalized table derived from table, directly or not."""
        return [n for n in self.order() if self.root(n) == table]

    def select_sql(self, name: str) -> str:
        """SELECT list of a generalized table: its root's columns, geometries simplified."""
        g = self.gen[name]
        tol = f"{g['tolerance']:f}"  # 6 decimals
        cols = []
        for c in self.tables[self.root(name)]["columns"]:
            if c["value_type"] == "geometry":
                cols.append(f'ST_SimplifyPreserveTopology({q(c["name"])}, {tol})::Geometry as {q(c["name"])}')
            elif c["value_type"] == "validated_geometry":
                cols.append(
                    f"ST_MakeValid(ST_SimplifyPreserveTopology({q(c['name'])}, {tol}), 'method=structure')"
                    f"::Geometry as {q(c['name'])}"
                )
            else:
                cols.append(q(c["name"]))
        return ",\n".join(cols)

    def create_sql(self, schema: str, name: str) -> str:
        """CREATE TABLE AS for a generalized table, from its source."""
        g = self.gen[name]
        where = f" WHERE {g['sql_filter']}" if g["sql_filter"] else ""
        return (
            f"CREATE TABLE {q(schema)}.{q(name)} AS (SELECT {self.select_sql(name)} "
            f"FROM {q(schema)}.{q(g['source'])}{where})"
        )

    def insert_sql(self, schema: str, name: str) -> str:
        """INSERT of one element's rows (its ID as %s) from the source into a generalized table."""
        g = self.gen[name]
        idc = id_column(self.tables[self.root(name)]["columns"])
        where = f" WHERE {q(idc)} = %s"
        if g["sql_filter"]:
            where += f" AND ({g['sql_filter']})"
        return f"INSERT INTO {q(schema)}.{q(name)} (SELECT {self.select_sql(name)} FROM {q(schema)}.{q(g['source'])}{where})"


def index_sqls(schema: str, name: str, cols: list[dict], generalized: bool) -> list[str]:
    """CREATE INDEX statements: GiST on geometries, B-tree on the ID column where it is not the key."""
    found_id = any(c["name"] == "id" for c in cols)
    out = []
    for c in cols:
        if is_geometry(c):
            out.append(
                f'CREATE INDEX {q(name + "_geom")} ON {q(schema)}.{q(name)} USING GIST ({q(c["name"])})'
            )
        if c["type"] == "id" and (found_id or generalized):
            out.append(
                f'CREATE INDEX {q(name + "_" + c["name"] + "_idx")} ON {q(schema)}.{q(name)} USING BTREE ({q(c["name"])})'
            )
    return out


# which element kinds a table type holds (for diff deletes)
KINDS = {
    "point": ("n",),
    "linestring": ("w",),
    "polygon": ("w", "r"),
    "relation": ("r",),
    "relation_member": ("r",),
    "geometry": ("n", "w", "r"),
}
