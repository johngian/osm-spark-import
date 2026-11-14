"""The SQL.

Everything is generated from the batch manifest's table specs, so the loader
needs no mapping file:

    tables: {name: {"type": "polygon", "columns": [{"name", "type", "value_type", "pg"}, ...]}}
    generalized: {name: {"source", "tolerance", "sql_filter"}}

Table names have no prefix.
"""

from __future__ import annotations


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def is_geometry(col: dict) -> bool:
    return col["pg"] == "GEOMETRY"


def id_column(cols: list[dict]) -> str:
    for c in cols:
        if c["type"] == "id":
            return c["name"]
    raise ValueError("missing id column")


def create_table_sql(schema: str, name: str, spec: dict) -> str:
    """TableSpec.CreateTableSQL."""
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
    """addGeometryColumn: the first geometry column only."""
    col = next((c["name"] for c in spec["columns"] if is_geometry(c)), None)
    if col is None:
        return None
    gtype = "geometry" if spec["type"] == "relation_member" else spec["type"]
    gtype = gtype.upper()
    if gtype == "POLYGON":
        gtype = "GEOMETRY"  # for multipolygon support
    return f"SELECT AddGeometryColumn('{schema}', '{name}', '{col}', '{srid}', '{gtype}', 2);"


def copy_columns(spec: dict) -> list[str]:
    return [c["name"] for c in spec["columns"]]


class Generalized:
    """GeneralizedTableSpec with its sources resolved (prepareGeneralizedTableSources)."""

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
        """Tables with non-generalized sources first (sortedGeneralizedTables)."""
        done, out = set(), []
        names = sorted(self.gen)
        while len(out) < len(names):
            progress = False
            for n in names:
                if n in done:
                    continue
                src = self.gen[n]["source"]
                if src not in self.gen or src in done:
                    done.add(n)
                    out.append(n)
                    progress = True
            if not progress:
                raise ValueError("cycle in generalized tables")
        return out

    def of_table(self, table: str) -> list[str]:
        """Source.Generalizations: every generalized table derived from table."""
        return [n for n in self.order() if self.root(n) == table]

    def _select(self, name: str) -> str:
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
        """generalizeTable."""
        g = self.gen[name]
        where = f" WHERE {g['sql_filter']}" if g["sql_filter"] else ""
        return (
            f"CREATE TABLE {q(schema)}.{q(name)} AS (SELECT {self._select(name)} "
            f"FROM {q(schema)}.{q(g['source'])}{where})"
        )

    def insert_sql(self, schema: str, name: str) -> str:
        """GeneralizedTableSpec.InsertSQL ($1 as %s)."""
        g = self.gen[name]
        idc = id_column(self.tables[self.root(name)]["columns"])
        where = f" WHERE {q(idc)} = %s"
        if g["sql_filter"]:
            where += f" AND ({g['sql_filter']})"
        return f"INSERT INTO {q(schema)}.{q(name)} (SELECT {self._select(name)} FROM {q(schema)}.{q(g['source'])}{where})"

    def delete_sql(self, schema: str, name: str) -> str:
        idc = id_column(self.tables[self.root(name)]["columns"])
        return f"DELETE FROM {q(schema)}.{q(name)} WHERE {q(idc)} = ANY(%s)"


def index_sqls(schema: str, name: str, cols: list[dict], generalized: bool) -> list[str]:
    """createIndex."""
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


def delete_sql(schema: str, name: str, spec: dict) -> str:
    """TableSpec.DeleteSQL for many IDs."""
    return f"DELETE FROM {q(schema)}.{q(name)} WHERE {q(id_column(spec['columns']))} = ANY(%s)"
