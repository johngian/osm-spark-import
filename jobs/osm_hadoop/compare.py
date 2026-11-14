"""osm-hadoop-compare: compare the tables of two schemas.

    osm-hadoop-compare A B [--samples N]

First exactly (every column as text, geometries as EWKB, as multisets,
without the serial id column). Rows that differ are then paired by their
other columns and accepted if their geometries have the same type and number
of points and are within 1e-6 (SRID units, after ST_Normalize, so
multipolygon part order does not count), and float columns are within 1e-6
relative. Anything else is reported as a difference.

Use it to check that an import plus diffs gives the same tables as an import
of the newer data (docs/design.md).
"""

from __future__ import annotations

import argparse
import sys

import psycopg

TOLERANCE = 1e-6
SKIP = {"spatial_ref_sys", "loader_state"}


def tables(cur, schema: str) -> set[str]:
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE'",
        (schema,),
    )
    return {r[0] for r in cur.fetchall()} - SKIP


def columns(cur, schema: str, table: str) -> list[tuple[str, str]]:
    """(name, kind) with kind geometry/float/other; without the serial id column."""
    cur.execute(
        """SELECT column_name, udt_name, column_default FROM information_schema.columns
           WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""",
        (schema, table),
    )
    out = []
    for name, udt, default in cur.fetchall():
        if name == "id" and default and default.startswith("nextval"):
            continue
        out.append(
            (name, "geometry" if udt == "geometry" else "float" if udt in ("float4", "float8") else "other")
        )
    return out


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def compare_table(cur, a: str, b: str, t: str, samples: int) -> tuple[str, bool]:
    cols = columns(cur, a, t)
    if cols != columns(cur, b, t):
        return "DIFFERENT columns", False
    exact = ", ".join(f"{qi(c)}::text" for c, _ in cols)
    diffs = []
    for x, y in ((a, b), (b, a)):
        cur.execute(
            f"SELECT count(*) FROM (SELECT {exact} FROM {qi(x)}.{qi(t)} EXCEPT ALL SELECT {exact} FROM {qi(y)}.{qi(t)}) d"
        )
        diffs.append(cur.fetchone()[0])
    if diffs == [0, 0]:
        return "ok", True

    # pair only the rows that differ: the exact differences as text, cast back
    keys = [c for c, k in cols if k == "other"]
    geoms = [c for c, k in cols if k == "geometry"]
    floats = [c for c, k in cols if k == "float"]
    for name, x, y in (("pa", a, b), ("pb", b, a)):
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        cur.execute(
            f"CREATE TEMP TABLE {name} AS SELECT {exact} FROM {qi(x)}.{qi(t)} EXCEPT ALL SELECT {exact} FROM {qi(y)}.{qi(t)}"
        )
    # one hashable key: all non-geometry, non-float columns (NULLs marked)
    pkey = (
        "md5(concat_ws(chr(1), " + ", ".join(f"coalesce({qi(c)}, chr(2))" for c in keys) + "))"
        if keys
        else "''"
    )
    order = ", ".join(f"ST_AsEWKB(ST_Normalize({qi(g)}::geometry))" for g in geoms) or "1"
    sel = lambda s: (  # noqa: E731
        f"SELECT *, {pkey} AS pkey, row_number() OVER (PARTITION BY {pkey} ORDER BY {order}) AS rn FROM {s}"
    )
    conds = []
    for g in geoms:
        ga, gb = f"a.{qi(g)}::geometry", f"b.{qi(g)}::geometry"
        conds.append(
            f"((a.{qi(g)} IS NULL AND b.{qi(g)} IS NULL) OR (ST_IsEmpty({ga}) AND ST_IsEmpty({gb})) OR "
            f"(GeometryType({ga}) = GeometryType({gb}) AND ST_NPoints({ga}) = ST_NPoints({gb}) "
            f"AND ST_HausdorffDistance(ST_Normalize({ga}), ST_Normalize({gb})) <= {TOLERANCE}))"
        )
    for f in floats:
        fa, fb = f"a.{qi(f)}::float8", f"b.{qi(f)}::float8"
        conds.append(
            f"((a.{qi(f)} IS NULL AND b.{qi(f)} IS NULL) OR abs({fa} - {fb}) <= {TOLERANCE} * greatest(abs({fa}), abs({fb}), 1))"
        )
    join = "a.pkey = b.pkey AND a.rn = b.rn"
    ok_cond = " AND ".join(conds) or "true"
    cur.execute(
        f"""SELECT count(*) FILTER (WHERE a.rn IS NULL OR b.rn IS NULL OR NOT ({ok_cond}))
            FROM ({sel('pa')}) a FULL JOIN ({sel('pb')}) b ON {join}"""
    )
    bad = cur.fetchone()[0]
    if bad == 0:
        return f"ok within tolerance ({diffs[0]} rows differ in the last digits)", True
    lines = [f"DIFFERENT: {diffs[0]} rows only in {a}, {diffs[1]} only in {b}; {bad} beyond tolerance"]
    first = qi(keys[0]) if keys else "rn"
    shown = (
        ", ".join(f"ST_AsText(a.{qi(g)}::geometry), ST_AsText(b.{qi(g)}::geometry)" for g in geoms)
        or "null, null"
    )
    cur.execute(
        f"""SELECT a.{first}, b.{first}, {shown}
            FROM ({sel('pa')}) a FULL JOIN ({sel('pb')}) b ON {join}
            WHERE a.rn IS NULL OR b.rn IS NULL OR NOT ({ok_cond}) LIMIT {samples}"""
    )
    for row in cur.fetchall():
        lines.append("    " + " | ".join((str(v)[:70] if v is not None else "-") for v in row))
    return "\n".join(lines), False


def compare(a: str, b: str, samples: int = 5) -> bool:
    ok = True
    with psycopg.connect() as conn, conn.cursor() as cur:
        ta, tb = tables(cur, a), tables(cur, b)
        if not ta:
            print(f"no tables in schema {a}")
            return False
        for t in sorted(ta ^ tb):
            print(f"{t:40} only in {a if t in ta else b}")
            ok = False
        for t in sorted(ta & tb):
            cur.execute(f"SELECT count(*) FROM {qi(a)}.{qi(t)}")
            n = cur.fetchone()[0]
            status, good = compare_table(cur, a, b, t, samples)
            print(f"{t:40} {n:8} rows  {status}")
            ok &= good
    return ok


def main() -> None:
    p = argparse.ArgumentParser(prog="osm-hadoop-compare", description=__doc__.split("\n\n")[0])
    p.add_argument("a")
    p.add_argument("b")
    p.add_argument("--samples", type=int, default=5)
    args = p.parse_args()
    sys.exit(0 if compare(args.a, args.b, args.samples) else 1)


if __name__ == "__main__":
    main()
