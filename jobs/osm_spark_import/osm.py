"""pyosmium: PBF chunks -> elements, and work units -> rows.

Both run in Spark tasks through mapInArrow (pyosmium is installed on every
node).

Coordinates are stored as uint32 values (mapping.geom q_pbf/q_osc), the
values every geometry is built from.

Splitting: a PBF file is a sequence of blobs ([4 byte length][BlobHeader]
[Blob]); the first is the OSMHeader, the others can be decoded independently.
A chunk is the header blob followed by a run of data blobs: a valid PBF file.

Building: mapping.writer turns a batch of work units into rows and expired
tiles.
"""

from __future__ import annotations

import json
import os
import struct
from collections.abc import Iterator

import numpy as np
import osmium
import pyarrow as pa

from .mapping import geom as igeom

# --- schemas -----------------------------------------------------------------

TAGS = pa.map_(pa.string(), pa.string())
MEMBER = pa.struct([("type", pa.string()), ("ref", pa.int64()), ("role", pa.string())])

ELEMENT = pa.schema(
    [
        ("k", pa.string()),
        ("id", pa.int64()),
        ("qlon", pa.int64()),
        ("qlat", pa.int64()),
        ("tags", TAGS),
        ("refs", pa.list_(pa.int64())),
        ("members", pa.list_(MEMBER)),
    ]
)

ROW = pa.schema(
    [
        ("tbl", pa.string()),
        ("id", pa.int64()),  # the element ID as written
        ("g", pa.bool_()),  # update the generalized tables for it (diffs)
        ("vals", pa.string()),  # JSON object: non-geometry column -> value
        ("geom", pa.binary()),  # EWKB
        ("tile", pa.string()),  # or an expired tile, z/x/y
    ]
)

# --- splitting ---------------------------------------------------------------


def _uvarint(b: bytes, i: int) -> tuple[int, int]:
    shift = value = 0
    while True:
        if i >= len(b):
            raise ValueError("truncated varint")
        c = b[i]
        i += 1
        value |= (c & 0x7F) << shift
        if c < 0x80:
            return value, i
        shift += 7


def _blob_header(b: bytes) -> tuple[str, int]:
    """BlobHeader: type (1, string), indexdata (2, bytes), datasize (3, int32)."""
    typ, size, i = None, None, 0
    while i < len(b):
        key, i = _uvarint(b, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            v, i = _uvarint(b, i)
            if field == 3:
                size = v
        elif wire == 2:
            n, i = _uvarint(b, i)
            if field == 1:
                typ = b[i : i + n].decode()
            i += n
        else:
            raise ValueError(f"unexpected wire type {wire} in blob header")
    if typ is None or size is None:
        raise ValueError("incomplete blob header")
    return typ, size


def _read_blob(f) -> tuple[str, bytes] | None:
    head = f.read(4)
    if not head:
        return None
    if len(head) < 4:
        raise ValueError("truncated PBF")
    (hlen,) = struct.unpack(">I", head)
    if hlen > 64 * 1024:
        raise ValueError(f"blob header too large: {hlen}")
    header = f.read(hlen)
    typ, size = _blob_header(header)
    data = f.read(size)
    if len(data) < size:
        raise ValueError("truncated PBF")
    return typ, head + header + data


def split_pbf(path: str, out_dir: str, chunk_bytes: int = 32 << 20) -> list[str]:
    """Split a PBF file into chunks of about chunk_bytes; returns their paths."""
    os.makedirs(out_dir, exist_ok=True)
    paths: list[str] = []
    with open(path, "rb") as f:
        first = _read_blob(f)
        if first is None or first[0] != "OSMHeader":
            raise ValueError(f"{path}: not a PBF file (no OSMHeader)")
        header = first[1]
        out, written = None, 0
        while (blob := _read_blob(f)) is not None:
            typ, data = blob
            if typ != "OSMData":
                continue
            if out is None or written >= chunk_bytes:
                if out:
                    out.close()
                paths.append(os.path.join(out_dir, f"chunk-{len(paths):06d}.osm.pbf"))
                out = open(paths[-1], "wb")
                out.write(header)
                written = len(header)
            out.write(data)
            written += len(data)
        if out:
            out.close()
    return paths


# --- decoding ----------------------------------------------------------------


class _Collector(osmium.SimpleHandler):
    def __init__(self, emit, batch_size: int):
        super().__init__()
        self.emit, self.batch_size = emit, batch_size
        self.reset()

    def reset(self):
        self.cols = {name: [] for name in ELEMENT.names}

    def add(self, k, id_, lon, lat, tags, refs, members):
        c = self.cols
        c["k"].append(k)
        c["id"].append(id_)
        c["qlon"].append(lon)
        c["qlat"].append(lat)
        c["tags"].append([(t.k, t.v) for t in tags] if len(tags) else None)
        c["refs"].append(refs)
        c["members"].append(members)
        if len(c["id"]) >= self.batch_size:
            self.flush()

    def flush(self):
        if self.cols["id"]:
            self.emit(pa.RecordBatch.from_pydict(self.cols, schema=ELEMENT))
            self.reset()

    def node(self, n):
        loc = n.location
        self.add("n", n.id, igeom.q_pbf(loc.x), igeom.q_pbf(loc.y), n.tags, None, None)

    def way(self, w):
        self.add("w", w.id, None, None, w.tags, [r.ref for r in w.nodes], None)

    def relation(self, r):
        self.add("r", r.id, None, None, r.tags, None, [(m.type, m.ref, m.role) for m in r.members])


def decode_pbf(data: bytes, batch_size: int = 100_000) -> list[pa.RecordBatch]:
    """Decode a PBF file (a chunk) into element record batches."""
    out: list[pa.RecordBatch] = []
    h = _Collector(out.append, batch_size)
    h.apply_buffer(data, "pbf")
    h.flush()
    return out


def decode_chunks(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """mapInArrow: binaryFile rows (column content) -> elements."""
    for batch in batches:
        for data in batch.column("content").to_pylist():
            yield from decode_pbf(data)


# --- building ----------------------------------------------------------------


def _json_value(v):
    if isinstance(v, np.floating):
        return float(v)  # float32 values: exact as float64
    return v


def build_rows(units: list[dict], mp, expire_zoom: int = 0, deleter_expire: bool = False) -> pa.RecordBatch:
    """Rows (and expired tiles) of a batch of work units."""
    from .mapping.writer import Builder, ewkb

    b = Builder(mp, expire_zoom, deleter_expire)
    b.process(units)
    cols: dict[str, list] = {n: [] for n in ROW.names}
    for r in b.rows:
        specs = mp.columns[r.table]
        geom = None
        vals = {}
        for c, v in zip(specs, r.values, strict=False):
            if c.is_geometry:
                if geom is None:
                    geom = ewkb(v, mp.srid)
            else:
                vals[c.name] = _json_value(v)
        cols["tbl"].append(r.table)
        cols["id"].append(r.id)
        cols["g"].append(r.gen)
        cols["vals"].append(json.dumps(vals, ensure_ascii=False))
        cols["geom"].append(geom)
        cols["tile"].append(None)
    if b.tiles is not None:
        for t in b.tiles.as_strings():
            for name in ROW.names:
                cols[name].append(t if name == "tile" else None)
    return pa.RecordBatch.from_pydict(cols, schema=ROW)


def make_build_fn(
    mapping_text: str, srid: int, expire_zoom: int = 0, deleter_expire: bool = False, batch_units: int = 1000
):
    """mapInArrow function: work units -> rows. Takes the mapping as text so
    the function pickles small and parses it once per task."""

    def fn(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        from .mapping import config
        from .mapping.writer import Mapping

        mp = Mapping(config.from_text(mapping_text), srid)
        for batch in batches:
            units = batch.to_pylist()  # maps become lists of (key, value)
            for i in range(0, len(units), batch_units):
                out = build_rows(units[i : i + batch_units], mp, expire_zoom, deleter_expire)
                if out.num_rows:
                    yield out

    return fn
