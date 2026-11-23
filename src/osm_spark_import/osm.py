"""pyosmium: PBF chunks -> elements, and work units -> rows.

Both run in Spark tasks through mapInArrow (pyosmium is installed on every
node).

Coordinates are stored as pyosmium's fixed-point integers (1e-7 degrees).

Splitting: a PBF file is a sequence of blobs ([4 byte length][BlobHeader]
[Blob]); the first is the OSMHeader, the others can be decoded independently.
A chunk is the header blob followed by a run of data blobs: a valid PBF file.

Building: mapping.writer turns a batch of work units into rows and expired
tiles.
"""

from __future__ import annotations

import json
import logging
import struct
import time
from collections.abc import Callable, Iterator
from typing import BinaryIO

import osmium
import pyarrow as pa

from osm_spark_import.mapping import config
from osm_spark_import.mapping.writer import Builder, Mapping, ewkb

log = logging.getLogger(__name__)

# --- schemas -----------------------------------------------------------------

TAGS = pa.map_(pa.string(), pa.string())
MEMBER = pa.struct([("type", pa.string()), ("ref", pa.int64()), ("role", pa.string())])

ELEMENT = pa.schema(
    [
        ("k", pa.string()),
        ("id", pa.int64()),
        ("lon", pa.int32()),
        ("lat", pa.int32()),
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


def uvarint(b: bytes, i: int) -> tuple[int, int]:
    """Decode a protobuf varint at b[i]: (value, next index)."""
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


def blob_header(b: bytes) -> tuple[str, int]:
    """BlobHeader: type (1, string), indexdata (2, bytes), datasize (3, int32)."""
    typ, size, i = None, None, 0
    while i < len(b):
        key, i = uvarint(b, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            v, i = uvarint(b, i)
            if field == 3:
                size = v
        elif wire == 2:
            n, i = uvarint(b, i)
            if field == 1:
                typ = b[i : i + n].decode()
            i += n
        else:
            raise ValueError(f"unexpected wire type {wire} in blob header")
    if typ is None or size is None:
        raise ValueError("incomplete blob header")
    return typ, size


def read_blob(f) -> tuple[str, bytes] | None:
    """The next blob of a PBF file: (type, raw bytes with its headers), None at the end."""
    head = f.read(4)
    if not head:
        return None
    if len(head) < 4:
        raise ValueError("truncated PBF")
    (hlen,) = struct.unpack(">I", head)
    if hlen > 64 * 1024:
        raise ValueError(f"blob header too large: {hlen}")
    header = f.read(hlen)
    typ, size = blob_header(header)
    data = f.read(size)
    if len(data) < size:
        raise ValueError("truncated PBF")
    return typ, head + header + data


def split(f, chunk_bytes: int, open_chunk: Callable[[int], BinaryIO]) -> tuple[bytes, list[int]]:
    """Split a PBF stream into chunks of about chunk_bytes, written as they are read.

    Each chunk is a valid PBF file (the header blob, then data blobs), written
    to open_chunk(index). Returns the header blob and the chunk sizes.
    """
    first = read_blob(f)
    if first is None or first[0] != "OSMHeader":
        raise ValueError("not a PBF file (no OSMHeader)")
    header = first[1]
    sizes: list[int] = []
    out = None
    try:
        while (blob := read_blob(f)) is not None:
            typ, data = blob
            if typ != "OSMData":
                continue
            if out is None or sizes[-1] >= chunk_bytes:
                if out is not None:
                    out.close()
                out = open_chunk(len(sizes))
                out.write(header)
                sizes.append(len(header))
            out.write(data)
            sizes[-1] += len(data)
    finally:
        if out is not None:
            out.close()
    return header, sizes


# --- decoding ----------------------------------------------------------------


class Collector(osmium.SimpleHandler):
    """Collects elements into ELEMENT record batches of batch_size, passed to emit."""

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
        c["lon"].append(lon)
        c["lat"].append(lat)
        c["tags"].append([(t.k, t.v) for t in tags] if len(tags) else None)
        c["refs"].append(refs)
        c["members"].append(members)
        if len(c["id"]) >= self.batch_size:
            self.flush()

    def flush(self):
        """Emit the collected elements, if any."""
        if self.cols["id"]:
            self.emit(pa.RecordBatch.from_pydict(self.cols, schema=ELEMENT))
            self.reset()

    def node(self, n):
        loc = n.location
        self.add("n", n.id, loc.x, loc.y, n.tags, None, None)

    def way(self, w):
        self.add("w", w.id, None, None, w.tags, [r.ref for r in w.nodes], None)

    def relation(self, r):
        self.add("r", r.id, None, None, r.tags, None, [(m.type, m.ref, m.role) for m in r.members])


def decode_pbf(data: bytes, batch_size: int = 100_000) -> list[pa.RecordBatch]:
    """Decode a PBF file (a chunk) into element record batches."""
    out: list[pa.RecordBatch] = []
    h = Collector(out.append, batch_size)
    h.apply_buffer(data, "pbf")
    h.flush()
    return out


def decode_chunks(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """mapInArrow: binaryFile rows (column content) -> elements."""
    for batch in batches:
        for data in batch.column("content").to_pylist():
            yield from decode_pbf(data)


# --- building ----------------------------------------------------------------


def build_rows(
    units: list[dict], mp: Mapping, expire_zoom: int = 0, deleter_expire: bool = False
) -> pa.RecordBatch:
    """ROW batch of the rows (and expired tiles) of a batch of work units."""
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
                vals[c.name] = v
        cols["tbl"].append(r.table)
        cols["id"].append(r.id)
        cols["g"].append(r.gen)
        # default=float: numpy float32 values, exact as float64
        cols["vals"].append(json.dumps(vals, ensure_ascii=False, default=float))
        cols["geom"].append(geom)
        cols["tile"].append(None)
    if b.tiles is not None:
        for t in b.tiles.as_strings():
            for name in ROW.names:
                cols[name].append(t if name == "tile" else None)
    return pa.RecordBatch.from_pydict(cols, schema=ROW)


def unit_nodes(unit: dict) -> int:
    """Number of coordinates a work unit carries (its size)."""
    return len(unit.get("nodes") or ())


def make_build_fn(
    mapping_text: str,
    srid: int,
    expire_zoom: int = 0,
    deleter_expire: bool = False,
    batch_units: int = 1000,
    big_unit_nodes: int = 0,
):
    """mapInArrow function: work units -> rows. Takes the mapping as text so
    the function pickles small and parses it once per task. Units with more
    than big_unit_nodes coordinates (if > 0) are logged with their build time."""

    def fn(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        mp = Mapping(config.from_text(mapping_text), srid)
        for batch in batches:
            units = batch.to_pylist()  # maps become lists of (key, value)
            for i in range(0, len(units), batch_units):
                part = units[i : i + batch_units]
                t0 = time.monotonic()
                out = build_rows(part, mp, expire_zoom, deleter_expire)
                if big_unit_nodes:
                    for u in part:
                        if unit_nodes(u) > big_unit_nodes:
                            log.warning(
                                f"big unit {u['k']}{u['id']}: {unit_nodes(u)} nodes, "
                                f"batch of {len(part)} built in {time.monotonic() - t0:.1f}s"
                            )
                if out.num_rows:
                    yield out

    return fn
