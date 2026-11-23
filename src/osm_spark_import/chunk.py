"""chunk: a PBF (path or URL) -> PBF chunks in storage, ready to import.

The PBF is streamed from its source (local path, https://, hdfs://, ...)
straight into the destination directory, without a local copy:

    DEST/chunk-000000.osm.pbf ...   valid PBF files: the header blob, then data blobs
    DEST/_chunks.json               written last: the chunks are complete

The import decodes one chunk per Spark task, so the chunk size sets the work
per task: choose it from the memory of a task (--chunk-mb), or from the
cluster's cores (--chunks, e.g. 2-4 per core). Read errors are retried from
where the stream stopped; the MD5 is checked against --md5 or SOURCE.md5.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import math
import tempfile
import time

import aiohttp
from fsspec import AbstractFileSystem
from osmium.replication.server import ReplicationServer
from osmium.replication.utils import get_replication_header

from osm_spark_import import files, osm
from osm_spark_import.settings import Settings

MANIFEST = "_chunks.json"
RETRIES = 8
RETRYABLE = (OSError, TimeoutError, aiohttp.ClientError)

log = logging.getLogger(__name__)


class ResumableReader:
    """A sequential reader that reopens the file at its offset after a read error,
    and computes the MD5 of what it reads."""

    def __init__(self, fs: AbstractFileSystem, path: str, retries: int = RETRIES):
        self.fs, self.path, self.retries = fs, path, retries
        self.offset = 0
        self.md5 = hashlib.md5()
        self.f = None

    def read(self, n: int) -> bytes:
        """Up to n bytes (fewer only at the end)."""
        for attempt in range(self.retries + 1):
            try:
                if self.f is None:
                    self.f = self.fs.open(self.path, "rb")
                    self.f.seek(self.offset)
                data = self.f.read(n)
                break
            except RETRYABLE as e:
                if attempt == self.retries:
                    raise
                log.warning(f"read error at byte {self.offset} ({e}); retrying")
                self.close()
                time.sleep(min(2**attempt, 60))
        self.offset += len(data)
        self.md5.update(data)
        return data

    def close(self) -> None:
        """Close the underlying file, ignoring errors."""
        if self.f is not None:
            try:
                self.f.close()
            except RETRYABLE:
                pass
            self.f = None


def published_md5(fs: AbstractFileSystem, path: str) -> str | None:
    """The hash in PATH.md5 ("<hash>  <name>"), if there is one."""
    try:
        if not fs.exists(path + ".md5"):
            return None
        return fs.cat(path + ".md5").decode().split()[0].lower()
    except RETRYABLE:
        return None


def replication_state(
    s: Settings, fs: AbstractFileSystem, path: str, sequence: int | None
) -> tuple[int, str | None]:
    """Replication sequence and time of a PBF, from its header blob (read alone)."""
    with fs.open(path, "rb") as f:
        first = osm.read_blob(f)
    if first is None or first[0] != "OSMHeader":
        raise SystemExit(f"{path}: not a PBF file (no OSMHeader)")
    with tempfile.NamedTemporaryFile(suffix=".osm.pbf") as tmp:
        tmp.write(first[1])
        tmp.flush()
        _, _, ts = get_replication_header(tmp.name)
    if sequence is None:
        if ts is None:
            raise SystemExit("no timestamp in the PBF header: pass --sequence")
        sequence = ReplicationServer(s.replication_url).timestamp_to_sequence(ts)
        if sequence is None:
            raise SystemExit(f"no replication sequence for {ts} at {s.replication_url}; pass --sequence")
    return sequence, ts.isoformat() if ts else None


def chunk(
    s: Settings,
    source: str,
    dest: str,
    chunk_mb: int | None = None,
    chunks: int | None = None,
    md5: str | None = None,
    sequence: int | None = None,
    force: bool = False,
) -> dict:
    """Split source into chunks in dest; returns the chunks manifest."""
    src_fs, src = files.url_fs(s, source)
    dst_fs, dst = files.url_fs(s, dest)
    if dst_fs.exists(f"{dst}/{MANIFEST}") and not force:
        raise SystemExit(f"{dest} already holds chunks; pass --force to replace them")
    if dst_fs.exists(dst):
        dst_fs.rm(dst, recursive=True)
    dst_fs.makedirs(dst, exist_ok=True)

    size = src_fs.size(src)
    if chunks:
        if not size:
            raise SystemExit(f"size of {source} unknown: use --chunk-mb")
        chunk_bytes = math.ceil(size / chunks)
    else:
        chunk_bytes = (chunk_mb or s.chunk_mb) << 20
    expected = (md5 or published_md5(src_fs, src) or "").lower() or None
    seq, osm_time = replication_state(s, src_fs, src, sequence)
    log.info(f"{source} ({size} bytes) -> {dest}, chunks of {chunk_bytes >> 20} MB")

    def open_chunk(i: int):
        if i % 100 == 0:
            log.info(f"chunk {i}")
        return dst_fs.open(f"{dst}/chunk-{i:06d}.osm.pbf", "wb")

    reader = ResumableReader(src_fs, src)
    try:
        _, sizes = osm.split(reader, chunk_bytes, open_chunk)
    finally:
        reader.close()
    actual = reader.md5.hexdigest()
    if expected and actual != expected:
        raise SystemExit(f"MD5 mismatch: {actual}, expected {expected}; chunks left incomplete")
    if expected is None:
        log.warning(f"no MD5 to check against (MD5 {actual})")
    manifest = {
        "source": source,
        "md5": actual,
        "size": reader.offset,
        "chunks": len(sizes),
        "max_chunk_bytes": max(sizes, default=0),
        "sequence": seq,
        "osm_time": osm_time,
        "created": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    with dst_fs.open(f"{dst}/{MANIFEST}", "w") as f:
        f.write(json.dumps(manifest, indent=2, sort_keys=True))
    log.info(f"{len(sizes)} chunks; PBF data till {osm_time}, replication continues after sequence {seq}")
    return manifest
