import hashlib
import io
import json

import pyarrow as pa
import pytest
from test_osm import write_pbf

from osm_spark_import import chunk, osm
from osm_spark_import.settings import Settings


@pytest.fixture
def pbf(tmp_path):
    path = tmp_path / "t.osm.pbf"
    write_pbf(path, n_nodes=30000)
    return path


def elements(data: bytes) -> list[tuple[str, int]]:
    t = pa.Table.from_batches(osm.decode_pbf(data))
    return sorted(zip(t.column("k").to_pylist(), t.column("id").to_pylist(), strict=True))


def test_chunk_round_trip(pbf, tmp_path):
    dest = tmp_path / "chunks"
    m = chunk.chunk(Settings(), str(pbf), f"file://{dest}", chunks=3, sequence=42)
    files = sorted(dest.glob("chunk-*.osm.pbf"))
    assert m["chunks"] == len(files) > 1
    assert m["sequence"] == 42 and m["osm_time"] is None and m["size"] == pbf.stat().st_size
    assert m["md5"] == hashlib.md5(pbf.read_bytes()).hexdigest()
    assert m["max_chunk_bytes"] == max(f.stat().st_size for f in files)
    assert json.loads((dest / chunk.MANIFEST).read_text()) == m
    parts = sorted(e for f in files for e in elements(f.read_bytes()))
    assert parts == elements(pbf.read_bytes())


def test_chunk_checks_md5(pbf, tmp_path):
    dest = tmp_path / "chunks"
    with pytest.raises(SystemExit, match="MD5 mismatch"):
        chunk.chunk(Settings(), str(pbf), str(dest), md5="0" * 32, sequence=1)
    assert not (dest / chunk.MANIFEST).exists()
    # SOURCE.md5, as published next to planet files
    (tmp_path / "t.osm.pbf.md5").write_text(f"{hashlib.md5(pbf.read_bytes()).hexdigest()}  t.osm.pbf\n")
    assert chunk.chunk(Settings(), str(pbf), str(dest), sequence=1)["chunks"] == 1


def test_chunk_refuses_to_overwrite(pbf, tmp_path):
    dest = str(tmp_path / "chunks")
    chunk.chunk(Settings(), str(pbf), dest, sequence=1)
    with pytest.raises(SystemExit, match="--force"):
        chunk.chunk(Settings(), str(pbf), dest, sequence=1)
    chunk.chunk(Settings(), str(pbf), dest, sequence=2, force=True)


def test_chunk_needs_a_sequence_without_timestamp(pbf, tmp_path):
    with pytest.raises(SystemExit, match="--sequence"):
        chunk.chunk(Settings(), str(pbf), str(tmp_path / "chunks"))


class FlakyFS:
    """Opens files whose first copy fails after `fail_at` bytes."""

    def __init__(self, data: bytes, fail_at: int):
        self.data, self.fail_at, self.opened = data, fail_at, 0

    def open(self, path, mode):
        self.opened += 1
        f = io.BytesIO(self.data)
        if self.opened == 1:
            read = f.read

            def flaky(n):
                if f.tell() + n > self.fail_at:
                    raise ConnectionResetError("reset")
                return read(n)

            f.read = flaky
        return f


def test_reader_resumes_after_error(monkeypatch):
    monkeypatch.setattr(chunk.time, "sleep", lambda s: None)
    data = bytes(range(256)) * 100
    fs = FlakyFS(data, fail_at=1000)
    r = chunk.ResumableReader(fs, "x")
    got = b"".join(iter(lambda: r.read(300), b""))
    assert got == data and fs.opened == 2 and r.md5.hexdigest() == hashlib.md5(data).hexdigest()
