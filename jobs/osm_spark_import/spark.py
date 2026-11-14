"""Spark session and small storage file operations on the driver."""

from __future__ import annotations

import json
import socket
from collections.abc import Iterable

from pyspark.sql import SparkSession


def session(app: str) -> SparkSession:
    builder = SparkSession.builder.appName(app)
    # Executors connect back to the driver: use an address they can reach
    # (the container's, under docker compose).
    try:
        builder = builder.config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
    except OSError:
        pass
    return (
        builder.config("spark.driver.bindAddress", "0.0.0.0")
        # work units are big: keep the Arrow batches handed to Python small
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2000").getOrCreate()
    )


class Storage:
    """File operations through Spark's FileSystem API (hdfs://, s3a://, file://)."""

    def __init__(self, spark: SparkSession):
        self.jvm = spark._jvm
        self.conf = spark._jsc.hadoopConfiguration()

    def _fs(self, path: str):
        p = self.jvm.org.apache.hadoop.fs.Path(path)
        return p.getFileSystem(self.conf), p

    def exists(self, path: str) -> bool:
        fs, p = self._fs(path)
        return fs.exists(p)

    def mkdirs(self, path: str) -> None:
        fs, p = self._fs(path)
        fs.mkdirs(p)

    def delete(self, path: str) -> None:
        fs, p = self._fs(path)
        if fs.exists(p):
            fs.delete(p, True)

    def put(self, local: str, remote: str) -> None:
        fs, p = self._fs(remote)
        fs.copyFromLocalFile(False, True, self.jvm.org.apache.hadoop.fs.Path(local), p)

    def put_files(self, locals_: Iterable[str], remote_dir: str) -> None:
        self.mkdirs(remote_dir)
        for path in locals_:
            self.put(path, remote_dir.rstrip("/") + "/" + path.rsplit("/", 1)[-1])

    def read_text(self, path: str) -> str | None:
        fs, p = self._fs(path)
        if not fs.exists(p):
            return None
        stream = fs.open(p)
        try:
            return self.jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
        finally:
            stream.close()

    def write_text(self, path: str, text: str) -> None:
        """Write to a temporary file, then rename it over path."""
        fs, p = self._fs(path)
        tmp = self.jvm.org.apache.hadoop.fs.Path(path + ".tmp")
        out = fs.create(tmp, True)
        try:
            out.write(bytearray(text.encode("utf-8")))
        finally:
            out.close()
        if fs.exists(p):
            fs.delete(p, False)
        if not fs.rename(tmp, p):
            raise OSError(f"renaming {tmp} to {path}")

    def read_json(self, path: str):
        text = self.read_text(path)
        return None if text is None else json.loads(text)

    def write_json(self, path: str, value) -> None:
        self.write_text(path, json.dumps(value, indent=2, sort_keys=True, default=str))
