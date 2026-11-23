"""Spark session and small storage file operations on the driver."""

from __future__ import annotations

import contextlib
import json
import os
from collections.abc import Iterable, Iterator
from urllib.parse import urlparse

from pyspark.sql import SparkSession

# work units are big: keep the Arrow batches handed to Python small
DEFAULTS = {"spark.sql.execution.arrow.maxRecordsPerBatch": "2000"}
# object stores: a PUT is atomic, a rename is a copy
OBJECT_STORES = {"s3", "s3a", "gs", "abfs", "abfss", "wasb", "wasbs"}


def session(app: str) -> SparkSession:
    """The Spark session, with DEFAULTS for settings the deployment leaves unset."""
    spark = SparkSession.builder.appName(app).getOrCreate()
    configured = spark.sparkContext.getConf()
    for key, value in DEFAULTS.items():
        if not configured.contains(key):
            spark.conf.set(key, value)
    return spark


@contextlib.contextmanager
def conf_scope(spark: SparkSession, settings: dict[str, str]) -> Iterator[None]:
    """Set session settings for the duration of a block (around the actions that need them)."""
    before = {key: spark.conf.get(key, None) for key in settings}
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, value in before.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


class Storage:
    """File operations through Spark's FileSystem API (hdfs://, s3a://, file://)."""

    def __init__(self, spark: SparkSession):
        self.jvm = spark._jvm
        self.gateway = spark.sparkContext._gateway
        self.conf = spark._jsc.hadoopConfiguration()

    def path(self, path: str):
        """A Hadoop Path."""
        return self.jvm.org.apache.hadoop.fs.Path(path)

    def resolve(self, path: str):
        """(Hadoop FileSystem, Path) of a URI."""
        p = self.path(path)
        return p.getFileSystem(self.conf), p

    def exists(self, path: str) -> bool:
        """Whether a file or directory exists."""
        fs, p = self.resolve(path)
        return fs.exists(p)

    def mkdirs(self, path: str) -> None:
        """Create a directory and its parents."""
        fs, p = self.resolve(path)
        fs.mkdirs(p)

    def delete(self, path: str) -> None:
        """Delete a file or directory tree, if it exists."""
        fs, p = self.resolve(path)
        if fs.exists(p):
            fs.delete(p, True)

    def put(self, local: str, remote: str) -> None:
        """Copy a local file to storage, overwriting."""
        fs, p = self.resolve(remote)
        fs.copyFromLocalFile(False, True, self.path(local), p)

    def put_files(self, locals_: Iterable[str], remote_dir: str) -> None:
        """Copy local files into a storage directory, keeping their names."""
        self.mkdirs(remote_dir)
        for path in locals_:
            self.put(path, f"{remote_dir.rstrip('/')}/{os.path.basename(path)}")

    def read_text(self, path: str) -> str | None:
        """A UTF-8 file's text, or None if it does not exist."""
        fs, p = self.resolve(path)
        if not fs.exists(p):
            return None
        stream = fs.open(p)
        try:
            return self.jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
        finally:
            stream.close()

    def write(self, fs, p, text: str, overwrite: bool) -> None:
        """Create a file with text; raises if it exists and overwrite is False."""
        out = fs.create(p, overwrite)
        try:
            out.write(bytearray(text.encode("utf-8")))
        finally:
            out.close()

    def write_text(self, path: str, text: str) -> None:
        """Replace a file atomically: on object stores with one PUT, elsewhere by
        renaming a temporary file over it (FileContext rename with OVERWRITE)."""
        fs, p = self.resolve(path)
        if urlparse(path).scheme in OBJECT_STORES:
            self.write(fs, p, text, True)
            return
        tmp = self.path(path + ".tmp")
        self.write(fs, tmp, text, True)
        fc = self.jvm.org.apache.hadoop.fs.FileContext.getFileContext(p.toUri(), self.conf)
        rename = self.jvm.org.apache.hadoop.fs.Options.Rename
        options = self.gateway.new_array(rename, 1)
        options[0] = rename.OVERWRITE
        fc.rename(tmp, p, options)

    def create_exclusive(self, path: str, text: str) -> bool:
        """Create a file with text unless it exists (atomic on HDFS); False if it exists."""
        fs, p = self.resolve(path)
        fs.mkdirs(p.getParent())
        try:
            self.write(fs, p, text, False)
        except Exception as e:  # py4j wraps FileAlreadyExistsException
            if "FileAlreadyExistsException" in str(e) or fs.exists(p):
                return False
            raise
        return True

    def read_json(self, path: str):
        """A JSON file's value, or None if it does not exist."""
        text = self.read_text(path)
        return None if text is None else json.loads(text)

    def write_json(self, path: str, value) -> None:
        """Write value as JSON, atomically (see write_text)."""
        self.write_text(path, json.dumps(value, indent=2, sort_keys=True, default=str))
