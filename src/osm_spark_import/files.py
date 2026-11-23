"""Storage access with fsspec, for the processes without Spark (loader, chunk).

hdfs:// URIs go over WebHDFS when OSM_WEBHDFS_URL is set (optionally with
Kerberos); anything else (hdfs:// through libhdfs, s3://, gs://, abfs://,
file://, https://) is resolved by fsspec.
"""

from __future__ import annotations

from urllib.parse import urlparse

import fsspec
from fsspec import AbstractFileSystem

from osm_spark_import.settings import Settings


def webhdfs(s: Settings) -> AbstractFileSystem:
    """The WebHDFS filesystem of OSM_WEBHDFS_URL."""
    u = urlparse(s.webhdfs_url)
    kw = {"host": u.hostname, "port": u.port or 9870, "use_https": u.scheme == "https"}
    if s.webhdfs_user:
        kw["user"] = s.webhdfs_user
    if s.webhdfs_kerberos:
        kw["kerberos"] = True
    return fsspec.filesystem("webhdfs", **kw)


def url_fs(s: Settings, uri: str) -> tuple[AbstractFileSystem, str]:
    """(filesystem, path) of a URI."""
    if s.webhdfs_url and urlparse(uri).scheme == "hdfs":
        return webhdfs(s), urlparse(uri).path.rstrip("/")
    fs, path = fsspec.core.url_to_fs(uri)
    return fs, path.rstrip("/")
