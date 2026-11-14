"""Settings, from the environment (see README.md)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

from osm_spark_import.mapping import config
from osm_spark_import.mapping.writer import Mapping


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default


@dataclass
class Settings:
    mapping: str = ""
    namespace: str = "osm"
    srid: int = 3857

    # writer (Spark)
    catalog: str = "osm"
    storage_root: str = "hdfs://namenode:8020/osm"
    buckets: int = 16
    partitions: int = 0  # 0: Spark's default parallelism
    chunk_mb: int = 32  # PBF chunk size: decoding runs one task per chunk
    replication_url: str = "https://planet.openstreetmap.org/replication/minute/"
    max_diffs: int = 24
    work_dir: str = "/data/work"

    # loader (PostgreSQL through libpq's PG* variables)
    webhdfs_url: str = "http://namenode:9870"
    webhdfs_user: str = ""
    schema: str = "public"
    schema_import: str = "import"
    schema_backup: str = "backup"
    loader_workers: int = 4
    expire_dir: str = ""
    expire_zoom: int = 15

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            mapping=env("OSM_MAPPING", ""),
            namespace=env("OSM_NAMESPACE", "osm"),
            srid=int(env("OSM_SRID", "3857")),
            catalog=env("OSM_CATALOG", "osm"),
            storage_root=env("OSM_STORAGE_ROOT", cls.storage_root).rstrip("/"),
            buckets=int(env("OSM_BUCKETS", "16")),
            partitions=int(env("OSM_PARTITIONS", "0")),
            chunk_mb=int(env("OSM_CHUNK_MB", "32")),
            replication_url=env("OSM_REPLICATION_URL", cls.replication_url),
            max_diffs=int(env("OSM_MAX_DIFFS", "24")),
            work_dir=env("OSM_WORK_DIR", "/data/work"),
            webhdfs_url=env("OSM_WEBHDFS_URL", cls.webhdfs_url).rstrip("/"),
            webhdfs_user=env("OSM_WEBHDFS_USER", ""),
            schema=env("OSM_SCHEMA", "public"),
            schema_import=env("OSM_SCHEMA_IMPORT", "import"),
            schema_backup=env("OSM_SCHEMA_BACKUP", "backup"),
            loader_workers=int(env("OSM_LOADER_WORKERS", "4")),
            expire_dir=env("OSM_EXPIRE_DIR", ""),
            expire_zoom=int(env("OSM_EXPIRE_ZOOM", "15")),
        )

    def check_mapping(self) -> None:
        if not self.mapping or not os.path.isfile(self.mapping):
            raise SystemExit(f"OSM_MAPPING: mapping file {self.mapping!r} not found")
        if self.srid not in (3857, 4326):
            raise SystemExit("OSM_SRID: 3857 or 4326")

    def load_mapping(self):
        """(mapping.writer.Mapping, mapping file text)."""

        self.check_mapping()
        with open(self.mapping) as f:
            text = f.read()
        return Mapping(config.from_text(text), self.srid), text

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.namespace}.{name}"

    @property
    def outbox(self) -> str:
        """Outbox directory as a URI (writer)."""
        return f"{self.storage_root}/outbox/{self.namespace}"

    @property
    def outbox_path(self) -> str:
        """Outbox directory as a path (loader, through WebHDFS)."""
        return urlparse(self.storage_root).path.rstrip("/") + f"/outbox/{self.namespace}"
