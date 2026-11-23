"""Settings, from the environment (see README.md)."""

from __future__ import annotations

import dataclasses
import logging
import os
import re
from dataclasses import dataclass

import fsspec

from osm_spark_import.mapping import config
from osm_spark_import.mapping.writer import Mapping

NAME_RE = re.compile(r"[a-z][a-z0-9_]*\Z")
TRUE = {"1", "true", "yes", "on"}


def setup_logging() -> None:
    """Timestamped INFO messages from this package; WARNING and above from the rest."""
    logging.basicConfig(format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("osm_spark_import").setLevel(logging.INFO)


@dataclass
class Settings:
    """Every field is read from OSM_<FIELD NAME> (e.g. OSM_STORAGE_ROOT)."""

    mapping: str = ""
    namespace: str = "osm"
    srid: int = 3857

    # writer (Spark)
    catalog: str = "osm"
    storage_root: str = "hdfs://namenode:8020/osm"
    buckets: int = 16
    partitions: int = 0  # 0: Spark's default parallelism
    chunk_mb: int = 32  # default PBF chunk size of `chunk`: decoding runs one task per chunk
    replication_url: str = "https://planet.openstreetmap.org/replication/minute/"
    max_diffs: int = 24
    big_unit_nodes: int = 100_000  # work units with more coordinates are built apart
    snapshot_retention_hours: int = 24  # `maintain` expires older cache snapshots

    # loader (PostgreSQL through libpq's PG* variables) and `chunk`
    webhdfs_url: str = ""  # read the outbox over WebHDFS; empty: fsspec on OSM_STORAGE_ROOT
    webhdfs_user: str = ""
    webhdfs_kerberos: bool = False
    schema: str = "public"
    schema_import: str = "import"
    schema_backup: str = "backup"
    loader_workers: int = 4
    expire_dir: str = ""
    expire_zoom: int = 15

    def __post_init__(self) -> None:
        self.storage_root = self.storage_root.rstrip("/")
        self.webhdfs_url = self.webhdfs_url.rstrip("/")
        for name in ("namespace", "catalog"):
            if not NAME_RE.match(getattr(self, name)):
                raise SystemExit(
                    f"OSM_{name.upper()}: lower case letters, digits and _, starting with a letter"
                )

    @classmethod
    def from_env(cls) -> Settings:
        """Settings from OSM_* variables; unset or empty ones keep their default."""
        values = {}
        for f in dataclasses.fields(cls):
            raw = os.environ.get(f"OSM_{f.name.upper()}")
            if raw:
                kind = type(f.default)
                values[f.name] = raw.lower() in TRUE if kind is bool else kind(raw)
        return cls(**values)

    def load_mapping(self) -> tuple[Mapping, str]:
        """The parsed mapping and the mapping file text. OSM_MAPPING is a path or a
        URL (e.g. hdfs://, for jobs whose driver runs in a cluster container)."""
        if self.srid not in (3857, 4326):
            raise SystemExit("OSM_SRID: 3857 or 4326")
        if not self.mapping:
            raise SystemExit("OSM_MAPPING: no mapping file")
        try:
            with fsspec.open(self.mapping, "r") as f:
                text = f.read()
        except FileNotFoundError as e:
            raise SystemExit(f"OSM_MAPPING: mapping file {self.mapping!r} not found") from e
        return Mapping(config.from_text(text), self.srid), text

    def table(self, name: str) -> str:
        """Fully qualified name of a cache table."""
        return f"{self.catalog}.{self.namespace}.{name}"

    @property
    def staging(self) -> str:
        """Staging directory of the namespace's runs."""
        return f"{self.storage_root}/staging/{self.namespace}"

    @property
    def outbox(self) -> str:
        """Outbox directory as a URI."""
        return f"{self.storage_root}/outbox/{self.namespace}"
