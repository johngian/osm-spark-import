"""The outbox: Parquet batches in storage, written by Spark, applied by the loader.

    <OSM_STORAGE_ROOT>/outbox/<namespace>/<batch, 12 digits>/
        tables/<table>/*.parquet     rows to insert: osm_type, osm_id, columns..., geom (EWKB)
        deletes/*.parquet            delta only: (osm_type, osm_id) to delete from every table
        _manifest.json               written last: the batch is complete

Batches are numbered 1, 2, 3, ... per namespace. A *snapshot* batch (an
import) holds all rows and replaces the tables. A *delta* batch (a diff run)
applies to the batch named by its `base`: delete the keys, insert the rows.

Manifest:

    {"batch": 7, "kind": "delta", "base": 6, "namespace": "andorra",
     "sequence": 4966, "osm_time": "2026-11-12T21:23:05+00:00",
     "created": "...", "srid": 3857,
     "tables": {"roads": {"geometry": "line", "columns": [["name", "text"], ...], "rows": 12}},
     "deletes": 30}
"""

from __future__ import annotations

MANIFEST = "_manifest.json"


def batch_name(batch: int) -> str:
    return f"{batch:012d}"


def batch_dir(root: str, batch: int) -> str:
    return f"{root}/{batch_name(batch)}"
