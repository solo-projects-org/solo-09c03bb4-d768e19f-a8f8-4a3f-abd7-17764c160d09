"""Sample water-level table bootstrap.

Creates a small unpartitioned Delta table containing several tiny parquet
files so that compaction has something to do.  Idempotent and race-safe:
initialisation itself commits via the same atomic log primitive.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa

from app.delta import init_table, load_snapshot
from .external_writer import append_rows


def _batch(station: str, start: int, levels: list[int], t0: int) -> list[dict]:
    return [
        {
            "station": station,
            "utc_seconds": t0 + start + i,
            "level_mm": level,
        }
        for i, level in enumerate(levels)
    ]


def create_sample_table(table: Path) -> dict:
    """Create the demo table (if absent) with 4 small files across 2 versions.

    Concurrent calls (e.g. two API replicas starting together) converge: a
    writer that loses a commit race re-reads the table and stops appending as
    soon as another writer has populated it.
    """
    table = Path(table)
    init_table(table)
    snap = load_snapshot(table)
    if snap.active:
        return {"table": str(table), "created": False, "version": snap.version}

    t0 = 1_731_600_000  # fixed UTC base so the sample data is deterministic
    batches = [
        _batch("ST-001", 0, [1204, 1207, 1210, 1208, 1211, 1209], t0),
        _batch("ST-002", 0, [980, 982, 979, 985, 988, 984], t0),
        _batch("ST-001", 3600, [1215, 1218, 1216, 1220, 1223, 1219], t0),
        _batch("ST-003", 0, [1502, 1505, 1501, 1499, 1504, 1507], t0),
    ]
    expected_files = len(batches)
    append_versions: list[int] = []
    for batch in batches:
        # If another replica is populating the table in parallel, stop as soon
        # as the complete sample exists; otherwise keep writing our share.
        if len(load_snapshot(table).active) >= expected_files:
            break
        append_versions.append(append_rows(table, batch)["version"])
    snap = load_snapshot(table)
    return {
        "table": str(table),
        "created": len(append_versions) == expected_files,
        "versions": {"create": 0, "appends": append_versions},
        "data_files": len(snap.active),
    }


if __name__ == "__main__":  # pragma: no cover
    import sys

    target = Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("TABLE_DIR", "/data/water_level"))
    print(create_sample_table(target))
