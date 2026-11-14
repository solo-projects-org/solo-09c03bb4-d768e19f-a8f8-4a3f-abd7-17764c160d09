"""External table writer.

This module intentionally lives outside the compaction service: it models a
separate process/app that keeps committing to the same Delta table while the
compaction API is running.  It speaks only the raw Delta JSON-log protocol
through the atomic commit primitive, never touching compaction state.

Rules honoured here:

* data files are immutable and every new file gets a unique path
* appends use ``dataChange=true`` adds
* deletes use ``dataChange=true`` removes (tombstones); physical files stay on
  disk so time travel to old snapshots keeps working
* commits never overwrite a concurrent log version (atomic link)
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .delta import (
    TABLE_SCHEMA,
    ConflictError,
    DeltaError,
    InvalidRequestError,
    TableNotFoundError,
    _commit_actions,
    _fsync_dir,
    _new_data_filename,
    _relative_data_path,
    _staging_dir,
    _utc_now_ms,
    load_snapshot,
)


def _stage_and_publish(table: Path, data: pa.Table) -> tuple[str, int]:
    table = Path(table)
    if data.schema != TABLE_SCHEMA:
        raise InvalidRequestError(f"schema mismatch: {data.schema}")
    if data.num_rows > 0 and sum(
        data.column(i).null_count for i in range(data.num_columns)
    ) != 0:
        raise InvalidRequestError("null values are not allowed")
    staging = _staging_dir(table)
    staging.mkdir(parents=True, exist_ok=True)
    stage_path = staging / f"append-{uuid.uuid4().hex}.parquet"
    pq.write_table(data, stage_path, compression="snappy", version="2.6")
    rel = _new_data_filename()
    final_abs = _relative_data_path(table, rel)
    try:
        import os

        os.link(stage_path, final_abs)
    except FileExistsError as exc:  # pragma: no cover
        raise ConflictError(f"data file already exists: {rel}") from exc
    _fsync_dir(final_abs.parent)
    stage_path.unlink(missing_ok=True)
    return rel, final_abs.stat().st_size


def append_rows(table: Path, rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Append *rows* as one new parquet file and commit a dataChange add.

    The commit retries on a log-version race so concurrent writers (the
    compaction service included) never make a plain append fail.
    """
    table = Path(table)
    if not rows:
        raise InvalidRequestError("nothing to append")
    try:
        load_snapshot(table)
    except TableNotFoundError:
        raise
    data = pa.Table.from_pylist(list(rows), schema=TABLE_SCHEMA)
    rel, size = _stage_and_publish(table, data)

    for attempt in range(25):
        now = _utc_now_ms()
        base = load_snapshot(table).version
        actions = [
            {
                "commitInfo": {
                    "timestamp": now,
                    "operation": "WRITE",
                    "operationParameters": {"mode": "Append"},
                    "readVersion": base,
                    "isBlindAppend": True,
                    "engineInfo": "external-writer/1.0",
                    "clientVersion": "external-writer/1.0",
                }
            },
            {
                "add": {
                    "path": rel,
                    "size": size,
                    "modificationTime": now,
                    "dataChange": True,
                    "stats": json.dumps(
                        {"numRecords": data.num_rows}, separators=(",", ":")
                    ),
                }
            },
        ]
        try:
            version = _commit_actions(
                table, base_version=base, actions=actions
            )
            return {"version": version, "file": rel, "rows": data.num_rows}
        except ConflictError:
            time.sleep(0.02 * (attempt + 1))
    raise DeltaError("too many concurrent commits while appending")


def delete_files(table: Path, paths: Sequence[str]) -> dict[str, Any]:
    """Commit tombstones for *paths* (dataChange removes).

    Physical parquet files are deliberately retained so older snapshots remain
    readable; the paths simply stop being active.
    """
    table = Path(table)
    if not paths:
        raise InvalidRequestError("no files given to delete")
    if len(set(paths)) != len(paths):
        raise InvalidRequestError("duplicate paths in delete request")

    # OCC: re-check at commit time that the files are still active.
    for attempt in range(25):
        snap = load_snapshot(table)
        missing = [p for p in paths if p not in snap.active]
        if missing:
            raise InvalidRequestError(
                "files not active in latest snapshot: " + ", ".join(sorted(missing))
            )
        now = _utc_now_ms()
        actions = [
            {
                "commitInfo": {
                    "timestamp": now,
                    "operation": "DELETE",
                    "operationParameters": {"predicate": "[]"},
                    "readVersion": snap.version,
                    "isBlindAppend": False,
                    "engineInfo": "external-writer/1.0",
                    "clientVersion": "external-writer/1.0",
                }
            }
        ]
        actions.extend(
            {
                "remove": {
                    "path": rel,
                    "deletionTimestamp": now,
                    "dataChange": True,
                    "size": snap.active[rel].size_bytes,
                }
            }
            for rel in sorted(paths)
        )
        try:
            version = _commit_actions(table, base_version=snap.version, actions=actions)
            return {
                "version": version,
                "removed_files": sorted(paths),
                "count": len(paths),
            }
        except ConflictError:
            time.sleep(0.02 * (attempt + 1))
    raise DeltaError("too many concurrent commits while deleting")
