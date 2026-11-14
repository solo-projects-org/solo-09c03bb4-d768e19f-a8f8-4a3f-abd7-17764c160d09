"""Minimal Delta Lake implementation (reader protocol v1 / writer protocol v2).

Only the subset required by the water-level compaction service is implemented:

* JSON (line-delimited) transaction logs numbered contiguously from 0
* ``protocol`` (1, 2), ``metaData`` (un-partitioned), ``add`` / ``remove``
  actions and ``commitInfo``
* snapshot replay with time travel
* optimistic concurrency control: an atomic ``link`` + unlink makes committing
  a new log version race free, conflicts are detected by re-reading the log
* immutable data files, unique non-reused paths, staging files outside any
  committed snapshot

There is deliberately no dependency on the ``delta`` / ``deltalake`` package:
the protocol pieces used here are small enough to implement directly, which
also keeps the concurrency rules explicit and testable.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

READER_VERSION = 1
WRITER_VERSION = 2

# The table only ever stores these three columns; all non-null, int64 for the
# numeric ones.  Schema evolution is out of scope for this service.
TABLE_SCHEMA = pa.schema(
    [
        pa.field("station", pa.string(), nullable=False),
        pa.field("utc_seconds", pa.int64(), nullable=False),
        pa.field("level_mm", pa.int64(), nullable=False),
    ]
)

# Delta "struct" JSON schema embedded in the metaData action.
_DELTA_SCHEMA: dict[str, Any] = {
    "type": "struct",
    "fields": [
        {"name": "station", "type": "string", "nullable": False, "metadata": {}},
        {"name": "utc_seconds", "type": "long", "nullable": False, "metadata": {}},
        {"name": "level_mm", "type": "long", "nullable": False, "metadata": {}},
    ],
    "metadata": {},
}

STAGING_DIR = "_staging"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DeltaError(Exception):
    """Base class for every Delta protocol error raised by this module."""


class TableNotFoundError(DeltaError):
    """The directory is not an initialised Delta table."""


class TableCorruptError(DeltaError):
    """The transaction log is missing, gappy or otherwise unreadable."""


class InvalidRequestError(DeltaError):
    """Caller asked for something the protocol/API does not allow (HTTP 400)."""


class VersionNotFoundError(DeltaError):
    """Requested base version does not exist (HTTP 404)."""


class FileNotActiveError(DeltaError):
    """A requested parquet path is not active in the snapshot (HTTP 404)."""


class ConflictError(DeltaError):
    """Concurrent commit invalidated this transaction (HTTP 409)."""


# ---------------------------------------------------------------------------
# Snapshot data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotEntry:
    path: str
    size_bytes: int
    modification_time: int


@dataclass
class Snapshot:
    version: int
    active: dict[str, SnapshotEntry]
    schema_json: dict[str, Any]
    protocol: tuple[int, int]
    metadata: dict[str, Any]
    # Ordered action history of this version only is not retained; callers
    # needing it replay via read_log().


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _log_dir(table: Path) -> Path:
    return table / "_delta_log"


def log_path(table: Path, version: int) -> Path:
    return _log_dir(table) / f"{version:020d}.json"


def _staging_dir(table: Path) -> Path:
    return table / STAGING_DIR


def _utc_now_ms() -> int:
    return int(time.time() * 1000)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _fsync_dir(path: Path) -> None:
    """fsync a directory so a rename/unlink is durable (POSIX)."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _new_data_filename() -> str:
    # UUID4 + timestamp makes every name globally unique; paths are never
    # reused, so a removed file can never be "resurrected".
    return f"{time.time_ns()}-{uuid.uuid4().hex}.parquet"


def _relative_data_path(table: Path, path: str) -> Path:
    """Resolve *path* inside the table and reject any escape / absolute path."""
    if not path or "\x00" in path:
        raise InvalidRequestError("empty or illegal file path")
    if Path(path).is_absolute():
        raise InvalidRequestError(f"absolute paths are not allowed: {path}")
    table_resolved = table.resolve()
    candidate = (table / path).resolve()
    try:
        candidate.relative_to(table_resolved)
    except ValueError as exc:  # pragma: no cover - defensive
        raise InvalidRequestError(f"path escapes the table directory: {path}") from exc
    if candidate.name.startswith(".") or STAGING_DIR in candidate.parts:
        raise InvalidRequestError(f"not a table data file: {path}")
    return candidate


def list_versions(table: Path) -> list[int]:
    """Return every committed log version, verifying contiguous numbering."""
    log_dir = _log_dir(table)
    if not log_dir.is_dir():
        raise TableNotFoundError(f"no Delta transaction log under {table}")
    versions: list[int] = []
    for entry in sorted(log_dir.glob("*.json")):
        # Only 20-digit zero-padded names are log segments; dotfiles such as
        # the atomic-commit .tmp-* files (visible while another process is
        # committing) must be ignored rather than treated as corruption.
        stem = entry.stem
        if not (len(stem) == 20 and stem.isdigit()):
            continue
        versions.append(int(stem))
    if not versions:
        raise TableCorruptError("transaction log directory contains no JSON files")
    expected = list(range(versions[0], versions[-1] + 1))
    if versions[0] != 0 or versions != expected:
        raise TableCorruptError(
            f"transaction log versions must start at 0 and be contiguous, "
            f"found {versions[:1]}..{versions[-1:]}"
        )
    return versions


def read_log(table: Path, version: int) -> list[dict[str, Any]]:
    """Parse one JSON log file (one JSON object per line)."""
    path = log_path(table, version)
    if not path.is_file():
        raise VersionNotFoundError(f"version {version} does not exist")
    actions: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    actions.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise TableCorruptError(
                        f"invalid JSON on {path.name}:{line_no}: {exc.msg}"
                    ) from exc
    except OSError as exc:
        raise TableCorruptError(f"cannot read {path}: {exc}") from exc
    return actions


# ---------------------------------------------------------------------------
# Snapshot replay
# ---------------------------------------------------------------------------


def load_snapshot(table: Path, version: int | None = None) -> Snapshot:
    """Replay the log up to *version* (default: latest) into a snapshot."""
    versions = list_versions(table)
    if version is None:
        version = versions[-1]
    elif version not in versions:
        raise VersionNotFoundError(f"version {version} does not exist")

    active: dict[str, SnapshotEntry] = {}
    schema_json: dict[str, Any] | None = None
    protocol: tuple[int, int] | None = None
    metadata: dict[str, Any] = {}

    for v in range(0, version + 1):
        for action in read_log(table, v):
            if "add" in action:
                add = action["add"]
                active[add["path"]] = SnapshotEntry(
                    path=add["path"],
                    size_bytes=int(add.get("size", 0)),
                    modification_time=int(
                        add.get("modificationTime", _utc_now_ms())
                    ),
                )
            elif "remove" in action:
                active.pop(action["remove"]["path"], None)
            elif "metaData" in action:
                md = action["metaData"]
                schema_json = md.get("schemaString")
                if isinstance(schema_json, str):
                    schema_json = json.loads(schema_json)
                metadata = md
            elif "protocol" in action:
                p = action["protocol"]
                protocol = (int(p["minReaderVersion"]), int(p["minWriterVersion"]))

    if protocol is None:
        raise TableCorruptError(f"no protocol action found up to version {version}")
    if schema_json is None:
        raise TableCorruptError(f"no metaData action found up to version {version}")
    return Snapshot(
        version=version,
        active=active,
        schema_json=schema_json,
        protocol=protocol,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Table initialisation
# ---------------------------------------------------------------------------


def init_table(table: Path, table_id: str | None = None) -> None:
    """Create an empty Delta table at protocol (1, 2) if it does not exist.

    Safe to call concurrently from several processes: whichever process loses
    the race for log version 0 simply verifies the winner's table and returns.
    """
    table = Path(table)
    log_dir = _log_dir(table)
    if log_path(table, 0).exists():
        return
    table.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    actions = [
        {
            "protocol": {
                "minReaderVersion": READER_VERSION,
                "minWriterVersion": WRITER_VERSION,
            }
        },
        {
            "metaData": {
                "id": table_id or str(uuid.uuid4()),
                "name": "water_level",
                "description": "Station water level observations (compaction demo)",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(_DELTA_SCHEMA),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": _utc_now_ms(),
            }
        },
        {
            "commitInfo": {
                "timestamp": _utc_now_ms(),
                "operation": "CREATE TABLE",
                "operationParameters": {"provider": "parquet", "location": str(table)},
                "isBlindAppend": True,
                "engineInfo": "delta-compaction-api/1.0",
                "clientVersion": "delta-compaction-api/1.0",
            }
        },
    ]
    try:
        _commit_actions(table, base_version=-1, actions=actions, next_version=0)
    except ConflictError:
        # Another process initialised the table first; make sure it is valid.
        load_snapshot(table, 0)


# ---------------------------------------------------------------------------
# Atomic commit
# ---------------------------------------------------------------------------


def _write_parquet_atomic(
    table: Path,
    table_out: pa.Table,
    *,
    staging_name: str,
    final_rel_path: str,
) -> tuple[str, int]:
    """Write a parquet file to staging, then hard-link it to its final name.

    The final file appears atomically.  Until the link call succeeds the file
    lives under ``_staging`` and cannot be part of any snapshot.
    """
    staging = _staging_dir(table)
    staging.mkdir(parents=True, exist_ok=True)
    stage_path = staging / staging_name
    # Fresh unique name -> never overwrite anything.
    pq.write_table(table_out, stage_path, compression="snappy", version="2.6")
    final_abs = _relative_data_path(table, final_rel_path)
    final_abs.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(stage_path, final_abs)
    except FileExistsError as exc:  # pragma: no cover - names are UUID based
        stage_path.unlink(missing_ok=True)
        raise ConflictError(f"data file already exists: {final_rel_path}") from exc
    # The final path now owns the inode; drop the staging link so successful
    # transactions leave no orphans (a crash before this line leaves one that
    # cleanup_staging() will reap later).
    stage_path.unlink(missing_ok=True)
    _fsync_dir(final_abs.parent)
    size = final_abs.stat().st_size
    return final_rel_path, size


def _commit_actions(
    table: Path,
    *,
    base_version: int,
    actions: Sequence[dict[str, Any]],
    next_version: int | None = None,
) -> int:
    """Atomically append a JSON log file.

    A hard link from a unique temp name to ``NNN.json`` is the atomic
    primitive: two processes can never end up with the same version and a
    committed log is never truncated or overwritten.
    """
    log_dir = _log_dir(table)
    log_dir.mkdir(parents=True, exist_ok=True)
    if next_version is None:
        next_version = base_version + 1
    target = log_path(table, next_version)
    tmp = log_dir / f".tmp-{uuid.uuid4().hex}-{next_version:020d}.json"
    payload = "".join(json.dumps(a, separators=(",", ":")) + "\n" for a in actions)
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    try:
        os.link(tmp, target)
    except FileExistsError as exc:
        tmp.unlink(missing_ok=True)
        raise ConflictError(
            f"log version {next_version} already committed by another writer"
        ) from exc
    tmp.unlink(missing_ok=True)
    _fsync_dir(log_dir)
    return next_version


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------


def _total_null_count(table: pa.Table) -> int:
    return sum(table.column(i).null_count for i in range(table.num_columns))


def _validate_schema(table_out: pa.Table) -> None:
    if table_out.schema != TABLE_SCHEMA:
        raise InvalidRequestError(
            "schema mismatch: expected "
            f"{TABLE_SCHEMA}, got {table_out.schema}"
        )
    if table_out.num_rows > 0 and _total_null_count(table_out) != 0:
        # The contract states all columns are non-null; refuse to compact data
        # that would violate the table contract rather than silently dropping
        # the guarantee.
        raise InvalidRequestError("input parquet contains null values")


def compact(
    table: Path,
    paths: Sequence[str],
    *,
    base_version: int | None = None,
    max_retries: int = 25,
    fault_inject: str | None = None,
) -> dict[str, Any]:
    """Compact *paths* (>= 2 active parquet files at *base_version*) into one.

    Optimistic concurrency control:

    * the set of files to remove is read at the base snapshot
    * on every commit conflict the snapshot is re-read; the transaction only
      succeeds if **all** selected files are still active.  Unrelated adds or
      removes (files outside the selection) never cause a conflict
    * no selected file may have been removed by another transaction: that
      data must not be resurrected, so a :class:`ConflictError` is raised
    """
    table = Path(table)
    if not isinstance(paths, (list, tuple)) or len(paths) < 2:
        raise InvalidRequestError(
            "compaction requires at least two parquet files"
        )
    if len(set(paths)) != len(paths):
        raise InvalidRequestError("duplicate paths in compaction request")

    snapshot = load_snapshot(table, base_version)  # 404 if version missing
    base = snapshot.version

    # All requested paths must be active data files at the base version.
    for rel in paths:
        abs_path = _relative_data_path(table, rel)
        if rel not in snapshot.active:
            raise FileNotActiveError(
                f"{rel} is not an active data file at version {base}"
            )
        if not abs_path.is_file():
            raise TableCorruptError(
                f"committed data file is missing on disk: {rel}"
            )

    # Read everything up front (also validates the parquet files themselves).
    tables: list[pa.Table] = []
    input_bytes = 0
    for rel in paths:
        abs_path = _relative_data_path(table, rel)
        try:
            part = pq.read_table(abs_path)
        except (OSError, pa.ArrowInvalid) as exc:
            raise InvalidRequestError(f"cannot read parquet {rel}: {exc}") from exc
        if part.schema != TABLE_SCHEMA:
            raise InvalidRequestError(
                f"schema of {rel} does not match the table schema"
            )
        if part.num_rows > 0 and _total_null_count(part) != 0:
            raise InvalidRequestError(f"{rel} contains null values")
        tables.append(part)
        input_bytes += abs_path.stat().st_size

    merged = pa.concat_tables(tables, promote_options="none")
    _validate_schema(merged)
    total_rows = merged.num_rows

    # Materialise the compacted file in staging, then publish it atomically.
    # A crash here leaves only an orphan under _staging; no snapshot changes.
    staging_name = f"compact-{uuid.uuid4().hex}.parquet"
    final_rel = _new_data_filename()
    try:
        final_rel, output_size = _write_parquet_atomic(
            table,
            merged,
            staging_name=staging_name,
            final_rel_path=final_rel,
        )
    except OSError as exc:
        # Make sure no half-written staging file leaks as a candidate.
        (table / STAGING_DIR / staging_name).unlink(missing_ok=True)
        raise DeltaError(f"failed to write compacted parquet: {exc}") from exc

    if fault_inject == "after_data_write":
        # Test hook: the data file exists but no commit has happened. Simulates
        # a process killed mid-transaction.
        raise RuntimeError("fault injection: killed after data file write")

    attempts = 0
    while True:
        attempts += 1
        current = load_snapshot(table)
        removed_by_others = [p for p in paths if p not in current.active]
        if removed_by_others:
            # Never resurrect data another transaction deleted: abort and
            # remove the now-useless compacted file.
            _safe_unlink(table / final_rel)
            raise ConflictError(
                "selected file(s) removed by a concurrent commit: "
                + ", ".join(sorted(removed_by_others))
            )

        now = _utc_now_ms()
        add_action = {
            "add": {
                "path": final_rel,
                "size": output_size,
                "modificationTime": now,
                "dataChange": False,
                "stats": json.dumps(
                    {"numRecords": total_rows}, separators=(",", ":")
                ),
                "tags": {"compaction": "true"},
            }
        }
        remove_actions = [
            {
                "remove": {
                    "path": rel,
                    "deletionTimestamp": now,
                    "dataChange": False,
                    "size": snapshot.active[rel].size_bytes,
                }
            }
            for rel in sorted(paths)
        ]
        commit_info = {
            "commitInfo": {
                "timestamp": now,
                "operation": "OPTIMIZE",
                "operationParameters": {"predicate": "[]"},
                "readVersion": base,
                "isBlindAppend": False,
                "engineInfo": "delta-compaction-api/1.0",
                "clientVersion": "delta-compaction-api/1.0",
                "metrics": {
                    "numRemovedFiles": len(paths),
                    "numAddedFiles": 1,
                    "numOutputRows": total_rows,
                    "numOutputBytes": output_size,
                    "inputBytes": input_bytes,
                },
            }
        }
        actions = [commit_info, add_action, *remove_actions]

        try:
            committed_version = _commit_actions(
                table,
                base_version=current.version,
                actions=actions,
            )
        except ConflictError:
            if attempts > max_retries:
                _safe_unlink(table / final_rel)
                raise ConflictError(
                    "too many concurrent commits, compaction aborted"
                )
            time.sleep(0.02 * attempts)
            continue

        return {
            "version": committed_version,
            "base_version": base,
            "added_files": 1,
            "removed_files": len(paths),
            "output_file": final_rel,
            "input_files": sorted(paths),
            "rows": total_rows,
            "input_bytes": input_bytes,
            "output_bytes": output_size,
            "attempts": attempts,
        }


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:  # pragma: no cover - best effort cleanup
        pass


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------


def read_table(
    table: Path, version: int | None = None, columns: Iterable[str] | None = None
) -> pa.Table:
    """Read the full snapshot at *version* (time travel when given)."""
    snap = load_snapshot(table, version)
    if not snap.active:
        return TABLE_SCHEMA.empty_table()
    frames = [
        pq.read_table(_relative_data_path(table, rel), columns=list(columns) if columns else None)
        for rel in sorted(snap.active)
    ]
    return pa.concat_tables(frames, promote_options="none")


def cleanup_staging(table: Path, *, older_than_seconds: int = 3600) -> list[str]:
    """Remove abandoned staging/temp files (e.g. left by a crashed process).

    Only files older than the threshold are touched, so files belonging to an
    in-flight transaction of another process are never disturbed.
    """
    removed: list[str] = []
    cutoff = time.time() - older_than_seconds
    staging = _staging_dir(table)
    if staging.is_dir():
        for entry in staging.iterdir():
            try:
                if entry.is_file() and entry.stat().st_mtime < cutoff:
                    entry.unlink()
                    removed.append(str(entry.relative_to(table)))
            except OSError:
                continue
    log_dir = _log_dir(table)
    if log_dir.is_dir():
        for entry in log_dir.glob(".tmp-*"):
            try:
                if entry.is_file() and entry.stat().st_mtime < cutoff:
                    entry.unlink()
                    removed.append(str(entry.relative_to(table)))
            except OSError:
                continue
    return removed
