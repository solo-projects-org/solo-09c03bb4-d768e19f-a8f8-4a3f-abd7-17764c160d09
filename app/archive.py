"""Version archiving: publish an independent Delta copy of one table version.

The water-level modelling group recomputes offline against a pinned version
of the observation table.  This module copies everything that version is
made of - the active parquet files, the protocol and the table metadata -
into a *separate* Delta table under ``<DATA_ROOT>/archives/<name>`` so that
later appends, deletes or compactions on the source table cannot change
what the offline recompute reads.

Guarantees:

* atomic publish: the archive is built in a hidden ``.tmp-*`` sibling
  directory and appears under its final name via a single ``os.rename``.
  Readers only ever see a complete archive - an interrupted copy or a disk
  write failure never leaves a half-finished archive queryable (abandoned
  staging directories are reaped by :func:`cleanup_archive_staging`).
* single publisher: two replicas racing the same archive name serialise on
  the atomic rename; the loser replays the winner's result.  Exactly one
  complete archive is ever published per name.
* idempotent retry: same name + same source version returns the already
  published archive; same name + different source version is a conflict.
* independence: parquet files are physically copied byte for byte (values
  and duplicate rows included), never linked or moved out of the source
  table, and the archive gets its own transaction log.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from . import delta

ARCHIVES_DIR_NAME = "archives"

# Flat path segments only; dot-prefixed names are reserved for staging dirs.
_ARCHIVE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


class ArchiveNotFoundError(delta.DeltaError):
    """The named archive does not exist (HTTP 404)."""


def _archives_root(data_root: Path) -> Path:
    return Path(data_root) / ARCHIVES_DIR_NAME


def validate_archive_name(name: str) -> str:
    """Reject anything that is not a single safe path segment."""
    if not isinstance(name, str) or not _ARCHIVE_NAME.fullmatch(name):
        raise delta.InvalidRequestError(
            "archive name must be 1-128 characters from [A-Za-z0-9._-] "
            "and start with a letter or digit"
        )
    return name


# ---------------------------------------------------------------------------
# Copy helpers
# ---------------------------------------------------------------------------


def _copy_file_atomic(src: Path, dst: Path) -> int:
    """Byte-copy *src* to *dst* (fsynced) and return the copied size.

    The copy lands under a temporary name first and is renamed into place,
    so even inside the staging directory a crash never leaves a half-written
    file under a data-file name.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    partial = dst.parent / f".partial-{uuid.uuid4().hex}"
    try:
        with src.open("rb") as fin, partial.open("wb") as fout:
            shutil.copyfileobj(fin, fout, length=1024 * 1024)
            fout.flush()
            os.fsync(fout.fileno())
        os.rename(partial, dst)
    except BaseException:
        try:
            partial.unlink(missing_ok=True)
        except OSError:  # pragma: no cover - best effort cleanup
            pass
        raise
    delta._fsync_dir(dst.parent)
    return dst.stat().st_size


def _archive_info_from_dir(archive_dir: Path, name: str) -> dict[str, Any]:
    """Describe a published archive: source version, row count, file count.

    Row counts come from parquet footers (exact, duplicates included) so
    querying an archive never has to materialise its data.
    """
    snap = delta.load_snapshot(archive_dir)
    config = snap.metadata.get("configuration") or {}
    source_version = config.get("archive.sourceVersion")
    row_count = 0
    for rel in snap.active:
        meta = pq.read_metadata(delta._relative_data_path(archive_dir, rel))
        row_count += meta.num_rows
    return {
        "archive": name,
        "source_table": config.get("archive.sourceTable"),
        "source_version": int(source_version) if source_version is not None else None,
        "row_count": row_count,
        "file_count": len(snap.active),
        "archive_version": snap.version,
        "path": str(archive_dir),
    }


def _reuse_or_conflict(final_dir: Path, name: str, version: int) -> dict[str, Any]:
    """Idempotent replay for an already published archive, else 409."""
    existing = _archive_info_from_dir(final_dir, name)
    if existing["source_version"] == version:
        existing["created"] = False
        return existing
    raise delta.ConflictError(
        f"archive {name!r} is already published from source version "
        f"{existing['source_version']} (requested {version})"
    )


# ---------------------------------------------------------------------------
# Archive creation
# ---------------------------------------------------------------------------


def create_archive(
    data_root: Path,
    table_dir: Path,
    name: str,
    version: int,
    *,
    fault_inject: str | None = None,
) -> dict[str, Any]:
    """Publish *table_dir* @ *version* as an independent archive named *name*.

    The whole archive (copied parquet files + its own Delta transaction log
    carrying the protocol and schema of the source version) is assembled in
    a hidden staging directory and published with one atomic rename, so a
    concurrent reader either sees the complete archive or nothing at all.
    """
    validate_archive_name(name)
    if not isinstance(version, int) or isinstance(version, bool) or version < 0:
        raise delta.InvalidRequestError(f"invalid source version: {version!r}")
    table_dir = Path(table_dir)
    snapshot = delta.load_snapshot(table_dir, version)  # 404 when missing
    source_table = snapshot.metadata.get("name") or table_dir.name

    data_root = Path(data_root)
    root = _archives_root(data_root)
    final_dir = root / name
    if final_dir.is_dir():
        return _reuse_or_conflict(final_dir, name, version)

    # Best-effort sweep of staging dirs left by previously crashed processes
    # (only entries older than the safety threshold are touched).
    cleanup_archive_staging(data_root)

    tmp_dir = root / f".tmp-{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=False)
    try:
        # 1. Physically copy every active parquet file of the pinned version.
        #    Source files are immutable and never deleted, so the copy is a
        #    stable, byte-identical image of the version (duplicates kept).
        file_stats: list[tuple[str, int, int]] = []
        total_rows = 0
        total_bytes = 0
        for rel in sorted(snapshot.active):
            src = delta._relative_data_path(table_dir, rel)
            if not src.is_file():
                raise delta.TableCorruptError(
                    f"committed data file is missing on disk: {rel}"
                )
            dst = delta._relative_data_path(tmp_dir, rel)
            size = _copy_file_atomic(src, dst)
            rows = pq.read_metadata(src).num_rows
            file_stats.append((rel, size, rows))
            total_rows += rows
            total_bytes += size

        if fault_inject == "after_data_copy":
            # Test hook: data files copied, no log written, no rename.  The
            # staging directory remains on disk exactly as after a crash.
            raise RuntimeError("fault injection: killed after data copy")

        # 2. Write the archive's own Delta log: v0 = protocol + metaData
        #    (copied from the source version), v1 = the archived files.
        now = delta._utc_now_ms()
        delta._commit_actions(
            tmp_dir,
            base_version=-1,
            next_version=0,
            actions=[
                {
                    "protocol": {
                        "minReaderVersion": snapshot.protocol[0],
                        "minWriterVersion": snapshot.protocol[1],
                    }
                },
                {
                    "metaData": {
                        "id": str(uuid.uuid4()),
                        "name": name,
                        "description": (
                            f"Archive of {source_table} version {version} "
                            "for offline recompute"
                        ),
                        "format": {"provider": "parquet", "options": {}},
                        "schemaString": snapshot.metadata["schemaString"],
                        "partitionColumns": snapshot.metadata.get(
                            "partitionColumns", []
                        ),
                        "configuration": {
                            "archive.sourceTable": source_table,
                            "archive.sourceVersion": str(version),
                        },
                        "createdTime": now,
                    }
                },
                {
                    "commitInfo": {
                        "timestamp": now,
                        "operation": "CREATE TABLE",
                        "operationParameters": {
                            "provider": "parquet",
                            "location": name,
                        },
                        "isBlindAppend": True,
                        "engineInfo": "delta-compaction-api/1.0",
                        "clientVersion": "delta-compaction-api/1.0",
                    }
                },
            ],
        )
        add_actions = [
            {
                "add": {
                    "path": rel,
                    "size": size,
                    "modificationTime": now,
                    "dataChange": True,
                    "stats": json.dumps(
                        {"numRecords": rows}, separators=(",", ":")
                    ),
                    "tags": {"archive": "true"},
                }
            }
            for rel, size, rows in file_stats
        ]
        delta._commit_actions(
            tmp_dir,
            base_version=0,
            next_version=1,
            actions=[
                {
                    "commitInfo": {
                        "timestamp": now,
                        "operation": "ARCHIVE",
                        "operationParameters": {
                            "sourceTable": source_table,
                            "sourceVersion": str(version),
                        },
                        "readVersion": version,
                        "isBlindAppend": True,
                        "engineInfo": "delta-compaction-api/1.0",
                        "clientVersion": "delta-compaction-api/1.0",
                        "metrics": {
                            "numFiles": len(file_stats),
                            "numOutputRows": total_rows,
                            "numOutputBytes": total_bytes,
                        },
                    }
                },
                *add_actions,
            ],
        )
        delta._fsync_dir(tmp_dir)

        # 3. Atomic publish: exactly one racing process can win this rename.
        try:
            os.rename(tmp_dir, final_dir)
        except OSError:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            if final_dir.is_dir():
                # Lost the race to a twin replica: replay its result when it
                # archived the same version, otherwise report the conflict.
                return _reuse_or_conflict(final_dir, name, version)
            raise delta.DeltaError(
                f"failed to publish archive {name!r}: atomic rename did not complete"
            )
        delta._fsync_dir(root)
        return {
            "archive": name,
            "source_table": source_table,
            "source_version": version,
            "row_count": total_rows,
            "file_count": len(file_stats),
            "archive_version": 1,
            "path": str(final_dir),
            "created": True,
        }
    except RuntimeError:
        raise  # simulated crash: leave the staging directory untouched
    except delta.DeltaError:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    except (OSError, pa.ArrowInvalid) as exc:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise delta.DeltaError(f"failed to create archive {name!r}: {exc}") from exc


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def get_archive_info(data_root: Path, name: str) -> dict[str, Any]:
    """Look up one published archive by name (404 when absent)."""
    validate_archive_name(name)
    final_dir = _archives_root(Path(data_root)) / name
    if not final_dir.is_dir():
        raise ArchiveNotFoundError(f"archive not found: {name}")
    return _archive_info_from_dir(final_dir, name)


def list_archives(data_root: Path) -> list[dict[str, Any]]:
    """List every published archive; staging directories are never shown."""
    root = _archives_root(Path(data_root))
    if not root.is_dir():
        return []
    archives: list[dict[str, Any]] = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        try:
            archives.append(_archive_info_from_dir(entry, entry.name))
        except delta.DeltaError:
            continue  # not a (complete) archive directory
    return archives


def cleanup_archive_staging(
    data_root: Path, *, older_than_seconds: int = 3600
) -> list[str]:
    """Remove staging directories abandoned by crashed archive attempts.

    Only entries older than the threshold are touched, so the staging
    directory of an in-flight archival in another process is never
    disturbed.
    """
    root = _archives_root(Path(data_root))
    removed: list[str] = []
    if not root.is_dir():
        return removed
    cutoff = time.time() - older_than_seconds
    for entry in root.iterdir():
        if not (entry.is_dir() and entry.name.startswith(".tmp-")):
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry)
                removed.append(entry.name)
        except OSError:  # pragma: no cover - best effort cleanup
            continue
    return removed
