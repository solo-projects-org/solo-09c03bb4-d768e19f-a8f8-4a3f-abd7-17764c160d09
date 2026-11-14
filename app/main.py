"""FastAPI service exposing Delta Lake compaction for the water-level table.

Two replicas of this service run in docker-compose against one shared table
directory, which is how the "two processes" requirement is demonstrated:
either process can compact, and the Delta OCC protocol serialises them.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from . import delta
from .sample_data import create_sample_table

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/data"))
TABLE_NAME = os.environ.get("TABLE_NAME", "water_level")
TABLE_DIR = DATA_ROOT / TABLE_NAME


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class CompactRequest(BaseModel):
    version: int | None = Field(
        default=None,
        ge=0,
        description="Base snapshot version. Defaults to the latest committed version.",
    )
    files: list[str] = Field(
        ...,
        min_length=2,
        description=(
            "At least two parquet file paths (relative to the table root) "
            "that are all active at the base version."
        ),
    )


class CompactResponse(BaseModel):
    table: str
    committed_version: int
    base_version: int
    files_added: int
    files_removed: int
    output_file: str
    input_files: list[str]
    rows: int
    input_bytes: int
    output_bytes: int
    attempts: int


class SnapshotResponse(BaseModel):
    table: str
    version: int
    latest_version: int
    protocol: dict[str, int]
    partition_columns: list[str]
    active_file_count: int
    active_files: list[str]
    row_count: int


class VersionsResponse(BaseModel):
    table: str
    latest_version: int
    versions: list[int]


class TableStatsResponse(BaseModel):
    table: str
    path: str
    latest_version: int
    active_file_count: int
    row_count: int
    columns: list[dict]
    staging_orphans_cleaned: list[str]


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    # Idempotent; both replicas may run it concurrently - init + OCC make it safe.
    create_sample_table(TABLE_DIR)
    # Best-effort sweep of files left by previously crashed processes.
    delta.cleanup_staging(TABLE_DIR)
    yield


app = FastAPI(
    title="Delta Lake Compaction API",
    description=(
        "Compact small parquet files of the unpartitioned water-level "
        "observation Delta table (reader protocol v1, writer protocol v2, "
        "no deletion vectors). Compaction commits use add/remove actions "
        "with dataChange=false and preserve every value, duplicate row, "
        "the schema and all old snapshots. Optimistic concurrency control "
        "lets external writers keep committing; the compaction only "
        "replaces its selected files while all of them are still active."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _delta_http(exc: delta.DeltaError) -> HTTPException:
    status = {
        delta.TableNotFoundError: 404,
        delta.VersionNotFoundError: 404,
        delta.FileNotActiveError: 404,
        delta.InvalidRequestError: 400,
        delta.ConflictError: 409,
        delta.TableCorruptError: 500,
    }
    for cls, code in status.items():
        if isinstance(exc, cls):
            return HTTPException(status_code=code, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health", tags=["ops"])
def health() -> dict:
    try:
        versions = delta.list_versions(TABLE_DIR)
        return {"status": "ok", "table": TABLE_NAME, "latest_version": versions[-1]}
    except delta.DeltaError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/compact", response_model=CompactResponse, tags=["compaction"])
def compact(req: CompactRequest) -> CompactResponse:
    """Merge >= 2 active parquet files of a chosen version into one file."""
    try:
        result = delta.compact(TABLE_DIR, req.files, base_version=req.version)
    except delta.DeltaError as exc:
        raise _delta_http(exc)
    return CompactResponse(
        table=TABLE_NAME,
        committed_version=result["version"],
        base_version=result["base_version"],
        files_added=result["added_files"],
        files_removed=result["removed_files"],
        output_file=result["output_file"],
        input_files=result["input_files"],
        rows=result["rows"],
        input_bytes=result["input_bytes"],
        output_bytes=result["output_bytes"],
        attempts=result["attempts"],
    )


@app.get("/table/versions", response_model=VersionsResponse, tags=["table"])
def versions() -> VersionsResponse:
    vs = delta.list_versions(TABLE_DIR)
    return VersionsResponse(table=TABLE_NAME, latest_version=vs[-1], versions=vs)


@app.get("/table/snapshot", response_model=SnapshotResponse, tags=["table"])
def snapshot(version: int | None = None) -> SnapshotResponse:
    """Describe a snapshot; pass ?version=N for time travel."""
    try:
        snap = delta.load_snapshot(TABLE_DIR, version)
        latest = delta.list_versions(TABLE_DIR)[-1]
        table = delta.read_table(TABLE_DIR, snap.version)
    except delta.DeltaError as exc:
        raise _delta_http(exc)
    return SnapshotResponse(
        table=TABLE_NAME,
        version=snap.version,
        latest_version=latest,
        protocol={
            "minReaderVersion": snap.protocol[0],
            "minWriterVersion": snap.protocol[1],
        },
        partition_columns=list(snap.metadata.get("partitionColumns", [])),
        active_file_count=len(snap.active),
        active_files=sorted(snap.active),
        row_count=table.num_rows,
    )


@app.get("/table/stats", response_model=TableStatsResponse, tags=["ops"])
def stats() -> TableStatsResponse:
    snap = delta.load_snapshot(TABLE_DIR)
    table = delta.read_table(TABLE_DIR)
    cleaned = delta.cleanup_staging(TABLE_DIR)
    return TableStatsResponse(
        table=TABLE_NAME,
        path=str(TABLE_DIR),
        latest_version=snap.version,
        active_file_count=len(snap.active),
        row_count=table.num_rows,
        columns=[
            {"name": f.name, "type": str(f.type), "nullable": f.nullable}
            for f in table.schema
        ],
        staging_orphans_cleaned=cleaned,
    )
