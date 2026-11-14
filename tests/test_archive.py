"""End-to-end tests for per-version archives (independent Delta copies)."""

from __future__ import annotations

import multiprocessing
import os
from pathlib import Path

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from app import archive, delta
from app.external_writer import append_rows, delete_files
from app.sample_data import create_sample_table


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def table(tmp_path: Path) -> Path:
    t = tmp_path / "water_level"
    create_sample_table(t)
    return t


@pytest.fixture()
def data_root(table: Path) -> Path:
    return table.parent


def _row_tuples(data: pa.Table) -> list[tuple]:
    return sorted(tuple(r.values()) for r in data.to_pylist())


# ---------------------------------------------------------------------------
# Archive content correctness
# ---------------------------------------------------------------------------


def test_archive_copies_rows_duplicates_schema_and_protocol(
    table: Path, data_root: Path
) -> None:
    # Two extra files with explicit duplicate rows: duplicates must survive.
    dup = [
        {"station": "ST-DUP", "utc_seconds": 100, "level_mm": 5},
        {"station": "ST-DUP", "utc_seconds": 100, "level_mm": 5},
    ]
    append_rows(table, dup)
    append_rows(table, list(dup))
    version = delta.load_snapshot(table).version
    before = delta.read_table(table, version=version)

    result = archive.create_archive(data_root, table, "wl-full", version)

    assert result["created"] is True
    assert result["source_version"] == version
    assert result["row_count"] == before.num_rows == 28
    assert result["file_count"] == 6

    adir = Path(result["path"])
    assert adir.is_dir()
    # The archive is a self-contained Delta table: contiguous log from 0,
    # protocol and schema copied from the source version.
    assert delta.list_versions(adir) == [0, 1]
    for v in (0, 1):
        for action in delta.read_log(adir, v):
            assert set(action) <= {"protocol", "metaData", "add", "remove", "commitInfo"}
    snap = delta.load_snapshot(adir)
    assert snap.protocol == (1, 2)
    assert snap.metadata["schemaString"] == delta.load_snapshot(table).metadata[
        "schemaString"
    ]
    assert snap.metadata["partitionColumns"] == []
    assert snap.metadata["configuration"]["archive.sourceTable"] == "water_level"
    assert snap.metadata["configuration"]["archive.sourceVersion"] == str(version)

    # Read-back: identical rows, duplicates included, identical schema.
    after = delta.read_table(adir)
    assert after.schema == before.schema
    assert _row_tuples(after) == _row_tuples(before)
    assert [tuple(r.values()) for r in after.to_pylist()].count(("ST-DUP", 100, 5)) == 4

    # Physical copy: same bytes on disk, source files untouched.
    for rel in snap.active:
        assert (adir / rel).read_bytes() == (table / rel).read_bytes()


def test_archive_of_empty_version_zero(table: Path, data_root: Path) -> None:
    result = archive.create_archive(data_root, table, "wl-v0", 0)
    assert result["row_count"] == 0 and result["file_count"] == 0
    adir = Path(result["path"])
    assert delta.list_versions(adir) == [0, 1]
    assert delta.read_table(adir).num_rows == 0


def test_archive_of_old_version_after_source_compaction(
    table: Path, data_root: Path
) -> None:
    old_rows = _row_tuples(delta.read_table(table, version=2))
    chosen = sorted(delta.load_snapshot(table).active)[:2]
    delta.compact(table, chosen)  # source table moves on to v5

    result = archive.create_archive(data_root, table, "wl-v2", 2)

    assert result["source_version"] == 2
    assert result["row_count"] == len(old_rows) == 12
    got = _row_tuples(delta.read_table(Path(result["path"])))
    assert got == old_rows


def test_archive_unaffected_by_source_append_delete_compact(
    table: Path, data_root: Path
) -> None:
    result = archive.create_archive(data_root, table, "wl-base", 4)
    adir = Path(result["path"])
    archived_rows = _row_tuples(delta.read_table(adir))
    archived_files = sorted(delta.load_snapshot(adir).active)

    # Mutate the source table in every supported way.
    append_rows(table, [{"station": "ST-NEW", "utc_seconds": 1, "level_mm": 1}])
    active = sorted(delta.load_snapshot(table).active)
    delta.compact(table, active[:2])
    delete_files(table, [active[2]])

    # The archive's file set, rows and reported stats are unchanged.
    assert sorted(delta.load_snapshot(adir).active) == archived_files
    assert _row_tuples(delta.read_table(adir)) == archived_rows
    info = archive.get_archive_info(data_root, "wl-base")
    assert info["source_version"] == 4
    assert info["row_count"] == 24
    assert info["file_count"] == 4


# ---------------------------------------------------------------------------
# Idempotency / conflicts / validation
# ---------------------------------------------------------------------------


def test_archive_same_name_same_version_is_idempotent(
    table: Path, data_root: Path
) -> None:
    r1 = archive.create_archive(data_root, table, "wl", 4)
    r2 = archive.create_archive(data_root, table, "wl", 4)
    assert r1["created"] is True
    assert r2["created"] is False
    assert r1["row_count"] == r2["row_count"] == 24
    assert r1["path"] == r2["path"]
    # The retry did not rewrite the published archive.
    assert delta.list_versions(Path(r1["path"])) == [0, 1]
    entries = [
        p.name for p in (data_root / "archives").iterdir() if not p.name.startswith(".")
    ]
    assert entries == ["wl"]


def test_archive_same_name_different_version_conflicts(
    table: Path, data_root: Path
) -> None:
    archive.create_archive(data_root, table, "wl", 4)
    with pytest.raises(delta.ConflictError):
        archive.create_archive(data_root, table, "wl", 2)


def test_archive_rejects_invalid_names(table: Path, data_root: Path) -> None:
    for bad in ["", "../escape", "a/b", ".hidden", "..", "a b", "x" * 129, "/abs"]:
        with pytest.raises(delta.InvalidRequestError):
            archive.create_archive(data_root, table, bad, 4)
    with pytest.raises(delta.InvalidRequestError):
        archive.get_archive_info(data_root, "../escape")


def test_archive_version_must_exist(table: Path, data_root: Path) -> None:
    with pytest.raises(delta.VersionNotFoundError):
        archive.create_archive(data_root, table, "wl", 99)


def test_get_unknown_archive_raises(data_root: Path) -> None:
    with pytest.raises(archive.ArchiveNotFoundError):
        archive.get_archive_info(data_root, "nope")
    assert archive.list_archives(data_root) == []


def test_multiple_archives_coexist(table: Path, data_root: Path) -> None:
    archive.create_archive(data_root, table, "wl-v2", 2)
    archive.create_archive(data_root, table, "wl-v4", 4)
    listing = archive.list_archives(data_root)
    assert [a["archive"] for a in listing] == ["wl-v2", "wl-v4"]
    assert listing[0]["source_version"] == 2 and listing[0]["row_count"] == 12
    assert listing[1]["source_version"] == 4 and listing[1]["row_count"] == 24


# ---------------------------------------------------------------------------
# Concurrency: two replicas racing the same archive name
# ---------------------------------------------------------------------------


def _worker_archive(
    data_root: str, table: str, name: str, version: int, q
) -> None:  # pragma: no cover - runs in a child process
    try:
        r = archive.create_archive(Path(data_root), Path(table), name, version)
        q.put(("ok", r["created"], r["row_count"]))
    except delta.ConflictError as exc:
        q.put(("conflict", str(exc)))
    except Exception as exc:  # noqa: BLE001
        q.put(("error", repr(exc)))


def _race(data_root: Path, table: Path, name: str, v1: int, v2: int) -> list[tuple]:
    ctx = multiprocessing.get_context("spawn")
    q1, q2 = ctx.Queue(), ctx.Queue()
    p1 = ctx.Process(target=_worker_archive, args=(str(data_root), str(table), name, v1, q1))
    p2 = ctx.Process(target=_worker_archive, args=(str(data_root), str(table), name, v2, q2))
    p1.start(); p2.start()
    p1.join(60); p2.join(60)
    assert p1.exitcode == 0 and p2.exitcode == 0
    return [q1.get(), q2.get()]


def test_concurrent_same_name_same_version_publishes_exactly_once(
    table: Path, data_root: Path
) -> None:
    r1, r2 = _race(data_root, table, "wl", 4, 4)
    assert r1[0] == "ok" and r2[0] == "ok"
    # Exactly one process published; the twin replayed the finished archive.
    assert sorted([r1[1], r2[1]]) == [False, True]
    assert r1[2] == r2[2] == 24

    root = data_root / "archives"
    entries = [p.name for p in root.iterdir() if not p.name.startswith(".")]
    assert entries == ["wl"]
    assert delta.read_table(root / "wl").num_rows == 24
    # No staging directories left behind by either process.
    assert not [p for p in root.iterdir() if p.name.startswith(".tmp-")]


def test_concurrent_same_name_different_version_single_winner(
    table: Path, data_root: Path
) -> None:
    r1, r2 = _race(data_root, table, "wl", 4, 2)
    statuses = sorted([r1[0], r2[0]])
    assert statuses == ["conflict", "ok"]

    info = archive.get_archive_info(data_root, "wl")
    assert info["source_version"] in (2, 4)
    assert info["row_count"] == (24 if info["source_version"] == 4 else 12)
    # The published archive is complete and readable.
    assert delta.read_table(data_root / "archives" / "wl").num_rows == info["row_count"]


# ---------------------------------------------------------------------------
# Crash / durability behaviour
# ---------------------------------------------------------------------------


def test_fault_after_data_copy_leaves_nothing_queryable(
    table: Path, data_root: Path
) -> None:
    with pytest.raises(RuntimeError):
        archive.create_archive(data_root, table, "wl", 4, fault_inject="after_data_copy")

    root = data_root / "archives"
    # Nothing was published: the name is absent and unqueryable.
    assert not (root / "wl").exists()
    with pytest.raises(archive.ArchiveNotFoundError):
        archive.get_archive_info(data_root, "wl")
    assert archive.list_archives(data_root) == []
    # The half-built copy is confined to a hidden staging directory.
    leftovers = [p for p in root.iterdir() if p.name.startswith(".tmp-")]
    assert leftovers

    # Same-name + same-version retry completes the job.
    result = archive.create_archive(data_root, table, "wl", 4)
    assert result["created"] is True
    assert result["row_count"] == 24
    assert archive.get_archive_info(data_root, "wl")["row_count"] == 24

    # Stale staging directories are reaped by the janitor.
    removed = archive.cleanup_archive_staging(data_root, older_than_seconds=0)
    assert any(name.startswith(".tmp-") for name in removed)
    assert not [p for p in root.iterdir() if p.name.startswith(".tmp-")]


def test_cleanup_archive_staging_respects_age(table: Path, data_root: Path) -> None:
    root = data_root / "archives"
    root.mkdir(parents=True)
    old = root / ".tmp-old"
    old.mkdir()
    (old / "junk.parquet").write_text("x")
    os.utime(old, (0, 0))  # ancient mtime
    fresh = root / ".tmp-fresh"
    fresh.mkdir()

    removed = archive.cleanup_archive_staging(data_root, older_than_seconds=3600)
    assert removed == [".tmp-old"]
    assert not old.exists() and fresh.exists()


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(tmp_path: Path, monkeypatch) -> TestClient:
    data_root = tmp_path / "data"
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    monkeypatch.setenv("TABLE_NAME", "water_level")
    import app.main as main

    monkeypatch.setattr(main, "DATA_ROOT", data_root)
    monkeypatch.setattr(main, "TABLE_DIR", data_root / "water_level")
    with TestClient(main.app) as c:
        yield c


def test_api_archive_lifecycle(client: TestClient) -> None:
    # Publish an archive of the pinned version.
    r = client.post("/archives", json={"name": "wl-v4", "version": 4})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["created"] is True
    assert body["source_table"] == "water_level"
    assert body["source_version"] == 4
    assert body["row_count"] == 24
    assert body["file_count"] == 4

    # Same name + same version: idempotent replay of the finished archive.
    r = client.post("/archives", json={"name": "wl-v4", "version": 4})
    assert r.status_code == 200
    assert r.json()["created"] is False
    assert r.json()["row_count"] == 24

    # Query the archive's source version and row count by name.
    r = client.get("/archives/wl-v4")
    assert r.status_code == 200
    info = r.json()
    assert info["source_version"] == 4
    assert info["row_count"] == 24
    assert info["file_count"] == 4

    # Listing shows the published archive.
    r = client.get("/archives")
    assert r.status_code == 200
    assert [a["archive"] for a in r.json()["archives"]] == ["wl-v4"]

    # Same name + different version: conflict.
    r = client.post("/archives", json={"name": "wl-v4", "version": 2})
    assert r.status_code == 409

    # Unknown source version.
    r = client.post("/archives", json={"name": "other", "version": 99})
    assert r.status_code == 404

    # Unknown archive name.
    r = client.get("/archives/nope")
    assert r.status_code == 404

    # Illegal archive names are rejected.
    r = client.post("/archives", json={"name": "../escape", "version": 4})
    assert r.status_code == 422
    r = client.post("/archives", json={"name": "", "version": 4})
    assert r.status_code == 422


def test_api_archive_survives_source_compaction(client: TestClient) -> None:
    r = client.post("/archives", json={"name": "wl", "version": 4})
    assert r.status_code == 201

    files = client.get("/table/snapshot").json()["active_files"][:2]
    r = client.post("/compact", json={"files": files})
    assert r.status_code == 200

    # The source table moved on; the archive still reports version 4.
    assert client.get("/table/snapshot").json()["version"] == 5
    info = client.get("/archives/wl").json()
    assert info["source_version"] == 4
    assert info["row_count"] == 24
    assert info["file_count"] == 4
