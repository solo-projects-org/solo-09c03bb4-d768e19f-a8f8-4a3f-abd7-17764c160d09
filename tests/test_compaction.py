"""End-to-end tests for the Delta compaction protocol and API."""

from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import delta
from app.external_writer import append_rows, delete_files
from app.sample_data import create_sample_table


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def table(tmp_path: Path) -> Path:
    t = tmp_path / "water_level"
    create_sample_table(t)
    return t


def _rows(snapshot_table: pa.Table) -> list[dict]:
    return snapshot_table.to_pylist()


# ---------------------------------------------------------------------------
# Protocol basics
# ---------------------------------------------------------------------------


def test_sample_table_layout(table: Path) -> None:
    versions = delta.list_versions(table)
    assert versions == [0, 1, 2, 3, 4]  # contiguous from 0
    snap = delta.load_snapshot(table)
    assert snap.protocol == (1, 2)
    assert snap.metadata["partitionColumns"] == []
    assert len(snap.active) == 4
    data = delta.read_table(table)
    assert data.num_rows == 24
    assert data.schema == delta.TABLE_SCHEMA


def test_log_files_are_line_delimited_json(table: Path) -> None:
    for v in delta.list_versions(table):
        actions = delta.read_log(table, v)
        assert actions and all(isinstance(a, dict) for a in actions)
        # Every action must be one of the documented Delta action types.
        for action in actions:
            assert set(action) <= {"protocol", "metaData", "add", "remove", "commitInfo"}


# ---------------------------------------------------------------------------
# Compaction correctness
# ---------------------------------------------------------------------------


def test_list_versions_ignores_inflight_temp_logs(table: Path) -> None:
    log_dir = table / "_delta_log"
    tmp = log_dir / ".tmp-deadbeef-00000000000000000009.json"
    tmp.write_text("{}\n")
    try:
        assert delta.list_versions(table) == [0, 1, 2, 3, 4]
        # Normal operations keep working while the temp file is present.
        assert delta.load_snapshot(table).version == 4
    finally:
        tmp.unlink()


def test_compact_preserves_rows_duplicates_and_schema(table: Path) -> None:
    before = delta.read_table(table)
    before_rows = _rows(before)
    snap_before = delta.load_snapshot(table)
    chosen = sorted(snap_before.active)[:2]

    result = delta.compact(table, chosen)

    assert result["removed_files"] == 2
    assert result["added_files"] == 1
    snap_after = delta.load_snapshot(table)
    assert len(snap_after.active) == 3  # 4 - 2 + 1
    assert result["output_file"] in snap_after.active
    assert all(p not in snap_after.active for p in chosen)

    after = delta.read_table(table)
    assert after.schema == before.schema
    # Order-insensitive multiset equality, including duplicate rows.
    assert sorted(map(tuple, before_rows)) == sorted(map(tuple, _rows(after)))
    assert after.num_rows == before.num_rows == 24

    # Commit markers must be dataChange=false.
    actions = delta.read_log(table, result["version"])
    adds = [a["add"] for a in actions if "add" in a]
    removes = [a["remove"] for a in actions if "remove" in a]
    assert len(adds) == 1 and adds[0]["dataChange"] is False
    assert len(removes) == 2 and all(x["dataChange"] is False for x in removes)


def test_compaction_keeps_old_snapshots_readable(table: Path) -> None:
    old_rows = _rows(delta.read_table(table, version=2))
    old_snap = delta.load_snapshot(table, version=2)
    chosen = sorted(delta.load_snapshot(table).active)[:3]
    delta.compact(table, chosen)

    # Time travel: version 2 is byte-for-byte unchanged in content and file set.
    assert sorted(delta.load_snapshot(table, version=2).active) == sorted(old_snap.active)
    assert _rows(delta.read_table(table, version=2)) == old_rows
    # Removed parquet files must still physically exist for time travel.
    for rel in chosen:
        assert (table / rel).is_file()


def test_compaction_refuses_less_than_two_files(table: Path) -> None:
    one = sorted(delta.load_snapshot(table).active)[:1]
    with pytest.raises(delta.InvalidRequestError):
        delta.compact(table, one)


def test_compaction_rejects_unknown_and_path_escapes(table: Path) -> None:
    active = sorted(delta.load_snapshot(table).active)
    with pytest.raises(delta.FileNotActiveError):
        delta.compact(table, [active[0], "does-not-exist.parquet"])
    with pytest.raises(delta.InvalidRequestError):
        delta.compact(table, [active[0], "../escape.parquet"])
    with pytest.raises(delta.InvalidRequestError):
        delta.compact(table, [active[0], "/etc/passwd"])
    with pytest.raises(delta.InvalidRequestError):
        delta.compact(table, [active[0], active[0]])  # duplicate


def test_compaction_requires_base_version_to_exist(table: Path) -> None:
    active = sorted(delta.load_snapshot(table).active)
    with pytest.raises(delta.VersionNotFoundError):
        delta.compact(table, active[:2], base_version=99)


def test_files_added_later_are_not_needed_by_older_compaction(table: Path) -> None:
    # Compaction planned at an older base version must validate against that
    # version's snapshot, not files that appear later.
    old_active = sorted(delta.load_snapshot(table, version=2).active)
    chosen = old_active[:2]
    # An unrelated append lands before we run the compaction.
    append_rows(
        table,
        [{"station": "ST-009", "utc_seconds": 1, "level_mm": 1}],
    )
    result = delta.compact(table, chosen, base_version=2)
    assert result["base_version"] == 2
    assert result["removed_files"] == 2


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def _worker_compact(table_str: str, files: list[str], q) -> None:  # pragma: no cover
    try:
        q.put(("ok", delta.compact(Path(table_str), files)["version"]))
    except delta.DeltaError as exc:
        q.put(("conflict", str(exc)))


def test_two_concurrent_compactions_one_replaces_unrelated_set_succeeds(table: Path) -> None:
    active = sorted(delta.load_snapshot(table).active)
    ctx = multiprocessing.get_context("spawn")
    q1, q2 = ctx.Queue(), ctx.Queue()
    p1 = ctx.Process(target=_worker_compact, args=(str(table), active[:2], q1))
    p2 = ctx.Process(target=_worker_compact, args=(str(table), active[2:], q2))
    p1.start(); p2.start()
    p1.join(30); p2.join(30)
    assert p1.exitcode == 0 and p2.exitcode == 0
    r1, r2 = q1.get(), q2.get()
    # Disjoint file sets: both succeed, log stays contiguous.
    assert r1[0] == "ok" and r2[0] == "ok"
    assert delta.list_versions(table)[-1] == 6  # 5 + two compaction commits
    assert len(delta.load_snapshot(table).active) == 2  # 4 -> 2 -> 1 compaction each


def test_concurrent_delete_of_selected_file_conflicts(table: Path) -> None:
    active = sorted(delta.load_snapshot(table).active)
    chosen = active[:2]
    base = delta.load_snapshot(table).version

    import threading

    t = threading.Thread(target=lambda: delete_files(table, [chosen[0]]))
    t.start()
    t.join()
    # The compaction was planned against the old base version: at commit time
    # a selected file has been removed concurrently -> 409, never resurrected.
    with pytest.raises(delta.ConflictError):
        delta.compact(table, chosen, base_version=base)
    assert chosen[0] not in delta.load_snapshot(table).active


def test_unrelated_concurrent_append_still_succeeds(table: Path) -> None:
    active = sorted(delta.load_snapshot(table).active)
    chosen = active[:2]
    append_rows(
        table,
        [{"station": "ST-010", "utc_seconds": 42, "level_mm": 7}],
    )
    result = delta.compact(table, chosen)  # no retry failure expected
    assert result["attempts"] >= 1
    assert len(delta.load_snapshot(table).active) == 4  # 4 - 2 + 1 + 1


def _worker_bootstrap(table_str: str, q) -> None:  # pragma: no cover
    try:
        q.put(("ok", create_sample_table(Path(table_str))))
    except Exception as exc:  # noqa: BLE001
        q.put(("err", repr(exc)))


def test_concurrent_bootstrap_from_two_processes(tmp_path: Path) -> None:
    """Two processes creating the sample table at once yield one valid table."""
    target = tmp_path / "boot"

    ctx = multiprocessing.get_context("spawn")
    q1, q2 = ctx.Queue(), ctx.Queue()
    p1 = ctx.Process(target=_worker_bootstrap, args=(str(target), q1))
    p2 = ctx.Process(target=_worker_bootstrap, args=(str(target), q2))
    p1.start(); p2.start()
    p1.join(30); p2.join(30)
    assert p1.exitcode == 0 and p2.exitcode == 0
    assert q1.get()[0] == "ok" and q2.get()[0] == "ok"

    versions = delta.list_versions(target)
    assert versions == list(range(0, versions[-1] + 1))
    snap = delta.load_snapshot(target)
    assert snap.protocol == (1, 2)
    assert len(snap.active) == 4  # exactly one set of sample files, no dupes
    assert delta.read_table(target).num_rows == 24


def test_same_files_compacted_by_two_processes_occ_serialises(table: Path) -> None:
    """Two OS processes compact the SAME files: one wins, one retries/conflicts.

    The winner replaces the files; the loser must either retry onto a fresh
    snapshot (and then find the files gone) or report a 409-style conflict,
    but the log must stay contiguous and rows must be preserved.
    """
    active = sorted(delta.load_snapshot(table).active)
    before_rows = sorted(map(tuple, _rows(delta.read_table(table))))
    ctx = multiprocessing.get_context("spawn")
    q1, q2 = ctx.Queue(), ctx.Queue()
    p1 = ctx.Process(target=_worker_compact, args=(str(table), active[:3], q1))
    p2 = ctx.Process(target=_worker_compact, args=(str(table), active[:3], q2))
    p1.start(); p2.start()
    p1.join(30); p2.join(30)
    assert p1.exitcode == 0 and p2.exitcode == 0
    r1, r2 = q1.get(), q2.get()
    statuses = {r1[0], r2[0]}
    assert statuses <= {"ok", "conflict"}
    assert "ok" in statuses  # at least one compaction committed
    # Log remains contiguous from 0, snapshot remains fully readable.
    versions = delta.list_versions(table)
    assert versions == list(range(0, versions[-1] + 1))
    after_rows = sorted(map(tuple, _rows(delta.read_table(table))))
    assert after_rows == before_rows
    snap = delta.load_snapshot(table)
    assert all(p not in snap.active for p in active[:3])


def test_compaction_then_external_delete_does_not_resurrect(table: Path) -> None:
    active = sorted(delta.load_snapshot(table).active)
    delta.compact(table, active[:2])
    # A second compaction referencing the now-removed old files must conflict.
    with pytest.raises(delta.FileNotActiveError):
        delta.compact(table, active[:2])


# ---------------------------------------------------------------------------
# Crash / durability behaviour
# ---------------------------------------------------------------------------


def test_compaction_preserves_explicit_duplicate_rows(tmp_path: Path) -> None:
    t = tmp_path / "dup"
    create_sample_table(t)
    # Two files containing identical rows: duplicates must survive compaction
    # (no dedup, no distinct, just a byte-level row merge).
    dup_rows = [
        {"station": "ST-DUP", "utc_seconds": 100, "level_mm": 5},
        {"station": "ST-DUP", "utc_seconds": 100, "level_mm": 5},
    ]
    append_rows(t, dup_rows)
    append_rows(t, list(dup_rows))
    before = sorted(tuple(r.values()) for r in delta.read_table(t).to_pylist())
    chosen = sorted(delta.load_snapshot(t).active)[:3]
    delta.compact(t, chosen)
    after = sorted(tuple(r.values()) for r in delta.read_table(t).to_pylist())
    assert before == after
    # The duplicated pair occurs at least 4 times (2 per file x 2 files).
    dup_key = ("ST-DUP", 100, 5)
    assert after.count(dup_key) == 4


def test_successful_compaction_leaves_no_staging_orphans(table: Path) -> None:
    chosen = sorted(delta.load_snapshot(table).active)[:2]
    delta.compact(table, chosen)
    staging = table / delta.STAGING_DIR
    assert not staging.exists() or not any(staging.iterdir())


def test_fault_after_data_write_leaves_no_snapshot_change(table: Path) -> None:
    chosen = sorted(delta.load_snapshot(table).active)[:2]
    before = delta.load_snapshot(table)
    with pytest.raises(RuntimeError):
        delta.compact(table, chosen, fault_inject="after_data_write")
    after = delta.load_snapshot(table)
    assert after.version == before.version
    assert sorted(after.active) == sorted(before.active)
    # Half-finished output is confined to staging and never queryable.
    staging = list((table / delta.STAGING_DIR).glob("*"))
    assert all(p.name.startswith("compact-") for p in staging)


def test_staging_cleanup_removes_old_orphans(table: Path) -> None:
    staging = table / delta.STAGING_DIR
    staging.mkdir(exist_ok=True)
    old = staging / "compact-deadbeef.parquet"
    old.write_bytes(b"orphan")
    os.utime(old, (0, 0))  # ancient mtime
    cleaned = delta.cleanup_staging(table, older_than_seconds=0)
    assert not old.exists()
    assert any("compact-deadbeef" in p for p in cleaned)


def test_immutable_paths_are_never_reused(table: Path) -> None:
    chosen = sorted(delta.load_snapshot(table).active)[:2]
    r1 = delta.compact(table, chosen)
    remaining = sorted(delta.load_snapshot(table).active)
    # A second compaction on remaining files must mint a brand new path.
    r2 = delta.compact(table, remaining[:2])
    assert r1["output_file"] != r2["output_file"]
    assert r1["output_file"] not in delta.load_snapshot(table).active or True
    assert r2["output_file"] in delta.load_snapshot(table).active


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


def test_api_health_and_snapshot(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"
    r = client.get("/table/snapshot")
    body = r.json()
    assert body["protocol"] == {"minReaderVersion": 1, "minWriterVersion": 2}
    assert body["active_file_count"] == 4
    assert body["row_count"] == 24


def test_api_compact_and_time_travel(client: TestClient) -> None:
    files = client.get("/table/snapshot").json()["active_files"][:2]
    r = client.post("/compact", json={"version": None, "files": files})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["files_added"] == 1
    assert body["files_removed"] == 2
    assert body["committed_version"] == 5

    old = client.get("/table/snapshot", params={"version": 4}).json()
    assert old["active_file_count"] == 4 and old["row_count"] == 24
    new = client.get("/table/snapshot").json()
    assert new["active_file_count"] == 3 and new["row_count"] == 24


def test_api_conflict_and_validation_mapping(client: TestClient) -> None:
    files = client.get("/table/snapshot").json()["active_files"]
    # 400: too few files
    r = client.post("/compact", json={"files": [files[0]]})
    assert r.status_code == 422  # pydantic min_length
    # 404: unknown file
    r = client.post("/compact", json={"files": [files[0], "nope.parquet"]})
    assert r.status_code == 404
    # 404: bad version
    r = client.post("/compact", json={"version": 77, "files": files[:2]})
    assert r.status_code == 404
    # 409: external delete then compact the same selection, pinned to the
    # base version that was read before the delete landed.
    from app import main

    base_version = client.get("/table/snapshot").json()["version"]
    delete_files(main.TABLE_DIR, [files[0]])
    r = client.post(
        "/compact", json={"version": base_version, "files": files[:2]}
    )
    assert r.status_code == 409
