"""Demo 3 - external delete racing with compaction produces a 409 conflict.

An external process removes one of the files selected for compaction while
the compaction is in flight. At commit time the API re-reads the snapshot,
notices a selected file is no longer active and refuses to replace anything:
deleted data is never resurrected. A retry with the still-valid subset then
succeeds.
"""

from __future__ import annotations

import multiprocessing
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._demo_common import API_2, TABLE_DIR, banner, api, show_snapshot  # noqa: E402


def _delete_worker(table_dir: str, victim: str, done) -> None:  # pragma: no cover
    from app.external_writer import delete_files

    info = delete_files(Path(table_dir), [victim])
    print(f"    [external-writer] delete committed at v{info['version']}: {victim}")
    done.set()


def main() -> None:
    # Drive the request through replica #2 to prove both processes serve it.
    client = api(API_2)
    banner("Demo 3: concurrent delete of a selected file -> 409, no resurrection")

    before = show_snapshot(client)
    chosen = before["active_files"][:2]
    victim = chosen[0]
    base_version = before["version"]
    print(f"\nplanned compaction at v{base_version}, files: {chosen}")
    print(f"external writer will concurrently delete: {victim}")

    ctx = multiprocessing.get_context("spawn")
    done = ctx.Event()
    deleter = ctx.Process(
        target=_delete_worker, args=(str(TABLE_DIR), victim, done)
    )
    deleter.start()
    # Wait until the external delete has actually committed so the conflict is
    # deterministic; the compaction still references the pre-delete version.
    done.wait(30)

    r = client.post("/compact", json={"version": base_version, "files": chosen})
    deleter.join(30)
    assert deleter.exitcode == 0, "external delete writer crashed"

    print(f"\nPOST /compact -> HTTP {r.status_code}: {r.json()}")
    assert r.status_code == 409, r.text
    assert "removed by a concurrent commit" in r.json()["detail"]

    latest = show_snapshot(client)
    assert victim not in latest["active_files"], "deleted file must stay removed"
    print(f"\n{victim} remains absent from the latest snapshot (not resurrected).")

    banner("Retry: compacting two still-active files succeeds")
    # Pick strictly from the current snapshot so the victim is never reused.
    retry_files = [f for f in latest["active_files"] if f != victim][:2]
    assert len(retry_files) == 2, "need two active files for the retry"
    r2 = client.post("/compact", json={"files": retry_files})
    print(f"retry POST /compact -> HTTP {r2.status_code}")
    r2.raise_for_status()
    body = r2.json()
    print(
        f"committed v{body['committed_version']}, "
        f"file count change: -{body['files_removed']} +{body['files_added']}"
    )
    final = show_snapshot(client)
    assert all(f not in final["active_files"] for f in retry_files)
    print("\nOK: conflicts are rejected safely and a corrected retry commits.")


if __name__ == "__main__":
    main()
