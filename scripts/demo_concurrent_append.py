"""Demo 2 - compaction while an external process keeps appending.

The external writer is a separate OS process that writes brand-new immutable
parquet files and commits its own Delta log entries (dataChange=true) while
the API compacts an unrelated set of files. The compaction must succeed:
changes to unrelated files never invalidate the transaction.
"""

from __future__ import annotations

import multiprocessing
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._demo_common import API_1, TABLE_DIR, banner, api, show_snapshot  # noqa: E402


def _append_worker(table_dir: str, rounds: int) -> None:  # pragma: no cover
    from app.external_writer import append_rows

    for i in range(rounds):
        rows = [
            {
                "station": f"ST-EXT{i}",
                "utc_seconds": 1_731_700_000 + i * 10 + j,
                "level_mm": 2000 + i * 10 + j,
            }
            for j in range(3)
        ]
        info = append_rows(Path(table_dir), rows)
        print(f"    [external-writer] append committed at v{info['version']}")
        time.sleep(0.05)


def main() -> None:
    client = api(API_1)
    banner("Demo 2: external appends during compaction (unrelated files -> success)")

    before = show_snapshot(client)
    chosen = before["active_files"][:2]
    base_version = before["version"]
    print(f"\ncompacting {len(chosen)} files planned at v{base_version}")

    ctx = multiprocessing.get_context("spawn")
    writer = ctx.Process(target=_append_worker, args=(str(TABLE_DIR), 6))
    writer.start()
    time.sleep(0.05)  # let the first append land first

    r = client.post("/compact", json={"version": base_version, "files": chosen})
    writer.join(30)
    assert writer.exitcode == 0, "external writer crashed"

    print(f"\nPOST /compact -> HTTP {r.status_code}")
    r.raise_for_status()
    body = r.json()
    print(
        f"committed v{body['committed_version']} after {body['attempts']} "
        f"attempt(s); removed {body['files_removed']}, added {body['files_added']}"
    )
    assert body["committed_version"] > base_version

    after = show_snapshot(client)
    print(
        f"\nrows before={before['row_count']} after={after['row_count']} "
        f"(new append rows are visible, compacted rows preserved)"
    )
    assert after["row_count"] >= before["row_count"] + 6
    assert all(f not in after["active_files"] for f in chosen)

    versions = client.get("/table/versions").json()["versions"]
    assert versions == list(range(0, versions[-1] + 1)), "log must stay contiguous"
    print(f"log still contiguous from 0: 0..{versions[-1]}")
    print("\nOK: concurrent appends and the compaction coexist without conflict.")


if __name__ == "__main__":
    main()
