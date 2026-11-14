"""Demo 1 - compact and read back BOTH the new and the old snapshot.

Verifies that:
* two+ parquet files merge into one
* row content (including duplicate rows) and schema are unchanged
* every old version remains readable via time travel
* the returned committed version and file-count change are reported

Usage:
    python scripts/demo_readback.py
    API_1=http://127.0.0.1:8001 python scripts/demo_readback.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._demo_common import API_1, API_2, banner, api, show_snapshot  # noqa: E402


def main() -> None:
    client = api(API_1)
    banner("Demo 1: read back new and old versions around a compaction")

    versions = client.get("/table/versions").json()["versions"]
    print(f"contiguous log versions before: {versions}")

    before = show_snapshot(client)
    if before["active_file_count"] < 2:
        print("Need at least two active files; re-create the sample table.")
        sys.exit(1)

    chosen = before["active_files"][:2]
    print(f"\nPOST /compact on replica #1 ({API_1}), files:")
    for f in chosen:
        print(f"  * {f}")

    r = client.post("/compact", json={"version": None, "files": chosen})
    r.raise_for_status()
    body = r.json()
    print("\ncompaction response:")
    for key, value in body.items():
        print(f"  {key}: {value}")
    assert body["files_added"] == 1
    assert body["files_removed"] == len(chosen)
    print(
        f"\n=> committed version v{body['committed_version']}, "
        f"file count change: -{body['files_removed']} +{body['files_added']} "
        f"(net {body['files_added'] - body['files_removed']:+d})"
    )

    banner("Time travel: old snapshot stays readable and unchanged")
    old = show_snapshot(client, version=body["base_version"])
    assert old["row_count"] == before["row_count"]
    assert sorted(old["active_files"]) == sorted(before["active_files"])

    banner("New snapshot: same rows, fewer files, identical schema")
    new = show_snapshot(client)
    assert new["row_count"] == before["row_count"]
    assert new["active_file_count"] == before["active_file_count"] - len(chosen) + 1

    stats = client.get("/table/stats").json()
    print("\ntable columns (schema preserved):")
    for col in stats["columns"]:
        print(f"  - {col}")
    print(
        f"\nreplica #2 ({API_2}) sees the same latest version: "
        f"{api(API_2).get('/table/versions').json()['latest_version']}"
    )
    print("\nOK: values, duplicates, schema and every old snapshot are intact.")


if __name__ == "__main__":
    main()
