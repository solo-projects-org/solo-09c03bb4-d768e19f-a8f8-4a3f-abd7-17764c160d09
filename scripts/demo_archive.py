"""Demo 4 - publish an immutable per-version archive for offline recompute.

Verifies that:
* POST /archives copies one committed version's parquet files, protocol and
  schema into an independent Delta archive on the shared data volume
* GET /archives/{name} reports the archive's source version and row count
* a same-name + same-version retry is idempotent (created=false)
* a same-name + different-version request is rejected with 409
* source-table compaction afterwards does not change the published archive

Usage:
    python scripts/demo_archive.py
    API_1=http://127.0.0.1:8001 API_2=http://127.0.0.1:8002 python scripts/demo_archive.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._demo_common import API_1, API_2, banner, api, show_snapshot  # noqa: E402


def main() -> None:
    client1 = api(API_1)
    client2 = api(API_2)
    banner("Demo 4: archive a pinned version for offline recompute")

    before = show_snapshot(client1)
    version = before["version"]
    rows = before["row_count"]
    name = f"wl-baseline-v{version}"
    print(f"\nPOST /archives on replica #1 ({API_1}): name={name!r}, version={version}")

    r = client1.post("/archives", json={"name": name, "version": version})
    assert r.status_code == 201, r.text
    body = r.json()
    print("\narchive response (HTTP 201):")
    for key, value in body.items():
        print(f"  {key}: {value}")
    assert body["created"] is True
    assert body["source_version"] == version
    assert body["row_count"] == rows

    banner("Idempotent retry: same name + same version")
    r = client1.post("/archives", json={"name": name, "version": version})
    assert r.status_code == 200, r.text
    assert r.json()["created"] is False
    print(f"HTTP 200, created=false - the already-published archive is returned "
          f"(rows={r.json()['row_count']})")

    banner(f"Query the archive by name from replica #2 ({API_2})")
    r = client2.get(f"/archives/{name}")
    r.raise_for_status()
    info = r.json()
    print(f"GET /archives/{name}:")
    for key, value in info.items():
        print(f"  {key}: {value}")
    assert info["source_version"] == version
    assert info["row_count"] == rows

    banner("Source table moves on: compaction via replica #2")
    files = show_snapshot(client2)["active_files"][:2]
    r = client2.post("/compact", json={"version": None, "files": files})
    r.raise_for_status()
    committed = r.json()["committed_version"]
    print(f"source table compacted: committed v{committed} "
          f"(-{r.json()['files_removed']} +{r.json()['files_added']} files)")

    banner("The published archive is unaffected by source-table changes")
    r = client2.get(f"/archives/{name}")
    r.raise_for_status()
    after = r.json()
    assert after == info
    print(
        f"archive unchanged: source_version={after['source_version']}, "
        f"rows={after['row_count']}, files={after['file_count']}"
    )

    if version > 0:
        banner("Same name + different version -> 409 conflict")
        r = client1.post("/archives", json={"name": name, "version": version - 1})
        print(f"POST /archives ({name!r}, v{version - 1}) -> HTTP {r.status_code}: "
              f"{r.json()['detail']}")
        assert r.status_code == 409

    banner("Archive listing")
    r = client1.get("/archives")
    r.raise_for_status()
    for a in r.json()["archives"]:
        print(
            f"  - {a['archive']}: source={a['source_table']}@v{a['source_version']}, "
            f"rows={a['row_count']}, files={a['file_count']}"
        )
    assert any(a["archive"] == name for a in r.json()["archives"])

    print("\nOK: one complete archive published, idempotent retry, 409 on "
          "version mismatch, independent of the source table.")


if __name__ == "__main__":
    main()
