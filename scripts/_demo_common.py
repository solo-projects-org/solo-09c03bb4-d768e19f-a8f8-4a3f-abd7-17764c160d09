"""Shared helpers for the demo scripts."""

from __future__ import annotations

import os
from pathlib import Path

import httpx

# Base URLs of the two API replicas (docker-compose host ports).
API_1 = os.environ.get("API_1", "http://127.0.0.1:8001")
API_2 = os.environ.get("API_2", "http://127.0.0.1:8002")

# When demos run outside docker they talk to a local table directory directly.
TABLE_DIR = Path(os.environ.get("TABLE_DIR", "/data/water_level"))


def api(base: str = API_1) -> httpx.Client:
    return httpx.Client(base_url=base, timeout=30.0)


def banner(title: str) -> None:
    bar = "=" * 72
    print(f"\n{bar}\n{title}\n{bar}")


def show_snapshot(client: httpx.Client, version: int | None = None) -> dict:
    params = {"version": version} if version is not None else {}
    r = client.get("/table/snapshot", params=params)
    r.raise_for_status()
    body = r.json()
    label = f"v{body['version']}" + (
        f" (latest=v{body['latest_version']})" if version is not None else " (latest)"
    )
    print(
        f"snapshot {label}: files={body['active_file_count']}, "
        f"rows={body['row_count']}, protocol={body['protocol']}, "
        f"partition_columns={body['partition_columns']}"
    )
    for f in body["active_files"]:
        print(f"  - {f}")
    return body


def active_files(client: httpx.Client, version: int | None = None) -> list[str]:
    return show_snapshot(client, version)["active_files"]
