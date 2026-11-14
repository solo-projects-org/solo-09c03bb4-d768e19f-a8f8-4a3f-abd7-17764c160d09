#!/usr/bin/env bash
# Start two API processes sharing one local table directory.
# Mirrors `docker compose up` (api-1 on 8001, api-2 on 8002) for environments
# without a Docker daemon.
set -euo pipefail

cd "$(dirname "$0")/.."
PY=.venv/bin/python
[ -x "$PY" ] || PY=python3

export DATA_ROOT="${DATA_ROOT:-$(pwd)/_demo_data}"
export TABLE_NAME="${TABLE_NAME:-water_level}"
mkdir -p "$DATA_ROOT"

echo "Starting api-1 on :8001 and api-2 on :8002 (data: $DATA_ROOT)"
$PY -m uvicorn app.main:app --host 127.0.0.1 --port 8001 \
    > "$DATA_ROOT/api-1.log" 2>&1 &
PID1=$!
$PY -m uvicorn app.main:app --host 127.0.0.1 --port 8002 \
    > "$DATA_ROOT/api-2.log" 2>&1 &
PID2=$!

cleanup() {
    kill "$PID1" "$PID2" 2>/dev/null || true
}
trap cleanup EXIT

echo "PIDs: $PID1 $PID2 — logs under $DATA_ROOT/api-*.log"
echo "Swagger: http://127.0.0.1:8001/docs  and  http://127.0.0.1:8002/docs"
wait
