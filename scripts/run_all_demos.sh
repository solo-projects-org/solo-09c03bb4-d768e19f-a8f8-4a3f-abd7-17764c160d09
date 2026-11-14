#!/usr/bin/env bash
# Run all three demos against the two local API processes.
set -euo pipefail

cd "$(dirname "$0")/.."
PY=.venv/bin/python
[ -x "$PY" ] || PY=python3

export API_1="${API_1:-http://127.0.0.1:8001}"
export API_2="${API_2:-http://127.0.0.1:8002}"
export TABLE_DIR="${TABLE_DIR:-$(pwd)/_demo_data/water_level}"

$PY scripts/demo_readback.py
$PY scripts/demo_concurrent_append.py
$PY scripts/demo_concurrent_delete.py

echo
echo "All demos completed successfully."
