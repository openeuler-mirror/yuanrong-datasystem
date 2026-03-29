#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "[build.sh] ERROR: python3 not found"
  exit 1
fi

if ! python3 -m pip --version >/dev/null 2>&1; then
  echo "[build.sh] ERROR: python3 pip is unavailable"
  exit 1
fi

if ! python3 -c "import wheel" >/dev/null 2>&1; then
  echo "[build.sh] Installing missing dependency: wheel"
  python3 -m pip install --user wheel
fi

echo "[build.sh] Building wheel package..."
python3 setup.py bdist_wheel

echo "[build.sh] Wheel package generated:"
ls -1 dist/*.whl
