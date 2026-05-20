#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KVTEST_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FAILED=0

echo "=== C++ Tests ==="
BUILD_DIR="$KVTEST_ROOT/build_tests"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
cmake "$SCRIPT_DIR/cxx" -DKVTEST_ROOT="$KVTEST_ROOT" > /dev/null
make -j$(nproc) 2>&1 | tail -1
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "BUILD FAILED"
    exit 1
fi
./kvtest_tests || FAILED=1

echo ""
echo "=== Python Tests ==="
cd "$KVTEST_ROOT"
python3 -m unittest discover -s "$SCRIPT_DIR/python" -v || FAILED=1

echo ""
if [ $FAILED -eq 0 ]; then
    echo "ALL TESTS PASSED"
else
    echo "SOME TESTS FAILED"
fi
exit $FAILED
