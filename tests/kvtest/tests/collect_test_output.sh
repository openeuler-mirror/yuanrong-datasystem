#!/bin/bash
# Run metrics tests and collect output files for inspection.
# Unlike normal tests, this preserves the temp directories.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KVTEST_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$KVTEST_ROOT/build_tests"
COLLECT_DIR="$KVTEST_ROOT/test_output"

mkdir -p "$COLLECT_DIR"
rm -rf "$COLLECT_DIR"/*

echo "=== Building ==="
cd "$BUILD_DIR"
cmake "$SCRIPT_DIR/cxx" -DKVTEST_ROOT="$KVTEST_ROOT" > /dev/null 2>&1
make -j$(nproc) 2>&1 | tail -1

echo ""
echo "=== Running tests (output preserved) ==="
cd "$BUILD_DIR"
./kvtest_tests 2>&1 || true

echo ""
echo "=== Collecting output ==="
# Find all test-generated directories and files
for dir in /tmp/kvtest_metrics_* /tmp/kvtest_pipeline_test; do
    if [ -d "$dir" ]; then
        name=$(basename "$dir")
        dest="$COLLECT_DIR/$name"
        mkdir -p "$dest"
        cp -r "$dir"/* "$dest/" 2>/dev/null || true
        echo "  Collected: $name/"
    fi
done

# List all collected files with sizes
echo ""
echo "=== Collected files ==="
find "$COLLECT_DIR" -type f | sort | while read f; do
    size=$(stat --format=%s "$f")
    rel=${f#$COLLECT_DIR/}
    echo "  $rel ($size bytes)"
done

echo ""
echo "Output directory: $COLLECT_DIR"
