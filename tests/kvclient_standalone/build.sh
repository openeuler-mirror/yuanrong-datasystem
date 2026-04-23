#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SDK_DIR="${DATASYSTEM_SDK_DIR:-$SCRIPT_DIR/../../output/cpp}"
BUILD_DIR="$SCRIPT_DIR/build"
JOBS="${JOBS:-$(nproc)}"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -s, --sdk DIR    SDK directory (default: \$DATASYSTEM_SDK_DIR or ../../output/cpp)"
    echo "  -j, --jobs N     Parallel jobs (default: \$(nproc))"
    echo "  -c, --clean      Clean build directory first"
    echo "  -h, --help       Show this help"
    echo ""
    echo "Examples:"
    echo "  $0                          # Build + package with default SDK"
    echo "  $0 -s /path/to/sdk          # Build with custom SDK"
    echo "  $0 -c -j8                   # Clean build with 8 jobs"
}

CLEAN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--sdk)   SDK_DIR="$2"; shift 2 ;;
        -j|--jobs)  JOBS="$2"; shift 2 ;;
        -c|--clean) CLEAN=1; shift ;;
        -h|--help)  usage; exit 0 ;;
        *)          echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

if [[ ! -d "$SDK_DIR/include" ]] || [[ ! -d "$SDK_DIR/lib" ]]; then
    echo "ERROR: Invalid SDK dir: $SDK_DIR"
    echo "  Expected: $SDK_DIR/include/ and $SDK_DIR/lib/"
    exit 1
fi

echo "SDK:   $SDK_DIR"
echo "Jobs:  $JOBS"

if [[ $CLEAN -eq 1 ]]; then
    echo "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
fi

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake -DDATASYSTEM_SDK_DIR="$SDK_DIR" ..
make -j"$JOBS"

echo ""
echo "Build OK: $BUILD_DIR/kvclient_standalone_test"
echo ""
echo "Packaging..."

cd "$SCRIPT_DIR"
make copy-sdk BAZEL_SDK_DIR="$SDK_DIR"
make package

echo ""
echo "Done: $SCRIPT_DIR/output/"
