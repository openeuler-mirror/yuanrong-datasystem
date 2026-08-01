#!/usr/bin/env bash
set -euo pipefail

# kvtest build script.
#
# Supports two build systems, mirroring the main repo's build.sh -b switch:
#   -b cmake  (default): build kvtest against a pre-installed datasystem SDK
#                        pointed to by -s/--sdk (default ../../output/cpp).
#   -b bazel : build kvtest as an in-tree Bazel target (//tests/kvtest:kvtest),
#              producing a self-contained binary that does not need an SDK.
#
# Common options:
#   -s, --sdk DIR     SDK directory (cmake only; default $DATASYSTEM_SDK_DIR or ../../output/cpp)
#   -j, --jobs N      Parallel jobs (default $(nproc))
#   -d, --debug       Debug build (cmake: -DCMAKE_BUILD_TYPE=Debug; bazel: --config=debug)
#   -r, --release     Release build (default)
#   -c, --clean       Clean build directory first (cmake) / bazel clean first (bazel)
#   -b, --build SYS   Build system: cmake or bazel (default: cmake)
#   -h, --help        Show this help

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SDK_DIR="${DATASYSTEM_SDK_DIR:-$SCRIPT_DIR/../../output/cpp}"
BUILD_DIR="$SCRIPT_DIR/build"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 8)}"
BUILD_SYSTEM="cmake"
BUILD_TYPE="Release"
CLEAN=0

usage() {
    sed -n '3,20p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--build)
            if [[ "$2" != "cmake" && "$2" != "bazel" ]]; then
                echo "ERROR: invalid value '$2' for -b, choose from cmake or bazel"
                exit 1
            fi
            BUILD_SYSTEM="$2"; shift 2 ;;
        -s|--sdk)   SDK_DIR="$2"; shift 2 ;;
        -j|--jobs)  JOBS="$2"; shift 2 ;;
        -d|--debug) BUILD_TYPE="Debug"; shift ;;
        -r|--release) BUILD_TYPE="Release"; shift ;;
        -c|--clean) CLEAN=1; shift ;;
        -h|--help)  usage ;;
        *)          echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "Build system: $BUILD_SYSTEM"
echo "Build type:   $BUILD_TYPE"
echo "Jobs:         $JOBS"

# -----------------------------------------------------------------------------
# Detect optional libnuma support. Used by both modes (cmake find_library and
# bazel --define=kvtest_numa) so the link decision stays consistent with
# cpu_affinity.h's __has_include(<numa.h>) auto-detection.
# -----------------------------------------------------------------------------
detect_numa() {
    local has_hdr=no has_lib=no
    if echo '#include <numa.h>' | ${CC:-gcc} -E -x c - >/dev/null 2>&1; then
        has_hdr=yes
    fi
    if command -v ldconfig >/dev/null 2>&1 && ldconfig -p 2>/dev/null | grep -q 'libnuma\.so'; then
        has_lib=yes
    fi
    [[ "$has_hdr" == yes && "$has_lib" == yes ]]
}

# -----------------------------------------------------------------------------

if [[ "$BUILD_SYSTEM" == "cmake" ]]; then
    if [[ ! -d "$SDK_DIR/include" ]] || [[ ! -d "$SDK_DIR/lib" ]]; then
        echo "ERROR: Invalid SDK dir: $SDK_DIR"
        echo "  Expected: $SDK_DIR/include/ and $SDK_DIR/lib/"
        echo "  Build the main repo first (bash build.sh), or pass -s /path/to/sdk."
        exit 1
    fi
    echo "SDK:          $SDK_DIR"

    if [[ $CLEAN -eq 1 ]]; then
        echo "Cleaning build directory..."
        rm -rf "$BUILD_DIR"
    fi

    mkdir -p "$BUILD_DIR"
    # Out-of-source cmake configure/build against the kvtest source tree.
    cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" \
        -DDATASYSTEM_SDK_DIR="$SDK_DIR" \
        -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
        -DCMAKE_SKIP_INSTALL_RPATH=ON
    cmake --build "$BUILD_DIR" -j"$JOBS"

    echo ""
    echo "Build OK: $BUILD_DIR/kvtest"
    echo ""
    echo "Packaging..."
    cd "$SCRIPT_DIR"
    make copy-sdk BAZEL_SDK_DIR="$SDK_DIR"
    make package
    echo ""
    echo "Done: $SCRIPT_DIR/output/"

else  # bazel
    if ! command -v bazel &>/dev/null; then
        echo "ERROR: bazel not found in PATH. Install bazelisk/bazel first."
        exit 1
    fi

    if [[ $CLEAN -eq 1 ]]; then
        echo "Running bazel clean..."
        (cd "$REPO_ROOT" && bazel clean)
    fi

    ba_args=()
    if [[ "$BUILD_TYPE" == "Debug" ]]; then
        ba_args+=(--config=debug)
    else
        ba_args+=(--config=release)
    fi
    if detect_numa; then
        echo "NUMA: libnuma detected, enabling -Dkvtest_numa=true"
        ba_args+=(--define=kvtest_numa=true)
    else
        echo "NUMA: not detected, numa_node config will be ignored"
    fi

    # Pass version + commit to the build_info genrule via action env so the
    # embedded values match the CMake path (build.sh runs in the real
    # workspace where git is available; the bazel sandbox is not).
    export KVTEST_BUILD_VERSION="$(tr -d '\n' < "$SCRIPT_DIR/VERSION")"
    export KVTEST_BUILD_COMMIT="$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    ba_args+=(--action_env=KVTEST_BUILD_VERSION --action_env=KVTEST_BUILD_COMMIT)

    echo "bazel command: bazel build ${ba_args[*]} --jobs=$JOBS //tests/kvtest:kvtest"
    (cd "$REPO_ROOT" && bazel build "${ba_args[@]}" --jobs="$JOBS" //tests/kvtest:kvtest)

    # Stage the bazel-built binary where the Makefile package target expects it.
    bazel_bin_dir="$REPO_ROOT/bazel-bin"
    kvtest_bin=""
    for cand in \
        "$bazel_bin_dir/tests/kvtest/kvtest" \
        "$bazel_bin_dir/tests/kvtest/kvtest.exe"; do
        if [[ -f "$cand" ]]; then kvtest_bin="$cand"; break; fi
    done
    if [[ -z "$kvtest_bin" ]]; then
        echo "ERROR: kvtest binary not found under $bazel_bin_dir/tests/kvtest/"
        exit 1
    fi
    mkdir -p "$BUILD_DIR"
    cp -f "$kvtest_bin" "$BUILD_DIR/kvtest"

    echo ""
    echo "Build OK: $BUILD_DIR/kvtest (bazel, self-contained binary)"
    echo ""
    echo "Packaging..."
    cd "$SCRIPT_DIR"
    # No `make copy-sdk` here: the bazel binary is self-contained and does not
    # need libdatasystem.so at runtime. The Makefile package target tolerates a
    # missing third_party/sdk/ for this mode.
    make package
    echo ""
    echo "Done: $SCRIPT_DIR/output/"
fi
