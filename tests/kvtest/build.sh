#!/usr/bin/env bash
set -euo pipefail

# kvtest build script.
#
# Supports two build systems, mirroring the main repo's build.sh -b switch:
#   -b bazel (default): build kvtest as an in-tree Bazel target
#                        (//tests/kvtest:kvtest), producing a self-contained
#                        binary that does not need an SDK at runtime. Picks up
#                        bthread/brpc headers transitively from the in-tree
#                        datasystem client, so the KVTEST_USE_BRPC control
#                        plane and bthread-backed pipeline/notify-pool workers
#                        are enabled.
#   -b cmake : build kvtest against a pre-installed datasystem SDK pointed to
#              by -s/--sdk (default ../../output/cpp). Two backends selectable
#              at build time:
#                - brpc + bthread (default, KVTEST_USE_BRPC=ON): reuses the
#                  main repo's cmake/external_libs/*.cmake to download/build
#                  brpc/protobuf/gflags/absl into $DS_OPENSOURCE_DIR (cached;
#                  first build ~5-10min, subsequent seconds). Same behavior as
#                  bazel mode.
#                - httplib + std::thread (--use-httplib, KVTEST_USE_BRPC=OFF):
#                  legacy path; no third-party deps, no brpc headers needed.
#                  Useful when the build host cannot reach GitHub / gitee.
#
# Common options:
#   -s, --sdk DIR     SDK directory (cmake only; default $DATASYSTEM_SDK_DIR or ../../output/cpp)
#   -j, --jobs N      Parallel jobs (default $(nproc))
#   -d, --debug       Debug build (cmake: -DCMAKE_BUILD_TYPE=Debug; bazel: --config=debug)
#   -r, --release     Release build (default)
#   -c, --clean       Clean build directory first (cmake) / bazel clean first (bazel)
#   -b, --build SYS   Build system: cmake or bazel (default: bazel)
#   -M on|off         Build the Bazel kvtest with URMA support (default: off)
#   --use-httplib     cmake mode only: build httplib+std::thread backend instead of
#                     the default brpc+bthread backend. The brpc backend reuses the
#                     main repo's cmake/external_libs scripts to download/build
#                     brpc/protobuf/gflags/absl into $DS_OPENSOURCE_DIR (cached).
#   -h, --help        Show this help

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SDK_DIR="${DATASYSTEM_SDK_DIR:-$SCRIPT_DIR/../../output/cpp}"
BUILD_DIR="$SCRIPT_DIR/build"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 8)}"
BUILD_SYSTEM="bazel"
BUILD_TYPE="Release"
BUILD_WITH_URMA="off"
CLEAN=0
USE_HTTPLIB=0

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
        -M)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: -M requires on or off"
                exit 1
            fi
            if [[ "$2" != "on" && "$2" != "off" ]]; then
                echo "ERROR: invalid value '$2' for -M, choose from on or off"
                exit 1
            fi
            BUILD_WITH_URMA="$2"; shift 2 ;;
        -c|--clean) CLEAN=1; shift ;;
        --use-httplib) USE_HTTPLIB=1; shift ;;
        -h|--help)  usage ;;
        *)          echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "Build system: $BUILD_SYSTEM"
echo "Build type:   $BUILD_TYPE"
echo "Jobs:         $JOBS"
if [[ "$BUILD_SYSTEM" == "cmake" ]]; then
    if [[ $USE_HTTPLIB -eq 1 ]]; then
        echo "Backend:      httplib + std::thread (--use-httplib)"
    else
        echo "Backend:      brpc + bthread (default; third-party libs reuse main repo's cmake/external_libs)"
    fi
elif [[ $USE_HTTPLIB -eq 1 ]]; then
    echo "ERROR: --use-httplib only applies to -b cmake (bazel mode is always brpc+bthread)"
    exit 1
fi

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
    # -DREPO_ROOT points at the main yuanrong-datasystem source tree so the
    #   cmake build can reuse cmake/external_libs/*.cmake + third_party/patches
    #   to download/build brpc/protobuf/gflags/absl when KVTEST_USE_BRPC=ON.
    # -DKVTEST_USE_BRPC selects the backend (ON by default; --use-httplib flips OFF).
    cmake_opts=(
        -DDATASYSTEM_SDK_DIR="$SDK_DIR"
        -DREPO_ROOT="$REPO_ROOT"
        -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
        -DCMAKE_SKIP_INSTALL_RPATH=ON
        -DKVTEST_USE_BRPC=$([[ $USE_HTTPLIB -eq 1 ]] && echo OFF || echo ON)
    )
    cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" "${cmake_opts[@]}"
    cmake --build "$BUILD_DIR" -j"$JOBS"

    echo ""
    echo "Build OK: $BUILD_DIR/kvtest"
    echo ""
    echo "Packaging..."
    cd "$SCRIPT_DIR"
    make package BAZEL_SDK_DIR="$SDK_DIR"
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
    if [[ "$BUILD_WITH_URMA" == "on" ]]; then
        ba_args+=(--config=urma)
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
    # bazel binary is self-contained — no SDK libs needed at runtime.
    make package BAZEL_SDK_DIR="$SDK_DIR"
    echo ""
    echo "Done: $SCRIPT_DIR/output/"
fi
