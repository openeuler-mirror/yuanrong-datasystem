#!/bin/bash
# Remote build script for feat-shm-fdpassing-ci on compile_32c_03
set -eo pipefail
exec 2>&1

BRANCH=feat-shm-fdpassing-ci
BUILD_ID=feat-shm-fdpassing-ci
JOBS=30
BUILD_DIR="$HOME/ougongchang/bazel_build/$BUILD_ID"
REPO_DIR="$BUILD_DIR/repo"
OPENSOURCE_DIR="$HOME/ougongchang/bazel_build/opensource_dir"
BAZEL_ROOT="$BUILD_DIR/bazel_root"
DISK_CACHE="$HOME/ougongchang/_bazel_disk_cache"
REPO_CACHE="$HOME/ougongchang/_bazel_repo_cache"
ENV_FILE="$BUILD_DIR/test_env.sh"

log(){ echo "[$(date +%H:%M:%S)] $*"; }

log "=== STAGE 0: SWAP ==="
# Use free -g (numeric Swap total) to avoid "32G" string-parsing bug in swapon --show
SWAP_GB=$(free -g | awk '/^Swap:/ {print $2}')
log "swap total = ${SWAP_GB:-0}GB"
if [ -z "$SWAP_GB" ] || [ "$SWAP_GB" -lt 16 ]; then
    log "swap low, adding 32G via /swapfile2"
    fallocate -l 32G /swapfile2 2>/dev/null || dd if=/dev/zero of=/swapfile2 bs=1M count=32768 status=none
    chmod 600 /swapfile2; mkswap /swapfile2 >/dev/null 2>&1; swapon /swapfile2 || log "swapon /swapfile2 returned $?"
else
    log "swap OK (${SWAP_GB}GB), skipping"
fi
swapon --show | head; free -g | grep -E "Mem|Swap"

mkdir -p "$BUILD_DIR" "$OPENSOURCE_DIR" "$BAZEL_ROOT" "$DISK_CACHE" "$REPO_CACHE"

log "=== STAGE 1: CLONE/FETCH ==="
if [ -d "$REPO_DIR/.git" ]; then
    log "repo exists, reusing + fetch"
    cd "$REPO_DIR"
    git remote set-url personal git@gitcode.com:OuGongChang/yuanrong-datasystem.git 2>/dev/null || git remote add personal git@gitcode.com:OuGongChang/yuanrong-datasystem.git
else
    log "fresh clone"
    git clone git@gitcode.com:OuGongChang/yuanrong-datasystem.git "$REPO_DIR"
    cd "$REPO_DIR"
fi

log "=== STAGE 2: CHECKOUT ==="
git remote add personal git@gitcode.com:OuGongChang/yuanrong-datasystem.git 2>/dev/null || git remote set-url personal git@gitcode.com:OuGongChang/yuanrong-datasystem.git
git fetch personal
git checkout -B "$BRANCH" "personal/$BRANCH"
git reset --hard "personal/$BRANCH"
git clean -fd
HEAD=$(git rev-parse HEAD)
log "HEAD=$HEAD"
if [ "$HEAD" != "f924539cbad21339f064cd1ddf4654c0724a23ca" ]; then
    log "WARN: HEAD mismatch (expected f924539cb), continuing anyway"
fi

log "=== STAGE 3: GITHUB MIRROR SED (two-step idempotent) ==="
find . -type f \( -name "*.bzl" -o -name "WORKSPACE" -o -name "WORKSPACE.bazel" -o -name "*.bazel" -o -name "*.cmake" -o -name "CMakeLists.txt" \) ! -path "./.git/*" \
  -exec sed -i -E 's|https://github\.com/|https://gh-proxy.com/https://github.com/|g; s|(https://gh-proxy\.com/)+https://github\.com/|https://gh-proxy.com/https://github.com/|g' {} \;
log "sed done; gh-proxy hits in WORKSPACE:"; grep -c "gh-proxy" WORKSPACE WORKSPACE.bazel 2>/dev/null || true

export DS_OPENSOURCE_DIR="$OPENSOURCE_DIR"
BAZEL_START_OPTS="--output_user_root=$BAZEL_ROOT"
BAZEL_CMD_OPTS="--disk_cache=$DISK_CACHE --repository_cache=$REPO_CACHE"

log "=== STAGE 4: BUILD WHEEL (--config=release) ==="
bazel $BAZEL_START_OPTS build //bazel:datasystem_wheel --config=release $BAZEL_CMD_OPTS --jobs=$JOBS
log "WHEEL BUILD DONE"
WHL=$(find bazel-bin/bazel -name "*.whl" -type f | head -1)
log "wheel=$WHL"

log "=== STAGE 5: INSTALL WHEEL ==="
pip uninstall openyuanrong-datasystem -y 2>&1 | tail -1 || true
pip install "$WHL" 2>&1 | tail -3
python3 -c "import yr.datasystem; print('SDK import OK:', yr.datasystem.__file__)"

log "=== STAGE 6: BUILD WORKER + 3 TEST TARGETS (--config=test --config=release) ==="
bazel $BAZEL_START_OPTS build --config=test --config=release $BAZEL_CMD_OPTS --jobs=$JOBS \
  //src/datasystem/worker:datasystem_worker \
  //tests/st/client/kv_cache:kv_client_transport_set_test \
  //tests/st/client/kv_cache:kv_client_transport_get_test \
  //tests/ut/client:ub_fault_p1_transport_admission_test
log "TEST TARGETS BUILD DONE"

log "=== STAGE 7: INSTALL TEST-CONFIG WORKER TO /usr/local/bin ==="
WORKER_BIN=$(find bazel-bin -path "*datasystem_worker" -type f -perm -u+x ! -name "*.stripped" ! -name "*.sym" | head -1)
# prefer the direct binary under src/datasystem/worker
WORKER_BIN=$(find bazel-bin/src/datasystem/worker -name "datasystem_worker" -type f -perm -u+x 2>/dev/null | head -1)
log "worker binary=$WORKER_BIN"
cp -f "$WORKER_BIN" /usr/local/bin/datasystem_worker
chmod +x /usr/local/bin/datasystem_worker
/usr/local/bin/datasystem_worker --version 2>&1 | head -3 || log "worker --version exited $?"
ls -la /usr/local/bin/datasystem_worker

log "=== STAGE 8: RESOLVE RUNTIME LIBS (ldd) ==="
# Find where the worker's shared libs live
WHEEL_LIB=$(python3 -c "import yr.datasystem,os; print(os.path.dirname(yr.datasystem.__file__))")
log "wheel pkg dir=$WHEEL_LIB"
# candidate lib dirs
LIBDIRS=$(find "$WHEEL_LIB" -name "libjemalloc.so.2" -exec dirname {} \; 2>/dev/null | head -1)
log "libjemalloc dir=$LIBDIRS"
# also locate test-built worker shared lib
WORKER_SO_DIR=$(find bazel-bin -name "libdatasystem_worker_shared.so" -exec dirname {} \; 2>/dev/null | head -1)
log "test-built worker_so dir=$WORKER_SO_DIR"
# Build LD_LIBRARY_PATH
LDLP=""
for d in "$LIBDIRS" "$WORKER_SO_DIR" "$WHEEL_LIB/lib" "$WHEEL_LIB"; do
    [ -n "$d" ] && [ -d "$d" ] && LDLP="$d:$LDLP"
done
log "initial LD_LIBRARY_PATH=$LDLP"
# ldd check with this LD_LIBRARY_PATH, find missing, resolve
MISSING=$(LD_LIBRARY_PATH="$LDLP" ldd /usr/local/bin/datasystem_worker 2>&1 | grep "not found" | awk '{print $1}' | sort -u)
if [ -n "$MISSING" ]; then
    log "missing libs: $MISSING"
    for lib in $MISSING; do
        HIT=$(find bazel-bin "$WHEEL_LIB" /usr/lib /usr/lib64 -name "$lib*" 2>/dev/null | head -1)
        if [ -n "$HIT" ]; then
            D=$(dirname "$HIT"); LDLP="$D:$LDLP"; log "resolved $lib -> $D";
        else
            log "UNRESOLVED $lib";
        fi
    done
fi
log "final LD_LIBRARY_PATH=$LDLP"
# Verify ldd clean
LD_LIBRARY_PATH="$LDLP" ldd /usr/local/bin/datasystem_worker 2>&1 | grep -i "not found" && log "WARN still missing libs" || log "ldd clean (no missing)"

log "=== STAGE 9: WRITE ENV FILE ==="
cat > "$ENV_FILE" <<EOF
export LD_LIBRARY_PATH="$LDLP"
export DS_OPENSOURCE_DIR="$OPENSOURCE_DIR"
export BAZEL_START_OPTS="$BAZEL_START_OPTS"
export BAZEL_CMD_OPTS="$BAZEL_CMD_OPTS"
export BUILD_DIR="$BUILD_DIR"
export REPO_DIR="$REPO_DIR"
EOF
log "env file written: $ENV_FILE"
cat "$ENV_FILE"

log "=== BUILD COMPLETE OK ==="
