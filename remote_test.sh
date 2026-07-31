#!/bin/bash
# Remote test runner for feat-shm-fdpassing-ci
set -eo pipefail
exec 2>&1

BUILD_ID=feat-shm-fdpassing-ci
BUILD_DIR="$HOME/ougongchang/bazel_build/$BUILD_ID"
REPO_DIR="$BUILD_DIR/repo"
ENV_FILE="$BUILD_DIR/test_env.sh"
LOG_DIR="$BUILD_DIR/test_logs"
mkdir -p "$LOG_DIR"

log(){ echo "[$(date +%H:%M:%S)] $*"; }

log "=== SOURCING ENV ==="
source "$ENV_FILE"
cd "$REPO_DIR"
log "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
export LD_LIBRARY_PATH

# Pass LD_LIBRARY_PATH into the bazel test sandbox/local env
TEST_ENV_FLAG="--test_env=LD_LIBRARY_PATH=$LD_LIBRARY_PATH"

# Sanity: worker present + ldd clean
ls -la /usr/local/bin/datasystem_worker || { log "FATAL: worker missing"; exit 2; }
ldd /usr/local/bin/datasystem_worker 2>&1 | grep -i "not found" && { log "FATAL: worker has missing libs"; exit 2; } || log "worker ldd OK"

run_test() {
    local name="$1"; shift
    local target="$1"; shift
    local filter="$1"; shift
    local out="$LOG_DIR/${name}.log"
    log ">>> RUN $name  target=$target  filter=$filter"
    set +e
    bazel $BAZEL_START_OPTS test "$target" --config=test --config=release $BAZEL_CMD_OPTS \
        --spawn_strategy=local --test_output=all --test_env=LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
        --test_filter="$filter" \
        > "$out" 2>&1
    local rc=$?
    set -e
    log "<<< $name rc=$rc  (log: $out)"
    # Print PASSED/FAILED summary lines from the log
    grep -E "PASSED|FAILED|FAIL|Expected|Actual|elapsed|Elapsed|Time limit|RUN.*OK" "$out" | head -40 || true
    echo "----- tail of $name -----"
    tail -25 "$out"
    echo "----------------------------------------"
    return 0
}

log "=== TEST A: SetTest (3 cases) ==="
run_test "A_settest" "//tests/st/client/kv_cache:kv_client_transport_set_test" \
    "*RoutedSetPublishesDataAndMetadata*:*U1U2DirectWriteReadAtMetadataOwner*:*DefaultPolicyRoutesSetAndMSetToSameNodeWorkers*"

log "=== TEST B: GetTest (3 cases incl Latch diagnose) ==="
run_test "B_gettest" "//tests/st/client/kv_cache:kv_client_transport_get_test" \
    "*NonBoundSameHostWorkerUsesWorkerOcFdPassing*:*BoundWorkerShmDoesNotEnableTargetWorkerShm*:*LatchFailureIsRetryableNotRuntimeError*"

log "=== TEST C: Admission UT (4 cases) ==="
run_test "C_admission" "//tests/ut/client:ub_fault_p1_transport_admission_test" \
    "*DedicatedProbeRestoresClientLocalSenderWithoutBusinessRetry*:*GlobalSnapshotDenyKeepsClientLocalSenderQuarantinedUntilReadmitted*:*RemovedFailureEndpointRecoversThroughAnotherAdmittedWorker*:*FailedRecoveryProbeRotatesAcrossAdmittedWorkers*"

log "=== COLLECT TEST XMLs ==="
find bazel-out -name "*.xml" -path "*testlogs*" 2>/dev/null | while read f; do
    cp "$f" "$LOG_DIR/" 2>/dev/null || true
done
ls -la "$LOG_DIR/"

log "=== ALL TESTS DONE ==="
