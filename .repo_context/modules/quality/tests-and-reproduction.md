# Tests And Reproduction

## Scope

- Status:
  - `active`
- Last verified against source:
  - `2026-07-18`
- Canonical source roots:
  - `tests`
  - `tests/README.md`
  - `tests/CMakeLists.txt`
  - `tests/ut/CMakeLists.txt`
  - `tests/st/CMakeLists.txt`
  - `tests/perf/CMakeLists.txt`
  - `tests/common/CMakeLists.txt`
  - `cmake/util.cmake`
  - `cmake/scripts/GoogleTestToCTest.cmake`
  - `build.sh`
  - `scripts/build_cmake.sh`
  - `scripts/build_bazel.sh`
- Why this module exists:
  - record the real test entrypoints already used by the repository;
  - help bugfix and review work jump to the right binary, label, and scenario class quickly;
  - preserve reproduction conventions in one stable place.

## Module Boundary Assessment

- Canonical boundary:
  - `quality.tests-and-reproduction` owns repository-wide test selection, test registration, and reproduction guidance.
- Sibling split assessment:
  - no new sibling module is required for `tests` as a whole today because the top-level test tree is a validation
    layer, not a production runtime module with its own persisted format or service lifecycle.
  - module-specific test families should still be referenced from their owning module docs when they are part of that
    module's design or recovery story, such as slot and l2 cache tests.
- Design/playbook assessment:
  - the CMake/gtest/CTest registration rules are stable enough to need a design note:
    `test-suite-design.md`.
  - adding or selecting tests is a recurring feature/review workflow, so use
    `../../playbooks/features/quality/test-implementation.md`.

## Test Layout

- Verified current top-level structure:

| Path | Role | Source-backed notes |
| --- | --- | --- |
| `tests/ut` | C++ unit and component tests | `tests/ut/CMakeLists.txt` builds the main UT binaries and filters stream/object/slot/flags tests into separate targets. |
| `tests/st` | C++ system tests | `tests/st/CMakeLists.txt` builds cache-specific ST binaries, embedded-client tests, device LLT, cluster helpers, the standalone braft election test, and post-build runtime assets. |
| `tests/perf` | Performance helper binaries | `tests/perf/CMakeLists.txt` adds `client`; `tests/perf/client/CMakeLists.txt` builds `peer_ub_admission_timeout_bench`. |
| `tests/common` | Shared test support | `tests/common/CMakeLists.txt` adds `binmock`; `tests/common/binmock` provides function-stub/binmock support and has its own spec test. |
| `tests/python` | Python unittest suites | `scripts/build_cmake.sh` runs `python3 -m unittest` from this directory after packaging and starting services. |
| `tests/benchmark` | Python benchmark script area | Contains standalone benchmark scripts, not part of CTest registration. |
| `tests/kvconnector` | External connector patch/test material | Contains versioned patch/deploy/benchmark material, not part of the main CMake gtest tree. |

- Verified current C++ source scale:
  - `tests/ut`: 197 `.cpp` files, grouped under `client`, `common`, `master`, and `worker`.
  - `tests/st`: 156 `.cpp` files, grouped under `client`, `cluster`, `common`, `device`, `embedded_client`,
    `master`, and `worker`.

## CTest Registration Model

- `tests/CMakeLists.txt` adds `ut`, `st`, `perf`, and `common`.
- `cmake/util.cmake` defines `ADD_DATASYSTEM_TEST`.
- Each registered gtest binary runs `--gtest_list_tests` at build time through
  `cmake/scripts/GoogleTestToCTest.cmake`.
- The generated CTest names strip leading `DISABLED_`, `EXCLUSIVE_`, `LEVEL1_`, and `LEVEL2_` prefixes from the
  displayed suite/test name while preserving the original gtest filter for execution.
- Label derivation is based on executable path/name and prefixes:
  - executables with `object` or `kv` in the path/name receive the `object` label;
  - executables with `stream` in the path/name receive the `stream` label;
  - executables under `tests/ut` receive `ut`;
  - executables under `tests/st` receive `st`;
  - tests with `LEVEL1_` receive `level1*`; tests with `LEVEL2_` receive `level2*`; other enabled tests receive
    `level0*`.
- Disabled tests are marked `DISABLED TRUE`.
- Exclusive tests are marked `RUN_SERIAL TRUE`.

## Naming And Labeling Rules

- Verified from `tests/README.md`:
  - `DISABLED_`: disabled test
  - `EXCLUSIVE_`: test should not run concurrently with others
  - `LEVEL1_`: tagged as `level1`
- Verified from `cmake/scripts/GoogleTestToCTest.cmake`:
  - `LEVEL2_` is also parsed and labeled as `level2*`, even though `tests/README.md` only documents `LEVEL1_`.
  - `DISABLED_` must be the leading prefix to trigger the disabled property.
  - `EXCLUSIVE_`, `LEVEL1_`, and `LEVEL2_` can appear in the suite or test name.
- Common labels and filters:
  - `ut`
  - `st`
  - `object`
  - `stream`
  - `level0`
  - `level1`
  - `level2`
  - `level*`
  - combined forms such as `st level0`

## Main Commands

- Build tests:

```bash
bash build.sh -t build
```

- Build and run all tests:

```bash
bash build.sh -t run
```

- Build and run with parallel jobs:

```bash
bash build.sh -t run -u 8
```

- Build and run a label:

```bash
bash build.sh -t run -l level0
```

- Run already-built labeled tests:

```bash
bash build.sh -t run_cases -l ut
bash build.sh -t run_cases -l st
bash build.sh -t run_cases -l "st level0"
```

- Run C++ tests only:

```bash
bash build.sh -t run_cpp -l "object level0"
```

- Run Python tests only:

```bash
bash build.sh -t run_python
```

- Run example smoke tests only:

```bash
bash build.sh -t run_example
```

- Run the transfer_engine same-node HIXL D2D smoke suite after building with `-X on` in an Ascend/HIXL environment
  where `build.sh` auto-enabled HIXL:

```bash
export TRANSFER_ENGINE_HIXL_ROUTE=hccs
export TRANSFER_ENGINE_HIXL_BASE_PORT=21000
transfer_engine/scripts/run_hixl_d2d_smoke_suite.sh
```

Override `OWNER_DEVICE`, `REQUESTER_DEVICE`, and `REQUESTER_DEVICE_STEP` when the smoke should select devices
explicitly, for example on validation hosts that need zero-based device selection:

```bash
export TRANSFER_ENGINE_HIXL_ROUTE=hccs
export TRANSFER_ENGINE_HIXL_BASE_PORT=21000
OWNER_DEVICE=0 REQUESTER_DEVICE=1 REQUESTER_DEVICE_STEP=1 \
  transfer_engine/scripts/run_hixl_d2d_smoke_suite.sh
```

Use the same suite for A3-oriented runs by passing `OWNER_DEVICE`, `REQUESTER_DEVICE`, and
`REQUESTER_DEVICE_STEP` explicitly when a specific device pair is required.
The HIXL suite defaults `TRANSFER_ENGINE_ACL_MALLOC_POLICY=huge_only` so smoke-owned HBM allocations satisfy HIXL
HCCS D2D registration requirements; set it to `huge_first` to reproduce the older allocation behavior.
The smoke requester explicitly registers its HIXL read-destination buffers before `BatchTransferSyncRead`; this mirrors
the production TransferEngine contract that receiver-driven read destinations are registered by the caller.

- Run a single generated CTest case from the build directory:

```bash
ctest -R test_suite.test_name
```

- Run Python unittest directly after services are available:

```bash
cd tests/python
python3 -m unittest
```

## Test Binaries Worth Knowing

- Verified from `tests/ut/CMakeLists.txt`:
  - `ds_ut`: default UT bucket after excluding device, binmock, flags, slot store, stream cache, and object cache files.
    URMA-specific client UTs such as `tests/ut/client/urma_send_lane_test.cpp` are excluded when `BUILD_WITH_URMA` is
    off because their headers require the URMA SDK path.
  - `ds_ut_stream`: UT files under `**/stream_cache`.
  - `ds_ut_object`: UT files under `**/object_cache`.
  - `ds_ut_slot_store`: `tests/ut/common/l2cache/slot_store_test.cpp`.
  - `flags_ut`: `tests/ut/common/flags/flags_test.cpp`.
- Verified from `tests/st/CMakeLists.txt`:
  - `ds_st`: default ST bucket after excluding cluster, stream, object, KV, embedded-client, device, and helper files.
  - `ds_st_stream_cache`: ST files under `**/stream_cache`.
  - `ds_st_object_cache`: default CTest-registered ST files under `**/object_cache`, excluding the manual Coordinator backend cluster suite; the remaining topology ST coverage uses the ETCD backend.
  - `ds_st_urma_numa_inflight_balance` and `ds_st_urma_numa_inflight_balance_round_robin`: separate URMA-Mock-only
    object-cache ST processes for round-robin-with-affinity and pure round-robin source-chip policies with inflight
    feedback. Process isolation is required because the first registered Worker freezes the Client policy. Each target
    starts three Workers and eight logical Clients with 16 threads each, performs concurrent 8 MiB Set plus ten
    same-key Gets, and checks the selected policy on the Client and every Worker, override observations, both selected
    chips, and full
    payload equality. Because Mock remaps anonymous registered memory onto memfd-backed VMAs, the ST uses test-only
    affinity and atomic decision injection for deterministic hard-depth and free-affinity decisions; NUMA range
    construction and two-arena allocation are verified separately by focused UTs.
  - `ds_st_coordinator_backend_manual`: manually executed CMake target for `coordinator_backend_cluster_test.cpp`; it covers Coordinator-backed cross-worker access, scale-out, graceful/passive scale-in, Worker restart topology propagation, Coordinator lease-path isolation, all-Worker shutdown and restart with the Coordinator continuously running followed by topology and fresh business-operation recovery, single- and multi-target Witness protection, and protected-then-real-failure removal closure, but is intentionally not registered with default CTest. Bazel exposes the matching `coordinator_backend_cluster_test` target with the `manual` tag.
  - `ds_st_kv_cache`: ST files under `**/kv_cache`.
  - `ds_st_embedded_client`: `tests/st/embedded_client` plus cluster helper sources.
  - `ds_device_llt`: device tests; generic hetero ST sources prefer a real runtime backend when
    `DeviceManagerFactory::ProbeBackend()` finds GPU or NPU, and fall back to `AclDeviceManagerMock` only when no
    accelerator backend is detected. Ascend manager self-tests still force the Ascend/mock path when no usable Ascend
    environment is present. Real hetero GPU/NPU builds still copy the matching plugin libraries when enabled.
  - `braft_cluster_test`: standalone three-node braft election coverage. It reuses the ST `CommonTest` path fixture and
    port allocator without linking the full worker/master ST graph.
  - helper tool: `curve_keygen`.
- Run the braft election case after building the matching backend:

```bash
# CMake build directory
ctest -R 'BraftClusterTest\.ThreeNodesElectOneLeader' --output-on-failure

# Repository root
bazel test --config=release --config=test \
  //tests/st/common/raft:braft_cluster_test \
  --test_output=all \
  '--test_filter=*.ThreeNodesElectOneLeader'
```

## Recommendation workload trace replay

- `tests/st/client/kv_cache/kv_client_eviction_rebalance_telemetry_test.cpp` remains self-contained and deterministic by
  default. When `DS_TLM_KUAIRAND_TRACE` names a normalized KuaiRand trace, its eviction/rebalance recall and fine-rank
  cases replay real exposure order/popularity while retaining the fixed key placement and object-size distribution.
- Create the bounded trace with `tests/st/client/kv_cache/tools/prepare_kuairand_trace.py`; usage, source checksum,
  engagement definition, format, and dataset-license notes are in `tests/st/client/kv_cache/tools/README.md`.
- The external dataset and generated trace are intentionally not repository or CI inputs. Recall uses all exposures;
  fine-rank uses engaged events as a proxy because KuaiRand-Pure has no complete candidate slate.
- This manual LEVEL1 workload overrides `ClusterTest::GetTestCaseTimeoutSecs()` with
  `max(80, DS_TLM_MEASURE_SEC + 60)` so a 60-second measurement is not aborted by the normal 80-second end-to-end
  ST guard after cluster startup, warm-up, and pressure setup. Other ST fixtures retain the 80-second default.
- The same source also provides `EXCLUSIVE_LEVEL1_KVClientCombinedWorkloadTelemetryTest`, a three-Worker manual
  topology that enables spill eviction, distributed master, data replication, keep-local-copy, and rebalance at the
  same time. It writes 400 resident objects per Worker, applies the remaining pressure only through worker0, and maps
  KuaiRand popularity ranks round-robin onto keys owned by all three Workers. The older eviction-only and
  rebalance-only fixtures retain contiguous popularity mapping for historical reproducibility.
- The ARM-calibrated combined default uses 48 MiB of physical shared memory per Worker and test-local 75%/65%
  eviction watermarks. The split keeps copy-on-Get/migration headroom while holding the Clock/Memory recall resident
  set near a 70% request-weighted memory-hit rate. `DS_TLM_COMBINED_MEMORY_MB` remains a calibration-only override.
- Heat workload fixtures use test-only 15s/15s primary/local half-lives so strategy comparisons share the same decay
  horizon. Production defaults and public configuration are unchanged.
- The allocator-size-normalized Heat validation keeps that topology, trace, capacity, watermarks, and decay horizon
  fixed. On ARM, `HeatEvictionListTest.*:HeatEvictionTest.*` covers the 4 KiB access-credit unit and exact-ranking
  boundary; the final 60-second x3 combined Recall run reached 89.990%, 90.300%, and 90.047% request-weighted memory
  hit rate with zero Get errors/wrong values and both mechanisms observed. Raw per-second CSV is archived under the
  sibling `zcode/eviction-rebalance-telemetry/csv/heat-size-credit-final-20260806/` directory.
- Set `DS_TLM_OUTPUT_DIR` to a persistent directory when per-round CSV artifacts must survive fixture teardown. If it
  is unset, the CSV remains under the cluster root and follows the existing temporary-lifecycle behavior.
- `EXCLUSIVE_LEVEL1_KVClientBlockRecallWorkloadTelemetryTest.RecallBlockWorkload` is an additional, independent
  three-Worker recall workload; the existing eviction-only, rebalance-only, and combined cases are unchanged. One
  logical request issues a single vector Get for 16 Block keys. Defaults are 64 tokens per Block and 12 KiB per token,
  hence 768 KiB per Block and exactly 12 MiB per logical KVC request. The geometry is test-configurable through
  `DS_TLM_BLOCKS_PER_REQUEST`, `DS_TLM_TOKENS_PER_BLOCK`, and `DS_TLM_BYTES_PER_TOKEN`.
- The Block recall case is disabled in routine CTest/CI discovery because its 10,000-request calibration warm-up and
  pressure setup exceed the normal 80-second ST budget and require an isolated performance runner. Invoke the
  `DISABLED_RecallBlockWorkload` GoogleTest case explicitly with `--gtest_also_run_disabled_tests` for manual runs.
- The Block workload applies a default 1:64 resource scale to both object payload and Worker memory: 12 KiB/token
  becomes 192 B/token and 70 GiB/node becomes 1,120 MiB/Worker. Three simulated nodes therefore offer 211 logical
  requests/s (`ceil(4500 * 3 / 64)`), preserving about 253 MiB/s of modeled 30%-miss traffic per node. Overrides are
  `DS_TLM_BLOCK_RESOURCE_SCALE`, `DS_TLM_BLOCK_MEMORY_MB`, `DS_TLM_TARGET_REQUEST_QPS`, and
  `DS_TLM_BLOCK_REQUEST_THREADS`.
- The KuaiRand mapping covers all 7,552 unique videos using 7,680 logical KVCs instead of truncating and renormalizing
  the top 32. A deterministic 10,000-request full-trace warm-up and 800-KVC low-frequency scan are excluded from
  measured deltas. During measurement, 64 entirely new KVCs/s model the 30% miss/refill stream; cold-write attempts,
  successes, and errors are reported separately and do not enter the Get hit-rate denominator.
  `DS_TLM_BLOCK_OWNER_AFFINITY=true`
  is a diagnostic mode that routes each KVC to its owning POD, matching per-node spill discoverability; the default
  distributed-copy mode retains the cross-Worker requester-local hit-rate comparison. The two modes must not be
  combined into one result: owner affinity removes the metadata-discovery failure mode but changes the capacity
  oracle and hit-rate distribution.
- The separate `block_summary_*.csv` preserves the old columns and appends payload-scale, modeled miss-bandwidth,
  warm-up, and cold-write fields. With 60-second measurement at the default 1:64 geometry, ARM measured Clock/Memory
  at 71.4557% and Heat/Heat at 79.4866%, both at the full 211 request/s with zero Get/value/cold-write errors and both
  eviction and rebalance observed. After the migration target-recycle fix, a 2026-08-10 formal rerun measured
  71.6393% / 79.1920% under the same geometry and correctness conditions; Heat remained +7.5527pp over Clock and both
  contracts passed. Subsequent requested experiments measured 71.0638% at 120s/80s and 72.9836% at 120s/120s; both
  intentionally failed the unchanged >=79% contract, while QPS, correctness, eviction, and rebalance remained healthy.
  The Block fixture therefore uses the previously validated 600s/300s window again. The product defaults were then
  aligned to 600s/300s; deployments that explicitly pass either flag remain unchanged.
  A 2026-08-12 current-worktree rebuild and single formal rerun measured Clock/Memory at 71.4701% and Heat/Heat at
  79.2024%, both at 211 requests/s and 3,376 Block Gets/s with zero errors and both mechanisms observed. The sanitized
  CSV is under the sibling `zcode/eviction-rebalance-telemetry/csv/block-current-code-retest-20260812/` directory.
  This is a one-run confirmation, not a variance-qualified three-round matrix. Do not weaken correctness checks or
  reinterpret mem+remote as a memory hit. The 68%-72% Clock and >=79% Heat contracts are enabled only for the calibrated 60-second,
  10,000-warm-up, 800-scan, 64-cold-write/s geometry; diagnostic overrides, including explicit zero scan/write, retain
  correctness and mechanism checks without applying the calibrated hit-rate contract.
  A test-only 2026-08-13 extension appends cold/warm Primary bytes and within-Primary ratios to a read-only snapshot
  refreshed before periodic Worker-to-Master resource reporting. Clock classifies counter 0/1/>=2 as cold/warm/hot;
  Heat retains the current
  `<eviction threshold` / inclusive middle / `>rebalance hot threshold` definitions. The Block comparison and sanitized
  CSV are under the sibling `zcode/eviction-rebalance-telemetry/block-cold-warm-hot-comparison-20260813.md` record.

- Verified from `tests/perf/client/CMakeLists.txt`:
  - `peer_ub_admission_timeout_bench`
- Verified from `tests/common/binmock/CMakeLists.txt`:
  - `binmock`
  - `binmock_spec`
- Transfer Engine HIXL smoke helpers:
  - `transfer_engine/scripts/run_cross_node_smoke_cases.sh`: manual owner/requester wrapper around
    `transfer_engine_cross_node_smoke`.
  - `transfer_engine/scripts/run_hixl_d2d_smoke_suite.sh`: same-node HIXL D2D suite covering batch reads, reverse
    direction, concurrent requesters, unregistered-address rejection, and a 4 x 16 MiB transfer.
- URMA send-lane state-machine coverage:
  - `//tests/ut/client:urma_send_lane_test` is hardware-independent and covers lane settlement, force-release
    preconditions and idempotency, retirement precedence, request-generation ownership, and first-timeout context
    publication. It needs no configured URMA device:

```bash
bazel test //tests/ut/client:urma_send_lane_test --config=test --test_output=streamed
```

- Manual URMA remote-Jetty reuse coverage:
  - `//tests/ut/client:urma_remote_jetty_reuse_test` is a Bazel `manual` target, deliberately separate from the
    header-only `urma_send_lane_test`.
  - It contains separate peer-backed cases: one performs the peer handshake twice through `ExchangeJfr` and verifies
    that both responses publish the same shared receive Jetty; the other validates multi-chunk `UrmaWriteImpl` lease
    reuse, completion-before-seal behavior, one release, and one retirement on injected failure.
  - Run it only on a host with the URMA SDK/runtime and a configured device, for example:

```bash
DS_URMA_DEV_NAME=<device> \
  bazel test //tests/ut/client:urma_remote_jetty_reuse_test --config=test --config=urma \
  --test_env=DS_URMA_DEV_NAME --test_output=streamed
```

- Manual URMA local send-Jetty lifecycle coverage:
  - `//tests/ut/client:urma_send_jetty_lifecycle_test` is a separate Bazel `manual` target. It verifies real
    send-Jetty pre-fill, acquire/release reuse and pool exhaustion, then triggers `RetireJetty` and observes a
    replacement Jetty created by the background refill thread.
  - It needs the same URMA SDK/runtime and configured device:

```bash
DS_URMA_DEV_NAME=<device> \
  bazel test //tests/ut/client:urma_send_jetty_lifecycle_test --config=test --config=urma \
  --test_env=DS_URMA_DEV_NAME --test_output=streamed
```

- URMA Jetty admission-gate coverage:
  - `//tests/ut/client:urma_jetty_gate_test` is a deterministic state-machine unit target. It uses no provider
    handle and checks the shared close/admission linearization point, concurrent permits, exactly-once finalizer
    scheduling, early/late `FLUSH_ERR_DONE`, and fail-closed quarantine.
  - It uses the real URMA build configuration but does not access a provider handle or require a configured device:

```bash
bazel test //tests/ut/client:urma_jetty_gate_test --config=test --config=urma --test_output=streamed
```

- URMA request-ID generation coverage:
  - `//tests/ut/client:urma_req_id_test` verifies contiguous low-40-bit request IDs, continuity across the 20-bit
    boundary, wraparound at the 40-bit boundary, and truncation of the unused high 24 input bits.
  - It uses the URMA build configuration but does not access a provider handle or require a configured device:

```bash
bazel test //tests/ut/client:urma_req_id_test --config=test --config=urma --test_output=streamed
```

- Manual URMA local send-Jetty fault coverage:
  - `//tests/ut/client:urma_send_jetty_fault_test` is a separate Bazel `manual` target. It covers manager-level
    pool exhaustion, repeated recoverable status-9 CQEs, non-recoverable CQE no-rebuild/no-leak behavior, and async
    `JETTY_ERR`. It also verifies that a timed-out write retains its Event for a late status-4 CQE while releasing the
    foreground waiter, drives a real business Event timeout, verifies immediate force release once timeout and seal are
    both observed, covers both timeout-before-seal and seal-before-timeout ordering, reuses the same local Jetty for a
    replacement request generation, and proves the old CQE does not consume the replacement lane's WR count.
    Orphan-pressure cases verify that 17 outstanding orphan WRs keep the Jetty reusable while emitting pressure DFX,
    and that the fixed threshold of 32 triggers asynchronous retirement plus pool refill. Refill/capacity assertions
    use unique registry Jetty identities; retirement installs one pending record synchronously rather than maintaining
    overlapping counters. The non-recoverable case drives the production CQE path, transport-owned lane completion,
    and business Event notify/wait-delete before reacquiring the same Jetty.
  - It needs the same URMA SDK/runtime and configured device:

```bash
DS_URMA_DEV_NAME=<device> \
  bazel test //tests/ut/client:urma_send_jetty_fault_test --config=test --config=urma \
  --test_env=DS_URMA_DEV_NAME --test_output=streamed
```

- URMA client failover ST setup:
  - `KVClientUrmaFailoverTest` uses `ExternalCluster::KillWorker()` when removing workers to construct remote discovery
    and failover scenarios. These cases validate service-discovery and URMA data-plane recovery rather than graceful
    worker shutdown, so their setup must not enter or depend on the URMA provider destruction path.

- Manual URMA send-Jetty pool system coverage:
  - `//tests/st/client/object_cache:urma_send_jetty_pool_test` starts real workers and clients with URMA enabled.
    Its admission-gate regression forces multi-chunk writes, pauses the second chunk after permit acquisition, injects
    the real ABI status-9 ACK timeout for the first CQE, and verifies `modify` cannot start until the paused permit
    is released.
    It verifies sequential small-pool reuse, one-lane ownership across overlapping ordinary-write and GatherWrite
    objects in a Batch Get with pool size 1, shared-lane cleanup after an injected GatherWrite failure, TCP-only
    fallback for ordinary and aggregate Batch Get when the sole lane is held, rejection when that TCP fallback payload
    reaches the limiter's 1 MiB exclusive upper bound, and fallback-disabled repeated pool backpressure without object
    WR post or TCP success. The fallback-disabled KVClient assertion expects an eventual `K_URMA_TRY_AGAIN` error rather
    than TCP success; the dedicated status prevents replaying exhausted URMA lanes as generic application
    `K_TRY_AGAIN`, while the manager fault UT checks the exact acquire error. Provider-side admission coverage injects
    CQE status 9 on a real remote GET writeback, keeps the requester quarantined during the assertion window, and
    verifies that subsequent ordinary and aggregate Batch Gets either carry the complete payload over TCP or return
    `K_URMA_WORKER_UNAVAILABLE` when fallback is disabled, without acquiring an URMA send lane. The target also covers
    configured-capacity concurrent remote Get, a manual `LEVEL1_` 64 concurrently started Batch Get × 64 ordinary sub-object scenario with exactly 64 lane releases and zero
    observed pool exhaustion (without claiming all lanes were simultaneously held), recovery after an injected
    recoverable CQE retires a send Jetty, and `LEVEL1_ConcurrentBatchGetsRecoverFromInFlightTimeoutStorm`: timeout
    releases each sealed lane while retaining observer-backed write Events. The timeout-storm case pauses production
    CQE classification, verifies timed-out Events are not deleted before polling resumes, then observes their cleanup
    after the late CQEs and verifies the same lanes remain usable.
  - `//tests/ut/worker:worker_oc_service_impl_test` drives the real single-object `GetObjectRemote(serverApi)` entry with
    a non-writable requester summary. It verifies that admission bypasses the unstable-connection check, pins the
    request to a complete TCP payload when fallback is enabled, and returns `K_URMA_WORKER_UNAVAILABLE` without an URMA
    post when fallback is disabled.
  - `UrmaClientSenderRecoveryTest.LateCqe4AfterSetTimeoutQuarantinesSender` pauses a real client-side Set CQE, forces
    the foreground wait to time out, rewrites that same delayed CQE to status 4, and verifies the next Set fast-fails
    with `K_URMA_WORKER_UNAVAILABLE` without reaching another URMA write.
  - It needs the same URMA SDK/runtime and configured device:

```bash
DS_URMA_DEV_NAME=<device> \
  bazel test //tests/st/client/object_cache:urma_send_jetty_pool_test --config=test --config=urma \
  --test_env=DS_URMA_DEV_NAME --test_output=streamed
```

## Python And Example Tests

- Python tests live under `tests/python`:
  - `test_ds_client.py`
  - `test_oc_client.py`
  - `test_kv_cache_client.py`
  - `test_sc_client.py`
  - `test_device_oc_client.py`
  - `test_ds_tensor_client.py`: NPU-only tensor paths require `BUILD_HETERO_NPU=on` with ACL/torch_npu or MindSpore
    dependencies; GPU tensor coverage requires `BUILD_HETERO_GPU=on`, PyTorch, and a usable CUDA device, then runs real
    CUDA tensors through DsTensorClient D2H/H2D paths.
  - `prefetch_tests/test_multi_key_prefetch.py`
- `scripts/build_cmake.sh` runs Python tests by:
  - extracting the packaged tarball under `output`;
  - optionally installing the wheel for `run_python`;
  - starting services through `start_all`;
  - running `python3 -m unittest` in `tests/python`;
  - stopping services through `stop_all`.
- `scripts/modules/llt_util.sh` now reserves service ports through the shared ST port lease directory rather than
  random `netstat` probing; shell cleanup traps release held `flock` file descriptors on normal exit or interruption.
- `tests/python/prefetch_tests/README.md` documents a manual prefetch path:

```bash
cd PATH_TO_ROOT
bash tests/python/prefetch_tests/start_worker.sh
cd tests/python
python -m unittest test_multi_key_prefetch.TestDeviceOcClientMethods.test_device_put_and_get
```

- Example smoke tests are launched by `scripts/build_cmake.sh::run_example`, which extracts the package, sanitizes
  `LD_LIBRARY_PATH`, and runs `example/run-example.sh`.

## Reproduction Guidance

- For ST failures that mention bind/listen/readiness issues, inspect allocator diagnostics under
  `/tmp/datasystem-st-ports-${UID}`:
  - `events.log` records reserve, release, stale cleanup, skipped candidate, and quarantine events.
  - `leases/*.json` records role, test name, owner pid, root dir, and child pids for currently leased ports.
- Public client API behavior:
  - start with `tests/ut/client` or the relevant common/client UT when the behavior is local and isolated.
  - move to `tests/st/client/*_cache` when behavior depends on client-worker-master interaction.
- Object cache behavior:
  - check `ds_ut_object`, `ds_st_object_cache`, and ST paths under `tests/st/client/object_cache` or
    `tests/st/worker/object_cache`.
  - `MasterClientRecoveryDfxTest.TestGetAfterMasterRestartReRegistersClient` covers the old Object client Get
    request racing a restarted master Worker before its in-memory client registration is restored; run it with the
    existing `MasterDfxTest.TestMasterCrashAndGet` when changing client-worker recovery or Get retry behavior.
- KV cache behavior:
  - check `ds_st_kv_cache` and ST paths under `tests/st/client/kv_cache`.
  - `kv_client_heat_functional_workload_test.cpp` contains Heat-only 8-Worker/4-Client functional workloads. The
    bursty-QPS case uses a fixed seed, defaults to 10,000 QPS for 15 seconds, exports per-client QPS and scheduler task
    directions, and treats reverse directions only as ping-pong candidates because task logs do not expose object
    IDs. `IdleTargetsEvictColdPrimariesAndContinueReceiving` is enabled: clients write only to workers 0--3 and the
    test requires workers 4--7 to receive migrated primaries, spill cold data, and complete another active-to-idle
    migration after spill begins. It uses `NONE_L2_CACHE_EVICT`, so each Set is immediately read/value-checked but
    historical keys are not required to remain readable after intentional end-life eviction.
  - `KVCacheClientExistTest.TestBatchSizeLimit` verifies the 100,000-key `Exist` boundary while other batch APIs
    remain capped at 10,000; `KVCacheClientExistTest.TestConcurrentLargeBatch` runs three clients concurrently with
    32,768 missing keys each and logs the aggregate wall-clock latency.
- Stream behavior:
  - check `ds_ut_stream`, `ds_st_stream_cache`, and ST paths under `tests/st/client/stream_cache`.
- Worker embedding or in-process worker startup:
  - inspect and run `ds_st_embedded_client`.
- Topology, scale, failover, or ETCD behavior:
  - start with `tests/ut/cluster`, `tests/st/client/*_scale*`, `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`,
    `tests/st/cluster`, and `.repo_context/modules/runtime/topology/README.md`.
  - cluster-backed ST fixtures use an 80-second watchdog by default. Hardware-bound fixtures can override
    `ClusterTest::GetTestCaseTimeoutSecs()` locally when their startup, shutdown, or recovery lifecycle requires a
    longer bound; keep the shared default unchanged.
- Device or hetero behavior:
  - start with `ds_device_llt`, `tests/st/device`, and Python device tests under `tests/python`.
- Transport or RPC behavior:
  - start with `tests/ut/common/rpc` and `tests/st/common/rpc` for RPC-specific checks.
  - `//tests/ut/common/rpc:fanout_collector_test` covers the deadline-bound fanout collector used by brpc
    `AsyncRead(DONTWAIT)` migrations: partial completion, first-error aggregation, timeout cleanup, cleanup retry, late
    completion, and concurrent completion.

## Build-System Facts That Affect Reproduction

- `build.sh -t run` builds and runs C++ CTest tests, Python tests, and example smoke tests when the matching packages
  are enabled.
- `build.sh -t run_cases` runs existing CTest/Python/example paths without treating the action as a pure build.
- `build.sh -t run_cpp` runs CTest and excludes Java labels.
- `build.sh -t run_python` runs the Python unittest path.
- `build.sh -t run_example` runs example smoke tests.
- `scripts/build_cmake.sh::run_ut` invokes `ctest --schedule-random --parallel ${TEST_PARALLEL_JOBS}` and retries
  failed tests with lower parallelism before failing.
- `scripts/build_bazel.sh::run_bazel_testcases` runs `bazel test ... //...` whenever Bazel mode and tests are enabled.
- Coverage is available through `-c on|html`.
- Sanitizer modes are available through `-S address|thread|undefined`.
- `tests/ut` and `tests/st` link against main client, worker, master, common, and persistence libraries, so regressions
  often cross module boundaries.

## Review Notes

- Common risks:
  - adding tests to the wrong binary can hide required runtime dependencies;
  - label mistakes change CI/runtime behavior because labels are derived from naming conventions;
  - disabled and exclusive prefixes affect generated CTest properties, not just names;
  - some ST binaries generate runtime assets and helper tools as post-build steps;
  - Python tests need packaged artifacts plus running services, not only importable source files.
- Useful places to inspect when a test "should exist" but is hard to find:
  - `tests/ut/CMakeLists.txt`
  - `tests/st/CMakeLists.txt`
  - `cmake/scripts/GoogleTestToCTest.cmake`
  - `scripts/build_cmake.sh`
  - `build.sh`

## Pending Verification

- The most representative smoke-test subsets for quick local iteration are not yet curated per production module.
- The Bazel test target taxonomy under nested `BUILD.bazel` files should be deepened in a later pass.
