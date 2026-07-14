# Tests And Reproduction

## Scope

- Status:
  - `active`
- Last verified against source:
  - `2026-07-10`
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
| `tests/st` | C++ system tests | `tests/st/CMakeLists.txt` builds cache-specific ST binaries, embedded-client tests, device LLT, cluster helpers, and post-build runtime assets. |
| `tests/perf` | Performance helper binaries | `tests/perf/CMakeLists.txt` adds `zmq`; `tests/perf/zmq/CMakeLists.txt` builds ZMQ perf client/server/agent binaries. |
| `tests/common` | Shared test support | `tests/common/CMakeLists.txt` adds `binmock`; `tests/common/binmock` provides function-stub/binmock support and has its own spec test. |
| `tests/python` | Python unittest suites | `scripts/build_cmake.sh` runs `python3 -m unittest` from this directory after packaging and starting services. |
| `tests/benchmark` | Python benchmark script area | Contains standalone benchmark scripts, not part of CTest registration. |
| `tests/kvconnector` | External connector patch/test material | Contains versioned patch/deploy/benchmark material, not part of the main CMake gtest tree. |

- Verified current C++ source scale:
  - `tests/ut`: 124 `.cpp` files, grouped under `client`, `common`, `master`, and `worker`.
  - `tests/st`: 155 `.cpp` files, grouped under `client`, `cluster`, `common`, `device`, `embedded_client`,
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
  - `ds_st_object_cache`: ST files under `**/object_cache`; includes `tests/st/worker/object_cache/coordinator_backend_cluster_test.cpp`, which starts one `datasystem_coordinator` plus two workers without ETCD and verifies coordinator watch propagation across worker restart.
  - `ds_st_kv_cache`: ST files under `**/kv_cache`.
  - `ds_st_embedded_client`: `tests/st/embedded_client` plus cluster helper sources.
  - `ds_device_llt`: device tests; generic hetero ST sources prefer a real runtime backend when
    `DeviceManagerFactory::ProbeBackend()` finds GPU or NPU, and fall back to `AclDeviceManagerMock` only when no
    accelerator backend is detected. Ascend manager self-tests still force the Ascend/mock path when no usable Ascend
    environment is present. Real hetero GPU/NPU builds still copy the matching plugin libraries when enabled.
  - helper tools: `curve_keygen` and `hashring_parser`.
- Verified from `tests/perf/zmq/CMakeLists.txt`:
  - `zmq_perf_client`
  - `zmq_perf_server`
  - `zmq_perf_agent`
- Verified from `tests/common/binmock/CMakeLists.txt`:
  - `binmock`
  - `binmock_spec`
- Transfer Engine HIXL smoke helpers:
  - `transfer_engine/scripts/run_cross_node_smoke_cases.sh`: manual owner/requester wrapper around
    `transfer_engine_cross_node_smoke`.
  - `transfer_engine/scripts/run_hixl_d2d_smoke_suite.sh`: same-node HIXL D2D suite covering batch reads, reverse
    direction, concurrent requesters, unregistered-address rejection, and a 4 x 16 MiB transfer.
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
    send-Jetty pre-fill, acquire/release reuse and pool exhaustion, then triggers `ReCreateJetty` and observes a
    replacement Jetty created by the background refill thread.
  - It needs the same URMA SDK/runtime and configured device:

```bash
DS_URMA_DEV_NAME=<device> \
  bazel test //tests/ut/client:urma_send_jetty_lifecycle_test --config=test --config=urma \
  --test_env=DS_URMA_DEV_NAME --test_output=streamed
```

- Manual URMA local send-Jetty fault coverage:
  - `//tests/ut/client:urma_send_jetty_fault_test` is a separate Bazel `manual` target. It covers manager-level
    pool exhaustion, the recoverable CQE path, async `JETTY_ERR`, and timeout retirement, then verifies background
    refill restores an idle send Jetty after each retirement.
  - It needs the same URMA SDK/runtime and configured device:

```bash
DS_URMA_DEV_NAME=<device> \
  bazel test //tests/ut/client:urma_send_jetty_fault_test --config=test --config=urma \
  --test_env=DS_URMA_DEV_NAME --test_output=streamed
```

- Manual URMA send-Jetty pool system coverage:
  - `//tests/st/client/object_cache:urma_send_jetty_pool_test` starts real workers and clients with URMA enabled.
    It verifies sequential small-pool reuse, one-lane ownership across overlapping objects in a Batch Get with pool
    size 1, configured-capacity concurrent remote Get, and recovery after an injected recoverable CQE retires a send
    Jetty.
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
- KV cache behavior:
  - check `ds_st_kv_cache` and ST paths under `tests/st/client/kv_cache`.
- Stream behavior:
  - check `ds_ut_stream`, `ds_st_stream_cache`, and ST paths under `tests/st/client/stream_cache`.
- Worker embedding or in-process worker startup:
  - inspect and run `ds_st_embedded_client`.
- Cluster, hash-ring, scale, failover, or ETCD behavior:
  - start in `tests/st/client/*_scale*`, `tests/st/worker/object_cache/hash_ring_test.cpp`,
    `tests/st/cluster`, and the owning runtime/cluster module docs.
- Device or hetero behavior:
  - start with `ds_device_llt`, `tests/st/device`, and Python device tests under `tests/python`.
- Transport or RPC behavior:
  - start with `tests/ut/common/rpc`, `tests/st/common/rpc`, and `tests/perf/zmq` for performance-specific checks.

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
