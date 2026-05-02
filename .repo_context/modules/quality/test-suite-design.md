# Test Suite Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation
- Primary code paths:
  - `tests`
  - `tests/CMakeLists.txt`
  - `tests/ut/CMakeLists.txt`
  - `tests/st/CMakeLists.txt`
  - `tests/perf/CMakeLists.txt`
  - `tests/common/CMakeLists.txt`
  - `cmake/util.cmake`
  - `cmake/scripts/GoogleTestToCTest.cmake`
  - `scripts/build_cmake.sh`
  - `scripts/build_bazel.sh`
- Primary source-of-truth files:
  - `tests/README.md`
  - `cmake/scripts/GoogleTestToCTest.cmake`
  - `tests/ut/CMakeLists.txt`
  - `tests/st/CMakeLists.txt`
- Last verified against source:
  - `2026-05-02`
- Related context docs:
  - `tests-and-reproduction.md`
  - `build-test-debug.md`
- Related playbooks:
  - `../../playbooks/features/quality/test-implementation.md`

## Purpose

- Why this design document exists:
  - explain how repository tests are discovered, split into binaries, labeled, and run.
- What problem this module solves:
  - gives future bugfix, review, and test-addition work a source-backed model of the test harness.
- Who or what depends on this module:
  - C++ gtest/CTest workflows, Python unittest workflows, example smoke tests, Bazel tests, and module-level validation
    guidance.

## Scope

- In scope:
  - test tree ownership and binary partitioning;
  - CMake/gtest to CTest registration;
  - CTest labels, disabled tests, and serial tests;
  - Python unittest and example smoke-test orchestration;
  - perf helper binaries under `tests/perf`.
- Out of scope:
  - production module behavior under test;
  - CI job definitions outside this repository;
  - external connector patch semantics under `tests/kvconnector`.
- Important boundaries with neighboring modules:
  - `build-test-debug.md` owns broad build flags, sanitizer, and coverage orientation.
  - module docs own module-specific test meaning when a test is part of a recovery, persistence, or compatibility
    contract.

## Current State And Design Choice Notes

- The test tree mirrors both test type and product domains:
  - `ut` and `st` define validation scope;
  - subdirectories such as `client/object_cache`, `client/kv_cache`, `common/rpc`, and `worker/object_cache` mirror
    code ownership.
- C++ tests are not registered one source file at a time. CMake builds a small set of gtest binaries, then generates
  per-case CTest entries from `--gtest_list_tests`.
- Labels are derived from generated CTest metadata rather than hand-written `add_test` calls.
- Python tests are manually orchestrated by `scripts/build_cmake.sh`, not by CTest.
- Bazel mode uses `bazel test ... //...` and does not reuse the CMake-generated CTest files.

## Architecture Overview

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| Top-level test tree | Routes CMake into UT, ST, perf, and shared helpers | `tests/CMakeLists.txt` | Adds `ut`, `st`, `perf`, and `common`. |
| UT CMake | Builds unit/component gtest binaries | `tests/ut/CMakeLists.txt` | Splits stream, object, slot-store, and flags tests out of the default `ds_ut` bucket. |
| ST CMake | Builds system-test binaries and runtime helpers | `tests/st/CMakeLists.txt` | Splits stream, object, KV, embedded-client, and device tests; generates runtime data and helper tools. |
| Perf CMake | Builds ZMQ performance helpers | `tests/perf/zmq/CMakeLists.txt` | Produces client, server, and agent binaries. |
| Common helpers | Provides binmock support | `tests/common/binmock` | Builds `binmock` and `binmock_spec`. |
| Test registration function | Converts gtest binaries into CTest cases | `cmake/util.cmake` | `ADD_DATASYSTEM_TEST` writes include files and invokes `GoogleTestToCTest.cmake`. |
| GTest-to-CTest script | Derives CTest names, labels, disabled state, and serial state | `cmake/scripts/GoogleTestToCTest.cmake` | Runs each binary with `--gtest_list_tests`. |
| CMake test runner | Runs examples, Python tests, and CTest | `scripts/build_cmake.sh` | Retries failed CTest cases with lower parallelism. |
| Bazel test runner | Runs Bazel tests | `scripts/build_bazel.sh` | Uses `bazel test --config=test //...`. |
| ST port allocator | Coordinates service ports across concurrent ST processes | `tests/st/cluster/test_port_allocator.*`, `scripts/modules/llt_util.sh` | Uses per-port `flock`, bind probing, lease metadata, and bounded stale cleanup. |

## Main Flows

### C++ CMake Test Registration

1. `tests/CMakeLists.txt` adds the test subdirectories.
2. `tests/ut/CMakeLists.txt` and `tests/st/CMakeLists.txt` collect source files and split them into gtest binaries.
3. Each gtest binary calls `add_datasystem_test`.
4. `ADD_DATASYSTEM_TEST` creates a post-build command that runs `cmake/scripts/GoogleTestToCTest.cmake`.
5. The script executes the binary with `--gtest_list_tests`.
6. The script writes generated CTest `add_test` and `set_tests_properties` commands.
7. CTest includes the generated files through directory `TEST_INCLUDE_FILES`.

Failure-sensitive steps:

- test binaries must be runnable at build time so `--gtest_list_tests` succeeds;
- runtime library paths in `TEST_ENVIRONMENT` must include worker, OpenSSL, gRPC, optional observability, UCX, and URMA
  paths when those builds are enabled;
- generated CTest files must exist before CTest discovery.

### CMake Test Execution

1. `build.sh` parses `-t`, `-l`, `-u`, `-m`, `-c`, and sanitizer/build options.
2. `scripts/build_cmake.sh::run_cmake_testcases` calls example, Python, and CTest runners according to `RUN_TESTS`.
3. `run_ut` invokes `ctest --schedule-random --parallel ${TEST_PARALLEL_JOBS}` with label include/exclude filters.
4. On failure, `run_ut` retries failed tests with lower parallelism, then once more with output on failure.

Failure-sensitive steps:

- `RUN_TESTS=run_cpp` excludes Java labels but still relies on generated CTest metadata.
- `RUN_TESTS=run_python` requires Python packaging and service startup.
- `RUN_TESTS=run_example` requires the packaged tarball and example assets.

### Python Test Execution

1. `run_manual_ut` extracts the package from `output`.
2. For `run_python`, it installs the generated wheel with `python3 -m pip install ... --force-reinstall`.
3. It starts services through `start_all`.
4. It runs `python3 -m unittest` in `tests/python`.
5. It stops services through `stop_all`.

Failure-sensitive steps:

- Python tests are not pure import tests; they assume running DataSystem services.
- Hetero-related Python tests can depend on `BUILD_HETERO`.

### ST Service Port Allocation

- C++ ST cases must allocate service ports through allocator-backed `ExternalClusterTest::GetFreePort()` or the
  default `ExternalClusterTest::SetDefaultOptions()` batch allocation path.
- The allocator stores state under `/tmp/datasystem-st-ports-${UID}` by default, or
  `/tmp/datasystem-st-ports-${UID}-${DS_ST_PORT_NAMESPACE}` when a namespace is configured.
- Allocated ports hold a per-port `flock` for the test-process lifetime and write lease metadata under `leases/` for
  DFX and stale cleanup.
- Socket bind probing is the authority for whether a candidate port is reusable; stale lease metadata alone is not
  proof that a port is free.
- Shell/Python service-start tests in `scripts/modules/llt_util.sh` use the same state directory and keep bash lock
  file descriptors open until `stop_all` or the cleanup trap releases them.
- New ST helper code should not choose service ports with ad hoc random selection plus `netstat` parsing.

### Bazel Test Execution

1. `build.sh -b bazel` selects the Bazel build path.
2. `scripts/build_bazel.sh` adds test configs when tests are enabled.
3. `run_bazel_testcases` runs `bazel test` with `--config=test`, `--jobs`, `--test_timeout`, and `//...`.

Failure-sensitive steps:

- Bazel target coverage depends on nested `BUILD.bazel` files, not CMake `add_datasystem_test`.
- Do not assume a CTest label has a one-to-one Bazel label.

## Compatibility And Invariants

- CTest case names intentionally strip leading test-control prefixes for readability.
- The original gtest filter still includes the prefixed suite/test name, so renaming prefixes changes generated metadata.
- `DISABLED_` only triggers the disabled CTest property when it starts the suite or test name.
- `EXCLUSIVE_` triggers `RUN_SERIAL TRUE` unless the test is disabled.
- `LEVEL1_` and `LEVEL2_` can appear on suite or test names.
- Executable path/name controls `object`, `stream`, `ut`, and `st` labels.
- Adding a test under a filtered directory can move it into a different binary from nearby files.

## Validation Guidance

- For local C++ behavior:
  - use the narrowest relevant gtest binary and `ctest -R` when possible.
- For cross-process behavior:
  - prefer ST binaries because they include cluster helpers, service processes, or generated runtime data.
- For Python API behavior:
  - use `bash build.sh -t run_python` when validating the full packaged service path.
- For example compatibility:
  - use `bash build.sh -t run_example`.
- For build-system changes:
  - validate both CMake and Bazel paths when the touched file has both `CMakeLists.txt` and `BUILD.bazel` coverage.

## Pending Verification

- Curated fast subsets per major module are still pending.
- Nested Bazel test target taxonomy is still broader than this design note records.
