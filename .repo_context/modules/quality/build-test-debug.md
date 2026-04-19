# Build, Test, And Debug

This document records repository-wide build and test entrypoints that are already checked into the repository.

## Build Entrypoints

Primary build script:

- `build.sh`

Primary build configuration:

- `CMakeLists.txt`
- `src/datasystem/CMakeLists.txt`
- `tests/CMakeLists.txt`
- `WORKSPACE`
- `.bazelrc`

## Default Build Facts

Backed by `build.sh` and `CMakeLists.txt`:

- default build mode is `Release`;
- default build directory is `./build`;
- default output directory is `./output`;
- C++ standard is `C++17`;
- Python API builds by default;
- Java API and Go API are optional;
- hetero build is enabled by default and can be disabled with `-X off`;
- tests are only added when test build options are enabled.

## Common Commands

Build release artifacts:

```bash
bash build.sh
```

Build without hetero features:

```bash
bash build.sh -X off
```

Build tests only:

```bash
bash build.sh -t build
```

Build and run tests:

```bash
bash build.sh -t run
```

Run tests in parallel:

```bash
bash build.sh -t run -u 8
```

Run tests for a label:

```bash
bash build.sh -t run -l level0
```

Run already-built tests only:

```bash
bash build.sh -t run_cases -l ut
```

Run a single CTest case:

```bash
ctest -R test_suite.test_name
```

## Test Taxonomy

Backed by `tests/README.md`:

- `tests/ut`: unit tests
- `tests/st`: system tests
- `tests/perf`: performance-related tests
- `tests/common`: shared test helpers and assets

GTest/CTest prefixes:

- `DISABLED_`: disabled
- `EXCLUSIVE_`: must not run concurrently with others
- `LEVEL1_`: tagged as `level1`

Common labels:

- `ut`
- `st`
- `level0`
- `level1`
- `level*`

## Debug And Safety-Relevant Build Facts

Backed by `CMakeLists.txt`:

- warnings are treated as errors with `-Werror`;
- release builds enable hardening flags such as `-fstack-protector-strong`, `-Wl,-z,relro`, `-Wl,-z,now`, `-Wl,-z,noexecstack`, `-D_FORTIFY_SOURCE=2`, and `-fPIE -pie`;
- sanitizers are supported through `build.sh -S address|thread|undefined`;
- coverage mode is supported through `build.sh -c on|html`.

Backed by `bazel/BUILD.bazel` and `bazel/datasystem_sdk.bzl`:

- `//bazel:datasystem_sdk` emits both `bazel-bin/bazel/datasystem_sdk` and `bazel-bin/bazel/datasystem_sdk.tar`;
- the SDK directory includes `cpp/BUILD.bazel`, all SDK headers under `cpp/include/datasystem/`, and `cpp/lib/libdatasystem.so`.
- `//bazel:datasystem_wheel` includes the `yr/datasystem/datasystem_worker` binary in the wheel payload alongside the Python package, CLI/config assets, and `yr/datasystem/lib/` shared libraries.

## Environment Notes

Backed by `build.sh` and current docs:

- CANN is optional but needed for Ascend hetero-related features.
- `rdma-core` is optional but needed for RDMA support.
- Python 3.9+ is expected by current docs and build packaging flows.

## Pending Verification

- The fastest minimal command set for local iteration on a single submodule still needs module-level documentation.
- Service boot, smoke-test, and deployment debug flows should be split into dedicated context files later.
