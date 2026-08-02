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

For detailed CMake build-system knowledge, including supported source-build platform boundaries, third-party
dependencies, compiler flags, install outputs, and target graph notes, read `cmake-build/README.md` and
`cmake-build/design.md`.

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
- the SDK directory includes `cpp/BUILD.bazel`, all SDK headers under `cpp/include/datasystem/`, and the client, worker, and coordinator shared libraries under `cpp/lib/`;
- `//bazel:datasystem_wheel` includes `yr/datasystem/datasystem_worker`, `yr/datasystem/datasystem_coordinator`, root worker/cluster/coordinator configs, the Python package, CLI assets, and `yr/datasystem/lib/` shared libraries;
- `scripts/build_bazel.sh` stages the coordinator executable, config, and shared library under `datasystem/service` before creating the deployment tar, matching the CMake service package layout.

Backed by `.bazelrc`, `bazel/workspace_status.sh`, `bazel/git_version.bzl`, and `src/datasystem/common/util/BUILD.bazel`:

- Bazel builds run `bazel/workspace_status.sh` through `--workspace_status_command` to expose `STABLE_GIT_HASH`
  and `STABLE_GIT_BRANCH`;
- `//src/datasystem/common/util:git_version_def` generates `git_version_def.h`, which defines `GIT_HASH` and
  `GIT_BRANCH` for Bazel-built code while CMake builds continue using compile definitions from `CMakeLists.txt`;
- worker startup logging and `datasystem::GetGitHash()` share the same generated Git version macros in Bazel builds.
- the `@datasystem_sdk_validation` source SDK repository is registered by `bazel/sdk/workspace.bzl` with
  `repository_ctx.symlink()` for source-tree entries and does not require host `rsync`.

Backed by `tests/kvtest/BUILD.bazel` and `tests/kvtest/build.sh`:

- `tests/kvtest` is a standalone KVClient performance tool with its own `build.sh` supporting `-b cmake|bazel`,
  mirroring the main repo's build-system switch;
- the CMake mode links against a pre-installed SDK (`-s/--sdk`, default `../../output/cpp`), while the Bazel mode
  builds `//tests/kvtest:kvtest` against the in-tree `//src/datasystem/client:datasystem`, producing a self-contained
  binary that does not need `libdatasystem.so` at runtime;
- `tests/kvtest/Makefile` `package` tolerates a missing `third_party/sdk/` so the same packaging step serves both
  modes (cmake ships SDK libs alongside; bazel ships only the fat binary);
- the control plane (Notify/Stats/Stop/Summary) is dual-transport via a `KVTEST_USE_BRPC` compile-time switch:
  bazel defines it and serves brpc (`src/rpc/brpc_server.cpp` + `kvtest_control.proto` + `src/rpc/peer_client.cpp`
  brpc channel/stub); cmake keeps the legacy httplib endpoints (`src/rpc/http_server.cpp`). The notify dispatch
  logic is shared in `src/rpc/notify_dispatcher.cpp`. The brpc server uses brpc restful mappings to PRESERVE the
  legacy HTTP paths (`/stats` `/stop` `/summary` `/notify`, `allow_default_url=false`), so external curl/scripts
  work unchanged; the C++ peer client (`BrpcPeerClient`) uses typed `KvtestControl::Stub` over binary protobuf.
  The SDK does not ship brpc/protobuf/gflags headers, so cmake mode cannot use brpc until the SDK packages
  third-party dev headers.

Backed by `tests/kvtest/deploy_coordinator.py`, `deploy_worker.py`, `deploy_common.py`, and `deploy_pods.py`:

- `deploy_coordinator.py` has two coordinator lifecycle entrypoints: `start` (start coordinators on already-running
  pods matched by `-p/--prefix`, single-node no-election mode) and `deploy` (full lifecycle: bring up N pods via
  `deploy_pods`, install the datasystem whl, then start N coordinators);
- `deploy_coordinator.py deploy --instances N` spreads the N pods across the cluster nodes discovered by
  `deploy_common.discover_nodes()` (balanced round-robin; the per-node distribution is not exposed on the CLI) and
  reuses `deploy_pods.cmd_deploy` with a computed `--replicas` string;
- for `N >= 2`, `deploy` injects `coordinator_raft_initial_peers` (full member list, including self) into each pod's
  config so the coordinators run static-peers Raft election; for `N == 1` the peers field is left untouched
  (single-node no-election mode, matching `start`). The config flag passed to dscli is role-aware:
  `dscli start -C <cfg>` for coordinators vs `dscli start -f <cfg>` for workers;
- the shared kubectl/procmon/whl-install/parallel orchestration lives in `deploy_common.py`; `deploy_pods.py` is the
  standalone pod-bringup CLI (`deploy`/`delete`/`status`).

## Environment Notes

Backed by `build.sh` and current docs:

- CANN is optional but needed for Ascend hetero-related features.
- `rdma-core` is optional but needed for RDMA support.
- Python 3.9+ is expected by current docs and build packaging flows.

## Pending Verification

- The fastest minimal command set for local iteration on a single submodule still needs module-level documentation.
- Service boot, smoke-test, and deployment debug flows should be split into dedicated context files later.
