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
- sanitizers are supported through `build.sh -S address|thread|undefined|address_undefined`;
- coverage mode is supported through `build.sh -c on|html`.

Backed by `tools/tsan/BUILD.bazel`, `.bazelrc` (`build:tsan`), `bazel/build_defs.bzl`, and the
`datasystem_worker` / `datasystem_coordinator` binary targets:

- ThreadSanitizer builds (`bazel build --config=tsan`, `bazel test --config=tsan`) link
  `//tools/tsan:default_suppressions` into every `ds_cc_test` and both server binaries. That
  translation unit defines `__tsan_default_suppressions()`, which the TSAN runtime calls at
  process startup to obtain compiled-in suppression entries. Suppressed scope is intentionally
  narrow: only known-benign third-party (brpc 1.15.0 / bthread M:N scheduler) init-time races
  that TSAN's happens-before model cannot precisely model. `datasystem::*` races are NOT
  suppressed, so real concurrency bugs in datasystem code still abort tests/binaries under TSAN.
- `tools/tsan/brpc_suppressions.txt` mirrors the same entries in runtime-file form. Operators
  who need to add more suppressions set `TSAN_OPTIONS=suppressions=<path>`; TSAN merges
  env-supplied suppressions on top of the compiled-in baseline, it does not replace them.
- The `.bazelrc` `build:tsan` block deliberately preserves TSAN's default `halt_on_error=1`
  so real races still fail tests; only the brpc/bthread third-party init races are silenced by
  the compiled-in baseline.
- The worker also pre-warms brpc's global init on the main thread before any RPC path runs;
  see `modules/runtime/worker-runtime.md` Startup section.

### ThreadSanitizer platform support

Verified by building and running `bazel build --config=tsan //src/datasystem/worker:datasystem_worker`
on both x86_64 and aarch64:

- **x86_64: SUPPORTED.** Worker starts successfully under TSAN with `enable_urma=false`,
  zero TSAN races reported, stable steady-state operation. Required runtime setup:
  - `setarch x86_64 -R` (disable ASLR) — without it TSAN aborts with "unexpected memory mapping"
    because ASLR places the shared-memory mmap into TSAN's shadow region.
  - `TSAN_OPTIONS='history_size=1:force_seq_cst=0'` — reduces TSAN shadow-memory overhead.
  - `shared_memory_size_mb` reduced to ≤256 — minimises mmap pressure on TSAN shadow region.
  - `enable_urma=false` — URMA device-memory mappings conflict with TSAN shadow region.
- **aarch64: NOT SUPPORTED.** The worker segfaults inside `libtsan.so.2` during brpc's
  internal health-check / timer tasks. Two architectural root causes make aarch64 + brpc +
  TSAN fundamentally incompatible:

  1. **brpc globally overrides `pthread_mutex_lock`** (`bthread/mutex.cpp`). TSAN detects
     mutex acquisition by intercepting the standard `pthread_mutex_lock`, but brpc's override
     bypasses the interceptor. Result: TSAN sees all brpc-internal mutex-protected code as
     "unprotected" and reports false data races. `usercode_in_pthread=true` does NOT help
     because brpc's INTERNAL tasks (HealthCheckTask, TimerThread, bvar SamplerCollector)
     still use bthread::Mutex regardless of that flag.

  2. **bthread uses custom-assembly fiber context switching** (`bthread_make_fcontext`,
     similar to boost::context's fcontext_t), not POSIX `swapcontext`. TSAN can only
     intercept `swapcontext` — it cannot track custom-assembly fiber stack switches.
     After a fiber switch, TSAN's per-pthread ThreadState holds a stale stack pointer;
     the next memory access computes a shadow address that lands in unmapped memory →
     `ThreadSanitizer: SEGV`. This only manifests on aarch64 (shadow memory layout
     conflicts with fiber stacks); x86_64's shadow layout does not conflict, so the
     fiber switch is tolerated.

- This is a **known brpc community issue** with no upstream fix:
  - [apache/brpc#2864](https://github.com/apache/brpc/issues/2864) (Open, 2025-01):
    brpc overrides `pthread_mutex_lock`, TSAN can't recognise internal mutex → false races.
  - [apache/brpc#3295](https://github.com/apache/brpc/issues/3295) (Open, 2026-05):
    aarch64 + openEuler + brpc 1.16, exact same races and SEGV as observed here
    (`add_vlog_site`, `TimerThread::Bucket::schedule`, `bthread_make_fcontext`).
  - [apache/brpc#1687](https://github.com/apache/brpc/issues/1687) (Closed without fix,
    2022-01): `bvar::detail::AgentCombiner` linked-list race under TSAN.

  The theoretical fix — integrating Clang's TSAN fiber API
  (`__tsan_switch_to_fiber`) into bthread's context-switch code — requires
  modifying brpc source and has not been done by the brpc community.

- **Recommendation for aarch64**: use `bazel build --config=asan` (AddressSanitizer) instead
  of TSAN. ASAN does not track fiber stacks and does not SEGV. For concurrency verification
  on aarch64, rely on code review and unit tests; use x86_64 TSAN as the automated
  concurrency-safety gate.

### AddressSanitizer platform support

Backed by `.bazelrc` (`build:asan`, `build:asan_ubsan`):

- ASan on aarch64 + brpc 1.15.0 needs a single per-file exclusion in
  `.bazelrc` for `external/com_github_apache_brpc/src/bthread/context.cpp`.
  brpc's `context.cpp` is a pure inline-assembly TU (one giant `__asm(...)`
  block per supported arch, defining `bthread_jump_fcontext` and
  `bthread_make_fcontext`). On aarch64 + `-fsanitize=address`, GCC emits
  an ASan module-init constructor into `.init_array` whose relocation
  targets a local symbol that the linker then discards together with its
  hosting section, breaking the link of `datasystem_coordinator` /
  `datasystem_worker` with:
    ```
    bthread/context.pic.o(.init_array.00099+0x0): error: relocation refers
      to local symbol "" [N], which is defined in a discarded section
    ```
  The file contains no C/C++ stack frames, globals, or heap access, so ASan
  has nothing to instrument; disabling ASan on this single file is safe and
  sufficient. The exclusion is expressed with the same `--per_file_copt`
  mechanism already used to disable UBSan on `external/.*`:
    ```
    build:asan       --per_file_copt=external/.*bthread/context\.cpp@-fno-sanitize=address
    build:asan_ubsan --per_file_copt=external/.*bthread/context\.cpp@-fno-sanitize=address
    ```
  The regex `external/.*bthread/context\.cpp` (rather than a repo-name-pinned
  `external/com_github_apache_brpc/.*...`) is intentional: brpc 1.15.0 is
  registered under TWO external repo names in this workspace. The main build
  uses `@com_github_apache_brpc` (from `bazel/sdk/repositories.bzl`
  `setup_brpc` default name), but `bazel/sdk/workspace.bzl`'s
  `datasystem_source_sdk` rule declares a `repo_mapping` that re-exposes
  brpc as `@ds_brpc` inside the `@datasystem_sdk_validation` source SDK
  repository (`bazel/sdk/deps.bzl:34` calls `setup_brpc(name="ds_brpc")`).
  Both instances emit the same `src/bthread/context.cpp` and both hit the
  same `.init_array.00099` link error. Pinning the regex to one repo name
  lets the other repo's link silently regress; the broad pattern also
  covers any future brpc repo_mapping additions without further edits.
  `build:ubsan` standalone needs no equivalent because UBSan is already
  disabled on all external repos via `--per_file_copt=external/.*@
  -fno-sanitize=undefined`.
- The CMake build path does not need the equivalent exclusion because
  `cmake/external_libs/brpc.cmake` builds brpc with `THIRDPARTY_SAFE_FLAGS`
  (defined in `cmake/util.cmake`, excludes `SANITIZER_FLAGS`), so brpc is
  never ASan-instrumented under CMake. Only the Bazel path applies
  `-fsanitize=address` globally via `--copt`, which is what triggers the
  brpc `context.cpp` link error on aarch64 without the per-file exclusion.

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
- local CUDA and URMA repository rules read declared environment variables through
  `repository_ctx.os.environ.get()` so the same Starlark works with Bazel 6.5 and 7.x; keep each variable in the
  repository rule's `environ` list so value changes still invalidate and refetch the external repository.
- the local Ascend repository declares the HIXL and transitive MetaDef public headers on `hixl_plugin_sdk`; an
  `includes` path alone does not make those headers available inside Bazel sandbox actions. Keep the repository rule
  local so changes to the detected CANN installation refresh the generated targets.
- the generated HIXL plugin hash header is exposed through the dedicated `hixl_plugin_sha256_header` C++ target. Its
  package-level strip prefix preserves the source include name `hixl_plugin_sha256.h` inside Bazel sandbox actions;
  keep it as a conditional dependency rather than adding the raw genrule output to `remote_h2d_manager.hdrs`.

Backed by `tests/kvtest/BUILD.bazel` and `tests/kvtest/build.sh`:

- `tests/kvtest` is a standalone KVClient performance tool with its own `build.sh` supporting `-b cmake|bazel`,
  mirroring the main repo's build-system switch; **bazel is the default** (so the brpc control plane and
  bthread-backed pipeline/notify-pool workers are enabled out of the box);
- the CMake mode links against a pre-installed SDK (`-s/--sdk`, default `../../output/cpp`), while the Bazel mode
  builds `//tests/kvtest:kvtest` against the in-tree `//src/datasystem/client:datasystem`, producing a self-contained
  binary that does not need `libdatasystem.so` at runtime;
- kvtest `build.sh -M on` mirrors the root build entrypoint for Bazel builds by adding `--config=urma`; it defaults
  to `off`, validates `on|off`, and rejects `-M on` with CMake because that mode consumes an already-built SDK
  whose compile-time transport capabilities cannot be changed by the kvtest build;
- the CMake mode further exposes a build-time backend switch via `option(KVTEST_USE_BRPC)` (default ON): when ON,
  `tests/kvtest/CMakeLists.txt` reuses the main repo's `cmake/util.cmake` + `cmake/external_libs/{absl,zlib,openssl,
  leveldb,gflags,protobuf,brpc}.cmake` to download and build brpc/protobuf/gflags/absl into `$DS_OPENSOURCE_DIR`
  (cached; first build 5-10min, subsequent seconds), then **statically links** all third-party archives into the
  kvtest binary (`KVTEST_BUILD_STATIC=ON`) — same brpc+bthread behavior as bazel mode, with no SDK packaging changes
  and no SDK symbol-export changes. Static linking avoids vtable interposition conflicts with libdatasystem.so's
  exported `_ZTV*` symbols. When OFF (`--use-httplib`), keeps the legacy httplib control plane + `std::thread`
  workers (no third-party deps, no network downloads).
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
- the data-plane pipeline threads (`KVWorker::PipelineLoop`, `CacheReader::ReaderLoop`) use the same
  `KVTEST_USE_BRPC` switch via `src/common/bthread_compat.h`: bazel mode spawns bthreads
  (`bthread_start_background`) backed by brpc's M:N worker pool, with `bthread::Mutex` / `bthread_rwlock_t` /
  `bthread::ConditionVariable` for hot-path synchronization and `bthread_usleep` for QPS-rate idling — a SDK
  Set/Get RPC wait inside a bthread yields the bthread instead of holding a pthread. CMake mode keeps `std::thread`
  + `std::mutex` / `std::shared_mutex` / `std::condition_variable` so the pre-installed SDK (no brpc headers)
  still builds unchanged. The same `bthread_compat.h` abstraction backs `src/common/thread_pool.h`, so
  `KVWorker::notifyPool_` and `NotifyDispatcher::notifyPool_` workers are bthreads in bazel mode too — peer-notify
  offload and the async notify-pipeline yield the bthread on `brpc::Channel::CallMethod` / SDK Set/Get instead of
  holding a pthread. The `ThreadPool` Submit / Stop / QueueSize contract and bounded-concurrency behavior are
  preserved verbatim in both modes; the kvtest unit tests (`tests/kvtest/tests/cxx/test_thread_pool.cpp`) run in
  cmake mode so they exercise the `std::thread` path unchanged.

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
