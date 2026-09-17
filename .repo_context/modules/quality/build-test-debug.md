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
- CMake and Bazel packages generate `yr/datasystem/jemalloc_build_config.py` from the same Python template. Its
  `JEMALLOC_PROF_ENABLED` field follows `SUPPORT_JEPROF` for CMake and `//:enable_jemalloc_prof` for Bazel, allowing
  dscli to reject profiling configuration without runtime allocator or shared-library probing.

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
- Bazel Worker builds always link and package an unprefixed shared jemalloc as the process allocator. `build.sh -x on`
  selects `.bazelrc`'s `jeprof` configuration, which swaps in an ABI-compatible shared jemalloc built with
  `--enable-prof`; the default build uses the non-profiling variant. Both variants are packaged as
  `service/lib/libjemalloc.so.2`, matching CMake's Worker runtime layout. The profiling build only adds allocator
  capability; runtime sampling remains disabled unless jemalloc receives an explicit `MALLOC_CONF` profiling policy.

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
- kvtest `build.sh -b bazel -x on` selects `--config=jeprof`, links `jemalloc_prof_shared`, and packages
  `libjemalloc.so.2` beside the binary in `lib/` with an origin-relative runtime path. Default `-x off` links and
  packages ordinary `jemalloc_shared`; CMake rejects `-x on`. Startup and `--version` report `jemalloc_prof_supported`,
  queried from the loaded allocator in both Bazel builds. The prefixed static shared-memory allocator remains unchanged;
- `deploy_client.py start/deploy --jemalloc_prof_conf` normalizes `MALLOC_CONF`, checks remote binary capability, and prepares
  the profile directory before either launcher or nohup startup. Default prefix is
  `<output_dir>/jemalloc/kvtest_<instance_id>`; when logging output is unspecified, profiling deployments fix a generated
  `metrics_<instance_id>_<timestamp>` directory in the uploaded config. Explicit prefixes override that default.
  Paths are interpreted on the target, and the binary's bundled jemalloc takes precedence over remote SDK libraries.
  Client installs rebuild a dedicated managed `allocator_lib/` under the host lock and prepend only that directory,
  not a persistent SDK-containing `lib/`, when a remote SDK is selected.
  Install owns runtime uploads; start only uploads instance configuration and launches. Deploy stops on install failure
  and propagates start failures to the CLI exit status;
- `//tests/kvtest:jemalloc_prof_test` checks relocated binary loading and actual heap output with `--config=jeprof`,
  and checks ordinary shared jemalloc without profiling with `--define=enable_jemalloc_prof=false`. Python regressions
  are in `tests/kvtest/tests/python/test_jemalloc_prof.py` and `test_build_sh.py`;
  the shared shell test checks actual malloc/free bindings to the relocated jemalloc via glibc LD_DEBUG.
  `test_jemalloc_runtime.py` covers a non-interposing DSO negative case and real SDK-library selection after redeploy;
- `coordinator_test` and `worker_test` use the same `jemalloc_prof_capability` dependency as `kvtest`, including
  allocator selection and origin-relative runtime lookup. All three binaries report actual capability at startup;
  standalone `--version` exits successfully. `coordinator_jemalloc_prof_test` and `worker_jemalloc_prof_test` reuse the
  relocated-binary/profile-dump test. The build entrypoint rejects missing or unlinked outputs for any of the three tools.
  Standalone service profiling is enabled via `MALLOC_CONF` before launch;
- `deploy_worker.py` and `deploy_coordinator.py` accept `start/deploy --jemalloc_prof_conf` for standalone services.
  They share `deploy_common.normalize_jemalloc_prof_conf` with `deploy_client.py`; the default service prefix is
  `<log_dir>/jemalloc/<binary_name>_<port>`, using the config after overrides. Missing log_dir requires explicit prof_prefix.
  Capability and directory checks run on the target before either launcher or nohup receives MALLOC_CONF.
  Explicit profiling on an already-running tool fails with a restart instruction. Expected standalone preflight
  failures return a per-Pod failure for batch aggregation. Shared normalization rejects disabled process/thread sampling
  and validates known profiling booleans and sampling/interval exponents.
  Coordinator dscli mode rejects the option because dscli only supports Worker profiling.
  Tests are in `tests/kvtest/tests/python/test_standalone_jemalloc_prof.py`;
- The kvtest README versions the allocator comparison baseline as `kvtest-bazel-allocator-v0` before this PR,
  `kvtest-bazel-jemalloc-v1` for ordinary shared jemalloc, and `kvtest-bazel-jemalloc-prof-v1` for profiling.
  Reports must retain tool/build/allocator/config identity; no cross-baseline performance equivalence is claimed.
  Rollback restores the complete archived v0 tool package, not merely `-x off` or a replacement DSO;
- optional NUMA support uses `HAS_LIBNUMA` as the single compile-time gate. `tests/kvtest/build.sh` enables the Bazel
  `kvtest_numa` setting only after a compiler probe can include `numa.h` and link `-lnuma`; CMake likewise requires
  both the header and library. A direct Bazel build without that setting keeps NUMA calls compiled out, even when a
  host happens to expose `numa.h`, so compile and link decisions cannot diverge;
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
- pipeline-mode write concurrency is `num_threads` (default 4); `num_total_threads` (default 16) bounds the combined
  Set/Get concurrency, and `NotifyDispatcher::notifyPool_` derives its read-worker count as
  `num_total_threads - num_threads`. When a Pipeline config explicitly sets only `num_threads`, kvtest derives
  `num_total_threads` as twice that write-thread count; configs that omit both fields retain the 4/16 defaults.

Backed by `tests/kvtest/deploy_coordinator.py`, `deploy_worker.py`, `deploy_common.py`, and `deploy_pods.py`:

- `deploy_coordinator.py` has two coordinator lifecycle entrypoints: `start` (start coordinators on already-running
  pods matched by `-p/--prefix`; injects `coordinator_raft_initial_peers` when 2+ pods match so a multi-instance
  cluster can run static-peers Raft election, single-pod stays in single-node no-election mode; skips pods that
  already have a live `datasystem_coordinator` process so passing every cluster prefix restarts only the stopped
  members, each still carrying the full peer list for Raft rejoin) and `deploy` (full lifecycle: bring up N pods via
  `deploy_pods`, install the datasystem whl, then start N coordinators);
- `deploy_coordinator.py deploy --instances N` spreads the N pods across the cluster nodes discovered by
  `deploy_common.discover_nodes()` (balanced round-robin; the per-node distribution is not exposed on the CLI) and
  reuses `deploy_pods.cmd_deploy` with a computed `--replicas` string;
- for `N >= 2`, both `start` and `deploy` inject `coordinator_raft_initial_peers` (full member list, including
  self) into each pod's config via the shared `_inject_raft_initial_peers` helper so the coordinators run
  static-peers Raft election; for `N == 1` the peers field is left untouched (single-node no-election mode). The
  config flag passed to dscli is role-aware: `dscli start -C <cfg>` for coordinators vs `dscli start -f <cfg>` for
  workers;
- the shared kubectl/procmon/whl-install/parallel orchestration lives in `deploy_common.py`; `deploy_pods.py` is the
  pod-bringup CLI (`deploy`/`delete`/`status`) and reuses `deploy_common.discover_nodes` (the single canonical
  kubectl-get-nodes helper, sorted by node name for deterministic cross-run distribution) rather than a local copy,
  so it depends on `deploy_common` for that one helper; everything else (kubectl transport, manifest apply/wait/delete)
  stays self-contained. `deploy_pods.py deploy` takes one mutually-exclusive distribution flag: `--replicas
  "ip:count,..."` (explicit per-node), `--replicas-pct "PCT:COUNT,..."` (percentage of discovered nodes each get COUNT
  pods, rounded by the Largest Remainder Method so the assigned node count matches exactly; nodes sorted by name then
  assigned contiguously to each bucket), or `--pods-per-node N` (uniform); the default is 1 pod per discovered node.
  Node discovery, spec parsing, percentage rounding, and IP validation live in `cmd_deploy` (via the pure helpers
  `parse_replicas_pct` / `distribute_nodes_by_percentage`; `discover_nodes` is imported from `deploy_common`);
  `generate_pod_manifest` only renders pod specs from a pre-computed `{node_ip: count}` plan. `deploy_pods.cmd_deploy`
  is also called by `deploy_coordinator.cmd_deploy` via a hand-rolled `SimpleNamespace` (no `replicas_pct` field), so
  `cmd_deploy` reads `replicas_pct` with `getattr` to tolerate that caller. `deploy_pods.py` is covered by
  `tests/kvtest/tests/python/test_deploy_pods.py` (pure-helper coverage of parse / distribute / rounding / manifest
  rendering and `cmd_deploy` wiring with kubectl mocked); `discover_nodes` is covered in
  `tests/kvtest/tests/python/test_deploy_common.py` since it lives in `deploy_common`.
- procmon (process watchdog) defaults to **disabled** across all three role CLIs: `deploy_worker.py` /
  `deploy_coordinator.py` `--enable-procmon` (argparse `default=False`) and `deploy_client.py` `deploy.json`
  `enable_procmon` (fallback `False`, `gen-config` writes `False`); opt in explicitly with `--enable-procmon` /
  `"enable_procmon": true`. Samples are written to `resource_monitor.csv`; `parse_resource.py` converts that CSV
  to a self-contained interactive HTML report with exact nearest-sample hover and click locking. When
  `brpc_enable_builtin_services` is enabled in the service config, procmon also probes BRPC `/vars/anon_jemalloc_*`
  at the pod IP (the Worker normally binds BRPC to `worker_address`, not loopback) and adds available jemalloc memory
  metrics without disrupting `/proc` collection when the endpoint or metrics are unavailable. When the bvar reports
  `anon_jemalloc_stats_available=0`, procmon records the status counters but leaves memory cells empty because the
  exported byte counters retain their last successful values.
- `deploy_worker.py start` / `deploy` enable Worker heap profiling solely through an explicit
  `--jemalloc_prof_conf <MALLOC_CONF>` value (`--jemalloc-prof-options` remains an alias); there is no separate runtime toggle.
  In dscli mode the option is forwarded to
  `dscli start --jemalloc_prof_conf`, which verifies that the package was built with root `build.sh -x on`, adds
  `prof:true`, and derives `<log_dir>/jemalloc/datasystem_worker` when `prof_prefix` is absent. Standalone mode uses
  the shared tool capability/directory checks and MALLOC_CONF startup environment described above.
  `deploy_common.discover_nodes` now sorts by node name so the same helper serves
  `deploy_pods` percentage distribution and `deploy_coordinator` round-robin spread deterministically.
- `deploy_worker.py start` / `deploy` accept repeatable `--env NAME=VALUE` and pass the pairs to
  `deploy_common.start_service`, which emits them as leading `NAME=<shell-quoted value>` assignments on the
  `sh -c` line that runs `dscli start`. dscli copies its own environment into the service it forks
  (`cli/start.py` uses `env = os.environ.copy()` then `Popen(env=env)`), so the assignments reach the Worker
  binary for settings dscli has no flag for (`ASAN_OPTIONS`, `MALLOC_CONF`, ...). Names are restricted to shell
  identifiers because the shell recognizes a leading assignment only while the name stays unquoted; values are
  `shlex.quote`d and may contain spaces and `=`. Rejected in standalone mode, where `start_service_standalone`
  derives its own `MALLOC_CONF` environment. Note that in dscli mode the Worker's stderr is a pipe owned by the
  `dscli start` process, which exits once the service is ready, so reports written to stderr mid-run are lost;
  sanitizer runs must set `ASAN_OPTIONS=log_path=<log_dir>/asan.%p.log` (a `*.log` name inside the Worker
  `log_dir`) and collect it with `collect`.
- `deploy_common.clean_pod` / `cmd_clean_impl` / `cmd_clean_shared` form the clean pipeline shared by
  `deploy_worker.py cmd_clean` and `deploy_coordinator.py cmd_clean`. Both role CLIs now accept
  `-S/--standalone` + `--remote-dir` on the `clean` subcommand; under `--standalone`, `cmd_clean_shared`
  switches the kill target from the dscli binary name (`datasystem_worker` / `datasystem_coordinator`) to the
  standalone test binary name (`worker_test` / `coordinator_test`) and passes `args.remote_dir` through so
  `clean_pod` issues `rm -rf {remote_dir}` after the `log_dir` + `resource_monitor.csv` cleanups. Without
  `--standalone`, `remote_dir` is left `None` so dscli-mode clean does not touch the package prefix. This
  closes the silent-staleness hole where a re-deploy stacked a new binary on a running stale one and
  `find_pid_by_port` returned the old PID; the `--remote-dir` default matches `install` / `deploy` so a
  clean after a default deploy needs no extra flags. Covered by `test_deploy_common.py`
  (`TestCleanPod`, `TestCmdCleanShared`) and `test_deploy_worker.py` / `test_deploy_coordinator.py`
  (`TestCmdClean` / `TestCmdWiring.test_cmd_clean_*`).
- `deploy_common.clean_pod(keep_binary=True)` / `cmd_clean_impl(keep_binary=True)` /
  `cmd_clean_logs_shared` form the clean-logs pipeline shared by `deploy_worker.py cmd_clean_logs` and
  `deploy_coordinator.py cmd_clean_logs`. The `clean-logs` subcommand mirrors `clean`'s args
  (`--remote-config` / `-S` / `--remote-dir`) and semantics exactly, but in standalone mode
  `clean_pod` issues `rm -f {remote_dir}/stdout.log` instead of `rm -rf {remote_dir}`, preserving the
  standalone binary + `lib/` .so so a re-deploy skips the 100M+ upload on large clusters. `stdout.log`
  is still removed explicitly because the standalone binary appends to it across runs (a no-op cleanup
  would let it stack). In dscli mode (`remote_dir=None`) clean-logs is identical to clean since clean
  never touched the package-prefix install path either -- the verb exists for call-site consistency in
  the deploy → run → clean-logs → start cycle so operators never need to remember "standalone uses
  clean-logs, dscli uses clean". Misuse-prevention: `clean-logs` NEVER removes the binary, so forgetting
  a flag cannot trigger the 100M+ re-upload that a forgotten `--keep-binary` on `clean` would. Covered
  by `test_deploy_common.py` (`TestCleanPodKeepBinary`, `TestCmdCleanImplKeepBinary`,
  `TestCmdCleanLogsShared`) and `test_deploy_worker.py` / `test_deploy_coordinator.py`
  (`TestCmdCleanLogs` / `TestCmdWiring.test_cmd_clean_logs_*`).
- `deploy_client.py Deployer.do_clean_logs` is the client-side clean-logs path (SSH/kubectl transport, not
  the K8s-pod `deploy_common` pipeline). Mirrors `do_clean`'s two-phase kill (TERM then `kill -9`, same
  `pgrep -x kvtest` + `pgrep -x procmon.py` targets) but the single `rm -rf {remote_work_dir}
  /root/.datasystem/logs/` of `do_clean` is replaced by a scoped removal that preserves the install-phase
  artifacts: the `kvtest` binary, `lib/` .so deps, `procmon.py`, `standalone_launcher.py`. Only run-time
  products are removed: `config_*.json` (per-instance, regenerated by `start_node`), `run.log`,
  `metrics_*/` output dirs, `resource_monitor.csv`, and `/root/.datasystem/logs/` SDK logs. The
  `remote_work_dir` layout difference (client puts everything in one dir; worker/coordinator split binary
  in `remote_dir` from logs in `log_dir`) is why this is a client-only implementation rather than another
  `cmd_clean_logs_shared` consumer. A `test -x {remote_work_dir}/kvtest` verify step flags a missing binary
  with a WARNING so the operator runs `install` before the next `start` instead of hitting a confusing
  "kvtest binary not found" failure. Lets a re-deploy skip the ~100MB upload on 2000+ node clusters.
  Covered by `test_deploy_client.py` (`TestDoCleanLogs`: preserves-binary / two-phase-kill-order /
  warns-when-binary-missing / kills-both-kvtest-and-procmon).
- `deploy_client.py` reuses `deploy_common.get_pods` and `deploy_common._print_timings` instead of its
  own private copies. `get_pods` now returns `{'name', 'ip', 'node', 'host_ip'}` per pod: `node`
  (`spec.nodeName`) and `host_ip` (`status.hostIP`) are needed by `deploy_client.cmd_gen_config` to
  spread writer/reader instances across physical nodes and to inject the `HOST_IP` env var; the
  worker/coordinator/jf callers only read `name`/`ip` and ignore the new fields (backward compatible).
  This consolidates two prior private duplicates in `deploy_client.py` (`_get_pods`, which had drifted
  to a 30s timeout vs the common `DEFAULT_TIMEOUT=300`, and `_print_timings`, a verbatim copy) into the
  single `deploy_common` source of truth. `deploy_client` still does NOT reuse the kubectl-only
  `cmd_*_impl` / `cmd_*_shared` orchestration (it has its own 3-transport `run_on`/`scp_to` layer for
  SSH/kubectl/localhost and kvtest-specific kill/collect/HTTP-stop logic); only the pure helpers are
  shared. Covered by `test_deploy_common.py::TestGetPods::test_extracts_node_and_host_ip_for_deploy_client`
  and the existing `test_deploy_client.py::TestGenConfig` (whose `_get_pods` mock was retargeted to
  `get_pods`).
- `deploy_common.collect_logs_from_pod` / `cmd_collect_impl` / `cmd_collect_shared` form the collect pipeline
  shared by `deploy_worker.py cmd_collect` and `deploy_coordinator.py cmd_collect`. Both role CLIs now accept
  `--remote-dir` on `collect` (no `-S` flag) defaulting to the role's install dir (`/tmp/ds_worker` /
  `/tmp/ds_coordinator`); `collect_logs_from_pod` gates stdout.log collection on `ls -d {remote_dir}`
  succeeding, so a dscli-mode pod (which never creates `remote_dir`) skips stdout.log silently while a
  standalone-mode pod (where `start_service_standalone` writes `{remote_dir}/stdout.log`) gets it collected.
  This fixes the prior bug where the code looked for `{remote_config_dir}/stdout.log` (= `/tmp/stdout.log`)
  and never matched the path the launcher actually writes. `cmd_collect_shared` reads `args.remote_dir` via
  `getattr` so older callers without the attr keep working. Covered by `test_deploy_common.py`
  (`TestCollectLogsFromPod`, `TestCollectLogsFromPodTarStream`, `TestCmdCollectShared`).
- Large-cluster collect optimizations (P0-P2): the collect pipeline was unbounded and per-file on
  500-2000 node clusters, causing API server overload (N_files+5 kubectl exec per pod, each a fresh
  TLS+impersonation connection) and base64 33% inflation. Optimizations:
  - `--max-workers` (P0): bounds the `do_for_all_pods` / `deploy_client` ThreadPoolExecutor. Defaults to
    `len(pods)` (unbounded) for backward compat; on large clusters pass e.g. `--max-workers 50` to cap
    concurrent kubectl/ssh processes. Added to worker/coordinator parent parsers and deploy_client collect
    subparser. `cmd_collect_impl` / `cmd_collect_shared` forward `max_workers` to `do_for_all_pods`.
  - `--count`/`--offset` (P0): coordinator parent parser and deploy_client collect subparser now support
    manual batching (worker already had it). `deploy_client.do_collect` accepts `node_slice=(offset, count)`.
  - tar-stream collect (P1): `deploy_common.collect_logs_from_pod` now streams `tar czf - {files}` via one
    `kubectl exec` and extracts locally, replacing the per-file `base64 {file}` loop (N+5 exec → 1). Falls
    back to per-file base64 if the container lacks `tar` (minimal images). `deploy_client._collect_remote_files`
    kubectl path mirrors the SSH path's tar+kubectl exec pattern (was per-file `kubectl exec cat`).
    Eliminates base64 33% inflation + gzip 3-5x compression on text logs.
  - collect stagger (P2): `set_collect_stagger_window` / `_collect_stagger_delay` mirror install's
    `_cp_stagger_window` — a random per-pod delay sized to the batch (`min(30, len(pods)*0.06)`) spreads
    the download wave into overlapping groups.
  - summary pipeline (P2): `deploy_client.do_collect` is now single-phase (per-node summary → collect in
    one bounded thread, no global barrier). Replaces the prior two-phase design where Phase 1 (summary)
    blocked on the slowest node for up to `_SUMMARY_TIMEOUT=60s`. `--summary-timeout` (default 5s) caps
    per-node `/summary` retries; a dead/unreachable node exhausts its own timeout and collects available
    files without stalling other nodes. `_collect_from_all_nodes` removed (was dead code after the merge).
  Covered by `test_deploy_common.py` (`TestCmdCollectShared::test_forwards_max_workers_from_args`,
  `TestCollectLogsFromPodTarStream`) and `test_deploy_client.py` (`TestDoCollect`: single-phase /
  summary-timeout-caps-retries / node-slice).
- start/stop false-positive fix: `deploy_worker.py deploy` / `start` printed "started" and stop printed "OK"
  even when the process had crashed immediately after launch (standalone) or was never running (stop). Three
  fixes in `deploy_common.py`:
  - `stop_service_standalone`: replaced `pkill ... || true` + unconditional `return True` with SIGTERM +
    `pgrep` verify after 3s grace period. Returns False if the process is still alive; returns True if gone
    (whether it was running and exited, or was already absent — idempotent stop). The `|| true` mask is
    removed so pkill's rc is visible (though the final verdict is pgrep, not pkill rc).
  - `start_service_standalone`: added a post-launch `pgrep -f {binary_name}` verify after the launcher /
    nohup path reports a PID. If the PID is gone (process crashed right after readiness), prints FAILED and
    returns False instead of the false "started (pid=...)".
  - `start_service` (dscli): added a post-launch `check_process` (ps aux | grep) verify after `dscli start`
    returns 0. If the process count is 0 (crashed right after dscli readiness), prints FAILED and returns
    False instead of the false "started".
  The non-standalone (dscli) path was already correct for `stop_service` (kubectl_exec check=True raises
  CalledProcessError on dscli stop non-zero). The standalone `stop_service_standalone` bug existed since
  its introduction (`83e794791`). Covered by `test_deploy_common.py` (`TestStopServiceStandalone`:
  process-exits / already-absent / refuses-to-exit / no-or-true-in-pkill; `TestStartServiceStandaloneCrashDetect`:
  crash-detected / started-when-survives; `TestStartService::test_start_fails_when_process_crashes_immediately`).
- standalone_launcher.py start/stop subcommands + pidfile: the launcher was upgraded from a single-action start
  script to a two-subcommand tool (start + stop), mirroring dscli's start/stop structure. The stop loop
  (SIGTERM → 0.2s /proc poll → SIGKILL → kill-wait) runs in-pod via one ``kubectl exec``, eliminating the
  deploy-side external polling that caused 960+ kubectl round-trips on a 32-node client stop and the 3s
  single-shot pgrep on worker stop. Pidfile (``{remote_dir}/{binary_name}.pid``) is the persistent state
  shared between start (writes after Popen) and stop (reads for exact-PID signaling + ``/proc/{pid}/cmdline``
  identity check to guard against PID recycling). Exit codes: 0=exited (incl. idempotent "already gone"),
  1=alive after SIGKILL, 3=no pidfile (deploy falls back to legacy pkill + pgrep polling). Covered by
  ``test_standalone_launcher.py`` (TestPidfileHelpers / TestParseStatState / TestCmdlineMatches /
  TestStopSubcommand: missing-pidfile / already-gone / recycled-pid / sigterm-exit / sigkill-escalation /
  still-alive / TestStopDispatch / TestStartWritesPidfile).
- kvtest binary TERM == /stop alignment (``main.cpp``): ``SignalHandler`` now sets ``gStopRequested`` (atomic)
  in addition to ``gRunning``. The main loop sleeps in 200ms chunks (was 3s) so a TERM is observed promptly.
  On shutdown, if ``gStopRequested`` is set, the main thread calls ``server.StopNow()`` + ``worker->RequestStop()``
  + ``cacheReader->RequestStop()`` — the same immediate stops the /stop handler applies — before the pre-drain
  sleep. This makes SIGTERM and HTTP /stop equivalent (both trigger ``WriteSummary`` via ``metrics.Stop()``
  in teardown). ``HttpServer::StopNow()`` and ``BrpcControlServer::StopNow()`` added as public wrappers around
  ``dispatcher_.StopNow()``. The handler itself only touches atomics (StopNow takes a mutex → not
  async-signal-safe); the mutex-holding calls run on the main thread.
- ``--start-timeout`` / ``--stop-timeout`` CLI parameters: worker/coordinator default 90s start / 180s stop
  (matching dscli); client default 5s / 5s. ``start_service_standalone`` accepts ``start_timeout`` (was
  ``min(timeout, 60)`` hardcoded); ``stop_service_standalone`` accepts ``grace`` (was fixed ``sleep(3)``);
  ``cmd_stop_shared`` standalone branch reads ``args.stop_timeout`` + ``args.remote_dir``. ``deploy_client``
  ``do_stop`` accepts ``stop_timeout``; ``start_node`` passes ``--port {listen_port}`` + ``--pidfile`` +
  ``--ready-timeout {start_timeout}`` to the launcher (was no-signal grace-poll → misleading "not ready
  within 2.0s" on every client). Worker/coordinator ``stop`` subparser gained ``--remote-dir`` (was missing).
- collect tar-stream fix: ``collect_logs_from_pod`` now does a merged ``ls -d`` existence check on optional
  extras (``resource_monitor.csv`` legacy path + ``stdout.log``) before building the tar list, filtering out
  missing files so ``tar`` does not abort with rc≠0. ``_collect_via_tar_stream`` attempts partial extraction
  even when tar rc≠0 but stdout is non-empty. Fixes the 100% "tar stream failed" observed on worker pods where
  ``/tmp/resource_monitor.csv`` did not exist (procmon writes to ``log_dir``, not ``/tmp``).
- `deploy_jf.py` is the JF mock pod lifecycle CLI (deploy/start/stop/check/clean/collect). It is
  self-contained (uses its own `_kubectl_exec`, not the shared `deploy_common` primitives) because the JF
  mock is a single Python script (`mock_jf_server.py`) with no whl, no dscli, and no per-pod config file.
  `cmd_collect` (new) mirrors `deploy_common.collect_logs_from_pod`'s existence-gate pattern: `ls -d
  {remote_dir}` first, then `ls *.log *.txt`, then `base64` each file into `{output}/{pod_name}/`; skips
  silently if the dir is absent (never deployed or already cleaned). Collects `jf_mock.log` (the
  `--background --log` redirect target) and any `stdout.log` if present (parity with worker/coordinator
  collect). Covered by `test_deploy_jf.py` (`TestCmdCollect`: files-present / dir-missing / no-files /
  base64-failure-skip / no-pods).
- `src/mock_jf_server.py` (the JF mock daemon) now emits one log line per API call into the `--log` file
  (which `deploy_jf.py collect` ships back). Uses a two-stream ``logging`` setup mirroring
  ``deploy_common.py``'s pattern: ``_stdout_logger`` (``jf_mock.stdout``) carries INFO-level request
  logs to stdout (redirected to ``jf_mock.log`` by ``_daemonize``'s ``os.dup2`` on fd 1 in
  ``--background`` mode); ``_stderr_logger`` (``jf_mock.stderr``) carries ERROR-level startup failures
  (bind/fork) to stderr so ``kubectl exec`` sees them before the redirect. Format
  ``[%(asctime)s] %(message)s`` (``datefmt='%Y-%m-%dT%H:%M:%S'``) includes a timestamp because this is
  a long-running server log, not CLI output (deploy_common uses bare ``%(message)s`` for CLI greps).
  ``_log(msg)`` is a drop-in for the old ``print(f'[{ts}] {msg}')``; ``_log_error(msg)`` replaces
  ``print(..., file=sys.stderr, flush=True)``. The only remaining ``print`` is ``print(pid, flush=True)``
  in ``_daemonize``'s parent path -- that is a PROTOCOL output parsed by ``deploy_jf._start_jf_mock``
  as the child PID, so it must stay bare (a ``_log``-formatted line would not be a pure digit).
  Logged endpoints: ``register`` (200 + gen, 400 missing field), ``heartbeat`` (200 + remaining_ttl,
  404 not found/expired, 400 missing field), ``unregister`` (200 + removed count, 400), ``discover``
  (200 + instance count), ``events`` (200 + count), ``health`` (200), unknown-path 404, and TTL expire
  (service + address + reason). ``_remove_expired_locked`` returns the expired list so callers
  (``discover``, ``ttl_sweeper_loop``) log outside the registry lock (keeps the locked section tight).
  Covered by ``test_mock_jf_server.py`` (``TestRequestLogging``: integration test that starts the
  server, hits each endpoint, reads the log, and substring-checks for each action keyword).

## Environment Notes

Backed by `build.sh` and current docs:

- CANN is optional but needed for Ascend hetero-related features.
- `rdma-core` is optional but needed for RDMA support.
- Python 3.9+ is expected by current docs and build packaging flows.

## Pending Verification

- The fastest minimal command set for local iteration on a single submodule still needs module-level documentation.
- Service boot, smoke-test, and deployment debug flows should be split into dedicated context files later.
