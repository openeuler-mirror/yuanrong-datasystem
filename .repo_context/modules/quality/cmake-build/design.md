# CMake Build Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation
- Primary code paths:
  - `build.sh`
  - `scripts/build_common.sh`
  - `scripts/build_cmake.sh`
  - `scripts/build_thirdparty.sh`
  - `scripts/verify_package_manifest.py`
  - `scripts/package_manifest/*`
  - `CMakeLists.txt`
  - `cmake/*.cmake`
  - `src/datasystem/**/CMakeLists.txt`
- Primary source-of-truth files:
  - `docs/source_zh_cn/installation/build_guide/cmake_build.md`
  - `build.sh`
  - `scripts/build_cmake.sh`
  - `scripts/build_thirdparty.sh`
  - `CMakeLists.txt`
  - `cmake/dependency.cmake`
  - `cmake/package.cmake`
- Last verified against source:
  - `2026-05-03`
- Related context docs:
  - `README.md`
  - `../build-test-debug.md`
  - `../tests-and-reproduction.md`
  - `../test-suite-design.md`
- Related playbooks:
  - `../../../playbooks/features/quality/cmake-build-optimization.md`

## Purpose

- Capture the current CMake build architecture as an optimization and review map.
- Make build-speed work safer by showing which targets are packaging-critical and which edges are optional.
- Keep platform, dependency, compiler-flag, install, and module-dependency facts source-backed.

## Goals

- Identify supported source-build platform assumptions.
- List open-source dependencies and conditional dependencies.
- Record build flags and third-party cache behavior.
- Record install outputs for C++, Python, Java, and Go.
- Summarize compile-time module dependencies for future build-speed optimization.

## Non-Goals

- Do not replace upstream user-facing build documentation.
- Do not document every source file in every target.
- Do not claim non-openEuler source-build support without source-backed verification.
- Do not describe Bazel target internals except where `build.sh` chooses between build systems.

## Platform And Toolchain Boundary

| Item | Current verified state | Source |
| --- | --- | --- |
| Source-build OS | `openEuler 22.03/24.03` | `docs/source_zh_cn/installation/build_guide/cmake_build.md` |
| Compile image | `v0.6.3-openeuler-22.03`, `X86_64` and `ARM64` | same |
| Python for source build | `3.9-3.13` in CMake build guide; `setup.py` requires `>=3.9` | build guide, `setup.py` |
| CMake | `3.18.3+` documented; top-level minimum is `3.14.1`; third-party parallel prebuild runs only when detected CMake is `>=3.28` | build guide, `CMakeLists.txt`, `scripts/build_cmake.sh` |
| Compiler | GCC/G++ `7.5.0+` documented | build guide |
| Optional device deps | CANN `8.5`, rdma-core `35.1` documented | build guide |

## Build Flow

1. `build.sh` initializes defaults in `scripts/build_common.sh`.
2. `build.sh` parses command-line options and validates `on/off`, test labels, sanitizer values, paths, and thread
   counts.
3. If hetero is on but Ascend toolkit cannot be found, `build.sh` sets `BUILD_HETERO=off`.
4. For CMake, `build.sh` sources `scripts/build_cmake.sh`.
5. `build_datasystem_cmake` cleans or preserves build/output directories based on `BUILD_INCREMENT`.
6. `build_datasystem_cmake` assembles CMake options and selects `Ninja` or `Unix Makefiles`.
7. If the local CMake version is `>= 3.28`, `scripts/build_thirdparty.sh` prebuilds third-party libraries in parallel.
8. Main CMake configure includes `cmake/options.cmake`, `cmake/util.cmake`, and `cmake/dependency.cmake`.
9. Main CMake adds `transfer_engine` when `BUILD_TRANSFER_ENGINE AND BUILD_HETERO AND BUILD_HETERO_NPU`.
10. Main CMake adds `src/datasystem`, `dsbench`, optional `tests`, optional top-level `java`, and `cmake/package.cmake`.
11. `cmake --build` compiles all targets.
12. `cmake --install` materializes SDK/service/language package staging.
13. `scripts/build_cmake.sh` strips installed files if enabled, optionally builds examples and Go package, then creates
    `yr-datasystem-v${VERSION}.tar.gz`.
14. Release/package CI can then run `scripts/verify_package_manifest.py` as a read-only guard against tar/wheel
    file-list drift.

## Compiler And Link Flags

| Scope | Flags / definitions | Source |
| --- | --- | --- |
| C++ standard | `C++17` | `CMakeLists.txt` |
| Common CXX flags | `-Wall -g3 -Werror -fsigned-char -Wextra -Wfloat-equal -fno-common -rdynamic`; `-fPIC` via `add_definitions` | `CMakeLists.txt` |
| Debug | `-ggdb -O0 -ftrapv -fstack-check -DDEBUG` | `CMakeLists.txt` |
| Release | `-O2 -fstack-protector-strong -Wl,-z,relro -Wl,-z,now -Wl,-z,noexecstack -D_FORTIFY_SOURCE=2 -fPIE -pie -DNDEBUG -Wl,--build-id=none` | `CMakeLists.txt` |
| Sanitizer | address: `-fsanitize=address -fno-omit-frame-pointer -g3`; thread: `-fsanitize=thread -g3`; undefined: `-fsanitize=undefined -fno-sanitize=alignment -fsanitize=float-cast-overflow -fno-sanitize-recover=all -O0` | `CMakeLists.txt` |
| Coverage | `-fprofile-arcs -ftest-coverage`, linker `-lgcov`, `BUILD_COVERAGE` define | `CMakeLists.txt` |
| Hetero | `BUILD_HETERO`, `USE_NPU`, `USE_GPU` compile definitions as enabled | `CMakeLists.txt` |
| Pipeline H2D | `BUILD_PIPLN_H2D` compile definition | `CMakeLists.txt` |
| Third-party safe flags | `-fPIC -D_FORTIFY_SOURCE=2 -O2 -fstack-protector-strong -ffunction-sections -fdata-sections -Wl,--gc-sections -Wl,--build-id=none -Wl,-z,relro,-z,noexecstack,-z,now` plus `EXT_FLAGS` | `cmake/util.cmake` |
| aarch64 tune | `-mtune=tsv110 -march=armv8-a` when host is `aarch64` and compiler version is `> 9.1.0` | `cmake/util.cmake` |

## Open Source Dependency Record

| Dependency | Version / source rule | Included when | Build or find notes |
| --- | --- | --- | --- |
| abseil-cpp | `20240722` | always | Built via `absl.cmake`; used by re2/protobuf/gRPC. |
| RE2 | `2024-07-02` | always | Depends on absl. |
| zlib | `1.3.1` | always | Used by gRPC/package libs. |
| libsodium | `1.0.18` | always through `libzmq.cmake` | Built before ZeroMQ. |
| ZeroMQ | `4.3.5` | always | Provides main RPC transport library. |
| jemalloc | `5.3.0` | always | Shared jemalloc is linked into `datasystem_worker_bin`; profiling controlled by `SUPPORT_JEPROF`. |
| RocksDB | `7.10.2` | always | Used by metadata/replica storage code. |
| SecureC / libboundscheck | `v1.1.16` | always | Also passed into p2p-transfer build. |
| TBB | `2020.3-5.oe2203sp1` from `v2020.3` source | always | Patches for GCC 14; adds `TBB_INTERFACE_VERSION` definition. |
| nlohmann_json | `3.11.3` | always | Header library target. |
| OpenSSL | `1.1.1wa` | always | Used by auth, gRPC, curl, package libs. |
| libcurl | `8.8.0` | always | Depends on OpenSSL. |
| protobuf | `3.25.5`, found as `25.5.0` config | always | Generates project proto and etcd proto sources. |
| c-ares | `1.19.1` | gRPC build | Built inside `grpc.cmake`. |
| gRPC | `1.65.4` | always | Uses package providers for protobuf, absl, c-ares, re2, OpenSSL, zlib; applies GCC 7.3 patch. |
| libiconv | `1.15` | always | Packaged into SDK/service libs. |
| libxml2 | `2.9.12` | always | Packaged into SDK/service libs. |
| PCRE | `8.45` | always | Packaged into SDK/service libs. |
| cJSON | `1.7.17` | always | Packaged into service/SDK libs. |
| spdlog | `1.12.0` | always | Built as `libds-spdlog.so`. |
| GTest | `1.12.1` | `WITH_TESTS=on` | Required for C++ test targets and CTest registration. |
| pybind11 | `2.10.3` | `BUILD_PYTHON_API=on` | Finds Python3 interpreter/development and builds `ds_client_py`. |
| JNI | system JNI | `BUILD_JAVA_API=on` | Finds JNI headers for `client_jni_api`. |
| Ascend CANN | external toolkit | `BUILD_HETERO AND BUILD_HETERO_NPU` | Located via `ASCEND_HOME_PATH`, `ASCEND_CUSTOM_PATH/latest`, or `/usr/local/Ascend/ascend-toolkit/latest`. |
| p2p-transfer | vendored `third_party/P2P-Transfer`, version `0.1.0` | `BUILD_HETERO AND BUILD_HETERO_NPU` | Built through third-party helper; not treated as DS_PACKAGE open-source package. |
| CUDA / NCCL | external toolkit | `BUILD_HETERO AND BUILD_HETERO_GPU` | Located via `CUDA_HOME_PATH`, `CUDA_CUSTOM_PATH`, or `/usr/local/cuda`. |
| URMA | system package | `BUILD_WITH_URMA=on` | `BUILD_PIPLN_H2D` requires URMA. |
| UCX | `1.18.0` plus rdma-core headers | `BUILD_WITH_RDMA=on` | Configure build with verbs/rdmacm/multithreading and no Go/Java. |
| OsTransport | external package | `BUILD_PIPLN_H2D=on` | Found with CUDA by `pipeline_rh2d.cmake`. |

## Third-Party Build Rules

- Main CMake includes dependencies sequentially through `cmake/dependency.cmake`.
- With CMake `>= 3.28`, `scripts/build_thirdparty.sh` prebuilds common third-party libraries in parallel before main
  configure:
  - L0: `cjson`, `absl`, `iconv`, `jemalloc`, `libzmq`, `nlohmann_json`, `openssl`, `pcre`, `rocksdb`, `securec`,
    `spdlog`, `tbb`, `xml2`, `zlib`, plus optional `gtest`, `pybind11`, `ascend`.
  - L1: `re2`, `protobuf`, `libcurl`, `grpc`.
  - L1 dependencies are explicitly tracked as `re2:absl`, `protobuf:absl`, `libcurl:openssl`,
    `grpc:protobuf:openssl:zlib:re2`.
- `DS_OPENSOURCE_DIR` controls third-party cache root. If not set, `cmake/util.cmake` hashes `CMAKE_BINARY_DIR` and
  uses `/tmp/<sha256>`.
- `DS_PACKAGE` switches dependency sources to local packages/source trees and generates package hashes.
- `DS_LOCAL_LIBS_DIR` switches many open-source URLs to local tarballs under `opensource_third_party`.
- Cache keys include dependency name, package hash, version, components, toolchain, configure options, compiler
  versions, flags, link flags, patches, and extra dependency roots.

## CMake Target Graph Summary

### Directory Graph

```text
root
├── transfer_engine                    # optional: BUILD_TRANSFER_ENGINE && BUILD_HETERO && BUILD_HETERO_NPU
├── src/datasystem
│   ├── protos                         # generated protobuf / ZMQ RPC proto targets
│   ├── client                         # libdatasystem.so and router client
│   ├── common                         # shared static/shared support libraries
│   ├── master                         # ds_master static target
│   ├── server                         # ds_server static target
│   ├── worker                         # worker static/shared/bin targets
│   ├── pybind_api                     # optional Python module
│   ├── c_api                          # optional Go-facing C wrapper
│   └── java_api                       # optional JNI library
├── dsbench
├── tests                              # optional WITH_TESTS
└── java                               # optional Java jar packaging
```

### Generated Proto Layer

| Layer | Targets | Notes |
| --- | --- | --- |
| base proto | `utils_protos`, `utils_protos_client`, `zmq_meta_protos`, `zmq_meta_protos_client`, `rpc_option_protos` | `rpc_option_protos` is shared and installed into SDK/service. |
| transport/object base | `share_memory_protos`, `share_memory_protos_client`, `hash_ring_protos`, `hash_ring_protos_client`, `slot_recovery_protos`, `meta_transport_protos`, `meta_transport_protos_client`, `p2p_subscribe_protos`, `p2p_subscribe_protos_client` | Generated before client/worker targets that consume message types. |
| worker proto | `posix_protos`, `posix_protos_client`, `worker_object_protos`, `worker_object_protos_client`, `worker_stream_protos`, `worker_stream_protos_client` | Client and worker both depend on these. |
| master proto | `master_object_protos`, `master_object_protos_client`, `master_stream_protos`, `master_stream_protos_client`, `master_heartbeat_protos`, `master_heartbeat_protos_client` | Worker/master dependency edge. |
| optional test/perf proto | `generic_service_protos`, `ut_object_protos`, `perf_posix_protos`, client variants | Only under `WITH_TESTS` or `ENABLE_PERF`. |
| etcd proto | `etcdapi_proto` | Generated from `third_party/protos`; shared target installed for SDK/service. |

### Product Targets

| Target | Kind | Main dependencies | Compile-speed notes |
| --- | --- | --- | --- |
| `datasystem` | shared C++ SDK | client sources, `common_*`, `common_device`, `common_rdma`, `common_etcd_client`, generated client protos, `worker_transport_api`, gRPC, protobuf, TBB, SecureC | High fan-in user-facing library; avoid linking service-only code into it. |
| `datasystem_static` | static SDK/test target | same source set and most dependencies as `datasystem` | Unit tests and C wrapper use this; changes affect test link time. |
| `ds_router_client` | shared router client | `common_etcd_client`, `etcdapi_proto`, `hash_ring_protos_client`, `common_util` | Smaller public client target. |
| `ds_client_py` | Python extension module | `datasystem`, `pybind11::module` | Build enabled by default via `build.sh`. |
| `datasystem_c` | shared C wrapper for Go | `datasystem_static`, `ds_spdlog::spdlog`, `CURL::libcurl` | Only built for Go SDK packaging. |
| `client_jni_api` | Java JNI shared lib | `ds_flags`, `datasystem`, JNI includes | Java jar packaging depends on stripped JNI staging. |
| `datasystem_worker_shared` | service shared lib | `common_*`, `ds_master`, `ds_server`, `worker_object_cache`, `worker_stream_cache`, `cluster_manager`, `worker_client_manager`, generated protos | Biggest service fan-in target. |
| `datasystem_worker_static` | static worker target | same worker sources/deps; adds ST implementation when `WITH_TESTS` | Test-only sources make test builds structurally different. |
| `datasystem_worker_bin` | executable `datasystem_worker` | `datasystem_worker_shared`, `jemalloc`, `nlohmann_json` | Package-critical service binary. |
| `ds_master` | static | object/stream master cache, common RPC/log/util/rocksdb, cluster/client manager, `ds_server` | Pulled into worker shared lib. |
| `ds_server` | static | common event loop/log/perf/metrics/RPC/util; `generic_service_protos` only in tests | Test builds add generic service implementation. |
| `common_rpc_zmq` / `_client` | static | ZeroMQ, protobuf, common log/perf/util/event loop/encrypt, proto targets | Core RPC fan-out; client variant links client proto targets. |
| `common_rdma` | static | base transport plus optional URMA/UCX/Hetero source sets | Feature flags significantly change its source list and deps. |
| `common_device` | static | `common_acl_device`, optional `common_cuda_device`, `common_util`, `common_inject` | Always adds Ascend subdir; plugin only when hetero NPU is on. |
| `acl_plugin` / `cuda_plugin` | shared plugin | external device libs, protobuf, p2p-transfer for Ascend | Hash header generation depends on stripped plugin output. |
| `common_persistence_api` | static | `common_obs`, `common_sfs_client`, `common_slot_client`, curl | L2 persistence aggregation point. |
| `dsbench_cpp` | executable | `datasystem`, pthread, `common_util` | Included in wheel payload. |

### Optional Edges That Matter For Optimization

| Flag | Added work | Source |
| --- | --- | --- |
| `WITH_TESTS` | `tests` subtree, GTest, test-only protos, test-only worker/server sources, CTest registration | `CMakeLists.txt`, `src/datasystem/protos/CMakeLists.txt`, `src/datasystem/worker/CMakeLists.txt` |
| `ENABLE_PERF` | perf client source, perf service source, perf proto targets, `perf_client.h` included in SDK headers | `src/datasystem/client/CMakeLists.txt`, `src/datasystem/worker/CMakeLists.txt`, `cmake/package.cmake` |
| `BUILD_HETERO_NPU` | Ascend find, p2p-transfer, `acl_plugin`, transfer_engine subproject, plugin hash generation | `cmake/dependency.cmake`, `CMakeLists.txt`, device CMake files |
| `BUILD_HETERO_GPU` | CUDA find, `common_cuda_device`, `cuda_plugin`, plugin hash generation | `cmake/dependency.cmake`, device CMake files |
| `BUILD_PIPLN_H2D` | FATAL unless URMA is on; adds `os_transport_pipeline` and links it into SDK/worker | `cmake/dependency.cmake`, `src/datasystem/common/os_transport_pipeline/CMakeLists.txt` |
| `BUILD_WITH_RDMA` | UCX source/library and rdma-core header check; installs UCX base and IB libs | `cmake/external_libs/ucx.cmake`, `cmake/package.cmake` |
| `BUILD_JAVA_API` | JNI target, Maven build, Java CTest cases when tests are on | `src/datasystem/java_api/CMakeLists.txt`, `java/CMakeLists.txt` |
| `BUILD_GO_API` | C API wrapper and Go package validation after install | `src/datasystem/c_api/CMakeLists.txt`, `scripts/package_go_sdk.sh` |

## Packaging And Install Invariants

- `cmake/package.cmake` installs both legacy `datasystem/sdk/cpp/...` and newer `cpp/...` C++ SDK layouts.
- `DatasystemConfig.cmake`, `DatasystemConfigVersion.cmake`, and exported targets are installed under
  `datasystem/sdk/cpp/lib/cmake/Datasystem`.
- `package_datasystem_wheel` stages from installed service and SDK paths, so install order matters for wheel content.
- `setup.py` prunes unused shared libraries with `ldd`, keeps UCX plugin-style libraries under `lib/ucx`, and strips the
  worker binary.
- `scripts/build_cmake.sh` creates the final tarball with `tar --remove-files`, so post-build tests that need
  `datasystem/service` or `datasystem/sdk` extract the tarball again.
- Plugin libraries `libacl_plugin.so` and `libcuda_plugin.so` are special: runtime hash checking means package scripts
  avoid normal strip behavior for them in some stages.
- `scripts/verify_package_manifest.py` is intentionally outside the package creation path. It reads the generated
  tarball and wheel and compares their internal path lists with `scripts/package_manifest/*` baselines.
- The checked-in package manifest pair currently covers the CMake release profile with hetero disabled (`-X off`),
  Python packaging enabled, and Java/Go disabled. Other profiles need their own baselines before being made mandatory
  in CI.

## Common Change Scenarios

### Reducing Build Time

- First classify the edge:
  - third-party cache or parallelism;
  - generated proto rebuild;
  - high fan-in target link time;
  - optional feature accidentally enabled;
  - install/package staging overhead.
- Prefer isolating feature-specific source sets behind existing flags before splitting public package targets.
- Preserve install target names and package paths unless the task is explicitly packaging-related.

### Adding A Dependency

- Add it through `cmake/external_libs/*.cmake` and `cmake/dependency.cmake` only when it is a repository-wide build
  dependency.
- Include version, SHA256, local package behavior, and `DS_PACKAGE` behavior if it uses third-party helper functions.
- Update package lib patterns if runtime shared libraries are needed in SDK, service, Python, Java, or Go outputs.

### Changing A Target Dependency

- Check both build and install consumers:
  - C++ SDK install exports;
  - worker service install;
  - Python wheel staging;
  - Java JNI jar staging;
  - Go wrapper/package validation;
  - tests that use static variants.
- For generated proto edges, check whether client and server variants both need the change.

## Self-Verification Cases

| ID | Test scenario | Purpose | Preconditions | Input / steps | Expected result |
| --- | --- | --- | --- | --- | --- |
| cmake-configure | Configure default build | Catch dependency and option regressions | openEuler source-build env | `bash build.sh -i on -j <n>` or direct script flow | configure/build/install succeeds |
| no-hetero | Build without CANN | Validate non-device path | no Ascend toolkit required | `bash build.sh -X off` | build succeeds without Ascend dependency |
| package-python | Default wheel packaging | Validate default package path | Python dev env | `bash build.sh` | output tarball and wheel exist |
| package-manifest | Release package shape guard | Catch accidental file add/remove/rename in tar/wheel | generated tarball and wheel | `python3 scripts/verify_package_manifest.py --install-dir output --tar-manifest scripts/package_manifest/cmake-release-xoff-python-tar.txt --wheel-manifest scripts/package_manifest/cmake-release-xoff-python-wheel.txt` | tar and wheel path lists match baselines |
| java-package | Java optional package | Validate JNI/Maven path | Java/Maven env | `bash build.sh -J on` | jar installed under `datasystem/sdk` before tar |
| go-package | Go optional package | Validate C wrapper and Go package | Go env | `bash build.sh -G on` | Go client package validation succeeds |
| tests | CTest integration | Validate test-only graph | test deps | `bash build.sh -t build` | test binaries and CTest files generated |

## Open Questions

- A generated target-dependency graph should be added if build-speed work needs exact transitive closure and timing.
- Whether CMake `>= 3.28` parallel third-party prebuild should become mandatory is not decided in source.
