# CMake Build Context

## Scope

- Paths:
  - `build.sh`
  - `scripts/build_common.sh`
  - `scripts/build_cmake.sh`
  - `scripts/build_thirdparty.sh`
  - `scripts/verify_package_manifest.py`
  - `scripts/package_manifest/*`
  - `CMakeLists.txt`
  - `cmake/options.cmake`
  - `cmake/dependency.cmake`
  - `cmake/package.cmake`
  - `cmake/util.cmake`
  - `cmake/external_libs/*.cmake`
  - `src/datasystem/**/CMakeLists.txt`
- Why this module exists:
  - record the current CMake build model, build-script contract, third-party dependency rules, install/package outputs,
    and compile-time target dependency graph for future build-speed work.
- Primary source files to verify against:
  - `build.sh`
  - `scripts/build_cmake.sh`
  - `scripts/build_thirdparty.sh`
  - `CMakeLists.txt`
  - `cmake/dependency.cmake`
  - `cmake/package.cmake`
  - `src/datasystem/CMakeLists.txt`

## Important Platform Statement

- Verified from `docs/source_zh_cn/installation/build_guide/cmake_build.md`:
  - source CMake build documentation currently supports `openEuler 22.03/24.03`;
  - the official compile image is `v0.6.3-openeuler-22.03` for `X86_64` and `ARM64`;
  - required build tools include Python `3.9-3.13`, GCC/G++ `7.5.0+`, CMake `3.18.3+`, `make`, `git`,
    `libtool`, and `patch`.
- Narrow interpretation:
  - treat non-openEuler source builds as unsupported unless a task explicitly verifies that environment from source,
    CI, or release documentation.
  - README pip-install support is broader (`Linux`, recommended `glibc 2.34+`, `x86-64`), but that does not expand the
    documented source-build support boundary.

## Companion Docs

- Matching metadata JSON:
  - `.repo_context/modules/metadata/quality.cmake-build.json`
- Matching `design.md`:
  - `design.md`
- Matching feature playbook:
  - `.repo_context/playbooks/features/quality/cmake-build-optimization.md`
- Reason for this split:
  - CMake build knowledge is broader than `build-test-debug.md`; it owns target graph, third-party build cache,
    install layout, and packaging behavior, which are repeated inputs for compile-speed optimization and review.

## Build Entrypoints

| Entrypoint | Role | Source-backed notes |
| --- | --- | --- |
| `bash build.sh` | primary user-facing build and package command | Defaults to CMake, `Release`, `./build`, `./output`, Python on, Java/Go off, hetero on. |
| `bash build.sh -b cmake` | explicit CMake path | Dispatches to `scripts/build_cmake.sh`. |
| `bash build.sh -i on` | incremental CMake build | Preserves `BUILD_DIR` and `INSTALL_DIR`; default is destructive clean. |
| `bash build.sh -n on` | Ninja generator | Otherwise `Unix Makefiles`. |
| `bash build.sh -X off` | disable hetero build | Sets `BUILD_HETERO=off`, `BUILD_HETERO_NPU=off`, and `BUILD_HETERO_GPU=off`. |
| `bash build.sh -X gpu/npu/all` | select hetero backend set | `gpu` enables CUDA without Ascend, `npu` enables Ascend only, and `all` enables both backends. |
| `bash build.sh -X on/npu/all` | enable transfer_engine HIXL D2D backend for CMake NPU builds | Requires Ascend/NPU support; the repository assumes the CANN environment provides HIXL. |
| `bash build.sh -P off` | disable Python SDK/wheel | Default is Python on. |
| `bash build.sh -J on` | build Java API | Adds JNI and Maven jar packaging. |
| `bash build.sh -G on` | build Go SDK | Adds C wrapper target and post-install Go package validation. |
| `bash build.sh -t build/run/...` | build or run tests | Turns CMake `WITH_TESTS` on unless running already-built cases only. |

Post-build package-shape guard:

```bash
python3 scripts/verify_package_manifest.py \
  --install-dir output \
  --tar-manifest scripts/package_manifest/cmake-release-xoff-python-tar.txt \
  --wheel-manifest scripts/package_manifest/cmake-release-xoff-python-wheel.txt
```

This verifier is read-only. It checks the file-level paths inside the generated deployment tarball and Python wheel
against checked-in baselines, and it must be run after `build.sh` creates packages rather than replacing `build.sh`.

## Build Script And CMake Contract

- `build.sh` owns:
  - option parsing and defaults through `scripts/build_common.sh`;
  - Ascend environment auto-detection for default hetero builds;
  - build-system dispatch between CMake and Bazel;
  - test mode selection;
  - version override through `-v` or `DS_VERSION`.
- `scripts/build_cmake.sh::build_datasystem_cmake` owns:
  - deleting or preserving build/output directories based on `BUILD_INCREMENT`;
  - writing root `config.cmake` for example builds;
  - passing CMake definitions;
  - running `scripts/build_thirdparty.sh` first when CMake version is `>= 3.28`;
  - invoking `cmake`, `cmake --build`, and `cmake --install`;
  - stripping installed binaries when `ENABLE_STRIP=on`;
  - building C++ examples for test builds;
  - packaging Go after install when `PACKAGE_GO=on`;
  - creating `yr-datasystem-v$(cat VERSION).tar.gz` from the installed `datasystem/` directory.
- CMake owns:
  - third-party dependency discovery and build rules;
  - core targets and generated protobuf targets;
  - install rules for SDK, service, Python wheel staging, Java jar, Go SDK staging, and CMake package files.

## CMake Options Passed By `build.sh`

| `build.sh` input | CMake variable | Default | Effect |
| --- | --- | --- | --- |
| `-d` / `-r` | `CMAKE_BUILD_TYPE` | `Release` | Selects Debug or Release flag set. |
| `-S address/thread/undefined/off` | `USE_SANITIZER` | `off` | Adds Google sanitizer flags to project and selected third-party builds. |
| `-t != off` | `WITH_TESTS` | `off` | Adds test subdirectories and `WITH_TESTS` define; also enables the test-only braft `1.1.2` dependency and its election test target. |
| `-c on/html` | `BUILD_COVERAGE` | `off` | Adds gcov flags and `BUILD_COVERAGE` define. |
| `-p on` | `ENABLE_PERF` | `off` | Adds `ENABLE_PERF`, perf service/client sources, and perf proto targets. |
| `-P on/off/path` | `BUILD_PYTHON_API` | `on` from script | Adds `pybind_api`, pybind11 dependency, and wheel packaging. |
| `-J on` | `BUILD_JAVA_API` | `off` | Adds JNI source target and Maven jar packaging. |
| `-G on` | `BUILD_GO_API` | `off` | Adds C API wrapper target and Go SDK install staging. |
| `-X on/off/gpu/npu/all` | `BUILD_HETERO` | `on` | Adds hetero compile definitions and device/plugin dependencies when on. |
| `-X off/gpu/npu/all` plus default | `BUILD_HETERO_NPU` | `on` | Enables Ascend/NPU backend; `-X off` and `-X gpu` force it off. |
| `-X off/gpu/npu/all` plus default | `BUILD_HETERO_GPU` | `off` | Enables CUDA/GPU backend; `-X gpu` and `-X all` force it on, `-X off` and `-X npu` force it off. |
| CMake `-X on/npu/all` | `TRANSFER_ENGINE_ENABLE_HIXL` | set by `build.sh`; transfer_engine may force it back off during configure | Adds the transfer_engine HIXL D2D backend only when CANN/HIXL `8.5.2+` is detected; lower or unknown HIXL versions keep the rest of the NPU build and compile without this backend. |
| `-T on` | `BUILD_PIPLN_H2D` | `off` | Temporarily ignored and forced back to `off`; Pipeline H2D currently remains disabled even when requested. |
| `-M on` | `BUILD_WITH_URMA` | `off` | Adds URMA dependency and RDMA-related source. |
| `-A on` | `BUILD_WITH_RDMA` | `off` | Adds UCX and rdma-core checks/source. |
| `-s on/off` | `ENABLE_STRIP` | `on` | Controls install/package stripping and symbol sidecars. |
| `-x on` | `SUPPORT_JEPROF` | `off` | Builds jemalloc with profiling support. |

### GPU-Only Hetero Build Notes

- Verified on `2026-06-15` from `build.sh`, top-level `CMakeLists.txt`, `src/datasystem/common/rdma/CMakeLists.txt`,
  `src/datasystem/common/device/CMakeLists.txt`, `tests/st/CMakeLists.txt`, and client/worker Remote H2D call sites:
  - `bash build.sh -X gpu ...` means `BUILD_HETERO=on`, `BUILD_HETERO_NPU=off`, and `BUILD_HETERO_GPU=on`.
  - If Ascend environment detection fails while GPU is enabled, `build.sh` keeps hetero on and disables only NPU.
  - CUDA/GPU hetero support is intended to compile without Ascend libraries when NPU-specific sources and include paths
    stay behind `BUILD_HETERO_NPU` or `USE_NPU`.
  - GPU-only test builds still compile `common_acl_device` and the `ds_device_llt` ACL mock coverage so the test surface
    stays close to master; `common_acl_device` uses repository CANN type shims when `USE_NPU` is absent, while
    `acl_plugin`, Ascend library discovery, HCCL library linkage, and transfer-engine integration remain gated by
    `BUILD_HETERO_NPU`.
  - Remote H2D currently remains NPU/Ascend-specific: its implementation is
    `src/datasystem/common/rdma/npu/remote_h2d_manager.cpp`, its public header depends on Ascend/HCCL types, and
    `IsRemoteH2DEnabled()` returns false when `USE_NPU` is absent.
- Regression pattern:
  - undefined references to `datasystem::RemoteH2DManager::*` from `libdatasystem.so` during GPU-only test/executable
    links mean a source file compiled a Remote H2D call under broad `BUILD_HETERO` while
    `remote_h2d_manager.cpp` was correctly omitted because `BUILD_HETERO_NPU=off`.
  - The narrow fix is to guard direct Remote H2D includes and calls with `USE_NPU`, not to link the NPU implementation
    back into GPU-only builds.
- Local verification notes from the same investigation:
  - `cmake --build build --target ds_device_llt -j 8` passed in a GPU-only build with `BUILD_HETERO=on`,
    `BUILD_HETERO_GPU=on`, `BUILD_HETERO_NPU=off`, and `WITH_TESTS=on`; the target compiled ACL mock tests such as
    `acl_resource_manager_fallback_test.cpp`, `ascend_device_manager_test.cpp`, and the broader device LLT sources.
  - A full GPU-only test build linked `libdatasystem.so`, `dsbench_cpp`, `ds_device_llt`, `datasystem_worker`,
    `ds_ut`, `ds_ut_object`, and ST binaries without `RemoteH2DManager` undefined references.
  - If CMake selects a Python interpreter without `wheel` or SSL support, Python wheel packaging can fail after C++
    build/link succeeds. In this workspace, passing `-P /usr` made CMake choose `/usr/bin/python3.12`, and
    `cmake --install build` completed wheel packaging after deleting stale `build/python_api` staging files.

## Install And Package Outputs

| Language / area | Enablement | Main installed or packaged outputs | Source of rule |
| --- | --- | --- | --- |
| C++ SDK | always | `datasystem/sdk/cpp/include`, `datasystem/sdk/cpp/lib`, `datasystem/sdk/cpp/lib/cmake/Datasystem`, plus mirrored `cpp/include` and `cpp/lib` layout | `cmake/package.cmake` |
| C++ client libs | always | `libdatasystem.so`, `libds_router_client.so`, `libetcdapi_proto.so`, `librpc_option_protos.so`, `libcommon_flags.so`, third-party `.so` copies | `cmake/package.cmake` |
| Service | always | `datasystem/service/datasystem_worker`, `datasystem/service/datasystem_coordinator`, `datasystem/service/coordinator_config.json`, `datasystem/service/lib`, `libdatasystem_worker.so`, `libdatasystem_coordinator.so`, `libcommon_metastore_service.so`, service third-party libs | `cmake/package.cmake` |
| Python wheel | `BUILD_PYTHON_API=on` | `openyuanrong_datasystem-*.whl` at output root; wheel payload includes Python package, CLI, C++ headers, libs including `libdatasystem_coordinator.so`, `datasystem_worker`, `datasystem_coordinator`, service configs, `dsbench_cpp`, Helm chart, docker entrypoints | `cmake/package.cmake`, `cmake/util.cmake`, `cmake/scripts/PackageDatasystem.cmake.in`, `setup.py` |
| Java SDK | `BUILD_JAVA_API=on` | `datasystem/sdk/datasystem-${DATASYSTEM_VERSION}_${CMAKE_HOST_SYSTEM_PROCESSOR}.jar` | `java/CMakeLists.txt`, `cmake/package.cmake` |
| Go SDK | `BUILD_GO_API=on` | `datasystem/sdk/go`, C API headers under `datasystem/sdk/go/include/datasystem/c_api`, Go libs under `datasystem/sdk/go/lib`; post-install validation builds example/client packages | `src/datasystem/c_api/CMakeLists.txt`, `cmake/package.cmake`, `scripts/package_go_sdk.sh` |
| Final tarball | always after install | `yr-datasystem-v$(cat VERSION).tar.gz`; the script removes the installed `datasystem/` tree while creating the tarball | `scripts/build_cmake.sh` |

The braft dependency is test-only and has no install or package rule; `libbraft`, its headers, and its tools are not
part of SDK, service, wheel, or final tarball outputs.

### Package Manifest Baselines

| Baseline | Covered profile | Notes |
| --- | --- | --- |
| `scripts/package_manifest/cmake-release-xoff-python-tar.txt` | CMake release tarball with `-X off`, Python on, Java/Go off | Contains the file and directory paths expected inside `yr-datasystem-v*.tar.gz`. |
| `scripts/package_manifest/cmake-release-xoff-python-wheel.txt` | CMake release wheel with `-X off`, Python on, Java/Go off | Contains file paths expected inside `openyuanrong_datasystem-*.whl`; wheel directory entries are ignored. |

Rules for updating baselines:

- treat any missing or extra path as a release-package shape change;
- do not update the manifest as part of an optimization unless the package-shape change is intentional and reviewed;
- use `--dump-current <dir>` to generate candidate current manifests for review, then inspect the path diff manually.

## Read Order For Build-Speed Work

1. `README.md` in this directory for the script/CMake contract.
2. `design.md` for the target graph and critical dependency edges.
3. `../../../playbooks/features/quality/cmake-build-optimization.md` before changing target boundaries, third-party
   build rules, or packaging dependencies.
4. The exact `CMakeLists.txt` files for any target being split, merged, or re-linked.

## Review And Bugfix Notes

- Common risks:
  - changing a target used by `datasystem`, `datasystem_worker_shared`, or `ds_client_py` can alter multiple language
    packages because install rules reuse those targets;
  - changing third-party flags can invalidate `DS_OPENSOURCE_DIR` cache keys and may change package ABI;
  - changing install paths can break wheel pruning, C++ examples, `find_package(Datasystem)`, Java JNI packaging, or Go
    package validation.
  - `ASCEND_HIXL_FOUND` means basic HIXL headers and libraries were found. HCCS RH2D is gated separately by
    `ASCEND_HIXL_HCCS_SUPPORTED`, which requires detected HIXL version `8.5.2+`; lower versions skip
    `hccs_transport.cpp` while retaining hetero, default ROCE builds, and the vendored `p2p-transfer` dependency. The
    transfer_engine HIXL D2D backend has the same effective version floor and is disabled during configure when
    CANN/HIXL is missing, lower than `8.5.2`, or has an unknown version.
- Good first files when a build regression appears:
  - `scripts/build_cmake.sh`
  - `cmake/dependency.cmake`
  - `cmake/package.cmake`
  - `scripts/verify_package_manifest.py` when the regression is only package-shape validation
  - the nearest `src/datasystem/**/CMakeLists.txt`

## Pending Verification

- Whether CI currently treats Bazel and CMake as equal release paths is outside this CMake-specific module.
- A machine-generated full target graph would be useful for compile-speed work; this context records the verified
  high-value graph manually.
