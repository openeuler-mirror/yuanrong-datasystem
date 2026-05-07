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
| `bash build.sh -X off` | disable hetero build | Useful when Ascend toolkit is absent; `build.sh` also auto-disables hetero when it cannot find Ascend. |
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
| `-t != off` | `WITH_TESTS` | `off` | Adds test subdirectories and `WITH_TESTS` define. |
| `-c on/html` | `BUILD_COVERAGE` | `off` | Adds gcov flags and `BUILD_COVERAGE` define. |
| `-p on` | `ENABLE_PERF` | `off` | Adds `ENABLE_PERF`, perf service/client sources, and perf proto targets. |
| `-P on/off/path` | `BUILD_PYTHON_API` | `on` from script | Adds `pybind_api`, pybind11 dependency, and wheel packaging. |
| `-J on` | `BUILD_JAVA_API` | `off` | Adds JNI source target and Maven jar packaging. |
| `-G on` | `BUILD_GO_API` | `off` | Adds C API wrapper target and Go SDK install staging. |
| `-X on/off` | `BUILD_HETERO` | `on` | Adds hetero compile definitions and device/plugin dependencies. |
| internal CMake default | `BUILD_HETERO_NPU` | `on` | Enables Ascend/NPU backend when hetero is on. |
| extra CMake option only | `BUILD_HETERO_GPU` | `off` | Enables CUDA/GPU backend when hetero is on. |
| `-T on` | `BUILD_PIPLN_H2D` | `off` | Requires `BUILD_WITH_URMA`; adds OS transport pipeline. |
| `-M on` | `BUILD_WITH_URMA` | `off` | Adds URMA dependency and RDMA-related source. |
| `-A on` | `BUILD_WITH_RDMA` | `off` | Adds UCX and rdma-core checks/source. |
| `-s on/off` | `ENABLE_STRIP` | `on` | Controls install/package stripping and symbol sidecars. |
| `-x on` | `SUPPORT_JEPROF` | `off` | Builds jemalloc with profiling support. |

## Install And Package Outputs

| Language / area | Enablement | Main installed or packaged outputs | Source of rule |
| --- | --- | --- | --- |
| C++ SDK | always | `datasystem/sdk/cpp/include`, `datasystem/sdk/cpp/lib`, `datasystem/sdk/cpp/lib/cmake/Datasystem`, plus mirrored `cpp/include` and `cpp/lib` layout | `cmake/package.cmake` |
| C++ client libs | always | `libdatasystem.so`, `libds_router_client.so`, `libetcdapi_proto.so`, `librpc_option_protos.so`, `libcommon_flags.so`, third-party `.so` copies | `cmake/package.cmake` |
| Service | always | `datasystem/service/datasystem_worker`, `datasystem/service/lib`, `libdatasystem_worker.so`, `libcommon_metastore_service.so`, service third-party libs | `cmake/package.cmake` |
| Python wheel | `BUILD_PYTHON_API=on` | `openyuanrong_datasystem-*.whl` at output root; wheel payload includes Python package, CLI, C++ headers, libs, `datasystem_worker`, `dsbench_cpp`, Helm chart, docker entrypoints | `cmake/package.cmake`, `cmake/util.cmake`, `cmake/scripts/PackageDatasystem.cmake.in`, `setup.py` |
| Java SDK | `BUILD_JAVA_API=on` | `datasystem/sdk/datasystem-${DATASYSTEM_VERSION}_${CMAKE_HOST_SYSTEM_PROCESSOR}.jar` | `java/CMakeLists.txt`, `cmake/package.cmake` |
| Go SDK | `BUILD_GO_API=on` | `datasystem/sdk/go`, C API headers under `datasystem/sdk/go/include/datasystem/c_api`, Go libs under `datasystem/sdk/go/lib`; post-install validation builds example/client packages | `src/datasystem/c_api/CMakeLists.txt`, `cmake/package.cmake`, `scripts/package_go_sdk.sh` |
| Final tarball | always after install | `yr-datasystem-v$(cat VERSION).tar.gz`; the script removes the installed `datasystem/` tree while creating the tarball | `scripts/build_cmake.sh` |

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
