# CMake Build Optimization Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - refactor
- Owning module or area:
  - `quality.cmake-build`
- Primary code paths:
  - `build.sh`
  - `scripts/build_cmake.sh`
  - `scripts/build_thirdparty.sh`
  - `CMakeLists.txt`
  - `cmake/*.cmake`
  - `src/datasystem/**/CMakeLists.txt`
- Related module docs:
  - `../../../modules/quality/cmake-build/README.md`
  - `../../../modules/quality/build-test-debug.md`
- Related design docs:
  - `../../../modules/quality/cmake-build/design.md`
- Related tests or validation entrypoints:
  - `bash build.sh -X off`
  - `bash build.sh -t build`
  - `bash build.sh -i on`
  - `bash build.sh -J on`
  - `bash build.sh -G on`
  - `python3 scripts/verify_package_manifest.py --install-dir output --tar-manifest scripts/package_manifest/cmake-release-xoff-python-tar.txt --wheel-manifest scripts/package_manifest/cmake-release-xoff-python-wheel.txt`
- Last verified against source:
  - `2026-05-03`

## Purpose

- Standardize how to approach CMake compile-speed, target-boundary, dependency, and package-time changes.
- Reduce risk of breaking multi-language outputs while optimizing the build graph.

## When To Use This Playbook

- Use when:
  - splitting or merging CMake targets;
  - changing `target_link_libraries`, generated proto dependencies, or optional feature gates;
  - changing third-party build cache keys, versions, flags, or parallel prebuild behavior;
  - moving install/package paths for SDK, service, wheel, jar, or Go package outputs.
- Do not use when:
  - only running existing build/test commands without changing build files.
- Escalate to design-first review when:
  - a change touches `datasystem`, `datasystem_worker_shared`, generated proto targets, `cmake/package.cmake`, or
    third-party dependency versions.

## Preconditions

- Required context to read first:
  - `../../../modules/quality/cmake-build/README.md`
  - `../../../modules/quality/cmake-build/design.md`
- Required source files to inspect first:
  - nearest `CMakeLists.txt`;
  - `cmake/dependency.cmake` for dependency changes;
  - `cmake/package.cmake` for install/package changes;
  - `scripts/build_cmake.sh` for build flow changes.
- Required assumptions to verify before coding:
  - whether the change affects C++, Python, Java, or Go outputs;
  - whether tests enable extra sources or proto targets;
  - whether hetero, RDMA, URMA, or pipeline H2D flags change the dependency edge.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| package ABI | Does the target feed SDK/service/wheel/jar/go outputs? | target is test-only or private static helper | public `.so`, worker binary, wheel payload, jar, or Go lib changes |
| target fan-in | Is this a high fan-in target? | leaf helper target | `datasystem`, `datasystem_worker_shared`, `common_rpc_zmq`, proto targets |
| optional flags | Is the dependency truly optional? | guarded by existing flag and package path | default build starts needing CANN/CUDA/RDMA/Java/Go unexpectedly |
| third-party cache | Will cache keys or versions change? | only local source target changes | new flags, patches, SHA256, versions, or helper function behavior |
| generated sources | Does it alter proto/plugin generation? | no generated files involved | `GENERATE_PROTO_CPP`, `GENERATE_ZMQ_CPP`, `GENERATE_GRPC_CPP` changes |
| install layout | Does an installed path move? | no install rule touched | C++ CMake config, wheel staging, service libs, Java/Go path moves |

## Source Verification Checklist

- [ ] confirm the exact target and all package consumers
- [ ] confirm whether `WITH_TESTS` changes the source/dependency set
- [ ] confirm whether default `build.sh` options enable the affected path
- [ ] confirm third-party cache behavior if external dependency rules change
- [ ] confirm install rules for every language package touched
- [ ] confirm the platform statement remains openEuler source-build only unless new source says otherwise

## Implementation Plan

1. Measure or identify the slow edge before changing the graph.
2. Prefer reducing optional default work or narrowing dependencies before splitting public targets.
3. Keep public target names, exported CMake package names, and installed paths stable unless the request explicitly
   changes packaging.
4. When adding a new helper target, make it private to the smallest owning directory first.
5. For third-party changes, update version/SHA256/local-package behavior and package lib patterns together.
6. For generated proto changes, verify both client and server variants.
7. Run the smallest relevant build first, then package or language-specific checks.
8. For release-package-sensitive changes, run package manifest validation after `build.sh` has generated tar/wheel
   outputs.
9. Update `.repo_context/modules/quality/cmake-build/*` if the graph, options, dependency rules, or package outputs
   changed.

## Guardrails

- Must preserve:
  - `bash build.sh` as the primary release package path;
  - default Python wheel packaging unless explicitly disabled;
  - C++ SDK `datasystem/sdk/cpp` and `cpp` install layouts;
  - service `datasystem/service` layout;
  - `DatasystemConfig.cmake` install path and exported target compatibility.
- Must not change without explicit review:
  - default source-build platform claim;
  - third-party dependency versions or security flags;
  - plugin strip/hash behavior for Ascend/CUDA plugins;
  - `tar --remove-files` package lifecycle.
- Must verify in source before claiming:
  - whether an edge is default or optional;
  - whether a target is used by Python, Java, or Go packages;
  - whether direct `cmake --build` is enough or `build.sh` post-processing is required.

## Validation Plan

- Fast checks:
  - `bash build.sh -X off -i on -j <n>`
  - `bash build.sh -t build -X off -j <n>`
- Package checks:
  - `bash build.sh`
  - confirm `output/yr-datasystem-v*.tar.gz`
  - confirm `output/openyuanrong_datasystem-*.whl` when Python is on
  - run `scripts/verify_package_manifest.py` with the matching `scripts/package_manifest/*` baselines
- Optional language checks:
  - `bash build.sh -J on`
  - `bash build.sh -G on`
- Feature-specific checks:
  - hetero NPU build in an Ascend environment for `BUILD_HETERO_NPU` changes
  - RDMA/URMA build environment for `BUILD_WITH_RDMA`, `BUILD_WITH_URMA`, or `BUILD_PIPLN_H2D` changes
- Negative-path verification:
  - `bash build.sh -X off` still avoids Ascend dependency
  - `bash build.sh -P off` does not require Python wheel packaging

## Review Checklist

- [ ] public package outputs and install paths are unchanged or intentionally migrated
- [ ] package manifest validation passes, or manifest changes are intentional and reviewed
- [ ] default build dependencies did not become broader by accident
- [ ] third-party cache invalidation is intentional and documented
- [ ] static/shared target duplication was checked for test and package consumers
- [ ] generated proto dependencies remain ordered
- [ ] context docs were updated when graph, options, or package outputs changed

## Context Update Requirements

- Module docs to update:
  - `../../../modules/quality/cmake-build/README.md`
  - `../../../modules/quality/cmake-build/design.md`
- Design docs to update:
  - this playbook's related design doc when target graph or dependency strategy changes
- Additional playbooks to update:
  - `test-implementation.md` if test harness build behavior changes

## Pending Verification

- Add a repeatable timing workflow once the repository has a canonical build-timing command or CI artifact.
