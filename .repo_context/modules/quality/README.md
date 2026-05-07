# Quality Context

`quality/` contains build, test, debug, and reproduction context.

Use this directory when the main question is how to build, validate, reproduce, or review changes safely.

Current docs:

- `build-test-debug.md`: build entrypoints, debug tooling, sanitizer, and coverage orientation.
- `cmake-build/README.md`: CMake build-system context, third-party dependencies, build script contract, install outputs,
  and target graph orientation.
- `cmake-build/design.md`: current CMake build design for compile-speed optimization and package-impact review.
- `tests-and-reproduction.md`: test selection and bug reproduction guidance.
- `test-suite-design.md`: current test harness structure, CMake/gtest-to-CTest registration, labels, and runtime flows.

Package-shape validation for CMake release artifacts is owned by `cmake-build/README.md` and implemented by
`scripts/verify_package_manifest.py` with baselines under `scripts/package_manifest/`.
