# Tests And Reproduction

## Scope

- Paths:
  - `tests`
  - `build.sh`
  - `tests/README.md`
  - `tests/ut/CMakeLists.txt`
  - `tests/st/CMakeLists.txt`
- Why this module exists:
  - record the real test entrypoints already used by the repository;
  - help bugfix and review work jump to the right binary, label, and scenario class quickly;
  - preserve reproduction conventions in one stable place.

## Test Layout

- Verified:
  - top-level test tree is split into:
    - `tests/ut`
    - `tests/st`
    - `tests/perf`
    - `tests/common`
  - `tests/README.md` defines the gtest/ctest prefix and label conventions used by the repository.
  - `tests/CMakeLists.txt` adds `ut`, `st`, `perf`, and `common`.

## Naming And Labeling Rules

- Verified from `tests/README.md`:
  - `DISABLED_`: disabled test
  - `EXCLUSIVE_`: test should not run concurrently with others
  - `LEVEL1_`: tagged as `level1`
- Verified label semantics:
  - `ut`
  - `st`
  - `level0`
  - `level1`
  - `level*`
  - combined forms such as `st level0`

## Main Commands

- Build tests:

```bash
bash build.sh -t build
```

- Build and run all tests:

```bash
bash build.sh -t run
```

- Build and run with parallel jobs:

```bash
bash build.sh -t run -u 8
```

- Build and run a label:

```bash
bash build.sh -t run -l level0
```

- Run already-built labeled tests:

```bash
bash build.sh -t run_cases -l ut
bash build.sh -t run_cases -l st
bash build.sh -t run_cases -l "st level0"
```

- Run a single case:

```bash
ctest -R test_suite.test_name
```

## Test Binaries Worth Knowing

- Verified from `tests/ut/CMakeLists.txt`:
  - `ds_ut`
  - `ds_ut_stream`
  - `ds_ut_object`
  - `ds_ut_slot_store`
  - `flags_ut`
- Verified from `tests/st/CMakeLists.txt`:
  - `ds_st`
  - `ds_st_stream_cache`
  - `ds_st_object_cache`
  - `ds_st_kv_cache`
  - `ds_st_embedded_client`
  - helper tools such as `curve_keygen` and `hashring_parser`

## Reproduction Guidance

- When the bug is in public client APIs:
  - start with `tests/ut` if the behavior is local and isolated;
  - move to `tests/st/*_cache` binaries when the bug depends on client-worker-master interaction.
- When the bug is in worker embedding or in-process worker startup:
  - inspect and run `ds_st_embedded_client`.
- When the bug is in stream behavior:
  - check `ds_ut_stream` and `ds_st_stream_cache`.
- When the bug is in object or KV behavior:
  - check `ds_ut_object`, `ds_st_object_cache`, and `ds_st_kv_cache`.

## Build-System Facts That Affect Reproduction

- Verified:
  - `build.sh -t run` builds and runs tests.
  - `build.sh -t run_cases` only runs existing tests.
  - coverage is available through `-c on|html`.
  - sanitizer modes are available through `-S address|thread|undefined`.
  - `tests/ut` and `tests/st` link against the main client and worker libraries, so regressions often cross module boundaries.

## Review Notes

- Common risks:
  - adding tests to the wrong binary can hide required runtime dependencies;
  - label mistakes change CI/runtime behavior because labels are derived from naming conventions;
  - some ST binaries generate runtime assets and helper tools as post-build steps.
- Useful places to inspect when a test “should exist” but is hard to find:
  - `tests/ut/CMakeLists.txt`
  - `tests/st/CMakeLists.txt`
  - `build.sh`

## Pending Verification

- the most representative smoke-test subsets for quick local iteration are not yet curated here;
- Python and example-test execution paths under `build.sh -t run_python` and `run_example` should be deepened in a later pass.
