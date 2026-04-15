# Repository Overview

This document captures a coarse, source-backed map of the repository. It is intentionally broad and should be refined into module-specific documents over time.

## High-Level Identity

- Repository: `yuanrong-datasystem`
- Role in the openYuanrong project: data system repository
- Top-level description source: `README.md`
- Build system entrypoints: `build.sh`, `CMakeLists.txt`, `WORKSPACE`
- Documentation root: `docs/source_zh_cn/`

## Coarse Module Map

| Path | Current role | Notes |
| --- | --- | --- |
| `src/datasystem/common` | shared infrastructure and utilities | includes device, kvstore, rpc, metrics, shared memory, logging, l2 cache, rdma, flags |
| `src/datasystem/client` | SDK-side client logic | contains `context`, `kv_cache`, `object_cache`, `stream_cache`, `hetero_cache`, `mmap` |
| `src/datasystem/worker` | worker-side runtime and data/cache services | includes cluster manager, hash ring, client manager, object cache, stream cache |
| `src/datasystem/master` | master/coordinator related logic | includes object and stream cache areas |
| `src/datasystem/server` | server assembly / executable-side wiring | verify concrete binaries and startup flow from source when working here |
| `src/datasystem/protos` | protobuf and RPC contracts | generated code dependencies likely flow outward from here |
| `src/datasystem/pybind_api` | Python binding layer | built when Python API is enabled |
| `src/datasystem/java_api` | Java binding layer | built when Java API is enabled |
| `src/datasystem/c_api` | C API / Go-related bridge layer | built when Go API is enabled according to `src/datasystem/CMakeLists.txt` |
| `include/datasystem` | public C++ headers and API surface | includes context, kv, object, stream, hetero, utils |
| `python/yr/datasystem` | Python SDK surface | includes `ds_client`, `kv_client`, `object_client`, `stream_client`, `hetero_client` |
| `java` | Java build and packaging area | see `java/pom.xml`, `java/CMakeLists.txt` |
| `go` | Go module area | see `go/go.mod` and subpackages |
| `cli` | operational and deployment CLI scripts | includes `start`, `stop`, `up`, `generate_config`, `generate_helm_chart`, benchmark tools |
| `tests` | C++/system/perf/common tests | `tests/CMakeLists.txt` adds `ut`, `st`, `perf`, `common` |
| `example` | multi-language usage samples | includes C++, Go, Java, Python examples |
| `transfer_engine` | separate but related transfer subsystem | linked from root build when hetero and NPU-related flags are enabled |
| `docs` | checked-in docs source and generated docs artifacts | current source docs live under `docs/source_zh_cn` |
| `.skills` | repository-local official Codex skills | includes reusable workflows for GitCode PR creation and online documentation refresh |

## Architectural Notes Backed By Current Docs

- `README.md` describes the repository as the data system component of openYuanrong.
- The main runtime roles described there are SDK, worker, and cluster management.
- `docs/source_zh_cn/design_document/cluster_management.md` shows that cluster management currently supports both ETCD and Metastore paths.
- `CMakeLists.txt` confirms the core build graph centers on `src/datasystem`, `dsbench`, tests, optional Java API, and optionally `transfer_engine`.

## Known Gaps

- Concrete binary targets and startup composition under `src/datasystem/server` still need a dedicated module document.
- The exact boundaries between `master` and `worker` need deeper source inspection before making stronger claims.
- Public API to internal implementation mapping across C++/Python/Java/Go is not yet documented here.

## Next Module Documents To Add

- `deployment-cli.md`
