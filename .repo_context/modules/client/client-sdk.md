# Client SDK

## Scope

- Paths:
  - `include/datasystem/*`
  - `src/datasystem/client`
  - `src/datasystem/pybind_api`
  - `python/yr/datasystem`
- Why this module exists:
  - provide the user-facing client APIs for KV, Object, Stream, hetero, and context operations;
  - connect SDK calls to worker/master services through shared memory, RPC, and optional device transfer paths;
  - expose the same core capabilities across C++ and Python.
- Primary source files to verify against:
  - `include/datasystem/datasystem.h`
  - `include/datasystem/kv_client.h`
  - `include/datasystem/object_client.h`
  - `include/datasystem/stream_client.h`
  - `include/datasystem/hetero_client.h`
  - `include/datasystem/context/context.h`
  - `include/datasystem/utils/connection.h`
  - `src/datasystem/client/CMakeLists.txt`
  - `src/datasystem/client/datasystem.cpp`
  - `src/datasystem/client/kv_cache/kv_client.cpp`
  - `src/datasystem/client/object_cache/object_client.cpp`
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
  - `src/datasystem/client/stream_cache/stream_client.cpp`
  - `src/datasystem/client/hetero_cache/hetero_client.cpp`
  - `src/datasystem/client/context/context.cpp`
  - `src/datasystem/client/service_discovery.cpp`
  - `src/datasystem/pybind_api/pybind_register_*.cpp`
  - `python/yr/datasystem/*.py`

## Responsibilities

- Verified:
  - `datasystem` shared library is built from `src/datasystem/client/*` and is the main user-facing client library.
  - `DsClient` is only a convenience aggregator. It constructs `KVClient`, `HeteroClient`, and `ObjectClient`, then initializes and shuts them down in order.
  - `ConnectOptions` is the common connection/auth/config carrier for C++ clients.
  - KV and Object client code share the same deep backend implementation through `object_cache::ObjectClientImpl`.
  - Stream uses its own `client::stream_cache::StreamClientImpl`.
  - Python bindings are not a separate reimplementation; they bind to C++ classes and helper types through `libds_client_py`.
  - Python package `yr.datasystem` also conditionally exposes transfer-engine bindings and lazily exposes `DsTensorClient`.
- Pending verification:
  - exact internal ownership split between `listen_worker.cpp`, `client_worker_common_api.cpp`, and `embedded_client_worker_api.cpp` for each API family;
  - whether Java and Go clients follow the same runtime layering closely enough to share one future module document.

## Public API Surface

- C++ aggregate:
  - `datasystem::DsClient`
  - obtains `KV()`, `Object()`, and `Hetero()`
- C++ direct clients:
  - `KVClient`
  - `ObjectClient`
  - `StreamClient`
  - `HeteroClient`
  - `Context`
- Shared config and utility surface:
  - `ConnectOptions`
  - `ServiceDiscovery`
  - `Status`
  - `Buffer`, `ReadOnlyBuffer`, stream producer/consumer types, hetero blob/future types

## Implementation Mapping

| Public surface | Main implementation path | Notes |
| --- | --- | --- |
| `DsClient` | `src/datasystem/client/datasystem.cpp` | convenience wrapper only |
| `KVClient` | `src/datasystem/client/kv_cache/kv_client.cpp` -> `object_cache::ObjectClientImpl` | KV create/set/get path is layered over object-cache client backend |
| `ObjectClient` | `src/datasystem/client/object_cache/object_client.cpp` -> `object_cache::ObjectClientImpl` | object semantics are thin wrappers around shared implementation |
| `HeteroClient` | `src/datasystem/client/hetero_cache/hetero_client.cpp` plus object/device helpers | integrates D2H/H2D/D2D style operations |
| `StreamClient` | `src/datasystem/client/stream_cache/stream_client.cpp` -> `client::stream_cache::StreamClientImpl` | separate stream cache implementation family |
| `Context` | `src/datasystem/client/context/context.cpp` | thread-local trace and tenant context helpers |
| `ServiceDiscovery` | `src/datasystem/client/service_discovery.cpp` | ETCD-backed worker selection for C++ callers using `ConnectOptions.serviceDiscovery` |

## Connection And Auth Model

- Verified in `ConnectOptions`:
  - direct worker address via `host` + `port`
  - connection and request timeout controls
  - token auth, curve key fields, AK/SK fields, tenant id
  - cross-node and exclusive connection toggles
  - remote H2D toggle
  - optional `ServiceDiscovery`
  - fast transport shared-memory size
- Verified in `ObjectClientImpl` constructor:
  - when relevant fields are empty, some connection and auth options are loaded from environment variables such as:
    - `DATASYSTEM_HOST`
    - `DATASYSTEM_PORT`
    - `DATASYSTEM_CLIENT_PUBLIC_KEY`
    - `DATASYSTEM_CLIENT_PRIVATE_KEY`
    - `DATASYSTEM_SERVER_PUBLIC_KEY`
    - `DATASYSTEM_ACCESS_KEY`
    - `DATASYSTEM_SECRET_KEY`
    - `DATASYSTEM_TENANT_ID`
- Pending verification:
  - whether `StreamClientImpl` applies the same environment fallback behavior as `ObjectClientImpl`.

## Python Mapping

- Package entry:
  - `python/yr/datasystem/__init__.py`
- Python facade files:
  - `ds_client.py`
  - `kv_client.py`
  - `object_client.py`
  - `stream_client.py`
  - `hetero_client.py`
  - `ds_tensor_client.py`
  - `util.py`

### Verified Python layering

- `DsClient` in Python mirrors the C++ aggregate pattern by composing Python `KVClient`, `HeteroClient`, and `ObjectClient`.
- `KVClient`, `ObjectClient`, `StreamClient`, and `HeteroClient` wrap `yr.datasystem.lib.libds_client_py` objects.
- `libds_client_py` is populated by `src/datasystem/pybind_api/pybind_register*.cpp`.
- `DsTensorClient` is a Python-side convenience layer built on top of `HeteroClient`, tensor pointer extraction, and page-attention helpers.

### Verified Python/C++ differences to remember

- Python `Context` currently exposes `set_trace_id`, but not `SetTenantId`, even though C++ `Context` has both APIs.
- Python `KVClient` is backed by a pybind class named `KVClient` whose underlying C++ object is `ObjectClientImpl`.
- Python package init preloads transfer-engine-related shared libraries when present and exposes transfer-engine bindings only if import succeeds.
- Python wrappers raise exceptions on error instead of returning `Status` objects in the same way the C++ API does.

## Important Internal Neighbors

- Upstream callers:
  - user C++ applications
  - Python applications through `yr.datasystem`
  - tests under `tests/ut` and `tests/st`
- Downstream modules:
  - `src/datasystem/worker` services
  - `src/datasystem/master` metadata and coordination services
  - `src/datasystem/common/*` for RPC, shared memory, logging, metrics, rdma, kvstore, device helpers
  - `src/datasystem/protos`
  - optional `transfer_engine`

## Build And Packaging

- Main build definition:
  - `src/datasystem/client/CMakeLists.txt`
  - `bazel/BUILD.bazel`
- Notable facts:
  - client sources build both `datasystem_static` and `datasystem` shared library
  - `ds_router_client` is a separate client-facing library built from `router_client.cpp`
  - Python bindings are built from `src/datasystem/pybind_api` when Python API build is enabled
  - transfer engine is only added from the root build when transfer-engine, hetero, and NPU-related conditions are satisfied
  - Bazel target `//bazel:datasystem_sdk` packages a C++ SDK directory tree at `bazel-bin/bazel/datasystem_sdk/cpp`, with all public headers under `cpp/include/datasystem/` and `lib/libdatasystem.so` + `lib/libds_client_py.so`

## Review And Bugfix Notes

- Common change risks:
  - changing `ConnectOptions` can affect multiple language bindings and shared backend initialization at once;
  - `ObjectClientImpl` is shared by both KV and Object API families, so “KV-only” changes may regress object behavior;
  - Python-facing behavior can differ from C++ because pybind wrappers convert statuses into exceptions and sometimes rename methods;
  - context propagation changes can affect tracing and multi-tenant behavior across all client operations.
- Important invariants:
  - `DsClient` init order is KV -> Hetero -> Object; shutdown order is Object -> Hetero -> KV.
  - worker connectivity and auth material may come from explicit options or environment fallback in shared client backend code.
  - Python `DsTensorClient` depends on `HeteroClient`; tensor features are not an independent transport stack.
- Useful debug points:
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
  - `src/datasystem/client/service_discovery.cpp`
  - `src/datasystem/pybind_api/pybind_register_*.cpp`

## Fast Verification

- Rebuild repository artifacts:
  - `bash build.sh`
- Rebuild tests:
  - `bash build.sh -t build`
- Run client-related C++ tests by label:
  - `bash build.sh -t run_cases -l ut`
  - `bash build.sh -t run_cases -l st`
- Narrow by test binary when iterating:
  - inspect `tests/ut/CMakeLists.txt` and `tests/st/CMakeLists.txt` for binaries such as `ds_ut`, `ds_ut_object`, `ds_st_object_cache`, `ds_st_kv_cache`

## Open Questions

- Should service discovery be documented as a C++-only advanced entrypoint for now, since Python constructors do not currently expose it directly?
- Should `DsTensorClient` live in this module document permanently, or split into a future hetero/transfer-engine focused document once that area is deepened?
