# Common Infrastructure

## Scope

- Paths:
  - `src/datasystem/common`
  - especially:
    - `rpc`
    - `shared_memory`
    - `kvstore`
    - `log`
    - `metrics`
    - `rdma`
    - `l2cache`
    - `device`
    - `os_transport_pipeline`
- Why this module exists:
  - provide the shared infrastructure layer that client, worker, and master all depend on;
  - centralize transport, storage backends, memory primitives, observability, and device-related helpers.
- Primary source files to verify against:
  - `src/datasystem/common/CMakeLists.txt`
  - `src/datasystem/common/rpc/CMakeLists.txt`
  - `src/datasystem/common/shared_memory/CMakeLists.txt`
  - `src/datasystem/common/kvstore/CMakeLists.txt`
  - `src/datasystem/common/kvstore/etcd/CMakeLists.txt`
  - `src/datasystem/common/kvstore/metastore/CMakeLists.txt`
  - `src/datasystem/common/kvstore/rocksdb/CMakeLists.txt`
  - `src/datasystem/common/log/CMakeLists.txt`
  - `src/datasystem/common/metrics/CMakeLists.txt`
  - `src/datasystem/common/rdma/CMakeLists.txt`
  - `src/datasystem/common/l2cache/CMakeLists.txt`
  - `src/datasystem/common/device/CMakeLists.txt`
  - `src/datasystem/common/os_transport_pipeline/CMakeLists.txt`

## Responsibilities

- Verified:
  - `src/datasystem/common` is the shared dependency root for both runtime-side and client-side code.
  - top-level common subdomains currently include:
    - auth and identity helpers: `ak_sk`, `iam`, `token`, `encrypt`
    - runtime plumbing: `rpc`, `eventloop`, `parallel`, `signal`, `flags`, `inject`, `util`
    - storage and memory: `shared_memory`, `kvstore`, `l2cache`, `object_cache`, `stream_cache`
    - observability: `log`, `metrics`, `perf`
    - transport and device paths: `rdma`, `device`, `os_transport_pipeline`
  - this layer is wide by design and should be split further when one subdomain becomes a frequent context target.
- Pending verification:
  - exact runtime ownership for each `common/*` component between client-only, worker-only, and truly shared callers;
  - which utility subdomains are stable enough to deserve their own dedicated module docs next.

## Stable Subdomains Worth Knowing

| Subdomain | Verified role | Build facts |
| --- | --- | --- |
| `rpc` | RPC plumbing plus code generation plugins and ZMQ-based transport implementation | builds rpc plugins, `common_rpc_zmq`, `common_rpc_zmq_client`, `rpc_stub_cache_mgr` |
| `shared_memory` | shared-memory allocator, arena, mmap abstractions, shared-disk detection | builds `common_shared_memory` and `common_shm_unit_info` |
| `kvstore` | metadata/backend storage families | split into `etcd`, `metastore`, `rocksdb` |
| `log` | logging, access recording, tracing, failure handling | builds `common_log` |
| `metrics` | resource metrics and exporters | builds `common_metrics` and exporter base |
| `rdma` | fast transport wrappers and optional URMA/RDMA support | conditional build based on feature flags |
| `l2cache` | persistence and secondary-storage support | includes OBS/SFS clients, distributed-disk slot client, and persistence API dispatch |
| `device` | Ascend and optional Nvidia device support wrappers | builds `common_device` over backend-specific device libs |
| `os_transport_pipeline` | optional pipeline H2D transport path | only built when `BUILD_PIPLN_H2D` is enabled |

Detailed follow-up docs now exist for:

- `modules/infra/l2cache/README.md`
- `modules/infra/l2cache/design.md`
- `modules/infra/l2cache/l2-cache-type.md`
- `modules/infra/slot/README.md`
- `modules/infra/slot/design.md`
- `modules/infra/logging/README.md`
- `modules/infra/logging/design.md`
- `modules/infra/logging/trace-and-context.md`
- `modules/infra/logging/access-recorder.md`
- `modules/infra/logging/log-lifecycle-and-rotation.md`
- `modules/infra/metrics/README.md`
- `modules/infra/metrics/design.md`
- `modules/infra/metrics/resource-collector.md`
- `modules/infra/metrics/exporters-and-buffering.md`
- `modules/infra/metrics/metric-families-and-registration.md`

## Storage And Metadata Backends

- Verified:
  - `kvstore` has three current backend families:
    - `etcd`
    - `metastore`
    - `rocksdb`
  - ETCD layer provides store, keep-alive, watch, health, and gRPC session pieces.
  - Metastore builds a lightweight ETCD-compatible shared service with KV, lease, watch, and maintenance service implementations.
  - RocksDB layer provides local persistent store and replica-related components.

## Transport And Memory Notes

- Verified:
  - `rpc` contains both generation tooling and the main ZMQ transport implementation.
  - `shared_memory` contains allocator, jemalloc integration, arenas, shm units, and several mmap backends; when UB
    numa affinity is enabled it also records per-allocation NUMA ownership. Shared-memory pre-touch distribution is
    controlled by `shared_memory_distribution_policy` with values `none`, `interleave_all_numa`,
    `interleave_affinity_numa`; for interleave policies the implementation performs 1GB chunk round-robin across
    selected NUMA nodes, applies `SYS_mbind` with `MPOL_BIND` per chunk, and then page-touches each chunk. The
    distribution policy takes effect when `enable_urma=true` and
    `urma_register_whole_arena=true`.
  - `rdma` always builds fast-transport wrapper pieces and conditionally adds URMA and RDMA implementations.
  - when hetero is enabled, RDMA dependencies also pull in device and shared-memory related components.
  - `os_transport_pipeline` is optional and only exists when pipeline H2D support is enabled.

## Observability Notes

- Verified:
  - `log` includes logging, access recording, tracing, and failure handling.
  - `metrics` includes exporter base, resource metric collection, and hard-disk exporter support.
  - `perf` is a separate common subdomain listed at top level and frequently appears in runtime loops and request paths.

## Review And Bugfix Notes

- Common change risks:
  - infra changes often have broad blast radius because both client and worker layers link here;
  - transport and shared-memory edits can affect correctness, performance, and deployment assumptions at the same time;
  - backend changes in `kvstore` can influence cluster coordination, metadata, and recovery behavior.
- Useful debugging orientation:
  - if a problem spans modules, check whether the common layer is the real coupling point before blaming business logic;
  - if a bug touches ETCD, Metastore, or RocksDB persistence, keep `common-infra` and `cluster-management` docs open together.

## Recommended Next Split

When this document gets too large, split it in this order:

1. `rpc-and-transport.md`
2. `shared-memory.md`
3. `kvstore-backends.md`
4. `device-and-rdma.md`
