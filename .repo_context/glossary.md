# Glossary

This file is intentionally short at first. Add terms only when they recur across modules and are easy to misread.

## Terms

- `DsClient`
  - aggregate client that bundles `KVClient`, `HeteroClient`, and `ObjectClient`
- `worker`
  - the datasystem runtime process serving client and cache operations on a node
- `master`
  - metadata and coordination side used by worker/runtime flows; in distributed mode the boundary with worker-side coordination needs source confirmation per change
- `Metastore`
  - built-in metadata service intended to replace external ETCD deployment in some cluster modes
- `ETCD`
  - external metadata and coordination backend used for cluster state and related operations
- `hash ring`
  - worker-side distribution and scaling structure used for routing and membership-related behaviors
- `object cache`
  - object-style data API family and corresponding worker-side cache services
- `KV cache`
  - key-value API family that currently shares deep client implementation with object-cache client backend
- `stream cache`
  - producer/consumer stream API family with separate client and worker implementation paths
- `hetero`
  - heterogeneous/device-oriented data path including H2D, D2H, and D2D style operations
- `common-infra`
  - shared infrastructure under `src/datasystem/common`, including RPC, shared memory, kvstore, logging, metrics, rdma, and related utilities
