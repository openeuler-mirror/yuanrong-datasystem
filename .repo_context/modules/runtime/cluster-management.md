# Cluster Management

## Scope

- Paths:
  - `src/datasystem/worker/cluster_manager`
  - `src/datasystem/worker/hash_ring`
  - cluster-related portions of `src/datasystem/worker/worker_oc_server.cpp`
  - cluster-related CLI flows in `cli/start.py`, `cli/up.py`, `cli/generate_config.py`
  - docs under `docs/source_zh_cn/design_document/cluster_management.md` and deployment docs
- Why this module exists:
  - describe how workers join and coordinate as a cluster;
  - capture the relationship between ETCD, Metastore, hash ring, worker readiness, and deployment tooling.
- Primary source files to verify against:
  - `docs/source_zh_cn/design_document/cluster_management.md`
  - `docs/source_zh_cn/deployment/deploy.md`
  - `src/datasystem/worker/cluster_manager/CMakeLists.txt`
  - `src/datasystem/worker/cluster_manager/etcd_cluster_manager.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/worker_cli.cpp`
  - `cli/start.py`
  - `cli/up.py`
  - `cli/generate_config.py`

## Responsibilities

- Verified:
  - current docs describe two metadata/cluster-management modes:
    - external ETCD
    - built-in Metastore
  - worker runtime enforces that at least one of `etcd_address` or `metastore_address` is configured.
  - `cluster_manager` currently builds around `etcd_cluster_manager.cpp` plus worker health-check support.
  - hash-ring logic is a separate worker subdomain that coordinates distribution/routing-related state and interacts with the metadata backend.
  - `dscli` supports both single-node `start` and multi-node `up`, and `up` has explicit handling for Metastore head-node sequencing.
- Pending verification:
  - the full event flow from worker join/leave through all hash-ring and replica side effects;
  - exact separation between “cluster management” and “master metadata placement” for every runtime path.

## Two Supported Metadata Modes

### ETCD mode

- Verified from docs and CLI:
  - requires an external ETCD service
  - `dscli start -w ... --etcd_address ...` is the quick local entrypoint
  - `dscli up -f cluster_config.json` can start all nodes in parallel in ETCD mode
- Verified from docs:
  - suitable when ETCD already exists or stronger external HA separation is wanted

### Metastore mode

- Verified from docs and CLI:
  - Metastore is built into the worker-side system as an ETCD-compatible metadata service
  - `metastore_head_node` identifies the node that starts Metastore service in cluster deployment
  - `cli/up.py` starts the Metastore head node first, then starts the remaining worker nodes in parallel
  - `cli/up.py` validates that `metastore_head_node` is in `worker_nodes` and that `metastore_address` exists in worker config
- Verified from docs:
  - intended to reduce external dependencies and simplify deployment

## Hash Ring Notes

- Verified from `hash_ring.cpp`:
  - hash-ring initialization can proceed with ETCD-backed or non-ETCD restoration paths
  - when both `etcd_address` and `metastore_address` are empty, ring init effectively short-circuits
  - hash ring chooses backend address from `metastore_address` first, otherwise `etcd_address`
  - hash ring also interacts with master address initialization and restart/scaling restoration
- Review caution:
  - cluster mode changes are rarely “just config changes”; they often affect hash-ring init, master address selection, and recovery behavior together

## Worker Cluster Manager Notes

- Verified from `etcd_cluster_manager.cpp`:
  - cluster manager owns worker-address keyed node state and subscribes to several hash-ring and cluster-related events
  - it can construct other-AZ hash rings when distributed-master and multi-cluster settings are enabled
  - shutdown removes subscribers and stops background threads cleanly
- Useful takeaway:
  - cluster management is event-driven and tightly coupled to hash-ring and node-state transitions

## CLI And Config Mapping

- `cli/generate_config.py`
  - copies `cluster_config.json` and `worker_config.json` templates to the target directory
- `cli/start.py`
  - single-node start flow
  - accepts either worker config file or inline worker args
  - has quick-start style worker arg mode
- `cli/up.py`
  - multi-node cluster start flow
  - reads cluster config and worker config
  - validates Metastore-specific settings
  - starts head node first only in Metastore mode

## Common Questions And First Places To Look

- “Why can’t a worker join or become ready?”
  - `worker_oc_server.cpp`
  - `cluster_manager/etcd_cluster_manager.cpp`
  - deployment config and `cli/start.py` or `cli/up.py`
- “Why is routing or placement wrong after topology change?”
  - `hash_ring/hash_ring.cpp`
  - related hash-ring helpers and events
- “How is external ETCD being replaced?”
  - docs `design_document/cluster_management.md`
  - `common/kvstore/metastore`
  - `cli/up.py` validation and sequencing

## Review And Bugfix Notes

- Common change risks:
  - changing CLI validation can silently desync deployment behavior from runtime assumptions;
  - changes around `etcd_address` / `metastore_address` fallback affect more than one subsystem;
  - hash-ring edits can break restart recovery and scaling flows even when startup still appears healthy.
- Good companion docs:
  - `modules/runtime/worker-runtime.md`
  - `modules/infra/common-infra.md`
  - `modules/quality/tests-and-reproduction.md`

## Recommended Next Split

When this document gets too large, split it in this order:

1. `metadata-backends.md`
2. `hash-ring-and-routing.md`
3. `deployment-config-flow.md`
