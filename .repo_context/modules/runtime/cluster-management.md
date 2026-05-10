# Cluster Management

## Scope

- Paths:
  - `src/datasystem/worker/cluster_manager`
  - `src/datasystem/worker/hash_ring`
  - `third_party/protos/etcd`
  - `src/datasystem/common/kvstore/etcd`
  - `src/datasystem/common/kvstore/metastore`
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
  - exact separation between "cluster management" and "master metadata placement" for every runtime path.

## Detailed Context Packages

- `modules/runtime/etcd-metadata/README.md`
  - ETCD v3 proto copy, `EtcdStore`, `GrpcSession`, watch compensation, lease keepalive, CAS/Txn, auth/TLS,
    election, and built-in Metastore.
- `modules/runtime/hash-ring/README.md`
  - persisted `HashRingPb`, initial token generation, add node, passive remove, voluntary scale-down, restart/rolling
    UUID restoration, other-AZ read rings, and hash-ring health-check repair.
- `modules/runtime/cluster-manager/README.md`
  - `EtcdClusterManager`, cluster-node table, event priority queue, restart reconciliation, passive/voluntary
    scale-down coordination, route lookup, health probes, and other-AZ membership.
- `modules/runtime/cluster-management-dfx-matrix.md`
  - standalone DFX scenario matrix for scale-up, passive/voluntary scale-down, restart, ETCD crash/recovery,
    fake-node repair, cross-AZ, and combined failure timing.

Use this file as the cross-module map. Use the detailed package for source-level behavior.

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
  - cluster mode changes are rarely "just config changes"; they often affect hash-ring init, master address selection, and recovery behavior together

## Worker Cluster Manager Notes

- Verified from `etcd_cluster_manager.cpp`:
  - cluster manager owns worker-address keyed node state and subscribes to several hash-ring and cluster-related events
  - it can construct other-AZ hash rings when distributed-master and multi-cluster settings are enabled
  - shutdown removes subscribers and stops background threads cleanly
  - `EtcdClusterManager::SetWorkerReady()` releases the internal worker-ready wait post; master-side utility work calls
    `WaitWorkerReadyIfNeed()` before processing node utility events
- Verified from `ServiceDiscovery`:
  - `ServiceDiscovery::GetAllWorkers()` obtains worker entries from `ETCD_CLUSTER_TABLE`, parses `KeepAliveValue`, and
    returns only workers whose keepalive state is `ETCD_NODE_READY`
  - host affinity is optional: `PREFERRED_SAME_NODE` partitions workers into same-host and other-host lists,
    `REQUIRED_SAME_NODE` returns only same-host workers, and `RANDOM` returns all workers through `otherAddrs`
- Verified from remote-get transport:
  - KV `Get` delegates to object-cache `Get`; with `enable_worker_worker_batch_get=true`, worker remote get can use
    `BatchGetObjectFromRemoteWorker`
  - URMA connection warmup bypasses client `Get` and metadata query; it prepares an internal local object and calls
    worker-side direct remote get so the request still reaches `GetObjectRemote` and `CheckConnectionStable`
  - when URMA is enabled, the requester fills URMA memory information into the remote-get request; a provider response
    with `K_URMA_NEED_CONNECT` triggers `WorkerRemoteWorkerTransApi::ExecOnceParrallelExchange()`, which calls
    `WorkerWorkerExchangeUrmaConnectInfo()` and finalizes the outbound connection
- Useful takeaway:
  - cluster management is event-driven and tightly coupled to hash-ring and node-state transitions

## Cross-Module Lifecycle Map

### Startup

1. `WorkerOCServer` validates `etcd_address` or `metastore_address`, initializes `EtcdStore`, and constructs
   `EtcdClusterManager`.
2. `ClusterInfo` is loaded from ETCD/Metastore, or from RocksDB and peer reconciliation when ETCD is down.
3. `EtcdClusterManager::Init` starts background event processing, enqueues startup node events, starts watches, and
   initializes `HashRing`.
4. `EtcdStore::InitKeepAlive` writes the local worker row into `ETCD_CLUSTER_TABLE` with a lease.
5. Worker object/stream services finish reconciliation, set the health probe, and move ETCD node state to `ready`.

### Scale Up

1. New worker keepalive creates a cluster-table PUT event.
2. Cluster-manager adds the worker to `clusterNodeTable_`.
3. The new worker also inserts itself into `HashRingPb.workers` as `INITIAL`.
4. Cluster-manager's utility loop calls `HashRing::InspectAndProcessPeriodically`.
5. Hash ring writes `add_node_info`, migration events move metadata/data, and replica-manager receives scale-up finish
   signals.

### Passive Scale Down

1. Lease expiration, keepalive failure, or fake event creates a cluster-table DELETE.
2. Cluster-manager marks the node `TIMEOUT`, later demotes it to `FAILED`, and notifies object/stream/replica/slot
   recovery events.
3. Cluster-manager periodically calls `HashRing::RemoveWorkers(GetFailedWorkers())`.
4. Hash ring writes `del_node_info`, recovery/cleanup subscribers run, and the worker is eventually removed from ring
   state.
5. Orphan cleanup removes stale node-table and worker API state.

### Voluntary Scale Down

1. Worker shutdown writes ETCD state `exiting`, drains clients and async work, then calls
   `EtcdClusterManager::VoluntaryScaleDown`.
2. Hash ring sets `need_scale_down`, moves metadata through `add_node_info`, migrates data, clears tokens/UUID maps, and
   notifies `BeforeVoluntaryExit`.
3. Cluster-manager interprets the later DELETE for an `exiting` node as either successful voluntary exit or crash during
   scale-down, based on hash-ring leaving state.

### Restart

1. Restarted worker writes `restart`, `recover`, or `d_rst`.
2. Cluster-manager triggers metadata reconciliation and network-recovery events when this process is current master.
3. Worker services set health only after reconciliation or controlled give-up, then call `InformEtcdReconciliationDone`
   to write `ready`.

## Current ETCD + Hash-Ring Cluster Management Assessment

### Strengths

- ETCD/Metastore gives a single watchable metadata backend for membership, ring state, replica groups, and startup
  recovery.
- The hash ring keeps route reads local after watch update, so normal object route lookup avoids synchronous ETCD reads.
- Lease keepalive gives a simple worker-liveness signal that integrates with passive scale-down.
- CAS on the ring key gives single-record version safety and makes restart recovery possible without a separate leader.
- Watch compensation and fake-node completion repair some missed-event and full-cluster-restart cases.
- The event-bus model lets object-cache, stream-cache, replica-manager, slot recovery, and worker APIs hook into topology
  changes without hard direct calls in every path.

### Weaknesses And Scale Limits

- `/datasystem/ring` is a single hot CAS key. Initial token generation, node join, passive deletion, voluntary
  scale-down, rolling UUID restoration, task completion, and self-healing all contend on one serialized protobuf.
- Scale changes can create severe ETCD pressure: many workers run periodic cluster-manager loops, many can try hash-ring
  CAS operations, and each CAS reads/parses/serializes/writes the whole ring.
- CAS conflict handling is bounded by generic retry counters in `EtcdStore::CAS`. High writer fanout can surface as
  operation failure and repeated retry instead of controlled backoff or leader-owned batching.
- The design does not fit thousand-node scale well: every worker watches broad prefixes, keeps full membership/ring
  state, runs keepalive/background loops, scans node tables, and can participate in ring inspection.
- State is duplicated across ETCD cluster-table values, `clusterNodeTable_`, `HashRingPb`, hash-ring derived maps,
  read-ring maps, RocksDB cluster snapshots, health files, and replica-manager state. Correctness depends on eventual
  convergence among all of them.
- The lifecycle state machine is fragmented across ETCD keepalive values, `ClusterNode` state, hash-ring worker state,
  hash-ring local state, task records, worker health, and service reconciliation counters.
- Watch and repair semantics are hard to reason about because real ETCD events, ETCD watch-compensation fake events,
  startup fake events, and cluster-manager fake add/delete events all feed similar handlers.
- Module boundaries are unclear. Cluster-manager reaches into hash-ring behavior, ETCD store behavior, replica-manager
  events, object/stream metadata events, slot recovery, worker APIs, fast-transport cleanup, and health probes.
- Routing depends on both hash-ring state and replica-manager primary DB state. A worker can be active in one state copy
  but not ready or not resolvable in another.
- Cross-AZ event dispatch and worker parsing use string-prefix conventions around cluster names and ETCD table names,
  which makes namespace changes risky.
- Metastore shares ETCD-compatible APIs but the verified implementation is in-memory with limited per-key history, so
  its operational envelope is not equivalent to an external persistent ETCD cluster.

### Design Direction Implied By The Code

- Split hot ring state into narrower records: membership, routing tokens, migration tasks, and UUID restoration should
  not all CAS the same value.
- Centralize topology writes behind one elected coordinator or bounded scheduler instead of letting many workers race on
  the same key.
- Replace string lifecycle tags with a typed membership state machine that owns ETCD and in-memory state together.
- Give watch-compensation and fake-repair events explicit provenance/idempotency markers.
- Isolate cluster-manager into membership watch, route lookup, scale policy, and reconciliation/repair components.

### DFX Matrix

- For explicit scale-up, scale-down, restart, and ETCD crash/recovery coverage, use
  `modules/runtime/cluster-management-dfx-matrix.md`.
- That matrix separates normal paths from combined DFX cases such as scale-down during scale-up, restart during
  scale-down, ETCD crash during restart, fake-node repair after full-cluster restart, and cross-AZ recovery.

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
- URMA connection warmup config surfaces:
  - there is no standalone warmup selector; when `enable_urma=true`, workers use URMA warmup
  - DaemonSet Helm deployment uses `global.performance.enableUrma`, rendered as `-enable_urma`
  - Deployment image mode uses `k8s_deployment/helm_chart/worker.config` because `worker_entry.sh` starts the worker with `dscli start -f ${CONFIG_FILE}`
  - worker process restarts recover selected Kubernetes env-derived values from `<log_dir>/env`; the file stores
    `pod_ip=<POD_IP>` plus the host-affinity environment variable selected by `FLAGS_host_id_env_name`, for example
    `JDOS_HOST_IP=<JDOS_HOST_IP>`, with no separate deployment flag.

## Common Questions And First Places To Look

- "Why can't a worker join or become ready?"
  - `worker_oc_server.cpp`
  - `cluster_manager/etcd_cluster_manager.cpp`
  - deployment config and `cli/start.py` or `cli/up.py`
- "Why is routing or placement wrong after topology change?"
  - `hash_ring/hash_ring.cpp`
  - related hash-ring helpers and events
- "How is external ETCD being replaced?"
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

The runtime cluster-management area is now split into detailed sibling packages:

1. `etcd-metadata/`
2. `hash-ring/`
3. `cluster-manager/`

The remaining future split is deployment and CLI config flow, if the CLI/deployment portions keep growing.
