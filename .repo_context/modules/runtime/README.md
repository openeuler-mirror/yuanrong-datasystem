# Runtime Context

`runtime/` contains worker lifecycle and distributed runtime coordination context.

Use this directory when the task depends on worker startup, service registration, cluster metadata, routing, or multi-node behavior.

Current docs:

- `worker-runtime.md`: worker startup, service surfaces, and runtime responsibilities.
- `cluster-management.md`: cross-module map for ETCD/Metastore, hash-ring, readiness, routing, and cluster coordination flow.
- `cluster-management-dfx-matrix.md`: standalone DFX matrix for scale-up, scale-down, restart, and ETCD crash/recovery.
- `etcd-metadata/README.md`: ETCD proto/client/watch/keepalive/CAS/Metastore backend context.
- `hash-ring/README.md`: distributed-master hash-ring topology, scale, recovery, and routing context.
- `cluster-manager/README.md`: worker membership, lifecycle, reconciliation, health, and route coordination context.
