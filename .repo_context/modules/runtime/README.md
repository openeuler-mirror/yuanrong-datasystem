# Runtime Context

`runtime/` contains worker lifecycle and distributed runtime coordination context.

Use this directory when the task depends on worker startup, service registration, topology metadata, routing, or multi-node behavior.

Current docs:

- `worker-runtime.md`: worker startup, service surfaces, and runtime responsibilities.
- `topology/README.md`: cluster topology, membership, routing, controller recovery, and scale-transition context.
- `etcd-metadata/README.md`: ETCD proto/client/watch/keepalive/CAS/Metastore backend context.
