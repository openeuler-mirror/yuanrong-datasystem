# Runtime Context

`runtime/` contains worker lifecycle and distributed runtime coordination context.

Use this directory when the task depends on worker startup, service registration, cluster metadata, routing, or multi-node behavior.

Current docs:

- `worker-runtime.md`: worker startup, service surfaces, and runtime responsibilities.
- `cluster-management.md`: metadata backends, readiness, routing, and cluster coordination flow.
