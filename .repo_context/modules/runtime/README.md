# Runtime Context

`runtime/` contains worker lifecycle and distributed runtime coordination context.

Use this directory when the task depends on worker startup, service registration, topology metadata, routing, or multi-node behavior.

Current docs:

| Area | Canonical entry | Design / feature workflow |
| --- | --- | --- |
| Worker runtime | `worker-runtime.md` | No dedicated design or feature playbook yet. |
| Coordinator election | `coordinator-election/README.md` | `coordinator-election/design.md`; `../../playbooks/features/runtime/coordinator-election/implementation.md` |
| Runtime topology | `topology/README.md` | `../../playbooks/features/runtime/topology/implementation.md` |
| ETCD metadata | `etcd-metadata/README.md` | `etcd-metadata/design.md`; `../../playbooks/features/runtime/etcd-metadata/implementation.md` |
