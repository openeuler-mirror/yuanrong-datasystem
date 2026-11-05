# Client Context

`client/` contains public API and SDK-facing context.

Use this directory when the task starts from a user-facing API, language binding, client initialization path, or auth/config flow.

Current docs:

- `client-sdk.md`: coarse map across C++ headers, client implementation, and Python bindings.

Design proposals, separate from current behavior:

- `docs/source_zh_cn/design_document/client_worker_failover_unification_detailed_design.md`: confirmed behavior and the complete WorkerFailover migration baseline, with outstanding development contracts listed in section 10. This is a design proposal, not an implementation or runtime-validation record.
