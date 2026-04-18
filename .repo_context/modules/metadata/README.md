# Module Metadata

`modules/metadata/` contains machine-readable canonical module metadata for `.repo_context/`.

Why this layer exists:

- give tools a stable way to discover canonical module ids and entry docs;
- make module-boundary and sibling-module decisions explicit;
- support deterministic routing without inferring everything from prose.

Design choices:

- metadata is centralized here instead of colocated beside every module doc because the current repository uses both
  flat module docs and directory-based module packages;
- each file is one canonical module record named `<module-id>.json`;
- the record points to canonical docs, playbooks, source roots, tests, and split signals;
- metadata should describe the current canonical layout only, never compatibility redirects.

Current canonical records:

- `overview.repository-overview.json`
- `client.client-sdk.json`
- `runtime.worker-runtime.json`
- `runtime.cluster-management.json`
- `infra.common-infra.json`
- `infra.l2cache.json`
- `infra.slot.json`
- `infra.logging.json`
- `infra.metrics.json`
- `infra.observability.json`
- `quality.build-test-debug.json`
- `quality.tests-and-reproduction.json`

Validation:

```bash
python3 scripts/ai_context/validate_module_metadata.py
```

Template:

- `../../templates/module-metadata-template.json`
