# <Module Name>

When using this template for a new module area or a substantial module backfill, explicitly assess whether the area
also needs:

- a sibling `design.md` for architecture and invariants;
- a matching feature playbook under `.repo_context/playbooks/...` for implementation workflow and risk gates.

For shared infra, persistence-sensitive, recovery-sensitive, or compatibility-sensitive areas, treat both as required
unless you record a narrow, source-backed reason not to create them yet.

If one sub-area has its own persisted format, recovery lifecycle, availability or reliability model, or DFX workflow,
split it into a sibling module context instead of leaving it only as a subsection inside the parent module.

Do not leave compatibility redirects or temporary placeholder docs behind. Record only the canonical current-state
module package.

## Scope

- Path(s):
- Why this module exists:
- Primary source files to verify against:

## Responsibilities

- Verified:
- Pending verification:

## Companion Docs

- Matching metadata JSON:
- Matching `design.md`:
- Matching feature playbook:
- Reason if either is intentionally omitted:

## Module Boundary Assessment

- Canonical module boundary:
- Candidate sibling submodules considered:
- Why they stay inside the parent module or split out:

## Key Entry Points

- Public APIs:
- Internal services / executables:
- Config flags or environment variables:

## Main Dependencies

- Upstream callers:
- Downstream modules:
- External dependencies:

## Build And Test

- Build commands:
- Fast verification commands:
- Representative tests:

## Review And Bugfix Notes

- Common change risks:
- Important invariants:
- Observability or debugging hooks:

## Open Questions

- 
