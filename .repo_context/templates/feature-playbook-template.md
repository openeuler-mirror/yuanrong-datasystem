# <Feature Name> Playbook

## Metadata

- Status:
  - `draft` | `active` | `deprecated`
- Feature scope:
  - small extension | behavior change | refactor | mixed
- Owning module or area:
- Primary code paths:
- Related module docs:
- Related design docs:
- Related tests or validation entrypoints:
- Last verified against source:

## Purpose

- Why this playbook exists:
- What change class it standardizes:
- What risks it is meant to reduce:

## When To Use This Playbook

- Use when:
- Do not use when:
- Escalate to design-first review when:

## Preconditions

- Required context to read first:
- Required source files to inspect first:
- Required assumptions to verify before coding:

## Request Intake

- Requested behavior change:
- Explicit non-goals:
- Affected users, processes, or services:
- Backward-compatibility expectations:

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| module boundaries |  |  |  |
| compatibility |  |  |  |
| observability |  |  |  |
| performance |  |  |  |
| security |  |  |  |
| operations |  |  |  |

## Source Verification Checklist

- [ ] confirm primary entrypoints in source
- [ ] confirm downstream consumers or integration points
- [ ] confirm config flags or environment variables involved
- [ ] confirm tests or verification path exists, or record that it does not
- [ ] confirm whether file format, wire format, schema, or naming stability matters

## Design Checklist

- [ ] identify the narrowest extension point
- [ ] identify affected invariants
- [ ] identify whether behavior changes are local or cross-module
- [ ] identify whether the change alters config semantics
- [ ] identify whether rollback or feature-gating is needed

## Implementation Plan

1. 
2. 
3. 

## Guardrails

- Must preserve:
- Must not change without explicit review:
- Must verify in source before claiming:

## Validation Plan

- Fast checks:
- Representative tests:
- Manual verification:
- Negative-path verification:

## Review Checklist

- [ ] change matches requested behavior and non-goals
- [ ] cross-module coupling was rechecked against source
- [ ] compatibility-sensitive outputs were preserved or intentionally migrated
- [ ] observability remains adequate after the change
- [ ] context docs and design docs were updated if needed

## Context Update Requirements

- Module docs to update:
- Design docs to update:
- Additional playbooks to update:

## Open Questions

- 

## Pending Verification

- 
