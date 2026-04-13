# <Module Name> Design

## Document Metadata

- Status:
  - `draft` | `active` | `deprecated`
- Design scope:
  - new feature | current implementation | refactor plan | mixed
- Primary code paths:
- Primary source-of-truth files:
- Last verified against source:
- Related context docs:
- Related user-facing or internal docs:

## Purpose

- Why this design document exists:
- What problem this module solves:
- Who or what depends on this module:

## Goals

- 

## Non-Goals

- 

## Scope

- In scope:
- Out of scope:
- Important boundaries with neighboring modules:

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
|  |  |  |

## Architecture Overview

- High-level structure:
- Key runtime roles or processes:
- Key persistent state, if any:
- Key control-flow or data-flow stages:

## Entry Points And Integration Points

- Public APIs:
- Internal service entrypoints:
- Background jobs / threads / callbacks:
- Config flags or environment variables:
- Cross-module integration points:

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
|  |  |  |  |

## Main Flows

### <Flow Name>

1. 
2. 
3. 

Key files:

- 

Failure-sensitive steps:

- 

### <Optional Second Flow>

1. 
2. 
3. 

Key files:

- 

Failure-sensitive steps:

- 

## Data And State Model

- Important in-memory state:
- Important on-disk or external state:
- Ownership and lifecycle rules:
- Concurrency or thread-affinity rules:
- Ordering requirements or invariants:

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
|  |  |  |  |  |

## Failure Model

- Expected failure modes:
- Partial-degradation behavior:
- Retry, fallback, or fail-fast rules:
- What must remain true after failure:

## Observability

- Main logs:
- Metrics:
- Traces or correlation fields:
- Debug hooks or inspection commands:
- How to tell the module is healthy:

## Security And Safety Constraints

- Authn/authz expectations:
- Input validation requirements:
- Data sensitivity or privacy requirements:
- Resource or isolation constraints:
- Unsafe changes to avoid:

## Compatibility And Invariants

- External compatibility constraints:
- Internal invariants:
- File format / wire format / schema stability notes:
- Ordering or naming stability notes:

## Performance Characteristics

- Hot paths:
- Known expensive operations:
- Buffering, batching, or async behavior:
- Resource limits or scaling assumptions:

## Build, Test, And Verification

- Build entrypoints:
- Fast verification commands:
- Representative unit/integration/system tests:
- Recommended validation for risky changes:

## Common Change Scenarios

### Adding A New Capability

- Recommended extension point:
- Required companion updates:
- Review checklist:

### Modifying Existing Behavior

- Likely breakpoints:
- Compatibility checks:
- Observability checks:

### Debugging A Production Issue

- First files to inspect:
- First runtime evidence to inspect:
- Common misleading symptoms:

## Open Questions

- 

## Pending Verification

- 

## Update Rules For This Document

- Keep design statements aligned with the real implementation and linked source files.
- When module boundaries, major flows, config semantics, or compatibility rules change, update this doc in the same task when practical.
- If a section becomes speculative or stale, narrow it or move it to `Pending verification` until confirmed from source.
