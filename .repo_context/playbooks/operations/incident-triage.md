# Incident Triage

## Metadata

- Status:
  - `active`
- Playbook scope:
  - incident triage | diagnosis workflow
- Owning module or area:
  - `modules/infra/observability`
- Primary context docs:
  - `.repo_context/modules/infra/observability/diagnosis-and-operations.md`
  - `.repo_context/modules/infra/observability/signal-map.md`
  - `.repo_context/modules/quality/tests-and-reproduction.md`
- Last verified against source:
  - `2026-04-13`

## When To Use

- Use when:
  - a runtime, health, observability, or cluster symptom needs a first-pass localization.
- Do not use when:
  - the issue is already narrowed to one implementation detail and coding can begin directly.

## First 15 Minutes

1. Name the symptom precisely: startup failure, not ready, liveness failure, request error, latency regression, missing monitor output, topology anomaly.
2. Record the exact time window, affected node or process, and recent config or rollout changes.
3. Collect the minimum evidence set:
   - readiness file
   - liveness file
   - ordinary logs
   - access logs if request-path issue
   - resource monitor files if DFX issue
4. Confirm key config:
   - `log_dir`
   - `log_monitor`
   - `ready_check_path`
   - `liveness_check_path`
   - `etcd_address` or `metastore_address`
5. Use `signal-map.md` to pick the first code entrypoint.

## Evidence Checklist

- Process or service identity
- Time window
- Probe-file state
- Log directory state
- Relevant config values
- One likely owning module
- One fallback module if the first guess is wrong

## Escalation Rule

- Escalate after first-pass evidence if:
  - the symptom crosses runtime, logging, and metrics with no clear first failure;
  - cluster-management or backend state appears inconsistent;
  - health artifacts contradict each other and source-backed interpretation is unclear.

## Outcome Format

- Current symptom:
- First failing or degraded signal:
- Most likely owning module:
- Evidence collected:
- Next reproduction or verification step:
