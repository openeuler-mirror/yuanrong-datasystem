# Log Sampler Design

## Document Metadata

- Status:
  - `accepted design` (revised: independent per-trace nested-threshold model)
- Design scope:
  - `feature implementation design`
- Primary source-of-truth files:
  - `src/datasystem/common/log/log.h`
  - `src/datasystem/common/log/log_sampler.h`
  - `src/datasystem/common/log/log_sampler.cpp`
  - `src/datasystem/common/log/log_sample_state.h`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/trace.h`
  - `src/datasystem/common/flags/dynamic_flag_config.cpp`
- Last verified against source:
  - `2026-09-15`
- Related context docs:
  - `.repo_context/modules/infra/logging/README.md`
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/logging/trace-and-context.md`
  - `.repo_context/modules/infra/logging/access-recorder.md`
- Related playbooks:
  - `.repo_context/playbooks/features/performance-change.md`
  - `.repo_context/playbooks/features/concurrency-and-memory-safety.md`

## Revision Note

The original design used forced retention (request sampled-in forced access/diagnostic output)
plus per-event supplement sampling for non-sampled-in requests, with a request-only derivation
rule (`access=min(1,3r)`, `diagnostic=min(1,4r)`) and sticky explicit state. That supplement
semantics has been removed. The current design gives each rate parameter independent control of
its own category through a shared per-trace hash with nested thresholds. The salt is always `0`
in production; `SetSaltForTest` remains as a test-only hook. The old `log_rate_limit` remains
removed and unmapped.

## Purpose

- Sample interface logs and request logs before expensive payload formatting, with each of the
  three rate parameters independently controlling its category's retention ratio.
- Preserve complete request-log links: when a category rate is `>= request_sample_rate`, every
  sampled-in trace keeps that category's logs — guaranteed by threshold nesting, not by forced
  retention code.

## Model

All three categories compare the same per-trace hash against their own precomputed threshold:

```text
H = Mix64(FNV1a(traceID) ^ sampleSalt)    // sampleSalt is 0 in production
REQUEST:    H <= T_req    // lazily created, cached in Trace, propagated as ADMIT/REJECT
ACCESS:     H <= T_acc
DIAGNOSTIC: H <= T_diag
```

- `BuildThreshold` is monotonic in ppm, and `RateToPpm` rounding can only make thresholds equal,
  never inverted. Therefore `acc_rate >= req_rate` implies `T_acc >= T_req`, which implies every
  sampled-in trace (`H <= T_req`) keeps access/diagnostics (`H <= T_acc`).
- Each category's retention ratio equals its configured rate exactly, independent of the other
  rates.
- Budget-first-on-link: because all categories share one hash, the access/diagnostic-kept set is
  prefix-aligned with the sampled-in set. A category rate below the request rate spends its whole
  budget on sampled-in traces (`access-kept ⊆ sampled-in`); the broken band
  (`request_rate - access_rate`) is the minimum under the volume constraint and is deterministic
  and reproducible.
- Sampling randomness comes from the traceID (a freshly minted UUID per request), not from the
  salt. Within one process run the request decision was always a deterministic function of
  traceID; salt `0` only makes that function globally stable, which is what cross-process nesting
  requires. A nonzero test salt shifts all categories uniformly and preserves nesting.

## Key Rules

- **FATAL never dropped** — `LOG(FATAL)` and `CHECK` always emit regardless of config.
- **Full link by nesting** — category rate `>= request_sample_rate` keeps that category for all
  sampled-in traces; category rate `< request_sample_rate` breaks the link for a deterministic
  subset (the operator's chosen trade-off).
- **Per-trace granularity** — all access logs of one trace are kept or dropped together; the same
  applies to request-context ERROR/WARNING/PLOG. This is required by the shared-hash nesting.
- **Background logs bypass this sampler** — logs without request context follow existing logging
  behavior. `AccessKeyType::REQUEST_OUT`, `request.log`, and resource logs are outside this
  design.
- **Lifecycle and control-plane APIs are not request-sampled** — SDK lifecycle entrypoints use
  `SetTraceUUID()` traces whose RPCs carry `LOG_SAMPLE_NONE` (issue #1174). Top-level invocation
  only: nested inside a data-plane request trace, the call inherits the outer decision.
- **Cross-process consistency** — `H` is a pure function of the propagated traceID, so client and
  worker compute identical access/diagnostic decisions for identical configs. The propagated
  request decision (1-byte brpc attachment state / `MetaPb.log_sample_state`) remains the
  authority for INFO/VLOG during rolling upgrades and config-mismatch windows.
- **SLOW_LOG threshold-hit bypasses the sampler** — `ShouldCreateSlowLogMessage()` only checks
  min-log-level when the slow condition is true; the exemption covers the slow log itself only
  and never mutates Trace sampling state. The non-hit fallback (`SLOW_LOG_IF(sev, false)`) is
  classified DIAGNOSTIC and follows the diagnostic rate.
- **Random, not first-N** — deterministic hash-threshold decisions; no windows, queues, or global
  counters.

## Configuration Model

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `request_sample_rate` | double | `1.0` | complete request-log sampling ratio per trace |
| `access_sample_rate` | double | `1.0` | access-log sampling ratio, independent |
| `diagnostic_sample_rate` | double | `1.0` | request-context ERROR/WARNING/PLOG sampling ratio, independent |

- Values must be finite doubles in `[0.0, 1.0]`; invalid startup config fails fast; invalid
  dynamic updates keep previous-good and log through sampler bypass.
- No derivation: unset parameters mean `1.0` (full retention). Explicit state is not tracked.
- Worker converts rates to ppm and precomputes integer thresholds at config time only. All rates
  `1.0` folds to disabled/pass-through.
- Worker→client transport stays `LogSampleConfigPb` (`enabled` + three ppm fields) carried by
  register/heartbeat responses; clients validate and apply it. `log_rate_limit` stays removed
  (`RegisterClientRspPb` field 24 reserved).
- Hot path reads only the atomic snapshot pointer and integer thresholds: no floating point,
  division, modulo, parsing, allocation, mutex, clock, RNG, or CAS.

## Detailed Design

### Entry Points

```cpp
bool ShouldCreateLogMessage(LogSeverity);      // log.h macro gate, non-PLOG
bool ShouldCreatePlogMessage(LogSeverity);     // log.h macro gate, PLOG/SLOW_LOG fallback
bool ShouldCreateSlowLogMessage(sev, hit);     // hit: min-log-level only; miss: PLOG path
LogSampler::ShouldCreateRuntimeLog(sev, isPlog);  // single classification + decision point
LogSampler::IsCurrentRequestSampledIn();       // request decision (GetOrCreate + propagated)
LogSampler::ShouldRecordAccess(key);           // AccessRecorder construction gate
LogSampler::ShouldRecordAccessType(type);      // type-based variant
```

`ShouldCreateRuntimeLog` classification: FATAL → pass; outside request trace → BYPASS pass;
PLOG/ERROR/WARNING → DIAGNOSTIC (own threshold on `H`); otherwise REQUEST (request decision).
The `LogMessageImpl::Init()` backstop reuses the same entry only for direct constructions
(`samplerChecked=false`).

### Decision Algorithm

```cpp
bool LogSampler::ShouldCreateRuntimeLog(LogSeverity severity, bool isPlog)
{
    auto *snap = snapshot_.load(std::memory_order_acquire);
    if (snap == nullptr || !snap->config.enabled) return true;
    if (severity == LogSeverity::FATAL) return true;

    auto kind = ClassifyRuntime(severity, isPlog);
    if (kind == LogSampleKind::BYPASS) return true;
    if (kind == LogSampleKind::REQUEST) {
        if (snap->config.requestRate.ppm == 0) {
            Trace::Instance().SetRequestSampleDecision(true, false);
            return false;
        }
        return IsCurrentRequestSampledIn(snap->config.requestRate);
    }

    // DIAGNOSTIC: own threshold on the shared per-trace hash.
    const SampleRate &rate = snap->config.diagnosticRate;
    if (rate.ppm == kSamplePpmBase) return true;
    if (rate.ppm == 0) return false;
    return ShouldPassRandom(rate, Trace::Instance().GetCachedHash(),
                            sampleSalt_.load(std::memory_order_relaxed));
}
```

`ShouldPassRandom(rate, key, salt)` = `Mix64(key ^ salt) <= rate.threshold` with `ppm == 0` /
`ppm == kSamplePpmBase` early returns. `IsCurrentRequestSampledIn(rate)` keeps the GetOrCreate
semantics: propagated/cached decision wins; `ppm == kSamplePpmBase` passes without creating
state; `ppm == 0` rejects without creating a reject decision; `(0,1M)` hashes once and caches in
Trace via `SetRequestSampleDecision`. Access guards skip the key→type mapping when
`access_rate == 1.0` and never consult or create the request decision.

### Snapshot And Salt

`LogSamplerSnapshot` is published through an atomic raw pointer; old snapshots are reclaimed at
process exit / `Shutdown()` without per-log refcount traffic. `sampleSalt_` is an atomic that is
always `0` in production (`Init()` and random generation were removed); `SetSaltForTest` exists
for deterministic tests and uniformly perturbs all categories.

## Integration Points

- `log.h`: `ShouldCreateLogMessage` / `ShouldCreatePlogMessage` / `ShouldCreateSlowLogMessage`
  macro gates; `samplerChecked=true` on the macro path keeps the `LogMessageImpl` backstop
  non-primary.
- `access_recorder.cpp`: `AccessRecorder` construction calls `ShouldRecordAccess()` once and
  caches `shouldRecord_`; `Trace::SetAccessShouldRecord()` feeds the latency-summary gate in
  `latency_phase.h`. `IsCurrentRequestLogSampled()` computes the `logSampled:true` marker from
  the request decision only (request INFO visibility); it is unrelated to access retention.
- `log_sample_state.h`: `GetOrCreateLogSampleState()` / `ApplyLogSampleState()` propagate and
  restore the request decision over brpc attachments and zmq `MetaPb.log_sample_state`. Wire
  formats are unchanged by this design.
- Configuration: `logging.cpp` defines the three dynamic gflags; `dynamic_flag_config.cpp`
  `ValidateAndCommitSamplerFlags()` / `CommitSamplerFlagsTransaction()` validate-then-commit
  batches and republish the snapshot from current flag values.

## Performance

- Disabled/all-1.0: one atomic snapshot/enabled check.
- Rate `1.0` / `0.0`: direct pass/drop after classification, no key construction or hashing.
- `(0.0,1.0)`: one `Mix64` + threshold compare per event. DIAGNOSTIC/ACCESS paths no longer read
  or write Trace decision state and no longer maintain a `thread_local` sequence — strictly
  cheaper than the previous forced-retention + per-event design.
- Forbidden on the hot path: string parsing, config parsing, heap allocation, mutex, file IO,
  RPC, logging inside the sampler, runtime RNG, clock reads, global atomic counters, CAS, spin,
  floating point, division, modulo, payload hashing, production debug counters.
- Distribution gate: 100000 fixed keys at `0.1/0.5/0.9` within ±1%; 20-bucket split of the 50%
  case within ±5% per bucket with samples in both halves (validates FNV1a+Mix64 uniformity under
  salt 0).

## Correctness Invariants

- FATAL/CHECK never dropped.
- `H <= T_req ∧ T_cat >= T_req ⟹ H <= T_cat` (full-link nesting).
- Category retention equals its configured rate, independent of other rates.
- Per-trace all-or-nothing for access and request-context diagnostics.
- Deterministic per traceID: same trace + same config gives identical decisions across processes
  and restarts; retries of one logical request share the decision.
- Rolling-upgrade window: mixed old (random salt, forced retention) and new binaries may disagree
  transiently; same-version clusters are fully consistent. The propagated ADMIT/REJECT byte still
  governs INFO/VLOG end to end.
- Dynamic config update mid-request can flip access/diagnostic decisions within one trace (same
  exposure class as the previous per-event design); the request INFO link stays stable through
  the cached Trace decision.
- `logSampled:true` means ordinary request INFO is visible; it does not describe access sampling.
- Config update failure retains previous-good and emits diagnostics through sampler bypass.

## Verification Cases

| ID | Scenario | Precondition | Expected |
| --- | --- | --- | --- |
| LS-001 | Default full retention | empty config | all logs emitted, no hash/clock/CAS |
| LS-001b | All-1.0 fold | three rates `1.0` | disabled/pass-through snapshot |
| LS-002 | INFO payload skip | `request_sample_rate=0.0` | rejected `LOG(INFO) << Expensive()` skips `Expensive()` |
| LS-003 | Access pre-drop | `access_sample_rate=0.0` guard | no recorder, no `clock::now()`, no `ToString()`, no exporter |
| LS-004 | FATAL always passes | all rates `0.0` | `LOG(FATAL)` / `CHECK(false)` abort |
| LS-005 | Diagnostics independent | `request=0.0, diagnostic=0.5` | ERROR/WARNING kept ≈50% per trace, unrelated to request decision |
| LS-006 | Nesting above | `request=0.5, access=0.8` | sampled-in ⟹ access kept; total access ≈80% |
| LS-007 | Nesting below (link break) | `request=0.5, access=0.2` | access-kept ⊆ sampled-in; broken band exists; access ≈20% |
| LS-008 | Retention precision | `request=0.1, access=0.3` | request ≈10%, access ≈30% ±3% |
| LS-009b | Config failure bypass | invalid dynamic update | previous-good retained, failure log bypasses sampler |
| LS-012 | Cross-process consistency | client-worker RPC | identical per-trace decisions for identical configs; propagated ADMIT/REJECT restores INFO/VLOG state |
| LS-013 | Worker register/heartbeat config | worker sends `LogSampleConfigPb` | client applies; missing field keeps current config |
| LS-014 | Access marker semantics | request sampled/rejected | `logSampled:true` follows request-level state only |
| LS-015 | Random ratio / buckets | 100000 keys | ±1% ratio; ±5% per bucket |
| LS-016 | Hot-path constraints | benchmark + allocator hooks | no allocation, lock, clock, global atomic, or CAS |
| LS-017 | Salt perturbation invariant | nonzero test salts | nesting invariant holds for any salt value |
| LS-018 | Determinism | same traceID + config across resets | identical decisions |

## Implementation Scope

| Module | Changes |
| --- | --- |
| `common/log/log_sampler.{h,cpp}` | shared-hash decision core, snapshot publication, salt hook |
| `log.h` | macro gates unchanged from the original design |
| `log_message_impl.cpp` | backstop reusing `ShouldCreateRuntimeLog` |
| `access_recorder.cpp` | construction-time gate + `logSampled` marker |
| `logging.cpp` | three dynamic gflags, startup snapshot publication |
| `dynamic_flag_config.cpp` | sampler flag batch validate-then-commit |
| `log_sample_state.h` / brpc attachment / `MetaPb` | request-decision propagation (unchanged formats) |
| `share_memory.proto` | `LogSampleConfigPb` (unchanged) |
| tests | nesting invariants, retention precision, determinism, distribution gates, concurrency |
