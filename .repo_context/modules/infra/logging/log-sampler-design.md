# Log Sampler Design

## Document Metadata

- Status:
  - `accepted design`
- Design scope:
  - `feature implementation design`
- Primary code paths:
  - `src/datasystem/common/log/*`
  - `src/datasystem/common/log/spdlog/*`
  - `src/datasystem/common/metrics/hard_disk_exporter/*`
  - `src/datasystem/common/rpc/zmq/*`
  - `src/datasystem/common/util/gflag/*`
- Primary source-of-truth files:
  - `src/datasystem/common/log/log.h`
  - `src/datasystem/common/log/logging.cpp`
  - `src/datasystem/common/log/access_recorder.cpp`
  - `src/datasystem/common/log/trace.h`
  - `src/datasystem/common/log/trace.cpp`
  - `src/datasystem/common/log/spdlog/log_message_impl.cpp`
  - `src/datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.cpp`
  - `src/datasystem/common/util/gflag/flags.cpp`
  - `src/datasystem/common/rpc/zmq/zmq_common.h`
  - `src/datasystem/protos/share_memory.proto`
  - `src/datasystem/protos/meta_zmq.proto`
  - `src/datasystem/worker/worker_service_impl.cpp`
  - `src/datasystem/client/client_worker_common_api.cpp`
- Last verified against source:
  - `2026-05-25`
- Related context docs:
  - `.repo_context/modules/infra/logging/README.md`
  - `.repo_context/modules/infra/logging/design.md`
  - `.repo_context/modules/infra/logging/trace-and-context.md`
  - `.repo_context/modules/infra/logging/access-recorder.md`
  - `.repo_context/modules/infra/logging/log-lifecycle-and-rotation.md`
- Related playbooks:
  - `.repo_context/playbooks/features/infra/logging/implementation.md`
  - `.repo_context/playbooks/features/performance-change.md`
  - `.repo_context/playbooks/features/concurrency-and-memory-safety.md`

## Purpose

- Why this design document exists:
  - record the implementation design for low-overhead request/access/diagnostic random log sampling.
- What problem this module solves:
  - sample interface logs and request logs before expensive payload formatting, while preserving complete logs for
    sampled-in requests and allowing diagnostic-only retention for non-sampled-in requests.

## Business And Scenario Overview

- Why this capability is needed:
  - current logging control is mostly request-trace based. It does not truly sample access logs before formatting, can drop useful request diagnostics with ordinary request sampling, and uses first-N style sampling that is biased toward the start of each window.
- Target users:
  - service operators setting log-volume policy, SDK/client developers, worker developers, and performance-test owners.
- How the feature is expected to be used:
  - operators configure three worker-side sampling rate parameters. Each parameter controls a specific log category.
  - code paths ask the sampler before formatting expensive payloads.
- Expected experience:
  - performance: disabled sampling keeps current behavior and cost. Enabled sampling performs only fixed integer work and skips payloads for dropped logs.
  - security: sampler does not inspect, hash, persist, or buffer log payload contents.
  - resilience: FATAL/CHECK behavior remains intact.
  - reliability: request ERROR/WARNING/PLOG diagnostics are forced out when the request is sampled in, and otherwise
    use one diagnostic supplement coefficient.

## Goals

- Support external API access / client access log sampling, with hot paths deciding before `AccessRecorder`,
  `RequestParam`, elapsed-time calculation, `RequestParam::ToString()`, and `HardDiskExporter::Send()`.
- Support runtime log sampling before `LOG` stream payload evaluation.
- Support complete request-log sampling plus request diagnostic supplement sampling. Background logs are no longer sampled by this design.
- Use random sampling, not fixed-window first-N, smoothed first-N variants, or global queue quotas.
- Support explicit sampling coefficients for complete request logs, access, and request diagnostics only.
- Preserve FATAL/CHECK abort behavior.
- Preserve request-level cross-process sampling consistency for the complete request-log sampled-in decision. Diagnostics
  and access logs are forced out when the request is sampled in; request rejection directly drops only ordinary INFO/VLOG.
- Treat worker effective config as the authority. Clients do not independently configure sampling; they receive
  structured config from worker register / heartbeat responses.
- Empty config means full retention: no sampling drops are introduced.
- Explicit all-`1.0` config is folded to disabled/pass-through at config publication time.
- PLOG must be checked before stream payload evaluation, same as ordinary `LOG`, even though it bypasses request-level sampling.

### Performance Goals

- Performance is the first priority of this design.
- Empty config: one lightweight snapshot/enabled check only; no clock read, counter access, CAS, hash, or payload work.
- Sampling coefficient of `1.0`: pass that category without building sampling keys or hashing.
- Sampling coefficient of `0.0`: avoid sampling-key construction and hashing. Ordinary request INFO/VLOG drops before
  payload evaluation; diagnostic/access first honor request sampled-in forced retention, then drop non-sampled-in events.
  For `request_sample_rate=0.0`, ordinary request INFO/VLOG or RPC metadata paths create and propagate request-trace
  reject decisions.
- Enabled sampling in `(0.0,1.0)`: fixed integer work plus one lightweight hash-threshold comparison.
- `request_sample_rate`: hash only when creating the first request-trace decision. Later ordinary INFO/VLOG logs in the
  same trace read the cached `Trace` decision only; diagnostics/access use only the positive sampled-in result for forced
  retention.
- Hot path must not parse strings, allocate heap memory, take mutexes, perform file IO/RPC, generate runtime random numbers, read time, use global atomic counters, use CAS, spin, use floating-point math, divide, modulo, or log from inside the sampler.
- Sampling coefficients must be converted to integer thresholds at config publication time.
- Config snapshot reads must not use per-log reference-count increments.
- Production hot paths must not increment global debug counters. Test observability must use disabled-by-default
  test-only hooks or thread-local aggregation.
- 4000+ QPS with 32 concurrent threads: empty config must show no measurable throughput or p99 regression; enabled sampling must report sampler-only overhead and pass performance review thresholds.

## Non-Goals

- Provide exact per-second hard caps.
- Provide cluster-wide global log-volume accounting.
- Replace log rotation, compression, retention, or downstream collection.
- Change ordinary log line format or access-log field schema.
- Buffer log payloads for later retrospective sampling.
- Sample `request.log`, `request_out.log`, resource logs, or `AccessKeyType::REQUEST_OUT`.
- Provide `log_rate_limit` compatibility. The old field is removed and not mapped into the new sampling rates.

## Scope

- In scope:
  - new process-local `LogSampler` component for three-category random sampling;
  - integration with existing request-level trace state for complete request-log sampled-in consistency;
  - three worker-side sampling-rate gflags plus dynamic config integration;
  - structured `LogSampleConfigPb` propagation from worker to client through register and heartbeat responses;
  - unit and performance tests, including 4000+ QPS with 32 concurrent threads.
- Out of scope:
  - centralized log collection service changes;
  - external observability backend integration;
  - downstream log parser changes;
  - standalone client-side sampling config or environment-variable sampling config.

## Terminology

The important new concepts are sampling terms and configuration attributes, not a list of every new implementation type.
Use these meanings throughout the design.

| Term / attribute | Meaning | Value or lifecycle | Implementation note |
| --- | --- | --- | --- |
| sample / sampled-in / sampled-out | A sampling decision. sampled-in means keep the log and continue construction; sampled-out means drop it | One result per request trace or per event, depending on the log path | sampled-out must happen before payload evaluation. FATAL/CHECK are always sampled-in |
| sample rate / `*_sample_rate` | User-facing sampling ratio expressed as a decimal | Worker parameter value in `[0.0,1.0]`; `1.0` keeps 100%, `0.0` means no supplement sampling for that class | This is ratio sampling, not QPS. Missing parameters default to `1.0`; request sampled-in forced retention still applies to diagnostic/access |
| explicit sample rate | Whether a `*_sample_rate` was explicitly configured | Explicit when present in startup args, dscli, EmbeddedConfig, or dynamic config | Used by the request-only derivation rule. Explicit `1.0` still counts as explicit |
| normalized ppm | Internal integer ratio produced by worker config parsing | Range `[0,1000000]`; used only for threshold computation and worker-to-client proto | Not exposed in user config. Hot path reads ppm/threshold only, no floating point |
| pass-through / disabled | Sampler is logically off and does not drop logs | All three parameters unset, or all three effective sample rates are `1.0`, normalizes to `enabled=false` | Hot path must be equivalent to no config: one lightweight enabled check |
| request trace sampling | One decision shared by a request trace | Controlled by `request_sample_rate`; propagated through `Trace` / `MetaPb.log_sample_state` | Controls the complete request-log baseline. sampled-in keeps request INFO/VLOG/ERROR/WARNING/PLOG and forces access retention |
| per-event sampling | One decision for one log event | Used by diagnostic and access only when the request is not sampled in | Not propagated across processes. New events immediately use the current config snapshot |
| sample key | Stable integer input to the sampling hash | request trace decisions use request trace hash; per-event uses trace hash, log kind, and sequence | Must not use log payload text, formatted strings, `ostringstream` content, file names, or line numbers |
| `thread_local sequence` | Per-thread incrementing sequence for decorrelating repeated per-event logs | Independent per thread for process lifetime | Must stay thread-local, not a global atomic |
| sample salt | Process-level hash perturbation value | Generated once at process start; stable across config updates; tests may inject a fixed seed | Log kind is part of the sample key, so no extra salt map is kept. Runtime RNG is forbidden |
| threshold | 64-bit cutoff derived from normalized ppm | Computed at config publication; `1.0` / ppm `1000000` can fold to always pass, `0.0` / ppm `0` to direct reject | Hot path only does `Mix64(key ^ sampleSalt) <= threshold`; no floating point, division, or modulo |
| previous-good config | Last successfully parsed and validated config snapshot | Retained after invalid dynamic update; if none exists, stay pass-through | Invalid config must not partially apply or replace the active config |
| sampler-bypass log | Error log for sampler/config failures | Used only from sampler/config paths | Must bypass the sampler so bad config cannot hide its own failure; do not classify it under `diagnostic_sample_rate` |
| `logSampled:true` | Access-log marker for request sampling semantics | Means ordinary request INFO is visible / not request-rejected | Does not mean this access record passed `access_sample_rate`; request-sampled access may carry it |
| statistical expected-volume reduction | Output volume approaches the configured ratio statistically | No exact per-second hard cap | Exact QPS needs clock reads, counters, CAS, or queues, which violate hot-path goals |

## Architecture Overview

### Log Processing Flow

Every log emission goes through this sequence:

```text
1. FATAL/CHECK -> always pass
2. External API access / client access log -> force emit when request is sampled in; otherwise apply access_sample_rate
3. ERROR/WARNING/PLOG with request context -> force emit if request is sampled in; otherwise check diagnostic_sample_rate
4. Request INFO/VLOG -> make one random request-trace decision via request_sample_rate; reuse it for the trace
5. Background logs -> bypass this sampler and follow existing logging behavior
```

### Log Categories And Configuration

| Log category | Examples | Affected by request rejection? | Sampling coefficient |
| --- | --- | --- | --- |
| FATAL | `LOG(FATAL)`, `CHECK` | No, always pass | Never sampled |
| Request diagnostic | `LOG(ERROR)`, `LOG(WARNING)`, `PLOG` with request context | sampled-in forces emit; sampled-out does not directly drop it | `request_sample_rate` sampled-in OR `diagnostic_sample_rate` |
| External API access / client access | `AccessRecorder` with `AccessKeyType::ACCESS` / `CLIENT` | Request sampled-in forces emit; otherwise independent sampling | `access_sample_rate` |
| request_out / resource | `AccessKeyType::REQUEST_OUT`, resource exporter | No | Not controlled by this design |
| Request INFO/VLOG | `LOG(INFO)` / `VLOG`, has request context | Yes, request rejection drops INFO/VLOG | `request_sample_rate` per request trace |
| Background logs | INFO/VLOG/ERROR/WARNING/PLOG without request context | No request sampling | Not controlled by this design |

### Random Sampling Model

`request_sample_rate` is the random sampling coefficient for complete request logging at request-trace granularity. It is
not a per-log-line counter. Once a request trace is sampled in, ordinary request INFO/VLOG and request
ERROR/WARNING/PLOG logs for that propagated trace are emitted as a complete request log across client/worker RPC hops.
Once rejected, ordinary request INFO/VLOG logs are dropped before stream payload evaluation, while request diagnostics
may still be retained by `diagnostic_sample_rate`.

ERROR, WARNING, and PLOG with request context are request diagnostics. They first consult the positive request sampled-in
decision: if the request is sampled in, they pass to preserve complete request logs; if the request is not sampled in,
they use per-event `diagnostic_sample_rate` supplement sampling. A sampled-out/reject decision is not itself a drop
condition for diagnostics. External API access / client access logs are controlled by `access_sample_rate`, but a request
sampled in by `request_sample_rate` must emit its access log.
`request.log`, `request_out.log`, resource logs, and `AccessKeyType::REQUEST_OUT` are outside `access_sample_rate`.
Background logs are not sampled by this design.

Sampling uses stable integer keys and a precomputed 64-bit threshold. Request INFO/VLOG uses the request trace hash as
the key. Per-event diagnostic/access sampling uses only `traceHash`, log kind, and `thread_local` sequence; it does not
use file names, line numbers, or `AccessRecorderKey`.

```text
REQUEST:      hash(trace key, sample salt) <= request threshold -> complete request log
DIAGNOSTIC:   request sampled-in || hash(event key, sample salt) <= diagnostic threshold
ACCESS:       request sampled-in || hash(event key, sample salt) <= access threshold
```

This controls expected output volume statistically. It is intentionally not an exact QPS cap: avoiding clock reads,
global contention, CAS, and queues on the logging hot path is more important than exact accounting.

### Key Rules

- **FATAL never dropped** — `LOG(FATAL)` and `CHECK` always emit regardless of config.
- **Complete request logs win** — request-context INFO/VLOG/ERROR/WARNING/PLOG all emit when the request is sampled in.
- **Diagnostics use supplement sampling** — request-context ERROR, WARNING, and PLOG use `diagnostic_sample_rate` only
  when the request is not sampled in. A sampled-out request decision does not directly drop diagnostics.
- **Interface logs use the simplest OR rule** — `AccessKeyType::ACCESS` / `CLIENT` access logs emit when the request is
  sampled in; otherwise they use independent `access_sample_rate` sampling. This does not compute conditional correction, so
  final access retention can exceed `access_sample_rate`; the benefit is minimum state and minimum hot-path logic.
- **Background logs bypass this sampler** — logs without request context follow existing logging behavior.
- **Cross-process consistency scope** — request-level sampling decisions propagate through existing
  `MetaPb.log_sample_state` as complete request-log sampled-in/reject state. Existing sampled-in decisions override local
  `request_sample_rate`; reject directly drops only ordinary INFO/VLOG. local `request_sample_rate=1.0` does not create
  sampled-in state when no upstream decision exists.
- **Random, not first-N** — no coefficient may be implemented with fixed-window first-N, time-smoothed first-N, a global queue, or a global counter.
- **Performance first** — no runtime RNG, no exact cluster quota, no reservoir sampling, no buffering, and no delayed
  payload decisions. `0.0`, `1.0`, and disabled configs must be fast paths.

## Entry Points And Integration

- Public APIs: three typed `EmbeddedConfig` setters approved for embedded worker sampling configuration:
  `RequestSampleRate(double)`, `AccessSampleRate(double)`, `DiagnosticSampleRate(double)`.
  These provide type safety, discoverability, and range validation over the `SetArg/SetArgs` string approach.
  `SetArg/SetArgs` remains available as an alternative.
  Call sites use typed `AccessRecorder::Object/Stream/RequestOut` facades to describe cheap
  values, same-scope references, or providers for expensive fields, then call `Record()`.
  Sampling decisions, sampled-out fast return, elapsed time, request formatting, and exporter
  writes are owned by `AccessRecorder`. `ShouldRecordAccess(AccessRecorderKey)` is an internal
  sampler API used by recorder internals. Ordinary business call sites must not add
  `ShouldRecordAccess()` or `ShouldRecord()` branches for access logging.
- Internal entrypoints:
  - `ShouldCreateLogMessage()` -> classification-first runtime lightweight sampler entrypoint.
  - `ShouldCreatePlogMessage()` -> severity check + runtime lightweight sampler entrypoint with PLOG classified as `DIAGNOSTIC`, before PLOG stream payload evaluation.
  - `LogMessageImpl::Init()` -> defensive backstop; PLOG (`forceLog_=true`) skips request reject as a direct drop
    condition only.
  - `ShouldRecordAccess(AccessRecorderKey)` -> internal sampler API used by `AccessRecorder` facade
    internals at construction time. Business call sites use `AccessRecorder::Object/Stream/RequestOut`
    facades with setter methods that short-circuit when sampled-out. `AccessRecorder::Record()` keeps
    a final gate before `RequestParam::ToString()`.
- RPC: `MetaPb.log_sample_state` is used for complete request-log sampled-in consistency. Per-event supplement sampling
  decisions are process-local and are not propagated.
- `logSampled:true` means ordinary request INFO is visible / not rejected under current request-level sampling semantics;
  it does not require an explicit sampled-in Trace state and does not describe access sampling. Request-sampled access logs
  may carry `logSampled:true` because their request INFO is visible.

## Configuration Model

### Worker Parameters

The user-facing worker configuration is split into three independent parameters:

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `request_sample_rate` | double | `1.0` | complete request-log sampling ratio per request trace |
| `access_sample_rate` | double | `1.0` | supplemental external API access / client access sampling ratio |
| `diagnostic_sample_rate` | double | `1.0` | request ERROR/WARNING/PLOG supplement sampling ratio when request is not sampled in |

These three parameters are the only user-facing sampling configuration entrypoints. The old unified JSON-string config
entrypoint is not provided. Ordinary clients do not parse these parameters; they receive structured
`LogSampleConfigPb` from the worker.

### Parameters

Three explicit sampling coefficients, each defaulting to `1.0` unless derived from `request_sample_rate`:

| Parameter | Controls | Default |
| --- | --- | --- |
| `request_sample_rate` | complete request logs, per request trace | `1.0` |
| `access_sample_rate` | access supplement sampling; request sampled-in forces access emit | `1.0` |
| `diagnostic_sample_rate` | request ERROR/WARNING/PLOG supplement sampling when request is not sampled in | `1.0` |

Example startup flags:

```bash
--request_sample_rate=0.5 --access_sample_rate=1.0 --diagnostic_sample_rate=1.0
```

For a service receiving 4000 QPS and producing one external API access log per request, `request_sample_rate=0.1` and
`access_sample_rate=0.3` means all access logs for the sampled-in 10% requests are emitted, and the remaining requests are
sampled independently at 30%. Expected access retention is roughly `0.1 + 0.9 * 0.3 = 0.37`, not a hard QPS cap.

### Validation And Derivation

- All three parameter values must be finite doubles in `[0.0, 1.0]`; negative values, values greater than `1.0`, NaN,
  inf, and values that cannot be parsed as finite doubles are invalid.
- Missing parameters default to `1.0`.
- Special case: when one config input explicitly contains only `request_sample_rate=r`, and neither
  `access_sample_rate` nor `diagnostic_sample_rate` is explicit, worker derives:
  - `access_sample_rate = min(1.0, r * 3)`;
  - `diagnostic_sample_rate = min(1.0, r * 4)`.
- If `access_sample_rate` or `diagnostic_sample_rate` is explicitly configured, request-only derivation does not apply
  for that config input. Explicit values are used, and missing values use the ordinary `1.0` default.
- Explicit `1.0` still counts as explicit. For example, `request_sample_rate=0.2,access_sample_rate=1.0` does not derive
  diagnostic as `4r`; `diagnostic_sample_rate` defaults to `1.0`.
- Dynamic config must preserve per-parameter explicit state. If only `request_sample_rate` is explicit, later request
  updates continue to recompute derived access/diagnostic rates. Once access or diagnostic is explicit, later request
  updates must not overwrite that explicit parameter.
- Worker config parsing converts rates to internal ppm with `ppm = round(rate * 1000000)` and precomputes integer thresholds. This conversion never runs on log emission.
- All three effective rates equal to `1.0` normalize to the disabled/pass-through snapshot.
- Dynamic update: validation failure -> keep previous-good config, log through sampler bypass.
- Startup with invalid explicit config -> fail fast.
- Rate parsing, rate-to-ppm conversion, and threshold conversion happen only at config change time. The process sample salt
  is generated once at process start and never on log emission.

### Deployment Scenarios

**K8s worker:** add to Pod spec args:

```yaml
args:
- "--request_sample_rate=0.2"
```

Workers never read environment variables.

**dscli / worker startup:**

`dscli` is treated as a command-line worker launcher / management tool here, not as an ordinary client. Its sampling
rate arguments configure the worker effective config:

```bash
dscli --request_sample_rate=0.2
```

**SDK embedded worker:**

`EmbeddedConfig::SetArg()` can set the three embedded worker / worker-side sampling parameters:

```cpp
EmbeddedConfig config;
config.SetArg("request_sample_rate", "0.2");
config.SetArg("access_sample_rate", "0.6");
config.SetArg("diagnostic_sample_rate", "0.8");
// must be called before Logging::Start()
```

Or using the typed setter approach (recommended for type safety and discoverability):

```cpp
EmbeddedConfig config;
config.RequestSampleRate(0.2).AccessSampleRate(0.6).DiagnosticSampleRate(0.8);
// must be called before Logging::Start()
```

**Client config source:**

Ordinary clients do not read environment variables, startup flags, or `EmbeddedConfig` for log sampling.
Before register, clients default to pass-through. Register and heartbeat responses carry the worker effective
`LogSampleConfigPb`; clients validate and apply that structured config.

**Precedence:** worker — dynamic config > startup flag / dscli / EmbeddedConfig > pass-through. client — worker
register / heartbeat config > pass-through. Missing config fields are handled defensively by leaving the current client
config unchanged; this is not a rolling-upgrade compatibility commitment.

### Worker-To-Client Proto

Worker local input uses the three sampling-rate parameters above. Worker-to-client transport uses structured protobuf,
not JSON:

```protobuf
message LogSampleConfigPb {
    bool enabled = 1;
    uint32 access_sample_ppm = 2;
    uint32 request_sample_ppm = 3;
    uint32 diagnostic_sample_ppm = 4;
}

message RegisterClientRspPb {
    reserved 24; // old log_rate_limit, removed; do not reuse
    LogSampleConfigPb log_sample_config = 25;
}

message HeartbeatRspPb {
    ...
    bool unhealthy = 7;
    LogSampleConfigPb log_sample_config = 8;
}
```

Rules:

- `LogSampleConfigPb` is a message field, so clients use `has_log_sample_config()` to distinguish present config from
  defensive missing fields.
- `enabled=false` means explicit pass-through. Worker leaves the three ppm fields unset / default `0` to reduce wire size;
  clients must not read ppm fields when disabled.
- `enabled=true` means all three ppm fields are populated. Missing parameters were already filled by default/derived rules by the worker.
- Clients validate enabled configs by checking all three ppm fields are in `[0,1000000]`. Invalid configs keep
  previous-good; if no previous-good exists, pass-through remains.
- Clients compare `enabled` and the three ppm fields to avoid unnecessary updates. For `enabled=false`, only `enabled`
  participates in semantic comparison.
- Responses are not AK/SK verified under the current protocol rules; adding this config does not add response signing.
- `log_rate_limit` is removed, not mapped to the new sampling rates or ppm, and field 24 is reserved.

## Detailed Design

### Core Data Structures

```cpp
static constexpr uint32_t kSamplePpmBase = 1000000;
static constexpr uint64_t kAlwaysSampleThreshold = UINT64_MAX;

struct LogSampleUserConfig {
    bool requestSampleRateExplicit;
    bool accessSampleRateExplicit;
    bool diagnosticSampleRateExplicit;
    double requestSampleRate;    // complete request logging, per request trace
    double accessSampleRate;     // access supplement sampling
    double diagnosticSampleRate; // supplement request ERROR/WARNING/PLOG when request is not sampled-in
};

struct SampleRate {
    uint32_t ppm;          // 0..1000000
    uint64_t threshold;    // precomputed from ppm; UINT64_MAX when ppm == kSamplePpmBase
};

struct LogSampleConfig {
    bool enabled;
    uint64_t sampleSalt;   // generated once at process start
    SampleRate requestRate;
    SampleRate accessRate;
    SampleRate diagnosticRate;
};

struct LogSamplerSnapshot {
    LogSampleConfig config;
};
```

There is no production catch-all sampling request object. Such a struct would mix runtime, access, request-trace, and
diagnostic inputs, and would encourage callers to read `Trace`, build access keys, or fill irrelevant fields before
fast-path checks. The production implementation exposes path-specific lightweight entrypoints; each entrypoint accepts
only the minimum data needed for that path.

`LogSamplerSnapshot` is published through an atomic raw pointer. Published snapshots are immutable and reclaimed at
process exit. This lifecycle strategy avoids use-after-free without adding `shared_ptr` reference-count traffic to every
log attempt and is part of the production implementation contract.

When all three parameters are unset or all three effective rates are `1.0`, the config normalizes to `enabled=false`.
The worker-to-client proto then sets only `enabled=false` and leaves ppm fields unset/default `0`. One process-level
`sampleSalt` is generated once at process start, remains stable across config updates, and can be injected from a fixed
seed in tests.

### Decision Algorithm

Production implementation must expose lightweight entrypoints instead of forcing every path through "build a unified
request object, then call one sampler entrypoint":

```cpp
bool IsSamplerEnabledFast();
bool ShouldCreateRuntimeLog(LogSeverity severity, bool isPlog);
bool ShouldRecordAccess(AccessRecorderKey key);
bool IsCurrentRequestSampledIn(const SampleRate &requestRate);
bool ShouldSampleEvent(uint64_t traceHash, LogSampleKind kind, const SampleRate &rate);
```

These entrypoints move fast paths earlier: disabled / all-`1.0` performs only the snapshot/enabled check; FATAL/CHECK
passes at the macro layer; rate `1.0` passes directly; rate `0.0` avoids hashing. Ordinary request INFO/VLOG drops
directly at `0.0`; diagnostic/access at `0.0` first honor request sampled-in forced retention, then drop non-sampled-in
events. Event-key construction, `thread_local` sequence increments, and hashing happen only for supplement rates in
`(0.0,1.0)`.

The following split pseudocode shows the intended control flow:

```cpp
bool LogSampler::ShouldCreateRuntimeLog(LogSeverity severity, bool isPlog)
{
    auto *snapshot = snapshot_.load(std::memory_order_acquire);
    if (snapshot == nullptr || !snapshot->config.enabled) {
        return true;
    }
    if (severity == FATAL) {
        return true;
    }

    auto kind = ClassifyRuntime(severity, isPlog);
    if (kind == BYPASS) {
        return true;
    }
    if (kind == REQUEST) {
        // Cached reject returns false for ordinary request INFO/VLOG before payload evaluation.
        return IsCurrentRequestSampledIn(snapshot->config.requestRate);
    }

    const SampleRate &rate = GetRate(snapshot->config, kind);
    if (rate.ppm == kSamplePpmBase) {
        return true;
    }
    if (kind == DIAGNOSTIC && IsCurrentRequestSampledIn(snapshot->config.requestRate)) {
        return true;
    }
    if (rate.ppm == 0) {
        return false;
    }

    uint64_t traceHash = GetCurrentTraceHashOrZero();
    return ShouldSampleEvent(traceHash, kind, rate);
}
```

`IsCurrentRequestSampledIn()` uses existing request sampling state in `Trace`: if a propagated sampled-in decision
exists, return true. Propagated sampled-in decisions override local `request_sample_rate`. If a reject decision is read,
ordinary request INFO/VLOG drops directly; diagnostic/access do not treat reject as a drop condition and continue to
supplement sampling. If no local decision exists:

- `request_sample_rate=1.0` (ppm `1000000`): pass directly and do not create sampled-in state. If an upstream decision exists, still
  obey and propagate that decision.
- `request_sample_rate=0.0` (ppm `0`): ordinary request INFO/VLOG or RPC metadata propagation creates a request-trace
  reject decision in `Trace`; diagnostic/access only need a sampled-in check, can treat this as false, and do not need to
  create reject just to run supplement sampling.
- `request_sample_rate` in `(0.0,1.0)` (ppm in `(0,1000000)`): compute one random decision from request trace hash and `requestRate`, store it
  in `Trace`, and reuse it for later ordinary INFO/VLOG logs in the same request context.

The same `GetOrCreate` logic is used by the first request INFO/VLOG, by diagnostic/access sampled-in checks, and by RPC
metadata sending. Diagnostic/access consume only the positive sampled-in result; sampled-out is not a direct drop
condition for them. Existing Trace decisions are not recomputed after dynamic config updates. If an upstream process with `request_sample_rate=1.0` did not create a
decision and a downstream process has a lower rate, the downstream process may create its local decision; production
config consistency is achieved by worker register / heartbeat propagation.

Internal classification mapping:

| Classification result | Sampling rate | Note |
| --- | --- | --- |
| `REQUEST` | `requestRate` | complete request-log decision, first decision per request trace, stored in `Trace` |
| `DIAGNOSTIC` | `diagnosticRate` | request-context ERROR/WARNING/PLOG; request sampled-in bypasses supplement sampling |
| `ACCESS` | `accessRate` | `AccessRecorder` path; access helper forces pass first when request sampled-in |
| `BYPASS` | none | FATAL, background logs, request_out/resource, and other out-of-scope paths |

### Random Sampling Constraints

Thresholds are computed at config publication:

```cpp
uint64_t BuildThreshold(uint32_t ppm)
{
    if (ppm == 0) {
        return 0;
    }
    if (ppm == kSamplePpmBase) {
        return kAlwaysSampleThreshold;
    }
    return static_cast<uint64_t>((static_cast<unsigned __int128>(ppm) * UINT64_MAX) / kSamplePpmBase);
}
```

Hard constraints:

- No runtime RNG; randomness comes from stable keys, process sample salt, and integer hash mixing.
- No clock reads; sampling does not depend on wall-clock windows and therefore cannot become first-N.
- No global counters, CAS, spin loops, queues, reservoir sampling, or background backfill.
- No hashing of log payload text, formatted strings, or `ostringstream` output.
- No floating-point math, division, modulo, or config-string parsing on the hot path.
- Per-event decorrelation uses only `traceHash`, log kind, and a `thread_local` incrementing sequence.
- The `thread_local` sequence must not be replaced with a global atomic.
- Do not add file/line hashing or `AccessRecorderKey` to sampling keys. `AccessRecorderKey -> AccessKeyType` mapping is
  only used to decide whether an access record is in scope; it is not part of the random key.
- Without request context, traceHash may be 0; classification must never depend on `traceHash == 0`.
- Keep only one process-level `sampleSalt`; log kind is part of the key, so no extra salt map is maintained.

## Integration Points

### Runtime Logs

Files: `log.h`, `log_message_impl.cpp`

1. `LOG` / `VLOG` macros check severity, minloglevel, and verbosity as today. Disabled `VLOG` never enters sampler.
2. `LOG(FATAL)` and all CHECK failure paths bypass sampler at the earliest macro / construction entrypoint. `isFatal=true`
   in `LogSampler` is only a defensive pass-through.
3. Run sampler disabled / all-`1.0` fast paths before reading `Trace` or constructing any sampling request metadata.
4. Classify by severity/source before consulting request sampling, and read request context lazily only when needed:
   - request-context ERROR/WARNING -> force pass when request is sampled in; otherwise `diagnostic_sample_rate`
   - request-context PLOG / latency -> force pass when request is sampled in; otherwise `diagnostic_sample_rate`; FATAL still bypasses
   - request INFO/VLOG with valid request log trace -> request-trace decision from `request_sample_rate`
   - background logs without request context -> bypass this sampler
5. Build `LogMessage` and evaluate stream payload only if sampled in.

`ShouldCreateLogMessage()` adds FATAL/CHECK bypass and ensures request sampled-in can force request diagnostics/access,
while request rejection applies only to ordinary request INFO/VLOG.
`sample_rate=0.0` and `sample_rate=1.0` must return from macro-level fast paths without key construction, hashing, or
`LogMessage` construction, and without eagerly reading unrelated state just to fill a unified sampling object. For
diagnostic/access `0.0`, the fast path still first honors request sampled-in forced retention.
`LogMessageImpl::Init()` remains a defensive backstop for direct `LogMessage` construction, but cannot be the primary
drop point for macro logs because stream payload may already have been evaluated.

### PLOG

Request-context PLOG bypasses request rejection as a drop condition, enters `DIAGNOSTIC`, and is emitted when the request
is sampled in; otherwise it is supplement-sampled by `diagnostic_sample_rate`. Background PLOG bypasses this sampler.
Fatal PLOG behavior still bypasses sampler.
`PLOG_IF` must call `ShouldCreatePlogMessage()` before constructing `LogMessage` so dropped PLOG statements do not
evaluate stream payload expressions. `forceLog_=true` continues to mean "skip request reject as a drop condition"; it
does not skip request sampled-in forced retention or diagnostic supplement sampling.

### Interface Logs

Files: `access_recorder.cpp` plus selected hot access paths.

`access_sample_rate` applies only to `AccessKeyType::CLIENT` and `AccessKeyType::ACCESS`. `AccessKeyType::REQUEST_OUT`,
`request.log`, `request_out.log`, and `resource.log` are out of scope.

1. Hot access paths use `AccessRecorder::Object`, `AccessRecorder::Stream`, or `AccessRecorder::RequestOut`.
   Call sites should pass cheap values, references with same-scope lifetime, or providers for expensive fields, then call
   `Record()`. Sampling decisions, skipped-path fast return, elapsed time, request formatting, and exporter writes stay
   inside `AccessRecorder`; ordinary business call sites must not add `ShouldRecordAccess()` / `ShouldRecord()` branches.
2. First implementation must cover C++ client KV/Object hot APIs, C/Python/Java wrappers, worker-side corresponding
   request access entrypoints, and existing shared wrapper / RAII paths.
3. Disabled/pass-through or `access_sample_rate=1.0`: sampler internal API returns true after only the lightweight enabled/rate
   check; it must not map key/type or build event keys.
4. Non-100% access rate: guard maps `AccessRecorderKey -> AccessKeyType`; `REQUEST_OUT` returns true; only
   `CLIENT`/`ACCESS` enters access logic.
5. For `CLIENT`/`ACCESS`, first check whether the current request trace is sampled in. Existing sampled-in decisions and
   `request_sample_rate=1.0` semantics force access emission. If no decision exists and request rate is in `(0.0,1.0)`,
   reuse the same `GetOrCreateRequestSampleDecision()` logic. With `request_sample_rate=0.0`, sampled-in is impossible
   and access does not need to create reject just to run supplement sampling.
6. If request is sampled in, access returns true immediately; otherwise perform one access per-event random sampling check.
7. `AccessRecorder::Record()` still has a fallback sampling check before `RequestParam::ToString()` /
   `StreamRequestParam::ToString()`. If the fallback samples out, set `isRecord_ = true` before returning.
8. Do not rely on `AccessRecorderManager::LogPerformance()` or `HardDiskExporter::Send()` as the primary decision
   point: `ToString()` is evaluated before those calls receive their arguments.
9. Preserve access record format.
10. For emitted access records, compute `logSampled` with the same request-decision helper: `request_sample_rate=1.0`
    without explicit state means sampled-in, and `request_sample_rate=0.0` means rejected. This marker computation does
    not create or propagate any access sampling state.

### Configuration

Files: `logging.cpp`, `flags.cpp`/`flags.h`, `share_memory.proto`, `worker_service_impl.cpp`,
`client_worker_common_api.cpp`

1. Worker-side `DS_DEFINE_double(request_sample_rate, 1.0, "...")`,
   `DS_DEFINE_double(access_sample_rate, 1.0, "...")`, and
   `DS_DEFINE_double(diagnostic_sample_rate, 1.0, "...")` in `logging.cpp`.
2. Add the three sampling-rate flags to `flagNameTrustList_`.
3. Ordinary clients do not read environment variables, startup flags, or `EmbeddedConfig` for log sampling.
4. Worker collects the three rates plus explicit-state bits -> fills default/derived `request/access/diagnostic` rates
   -> normalizes enabled -> precomputed `SampleRate` thresholds bound to process-stable salt -> `LogSampleConfig` ->
   atomic publish.
5. Dynamic update uses validate-then-commit. Invalid values do not update the effective gflag values and do not publish a
   new sampler config. Config-update failure logs must bypass `LogSampler`.
6. `flags.cpp::UpdateFlagParameter()` handles the three sampling-rate flags and calls one shared
   `LogSampler::UpdateConfigFromFlags()` path that rebuilds the snapshot from all three current values and explicit bits.
7. Worker converts the validated effective config into `LogSampleConfigPb` and sends it in register and heartbeat
   responses. Invalid dynamic values never become effective config and are never sent to clients.
8. Clients default to pass-through before register. Presence of `LogSampleConfigPb` triggers structured validation and
   update. Missing fields leave the current client config unchanged as defensive handling.

`LogSampleConfigPb` details are in the configuration model above. The `log_sample_config` proto field name is only a
structured worker-to-client transport field, not a user-facing parameter. Responses are not AK/SK verified under the current
protocol rules, so this feature does not add response signing. `log_rate_limit` is removed, not mapped to the new sampling rates or ppm, and
`RegisterClientRspPb` field 24 is reserved.

### RPC Consistency

`MetaPb.log_sample_state` propagation is used for the complete request-log sampled-in/reject decision.
`GetOrCreateLogSampleState()` still writes the request-level decision when sending RPC metadata.
`ApplyLogSampleState()` and `SetTraceContextFromMeta()` still restore that decision on the receiving side.

Rules:

- Request logs use propagated sampled-in/reject decisions when they exist; existing sampled-in decisions override local
  `request_sample_rate`.
- `request_sample_rate=0.0` creates and propagates request-trace reject. `request_sample_rate=1.0` without upstream
  decision does not create sampled-in state.
- `request_sample_rate` in `(0.0,1.0)` may create the request decision from either first request INFO/VLOG or RPC
  metadata sending through the same `GetOrCreate` logic.
- Existing Trace decisions are not recomputed after dynamic config changes.
- Diagnostics/access read sampled-in semantics from `log_sample_state` for forced retention. sampled-out/reject is not a
  direct drop condition for diagnostics/access; it only means continue to local supplement sampling. Per-event supplement
  decisions are not propagated.
- Trace request sampling fields remain in place and are treated as complete request-log sampled-in/reject state, not
  general per-event log sampling state.

## Performance

### Hot-Path Hard Constraints

| Scenario | Allowed | Forbidden |
| --- | --- | --- |
| Three sampling parameters unset / all-1.0 folded config | One snapshot/enabled check | Clock, counters, hash, CAS, payload construction, class key construction |
| FATAL/CHECK | Pass immediately | Sampling checks |
| `sample_rate=1.0` | Classify then direct pass | Key construction, hash, clock, CAS |
| `sample_rate=0.0` | Classify then drop non-sampled-in events; ordinary request INFO/VLOG writes reject Trace; diagnostic/access still pass when request is sampled in | Key construction, hash, payload construction, clock, CAS |
| Request trace already has decision | sampled-in keeps complete request logs and access; cached reject directly drops only ordinary INFO/VLOG, while diagnostic/access continue supplement sampling | Hash, clock, CAS, per-event key construction when sampled-in |
| First request decision | Build trace key + one lightweight hash + threshold compare only for `(0.0,1.0)` / ppm `(0,1000000)` | Global atomic, runtime RNG, floating point, division/modulo |
| Per-event decision | Build event key + one lightweight hash + threshold compare | Payload hashing, heap allocation, mutex |
| Config snapshot load | Atomic raw pointer load | Per-log `shared_ptr` refcount increments |
| Access guard pass-through / access 100% | Direct true | key/type mapping, event key construction, hash |
| Access guard request sampled-in | Read/create request decision then direct true | access event key construction, access hash, ToString |
| Access guard sampled out | Caller skips recorder/param/elapsed/ToString/exporter | `AccessRecorder` construction, `clock::now()`, parameter filling |

Call-order constraints:

- Do not unconditionally read `Trace`, compute `traceHash`, map `AccessRecorderKey`, construct `RequestParam`, or read
  time just to call one unified sampler entrypoint.
- Disabled/pass-through, all-`1.0` folded config, FATAL/CHECK, and per-rate `1.0` / `0.0` must return directly from the
  macro layer or callsite guard.
- `Trace::Instance()`, `Trace::GetCachedHash()`, request decision creation, `thread_local` sequence increments, and
  event-key construction may happen only when the relevant rate is in `(0.0,1.0)`, when ordinary request INFO/VLOG must
  create a reject decision, or when diagnostic/access needs to check sampled-in forced retention.
- Access guard must not map `AccessRecorderKey -> AccessKeyType` when `access_sample_rate=1.0`; that mapping is allowed
  only when access rate is not `1.0`, and only to decide `CLIENT` / `ACCESS` / `REQUEST_OUT` scope.

Absolutely forbidden on the hot path: string parsing, config parsing, heap allocation, mutex, file IO, RPC, logging inside
the sampler, runtime random generation, system time reads, global atomic counters, CAS, spin, floating-point math,
division, modulo, hashing log payload text, and production global debug-counter increments.

### 4000 QPS + 32 Concurrency Gate

This is a hard pre-merge gate:

- Empty config: 4000+ QPS with 32 concurrent threads must show no measurable throughput regression and no p99
  degradation versus the no-`LogSampler` baseline.
- All coefficients `1.0`: config publication folds to disabled/pass-through and must not add classification, hash,
  clock reads, or CAS over empty-config behavior.
- A coefficient of `0.0`: non-sampled-in logs for that supplement path must return before stream payload / `ToString()`
  evaluation; request sampled-in diagnostic/access still emits under complete-request semantics.
- Partial enablement: access-only sampling must not slow runtime-log paths; request-only sampling may make diagnostic or
  sampler internal API read/create the request sampled-in decision, but must not build per-event keys or run diagnostic/access
  hashing when request is sampled in.
- Coefficients in `(0.0,1.0)`: sampler-only benchmark must prove no heap allocation, mutex, system time read, global
  atomic contention, or CAS retry.
- 32 concurrent threads must not introduce a shared write hotspot; per-event decorrelation uses `thread_local` state.
- Access drop verifies hot-path guard skips `AccessRecorder` construction, `clock::now()`, `RequestParam`
  construction/filling, `RequestParam::ToString()`, `ostringstream`, and `HardDiskExporter::Send()`. `Record()` fallback
  is only a safety net.
- PLOG drop verifies `PLOG(INFO) << Expensive()` does not evaluate `Expensive()`.
- Random-distribution tests prove the implementation is not first-N: use fixed 100000 keys with `sample_rate=0.1`,
  `0.5`, and `0.9` (internal ppm `100000/500000/900000`); hit rate must be target +/- 1%. For `0.5`, split the same 100000-key sequence into 20
  buckets; every bucket must be 50% +/- 5%, and samples must appear in both the first and second halves.
- Same-trace sequential-event tests fix traceHash and log kind while incrementing only `thread_local` sequence, and must
  pass the same ratio and bucket distribution checks.

## Correctness And Reliability

Invariants:

- FATAL/CHECK never dropped.
- Request-context ERROR/WARNING/PLOG emit when request is sampled in; when request is not sampled in, they use
  `diagnostic_sample_rate` supplement sampling. request reject must not directly drop diagnostics.
- External API access / client access emits when request is sampled in; otherwise it uses `access_sample_rate`.
  `REQUEST_OUT`, `request.log`, and resource logs are outside this design.
- Request INFO/VLOG is dropped when the request-trace sampling decision rejects it.
- Background logs bypass this sampler and follow existing logging behavior.
- Request-context PLOG is diagnostic and follows request sampled-in forced retention / diagnostic supplement sampling;
  background PLOG bypasses this sampler; fatal behavior bypasses sampler.
- Sampling coefficients are independent and never compete for shared budget.
- Sampling uses random hash-threshold decisions and must not regress to fixed-window first-N, smoothed first-N, or a
  global counter.
- Unset config and explicit all-`1.0` config fold to disabled/pass-through: all logs are retained.
- `sample_rate=0.0` means supplement sampling for that class is zero: ordinary request INFO/VLOG drops, non-sampled-in
  diagnostic/access drops, with only FATAL/CHECK and request sampled-in forced retention as exceptions.
- Access log field order, separators, file paths, and file names remain stable.
- `logSampled:true` marker means ordinary request INFO is visible / not rejected under current request-level sampling
  semantics. It does not require explicit sampled-in Trace state and does not mean access sampling kept the record.
- Config update failure retains previous-good config and emits diagnostics through sampler bypass.
- Request sampling decisions propagate across RPC via `MetaPb.log_sample_state` as complete request-log sampled-in/reject
  state. sampled-in forces diagnostics/access; reject directly drops only ordinary request INFO/VLOG. Existing decisions
  are not recomputed after config updates.
- Worker is the config authority. Clients apply only worker register / heartbeat structured config; they do not parse
  user-facing sampling parameters or read environment variables for sampling.

Rollback: set all three sampling parameters to `1.0` to disable the new random sampler; config publication folds that
state to disabled/pass-through.

## Observability

Production hot paths do not maintain global debug counters or per-log metrics. Tests and debug may observe sampled-in,
dropped-by-random-sample, and bypass-fatal outcomes only through disabled-by-default test-only hooks or thread-local
aggregation. Metrics subsystem integration requires separate hot-path cost evaluation and must not add per-log global
atomic write hotspots.

## Verification Cases

| ID | Scenario | Precondition | Expected |
| --- | --- | --- | --- |
| LS-001 | Default full retention | Empty config | All logs emitted; no hash, clock, or CAS |
| LS-001b | Explicit full-retention fold | all three `sample_rate` values are `1.0` | published as disabled/pass-through; hot path equals empty config |
| LS-002 | INFO payload skip | `request_sample_rate=0.0` | rejected request `LOG(INFO) << Expensive()` does not call `Expensive()` |
| LS-002b | Request reject propagation | `request_sample_rate=0.0` + client-worker RPC | request-trace reject created and propagated; both sides read cached reject for ordinary INFO/VLOG |
| LS-003 | Interface pre-drop | `request_sample_rate=0.0` + `access_sample_rate=0.0` and hot-path guard | no `AccessRecorder`, no `clock::now()`, no `RequestParam` construction/fill, no `ToString()`, exporter not invoked |
| LS-003b | request_out excluded | `access_sample_rate=0.0` + `AccessKeyType::REQUEST_OUT` | request_out follows old behavior and is not dropped by access sampling |
| LS-004 | FATAL always passes | all coefficients `0.0` | `LOG(FATAL)` / `CHECK(false)` still aborts |
| LS-003c | Request sampled-in forces access | `request_sample_rate=1.0` + `access_sample_rate=0.0` | `CLIENT` / `ACCESS` access logs still emit |
| LS-003d | Access OR rule | `request_sample_rate=0.5` + `access_sample_rate=0.0/0.5` | request sampled-in forces access; sampled-out request follows access sampling only |
| LS-005 | Request sampled-in forces diagnostics | request sampled-in + `diagnostic_sample_rate=0.0` | request ERROR/WARNING/PLOG still emitted, preserving complete request logs |
| LS-005b | Diagnostic supplement sampling | request INFO rejected + `diagnostic_sample_rate=1.0` | request ERROR/WARNING/PLOG still emitted; reject is not a direct diagnostic drop condition |
| LS-006 | PLOG payload skip | request not sampled-in + `diagnostic_sample_rate=0.0` | request-context `PLOG(INFO) << Expensive()` does not call `Expensive()` |
| LS-007 | Background bypass | `request_sample_rate=0.0` + `diagnostic_sample_rate=0.0` | background INFO/ERROR/WARNING/PLOG follows existing behavior |
| LS-008 | Sampling parameter validation | valid/invalid/out-of-range/NaN/inf values | valid published, invalid keeps previous-good |
| LS-008b | Request-only derived config | only `request_sample_rate=r` configured | access becomes `min(1,3r)`; diagnostic becomes `min(1,4r)`; values clamp to `1.0` |
| LS-009b | Config failure bypass log | previous-good would drop ordinary ERROR + invalid dynamic config | failure log bypasses sampler, previous-good remains |
| LS-010 | 4000+ QPS empty config | 32 concurrent benchmark | no throughput or p99 regression |
| LS-011 | 4000+ QPS enabled sampling | empty, all-1.0, single-class 0.0, single-class 0.5, access-only 0.5, request-only 0.5 | sampler-only overhead quantified; no heap, mutex, clock, global atomic, or CAS; partial enablement isolated |
| LS-012 | Cross-process consistency | client-worker RPC | complete request-log decision consistent on both sides; sampled-in forces diagnostic/access; reject directly drops only ordinary INFO/VLOG |
| LS-013 | Worker register/heartbeat config | worker sends `LogSampleConfigPb` | client applies structured config; missing field does not change current config; invalid fields keep previous-good |
| LS-013b | Proto pass-through | `LogSampleConfigPb.enabled=false` and ppm fields default 0 | client passes through and does not interpret default 0 as drop-all |
| LS-014 | Access marker semantics | access sampled in + request sampled/rejected | `logSampled:true` follows request-level sampled-in state only |
| LS-015 | Random ratio | fixed 100000 keys, `sample_rate=0.1/0.5/0.9` | pass ratio within target +/- 1% |
| LS-015b | Random buckets | fixed 100000 keys, 20 buckets, `sample_rate=0.5` | every bucket is 50% +/- 5%; samples appear in both halves and are not first-N |
| LS-015c | Same-trace sequential-event decorrelation | same traceHash and log kind; only sequence increments | ratio and bucket tests pass, proving consecutive events are not permanent pass/drop |
| LS-016 | Hot-path instruction constraints | benchmark + allocator hooks | sampled path has no allocation, lock, clock read, global atomic, or CAS |

## Implementation Scope

| Module | Changes |
| --- | --- |
| `common/log` | New `LogSampler`, config parser, threshold snapshot, lightweight hash |
| `log.h` | `ShouldCreateLogMessage()`: FATAL bypass + classification-first random sampling; `ShouldCreatePlogMessage()` pre-payload sampling |
| `log_message_impl.cpp` | Backstop + PLOG skips request reject as a direct drop condition, but still checks sampled-in / `diagnostic_sample_rate` |
| `access_recorder.cpp` / hot access paths | `AccessRecorder` facade: `Object/Stream/RequestOut` builders with sampled-out setter no-op and provider deferred evaluation; `ShouldRecordAccess()` internal API at construction; `Record()` final gate before `ToString()` |
| `logging.cpp` | worker-side three sample-rate gflags + validation; no client env var |
| `flags.cpp` / `flags.h` | Trust list + two-phase parse-commit config sync |
| `share_memory.proto` | Add `LogSampleConfigPb`; `RegisterClientRspPb reserved 24` and `log_sample_config = 25`; `HeartbeatRspPb.log_sample_config = 8` |
| `worker_service_impl.cpp` | Register / heartbeat responses carry worker effective `LogSampleConfigPb` |
| `client_worker_common_api.cpp` | Receive register / heartbeat config, validate structured fields, compare changes, and apply |
| `meta_zmq.proto` / `zmq_common.h` | Retain request sampling propagation as complete request-log sampled-in/reject state |
| tests | Functional, cross-process, random-distribution, and performance tests |

Implementation order:

1. `LogSampler`, sampling parser, threshold conversion, lightweight hash, and unit tests.
2. `ShouldCreateLogMessage()`: FATAL/CHECK bypass + classification-first random sampling; request sampled-in preserves
   complete request logs, while request reject applies directly only to ordinary request INFO/VLOG.
3. PLOG integration with macro-level `ShouldCreatePlogMessage()` precheck.
4. Access integration: implement AccessRecorder facade and `Record()` final gate, then cover C++ client KV/Object, C/Python/Java
   wrappers, worker corresponding request access entrypoints, and shared wrapper/RAII paths.
5. Worker three-parameter config, structured proto propagation, and client register/heartbeat application.
6. Random-distribution, access pre-drop, and 4000+ QPS with 32 concurrent threads performance validation; do not merge implementation until this passes.

## Design Review

| Requirement / concern | Design result |
| --- | --- |
| Interface logs support sampling | Satisfied. `access_sample_rate` controls `AccessKeyType::ACCESS` / `CLIENT`, with hot guard skipping recorder/param/elapsed/ToString/exporter. `REQUEST_OUT`, `request.log`, and resource logs are excluded. |
| Runtime request-log reduction and diagnostic sampling | Satisfied for the adjusted requirements: `request_sample_rate` decides the complete request-log sampled-in baseline; request ERROR/WARNING/PLOG are forced out when sampled-in and use one `diagnostic_sample_rate` supplement when not sampled-in; background logs are outside this sampler. |
| Random sampling | Satisfied. Deterministic hash-threshold sampling is required; first-N, windows, queues, and global counters are forbidden. Ratio and bucket tests validate random effect. |
| Sampling coefficient config | Satisfied. Worker local config has three independent decimal rate parameters; worker normalizes them to ppm and sends structured `LogSampleConfigPb` to clients. |
| Performance goals | Satisfied at design level. Disabled, all-1.0, and 0.0 supplement paths avoid key construction and hashing; only `(0.0,1.0)` paths build keys/hash; no clocks, CAS, global atomics, payload hashing, or production counters. Final acceptance depends on benchmark evidence. |
| Over-design risk | Reduced to the smallest mechanism for the new requirements: three independent config parameters, three rates, one process salt, and existing Trace request decisions. Background logs bypass the sampler; the design does not add conditional-correction math, extra salt maps, exact QPS caps, versioning, signing, client-side sampling config/env config, or production counters. |
