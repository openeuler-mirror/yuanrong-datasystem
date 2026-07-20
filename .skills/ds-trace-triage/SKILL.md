---
name: ds-trace-triage
description: >
  Analyze DataSystem trace bundles for slow latency or errors. Use for gzip/tar
  trace packages, worker/client trace IDs, RPC deadline exceeded, rpc slow,
  client summary, RemotePull, Get/Set/Create/Publish, URMA_ELAPSED_*,
  worker/time/flow/breakdown aggregation, and CodeGraph-backed source mapping.
---

# DataSystem Trace Triage

Use this skill when the input is a small or medium trace corpus and the goal is
root-cause analysis rather than broad access/resource trending.

## Required workflow

1. Pin the source:
   ```bash
   git fetch main master
   git rev-parse main/master
   ```
2. Build or refresh CodeGraph on a clean `main/master` worktree when source
   causality is requested:
   ```bash
   /home/t14s/.local/bin/codegraph init <clean-worktree>
   /home/t14s/.local/bin/codegraph index <clean-worktree>
   ```
3. Run the deterministic parser first:
   ```bash
   python3 scripts/ds_trace_triage.py run <trace_dir_or_tar_gz> \
       --code-ref "$(git rev-parse main/master)" \
       --case <case-name> \
       --scenario <scenario> \
       --out /tmp/ds-trace-runs
   ```
   For manual debugging or CI artifact checks, the same pipeline can be run as
   explicit stages:
   ```bash
   run_dir=$(python3 scripts/ds_trace_triage.py parse <trace_dir_or_tar_gz> \
       --code-ref "$(git rev-parse main/master)" \
       --case <case-name> \
       --scenario <scenario> \
       --out /tmp/ds-trace-runs)
   python3 scripts/ds_trace_triage.py aggregate "$run_dir"
   python3 scripts/ds_trace_triage.py triage "$run_dir"
   python3 scripts/ds_trace_triage.py render-local "$run_dir"
   python3 scripts/ds_trace_triage.py render-site "$run_dir"
   python3 scripts/ds_trace_triage.py publish-site "$run_dir" --dry-run
   # After reviewing site_publish.md, omit --dry-run to scp and curl-verify.
   ```
4. Read the timestamped run directory:
   - `manifest.json`: case/scenario/ref/time range and render targets
   - `events.jsonl`: trace-scoped raw and UB events with source/member/line
   - `parsed_traces.json`: parser output consumed by aggregate
   - `summary.json`: time/worker/flow/latency/RPC/UB/error dimensions
   - `triage.json` and `triage.md`: classifications and issue candidates
   - `report.local.html`: self-contained local report
   - `report.site.html`: yche.me-shaped report draft; keep the same core
     components as local HTML and include `/assets/css/site.css` plus
     `/assets/js/site.js`
   - `site_publish.md`: xqyun/yche.me publish checklist with target path,
     URL, copy command, and validation command
   - `manifest.json` `render_targets.site.publish`: dry-run/publish status
     recorded by the `publish-site` stage
5. Inspect selected full logs for the top slow/error traces. Keep aggregate
   distributions first, then per-trace evidence.
6. Cross-check any source-level conclusion with CodeGraph plus direct source
   reads. CodeGraph is discovery, not sole proof.

## Self verification and CI

The script has a built-in fixture:

```bash
python3 scripts/ds_trace_triage.py verify
python3 scripts/ds_trace_triage.py --self-test
python3 -m pytest -s tests/scripts/test_ds_trace_triage.py -q
```

Add those commands to CI as a low-cost parser contract. They verify gzip-tar
handling, trace grouping, access latency, breakdown, rpc slow, URMA elapsed,
UB field extraction, time buckets, worker/edge aggregation, local/site HTML
generation, inline report JavaScript syntax when Node.js is available, and
error classification.
It also verifies the yche publish checklist and `publish-site --dry-run`
manifest status.

The repository CI runs the dependency-light gate in `.gitee/ci_build.sh`:

```bash
python3 -m py_compile scripts/ds_trace_triage.py tests/scripts/test_ds_trace_triage.py
python3 scripts/ds_trace_triage.py verify
```

Run pytest locally when changing parser behavior:

```bash
python3 -m pytest -s tests/scripts/test_ds_trace_triage.py -q
```

The self-test must keep covering the historical contract learned from the trace
threads: `latencySummary` raw text and key/value fields, RPC slow server/network
subfields, `URMA_ELAPSED_TOTAL/POLL_JFC/NOTIFY/THREAD_SHED`, and classification
counts. When DataSystem log wording changes, update the fixture and tests in the
same patch as parser logic.

For small log-format extensions, keep the analyzer stable by registering new
markers instead of rewriting the parse loop:

```python
mod.register_error_pattern("DMA_WAIT_TIMEOUT")
mod.register_metric_rule(
    "urma_dma",
    r"\[URMA_ELAPSED_DMA\].*?cost\s+([\d.]+)\s*(us|ms)",
    unit_group=2,
)
```

Every registered rule must have a focused pytest fixture that verifies the trace
level output and the aggregate `dimensions.custom_metrics_ms` or
`dimensions.errors` output.

The script keeps these responsibilities separated inside one file:

- `ParserRules`: owns log wording extension rules.
- `TraceInputReader`: reads directories, files, gzip logs, and tar bundles.
- `TraceParser`: parses one log line into trace-scoped facts.
- `TraceAnalyzer`: coordinates reader/parser output into report dimensions.
- `TraceReportRenderer`: renders events, triage, Markdown, and HTML.
- `TraceRunPipeline`: owns staged run directories, cache, manifest, and render
  target status.

Keep compatibility wrappers such as `analyze_inputs`, `parse_stage`, and
`run_pipeline`, but put new behavior behind the responsible class first.

## Report expectations

Always cover:

- time: first/last timestamp, burst windows if visible
- worker: entry/provider/target concentration where logs expose it
- flow: Get/Set/Create/Publish/RemotePull/GetObjMetaInfo/RPC methods
- latency: access latency percentiles and top slow traces
- breakdown: `ProcessGetObjectRequest`, QueryMeta/CreateMeta, SafeObject locks,
  client summary and worker summary fields
- rpc slow: method, count, e2e/client/server/network fields when present
- URMA: `URMA_ELAPSED_TOTAL`, `URMA_ELAPSED_POLL_JFC`,
  `URMA_ELAPSED_NOTIFY`, `URMA_ELAPSED_THREAD_SHED`, `URMA_PERF`
- errors: non-zero access status, deadline exceeded, not found, object in use,
  URMA timeout, fallback rejection, etcd abnormal
- source: pinned ref, key files/functions, and evidence boundary

For customer-facing reports, write like a diagnosis note:

- Start with the user-visible symptom, then separate it from worker-side
  evidence. For example, a client 20ms deadline and a 250ms worker completion
  can both be true.
- Include one concise "core judgment" paragraph with negative boundaries:
  "not client local processing", "not QueryMeta with current evidence", "URMA
  total is only 0.x ms".
- Every chart needs a caption explaining what question it answers.
- Keep trace drilldown usable: search, filters, pagination, selected trace
  breakdown, and full logs.
- For UB/URMA, describe the write/wait/notify timeline and compare total,
  poll JFC, notify, thread scheduling, data size, CPU, inflight, and edge.
- For multiple packages or noisy-vs-clean comparisons, treat each package as a
  cohort and compare distributions before carrying over root-cause labels.
- The HTML report should follow the `/var/www/html/perf` trace-report pattern:
  fixed left navigation, KPI cards, a core-judgment panel, ECharts with
  captions, cohort comparison, trace search/filter/pagination, selected-trace
  breakdown, highlighted full logs, run/input provenance from `manifest.json`,
  evidence coverage/missing-surface tables, and downloads for selected raw logs
  plus filtered evidence and report-summary Markdown.

Machine-readable summaries should expose these buckets when the input contains
them:

- `dimensions.latency_summary_us`: parsed `latencySummary:{...}` fields, while
  `traces[*].latency_summary_raw` preserves the original summary line text
- `dimensions.rpc_slow`: method plus `e2e_us`, framework, server queue/exec, and
  `network_residual_us`
- `dimensions.urma_elapsed`: total, poll JFC, notify, and thread scheduling
- `dimensions.ub_summary`: transfer path and `src -> target` UB edges
- `dimensions.cohorts`: per-input-package trace/error/classification/latency
  comparison for multi-package and noisy-vs-clean analysis
- `dimensions.diagnosis`: customer-facing diagnosis lines for symptom,
  latency, evidence boundary, and customer expression. HTML should render this
  data instead of re-deriving report language in JavaScript.
- `dimensions.recommendations`: source-validation, observability,
  cohort-compare, UB/URMA, and deadline follow-up actions for the appendix.
- `dimensions.source_appendix`: maps log surfaces to read/write flow stages,
  source hints, CodeGraph/source validation, and customer-facing report reading.
- `dimensions.flow_stages`: graph-ready Client/Entry/Meta/Data/UB nodes and
  read/write edges with evidence coverage for the flow-stage chart and table.
- `dimensions.time_buckets`: 1s/10s burst and gap candidates
- `dimensions.worker_summary`: role-aware client/entry/data/meta worker views
- `dimensions.classifications`: parser-assigned root-cause families such as
  `client_deadline_with_urma_wait`, `client_deadline_20ms`,
  `write_memory_copy_dominant`, `remote_fast_transport_wait`, and `rpc_slow`

## Error-trace tactics

Use several independent cuts before deciding root cause:

- **Status/error family cut**: group non-zero access status and repeated text
  such as deadline exceeded, not found, object in use, fallback rejected, and
  URMA wait timeout.
- **Deadline-budget cut**: align access latency with configured timeout,
  `reqTimeoutDuration.CalcRemainingTime()`, RPC slow e2e, and worker completion
  time. A client timeout can coexist with a later worker-side slow completion.
- **Worker ownership cut**: separate client, entry worker, provider/data worker,
  master, and fallback target. Do not label a target worker unless the log
  explicitly prints it.
- **Transport cut**: split TCP/UB/URMA/RDMA/fallback evidence. Tracker defaults
  or response-only fields are not proof of request-side transport.
- **URMA lifecycle cut**: compare `URMA_ELAPSED_TOTAL`, poll JFC, notify, thread
  scheduling, data size, CPU, inflight, source chip, and target address.
- **Source-evolution cut**: re-run CodeGraph on current `main/master`, then
  verify direct source for timeout propagation and current data-plane branches.

## Historical trace-thread lessons

The workflow is calibrated from eight Codex trace-analysis threads:

- `019f753c`: 248 Get traces showed RemotePull/URMA completion wait dominating
  `ProcessGetObjectRequest`; avoid treating QueryMeta as the bottleneck when
  URMA and worker completion distributions align.
- `019f75a9` and `019f7606`: after hardware-port isolation, seconds-scale URMA
  tails disappeared and residual failures clustered around 20ms client/worker
  RPC deadline. Keep these as separate families.
- `019f7686`: ZMQ/brpc slow reports need subfield parsing, especially
  `server_exec_us` and `network_residual_us`, plus source mapping for
  `GetObjMetaInfo`, `ProcessGetObjectRequest`, `BatchGetObjectRemote`, and
  `UrmaGatherWrite`.
- `019f76d0`: write traces require original `latencySummary` preservation;
  `client.process.memory_copy` can dominate Set/Create/Publish without a
  standalone slow log when below threshold.
- `019f7970`: interactive report fixes taught that table and card filters,
  category downloads, edge-role filtering, and complete evidence exports should
  be independently validated.
- `019f79c0`: generated HTML/index artifacts need inline JS syntax checks,
  deduped quoted metadata, and live verification; bad report registration can
  break the whole homepage.
- `019f7b27`: failure traces should be split into issue-grade families:
  DataWorker UB/URMA server exec, RPC network residual, client deadline with
  fast server completion, EntryWorker processing late, remote_get/brpc mismatch,
  and QueryMeta/log-mixing anomalies.
