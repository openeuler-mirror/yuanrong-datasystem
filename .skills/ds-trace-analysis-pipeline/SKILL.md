---
name: ds-trace-analysis-pipeline
description: >
  Use when analyzing DataSystem trace directories or archives: triage, read/write bottlenecks, NUMA/WR diagnosis, single/multi-run reports, or refreshing an existing report from validated models.
---

# DataSystem Trace Analysis Pipeline

This is the single Trace analysis skill. Select the requested mode below; a focused request does not require generating every report. The pipeline owns orchestration and validation, and the mode references retain the domain-specific evidence rules.
`trace-triage`, `bottleneck-analysis` (read/write), and `numa-analysis` name capabilities of this skill, not separate skills. A user may request any one capability without asking for the complete pipeline.

## Tool entry and operating guide

Use `python3 scripts/ds_trace_analysis.py --help` from the repository root for the unified entry point.
For a wheel installation outside the checkout, use the equivalent `ds-trace-analysis` command.
Build/install instructions and the Python 3.9 floor are in the operating guide.
Implementation and page assets live under `scripts/trace_analysis/`. The single public entry is `scripts/ds_trace_analysis.py`; use its subcommands for every stage. Former standalone paths have been removed.
The pipeline calls package stage interfaces in `trace_analysis/stages.py`, not the CLI dispatcher.
Stage results return artifact paths explicitly; `validation.py` owns shared model validation for normal
and render-only execution. In the complete pipeline, read, write, and NUMA branch from validated Evidence;
write and NUMA do not consume the read model. They keep attribution and budgets separate.
Field parsing and inventory live in `ingest/`, Run storage in `orchestration/`, common WR/RPC facts
in `evidence/`, independent attribution in `analysis/{read,write}.py`,
and page implementations in `rendering/`. Render-only consumes the persisted refined write model;
it must not rerun write attribution just to repaint HTML.
For commands, manifest fields, output validation, and a copyable prompt, read the
[operating guide](../../docs/source_zh_cn/appendix/trace_analysis_usage.md).

## Choose the mode before executing

| Requested result | Read | Command |
| --- | --- | --- |
| Complete single/multi-Run report | [triage](references/triage.md), [bottleneck](references/bottleneck.md), [NUMA](references/numa.md), [reports](references/reports.md) | `pipeline` |
| Parse and diagnose Trace evidence only | [triage](references/triage.md) | `triage` |
| Read, write, or both bottlenecks | [bottleneck](references/bottleneck.md); triage if no completed Run exists | `read`, `write` |
| NUMA/WR analysis | [NUMA](references/numa.md); complete required upstream artifacts first | `numa` |
| Repaint existing reports without changing attribution | [reports](references/reports.md) | `pipeline --render-only` |
| Offline delivery | [reports](references/reports.md) | `package` |

All commands use `python3 scripts/ds_trace_analysis.py <command>`. Read only the mode references needed
for the request. The former three specialist skill names have been consolidated here; each stage retains its arguments through a subcommand. Requests to explain a field do not authorize a full report run.

## Full report workflow

The numbered steps describe the orchestrator's stages. For a complete report, run `pipeline` once;
do not pre-run each standalone CLI and then parse the same inputs again. Standalone commands below
are for a deliberately selected partial mode or diagnosis of a failing stage.

1. Pin and record `source_head`, `source_base`, and optional numeric PR ID `pr` in a pipeline manifest. Put the PR URL in prose or links, not in `pr`. Do not guess a source revision or infer `local_cache` from service names.
2. For every Run, pass the complete input directory/archive set in the pipeline manifest; the pipeline invokes Triage itself. Never pass an individual trace file when the source package is a directory/archive. Keep the returned run directory, raw inputs, `manifest.json`, the validated `inventory.json`, `summary.json`, `triage.json`, and `report.local.html` together. Use the standalone `triage run` command only for a requested partial analysis.
   Before a cold multi-Run execution, run `pipeline --manifest <file> --preflight-only`; it checks every Run without creating report output. Keep one cold run and read `pipeline.validation.json.execution` plus each `stage.execution.json` for timing instead of repeating the cold run solely to collect timings.
3. Validate the persisted triage summary, then generate and validate `evidence.json` before read or write attribution. Evidence binds to the summary SHA256, retains normalized RPC/URMA/error and write observations with source references, and omits duplicate raw log text. Read and write consume this artifact independently; invalid Trace coverage or references stop the Run. This is a structural gate, not proof of RPC/URMA causality.
4. Render `bottleneck.analysis.json` and the read bottleneck page with `python3 scripts/ds_trace_analysis.py read`; keep GET/read attribution independent from SET/write attribution.
5. In the complete pipeline, build the write model directly from Evidence and render its page. For a historical partial workflow, `python3 scripts/ds_trace_analysis.py write` still accepts the existing bottleneck model. A missing write flow is reported as `0条/未采集`, never inferred from GET.
6. In the complete pipeline, build `numa.analysis.json` directly from validated Triage/Evidence, independently of read attribution. The standalone `numa` subcommand retains its legacy read-model input. Keep `URMA_ELAPSED_TOTAL` and chip/inflight evidence separate; missing receiver/chip/CPU evidence remains `未观测`.
7. After independent read/write and NUMA validation, persist `issues.analysis.json`: keep GET/SET final failures separate from observed error Traces and timeout events; retries without reconstructed attempt evidence remain null with a reason. Validate affected Trace IDs and evidence boundaries before creating the multi-Run manifest and `index.html`. Each Run row links to its own triage, read, write, and NUMA pages; the summary may compare runs but must never merge Trace rows.
8. Inspect the returned publication manifest and `publication.validation.json`, together with the attempt-level `pipeline.validation.json`. Export a ZIP only after model, browser, link and download checks pass.

Run the orchestrator:

```bash
python3 scripts/ds_trace_analysis.py pipeline \
  --manifest <pipeline.manifest.json> \
  --output <report-root> \
  --jobs 1 --resume
```

Use a positive `--jobs N` to bound heavy stages across all Runs (default 1; no fixed upper cap). Use `--jobs auto` with measured `execution.stage_estimates_mb` to choose concurrency from CPU affinity/quota and available memory. Auto mode reserves 20% of available memory, honors a smaller declared `memory_mb`, and admits at most the budget divided by the largest stage estimate. Missing or invalid estimates fail before producing reports; the calculation is recorded in `pipeline.validation.json.execution`. This is startup admission based on estimates, not a live RSS cap. For multiple independent Runs on a machine with enough memory, benchmark `--run-executor process --jobs 2` against the default thread executor; do not assume that higher thread counts improve throughput. The process executor admits whole Runs using declared peak stage estimates when `execution.memory_mb` is set, so estimates must cover all heavy stages and should be measured on the target host. Read, write, and NUMA consume validated Evidence independently and may overlap when the resource budget admits them. Standalone `numa` still accepts the legacy read model. Run only one pipeline process per output directory. `--resume`
validates input/configuration/rule versions and artifact hashes per stage; successful upstream stages
survive downstream failures. A hash-verified `stage.validation.json` receipt restores successful validation
only when its Run, stage and cache key match; missing or invalid receipts rebuild the stage. This does
not skip artifact hashes or authenticate an untrusted cache. `--force` bypasses reuse. Optional `execution.memory_mb` and
`execution.stage_estimates_mb` require measured estimates for triage/evidence/read/write/numa/issues/render/suite;
older manifests without evidence or issues use the read estimate until those stages are measured separately;
this is declared-estimate admission, not an operating-system RSS cap. Unknown estimates are rejected
when a memory budget is requested.

Pipeline outputs are immutable generations. The returned `index` and `manifest` identify the actual
published version; `stable_index` is the atomically updated entry point. Root `suite.manifest.json`
is only a compatibility snapshot. A failed attempt does not replace a prior successful publication.
Do not edit sealed publication files in place or automatically delete older generations.

For presentation-only changes, reuse validated intermediate models:

```bash
python3 scripts/ds_trace_analysis.py pipeline --output <report-root> --render-only --jobs 3
```

This path writes `render.validation.json` and does not reparse logs or recompute read attribution. Parser/diagnosis changes require normal analysis, not render-only. Both modes render the homepage through `trace_analysis/overview.py` and `scripts/trace_analysis/assets/overview/overview.{html,css,js}`; `runs/<id>/run.summary.json` inside the selected publication carries compact comparison data. The homepage supports one or multiple Runs, four-row pagination, and new-tab Run entry. Inside each Run, shared page navigation stays in the same tab. Optional `focus` and `overview` manifest fields supply case-specific findings without modifying templates.

The write stage persists `write.refined.analysis.json` with a `rows` array. Validate Trace coverage and mutually exclusive stage-budget closure before including it in the summary. Do not substitute the GET model for write attribution.

The manifest uses `schema_version: 1`, a `runs` list, and these per-Run fields:

- required: `id`, `inputs`, `input_archive`
- recommended labels: `label`, `implementation`, `size`, `load`, `client_shape`, `placement`
- analysis controls: `local_cache`, `deadline_ms`, `read_path`, `qps_per_node`, `client_count`, `threads_per_client`, `workers_per_node`
- optional: `case`, `scenario`, `allow_partial_inputs`, `case_study_only`, `sampling_cap_per_band`
- view controls: `view.read_top` (0, 100, or 1000; default 0). Pipeline `top` is a compatibility alias for this view selection, never an analysis limit. Conflicting values are rejected.

Pipeline always analyzes the full collected GET/SET corpus. Changing the view reuses models and changes only presentation; standalone `read --top N` and the historical stage API retain their explicit input-limit behavior. Use standalone `--top 0` for full analysis.

The top-level manifest records `source_head`, `source_base`, `pr`, `title`, `sampling`, and `overview`. If present, `sampling` must be an object; `max_per_band` must be a non-negative integer. Use paths that can be resolved from the manifest location. Keep report links relative to the output root.

## Evidence and failure contract

- A Run fails closed when its input set, triage output, bottleneck model, NUMA model, or validation result is missing.
- `allow_partial_inputs` accepts only JSON `true` / `false` (omitted: `false`; `null` is rejected). Partial input analysis is opt-in and must be visible in the validation output and final limitations.
- `local_cache` accepts only JSON `true` / `false` / `null` (omitted: `null`, unknown). Strings such as `"false"`, numbers and containers are rejected. Every Run is checked before any stage starts; direct stage calls enforce the same contract.
- `pipeline.validation.json` describes the latest attempt; `publication.validation.json` describes the
  selected successful publication. Inspect every Run and browser audit before claiming readiness.
- Run `ReportRegistry.audit({requireComplete: true, checkLayout: true})` after scrolling/lazy rendering
  and filter tests. Missing expected components, pending renders and stale revisions must not pass.
- Inspect `stage.execution.json` for cache decisions and `stage.provenance.json` for input/rule/tool
  fingerprints and the analyzer revision. A `+dirty` suffix marks local source changes; the wheel
  embeds its build revision. Neither this field nor a declared source reference proves the deployed
  DataSystem revision.
- The suite summary is a comparison view, not a denominator for occurrence rates. Capped latency-band samples cannot prove whole-run P99, failure rate, or performance gain.
- Do not publish automatically. Keep output local unless the user separately requests packaging or publication.

## Artifact references

- Read [triage.md](references/triage.md) for parsing, the normalized Run contract, and input rules.
- Read [bottleneck.md](references/bottleneck.md) for independent read/write attribution and [numa.md](references/numa.md) for NUMA evidence boundaries.
- Read [reports.md](references/reports.md) when changing page links, validation, packaging, or multi-Run summary behavior.
