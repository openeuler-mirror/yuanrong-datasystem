# Report/pipeline mode reference

This is the full-report and render-only capability of `ds-trace-analysis-pipeline`, not a separate skill.

Each Run is isolated under the output root and contains:

- `bottleneck.local.html` + `bottleneck.analysis.json` for read/GET analysis;
- `bottleneck.write.html` + `write.refined.analysis.json` for independent write analysis;
- `numa.local.html` + `numa.analysis.json` for WR/chip/NUMA analysis;
- `issues.analysis.json` for operation-separated observed failures, timeout counts and evidence gaps;
- validation JSON for triage, bottleneck, write, NUMA and issues, plus the aggregate results in `pipeline.validation.json`;
- `run.summary.json` for compact homepage comparisons.

`ds_trace_analysis.py suite` assembles the suite model and delegates the default homepage rendering to
`trace_analysis/overview.py` and `scripts/trace_analysis/assets/overview/overview.{html,css,js}`. The stable root `index.html` points atomically to the latest successful publication. Its real homepage provides
Run filters and links to all pages. Keep links relative inside that self-contained publication; packaging resolves
the authoritative entry, validates its seal/models, and preserves the selected generation in its package manifest. A successful pipeline JSON check does not replace browser rendering checks.

## Copyable request for a colleague

Replace the placeholders before sending this prompt. If the deployed revision is unknown, provide a verified source
reference for interpretation and explicitly retain that uncertainty; do not invent a runtime revision or configuration.

```text
请用 ds-trace-analysis-pipeline 分析 <输入目录/归档> 的全部 Runs，输出到 <报告目录>；
源码参考 <source_head>、基线 <source_base>，重点看 <异常现象/Run>。
先生成manifest：同Run的all-core/time合并，不同Run隔离，以完整Run目录/归档为输入，top=0解析全部已有Trace。
按triage规范化并校验 → 独立读取模型 → 独立写入模型 → NUMA → 问题分析中间产物 → 多Run首页执行，复用统一skill的模式规则和仓库模板。
先用 --jobs 1 --resume；多 Run 在目标机器对比 --run-executor process --jobs 2，并监测所有子进程内存。更高并行度依据实测调整。同一输出目录只运行一个pipeline，失败Run不能跳过。
缺失Worker日志保留“未观测”；无证据不推断POD被kill、clients配置、物理网络根因或整场失败率/P99。
首页与Run页统一风格，提供Run按钮；校验中间结果/阶段预算、全部Run状态、浏览器错误/遮挡及离线链接。
通过后交付本地首页、离线ZIP、关键结论和验证结果，不自动发布网站或推送代码。
```

The verified CLI entry point is:

```bash
python3 scripts/ds_trace_analysis.py pipeline \
  --manifest <pipeline.manifest.json> --output <report-root> --jobs 1 --resume
```

Only presentation changes may reuse the persisted models with
`python3 scripts/ds_trace_analysis.py pipeline --output <report-root> --render-only --jobs 1`.
The cached write model must have `write_phase_schema_version=2`; render-only rejects older models
before creating a publication. Use the normal pipeline with `--resume` to rebuild the write stage;
do not label absent fields in a legacy model as absent log evidence. Parsing or attribution changes
require normal analysis. Inspect `render.validation.json` and repeat browser checks for render-only
runs; it does not prove that older analysis used the current parser.

For generated publications, inspect `publication.validation.json` as well as the attempt-level
`pipeline.validation.json`. A failed new attempt can coexist with a valid previous publication;
do not mistake an old root compatibility manifest for a newly completed report. Never edit sealed
publication files in place. Run render-only to build a fresh view generation.

Browser acceptance must finish lazy rendering, exercise Worker/RPC filters and pagination, then call
`ReportRegistry.audit({requireComplete: true, checkLayout: true})`. Validate 1500/1280/900/390px.
Expected components that remain pending, absent, or on an earlier filter revision are failures;
empty/unavailable states need their observed-data reason. Model validation alone is not browser validation.

Component contracts bind actual model collections/fields and the shared filter scope. The registry recomputes
source availability; renderer-reported counts alone are not evidence. Test both directions: non-empty observed
sources with missing charts must fail, and empty selections with stale charts/tables must also fail. Distinguish
missing required fields (error), empty source, no filter match, and null-only observations. When adding a chart
or data table, register its model binding as well as its caption and DOM identity.

Exercise a real scatter point in each Trace timeline, not just chart initialization. The selected evidence must
change to that event and retain process/component/time/source metadata, URMA/RPC/summary field highlighting,
and error highlighting. Log text containing HTML must remain escaped text. Check long selected evidence at
narrow width for wrapping without clipped content; an empty-state chart audit does not cover this interaction.

After hovering or clicking a timeline point at desktop width, resize to 390px with the tooltip still active.
The stale tooltip must not widen the document; hovering again must show a readable confined tooltip.

Check full Trace log tables with multiline errors duplicated across core/time collections. Original
file:line provenance also applies to continuation lines without timestamps; merge identical copies
within the same source scope and retain their collection origins. Different original lines, workers,
processes, or source scopes must remain distinct. Do not infer event time for undated continuations
or change model aggregates merely to deduplicate the displayed evidence.

Measure text insets against chapter background edges as well as bordered panels. A white section
without a CSS border can still have zero padding and cramped headings/filters. Check all homepage
chapters at desktop and narrow widths without adding redundant nested frames.

Homepage quantity axes must reduce tick density with the actual plot width, preserve integer counts,
and retain sub-millisecond precision on latency axes. Run `node tests/scripts/ds_trace_analysis/browser/check_ds_trace_overview_layout.js
<index.html>` against the generated homepage: it checks chapter insets, visible numeric-label spacing,
registry completeness, canvas/content-box height agreement, axis-name/caption clearance and document
overflow at 1500/1280/900/390px. After chart initialization, let the shared chart layout own its height;
page renderers must not independently overwrite the container height. Check axis names on every page,
including category-axis endpoints and vertical-axis names, against the canvas bounds. Set `DS_PLAYWRIGHT_MODULE` and
`DS_CHROMIUM_EXECUTABLE` when browser dependencies are not on the default path.
Check the same chapter-background insets in Triage and NUMA. For horizontal quantity plots in
read/write detail pages, use the remaining grid width after category labels and margins, not the
container width, to size numeric tick spacing. Preserve explicit intervals and latency precision.
Check visible numeric labels against canvas edges too: no-overlap does not detect clipped leading
digits. Reserve both left and right value-axis margins while retaining explicit axis scales.
Use `node tests/scripts/ds_trace_analysis/browser/check_ds_trace_detail_layout.js <triage.html> <read.html> <write.html>
<numa.html>` for the same four-width checks on Run pages; it also measures visible numeric text
bounds against the canvas and chapter background edges. Axis names on both category and value axes
must also fit inside the canvas; successful series rendering alone does not prove their names are visible.
