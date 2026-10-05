# NUMA mode reference

This is the NUMA/WR capability of `ds-trace-analysis-pipeline`, not a separate skill. Run it alone when the requested upstream Triage and read-model artifacts already exist; use `pipeline` for a complete report. The pipeline reads validated Evidence directly, so NUMA no longer waits for the read attribution stage.


## Boundary and order

This mode consumes existing Trace evidence rather than parsing raw logs again. First run
`scripts/ds_trace_analysis.py triage run`, then `scripts/ds_trace_analysis.py read`, and finally
run `scripts/ds_trace_analysis.py numa` with the resulting run directory and
`bottleneck.analysis.json`. The NUMA script may inspect archive member names to
recover collection cohorts, but trace contents continue to come from triage.

Keep the generic bottleneck page and NUMA page separate. They may be delivered
in one PR and one report package, but the generic script must not acquire
PR-specific chip semantics and the NUMA script must not duplicate RPC/URMA
parsing.

## Command

```bash
python3 scripts/ds_trace_analysis.py numa \
  --run-dir <triage-run-dir> \
  --bottleneck-analysis <triage-run-dir>/bottleneck.analysis.json \
  --archive <original-trace-archive.tar.gz> \
  --source-head <verified-head> --source-base <verified-base> --pr <number> \
  --output <share-dir>/numa.html \
  --analysis-json <share-dir>/numa.analysis.json
```

Add known runtime axes such as QPS, client count, threads per client, and workers
per node. Filenames and directory names describe experiment intent only; verify
source behavior against the pinned source and runtime behavior against evidence.

## Timeout and missing-evidence rules

Normalize `URMA_WAIT_TIMEOUT`, `URMA-WAIT-TIMEOUT`, `URMA WAIT TIMEOUT`, and
`Timed out waiting for urma_request_id` as the `URMA超时` evidence family.
Preserve GET status 1004 and PUT status 1010 as distinct upward error chains.
If a timed-out WR has no completed `URMA_ELAPSED_TOTAL`, its URMA duration is
缺失/未观测, not 0; an explicit `elapsedMs` is timeout evidence, not a completed
transport duration.

Keep missing RPC, URMA, chip, CPU, lock, and scheduling fields as 未观测. Do not
claim throughput or performance benefit from `srcChipInflight` alone, and do not
infer receiver bandwidth from a sender-side inflight snapshot. Deduplicate the
same Trace across collection cohorts while retaining every cohort label.

## Required checks

```bash
python3 -m pytest -q -s tests/scripts/ds_trace_analysis/analysis/test_ds_trace_numa_analysis.py
python3 -m pytest -q -s tests/scripts/ds_trace_analysis/delivery/test_ds_trace_analysis_skills.py
python3 -m py_compile scripts/ds_trace_analysis.py scripts/trace_analysis/numa.py
```

Render once with a non-default PR number and confirm the title, navigation,
source section, filters, pagination, downloads, responsive table, and missing
evidence wording are all data-driven.

## 报告集成与版式验收

生成、后处理和打包页面时，遵守共享的
[报告集成与防遮挡约束](../../../.repo_context/modules/infra/observability/performance-troubleshooting.md#报告集成与防遮挡约束)。
保留原生布局；跨 Run 总结和统一口径放在首页或问题分析页，不在明细布局外追加重复说明横幅。
侧栏样式须隔离到专用容器，本页滚动高亮不能处理跨页链接。
交付前检查最终页面在桌面、窄屏及滚动后的实际遮挡、可读性和导航交互；
不能仅凭无横向溢出或 JavaScript 无异常判定版式通过。必要的局部证据限制仍保留在原有说明区域。
