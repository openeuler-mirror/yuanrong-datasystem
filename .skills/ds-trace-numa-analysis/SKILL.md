---
name: ds-trace-numa-analysis
description: Use when DataSystem trace work needs NUMA or source-chip selection, srcChipInflight, multi-arena behavior, URMA timeout error chains, GET 1004 or PUT 1010 diagnosis after ds-trace-triage and bottleneck analysis.
---

# DataSystem NUMA Trace Analysis

## Boundary and order

This is a focused post-processor, not another trace parser. First run
`scripts/ds_trace_triage.py`, then `scripts/ds_trace_bottleneck.py`, and finally
run `scripts/ds_trace_numa_analysis.py` with the resulting run directory and
`bottleneck.analysis.json`. The NUMA script may inspect archive member names to
recover collection cohorts, but trace contents continue to come from triage.

Keep the generic bottleneck page and NUMA page separate. They may be delivered
in one PR and one report package, but the generic script must not acquire
PR-specific chip semantics and the NUMA script must not duplicate RPC/URMA
parsing.

## Command

```bash
python3 scripts/ds_trace_numa_analysis.py \
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
python3 -m pytest -q -s tests/scripts/test_ds_trace_numa_analysis.py
python3 -m pytest -q -s tests/scripts/test_ds_trace_analysis_skills.py
python3 -m py_compile scripts/ds_trace_numa_analysis.py
```

Render once with a non-default PR number and confirm the title, navigation,
source section, filters, pagination, downloads, responsive table, and missing
evidence wording are all data-driven.
