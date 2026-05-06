---
name: ds-log-analysis
description: >
  Analyze KVCache logs and generate interactive HTML reports with ECharts charts.
  Two scripts: access log analysis (QPS, latency, errors) and worker resource log
  analysis (memory, thread pools, cache hit rate, ETCD).
  Triggers: log analysis, access log, resource log, KVCache report, error analysis,
  QPS trend, latency P99, hit rate, thread pool, memory usage, worker resource,
  performance analysis, log report generation.
---

# KVCache Log Analysis

Generate self-contained HTML reports with ECharts interactive charts from KVCache logs.

**Scripts:**
- `scripts/generate_access_log_report.py` — Access log analysis (client/worker)
- `scripts/generate_resource_report.py` — Worker resource log analysis

---

## When to Use Which Script

| Scenario | Script |
|---|---|
| Access log: QPS, latency P90/P99/P99.99, error rate, per-API stats | `generate_access_log_report.py` |
| Worker resource: memory, thread pools, cache hit rate, ETCD queue, active clients | `generate_resource_report.py` |

---

## Access Log Report

```bash
python3 scripts/generate_access_log_report.py <log_dir> \
    --since "YYYY-MM-DDTHH:MM:SS" \
    [-o output.html] [--until "YYYY-MM-DDTHH:MM:SS"] [--open]
```

| Arg | Required | Description |
|---|---|---|
| `log_dir` | Yes | Root directory containing access log files |
| `--since` | Yes | Start time (inclusive), format: `YYYY-MM-DDTHH:MM:SS` |
| `--until` | No | End time (exclusive) |
| `-o` | No | Output HTML path (default: `kvcache_log_analysis.html`) |
| `--log-pattern` | No | Filename substring to match (default: `access`) |
| `--open` | No | Open in browser after generation |

**Report sections:** Summary Cards (total requests, peak QPS, errors), QPS Trend, Avg/P90/P99/P99.99/Max Latency Trends, Error Count (bar chart), Error Code Distribution (pie chart), Latency Distribution per API (grouped bars), Overall Summary Table, Hourly Detail Table.

---

## Resource Log Report

```bash
python3 scripts/generate_resource_report.py <log_dir> \
    --since "YYYY-MM-DDTHH:MM:SS" \
    [-o output.html] [--until "..."] [--interval 60] [--open]
```

| Arg | Required | Description |
|---|---|---|
| `log_dir` | Yes | Root directory containing worker resource.log files |
| `--since` | Yes | Start time (inclusive), format: `YYYY-MM-DDTHH:MM:SS` |
| `--until` | No | End time (exclusive) |
| `-o` | No | Output HTML path (default: `resource_report.html`) |
| `--log-pattern` | No | Filename substring to match (default: `resource`) |
| `--interval` | No | Time bin in minutes (default: 60). Smaller = finer granularity |
| `--open` | No | Open in browser after generation |

**Report sections (10 charts + detail table):**
1. Memory Usage Trend (avg/max/limit)
2. Memory Usage Ratio (usage% + per-host distribution)
3. Object Cache (object count & size)
4. Cache Hit Rate (hit rate trend + hit/miss breakdown, 99% threshold reference line)
5. Thread Pool Utilization (5 pools: Active/Total/Max/Waiting)
6. ETCD Queue (queue depth vs limit)
7. Active Clients (connected client count)
8. ETCD Success Rate (ETCD request success rate)
9. Per-Host Memory (interactive per-host selector)
10. Hourly Detail Table (all metrics)

**Interval guidance:** < 3h span → `--interval 10`, < 12h → `--interval 30`, > 12h → default 60.

---

## Directory Layouts (Auto-Detected)

Both scripts auto-detect 3 directory structures:

```
Layout 1  <log_dir>/<host_ip>/logs/<log_file>
          e.g. /data/worker_logs/192.168.1.10/logs/access.log

Layout 2  <log_dir>/<wrapper>/<host_ip>/logs/<log_file>
          e.g. /data/worker_logs/kvcache_logs_20260506/192.168.1.10/logs/resource.log

Layout 3  <log_dir>/<log_file>   (single host, flat)
```

---

## Workflow

1. **Identify log type**: access log (contains API names like `DS_KV_CLIENT_GET`) vs resource log (contains `SHARED_MEMORY`, thread pool stats).
2. **Get log directory and time range** from user. `--since` is required.
3. **Run the appropriate script**:
   ```bash
   python3 scripts/generate_access_log_report.py <log_dir> \
       --since "2026-05-05T22:30:00" -o report.html --open
   ```
4. **Report result**: output path, file size. Note: report needs internet for ECharts CDN.

### WSL

```bash
python3 scripts/generate_resource_report.py <log_dir> \
    --since "2026-05-05T22:30:00" \
    -o /mnt/d/html/report.html --open
```

---

## Notes

- Reports require internet to load ECharts from CDN (jsdelivr.net).
- OC_HIT_NUM is a cumulative counter; per-bin deltas are computed automatically.
- Thread pool "Active" = Total - Idle (actual working threads).
- Cache Hit Rate chart has a 99% dashed reference line — normal hit rate should be above this line.
