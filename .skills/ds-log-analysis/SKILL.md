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
    [-o output.html] [--until "YYYY-MM-DDTHH:MM:SS"] [--interval 1] [--open]
```

| Arg | Required | Description |
|---|---|---|
| `log_dir` | Yes | Root directory containing access log files |
| `--since` | Yes | Start time (inclusive), format: `YYYY-MM-DDTHH:MM:SS` |
| `--until` | No | End time (exclusive) |
| `-o` | No | Output HTML path (default: `kvcache_log_analysis.html`) |
| `--log-pattern` | No | Filename substring to match (default: `access`) |
| `--interval` | No | Time bin in minutes (default: 60). Use 1 for per-minute, 10 for per-10min |
| `--open` | No | Open in browser after generation |

**Report sections:** Summary Cards (total requests, peak QPS, errors), QPS Trend, Avg/P90/P99/P99.99/Max Latency Trends, Error Count (bar chart), Error Code Distribution (pie chart), Latency Distribution per API (grouped bars), Overall Summary Table, Detail Table.

---

## Resource Log Report

```bash
python3 scripts/generate_resource_report.py <log_dir> \
    --since "YYYY-MM-DDTHH:MM:SS" \
    [-o output.html] [--until "..."] [--interval 1] [--open]
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
10. Detail Table (all metrics)

**Interval guidance:** < 3h span -> `--interval 1`, < 12h -> `--interval 10`, > 12h -> default 60.

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

Both scripts support `.log` and `.log.gz` (gzip compressed rotated logs).

---

## Log Types & Naming Conventions

| Log Type | Client (SDK) | Worker |
|----------|-------------|--------|
| Access log prefix | `ds_client_access_<PID>` | `access` |
| API names | `DS_KV_CLIENT_GET`, `DS_KV_CLIENT_SET`, `DS_KV_CLIENT_CREATE` | `DS_POSIX_GET`, `DS_POSIX_CREATE`, `DS_POSIX_PUBLISH` |
| Resource log | N/A | `resource.log` |

When using `--log-pattern`:
- Client access logs: `--log-pattern "ds_client_access"` (matches `ds_client_access_4047.log` etc.)
- Worker access logs: `--log-pattern "access"` (matches `access.log`, `access.20260508*.log.gz`)

---

## Workflow

1. **Identify log type**: access log (contains API names like `DS_KV_CLIENT_GET` or `DS_POSIX_GET`) vs resource log (contains `SHARED_MEMORY`, thread pool stats).
2. **Determine time range**: check first/last timestamps in log files. Use `--since` (required) and `--until` to scope.
3. **Choose interval**: 1min for < 3h spans, 10min for < 12h, 60min for > 12h.
4. **Run the appropriate script**:
   ```bash
   python3 scripts/generate_access_log_report.py <log_dir> \
       --since "2026-05-05T22:30:00" --interval 1 -o report.html
   ```
5. **Report result**: output path, file size. Note: report needs internet for ECharts CDN.

### WSL (open in Windows browser)

```bash
python3 scripts/generate_resource_report.py <log_dir> \
    --since "2026-05-05T22:30:00" --interval 1 \
    -o /mnt/d/html/report.html

# Open in Edge:
EDGE="/mnt/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"
"$EDGE" "$(wslpath -w /path/to/report.html)" &
```

---

## Root Cause Analysis Guide

When investigating high latency in access logs, follow this diagnostic flow:

### 1. Identify the bottleneck side

Cross-reference client traceIDs in worker access logs to determine where time is spent:

```python
# Client access log fields (pipe-separated):
# timestamp | level | source | host | pid:tid | traceId | | status | operation | duration_us | size | params | message

# Worker access log has same traceId but different operation names:
# Client: DS_KV_CLIENT_GET  <->  Worker: DS_POSIX_GET
# Client: DS_KV_CLIENT_SET  <->  Worker: DS_POSIX_PUBLISH
```

If **worker duration < 1ms** but **client duration ~20ms+,** the bottleneck is in the **client-side RPC layer** (not worker processing).

### 2. Common high-latency patterns

| Pattern | Client Duration | Worker Duration | Root Cause |
|---------|----------------|-----------------|------------|
| RPC timeout | ~20-22ms (matches rpc timeout) | < 1ms | Client waiting for RPC timeout; check `async_release_buffer` thread pool |
| Worker slow | 20ms+ | 20ms+ | Worker processing bottleneck; check thread pools, memory pressure |
| SHM transfer | ~8-12ms, size=8MB | < 1ms | Normal SHM data copy time for large objects |
| Network jitter | Variable, no ceiling | < 1ms | Network latency; check RDMA/UB link quality |

### 3. Key metrics to check

- **`async_release_buffer` thread pool fullness**: If 100% full continuously, client-side buffer release blocks new requests
- **Failure rate**: `size=0` or "Cannot get objects from worker" messages indicate cache miss or worker unavailability
- **RPC timeout value**: Logged in INFO as `rpc timeout: N`; default is 20ms. Top latency often clusters at this value
- **QPS/Concurrency**: Use Little's Law (`concurrency = QPS * avg_latency`) to estimate; compare against thread pool sizes

### 4. INFO log chain for GET requests

```
Begin to get object, dst=<IP:PORT>          <- client starts
Start to send rpc, rpc timeout: 20          <- RPC sent
Not all expected objects were obtained       <- failure (WARNING)
Finish to Get objects                        <- complete
```

Gap between operation start and "Begin to get object" indicates client-side queuing delay.

---

## Script Maintenance Notes

### Supported file formats
- Plain text: `*.log`
- Gzip compressed: `*.log.gz` (rotated logs)
- Both are auto-detected and handled by `_find_log_files()` and `parse_records()`

### API color palette
Defined in `API_COLORS` dict in `generate_access_log_report.py`. Must include both client (`DS_KV_CLIENT_*`) and worker (`DS_POSIX_*`) API names. Missing entries default to gray `#999`.

### Adding new APIs
1. Add entry to `API_COLORS` with a distinct color
2. The `_make_series()` function strips `DS_KV_CLIENT_` and `DS_POSIX_` prefixes for display names

---

## Notes

- Reports require internet to load ECharts from CDN (jsdelivr.net).
- OC_HIT_NUM is a cumulative counter; per-bin deltas are computed automatically.
- Thread pool "Active" = Total - Idle (actual working threads).
- Cache Hit Rate chart has a 99% dashed reference line -- normal hit rate should be above this line.
