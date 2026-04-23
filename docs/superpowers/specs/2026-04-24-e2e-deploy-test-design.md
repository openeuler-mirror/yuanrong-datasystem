# E2E Deploy/Stop/Collect Test Design

## Context

`kvclient_standalone_test` 的部署工具链（deploy.py / worker_manager.py / procmon.py）需要在真实多节点环境中验证 deploy、stop、collect 三个核心操作的**功能正确性、稳定性、采集数据准确性**。同时在运行过程中监控 KVClient 和 datasystem worker 的异常现象（Error、Coredump、请求报错），分析根因，修复 kvclient_standalone_test 引入的问题。

## Environment

```
Local compile server
  |
  | SSH (scp binary + SDK)
  +--- 1.95.199.126:22223 (Machine A, root, password: 1qaz@WSX)
  |     +-- etcd (localhost:2379)
  |     +-- datasystem worker (pre-installed)
  |     +-- kvclient_standalone_test (writer, instance_id=0)
  |
  +--- 1.95.199.126:22224 (Machine B, root, password: 1qaz@WSX)
        +-- datasystem worker (pre-installed)
        +-- kvclient_standalone_test (reader, instance_id=1)
```

Transport: SSH. Machine A runs etcd + writer. Machine B runs reader. Writer notifies reader via HTTP /notify.

**Setup prerequisites:**
1. Discover actual IPs of Machine A and Machine B (via `ip addr` after SSH)
2. Install etcd on Machine A
3. Install and start datasystem workers on both machines
4. Compile kvclient_standalone_test locally with `-static-libstdc++ -static-libgcc`

## Test Cases

### TC1: Deploy

**Precondition:** Datasystem workers running on both machines.

**Steps:**
```bash
./deploy.py --deploy config/deploy.test.json config/config.test.json
```

**Pass criteria:**
- Both nodes print `-> OK`
- Deploy result: `2/2 succeeded`
- SSH check: `pgrep -f kvclient_standalone_test` returns PID on both machines
- SSH check: config files exist at remote_work_dir
- SSH check: procmon.py running on both machines

### TC2: Metrics Data Accuracy (30s run)

**Precondition:** TC1 passed.

**Steps:** Sleep 35 seconds, then check metrics.

**Pass criteria:**
- Machine A `metrics_0.csv`: setStringView rows with count > 0
- Machine B `metrics_1.csv`: getBuffer rows with count > 0
- All latency values > 0 (no 0.000 after setprecision(3) fix)
- QPS within +-50% of target_qps
- No verify_fail in output

### TC3: Stop

**Precondition:** TC1 passed.

**Steps:**
```bash
./deploy.py --stop config/deploy.test.json config/config.test.json
```

**Pass criteria:**
- Both nodes print `-> OK`
- Stop result: `2/2 succeeded`
- SSH check: `pgrep -f kvclient_standalone_test` returns nothing
- SSH check: `pgrep -f procmon.py` returns nothing
- Completes within 30 seconds (no hang)

### TC4: Collect

**Precondition:** TC1 ran 30s, then TC3 stop.

**Steps:**
```bash
./deploy.py --collect config/deploy.test.json config/config.test.json
```

**Pass criteria:**
- Output: `2 collected, 0 empty, 0 failed`
- `collected/` directory has 2 subdirectories
- Each subdirectory contains: `metrics_*.csv`, `summary_*.txt`, `procmon_*.log`, `stdout_*.log`
- CSV files non-empty with data rows
- Summary files show Total > 0 for setStringView/getBuffer

### TC5: Stability (2-round deploy-stop cycle)

**Steps:**
1. deploy -> 30s run (with TC7 health check) -> stop
2. deploy -> 30s run (with TC7 health check) -> stop
3. collect

**Pass criteria:**
- All 4 deploy/stop operations: `2/2 succeeded`
- No hangs or timeouts
- No coredumps in either round
- Process alive after each deploy, dead after each stop
- Final collect succeeds
- Second-round metrics data present and normal
- No new errors in second round that weren't in first round

### TC6: Stop on Empty Instances

**Precondition:** No kvclient_standalone_test running.

**Steps:**
```bash
./deploy.py --stop config/deploy.test.json config/config.test.json
```

**Pass criteria:**
- Does not hang
- Reports errors but exits with code 0
- Completes within 15 seconds

### TC7: Runtime Health Monitoring

Runs in parallel with TC2/TC5 during the 30s test window. Monitors both kvclient_standalone_test and datasystem worker for anomalies.

**Checks performed during run:**

1. **Coredump detection:**
   - SSH: `dmesg | grep -i "coredump\|segfault\|killed" | tail -5` on both machines
   - SSH: `ls /var/lib/apport/coredump/ 2>/dev/null; ls core.* 2>/dev/null` in work dir
   - If coredump found: FAIL. Analyze with `coredumpctl info <pid>` or `gdb`.

2. **Process liveness:**
   - SSH: `pgrep -f kvclient_standalone_test` — process must still exist
   - SSH: `pgrep -f datasystem_worker` — worker must still exist
   - If process disappeared: FAIL. Check stdout_*.log for crash info.

3. **KVClient error log scan:**
   - SSH: `grep -i "error\|fail\|exception\|abort" stdout_*.log` on both machines
   - Ignore known benign patterns (e.g. connection refused during startup, ZMQ retries)
   - If unexpected errors found: FAIL. Classify root cause.

4. **Datasystem worker log scan:**
   - SSH: `grep -i "error\|fatal\|coredump" <worker_log_path>` on both machines
   - If errors found: analyze whether caused by kvclient behavior or worker bug.

5. **Request failure metrics:**
   - Check `summary_*.txt` after stop: `Fail` count for each op
   - Check metrics CSV: any op with success rate < 100% warrants investigation
   - Check `verify_fail` count in summary

**Error classification:**

| Category | Example | Expected action |
|----------|---------|----------------|
| kvclient bug | segfault in pattern data, wrong buffer size | Fix in kvclient source |
| SDK misuse | wrong SetParam, invalid WriteMode | Fix in kvclient source |
| Worker issue | worker OOM, worker restart | Report, not kvclient fault |
| Network transient | connection timeout, ZMQ retry | Expected, not a bug |
| Config error | wrong etcd address, wrong port | Fix test config |

**Deliverable for this TC:** A health report printed at end of test:
```
=== Health Report ===
Machine A (writer):
  kvclient process: ALIVE
  worker process: ALIVE
  coredumps: NONE
  errors in stdout_0.log: 0 unexpected
  summary_0.txt: setStringView 300/300 success, 0 fail, verify_fail=0

Machine B (reader):
  kvclient process: ALIVE
  worker process: ALIVE
  coredumps: NONE
  errors in stdout_1.log: 0 unexpected
  summary_1.txt: getBuffer 280/280 success, 0 fail, verify_fail=0
```

## Deliverables

1. **`test_e2e.sh`** - Automated smoke test script covering TC1-TC7
2. **`config/deploy.test.json`** - Deploy config for the two test machines
3. **`config/config.test.json`** - Kvclient config (small data, low QPS, short TTL)
4. **Manual checklist** - Data accuracy steps + error root cause analysis

## test_e2e.sh Structure

```bash
#!/usr/bin/env bash
# E2E test for kvclient_standalone_test deploy/stop/collect
set -euo pipefail

SSH_A="ssh -o StrictHostKeyChecking=no -p 22223 root@1.95.199.126"
SSH_B="ssh -o StrictHostKeyChecking=no -p 22224 root@1.95.199.126"
DEPLOY="config/deploy.test.json"
CONFIG="config/config.test.json"
PASS=0; FAIL=0

log_pass() { echo "  PASS: $1"; ((PASS++)); }
log_fail() { echo "  FAIL: $1"; ((FAIL++)); }

# --- Setup ---
# 1. Get actual IPs of both machines
# 2. Install etcd on Machine A
# 3. Start datasystem workers on both

# --- TC1: Deploy ---
# --- TC2: Metrics (30s) ---
# --- TC7: Health check (in parallel with TC2) ---
#   check_process_liveness()
#   check_coredumps()
#   check_error_logs()
#   print_health_report()
# --- TC3: Stop ---
# --- TC4: Collect ---
# --- TC5: Stability (2 rounds, health check each round) ---
# --- TC6: Stop empty ---

echo "Results: $PASS passed, $FAIL failed"
```

## Config Files

### config/deploy.test.json

deploy.py needs a new per-node `ssh_port` field. When set, `run_on` and `scp_to` pass `-p <port>` to ssh/scp commands.

```json
{
  "remote_work_dir": "/tmp/kvclient_test",
  "transport": "ssh",
  "ssh_user": "root",
  "ssh_options": "-o StrictHostKeyChecking=no",
  "enable_procmon": true,
  "nodes": [
    {
      "host": "<MACHINE_A_IP>",
      "ssh_port": 22223,
      "instance_id": 0,
      "role": "writer",
      "pipeline": ["setStringView"],
      "notify_pipeline": ["getBuffer"]
    },
    {
      "host": "<MACHINE_B_IP>",
      "ssh_port": 22224,
      "instance_id": 1,
      "role": "reader",
      "pipeline": [],
      "notify_pipeline": ["getBuffer"]
    }
  ]
}
```

### config/config.test.json
```json
{
  "listen_port": 9000,
  "etcd_address": "<MACHINE_A_IP>:2379",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1KB", "4KB"],
  "ttl_seconds": 30,
  "target_qps": 10,
  "num_set_threads": 1,
  "notify_count": 1,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_{instance_id}.csv"
}
```

Note: host and etcd_address placeholders filled at runtime with actual IPs discovered via SSH.
