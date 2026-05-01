# kvclient_standalone_test

独立的 datasystem KVClient 集成测试工具，支持 Writer/Reader 角色分离、可配置 Pipeline、多节点部署、K8s 自动发现。

## 快速开始

### 1. 编译

```bash
cd tests/kvclient_standalone

# 构建 SDK（项目根目录）
cd /path/to/yuanrong-datasystem && bash build.sh -b bazel

# 编译
mkdir -p build && cd build
cmake -DDATASYSTEM_SDK_DIR=/path/to/yuanrong-datasystem/output/cpp ..
make -j$(nproc)

# 拷贝 SDK 到部署目录
cd .. && make copy-sdk
```

### 2. 启动依赖

```bash
# etcd
etcd &

# worker（必须从短路径启动）
mkdir -p /tmp/ds_worker && cd /tmp/ds_worker
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379
```

### 3. 单节点运行

```bash
export LD_LIBRARY_PATH=/path/to/sdk/lib:$LD_LIBRARY_PATH
./build/kvclient_standalone_test config/my_config.json

# 查看统计
curl http://127.0.0.1:9000/stats | python3 -m json.tool

# 停止
curl -X POST http://127.0.0.1:9000/stop
```

## 多节点部署

### deploy.json

```json
{
  "remote_work_dir": "/tmp/kvclient_test",
  "duration": "10m",
  "transport": "ssh",
  "ssh_user": "root",
  "ssh_options": "-o StrictHostKeyChecking=no",
  "remote_sdk_dir": "/usr/local/datasystem/lib",
  "nodes": [
    {
      "host": "10.0.0.1", "ssh_port": 22,
      "comm_host": "10.0.0.1",
      "instance_id": 0, "role": "writer",
      "pipeline": ["setStringView"],
      "notify_pipeline": ["getBuffer"],
      "port": 9000
    },
    {
      "host": "10.0.0.2", "ssh_port": 22,
      "comm_host": "10.0.0.2",
      "instance_id": 1, "role": "reader",
      "pipeline": [],
      "notify_pipeline": ["getBuffer"],
      "listen_port": 9001, "port": 9001
    }
  ]
}
```

### 命令

```bash
# 部署 + 启动（若设了 duration，到时自动 stop + collect）
python3 deploy.py --deploy deploy.json [config_template.json]

# 停止
python3 deploy.py --stop deploy.json

# 收集结果到 collected/
python3 deploy.py --collect deploy.json

# 清理（杀进程 + 删除远程目录）
python3 deploy.py --clean deploy.json
```

### duration 定时停止

在 deploy.json 中加 `"duration": "10m"` 即可无人值守运行。支持格式：`30s`、`5m`、`2h`、纯数字（秒）。不设或 `"0"` 则不自动停止。

等待期间 `Ctrl+C` 可提前停止并触发 stop + collect。

### Kubernetes 部署

用 `gen_deploy_config.py` 自动从 k8s Pod 生成 deploy.json，自动分配 writer/reader 角色：

```bash
# 1 writer + 10 readers
python3 gen_deploy_config.py -p ds-worker -n datasystem -w 1
# 1 writer + 10 readers
python3 gen_deploy_config.py -p mypod -n myns -w 1

# 生成 config/deploy.json + config/config.json
python3 deploy.py --deploy config/deploy.json config/config.json
```

`gen_deploy_config.py` 参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-p` / `--prefix` | (必填) | Pod 名称前缀 |
| `-n` / `--namespace` | `default` | k8s 命名空间 |
| `-w` / `--writer-count` | `1` | Writer 数量，前 N 个 Pod 为 writer，其余为 reader |
| `--pipeline` | `setStringView` | Writer pipeline ops，逗号分隔 |
| `--notify-pipeline` | `getBuffer` | Notify pipeline ops，逗号分隔（所有节点共用） |
| `--batch-keys-count` | `1` | Batch ops 的 key 数量，>1 时写入 deploy.json |
| `-e` / `--etcd-address` | | 覆盖 config 中的 etcd_address |
| `-c` / `--cluster-name` | | 设置 config 中的 cluster_name |
| `--remote-sdk-dir` | | 容器内 SDK 路径（跳过从本机拷贝） |

示例：

```bash
# 默认：setStringView 写入，getBuffer 通知读取
python3 gen_deploy_config.py -p ds-worker -n datasystem -w 1

# Batch 模式：mCreate+mSet 写入，mGet 通知读取，每次 5 个 key
python3 gen_deploy_config.py -p ds-worker -n datasystem -w 1 \
  --pipeline mCreate,mSet \
  --notify-pipeline mGet \
  --batch-keys-count 5
```

## 典型场景

### 场景 1：单机双实例（1 writer + 1 reader）

同一台机器跑 writer 和 reader，适合功能验证和 E2E 测试。

**config.json：**

```json
{
  "etcd_address": "127.0.0.1:2379",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1KB", "4KB"],
  "ttl_seconds": 30,
  "target_qps": 10,
  "num_set_threads": 1,
  "notify_count": 1,
  "metrics_interval_ms": 3000
}
```

**deploy.json：**

```json
{
  "remote_work_dir": "/tmp/kvclient_test",
  "transport": "ssh",
  "ssh_user": "root",
  "remote_sdk_dir": "/usr/local/datasystem/lib",
  "nodes": [
    {
      "host": "10.0.0.1", "ssh_port": 22,
      "comm_host": "10.0.0.1",
      "instance_id": 0, "role": "writer",
      "pipeline": ["setStringView"],
      "notify_pipeline": ["getBuffer"],
      "port": 9000
    },
    {
      "host": "10.0.0.1", "ssh_port": 22,
      "comm_host": "10.0.0.1",
      "instance_id": 1, "role": "reader",
      "pipeline": [],
      "notify_pipeline": ["getBuffer"],
      "listen_port": 9001, "port": 9001
    }
  ]
}
```

### 场景 2：1 writer + N readers（压测模式）

Writer 不限速全速写入，通知所有 reader 读取验证。

**config.json：**

```json
{
  "etcd_address": "10.0.0.1:2379",
  "data_sizes": ["1KB", "4KB"],
  "ttl_seconds": 30,
  "target_qps": 0,
  "num_set_threads": 5,
  "notify_count": 100,
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "metrics_interval_ms": 3000
}
```

> `target_qps: 0` 不限速，`notify_count: 100` 确保通知到所有 reader。

**deploy.json（Kubernetes，1 writer + 10 readers）：**

```bash
python3 gen_deploy_config.py -p ds-worker -n datasystem -w 1
```

自动生成 11 个节点：第一个为 writer，后 10 个为 reader。每个 node 带 `role`/`pipeline`/`notify_pipeline`。

然后加 duration 无人值守：

```json
{
  "duration": "30m"
}
```

```bash
python3 deploy.py --deploy config/deploy.json config/config.json
# 自动跑 30 分钟后 stop + collect
```

### 场景 3：多 writer 并发写入

多 writer 分片写入，每个 writer 通知所有 reader。

```json
{
  "target_qps": 0,
  "num_set_threads": 10,
  "notify_count": 100,
  "data_sizes": ["1KB"]
}
```

```bash
# 3 writers + 5 readers
python3 gen_deploy_config.py -p ds-worker -n datasystem -w 3
```

### 场景 4：长时间稳定性测试

低 QPS 长时间运行，验证内存泄漏和稳定性。

**config.json：**

```json
{
  "target_qps": 100,
  "num_set_threads": 2,
  "data_sizes": ["1KB", "4KB", "1MB"],
  "ttl_seconds": 60,
  "metrics_interval_ms": 10000
}
```

**deploy.json：**

```json
{
  "duration": "24h"
}
```

```bash
python3 deploy.py --deploy deploy.json config.json
# 跑 24 小时后自动停止收集
```

### 场景 5：跨子网部署

通过 SSH 隧道桥接，适合本机与远程不同子网的场景。

```bash
# 1. etcd 隧道（让远端访问本地 etcd）
ssh -fNR 22379:localhost:2379 remote_host

# 2. Worker 隧道（让远端 KVClient 连本地 worker）
ssh -fNR 31501:localhost:31501 remote_host

# 3. HTTP 隧道（让本地访问远端 /notify 端口）
ssh -fNL 19000:localhost:9000 remote_host
```

deploy.json 中用 `comm_host` 指向隧道地址：

```json
{
  "nodes": [
    {
      "host": "remote_host", "ssh_port": 22,
      "comm_host": "127.0.0.1",
      "instance_id": 0, "port": 9000
    }
  ]
}
```

或直接使用 `test_deploy.sh` 自动配置隧道：

```bash
bash test_deploy.sh
REMOTE_HOST=myserver bash test_deploy.sh
```

## 配置说明

### config.json

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `etcd_address` | (必填) | etcd 地址，`ip:port`，不加 `http://` |
| `cluster_name` | `""` | 集群名，本地测试留空 |
| `host_id_env_name` | `"JD_HOST_IP"` | ServiceDiscovery 的 hostId 环境变量名 |
| `listen_port` | 9000 | HTTP 服务端口 |
| `role` | `"writer"` | `writer` 或 `reader` |
| `pipeline` | `["setStringView"]` | Writer 主循环操作序列 |
| `notify_pipeline` | `["getBuffer"]` | 收到 /notify 后执行的操作序列 |
| `target_qps` | 100 | 目标总 QPS，`0` = 不限速全速运行 |
| `num_set_threads` | 16 | Writer 线程数 |
| `notify_count` | 10 | 每次写入后通知几个 peer（0 = 不通知） |
| `notify_interval_us` | 0 | 逐个通知 peer 的间隔(微秒)，`0` = 并行通知 |
| `enable_jitter` | true | 请求间隔随机化，打散多实例同步脉冲，QPS 均值不变 |
| `batch_keys_count` | 1 | Batch ops 的 key 数量，`1` = 单 key 兼容 |
| `enable_cross_node_connection` | true | 允许 failover 到其他节点的 standby worker |
| `cpu_affinity` | `""` | CPU 绑核，如 `"0-7"` 或 `"0,2,4,6"`，空 = 自动检测容器可用 CPU |
| `data_sizes` | `["8MB","512KB"]` | 测试数据大小 |
| `ttl_seconds` | 5 | 写入数据 TTL |
| `connect_timeout_ms` | 1000 | 连接超时(ms) |
| `request_timeout_ms` | 20 | 请求超时(ms) |
| `metrics_interval_ms` | 3000 | Metrics 采集间隔(ms) |
| `metrics_file` | `metrics_{instance_id}.csv` | Metrics 输出文件名 |

> `instance_id`、`nodes`、`peers` 由 deploy.py 自动注入，无需手动配置。

### deploy.json node 字段

| 字段 | 说明 |
|------|------|
| `host` | SSH 主机（`localhost` = 本机） |
| `ssh_port` | SSH 端口 |
| `comm_host` | 实例间通信 IP（默认同 host） |
| `instance_id` | 实例 ID |
| `role` | `writer` 或 `reader` |
| `pipeline` / `notify_pipeline` | 覆盖 config 模板 |
| `port` | kvclient 通信端口 |
| `listen_port` | HTTP 监听端口（默认同 port） |

Kubernetes 节点额外支持：`pod_name`、`pod_ip`、`namespace`、`transport: "kubectl"`。

## Pipeline 操作

| 操作名 | 说明 |
|--------|------|
| `setStringView` | 写入字符串数据 |
| `getBuffer` | 读取并验证数据 |
| `exist` | 检查 key 是否存在 |
| `createBuffer` | 创建共享内存 Buffer |
| `memoryCopy` | 向 Buffer 写入数据 |
| `setBuffer` | 提交 Buffer |
| `mCreate` | 批量创建 Buffer（配合 `batch_keys_count`） |
| `mSet` | 批量提交 Buffer（配合 `batch_keys_count`） |
| `mGet` | 批量读取并验证数据（配合 `batch_keys_count`） |

## HTTP API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/notify` | POST | 接收通知，执行 notify_pipeline |
| `/stats` | GET | 实时统计 JSON |
| `/stop` | POST | 优雅停止 |
| `/summary` | POST | 触发生成 summary 文件（不停止进程） |

## Metrics

- `metrics_{id}.csv` — 每 `metrics_interval_ms` 秒采集一次，含 count/avg/p90/p99/qps/throughput_MB_s
- `summary_{id}.txt` — 停止时生成，含完整运行期间的汇总（含 Throughput MB/s）

## 常见问题

**"Invalid etcd address"** — `etcd_address` 不加 `http://`，用 `ip:port`。

**"No available worker"** — 确认 worker 已启动（`etcdctl get --prefix "/datasystem/cluster"`），本地测试 `cluster_name` 设 `""`。

**"domain socket len > 108"** — worker 从短路径启动：`mkdir -p /tmp/ds_worker && cd /tmp/ds_worker`。

**"libdatasystem.so: cannot open"** — 设置 `LD_LIBRARY_PATH` 指向 SDK lib 目录。
