# kvclient_standalone_test

独立的 datasystem KVClient 集成测试工具，支持可配置的操作 Pipeline、Writer/Reader 角色分离、多节点部署。

## 目录结构

```
kvclient_standalone/
├── src/                    # 源代码
│   ├── main.cpp            # 入口
│   ├── config.h/cpp        # 配置解析
│   ├── pipeline.h/cpp      # Pipeline 执行引擎 + 6 个原子操作
│   ├── kv_worker.h/cpp     # Writer 主循环
│   ├── http_server.h/cpp   # HTTP 服务 (/notify, /stats, /stop)
│   ├── metrics.h/cpp       # 延迟统计 + CSV 输出
│   ├── stop.cpp            # --stop 模式
│   ├── simple_log.h        # 日志宏 (避免与 SDK spdlog 冲突)
│   └── data_pattern.h      # 测试数据生成
├── config/                 # 配置文件
│   ├── config.json.example # 配置模板
│   └── deploy.json.example # 部署模板
├── third_party/            # 第三方依赖
│   ├── spdlog-include/     # ds_spdlog 头文件
│   ├── json-include/       # nlohmann_json 头文件
│   └── sdk/               # SDK 动态库 libdatasystem.so (make copy-sdk, gitignore)
├── deploy.py               # 多节点部署脚本
├── test_deploy.sh          # 跨子网部署测试脚本
├── CMakeLists.txt
└── build/                  # 构建输出 (gitignore)
```

## 从零开始

### 1. 环境准备

需要以下组件：

| 组件 | 说明 |
|------|------|
| CMake 3.14+ | 构建工具 |
| C++17 编译器 | GCC 9+ 或 Clang 10+ |
| datasystem SDK | Bazel 构建产出 `libdatasystem.so` (~48MB) |
| etcd | 元数据服务 |
| datasystem worker | 通过 `dscli start -w` 启动 |

**安装 datasystem SDK：**

在项目根目录执行构建（使用 Bazel）：

```bash
cd /path/to/yuanrong-datasystem
bash build.sh -b bazel
```

构建产物在 `output/datasystem/` 目录下：
- `sdk/cpp/` — SDK 头文件 + 动态库
- `service/` — worker 二进制 + 依赖库

**安装 dscli：**

```bash
pip install output/openyuanrong_datasystem-*.whl
```

**安装 etcd：**

```bash
# 下载 etcd (示例版本，按需调整)
curl -L https://github.com/etcd-io/etcd/releases/download/v3.5.15/etcd-v3.5.15-linux-amd64.tar.gz | tar xz
export PATH=$PWD/etcd-v3.5.15-linux-amd64:$PATH
```

### 2. 编译

```bash
cd tests/kvclient_standalone

mkdir -p build && cd build

# DATASYSTEM_SDK_DIR 指向 SDK 安装路径
cmake -DDATASYSTEM_SDK_DIR=/path/to/yuanrong-datasystem/output/datasystem/sdk/cpp ..

make -j$(nproc)
cd ..
```

编译成功后生成 `build/kvclient_standalone_test` 二进制。

### 2.1 拷贝 SDK 动态库（部署用）

部署到远程节点前，需要将 SDK 动态库拷贝到 `third_party/sdk/`：

```bash
make copy-sdk
```

这会将 Bazel 构建的 `libdatasystem.so`（~48MB）拷贝到 `third_party/sdk/`，deploy.py 部署时会将它发送到远端节点。

> 如果 SDK 产物在其他路径：`make copy-sdk BAZEL_SDK_DIR=/path/to/output/cpp`

### 3. 启动依赖服务

**启动 etcd：**

```bash
etcd &
# 验证
etcdctl endpoint health
```

**启动 worker：**

```bash
# worker 必须从短路径目录启动（UDS路径不能超过108字符）
mkdir -p /tmp/ds_worker && cd /tmp/ds_worker

dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379

# 验证
etcdctl get --prefix "/datasystem/cluster"
```

### 4. 单节点测试

#### 4.1 创建配置文件

复制模板并修改：

```bash
cp config/config.json.example config/my_config.json
```

编辑 `config/my_config.json`：

```json
{
  "instance_id": 0,
  "listen_port": 9000,
  "etcd_address": "127.0.0.1:2379",
  "cluster_name": "",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1KB", "4KB"],
  "ttl_seconds": 30,
  "target_qps": 10,
  "num_set_threads": 1,
  "notify_count": 0,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_0.csv",
  "role": "writer",
  "pipeline": ["setStringView"],
  "notify_pipeline": ["getBuffer"],
  "peers": []
}
```

> **注意**：`etcd_address` 不要加 `http://` 前缀，用 `127.0.0.1:2379`。本地测试 `cluster_name` 设为空字符串 `""`。

#### 4.2 运行

```bash
export LD_LIBRARY_PATH=/path/to/sdk/lib:$LD_LIBRARY_PATH

./build/kvclient_standalone_test config/my_config.json
```

#### 4.3 查看结果

```bash
# 实时查看统计
curl http://127.0.0.1:9000/stats | python3 -m json.tool

# 优雅停止（会写 summary 文件）
curl -X POST http://127.0.0.1:9000/stop

# 查看汇总报告
cat summary_0.txt
```

汇总报告示例：

```
=== KVClient Standalone Test Summary ===
Instance ID: 0
Uptime: 30 seconds

--- setStringView ---
Total: 150, Success: 150, Fail: 0
Avg: 1.8ms, P90: 2.2ms, P99: 4.5ms, Min: 0.9ms, Max: 4.6ms
QPS: 5.0
```

## Pipeline 操作

支持 6 种可组合的原子操作：

| 操作名 | SDK 调用 | 说明 |
|--------|----------|------|
| `setStringView` | `client->Set(key, StringView(data), param)` | 写入字符串数据 |
| `getBuffer` | `client->Get(key, Optional<Buffer>&)` | 读取并验证数据 |
| `exist` | `client->Exist({key}, exists)` | 检查 key 是否存在 |
| `createBuffer` | `client->Create(key, size, param, buffer)` | 创建共享内存 Buffer |
| `memoryCopy` | `buffer->MemoryCopy(data, size)` | 向 Buffer 写入数据 |
| `setBuffer` | `client->Set(buffer)` | 提交 Buffer 到 datasystem |

### Pipeline 示例

**基本 Set/Get：**

```json
{
  "role": "writer",
  "pipeline": ["setStringView"],
  "notify_pipeline": ["getBuffer"]
}
```

**Buffer 写入流程：**

```json
{
  "role": "writer",
  "pipeline": ["createBuffer", "memoryCopy", "setBuffer"],
  "notify_pipeline": ["getBuffer", "exist"]
}
```

**全量测试：**

```json
{
  "role": "writer",
  "pipeline": ["setStringView", "getBuffer", "exist", "createBuffer", "memoryCopy", "setBuffer"],
  "notify_pipeline": ["getBuffer", "exist"]
}
```

**只读实例：**

```json
{
  "role": "reader",
  "pipeline": [],
  "notify_pipeline": ["getBuffer", "exist"]
}
```

## Writer/Reader 角色模式

| 角色 | 行为 |
|------|------|
| `writer` | 运行 `pipeline` 主循环 + 成功后通知 peers |
| `reader` | 不启动主循环，只等 `/notify` 触发执行 `notify_pipeline` |

Writer 每次成功执行 pipeline 后，从 `peers` 列表中随机选择 `notify_count` 个节点发送 HTTP POST `/notify`。Reader 收到通知后执行 `notify_pipeline` 中的操作序列。

## 配置说明

### config.json 字段

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `instance_id` | int | 0 | 实例唯一标识 |
| `listen_port` | int | 9000 | HTTP 服务端口 |
| `etcd_address` | string | (必填) | etcd 地址，格式 `ip:port`，不加 `http://` |
| `cluster_name` | string | `""` | 集群名，本地测试留空 |
| `host_id_env_name` | string | `""` | ServiceDiscovery 的 hostId 环境变量名 |
| `connect_timeout_ms` | int | 1000 | 连接超时(ms) |
| `request_timeout_ms` | int | 20 | 请求超时(ms) |
| `data_sizes` | string[] | `["8MB"]` | 测试数据大小列表，支持 KB/MB/GB 后缀 |
| `ttl_seconds` | int | 5 | 写入数据的 TTL(秒) |
| `target_qps` | int | 1600 | 目标总 QPS |
| `num_set_threads` | int | 16 | Writer 线程数 |
| `notify_count` | int | 10 | 每次写入后通知几个 peer |
| `metrics_interval_ms` | int | 3000 | Metrics 采集间隔(ms) |
| `metrics_file` | string | `metrics_{instance_id}.csv` | Metrics 输出文件 |
| `role` | string | `"writer"` | 角色：`writer` 或 `reader` |
| `pipeline` | string[] | `["setStringView"]` | Writer 主循环的操作序列 |
| `notify_pipeline` | string[] | `["getBuffer"]` | `/notify` 触发的操作序列 |
| `peers` | string[] | `[]` | 对等节点 URL 列表 |

## 多节点部署

### deploy.json 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `remote_work_dir` | string | 远程工作目录 |
| `binary_path` | string | 本地二进制路径 |
| `sdk_lib_dir` | string | SDK 动态库目录，默认 `third_party/sdk` |
| `ssh_user` | string | 默认 SSH 用户 |
| `ssh_options` | string | SSH 选项 |
| `nodes` | array | 节点列表 |

每个 node 支持：
- `host` — 主机名/IP (`localhost` 表示本机)
- `instance_id` — 实例 ID
- `ssh_user` — 覆盖默认 SSH 用户
- `peers` — 覆盖默认 peers 列表
- `role` / `pipeline` / `notify_pipeline` — 覆盖 config 模板中的值

### 部署命令

```bash
# 部署到所有节点并启动
python3 deploy.py --deploy config/deploy.json config/config.json.example

# 停止所有实例
python3 deploy.py --stop config/deploy.json config/config.json.example

# 清理：杀进程 + 删除远程目录
python3 deploy.py --clean config/deploy.json config/config.json.example
```

### 跨子网部署

当测试机器在不同子网时，需要 SSH 隧道桥接三个端口：

```bash
# 1. etcd 隧道 (reverse): 让远程访问本地 etcd
ssh -fNR 22379:localhost:2379 remote_host

# 2. Worker 隧道 (reverse): 让远程 KVClient 连接本地 worker
ssh -fNR 31501:localhost:31501 remote_host

# 3. HTTP 隧道 (forward): 让本地访问远程的 /notify 端口
ssh -fNL 19000:localhost:9000 remote_host
```

`test_deploy.sh` 封装了完整的跨子网测试流程：

```bash
# 默认使用 openclaw 作为远程节点
bash test_deploy.sh

# 或指定其他远程节点
REMOTE_HOST=myserver bash test_deploy.sh
```

## HTTP API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/notify` | POST | 接收写入通知，执行 notify_pipeline |
| `/stats` | GET | 获取 JSON 格式实时统计 |
| `/stop` | POST | 优雅停止实例 |

`/notify` 请求体：

```json
{
  "key": "kv_test_0_0_1682224800000000",
  "sender": 0,
  "size": 1024
}
```

## Metrics 输出

### CSV 格式 (`metrics_{id}.csv`)

```csv
timestamp,op,count,avg_ms,p90_ms,p99_ms,min_ms,max_ms,qps
2026-04-23 10:00:03.123,setStringView,30,12.3,18.5,22.1,8.1,25.3,10.0
2026-04-23 10:00:03.123,getBuffer,30,8.7,12.1,15.6,3.2,18.9,10.0
```

### Summary 文件 (`summary_{id}.txt`)

在实例停止时自动生成，包含完整运行期间的汇总统计。

### /stats JSON

```json
{
  "instance_id": 0,
  "uptime_seconds": 30,
  "setStringView_count": 150,
  "setStringView_success": 150,
  "setStringView_fail": 0,
  "getBuffer_count": 0,
  "verify_fail": 0
}
```

## 常见问题

**Q: "Invalid etcd address" 错误**

`etcd_address` 不要加 `http://` 前缀，用 `127.0.0.1:2379`。

**Q: "No available worker" 错误**

1. 确认 worker 已启动：`etcdctl get --prefix "/datasystem/cluster"`
2. 本地测试 `cluster_name` 必须设为空字符串 `""`
3. 多节点部署时需要 worker 端口的 SSH 隧道

**Q: Worker 启动失败 "domain socket len > 108"**

从短路径目录启动 worker：

```bash
mkdir -p /tmp/ds_worker && cd /tmp/ds_worker
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379
```

**Q: 编译报 "datasystem/kv_client.h: No such file"**

检查 `DATASYSTEM_SDK_DIR` 路径是否正确指向 SDK 根目录（包含 `include/` 和 `lib/` 子目录）。

**Q: 运行报 "libdatasystem.so: cannot open shared object file"**

设置 `LD_LIBRARY_PATH`：

```bash
export LD_LIBRARY_PATH=/path/to/sdk/lib:$LD_LIBRARY_PATH
```
