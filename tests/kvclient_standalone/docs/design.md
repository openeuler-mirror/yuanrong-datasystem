# kvclient_standalone_test 设计文档

> 版本: 3 | 最后更新: 2026-04-26

## 1. 概述

kvclient_standalone_test 是 datasystem KVClient 的集成测试工具，用于在真实集群环境中持续执行 KV 读写操作，收集性能指标（时延、QPS、吞吐），验证系统在长时间运行下的稳定性和性能表现。

**核心能力**:
- Writer/Reader 角色：Writer 持续写入数据并通知 Reader，Reader 接收通知后读取验证
- Pipeline 模式：可组合多种 KV 操作（set/get/exist/createBuffer/memoryCopy/setBuffer）形成测试流水线
- 性能指标：窗口指标（3s 粒度 CSV）+ 全局汇总（环形缓冲区百分位时延）
- 进程级 CPU 绑核：自动检测容器可用 CPU 或手动指定
- 远程部署：SSH/Kubectl 两种传输方式，支持批量启停和日志收集

## 2. 系统架构

```
                    ┌─────────────────────────────────────────┐
                    │            Writer 进程                   │
                    │                                         │
                    │  ┌───────────┐  ┌──────────────────┐   │
                    │  │ Pipeline  │  │ NotifyPool (100T) │   │
                    │  │ Threads   │──▶│ HTTP POST /notify │   │
                    │  │ (16T)     │  │ to random peers   │   │
                    │  └─────┬─────┘  └──────────────────┘   │
                    │        │                                │
                    │        ▼                                │
                    │  ┌───────────┐  ┌──────────────────┐   │
                    │  │ KVClient  │  │ MetricsCollector  │   │
                    │  │ (SDK)     │  │ (CSV + Summary)   │   │
                    │  └───────────┘  └──────────────────┘   │
                    │        │                                │
                    │  ┌─────┴─────┐                          │
                    │  │HttpServer │◀──── /stop, /stats,     │
                    │  │ (port)    │      /notify, /summary  │
                    │  └───────────┘                          │
                    └─────────────────────────────────────────┘
                          │ HTTP /notify
                          ▼
                    ┌─────────────────────────────────────────┐
                    │            Reader 进程                   │
                    │                                         │
                    │  ┌───────────┐  ┌──────────────────┐   │
                    │  │HttpServer │──▶│ NotifyPool (100T) │   │
                    │  │ /notify   │  │ Execute Pipeline  │   │
                    │  └───────────┘  └──────────────────┘   │
                    │        │                                │
                    │        ▼                                │
                    │  ┌───────────┐  ┌──────────────────┐   │
                    │  │ KVClient  │  │ MetricsCollector  │   │
                    │  └───────────┘  └──────────────────┘   │
                    └─────────────────────────────────────────┘
```

### 2.1 线程模型

| 线程 | 数量 | 职责 |
|------|------|------|
| Pipeline Threads | `num_set_threads`（默认16） | Writer 主循环：构造数据 → 执行 Pipeline → 通知对端 |
| NotifyPool | 100 | Writer 发送 HTTP 通知、Reader 处理通知 |
| HttpServer | 1 | 接收 /notify、/stop、/stats、/summary 请求 |
| Metrics Flush | 1 | 定时将窗口指标写入 CSV |
| Main Loop | 1 | 3s 周期打印速率和队列深度 |

### 2.2 进程生命周期

```
main()
  ├── CPU 绑核（sched_setaffinity，所有子线程继承）
  ├── ServiceDiscovery::Init() → 连接 etcd 发现 worker
  ├── KVClient::Init() → 建立 SDK 连接
  ├── MetricsCollector::Start() → 预创建 Op、启动 flush 线程
  ├── signal(SIGTERM/SIGINT) → 设置 gRunning=false
  ├── HttpServer::Start() → 监听端口
  ├── KVWorker::Start() → 启动 pipeline 线程（仅 writer）
  ├── 主循环（3s 打印监控）
  └── 关闭：Worker::Stop → HttpServer::Stop → Metrics::Stop
```

## 3. 核心模块设计

### 3.1 Pipeline 引擎

**文件**: `pipeline.h` / `pipeline.cpp`

Pipeline 是可组合的 KV 操作序列。每个操作独立计时，失败立即中断。

**支持的 Op**:

| Op | SDK 调用 | 说明 |
|----|----------|------|
| setStringView | `client->Set(key, StringView(data), param)` | 最常用，直接写入字符串数据 |
| getBuffer | `client->Get(key, Optional<Buffer>&)` | 读取并校验数据大小 |
| exist | `client->Exist({key}, exists)` | 检查 key 是否存在 |
| createBuffer | `client->Create(key, size, param, buf)` | 创建共享内存 Buffer |
| memoryCopy | `buffer->MemoryCopy(data, size)` | 向 Buffer 写入数据 |
| setBuffer | `client->Set(buffer)` | 提交 Buffer |

**执行流程**:

```
ExecutePipeline(ops, ctx, metrics)
  for each (name, fn) in ops:
    latencyMs = 0
    ok = fn(ctx, latencyMs)        ← Measure() 内计时
    metrics.Record(name, latencyMs, ok, ctx.size)
    if !ok: break                   ← 失败中断，不继续后续 Op
  return allOk
```

**PipelineContext** 携带一次操作的全部上下文：key、data、size、client、buffer、verifyFailCount。各 Op 通过引用访问，避免拷贝。

**Writer Pipeline** 由 `config.pipeline` 配置（默认 `["setStringView"]`）。

**Reader Pipeline** 由 `config.notify_pipeline` 配置（默认 `["getBuffer"]`）。

### 3.2 Writer 主循环 (KVWorker)

**文件**: `kv_worker.h` / `kv_worker.cpp`

```
PipelineLoop(threadId):
  计算 qpsPerThread = targetQps / numSetThreads（0=不限速）
  预生成随机数引擎 rng(threadId + instanceId * 1000)

  while running:
    随机选择 dataSize
    生成唯一 key: kv_test_{instanceId}_{threadId}_{timestamp}
    构造 PipelineContext（使用预生成的 pattern data）
    执行 Pipeline，记录耗时

    if 成功:
      sleep(notifyDelayMs)          ← 可配置延迟，默认 10ms
      NotifyPeers(key, size)        ← Fisher-Yates 随机选 peer

    计算 sleepMs = intervalMs - elapsedMs
    if sleepMs > 0: sleep           ← QPS 限速（0=全速）
```

**数据预生成**: 构造时按 size 预生成 pattern data（`data_pattern.h`），运行时直接引用，避免每次请求分配内存。

**QPS 限速**: 每线程 `intervalMs = 1000 / qpsPerThread`。`targetQps=0` 表示不限速全速运行。

**通知机制 (NotifyPeers)**:
- Fisher-Yates 部分洗牌从 peers 中随机选 `notifyCount` 个
- 构造 JSON body `{key, sender, size}`
- 提交到 NotifyPool（100 线程）异步发送 HTTP POST
- `thread_local` httplib::Client 缓存，复用 TCP 连接

### 3.3 HTTP 服务 (HttpServer)

**文件**: `http_server.h` / `http_server.cpp`

基于 cpp-httplib 的轻量 HTTP 服务，提供 4 个端点：

| 端点 | 方法 | 用途 |
|------|------|------|
| `/notify` | POST | Reader 接收写入通知，触发 getBuffer |
| `/stop` | POST | 优雅停止（设置 gRunning=false） |
| `/stats` | GET | 返回 JSON 格式当前计数 |
| `/summary` | POST | 触发 WriteSummary，不停止进程 |

**HandleNotify 流程**:
1. 解析 JSON body（key, sender, size）
2. 提交到 NotifyPool（100 线程）异步处理
3. Worker 线程中构造 PipelineContext
4. 若 `notifyNeedsData_=true`（pipeline 含 setStringView/memoryCopy），生成 pattern data
5. 执行 Reader Pipeline

`notifyNeedsData_` 标志：当 Reader pipeline 只含 getBuffer 时，不需要生成数据（getBuffer 从远端读取），节省 CPU。

### 3.4 指标收集 (MetricsCollector)

**文件**: `metrics.h` / `metrics.cpp`

两层指标架构：

**窗口指标**（定时 Flush 到 CSV）:
- 每 `metricsIntervalMs`（默认 3s）flush 一次
- 每个 Op 独立统计：count、avg、p90、p99、min、max、QPS、吞吐
- 通过 `windowLatencies` vector + `swap` 实现无锁写入

**全局汇总**（WriteSummary 输出到 summary_{id}.txt）:
- 环形缓冲区 `globalRing`（容量 100000 条）存储全量时延
- 支持 O(1) 写入、O(n) 读取百分位
- QPS 从 `totalCount / uptime` 计算（不受环形缓冲区容量限制）
- 百分位时延从环形缓冲区数据计算（保留最近 100000 条样本）

**线程安全设计**:
- `totalCount/successCount/failCount/totalBytes`: atomic，无锁
- `windowLatencies/windowBytes`: windowMutex 保护，swap 实现零拷贝切换
- `globalRing/globalHead/globalCount`: globalMutex 保护
- `opsMap_`: `started_` 标志实现 fast-path（Start 后不再加锁查找）

**启动流程**:
```
Start():
  ops_.reserve(N)                  ← 防止 reallocation 导致指针失效
  for each opName:
    GetOrCreateOp(opName)
    globalRing.resize(100000)      ← 预分配，避免运行时动态分配
  started_ = true                  ← 开启 fast-path
```

### 3.5 CPU 绑核

**文件**: `cpu_affinity.h`

进程级绑核，在 main() 创建任何线程之前执行：

1. 检测逻辑：`config.cpuAffinity` 非空 → `ParseCpuList` 解析；否则 → `sched_getaffinity` 自动检测
2. 调用 `sched_setaffinity` 设置进程亲和性
3. 后续所有子线程（Pipeline、NotifyPool、HttpServer）自动继承

**ParseCpuList** 支持格式：`"0-7"`、`"0,2,4,6"`、`"0-3,7,10-12"`。包含边界校验（[0, CPU_SETSIZE)）、反转范围自动交换、非法输入跳过。

### 3.6 线程池 (ThreadPool)

**文件**: `thread_pool.h`

通用线程池，用于 Writer NotifyPeers 和 Reader HandleNotify：
- 构造时创建固定数量 worker 线程
- `Submit(task)` 入队，`cv_.notify_one` 唤醒
- `Stop()` 设置停止标志，等待所有任务完成后销毁
- `QueueSize()` 返回当前队列深度（用于监控告警）

### 3.7 配置系统

**文件**: `config.h` / `config.cpp`

JSON 配置文件，支持以下字段：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| instance_id | int | 0 | 实例标识 |
| listen_port | int | 9000 | HTTP 监听端口 |
| etcd_address | string | (必填) | etcd 地址 |
| cluster_name | string | "" | 集群名称 |
| host_id_env_name | string | "JD_HOST_IP" | 主机 IP 环境变量 |
| connect_timeout_ms | int | 1000 | 连接超时 |
| request_timeout_ms | int | 20 | 请求超时 |
| data_sizes | string[] | ["8MB"] | 数据大小列表 |
| ttl_seconds | int | 5 | 数据 TTL |
| target_qps | int | 100 | 目标 QPS（0=不限） |
| num_set_threads | int | 16 | Writer 线程数 |
| notify_count | int | 10 | 通知对端数量 |
| notify_delay_ms | int | 10 | 通知延迟（ms） |
| cpu_affinity | string | "" | CPU 绑核（空=自动检测） |
| metrics_interval_ms | int | 3000 | 指标采集间隔 |
| metrics_file | string | "metrics_{instance_id}.csv" | 指标文件 |
| role | string | "writer" | 角色（writer/reader） |
| pipeline | string[] | ["setStringView"] | Writer 操作流水线 |
| notify_pipeline | string[] | ["getBuffer"] | Reader 操作流水线 |
| nodes | array | [] | 节点列表（自动生成 peers） |
| peers | string[] | [] | 对端地址列表（覆盖 nodes） |

`data_sizes` 支持单位：GB/MB/KB/B（不区分大小写）。

`peers` 自动生成：从 nodes 中排除 `instance_id == self` 的节点，生成 `http://{host}:{port}`。

### 3.8 远程停止 (StopMode)

**文件**: `stop.h` / `stop.cpp`

`--stop` 模式：并发向所有 peers 发送 HTTP POST `/stop`，等待所有响应后返回成功数。

## 4. 部署运维

### 4.1 部署架构

部署脚本 `deploy.py` 支持 SSH 和 Kubectl 两种传输方式：

```
deploy.py start <deploy.json>
  ├── 解析 deploy.json
  ├── gen_deploy_config.py → 生成每个节点的 config.json
  ├── SCP 二进制 + SDK .so + config → 每个节点
  ├── 启动进程（SSH: nohup / Kubectl: kubectl exec）
  └── procmon.py 监控进程（自动重启）

deploy.py stop <deploy.json>
  ├── HTTP POST /stop（curl/wget/python3 降级链）
  ├── 等待 5s 优雅关闭
  └── pgrep + kill -9 强制终止残留

deploy.py collect <deploy.json>
  ├── 触发每个节点 WriteSummary（POST /summary）
  └── 收集 metrics CSV + summary TXT（kubectl: exec cat 逐文件）

deploy.py clean <deploy.json>
  └── 删除远程工作目录
```

### 4.2 gen_deploy_config.py

根据 deploy.json 的 nodes 列表，自动生成每个节点的 config.json：
- 按 instance_id 排序分配角色：前 N 个为 writer，其余为 reader
- Writer 轮询分配到各节点（节点内可能多个 writer）
- 自动生成 peers 列表（所有其他节点的 HTTP 地址）

### 4.3 进程监控 (procmon.py)

可选的进程看门狗：
- 定期检查进程是否存活（pgrep）
- 进程退出时自动重启
- 支持 `--pid` 参数直接指定 PID

### 4.4 输出文件

| 文件 | 生成时机 | 内容 |
|------|----------|------|
| metrics_{id}.csv | 每 3s 追加 | 窗口指标：时间戳、Op、计数、avg/p90/p99/min/max、QPS、吞吐 |
| summary_{id}.txt | 停止时或 POST /summary | 全局汇总：uptime、每个 Op 的总计数/成功率/百分位时延/QPS/吞吐 |
| procmon.log | procmon 运行中 | 进程监控日志 |

## 5. 关键设计决策

| 决策 | 原因 |
|------|------|
| 进程级绑核而非线程级 | 一次 sched_setaffinity，所有子线程自动继承，实现简单 |
| 环形缓冲区 100000 条 | 覆盖 ~27 分钟 @100 QPS 的百分位时延，内存可控 |
| NotifyPool 100 线程 | Reader 需要并发处理大量通知（10 个 Writer × 100 QPS = 1000 通知/s） |
| thread_local httplib::Client | 复用 TCP 连接，避免每次通知创建新连接 |
| 数据预生成 | 避免 8MB 数据在热循环中反复分配 |
| Fisher-Yates 部分洗牌 | O(k) 随机选 k 个 peer，不拷贝 peers 列表 |
| QPS 从 totalCount/uptime 算 | 不受环形缓冲区容量限制，长时间运行统计准确 |

## 6. 构建与打包

```
# 编译（需要 SDK 路径）
cmake -B build -DDATASYSTEM_SDK_DIR=/path/to/sdk
cmake --build build

# 打包（二进制 + SDK .so + 脚本 + 配置）
make copy-sdk  # 从 Bazel 输出复制 SDK
make package   # 打包到 output/
```

构建系统通过 VERSION 文件嵌入版本号（`-DBUILD_VERSION`），二进制启动时打印。
静态链接 libstdc++ 和 libgcc，兼容低版本 GCC 的目标系统。

## 7. 文件清单

| 文件 | 行数 | 职责 |
|------|------|------|
| src/main.cpp | 184 | 入口：绑核、初始化、监控循环、关闭 |
| src/kv_worker.cpp | 158 | Writer：Pipeline 主循环、通知 |
| src/kv_worker.h | 39 | KVWorker 类定义 |
| src/http_server.cpp | 98 | HTTP 服务：4 个端点 |
| src/http_server.h | 36 | HttpServer 类定义 |
| src/pipeline.cpp | 139 | Pipeline：Op 注册、执行引擎 |
| src/pipeline.h | 46 | PipelineContext、OpFunc 定义 |
| src/metrics.cpp | 228 | 指标：窗口 CSV + 全局汇总 |
| src/metrics.h | 65 | OpMetrics、MetricsCollector 定义 |
| src/config.cpp | 137 | JSON 配置解析 |
| src/config.h | 42 | Config 结构体定义 |
| src/thread_pool.h | 66 | 通用线程池 |
| src/cpu_affinity.h | 54 | CPU 绑核工具函数 |
| src/data_pattern.h | 19 | 数据 pattern 生成与校验 |
| src/simple_log.h | 11 | 简单日志宏（避免 spdlog 冲突） |
| src/stop.cpp | 52 | 远程停止 |
| src/httplib.h | 9370 | cpp-httplib 单文件库 |
| CMakeLists.txt | 41 | 构建配置 |
| Makefile | 41 | 打包配置 |
| deploy.py | ~400 | 部署脚本 |
| gen_deploy_config.py | ~200 | 配置生成 |
| procmon.py | ~100 | 进程监控 |
