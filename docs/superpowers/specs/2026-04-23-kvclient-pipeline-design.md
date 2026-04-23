# KVClient Standalone Test - Pipeline & Role Mode Design Spec

## Context

在现有 kvclient_standalone_test 工具基础上扩展 4 个新能力：
1. notify_count 已可配置（无需改动）
2. 支持读写角色分离（writer/reader）
3. 新增 Create、MemoryCopy、Set(Buffer)、Exist 四个 SDK 接口测试
4. 通过配置字符串定义操作组合 pipeline

## Directory Structure (新增/修改文件)

```
tests/kvclient_standalone/
├── pipeline.h / pipeline.cpp       # 新增：Pipeline 执行引擎 + 6 个原子操作
├── config.h / config.cpp           # 修改：新增 role, pipeline, notify_pipeline 字段
├── kv_worker.h / kv_worker.cpp     # 修改：SetLoop 改为 PipelineLoop
├── http_server.h / http_server.cpp # 修改：/notify 使用 notify_pipeline
├── main.cpp                        # 修改：根据 role 决定是否启动 KVWorker
├── metrics.h / metrics.cpp         # 修改：操作名从 "set"/"get" 改为 pipeline op 名
├── deploy.sh                       # 修改：支持节点级 role/pipeline 覆盖
└── config.json.example             # 修改：新增字段示例
```

## Configuration

### config.json 新增字段

```json
{
  "role": "writer",
  "pipeline": ["setStringView"],
  "notify_pipeline": ["getBuffer", "exist"]
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| role | string | "writer" | `writer`=运行主循环+发通知, `reader`=只等通知执行 notify_pipeline |
| pipeline | string[] | ["setStringView"] | writer 主循环的操作序列 |
| notify_pipeline | string[] | ["getBuffer"] | 被 /notify 触发时执行的操作序列 |

### deploy.json 节点级覆盖

```json
{
  "nodes": [
    { "host": "node1", "instance_id": 0, "role": "writer", "pipeline": ["setStringView"] },
    { "host": "node2", "instance_id": 1, "role": "reader" },
    { "host": "node3", "instance_id": 2, "role": "writer", "pipeline": ["createBuffer", "memoryCopy", "setBuffer"] }
  ]
}
```

- 未指定 role 的节点默认为 "writer"
- 未指定 pipeline/notify_pipeline 的节点使用 config 模板中的默认值
- deploy.sh 生成每节点配置时注入这些字段

## Pipeline Operations

### 6 个原子操作

| Name | SDK Call | Description |
|------|----------|-------------|
| setStringView | `client->Set(key, StringView(data), param)` | 简单 Set，data 由 GeneratePatternData 生成 |
| getBuffer | `client->Get(key, Optional<Buffer>&, subTimeoutMs)` | Get 到 Buffer，读回数据验证 |
| exist | `client->Exist({key}, exists)` | 检查 key 是否存在 |
| createBuffer | `client->Create(key, size, param, buffer)` | 创建共享内存 Buffer，结果存入 PipelineContext |
| memoryCopy | `buffer->MemoryCopy(data, size)` | 向 Create 返回的 Buffer 写入数据 |
| setBuffer | `client->Set(buffer)` | 提交 Buffer 到 datasystem |

### PipelineContext

Pipeline 操作间通过上下文传递数据：

```cpp
struct PipelineContext {
    std::string key;
    std::string data;           // GeneratePatternData 生成
    uint64_t size;
    int senderId;
    std::shared_ptr<datasystem::Buffer> buffer;  // Create 返回，MemoryCopy/Set 使用
    datasystem::SetParam param;                   // SetParam (writeMode, ttl)
    std::shared_ptr<datasystem::KVClient> client; // KVClient 实例
};
```

### 操作执行

每个操作统一签名：`bool op(PipelineContext& ctx, double& latencyMs)`

返回 true=成功，false=失败。latencyMs 由操作内部测量。

## Pipeline Execution

### Writer 主循环

```
while (running):
    ctx.key = "kv_test_{instanceId}_{threadId}_{timestamp}"
    ctx.size = data_sizes[rand()]
    ctx.data = GeneratePatternData(ctx.size, instanceId)
    ctx.buffer = nullptr

    bool allOk = true
    for op in pipeline:
        ok, latency = op(ctx)
        metrics.Record(op.name, latency, ok)
        if !ok:
            allOk = false
            break  // pipeline 中某个操作失败则停止后续操作

    if allOk:
        NotifyPeers(ctx.key, ctx.size)

    sleep(interval - elapsed)
```

### Reader 模式

reader 角色不启动 KVWorker 线程，只启动 HTTP Server 等待 /notify。

### /notify Handler

```
POST /notify:
    parse body: key, sender, size

    ctx.key = key
    ctx.size = size
    ctx.senderId = sender
    ctx.data = GeneratePatternData(size, sender)
    ctx.buffer = nullptr

    for op in notify_pipeline:
        ok, latency = op(ctx)
        metrics.Record(op.name, latency, ok)
```

### /notify Request Body

```json
{
  "key": "kv_test_0_0_1682224800000000",
  "sender": 0,
  "size": 1024
}
```

不变，与现有格式兼容。

## Verification

| 操作 | 验证逻辑 |
|------|---------|
| setStringView | 只记录成功/失败 |
| getBuffer | 成功后验证 `buffer.GetSize() == ctx.size`，然后按 sender+size 重新生成 pattern 比对内容 |
| exist | 验证 `exists[0] == true`（写后 key 应存在），否则记录 verify_fail |
| createBuffer | 只记录成功/失败 |
| memoryCopy | 只记录成功/失败 |
| setBuffer | 只记录成功/失败 |

getBuffer 验证细节：
```cpp
// getBuffer 成功后
auto& buf = optionalBuffer.Value();
if (buf.GetSize() != (int64_t)ctx.size) {
    metrics.RecordVerifyFail();
    return true;  // 操作本身成功，但验证失败
}
// 比对内容：从 Buffer 读取数据与 expected pattern 比较
std::string expected = GeneratePatternData(ctx.size, ctx.senderId);
std::string actual(buf.GetData(), buf.GetSize());
if (actual != expected) {
    metrics.RecordVerifyFail();
}
```

## Metrics

### 操作名

Metrics 桶名称与操作名一致：`setStringView`, `getBuffer`, `exist`, `createBuffer`, `memoryCopy`, `setBuffer`。

metrics 系统预创建所有 6 个桶，避免运行时动态创建的竞态。

### CSV 输出

```
timestamp,op,count,avg_ms,p90_ms,p99_ms,min_ms,max_ms,qps
2026-04-23 10:00:03.123,setStringView,30,12.3,18.5,22.1,8.1,25.3,10.0
2026-04-23 10:00:03.123,getBuffer,30,8.7,12.1,15.6,3.2,18.9,10.0
2026-04-23 10:00:03.123,exist,30,1.2,2.0,3.1,0.5,4.2,10.0
```

### Summary Report

```
--- setStringView ---
Total: 12340, Success: 12300, Fail: 40
Avg: 12.1ms, P90: 18.3ms, P99: 22.5ms, Min: 7.8ms, Max: 45.2ms

--- getBuffer ---
Total: 5678, Success: 5670, Fail: 8
Avg: 8.5ms, P90: 12.0ms, P99: 15.8ms, Min: 3.1ms, Max: 32.1ms
Verify Fail: 2
```

## Main Flow

### Initialization Flow (修改)

1. 加载 config.json
2. 创建 ServiceDiscovery → Init
3. 创建 ConnectOptions + KVClient → Init
4. 启动 MetricsCollector
5. 启动 HttpServer
6. **if role == "writer":** 启动 KVWorker（pipeline 循环）
7. **if role == "reader":** 不启动 KVWorker，仅等待通知
8. 等待 /stop 或 SIGTERM

### Graceful Shutdown

与现有逻辑相同。reader 角色直接跳过 worker.Stop()。

## deploy.sh 修改

1. deploy.json 的 nodes 中支持 `role`、`pipeline`、`notify_pipeline` 字段
2. 生成每节点配置时，将节点级覆盖字段合并到 config 中
3. 其他逻辑（scp、nohup、SDK 部署）不变

## Pipeline 配置示例

### 示例 1: 基本 Set/Get

writer pipeline: `["setStringView"]`
notify_pipeline: `["getBuffer"]`

等价于当前行为。

### 示例 2: Buffer 写入流程

writer pipeline: `["createBuffer", "memoryCopy", "setBuffer"]`
notify_pipeline: `["getBuffer", "exist"]`

writer 通过 Create→MemoryCopy→Set 写入数据，reader 通过 Get+Exist 验证。

### 示例 3: 全量测试

writer pipeline: `["setStringView", "getBuffer", "exist", "createBuffer", "memoryCopy", "setBuffer"]`
notify_pipeline: `["getBuffer", "exist"]`

writer 先 SetStringView，再 Get 验证，Exist 检查，然后 Create→MemoryCopy→SetBuffer 覆写。

### 示例 4: 只读实例

role: `"reader"`
notify_pipeline: `["getBuffer", "exist"]`

不写数据，只被动接收通知执行 Get+Exist。
