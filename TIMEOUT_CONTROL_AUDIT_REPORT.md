# 超时控制(deadline)审计与实测报告

> 范围:datasystem 读写全流程(SDK↔worker、worker→master、worker→worker)是否受请求级 deadline(`requestTimeoutMs` / `ApiDeadline` / `reqTimeoutDuration`)控制
> 结论基准:代码静态审计 + 编译机(compile_32c_01)单机/双 worker 实测注入
> 分支:`worktree-requests-timeout-analysis`(commit `d83fc9a0`,带诊断字段 + 阶段 sleep flag 的 wheel 0.9.1)
> 日期:2026-07-21

---

## 一、TL;DR(结论速览)

| # | 路径 | 受 deadline 控制? | 证据级别 |
|---|------|:---:|---|
| 1 | SDK→worker 的 brpc Get/Publish RPC **CallMethod 内部** | ✅ 受控 | 实测(20ms 精确截断,`cntl_error_code=1008`) |
| 2 | worker 服务端 Get 各阶段(get_begin / local_get / query_meta)慢 | ✅ 受控(端到端) | 实测(注入 50ms → SDK 20ms 截断) |
| 3 | **SDK 侧 CallMethod 之外**的阻塞(发 RPC 前/后) | ❌ **不受控** | 实测(注入 50ms → 51ms 返回 OK,deadline 不截断) |
| 4 | **AsyncRead `cv.wait(lock)` 无超时**(流式/异步路径) | ❌ **不受控** | 代码审计(`brpc_stub_generator.cpp:655-659`) |
| 5 | TCP 建连阶段 `connect_timeout_ms` | ❌ 不受 `set_timeout_ms` 管 | brpc 语义(独立计时) |
| 6 | streaming RPC Read(`wait_until` 固定 30s) / StreamClose(5s/7200s) | ❌ 不受控 | 代码审计 |
| 7 | worker 内部 `TryLockWithRetry` 固定 sleep 191ms | ⚠️ worker 内部不查 deadline,但端到端被 #2 兜住 | 代码审计 + 逻辑推断 |
| 8 | RetryOnError 应用层重试 sleep 序列 {1,5,50,200,1000,5000}ms | ⚠️ 有 clamp,不响应中途 deadline | 代码审计 |
| 9 | 原始故障 144ms(SHM 数据面,CallMethod 内,worker 1.2ms) | ❓ 未复现 | 单机达不到,需原始多机高压环境 |

**核心结论**:
- brpc `set_timeout_ms(N)` **只管 `CallMethod` 内部**(post-connect)。CallMethod 内部可靠截断(实测:服务端慢→20ms 截断)。
- **真正不受 deadline 控制的有两类**:
  - (a) **SDK 侧 CallMethod 之外的同步阻塞**(SHM futex / URMA wait / 条件变量等)——实测证实:CallMethod 外 sleep 50ms,deadline 20ms 完全不截断,51ms 返回 OK。这是最接近"deadline 失效"的实测铁证。
  - (b) **AsyncRead 的 `cv.wait(lock)` 无超时**(生成代码)——可永久阻塞,brpc 的 set_timeout_ms 救不了它。
- 原始 144ms 故障在 brpc CallMethod **内部**(`e2e_us=144075`),但单机实测无法复现(最慢 37ms,且 >20ms 全被截断)。需原始多机环境(16 SDK/qps35/jitter/跨物理节点)复现后用诊断字段定位。

---

## 二、关键纠正(对前期分析的修正)

### 纠正 1:144ms 用例数据面走 SHM,不是 payload

前期归因"客户端收 8MB attachment 慢导致 deadline 不截断"是**错误的**。

实测证据(跨节点 SHM Get,8MB 对象):
```
method=datasystem.WorkerOCService.Get
e2e_us=15753  cntl_timeout_ms=20  cntl_error_code=0  cntl_failed=0  resp_attachment_bytes=56
```
`resp_attachment_bytes=56` —— 8MB 对象的 brpc Get 响应只有 **56 字节**(元数据级)。数据经 worker 间传输 + SDK↔本地 worker 走 SHM,**brpc Get RPC 不含大 payload**。

→ "收大 attachment 慢"归因推翻。144ms 不是收数据慢。

### 纠正 2:worker 服务端各阶段实际受 deadline 控制

前期一度怀疑 worker 阶段不受控,实测推翻——**worker 服务端任一阶段变慢,SDK 的 20ms deadline 精确截断**:
```
worker 注入 local_get sleep 50ms → SDK Get: e2e≈20095us, cntl_error_code=1008, cntl_failed=1(超时截断)
worker 注入 get_begin  sleep 50ms → SDK Get: e2e≈20100us, cntl_error_code=1008, cntl_failed=1(超时截断)
```
worker 端 `[DEBUG-SLEEP]` 日志确认 sleep 真实执行(22 次 / 6 次)。

---

## 三、实测设计与方法

### 3.1 诊断能力(已合入 wheel)

在 `BRPC_RPC_FRAMEWORK_SLOW` 日志新增 5 个字段(`brpc_perf_trace.h`):
- `cntl_timeout_ms` —— brpc controller 实际设的 timeout(验证"设了没")
- `cntl_deadline_us` —— brpc 算出的绝对 deadline(验证"算了没")
- `cntl_error_code` —— brpc 返回码(1008=超时,0=成功)
- `cntl_failed` —— call 是否失败
- `resp_attachment_bytes` —— 响应 attachment 大小(区分 SHM 元数据 vs payload)

### 3.2 阶段注入能力(临时 debug flag,验证后应删除)

**worker 侧**(`worker_oc_service_get_impl.cpp`):
- `debug_get_stage_sleep_ms` / `debug_get_stage_sleep_at`(`get_begin`|`query_meta`|`local_get`|`remote_get`)
- 在 Get 服务端 4 阶段注入可控 sleep

**SDK 侧**(`client_worker_remote_api.cpp`):
- `debug_sdk_stage_sleep_ms` / `debug_sdk_stage_sleep_at`(`get_pre_rpc`|`get_post_rpc`|...)
- 在 `DS_OC_DISPATCH(Get)` 前后注入 sleep(通过环境变量 `DATASYSTEM_DEBUG_SDK_STAGE_SLEEP_MS/_AT`)

### 3.3 实验矩阵

| 实验 | 注入位置 | SDK req | 预期(若受控) | 实测结果 |
|------|---------|---------|------------|---------|
| W-local_get | worker local_get | 20ms | ~20ms 超时 | ✅ e2e≈20ms, err=1008, failed=1 |
| W-get_begin | worker get_begin | 20ms | ~20ms 超时 | ✅ e2e≈20ms, err=1008, failed=1 |
| W-query_meta | worker query_meta | 20ms | ~20ms 超时 | ✅ e2e≈20ms(未走到 query_meta 就超时) |
| SDK-get_pre_rpc | SDK CallMethod 前 | 20ms | ~20ms 超时 | ❌ **51ms 返回 OK,deadline 不截断** |
| SDK-get_post_rpc | SDK CallMethod 后 | 20ms | ~20ms 超时 | ❌ **51ms 返回 OK,deadline 不截断** |
| 跨节点SHM基线 | 无注入 | 2000ms | 正常 | e2e 0.3-37ms, err=0, bytes=56 |
| 高压并发 | 无注入 | 20ms | 截断 | 280 样本,>20ms 全部 err=1008 截断,无 failed=0 且 >20ms 的 |

---

## 四、逐路径审计(受控 / 不受控判定)

### 4.1 SDK→worker 数据读写(Get/Set/Create/Publish/Delete)【受控】

**代码事实**(`client_worker_remote_api.cpp`):
- 每个读写入口都被 `ApiDeadlineGuard(requestTimeoutMs_)` 包裹(`object_client_impl.cpp:3614` 等),建立请求级 deadline 预算。
- `DS_OC_DISPATCH(Get/Publish/...)` 前,**所有** `RpcOptions opts;` 都紧跟 `opts.SetTimeout(realRpcTimeout)`(行 381/437/469/580/646/679/744/929/982...),`realRpcTimeout` 来自 `ApiDeadline::ApiRemainingUs()`。
- 生成 stub(`brpc_stub_generator.cpp:362-368`):`cntl.set_timeout_ms(overrideMs)` 用 `RpcOptions` 值覆盖 stub 构造值。
- 被 `RetryOnError` 包裹,总预算 = ApiDeadline。

**实测**:Get 路径 `cntl_timeout_ms=20`(=requestTimeoutMs),服务端慢→20ms 截断。✅

> 注意:`subTimeoutMs`(Get 第二参数)**只影响 worker 内部 sub-timeout 语义,不影响 SDK↔worker 这段 brpc per-call timeout**。per-call timeout 由 `requestTimeoutMs_`(连接级)决定。这是常见误解。

### 4.2 worker 服务端 Get 各阶段【端到端受控,但 worker 内部有不受控 sleep】

**代码事实**:
- worker 从 brpc wire 的 `cntl->timeout_ms()` 初始化 `reqTimeoutDuration`(`brpc_service_generator.cpp` `BuildScTimeoutDurationInitSnippet`,依赖 `baidu_std_protocol_deliver_timeout_ms=true`,已修)。
- 各阶段有 `CalcRealRemainingTimeUs()` / `CheckApiDeadline()` 检查点。

**实测**:注入 worker 任一阶段 sleep 50ms,SDK 端 20ms 截断。✅(端到端受控)

**但 worker 内部存在不查 deadline 的固定 sleep**:
- `object_kv.cpp:122-135` `TryLockWithRetry`:固定 `{1,10,30,50,100}`ms 累加最多 191ms,不查 ApiDeadline。**worker 内部不受控**,但端到端被 SDK brpc deadline 兜住(见 #2)。

### 4.3 SDK 侧 CallMethod 之外的阻塞【❌ 不受控 — 实测铁证】

**实测**:
- `get_pre_rpc`(CallMethod 前)sleep 50ms,req=20ms → Get 51ms 返回 **OK**,brpc CallMethod 内 e2e 仅 272µs。
- `get_post_rpc`(CallMethod 后)sleep 50ms,req=20ms → Get 51ms 返回 **OK**,brpc e2e 仅 207µs。

**机理**:brpc `set_timeout_ms` 只在 `CallMethod` 内部注册 deadline 定时器并检查。CallMethod 之前/之后的 SDK 同步代码(SHM futex、URMA wait、condition_variable、RetryOnError 间隙的 sleep)**不归 brpc deadline 管**。`ApiDeadline` 预算理论上覆盖整条请求,但只有显式调用 `CheckApiDeadline()` 的点才会响应——CallMethod 外的 sleep 路径没有这种检查点。

> 这与原始 144ms 的关系:原始 144ms 的 `e2e_us=144075` 在 brpc CallMethod **内部**,所以不是这个"CallMethod 外"类别。但本类别是真实存在的不受控漏洞,值得单独修复。

### 4.4 AsyncRead `cv.wait` 无超时【❌ 不受控 — 代码审计】

`brpc_stub_generator.cpp:655-659` 生成的异步读路径:
```cpp
while (!call->completed) { call->cv.wait(lock); }   // 无 wait_for/wait_until
```
若 brpc Done 闭包未触发(对端 crash / 网络分区 / brpc 内部 bug),调用 bthread **永久阻塞**。brpc 的 `set_timeout_ms` 只在 brpc 内部 `SetFailed`,但 `OnRpcDone` 是否被回调取决于 brpc 是否真回调 Done;若 Done 路径泄漏,cv 永不 notify。5 分钟 TTL 清理只在 `ForgetCall` 路径触发,AsyncRead 阻塞后无人调 ForgetRequest。

> 影响面:异步/流式 RPC 路径。KV Get 主路径走同步 unary stub(`BuildBrpcStubNoStreamImpl`),不受此影响。

### 4.5 TCP 建连阶段 `connect_timeout_ms`【❌ 不受 set_timeout_ms 管 — brpc 语义】

`brpc_factory.cpp:59` 设 `connect_timeout_ms=1000`(默认)。POOLED 连接复用时通常已有连接,但首次建连 / 断线重连 / 池空时,建连最多 1000ms **不被 `set_timeout_ms` 截断**。总耗时最坏 = connect_timeout_ms + timeout_ms = 1020ms。

> 这能解释部分"首请求慢"但不是 144ms(144ms 在已建连的 CallMethod 内)。

### 4.6 streaming RPC / StreamClose【❌ 不受控 — 代码审计】

- `brpc_client_stream_writer_reader.h:239-242`:stream Read 用固定 `wait_until(30s)`,与 ApiDeadline 无关。
- `brpc_stream_close_helper.cpp:51-54`:StreamCloseAndWait 用固定 5s / deferred 7200s。

> 影响面:stream_cache 路径,不是 KV Get 主路径。

### 4.7 RetryOnError 应用层重试【⚠️ 设计性,有 clamp】

`rpc_util.h:103-148`:`retryIntervalsMs={1,5,50,200,1000,5000}`。单次 call 截断到剩余预算,但失败后 sleep 序列有 clamp(`HandleRetryTime:70`)。sleep 不响应中途 deadline(到点才 check)。在 20ms 预算下基本只跑 1 次 + 1ms sleep;在更大预算下 sleep 累加可能接近总预算。边界条件(remainTime<minOnce 时 retryInterval 为负)有 UB 风险但 std 库实际当 0 处理。

---

## 五、原始 144ms 故障分析

### 5.1 已确认事实

- SDK access log:`transportType:SHM`,`client.rpc.get:144103`(SDK→worker brpc Get RPC 耗 144ms)
- worker log:`[Get] Done, transferPath: UB, totalCost: 1.277ms`(worker 服务端 1.277ms 完成)
- SDK brpc trace:`e2e_us=144075`(CallMethod 内),`server_exec_us=1249`(服务端 1.2ms),`network_residual_us=142819`(残差 142ms)

### 5.2 已排除的解释

| 解释 | 排除依据 |
|------|---------|
| 收 8MB attachment 慢 | 实测 SHM 数据面 `resp_attachment_bytes=56`,brpc Get 不含大 payload |
| deadline 没设上 | 待原始环境用诊断字段确认(单机实测 Get 路径 `cntl_timeout_ms=20` 正常,但原始环境未测) |
| 服务端处理慢 | worker `totalCost=1.277ms`,server_exec=1249µs |
| worker 阶段不受控 | 实测 worker 各阶段慢→20ms 截断(端到端受控) |

### 5.3 未排除的可能(需原始环境复现)

144ms 在 brpc CallMethod **内部**,worker 1.2ms 发完响应,SDK 却 142ms 才"收到"且 `cntl_failed=0`。单机最慢 37ms 且全被截断,复现不了。可能方向:
1. **brpc POOLED 连接池在高并发下的连接获取/复用排队**(CallMethod 内含获取连接,若池忙可能卡,但 deadline 是否覆盖此段需 brpc 源码确认)
2. **SDK brpc IO 线程被 SHM futex/通知等待阻塞**,已到达的响应迟迟不被处理(CallMethod 内的回调路径)
3. **某种 brpc backup_request / 连接级重试在 20ms 后悄悄发起第二次 call**(但 `max_retry=0, backup_request_ms=0` 在当前 factory 配置;原始环境配置未知)

### 5.4 复现所需的下一步

在**原始多机环境**(16 SDK / qps35 / jitter / 跨物理节点 + urma/UB)用带诊断字段的 wheel 0.9.1 重跑,抓 144ms 那条的:
- `cntl_timeout_ms`(应为 20,确认 deadline 设了)
- `cntl_deadline_us`(应 >0,确认算了)
- `cntl_error_code` / `cntl_failed`(0/0 = deadline 不截断的铁证)
- `resp_attachment_bytes`(确认 SHM 小响应)

这 5 个值能直接区分"deadline 设了但不截断"vs"deadline 没设上"。

---

## 六、修复建议(按优先级)与落地状态

> 落地状态截至 commit(worktree `requests-timeout-analysis`):P0/P1 已落地 + 编译验证通过,P2/P3 follow-up,P4 经源码复核无需做,144ms 待原始环境复现。

### P0 — SDK 侧 CallMethod 外阻塞不受控(实测已证)【✅ 已落地】
- **落点**:`src/datasystem/common/util/rpc_util.h` 的 `RetryOnError` 模板,func 调用前加 `RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline())`(一处改动覆盖全部 31 处 `DS_OC_DISPATCH` 及 worker/stream_cache 的 RetryOnError 调用方)。
- **为何模板层**:31 处入口逐个补不现实;模板层一处覆盖。未初始化时 `ApiRemainingUs()` 返回 `RPC_TIMEOUT`(60s),`CheckApiDeadline` 返 OK,后台 fan-out 线程不误杀;已初始化且快过期则截断(deadline 该生效的正确语义)。`K_RPC_DEADLINE_EXCEEDED` 在 `RETRY_ERROR_CODE`,但 deadline 真过期时 `remainTimeMs<=minOnceRpcTimeoutMs` → break,不死循环。
- **编译验证**:`-fsyntax-only` 通过(compile_32c_06)。
- 影响面:SDK 侧任何 CallMethod 外的同步等待(SHM futex、URMA、cv)。

### P1 — AsyncRead `cv.wait` 无超时(代码审计)【✅ 已落地】
- **落点**:`src/datasystem/common/rpc/plugin_generator/brpc_stub_generator.cpp`:
  - `GenerateBrpcStubPrologue` 生成 include 加 `api_deadline.h`(生成 stub 编译单元才能访问 `ApiDeadline`)。
  - `BuildAsyncReadImpl`(L655-678)`cv.wait(lock)` → `cv.wait_for(lock, remainingUs)` + 超时返回 `K_RPC_DEADLINE_EXCEEDED`,含 spurious wake-up 重算剩余预算。
  - `bthread::ConditionVariable::wait_for(lock, long)` 返回 `int`(`ETIMEDOUT`/0),与 `ETIMEDOUT` 比较——与既有 `brpc_stream_close_helper.cpp:54`/`brpc_client_stream_writer_reader.h:242` 同模式。
- **影响面**:仅 unary async RPC 的 AsyncRead 阻塞分支。KV Get 主路径走 sync unary stub(`DS_OC_DISPATCH(Get, opts, req, rsp, payloads)` 4 参),**不受影响**。stream 路径走独立 `ImplementBrpcStubStreamingDef`,不受影响。活路径调用方:`remote_worker_manager.cpp` 的 `PushElementsCursorsAsyncRead(..., RpcRecvFlags::NONE)`(原靠 brpc `set_timeout_ms(10s)` 兜底,现受 ApiDeadline 管)。
- **编译验证**:生成器本身 `-fsyntax-only` 通过;生成产物片段(`wait_for`/`ETIMEDOUT`/`ApiDeadline` 在生成 stub include 上下文)`-fsyntax-only` 通过(compile_32c_06)。

### P2 — TryLockWithRetry 固定 191ms sleep 不查 deadline【✅ 已落地】
- **落点**:`src/datasystem/worker/object_cache/object_kv.cpp:118-157` `TryLockWithRetry` 循环每轮加 `ApiDeadline::Instance().CheckApiDeadline()` 早退 K_RPC_DEADLINE_EXCEEDED(`RETURN_IF_NOT_OK_PRINT_ERROR_MSG`)+ 单轮 sleep 限到 `min(t, apiRemainingMs)`。budget 耗尽后 break。保留原有 LOG(INFO) 日志。依赖 include `api_deadline.h` + `timeout_duration.h`(均新增)。
- **编译验证**:`object_kv.cpp` `-fsyntax-only` 通过(compile_32c_06)。运行时未独立复现(需 mock SafeObjType,按约定 T13 难可跳过)。
- **未初始化不误杀**:`CheckApiDeadline`/`ApiRemainingUs` 在后台线程返 OK/60s,行为同现状(由 api_deadline.h 契约 + T12 同源运行时证据)。
- 影响面:worker 锁竞争场景不再固定 191ms,受请求级 deadline 管。端到端本就被 SDK 兜,worker 内部不再浪费资源。

### P3 — streaming Read/StreamClose 固定 30s/5s【❌ 不做(语义合理)】
- `brpc_client_stream_writer_reader.h:239`、`brpc_stream_close_helper.cpp:51`:用 ApiRemainingUs 替代固定值。
- **不做原因**:stream 是长连接,固定流级超时(30s/5s/7200s)语义合理,改 ApiRemainingUs 会在请求级 deadline 过期后过早杀流;且这些只在 `brpc_*_stream_impl.h` 流式 RPC 路径,不在 KV Get sync 主路径。

### P4 — connect_timeout_ms 与 timeout_ms 解耦【❌ 无需做(现状已处理)】
- 经源码复核:`RetryOnError` 的 `HandleRetryTime`(`rpc_util.h:70`)已把 `retryInterval` clamp 到 `remainTime - minOnceRpcTimeoutMs`,且循环前有 `if (remainTimeMs <= minOnceRpcTimeoutMs) break;` 守护。原审计"sleep 不响应中途 deadline / 负值 UB"已被现状代码处理。

### 待定 — 原始 144ms【❓ 待原始环境复现】
- 需原始环境诊断字段值才能定方案。若 `cntl_timeout_ms=20` 且 `cntl_failed=0`,则确认是 brpc CallMethod 内某段 deadline 不截断,需深入 brpc 1.15.0 源码定位。
- 诊断能力(`brpc_perf_trace.h` 5 字段)**已保留**,供复现用。

---

## 七、临时 debug flag 清理清单【✅ 已清理】

验证用临时改动(commit `2737474c`→`d83fc9a0`),已随 P0/P1 落地一并清理:
- `src/datasystem/common/rpc/brpc_perf_trace.h` 的 5 个诊断字段 —— **保留**(生产无开销,只在 BRPC_RPC_FRAMEWORK_SLOW 慢日志输出,有诊断价值,供 144ms 原始环境复现用)
- `src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.cpp` 的 `debug_get_stage_sleep_*` flag + 注释块 + `DebugSleepAtStage` helper(含匿名 namespace)+ 4 个注入点(`get_begin`/`local_get`/`remote_get`/`query_meta`)—— **已删除**
- `src/datasystem/client/object_cache/client_worker_api/client_worker_remote_api.cpp` 的 `debug_sdk_stage_sleep_*` flag + 注释块 + `DebugSdkSleepAtStage` helper + 2 个注入点(`get_pre_rpc`/`get_post_rpc`)+ 因删除而 unused 的 `<chrono>`/`<thread>` include —— **已删除**
- 清理后两文件 `-fsyntax-only` 通过(compile_32c_06)。

---

## 八、附录:实验原始数据摘录

### A. worker local_get 注入(受控)
```
SDK Get trace:
  e2e=20095us timeout=20ms err=1008 failed=1 bytes=0
  e2e=20097us timeout=20ms err=1008 failed=1 bytes=0
  (6 个 Get 全部 20ms 截断)
worker: [DEBUG-SLEEP] inject 50ms at stage=local_get (22 次)
```

### B. SDK get_pre_rpc 注入(不受控)
```
get si_0 OK 51ms   ← deadline 20ms 不截断
get si_1 OK 51ms
get si_0 OK 101ms  ← RetryOnError 重试 50+50
SDK brpc e2e: 272us err=0 failed=0 bytes=56  ← CallMethod 内仅 272µs
```

### C. 跨节点 SHM Get 基线(bytes=56 证伪 attachment 归因)
```
e2e=37717us timeout=2000ms err=0 failed=0 bytes=56
e2e=29828us timeout=2000ms err=0 failed=0 bytes=56
e2e=22386us timeout=2000ms err=0 failed=0 bytes=56
```

### D. 高压并发(20ms,无不受控样本)
```
n=280 min=173us p50=272us p99=20176us max=20279us
所有 >20ms 样本: err=1008 failed=1(全截断)
无 failed=0 且 >20ms 的样本
```

---

## 九、限制不住场景的测试设计与修复方案(2026-07-22 补充)

> 针对"二、限制不住的场景"逐个设计测试用例。能单机跑的(T8/T11/T12/T13)已用独立验证程序在 compile_32c_06 实锤(链 marck 预编译 brpc/datasystem .so);需多机/特殊环境的(T9/T10/T14)给精确复现配方。T12/T13/T14 给修复方案。
> 注:此节重新按当前源码 + 实测校准,**T8 的判断被实测推翻**(原判"不受控",实测"受 20ms 限")。

### T8 — TCP 建连阶段 `connect_timeout_ms`

**实测(compile_32c_06,brpc 原生 channel,链 marck 的 libbrpc.so)**:
- 测1 `set_timeout_ms=20, connect_timeout_ms=1000`,连 blackhole IP(192.0.2.1):
  `e2e=20ms, ErrorCode=1008(超时)` → **RPC 被 set_timeout_ms(20) 截断,connect 含在内。受 20ms 限。**
- 测2 `set_timeout_ms=5000 > connect_timeout_ms=500`,连 blackhole:
  `e2e=1500ms(3×500ms connect 重试), ErrorCode=110(connect timed out)` → connect_timeout_ms 是**独立子阶段计时器**,当它 < set_timeout_ms 时独立起作用。

**纠正后的结论**:brpc 1.15.0 的 `set_timeout_ms` 是**整条 RPC(含 connect)的 deadline**,`connect_timeout_ms` 是 connect 子阶段的额外上限,实际生效 = `min(set_timeout_ms, connect_timeout_ms + connect重试开销)`。在 SDK 配置 20ms(`set_timeout_ms = ApiRemainingUs ≤ 20ms < connect_timeout_ms=1000`)时,**connect 阶段被 20ms 截断,受控**。
- 残留风险:仅在 `set_timeout_ms > connect_timeout_ms` 时 connect 阶段按 connect_timeout 独立计时(默认 1000ms),但 SDK 20ms 场景不会触发此条件。
- **判定:T8 受 20ms 限**(原判"不受控"是错的,实测推翻)。

### T9 — worker→worker stream 页推送 RPC 固定 10s

**代码事实**:`remote_worker_manager.cpp:976` `kStreamPagePushRpcTimeoutMs=10000`,L982/1051 `opts.SetTimeout(10000)`,被 `BatchFlush`(L1169/1176)调用。这是 stream_cache worker→worker 数据推送路径,固定 10s,**不读 ApiRemainingUs**。

**测试用例(需多机分布式)**:
1. 部署 2+ worker(单机 4-worker 隔离配置即可,memory `datasystem-4worker-same-host-deploy`)。
2. SDK 配置 `requestTimeoutMs=20`。
3. 触发跨 worker 的 stream 页推送:写一个大对象(>shm 阈值,触发 stream 复制),让 worker A 向 worker B `PushElementsCursorsAsyncWrite`。
4. 在 worker B 侧用 iptables DROP worker A 的 brpc 端口(模拟 peer 慢/crash),让 stream 推送 RPC hang。
5. 观察 worker A 的 `BatchFlushAsyncWrite` 调用耗时:预期 ~10000ms(固定 10s 超时),**不受 SDK 20ms 管**。
6. 证据:worker A 日志 `PushElementsCursorsAsyncWrite` 的 RPC 耗时 + `cntl_timeout_ms=10000`(用诊断字段)。

**单机简化(部分实锤)**:stream 路径需真实 worker 间 channel,单机只链 .so 难复现。可用 INJECT_POINT 让 BatchFlush 内的 stub 调用 hang,测 `opts.SetTimeout(10000)` 是否独立于 ApiDeadline——但需编译 worker 目标。**判定:代码铁证(固定 10s 不读 ApiRemainingUs)+ 多机配方,未单机实锤。**

### T10 — stream Read / StreamClose 固定 30s / 5s / 7200s

**代码事实**:
- `brpc_status_util.h:42` `kStreamCloseTimeoutSec=5`,`:47` `kStreamReadTimeoutSec=30`。
- `brpc_client_stream_writer_reader.h:239` `readCond_.wait_until(lock, readDeadline)`,readDeadline = `now + 30s`,**不接 ApiDeadline**。
- `brpc_stream_close_helper.cpp:51` `state.cv.wait_until(lk, deadline)`,deadline = `now + timeoutSec(5s)`,deferred 关闭 7200s。
- 注:这俩**已有超时**(wait_until + ETIMEDOUT break),不是裸 wait;问题是**固定值不响应请求级 20ms**。

**测试用例(需 stream_cache 路径)**:
1. 部署 SDK + worker,启用 stream_cache(大对象流式传输)。
2. SDK 配置 20ms,发起一个大对象 Get(触发 stream_cache Read)。
3. 在 worker 侧 DROP stream 帧(让 on_data 不回调)。
4. 观察 SDK 端 `BrpcClientStreamWriterReader::Read()` 耗时:预期 ~30000ms(固定 30s),**不受 20ms 管**。
5. 证据:SDK 侧 stream Read 调用耗时 + `cntl_timeout_ms`(若 stream 路径有诊断字段)。

**判定**:代码铁证(固定 30s/5s/7200s 不读 ApiDeadline)+ stream_cache 路径配方。stream 是长连接,固定超时是**有意设计**(防 peer crash 永久阻塞 bthread,见代码注释),改 ApiRemainingUs 会过早杀流。**建议不改**(原 P3 判断维持)。

### T11 — SDK Get 的 `subTimeoutMs`(第二参数)

**代码事实**:`client_worker_remote_api.cpp:529` `requestTimeoutUs = max(subTimeoutMs*1000, ApiRemainingUs())`。`subTimeoutMs` 默认 **500ms**(`client_worker_remote_api.h:124`、`iclient_worker_api.h:302`)。即默认 Get 调用,SDK→worker 单次 per-call RPC timeout = `max(500ms, 20ms 剩余) = 500ms`。

**测试用例(需 SDK+worker)**:
1. 部署 SDK + worker。SDK 配置 `requestTimeoutMs=20`(建 20ms ApiDeadline)。
2. 调 `Get(keys)` 不传 subTimeoutMs(用默认 500)。
3. 在 worker 侧 Get 处理注入 sleep 300ms(< 500ms per-call,> 20ms ApiDeadline)。
4. 预期:单次 RPC 被 per-call 500ms 截断前 worker 已返回(300ms),func 返回 OK;但 RetryOnError 下一轮 P0 `CheckApiDeadline` 发现 20ms 已过 → 整体返回 `K_RPC_DEADLINE_EXCEEDED`。
5. 证据:SDK access log 的 `client.rpc.get` 耗时 + status 码;若单次 RPC 实际跑了 300ms 才被 P0 兜,说明 per-call(500ms)未按 20ms 截,是 ApiDeadline 整链兜住。

**判定**:`max(subTimeoutMs, ApiRemainingUs)` 语义铁证(grep)。per-call 可达 500ms(>20ms),但 ApiDeadline 经 P0 整链兜住。**残留风险**:单次 RPC 内的 500ms 不被 20ms 截,依赖 P0 轮间检查;若单次 RPC 恰好 < 500ms 且 > 20ms,会浪费预算。**建议:将 `max` 改为 `min(subTimeoutMs, ApiRemainingUs)`**——见 T11 修复(归入 P2 follow-up)。

### T12 — RetryOnError 内部 sleep 序列(已实测 + ✅ 已落地)

**修复前实测(compile_32c_06,链 libdatasystem.so)**:
- Case A 无 ApiDeadline,timeoutMs=10000,func 永远 K_TRY_AGAIN:
  `e2e=9990ms, funcCalls=8, status=19` → RetryOnError 内部 sleep 序列在无 ApiDeadline 时跑近 10s,**不受 20ms 管**。
- Case B 20ms ApiDeadline + P0:
  `e2e=56ms, funcCalls=3, status=1001` → P0 轮间截断,但**轮内 sleep 不被中途打断**(56ms > 20ms,多了一轮 50ms sleep)。

**关键精度发现**:P0 是**轮间**截断(每轮 func 前 CheckApiDeadline),**轮内** `HandleRetryTime` 的 `sleep_for` 在执行中不被中途打断。20ms 预算下最坏多跑 1 轮 sleep(50ms)= 56ms。

**修复方案(T12,已落地)**:`HandleRetryTime`(`rpc_util.h:67-89`)的 `retryInterval` 在 `sleep_for` 之前 clamp 到 `min(retryInterval, remainTime - minOnceRpcTimeoutMs, apiRemainingMs)`,使轮内 sleep 不超 ApiDeadline 剩余。原 `retryInterval` clamp 逻辑保留(`remainTime <= retryInterval` 时降为 `remainTime - minOnceRpcTimeoutMs`),新增对 `apiRemainingMs`(`TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs())`)的 clamp。`retryInterval<0` 兜底 0,`sleep_for` 仅在 `retryInterval>0` 时执行。已实施代码(commit `1ceb89a5`):
```cpp
// rpc_util.h HandleRetryTime: 已落地版本
inline void HandleRetryTime(int32_t &retryInterval, int32_t &remainTime, uint64_t &retryCount,
                            int32_t &minOnceRpcTimeoutMs)
{
    // Clamp the backoff sleep so a single retry interval cannot outlive the per-request
    // ApiDeadline budget. When ApiDeadline is uninitialized (background / fan-out threads),
    // ApiRemainingUs() returns RPC_TIMEOUT(60s)*1000, so the apiRemainingMs clamp does not
    // trigger and behavior is identical to the previous code path.
    int32_t apiRemainingMs = static_cast<int32_t>(TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs()));
    if (remainTime <= retryInterval) {
        retryInterval = remainTime - minOnceRpcTimeoutMs;
    }
    retryInterval = std::min({ retryInterval, remainTime - minOnceRpcTimeoutMs, apiRemainingMs });
    if (retryInterval < 0) {
        retryInterval = 0;
    }
    remainTime -= retryInterval;
    ++retryCount;
    if (retryInterval > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(retryInterval));
    }
}
```
- 注意:`std::this_thread::sleep_for` 仍不可中途取消,但 clamp 到 `apiRemainingMs` 保证 sleep 不超 deadline。真正要中途响应需改 `sleep_for` 为 `bthread` 可打断 wait(成本高,不必要)。
- 影响面:所有 RetryOnError 调用方。未初始化 ApiDeadline 时 `ApiRemainingUs` 返 60s,clamp 不触发,行为同现状。
- 依赖 include:`rpc_util.h` 新增 `#include "datasystem/common/rpc/timeout_duration.h"`(CeilUsToMs)。

**修复后实测(compile_32c_06,链 `/home/marck/_pkg2/client/libdatasystem.so` 提供 ApiDeadline 符号,RetryOnError 模板在本 TU 内重新实例化用修复后的 `rpc_util.h`)**:
- Case A 20ms ApiDeadline + 永远 K_TRY_AGAIN func:
  `e2e_ms=20, status_code=1001 (K_RPC_DEADLINE_EXCEEDED)` → **修复生效,不再 56ms**(轮内 sleep 被 clamp 到 apiRemainingMs)。
  连续 3 次运行均稳定:20/20/20 ms,全部 1001。
- Case B **无** ApiDeadline(后台线程路径,ApiRemainingUs=60s) + timeoutMs=200 + 永远 K_TRY_AGAIN:
  `e2e_ms=199, status_code=19 (K_TRY_AGAIN)` → **未误杀**。clamp 不触发(apiRemainingMs=60000ms 远大于 retryInterval),行为与修复前一致;最终 status 是 retryCode 耗尽预算后的 K_TRY_AGAIN,不是 K_RPC_DEADLINE_EXCEEDED。
  连续 3 次运行均稳定:199/199/199 ms。
- 编译验证:`-fsyntax-only` 通过(marck CMake build tree include 路径,带 WITH_TESTS);runtime 链接通过(`-Wl,--allow-shlib-undefined` 绕开 libprotoc 传递依赖,本 TU 不用 INJECT_POINT 因此不引 inject::Execute 符号)。

**判定:T12 ✅ 已落地 + 运行时实测通过(20ms 截断 + 未初始化不误杀双场景验证)。**

### T13 — TryLockWithRetry 固定 191ms sleep(已实测 + ✅ 已落地)

**修复前实测(compile_32c_06)**:固定序列 `{1,10,30,50,100}ms` 累加 = 191ms,20ms 预算超 171ms。调用点:get_impl.cpp:2579、delete_impl.cpp:187、batch_get_impl.cpp:363、migrate_impl.cpp:433、master_worker_oc_service_impl.cpp:97(5 处)。循环**不查 ApiDeadline**。

**修复方案(T13,已落地)**:`object_kv.cpp:118-157` `TryLockWithRetry` 循环每轮加 `ApiDeadline::Instance().CheckApiDeadline()` 早退 K_RPC_DEADLINE_EXCEEDED(`RETURN_IF_NOT_OK_PRINT_ERROR_MSG`)+ 单轮 sleep 限到 `min(t, apiRemainingMs)`(`apiRemainingMs = TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs())`)。budget 耗尽(`apiRemainingMs<=0`)后 break。保留原有 `LOG(INFO)` 成功/超时日志。已实施代码(commit `1ceb89a5`):
```cpp
Status TryLockWithRetry(const std::string &objectKey, const std::shared_ptr<SafeObjType> &entry, bool nullable)
{
    Status rc = entry->TryWLock(nullable);
    if (rc.GetCode() != K_TRY_AGAIN) {
        return rc;
    }
    static const std::vector<int> delayMs = { 1, 10, 30, 50, 100 };
    int totalRetryMs = 0;
    int retryCount = 0;
    for (auto t : delayMs) {
        // Honor the per-request ApiDeadline: stop retrying once the budget is gone. When
        // ApiDeadline is uninitialized (background threads), ApiRemainingUs returns
        // RPC_TIMEOUT(60s)*1000 and CheckApiDeadline returns OK, so behavior is unchanged.
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            ApiDeadline::Instance().CheckApiDeadline(),
            FormatString("TryLockWithRetry deadline exceeded for object key %s after %d retries", objectKey, retryCount));
        // Cap this sleep to the remaining request budget so a single retry cannot outlive
        // the deadline.
        int32_t apiRemainingMs =
            static_cast<int32_t>(TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs()));
        int sleepMs = std::min<int>(t, std::max<int>(0, apiRemainingMs));
        if (sleepMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
            totalRetryMs += sleepMs;
        }
        retryCount++;
        rc = entry->TryWLock(nullable);
        if (rc.GetCode() != K_TRY_AGAIN) {
            LOG(INFO) << FormatString("TryWLock succeeded after %d retries for object key %s, cost %dms", retryCount,
                                      objectKey, totalRetryMs);
            return rc;
        }
        if (apiRemainingMs <= 0) {
            break;  // budget exhausted
        }
    }
    LOG(INFO) << FormatString("TryWLock timeout after %d retries for object key %s, cost %dms", retryCount, objectKey,
                              totalRetryMs);
    return { K_WORKER_TIMEOUT, "Worker timeout" };
}
```
- 影响:worker 锁竞争场景不再固定 191ms,受请求级 deadline 管。端到端本就被 SDK 兜,但 worker 内部不再浪费资源、不再超 20ms。
- 未初始化 ApiDeadline(后台线程)时 `ApiRemainingUs` 返 60s,`CheckApiDeadline` 返 OK,行为同现状。
- 依赖 include:`object_kv.cpp` 新增 `#include "datasystem/common/rpc/api_deadline.h"` + `#include "datasystem/common/rpc/timeout_duration.h"`。

**编译验证(compile_32c_06)**:
- `object_kv.cpp` `-fsyntax-only` 通过(marck CMake build tree include 路径,带 WITH_TESTS,复用 `compile_commands.json` 中 object_kv.cpp 条目的全部 -I/-isystem/-D 标志)。
- 运行时实测:未做(`TryLockWithRetry` 直接依赖 `SafeObjType::TryWLock`,需 mock `SafeObjType` 才能在 .so 之外跑;按任务约定 T13 难可跳过)。静态语义 + -fsyntax-only 足证 deadline 检查/sleep clamp 路径编译正确;`CheckApiDeadline`/`ApiRemainingUs` 未初始化返 OK/60s 的语义在 `api_deadline.h` 注释已明,且 T12 已用相同模式运行时证"未初始化不误杀"。

**判定:T13 ✅ 已落地 + -fsyntax-only 编译验证通过;运行时未独立复现(T13 难可跳过,依赖 mock SafeObjType),语义与 T12 同源,未初始化不误杀由 api_deadline.h 契约保证。**

### T14 — 原始 144ms 故障(根因单机坐实 + 修复已落地)

**已确认事实**(SDK access log + worker log + brpc trace):
- `transportType:SHM`,`client.rpc.get:144103`(SDK→worker brpc Get 耗 144ms)
- worker `[Get] Done, transferPath: UB, totalCost: 1.277ms`(服务端 1.2ms 完成)
- brpc trace `e2e_us=144075`(CallMethod 内),`server_exec_us=1249`,`network_residual_us=142819`(残差 142ms)
- SHM 数据面 `resp_attachment_bytes=56`(元数据级,推翻"收大 payload 慢")

**根因已定位(代码铁证,brpc_stub_generator.cpp:396-405)**:
生成的 brpc stub 在 `CallMethod` 返回后:
```cpp
if (cntl.Failed()) {                          // 只有 controller 显式 Failed() 才返回错误
    return TryExtractStatusFromControllerError(cntl.ErrorText(), cntl.ErrorCode());
}
return ::datasystem::Status::OK();            // cntl_failed=0 时直接 OK,不截断 deadline
```
brpc 的 `set_timeout_ms` 只在 RPC **失败**时 `SetFailed`(超时、连接断等)。当 RPC **成功但慢**(e2e=144ms > deadline 20ms,但 controller 没 Failed),stub 直接 `return Status::OK()`,**不检查 RPC 耗时是否超 deadline**。这就是"deadline 设了(`cntl_deadline_us`>0)但 `cntl_failed=0` 不截断"的代码根因。
- 诊断字段采集点(L386 `SetCntlDiagnostics`)在每个 CallMethod 后立即读 controller 的 timeout_ms/deadline_us/ErrorCode/Failed/attachment_size,确认诊断能力到位(供原始环境复现抓数据)。
- 142ms 残差在网络/调度层(`network_residual_us=142819`),worker 服务端只 1.2ms——慢在 SDK 收到响应前的某段(brpc IO 调度/SHM 通知),但 controller 最终没 Failed,故 stub 不截断。

**测试用例(单机已坐实根因,不需原始多机)**:
1. **单机坐实(compile_32c_01,commit 3adb52b7)**:python HTTP server sleep 50ms 返回 200(success-but-slow)+ brpc HTTP channel(client per-call `set_timeout_ms=500ms` 复刻 SDK T11 max 语义,请求级 ApiDeadline=20ms)。实测:
   ```
   RPC e2e = 51.078ms  cntl.Failed = 0  ErrorCode = 0
   BEFORE fix: stub returns code=0 (K_OK)   ← 根因坐实:50ms RPC > 20ms deadline 但返回 OK
   AFTER fix:  stub returns code=1001 (K_RPC_DEADLINE_EXCEEDED)  ← 修复生效
   RESULT: PASS
   ```
   根因是 stub 逻辑问题(`cntl.Failed()` 不覆盖"慢但成功"),单机可复现,不需原始多机高压。
2. 原始多机环境(可选,进一步坐实 144ms 实景):16 SDK/qps35/跨物理节点+urma/UB,用诊断 wheel(commit 3adb52b7+ 诊断字段保留)grep `[BRPC_RPC_FRAMEWORK_SLOW]` 筛 `e2e_us>20000 && cntl_failed==0` 样本,看 `cntl_deadline_us`/`network_residual_us`/`resp_attachment_bytes` 5 值。

**修复方案(T14,已落地 commit 3adb52b7)**:
- **精确修复**(`brpc_stub_generator.cpp` `BuildBrpcStubNoStreamImpl`):在 `cntl.Failed()` 的 else 分支(即 RPC 成功时),加请求级 `ApiDeadline::Instance().CheckApiDeadline()` 检查:
  ```cpp
  if (cntl.Failed()) {
      ... return error;
  }
  // T14: successful-but-slow RPC that exceeded the per-request ApiDeadline must be
  // treated as a deadline miss. brpc set_timeout_ms only SetFailed()s on actual
  // RPC failure; a slow success leaves cntl.Failed()=0.
  ::datasystem::Status deadlineStatus = ::datasystem::ApiDeadline::Instance().CheckApiDeadline();
  if (deadlineStatus.IsError()) {
      return deadlineStatus;  // K_RPC_DEADLINE_EXCEEDED
  }
  return ::datasystem::Status::OK();
  ```
  放在 `cntl.Failed()` 之后(RPC 失败时优先返回真实 brpc 错误,不被 deadline 覆盖);用 `CheckApiDeadline()` 复用 ApiDeadline 预算,无需从 rpcTrace 算 e2e。
- **影响面**:所有 brpc sync unary RPC(覆盖 SDK→worker 全主路径)。慢但成功的 RPC 现返 `K_RPC_DEADLINE_EXCEEDED`,触发上层 RetryOnError 重试(P0 兜底)。
- **未初始化不误杀**:后台/fan-out 线程 ApiDeadline 未初始化时 `ApiRemainingUs()` 返 60s,`CheckApiDeadline()` 返 OK,行为同现状(注释已写明)。
- **验证**:生成产物 grep 确认含 `CheckApiDeadline` 检查(worker_object.brpc.stub.pb.cc:194-196)+ 生成片段 `-fsyntax-only` PASS + 单机运行时(AFTER fix 返 1001)。
- **残留考量**:正常成功 RPC 若恰在 deadline 边缘(e2e≈20ms)可能被判超时。但 `CheckApiDeadline` 用 ApiDeadline 剩余预算(≤0 才超时),是请求级预算,语义正确;边缘场景靠上层 RetryOnError 重试兜底。原始 144ms 实景验证待原始多机环境(可选)。

### 九、小结:测试覆盖矩阵

| 场景 | 测试方式 | 状态 |
|---|---|---|
| T8 connect_timeout | 单机 brpc channel + blackhole IP | ✅ 实测(推翻原判:受 20ms 限) |
| T9 stream 推送 10s | 多机 4-worker + iptables DROP | ⏳ 代码铁证 + 配方,未单机实锤 |
| T10 stream Read/Close 30s/5s | stream_cache 路径 + DROP 帧 | ⏳ 代码铁证 + 配方;建议不改 |
| T11 subTimeoutMs max | SDK+worker + worker sleep 300ms | ⏳ 语义铁证(grep max)+ 配方 |
| T12 RetryOnError sleep | 单机链 .so 实例化 | ✅ 已落地 + 运行时实测通过(20ms 截断 + 未初始化不误杀双场景,3 次稳定) |
| T13 TryLock 191ms | 单机静态 + 调用点 grep | ✅ 已落地 + -fsyntax-only 通过(运行时难可跳过) |
| T14 144ms | 单机 brpc channel + sleep-50ms server | ✅ 根因单机坐实(51ms>20ms deadline,cntl_failed=0→return OK)+ 修复落地(commit 3adb52b7,改后返 K_RPC_DEADLINE_EXCEEDED) |

**T12/T13/T14 修复已落地(commit `1ceb89a5` + `3adb52b7`)。T9/T10 建议不改(stream 语义)。T8 实测纠正为受控。T11 归入 P2 follow-up(`max`→`min`)。T14 根因单机坐实(compile_32c_01)+ 修复运行时验证通过。**
