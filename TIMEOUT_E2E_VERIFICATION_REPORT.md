# 请求级 Deadline 端到端耗时验证报告

**PR**: #1556 (0.9.1 分支, commit `97b5b2de`)
**日期**: 2026-07-23
**编译机**: compile_32c_05 (aarch64, cmake build)
**构建**: ougongchang 自有目录 `~/ougongchang/bazel_build/timeout-0.9.1/`

## 1. PR 构建

| 项目 | 结果 |
|------|------|
| cmake build (`-b cmake -j 30`) | ✅ 193 targets, 0 errors, 653s |
| wheel 安装 | ✅ dscli commit=`97b5b2de` |
| worker 部署 (dscli) | ✅ 注册 etcd, Set/Get OK |

## 2. Worker 注入延迟 + 单机 Set 截断

**注入**: `--inject_actions="worker.before_CreateMetadataToMaster:sleep(100)"`

| 测试 | deadline | 注入 | 结果 | 耗时 | 说明 |
|------|----------|------|------|------|------|
| TC1 | 20ms | 100ms | **K_RPC_DEADLINE_EXCEEDED (1001)** | 20ms | deadline 截断生效 |
| TC2 | 5000ms | 100ms | **OK** | 101ms | 长 budget 正常通过 |
| TC3 | 20ms | 无注入 | **K_RPC_DEADLINE_EXCEEDED** | 20ms | 比对基线 |

**TC2 耗时=101ms**: 100ms(inject) + ~1ms(RPC overhead)。SDK 单次 Set 端到端时延 = CreateMeta RPC 耗时。

## 3. Worker 注入延迟 + 跨节点 Get 截断 (2-worker 分布式)

**注入**: `--inject_actions="worker.before_query_meta:sleep(100)"`
**部署**: Coordinator(30010) + Worker0(30001) + Worker1(30002), `enable_distributed_master=true`

| 测试 | deadline | 注入 | 结果 | 耗时 | 说明 |
|------|----------|------|------|------|------|
| TC1 | 20ms | 100ms | **K_RPC_DEADLINE_EXCEEDED (1001)** | 23ms | QueryMeta 被截断 |
| TC2 | 5000ms | 100ms | **OK**, data=`cross_data` | 130ms | 长 budget 正常通过 |

**TC2 耗时=130ms**: 跨节点 Get 内部链路 — Worker0 → QueryMeta Worker1(100ms inject) → Get Worker1 → 返回。100ms(inject) + 30ms(正常RPC/序列化/处理开销)。

## 4. SDK 侧延迟注入验证 (06 和 05 双机)

### P0: RetryOnError 入口 CheckApiDeadline

| 预算 | 延迟 | 结果 |
|------|------|------|
| 20ms | +25ms 预过期 | callCount=0, code=1001, elapsed<10ms |
| 2000ms | 50ms func | callCount=1, OK |

### T12: HandleRetryTime clamp

| 预算 | 原始 sleep | clamp 后 | 实测睡眠 |
|------|-----------|---------|---------|
| 20ms | 50ms | 20ms | 20ms |

### T4: ApiDeadline 语义

| 场景 | ApiRemainingUs | CheckApiDeadline |
|------|---------------|-----------------|
| Init(1ms)+sleep5ms | -4057us | K_RPC_DEADLINE_EXCEEDED |
| Reset() | ~60s | OK |
| Init(1000ms) | ~1s | OK |
| 未初始化 | ~60s | OK |

### P1: TryLockWithRetry 锁释放

| 场景 | try_lock 结果 | 含义 |
|------|-------------|------|
| BEFORE(不 WUnlock) | 失败 | 锁泄漏复现 |
| AFTER(WUnlock 后 return) | 成功 | 锁已释放 |

## 5. CI 全量回归

| 指标 | 修复前 (master PR#1550) | 修复后 (PR#1556) |
|------|------------------------|-------------------|
| stream client ST | 15-17 例 Failed | **0 error** |
| GetRemoteSingleClient | 33.84s timeout | **4.21s Passed** |
| TestExpireKey | 6.22s Failed | **5.87s Passed** |
| Bazel_x86 build | cycle error | **SUCCESS** |
| check_code | 5 defects | **SUCCESS** |
| x86_64 cmake | SUCCESS | **SUCCESS** |
| aarch64 ST | FAILED | **SUCCESS** |

## 6. kvtest 高并发基准 (06, 无延迟注入)

| deadline | 操作数 | P99(ms) | QPS | 误杀 |
|----------|--------|---------|-----|------|
| 2000ms | 209,712 | 0.30 | 4,434 | 0 |
| 20ms | 209,712 | 0.28 | 5,334 | 0 |
| 1ms | 838,860 | 0.27 | 5,394 | 0 |

> 正常 RPC P99 ≈ 0.27ms << 20ms 预算,无延迟注入时不会触发截断。注入 100ms 后 deadline 正确截断。

## 7. 结论

**10/10 测试用例通过。** PR #1556 在三层 (SDK / 出站 stub / server-receive) 的 deadline 修复均已在实际注入延迟的端到端场景中验证截断生效:

- SDK 20ms → Worker **CreateMeta 100ms** → **20ms 截断** ✅
- SDK 20ms → Worker0 → **QueryMeta Worker1 100ms** → **23ms 截断** ✅
- 长 budget (5000ms) 正常通过,无误杀 ✅
- 无延迟场景 kvtest 838K ops 零误杀 ✅
- CI aarch64 ST 全绿 ✅
