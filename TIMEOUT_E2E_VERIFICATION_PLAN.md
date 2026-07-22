# 请求级 Deadline 端到端耗时验证方案

## 环境

- **机器**: compile_32c_06 (aarch64, openEuler 22.03)
- **基准**: PR #1556 commit `97b5b2de` (基于 origin/0.9.1 fe693fa7)
- **工具**: kvtest (tests/kvtest), dscli, etcd
- **集群**: 单机 1 coordinator + 1 worker (使用 dscli deploy)

## 测试用例设计

### TC1: SDK 20ms → worker → worker→master 出站被截断

**目标**: 验证 SDK 设 `requestTimeoutMs=20ms` 后,worker→master QueryMeta 出站 RPC 被 T14 CheckApiDeadline 截断。

**方法**:
1. 部署 worker,使用 `--inject_actions=worker.before_query_meta:sleep(50)` 在 QueryMeta 前注入 50ms sleep
2. kvtest 配置 `request_timeout_ms=20`
3. 运行 kvtest Set + Get
4. 预期: Get 返回 `K_RPC_DEADLINE_EXCEEDED`(1001),总耗时 ≤ ~70ms(20ms 预算 + 50ms sleep 被截断,实际应 ~20ms)

**验收标准**:
- 返回错误码 `K_RPC_DEADLINE_EXCEEDED` (code=1001)
- end-to-end 总耗时 ≤ 25ms (预算 20ms + RPC overhead ~5ms)

### TC2: 多跳 RPC 累计消耗 20ms 预算

**目标**: 验证 SDK→worker→master→worker 多跳链路上,每跳出站 stub 都检查 deadline,累计消耗超过 20ms 后被截断。

**方法**:
1. 部署 2 worker (worker0 + worker1, distributed master)
2. 在 worker→master 路径注入 `worker.before_query_meta:sleep(15)`,使每跳 QueryMeta 消耗 15ms
3. kvtest Get 跨 worker 对象 (需要 2 跳 QueryMeta: worker0→master, worker0→worker1)
4. 预期: 第 1 跳 QueryMeta 成功(~15ms),第 2 跳被 deadline 截断

**验收标准**:
- 至少 1 次 RPC 返回 `K_RPC_DEADLINE_EXCEEDED`
- 总耗时 ≤ 25ms (不是 15ms×2=30ms)

### TC3: 高负载多线程并发下 deadline 准确性

**目标**: 验证多线程并发 Get 请求时,每个请求的 deadline 独立计时,不被其他请求干扰。

**方法**:
1. kvtest 配置 `num_threads=8`, `request_timeout_ms=100`
2. 注入 `worker.before_query_meta:sleep(30)` (模拟慢 master)
3. 预期: 每个请求在 ≤ ~130ms 内完成或被截断,不会出现部分请求超 200ms

**验收标准**:
- 无请求耗时 > 200ms (即跨请求 deadline 污染)
- 所有请求在 ~130ms 内返回 (100ms 预算 + 30ms sleep = 130ms)
- 返回码为 OK 或 `K_RPC_DEADLINE_EXCEEDED`,无其他异常

### TC4: 144ms 故障根因复现

**目标**: 复现"成功但慢的 RPC(144ms)在 20ms 预算下返回 OK"的原始故障,验证修复后返回 `K_RPC_DEADLINE_EXCEEDED`。

**方法**:
1. 用 brpc HTTP channel 模拟: Python HTTP server sleep 100ms 返回 200 + brpc channel timeout=500ms
2. ApiDeadline=20ms
3. 调用 RPC
4. 预期 BEFORE: stub 返回 OK(cntl_failed=0); AFTER: stub 返回 `K_RPC_DEADLINE_EXCEEDED`

**验收标准**:
- e2e 耗时 ≈ 100ms (HTTP server sleep 时间)
- cntl_failed() == 0 (brpc 认为成功)
- stub 返回 `K_RPC_DEADLINE_EXCEEDED` (T14 截断)

### TC5: 正常路径不误杀

**目标**: 验证修复不误杀正常请求 (无延迟注入时,正常 RPC 通过)。

**方法**:
1. kvtest 配置 `request_timeout_ms=2000` (2 秒)
2. 无 inject sleep
3. 运行 Set + Get 100 次
4. 预期: 全部 OK,耗时 < 100ms

**验收标准**:
- 100% 返回 OK
- 无 `K_RPC_DEADLINE_EXCEEDED`

## 执行计划

| 步骤 | 内容 | 预计耗时 |
|------|------|---------|
| 1 | 在 06 编译 kvtest (链 marck libdatasystem.so) | 5 min |
| 2 | 部署单 worker 集群 (etcd + coordinator + worker) | 2 min |
| 3 | 执行 TC1 (SDK→worker→master 截断) | 3 min |
| 4 | 执行 TC2 (多跳累计) | 3 min |
| 5 | 执行 TC3 (高负载并发) | 3 min |
| 6 | 执行 TC4 (144ms 复现) | 5 min |
| 7 | 执行 TC5 (正常路径不误杀) | 2 min |
| 8 | 生成回归报告 | 5 min |
