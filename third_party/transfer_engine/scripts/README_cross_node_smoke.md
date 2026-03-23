# Cross Node Smoke 用法

## 目标
- 一个进程只使用一张卡（由 `--device-id` 指定）。
- requester 统一走 `BatchTransferSyncRead`。
- batch 大小由 `--remote-addrs` 的地址个数决定。

## 1) 编译
```bash
cmake -S . -B build
cmake --build build -j --target transfer_engine_cross_node_smoke
```

## 2) 基础跨节点验证（basic）

### 节点A（owner）
```bash
scripts/run_cross_node_smoke_cases.sh basic owner \
  --local-ip 10.10.10.1 --local-port 65051 --device-id 0 --size 1048576 --hold-seconds 600
```

owner 启动后会打印：
```text
[OWNER_READY_FOR_REQUESTER] --peer-ip 10.10.10.1 --peer-port 65051 --peer-device-base-id 0 --remote-addrs 0x...
```

### 节点B（requester）
```bash
scripts/run_cross_node_smoke_cases.sh basic requester \
  --local-ip 10.10.10.2 --local-port 65151 --device-id 1 --size 1048576 \
  --peer-ip 10.10.10.1 --peer-port 65051 --peer-device-base-id 0 \
  --remote-addrs 0x7f1234567800 --auto-verify-data
```

## 3) 并发验证（一个 owner，多个 requester 进程）

### 节点A（owner）
```bash
scripts/run_cross_node_smoke_cases.sh concurrent owner \
  --local-ip 10.10.10.1 --local-port 65051 --device-id 0 --size 1048576 --hold-seconds 600
```

### 节点B（requester，多进程并发）
```bash
scripts/run_cross_node_smoke_cases.sh concurrent requester \
  --local-ip 10.10.10.2 --local-port 65151 --device-id 1 --size 1048576 \
  --peer-ip 10.10.10.1 --peer-port 65051 --peer-device-base-id 0 \
  --remote-addrs 0x7f1234567800 --requester-count 4 --requester-port-step 1 --requester-device-step 1
```

说明：
- 上面请求端会 fork 4 个子进程并发读取。
- 每个子进程都会发一次 `BatchTransferSyncRead`。
- `--remote-addrs` 支持逗号分隔：`a0,a1,a2`，传几个地址就做几项 batch。
- `concurrent requester` 脚本默认启用 `--auto-verify-data`。

## 4) TransferEngine 注册多个内存配置

当 owner 需要一次注册多个内存块时，建议直接使用 `BatchRegisterMemory`：

```cpp
TransferEngine owner;
owner.Initialize("127.0.0.1:65051", "ascend", "npu:0");

std::vector<uintptr_t> bufferAddrs = {
  reinterpret_cast<uintptr_t>(src0),
  reinterpret_cast<uintptr_t>(src1),
  reinterpret_cast<uintptr_t>(src2),
};
std::vector<size_t> lengths = {size0, size1, size2};
owner.BatchRegisterMemory(bufferAddrs, lengths);
```

requester 侧按相同顺序传 `remote-addrs`，batch 大小与地址个数一致：

```bash
scripts/run_cross_node_smoke_cases.sh basic requester \
  --local-ip 10.10.10.2 --local-port 65151 --device-id 1 --size 1048576 \
  --peer-ip 10.10.10.1 --peer-port 65051 --peer-device-base-id 0 \
  --remote-addrs 0xaddr0,0xaddr1,0xaddr2 --auto-verify-data
```

注意：
- `remote-addrs` 顺序要和 owner 注册顺序一致。
- `BatchTransferSyncRead` 会按顺序把第 `i` 个 `remote-addr` 读到第 `i` 个本地 buffer。
