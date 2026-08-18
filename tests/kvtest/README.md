# kvtest

独立的 datasystem KVClient 性能测试工具，支持 Writer/Reader 角色分离、Cache 模式、Benchmark Set/Get 模式、多节点部署、K8s 自动发现。同时提供 Worker/Coordinator 独立部署测试程序（coordinator_test / worker_test）和外部服务发现模拟（mock_jf_server.py），用于验证独立集成部署与服务发现对接流程。

## 编译与运行

```bash
cd tests/kvtest

# 编译（默认 Bazel：in-tree datasystem，自包含二进制，无需预装 SDK；
#       自动启用 KVTEST_USE_BRPC ---- brpc 控制面 + bthread pipeline/notify 池）
./build.sh

# 或指定 SDK 路径
./build.sh -s /path/to/sdk

# 用 Bazel 构建并将 URMA 支持编入自包含 kvtest（与根目录 build.sh -M on 对齐）
./build.sh -b bazel -M on

# Debug 构建
./build.sh -d                # bazel: --config=debug
./build.sh -b bazel -d

# 用 CMake + brpc 后端构建（默认 KVTEST_USE_BRPC=ON，行为与 bazel 一致；
#       复用主仓 cmake/external_libs/*.cmake 自动下载编译 brpc/protobuf/gflags/absl，
#       缓存到 $DS_OPENSOURCE_DIR，首次 5-10 分钟，之后秒级；仍链预装 libdatasystem.so）
./build.sh -b cmake
./build.sh -b cmake -s /path/to/sdk

# CMake + httplib 后端（fallback：无第三方依赖，无网络下载，纯 std::thread）
./build.sh -b cmake --use-httplib
```

Bazel 模式不会在运行时加载外部 `libdatasystem.so`。需要 UB/URMA 数据面时，必须在构建 kvtest 本身时传入
`-M on`；该选项会映射为 Bazel 的 `--config=urma`。默认值为 `off`。CMake 模式的能力由 `-s` 指定的
预构建 SDK 决定，因此不接受 `-M on`。

> cmake+brpc 模式下第三方件源码默认从 gitee/github 下载。如需离线/加速，可设
> `export DS_LOCAL_LIBS_DIR=/path/to/opensource_third_party` 指向主仓

## 运行

```bash
# 启动依赖
etcd &
mkdir -p /tmp/ds_worker && cd /tmp/ds_worker
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379

# 运行
cd tests/kvtest/output
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/my_config.json

# 查看统计 / 停止（HTTP 端点路径不变：/stats、/stop、/summary、/notify）
# bazel 构建：brpc 经 restful 映射保留旧路径，响应 /stats 为 {"stats_json":"<metrics json>"}
curl -s http://127.0.0.1:9000/stats | python3 -m json.tool
curl -X POST http://127.0.0.1:9000/stop
# cmake 构建仍走 httplib：/stats 直接返回 metrics JSON
```

## Benchmark Set/Get 模式

用于精确测量 Set/Get 吞吐和延迟，支持 8 种测试模式：

```bash
# 本地 Set 吞吐基线（8线程，5轮）
cat > config/bench.json << 'EOF'
{
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "test_mode": "set_local",
  "worker_memory_mb": 4096,
  "num_threads": 8,
  "total_rounds": 5,
  "data_sizes": ["8MB"],
  "set_api": "string_view",
  "cleanup_method": "del"
}
EOF

LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/bench.json
```

**测试模式：** `set_local` / `set_remote` / `get_local` / `get_cross_node` / `get_remote_direct` / `get_remote_cross` / `mixed_local` / `mixed_cross_node`

**Set API：** `string_view`（直接写入）/ `create_buffer`（SHM Buffer + latch）/ `create_buffer_raw`（SHM Buffer，无锁 memcpy）

**输出：** `benchmark_phases.csv`（per-round per-phase 延迟和 QPS）

## 测试

```bash
# C++ 单元测试 (68) + Python 单元测试 (53)
cd tests/kvtest
bash tests/run_all_tests.sh

# 集成测试（需要真实集群环境）
bash tests/test_cpu_affinity.sh   # CPU 绑核验证
bash tests/test_deploy.sh          # 多节点部署验证
bash tests/test_e2e.sh             # 端到端验收测试

# Worker/Coordinator 独立部署 + 服务发现模拟测试
bash tests/test_standalone_mode.sh
# 覆盖场景：Coordinator 注册/心跳/反注册、Worker 从服务发现获取 Coordinator、
#           Coordinator 崩溃 + TTL 过期、Coordinator 重启恢复
```

## 文档

| 文档 | 内容 |
|------|------|
| [docs/user-guide.md](docs/user-guide.md) | 编译部署、配置参数、远程部署、指标采集、故障排查 |
| [docs/pipeline-guide.md](docs/pipeline-guide.md) | Pipeline 模式：Writer/Reader 角色、QPS 控制、多实例部署 |
| [docs/cache-guide.md](docs/cache-guide.md) | Cache 模式：cacheGetOrCreate、命中率控制、Key Pool 管理 |
| [docs/benchmark-guide.md](docs/benchmark-guide.md) | Benchmark 模式：8 种 Set/Get/Mixed 测试模式、per-phase 计时 |
| [docs/design.md](docs/design.md) | 架构设计：模块设计、线程模型、指标系统、QPS 控制机制 |
| [docs/jf-integration-design.md](docs/jf-integration-design.md) | 独立部署 + 服务发现模拟：JfClient、mock server、deploy 脚本 standalone 模式、E2E 测试 |
