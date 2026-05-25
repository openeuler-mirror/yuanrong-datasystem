# kvtest

独立的 datasystem KVClient 性能测试工具，支持 Writer/Reader 角色分离、Cache 模式、Benchmark Set/Get 模式、多节点部署、K8s 自动发现。

## 编译与运行

```bash
cd tests/kvtest

# 编译（使用默认 SDK 路径 ../../output/cpp）
./build.sh

# 或指定 SDK 路径
./build.sh -s /path/to/sdk

# 启动依赖
etcd &
mkdir -p /tmp/ds_worker && cd /tmp/ds_worker
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379

# 运行
cd tests/kvtest/output
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/my_config.json

# 查看统计 / 停止
curl http://127.0.0.1:9000/stats | python3 -m json.tool
curl -X POST http://127.0.0.1:9000/stop
```

## Benchmark Set/Get 模式

用于精确测量 Set/Get 吞吐和延迟，支持 6 种测试模式：

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

**测试模式：** `set_local` / `set_remote` / `get_local` / `get_cross_node` / `get_remote_direct` / `get_remote_cross`

**Set API：** `string_view`（直接写入）或 `create_buffer`（SHM Buffer 路径）

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
```

## 文档

| 文档 | 内容 |
|------|------|
| [docs/user-guide.md](docs/user-guide.md) | 编译部署、配置参数、远程部署、指标采集、故障排查 |
| [docs/pipeline-guide.md](docs/pipeline-guide.md) | Pipeline 模式：Writer/Reader 角色、QPS 控制、多实例部署 |
| [docs/cache-guide.md](docs/cache-guide.md) | Cache 模式：cacheGetOrCreate、命中率控制、Key Pool 管理 |
| [docs/benchmark-guide.md](docs/benchmark-guide.md) | Benchmark 模式：6 种 Set/Get 测试模式、per-phase 计时 |
| [docs/design.md](docs/design.md) | 架构设计：模块设计、线程模型、指标系统、QPS 控制机制 |
