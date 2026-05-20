# kvtest

独立的 datasystem KVClient 性能测试工具，支持 Writer/Reader 角色分离、Cache 模式、多节点部署、K8s 自动发现。

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
| [docs/user-guide.md](docs/user-guide.md) | 完整操作指南：配置参数、6 种典型场景、远程部署、故障排查 |
| [docs/design.md](docs/design.md) | 架构设计：模块设计、线程模型、指标系统、QPS 控制机制 |
