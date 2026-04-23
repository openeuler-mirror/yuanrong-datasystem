# deploy.py kubectl 传输方式支持设计

**Goal:** 在 deploy.py 中新增 kubectl 传输方式，支持将 kvclient-standalone-test 部署到 k8s 容器中。

**文件:** `tests/kvclient_standalone/deploy.py`（修改）

## 背景

当前 deploy.py 仅支持 SSH/SCP 方式部署到远程节点。在 k8s 环境中，容器内无法配置 SSH，需要使用 `kubectl exec` 和 `kubectl cp` 替代。

## 方案

在 Deployer 类的 `run_on()`、`scp_to()`、`scp_from()` 三个底层方法中，根据 node 的 `transport` 字段分发到不同实现（SSH/kubectl/localhost）。上层 `deploy_node()`、`do_stop()`、`do_clean()` 逻辑不变。

## deploy.json 节点配置

### SSH 节点（向后兼容，默认）

```json
{ "host": "192.168.1.1", "instance_id": 0 }
{ "host": "192.168.1.2", "instance_id": 1, "ssh_user": "ubuntu" }
```

### kubectl 节点

```json
{
  "pod_name": "ds-worker-0",
  "pod_ip": "10.244.1.5",
  "namespace": "datasystem",
  "instance_id": 2,
  "transport": "kubectl"
}
```

### 字段说明

| 字段 | SSH 节点 | kubectl 节点 | 说明 |
|------|---------|-------------|------|
| `host` | 必填 | - | SSH 连接地址，也是 kvclient 通信地址 |
| `pod_name` | - | 必填 | kubectl exec/cp 的目标 Pod 名称 |
| `pod_ip` | - | 可选 | kvclient 实例间通信地址（Pod IP 或 DNS），未指定时用 pod_name |
| `namespace` | - | 可选 | kubectl 命名空间，默认 "default" |
| `transport` | 可选 | 必填 | `"ssh"` 或 `"kubectl"`，默认 `"ssh"` |
| `instance_id` | 必填 | 必填 | 实例编号 |

### 完整示例

```json
{
  "remote_work_dir": "/home/user/kvclient_test",
  "binary_path": "build/kvclient_standalone_test",
  "sdk_lib_dir": "third_party/sdk",
  "enable_procmon": true,
  "ssh_user": "root",
  "ssh_options": "-o StrictHostKeyChecking=no",
  "nodes": [
    { "host": "192.168.1.1", "instance_id": 0 },
    { "host": "192.168.1.2", "instance_id": 1, "ssh_user": "ubuntu" },
    {
      "pod_name": "ds-worker-0",
      "pod_ip": "10.244.1.5",
      "namespace": "datasystem",
      "instance_id": 2,
      "transport": "kubectl"
    }
  ]
}
```

## 命令映射

### run_on() — 执行远程命令

| transport | 命令 |
|-----------|------|
| localhost | `subprocess.run(cmd, shell=True)` |
| ssh | `ssh {options} {user}@{host} {cmd}` |
| kubectl | `kubectl exec {pod_name} -n {namespace} -- sh -c {cmd}` |

### scp_to() — 拷贝文件到远端

| transport | 命令 |
|-----------|------|
| localhost | `shutil.copy2(src, dst)` |
| ssh | `scp {options} -r {src} {user}@{host}:{dst}` |
| kubectl | `kubectl cp {src} {namespace}/{pod_name}:{dst}` |

kubectl cp 不支持 `-r` 对目录的递归拷贝，目录需要先 tar 再 cp：
- 如果 src 是文件：直接 `kubectl cp`
- 如果 src 是目录：先本地 `tar czf`，再 `kubectl cp` 传 tar 包，远端 `tar xzf` 解压

### collect_files() — 文件收集（替换原 scp_from）

统一使用 tar 策略（SSH 和 kubectl 都受益），替代原来的 glob + scp 逐文件拷贝：

1. **远端打包**：`run_on(..., "tar czf /tmp/collect_{iid}.tar.gz -C {work_dir} '*.csv' '*.txt' '*.log'")`
2. **拷贝到本地**（按 transport 分发）：
   - SSH: `scp {options} {user}@{host}:/tmp/collect_{iid}.tar.gz /tmp/`
   - kubectl: `kubectl cp {namespace}/{pod_name}:/tmp/collect_{iid}.tar.gz /tmp/`
   - localhost: `shutil.copy2`
3. **本地解压**：`tar xzf /tmp/collect_{iid}.tar.gz -C collected/{host}_{iid}/`
4. **清理远端**：`run_on(..., "rm -f /tmp/collect_{iid}.tar.gz")`

## config.json 简化

config.json.example 移除 `instance_id` 和 `nodes` 字段，由 deploy.py 从 deploy.json 自动注入：

**config.json.example（简化后）**：
```json
{
  "listen_port": 9000,
  "etcd_address": "127.0.0.1:2379",
  "cluster_name": "",
  "host_id_env_name": "",
  "connect_timeout_ms": 1000,
  "request_timeout_ms": 20,
  "data_sizes": ["8MB", "512KB"],
  "ttl_seconds": 5,
  "target_qps": 1600,
  "num_set_threads": 16,
  "notify_count": 10,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_{instance_id}.csv",
  "role": "writer",
  "pipeline": ["setStringView"],
  "notify_pipeline": ["getBuffer"]
}
```

**deploy.py generate_config() 注入**：
- `instance_id`: 从 node 的 `instance_id`
- `nodes`: 从 deploy.json 所有节点的通信地址生成（SSH 用 `host`，kubectl 用 `pod_ip || pod_name`）
- `peers`: 从 nodes 自动排除自身 instance_id 生成

## 实现改动点

1. **deploy.py**：
   - `_node_transport()` / `_node_target()` 辅助方法获取 transport 和目标标识
   - `run_on()` 新增 `transport`/`namespace`/`pod_name` 参数
   - `scp_to()` 支持 kubectl cp
   - `scp_from()` 改为 tar-based `collect_files()` 方法
   - `generate_config()` 注入 `nodes` 数组
   - 所有调用 `run_on()`/`scp_to()` 的地方传入 transport 信息

2. **config.json.example**：移除 `instance_id` 和 `nodes` 字段

3. **deploy.json.example**：添加 kubectl 节点示例

4. **C++ config.cpp**：不需要修改（`instance_id` 默认 0，`nodes` 为空时 peers 需要手动配置）
