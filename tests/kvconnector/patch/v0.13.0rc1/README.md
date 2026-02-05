# vllm-ascend v0.13.0rc1 + YuanRong KV Pool 调测指南

本文用于在 vllm-ascend v0.13.0rc1 上启用 YuanRong Datasystem 作为 KV Pool 后端进行调测。

## 依赖与说明

- Python 3.10~3.11
- CANN 8.3.rc2
- PyTorch 2.8.0 + torch-npu 2.8.0
- vLLM / vLLM-Ascend 版本：v0.13.0rc1

## 环境准备

使用 vllm-ascend v0.13.0rc1 镜像（或自行替换为内网镜像）：

```bash
export IMAGE=quay.io/ascend/vllm-ascend:v0.13.0rc1-openeuler

docker run \
--name "docker_name" \
--privileged -itu root -d --shm-size 64g \
--net=host \
--device=/dev/davinci0:/dev/davinci0 \
--device=/dev/davinci1:/dev/davinci1 \
--device=/dev/davinci2:/dev/davinci2 \
--device=/dev/davinci3:/dev/davinci3 \
--device=/dev/davinci4:/dev/davinci4 \
--device=/dev/davinci5:/dev/davinci5 \
--device=/dev/davinci6:/dev/davinci6 \
--device=/dev/davinci7:/dev/davinci7 \
--device=/dev/davinci_manager:/dev/davinci_manager \
--device=/dev/devmm_svm:/dev/devmm_svm \
--device=/dev/hisi_hdc:/dev/hisi_hdc \
-v /usr/local/dcmi:/usr/local/dcmi \
-v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi \
-v /usr/local/Ascend/driver/lib64/:/usr/local/Ascend/driver/lib64/ \
-v /usr/local/Ascend/driver/version.info:/usr/local/Ascend/driver/version.info \
-v /usr/bin/hccn_tool:/usr/bin/hccn_tool \
-v /etc/ascend_install.info:/etc/ascend_install.info \
-v /root/.cache:/root/.cache \
-v /workspace:/workspace \
-it $IMAGE bash
```

## 编译安装（vllm-ascend）

1. 下载源码并切换版本：

```bash
git clone https://github.com/vllm-project/vllm-ascend.git
cd vllm-ascend
git checkout v0.13.0rc1
```

2. 应用补丁：

```bash
# 确保 git user.name / user.email 已配置
# 补丁路径：tests/kvconnector/patch/v0.13.0rc1/0001-Implement-yuanrong-backend.patch

git am /path/to/0001-Implement-yuanrong-backend.patch
```

3. 编译安装：

```bash
python setup.py develop
```

vLLM 可直接使用镜像自带版本，或按官方方式自行编译安装。

## 启用 YuanRong 作为 KV Pool 后端

### 1. 安装 Datasystem

```bash
pip install openyuanrong-datasystem
```

### 2. 启动 etcd

```bash
ETCD_VERSION="v3.5.12"
ETCD_ARCH="linux-arm64"
wget https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-${ETCD_ARCH}.tar.gz
tar -xvf etcd-${ETCD_VERSION}-${ETCD_ARCH}.tar.gz
cd etcd-${ETCD_VERSION}-${ETCD_ARCH}
sudo cp etcd etcdctl /usr/local/bin/

etcd \
  --name etcd-single \
  --data-dir /tmp/etcd-data \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://0.0.0.0:2380 \
  --initial-cluster etcd-single=http://0.0.0.0:2380 &

etcdctl --endpoints "127.0.0.1:2379" put key "value"
etcdctl --endpoints "127.0.0.1:2379" get key
```

### 3. 启动 Datasystem Worker

```bash
dscli start -w \
  --worker_address "${WORKER_IP}:31501" \
  --etcd_address "${ETCD_IP}:2379" \
  --shared_memory_size_mb 20480
```

常用可选参数（`-w` 必须放在命令行最后，以下参数需放在 `-w` 之后）：

- `--enable_huge_tlb true`
- `--log_dir /path/to/logs`

更多参数请参考 `docs/source_zh_cn/deployment/dscli.md`。

停止 worker：

```bash
dscli stop --worker_address "${WORKER_IP}:31501"
```

### 4. 环境变量

```bash
export PYTHONHASHSEED=0
export DS_WORKER_ADDR="${WORKER_IP}:31501"
# 可选（默认 0）
export DS_ENABLE_EXCLUSIVE_CONNECTION=0
# 远端 H2D（R2HD）开关
export DS_ENABLE_REMOTE_H2D=0
```

### 5. 启动 vLLM（指定 YuanRong backend）

```bash
python3 -m vllm.entrypoints.openai.api_server \
  --model /path/to/Qwen2.5-7B-Instruct \
  --port 8100 \
  --trust-remote-code \
  --enforce-eager \
  --no_enable_prefix_caching \
  --tensor-parallel-size 1 \
  --data-parallel-size 1 \
  --max-model-len 10000 \
  --block-size 128 \
  --max-num-batched-tokens 4096 \
  --kv-transfer-config \
  '{
    "kv_connector": "AscendStoreConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
      "lookup_rpc_port": "1",
      "backend": "yuanrong"
    }
  }'
```

KV Pool 关键参数说明：

- `kv_connector_extra_config`：KV Pool 的附加配置。
- `lookup_rpc_port`：scheduler 与 worker 的 RPC 端口，每个实例需唯一。
- `load_async`：是否启用异步加载，默认 `false`。
- `backend`：KV Pool 存储后端，使用 YuanRong 时设为 `yuanrong`。

为保证 KV Pool 的 key 哈希一致，所有节点需统一：

## KV Pool 规模与参数提示

- Key 数量与 `block_size`、请求长度、模型层数相关。
- Blob 大小与 KV cache shape、dtype 以及 token 范围相关。
- 实际负载下建议结合模型规模与并发量调大 `shared_memory_size_mb`。

## 验证

```bash
curl -s http://localhost:8100/v1/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"/path/to/Qwen2.5-7B-Instruct","prompt":"hello","max_tokens":16,"temperature":0.0}'
```
