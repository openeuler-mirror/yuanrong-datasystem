# 基于 openYuanrong 的 GLM-5 W4A8 单实例部署

## 概述

本指南提供在 Atlas 800I A2 服务器（双机）或 Atlas 800I A3 / Atlas 800T A3 服务器（单机）上部署 GLM-5 W4A8 模型并使用
openYuanrong Datasystem 作为 KV Pool 后端的详细步骤。

GLM-5 是采用混合专家架构的高效推理模型，专为复杂系统工程和长时序智能体任务设计。使用 openYuanrong 作为
KV Pool 后端可以实现高效的 KV Cache 存储和请求间的复用。

**部署全景流程**：

1. 环境要求确认
2. 准备模型权重
3. 拉取镜像与创建容器
4. 容器内环境配置（升级依赖 + 打补丁，若vllm-ascend版本>= 0.20则跳过）
5. 安装 openYuanrong Datasystem
6. 启动 openYuanrong Worker
7. 启动VLLM推理服务
8. 功能验证

## 环境准备

### 硬件要求

- **A3 单机**：1 × Atlas 800I A3 或 Atlas 800T A3 服务器，配备 8 张 NPU 卡（每张 128G 显存）
- **A2 双机**：2 × Atlas 800I A2 服务器，每台配备 8 张 NPU 卡（每张 64G 显存）
- 已配置 RoCE 或灵衢网络以获得最佳性能

### 软件要求

采用模型配套的Docker镜像，软件版本与 Docker 镜像内置版本保持一致，确保hdk、固件等软件在配套范围内。
此外：CANN版本要求至少高于8.5.0，HDK版本要求至少高于25.2.3（启用 RH2D，版本需要 25.5.0 以上）。

执行`npu-smi info`查看

```
(base) xx@A800T-03:~$ npu-smi info
+------------------------------------------------------------------------------------------------+ | npu-smi 25.5.0                   Version: 25.5.0 | +---------------------------+---------------+----------------------------------------------------+
```

### 模型权重

下载 GLM-5 W4A8 模型权重并放置到指定目录，如 `/home/models/GLM-5-w4a8/`。

> **模型下载地址**：[魔搭社区](https://modelscope.cn/models/Eco-Tech/GLM-5-w4a8)

## 使用 Docker 运行

本教程使用的 Docker 镜像版本为 `vllm-ascend:0.18.0rc1`。如本地尚未下载，可先按机器类型执行以下命令：

```bash

# A2 双机

docker pull quay.io/ascend/vllm-ascend:0.18.0rc1

# A3 单机

docker pull quay.io/ascend/vllm-ascend:0.18.0rc1-a3
```

如果下载较慢，可将 `quay.io` 替换为 `m.daocloud.io/quay.io` 或 `quay.nju.edu.cn` 以加速拉取。更多镜像说明可参考[安装文档](../../installation.md#set-up-using-docker)。

### 运行容器

#### A3 单机

可参考以下脚本启动 A3 容器。先将下面内容保存为 `start-docker.sh`：

```bash
#!/bin/bash

IMAGES_ID="$1"
NAME="$2"

# 检查参数数量（需要 2 个：镜像 ID 和容器名）

if [ $# -ne 2 ]; then
    echo "error: 需要传入2个参数，格式：$0 <镜像ID> <容器名>"
    exit 1
fi

# 检查镜像是否存在

if ! docker images --format "{{.ID}}" | grep -q "^${IMAGES_ID:0:12}$"; then
    echo "error: 镜像ID $IMAGES_ID 不存在"
    exit 1
fi

docker run --name "${NAME}" -it -d --net=host --shm-size=500g \
    --privileged=true \
    -w /home \
    --device=/dev/davinci_manager \
    --device=/dev/hisi_hdc \
    --device=/dev/devmm_svm \
    --entrypoint=bash \
    -v /usr/local/Ascend/driver:/usr/local/Ascend/driver \
    -v /usr/local/dcmi:/usr/local/dcmi \
    -v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi \
    -v /etc/ascend_install.info:/etc/ascend_install.info \
    -v /usr/local/sbin:/usr/local/sbin \
    -v /etc/hccn.conf:/etc/hccn.conf \
    -v /home:/home \
    -v /mnt:/mnt \
    -v /tmp:/tmp \
    -v /data:/data \
    -v /usr/share/zoneinfo/Asia/Shanghai:/etc/localtime \
    -e http_proxy="$http_proxy" \
    -e https_proxy="$https_proxy" \
    "${IMAGES_ID}"
```

拉取镜像后，可先通过以下命令查看镜像 ID：

```bash
docker images | grep vllm-ascend
```

然后执行以下命令创建容器：

```bash
bash start-docker.sh ac1c767e5aa2 glm5-v0.18.0rc1-a3
```

创建完成后进入容器：

```bash
docker exec -it glm5-v0.18.0rc1-a3 bash
```

#### A2 双机

A2 双机场景也可采用与 A3 类似的脚本方式。在两个节点分别保存同一份 `start-docker.sh`，内容如下：

```bash
#!/bin/bash

IMAGES_ID="$1"
NAME="$2"

# 检查参数数量（需要 2 个：镜像 ID 和容器名）

if [ $# -ne 2 ]; then
    echo "error: 需要传入2个参数，格式：$0 <镜像ID> <容器名>"
    exit 1
fi

# 检查镜像是否存在

if ! docker images --format "{{.ID}}" | grep -q "^${IMAGES_ID:0:12}$"; then
    echo "error: 镜像ID $IMAGES_ID 不存在"
    exit 1
fi

docker run --name "${NAME}" -it -d --net=host --shm-size=500g \
    --privileged=true \
    -w /home \
    --device=/dev/davinci_manager \
    --device=/dev/hisi_hdc \
    --device=/dev/devmm_svm \
    --entrypoint=bash \
    -v /usr/local/Ascend/driver:/usr/local/Ascend/driver \
    -v /usr/local/dcmi:/usr/local/dcmi \
    -v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi \
    -v /etc/ascend_install.info:/etc/ascend_install.info \
    -v /usr/local/sbin:/usr/local/sbin \
    -v /etc/hccn.conf:/etc/hccn.conf \
    -v /home:/home \
    -v /mnt:/mnt \
    -v /tmp:/tmp \
    -v /data:/data \
    -v /usr/share/zoneinfo/Asia/Shanghai:/etc/localtime \
    -e http_proxy="$http_proxy" \
    -e https_proxy="$https_proxy" \
    "${IMAGES_ID}"
```

两个节点都先执行以下命令查看各自本机的镜像 ID：

```bash
docker images | grep vllm-ascend
```

然后分别创建容器，例如：

```bash

# 节点 0

bash start-docker.sh ac1c767e5aa2 glm5-v0.18.0rc1-a2-node0

# 节点 1

bash start-docker.sh ac1c767e5aa2 glm5-v0.18.0rc1-a2-node1
```

创建完成后分别进入容器：

```bash

# 节点 0

docker exec -it glm5-v0.18.0rc1-a2-node0 bash

# 节点 1

docker exec -it glm5-v0.18.0rc1-a2-node1 bash
```

### 容器内环境配置

以下操作均在容器内执行，**若vllm-ascend版本>= 0.20则跳过**

#### 升级 transformers 版本

GLM-5 模型要求较高版本的 transformers，进入容器后需先升级：

```
pip install transformers==5.2.0 --no-deps --force-reinstall
pip install huggingface_hub==1.5.0 --no-deps --force-reinstall
```

#### 应用补丁

使用 openYuanrong 多级缓存前，需要根据镜像版本打对应的补丁。建议先将补丁文件上传到容器内固定目录 `/workspace/yuanrong_patches/`，然后按版本执行。

**准备工作**（所有版本通用）：

```bash
mkdir -p /workspace/yuanrong_patches

git config --global user.email "deploy@local"
git config --global user.name "deploy"
```

##### vllm-ascend:0.18.0 补丁

| 补丁文件 | 目标仓库 | 用途 |
|----------|----------|------|
| `0001-Bugfix-Fix-negative-local_cache_hit-in-P-D-disaggreg.patch` | `/vllm-workspace/vllm` | 修复 `local_cache_hit` 指标出现负值的问题 |
| `0001-Implement-yuanrong-backend.patch` | `/vllm-workspace/vllm-ascend` | 补充 openYuanrong backend 支持 |
| `0001-fix-kv-pool-update-yuanrong-backend-handling.patch` | `/vllm-workspace/vllm-ascend` | 修复超过10000个对象时分批传输问题 |

执行步骤
```bash

# vllm patch

cd /vllm-workspace/vllm
git am /workspace/yuanrong_patches/0001-Bugfix-Fix-negative-local_cache_hit-in-P-D-disaggreg.patch

# vllm-ascend patches

cd /vllm-workspace/vllm-ascend
git am /workspace/yuanrong_patches/0001-Implement-yuanrong-backend.patch
git am /workspace/yuanrong_patches/0001-fix-kv-pool-update-yuanrong-backend-handling.patch
```

##### vllm-ascend:0.18.0rc1 补丁

> **注意**：0.18.0rc1 版本的补丁与 0.18.0 正式版不同，请使用对应版本的补丁文件。若环境中已包含这些补丁的改动，可跳过此步骤。

| 补丁文件 | 目标仓库 | 用途 |
|----------|----------|------|
| `0001-Bugfix-Fix-negative-local_cache_hit-in-P-D-disaggreg.patch` | `/vllm-workspace/vllm` | 修复 `local_cache_hit` 指标出现负值的问题 |
| `0001-Implement-yuanrong-backend.patch` | `/vllm-workspace/vllm-ascend` | 补充 openYuanrong backend 支持 |
| `0001-BugFix-0.18.0-KV-Pool-Fix-KV-Pool-not-putting-kv-cac.patch` | `/vllm-workspace/vllm-ascend` | 修复 vLLM v0.18.0 在 speculative decoding 场景下 KV Pool 未正确执行 KV Cache put / finalize 的问题，并规避后续 vLLM metrics 统计相关报错 |

执行步骤
```bash

# vllm patch

cd /vllm-workspace/vllm
git am /workspace/yuanrong_patches/0001-Bugfix-Fix-negative-local_cache_hit-in-P-D-disaggreg.patch

# vllm-ascend patches

cd /vllm-workspace/vllm-ascend
git am /workspace/yuanrong_patches/0001-Implement-yuanrong-backend.patch
git am /workspace/yuanrong_patches/0001-BugFix-0.18.0-KV-Pool-Fix-KV-Pool-not-putting-kv-cac.patch
```

## 安装 openYuanrong Datasystem

### 检查页大小

openYuanrong 默认提供的是面向 **4 KB 内存页大小**机器的安装包。如果目标机器使用 **64 KB 页大小**，则需要使用额外编译的安装包，默认包可能无法正常安装或运行。

查看页大小：

```bash
getconf PAGE_SIZE
```

如果输出为 `65536`，请使用针对 64 KB 页大小单独编译的 openYuanrong 安装包。

### 在线安装

```bash
pip install openyuanrong-datasystem
```

### 离线安装

如果目标环境无外网权限，可以从以下地址下载 whl 包进行离线安装：

```bash

# 下载 whl 包

wget https://gitcode.com/openeuler/yuanrong-datasystem/releases/download/v0.7.6.rc1/openyuanrong_datasystem-0.7.6rc1-cp311-cp311-manylinux_2_35_aarch64.whl

# 安装

pip install openyuanrong_datasystem-0.7.6rc1-cp311-cp311-manylinux_2_35_aarch64.whl
```

验证安装：

```bash
python -c "import yr.datasystem; print('Yuanrong Datasystem 安装成功')"
```

## 安装 etcd

后续 openYuanrong 服务启动脚本依赖 `etcd` 和 `etcdctl`。请在需要启动 `etcd` 的节点或容器中先完成安装：

- A3 单机场景安装一次即可。
- A2 双机场景至少在节点 0 安装。

```bash
ETCD_VERSION="v3.5.12"
if [ "$(uname -m)" = "aarch64" ]; then
  ETCD_ARCH="linux-arm64"
else
  ETCD_ARCH="linux-amd64"
fi
wget https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-${ETCD_ARCH}.tar.gz
tar -xvf etcd-${ETCD_VERSION}-${ETCD_ARCH}.tar.gz
cd etcd-${ETCD_VERSION}-${ETCD_ARCH}
sudo cp etcd etcdctl /usr/local/bin/
```

如果当前容器以 `root` 用户运行且未安装 `sudo`，可以直接执行：

```bash
cp etcd etcdctl /usr/local/bin/
```

验证安装：

```bash
etcd --version
etcdctl version
```

## 启动 openYuanrong 服务

### A3 单机

A3 单机部署只需在一个节点启动 etcd 和 Datasystem Worker。

创建启动脚本 `run_yr_a3.sh`：

```bash
#!/bin/bash

# 配置参数

export HOST_IP="<您的节点IP地址>"
export ETCD_IP="${HOST_IP}"
export WORKER_PORT=18481
export ETCD_PORT=2379
export SHM_SIZE=512000
export NODE_TIMEOUT=30
export NODE_DEAD_TIMEOUT=60
export LIVENESS_PATH=/workspace/liveness

# 启动 etcd（单实例模式）

etcd \
  --name etcd-single \
  --data-dir /tmp/etcd-data \
  --listen-client-urls http://<IP_ADDRESS>:2379 \
  --advertise-client-urls http://${ETCD_IP}:2379 \
  --listen-peer-urls http://<IP_ADDRESS>:2380 \
  --initial-advertise-peer-urls http://${ETCD_IP}:2380 \
  --initial-cluster etcd-single=http://${ETCD_IP}:2380 \
  > /tmp/etcd.log 2>&1 &

# 等待 etcd 启动

sleep 3

# 验证 etcd 是否正常运行

etcdctl --endpoints "${ETCD_IP}:2379" put key "value"
etcdctl --endpoints "${ETCD_IP}:2379" get key

# 启动 Datasystem Worker

dscli start -w \
  --worker_address ${HOST_IP}:${WORKER_PORT} \
  --etcd_address ${ETCD_IP}:${ETCD_PORT} \
  --shared_memory_size_mb ${SHM_SIZE} \
  --node_timeout_s ${NODE_TIMEOUT} \
  --node_dead_timeout_s ${NODE_DEAD_TIMEOUT} \
  --liveness_check_path ${LIVENESS_PATH}

echo "Yuanrong 服务启动完成"
echo "etcd 日志: /tmp/etcd.log"
```

运行脚本：

```bash
bash run_yr_a3.sh
```

### A2 双机

A2 双机部署需要在两个节点都启动 Datasystem Worker，并连接同一个 etcd。

**节点 0** 创建启动脚本 `run_yr_a2_node0.sh`（启动 etcd 和 Worker）：

```bash
#!/bin/bash

# 配置参数

export HOST_IP="<IP_ADDRESS>"
export ETCD_IP="${HOST_IP}"
export WORKER_PORT=18481
export ETCD_PORT=2379
export SHM_SIZE=512000
export NODE_TIMEOUT=30
export NODE_DEAD_TIMEOUT=60
export LIVENESS_PATH=/workspace/liveness

# 启动 etcd（单实例模式）

etcd \
  --name etcd-single \
  --data-dir /tmp/etcd-data \
  --listen-client-urls http://<IP_ADDRESS>:2379 \
  --advertise-client-urls http://${ETCD_IP}:2379 \
  --listen-peer-urls http://<IP_ADDRESS>:2380 \
  --initial-advertise-peer-urls http://${ETCD_IP}:2380 \
  --initial-cluster etcd-single=http://${ETCD_IP}:2380 \
  > /tmp/etcd.log 2>&1 &

# 等待 etcd 启动

sleep 3

# 验证 etcd 是否正常运行

etcdctl --endpoints "${ETCD_IP}:2379" put key "value"
etcdctl --endpoints "${ETCD_IP}:2379" get key

# 启动 Datasystem Worker

dscli start -w \
  --worker_address ${HOST_IP}:${WORKER_PORT} \
  --etcd_address ${ETCD_IP}:${ETCD_PORT} \
  --shared_memory_size_mb ${SHM_SIZE} \
  --node_timeout_s ${NODE_TIMEOUT} \
  --node_dead_timeout_s ${NODE_DEAD_TIMEOUT} \
  --liveness_check_path ${LIVENESS_PATH}

echo "节点 0 Yuanrong 服务启动完成"
echo "etcd 日志: /tmp/etcd.log"
```

**节点 1** 创建启动脚本 `run_yr_a2_node1.sh`（只启动 Worker，连接节点 0 的 etcd）：

```bash
#!/bin/bash

# 配置参数

export HOST_IP="<IP_ADDRESS>"
export ETCD_IP="<IP_ADDRESS>"
export WORKER_PORT=18481
export ETCD_PORT=2379
export SHM_SIZE=512000
export NODE_TIMEOUT=30
export NODE_DEAD_TIMEOUT=60
export LIVENESS_PATH=/workspace/liveness

# 启动 Datasystem Worker（连接节点 0 的 etcd）

dscli start -w \
  --worker_address ${HOST_IP}:${WORKER_PORT} \
  --etcd_address ${ETCD_IP}:${ETCD_PORT} \
  --shared_memory_size_mb ${SHM_SIZE} \
  --node_timeout_s ${NODE_TIMEOUT} \
  --node_dead_timeout_s ${NODE_DEAD_TIMEOUT} \
  --liveness_check_path ${LIVENESS_PATH}

echo "节点 1 Yuanrong 服务启动完成"
```

运行脚本：

```bash

# 节点 0 先启动

bash run_yr_a2_node0.sh

# 节点 1 后启动

bash run_yr_a2_node1.sh
```

### etcd 参数说明

| 参数 | 值 | 说明 |
|------|-----|------|
| name | etcd-single | etcd 节点名称，集群中必须唯一 |
| data-dir | /tmp/etcd-data | 数据存储目录，用于持久化保存 etcd 数据 |
| listen-client-urls | http://<IP_ADDRESS>:2379 | 监听客户端请求的 URL 地址，`<IP_ADDRESS>` 表示监听所有网卡 |
| advertise-client-urls | http://${ETCD_IP}:2379 | 对外广播的客户端 URL，其他节点通过此地址连接 |
| listen-peer-urls | http://<IP_ADDRESS>:2380 | 监听集群节点间通信的 URL 地址 |
| initial-advertise-peer-urls | http://${ETCD_IP}:2380 | 对外广播的集群通信 URL，其他 etcd 节点通过此地址进行数据同步 |
| initial-cluster | etcd-single=http://${ETCD_IP}:2380 | 初始集群配置，格式为 `节点名=URL`，多节点时用逗号分隔 |

> **参考文档**：[etcd 官方文档](https://etcd.io/docs/)，了解更多参数配置。
>
> **生产环境建议**：上述示例为单实例部署，适用于测试和开发环境。对于可靠性要求较高的生产环境，建议部署 etcd 集群（通常 3 或 5 个节点）。集群部署请参考 [etcd 集群部署指南](https://etcd.io/docs/latest/op-guide/clustering/)。

### Datasystem Worker 参数说明

| 参数 | 值 | 说明 |
|------|-----|------|
| worker_address | ${HOST_IP}:${WORKER_PORT} | Worker 服务地址和端口 |
| etcd_address | ${ETCD_IP}:${ETCD_PORT} | etcd 服务发现地址 |
| shared_memory_size_mb | ${SHM_SIZE} (512000) | 共享内存大小（500 GB） |
| node_timeout_s | ${NODE_TIMEOUT} (30) | 节点超时时间（秒） |
| node_dead_timeout_s | ${NODE_DEAD_TIMEOUT} (60) | 节点死亡超时时间（秒） |
| liveness_check_path | ${LIVENESS_PATH} | 存活检查路径 |

> **参考文档**：[openYuanrong Datasystem 文档](https://atomgit.com/openeuler/yuanrong-datasystem)，了解更多 worker 参数和环境变量配置。

停止 Worker：

```bash
dscli stop --worker_address ${HOST_IP}:${WORKER_PORT}
```

## 部署 GLM-5 W4A8 与 openYuanrong KV Pool

### A3 单机部署

创建启动脚本 `run_glm5_w4a8_yuanrong_a3.sh`。下面示例参考你给出的单机 A3 启动参数，并补充 openYuanrong 相关配置：

```bash
#!/bin/bash

# NPU 性能优化配置

export HCCL_OP_EXPANSION_MODE="AIV"
export OMP_PROC_BIND=false
export OMP_NUM_THREADS=10
export HCCL_BUFFSIZE=256
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
export VLLM_ASCEND_BALANCE_SCHEDULING=1

# vLLM 配置

export VLLM_USE_V1=1
export VLLM_ENGINE_READY_TIMEOUT_S=1800
export PYTHONHASHSEED=0

# openYuanrong Datasystem 配置

export DS_WORKER_ADDR="<节点IP>:18481"
export DS_H2D_MEMCPY_POLICY="direct"
export DS_D2H_MEMCPY_POLICY="direct"
unset GOOGLE_LOGTOSTDERR GOOGLE_ALSOLOGTOSTDERR

MODEL_PATH="/data/GLM-5-w4a8"

vllm serve $MODEL_PATH \
  --host <IP_ADDRESS> \
  --port 8077 \
  --data-parallel-size 2 \
  --tensor-parallel-size 8 \
  --enable-expert-parallel \
  --seed 1024 \
  --served-model-name glm-5 \
  --max-num-seqs 48 \
  --max-model-len 202752 \
  --max-num-batched-tokens 8192 \
  --trust-remote-code \
  --gpu-memory-utilization 0.92 \
  --quantization ascend \
  --enable-chunked-prefill \
  --enable-prefix-caching \
  --async-scheduling \
  --enable-auto-tool-choice \
  --tool-call-parser glm47 \
  --reasoning-parser glm45 \
  --additional-config '{"enable_npugraph_ex": true, "fuse_muls_add": true, "multistream_overlap_shared_expert": true}' \
  --compilation-config '{"cudagraph_mode": "FULL_DECODE_ONLY"}' \
  --speculative-config '{"num_speculative_tokens": 3, "method": "deepseek_mtp"}' \
  --kv-transfer-config '{
      "kv_connector": "AscendStoreConnector",
      "kv_role": "kv_both",
      "kv_connector_extra_config": {
          "lookup_rpc_port": "0",
          "backend": "yuanrong"
      }
  }' 2>&1 | tee ./glm-5_yuanrong_a3.log
```

### A2 双机部署

在两个节点上分别创建启动脚本。下面示例参考实际可用配置，使用前请根据 `ifconfig` 输出、模型目录和日志命名习惯修改。

**节点 0** 创建 `run_glm5_w4a8_yuanrong_a2_node0.sh`：

```bash
#!/bin/bash

# 通过 ifconfig 获取

# nic_name 为当前节点 local_ip 对应的网卡名称

nic_name="bond0"
local_ip="<IP_ADDRESS>"

# node0_ip 必须与节点 0（主节点）脚本中的 local_ip 保持一致

node0_ip="<IP_ADDRESS>"

export HCCL_OP_EXPANSION_MODE="AIV"

export HCCL_IF_IP=$local_ip
export GLOO_SOCKET_IFNAME=$nic_name
export TP_SOCKET_IFNAME=$nic_name
export HCCL_SOCKET_IFNAME=$nic_name

# NPU 性能优化配置

export OMP_PROC_BIND=false
export OMP_NUM_THREADS=10
export HCCL_BUFFSIZE=256
export VLLM_ASCEND_BALANCE_SCHEDULING=1
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True

# vLLM 配置

export VLLM_USE_V1=1
export VLLM_ENGINE_READY_TIMEOUT_S=1800
export PYTHONHASHSEED=0

# openYuanrong Datasystem 配置

export DS_WORKER_ADDR="${local_ip}:18481"
export DS_H2D_MEMCPY_POLICY="direct"
export DS_D2H_MEMCPY_POLICY="direct"
unset GOOGLE_LOGTOSTDERR GOOGLE_ALSOLOGTOSTDERR

MODEL_PATH="/home/models/GLM-5-w4a8"

vllm serve $MODEL_PATH \
  --host <IP_ADDRESS> \
  --port 1025 \
  --data-parallel-size 2 \
  --data-parallel-size-local 1 \
  --data-parallel-address $node0_ip \
  --data-parallel-rpc-port 12890 \
  --tensor-parallel-size 8 \
  --quantization ascend \
  --seed 1024 \
  --served-model-name glm-5 \
  --enable-expert-parallel \
  --max-num-seqs 40 \
  --max-model-len 202752 \
  --max-num-batched-tokens 4096 \
  --trust-remote-code \
  --gpu-memory-utilization 0.92 \
  --enable-chunked-prefill \
  --enable-prefix-caching \
  --async-scheduling \
  --enable-auto-tool-choice \
  --tool-call-parser glm47 \
  --reasoning-parser glm45 \
  --additional-config '{"multistream_overlap_shared_expert":true, "fuse_qknorm_rope": false, "fuse_muls_add": true, "enable_npugraph_ex": true}' \
  --compilation-config '{"cudagraph_capture_sizes": [1,4,8,12,16,20,24,28,32,36,40,48,56,64,80,96], "cudagraph_mode": "FULL_DECODE_ONLY"}' \
  --speculative-config '{"num_speculative_tokens": 3, "method": "deepseek_mtp"}' \
  --kv-transfer-config '{
      "kv_connector": "AscendStoreConnector",
      "kv_role": "kv_both",
      "kv_connector_extra_config": {
          "lookup_rpc_port": "0",
          "backend": "yuanrong"
      }
  }' 2>&1 | tee ./glm-5_yuanrong_a2_node0.log
```

**节点 1** 创建 `run_glm5_w4a8_yuanrong_a2_node1.sh`：

```bash
#!/bin/bash

# 通过 ifconfig 获取

# nic_name 为当前节点 local_ip 对应的网卡名称

nic_name="enp61s0f2"
local_ip="<IP_ADDRESS>"

# node0_ip 必须与节点 0（主节点）脚本中的 local_ip 保持一致

node0_ip="<IP_ADDRESS>"

export HCCL_OP_EXPANSION_MODE="AIV"

export HCCL_IF_IP=$local_ip
export GLOO_SOCKET_IFNAME=$nic_name
export TP_SOCKET_IFNAME=$nic_name
export HCCL_SOCKET_IFNAME=$nic_name

# NPU 性能优化配置

export OMP_PROC_BIND=false
export OMP_NUM_THREADS=10
export HCCL_BUFFSIZE=256
export VLLM_ASCEND_BALANCE_SCHEDULING=1
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True

# vLLM 配置

export VLLM_USE_V1=1
export VLLM_ENGINE_READY_TIMEOUT_S=1800
export PYTHONHASHSEED=0

# openYuanrong Datasystem 配置

export DS_WORKER_ADDR="${local_ip}:18481"
export DS_H2D_MEMCPY_POLICY="direct"
export DS_D2H_MEMCPY_POLICY="direct"
unset GOOGLE_LOGTOSTDERR GOOGLE_ALSOLOGTOSTDERR

MODEL_PATH="/home/models/GLM-5-w4a8"

vllm serve $MODEL_PATH \
  --host <IP_ADDRESS> \
  --port 1026 \
  --headless \
  --data-parallel-size 2 \
  --data-parallel-size-local 1 \
  --data-parallel-start-rank 1 \
  --data-parallel-address $node0_ip \
  --data-parallel-rpc-port 12890 \
  --tensor-parallel-size 8 \
  --quantization ascend \
  --seed 1024 \
  --served-model-name glm-5 \
  --enable-expert-parallel \
  --max-num-seqs 40 \
  --max-model-len 202752 \
  --max-num-batched-tokens 4096 \
  --trust-remote-code \
  --gpu-memory-utilization 0.92 \
  --enable-chunked-prefill \
  --enable-prefix-caching \
  --async-scheduling \
  --enable-auto-tool-choice \
  --tool-call-parser glm47 \
  --reasoning-parser glm45 \
  --additional-config '{"multistream_overlap_shared_expert":true, "fuse_qknorm_rope": false, "fuse_muls_add": true, "enable_npugraph_ex": true}' \
  --compilation-config '{"cudagraph_capture_sizes": [1,4,8,12,16,20,24,28,32,36,40,48,56,64,80,96], "cudagraph_mode": "FULL_DECODE_ONLY"}' \
  --speculative-config '{"num_speculative_tokens": 3, "method": "deepseek_mtp"}' \
  --kv-transfer-config '{
      "kv_connector": "AscendStoreConnector",
      "kv_role": "kv_both",
      "kv_connector_extra_config": {
          "lookup_rpc_port": "1",
          "backend": "yuanrong"
      }
  }' 2>&1 | tee ./glm-5_yuanrong_a2_node1.log
```

### 配置参数说明

#### A3 单机参数

| 参数 | 值 | 说明 |
|------|-----|------|
| tensor-parallel-size | 8 | 单实例使用 8 路张量并行 |
| data-parallel-size | 2 | 单机内数据并行大小 |
| max-model-len | 202752 | 最大上下文长度 |
| max-num-batched-tokens | 8192 | 最大批处理 token 数 |
| max-num-seqs | 48 | 最大并发序列数 |
| gpu-memory-utilization | 0.92 | GPU 显存利用率 |
| quantization | ascend | 使用 Ascend 量化 |
| enable-expert-parallel | (标志) | 启用 MoE 专家并行 |
| enable-chunked-prefill | (标志) | 启用分块预填充 |
| enable-prefix-caching | (标志) | 启用前缀缓存 |
| async-scheduling | (标志) | 启用异步调度 |
| enable-auto-tool-choice | (标志) | 启用自动工具选择 |
| tool-call-parser | glm47 | 工具调用解析器 |
| reasoning-parser | glm45 | reasoning 解析器 |
| kv_connector | AscendStoreConnector | 使用 AscendStoreConnector |
| kv_role | kv_both | 同时支持生产和消费 |
| backend | yuanrong | 使用 openYuanrong 后端 |
| lookup_rpc_port | 0 | RPC 查找端口 |

#### A2 双机参数

| 参数 | 值 | 说明 |
|------|-----|------|
| tensor-parallel-size | 8 | 每节点使用 8 张 NPU 卡 |
| data-parallel-size | 2 | 数据并行大小（2 节点） |
| data-parallel-size-local | 1 | 本地数据并行大小 |
| max-model-len | 202752 | 最大上下文长度 |
| max-num-batched-tokens | 4096 | 最大批处理 token 数 |
| max-num-seqs | 40 | 最大并发序列数 |
| gpu-memory-utilization | 0.92 | GPU 显存利用率 |
| quantization | ascend | 使用 Ascend 量化 |
| enable-expert-parallel | (标志) | 启用 MoE 专家并行 |
| enable-auto-tool-choice | (标志) | 启用自动工具选择 |
| tool-call-parser | glm47 | 工具调用解析器 |
| reasoning-parser | glm45 | reasoning 解析器 |
| lookup_rpc_port (node0) | 0 | 节点 0 RPC 查找端口 |
| lookup_rpc_port (node1) | 1 | 节点 1 RPC 查找端口 |

### 环境变量说明

| 环境变量 | 值 | 说明 |
|----------|-----|------|
| `HCCL_OP_EXPANSION_MODE` | AIV | HCCL 算子扩展模式（AI Vector 优化） |
| `OMP_PROC_BIND` | false | OpenMP 线程绑定配置 |
| `OMP_NUM_THREADS` | 示例中为 10 | OpenMP 线程数 |
| `HCCL_BUFFSIZE` | 示例中为 256 | HCCL 缓冲区大小 |
| `PYTORCH_NPU_ALLOC_CONF` | expandable_segments:True | NPU 显存分配策略（减少碎片） |
| `VLLM_ASCEND_BALANCE_SCHEDULING` | 1 | 启用平衡调度 |
| `VLLM_USE_V1` | 1 | 启用 vLLM v1 架构 |
| `VLLM_ENGINE_READY_TIMEOUT_S` | 1800 | 引擎就绪超时时间（秒） |
| `PYTHONHASHSEED` | 0 | Python 哈希种子，确保 KV Cache 键一致性 |
| `DS_WORKER_ADDR` | `${local_ip}:18481` | openYuanrong Worker 地址，必须与当前节点 `dscli` 启动参数一致 |
| `DS_H2D_MEMCPY_POLICY` | direct | Host-to-Device 内存拷贝策略 |
| `DS_D2H_MEMCPY_POLICY` | direct | Device-to-Host 内存拷贝策略 |

## 功能验证

服务启动后，验证部署是否成功。可先按部署场景设置服务端口：

```bash

# A3 单机示例

SERVICE_PORT=8077

# A2 双机通常验证节点 0

# SERVICE_PORT=1025

```

### 测试推理

```bash
curl -H "Accept: application/json" \
    -H "Content-type: application/json" \
    -X POST \
    -d '{
        "model": "glm-5",
        "messages": [{
            "role": "user",
            "content": "你好，请介绍一下人工智能的未来发展趋势。"
        }],
        "stream": false,
        "ignore_eos": false,
        "temperature": 0,
        "max_tokens": 200
    }' http://localhost:$SERVICE_PORT/v1/chat/completions
```

## 缓存命中率监控

### 查看 vLLM 日志

日志文件命名格式：

- A3 单机：`glm-5_yuanrong_a3.log`
- A2 双机节点 0：`glm-5_yuanrong_a2_node0.log`
- A2 双机节点 1：`glm-5_yuanrong_a2_node1.log`

```bash

# A3 可设为 glm-5_yuanrong_a3.log

# A2 节点 0 / 节点 1 分别设为 glm-5_yuanrong_a2_node0.log / glm-5_yuanrong_a2_node1.log

LOG_FILE=glm-5_yuanrong_a3.log

# 查看最新日志

tail -f $LOG_FILE

# 实时监控命中率相关日志

tail -f $LOG_FILE | grep -E "Prefix cache hit rate|External prefix cache hit rate|num_computed_tokens"
```

### 使用脚本持续监控命中率

如果当前环境包含 `vllm-ascend` 仓库源码，也可以直接使用仓库自带脚本持续观测命中率：

```bash

# 在 vllm-ascend 仓库根目录执行

bash tools/watch_cache_hit_rate.sh -u http://localhost:$SERVICE_PORT/metrics -i 10

# 如需将结果同时保存到文件

bash tools/watch_cache_hit_rate.sh \
  -u http://localhost:$SERVICE_PORT/metrics \
  -i 10 \
  -o cache_hit_rate.log
```

脚本会同时输出每个 engine 以及汇总行 `all` 的命中率，常用字段如下：

- `local_win`：vLLM 本地 Prefix Cache 的窗口命中率
- `local_total`：vLLM 本地 Prefix Cache 的累计命中率
- `ext_win`：openYuanrong 外部 KV Cache 的窗口命中率
- `ext_total`：openYuanrong 外部 KV Cache 的累计命中率
- `eff_total`：综合本地和外部缓存后的端到端有效命中率

### 缓存命中率指标说明

| 指标 | 说明 | 统计方式 |
|------|------|----------|
| Prefix cache hit rate | **HBM（本地显存）**命中率 | **滑动窗口**：最近 1000 个请求 |
| External prefix cache hit rate | **openYuanrong（外部 KV Cache）**命中率 | **累计统计**：从服务启动到当前时刻 |
| TTFT (Time to First Token) | 首个 token 延迟 | 单次请求指标，命中率高时 TTFT 显著降低 |

**统计周期说明**：
- **`Prefix cache hit rate`**：对应 HBM / 本地 Prefix Cache 命中率，来自 vLLM 上游 `CachingMetrics` 模块，使用滑动窗口统计最近 1000 个请求
- **`External prefix cache hit rate`**：对应 openYuanrong 外部 KV Cache 命中率，来自 Prometheus Counter 指标，**累计统计**从服务启动到当前时刻的总命中数和查询数
- 日志每 **10 秒** 输出一次 HBM 命中率
- HBM 命中率计算公式：`hit_rate = window_hits / window_queries`
- openYuanrong 命中率计算公式：`hit_rate = total_hits / total_queries`

**查看 openYuanrong 外部缓存命中率**：
```bash

# 通过 Prometheus metrics 查看外部缓存统计

curl http://localhost:$SERVICE_PORT/metrics | grep external_prefix_cache

# 输出示例：

# vllm:external_prefix_cache_queries_total{...} 10000

# vllm:external_prefix_cache_hits_total{...} 8500

# 命中率 = hits / queries = 85%

```

**注意**：外部缓存指标为 Prometheus Counter 类型，表示从服务启动以来的累计值。

**如何查看单次请求是否命中**：
```bash

# 查看单个请求的缓存命中情况

grep "num_computed_tokens" $LOG_FILE

# 如果 num_computed_tokens > 0，表示该请求命中了缓存

# num_computed_tokens 表示从缓存中复用的 token 数量

```

## 性能测试

### 选项一：快速测试

使用 `vllm bench serve` 对已启动的 OpenAI 兼容服务进行快速压测。下面示例默认以本机服务为目标：

```shell
BENCH_HOST=<IP_ADDRESS>
BENCH_PORT=1025
TOKENIZER_PATH=/home/models/GLM-5-w4a8

vllm bench serve \
  --backend openai-chat \
  --endpoint /v1/chat/completions \
  --dataset-name prefix_repetition \
  --prefix-repetition-prefix-len 31744 \
  --prefix-repetition-suffix-len 1024 \
  --prefix-repetition-output-len 2048 \
  --num-prompts 100 \
  --prefix-repetition-num-prefixes 5 \
  --ignore-eos \
  --model glm-5 \
  --tokenizer $TOKENIZER_PATH \
  --seed 1000 \
  --host $BENCH_HOST \
  --port $BENCH_PORT \
  --max-concurrency 10
```

参数含义如下：

| 参数 | 含义 |
|------|------|
| `--backend openai-chat` | 按 OpenAI Chat Completions 接口格式发请求 |
| `--endpoint /v1/chat/completions` | 指定压测目标接口路径 |
| `--dataset-name prefix_repetition` | 使用前缀重复数据集，便于观察缓存收益 |
| `--prefix-repetition-prefix-len 31744` | 共享前缀长度 |
| `--prefix-repetition-suffix-len 1024` | 每条请求的独立后缀长度 |
| `--prefix-repetition-output-len 2048` | 生成输出长度 |
| `--num-prompts 100` | 总请求数 |
| `--prefix-repetition-num-prefixes 5` | 共享前缀模板数量 |
| `--ignore-eos` | 忽略 EOS，尽量生成到目标长度 |
| `--model glm-5` | 请求时使用的模型名，需要与服务端 `served-model-name` 一致 |
| `--tokenizer $TOKENIZER_PATH` | tokenizer 路径，需指向本地 GLM-5 W4A8 模型目录 |
| `--seed 1000` | 固定随机种子，便于复现 |
| `--host $BENCH_HOST` | 压测目标地址 |
| `--port $BENCH_PORT` | 压测目标端口；A2 示例通常为 `1025`，A3 若按本文部署则改为 `8077` |
| `--max-concurrency 10` | 最大并发请求数 |

### 选项二：深度测试

使用 AISBench 进行深度性能测试：

```shell
ais_bench --models vllm_api_stream_chat \
  --datasets gsm8k_gen_0_shot_cot_str_perf \
  --debug --summarizer default_perf --mode perf
```

## 故障排除

### 常见问题

1. **etcd 连接失败**

   确保 etcd 正在运行且可访问：
```bash
   etcdctl --endpoints "${ETCD_IP}:2379" endpoint health
```

2. **Worker 注册失败**

   检查 Worker 地址是否正确配置：
```bash
   netstat -tlnp | grep 18481
```

3. **KV Cache 未找到**

   验证 `PYTHONHASHSEED` 设置一致：
```bash
   echo $PYTHONHASHSEED
```

4. **yr.datasystem 导入错误**

   确保 `openyuanrong-datasystem` 已安装：
```bash
   pip install openyuanrong-datasystem
   python -c "from yr.datasystem.hetero_client import HeteroClient; print('OK')"
```

5. **64 KB 页大小机器无法直接使用默认 openYuanrong 安装包**

   openYuanrong 默认提供的是面向 **4 KB 内存页大小**机器的安装包。如果目标机器使用 **64 KB 页大小**，则需要使用额外编译的安装包，默认包可能无法正常安装或运行。

   可先检查页大小：
```bash
   getconf PAGE_SIZE
```

   如果输出为 `65536`，请使用针对 64 KB 页大小单独编译的 openYuanrong 安装包。

6. **引擎启动超时**

   增加 `VLLM_ENGINE_READY_TIMEOUT_S` 的值：
```bash
   export VLLM_ENGINE_READY_TIMEOUT_S=3600
```

7. **双机节点时间不一致**

   A2 双机场景下，建议两个节点的系统时间保持一致，否则可能影响日志对齐、问题定位以及部分依赖时间戳的排障判断。

   可分别在两个节点检查时间：
```bash
   date
   timedatectl
```

### 日志查看

查看 vLLM 日志以获取详细错误信息：
```bash
tail -f $LOG_FILE
```

## 参考资料

- [openYuanrong Datasystem 文档](https://atomgit.com/openeuler/yuanrong-datasystem)
- [etcd 文档](https://etcd.io/docs/)
- [vLLM Ascend 文档](https://docs.vllm.ai/projects/ascend/)
- [GLM-5 教程](../../tutorials/models/GLM5.md)