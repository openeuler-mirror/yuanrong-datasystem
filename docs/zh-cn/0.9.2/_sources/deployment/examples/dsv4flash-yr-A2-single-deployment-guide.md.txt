# 基于 openYuanrong 的 DeepSeek-V4-Flash-w8a8-mtp 8机 A2 大EP PD分离部署

## 概述

本指南提供在 1 台 Atlas 800I A2 服务器上部署 DeepSeek-V4-Flash-w8a8-mtp 模型，使用 PD 混部并叠加 openYuanrong Datasystem 作为 KV Pool 后端的详细步骤。

**部署全景流程**：

前置检查：环境要求确认

1. 准备模型权重
2. 拉取镜像与创建容器
3. 安装 openYuanrong Datasystem
4. 安装并启动 etcd
5. 启动 openYuanrong Worker
6. 启动 VLLM 推理服务
7. 功能验证

## 环境要求

### 硬件要求

- **1 × Atlas 800I A2 服务器**，每台配备 8 张 NPU 卡（每张 64G 显存）
- 已配置 RoCE 网络以获得最佳性能

### 软件要求

采用模型配套的 Docker 镜像，软件版本与 Docker 镜像内置版本保持一致，确保 HDK、固件等软件在配套范围内。
此外：CANN 版本要求至少高于 8.5.0，HDK 版本要求至少高于 25.2.3（启用 RH2D 通过 RoCE 传输时，HDK 版本需要 25.5.0 以上）。

### 环境信息

| 组件 | 版本 | 备注 |
| --- | --- | --- |
| 服务器硬件 | Atlas 800I A2 × 8 | |
| vLLM-Ascend | vllm-ascend:v0.20.2rc1 | |
| CANN | ≥ 8.5.0 | |
| HDK | ≥ 25.2.3（启用 RH2D 需 ≥ 25.5.0） | RH2D 通过 RoCE 需要 HDK ≥ 25.5.0 |
| DeepSeek-V4-Flash-w8a8-mtp 权重 | w8a8-mtp | |

## 部署流程

### 1. 准备模型权重

下载 DeepSeek-V4-Flash-w8a8-mtp 模型权重并放置到指定目录，如 `/home/models/DeepSeek-V4-Flash-w8a8-mtp/`。

> **模型下载地址**：[魔搭社区](https://www.modelscope.cn/models/Eco-Tech/DeepSeek-V4-Flash-w8a8-mtp/files)

### 2. 拉取镜像与创建容器

#### 拉取镜像

本教程使用的 Docker 镜像版本为 `vllm-ascend:v0.20.2rc1`。如本地尚未下载，可执行以下命令：

```bash
docker pull quay.io/ascend/vllm-ascend:v0.20.2rc1
```

如果下载较慢，可将 `quay.io` 替换为 `m.daocloud.io/quay.io` 或 `quay.nju.edu.cn` 以加速拉取。更多镜像说明可参考[安装文档](../../installation.md#set-up-using-docker)。

#### 创建容器

在所有节点上分别保存同一份 `start-docker.sh`：

```bash
#!/bin/bash

IMAGES_ID="$1"
NAME="$2"

docker run --name "${NAME}" -it -d --net=host --shm-size=800g \
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

查看镜像 ID：

```bash
docker images | grep vllm-ascend
```

在每个节点分别创建容器：

```bash

# 在每个节点执行（替换为实际镜像ID）

bash start-docker.sh quay.io/ascend/vllm-ascend:v0.20.2rc1 ds_v4_flash_yr
```

进入容器：

```bash
docker exec -it ds_v4_flash_yr bash
```

#### 打补丁（根据版本选择）

需要根据镜像版本打对应的补丁。建议先将补丁文件上传到容器内固定目录 `/workspace/yuanrong_patches/`，然后按版本执行。

**准备工作**（所有版本通用）：

```bash
mkdir -p /workspace/yuanrong_patches

git config --global user.email "deploy@local"
git config --global user.name "deploy"
```

##### vllm-ascend:0.21 补丁

| 补丁文件 | 目标仓库 | 用途 |
|----------|----------|------|
| 0001-feat-kv-pool-support-load-failure-block-ids-for-yuan.patch | `/vllm-workspace/vllm-ascend` | 为 KV Pool 增加了对**非混合模型**（non-hybrid models）的 KV 加载失败块 ID（load failure block IDs）上报支持 |

```bash

# vllm-ascend patches

cd /vllm-workspace/vllm-ascend
git am /workspace/yuanrong_patches/0001-feat-kv-pool-support-load-failure-block-ids-for-yuan.patch
```

### 3. 安装 openYuanrong Datasystem

```bash
wget https://gitcode.com/openeuler/yuanrong-datasystem/releases/download/v0.7.6.rc1/openyuanrong_datasystem-0.7.6rc1-cp311-cp311-manylinux_2_35_aarch64.whl

pip install openyuanrong_datasystem-0.7.6rc1-cp311-cp311-manylinux_2_35_aarch64.whl
```

验证安装：

```bash
python -c "import yr.datasystem; print('openYuanrong Datasystem 安装成功')"
```

### 4. 安装并启动 etcd

#### 安装 etcd

后续 openYuanrong 服务启动脚本依赖 `etcd` 和 `etcdctl`。至少在 P 主节点安装。

```bash

# 1、下载etcd安装包（如果存在，则跳过）

ETCD_VERSION="v3.5.12"
if [ "$(uname -m)" = "aarch64" ]; then
  ETCD_ARCH="linux-arm64"
else
  ETCD_ARCH="linux-amd64"
fi
wget https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-${ETCD_ARCH}.tar.gz

# 2、解压并安装

tar -xvf etcd-${ETCD_VERSION}-${ETCD_ARCH}.tar.gz
cd etcd-${ETCD_VERSION}-${ETCD_ARCH}
cp etcd etcdctl /usr/local/bin/
```

验证安装：

```bash
etcd --version
etcdctl version
```

#### 启动 etcd

创建启动脚本 `run_etcd.sh`，在节点0启动 etcd，`ETCD_IP`默认设置为当前节点IP：

```sh
#!/bin/bash

export ETCD_IP=`hostname -I|awk -F " " '{print$1}'`
export ETCD_PORT=2379
export ETCD_PEER_PORT=2380

etcd \
  --name etcd-single \
  --data-dir /tmp/etcd-data \
  --listen-client-urls http://<IP_ADDRESS>:${ETCD_PORT} \
  --advertise-client-urls http://${ETCD_IP}:${ETCD_PORT} \
  --listen-peer-urls http://<IP_ADDRESS>:${ETCD_PEER_PORT} \
  --initial-advertise-peer-urls http://${ETCD_IP}:${ETCD_PEER_PORT} \
  --initial-cluster etcd-single=http://${ETCD_IP}:${ETCD_PEER_PORT} \
  > /tmp/etcd.log 2>&1 &

sleep 3

etcdctl --endpoints "${ETCD_IP}:${ETCD_PORT}" put key "value"
etcdctl --endpoints "${ETCD_IP}:${ETCD_PORT}" get key

echo "ETCD start finished, log dir: /tmp/etcd.log"
```

验证：

```sh

# 方式1，预期输出：{"health":"true","reason":""}

etcdctl --endpoints "${ETCD_IP}:${ETCD_PORT}" endpoint health

# 方式2，预期输出：100.100.xxx.xxx:2379 is healthy: successfully committed proposal: took = 1.43913ms

curl -L http://${ETCD_IP}:${ETCD_PORT}/health
```

**etcd 参数说明**：

| 参数 | 值 | 说明 |
| --- | --- | --- |
| name | etcd-single | etcd 节点名称，集群中必须唯一 |
| data-dir | /tmp/etcd-data | 数据存储目录，用于持久化保存 etcd 数据 |
| listen-client-urls | http://<IP_ADDRESS>:2379 | 监听客户端请求的 URL 地址 |
| advertise-client-urls | http://${ETCD_IP}:2379 | 对外广播的客户端 URL |
| listen-peer-urls | http://<IP_ADDRESS>:2380 | 监听集群节点间通信的 URL 地址 |
| initial-advertise-peer-urls | http://${ETCD_IP}:2380 | 对外广播的集群通信 URL |
| initial-cluster | etcd-single=http://${ETCD_IP}:2380 | 初始集群配置 |

> **参考文档**：[etcd 官方文档](https://etcd.io/docs/)
>
> **生产环境建议**：上述示例为单实例部署，适用于测试和开发环境。对于可靠性要求较高的生产环境，建议部署 etcd 集群（通常 3 或 5 个节点）。

### 5. 启动 openYuanrong Worker

每个节点创建启动脚本 `run_yr_worker.sh`：

> `SHM_SIZE`为当前机器的`DRAM`内存，默认为500G，需要根据当前机器可用内存修改

```bash
#!/bin/bash

export HOST_IP=`hostname -I|awk -F " " '{print$1}'`
export ETCD_IP=`hostname -I|awk -F " " '{print$1}'`
export WORKER_PORT=18481
export ETCD_PORT=2379
export SHM_SIZE=512000
export NODE_TIMEOUT=30
export NODE_DEAD_TIMEOUT=60
export LIVENESS_PATH=/workspace/liveness

dscli start -t 600 -w \
  --worker_address ${HOST_IP}:${WORKER_PORT} \
  --etcd_address ${ETCD_IP}:${ETCD_PORT} \
  --shared_memory_size_mb ${SHM_SIZE} \
  --node_timeout_s ${NODE_TIMEOUT} \
  --node_dead_timeout_s ${NODE_DEAD_TIMEOUT} \
  --liveness_check_path ${LIVENESS_PATH}

echo "Yuanrong service start finished!"
```

**Datasystem Worker 参数说明**：

| 参数 | 值 | 说明 |
| --- | --- | --- |
| worker_address | ${HOST_IP}:${WORKER_PORT} | Worker 服务地址和端口 |
| etcd_address | ${ETCD_IP}:${ETCD_PORT} | etcd 服务发现地址 |
| shared_memory_size_mb | 512000 | 共享内存大小（500 GB） |
| node_timeout_s | 30 | 节点超时时间（秒） |
| node_dead_timeout_s | 60 | 节点死亡超时时间（秒） |
| rpc_thread_num | 16 | 处理线程数量 |
| enable_worker_worker_batch_get | false | 批量传输key |
| liveness_check_path | /workspace/liveness | 存活检查路径 |

> **参考文档**：[openYuanrong Datasystem 文档](https://atomgit.com/openeuler/yuanrong-datasystem)

### 6. 启动VLLM服务

新建脚本 `run_ds_v4_flash_w8a8_mtp.sh`, 按需修改`nic_name`

> 查看网卡nic_name：ifconfig

```sh
#!/bin/bash

nic_name="enp61s0f2"
local_ip=`hostname -I|awk -F " " '{print$1}'`

export HCCL_IF_IP=$local_ip
export GLOO_SOCKET_IFNAME=$nic_name
export TP_SOCKET_IFNAME=$nic_name
export HCCL_SOCKET_IFNAME=$nic_name

export OMP_PROC_BIND=false
export OMP_NUM_THREADS=10
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
export LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2:$LD_PRELOAD
export HCCL_BUFFSIZE=1024
export VLLM_ASCEND_APPLY_DSV4_PATCH=1
export TASK_QUEUE_ENABLE=1
export HCCL_OP_EXPANSION_MODE="AIV"

export DS_WORKER_ADDR="$local_ip:18481"
export DS_ENABLE_REMOTE_H2D=0
unset GOOGLE_LOGTOSTDERR GOOGLE_ALSOLOGTOSTDERR
timestamp=$(date "+%Y%m%d%H%M%S")

vllm serve /mnt/cephfs/models/DeepSeek-V4-Flash-w8a8-mtp \
    --max_model_len 204800 \
    --max-num-batched-tokens 4096 \
    --served-model-name dsv4 \
    --gpu-memory-utilization 0.9 \
    --max-num-seqs 8 \
    --data-parallel-size 1 \
    --tensor-parallel-size 8 \
    --enable-expert-parallel \
    --tokenizer-mode deepseek_v4 \
    --tool-call-parser deepseek_v4 \
    --enable-auto-tool-choice \
    --reasoning-parser deepseek_v4 \
    --safetensors-load-strategy 'prefetch' \
    --enable-prefix-caching \
    --model-loader-extra-config='{"enable_multithread_load": "true", "num_threads": 128}' \
    --quantization ascend \
    --trust-remote-code \
    --port 8080 \
    --block-size 128 \
    --no-disable-hybrid-kv-cache-manager \
    --kv-transfer-config \
  '{
    "kv_connector": "AscendStoreConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
        "lookup_rpc_port":"1",
        "backend": "yuanrong"
      }
    }' \
    --speculative-config '{"num_speculative_tokens": 1,"method": "mtp","enforce_eager": true}' \
    --compilation-config '{"cudagraph_mode": "FULL_DECODE_ONLY", "cudagraph_capture_sizes":[8]}'\
    --async-scheduling \
    --additional-config '
    {"ascend_compilation_config":{
        "enable_npugraph_ex":true,
        "enable_static_kernel":false
        },
    "enable_flashcomm1": false,
    "enable_dsa_cp": false,
    "enable_cpu_binding": "true",
    "enable_shared_expert_dp": false,
    "multistream_overlap_shared_expert":true}' \
    2>&1 | tee deepseekv4_yuanrong_$timestamp.log
```

运行脚本

```
bash run_ds_v4_flash_w8a8_mtp.sh
```

### 7. 功能验证

服务启动后，验证部署是否成功。

#### 测试推理

```bash
curl -H "Accept: application/json" \
    -H "Content-type: application/json" \
    -X POST \
    -d '{
        "model": "dsv4",
        "messages": [{
            "role": "user",
            "content": "你好, 你是谁"
        }],
        "stream": false,
        "ignore_eos": false,
        "temperature": 0,
        "max_tokens": 200
    }' http://localhost:8080/v1/chat/completions
```

## 性能测试

### VLLM bench测试

新建`vllm_bench_test/run_vllm_bench.py`,  修改`TOKENIZER_PATH`为对应模型的权重路径:

```python
#!/usr/bin/env python3

"""
vLLM Benchmark Runner - Single Test
运行单个 vLLM benchmark 测试

使用示例：
    python run_vllm_bench.py \
        --total-len 2048 \
        --repeat-prefix-ratio 0.9 \
        --output-len 10 \
        --num-prompts 1 \
        --num-prefixes 1 \
        --concurrency 8

可选参数：
    --output-dir      输出目录（默认: 当前目录）
    --seed            随机种子（默认: 1000）
    --request-rate    请求速率（不传则不限制）
"""

import argparse
import os
import subprocess
import sys

sys.stdout.reconfigure(line_buffering=True)
from pathlib import Path
from datetime import datetime

# TODO： Need to modify

BENCH_HOST = "localhost"
BENCH_PORT = 8080
MODEL = "dsv4"
TOKENIZER_PATH = "/mnt/cephfs/models/DeepSeek-V4-Flash-w8a8-mtp"

def run_bench(
    total_len: int,
    repeat_prefix_ratio: float,
    output_len: int,
    num_prompts: int,
    num_prefixes: int,
    concurrency: int,
    output_dir: str = None,
    seed: int = 1000,
    request_rate: float = None,
):
    """
    运行 vLLM benchmark 测试

    Args:
        total_len: 总长度（例如 32768 表示 32k）
        repeat_prefix_ratio: 重复前缀率（例如 0.9）
        output_len: 输出长度（例如 1024）
        num_prompts: prompt 数量
        num_prefixes: prefix 数量
        concurrency: 并发数
        output_dir: 输出目录
        seed: 随机种子
        request_rate: 请求速率（None 表示不限制）
    """
    # 计算前缀和后缀长度
    prefix_len = int(total_len * repeat_prefix_ratio)
    suffix_len = total_len - prefix_len

    print("=======bench info========")
    print(f"总长度: {total_len}")
    print(f"重复前缀率: {repeat_prefix_ratio}")
    print(f"前缀长度: {prefix_len}")
    print(f"输出长度: {output_len}")
    print(f"并发数: {concurrency}")

    # 创建输出目录
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # 构建 vllm bench serve 命令
    cmd = [
        "vllm", "bench", "serve",
        "--backend", "openai-chat",
        "--endpoint", "/v1/chat/completions",
        "--dataset-name", "prefix_repetition",
        "--prefix-repetition-prefix-len", str(prefix_len),
        "--prefix-repetition-suffix-len", str(suffix_len),
        "--prefix-repetition-output-len", str(output_len),
        "--num-prompts", str(num_prompts),
        "--prefix-repetition-num-prefixes", str(num_prefixes),
        "--ignore-eos",
        "--model", MODEL,
        "--tokenizer", TOKENIZER_PATH,
        "--seed", str(seed),
        "--host", BENCH_HOST,
        "--port", str(BENCH_PORT),
        "--max-concurrency", str(concurrency),
    ]

    # 如果指定了 request_rate，添加到命令
    if request_rate is not None:
        cmd.extend(["--request-rate", str(request_rate)])

    print(f"\n执行命令:")
    print(" ".join(cmd))
    print("===============")
    print("")

    # 运行命令
    result = subprocess.run(cmd, capture_output=False)

    return result.returncode

def main():
    parser = argparse.ArgumentParser(description="运行 vLLM benchmark 单次测试")

    # 必需参数
    parser.add_argument("--total-len", type=int, required=True,
                        help="总长度（例如 32768 表示 32k）")
    parser.add_argument("--repeat-prefix-ratio", type=float, required=True,
                        help="重复前缀率（例如 0.9）")
    parser.add_argument("--output-len", type=int, required=True,
                        help="输出长度")
    parser.add_argument("--num-prompts", type=int, required=True,
                        help="prompt 数量")
    parser.add_argument("--num-prefixes", type=int, required=True,
                        help="prefix 数量")
    parser.add_argument("--concurrency", type=int, required=True,
                        help="并发数")

    # 可选参数
    parser.add_argument("--output-dir", type=str, default=".",
                        help="输出目录（默认: 当前目录）")
    parser.add_argument("--seed", type=int, default=1000,
                        help="随机种子（默认: 1000）")
    parser.add_argument("--request-rate", type=float, default=None,
                        help="请求速率（不传则不限制）")

    args = parser.parse_args()

    returncode = run_bench(
        total_len=args.total_len,
        repeat_prefix_ratio=args.repeat_prefix_ratio,
        output_len=args.output_len,
        num_prompts=args.num_prompts,
        num_prefixes=args.num_prefixes,
        concurrency=args.concurrency,
        output_dir=args.output_dir,
        seed=args.seed,
        request_rate=args.request_rate,
    )

    sys.exit(returncode)

if __name__ == "__main__":
    main()
```

使用脚本进行性能测试

```sh

# 前台运行

python run_vllm_bench.py --total-len 32768 --repeat-prefix-ratio 0.9 --output-len 512 --num-prompts 50 --num-prefixes 10 --concurrency 8 --seed 1002

# 后台运行

nohup python run_vllm_bench.py --total-len 32768 --repeat-prefix-ratio 0.9 --output-len 512 --num-prompts 50 --num-prefixes 10 --concurrency 8 --seed 1002 > ./run_bench_yr_32k_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

参数说明：

| 参数 | 值 | 说明 |
| :---------------------- | :---- | :------------------------------ |
| `--total-len` | 32748 | 单请求总长度（输入+输出） |
| `--repeat-prefix-ratio` | 0.9 | 共享前缀的请求比例，0.9=90%复用 |
| `--output-len` | 512 | 每个请求生成的输出token数 |
| `--num-prompts` | 50 | 总请求数量 |
| `--num-prefixes` | 10 | 不同前缀的种数 |
| `--concurrency` | 8 | 同时发送的并发请求数 |
| `--seed` | 1002 | 随机种子，保证结果可复现 |

### 缓存命中率监控

#### 查看 vLLM 日志

按需修改日志文件路径`vllm_log.log`

```bash

# 查看最新日志

tail -f vllm_log.log

# 实时监控命中率相关日志

tail -f vllm_log.log | grep -E "Prefix cache hit rate|External prefix cache hit rate|num_computed_tokens"
```

#### 使用脚本持续监控命中率

如果当前环境包含 `vllm-ascend` 仓库源码，可以使用仓库自带脚本持续观测命中率：

```bash
bash tools/watch_cache_hit_rate.sh -u http://localhost:18020/metrics -i 10

# 如需将结果同时保存到文件

bash tools/watch_cache_hit_rate.sh \
  -u http://localhost:18020/metrics \
  -i 10 \
  -o cache_hit_rate.log
```

常用字段：

- `local_win`：vLLM 本地 Prefix Cache 的窗口命中率
- `local_total`：vLLM 本地 Prefix Cache 的累计命中率
- `ext_win`：openYuanrong 外部 KV Cache 的窗口命中率
- `ext_total`：openYuanrong 外部 KV Cache 的累计命中率
- `eff_total`：综合本地和外部缓存后的端到端有效命中率

#### 缓存命中率指标说明

| 指标 | 说明 | 统计方式 |
| ------------------------------ | ----------------------------------- | -------------------------------------- |
| Prefix cache hit rate | **HBM（本地显存）**命中率 | **滑动窗口**：最近 1000 个请求 |
| External prefix cache hit rate | **openYuanrong（外部 KV Cache）**命中率 | **累计统计**：从服务启动到当前时刻 |
| TTFT (Time to First Token) | 首个 token 延迟 | 单次请求指标，命中率高时 TTFT 显著降低 |

**查看 openYuanrong 外部缓存命中率**：

```bash
curl http://localhost:18020/metrics | grep external_prefix_cache
```

**查看单次请求是否命中**：

```bash
grep "num_computed_tokens" $LOG_FILE
```

如果 `num_computed_tokens > 0`，表示该请求命中了缓存。

## 可靠性检查

### 可靠性方案介绍

在8机大EP部署场景中，可靠性方案主要依赖健康检查实现。建议使用监控系统通过存活探针周期性针对vllm服务、元戎、ETCD进行存活检测，若检测失败则重启推理服务容器，进行一次完整推理部署操作。 健康检查脚本目录结构如下图所示：

![image-20260526154318108](https://gitee.com/iamk123/md_pic/raw/master/image-20260526154318108.png)

  注意事项：

- 就绪探针中仅检查vllm服务状态
- 存活探针中检查vllm服务、元戎、ETCD状态

### 就绪探针检查脚本

拉起服务后进行首次可用性检查，脚本执行操作如下：

- 在主节点构造http协议请求模型服务(v1/chat/comoletions)；
- 判断请求是否正确返回；

`vllm_probe.py`内容如下：

```python
import sys
import subprocess
import requests
import socket
import logging
import json

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    filename='./health_check_probe.log',
    filemode='a'
)

if __name__ == "__main__":
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    api_url = f"http://{local_ip}:18020/v1/chat/completions"

    headers = {
        'Content-Type': 'application/json',
    }
    request_data = {
        "model": "deepseek_v4",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": False,
        "max_tokens": 2,
        "temperature": 0.6,
        "chat_template_kwargs": {"enable_thinking": False}
    }

    try:
        response = requests.post(
            api_url,
            json=request_data,
            headers=headers,
            stream=False,
            timeout=1200
        )
    except Exception as e:
        logging.error(f"requests post failed, Exception: {e}")
        sys.exit(1)

    if response.status_code != 200:
        logging.error(f"Response error, status code: {response.status_code}, text: {response.text}")
        sys.exit(1)

    try:
        response_info = json.loads(response.text)
        if len(response_info['choices'][0]['message']['content']) == 0:
            logging.error("response content len is 0")
            sys.exit(1)
        print("vLLM serve check success!")
    except Exception as e:
        logging.error(f"json parse failed, text: {response.text}, Exception: {e}")
        sys.exit(1)

    logging.info(f"health check success, response: {response.text}")
    logging.info("Master node health check completed")
```

`vllm_probe.py`用法：

```python

# 主节点执行：

python vllm_probe.py
```

预期输出:（详细日志查看`health_check_probe.log`）

```
vLLM serve check success!
```

### 存活探针检查脚本

部署过程中可手动执行以下脚本验证各组件状态；部署后运行期间，可将这些脚本加入监控系统中周期性执行。

脚本执行流程如下：

- 执行元戎worker健康检查；
- 在主节点执行ETCD健康检查；
- 在主节点构造http协议请求模型服务(v1/chat/comoletions)；
- 判断请求是否正确返回；

`vllm_probe_yr.py`内容如下：

```python
import sys
import subprocess
import socket
import requests
import logging
import json

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    filename='./health_check_probe.log',
    filemode='a'
)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Please specify node role: master or slave")
        sys.exit(1)

    node_role = sys.argv[1].lower()
    if node_role not in ["master", "slave"]:
        logging.error(f"Invalid node role: {node_role}. Please use 'master' or 'slave'")
        sys.exit(1)

    # 1、Check yuanrong
    try:
        ret = subprocess.run(["bash", "./yr_liveness_check.sh"])
        if ret.returncode == 1:
            logging.error("yuanrong check fail !")
            sys.exit(1)

    except Exception as e:
        logging.error(f"yuanrong check failed, Exception: {e}")
        sys.exit(1)

    logging.info("yuanrong check success !")

    # only master node need to check etcd & vllm serve
    if node_role == "slave":
        logging.info("Slave node health check completed (only yuanrong check)")
        sys.exit(0)
    logging.info("Master node health check start")

    local_ip = socket.gethostbyname(socket.gethostname())

    # 2、Check ETCD：Master node should check etcd
    etcd_port = 12379
    try:
        ret = subprocess.run(["bash", "./etcd_liveness_check.sh", f"{local_ip}:{etcd_port}"])
        if ret.returncode == 1:
            logging.error("etcd check fail !")
            sys.exit(1)
    except Exception as e:
        logging.error(f"etcd check failed, Exception: {e}")
        sys.exit(1)
    logging.info("yuanrong etcd check success !")

    # Check vllm serve
    api_url = f"http://{local_ip}:18020/v1/chat/completions"

    headers = {
        'Content-Type': 'application/json',
    }
    request_data = {
        "model": "deepseek_v4",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": False,
        "max_tokens": 2,
        "temperature": 0.6,
        "chat_template_kwargs": {"enable_thinking": False}
    }

    try:
        response = requests.post(
            api_url,
            json=request_data,
            headers=headers,
            stream=False,
            timeout=1200
        )
    except Exception as e:
        logging.error(f"requests post failed, Exception: {e}")
        sys.exit(1)

    if response.status_code != 200:
        logging.error(f"response error, status code: {response.status_code}, text: {response.text}")
        sys.exit(1)

    try:
        response_info = json.loads(response.text)
        if len(response_info["choices"][0]["message"]["content"]) == 0:
            logging.error("response content len is 0")
            sys.exit(1)
        print("vLLM serve check success!")
    except Exception as e:
        logging.error(f"json parse failed, text: {response.text}, Exception: {e}")
        sys.exit(1)

    logging.info(f"health check success, response: {response.text}")

    logging.info("end")
```

`vllm_probe_yr.py`脚本依赖`utils.sh`、`etcd_liveness_check.sh`、`yr_liveness_check.sh`, 内容如下：

创建脚本`utils.sh`

```sh
#!/bin/bash

# Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.

# # Licensed under the Apache License, Version 2.0 (the "License");

# you may not use this file except in compliance with the License.

# You may obtain a copy of the License at

# # http://www.apache.org/licenses/LICENSE-2.0

# # Unless required by applicable law or agreed to in writing, software

# distributed under the License is distributed on an "AS IS" BASIS,

# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions and

# limitations under the License.

set -e

shopt -s expand_aliases

readonly UTILS_WORK_DIR=$(dirname "$(readlink -f "$0")")
readonly UTILS_LOG_FILE=${WORKER_LOG_DIR}/container.log
readonly UTILS_LOCK_FILE=${WORKER_LOG_DIR}/.loglock
readonly UTILS_LOCK_DIR=${WORKER_LOG_DIR}/.loglockdir
readonly UTILS_MAX_LOG_SIZE=10485760 # 10MB
readonly UTILS_MAX_LOG_COUNT=9 # not include the current log file.
readonly UTILS_PREFIX=${UTILS_LOG_FILE%.*}
readonly UTILS_SUFFIX=${UTILS_LOG_FILE##*.}
alias ilog='utils_log I ${BASH_SOURCE##*/}:$LINENO'
alias wlog='utils_log W ${BASH_SOURCE##*/}:$LINENO'
alias elog='utils_log E ${BASH_SOURCE##*/}:$LINENO'

function utils_log_impl() {
    echo -e "$(date -u '+%Y-%m-%dT%H:%M:%S.%6N') | $1 | $2 |  | $$ |  |  | $3" >> ${UTILS_LOG_FILE}
    if [ "$1" == "E" -o "$1" == "W" ]; then
        echo -e "$3" >&2
    fi
}

function utils_rm_logfile() {
    local files=($(ls ${UTILS_PREFIX}.*.${UTILS_SUFFIX}))
    local to_del_count=$((${#files[@]} - $UTILS_MAX_LOG_COUNT))
    for file in "${files[@]}"; do
        if [ ${to_del_count} -lt 1 ]; then
            break
        fi
        to_del_count=$((to_del_count-1))
        utils_log_impl I ${BASH_SOURCE##*/}:$LINENO "rm log file ${file}"
        rm ${file}
    done
}

# rotate the logs if log file exceeds the max size

function utils_rotate_logfile() {
    local cur_time_str=$(date -u "+%Y%m%d%H%M%S")
    local new_log_file=${UTILS_PREFIX}.${cur_time_str}.${UTILS_SUFFIX}
    local file_size=$((du -b ${UTILS_LOG_FILE} 2>/dev/null |  | echo 0) | awk '{print $1}')
    if [ ${file_size} -gt ${UTILS_MAX_LOG_SIZE} ]; then
        mv ${UTILS_LOG_FILE} ${new_log_file}
        touch ${UTILS_LOG_FILE}
        utils_rm_logfile
    fi
}

function utils_trylock_exec() {
    local cmd="$1"
    if command -v flock >/dev/null 2>&1; then
        flock -n ${UTILS_LOCK_FILE} -c "$cmd"
    else
        # using create dir to simulate flock.
        if mkdir "${UTILS_LOCK_DIR}" 2>/dev/null; then
            ${cmd} |  | true
            rmdir "${UTILS_LOCK_DIR}"
        else
            # remove dir if the lock dir was created 30s ago.
            local current_time=$(date +%s)
            local update_time=$(stat -c %Z ${UTILS_LOCK_DIR} 2>/dev/null |  | echo ${current_time})
            local timeout=30
            if [[ $((current_time - update_time)) -gt $timeout ]]; then
                utils_log_impl I ${BASH_SOURCE##*/}:$LINENO "rm timeout lock dir ${UTILS_LOCK_DIR}"
                rmdir "${UTILS_LOCK_DIR}" 2>/dev/null
	        fi
        fi
    fi
}

function utils_log() {
    utils_log_impl "$@"
    local file_size=$((du -b ${UTILS_LOG_FILE} 2>/dev/null |  | echo 0) | awk '{print $1}')
    if [ ${file_size} -gt ${UTILS_MAX_LOG_SIZE} ]; then
        utils_trylock_exec "bash ${UTILS_WORK_DIR}/utils.sh ROTATE_LOG" |  | true
    fi
    chmod 640 ${UTILS_LOG_FILE} 2>/dev/null |  | true
}

# get value from datasystem_worker args, like: --key=value

function utils_get_worker_arg_value() {
    local key="$1"
    ps -ef | awk -v key="${key}" '/datasystem_worker/ {
        for (i = 1; i <= NF; i++) {
            idx = index($i, "=")
            if(idx > 1 && substr($i, 0, idx - 1) == "--" key) {
                print substr($i, idx + 1)
                exit
            }
        }
    }'
}

if [ "${1}" == "ROTATE_LOG" ]; then
    utils_rotate_logfile
fi

```

创建脚本 `etcd_liveness_check.sh`，用于etcd 健康检查：

```bash
#!/usr/bin/env bash

set -euo pipefail

ETCD_ADDR="${1:-<IP_ADDRESS>:2379}"

if etcdctl --endpoints="http://${ETCD_ADDR}" endpoint health >/dev/null 2>&1; then
    echo "etcd is ready: ${ETCD_ADDR}"
    exit 0
fi

echo "etcd is not ready: ${ETCD_ADDR}" >&2
exit 1
```

创建脚本 `yr_liveness_check.sh`, 用于 openYuanrong Worker 健康检查：

```bash
set -e

readonly USAGE="Options:
-f location of the liveness probe file, it must be set.
  For example, set to \"../datasystem/liveness\"
-t liveness probe timeout in seconds.
"
readonly WORK_DIR=$(dirname "$(readlink -f "$0")")
source ${WORK_DIR}/utils.sh

function main() {
    PROBE_PATH=/workspace/liveness
    PROBE_TIMEOUT=30

    while getopts 'f:t:' OPT; do
        case "${OPT}" in
            f)
                PROBE_PATH="${OPTARG}"
                ;;
            t)
                PROBE_TIMEOUT="${OPTARG}"
                ;;
            ?)
                echo -e "${USAGE}"
                exit 1
                ;;
        esac
    done

    if [ ! -e "${PROBE_PATH}" ]; then
        elog "liveness probe file ${PROBE_PATH} not exists!"
        exit 1
    fi

    content="$(cat ${PROBE_PATH})"
    if ! grep -q "liveness check success" "${PROBE_PATH}"; then
        elog "liveness probe ${PROBE_PATH} check failed: ${content}"
        exit 1
    fi

    PROBE_LAST_UPDATE_TIME=$(stat -c %Y ${PROBE_PATH})
    CURRENT_TIME=$(date +%s)
    if [[ $((CURRENT_TIME - PROBE_LAST_UPDATE_TIME)) -gt $PROBE_TIMEOUT ]]; then
        elog "liveness probe not update in ${PROBE_TIMEOUT}"
        exit 1
    fi
}
main "$@"
```

`vllm_probe_yr.py`用法：

```python

# 主节点执行：

python vllm_probe_yr.py master

# 从节点执行

python vllm_probe_yr.py slave
```

预期输出:（详细日志查看`health_check_probe.log`）

```
yuanrong check success !
etcd is ready: <IP_ADDRESS>:12379
vLLM serve check success!
```

## 附录

### openYuanrong 性能优化

#### 使用大页内存

**1. 配置大页内存**

1）检查是否分配

```
grep -i huge /proc/meminfo
```

- `HugePages_Total`显示为0表示未分配
- `Hugepagesize`表示单个页大小，通常为2MB

2）检查当前可用内存，要求 MemAvailable 大于要分配的内存大小：

```bash
grep MemAvailable /proc/meminfo
```

3）分配大页（此处分配250页，共500G，按需调整）：

```bash
"echo 250000 > /proc/sys/vm/nr_hugepages"
```

4）验证分配结果：

```bash
grep -i huge /proc/meminfo
```

预期配置：`HugePages_Total` 达到或接近目标值（250000）。

**2. 服务端（openYuanrong Worker）使用大页**

在 `run_yr_worker.sh` 的 `dscli start` 命令中调整超时时间，添加 `--arena_per_tenant`、`enable_huge_tlb` 参数：

```bash
export NODE_TIMEOUT=300
export NODE_DEAD_TIMEOUT=600

dscli start -t 600 -w \
  ......
  --arena_per_tenant 1 \
  --enable_huge_tlb true
```

新增参数说明：

| 参数 | 示例值 | 说明 |
| ---------------- | ------ | ------------------------------------------------------------ |
| arena_per_tenant | 1 | 每个 tenant 的 arena 数量，初始建议值为 1，在保证功能的前提下提供最快的启动速度 |
| enable_huge_tlb | true | 开启共享内存大页内存，可有效提升内存的分配与拷贝性能。共享内存大于 21G 时需开启，并需提前配置系统大页 |

完整的 openYuanrong worker启动脚本 `run_yr_worker.sh`如下：

```bash
#!/bin/bash

export HOST_IP="<当前节点IP>"
export ETCD_IP="<ETCD_IP>"
export WORKER_PORT=18481
export ETCD_PORT=2379
export SHM_SIZE=512000
export NODE_TIMEOUT=300
export NODE_DEAD_TIMEOUT=600
export LIVENESS_PATH=/workspace/liveness

dscli start -t 600 -w \
  --worker_address ${HOST_IP}:${WORKER_PORT} \
  --etcd_address ${ETCD_IP}:${ETCD_PORT} \
  --shared_memory_size_mb ${SHM_SIZE} \
  --node_timeout_s ${NODE_TIMEOUT} \
  --node_dead_timeout_s ${NODE_DEAD_TIMEOUT} \
  --liveness_check_path ${LIVENESS_PATH} \
  --arena_per_tenant 1 \
  --enable_huge_tlb true

echo "Yuanrong service start finished!"
```

#### 开启RH2D

RH2D（Remote Host to Device）是一种基于昇腾 NPU 的跨节点数据传输机制，支持从远端节点主机侧共享内存到设备侧 HBM 内存的直接传输，可显著提升 KV Cache 跨节点传输性能。

> **前提条件**：HDK 版本需 ≥ 25.5.0，CANN 版本需 ≥ 8.5.0。

**1.开启大页**

开启 RH2D 需要启用大页内存（`--enable_huge_tlb true`），需先在系统中完成大页内存配置。参考[附录：使用大页内存](#使用大页内存)

**2. 服务端（openYuanrong Worker）开启 RH2D**

在 `run_yr_worker.sh` 的 `dscli start` 命令中调整超时时间，添加 `--remote_h2d_device_ids` 、`arena_per_tenant`、`enable_huge_tlb`参数：

```bash
export NODE_TIMEOUT=300
export NODE_DEAD_TIMEOUT=600

dscli start -t 600 -w \
  ......
  --arena_per_tenant 1 \
  --remote_h2d_device_ids "0,1,2,3,4,5,6,7" \
  --enable_huge_tlb true
```

新增参数说明：

| 参数 | 示例值 | 说明 |
| --------------------- | ----------------- | ------------------------------------------------------------ |
| arena_per_tenant | 1 | 每个 tenant 的 arena 数量，初始建议值为 1，在保证功能的前提下提供最快的启动速度 |
| remote_h2d_device_ids | "0,1,2,3,4,5,6,7" | Worker 使用的 NPU 设备 ID 列表，逗号分隔。赋值即启用 RH2D，默认为空表示不启用。建议配置所有可用 NPU 以轮询建立传输连接，提高总传输带宽 |
| enable_huge_tlb | true | 开启共享内存大页内存，可有效提升内存的分配与拷贝性能。共享内存大于 21G 时需开启，并需提前配置系统大页 |

完整的 openYuanrong worker启动脚本 `run_yr_worker.sh`如下：

```bash
#!/bin/bash

export HOST_IP="<当前节点IP>"
export ETCD_IP="<ETCD_IP>"
export WORKER_PORT=18481
export ETCD_PORT=2379
export SHM_SIZE=512000
export NODE_TIMEOUT=300
export NODE_DEAD_TIMEOUT=600
export LIVENESS_PATH=/workspace/liveness

dscli start -t 600 -w \
  --worker_address ${HOST_IP}:${WORKER_PORT} \
  --etcd_address ${ETCD_IP}:${ETCD_PORT} \
  --shared_memory_size_mb ${SHM_SIZE} \
  --node_timeout_s ${NODE_TIMEOUT} \
  --node_dead_timeout_s ${NODE_DEAD_TIMEOUT} \
  --liveness_check_path ${LIVENESS_PATH} \
  --arena_per_tenant 1 \
  --remote_h2d_device_ids "0,1,2,3,4,5,6,7" \
  --enable_huge_tlb true

echo "Yuanrong service start finished!"
```

**3. 客户端（vLLM）开启 RH2D**

在 Prefill 和 Decode 节点的启动脚本中添加环境变量`export DS_ENABLE_REMOTE_H2D=1`, 启用客户端侧 RH2D 功能, 该环境变量需在启动 vLLM 服务之前设置，修改P节点/D节点`run_dp_template.sh` 模板。完整的 openYuanrong 相关环境变量配置如下：

```bash

# openYuanrong Datasystem

export DS_WORKER_ADDR="${local_ip}:18481"
export DS_ENABLE_REMOTE_H2D=1
unset GOOGLE_LOGTOSTDERR GOOGLE_ALSOLOGTOSTDERR
```

注意：启用RH2D功能最大会占用300M左右HBM显存用于建链传输。

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
   所有节点必须设置为 `0`。

4. **yr.datasystem 导入错误**

   确保 `openyuanrong-datasystem` 已安装：

```bash
   pip install openyuanrong-datasystem
   python -c "from yr.datasystem.hetero_client import HeteroClient; print('OK')"
```

5. **64 KB 页大小机器无法直接使用默认 openYuanrong 安装包**

   检查页大小：

```bash
   getconf PAGE_SIZE
```

   如果输出为 `65536`，请使用针对 64 KB 页大小单独编译的 openYuanrong 安装包。

6. **引擎启动超时**

   增加 `VLLM_ENGINE_READY_TIMEOUT_S` 的值：

```bash
   export VLLM_ENGINE_READY_TIMEOUT_S=3600
```

7. **节点时间不一致**

   8 机场景下，建议所有节点的系统时间保持一致，否则可能影响日志对齐、问题定位以及部分依赖时间戳的排障判断。

```bash
   date
   timedatectl
```

7. `yuanrong_backend`报错`Failed to get xx keys.` 、`[key not found]`等类似错误。

   非正常退出，导致数据有残留。建议每次启动前将yuanrong的client日志、worker数据目录、etcd数据目录清除。默认位置如下：

   client日志：~/.datasystem
   worker数据目录：`run_yr_worker.sh`启动目录下的datasystem目录
   etcd数据目录：/tmp/etcd-data

建议通过优雅退出方式停止worker，执行`dscli stop`命令

```bash
dscli stop --worker_address ${HOST_IP}:${WORKER_PORT}
```

### 参考资料

- [openYuanrong Datasystem 文档](https://atomgit.com/openeuler/yuanrong-datasystem)
- [etcd 文档](https://etcd.io/docs/)
- [vLLM Ascend 文档](https://docs.vllm.ai/projects/ascend/)
- [DeepSeek-V4-Flash-w8a8-mtp A2 部署调优实践](GLM5.md)
- [基于 openYuanrong 的 GLM-5 W4A8 单实例部署](pd_colocated_yuanrong_glm5_cn.md)
- [KV Pool 使用指南](../../user_guide/feature_guide/kv_pool.md)