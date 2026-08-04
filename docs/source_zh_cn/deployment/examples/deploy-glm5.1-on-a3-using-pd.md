# 在 Atlas 800 A3 上使用 PD 分离方式部署 GLM-5.1-w8a8

PD 分离架构下，Prefill 节点与 Decode 节点各司其职，通过 MultiConnector 组合 KV Transfer 与 KV Pool 能力。同时，通过 AscendStoreConnector（openYuanrong 后端）实现外部 KV 缓存池，支持前缀缓存复用，可有效降低重复前缀场景下的首 token 时延。

## 方案介绍

本案例在 4 台 Atlas 800I A3 / Atlas 800T A3 服务器上部署 GLM-5.1-w8a8 模型，使用 PD 分离架构并叠加 openYuanrong Datasystem 作为 KV Pool 后端。其中，两台 A3 作为 Prefill 组，剩余两台 A3 作为 Decode 组。

## 准备工作

- **硬件要求**：Atlas 800I A3 或 Atlas 800T A3 ，4机64卡
- **通信网络**：4 台服务器之间已完成灵衢组网，业务网卡和 NPU 通信网络互通。
- **CANN版本**：≥ 8.5.0
- **HDK**：≥ 25.5.0
- **vLLM-Ascend**：v0.18.0-a3
- **模型权重**：[GLM-5.1-w8a8](https://modelscope.cn/models/Eco-Tech/GLM-5.1-w8a8)
- **页大小**：2k （可通过 `getconf PAGE_SIZE` 命令查询）
- **系统内存**：1T  

若希望在 A3 上启用 `ASCEND_ENABLE_USE_FABRIC_MEM=1`，建议满足 `HDK >= 26.0.0` 且 `CANN >= 9.0.0`

:::{note}

A3 上配置 `VLLM_ASCEND_ENABLE_FUSED_MC2=1` 可以开启 Prefill/Decode 两侧的 MoE 融合算子。这一能力当前仅支持 `W8A8`，仍属于实验特性，如遇稳定性问题，可先回退为 `VLLM_ASCEND_ENABLE_FUSED_MC2=0`。

:::

### 网络检查

在每个节点上检查 NPU 状态：

```bash
npu-smi info
```

获取各 NPU 的 vNIC 信息：

```bash
for i in {0..15}; do hccn_tool -i $i -vnic -g; done
```

如果需要排查 NPU 直连网络连通性，可按实际对端 NPU IP 执行：

```bash
for i in {0..15}; do hccn_tool -i $i -hccs_ping -g address <对端NPU_IP>; done
```

确保所有关键链路都处于可用状态，再启动推理服务。

## 部署流程

在开始部署前，先在所有节点上配置以下环境变量（替换为实际值）：

```bash
# ===== 节点 IP（所有节点均需配置） =====
export P0_IP="<节点0业务IP>"      # Prefill 主节点（同时运行 etcd）
export P1_IP="<节点1业务IP>"      # Prefill 节点
export D0_IP="<节点2业务IP>"      # Decode 主节点
export D1_IP="<节点3业务IP>"      # Decode 节点
export ETCD_IP="${P0_IP}"         # etcd 部署在节点 0

# ===== 本节点配置（每台节点按自身情况设置） =====
export LOCAL_IP="<当前节点业务IP>"  # 当前节点的业务 IP
export NIC_NAME="<业务网卡名>"     # 例如 enp3s0、bond0
```

### 步骤 1、配置内存大页

根据 Hugepagesize 大小选择配置方法，在每个宿主机执行对应命令，分配 500G 大页内存。

使用命令查看页大小：`grep Huge /proc/meminfo`。

:::{tip}
若内存容量为 2T，为获取更好性能，可以配置 1T 大页内存，同时调整以下步骤的配置：

- **步骤 2** 中容器容量设置为 1024g；
- **步骤 4** 中所有节点的 `SHM_SIZE` 改为 `1024000`
:::

**Hugepagesize 2M**

```bash
# 分配256000页共500G
echo 256000 > /proc/sys/vm/nr_hugepages 
```

**Hugepagesize 512M**

```sh
# 分配1000页共500G
echo 1000 > /proc/sys/vm/nr_hugepages
```

执行 `grep Huge /proc/meminfo` 查看内存大页配置是否配置成功。

### 步骤 2、拉取镜像与创建容器

#### 拉取镜像

```bash
docker pull quay.io/ascend/vllm-ascend:v0.18.0-a3
```

如果下载较慢，可将 `quay.io` 替换为 `m.daocloud.io/quay.io` 或 `quay.nju.edu.cn` 以加速拉取。

#### 创建容器

每个节点上创建脚本 `start-docker.sh`，内容如下：

```bash
#!/bin/bash
IMAGES_ID="$1"
NAME="$2"
SHM_SIZE="$3"

docker run --name "${NAME}" -it -d --net=host --shm-size="${SHM_SIZE}" \
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

创建并进入容器：

```bash
bash start-docker.sh quay.io/ascend/vllm-ascend:v0.18.0-a3 yr_glm5 500g
docker exec -it yr_glm5 bash
```

#### 升级 transformers 版本

GLM-5 模型要求较高版本的 transformers，进入容器后需先升级：

```bash
pip install transformers==5.2.0 --no-deps --force-reinstall
pip install huggingface_hub==1.5.0 --no-deps --force-reinstall
```

#### 添加补丁

根据镜像版本打对应的补丁。将补丁文件上传到容器内固定目录 `/workspace/yuanrong_patches/`，然后按版本选择执行。

准备工作（所有版本通用）：

```bash
mkdir -p /workspace/yuanrong_patches

git config --global user.email "deploy@local"
git config --global user.name "deploy"
```

根据 vllm-ascend 版本打补丁：
- vllm-ascend:0.18.0 版本：

    | 补丁文件                                                     | 目标仓库                      | 用途                                      |
    | ------------------------------------------------------------ | ----------------------------- | ----------------------------------------- |
    | `0001-Bugfix-Fix-negative-local_cache_hit-in-P-D-disaggreg.patch` | `/vllm-workspace/vllm`        | 修复 `local_cache_hit` 指标出现负值的问题 |
    | `0001-Implement-yuanrong-backend.patch`                      | `/vllm-workspace/vllm-ascend` | 补充 Yuanrong backend 支持                |
    | `0001-fix-kv-pool-update-yuanrong-backend-handling.patch`    | `/vllm-workspace/vllm-ascend` | 修复超过10000个对象时分批传输问题         |

    ```bash
    # vllm patch
    cd /vllm-workspace/vllm
    git am /workspace/yuanrong_patches/0001-Bugfix-Fix-negative-local_cache_hit-in-P-D-disaggreg.patch

    # vllm-ascend patches
    cd /vllm-workspace/vllm-ascend
    git am /workspace/yuanrong_patches/0001-Implement-yuanrong-backend.patch
    git am /workspace/yuanrong_patches/0001-fix-kv-pool-update-yuanrong-backend-handling.patch
    ```

- vllm-ascend:0.18.0rc1 版本：

    | 补丁文件                                                     | 目标仓库                      | 用途                                                         |
    | ------------------------------------------------------------ | ----------------------------- | ------------------------------------------------------------ |
    | `0001-Bugfix-Fix-negative-local_cache_hit-in-P-D-disaggreg.patch` | `/vllm-workspace/vllm`        | 修复 `local_cache_hit` 指标出现负值的问题                    |
    | `0001-Implement-yuanrong-backend.patch`                      | `/vllm-workspace/vllm-ascend` | 补充 Yuanrong backend 支持                                   |
    | `0001-BugFix-0.18.0-KV-Pool-Fix-KV-Pool-not-putting-kv-cac.patch` | `/vllm-workspace/vllm-ascend` | 修复 vLLM v0.18.0 在 speculative decoding 场景下 KV Pool 未正确执行 KV Cache put / finalize 的问题，并规避后续 vLLM metrics 统计相关报错 |

    ```bash
    # vllm patch
    cd /vllm-workspace/vllm
    git am /workspace/yuanrong_patches/0001-Bugfix-Fix-negative-local_cache_hit-in-P-D-disaggreg.patch

    # vllm-ascend patches
    cd /vllm-workspace/vllm-ascend
    git am /workspace/yuanrong_patches/0001-Implement-yuanrong-backend.patch
    git am /workspace/yuanrong_patches/0001-BugFix-0.18.0-KV-Pool-Fix-KV-Pool-not-putting-kv-cac.patch
    ```

### 步骤 3、安装并启动 etcd

示例为单实例部署，etcd 只需在 P 主节点安装和启动，其他节点无需操作。

#### 安装 etcd

openYuanrong 服务启动脚本依赖 `etcd` 和 `etcdctl`。至少在节点0（P0节点）安装，其他节点连接同一个 etcd。

执行如下脚本下载并安装 etcd。若本地已有 etcd 安装包，可跳过 `wget` 下载步骤。

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
cp etcd etcdctl /usr/local/bin/
```

执行以下命令验证安装成功：

```bash
etcd --version
etcdctl version
```

#### 启动 etcd

创建启动脚本 `run_etcd.sh`，在节点 0（P0节点）启动 etcd：

```sh
#!/bin/bash

export ETCD_IP="${ETCD_IP:-${P0_IP}}"
export ETCD_PORT=2379
export ETCD_PEER_PORT=2380

etcd \
  --name etcd-single \
  --data-dir /tmp/etcd-data \
  --listen-client-urls http://0.0.0.0:${ETCD_PORT} \
  --advertise-client-urls http://${ETCD_IP}:${ETCD_PORT} \
  --listen-peer-urls http://0.0.0.0:${ETCD_PEER_PORT} \
  --initial-advertise-peer-urls http://${ETCD_IP}:${ETCD_PEER_PORT} \
  --initial-cluster etcd-single=http://${ETCD_IP}:${ETCD_PEER_PORT} \
  > /tmp/etcd.log 2>&1 &

sleep 3

etcdctl --endpoints "${ETCD_IP}:${ETCD_PORT}" put key "value"
etcdctl --endpoints "${ETCD_IP}:${ETCD_PORT}" get key

echo "ETCD start finished, log dir: /tmp/etcd.log"
```

验证 etcd 服务可用：

```sh
# 方式1，预期输出：{"health":"true","reason":""}
etcdctl --endpoints "${ETCD_IP}:${ETCD_PORT}" endpoint health

# 方式2，预期输出：100.100.xxx.xxx:2379 is healthy: successfully committed proposal: took = 1.43913ms
curl -L http://${ETCD_IP}:${ETCD_PORT}/health
```

### 步骤 4、安装并启动 openYuanrong 服务

#### 安装 openYuanrong Datasystem

:::{note}

若执行 `getconf PAGE_SIZE` 显示的页大小为 64k，需要使用 64k 包，请联系我们的支持团队获取安装包。

:::

```sh
wget https://gitcode.com/openeuler/yuanrong-datasystem/releases/download/v0.7.6.rc1/openyuanrong_datasystem-0.7.6rc1-cp311-cp311-manylinux_2_35_aarch64.whl

pip install openyuanrong_datasystem-0.7.6rc1-cp311-cp311-manylinux_2_35_aarch64.whl
```

验证安装：

```bash
python -c "import yr.datasystem; print('Yuanrong Datasystem 安装成功')"
```

#### 启动 openYuanrong 服务

所有节点都需要启动 openYuanrong Datasystem Worker，并连接到节点 0 上的 etcd。

在每个节点创建 `run_yr_worker.sh`，修改 `HOST_IP` 和 `ETCD_IP`（可直接引用上方定义的环境变量，如 `export HOST_IP="${P0_IP}"`）：

```bash
#!/bin/bash

export HOST_IP="${HOST_IP:-<当前节点IP>}"
export ETCD_IP="${ETCD_IP:-${P0_IP}}"
export WORKER_PORT=18481
export ETCD_PORT=2379
export SHM_SIZE=512000
export NODE_TIMEOUT=20
export NODE_DEAD_TIMEOUT=30
export LIVENESS_PATH=/workspace/liveness

dscli start --interleave 0-7 -w \
--worker_address ${HOST_IP}:${WORKER_PORT} \
--etcd_address ${ETCD_IP}:${ETCD_PORT} \
--shared_memory_size_mb ${SHM_SIZE} \
--node_timeout_s ${NODE_TIMEOUT} \
--node_dead_timeout_s ${NODE_DEAD_TIMEOUT} \
--arena_per_tenant 1 \
--enable_huge_tlb true \
--liveness_check_path ${LIVENESS_PATH}
```

### 步骤 5、部署 vLLM 服务

#### 准备部署脚本

以下所有步骤均在 `/workspace/glm5-a3-large-ep` 下执行。

1. 拷贝 vLLM 相关脚本到 `/workspace/glm5-a3-large-ep` 目录。

    ```sh
    mkdir -p /workspace/glm5-a3-large-ep
    cd /workspace/glm5-a3-large-ep

    cp /vllm-workspace/vllm-ascend/examples/external_online_dp/launch_online_dp.py /workspace/glm5-a3-large-ep
    cp /vllm-workspace/vllm-ascend/examples/disaggregated_prefill_v1/load_balance_proxy_server_example.py /workspace/glm5-a3-large-ep
    ```

2. 创建 Prefill 启动脚本

    在节点 0 和 1（Prefill 节点）上分别创建 `run_dp_template.sh`，脚本会从环境变量 `NIC_NAME` 和 `LOCAL_IP` 读取网卡名和本机 IP（已在部署流程开头配置）。

    ```bash
    #!/bin/bash

    nic_name="${NIC_NAME:-xxxx}" # change to your own nic name
    local_ip="${LOCAL_IP:-xxxx}" # change to your own ip
    export VLLM_ASCEND_ENABLE_FUSED_MC2=1
    export HCCL_OP_EXPANSION_MODE="AIV"
    export VLLM_ASCEND_ENABLE_FLASHCOMM1=1

    export HCCL_IF_IP=$local_ip
    export GLOO_SOCKET_IFNAME=$nic_name
    export TP_SOCKET_IFNAME=$nic_name
    export HCCL_SOCKET_IFNAME=$nic_name

    export OMP_PROC_BIND=false
    export OMP_NUM_THREADS=10
    export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
    export HCCL_BUFFSIZE=256

    export ASCEND_AGGREGATE_ENABLE=1
    export ACL_OP_INIT_MODE=1
    export ASCEND_A3_ENABLE=1
    export VLLM_NIXL_ABORT_REQUEST_TIMEOUT=300000

    export ASCEND_RT_VISIBLE_DEVICES=$1
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

    # Yuanrong
    export DS_WORKER_ADDR="${local_ip}:18481"
    unset GOOGLE_LOGTOSTDERR GOOGLE_ALSOLOGTOSTDERR
    export PYTHONHASHSEED=0

    vllm serve /data/GLM-5.1-w8a8 \
        --host 0.0.0.0 \
        --port $2 \
        --data-parallel-size $3 \
        --data-parallel-rank $4 \
        --data-parallel-address $5 \
        --data-parallel-rpc-port $6 \
        --tensor-parallel-size $7 \
        --enable-expert-parallel \
        --speculative-config '{"num_speculative_tokens": 3, "method":"deepseek_mtp"}' \
        --seed 1024 \
        --served-model-name glm-5.1 \
        --max-model-len 202752 \
        --additional-config '{"enable_npugraph_ex": true, "fuse_muls_add":true,"multistream_overlap_shared_expert":true,"recompute_scheduler_enable" : true}' \
        --max-num-batched-tokens 4096 \
        --trust-remote-code \
        --max-num-seqs 64 \
        --quantization ascend \
        --gpu-memory-utilization 0.95 \
        --enforce-eager \
        --enable-auto-tool-choice \
        --tool-call-parser glm47 \
        --reasoning-parser glm45 \
        --kv-transfer-config \
        '{
            "kv_connector": "MultiConnector",
            "kv_role": "kv_producer",
            "engine_id": "0",
            "kv_connector_extra_config": {
                "connectors": [
                    {
                        "kv_connector": "MooncakeConnectorV1",
                        "kv_role": "kv_producer",
                        "kv_port": "30000",
                        "kv_connector_module_path": "vllm_ascend.distributed.mooncake_connector",
                        "kv_connector_extra_config": {
                            "use_ascend_direct": true,
                            "prefill": {
                                "dp_size": 2,
                                "tp_size": 16
                            },
                            "decode": {
                                "dp_size": 2,
                                "tp_size": 16
                            }
                        }
                    },
                    {
                        "kv_connector": "AscendStoreConnector",
                        "kv_role": "kv_producer",
                        "kv_connector_extra_config": {
                            "lookup_rpc_port": "0",
                            "backend": "yuanrong"
                        }
                    }
                ]
            }
        }'
    ```

3. 创建 Decode 启动脚本

    在节点 2 和 3（Decode 节点）上分别创建 `run_dp_template.sh`。脚本会从环境变量 `NIC_NAME` 和 `LOCAL_IP` 读取网卡名和本机 IP（已在部署流程开头配置）。

    ```bash
    #!/bin/bash

    nic_name="${NIC_NAME:-xxxx}" # change to your own nic name
    local_ip="${LOCAL_IP:-xxxx}" # change to your own ip
    export VLLM_ASCEND_ENABLE_FUSED_MC2=1
    export HCCL_OP_EXPANSION_MODE="AIV"

    export HCCL_IF_IP=$local_ip
    export GLOO_SOCKET_IFNAME=$nic_name
    export TP_SOCKET_IFNAME=$nic_name
    export HCCL_SOCKET_IFNAME=$nic_name

    # Mooncake
    export OMP_PROC_BIND=false
    export OMP_NUM_THREADS=10

    export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
    export HCCL_BUFFSIZE=256

    export ASCEND_AGGREGATE_ENABLE=1
    export ASCEND_TRANSPORT_PRINT=1
    export ACL_OP_INIT_MODE=1
    export ASCEND_A3_ENABLE=1
    export VLLM_NIXL_ABORT_REQUEST_TIMEOUT=300000

    export TASK_QUEUE_ENABLE=1

    export ASCEND_RT_VISIBLE_DEVICES=$1
    export VLLM_ASCEND_ENABLE_MLAPO=1
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

    # Yuanrong
    export DS_WORKER_ADDR="${local_ip}:18481"
    unset GOOGLE_LOGTOSTDERR GOOGLE_ALSOLOGTOSTDERR
    export PYTHONHASHSEED=0

    vllm serve /data/GLM-5.1-w8a8 \
        --host 0.0.0.0 \
        --port $2 \
        --data-parallel-size $3 \
        --data-parallel-rank $4 \
        --data-parallel-address $5 \
        --data-parallel-rpc-port $6 \
        --tensor-parallel-size $7 \
        --enable-expert-parallel \
        --speculative-config '{"num_speculative_tokens": 3,  "method":"deepseek_mtp"}' \
        --seed 1024 \
        --served-model-name glm-5.1 \
        --max-model-len 202752 \
        --max-num-batched-tokens 32 \
        --compilation-config '{"cudagraph_mode":"FULL_DECODE_ONLY", "cudagraph_capture_sizes":[4, 8, 12, 16, 20, 24, 28, 32]}' \
        --additional-config '{"enable_npugraph_ex": true, "fuse_muls_add":true,"multistream_overlap_shared_expert":true,"recompute_scheduler_enable" : true}' \
        --trust-remote-code \
        --max-num-seqs 8 \
        --gpu-memory-utilization 0.92 \
        --async-scheduling \
        --quantization ascend \
        --enable-auto-tool-choice \
        --tool-call-parser glm47 \
        --reasoning-parser glm45 \
        --kv-transfer-config \
        "{
            \"kv_connector\": \"MultiConnector\",
            \"kv_role\": \"kv_consumer\",
            \"kv_connector_extra_config\": {
                \"connectors\": [
                    {
                        \"kv_connector\": \"MooncakeConnectorV1\",
                        \"kv_role\": \"kv_consumer\",
                        \"kv_port\": \"30100\",
                        \"kv_connector_module_path\": \"vllm_ascend.distributed.mooncake_connector\",
                        \"kv_connector_extra_config\": {
                            \"use_ascend_direct\": true,
                            \"prefill\": {
                                \"dp_size\": 2,
                                \"tp_size\": 16
                            },
                            \"decode\": {
                                \"dp_size\": 2,
                                \"tp_size\": 16
                            }
                        }
                    },
                    {
                        \"kv_connector\": \"AscendStoreConnector\",
                        \"kv_role\": \"kv_consumer\",
                        \"kv_connector_extra_config\": {
                            \"lookup_rpc_port\": \"$4\",
                            \"backend\": \"yuanrong\"
                        }
                    }
                ]
            }
        }"
    ```

4. 创建请求转发脚本

    在节点 0（Prefill 节点）上创建 `proxy.sh`：

    ```bash
    unset ftp_proxy
    unset https_proxy
    unset http_proxy

    python load_balance_proxy_server_example.py \
    --port 8088 \
    --host 0.0.0.0 \
    --prefiller-hosts \
        ${P0_IP} \
        ${P1_IP} \
    --prefiller-ports \
        6700 \
        6700 \
    --decoder-hosts \
        ${D0_IP} \
        ${D1_IP} \
    --decoder-ports \
        6700 \
        6700 \
    ```

#### 启动 vLLM 服务

每个节点对应执行以下命令拉起 vLLM 服务

节点 0（Prefill 节点）：

```
python launch_online_dp.py --dp-size 2 --tp-size 16 --dp-size-local 1 --dp-rank-start 0 --dp-address ${P0_IP} --dp-rpc-port 10521 --vllm-start-port 6700
```

节点 1（Prefill 节点）：

```
python launch_online_dp.py --dp-size 2 --tp-size 16 --dp-size-local 1 --dp-rank-start 1 --dp-address ${P0_IP} --dp-rpc-port 10521 --vllm-start-port 6700
```

节点 2（Decode 节点）：

```
python launch_online_dp.py --dp-size 2 --tp-size 16 --dp-size-local 1 --dp-rank-start 0 --dp-address ${D0_IP} --dp-rpc-port 10521 --vllm-start-port 6700
```

节点 3（Decode 节点）：

```
python launch_online_dp.py --dp-size 2 --tp-size 16 --dp-size-local 1 --dp-rank-start 1 --dp-address ${D0_IP} --dp-rpc-port 10521 --vllm-start-port 6700
```

待所有节点就绪，在节点 0（Prefill 节点）上执行请求转发脚本

```
bash proxy.sh
```


## 测试验证

### 执行推理请求

```bash
curl -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -X POST \
    -d '{
        "model": "glm-5.1",
        "messages": [{
            "role": "user",
            "content": "你好"
        }],
        "stream": false,
        "temperature": 0,
        "max_tokens": 256
    }' http://${P0_IP}:8088/v1/chat/completions
```

### 查看 metrics

代理不会聚合各后端的 metrics，建议直接查看具体后端实例：

```bash
curl http://${P0_IP}:6700/metrics | head
```

### 缓存命中率监控

`launch_online_dp.py` 默认将 vLLM 日志输出到当前工作目录下的 `vllm_log.log`。

```sh
# 查看最新日志
tail -f vllm_log.log

# 实时监控命中率相关日志
tail -f vllm_log.log | grep -E "Prefix cache hit rate|External prefix cache hit rate|num_computed_tokens"
```

## FAQ

1. **`zmq.error.ZMQError: Address already in use`**

   检查 `kv_port` 是否落在 `[20000, 35999]`。A3 16 device 场景建议使用本文示例的 `30000` 和 `30100`。

2. **DP 组无法建立连接**

   检查 `--data-parallel-address` 是否填写对应组的主节点业务 IP。Prefill 组应使用节点 0 IP，Decode 组应使用节点 2 IP。

3. **HCCL 或 Gloo 通信失败**

   检查 `nic_name`、`local_ip`、`HCCL_SOCKET_IFNAME`、`GLOO_SOCKET_IFNAME` 是否与实际网卡一致，并确认防火墙放行 DP RPC 和后端服务端口。

4. **启用 `VLLM_ASCEND_ENABLE_FUSED_MC2=1` 后启动失败**

   该融合路径面向 W8A8。可先回退：

   ```bash
   export VLLM_ASCEND_ENABLE_FUSED_MC2=0
   ```

   如果回退后恢复正常，再继续排查融合算子相关问题。

5. **代理健康检查失败**

   先确认所有后端实例已启动，再检查 `proxy.sh` 中的 host 和 port 数量是否与实际实例一致。本文应配置 2 个 Prefill 实例和 2 个 Decode 实例。

6. **推理请求返回模型不存在**

   请求中的 `model` 字段需要与启动参数 `--served-model-name glm-5.1` 保持一致。

7. **Yuanrong Worker 注册失败**

   检查当前节点 Worker 是否监听，以及 `DS_WORKER_ADDR` 是否与 `dscli start -w` 的 `--worker_address` 一致：

   ```bash
   netstat -tlnp | grep 18481
   echo $DS_WORKER_ADDR
   ```

8. **etcd 连接失败**

   在节点 0 检查 etcd 健康状态，并确认其他节点可以访问节点 0 的 `2379` 端口：

   ```bash
   etcdctl --endpoints "${ETCD_IP}:2379" endpoint health
   ```
