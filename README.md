![](./docs/source_zh_cn/images/logo-large.png)

openYuanrong 是一个 Serverless 分布式计算引擎，致力于以一套统一 Serverless 架构支持 AI、大数据、微服务等各类分布式应用。它提供多语言函数编程接口，以单机编程体验简化分布式应用开发；提供分布式动态调度和数据共享等能力，实现分布式应用的高性能运行和集群的高效资源利用。

## 简介

![](./docs/source_zh_cn/images/introduction.png)

openYuanrong 由多语言函数运行时、函数系统和数据系统组成，支持按需灵活单独或组合使用。

- **多语言函数运行时**：提供函数分布式编程，支持 Python、Java、C++ 语言，实现类单机编程高性能分布式运行。
- **函数系统**：提供大规模分布式动态调度，支持函数实例极速弹性扩缩和跨节点迁移，实现集群资源高效利用。
- **数据系统**：提供异构分布式多级缓存，支持 Object、Stream 语义，实现函数实例间高性能数据共享及传递。

openYuanrong 分为三个代码仓库：[yuanrong-runtime](https://gitcode.com/openeuler/yuanrong-runtime) 对应多语言函数运行时；[yuanrong-functionsystem](https://gitcode.com/openeuler/yuanrong-functionsystem) 对应函数系统；[yuanrong-datasystem](https://gitcode.com/openeuler/yuanrong-datasystem) 对应数据系统，即当前代码仓。

**数据系统（openYuanrong datasystem）** 是 openYuanrong 的核心概念抽象，是一个分布式缓存系统，利用计算集群的 HBM/DRAM/SSD 资源构建近计算多级缓存，提升模型训练及推理、大数据、微服务等场景数据访问性能。

openYuanrong datasystem 的主要特性包括：

- **高性能分布式多级缓存**：基于 DRAM/SSD 构建分布式多级缓存，应用实例通过共享内存免拷贝读写 DRAM 数据，并提供高性能 H2D(host to device)/D2H(device to host) 接口，实现 HBM 与 DRAM 之间快速 swap。
- **NPU 间高效数据传输**：将 NPU 的 HBM 抽象为异构对象，自动协调 NPU 间 HCCL 收发顺序，实现简单易用的卡间数据异步并发传输。并支持P2P传输负载均衡策略，充分利用卡间链路带宽。
- **灵活的生命周期管理**：支持设置 TTL、LRU 缓存淘汰以及 delete 接口等多种生命周期管理策略，数据生命周期既可由数据系统管理，也可交由上层应用管理，提供更高的灵活性。
- **热点数据多副本**：数据跨节点读取时自动在本地保存副本，支撑热点数据高效访问。本地副本使用 LRU 策略自动淘汰。
- **多种数据可靠性策略**：支持 write_through、write_back 及 none 多种持久化策略，满足不同场景的数据可靠性需求。
- **数据一致性**：支持 Causal 及 PRAM 两种数据一致性模型，用户可按需选择，实现性能和数据一致性的平衡。
- **数据发布订阅**：支持数据订阅发布，解耦数据的生产者（发布者）和消费者（订阅者），实现数据的异步传输与共享。
- **高可靠高可用**：支持分布式元数据管理，实现系统水平线性扩展。支持元数据可靠性，支持动态资源伸缩自动迁移数据，实现系统高可用。

### openYuanrong datasystem 适用场景

- **LLM 长序列推理 KVCache**：基于异构对象提供分布式多级缓存 (HBM/DRAM/SSD) 和高吞吐 D2D/H2D/D2H 访问能力，构建分布式 KV Cache，实现 Prefill 阶段的 KVCache 缓存以及 Prefill/Decode 实例间 KV Cache 快速传递，提升推理吞吐。
- **模型推理实例 M->N 快速弹性**：利用异构对象的卡间直通及 P2P 数据分发能力实现模型参数快速复制。
- **强化学习模型参数重排**：利用异构对象的卡间直通传输能力，快速将模型参数从训练侧同步到推理侧。
- **训练场景 CheckPoint 快速保存及加载**：基于 KV 接口快速写 Checkpoint，并支持将数据持久化到二级缓存保证数据可靠性。Checkpoint恢复时各节点将 Checkpoint 分片快速加载到异构对象中，利用异构对象的卡间直通传输及 P2P 数据分发能力，快速将 Checkpoint 传递到各节点 HBM。
- **微服务状态数据快速读写**：基于 KV 接口实现内存级读写微服务状态数据，并支持将数据持久化到二级缓存保证数据可靠性。

### openYuanrong datasystem 架构

![](./docs/source_zh_cn/images/logical_architecture.png)

openYuanrong datasystem 由三个部分组成：

- **多语言SDK**：提供 Python/C++ 语言接口，封装 heterogeneous object 、KV 以及 object 接口，支撑业务实现数据快速读写。提供两种类型接口：
  - **heterogeneous object**：基于 NPU 卡的 HBM 内存抽象异构对象接口，实现昇腾 NPU 卡间数据高速直通传输。同时提供 H2D/D2H 高速迁移接口，实现数据快速在 DRAM/HBM 之间传输。
  - **KV**：基于共享内存实现免拷贝的 KV 接口，实现高性能数据缓存，支持通过对接外部组件提供数据可靠性语义。
  - **object**：基于共享内存实现近计算的本地对象缓存，实现函数间高效数据流转，支撑Distributed Futures编程模型。

- **worker**：openYuanrong datasystem 的核心组件，用于分配管理 DRAM/SSD 资源以及元数据，提供分布式多级缓存能力。

- **集群管理**：依赖 ETCD，实现节点发现/健康检测，支持故障恢复及在线扩缩容。

![](./docs/source_zh_cn/images/deployment.png)

openYuanrong datasystem 的部署视图如上图所示：

- 需部署 ETCD 用于集群管理。
- 每个节点需部署 worker 进程并注册到 ETCD。
- SDK 集成到用户进程中并与同节点的 worker 通信。

各组件间的数据传输协议如下：

- SDK 与 worker 之间通过共享内存读写数据。
- worker 和 worker 之间通过 TCP/RDMA 传输数据（当前版本仅支持 TCP，RDMA/UB 即将支持）。
- 异构对象 HBM 之间通过 HCCS/RoCE 卡间直通传输数据。

## 入门

### 安装 openYuanrong datasystem

#### pip 方式安装

**前置要求：** Python 版本需为 3.9、3.10 或 3.11。

- 安装 openYuanrong datasystem 完整发行版（包含Python SDK、C++ SDK以及命令行工具）：
  ```bash
  pip install openyuanrong-datasystem
  ```

#### 源码编译方式安装

使用源码编译方式安装 openYuanrong datasystem 可以参考文档：[源码编译安装 openYuanrong datasystem](./docs/source_zh_cn/installation/installation_linux.md#源码编译安装)

### 部署 openYuanrong datasystem

#### 进程部署

- 准备ETCD
  
  openYuanrong datasystem 的集群管理依赖 ETCD，请先在后台启动单节点 ETCD（示例端口 2379）：
  ```bash
  etcd --listen-client-urls http://0.0.0.0:2379 \
       --advertise-client-urls http://localhost:2379 &
  ```
- 一键部署

  安装 openYuanrong datasystem 完整发行版后，即可通过随包自带的 dscli 命令行工具一键完成集群部署。在当前启动一个监听端口号为 31501 的服务端进程：
  ```bash
  dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
  ```

- 一键卸载
  ```bash
  dscli stop --worker_address "127.0.0.1:31501"
  ```

更多进程部署参数与部署方式请参考文档：[openYuanrong datasystem 进程部署](./docs/source_zh_cn/deployment/deploy.md#openyuanrong-datasystem进程部署)

#### Kubernetes 部署

openYuanrong datasystem 还提供了基于 Kubernetes 容器化部署方式，部署前请确保部署环境集群已就绪 Kubernetes、Helm 及可访问的 ETCD 集群。

- 获取 openYuanrong datasystem helm chart 包

  安装 openYuanrong datasystem 完整发行版后，即可通过随包自带的 dscli 命令行工具在当前路径下快速获取 helm chart 包：
  ```
  dscli generate_helm_chart -o ./
  ```

- 编辑集群部署配置

  openYuanrong datasystem 通过 ./datasystem/values.yaml 文件进行集群相关配置，其中必配项如下：

  ```yaml
  global:
    # 其他配置项...

    # 镜像仓地址
    imageRegistry: ""
    # 镜像名字和镜像tag
    images:
      datasystem: "openyuanrong-datasystem:0.5.0"
    
    etcd:
      # ETCD集群地址
      etcdAddress: "127.0.0.1:2379"
  ```

- 集群部署

  Helm 会提交 DaemonSet，按节点依次拉起 openYuanrong datasystem 实例：

  ```bash
  helm install openyuanrong_datasystem ./datasystem
  ```

- 集群卸载

  ```bash
  helm uninstall openyuanrong_datasystem
  ```

更多 openYuanrong datasystem Kubernetes 高级参数配置请参考文档：[openYuanrong datasystem Kubernetes 部署](./docs/source_zh_cn/deployment/deploy.md#openyuanrong-datasystem-kubernetes部署)

### 代码样例

- heterogeneous object

  通过异构对象接口，将任意二进制数据以键值对形式写入 HBM：

  ```python
  import acl
  import os
  from yr.datasystem import Blob, DsClient, DeviceBlobList

  # hetero_dev_mset and hetero_dev_mget must be executed in different processes
  # because they need to be bound to different NPUs.
  def hetero_dev_mset():
      client = DsClient("127.0.0.1", 31501)
      client.init()

      acl.init()
      device_idx = 1
      acl.rt.set_device(device_idx)

      key_list = [ 'key1', 'key2', 'key3' ]
      data_size = 1024 * 1024
      test_value = "value"

      in_data_blob_list = []
      for _ in key_list:
          tmp_batch_list = []
          for _ in range(4):
              dev_ptr, _ = acl.rt.malloc(data_size, 0)
              acl.rt.memcpy(dev_ptr, data_size, acl.util.bytes_to_ptr(test_value.encode()), data_size, 1)
              blob = Blob(dev_ptr, data_size)
              tmp_batch_list.append(blob)
          blob_list = DeviceBlobList(device_idx, tmp_batch_list)
          in_data_blob_list.append(blob_list)
      client.hetero().dev_mset(key_list, in_data_blob_list)

  def hetero_dev_mget():
      client = DsClient("127.0.0.1", 31501)
      client.init()

      acl.init()
      device_idx = 2
      acl.rt.set_device(device_idx)

      key_list = [ 'key1', 'key2', 'key3' ]
      data_size = 1024 * 1024
      out_data_blob_list = []
      for _ in key_list:
          tmp_batch_list = []
          for _ in range(4):
              dev_ptr, _ = acl.rt.malloc(data_size, 0)
              blob = Blob(dev_ptr, data_size)
              tmp_batch_list.append(blob)
          blob_list = DeviceBlobList(device_idx, tmp_batch_list)
          out_data_blob_list.append(blob_list)
      client.hetero().dev_mget(key_list, out_data_blob_list, 60000)
      client.hetero().dev_delete(key_list)
  
  pid = os.fork()
  if pid == 0:
      hetero_dev_mset()
      os._exit(0)
  else:
      hetero_dev_mget()
      os.wait()
  ```

- KV

  通过 KV 接口，将任意二进制数据以键值对形式写入 DDR：

  ```python
  from yr.datasystem.ds_client import DsClient

  client = DsClient("127.0.0.1", 31501)
  client.init()

  key = "key"
  expected_val = b"value"
  client.kv().set(key, expected_val)

  val = client.kv().get([key])
  assert val[0] == expected_val

  client.kv().delete([key])
  ```

- object

  通过 object 接口，实现基于引用计数的缓存数据管理：

  ```python
  from yr.datasystem.ds_client import DsClient

  client = DsClient("127.0.0.1", 31501)
  client.init()

  # Increase the key's global reference
  key = "key"
  client.object().g_increase_ref([key])

  # Create shared memory buffer for key.
  value = bytes("val", encoding="utf8")
  size = len(value)
  buf = client.object().create(key, size)

  # Copy data to shared memory buffer.
  buf.memory_copy(value)

  # Publish the key.
  buf.publish()

  # Get the key.
  buffer_list = client.get([key], True)

  # Decrease the key's global reference, the lifecycle of this key will end afterwards.
  client.g_decrease_ref([key])
  ```

## 文档

有关 openYuanrong datasystem 安装指南、教程和 API 的更多详细信息，请参阅 [用户文档](https://pages.openeuler.openatom.cn/openyuanrong-datasystem/docs/zh-cn/latest/index.html)。

有关 openYuanrong 更多详细信息请参阅 [openYuanrong 文档](https://pages.openeuler.openatom.cn/openyuanrong/docs/zh-cn/latest/index.html)，了解如何使用 openYuanrong 开发分布式应用。

## 贡献

我们欢迎您对 openYuanrong 做各种形式的贡献，请参阅我们的[贡献者指南](https://pages.openeuler.openatom.cn/openyuanrong/docs/zh-cn/latest/contributor_guide/index.html)。

## 许可证

[Apache License 2.0](LICENSE)