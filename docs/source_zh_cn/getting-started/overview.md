# 入门

yuanrong-datasystem 是一个分布式缓存系统，利用计算集群的 HBM/DRAM/SSD 资源构建近计算多级缓存，提升模型训练及推理、大数据、微服务等场景数据访问性能。

## 整体架构

![](./image/logical_architecture.png)

yuanrong-datasystem 由三个部分组成：

- **多语言SDK**：提供 Python/C++ 语言接口，封装 heterogeneous object/KV/object 多种语义，支撑业务实现数据快速读写。
  - **heterogeneous object**：基于 NPU 卡的 HBM 内存抽象异构对象接口，实现昇腾 NPU 卡间数据高速直通传输。同时提供 H2D/D2H 高速迁移接口，实现数据快速在 DRAM/HBM 之间传输。
  - **KV**：基于共享内存实现免拷贝的 KV 数据读写，实现高性能数据缓存，支持通过对接外部组件提供数据可靠性语义。
  - **object**：基于 host 侧共享内存抽象数据对象，实现基于引用计数的生命周期管理，将共享内存封装为 buffer，提供指针直接读写。

- **worker**：yuanrong-datasystem 的核心组件，用于管理 DRAM/SSD 及元数据，提供近计算缓存能力。
  - **共享内存免拷贝**：基于共享内存对外提供数据读写接口，避免 SDK 与 worker 间数据拷贝，提升性能。
  - **NPU 间并发通信**：通过异构对象抽象实现卡间直通通信，自动协调 NPU 间 HCCL 收发顺序，实现 NPU 卡间数据并发传输。
  - **P2P 数据分发**：大规模数据复制时，实现 P2P 传输负载均衡策略，允许由新的数据接收者提供数据，充分利用卡间链路带宽。
  - **元数据管理**：分布式元数据管理，实现系统水平线性扩展，避免单点瓶颈。支持元数据可靠性，提升集群可用性。提供数据订阅发布能力。
  - **多级缓存淘汰置换**：支持基于 LRU 的多级缓存置换淘汰，热数据在内存，暖数据在磁盘，冷数据在二级缓存。
  - **数据管理**：提供引用计数，TTL 等多种数据生命周期管理能力。控制数据读写一致性，提供多种一致性能力。管理各节点数据副本，热点数据多副本缓存，提升读取效率。

- **集群管理**：依赖 ETCD，实现节点发现/健康检测，支持故障恢复及在线扩缩容。

![](./image/deployment.png)

yuanrong-datasystem 的部署视图(如上图所示)：

- 需部署 ETCD 用于集群管理。
- 每个节点需部署 worker 进程并注册到 ETCD。
- SDK 集成到用户进程中并与同节点的 worker 通信。

数据传输协议：

- SDK 与 worker 之间通过共享内存读写数据。
- worker 和 worker 之间通过 TCP/RDMA 传输数据（当前版本仅支持 TCP，后续版本支持 RDMA）。
- 异构对象 HBM 之间通过 HCCS/RoCE 卡间直通传输数据。

# 快速开始

## 安装部署

yuanrong-datasystem 提供了 pip install 及源码安装两种方式，详细请参考[安装指南](install.md)。
yuanrong-datasystem 提供了两种部署方式：

- [快速进程部署](deploy.md#openyuanrong-datasystem进程部署)
- [在 Kubernetes上部署](deploy.md#openyuanrong-datasystem-kubernetes部署)

## 开发指南

### 异构对象

异构对象实现对 HBM 内存的抽象管理，能够高效实现 D2D/H2D/D2H 的数据传输，加速 AI 训推场景数据读写。

主要应用场景

- **LLM 长序列推理 KVCache**：基于异构对象提供分布式多级缓存 (HBM/DRAM/SSD) 和高吞吐 D2D/H2D/D2H 访问能力，构建分布式 KV Cache，实现 Prefill 阶段的 KVCache 缓存以及 Prefill/Decode 实例间 KV Cache 快速传递，提升推理吞吐。
- **模型推理实例 M->N 快速弹性**：利用异构对象的卡间直通及 P2P 数据分发能力实现模型参数快速复制。
- **训练场景 CheckPoint 快速加载到 HBM**：各节点将待恢复的 Checkpoint 分片加载到异构对象中，利用异构对象的卡间直通传输及 P2P 数据分发能力，快速将 Checkpoint 传递到各节点 HBM。

[异构对象开发指南](../development-guide/hetero.md)

### KV

基于共享内存实现免拷贝的 KV 数据读写，支持通过对接外部组件提供数据可靠性语义，支持数据在 DRAM / SSD / 二级缓存之间置换，实现大容量高性能缓存。

主要应用场景

- **训练场景 Checkpoint 快速保存及加载**：基于 KV 接口快速读写 Checkpoint，并支持将数据持久化到二级缓存保证数据可靠性。

[KV开发指南](../development-guide/kv.md)

### Object

基于共享内存实现 Object 语义读写，提供基于引用计数管理生命周期，将共享内存抽象为 buffer，直接映射共享内存指针，提供更底层灵活的编程接口。

[Object开发指南](../development-guide/object.md)