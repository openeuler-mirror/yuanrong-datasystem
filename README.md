[查看中文](./README_CN.md)


<!-- TOC -->

- [What Is yr-datasystem](#what-is-yr-datasystem)
- [Installation](#installation)
  - [Pip mode method installation](#pip-mode-method-installation)
  - [Source code compilation installation](#source-code-compilation-installation)
- [Deployment](#deployment)
  - [Process deployment](#process-deployment)
  - [Kubernetes deployment](#kubernetes-deployment)
- [Quickstart](#quickstart)
- [Docs](#docs)
- [License](#license)

<!-- /TOC -->

## What Is yr-datasystem

yuanrong-datasystem is a distributed data caching system that leverages the HBM/DRAM/SSD resources of a compute cluster to build a multi-level cache close to computation, improving data access performance in scenarios such as model training and inference, big data, and microservices.

![](https://gitee.com/mindspore/yr-datasystem/raw/develop/docs/source_en/getting-started/image/logical_architecture.png)

yuanrong-datasystem consists of three components:

- **Multi-language SDK**: It provides Python/C++ language interfaces, encapsulating various semantics such as heterogeneous objects, key-value (KV), and objects, supporting fast data read and write for business implementation.
  - **heterogeneous object**: Based on the NPU card's HBM memory abstraction heterogeneous object interface, it enables high-speed direct data transmission between Ascend NPU cards. It also provides H2D (Host to Device) and D2H (Device to Host) high-speed migration interfaces, facilitating fast data transfer between DRAM and HBM.
  - **KV**: Implements zero-copy Key-Value data read/write via shared memory for high-performance caching, with support for data reliability semantics through external component integration.
  - **object**: Abstracts data objects using host-side shared memory, implements reference-counted lifecycle management, and encapsulates shared memory as buffers with direct pointer-based read/write access.

- **worker**: The core component of yuanrong-datasystem, responsible for managing DRAM/SSD and metadata while providing near-computing caching capabilities.
  - **Zero-Copy Shared Memory**: Provides data read/write interfaces through shared memory, eliminating data copying between the SDK and worker to enhance performance.
  - **NPU Concurrent Communication**: The heterogeneous object abstraction enables direct inter-NPU communication with automatic HCCL send/receive sequencing, achieving concurrent data transfers across both local NPUs and remote nodes via H2H access.
  - **P2P Data Distribution**: Implements a load-balancing strategy for P2P transfers during large-scale data replication, enabling new data receivers to serve as providers and fully utilizing inter-card link bandwidth.
  - **Metadata Management**: Implements distributed metadata management to achieve linear horizontal scaling and eliminate single-point bottlenecks. Supports metadata reliability for enhanced cluster availability, while providing data publish-subscribe capabilities.
  - **Multi-Level Cache Eviction**: Implements LRU-based cache replacement across tiers, maintaining hot data in memory, warm data on disk, and cold data in secondary cache.
  - **Data Management**: Provides multiple data lifecycle management capabilities including reference counting and TTL. Controls read-write consistency with support for multiple consistency levels. Manages data replicas across nodes, employing multi-copy caching for hot data to improve read efficiency.

- **Cluster Management**: Leverages ETCD to enable node discovery/health monitoring, supporting fault recovery and online scaling (both expansion and contraction).

![Deployment View of yuanrong-datasystem](https://gitee.com/mindspore/yr-datasystem/raw/develop/docs/source_en/getting-started/image/deployment.png)

Deployment Architecture of yuanrong-datasystem (as illustrated in the diagram above):

- The cluster requires **ETCD** deployment for cluster management.
- Each node must deploy a **worker** process registered with ETCD.
- The **SDK** is integrated into user processes and communicates with the local worker.

Data Transmission:
- **SDK ↔ worker**: Shared memory for data read/write.
- **worker ↔ worker**: TCP/RDMA (current version supports TCP only; RDMA support planned).
- **HBM Heterogeneous Objects**: Direct cross-device transmission via HCCS/RoCE.

## Installation

### Pip mode method installation

Install the version available on PyPI:

```bash
pip install yr-datasystem
```

To install a custom version, refer to the documentation [yr-datasystem custom version installation](./docs/source_zh_cn/getting-started/install.md#安装自定义版本)

### Source code compilation installation

To install yr-datasystem using the source code compilation method, refer to the documentation: [Source Code Compilation Installation of yr-datasystem](./docs/source_zh_cn/getting-started/install.md#源码编译方式安装yr-datasystem版本).

## Deployment

### Process deployment

yr-datasystem cluster can be quickly deployed using the ds-cli tool. For more information, see [yr-datasystem Process Deployment](./docs/source_zh_cn/getting-started/deploy.md#yr-datasystem进程部署).

### Kubernetes deployment

yr-datasystem also supports containerized deployment on Kubernetes. For more information, see [yr-datasystem Kubernetes Deployment](./docs/source_zh_cn/getting-started/deploy.md#yr-datasystem-kubernetes部署).

## Quickstart

For a quick introduction to heterogeneous objects, key-value (KV), and object semantics, refer to the following documentation.
- [Heterogeneous object quickstart](./docs/source_zh_cn/getting-started/overview.md#异构对象)
- [KV quickstart](./docs/source_zh_cn/getting-started/overview.md#kv)
- [Object quickstart](./docs/source_zh_cn/getting-started/overview.md#object)

## Docs

More details about installation guide, tutorials and APIs, please see the [User Documentation](docs).

## License

[Apache License 2.0](LICENSE)