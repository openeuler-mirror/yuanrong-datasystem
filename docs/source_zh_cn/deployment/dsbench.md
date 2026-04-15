# dsbench性能测试工具

<!-- TOC -->
- [dsbench性能测试工具](#dsbench性能测试工具)
  - [环境准备](#环境准备)
    - [安装Python](#安装python)
    - [安装并部署ETCD](#安装并部署etcd)
    - [SSH互信配置](#ssh互信配置)
    - [部署datasystem worker](#部署datasystem-worker)
  - [dsbench安装教程](#dsbench安装教程)
    - [安装方式](#安装方式)
  - [dsbench使用教程](#dsbench使用教程)
    - [KV性能测试](#kv性能测试)
      - [1. 单个测试（SINGLE模式）](#1-单个测试single模式)
      - [2. 全量测试（FULL模式）](#2-全量测试full模式)
      - [3. 定制化测试（CUSTOMIZED模式）](#3-定制化测试customized模式)
    - [全局参数使用示例](#全局参数使用示例)
    - [查看节点信息](#查看节点信息)
  - [命令行参数说明](#命令行参数说明)
    - [全局参数](#全局参数)
    - [dsbench kv](#dsbench-kv)
      - [测试配置参数](#测试配置参数)
      - [运行配置参数](#运行配置参数)
      - [集群配置参数](#集群配置参数)
    - [dsbench show](#dsbench-show)
  - [注意事项](#注意事项)

<!-- /TOC -->

本文档介绍dsbench性能测试工具的使用方法、命令行参数和配置说明。dsbench是一个用于测试datasystem性能的命令行工具，它会模拟客户端向datasystem的worker节点发送各种操作请求，以此来评估datasystem的性能表现。

## 环境准备

dsbench工具需要配合已部署好的datasystem worker使用，而datasystem worker的运行又依赖etcd服务，所以需要先完成以下环境准备工作：

### 安装Python

安装Python的详细步骤可以参考[dscli.md中的安装Python部分](dscli.md#安装python)。

### 安装并部署ETCD

datasystem worker运行需要etcd服务，因此需要先安装并部署etcd。

安装并部署ETCD的步骤可以参考[dscli.md中的安装并部署ETCD部分](dscli.md#安装并部署etcd)。

### SSH互信配置

如果需要在多台机器上运行dsbench测试，或者需要从远程机器访问datasystem worker，建议配置SSH互信（免密码登录）。

SSH互信配置的步骤可以参考[dscli.md中的SSH互信配置部分](dscli.md#ssh互信配置)。

### 部署datasystem worker

在使用dsbench进行性能测试之前，需要先部署好datasystem worker。可以使用dscli工具的start或up命令来部署worker节点。

部署datasystem worker的步骤可以参考[dscli.md中的dscli使用教程部分](dscli.md#dscli使用教程)。

## dsbench安装教程

dsbench命令与dscli命令集成在同一个openYuanrong datasystem安装包中，因此只要安装了openYuanrong datasystem，就可以同时使用dsbench和dscli这两个命令行工具。

### 安装方式

详细安装步骤请参考[openYuanrong datasystem的安装教程](dscli.md#dscli安装教程)，包括：

- [pip安装](dscli.md#pip安装)
- [安装自定义版本](dscli.md#安装自定义版本)
- [源码编译安装](dscli.md#源码编译安装)

安装完成后，可以通过以下命令检查dsbench是否安装成功：

```bash
dsbench --help
```

如果能正常显示dsbench的帮助信息，说明安装成功。

## dsbench使用教程

本节介绍dsbench的使用方法，dsbench主要提供了两个命令：`kv`（用于KV性能测试）和`show`（用于查看节点信息）。

### KV性能测试

使用`dsbench kv`命令可以进行KV性能测试，该命令支持三种测试模式：全量测试、定制化测试和单个测试。

#### 1. 单个测试（SINGLE模式）

如果不需要使用全量测试或定制化测试模式，可以直接指定测试参数来运行单个测试用例：

```bash
dsbench kv -n 100 -s 72KB -c 16 -t 2 -b 10 -S "127.0.0.1:31501,127.0.0.2:31501" -G "127.0.0.1:31501,127.0.0.2:31501"
```

参数说明：
- `-c, --client_num`：KVClient对象的数量，即客户端数（默认：8，范围：1-128）。每个客户端是线程安全的，多个线程可以共享同一个客户端对象
- `-t, --thread_num`：每个客户端使用的线程数（默认：1）。实际总线程数 = client_num × thread_num（总线程数范围：1-128）
- `-b, --batch_num`：每批处理的请求数量（默认：1，范围：1-10000）
- `-n, --num`：每个worker进程的key数量，会被均匀分配给所有线程（默认：100）
- `-s, --size`：数据大小（默认：1MB，支持2B/4KB/8MB/1GB等格式，也可以直接写数字，单位为字节）
- `-p, --prefix`：键的前缀（默认：Bench，长度：1-64，只能包含字母、数字和下划线）

也可以使用默认参数快速运行测试：

```bash
dsbench kv -S "127.0.0.1:31501" -G "127.0.0.1:31501"
```

默认参数如下：
- 客户端数：8
- 每客户端线程数：1
- 批次大小：1
- 测试数据数量：100
- 数据大小：1MB
- 键前缀：Bench

使用`--concurrent`参数可以运行并发读写工作负载测试：

```bash
dsbench kv --concurrent -c 16 -t 2 -n 100 -s 1MB -b 10 -S "127.0.0.1:31501" -G "127.0.0.1:31501"
```

并发模式与默认模式的区别：
- **默认模式**：按顺序执行 set → get → del
- **并发模式**：先预填充数据（prefill），然后并发执行 set 和 get 操作，最后执行 del 清理。适用于测试并发读写场景下的性能表现

#### 2. 全量测试（FULL模式）

使用`-a`或`--all`参数可以运行所有内置的测试用例，这种模式需要每个worker节点至少有25GB的共享内存：

```bash
dsbench kv --all -S "127.0.0.1:31501,127.0.0.2:31501,127.0.0.3:31501" -G "127.0.0.1:31501,127.0.0.2:31501,127.0.0.3:31501"
```

这种模式会使用内置配置运行一系列预设的测试用例，全面评估datasystem在不同场景下的性能表现。

#### 3. 定制化测试（CUSTOMIZED模式）

使用`-f`或`--testcase_file`参数可以指定自定义的测试用例文件，按照自己的需求运行特定的测试场景：

```bash
dsbench kv -f testcases.json -S "127.0.0.1:31501,127.0.0.2:31501" -G "127.0.0.1:31501,127.0.0.2:31501"
```

测试用例文件格式（JSON格式）如下：

```json
[
    {"num": 1000, "size": "1MB", "client_num": 1, "thread_num": 1, "batch_num": 1},
    {"num": 250, "size": "1MB", "client_num": 4, "thread_num": 1, "batch_num": 1},
    {"num": 125, "size": "1MB", "client_num": 8, "thread_num": 1, "batch_num": 1}
]
```

每个测试用例需要包含以下参数：
- `num`：每个worker进程的key数量（分配给所有线程）
- `size`：数据大小（支持2B/4KB/8MB/1GB等格式，也可以直接写数字，单位为字节）
- `client_num`：客户端数量
- `thread_num`：每个客户端使用的线程数
- `batch_num`：每批处理的请求数量

### 全局参数使用示例

全局参数是指适用于所有dsbench子命令（如kv、show等）的参数，用于控制通用行为（如日志级别）。这些参数需要放在子命令前面，可以与任何子命令一起使用。详细说明请参考[全局参数](#全局参数)部分：

```bash
# 设置日志级别为INFO并启用日志监控
dsbench --min_log_level=0 --log_monitor_enable=true kv --all -S "127.0.0.1:31501" -G "127.0.0.1:31501"

# 仅设置日志级别为WARNING
dsbench --min_log_level=1 kv -f testcases.json -S "127.0.0.1:31501" -G "127.0.0.1:31501"
```

### 查看节点信息

使用`dsbench show`命令可以查看当前节点的系统信息，包括IP地址、版本号、内存使用情况、CPU信息等。

```bash
dsbench show
```

输出示例：

```
IP: 192.168.1.100
Version: 0.1.0
Commit ID: abcdef1234567890
Total Memory: 64GB
Free Memory: 48GB
THP: disabled
HugePages: Total=1024, Free=1024, Size=2MB
CPU MHz: 2500
```

## 命令行参数说明

### 全局参数

这些参数可以与任何子命令一起使用，需要放在子命令前面：

| 参数 | 类型 | 说明 |
|------|------|------|
| `--min_log_level` | 整数 | 设置数据系统客户端日志级别：INFO=0, WARNING=1, ERROR=2（默认：2，只显示错误日志） |
| `--log_monitor_enable` | 布尔值 | 启用数据系统客户端日志监控功能，需要传入true或false（默认：false） |

### dsbench kv

`dsbench kv`命令用于进行KV性能测试，支持以下参数：

#### 测试配置参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `-a, --all` | 开关 | 运行所有内置测试用例，需要每个worker至少有25GB共享内存 |
| `-f, --testcase_file` | 字符串 | 自定义测试用例文件的路径（必须是.json格式） |

#### 运行配置参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `-c, --client_num` | 整数 | KVClient对象的数量（默认：8，范围：1-128）。每个客户端是线程安全的，多个线程可共享同一个客户端对象 |
| `-t, --thread_num` | 整数 | 每个客户端使用的线程数（默认：1）。实际总线程数 = client_num × thread_num(总线程数范围：1-128) |
| `-b, --batch_num` | 整数 | 每批处理的请求数量（默认：1，范围：1-10000） |
| `-n, --num` | 整数 | 每个worker进程的key数量，会被均匀分配给所有线程（默认：100） |
| `-s, --size` | 字符串 | 数据大小（默认：1MB，支持2B/4KB/8MB/1GB等格式，也可以直接写数字，单位为字节） |
| `-p, --prefix` | 字符串 | 键的前缀（默认：Bench，长度：1-64，只能包含字母、数字和下划线） |
| `--owner_worker` | 字符串 | 对象元数据所属的worker地址（默认：空） |
| `--numa` | 字符串 | 绑定测试进程到特定的NUMA节点（例如0-3,10-20,1,2,3），与worker参数的CPU亲和性设置不同（默认：空） |
| `--concurrent` | 开关 | 启用并发读写工作负载模式。默认模式按顺序执行 set->get->del；并发模式先预填充数据，然后并发执行 set 和 get，最后 del 清理。适用于压测并发读写场景 |
| `--skip_local` | 开关 | 在执行 get 操作时跳过本地 worker 的数据。启用后，每个 get 任务会跳过 `set_worker_addresses` 中与当前 get worker 地址相同的那一份数据；若当前地址不在 `set_worker_addresses` 中，则不跳过 |

#### 集群配置参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `-S, --set_worker_addresses` | 字符串 | 用于执行set操作的worker地址列表（必须填写，格式："127.0.0.1:31501,127.0.0.2:31501"）。dsbench会为列表中的每个worker创建一个任务来执行set操作。 |
| `-G, --get_worker_addresses` | 字符串 | 用于执行get操作的worker地址列表（必须填写，格式："127.0.0.1:31501,127.0.0.2:31501"）。dsbench会为列表中的每个worker创建一个任务来执行get操作，同时也会使用该列表中的第一个worker来执行del操作。 |
| `--access_key` | 字符串 | 访问密钥（默认：空） |
| `--secret_key` | 字符串 | 密钥（默认：空） |

### dsbench show

`dsbench show`命令用于查看当前节点的系统信息，该命令不需要额外参数。

## 注意事项

1. 在运行性能测试之前，请确保：
   - datasystem worker已经正确部署并正常运行
   - 如果是多节点环境，已配置好SSH互信（免密码登录），dsbench当前使用的SSH配置有如下限制：
     * 用户名：当前执行命令的用户
     * RSA私钥路径：~/.ssh/id_rsa
     * 默认端口号22，可通过~/.ssh/config自定义
   - 多个节点之间网络通畅，可以互相访问
