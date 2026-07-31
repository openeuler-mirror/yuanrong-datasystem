# 基于 Bazel 自定义编译

DataSystem 支持通过 `bash build.sh` 一键 Bazel 编译，也支持直接使用 `bazel build` 命令进行精细控制。

## 一、环境依赖

### OpenEuler OS

```bash
yum install -y autoconf automake libtool m4
```

### 安装 Bazel

参考 [Bazel 官方文档](https://bazel.build/install) 安装 Bazel 7.4 及以上版本。

### Sanitizer 运行时（可选）

使用 `-S address` 或 `-S thread` 时，编译和测试环境需要提供 GCC sanitizer 运行时库。OpenEuler 可通过以下命令安装：

```bash
yum install -y libasan libtsan
```

## 二、通过 build.sh 编译（推荐）

`build.sh` 已支持 Bazel 构建系统，通过 `-b bazel` 参数切换。编译产物输出到 `output/` 目录，与 CMake 编译结构对齐。

### 基础用法

```bash
# Release 编译（默认）
bash build.sh -b bazel

# Debug 编译
bash build.sh -b bazel -d

# 指定编译线程数
bash build.sh -b bazel -j 16
```

### 编译产物

编译完成后 `output/` 目录结构：

```text
output/
├── yr-datasystem-v0.7.7.tar.gz           # 部署包
├── cpp/                                  # 外部 SDK（find_package / Bazel 集成）
│   ├── BUILD.bazel                       # Bazel 外部项目模板
│   ├── DATASYSTEM_SYM 
│   |    └── libdatasystem.so.sym         # 符号表文件
│   ├── include/datasystem/               # C++ 头文件
│   └── lib/
│       ├── libdatasystem.so              # SDK 动态库
│       └── cmake/Datasystem/             # CMake find_package 配置
└── openyuanrong_datasystem-*.whl         # Python wheel（-P on 时）
```

`yr-datasystem-v*.tar.gz` 解压内容：

```text
datasystem/
├── sdk/cpp/                 # C++ SDK
│   ├── include/datasystem/  # 头文件
│   └── lib/
│   │    ├── libdatasystem.so
│   │    └── cmake/Datasystem/
|   └── DATASYSTEM_SYM
│        └── libdatasystem.so.sym # 符号表文件
├── service/                 # 服务端
│   ├── datasystem_worker    # Worker 进程
│   ├── lib/
│   │   ├── libdatasystem_worker.so
│   │   └── libjemalloc.so.2
|   └── DATASYSTEM_SYM
│       └── libdatasystem_worker.sym # worker符号表文件
│   ├── worker_config.json
│   └── cluster_config.json
├── cli/                     # CLI 工具
├── tools/                   # 附加工具（仅 -t build/run 时）
├── VERSION
├── README.md
└── .commit_id
```

### 常用参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-b bazel` | 使用 Bazel 构建 | cmake |
| `-r` | Release 模式 | 默认 |
| `-d` | Debug 模式 | - |
| `-j <N>` | 编译线程数 | 8 |
| `-s on\|off` | Strip 符号表 | on |
| `-P on\|off` | 构建 Python wheel | on |
| `-t build\|run\|off` | 测试构建/运行 | off |
| `-u <N>` | 测试并行度 | 8 |
| `-S address\|thread\|undefined\|off` | Sanitizer | off |
| `-c on\|off` | 覆盖率 | off |

### 使用示例

```bash
# Release 编译 + 生成 wheel
bash build.sh -b bazel -r -j 8 -P on

# 编译并运行测试
bash build.sh -b bazel -r -j 8 -t run

# 仅编译测试（不运行）
bash build.sh -b bazel -r -j 8 -t build

# 开启 ASan
bash build.sh -b bazel -r -S address

# 开启 TSan
bash build.sh -b bazel -r -S thread

# 开启 URMA
bash build.sh -b bazel -r -M on

# 开启覆盖率
bash build.sh -b bazel -r -c on
```

### Sanitizer 模式

Bazel 模式复用 CMake 的 `-S` 参数：`address` 对应 ASan，`thread` 对应 TSan。

```bash
# ASan level0 验证
bash build.sh -b bazel -r -S address -t run -l level0

# TSan level0 验证
bash build.sh -b bazel -r -S thread -t run -l level0
```

启用 sanitizer 后，脚本会先用 `bazel query` 按测试标签筛选目标，而不是直接运行
`bazel test //...`。测试需要同时满足本次指定的范围标签（如 `level0`、`ut`）和
sanitizer 标签（如 `asan`、`tsan` 或 `sanitizer`），并且不能带有 `manual`
标签。TSan 对线程调度更敏感，建议只给已确认稳定的测试添加 `tsan` 标签。

### 参数兼容性说明

以下参数在 Bazel 模式下不支持或自动忽略：

| 参数 | Bazel 模式行为 |
|------|---------------|
| `-n` (Ninja) | 忽略，Bazel 有自己的调度器 |
| `-i` (增量) | 忽略，Bazel 默认增量编译 |
| `-D` (下载 UB) | 忽略 |
| `-x` (jemalloc profiling) | 暂不支持 |
| `-J` (Java API) | 暂不支持 |
| `-G` (Go SDK) | 暂不支持 |
| `-A` (RDMA/UCX) | 暂不支持 |

## 三、bazel build 高级用法

直接使用 `bazel build` 命令可以对编译目标进行精细控制。

### 常用构建目标

```bash
# 构建 SDK 包（头文件 + libdatasystem.so + cmake config）
bazel build //bazel:datasystem_sdk --config=release

# 构建 Python wheel
bazel build //bazel:datasystem_wheel --config=release

# 仅编译 libdatasystem.so
bazel build //:datasystem --config=release

# 编译 worker 进程
bazel build //src/datasystem/worker:datasystem_worker --config=release

# 编译 worker 共享库
bazel build //src/datasystem/worker:datasystem_worker_shared --config=release

# 编译单个 target（调试时常用）
bazel build //src/datasystem/common/rdma:fast_transport_manager_wrapper --config=release
```

### 编译配置选项

#### 构建类型

```bash
# Release 版本（-O2 优化，strip 符号）
bazel build //bazel:datasystem_sdk --config=release

# Debug 版本（-O0，-ggdb，保留符号）
bazel build //bazel:datasystem_sdk --config=debug
```

#### 功能开关

```bash
# 开启 URMA 支持
bazel build //bazel:datasystem_sdk --config=urma --config=release

# 开启 Pipeline H2D（自动启用 URMA）
bazel build //bazel:datasystem_sdk --config=pipeline_h2d --config=release

# 开启异构计算
bazel build //bazel:datasystem_sdk --config=hetero --config=release

# 开启 Perf 日志
bazel build //bazel:datasystem_sdk --config=perf --config=release

# 编译测试目标
bazel build //... --config=test --config=release
```

#### Sanitizer（内存/线程错误检测）

```bash
# Address Sanitizer（内存越界、泄漏）
bazel build //bazel:datasystem_sdk --config=asan

# Thread Sanitizer（数据竞争）
bazel build //bazel:datasystem_sdk --config=tsan

# Undefined Behavior Sanitizer（未定义行为）
bazel build //bazel:datasystem_sdk --config=ubsan
```

#### 覆盖率

```bash
bazel build //... --config=coverage --config=release
```

#### 指定 Python 版本

```bash
bazel build //bazel:datasystem_wheel --config=py39 --config=release
bazel build //bazel:datasystem_wheel --config=py310 --config=release
bazel build //bazel:datasystem_wheel --config=py311 --config=release
bazel build //bazel:datasystem_wheel --config=py312 --config=release
bazel build //bazel:datasystem_wheel --config=py313 --config=release
```

> 可选版本：3.9, 3.10, 3.11, 3.12, 3.13

#### 指定 glibc 版本

```bash
bazel build //bazel:datasystem_wheel --define glibc_version=2.34 --config=release
```

> 可选版本：2.34, 2.35, 2.36, 2.37, 2.38。未指定时自动使用系统 glibc 版本。

#### 运行测试

```bash
# 运行所有测试
bazel test //... --config=test --config=release --jobs=8

# 运行指定包下的测试
bazel test //tests/st/... --config=test --config=release

# 设置超时时间（秒）
bazel test //... --config=test --config=release --test_timeout=120
```

### 配置选项完整参考

| 配置 | 说明 | 对应 .bazelrc |
|------|------|--------------|
| `--config=release` | Release 构建 (-O2) | `-O2 -DNDEBUG -fstack-protector-strong` |
| `--config=debug` | Debug 构建 (-O0) | `-O0 -ggdb -ftrapv -fstack-check` |
| `--config=urma` | 启用 URMA | `-DUSE_URMA -DURMA_OVER_UB` |
| `--config=pipeline_h2d` | 启用 Pipeline H2D | 自动启用 URMA |
| `--config=hetero` | 启用异构计算 | `-DBUILD_HETERO` |
| `--config=perf` | 启用 Perf 日志 | `-DENABLE_PERF` |
| `--config=test` | 编译测试目标 | `-DWITH_TESTS` |
| `--config=asan` | Address Sanitizer | `-fsanitize=address` |
| `--config=tsan` | Thread Sanitizer | `-fsanitize=thread` |
| `--config=ubsan` | UBSan | `-fsanitize=undefined` |
| `--config=coverage` | 覆盖率 | `-fprofile-arcs -ftest-coverage` |
| `--config=py39`~`py313` | Python 版本 | rules_python 版本选择 |

### 组合示例

```bash
# Release + URMA + 测试
bazel build //... --config=release --config=urma --config=test -j 16

# Debug + ASan + 覆盖率
bazel build //... --config=debug --config=asan --config=coverage -j 8

# Release wheel for Python 3.11 + glibc 2.34
bazel build //bazel:datasystem_wheel --config=release --config=py311 --define glibc_version=2.34
```

## 四、源码编译安装Client

### 1. bazelrc 默认选项

```text
common --enable_bzlmod=false   # 关闭 bzlmod
build --cxxopt=-std=c++17    # 使用 c++17编译
# 关闭 RDMA 支持
build --cxxopt=-DDISABLE_RDMA
# 去掉grpc部分功能和依赖
build --define=grpc_no_xds=true
build --define=grpc_no_binder=true
build --define=grpc_no_ares=true
# urma 支持
build:urma --define=enable_urma=true
build:urma --copt=-DUSE_URMA
build:urma --copt=-DURMA_OVER_UB

# sanitizer 支持
build:asan --copt=-fsanitize=address
build:asan --copt=-fno-omit-frame-pointer
build:asan --linkopt=-fsanitize=address
test:asan --test_env=ASAN_OPTIONS=detect_leaks=0:detect_odr_violation=0
test:asan --test_env=LSAN_OPTIONS=detect_leaks=0

build:tsan --copt=-fsanitize=thread
build:tsan --linkopt=-fsanitize=thread
```

### 2. WORKSPACE 配置

```python
load("@yuanrong-datasystem//bazel:ds_deps.bzl", "ds_deps", "setup_grpc")

ds_deps()    # 加载 datasystem 默认依赖

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

# 关闭 grpc 对部分语言支持
switched_rules_by_language(
    name = "com_google_googleapis_imports",
    cc = True,
    go = False,
    grpc = True,
    java = False,
    python = False,
)

# 加载 grpc 依赖
setup_grpc()

# 配置 python 构建环境
load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")

python_configure(name = "local_config_python")

load("@rules_python//python:repositories.bzl", "py_repositories")

py_repositories()
```

### 3. BUILD 添加 client 依赖关系

依赖关系添加：`@yuanrong-datasystem//src/datasystem/client:datasystem`

```python
cc_binary(
    name = "datasystem_example",
    srcs = [
        "datasystem_example.cpp",
    ],
    deps = [
        "@yuanrong-datasystem//src/datasystem/client:datasystem",
    ],
)
```

### 4. C++ 代码

添加 datasystem 头文件，使用接口：

```cpp
#include "datasystem/datasystem.h"

using datasystem::KVClient;
```

## 五、源码编译安装Data Worker
### 1. bazelrc 默认选项
```text
common --enable_bzlmod=false   # 关闭 bzlmod
build --cxxopt=-std=c++17    # 使用 c++17编译
# 关闭 RDMA 支持
build --cxxopt=-DDISABLE_RDMA
# 去掉grpc部分功能和依赖
build --define=grpc_no_xds=true
build --define=grpc_no_binder=true
build --define=grpc_no_ares=true
# urma 支持
build:urma --define=enable_urma=true
build:urma --copt=-DUSE_URMA
build:urma --copt=-DURMA_OVER_UB
```

### 2. WORKSPACE 配置
```python
workspace(name = "my_worker_project")

local_repository(
    name = "yuanrong-datasystem",
    path = "/path/to/yuanrong-datasystem",
)

load("@yuanrong-datasystem//bazel:ds_deps.bzl", "ds_deps", "setup_grpc")
load("@yuanrong-datasystem//bazel:ascend_configure.bzl", "ascend_configure")

ds_deps()
ascend_configure(name = "local_ascend")

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")
switched_rules_by_language(
    name = "com_google_googleapis_imports",
    cc = True,
    go = False,
    grpc = True,
    java = False,
    python = False,
)

setup_grpc()

load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")
python_configure(name = "local_config_python")

load("@rules_python//python:repositories.bzl", "py_repositories")
py_repositories()
```

### 3. BUILD 添加Data Worker依赖关系

依赖关系添加：`@yuanrong-datasystem//src/datasystem/worker:worker_main`
```python
cc_binary(
    name = "my_worker",
    srcs = [
        "my_main.cpp",
    ],
    deps = [
        "@yuanrong-datasystem//src/datasystem/worker:worker_main",
    ],
)
```

### 4. C++ 代码
```cpp
#include "datasystem/data_worker.h"
#include "datasystem/utils/coordinator_discovery.h"

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

class UserCoordinatorDiscovery final : public datasystem::ICoordinatorDiscovery {
public:
    datasystem::Status GetCoordinators(std::vector<std::string> &addresses) override
    {
        addresses = { "coordinator.example.com:31501" };
        return datasystem::Status::OK();
    }
};

int main() {
    datasystem::DataWorkerOptions options;
    options.configFilePath = "/path/to/worker_config.json";
    options.coordinatorDiscovery = std::make_shared<UserCoordinatorDiscovery>();

    auto status = datasystem::DataWorker::GetInstance()->InitAndRun(options);
    if (status.IsError()) {
        fprintf(stderr, "Worker InitAndRun failed: %s\n", status.ToString().c_str());
        return -1;
    }
    printf("Worker exited normally\n");
    return 0;
}
```

`coordinatorDiscovery`在带参数启动时必须非空，带参数入口始终使用它选择并访问Coordinator后端，
无需在配置中设置`coordinator_address`。命令行和嵌入式静态启动入口才通过`coordinator_address`选择
Coordinator或ETCD/metastore。`onStart`和`onStop`为可选回调，但必须同时配置或同时留空。

### 5. 编译命令
```bash
bazel build //:my_worker
```

产物为 `bazel-bin/my_worker`，全静态链接，不依赖外部 `.so`。

## 六、源码编译安装 Coordinator

### 1. bazelrc 默认选项

```text
common --enable_bzlmod=false   # 关闭 bzlmod
build --cxxopt=-std=c++17    # 使用 c++17编译
# 关闭 RDMA 支持
build --cxxopt=-DDISABLE_RDMA
# 去掉grpc部分功能和依赖
build --define=grpc_no_xds=true
build --define=grpc_no_binder=true
build --define=grpc_no_ares=true
# urma 支持
build:urma --define=enable_urma=true
build:urma --copt=-DUSE_URMA
build:urma --copt=-DURMA_OVER_UB
```

### 2. WORKSPACE 配置

```python
workspace(name = "my_coordinator_project")

local_repository(
    name = "yuanrong-datasystem",
    path = "/path/to/yuanrong-datasystem",
)

load("@yuanrong-datasystem//bazel:ds_deps.bzl", "ds_deps", "setup_grpc")
load("@yuanrong-datasystem//bazel:ascend_configure.bzl", "ascend_configure")

ds_deps()
ascend_configure(name = "local_ascend")

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")
switched_rules_by_language(
    name = "com_google_googleapis_imports",
    cc = True,
    go = False,
    grpc = True,
    java = False,
    python = False,
)

setup_grpc()

load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")
python_configure(name = "local_config_python")

load("@rules_python//python:repositories.bzl", "py_repositories")
py_repositories()
```

### 3. BUILD 添加 Coordinator 依赖关系

依赖关系添加：`@yuanrong-datasystem//src/datasystem/coordinator:coordinator_server`

```python
cc_binary(
    name = "my_coordinator",
    srcs = [
        "my_main.cpp",
    ],
    deps = [
        "@yuanrong-datasystem//src/datasystem/coordinator:coordinator_server",
    ],
)
```

### 4. C++ 代码

```cpp
#include "datasystem/coordinator_server.h"
#include <cstdio>

int main(int argc, char **argv) {
    auto status = datasystem::CoordinatorServer::GetInstance()->InitAndRun();
    if (status.IsError()) {
        fprintf(stderr, "Coordinator InitAndRun failed: %s\n", status.ToString().c_str());
        return -1;
    }
    printf("Coordinator exited normally\n");
    return 0;
}
```

上述无参`InitAndRun()`是兼容入口，不启用Coordinator选举。需要启用选举时应使用下一节的参数化入口。

### 5. 配置文件启动方式

```cpp
#include "datasystem/coordinator_server.h"
#include "datasystem/utils/coordinator_discovery.h"

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

class UserCoordinatorDiscovery final : public datasystem::ICoordinatorDiscovery {
public:
    datasystem::Status GetCoordinators(std::vector<std::string> &addresses) override
    {
        addresses = { "127.0.0.1:31511" };
        return datasystem::Status::OK();
    }
};

int main() {
    datasystem::CoordinatorOptions options;
    options.configFilePath = "/path/to/coordinator_config.json";
    options.coordinatorDiscovery = std::make_shared<UserCoordinatorDiscovery>();
    options.expectedMemberCount = 1;

    auto status = datasystem::CoordinatorServer::GetInstance()->InitAndRun(options);
    if (status.IsError()) {
        fprintf(stderr, "Coordinator InitAndRun failed: %s\n", status.ToString().c_str());
        return -1;
    }
    printf("Coordinator exited normally\n");
    return 0;
}
```

`CoordinatorServer`只能通过`GetInstance()`获取，它是生产单例façade，并拥有一个`CoordinatorRuntime`。
参数化入口启用Coordinator选举，要求`configFilePath`非空、`coordinatorDiscovery`非空、
`expectedMemberCount > 0`，并在`coordinator_config.json`中设置`use_brpc=true`。文件访问、JSON解析和flag校验
由`FlagManager`及Runtime启动流程负责。`expectedMemberCount = N`
表示目标voting成员数，不表示首次bootstrap时Discovery必须精确返回N个候选。Discovery必须来自同一个全局
收敛的部署控制域，返回值必须是数字IPv4 `host:port`；同步`GetCoordinators`实现必须在provider自身控制的
有限时间内返回。

本地Raft data root和权威配置按以下规则处理：

- 本地metadata为`VALID`：直接从本地braft状态recover，不调用Discovery。
- 本地metadata为`ABSENT`（目录不存在或为空）：规范化、去重并排序Discovery候选，探测所有可见候选后，
  按下表决定首次bootstrap或等待。
- 本地metadata为`ABSENT`且观察到一致的、非空的权威committed configuration：配置包含本机endpoint时，
  使用该配置重建本机的选举/成员关系metadata；配置不包含本机时保持waiting，等待后续成员关系接纳。
- 本地metadata为`CORRUPT`或`UNKNOWN`：进入terminal状态，不自动bootstrap或从peer修复。运维时不要通过
  自动删除Raft data root强行启动；应先备份并恢复与稳定`coordinator_address`匹配的完整目录。
- 多个权威配置不一致（例如成员变更期间同时观察到N和N+1）时返回可重试状态，持续探测，直到全量配置一致。

首次bootstrap阈值为`Q = floor(N / 2) + 1`：

| 规范化候选数 | 行为 |
|---:|---|
| `< Q` | endpoint保持监听，bootstrap phase进入`RETRYING`，不创建Raft配置，业务gate关闭 |
| `Q ... N` | 在所有可见候选均可验证且无权威配置时，使用全部候选作为初始配置 |
| `> N` | 先探测全部候选，再按规范化地址排序选择前N个；未选节点等待权威committed configuration |
| 已观察到一致权威配置 | 本地`ABSENT`且配置包含本机时重建选举/成员关系metadata；不包含本机时等待加入 |

生产部署每个进程只运行一个Coordinator Runtime。参数化façade将非空`configFilePath`交给Runtime解析，解析
成功后Runtime恰好调用一次`GetRaftFlags()`，取得本机地址、独占Raft数据目录和选举时序快照。每个Coordinator
节点必须独占自己的`coordinator_raft_data_dir`。配置文件解析失败统一返回不包含路径和parser原文的错误。
`onStart`和`onStop`必须同时配置或同时留空。

参数化生命周期按以下顺序执行：Service完成`Init`；Service `Start`注册业务和braft services并开始监听，
状态保持`STARTING`；Runtime通过`onStart`注册endpoint；Service `StartElectionManager`启动后台Manager worker
并发布`RUNNING`；Manager随后异步执行本地recover或Discovery/peer探测、创建Node和Membership；Runtime进入
event loop。`InitAndRun(options)`提交以及Service `RUNNING`只表示后台Manager已经启动且endpoint可访问，不表示
选主完成或业务ready。只有当前braft Leader的回调打开业务gate；follower、waiting节点、bootstrap重试节点和
丢失quorum的节点均对业务RPC返回`K_NOT_READY`。`GetRaftBootstrapState`用于观察`OBSERVING`、`RETRYING`、
`STARTED`、`TERMINAL` phase及稳定的数值status code，不返回原始Status文本或数据目录。

直接调用`CoordinatorRuntime`并传入空`configFilePath`是内部同进程测试能力，不是生产façade契约。此时Runtime
跳过文件解析，使用调用方预先设置的进程flags，并为每个实例调用一次`GetRaftFlags()`。测试fixture必须在启动
任何Runtime前设置公共flags，在任一Runtime活动期间保持它们不变，并在所有Runtime停止和线程join后恢复；
endpoint、Raft数据目录和选举时序由实例快照隔离。达到`RUNNING`前的失败完成回调和Service清理后可重试；提交
后即使正常`Stop`退出也保持one-shot。进程signal handler只设置`g_exitFlag`；显式Runtime `Stop`只唤醒本实例。

当前Coordinator Raft管理选举和voting membership，不复制Coordinator业务键值或拓扑数据。业务准入由当前
本地braft Leader gate决定；Raft `STARTED`和Service `RUNNING`仅表示对应生命周期阶段，不表示业务ready。

仓库内同进程集成测试目标为：

```bash
bazel test //tests/st/common/raft:coordinator_runtime_election_test --config=test --config=release
```

该测试使用fixture预设且保持稳定的公共flags，以空配置路径在同一进程启动三个真实
`CoordinatorRuntime::InitAndRun(options)`；每个实例通过`GetRaftFlags()`隔离endpoint、Raft数据目录和选举时序，
与多进程Coordinator选举ST互补。

### 6. 编译命令

```bash
bazel build //:my_coordinator
```

产物为 `bazel-bin/my_coordinator`，全静态链接，不依赖外部 `.so`。

## 七、CMake find_package 集成

Bazel 编译的 SDK 同时提供 CMake 配置文件，支持通过 `find_package` 集成：

```cmake
cmake_minimum_required(VERSION 3.14)
project(my_project LANGUAGES CXX)

# 指向 Bazel 输出的 cpp/ 目录
set(CMAKE_PREFIX_PATH "/path/to/output/cpp")
find_package(Datasystem REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app datasystem)
```
