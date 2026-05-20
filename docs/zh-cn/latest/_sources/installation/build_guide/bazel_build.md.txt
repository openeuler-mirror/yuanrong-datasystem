# 基于 Bazel 自定义编译

DataSystem 支持通过 `bash build.sh` 一键 Bazel 编译，也支持直接使用 `bazel build` 命令进行精细控制。

## 一、环境依赖

### OpenEuler OS

```bash
yum install -y autoconf automake libtool m4
```

### 安装 Bazel

参考 [Bazel 官方文档](https://bazel.build/install) 安装 Bazel 7.4 及以上版本。

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

# 开启 URMA
bash build.sh -b bazel -r -M on

# 开启覆盖率
bash build.sh -b bazel -r -c on
```

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

## 四、源码编译安装

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

## 五、CMake find_package 集成

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
