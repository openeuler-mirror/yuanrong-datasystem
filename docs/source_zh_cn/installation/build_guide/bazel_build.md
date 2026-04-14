# 基于Bazel自定义编译

## 一，环境依赖

### OpenEuler OS

```bash
yum install -y autoconf automake libtool m4
```

## 二，常用构建命令

**1. sdk 包构建：**

```bash
bazel build //bazel:datasystem_sdk
#输出sdk目录：bazel-bin/bazel/datasystem_sdk/cpp
#主要内容包括：BUILD.bazel、头文件（统一在 cpp/include/datasystem 目录下）、
#以及动态库 cpp/lib/libdatasystem.so 和 cpp/lib/libds_client_py.so
```

性能优化的Release版本，需要带上 --config=release，下同

```bash
bazel build //bazel:datasystem_sdk --config=release
```

Debug版本，需要带上 --config=debug

```bash
bazel build //bazel:datasystem_sdk --config=debug
```

**2. wheel包构建：**

```bash
bazel build //bazel:datasystem_wheel --config=release
# 输出wheel包：bazel-bin/bazel/openyuanrong_datasystem-0.7.0-cp311-cp311-manylinux_2_34_x86_64.whl
```

**3. 只编译 libdatasystem.so:**

```bash
bazel build //:datasystem --config=release
```

**4. 开启URMA选项编译:**
添加选项 --config=urma

```bash
bazel build --config=urma --config=release //bazel:datasystem_sdk
bazel build --config=urma --config=release //bazel:datasystem_wheel
```

**5. 编译单个target:**
新增源码文件时需要在对应目录的BUILD.bazel中补充依赖关系。
如果需要编译单个target，可以指定目录+target名，例如：

```bash
bazel build //src/datasystem/common/rdma:fast_transport_manager_wrapper
```

## 二，构建指定版本

**1. 指定python版本编译：**

```bash
bazel build //bazel:datasystem_wheel --config=py39 --config=release
#可选版本包括：3.9, 3.10, 3.11, 3.12, 3.13
```

**2. 指定glibc版本编译：**
glibc未指定时默认自动读取并使用系统glibc版本；若指定可选版本用以下指令：

```bash
bazel build //bazel:datasystem_wheel --define glibc_version=2.35 --config=release
#可选版本包括：2.34, 2.35, 2.36, 2.37, 2.38
```

## 三，做为第三方组件源码集成
**1. bazelrc 默认选项：**
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
使用 --config=urma 支持urma

**2. WORKSPACE 配置**

```python
load("@yuanrong-datasystem//bazel:ds_deps.bzl", "ds_deps", "setup_grpc")

ds_deps()    #加载datasystem默认依赖

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

# 关闭grpc对部分语言支持
switched_rules_by_language(
    name = "com_google_googleapis_imports",
    cc = True,
    go = False,
    grpc = True,
    java = False,
    python = False,
)

# 加载grpc依赖
setup_grpc()


# 配置python构建环境
load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")

python_configure(name = "local_config_python")

load("@rules_python//python:repositories.bzl", "py_repositories")

py_repositories()
```
**3. BUILD 添加client依赖关系**
依赖关系添加：@yuanrong-datasystem//src/datasystem/client:datasystem
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
**4. C++ 代码**
添加 数据系统头文件，使用接口
```cpp
#include "datasystem/datasystem.h"

using datasystem::KVClient;

```
