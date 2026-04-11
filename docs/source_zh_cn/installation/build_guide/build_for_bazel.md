# Bazel编译源码集成SDK

## 适用范围

本文档面向需要在 Bazel 构建体系中集成 openYuanrong DataSystem C++ SDK 的开发人员、CI 维护人员及第三方系统厂商。

## 发布包目录结构说明

在源码编译安装完 openYuanrong datasystem 后，C++ 相关的发布包如下：

```text
output/cpp
├── BUILD.bazel   # datasystem Bazel 模块定义文件（*.bzl）
├── include       # 头文件（datasystem.h）
├── lib           # 动态链接库（libdatasystem.so 及其依赖）
└── WORKSPACE     # Bazel WORKSPACE 文件
```

## 集成示例

### 1. 新建工程并拷贝发布包

新建一个 Demo 工程，并将 `output/cpp` 下的文件拷贝到 `thirdparty/datasystem` 下，最终目录示例：

```text
demo
├── BUILD
├── main.cpp
├── thirdparty
│   └── datasystem
│       ├── BUILD.bazel
│       ├── include
│       ├── lib
|       └── WORKSPACE
└── WORKSPACE
```

### 2. 声明 datasystem 为本地仓库

在 `WORKSPACE` 文件中将datasystem注册本地仓库：

```bazel
local_repository(
    name = "datasystem",
    path = "thirdparty/datasystem",
)
```

### 3. 添加 datasystem 目标依赖

在需要使用 DataSystem 的 `BUILD` 文件内添加依赖：

```bazel
cc_binary(
    name = "demo",
    srcs = ["main.cpp"],
    deps = ["@datasystem//:datasystem"],
)
```

### 4. 示例 Demo 程序

以下代码片段 `main.cpp` 演示最小可用调用：

```cpp
#include <datasystem/datasystem.h>

using namespace datasystem;

int main() {
    ConnectOptions opts{ .host = "127.0.0.1", .port = 31501 };
    auto client = std::make_unique<KVClient>(opts);
    Status status = client->Init();
    return 0;
}
```

编译验证

```bash
cd demo
bazel build //:demo
bazel run   //:demo
```

无报错且进程正常退出，即视为集成成功。

## 版本升级策略

当需要升级数据系统 SDK 版本时，升级步骤如下：
1. 备份旧版本：`mv thirdparty/datasystem thirdparty/datasystem.bak.$(date +%F)`
2. 使用新版本发布包覆盖 `thirdparty/datasystem`；
3. 重新执行 `bazel build //...` 进行全量编译验证；
4. 通过回归测试后，删除备份目录。

> 升级期间无需修改任何 `BUILD` 或 `WORKSPACE` 文件。
