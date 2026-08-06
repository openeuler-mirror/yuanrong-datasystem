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
| `-S address\|thread\|undefined\|address_undefined\|off` | Sanitizer | off |
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

# 开启 UBSan
bash build.sh -b bazel -r -S undefined

# 同时开启 ASan + UBSan
bash build.sh -b bazel -r -S address_undefined

# 开启 URMA
bash build.sh -b bazel -r -M on

# 开启覆盖率
bash build.sh -b bazel -r -c on
```

### Sanitizer 模式

Bazel 模式复用 CMake 的 `-S` 参数：`address` 对应 ASan，`thread` 对应 TSan，`undefined` 对应 UBSan，
`address_undefined` 同时开启 ASan + UBSan（对应 `.bazelrc` 的 `build:asan_ubsan`）。

```bash
# ASan level0 验证
bash build.sh -b bazel -r -S address -t run -l level0

# TSan level0 验证
bash build.sh -b bazel -r -S thread -t run -l level0

# ASan + UBSan level0 验证（ASan + UBSan 共享 libasan 运行时，可安全组合；
# TSan 与 ASan/UBSAN 运行时互斥，不可叠加）
bash build.sh -b bazel -r -S address_undefined -t run -l level0
```

启用 sanitizer 后，脚本对 `-S address|thread|address_undefined` 会先用 `bazel query`
按测试标签筛选目标（而非直接跑 `bazel test //...`），测试需同时满足范围标签
（如 `level0`、`ut`）和 sanitizer 标签（`asan`、`tsan` 或通用的 `sanitizer`），
且不能带 `manual` 标签。具体匹配规则：

- `-S address` 与 `-S address_undefined` 都按 `asan` tag 筛选。组合模式叠加了
  ASan 的运行时约束（ASLR、shadow memory、leak detection），是 binding 项，因此
  复用 ASan 的测试白名单。
- `-S thread` 按 `tsan` tag 筛选。
- `-S undefined` **不做 tag 筛选**，直接跑全部测试。UBSan 没有 ASan/TSan 那种
  运行时环境约束，所有测试都可在 UBSan 下运行，无需收窄测试集。

TSan 对线程调度更敏感，建议只给已确认稳定的测试添加 `tsan` 标签。

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

# 同时开启 ASan + UBSan（共享 libasan 运行时，TSan 与二者互斥不可叠加）
bazel build //bazel:datasystem_sdk --config=asan_ubsan
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
| `--config=asan_ubsan` | ASan + UBSan（组合） | `-fsanitize=address,undefined` |
| `--config=coverage` | 覆盖率 | `-fprofile-arcs -ftest-coverage` |
| `--config=py39`~`py313` | Python 版本 | rules_python 版本选择 |

> UBSan 说明：`build:ubsan` 在 `-fsanitize=undefined` 基础上额外排除 `alignment` 子检查，并通过
> `--per_file_copt=external/.*@-fno-sanitize=undefined` 把 UBSan 对所有外部依赖（absl/grpc/rocksdb 等）
> 整体关闭，仅保留对 `datasystem::*` 自有代码的检查。原因是 GCC 14 + absl `raw_hash_set` 存在已知
> 兼容问题（`get_hash_slot_fn<Hash>() == nullptr` 在 `constexpr` 上下文中报 "is not a constant expression"）；
> 之前的 `-fno-sanitize=function` 方案在系统 GCC（不识别 `function` 子检查名）下会报
> `unrecognized argument to '-fno-sanitize=' option: 'function'`。`--per_file_copt` 方案与 GCC 版本无关，
> 同时兼容 gcc-toolset-14 与系统 GCC。CMake 构建路径（`USE_SANITIZER=undefined`）则用
> `check_cxx_compiler_flag` 检测后条件加 `-fno-sanitize=function`。
>
> 此外，`build:ubsan` 还通过 `--per_file_copt=third_party/protos/etcd/.*@-fno-sanitize=undefined`
> 关闭 etcd proto 生成的 grpc stub 代码（`rpc.grpc.pb.cc` 等）的 UBSan 插桩。原因是这些 stub 引用了
> `grpc::internal::ClientReactor` 的 typeinfo（其析构为 `= default`，无关键函数），在 stub 被插桩而 grpc
> 库本身不插桩时，链接会报 `undefined reference to 'typeinfo for grpc::internal::ClientReactor'`。

> ASan + UBSan 组合说明：`build:asan_ubsan` 用单一 `-fsanitize=address,undefined` 编译/链接标志启用
> 两个 sanitizer。GCC 与 Clang 都显式支持这种组合：ASan 运行时（`libasan`）已经把 `libubsan` 作为
> 依赖一并加载，二者共享 shadow-memory 拦截器基础设施。TSan 与 ASan/UBSAN 运行时互斥，**不能**
> 与二者组合。`build:asan_ubsan` 复用 `build:ubsan` 的全部 UBSAN 子检查调优（`-fno-sanitize=alignment`、
> `-fsanitize=float-cast-overflow`、`-fno-sanitize-recover=all`、`-O0`、外部依赖与 etcd stub 的
> `--per_file_copt` 关闭），并叠加 ASan 的 `-fno-omit-frame-pointer`、`-g3`、`--define=asan=1`（使根
> `BUILD.bazel` 的 `config_setting(name = "is_asan")` 命中，所有 ASan 专属 `select()` 分支激活）以及
> `test:asan_ubsan` 运行时环境（`ASAN_OPTIONS=detect_leaks=0:detect_odr_violation=0`、
> `LSAN_OPTIONS=detect_leaks=0`，与 `test:asan` 一致）。
>
> Flag 放置：ASan 来源的标志（`-fsanitize=address,undefined`、`-fno-omit-frame-pointer`、`-g3`）用
> `--copt` 传给 C 和 C++ 编译器（与 `build:asan` 一致），目的是让第三方 C 源文件也获得 ASan 插桩——
> 仓库中 zlib（15 个 `.c`）、securec（`glob src/*.c`）、libcurl（`glob lib/**/*.c`）、ZeroMQ
> （`external/sha1/*.c`）都通过 `cc_library` 编译 C 源码，若用 `--cxxopt` 这些 C 文件会静默丢失 ASan
> 覆盖，是相对 `--config=asan` 的回归。UBSan 专属调优标志（`-fno-sanitize=alignment`、
> `-fsanitize=float-cast-overflow`、`-fno-sanitize-recover=all`、`-O0`）保持 `--cxxopt`，与
> `build:ubsan` 一致；由于 `--per_file_copt=external/.*@-fno-sanitize=undefined` 已对全部外部依赖
> 关闭 UBSan（仅移除 `undefined` 子检查组，不影响 `address`），UBSan 调优标志对外部 C 文件是否生效
> 不影响最终行为。

> TSan 平台支持说明：
>
> - **x86_64：已验证可用。** `bazel build --config=tsan` 构建的 worker 在 `enable_urma=false` 配置下
>   可正常启动并稳定运行，无 TSAN race 报告。运行时需要：
>   - `setarch x86_64 -R`（禁用 ASLR），否则 TSAN 报 "unexpected memory mapping"（ASLR 将共享内存 mmap
>     落入 TSAN 影子内存区）。
>   - `TSAN_OPTIONS='history_size=1:force_seq_cst=0'`，减小 TSAN 影子内存开销。
>   - `shared_memory_size_mb` 建议不超过 256，`enable_urma` 设为 `false`。
>
> - **aarch64：不支持。** worker 在 TSAN 下启动时 `libtsan.so` 内部段错误（SEGV）。两个架构性根因：
>
>   1. **brpc 全局覆盖 `pthread_mutex_lock`**（`bthread/mutex.cpp`），TSAN 的拦截器被绕过，所有使用
>      brpc 内部 mutex 的代码都被 TSAN 误报为 data race。`usercode_in_pthread=true` 无效，因为
>      brpc 的内部任务（HealthCheckTask、TimerThread、bvar 采样器）仍使用 bthread::Mutex。
>   2. **bthread 使用自定义汇编 fiber 栈切换**（`bthread_make_fcontext`，类似 boost::context 的
>      fcontext_t），不走 POSIX `swapcontext`。TSAN 只能拦截 `swapcontext`，无法跟踪自定义汇编
>      fiber 切换 → 切换后 TSAN 的 per-pthread ThreadState 持有过期栈指针 → 影子内存地址落入未映射区
>      → `ThreadSanitizer: SEGV`。此问题仅在 aarch64 上出现（影子内存布局与 fiber 栈冲突）；
>      x86_64 的影子内存布局不冲突，故 fiber 切换被容忍。
>
>   这是 brpc 社区的已知问题，3 个相关 issue（
>   [#2864](https://github.com/apache/brpc/issues/2864)、
>   [#3295](https://github.com/apache/brpc/issues/3295)、
>   [#1687](https://github.com/apache/brpc/issues/1687)）均未修复。理论上的解法
>   是在 bthread 的 fiber 切换代码中集成 Clang TSAN fiber API（`__tsan_switch_to_fiber`），
>   但 brpc 社区未做此集成。
>
> - **aarch64 建议**：使用 `bazel build --config=asan`（AddressSanitizer）替代 TSAN。ASAN 不跟踪
>   fiber 栈，不会 SEGV。并发安全验证在 x86_64 TSAN 上做，或依赖代码审查 + 单元测试。

### 组合示例

```bash
# Release + URMA + 测试
bazel build //... --config=release --config=urma --config=test -j 16

# Debug + ASan + 覆盖率
bazel build //... --config=debug --config=asan --config=coverage -j 8

# Debug + ASan + UBSan + 覆盖率（组合 sanitizer；不可再加 --config=tsan）
bazel build //... --config=debug --config=asan_ubsan --config=coverage -j 8

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

上述无参`InitAndRun()`是兼容入口。默认 `coordinator_raft_initial_peers` 为空时按单节点无选主模式启动；如果通过进程 flag 配置了多个静态 peers，则会使用该列表启动 Raft 选主。需要自定义 Discovery、成员注册回调或配置文件解析流程时，应使用下一节的参数化入口。

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
