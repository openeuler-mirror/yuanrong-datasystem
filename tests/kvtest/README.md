# kvtest

独立的 datasystem KVClient 性能测试工具，支持 Writer/Reader 角色分离、Cache 模式、Benchmark Set/Get 模式、多节点部署、K8s 自动发现。同时提供 Worker/Coordinator 独立部署测试程序（coordinator_test / worker_test）和外部服务发现模拟（mock_jf_server.py），用于验证独立集成部署与服务发现对接流程。

## 编译与运行

```bash
cd tests/kvtest

# 编译（默认 Bazel：in-tree datasystem，自包含二进制，无需预装 SDK；
#       自动启用 KVTEST_USE_BRPC ---- brpc 控制面 + bthread pipeline/notify 池）
./build.sh

# 或指定 SDK 路径
./build.sh -s /path/to/sdk

# 用 Bazel 构建并将 URMA 支持编入自包含 kvtest（与根目录 build.sh -M on 对齐）
./build.sh -b bazel -M on

# Debug 构建
./build.sh -d                # bazel: --config=debug
./build.sh -b bazel -d

# 用 CMake + brpc 后端构建（默认 KVTEST_USE_BRPC=ON，行为与 bazel 一致；
#       复用主仓 cmake/external_libs/*.cmake 自动下载编译 brpc/protobuf/gflags/absl，
#       缓存到 $DS_OPENSOURCE_DIR，首次 5-10 分钟，之后秒级；仍链预装 libdatasystem.so）
./build.sh -b cmake
./build.sh -b cmake -s /path/to/sdk

# CMake + httplib 后端（fallback：无第三方依赖，无网络下载，纯 std::thread）
./build.sh -b cmake --use-httplib
```

Bazel 模式不会在运行时加载外部 `libdatasystem.so`。需要 UB/URMA 数据面时，必须在构建 kvtest 本身时传入
`-M on`；该选项会映射为 Bazel 的 `--config=urma`。默认值为 `off`。CMake 模式的能力由 `-s` 指定的
预构建 SDK 决定，因此不接受 `-M on`。

> cmake+brpc 模式下第三方件源码默认从 gitee/github 下载。如需离线/加速，可设
> `export DS_LOCAL_LIBS_DIR=/path/to/opensource_third_party` 指向主仓

## 运行

### Jemalloc profiling（Bazel）

```bash
# 编译支持，默认 -x off；CMake 不接受 -x on
./build.sh -b bazel -x on
./output/kvtest --version
# jemalloc_prof_supported=true

# 部署时独立开启采样及周期性 dump
python3 deploy_client.py deploy config/deploy.json config/config.json \
  --jemalloc_prof_conf 'lg_prof_sample:19,lg_prof_interval:30,prof_final:true'
```

Bazel 构建的 `kvtest`、`coordinator_test`、`worker_test` 默认链接普通版共享 jemalloc；
`-x on` 复用 `--config=jeprof`，同时将三个工具切换到 profiling 版。
两种构建都将 `libjemalloc.so.2` 打包到 `output/lib/`。部署时应保留二进制旁的 `lib/` 目录；
客户端安装将该库单独上传到受管的 `allocator_lib/`，每次在 host lock 内重建该目录。
即使指定 `remote_sdk_dir`，也只将 `allocator_lib/` 置于指定 SDK 之前，不提升历史 `lib/`
中其他 SDK/URMA 库的优先级。该受管目录只允许存放配套 jemalloc，不应放入用户文件。
普通启动和 `--version` 都打印
`jemalloc_prof_supported=true/false`，两种 Bazel 构建都通过实际加载库的 `mallctl("config.prof")` 查询能力。
编译支持并不自动开启采样。

独立启动 `coordinator_test` 或 `worker_test` 时，在进程启动前设置 `MALLOC_CONF`：

```bash
cd tests/kvtest
bash build.sh -b bazel -M off -x on
mkdir -p "$PWD/output/logs/jemalloc"
MALLOC_CONF="prof:true,lg_prof_sample:19,prof_final:true,prof_prefix:$PWD/output/logs/jemalloc/worker_test" \
  ./output/worker_test --version
```

上述命令用正常退出的 `--version` 验证能力及最终 dump；业务运行时替换为实际启动参数。
`coordinator_test` 用法相同，设置独立的 `prof_prefix`。运行时目录应使用实际日志目录下的
`jemalloc/`，并提前创建。通过部署脚本启动时，使用下面的统一参数自动准备目录与环境变量：

```bash
python3 deploy_worker.py start -p worker -c worker.config -S --jf jf:31500 \
  --jemalloc_prof_conf 'lg_prof_sample:19,lg_prof_interval:30,prof_final:true'
python3 deploy_coordinator.py start -p coordinator -c coordinator.config -S --jf jf:31500 \
  --jemalloc_prof_conf 'lg_prof_sample:19,lg_prof_interval:30,prof_final:true'
```

两者的 `deploy` 子命令也支持该参数，安装阶段应提供 `-x on` 编译的二进制及配套动态库。
日志目录取应用 `--set` 后配置中的 `log_dir`（支持字符串及 `{"value": ...}` 格式），默认前缀为：

| 工具 | 默认 `prof_prefix` |
|---|---|
| `worker_test` | `<log_dir>/jemalloc/worker_test_<port>` |
| `coordinator_test` | `<log_dir>/jemalloc/coordinator_test_<port>` |

例如 `log_dir=/path/to/log`、Worker 端口为 31501，则前缀为
`/path/to/log/jemalloc/worker_test_31501`。未配置 `log_dir` 时必须显式指定 `prof_prefix`。
相对路径在目标进程工作目录下解析；在远端检查能力、创建并检查目录后，才通过 launcher 或
nohup 启动。缺少 profiling 能力或目录不可访问时启动失败，不会静默忽略配置。

Worker 的旧参数 `--jemalloc-prof-options` 保留为别名，支持 standalone 与 dscli 两种模式。
dscli Worker 仍由 dscli 自身准备 profiling 环境。Coordinator 的该参数仅支持 `-S`：
底层 dscli 目前只支持 Worker profiling，因此 Coordinator 非 standalone 模式会明确拒绝该参数。

分步部署时，`install` 上传二进制和动态库，`start --jemalloc_prof_conf ...` 在启动前设置采样配置；
`deploy` 则依次执行安装和启动，任一阶段失败都会返回非零状态。

`--jemalloc_prof_conf` 按 jemalloc 的 `MALLOC_CONF` 格式接收配置，拒绝空配置、格式错误、
重复键和 `prof:false`、`prof_active:false`、`prof_thread_active_init:false`，避免关闭采样。
缺省补充 `prof:true`，已知 profiling 布尔项只接受 true/false；
`lg_prof_sample` 接受 0–63，`lg_prof_interval` 接受 -1–63（-1 表示不做周期 dump）。
其余采样和 dump 策略由用户指定。
该参数覆盖实例 `env` 中的 `MALLOC_CONF`。未指定参数时保持原有环境变量行为。

默认 profile 目录为 **kvtest 日志目录 `output_dir` 下的 `jemalloc/`**：

| 配置 | 路径 |
|---|---|
| `output_dir` | `/path/to/log` |
| profile 目录 | `/path/to/log/jemalloc/` |
| 默认 `prof_prefix`，实例 ID 为 3 | `/path/to/log/jemalloc/kvtest_3` |

未配置 `output_dir` 且传入 profiling 参数时，部署端生成 `metrics_<instance_id>_<时间戳>`，
写入上传的实例配置，保证日志与 profile 使用同一目录。相对路径以远端工作目录为基准，
不是部署机的当前目录。显式 `prof_prefix` 优先，且必须包含目录。
启动前会检查远端二进制支持情况，并在远端主机或目标容器内创建和检查 profile 父目录。
能力不足或目录不可写时，该实例部署失败，部署命令返回非零。
显式传入 profiling 配置时，若发现工具进程已运行，命令会失败并提示先停止再启动，不会把
“已有进程”报告为配置生效。服务工具单 Pod 的预检失败会计入该 Pod 的失败结果，
其余 Pod 继续完成，最后汇总成功数并返回非零。

profile 文件名由 jemalloc 在前缀后追加 PID、序号和 dump 类型。
仅打开 `prof:true` 不保证立即生成文件；上例配置周期性 dump 和正常退出 dump，
强制终止进程不能依赖退出 dump。profiling 会影响压测吞吐、时延和磁盘占用，应按需开启，
并由使用者清理采集文件；关闭采样需移除部署参数及自行设置的 `MALLOC_CONF`。
这些 profile 主要覆盖普通堆分配，不代表全部 RSS，也不覆盖独立的 `datasystem_*` 分配器。

回归验证（仓库根目录）：

```bash
bazel test //tests/kvtest:jemalloc_prof_test --config=jeprof --test_output=errors
bazel test //tests/kvtest:jemalloc_prof_test --define=enable_jemalloc_prof=false --test_output=errors
```

测试会复制二进制到独立目录，验证运行库加载、能力输出及 malloc/free 的实际提供者。
使用 glibc 的 `LD_DEBUG=bindings` 和立即绑定检查二进制的符号解析结果必须指向随包 jemalloc，
而不是仅检查 ELF NEEDED。在支持版生成实际 heap 文件。三个工具各有对应的测试目标；
Python 反例测试还构造仅提供 mallctl、不接管 malloc/free 的 DSO，确认断言会拒绝该错误链接。

### 分配器基线版本与回退

本 PR 是压测工具的分配器基线切换，不是与历史结果可直接混用的诊断开关。

| 基线标识 | 范围 | 比较规则 |
|---|---|---|
| `kvtest-bazel-allocator-v0` | 本 PR 之前的已归档二进制、库和构建提交；参考基点 `a0b1a39ec` | 保持原有工具及依赖；实际分配器以归档的符号绑定记录为准 |
| `kvtest-bazel-jemalloc-v1` | 本 PR 起，三个 Bazel 工具默认共享 jemalloc，`-x off` | 建立新的吞吐、时延、CPU、RSS 基线，不直接与 v0 合并 |
| `kvtest-bazel-jemalloc-prof-v1` | 同一源码的 `-x on` 及显式采样策略 | 单独标记采样配置，不用作无采样性能基线 |

每份报告应记录基线标识、三个工具的完整构建 SHA、构建后端、`--version` 输出、二进制/运行库
校验值、malloc/free 提供者、`MALLOC_CONF`、服务端版本和 CPU/NUMA 绑定。
不能仅凭工具 VERSION 相同就认定结果可比。CMake/SDK 构建单独记录，不归入上述 Bazel 基线。
`-x off` 的普通 jemalloc 不具备 profiling 能力、没有 profiling 采样开销，但仍会改变
分配延迟、线程缓存、碎片和 RSS；它不是回退至 libc 的开关。

尚未获得代表性性能对照数据，也没有据此承诺性能回归接受阈值。需要跨 v0/v1 比较时，
固定服务端版本、CPU/NUMA 和负载参数，对短对象高 QPS/较大对象、单实例/多实例各运行多轮，
记录吞吐、p50/p99、CPU、RSS/碎片及方差，由压测负责人确认接受阈值后再判断回归。
若新基线不能接受，停止新工具，在独立工作目录恢复已归档的 v0 二进制及完整依赖并重新启动，
同时取消 profiling 参数和相关环境变量；不要只替换同名 jemalloc DSO 或混用新旧工具包。

### 常规运行

```bash
# 启动依赖
etcd &
mkdir -p /tmp/ds_worker && cd /tmp/ds_worker
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379

# 运行
cd tests/kvtest/output
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/my_config.json

# 查看统计 / 停止（HTTP 端点路径不变：/stats、/stop、/summary、/notify）
# bazel 构建：brpc 经 restful 映射保留旧路径，响应 /stats 为 {"stats_json":"<metrics json>"}
curl -s http://127.0.0.1:9000/stats | python3 -m json.tool
curl -X POST http://127.0.0.1:9000/stop
# cmake 构建仍走 httplib：/stats 直接返回 metrics JSON
```

## Benchmark Set/Get 模式

用于精确测量 Set/Get 吞吐和延迟，支持 8 种测试模式：

```bash
# 本地 Set 吞吐基线（8线程，5轮）
cat > config/bench.json << 'EOF'
{
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "test_mode": "set_local",
  "worker_memory_mb": 4096,
  "num_threads": 8,
  "total_rounds": 5,
  "data_sizes": ["8MB"],
  "set_api": "string_view",
  "cleanup_method": "del"
}
EOF

LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/bench.json
```

**测试模式：** `set_local` / `set_remote` / `get_local` / `get_cross_node` / `get_remote_direct` / `get_remote_cross` / `mixed_local` / `mixed_cross_node`

**Set API：** `string_view`（直接写入）/ `create_buffer`（SHM Buffer + latch）/ `create_buffer_raw`（SHM Buffer，无锁 memcpy）

**输出：** `benchmark_phases.csv`（per-round per-phase 延迟和 QPS）

## 测试

```bash
# C++ 单元测试 (68) + Python 单元测试 (53)
cd tests/kvtest
bash tests/run_all_tests.sh

# 集成测试（需要真实集群环境）
bash tests/test_cpu_affinity.sh   # CPU 绑核验证
bash tests/test_deploy.sh          # 多节点部署验证
bash tests/test_e2e.sh             # 端到端验收测试

# Worker/Coordinator 独立部署 + 服务发现模拟测试
bash tests/test_standalone_mode.sh
# 覆盖场景：Coordinator 注册/心跳/反注册、Worker 从服务发现获取 Coordinator、
#           Coordinator 崩溃 + TTL 过期、Coordinator 重启恢复
```

## 文档

| 文档 | 内容 |
|------|------|
| [docs/user-guide.md](docs/user-guide.md) | 编译部署、配置参数、远程部署、指标采集、故障排查 |
| [docs/pipeline-guide.md](docs/pipeline-guide.md) | Pipeline 模式：Writer/Reader 角色、QPS 控制、多实例部署 |
| [docs/cache-guide.md](docs/cache-guide.md) | Cache 模式：cacheGetOrCreate、命中率控制、Key Pool 管理 |
| [docs/benchmark-guide.md](docs/benchmark-guide.md) | Benchmark 模式：8 种 Set/Get/Mixed 测试模式、per-phase 计时 |
| [docs/design.md](docs/design.md) | 架构设计：模块设计、线程模型、指标系统、QPS 控制机制 |
| [docs/jf-integration-design.md](docs/jf-integration-design.md) | 独立部署 + 服务发现模拟：JfClient、mock server、deploy 脚本 standalone 模式、E2E 测试 |


### 按需收集 Worker / Client 日志

继续使用 `deploy_worker.py collect` 和 `deploy_client.py collect`，无需新增命令行脚本。以下命令在 `tests/kvtest` 目录执行：

```bash
# 使用现有 -p 选择 Pod，仅收 worker.log 中包含 URMA_PERF 的行
python3 deploy_worker.py collect -p worker-1 -p worker-10 --file-pattern 'worker.log' --keyword URMA_PERF -o collected-perf

# 使用 -p 选择 Client Pod，只收 *access*.log
python3 deploy_client.py collect deploy.json -p client-1 -p client-10 --file-pattern '*access*.log' -o collected-access

# 也可以按 deploy.json 中的 instance_id 选择 Client
python3 deploy_client.py collect deploy.json --instance-ids 1 10 --file-pattern '*access*.log' --keyword URMA_PERF -o collected-client-perf

# 全部目标，只收未压缩日志（含未压缩轮转文件）
python3 deploy_worker.py collect -p worker- --uncompressed-only --max-workers 16 -o collected-recent
python3 deploy_client.py collect deploy.json --uncompressed-only --max-workers 16 -o collected-client-recent

# 全量收集并启用 Pod/IP 目录名
python3 deploy_worker.py collect -p worker- --pod-info -o collected-with-addresses
python3 deploy_client.py collect deploy.json --pod-info -o collected-client-with-addresses

# 不传新增选项，保留原有全量收集流程
python3 deploy_worker.py collect -p worker- -o collected-all
python3 deploy_client.py collect deploy.json -o collected-client-all
```

Worker 继续使用现有 `-p/--prefix`，可重复传多个前缀或完整 Pod 名，保持原有前缀匹配语义（`pod-1` 也可能匹配 `pod-10`），不新增 `--pods`。Client 同样使用可重复的 `-p/--prefix`，按任一前缀匹配 deploy.json 中的 Pod，不重复收集或改变实例编号；不传 `-p` 时仍收集配置中的全部 Client。同时指定前缀和实例 ID 时取交集。原有 count/offset 仍用于分批，建议不要与前缀选择混用。前缀无匹配时告警，全部前缀无匹配时返回失败，不退回全量收集。

`--file-pattern` 匹配日志文件名；含 `/` 时匹配相对于对应日志根目录的路径。多个 pattern 或 keyword 可以重复传入，各自按 OR 匹配，两类条件之间取交集。keyword 为区分大小写的 UTF-8 字面子串，不是正则表达式；仅输出匹配行，无上下文行。文件名通配符须加引号，防止本地 shell 提前展开。筛选适用于日志根目录、Worker stdout/procmon、Client output/SDK；不收不匹配的附带文件。

关键字筛选在远端执行，结果文件保留相对路径并追加 `.matched` 后缀，内容为未压缩文本；无命中时不产生文件。支持读取 gzip/bzip2/xz 日志后筛选，其他压缩格式在关键字模式下明确报错。`--uncompressed-only` 按压缩后缀排除归档，不按 mtime 判断，也不会排除仍未压缩的旧轮转文件。它与关键字或文件名筛选可组合；传输本身仍使用 gzip，以减少网络流量。

显式传入 `--pod-info` 后，Pod 收集目录为 `<Pod>__podip-<PodIP>__hostip-<HostIP>`；Client 再追加 `__client-<instance_id>`。仅在启用该参数时使用包含地址的目录；地址读取自当前 Kubernetes 状态，缺失 HostIP 时明确标为 unknown。SSH/localhost Client 保留原来的 host_instance_id 目录。筛选模式使用 logs/procmon/stdout 或 output/sdk 子目录，避免不同来源同名文件覆盖；不改变源文件。不传新增参数时完整保留原有收集范围、目录命名、目录内部布局、并发默认值和 Client summary 流程。

并发默认行为不变；大量节点建议显式传 `--max-workers 16`。筛选模式需要目标节点有 Python 3，按行处理关键字，传输结果落本地临时文件再解包，避免整批日志驻留内存；匹配行临时文件使用远端临时目录，需有足够空间。筛选 Client 日志不触发 `/summary`。单个目标失败会报告失败并继续其他目标，筛选模式最终返回非零；不会改用全量下载掩盖筛选失败。请每次使用新的 `-o` 目录，避免上次收集结果混入本次分析。线上文件仍可能轮转，不保证跨节点同一时刻的日志快照。


每次 collect 另外归档复现配置，不受 `--file-pattern`、`--keyword` 或 `--uncompressed-only` 影响：

- Client：传入 `aaa/deploy.json aaa/config.json` 时，只将这两个文件复制为 `<output>/aaa/deploy.json`、`<output>/aaa/config.json`，不复制 aaa 下其他文件或子目录。重复传入同一文件只复制一次；不同输入映射到同一归档文件名时明确报错。
- Worker：将每个选中 Pod 的 `--remote-config` 内容保存为 `<output>/<Pod目录>/worker_config.json`，保留实际部署参数；开启 `--pod-info` 时放入包含 IP 的 Pod 目录。读取失败会报告配置收集失败并返回非零。

这是额外的配置归档；原有日志收集范围、并发默认值和 Client summary 流程继续保留。


`--file-pattern` 示例适用于 Worker 和 Client：`'*access*.log'`、`'*INFO*.log'`、`'*operation*.log'`、`'*metrics*.log'`、`'*request*.log'`、`'*resource*.log'`。例如 `'*resource*.log'` 同时覆盖 `kv_resource.log`、`resource.log`、`resource_monitor.log`。筛选收集明确排除 `env` 和 `procmon.py`，即使传入 `--file-pattern '*'` 也不收集；原有全量日志匹配规则不变。


### collect 的传输压缩与本地解包

Worker 和 Client 均支持 `--compress/--no-compress`、`--extract/--no-extract`。不传这些参数时保持原有流程，包括全量收集的传输与回退行为；筛选收集默认 gzip 传输并自动解包。显式使用任一参数时，未指定的另一项默认为启用。

```bash
# 压缩传输，收回后保留归档，不解包
python3 deploy_worker.py collect -p worker- --compress --no-extract -o worker-archives
python3 deploy_client.py collect aaa/deploy.json aaa/config.json -p client- --compress --no-extract -o client-archives

# 压缩传输，收回后解包（不另外保留传输归档）
python3 deploy_worker.py collect -p worker- --compress --extract -o worker-logs

# 不使用 gzip 压缩，收回后解包
python3 deploy_client.py collect aaa/deploy.json aaa/config.json --no-compress -o client-logs

# 可与已有日志筛选组合
python3 deploy_worker.py collect -p worker- --file-pattern '*access*.log' --keyword URMA_PERF --no-compress --no-extract -o access-archives
```

| 选项组合 | 本地结果 |
| --- | --- |
| `--compress --extract` | 解包后的日志文件 |
| `--compress --no-extract` | `.tar.gz` 归档 |
| `--no-compress --extract` | 解包后的日志文件，传输不做 gzip 压缩 |
| `--no-compress --no-extract` | 未压缩的 `.tar` 归档 |

不解包时，Worker 每个 Pod 目录保存 `logs.tar.gz`（或 `logs.tar`）；Client 全量收集分别保存 `output.tar.gz`、`sdk.tar.gz`，筛选收集将两个来源放入一个 `logs.tar.gz`，未压缩时后缀均为 `.tar`。Worker 全量归档保留远端文件路径（无开头 `/`），自动解包仍按原有规则取文件名；Client 全量归档保留对应 output/SDK 根目录内的相对路径，筛选归档保留来源子目录。

这些参数控制传输归档，不改变源日志，也不改变 `--uncompressed-only` 对源文件的筛选。原有 `.log.gz` 文件不会因 `--extract` 自动展开；该参数只解包收集生成的外层归档。Client 的 deploy/config 文件及 Worker 的 worker_config.json 仍按现有规则单独归档，不放进日志压缩包。

显式压缩/解包模式下，全量收集需要目标环境提供 tar（gzip 模式还需 tar 支持 gzip），筛选收集使用目标 Python 3。下载失败不回退到其他传输模式，也不会覆盖已有的完整归档；每个归档通过临时文件完成后原子替换。建议每次使用新的 `-o`，避免此前解包的日志或其他模式的归档混入当前结果。大集群仍可用 `--max-workers` 控制并发，默认并发规则不变。
