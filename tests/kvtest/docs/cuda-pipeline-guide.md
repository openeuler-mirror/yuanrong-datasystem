# Pipeline CUDA / Pin 测试

## 范围和配置

仅适配 `mode=pipeline`，不改 Benchmark、Cache 的执行逻辑。服务发现、Client 配置、QPS RateGate、Writer/Reader 线程池、通知和指标收集沿用 kvtest。

| 容器能力和配置 | 行为 |
| --- | --- |
| 没有 Runtime 或可用 GPU，未开启搬运 | 原 CPU pipeline，不注册 CUDA 回调 |
| Runtime 和 GPU 可用，未开启搬运，默认 `pin=true` | 原 CPU pipeline，Init 前注册回调，看护共享内存 Pin/Unpin |
| `transfer_enabled=true, pin=true` | 显式 GPU 步骤使用 `DsCudaMemcpyAsync`，使用 SDK 分片逻辑 |
| `transfer_enabled=true, pin=false` | 不注册回调，GPU 步骤使用原生 `cudaMemcpyAsync` |
| `transfer_enabled=false, pin=false` | 不探测 CUDA，关闭这部分测试能力 |
| 开启搬运但没有可用 GPU / 设备号错误 | 启动失败，不静默退化成 CPU 测试 |

配置项位于 `cuda` 对象内：

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `transfer_enabled` | `false` | 允许 `d2h/h2d/mD2h/mH2d` 操作 |
| `pin` | `true` | 有 GPU 时在 Client Init 前注册四个 CUDA 回调 |
| `device_id` | `0` | 容器内可见设备序号，不是宿主机物理卡号 |
| `runtime_library` | 空 | 可选的容器内 libcudart 完整路径 |
| `client_init_wait_seconds` | `0` | KVClient 初始化成功后，等待指定秒数再启动统计、控制服务和 pipeline 请求；非负整数，仅 pipeline 模式生效 |

可用 `gen-config --cuda-client-init-wait-seconds 60` 生成一分钟等待配置。等待不计入性能统计，
SIGTERM/SIGINT 可中断等待；Pin on/off 均可设置相同等待时间。它不检查 Pin 完成状态，不能保证等待结束时全部 Pin 完成。
部署脚本将该等待时间加到 Launcher 的 `--start-timeout` 预算中，并相应延长外层命令超时。
正常启动就绪后才开始 `deploy.json` 的 duration 计时。该等待不替代原有启动失败检查。

编译不需要 CUDA 头文件、nvcc 或 CUDA 链接库。运行 GPU 测试时，容器必须有兼容驱动访问权限及 libcudart。默认按 `libcudart.so`、`.so.13`、`.so.12`、`.so.11.0` 查找，也可以设置 `runtime_library` 或 `LD_LIBRARY_PATH`。

仅透传 GPU 而没有 libcudart，不能完成 Pin 测试，启动日志会说明未启用原因。CPU 回退针对未安装 Runtime、无设备或驱动不可用等预期情况；显式库路径加载失败、缺符号、其他初始化错误会报错。

## 操作链和数据来源

开启 GPU 搬运时，Client 初始化及 `client_init_wait_seconds` 等待结束后，正式统计和压测线程启动前，
先使用 `data_sizes` 第一项大小的一个独立 key 执行 Create → D2H → Set → Get → H2D。
预热复用 CUDA 资源和正式操作实现，逐步等待搬运完成，按配置校验内容；释放 Buffer 并删除 key 后才开始压测。
预热阶段只打印 INFO 耗时，不进入性能统计；预热或清理失败则启动失败。正式请求全部正常统计。
这不是所有线程、Worker 或所有数据大小的预热，也不保证后台 Pin 已全部完成。CPU / 仅 Pin 模式不执行搬运预热。

```text
单对象卸载：createBuffer -> d2h -> setBuffer
单对象加载：getBuffer -> h2d
批量完整链：mCreate -> mD2h -> mSet -> mGet -> mH2d
```

- Create/MCreate 只申请 Buffer；D2H 完成后才执行 Set/MSet。
- Get/MGet 保留 Buffer 所有权，H2D 完成后才能释放。
- GPU Source 在启动时用 `GeneratePatternData` 初始化，字节由 `senderId + offset` 决定。业务 D2H 从 Source 写入 Datasystem；H2D 的源数据来自 Get 返回的 Buffer。
- GPU Destination 与 Source 分离，并发执行单元之间不共用 Buffer 或 Event。Source 数据准备不计入业务 D2H。
- GPU 模式会验证步骤顺序，拒绝未 Create 就 D2H、未 D2H 就发布 Buffer、未 Get 就 H2D等错误；不会自动插入步骤。
- 单对象链使用 `batch_keys_count=1`。批量链中的每个请求处理同一批 key；Reader 配置的批量容量、最大对象大小须不小于 Writer。

## 并发和生命周期

不新增 GPU 调度线程池。启动时按 `num_total_threads` 预分配资源，每个执行中的 GPU pipeline 独占一份，结束后归还。资源跟随逻辑执行单元而非 pthread TLS，避免 bthread 调用 SDK 后迁移到别的 pthread 而错用显存。

整个进程共用一个非阻塞 Stream。每份资源独立拥有 Event；一批异步复制提交后 Record Event 并等待完成。它可能同时等待别的线程插入到该 Event 之前的操作，因此统计不是纯 DMA 时间。

资源池只在领取/归还时短暂加锁，不持锁执行 SDK/CUDA 调用。适配函数在实际调用线程上设置设备，覆盖后台 Pin/Unpin 和 bthread 迁移。

即使复制部分提交后失败，也先等待 Event；Event 出错时回退到 Stream 同步。如果连 Stream 同步都失败，工具会记录错误并直接终止进程，避免普通错误返回时释放在途 Buffer。CUDA 自身卡住没有安全强制取消机制，退出仍可能等待驱动返回。

停止时先 join 原有执行线程，再释放 GPU Buffer/Event/Stream。本次修正 `ThreadPool::StopNow -> Stop` 跳过 join 的路径。libcudart 和回调保留到进程结束，不调用 `cudaDeviceReset`，避免 Datasystem 后台 Unpin 调用失效地址。

显存预算约为 `num_total_threads × batch_keys_count × max(data_sizes) × 2`，另有 Context 开销。sample/full 校验每份资源额外准备 `max(data_sizes)` 大小的 Host 校验缓冲区。资源不足会在启动时失败，不在业务请求内临时扩容。

## 校验和性能指标

复用 `verify.level`：

- `off/size` 不做 GPU 回读，但 GPU 步骤始终检查 Buffer 存在、非空、大小正确，避免越界。
- `sample/full` 在 H2D 完成后把 GPU Destination 回读到独立 Host 区，复用现有抽样/全量比较。当前 sample 仍回读整个对象，只抽样比较。该模式用于正确性验证，不用于纯性能对照。
- `verify.fail_op=true` 时内容不匹配使操作失败；false 只记录内容校验失败。CUDA API 和安全检查失败始终按操作失败处理。

继续输出 `latency_timeseries.csv` 和 `run_summary.txt`，单位仍是毫秒，沿用原有平均值、分位数、最大值、成功/失败统计。

| 指标 | 含义 |
| --- | --- |
| `createBuffer/mCreate`、`setBuffer/mSet`、`getBuffer/mGet` | 对应 SDK API 耗时，不混入搬运 |
| `d2h/h2d/mD2h/mH2d` | 单个/整批搬运从提交开始到确认完成的 Host 侧总耗时 |
| `*_enqueue` | 整批复制 API 返回时间，包含 SDK 分片处理与 CUDA Runtime 阻塞 |
| `*_event` | 设置设备及 Record Event 耗时 |
| `*_wait` | Event 等待及异常回退 Stream 同步耗时 |
| `cuda_verify` | 额外 GPU 回读与比较，不计入业务 H2D/D2H 时延 |
| `cuda_prepare` | 资源用于不同 sender 的写请求时重新准备 Source；普通 Writer 通常不触发 |

批量一次计一个样本，字节数按 key 数累计。指标、校验及同步有测试开销；不声称零开销。

`target_qps` 仍是每个 Writer 实例的 pipeline 请求速率，由既有 RateGate 分摊到写线程。B 个 key/批的理想 key 速率为 `target_qps × B`。操作太慢会达不到目标，沿用跳过过期时间槽的行为，没有新增无界 QPS 队列。

## 使用示例

```bash
cd tests/kvtest
./build.sh
# 或 ./build.sh -b cmake -s /path/to/sdk

# 先修改 coordinator 地址、环境变量及设备序号
./output/kvtest config/config.cuda.json.example
```

示例默认完整批量链、8 key/批、100 pipeline QPS、8 写线程。先用 `verify.level=full, fail_op=true` 验证，再用 `size` 测性能。Pin 对照只改 `cuda.pin`，每次重新启动进程；不支持同一进程切换注册状态。

K8s 保留原 deploy.json 的节点与服务发现配置，在配置模板合入以下片段：

```json
{
  "cuda": {"transfer_enabled": true, "pin": true, "device_id": 0},
  "pipeline": ["mCreate", "mD2h", "mSet"],
  "notify_pipeline": ["mGet", "mH2d"],
  "batch_keys_count": 8,
  "notify_count": 10
}
```

Writer 写成功后通过原有通知机制发送 key、size、sender，Reader 执行 Get+H2D。“一写十读”需要至少 10 个不同 Reader peer；不会自动在不足的 peers 中重复读取。不要在通知读完前立即删除对象；TTL 须覆盖通知排队和读取时间。

`deploy_client.py gen-config` 新增以下参数，可与原 `--pipeline`、`--notify-pipeline`、QPS、线程数参数组合：

```text
--cuda-transfer
--cuda-pin true|false
--cuda-device-id 0
--cuda-runtime-library /path/in/container/libcudart.so
```

deploy.json 的单个 node 支持 `cuda` 字段覆盖模板，便于混合 GPU/非 GPU 容器。无 GPU node 须关闭搬运并使用 CPU pipeline/notify_pipeline，不能继续带 H2D/D2H 步骤。

## 验证

无 GPU 的 Linux 构建机可运行 fake CUDA Runtime 契约测试：

```bash
cmake -S tests/kvtest/tests/cxx -B /tmp/kvtest-cuda-tests
cmake --build /tmp/kvtest-cuda-tests -j8
ctest --test-dir /tmp/kvtest-cuda-tests --output-on-failure
```

覆盖 CPU 回退、显式 GPU 模式失败、Pin-only/on/off、单个/批量、共享 Stream 并发、枚举转换、第二片失败后的同步、Event 回退、容量检查、数据损坏与 off/sample/full 校验。完整入口仍为 `bash tests/kvtest/tests/run_all_tests.sh`。

Fake Runtime 不代表真实 GPU 验证。仍需在 Linux GPU 容器验证两种线程后端、Writer/Reader、8 Client/8 Worker 上下线、退出及 Pin on/off 吞吐长尾。驱动调用没有“最多 1–2 个分片”的时延硬上界。
