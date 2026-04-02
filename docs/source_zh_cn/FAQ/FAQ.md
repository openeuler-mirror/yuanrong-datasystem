# FAQ

## 一、多集群模式为实验性特性，可能出现以下问题
    1. 启用多集群功能之后，如果集群B上有节点正在主动缩容或扩容，集群A上的节点a在此时删除设置在集群B上的数据可能会导致a节点coredump

    2. 启用多集群功能之后，集群第一次启动和扩容的场景，node_dead_timeout_s - node_timeout_s内部分请求可能会失败，之后集群可以自动恢复正常
    
    3. 启用多集群功能之后，集群第一次启动的场景，偶现worker在etcd中的状态一直是recover，且无法恢复成ready

## 二、`docker` 容器内 `dscli` 当前工作目录如果在挂载目录下，启动会失败

数据系统启动过程中，会修改临时创建的 unix domain socket 的文件权限。但在容器中这个操作可能会被阻止。如果 `dscli start` 如果没有指定 `-d` 工作目录，会默认在当前路径下创建 `datasystem/uds` 来存放临时 socket 文件。又由于 `docker` 容器默认用户命名空间隔离，如果 `dscli` 在挂载路径下执行，您可能会遇到如下的启动错误。

```
[root@efdb9297de57 yuanrong-datasystem]# dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
[INFO] Log directory configured at: /workspace/yuanrong-datasystem/datasystem/logs
[INFO] Starting worker service with PID: 3757
[ERROR] [  FAILED  ] Worker exited with code 255
 output: WARNING: Ignore register validator for flag: validator already registered
2026-01-14T02:57:34.144815 | E | file_util.cpp:642 | efdb9297de57 | 3757:3757 | 3ebd1347-1ae2-46a4-906b-a1dcde8db79e |  |  Change mode on /workspace/yuanrong-datasystem/datasystem/uds/643b8b95 fail: 22
2026-01-14T02:57:34.144822 | E | worker_oc_server.cpp:585 | efdb9297de57 | 3757:3757 | 3ebd1347-1ae2-46a4-906b-a1dcde8db79e |  |  InitWorkerService failed. Detail: code: [IO error], msg: [Thread ID 281473735008288 IO error. Change mode on /workspace/yuanrong-datasystem/datasystem/uds/643b8b95 fail: 22
Line of code : 642
File         : file_util.cpp
traceId      : 3ebd1347-1ae2-46a4-906b-a1dcde8db79e]
2026-01-14T02:57:34.144826 | E | worker_main.cpp:270 | efdb9297de57 | 3757:3757 | 3ebd1347-1ae2-46a4-906b-a1dcde8db79e |  |  code: [IO error], msg: [Thread ID 281473735008288 IO error. Change mode on /workspace/yuanrong-datasystem/datasystem/uds/643b8b95 fail: 22
Line of code : 642
File         : file_util.cpp
traceId      : 3ebd1347-1ae2-46a4-906b-a1dcde8db79e]
2026-01-14T02:57:45.182239 | E | worker_main.cpp:308 | efdb9297de57 | 3757:3757 |  |  |  Worker runtime error:code: [IO error], msg: [Thread ID 281473735008288 IO error. Change mode on /workspace/yuanrong-datasystem/datasystem/uds/643b8b95 fail: 22
Line of code : 642
File         : file_util.cpp
traceId      : 3ebd1347-1ae2-46a4-906b-a1dcde8db79e]

[ERROR] [  FAILED  ] Start worker service @ 127.0.0.1:31501 failed: Worker service exited abnormally with code 255
[ERROR] Start failed: The worker service exited abnormally
```

解决方法：
1. 在容器的非挂载目录中执行 `dscli start`
2. 给 `dscli start` 命令增加参数 `-d /tmp/` ，指定工作路径为非挂载目录

## 三、Kubernetes 场景下旧 Worker 被 kill 后立即重启，新 Worker 退出码为 `-9`

在 Kubernetes 部署场景下，如果 Client 仍持有旧 Worker 共享内存的本地映射，即使旧 Worker 已经被 `kill -9`，对应共享内存也不会被内核立即回收。

如果此时马上重启新 Worker，在开启 UB 或开启共享内存大页的场景下，新 Worker 启动时会实际申请配置的共享内存。旧 Worker 尚未释放的共享内存与新 Worker 新申请的共享内存会同时计入 Pod 内存使用量，可能导致 Pod 触发 OOM，最终表现为新 Worker 启动失败并被 Kubernetes 以退出码 `-9` 杀死。

建议处理方式：
1. 优先确认是否存在 Client 长时间持有共享内存 buffer 的情况，并在条件允许时释放相关引用。
2. 对高可用场景，建议 Client 启用备用节点连接能力，在本地 Worker 重启失败期间切换到备用节点处理请求。
3. 在资源充足且开启大页或UB的情况下，不要仅依据 `sharedMemory < requests.memory` 进行配置，建议进一步将共享内存控制在 Pod 内存上限的 `1/2` 以下，为 Worker 重启窗口预留余量。

更多约束说明请参考：[共享内存生命周期约束](../deployment/k8s_configuration.md#共享内存生命周期约束)。
