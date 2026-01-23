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
