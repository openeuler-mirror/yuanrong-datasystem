# 1 Proto File Design

## 1.1 Rule design

1) A hierarchy with a large number can depend on a hierarchy with a small number, but a hierarchy with a small number
   cannot depend on a hierarchy with a high number.
    - For example: The `worker_object.proto` can depend on `object_posix.proto`, but `object_posix.proto` cannot
      ipmort `worker_object.proto`.
2) Levels at the same level cannot depend on each other. For example, `rpc_options.proto` cannot depend on `utils.proto`.
3) Incompatible modifications should be avoided at layer 0 and layer 1.
4) The package name at layer 0 and layer 1 must be datasystem.

## 1.2 Hierarchical Design of Proto Files

### 1.2.1 Object/Stream proto files

| layer | type              | protos files                                                    |
| ----- | ----------------- |-----------------------------------------------------------------|
| 0     | utils proto files | rpc_options.proto、utils.proto、                                  |
| 1     | POSIX API         | object_posix.proto/stream_posix.proto/share_memory.proto        |
| 2     | worker RPC API    | worker_object.proto/worker_stream.proto                         |
| 3     | master RPC API    | master_object.proto/master_stream.proto/master_heartbeat.protoc |

### 1.2.2 Agent proto files

| layer | type               | protos files                             |
|-------| ------------------ | ---------------------------------------- |
| 0     | utils  proto files | rpc_options.proto、utils.proto           |
| 2     | Agent RPC          | agent_server.proto/agent_heartbeat.proto |


### 1.2.3 Test proto files

| layer | type              | protos files                  |
| ----- | ----------------- |-------------------------------|
| 0     | utils proto files | rpc_options.proto、utils.proto |
| 1     | unittest   | fd_test.proto, zmq_test.proto |

### 1.2.4 Zmq perf proto files

| layer | type              | protos files                                      |
| ----- |-------------------|---------------------------------------------------|
| 0     | utils proto files | rpc_options.proto、meta_zmq.proto/rpc_option.proto |
| 1     | zmq perf          | zmq_perf.proto                                    |