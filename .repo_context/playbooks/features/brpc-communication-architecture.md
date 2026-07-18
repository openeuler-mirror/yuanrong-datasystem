# brpc Communication Architecture Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - brpc/ZMQ transport selection, RPC service registration, generated stubs, async RPC, streaming RPC, attachments, TraceID propagation, and RPC data-plane bypasses
- Owning module or area:
  - repository-wide RPC and distributed communication paths
- Primary source paths:
  - `src/datasystem/common/rpc`
  - `src/datasystem/client`
  - `src/datasystem/worker`
  - `src/datasystem/master`
  - `src/datasystem/protos`
- Last verified against source:
  - `2026-07-01`

## Architecture Summary

brpc is a transport backend for control-plane and metadata RPCs. It is not the primary large-object data plane.

The repository keeps brpc and ZMQ as two interchangeable backends selected by `FLAGS_use_brpc` / `DATASYSTEM_USE_BRPC` (default `false`, i.e. ZMQ). They share the same configured worker/master port because `kBrpcPortOffset = 0`; only one backend should bind the endpoint in a process.

The worker process may host both worker-side and master-side services. When worker and master addresses resolve to the same local process, worker-master and master-worker APIs can bypass RPC and call local service objects directly.

Large data usually bypasses brpc:

| Direction | brpc role | Data path |
| --- | --- | --- |
| client -> worker | control and metadata RPCs | shared memory fd transfer, mmap, `ShmCircularQueue`, or URMA where enabled |
| worker -> worker | remote-get/migrate metadata and fallback payload framing | URMA/RDMA first where available, brpc attachment fallback |
| worker <-> master | metadata/control | protobuf metadata only in normal production paths |

## Core Invariants

- Do not run brpc and ZMQ listeners on the same endpoint at the same time. `RpcServer::BuildAndStart()` skips ZMQ binding in brpc mode, then callers register brpc adapters and call `StartBrpcServer()` explicitly.
- Register all brpc service adapters before `StartBrpcServer()`. The two-phase lifecycle exists because `BuildAndStart()` creates the server skeleton before service adapters are available.
- Preserve generated brpc/ZMQ stub interface parity. Business code commonly dispatches with `brpcSession_ ? brpc : zmq`, so proto option changes must keep both generated paths compatible.
- Preserve channel/stub lifetime coupling. `RpcStubCacheMgr::BrpcCreatorTemplate()` returns an aliasing `shared_ptr` whose control block owns both channel and stub; do not replace it with a raw stub-only owner.
- Treat client->worker stubs differently from worker->worker/master stubs. Client OC uses an atomically published `BrpcSession` bundle for hot replacement; worker/master paths use `RpcStubCacheMgr` LRU plus stale-socket eviction.
- Keep TraceID at the front of the brpc request attachment. The server adapter strips the `TRCID:V1` prefix before payload parsing, so payload framing depends on this ordering.
- Do not rely on brpc attachment as a true zero-copy large-data channel. The attachment framing path can avoid some application-level copies through `RpcMessage::ZeroCopyBuffer`, but brpc attachment serialization still differs from shm/RDMA bypass semantics.
- Keep production streaming assumptions narrow. The only production streaming RPC verified here is `MasterWorkerSCService.QueryMetadata`; test protos contain additional stream examples.

## BRPC Latency Metric Semantics

`BrpcPerfTrace` uses `steady_clock` timestamps in nanoseconds and exports histogram samples in truncated microseconds.
The following semantics apply to the generated non-streaming unary RPC paths that return a complete server trace
trailer. Let `C*` denote client timestamps and `S*` server timestamps.

| Metric | Formula | Meaning and boundary | Accuracy / interpretation |
| --- | --- | --- | --- |
| `brpc_rpc_e2e_latency` | `CEnd - CStart` | Client-observed elapsed time for the whole synchronous RPC wrapper. | Accurate end-to-end client latency; includes client preparation and response handling, so it is not a pure transport metric. |
| `brpc_client_req_framework_latency` | `CSend - CStart` | Local work before entering `channel_->CallMethod()`. | Accurate for its instrumented wrapper boundary. Depending on the call path, it can include Datasystem request attachment preparation. |
| `brpc_remote_processing_latency` | `CRecv - CSend` | Client wait from entering `CallMethod()` until it returns with a response or error. | Accurate client-side RPC wait time. Includes brpc protocol work, network transit, brpc scheduling, and server work; it does not mean server processing alone. |
| `brpc_client_rsp_framework_latency` | `CEnd - CRecv` | Local work after `CallMethod()` returns. | Includes Datasystem response attachment decoding, response copying, and trace-trailer parsing where applicable; it is not purely brpc work. |
| `brpc_server_req_queue_latency` | `SExecStart - SRecv` | Server generated-adapter work before calling the Datasystem service implementation. | This is handler pre-processing, not a direct measurement of the brpc socket-to-handler queue. For payload requests it includes Datasystem attachment decoding; ordinary unary requests are usually near zero. |
| `brpc_server_exec_latency` | `SExecEnd - SExecStart` | Time spent in the Datasystem service implementation (`impl_.Method(...)`). | Accurate for the generated call boundary. Any business lock wait, thread-pool wait, storage I/O, or downstream RPC performed synchronously by the implementation is included. |
| `brpc_server_rsp_queue_latency` | `SSend - SExecEnd` | Server generated-adapter work after the service implementation returns and before appending the trace trailer. | The name is historical: it is not a direct brpc kernel/socket send-queue measurement. Payload responses include Datasystem attachment encoding; ordinary unary responses are usually near zero. |
| `brpc_rpc_network_residual_latency` | `max(0, (CRecv - CSend) - (SSend - SRecv))` | The part of client RPC wait not covered by the server generated handler. | A clock-sync-free residual estimate, not a pure network RTT. It includes both network directions, brpc request/response protobuf protocol processing, brpc scheduling/queueing outside the generated handler, and other handler-external overhead. |

### Ownership Of Serialization And Queuing

The word `brpc` below means code executed by the brpc library, not Datasystem's generated protocol adapter code.

| Operation | Owner | Included in the latency decomposition |
| --- | --- | --- |
| Protobuf request/response binary serialization, protocol framing/unframing, and filling the request/response protobuf objects | brpc, which invokes protobuf facilities | Outside the generated server handler and therefore normally part of `brpc_rpc_network_residual_latency`; client-side wrapper work remains in the two client framework metrics. |
| Socket I/O, connection reuse/backpressure, event-loop and bthread scheduling | brpc / operating system | Normally part of `brpc_rpc_network_residual_latency` when it occurs outside `SRecv` through `SSend`. |
| Datasystem attachment payload framing and parsing (`[count][size][data]`) | Datasystem generated adapter | Server request parsing is in `brpc_server_req_queue_latency`; response encoding is in `brpc_server_rsp_queue_latency`; client response parsing is in `brpc_client_rsp_framework_latency`. These response phases are metrics only, not BRPC slow-log fields. |
| Server trace trailer append/consume | Datasystem trace code | Server append is included before `SSend`; client consume is included after `CRecv`. |

The server writes `SRecv`, `SExecStart`, `SExecEnd`, and `SSend` as a versioned response-attachment trailer. The
client removes and merges this trailer before recording the metrics, so subtracting the two local durations does not
require client and server wall clocks to be synchronized.

If either endpoint timestamp of a phase is absent or non-monotonic, its delta is zero and is not added to the
histogram. Consequently, an empty/zero-count histogram means that no positive valid sample was recorded; it does not
establish that the measured operation took zero microseconds. Streaming RPCs do not currently return the complete
server trace trailer. Their server phase metrics are therefore recorded independently by the server at stream end;
the client-side network-residual metric is not available for those paths.

## Direction Map

### client -> worker

- Control APIs use `ClientWorkerRemoteCommonApi`; OC uses `ClientWorkerRemoteApi`; SC uses `stream_cache::ClientWorkerApi`.
- Client control and SC brpc channels are owned by the client API object. Client OC stores channel + stub in a shared `BrpcSession` and publishes it with `std::atomic_store`.
- `RecreateOCStub()` can swap the OC session while request hot paths read it through `std::atomic_load`.
- Request payload options use brpc attachments, but shared-memory setup, fd receive, mmap, and shm reference decrement paths are outside brpc.

### worker -> worker

- OC and SC worker-worker APIs get stubs from `RpcStubCacheMgr`.
- OC remote get and migration carry metadata in protobuf and data through URMA/RDMA or brpc attachment fallback. `data_source` records which path was used.
- `WorkerWorkerTransportService` is a URMA handshake service, not a streaming data service.
- SC remote push uses `RemoteWorkerManager`, batched async write/read calls, ack tracking, blocking/unblocking, and loop prevention for remote-origin elements.

### worker <-> master

- Worker-master and master-worker APIs use `RpcStubCacheMgr` unless a local-bypass service object is available.
- Worker->master OC has the broadest async usage through generated `AsyncWrite`/`AsyncRead` and `AsyncRpcRequestManager`.
- Master-worker SC owns the production bidi streaming RPC `QueryMetadata`; most other calls are unary or unary plus generated async helpers.
- Master-master OC reuses `MasterOCService` remotely for metadata migration and cross-master reference coordination.

## Correctness Review Notes

These points were checked directly against source on `2026-07-01`:

- `src/datasystem/common/rpc/rpc_stub_cache_mgr.h` defines `kBrpcPortOffset = 0`.
- `src/datasystem/common/rpc/rpc_server.cpp` implements brpc adapter registration, brpc start, stop/join behavior, and brpc-mode ZMQ bind skipping.
- `src/datasystem/common/rpc/rpc_stub_cache_mgr.h/.cpp` implements channel/stub alias ownership, `WaitForBrpcSocketAvailable()`, and stale brpc stub eviction.
- `src/datasystem/common/rpc/trace_attachment.h` and brpc generator/client helpers inject TraceID into request attachments; `brpc_service_generator.cpp` strips it before method dispatch.
- `src/datasystem/client/object_cache/client_worker_api/client_worker_remote_api.cpp` atomically publishes and reloads the OC brpc session bundle.
- `src/datasystem/worker/worker_oc_server.cpp` registers brpc adapters for worker, worker-worker, master-worker, client-worker SC, and master services before `StartBrpcServer()`.
- `src/datasystem/protos/worker_stream.proto` contains the production `QueryMetadata(stream ...) returns (stream ...)` RPC; additional streaming methods found under test protos are not production paths.
- `src/datasystem/worker/object_cache/worker_worker_oc_service_impl.cpp` sets `DATA_ALREADY_TRANSFERRED`, `DATA_ALREADY_TRANSFERRED_MEMSET_META`, or `DATA_IN_PAYLOAD` according to RDMA success/fallback.

## When To Use

Use this playbook when a change touches:

- brpc/ZMQ backend selection or server startup;
- `RpcServer`, `RpcStubCacheMgr`, generated RPC code, `RpcOptions`, or retry/deadline behavior;
- proto service definitions or RPC options such as payload, unary socket, channel number, or streaming;
- TraceID propagation, brpc attachment framing, or payload parsing;
- client-worker reconnect, standby-worker switch, or OC atomic stub replacement;
- worker-worker remote get, migration, URMA handshake, SC remote push, or master-worker notifications;
- async RPC tag lifecycle, `BrpcAsyncContext`, or `AsyncRpcRequestManager`.

## Change Risk Gates

Treat the following as review-blocking until justified:

- changing endpoint binding, port offset, or startup order without proving brpc/ZMQ exclusivity;
- adding a new production streaming RPC without testing cancellation, backpressure, close ordering, and server shutdown;
- moving TraceID framing after payload bytes or parsing payload before stripping TraceID;
- returning a brpc stub whose channel lifetime is not co-owned;
- replacing client OC atomic session publication with independently swapped channel/stub fields;
- adding large protobuf fields for object/stream data that should remain in shm, RDMA, or attachment payload;
- holding locks across synchronous RPC, `AsyncRead`, stream reads/writes, or RDMA visibility waits;
- enabling brpc internal retry on paths already using datasystem `RetryOnError` slicing;
- retrying non-idempotent metadata operations such as seal/update paths without explicit idempotency proof;
- adding high-frequency success logs in request-critical RPC paths.

## Required Design Notes

Record these before implementation or in the PR body:

| Topic | Required answer |
| --- | --- |
| direction | client->worker, worker->worker, worker->master, master->worker, master->master, or coordinator |
| backend impact | brpc only, ZMQ only, or generated parity required |
| data path | protobuf only, brpc attachment, shared memory, URMA/RDMA, or mixed fallback |
| ownership | who owns channel, stub, stream, payload buffer, and async tag |
| timeout/retry | per-call timeout, overall deadline, retryable status codes, and idempotency |
| local bypass | whether same-process master/worker or embedded client paths must match remote behavior |
| observability | trace propagation, RPC diagnostics, metrics, and failure logs |
| verification | unit/component/system/perf test or reason a narrower check is sufficient |

## Useful Searches

```bash
rg -n "FLAGS_use_brpc|kBrpcPortOffset|StartBrpcServer|AddBrpcService" src/datasystem
rg -n "RpcStubCacheMgr::Instance\\(\\)\\.GetStub|BrpcChannelStubHolder|MaybeEvictStaleBrpcStub" src/datasystem
rg -n "AttachTraceIDToAttachment|TRCID:V1|request_attachment|response_attachment" src/datasystem/common/rpc
rg -n "AsyncWrite|AsyncRead|ForgetRequest|BrpcAsyncContext|AsyncRpcRequestManager" src/datasystem
rg -n "DATA_IN_PAYLOAD|DATA_ALREADY_TRANSFERRED|ZeroCopyBuffer|WriteViaFastTransport|HandlePayloadFallback" src/datasystem
rg -n "rpc .*stream|QueryMetadata" src/datasystem/protos src/datasystem/master src/datasystem/worker
```

## Verification Options

- For generated stub/service changes, rebuild the generated RPC targets and run at least one brpc-mode caller test plus one ZMQ/parity test when feasible.
- For server startup or service registration changes, run a worker startup/system test with `DATASYSTEM_USE_BRPC=true` and verify all expected service adapters are reachable.
- For attachment or TraceID changes, test a payload RPC and a no-payload RPC; confirm TraceID stripping does not shift payload framing.
- For async tag lifecycle changes, test success, timeout, `DONTWAIT`, and forgotten-request cleanup paths.
- For worker-worker data transfer changes, test RDMA-enabled behavior if available and the attachment fallback path even when RDMA is unavailable.
- For client OC reconnect changes, test concurrent request dispatch during `RecreateOCStub()` or standby-worker switch.

## Review Checklist

- [ ] brpc/ZMQ endpoint exclusivity is preserved.
- [ ] Service registration still happens before brpc listen starts.
- [ ] Generated brpc and ZMQ APIs remain behaviorally compatible where business code switches between them.
- [ ] Channel, stub, stream, async tag, and payload lifetimes are explicit and race-safe.
- [ ] TraceID and payload attachment framing remain compatible.
- [ ] Large data remains on shm/RDMA/attachment fallback instead of protobuf metadata fields.
- [ ] Timeout and retry behavior is bounded and idempotency-safe.
- [ ] Local-bypass behavior matches remote RPC behavior where applicable.
- [ ] Tests or explicit evidence cover the changed direction and data path.
