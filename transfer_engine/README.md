# transfer_engine Python API Guide

The control endpoint is an internal trusted-cluster interface. Restrict it with the bind address and network policy;
the current protocol does not provide TLS or cluster identity authentication.

## 1. Build the Python Module

The transfer_engine wheel is no longer published separately. The `_transfer_engine` extension module is built with
CMake and packaged into the main datasystem wheels (`python/setup.py` collects `_transfer_engine*.so` into
`yr/datasystem`).

### Option A: artifact script
```bash
scripts/build_python_artifacts.sh <python> <transfer_engine_src_dir> <build_dir> <package_dir>
```

Configures Release with `TRANSFER_ENGINE_BUILD_PYTHON=ON`, `TRANSFER_ENGINE_BUILD_TESTS=OFF`, and
`TRANSFER_ENGINE_ENABLE_HIXL=ON`, builds the `_transfer_engine` target, and writes the module into `<package_dir>`;
the `libds-spdlog.so` runtime is copied to `<package_dir>/lib` when produced.

### Option B: manual CMake
```bash
cmake -S . -B build -DTRANSFER_ENGINE_BUILD_PYTHON=ON -DTRANSFER_ENGINE_BUILD_TESTS=OFF \
  -DTRANSFER_ENGINE_ENABLE_HIXL=ON
cmake --build build --target _transfer_engine --parallel
```

The module is written to `python/yr/datasystem` by default; set `TRANSFER_ENGINE_PYTHON_OUTPUT_DIR` to override.

## 2. Python Package

```python
from yr.datasystem import TransferEngine, Result, ErrorCode
```

## RPC Port Selection

`initialize()` accepts port `0` (for example, `"127.0.0.1:0"`). The engine binds an available port while holding the
listening socket, so the resolved port is race-free; read it back with `get_rpc_port()` and advertise that endpoint to
peers. By default the operating system assigns an ephemeral port.

`YR_TE_RPC_PORT_MIN` and `YR_TE_RPC_PORT_MAX` constrain OS-assigned ports to a fixed range, mirroring Mooncake's
`MC_MIN_RPC_PORT`/`MC_MAX_RPC_PORT`:

```bash
export YR_TE_RPC_PORT_MIN=15000
export YR_TE_RPC_PORT_MAX=17000
```

- Both variables must be set together; values must be within `1024-65535`, outside the ephemeral range `32768-60999`,
  and `MIN <= MAX`.
- The engine randomly probes the range (up to 500 attempts) and holds the first port it binds.
- Invalid or partial configuration logs a warning and falls back to OS assignment.
- Explicit nonzero ports passed to `initialize()` are never remapped.

## Backend and HIXL Route Selection

TransferEngine exposes `"ascend"` as its only protocol. The Ascend backend uses HIXL internally; HIXL is not a
separate public protocol selector.

`YR_TE_HIXL_CS_MODE` accepts `auto`, `on`, or `off` and defaults to `on`:

- `auto` uses HIXL CS when `GetCapability(CLIENT_SERVER_COMM)` reports support, and otherwise keeps the legacy
  CommEngine path.
- `on` requires CANN/HIXL 9.1.0 or newer with the CS capability. Initialization returns `kNotSupported` instead of
  silently falling back when the capability is unavailable.
- `off` is the rollback setting and always keeps the legacy path.

`YR_TE_HIXL_ROUTE` accepts `auto`, `hccs`, or `roce` and defaults to `roce`. Both peers must use the same
effective CS mode and route. In CS mode, TransferEngine maps an explicit route to HIXL's endpoint filter:

- `roce` injects `comm_resource_config.protocol_desc=roce:device`.
- `hccs` injects `comm_resource_config.protocol_desc=hccs:device`.
- `auto` leaves endpoint matching to HIXL; on A3, HCCS can still win when both endpoints are in the same network
  instance.

The default `CS_MODE=on` and `ROUTE=roce` combination requires HIXL client-server capability and selects CS Device RoCE.
If the capability is unavailable, initialization fails with `kNotSupported` instead of falling back to legacy. Set
`YR_TE_HIXL_CS_MODE=auto` explicitly to restore capability-driven legacy fallback, or `off` to require legacy.
Set `YR_TE_HIXL_ROUTE=auto` explicitly to restore vendor automatic route matching.

CANN/HIXL 9.1.0 is the minimum fully supported version. Builds that detect HIXL 8.5.2 through 9.0.x retain a
compatibility-only legacy path and print a CMake warning. They must disable CS explicitly on both peers:

```bash
export YR_TE_HIXL_CS_MODE=off
export YR_TE_HIXL_AUTO_CONNECT=off
export YR_TE_HIXL_ROUTE=auto
unset HCCL_INTRA_ROCE_ENABLE

# Legacy RoCE alternative:
export YR_TE_HIXL_ROUTE=roce
export HCCL_INTRA_ROCE_ENABLE=1
```

The core `hixl::Hixl` Engine supports AutoConnect starting with HIXL 9.1.0. Use `off` on the compatibility-only legacy
path; on 9.1+, leave it at `auto` for capability-driven selection or set it to `off` for explicit Connect.

Use this deterministic A3 Device RoCE configuration on both peers:

```bash
# CANN/HIXL >= 9.1.0.
export YR_TE_HIXL_CS_MODE=on
export YR_TE_HIXL_ROUTE=roce
unset HCCL_INTRA_ROCE_ENABLE

# Deterministic Device HCCS through CS.
export YR_TE_HIXL_CS_MODE=on
export YR_TE_HIXL_ROUTE=hccs
unset HCCL_INTRA_ROCE_ENABLE
```

TransferEngine does not set `HCCL_INTRA_ROCE_ENABLE`. In CS mode the `roce:device` filter is sufficient, so A3 RoCE
does not require that environment variable. In legacy mode, an explicit `roce` route still requires
`HCCL_INTRA_ROCE_ENABLE=1`; otherwise initialization rejects the ambiguous configuration. TransferEngine also rejects
`hccs` together with `HCCL_INTRA_ROCE_ENABLE=1`.

### Protocol and backend compatibility

`ascend` is the public protocol name. It replaced the former public `hixl` selector; HIXL remains the internal
implementation. New callers must pass `"ascend"` (case-insensitive). `"hixl"`, the empty protocol, and the former
`TRANSFER_ENGINE_BACKEND` selector are not compatibility aliases. During a rolling upgrade, both peers must expose the
`ascend` backend kind; an older peer that advertises `hixl` fails the backend-kind handshake with `kNotSupported`.
Data-plane peer-info parsing rejects a non-`ascend` backend tag earlier with `kInvalid`.

`IDataPlaneBackend::BackendKind()` is a public C++ extension point whose default is now `"ascend"`. An injected custom
backend must report `"ascend"` to initialize successfully, and both peers must report the same kind. A custom backend
that still relies on the old default `"hixl"` must update its override before upgrading.

`YR_TE_HIXL_GLOBAL_RESOURCE_CONFIG` remains available for additional HIXL JSON settings. An explicit route
adds its `protocol_desc` while preserving other fields; a conflicting user-supplied `protocol_desc` returns `kInvalid`.
`YR_TE_HIXL_LOCAL_COMM_RES` can supply an explicit HIXL 1.3 JSON object when deployment must provide
`net_instance_id` and a deterministic endpoint list. It is rejected outside CS mode or when its version is not `1.3`.

`YR_TE_HIXL_AUTO_CONNECT` accepts `auto`, `on`, or `off` and defaults to `auto`. The core `hixl::Hixl` Engine
and `GetCapability(AUTO_CONNECT)` support it starting with HIXL 9.1.0. `on` fails closed when unsupported, and `off` is
the connection-policy rollback. Existing `1` and `0` values remain accepted as aliases for `on` and `off`. AutoConnect
does not bypass TransferEngine's mode/route compatibility check, owner authorization, read lease, or memory generation
check.

For memory registration, the backing base address must be 2 MiB-aligned when `YR_TE_HIXL_ROUTE` is `auto` or
`hccs`; transfer lengths remain byte-granular. Explicit `roce` does not impose this alignment check. This validation is
also applied in legacy mode because `auto` may still select HCCS, so a legacy deployment that previously used an
unaligned backing allocation and reached RoCE through `route=auto` can fail registration after upgrading. For a
RoCE-only legacy deployment, set `YR_TE_HIXL_ROUTE=roce` on both peers and set
`HCCL_INTRA_ROCE_ENABLE=1` as required by the legacy path.

Retryable synchronous READ failures (`kNotReady` or `kRuntimeError`) trigger one route cleanup and full authorization/
connection rebuild before the error is returned. Other failures are not retried.

Additional `TRANSFER_ENGINE_*` environment variables (endpoint, port base, timeouts, lease TTL, and logging) are
listed in [PYTHON_API.md](PYTHON_API.md#environment-variables).

## 3. API Reference

`TransferEngine`:

```python
engine = TransferEngine()
```

Methods:

1. `initialize(local_hostname: str, protocol: str, device_name: str) -> Result`
   `protocol` only accepts `"ascend"` (case-insensitive). `device_name` must match `npu:${device_id}`. Set the endpoint
   port to `0`, such as `"127.0.0.1:0"`, to have the OS atomically allocate and bind an available control-plane port.
2. `initialize(local_hostname: str, metadata_server: str, protocol: str, device_name: str) -> Result`
   Compatibility form; `metadata_server` must be empty or `"P2PHANDSHAKE"` (case-insensitive), and does not select a
   separate metadata service.
3. `get_rpc_port() -> int`
   Returns the actual bound control-plane port, including the nonzero port selected for `initialize(...:0, ...)`.
4. `get_route_policy() -> str`
   Returns `auto`, `hccs`, or `roce` after initialization, and an empty string before initialization.
5. `register_memory(buffer_addr: int, capacity: int, location: str = "*") -> Result`
6. `batch_register_memory(buffer_addresses: list[int], capacities: list[int], location: str = "*") -> Result`
7. `register_memory_ex(registration: MemoryRegistration, location: str = "*") -> Result`
8. `batch_register_memory_ex(registrations: list[MemoryRegistration], location: str = "*") -> Result`
9. `unregister_memory(buffer_addr: int) -> Result`
10. `batch_unregister_memory(buffer_addresses: list[int]) -> Result`
11. `transfer_sync_read(target_hostname: str, buffer: int, peer_buffer_address: int, length: int,
    transport_hint: str = "") -> Result`
12. `batch_transfer_sync_read(target_hostname: str, buffers: list[int], peer_buffer_addresses: list[int],
    lengths: list[int], transport_hint: str = "") -> Result`
13. `finalize() -> Result`

`location` is a validation-only compatibility argument. It accepts `""`, `"*"`, or the exact initialized device name
(for example, `"npu:0"`); it does not choose a device or route. `transport_hint` is also compatibility-only: it accepts
an empty string or `"ascend"` and does not choose HCCS or RoCE. Configure the route with
`YR_TE_HIXL_ROUTE` instead.

`MemoryRegistration(logical_addr, logical_length, backing_addr, backing_length)` authorizes the logical byte range to
peers while registering the caller-owned backing range with the backend. The backing range must contain the logical
range, and the underlying allocation must remain alive until the registration is successfully unregistered and any
remote read lease has drained. The non-`_ex` registration methods use the same address and capacity for both ranges.
Registration, unregistration, and batch-read methods accept at most 4096 items per call.

`Result`:

1. `is_ok() -> bool`
2. `is_error() -> bool`
3. `get_code() -> ErrorCode`
4. `get_msg() -> str`
5. `to_string() -> str`

`ErrorCode`:

- `kOk`
- `kInvalid`
- `kNotFound`
- `kRuntimeError`
- `kNotReady`
- `kNotAuthorized`
- `kNotSupported`

`finalize()` waits for in-flight reads, stops new read-lease admission, and then waits for active remote leases. It may
return `kNotReady` when leases do not drain within the shutdown wait window; keep every registered allocation alive and
retry until it returns `kOk`. The native destructor retries this operation, so relying on Python garbage collection can
block for the configured read-lease TTL (30 seconds by default, `YR_TE_HIXL_READ_LEASE_TTL_MS`). Do not
release or reuse registered device memory merely because `finalize()` has returned `kNotReady`.

## 4. Quick Example (single process)

```python
import torch
import torch_npu
from yr.datasystem import TransferEngine

owner = TransferEngine()
requester = TransferEngine()

owner_device_id = 0
requester_device_id = 1
owner.initialize("127.0.0.1:60551", "ascend", f"npu:{owner_device_id}")
requester.initialize("127.0.0.1:60552", "ascend", f"npu:{requester_device_id}")

size = 64
src = torch.arange(size, dtype=torch.uint8, device=f"npu:{owner_device_id}")
dst = torch.zeros(size, dtype=torch.uint8, device=f"npu:{requester_device_id}")

src_addr = src.data_ptr()
dst_addr = dst.data_ptr()
owner.register_memory(src_addr, size)

rc = requester.transfer_sync_read("127.0.0.1:60551", dst_addr, src_addr, size)
print(rc.to_string())
print("equal:", torch.equal(src.cpu(), dst.cpu()))

requester.finalize()
owner.finalize()
```

### IPv6 and address selection

TransferEngine accepts IPv6 control-plane endpoints in bracketed form, for example `"[::1]:60551"` or
`"[fd00::1]:60551"`.

## 5. Cross-node Smoke Example (owner/requester)

Smoke script:

`tests/python/smoke/test_python_api_smoke.py`

### 5.1 Start owner (Node A)

```bash
PYTHONPATH=.:python python3 tests/python/smoke/test_python_api_smoke.py \
  --role owner \
  --local-hostname 10.10.10.1:18481 \
  --device-id 0 \
  --size 4096 \
  --register-count 4 \
  --hold-seconds 600
```

Owner will print:

- `[OWNER_READY] ... remote_addrs=...`
- `[OWNER_READY_FOR_REQUESTER] --peer-hostname ... --peer-device-id ... --remote-addrs ...`

### 5.2 Run requester (Node B)

Use the printed values from owner:

```bash
PYTHONPATH=.:python python3 tests/python/smoke/test_python_api_smoke.py \
  --role requester \
  --local-hostname 10.10.10.2:18482 \
  --device-id 1 \
  --size 4096 \
  --peer-hostname 10.10.10.1:18481 \
  --peer-device-id 0 \
  --remote-addrs 0x1234,0x5678,0x9abc,0xdef0 \
  --auto-verify-data
```

## 6. ST Test

ST case file:

`tests/python/st/test_python_api_st.py`

Run:
```bash
PYTHONPATH=.:python python3 -m unittest tests.python.st.test_python_api_st -v
```

Notes:

1. ST currently requires `torch` + `torch_npu`.
2. ST expects at least 2 NPUs on one node (same-node, different `device_id`).
