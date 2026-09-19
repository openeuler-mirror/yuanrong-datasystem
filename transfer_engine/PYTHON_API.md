# Transfer Engine Python API

## Contents

- [Transfer Engine Python API](#transfer-engine-python-api)
  - [Contents](#contents)
  - [Overview](#overview)
  - [Package Layout](#package-layout)
  - [Quick Start](#quick-start)
  - [Protocol, backend, and route compatibility](#protocol-backend-and-route-compatibility)
  - [Environment variables](#environment-variables)
  - [API Reference](#api-reference)
    - [Class: TransferEngine](#class-transferengine)
      - [Constructor](#constructor)
    - [Initialization](#initialization)
      - [`initialize()`](#initialize)
    - [Engine Information](#engine-information)
      - [`get_rpc_port()`](#get_rpc_port)
      - [`get_route_policy()`](#get_route_policy)
    - [Memory Registration](#memory-registration)
      - [Class: MemoryRegistration](#class-memoryregistration)
      - [`register_memory()`](#register_memory)
      - [`batch_register_memory()`](#batch_register_memory)
      - [`register_memory_ex()`](#register_memory_ex)
      - [`batch_register_memory_ex()`](#batch_register_memory_ex)
      - [`unregister_memory()`](#unregister_memory)
      - [`batch_unregister_memory()`](#batch_unregister_memory)
    - [Data Transfer Operations](#data-transfer-operations)
      - [`transfer_sync_read()`](#transfer_sync_read)
      - [`batch_transfer_sync_read()`](#batch_transfer_sync_read)
    - [Lifecycle](#lifecycle)
      - [`finalize()`](#finalize)
    - [Class: Result](#class-result)
      - [`is_ok()`](#is_ok)
      - [`is_error()`](#is_error)
      - [`get_code()`](#get_code)
      - [`get_msg()`](#get_msg)
      - [`to_string()`](#to_string)
    - [Enum: ErrorCode](#enum-errorcode)
  - [Usage Examples](#usage-examples)
    - [Basic Setup and Single Read](#basic-setup-and-single-read)
    - [Batch Read Pattern](#batch-read-pattern)
  - [Error Handling](#error-handling)
  - [Notes and Limitations](#notes-and-limitations)

## Overview

The Transfer Engine Python API exposes the native `TransferEngine` runtime to Python through `pybind11`.
It is designed for registering local device memory, then reading data from remote registered memory into local buffers.

At the moment, the Python binding exposes a focused subset of functionality:

- engine initialization
- RPC port query
- route-policy query
- single, batch, and backing-aware memory registration
- single and batch synchronous read
- engine finalization
- status-code based error handling

## Package Layout

Recommended import:

```python
from yr.datasystem import TransferEngine, Result, ErrorCode
```

Python entry files:

- `transfer_engine/python/yr/datasystem/__init__.py`

- the transfer_engine wheel is no longer published separately; its Python API is merged into the main datasystem wheels
- the Python import path remains `yr.datasystem`

Binding implementation:

- `transfer_engine/src/python/py_transfer_engine.cpp`

## Quick Start

```python
from yr.datasystem import TransferEngine

engine = TransferEngine()
rc = engine.initialize("127.0.0.1:60551", "ascend", "npu:0")
if rc.is_error():
    raise RuntimeError(rc.to_string())

port = engine.get_rpc_port()
print("rpc port:", port)

engine.finalize()
```

## Protocol, backend, and route compatibility

`ascend` is the only public protocol name. It replaced the former public `hixl` selector; HIXL remains the internal
data-plane implementation. Pass `"ascend"` (case-insensitive) to `initialize()`. `"hixl"`, the empty protocol, and
the former `TRANSFER_ENGINE_BACKEND` selector are not accepted aliases. Peers upgraded from the old selector must be
upgraded together: the control-plane handshake requires backend kind `ascend` on both sides. Data-plane peer-info
parsing rejects a non-`ascend` backend tag earlier with `ErrorCode.kInvalid`.

The C++ `IDataPlaneBackend::BackendKind()` default is also `"ascend"`. Applications that inject a custom backend
through the C++ API must update an old `"hixl"` override to `"ascend"`; an injected backend with another kind is
rejected with `ErrorCode.kNotSupported`, and peers with different kinds cannot connect.

`YR_TE_HIXL_CS_MODE` defaults to `on`, and `YR_TE_HIXL_ROUTE` defaults to `roce`. This default
combination requires HIXL client-server capability and selects CS Device RoCE; initialization fails with
`ErrorCode.kNotSupported` when CS is unavailable. Set CS mode to `auto` explicitly to restore capability-driven legacy
fallback, or `off` to require legacy. Set the route to `auto` explicitly to let HIXL match HCCS or RoCE endpoints.
`get_route_policy()` reports the normalized route after initialization. For `auto` and `hccs`, every registered backing
base address must be aligned to 2 MiB. Registration lengths remain byte-granular. Explicit `roce` permits an unaligned
backing base.

CANN/HIXL 9.1.0 is the minimum fully supported version. HIXL 8.5.2 through 9.0.x remains available only through the
legacy compatibility path; set `YR_TE_HIXL_CS_MODE=off` on both peers. Use
`YR_TE_HIXL_ROUTE=auto` with `HCCL_INTRA_ROCE_ENABLE` unset, or set the route to `roce` together with
`HCCL_INTRA_ROCE_ENABLE=1`. The core `hixl::Hixl` Engine supports AutoConnect from HIXL 9.1.0; set it to `off` on the
legacy compatibility path.

The 2 MiB rule also applies in legacy mode because `route=auto` can select HCCS. A legacy deployment that previously
reached cross-instance RoCE through `auto` with an unaligned allocation can therefore fail registration after an
upgrade. For a RoCE-only legacy deployment, configure `YR_TE_HIXL_ROUTE=roce` on both peers and set
`HCCL_INTRA_ROCE_ENABLE=1`; in CS mode, explicit `roce` uses the HIXL `roce:device` filter and does not require that
variable.

### Environment variables

`initialize()` reads the following variables. Route, CS-mode, and AutoConnect semantics are described above.

| Variable | Default | Purpose |
| --- | --- | --- |
| `YR_TE_HIXL_CS_MODE` | `on` | `auto`/`on`/`off` client-server mode selection |
| `YR_TE_HIXL_ROUTE` | `roce` | `auto`/`hccs`/`roce` route selection |
| `YR_TE_HIXL_AUTO_CONNECT` | `auto` | `auto`/`on`/`off` (`1`/`0` accepted) HIXL AutoConnect selection |
| `YR_TE_HIXL_GLOBAL_RESOURCE_CONFIG` | unset | Additional HIXL JSON options; an explicit route injects `protocol_desc` into it |
| `YR_TE_HIXL_LOCAL_COMM_RES` | unset | Explicit HIXL 1.3 JSON with `net_instance_id` and an endpoint list; CS mode only |
| `YR_TE_HIXL_ENDPOINT` | unset | Overrides the auto-derived HIXL endpoint |
| `YR_TE_HIXL_BASE_PORT` | `22000` | Listening-port segment base; each physical device owns 100 ports. The default stays 2000 above Mooncake ADXL's `20000`-based segments so co-located engines never probe the same ports |
| `YR_TE_HIXL_BUFFER_POOL` | `0:0` | HIXL buffer-pool option |
| `YR_TE_HIXL_CONNECT_TIMEOUT_MS` | `10000` | HIXL connection timeout in milliseconds |
| `YR_TE_HIXL_TRANSFER_TIMEOUT_MS` | `10000` | HIXL transfer timeout in milliseconds; the read-lease TTL must exceed it by at least 1000 ms |
| `YR_TE_HIXL_READ_LEASE_TTL_MS` | `30000` | Remote READ lease TTL in milliseconds |
| `YR_TE_ENABLE_ENV_DUMP` | unset | Dumps the process environment once when set to `1`/`true`/`on`/`yes` |
| `ASCEND_RT_VISIBLE_DEVICES` / `RT_ASCEND_VISIBLE_DEVICES` | unset | CANN-wide device visibility, not YuanRong-specific: maps the logical `npu:${device_id}` to a physical device id for endpoint port selection. Shared with the CANN runtime, HCCL, and torch_npu in the same process; `RT_ASCEND_VISIBLE_DEVICES` is the legacy spelling fallback |
| `YR_TE_HIXL_RDMA_TC` / `HCCL_RDMA_TC` | unset | RDMA traffic-class override for YuanRong TE. `YR_TE_HIXL_RDMA_TC` wins when both are set; `HCCL_RDMA_TC` is the official CANN/HCCL variable that also configures HCCL itself (for example torch_npu) so QoS stays aligned |
| `YR_TE_HIXL_RDMA_SL` / `HCCL_RDMA_SL` | unset | RDMA service-level override with the same precedence and HCCL-sharing semantics as `YR_TE_HIXL_RDMA_TC` / `HCCL_RDMA_TC` |

Logging behavior (level, destination, buffering, rotation, and format) is controlled by `YR_TE_LOG_LEVEL`,
`YR_TE_VLOG_LEVEL`, `YR_TE_VMODULE`, `YR_TE_LOG_DIR`, `YR_TE_LOG_TO_STDERR`,
`YR_TE_ALSO_LOG_TO_STDERR`, `YR_TE_LOG_TO_STDOUT`, `YR_TE_STDERR_THRESHOLD`,
`YR_TE_LOG_BUFFER_LEVEL`, `YR_TE_LOG_BUFFER_SECONDS`, `YR_TE_MAX_LOG_SIZE_MB`,
`YR_TE_LOG_FILE_MODE`, `YR_TE_TIMESTAMP_IN_LOG_FILE_NAME`, `YR_TE_LOG_FILE_HEADER`,
`YR_TE_LOG_PREFIX`, `YR_TE_LOG_YEAR_IN_PREFIX`, and `YR_TE_LOG_UTC_TIME`.

YuanRong TE reads only the `YR_TE_*` names above plus the CANN/HCCL ecosystem variables listed here. It never reads
Mooncake TE's `MC_*` variables, so YuanRong TE and Mooncake TE can run in the same process or container with fully
independent configuration: use the private `YR_TE_*` names for YuanRong-only control, and the
`HCCL_*`/CANN names only when the setting must also apply to HCCL or the CANN runtime.

## API Reference

### Class: TransferEngine

The main Python class for transfer engine operations.

#### Constructor

```python
TransferEngine()
```

Creates a new transfer engine instance.

### Initialization

#### `initialize()`

```python
initialize(local_hostname, protocol, device_name)
initialize(local_hostname, metadata_server, protocol, device_name)
```

Initializes the transfer engine control plane and binds the engine instance to a specific local device.

Parameters:

- `local_hostname` (`str`): Local endpoint in `host:port` format, for example `"127.0.0.1:60551"`. Use port `0`
  (for example, `"127.0.0.1:0"`) to let the operating system atomically select and bind an available port.
- `protocol` (`str`): The only supported value is `"ascend"` (case-insensitive). The Ascend backend uses HIXL
  internally.
- `device_name` (`str`): Device identifier string. It must match `npu:${device_id}`, for example `"npu:0"` or `"npu:1"`.
- `metadata_server` (`str`, four-argument form only): Compatibility value. It must be empty or `"P2PHANDSHAKE"`
  (case-insensitive); it does not select or contact a separate metadata service.

Returns:

- `Result`: `ErrorCode.kOk` on success; otherwise an error status

Notes:

- `device_name` is parsed internally and its numeric suffix is stored as the engine `device_id`
- malformed `device_name` returns `ErrorCode.kInvalid`
- Python does not expose `rpc_threads`; the engine uses a fixed internal value
- IPv6 endpoints must use bracketed host syntax, for example `"[::1]:60551"` or `"[fd00::1]:60551"`.
- When port `0` is requested, initialization resolves the actual nonzero port before initializing the data-plane
  backend or starting control-plane threads. Call `get_rpc_port()` after successful initialization and advertise that
  returned port to peers.
- When port `0` is requested and `YR_TE_RPC_PORT_MIN`/`YR_TE_RPC_PORT_MAX` are both set to valid values, the engine
  binds a randomly selected available port from that range (up to 500 attempts) instead of an OS-assigned ephemeral
  port. Valid ports are within `1024-65535` and outside the ephemeral range `32768-60999`. Invalid or partial
  configuration logs a warning and falls back to OS assignment; explicit nonzero ports are never remapped.
- On CANN/HIXL 9.1.0 or newer, set `YR_TE_HIXL_CS_MODE=on` and
  `YR_TE_HIXL_ROUTE=roce` on both peers to require CS Device RoCE. This path does not require
  `HCCL_INTRA_ROCE_ENABLE`; see [Backend and HIXL Route Selection](README.md#backend-and-hixl-route-selection).

The three-argument form is the compatibility form used by existing callers. The four-argument form is accepted for
callers that still provide the historical metadata-server slot; both forms initialize the same P2P handshake path.

### Engine Information

#### `get_rpc_port()`

```python
get_rpc_port()
```

Returns the actual local RPC listening port after successful initialization. If initialization requested port `0`, this
is the resolved nonzero port rather than `0`: an OS-assigned port by default, or a port within the configured
`YR_TE_RPC_PORT_MIN`/`YR_TE_RPC_PORT_MAX` range when that range is valid.

Returns:

- `int`: The local RPC port, or `-1` if the engine is not initialized

#### `get_route_policy()`

```python
get_route_policy()
```

Returns the normalized HIXL route policy selected during initialization: `"auto"`, `"hccs"`, or `"roce"`.
Returns an empty string before successful initialization. This method reports policy; it does not change the route.

### Memory Registration

#### Class: `MemoryRegistration`

```python
MemoryRegistration()
MemoryRegistration(logical_addr, logical_length, backing_addr, backing_length)
```

Describes a logical range that peers may read and the caller-owned backing range registered with the data-plane
backend. The logical range must be fully contained in the backing range. Keep the backing allocation alive until the
registration is successfully unregistered and all remote read leases have drained.

Public fields:

- `logical_addr` (`int`): Start address of the peer-authorized logical range
- `logical_length` (`int`): Length of the logical range in bytes
- `backing_addr` (`int`): Start address registered with the backend
- `backing_length` (`int`): Length of the backend backing range in bytes

The no-argument constructor creates a zero-valued descriptor; fill all four fields before passing it to an `_ex`
method.

The no-`_ex` methods below are shorthand for a registration whose logical and backing ranges are identical.

#### `register_memory()`

```python
register_memory(buffer_addr, capacity, location="*")
```

Registers one local memory region so that it can participate in transfer operations.

Parameters:

- `buffer_addr` (`int`): Local buffer address
- `capacity` (`int`): Buffer size in bytes
- `location` (`str`): Compatibility location selector. It must be empty, `"*"`, or exactly the initialized device name
  (for example, `"npu:0"`); the default is `"*"` and the value does not select a device.

Returns:

- `Result`: Success or error status

Common validation:

- `buffer_addr` must be positive
- `capacity` must be positive
- engine must already be initialized

#### `batch_register_memory()`

```python
batch_register_memory(buffer_addresses, capacities, location="*")
```

Registers multiple local memory regions in one call.

Parameters:

- `buffer_addresses` (`list[int]`): Local buffer addresses
- `capacities` (`list[int]`): Length for each buffer
- `location` (`str`): Same validation-only compatibility argument as `register_memory()`

Returns:

- `Result`: Success or error status

Common validation:

- `buffer_addresses` must not be empty
- `buffer_addresses` and `capacities` must have the same length
- every address must be positive
- every capacity must be positive

#### `register_memory_ex()`

```python
register_memory_ex(registration, location="*")
```

Registers one `MemoryRegistration`, allowing the peer-visible logical range to be a subrange of the backend backing
range. `location` has the same validation-only semantics as `register_memory()`.

#### `batch_register_memory_ex()`

```python
batch_register_memory_ex(registrations, location="*")
```

Registers multiple `MemoryRegistration` objects atomically from the Python caller's perspective. The list must not be
empty; each logical range must be positive and contained by its positive backing range, and overlapping backing ranges
are rejected unless they are the same range. Batch registration, unregistration, and read methods accept at most 4096
items per call.

#### `unregister_memory()`

```python
unregister_memory(buffer_addr)
```

Unregisters one previously registered local memory region.

Parameters:

- `buffer_addr` (`int`): Registered local buffer address

Returns:

- `Result`: Success or error status

#### `batch_unregister_memory()`

```python
batch_unregister_memory(buffer_addrs)
```

Unregisters multiple registered memory regions.

Parameters:

- `buffer_addrs` (`list[int]`): Registered local buffer addresses

Returns:

- `Result`: Success or error status

### Data Transfer Operations

#### `transfer_sync_read()`

```python
transfer_sync_read(target_hostname, buffer, peer_buffer_address, length, transport_hint="")
```

Synchronously reads data from a remote registered buffer into a local buffer.

Parameters:

- `target_hostname` (`str`): Remote owner endpoint, for example `"127.0.0.1:60551"`
- `buffer` (`int`): Local destination buffer address
- `peer_buffer_address` (`int`): Remote registered buffer address
- `length` (`int`): Number of bytes to read
- `transport_hint` (`str`): Compatibility hint; only empty string or `"ascend"` is accepted. It does not select HCCS
  or RoCE; use `YR_TE_HIXL_ROUTE` before initialization.

Returns:

- `Result`: Success or error status

Notes:

- the local buffer should already be allocated by the caller
- the remote buffer must have been registered on the peer side

#### `batch_transfer_sync_read()`

```python
batch_transfer_sync_read(target_hostname, buffers, peer_buffer_addresses, lengths, transport_hint="")
```

Synchronously reads multiple remote buffers into multiple local buffers in one batch.

Parameters:

- `target_hostname` (`str`): Remote owner endpoint
- `buffers` (`list[int]`): Local destination buffer addresses
- `peer_buffer_addresses` (`list[int]`): Remote registered buffer addresses
- `lengths` (`list[int]`): Number of bytes for each item
- `transport_hint` (`str`): Compatibility hint with the same accepted values and route semantics as
  `transfer_sync_read()`

Returns:

- `Result`: Success or error status

Common validation:

- all three lists must be non-empty
- all three lists must have the same length
- each address must be positive
- each length must be positive
- at most 4096 items per call

Behavior:

- item `i` in `peer_buffer_addresses` is read into item `i` in `buffers`

### Lifecycle

#### `finalize()`

```python
finalize()
```

Shuts down the engine instance and releases internal runtime state.

The call releases the Python GIL while native shutdown is running. It waits for in-flight synchronous reads, stops new
read-lease admission, and then waits for active remote READ leases. If leases do not drain within the 30-second
shutdown wait window, it returns `ErrorCode.kNotReady`; keep every registered backing allocation alive and retry
`finalize()` until it returns `ErrorCode.kOk`.

The native destructor retries `finalize()` after a `kNotReady` result. Therefore, relying on Python reference counting or
garbage collection for shutdown can block until existing leases expire (the read-lease TTL defaults to 30 seconds and
is configurable with `YR_TE_HIXL_READ_LEASE_TTL_MS`). Do not free, reuse, or let the tensor/array owning a
registered address be collected while finalization is pending.

Returns:

- `Result`: Success or error status

### Class: Result

`Result` is the common return type for `TransferEngine` methods.

Constructor:

```python
Result()
```

Methods:

#### `is_ok()`

```python
is_ok()
```

Returns:

- `bool`: `True` when the operation succeeded

#### `is_error()`

```python
is_error()
```

Returns:

- `bool`: `True` when the operation failed

#### `get_code()`

```python
get_code()
```

Returns:

- `ErrorCode`: The status code of the result

#### `get_msg()`

```python
get_msg()
```

Returns:

- `str`: The detail message carried by the status

#### `to_string()`

```python
to_string()
```

Returns:

- `str`: Combined string representation of code and message

### Enum: ErrorCode

Available enum values:

```python
ErrorCode.kOk
ErrorCode.kInvalid
ErrorCode.kNotFound
ErrorCode.kRuntimeError
ErrorCode.kNotReady
ErrorCode.kNotAuthorized
ErrorCode.kNotSupported
```

## Usage Examples

### Basic Setup and Single Read

```python
import torch
import torch_npu

from yr.datasystem import TransferEngine

owner = TransferEngine()
requester = TransferEngine()

owner_device_id = 0
requester_device_id = 1
size = 64

rc = owner.initialize("127.0.0.1:60551", "ascend", f"npu:{owner_device_id}")
assert rc.is_ok(), rc.to_string()

rc = requester.initialize("127.0.0.1:60552", "ascend", f"npu:{requester_device_id}")
assert rc.is_ok(), rc.to_string()

src = torch.arange(size, dtype=torch.uint8, device=f"npu:{owner_device_id}")
dst = torch.zeros(size, dtype=torch.uint8, device=f"npu:{requester_device_id}")

src_addr = int(src.data_ptr())
dst_addr = int(dst.data_ptr())

rc = owner.register_memory(src_addr, size)
assert rc.is_ok(), rc.to_string()

rc = requester.transfer_sync_read("127.0.0.1:60551", dst_addr, src_addr, size)
assert rc.is_ok(), rc.to_string()

print(torch.equal(src.cpu(), dst.cpu()))

requester.finalize()
owner.finalize()
```

### Batch Read Pattern

```python
import torch
import torch_npu

from yr.datasystem import TransferEngine

owner = TransferEngine()
requester = TransferEngine()

owner_id = 0
requester_id = 1
size = 256
batch_count = 3

assert owner.initialize("127.0.0.1:61051", "ascend", f"npu:{owner_id}").is_ok()
assert requester.initialize("127.0.0.1:61052", "ascend", f"npu:{requester_id}").is_ok()

src_tensors = [
    torch.full((size,), (i + 1) * 17, dtype=torch.uint8, device=f"npu:{owner_id}")
    for i in range(batch_count)
]
dst_tensors = [
    torch.zeros((size,), dtype=torch.uint8, device=f"npu:{requester_id}")
    for _ in range(batch_count)
]

src_addrs = [int(t.data_ptr()) for t in src_tensors]
dst_addrs = [int(t.data_ptr()) for t in dst_tensors]
lengths = [size] * batch_count

assert owner.batch_register_memory(src_addrs, lengths).is_ok()
assert requester.batch_transfer_sync_read("127.0.0.1:61051", dst_addrs, src_addrs, lengths).is_ok()

requester.finalize()
owner.finalize()
```

Reference examples in the repository:

- `transfer_engine/tests/python/st/test_python_api_st.py`
- `transfer_engine/tests/python/smoke/test_python_api_smoke.py`

## Error Handling

Recommended pattern:

```python
rc = engine.batch_register_memory(buffer_addresses, capacities)
if rc.is_error():
    print("code:", rc.get_code())
    print("msg:", rc.get_msg())
    raise RuntimeError(rc.to_string())
```

Typical failure cases:

- invalid `device_name`, such as `"gpu:0"` or `"0"`, returns `ErrorCode.kInvalid`
- protocol `"hixl"` or an empty protocol returns `ErrorCode.kInvalid`; use `"ascend"`
- an unsupported four-argument `metadata_server` returns `ErrorCode.kNotSupported`
- transfer before `initialize()` returns `ErrorCode.kNotReady`
- empty batch input returns `ErrorCode.kInvalid`
- mismatched batch list length returns `ErrorCode.kInvalid`
- an invalid `location` or `transport_hint` returns `ErrorCode.kNotSupported`
- an unaligned backing base under `auto` or `hccs` route returns `ErrorCode.kInvalid`
- missing registered region may return `ErrorCode.kNotFound`
- unauthorized or invalid remote read may return `ErrorCode.kNotAuthorized`

## Notes and Limitations

- The current Python binding only exposes synchronous read operations. It does not expose write APIs or async transfer APIs.
- `protocol` only accepts `"ascend"` (case-insensitive).
- The four-argument `initialize()` form accepts only the compatibility metadata value `"P2PHANDSHAKE"` or empty.
- HIXL `auto` route selection and the settings for deterministic HCCS or RoCE behavior are documented in
  [Backend and HIXL Route Selection](README.md#backend-and-hixl-route-selection).
- Backing bases must be 2 MiB-aligned for `auto` and `hccs`; explicit `roce` does not require this alignment.
- `device_name` must use the `npu:${device_id}` format.
- The transfer_engine wheel is no longer published separately, and the installed Python import path remains `yr.datasystem`.
