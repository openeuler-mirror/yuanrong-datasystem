# Transfer Engine Python API

## Contents

- [Transfer Engine Python API](#transfer-engine-python-api)
  - [Contents](#contents)
  - [Overview](#overview)
  - [Package Layout](#package-layout)
  - [Quick Start](#quick-start)
  - [API Reference](#api-reference)
    - [Class: TransferEngine](#class-transferengine)
      - [Constructor](#constructor)
    - [Initialization](#initialization)
      - [`initialize()`](#initialize)
    - [Engine Information](#engine-information)
      - [`get_rpc_port()`](#get_rpc_port)
    - [Memory Registration](#memory-registration)
      - [`register_memory()`](#register_memory)
      - [`batch_register_memory()`](#batch_register_memory)
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
- single and batch memory registration
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
```

Initializes the transfer engine control plane and binds the engine instance to a specific local device.

Parameters:

- `local_hostname` (`str`): Local endpoint in `host:port` format, for example `"127.0.0.1:60551"`
- `protocol` (`str`): Transport protocol name. The current implementation accepts the value and does not validate it. Existing examples use `"ascend"`.
- `device_name` (`str`): Device identifier string. It must match `npu:${device_id}`, for example `"npu:0"` or `"npu:1"`.

Returns:

- `Result`: `ErrorCode.kOk` on success; otherwise an error status

Notes:

- `device_name` is parsed internally and its numeric suffix is stored as the engine `device_id`
- malformed `device_name` returns `ErrorCode.kInvalid`
- Python does not expose `rpc_threads`; the engine uses a fixed internal value

### Engine Information

#### `get_rpc_port()`

```python
get_rpc_port()
```

Returns the local RPC listening port after successful initialization.

Returns:

- `int`: The local RPC port, or `-1` if the engine is not initialized

### Memory Registration

#### `register_memory()`

```python
register_memory(buffer_addr, length)
```

Registers one local memory region so that it can participate in transfer operations.

Parameters:

- `buffer_addr` (`int`): Local buffer address
- `length` (`int`): Buffer size in bytes

Returns:

- `Result`: Success or error status

Common validation:

- `buffer_addr` must be positive
- `length` must be positive
- engine must already be initialized

#### `batch_register_memory()`

```python
batch_register_memory(buffer_addrs, lengths)
```

Registers multiple local memory regions in one call.

Parameters:

- `buffer_addrs` (`list[int]`): Local buffer addresses
- `lengths` (`list[int]`): Length for each buffer

Returns:

- `Result`: Success or error status

Common validation:

- `buffer_addrs` must not be empty
- `buffer_addrs` and `lengths` must have the same length
- every address must be positive
- every length must be positive

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
transfer_sync_read(target_hostname, buffer, peer_buffer_address, length)
```

Synchronously reads data from a remote registered buffer into a local buffer.

Parameters:

- `target_hostname` (`str`): Remote owner endpoint, for example `"127.0.0.1:60551"`
- `buffer` (`int`): Local destination buffer address
- `peer_buffer_address` (`int`): Remote registered buffer address
- `length` (`int`): Number of bytes to read

Returns:

- `Result`: Success or error status

Notes:

- the local buffer should already be allocated by the caller
- the remote buffer must have been registered on the peer side

#### `batch_transfer_sync_read()`

```python
batch_transfer_sync_read(target_hostname, buffers, peer_buffer_addresses, lengths)
```

Synchronously reads multiple remote buffers into multiple local buffers in one batch.

Parameters:

- `target_hostname` (`str`): Remote owner endpoint
- `buffers` (`list[int]`): Local destination buffer addresses
- `peer_buffer_addresses` (`list[int]`): Remote registered buffer addresses
- `lengths` (`list[int]`): Number of bytes for each item

Returns:

- `Result`: Success or error status

Common validation:

- all three lists must be non-empty
- all three lists must have the same length
- each address must be positive
- each length must be positive

Behavior:

- item `i` in `peer_buffer_addresses` is read into item `i` in `buffers`

### Lifecycle

#### `finalize()`

```python
finalize()
```

Shuts down the engine instance and releases internal runtime state.

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
rc = engine.batch_register_memory(buffer_addrs, lengths)
if rc.is_error():
    print("code:", rc.get_code())
    print("msg:", rc.get_msg())
    raise RuntimeError(rc.to_string())
```

Typical failure cases:

- invalid `device_name`, such as `"gpu:0"` or `"0"`, returns `ErrorCode.kInvalid`
- transfer before `initialize()` returns `ErrorCode.kNotReady`
- empty batch input returns `ErrorCode.kInvalid`
- mismatched batch list length returns `ErrorCode.kInvalid`
- missing registered region may return `ErrorCode.kNotFound`
- unauthorized or invalid remote read may return `ErrorCode.kNotAuthorized`

## Notes and Limitations

- The current Python binding only exposes synchronous read operations. It does not expose write APIs or async transfer APIs.
- `protocol` is currently accepted but not checked by the engine implementation.
- `device_name` must use the `npu:${device_id}` format.
- The loader in `yr.datasystem` preloads several runtime shared libraries when available.
- The transfer_engine wheel is no longer published separately, and the installed Python import path remains `yr.datasystem`.
