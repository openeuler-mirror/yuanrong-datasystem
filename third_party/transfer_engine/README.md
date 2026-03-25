# transfer_engine Python API Guide

## 1. Build Python Wheel

### Option A: one-click script
```bash
./build.sh
```

### Option B: manual
```bash
python3 -m pip install wheel
python3 setup.py bdist_wheel
```

Wheel output:
```bash
dist/*.whl
```

## 2. Python Package

```python
from yr.datasystem import TransferEngine, Status, StatusCode
```

## 3. API Reference

`TransferEngine`:

```python
engine = TransferEngine()
```

Methods:

1. `initialize(local_hostname: str, protocol: str, device_name: str) -> Status`
   `protocol` is currently not validated. `device_name` must match `npu:${device_id}`.
2. `register_memory(buffer_addr_regisrterch: int, length: int) -> Status`
3. `batch_register_memory(buffer_addrs: list[int], lengths: list[int]) -> Status`
4. `unregister_memory(buffer_addr_regisrterch: int) -> Status`
5. `batch_unregister_memory(buffer_addrs: list[int]) -> Status`
6. `transfer_sync_read(target_hostname: str, buffer: int, peer_buffer_address: int, length: int) -> Status`
7. `batch_transfer_sync_read(target_hostname: str, buffers: list[int], peer_buffer_addresses: list[int], lengths: list[int]) -> Status`
8. `finalize() -> Status`

`Status`:

1. `is_ok() -> bool`
2. `is_error() -> bool`
3. `get_code() -> StatusCode`
4. `get_msg() -> str`
5. `to_string() -> str`

`StatusCode`:

- `kOk`
- `kInvalid`
- `kNotFound`
- `kRuntimeError`
- `kNotReady`
- `kNotAuthorized`
- `kNotSupported`

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
