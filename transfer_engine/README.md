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
from yr.datasystem import TransferEngine, Result, ErrorCode
```

## Backend and HIXL Route Selection

TransferEngine selects the data-plane backend in this order:

1. `TRANSFER_ENGINE_BACKEND=p2p|hixl`, when set.
2. An explicit `protocol` value: `"p2p"` selects P2P-Transfer; `"hixl"` selects HIXL.
3. An empty `protocol` or `"ascend"` selects HIXL.

When HIXL is selected, `TRANSFER_ENGINE_HIXL_ROUTE` accepts `auto`, `hccs`, or `roce` and defaults to `auto`.
This value is a TransferEngine peer-consistency policy: both peers must use the same value. TransferEngine does not pass
it to HIXL as an endpoint filter. HIXL generates and matches endpoints as follows on the supported Atlas A2/A3 path:

- With `HCCL_INTRA_ROCE_ENABLE=1`, HIXL keeps or generates only RoCE endpoints, so the connection uses RoCE.
- Otherwise, HIXL normally advertises both device RoCE and HCCS endpoints. If both peers have the same
  `net_instance_id`, HIXL prefers HCCS and falls back to a mutually available RoCE endpoint. If their
  `net_instance_id` values differ, HIXL does not consider HCCS and selects a mutually available RoCE endpoint.
- HIXL derives `net_instance_id` from the SuperPod ID on Atlas A3 and from the local host IP on Atlas A2.

Use the following settings on both peers to make the route intent explicit:

```bash
# Force HIXL to advertise and use RoCE endpoints only.
export TRANSFER_ENGINE_BACKEND=hixl
export TRANSFER_ENGINE_HIXL_ROUTE=roce
export HCCL_INTRA_ROCE_ENABLE=1

# Declare an expected HCCS route and reject a peer with a different TransferEngine route policy. HIXL still selects
# HCCS only when both peers are in the same network instance and have a mutually available HCCS endpoint.
export TRANSFER_ENGINE_BACKEND=hixl
export TRANSFER_ENGINE_HIXL_ROUTE=hccs
unset HCCL_INTRA_ROCE_ENABLE
```

Setting only `TRANSFER_ENGINE_HIXL_ROUTE=roce` does not force HIXL to remove HCCS endpoints; use
`HCCL_INTRA_ROCE_ENABLE=1` as shown above. Likewise, `TRANSFER_ENGINE_HIXL_ROUTE=hccs` is not itself an HIXL endpoint
filter. TransferEngine rejects `hccs` together with `HCCL_INTRA_ROCE_ENABLE=1` because those settings conflict.

## 3. API Reference

`TransferEngine`:

```python
engine = TransferEngine()
```

Methods:

1. `initialize(local_hostname: str, protocol: str, device_name: str) -> Result`
   `protocol` accepts `"p2p"` for P2P-Transfer and `"hixl"`, `"ascend"`, or an empty string for HIXL.
   `TRANSFER_ENGINE_BACKEND=p2p|hixl` overrides `protocol`. `device_name` must match `npu:${device_id}`.
2. `register_memory(buffer_addr_regisrterch: int, length: int) -> Result`
3. `batch_register_memory(buffer_addrs: list[int], lengths: list[int]) -> Result`
4. `unregister_memory(buffer_addr_regisrterch: int) -> Result`
5. `batch_unregister_memory(buffer_addrs: list[int]) -> Result`
6. `transfer_sync_read(target_hostname: str, buffer: int, peer_buffer_address: int, length: int) -> Result`
7. `batch_transfer_sync_read(target_hostname: str, buffers: list[int], peer_buffer_addresses: list[int], lengths: list[int]) -> Result`
8. `finalize() -> Result`

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
