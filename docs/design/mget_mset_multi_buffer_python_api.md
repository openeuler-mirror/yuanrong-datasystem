# MGetH2D/MSetD2H Multi-Buffer Python API Design

## 1. Document Metadata

- Status: Implemented; pending Linux build and performance validation
- Design scope: Python HeteroClient synchronous Get/Set hot-path optimization and implementation record
- Target scenario: vLLM Ascend KV cache loading and storing through Yuanrong Datasystem
- Primary code paths:
  - `python/yr/datasystem/hetero_client.py`
  - `src/datasystem/pybind_api/pybind_register_hetero.cpp`
  - `src/datasystem/client/hetero_cache/hetero_client.cpp`
  - `src/datasystem/client/object_cache/object_client_impl.cpp`
- Related benchmark:
  - `tests/st/device/parallel_h2d_perf_test.cpp`
- Last verified against source: 2026-07-15

## 2. Background

The vLLM Ascend Yuanrong backend receives the destination layout for one KV cache load as three native Python
containers:

```python
keys: list[str]
addrs: list[list[int]]
sizes: list[list[int]]
```

Each outer element represents one Datasystem object key. Each inner address and size pair represents one destination
device-memory blob for that key.

The original Yuanrong Python API did not accept this representation directly for either Get or Set. The caller had to
create one Python `Blob` object for every address and one Python `DeviceBlobList` object for every key:

```python
blob_lists = []
for key_addrs, key_sizes in zip(addrs, sizes):
    blobs = [Blob(addr, size) for addr, size in zip(key_addrs, key_sizes)]
    blob_lists.append(DeviceBlobList(device_idx, blobs))

failed_keys = client.mget_h2d(keys, blob_lists, timeout_ms)
client.mset_d2h(keys, blob_lists, set_param)
```

`HeteroClient.mget_h2d()` and `HeteroClient.mset_d2h()` subsequently traverse these wrapper objects again in
`_change_blob_list_from_py_to_cpp()` and creates pybind `Blob` and `DeviceBlobList` objects before entering the native
`HeteroClient::MGetH2D()` or `HeteroClient::MSetD2H()` implementation.

This object construction is not part of H2D or D2H transfer. It is Python binding overhead that grows linearly with the
number of source or destination blobs.

The first multi-buffer implementation added `mget_h2d_from_multi_buffers` and `mset_d2h_from_multi_buffers`, removing
the custom Python wrapper graph. Its initial pybind signature still automatically converted `addrs` and `sizes` into
temporary `vector<vector<uint64_t>>` containers before `BuildDeviceBlobLists` traversed them again. The current design
keeps those method names and semantics, but changes their binding internals to write Python integers directly into the
final descriptors. It also adds `batch_is_exist` so batch consumers do not copy the existing `exist()` boolean result
into a second integer list.

## 3. Measured Problem

The GLM 5.0 W4A8, TP=8, block-size=128, 64K-input simulation produced the following breakdown for one load:

| Item | Value |
| --- | ---: |
| Keys | 464 |
| Destination blobs | 109,968 |
| Total bytes | 6,606,290,944 |
| Input validation | 0.012 ms |
| Python wrapper to pybind object conversion | 132.239 ms |
| pybind call and native MGetH2D | 320.609 ms |
| Total Python MGetH2D call | 452.859 ms |

Approximately 29% of the observed Get API latency is spent constructing and converting descriptor wrapper objects
before the native H2D path starts. Parallel FFTS improves the native copy portion, which makes this fixed Python-side
cost a larger fraction of end-to-end latency.

The Set path has the same Python descriptor-construction sequence before `MSetD2H`, although a matching Set breakdown
has not yet been captured. The optimization is therefore applied symmetrically to the synchronous Get and Set APIs;
the final Set benefit must be established by measurement rather than inferred from the Get number.

The optimization target is descriptor construction. This design does not change object metadata, host-memory layout,
NUMA policy, FFTS execution, stream synchronization, or the H2D data path.

## 4. Goals

1. Allow Python callers to pass `keys`, device addresses, and sizes directly to Yuanrong for synchronous Get and Set.
2. Eliminate per-blob Python `Blob` construction from high-cardinality MGetH2D calls.
3. Build the existing C++ `DeviceBlobList` representation directly from Python nested lists in one pybind traversal,
   without first materializing `vector<vector<uint64_t>>` temporaries.
4. Reuse `HeteroClient::MGetH2D()` and `HeteroClient::MSetD2H()` so transfer behavior, validation, perf points, tracing,
   and error semantics remain unchanged.
5. Preserve the existing object-based `mget_h2d` and `mset_d2h` APIs for compatibility.
6. Keep all descriptor storage alive until the synchronous native call completes.
7. Provide focused correctness and performance validation for the new entrypoint.
8. Let batch-oriented callers receive integer existence indicators without copying `list[bool]` into `list[int]`.

## 5. Non-Goals

1. Removing or changing `metadata_header` or stored-object metadata.
2. Changing the public C++ `HeteroClient::MGetH2D()` signature or ABI.
3. Changing Parallel Direct, Parallel FFTS, Remote H2D, HCCS, or RoCE behavior.
4. Adding asynchronous Get or Set multi-buffer APIs in the first version.
5. Accepting NumPy, Torch tensors, arbitrary buffer-protocol objects, or device pointer ownership from Python.
6. Combining keys, addresses, sizes, and offsets into one persisted or wire-format descriptor.
7. Guaranteeing a particular latency reduction before benchmark validation.

## 6. Existing Backend Comparison

### 6.1 Mooncake

The vLLM Ascend Mooncake backend passes the native input containers directly:

```python
store.batch_get_into_multi_buffers(keys, addrs, sizes)
```

Its pybind interface receives:

```cpp
const std::vector<std::string> &keys,
const std::vector<std::vector<uintptr_t>> &allBufferPtrs,
const std::vector<std::vector<size_t>> &allSizes
```

The binding converts integer addresses to `void *`, releases the GIL, and enters the C++ multi-buffer API. It does not
require Python objects for individual buffers.

### 6.2 Memcache

The vLLM Ascend Memcache backend similarly calls:

```python
store.batch_get_into_layers(keys, addrs, sizes, direct)
```

The pybind layer accepts two-dimensional address and size vectors. Native code validates the shape and creates one
`MmcBufferArray` per key. Lower layers aggregate those arrays into batch-copy descriptors.

### 6.3 Memfabric Hybrid

Memfabric Hybrid is a lower-level C++ transport rather than the Python backend boundary. It consumes native
`CopyDescriptor` arrays and converts them into bounded SGL/IOV submissions such as `Channel_OneSideRequestSgl` and
`ChannelGetV`.

All three implementations follow the same relevant principle: Python or an upper layer supplies numeric address and
size arrays, while native code owns descriptor construction and batching.

## 7. Selected API

Keep the following synchronous multi-buffer methods on `yr.datasystem.HeteroClient` and add `batch_is_exist`:

```python
def mget_h2d_from_multi_buffers(
    self,
    keys: list[str],
    addrs: list[list[int]],
    sizes: list[list[int]],
    sub_timeout_ms: int = 0,
) -> list[str]:
    """Get host objects directly into multiple device buffers per key."""

def mset_d2h_from_multi_buffers(
    self,
    keys: list[str],
    addrs: list[list[int]],
    sizes: list[list[int]],
    set_param: SetParam = SetParam(),
) -> None:
    """Set multiple device buffers per key directly into host objects."""

def batch_is_exist(self, keys: list[str]) -> list[int]:
    """Return 1 for an existing key and 0 for a missing key."""
```

The device holding every address in a request is not passed in by the caller. The pybind layer queries the calling
thread's current device context (the ACL context a vLLM rank worker binds beforehand, equivalent to Mooncake's
`aclrtGetDevice()` convention) once per call while the GIL is held, and stamps it into every `DeviceBlobList.deviceIdx`.
This is safe because `deviceIdx` is the local-process NPU for the entire H2D/D2H path: it feeds `aclrtMemcpyBatch`'s
`aclrtMemLocation`, the H2D/D2H stream pool, and `UpdateClientRemoteH2DConfig`'s `clientDeviceId_` — all of which refer
to the client's own device, including the Remote H2D case where the target NPU is local and only the source host memory
is remote.

Parameter semantics:

| Parameter | Direction | Meaning |
| --- | --- | --- |
| `keys` | input | Object keys to load from or store into Datasystem. |
| `addrs[i][j]` | Get output / Set input | Integer device address of blob `j` for key `i`. |
| `sizes[i][j]` | input | Buffer size in bytes corresponding to `addrs[i][j]`. |
| `sub_timeout_ms` | Get input | Same per-subrequest timeout semantics as existing `mget_h2d`. |
| `set_param` | Set input | Same write mode, existence, TTL, and cache-type semantics as existing `mset_d2h`. |
| (resolved) device | internal | Queried from the calling thread's device context; stamped into every `DeviceBlobList.deviceIdx`. |
| Get return value | output | Failed keys, with the same semantics as existing `mget_h2d`. |
| Set return value | output | `None`, with native errors raised as in existing `mset_d2h`. |
| `batch_is_exist` return value | output | Integer indicators corresponding one-to-one with `keys`. |

The correspondence invariant is:

```text
keys[i] <-> addrs[i] <-> sizes[i]
addrs[i][j] <-> sizes[i][j]
```

All device addresses in one call must still reside on the same device, and that device is the calling thread's current
device. A caller loading or storing multiple devices must bind the corresponding device context and invoke the method
once per device, matching the existing `DeviceBlobList.deviceIdx` semantics used by vLLM workers.

### 7.1 Why the Name Uses `multi_buffers`

The name describes the externally visible representation: every key may map to multiple destination buffers. It also
aligns with the established Mooncake terminology and avoids exposing Yuanrong's internal `DeviceBlobList` type to new
callers.

### 7.2 Why the First Version Is Two-Dimensional

The upstream vLLM interface already provides `list[list[int]]`. Requiring callers to produce:

```text
flat_addrs + flat_sizes + per-key offsets
```

would introduce another Python traversal and two additional large Python lists. It would also make shape validation and
error reporting less direct.

The selected interface removes expensive wrapper-object construction while preserving the caller's existing data
shape. A future buffer-protocol or contiguous-array API can be considered if measurements show pybind's integer-list
conversion remains material after this change.

## 8. Native Binding Design

Keep the two Python-only multi-buffer adapters in `pybind_register_hetero.cpp` and add the Python-only
`batch_is_exist` adapter. Do not add new public methods to `include/datasystem/hetero_client.h`. Both multi-buffer
adapters reuse one private descriptor-construction helper so validation, reserve behavior, `deviceIdx`, and `srcOffset`
cannot diverge.

The existing multi-buffer method names and Python signatures remain unchanged. Their pybind arguments use `py::list`
so the adapter can validate and convert each nested integer directly into its final `Blob` entry. This replaces the
original automatic conversion to temporary `vector<vector<uint64_t>>` containers; it does not add a V2 method.

Conceptual implementation:

```cpp
.def("mget_h2d_from_multi_buffers",
     [](HeteroClient &client, const std::vector<std::string> &keys,
        const py::list &addrs, const py::list &sizes, uint64_t subTimeoutMs) {
         auto blobLists = BuildDeviceBlobLists(keys, addrs, sizes);

         py::gil_scoped_release release;
         TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
         std::vector<std::string> failedKeys;
         auto status = client.MGetH2D(keys, blobLists, failedKeys, subTimeoutMs);
         return std::make_pair(status, std::move(failedKeys));
     })

.def("mset_d2h_from_multi_buffers",
     [](HeteroClient &client, const std::vector<std::string> &keys,
        const py::list &addrs, const py::list &sizes, const SetParam &setParam) {
         auto blobLists = BuildDeviceBlobLists(keys, addrs, sizes);

         py::gil_scoped_release release;
         TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
         return client.MSetD2H(keys, blobLists, setParam);
     })

.def("batch_is_exist",
     [](HeteroClient &client, const std::vector<std::string> &keys) {
         std::vector<bool> exists;
         auto status = CallExistWithoutGil(client, keys, exists);
         return std::make_pair(status, BuildPythonIntList(exists));
     })
```

`BuildDeviceBlobLists` performs the shared shape validation, reserves the outer and inner vectors, queries the calling
thread's current device once via `DeviceManagerFactory::GetDeviceManager()->GetDeviceIdx()` (which calls
`aclrtGetDevice`) and stamps it into every `DeviceBlobList.deviceIdx`, sets `srcOffset` to zero, and writes every
Python address/size pair directly into its final native `Blob` descriptor. The device query runs while the GIL is held
because it does not touch Python; if no accelerator is detected or the thread has not bound a device context, the
helper raises a `RuntimeError` pointing the caller at `mget_h2d`/`mset_d2h` with an explicit `DeviceBlobList.deviceIdx`.
`batch_is_exist` reuses the existing native `HeteroClient::Exist()` operation but constructs Python integer objects
directly; the existing `exist()` method and its boolean return contract are unchanged.

### 8.1 GIL Boundary

Python-container traversal and native descriptor construction happen while the GIL is held. The GIL is released
immediately before `HeteroClient::MGetH2D()` or `HeteroClient::MSetD2H()` and remains released for the blocking native
request.

Releasing the GIL before reading Python containers would be unsafe. Holding it during the actual transfer would
unnecessarily serialize other Python threads.

### 8.2 Ownership And Lifetime

- `addrs` contain borrowed numeric device addresses. Yuanrong does not allocate, own, or free this memory.
- For Get the addresses are destinations; for Set they are sources.
- The caller must keep every referenced device allocation valid until the synchronous method returns.
- `blobLists` and their `Blob` vectors are owned by the pybind stack frame.
- `HeteroClient::MGetH2D()` and `HeteroClient::MSetD2H()` are synchronous and take the descriptor vector by const
  reference, so the stack-owned descriptors remain valid for the complete request.
- The existing native implementation owns any deeper temporary state needed during the call.
- An asynchronous version must not reuse this stack-lifetime design. It would require owned descriptors captured by the
  asynchronous state, consistent with the existing async Get and Set implementations.

## 9. Validation And Error Semantics

The shared binding helper should reject malformed shapes before either adapter calls native code:

1. `keys`, `addrs`, and `sizes` must be non-empty.
2. `keys.size() == addrs.size() == sizes.size()`.
3. Every `addrs[i]` must be non-empty.
4. `addrs[i].size() == sizes[i].size()` for every key.
5. Address and size values must fit their native unsigned types.

The helper then queries the calling thread's current device once. If no accelerator is detected
(`DeviceManagerFactory::GetDeviceManager() == nullptr`) or the query returns a negative index, the helper raises a
`RuntimeError` before any native Get or Set call, pointing the caller at `mget_h2d`/`mset_d2h` with an explicit
`DeviceBlobList.deviceIdx`. This mirrors the existing fallback in the device-object path
(`ClientDeviceObjectManager`) and `RemoteH2DManager::SetDeviceIdx`, both of which resolve a missing device index from
the current context.

The adapters should not duplicate transfer-specific validation already owned by
`ObjectClientImpl::CheckMGetH2DInput()`, `CheckMSetD2HInput()`, or later transfer processing. In particular, object size,
sender/receiver blob-count compatibility, pointer validity, SetParam semantics, and transport errors remain native Get
and Set responsibilities.

The high-level Python method follows the existing behavior:

```python
status, failed_keys = self._client.mget_h2d_from_multi_buffers(...)
if status.is_error():
    raise RuntimeError(status.to_string())
return failed_keys

status = self._client.mset_d2h_from_multi_buffers(...)
if status.is_error():
    raise RuntimeError(status.to_string())
```

Malformed Python argument types continue to produce `TypeError`. Invalid shapes should produce a clear `RuntimeError`
or `ValueError` before transfer starts. The implementation should use one consistent exception type for all shape
errors and document it in the Python API reference.

## 10. End-to-End Call Flow

```text
vLLM Ascend
  keys + addrs[][] + sizes[][]
          |
          v
Python HeteroClient multi-buffer Get/Set entrypoint
  basic Python argument validation
          |
          v
pybind HeteroClient multi-buffer Get/Set adapter
  validate outer/inner dimensions
  reserve vector capacity
  construct vector<DeviceBlobList>
  release GIL
          |
          v
HeteroClient::MGetH2D or HeteroClient::MSetD2H
  existing trace and perf points
          |
          v
ObjectClientImpl Get or Set path
  existing metadata, allocation, and transfer selection
          |
          v
Existing H2D or D2H transport
```

No worker RPC, metadata format, shared-memory object, or transport protocol changes.

## 11. Hot-Path Assessment

### 11.1 Complexity

Let `K` be the number of keys and `B` the total number of device blobs.

- Current Python Get/Set paths: `O(B)` Python wrapper construction plus `O(B)` wrapper-to-pybind conversion.
- Initial multi-buffer binding: `O(B)` conversion into temporary nested vectors plus `O(B)` construction of final
  descriptors.
- Optimized multi-buffer binding: one `O(B)` pass that converts each integer directly into its final descriptor, plus
  one `O(1)` `aclrtGetDevice` call to resolve the device index for the request (does not scale with `K` or `B`).
- Native MGetH2D and MSetD2H transfer complexity is unchanged.

### 11.2 Allocation Plan

- Reserve `keys.size()` entries in the outer `DeviceBlobList` vector.
- Reserve `addrs[i].size()` entries in every inner `Blob` vector.
- Do not create per-blob Python objects.
- Do not create a second Python flattened representation.

### 11.3 Copy Plan

- No object payload copy is introduced.
- Numeric address and size values are not stored in intermediate nested C++ vectors.
- Each pair is written once into its final native `Blob` descriptor.
- The existing H2D or D2H payload copy remains unchanged.

### 11.4 Locking And Concurrency

- No new global or shared state is introduced.
- No new mutex is required.
- Descriptor construction is request-local.
- The GIL remains the only synchronization during Python container conversion and is released for native execution.
- Existing MGetH2D and MSetD2H concurrency, thread-pool, stream, and queue behavior is unchanged.

### 11.5 Logging

Do not add unconditional success-path logging. Existing perf points continue to measure native Get and Set operations.
Temporary Python breakdown logging may be used during validation, but it should remain opt-in and must not enumerate
keys or addresses.

## 12. Compatibility And Rollout

### 12.1 Backward Compatibility

- Keep `mget_h2d(keys, data_blob_list, sub_timeout_ms)` and `mset_d2h(keys, data_blob_list, set_param)` unchanged.
- Keep `Blob` and `DeviceBlobList` Python classes unchanged.
- Keep the public C++ API and ABI unchanged.
- The multi-buffer Python methods keep their names but drop the `device_idx` parameter: the device is resolved from
  the calling thread's device context. Because these methods have not shipped in a released Yuanrong package yet,
  this is not a breaking change for external callers.
- Add `batch_is_exist`; keep the existing `exist` interface and boolean result unchanged.

### 12.2 vLLM Ascend Integration

During mixed-version rollout, the Yuanrong backend may use capability detection:

```python
fast_get = getattr(self._hetero_client, "mget_h2d_from_multi_buffers", None)
if fast_get is not None:
    failed_keys = fast_get(keys, addrs, sizes, 0)
else:
    blob_lists = self._helper.make_blob_lists(addrs, sizes)
    failed_keys = self._hetero_client.mget_h2d(keys, blob_lists, 0)

fast_set = getattr(self._hetero_client, "mset_d2h_from_multi_buffers", None)
if fast_set is not None:
    fast_set(keys, addrs, sizes, set_param)
else:
    blob_lists = self._helper.make_blob_lists(addrs, sizes)
    self._hetero_client.mset_d2h(keys, blob_lists, set_param)

batch_is_exist = getattr(self._hetero_client, "batch_is_exist", None)
if batch_is_exist is not None:
    exists = batch_is_exist(keys)
else:
    exists = [1 if value else 0 for value in self._hetero_client.exist(keys)]
```

This fallback supports older Yuanrong Python packages. Once the minimum supported Yuanrong version includes both new
methods, vLLM may remove the fallback and the old descriptor-construction helper if it has no remaining callers.

### 12.3 Rollback

Rollback requires only switching the caller back to the existing object-based Get and Set APIs. No stored data,
metadata, worker version, configuration, or migration must be reverted.

## 13. Alternatives Considered

### 13.1 Keep Constructing Python Blob Objects

Rejected because measured Get wrapper conversion consumes 132.239 ms for 109,968 blobs and scales with blob count. Set
uses the same wrapper-conversion helper. Native transfer improvements cannot remove this overhead.

### 13.2 Add a New Public C++ HeteroClient Overload

Rejected for the first version because the native core already uses the correct `DeviceBlobList` representation. The
problem exists only at the Python boundary. A public overload would expand ABI and documentation surface without
improving the transfer implementation.

### 13.3 Require Flat Arrays and Per-Key Offsets

Deferred. It may reduce native nested-vector allocations when inputs already reside in contiguous numeric storage, but
vLLM currently owns nested Python lists. Flattening them in Python adds allocations and traversal. This alternative
should be reconsidered only with a caller capable of supplying contiguous buffers without rebuilding them for each Get
or Set.

### 13.4 Remove Object Metadata

Rejected because metadata lookup and descriptor construction are separate costs. Removing metadata does not eliminate
Python `Blob` creation and would change correctness, compatibility, and storage semantics.

### 13.5 Cache Python Blob Objects

Rejected as the primary design because device allocations can change across requests and caching creates pointer
lifetime and invalidation risks. It also retains a large Python object graph and does not improve first-use latency.

## 14. Test And Verification Plan

### 14.1 Python Unit Tests

Add focused tests for:

1. One key with one buffer.
2. Multiple keys with different buffer counts.
3. Empty keys, addresses, and sizes for both methods.
4. Outer dimension mismatch.
5. Inner address/size dimension mismatch.
6. Negative device index.
7. Get success returns an empty failed-key list.
8. Get partial failure preserves the failed-key result.
9. Get and Set native error statuses become the same Python exceptions as their existing APIs.
10. SetParam fields are forwarded without changing their values.
11. Existing object-based Get and Set remain functional.
12. `batch_is_exist` returns integer values while `exist` continues to return booleans.

### 14.2 Pybind Or Component Tests

Verify that the adapter produces descriptors equivalent to manually constructed `DeviceBlobList` values:

- identical `deviceIdx`;
- `srcOffset == 0`;
- identical blob order;
- identical pointer and size values;
- no descriptor reordering across keys.

Malformed shapes must fail before `HeteroClient::MGetH2D()` or `HeteroClient::MSetD2H()` is invoked.

### 14.3 System Test

Run the same data through both APIs:

1. Store identical objects once through the existing object-based Set and once through the multi-buffer Set.
2. Allocate independent destination regions.
3. Load one object set with the existing object-based Get.
4. Load the other object set with the multi-buffer Get.
5. Compare Set status, Get status, failed keys, and destination content.

Cover the active local H2D policy and retain one remote-H2D compatibility case where the environment supports it.

### 14.4 Performance Test

Use the same GLM simulation shape for the Get before/after comparison:

- 464 keys;
- 109,968 blobs;
- 6,606,290,944 bytes;
- same H2D policy;
- same device and NUMA placement;
- same warm-up and measured rounds.

Record at least:

```text
descriptor_conversion_ms
native_mget_h2d_ms
total_mget_h2d_ms
aggregate_gbps
failed_key_count
```

Acceptance criteria:

1. Results and loaded data match the existing API.
2. Descriptor conversion is materially lower than the measured 132.239 ms baseline.
3. Native MGetH2D latency does not regress outside normal run-to-run variance.
4. End-to-end `load_kvc` latency improves consistently in repeated runs.
5. No additional per-request success logs or persistent memory growth are introduced.

The exact latency target should be set after measuring the implementation because automatic pybind conversion still
visits every Python integer.

Add a separate Set comparison using the same key/blob shape and source buffers. Record descriptor conversion,
native MSetD2H, and total store latency. Do not claim the measured 132.239 ms Get conversion cost as a Set result.

Performance validation should complement the single-call conversion comparison with the deployment-shaped concurrency
cases used by vLLM Ascend:

- one Datasystem worker, eight NPU client processes, and one synchronous multi-buffer request per NPU;
- 64K Get: 512 keys, 119,808 descriptors, and 6.703125 GiB per NPU, with eight independent resident rank copies;
- 4K Set: 32 fresh keys, 7,488 descriptors, and 0.418945 GiB per NPU, modeling one chunk-prefill update;
- Direct versus Parallel Direct and single-thread versus multi-thread FFTS under identical data, worker, NUMA, and
  huge-page settings.

This validation remains outside CTest because the default Get profile needs eight real NPUs and keeps 53.625 GiB in one
worker. The timed interval should cover the complete public multi-buffer call, including Python validation and pybind
conversion; allocation, preload, existence verification, and cleanup should be excluded.

## 15. Build And Documentation Impact

Expected implementation files:

- `src/datasystem/pybind_api/pybind_register_hetero.cpp`
- `python/yr/datasystem/hetero_client.py`
- focused Python or pybind tests
- Python API reference for both multi-buffer methods and `batch_is_exist`

No worker, master, protobuf, RPC, configuration, CMake target, persisted format, or public C++ header change is expected.
If the implementation adds a new test source rather than extending an existing target, the corresponding CMake and
Bazel test manifests must remain synchronized where both build systems own that test area.

## 16. Risks And Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Outer or inner arrays are misaligned | Data written to an incorrect destination | Strict dimension checks before native call. |
| Source or destination pointer lifetime ends early | Memory corruption or runtime failure | Synchronous-only first version and explicit lifetime contract. |
| Calling thread has not bound a device context | `aclrtGetDevice` returns no usable index; copy cannot target the device | Helper raises `RuntimeError` before any native call, pointing the caller at `mget_h2d`/`mset_d2h` with an explicit `DeviceBlobList.deviceIdx`. |
| Addresses span multiple devices in one call | Native copy/stream-pool keyed on a single resolved device fails or targets the wrong device | Same single-device constraint as before; callers binding multiple devices invoke once per device. |
| Large Python integer conversion remains costly | Smaller-than-expected gain | Measure conversion separately; consider contiguous buffer protocol only if needed. |
| Old vLLM with new SDK or new vLLM with old SDK | Missing method or inconsistent behavior | Additive API and optional caller-side capability fallback. |
| Duplicated validation diverges | Inconsistent errors | Shared binding helper validates only representation shape; native Get/Set paths own transfer semantics. |
| Async API copies stack descriptors after return | Use-after-free | Do not add async adapter without owned async state. |

## 17. Open Questions

1. Should shape errors use `ValueError` for Python consistency or `RuntimeError` for compatibility with the existing
   wrapper? The implementation and API reference must choose one consistently.
2. Should the vLLM compatibility fallback remain indefinitely or be removed after raising its minimum Yuanrong version?
3. After the direct nested-list conversion is measured, is the remaining Python-integer conversion sufficiently small,
   or should the same methods later accept an additional contiguous buffer representation?
4. Does Set need a long-term perf point for descriptor conversion, or is temporary benchmark instrumentation sufficient
   after the symmetric API is validated?

## 18. Recommended Implementation Sequence

1. Add the shared pybind shape validation and native descriptor-construction helper.
2. Add synchronous Get and Set pybind adapters, high-level Python methods, and documentation.
3. Add equivalence and invalid-input tests.
4. Build and run focused Python/component tests.
5. Integrate both methods in vLLM Ascend with temporary capability fallback.
6. Run the existing GLM simulation and end-to-end vLLM comparison.
7. Remove temporary diagnostic logging after the performance result is captured, or keep it strictly opt-in if it is
   still needed for production diagnosis.
