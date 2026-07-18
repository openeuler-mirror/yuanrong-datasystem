/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Register function to python.
 */
#include <cstdint>
#include <limits>
#include <vector>

#include <pybind11/detail/common.h>

#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/hetero_client.h"
#include "datasystem/hetero/future.h"
#include "datasystem/kv_client.h"
#include "datasystem/pybind_api/pybind_register.h"

using datasystem::ConnectOptions;
namespace datasystem {
constexpr int32_t DEFAULT_FUTURE_GET_TIMEOUT_MS = 60000;

class AsyncResultFuture {
public:
    AsyncResultFuture(std::shared_future<AsyncResult> future, std::string traceId)
        : future_(std::move(future)), traceId_(std::move(traceId))
    {
    }

    py::object Get(uint64_t timeoutMs)
    {
        AsyncResult result;
        {
            if (timeoutMs > std::numeric_limits<int64_t>::max()) {
                throw std::runtime_error(
                    FormatString("timeoutMs out of scope, timeoutMs: %s, traceId: %s", timeoutMs, traceId_));
            }
            // When C++ blocks waiting for a future, release the GIL to avoid blocking Python execution.
            py::gil_scoped_release release;
            auto status = future_.wait_for(std::chrono::milliseconds(timeoutMs));
            if (status == std::future_status::timeout) {
                py::gil_scoped_acquire acquire;
                throw std::runtime_error(
                    FormatString("Future get timeout, timeoutMs: %s, traceId: %s", timeoutMs, traceId_));
            }
            result = future_.get();
        }

        // Operating Python objects requires GIL lock protection.
        py::gil_scoped_acquire acquire;
        py::dict res_dict;
        res_dict["status"] = result.status;
        res_dict["failed_keys"] = result.failedList;
        return res_dict;
    }

private:
    std::shared_future<AsyncResult> future_;
    std::string traceId_;
};

namespace {
// Discover the caller's current device from its thread-local device context (e.g. the ACL
// context vLLM binds per rank worker). Every destination/source address in one multi-buffer
// request must reside on the same device, and that device is the local process device for
// both direct and Remote H2D paths, so it can be queried once instead of being passed in.
int32_t ResolveCurrentDeviceIdx()
{
    auto *deviceManager = DeviceManagerFactory::GetDeviceManager();
    if (deviceManager == nullptr) {
        throw std::runtime_error(
            "No accelerator device detected. Bind a device context on the calling thread before calling "
            "mget_h2d_from_multi_buffers/mset_d2h_from_multi_buffers, or use mget_h2d/mset_d2h with an "
            "explicit DeviceBlobList.deviceIdx.");
    }
    int32_t deviceIdx = -1;
    auto rc = deviceManager->GetDeviceIdx(deviceIdx);
    if (rc.IsError() || deviceIdx < 0) {
        throw std::runtime_error(FormatString(
            "Failed to query the current device index via aclrtGetDevice (rc=%s). Bind a device context on the "
            "calling thread before calling mget_h2d_from_multi_buffers/mset_d2h_from_multi_buffers.",
            rc.ToString()));
    }
    return deviceIdx;
}

std::vector<DeviceBlobList> BuildDeviceBlobLists(const std::vector<std::string> &objectKeys, const py::list &devPtrs,
                                                 const py::list &sizes)
{
    if (objectKeys.empty() || devPtrs.size() == 0 || sizes.size() == 0) {
        throw py::value_error("keys, dev_ptrs and sizes must not be empty");
    }
    auto objectCount = objectKeys.size();
    if (objectCount != devPtrs.size() || objectCount != sizes.size()) {
        throw py::value_error("keys, dev_ptrs and sizes must have the same outer size");
    }

    // All addresses in this request belong to the calling thread's current device. Query it once
    // while the GIL is held (the call does not touch Python) and stamp it into every blob list.
    int32_t deviceIdx = ResolveCurrentDeviceIdx();

    std::vector<DeviceBlobList> devBlobLists;
    devBlobLists.reserve(objectKeys.size());
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        auto devPtrGroupObj = devPtrs[i];
        auto sizeGroupObj = sizes[i];
        if (!py::isinstance<py::list>(devPtrGroupObj) || !py::isinstance<py::list>(sizeGroupObj)) {
            throw py::type_error(FormatString("dev_ptrs[%zu] and sizes[%zu] must be lists", i, i));
        }
        auto devPtrGroup = py::reinterpret_borrow<py::list>(devPtrGroupObj);
        auto sizeGroup = py::reinterpret_borrow<py::list>(sizeGroupObj);
        if (devPtrGroup.size() == 0) {
            throw py::value_error(FormatString("dev_ptrs[%zu] must not be empty", i));
        }
        if (devPtrGroup.size() != sizeGroup.size()) {
            throw py::value_error(FormatString("dev_ptrs[%zu] and sizes[%zu] must have the same size", i, i));
        }

        DeviceBlobList devBlobList;
        devBlobList.deviceIdx = deviceIdx;
        devBlobList.srcOffset = 0;
        auto blobCount = devPtrGroup.size();
        devBlobList.blobs.reserve(static_cast<size_t>(blobCount));
        for (size_t j = 0; j < blobCount; ++j) {
            auto devPtr = py::cast<uint64_t>(devPtrGroup[j]);
            auto size = py::cast<uint64_t>(sizeGroup[j]);
            devBlobList.blobs.emplace_back(Blob{ reinterpret_cast<void *>(devPtr), size });
        }
        devBlobLists.emplace_back(std::move(devBlobList));
    }
    return devBlobLists;
}
} // namespace

PybindDefineRegisterer g_pybind_define_f_AsyncResultFuture("AsyncResultFuture", PRIORITY_LOW, [](const py::module *m) {
    py::class_<AsyncResultFuture>(*m, "AsyncResultFuture")
        .def("get", &AsyncResultFuture::Get, py::arg("timeoutMs") = DEFAULT_FUTURE_GET_TIMEOUT_MS);
});

PybindDefineRegisterer g_pybind_define_f_Future("Future", PRIORITY_LOW, [](const py::module *m) {
    py::class_<Future>(*m, "Future")
        .def(
            "get",
            [](Future &future, uint64_t timeoutMs) {
                py::gil_scoped_release release;
                Status rc = future.Get(timeoutMs);
                if (rc.GetCode() == K_FUTURE_TIMEOUT) {
                    throw FutureTimeoutException(rc.GetMsg());
                } else if (rc.IsError()) {
                    throw std::runtime_error(rc.ToString());
                }

                return rc;
            },
            py::arg("timeoutMs") = DEFAULT_FUTURE_GET_TIMEOUT_MS);
});

PybindDefineRegisterer g_pybind_define_f_HeteroClient("HeteroClient", PRIORITY_LOW, [](const py::module *m) {
    py::class_<HeteroClient, std::shared_ptr<HeteroClient>>(*m, "HeteroClient")
        .def(py::init([](const std::string &host, int32_t port, int32_t connectTimeoutMs, const std::string &token,
                         const std::string &clientPublicKey, const std::string &clientPrivateKey,
                         const std::string &serverPublicKey, const std::string &accessKey, const std::string &secretKey,
                         const std::string &tenantId, const bool enableCrossNodeConnection, int32_t reqTimeoutMs,
                         bool enableRemoteH2D) {
            ConnectOptions connectOpts{
                .host = host,
                .port = port,
                .connectTimeoutMs = connectTimeoutMs,
                .requestTimeoutMs = reqTimeoutMs,
                .token = token,
                .clientPublicKey = clientPublicKey,
                .clientPrivateKey = clientPrivateKey,
                .serverPublicKey = serverPublicKey,
                .accessKey = accessKey,
                .secretKey = secretKey,
                .tenantId = tenantId,
                .enableCrossNodeConnection = enableCrossNodeConnection,
                .enableRemoteH2D = enableRemoteH2D
            };
            return std::make_unique<HeteroClient>(connectOpts);
        }))
        .def("init",
             [](HeteroClient &client) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 return client.Init();
             })

        .def("delete",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<std::string> failedObjectKeys;
                 auto status = client.Delete(objectKeys, failedObjectKeys);
                 return std::make_pair(status, std::move(failedObjectKeys));
             })

        .def("mget_h2d",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys,
                const std::vector<DeviceBlobList> &devBlobList, uint64_t subTimeoutMs) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.MGetH2D(objectKeys, devBlobList, failedKeys, subTimeoutMs);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("mget_h2d_from_multi_buffers",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys, const py::list &devPtrs,
                const py::list &sizes, uint64_t subTimeoutMs) {
                 auto devBlobLists = BuildDeviceBlobLists(objectKeys, devPtrs, sizes);
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.MGetH2D(objectKeys, devBlobLists, failedKeys, subTimeoutMs);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("pre_register_device_memory",
             [](HeteroClient &client, const std::vector<uint64_t> &devPtrs, const std::vector<uint64_t> &sizes) {
                 std::vector<void *> ptrs;
                 ptrs.reserve(devPtrs.size());
                 for (auto devPtr : devPtrs) {
                     ptrs.emplace_back(reinterpret_cast<void *>(devPtr));
                 }
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 return client.PreRegisterDeviceMemory(ptrs, sizes);
             })

        .def("mset_d2h",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys,
                const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 return client.MSetD2H(objectKeys, devBlobList, setParam);
             })

        .def("mset_d2h_from_multi_buffers",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys, const py::list &devPtrs,
                const py::list &sizes, const SetParam &setParam) {
                 auto devBlobLists = BuildDeviceBlobLists(objectKeys, devPtrs, sizes);
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 return client.MSetD2H(objectKeys, devBlobLists, setParam);
             })

        .def("async_mset_d2h",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys,
                const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::shared_future<AsyncResult> future = client.AsyncMSetD2H(objectKeys, devBlobList, setParam);
                 return AsyncResultFuture(std::move(future), Trace::Instance().GetTraceID());
             })

        .def("async_mget_h2d",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys,
                const std::vector<DeviceBlobList> &devBlobList, uint64_t subTimeoutMs) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::shared_future<AsyncResult> future = client.AsyncMGetH2D(objectKeys, devBlobList, subTimeoutMs);
                 return AsyncResultFuture(std::move(future), Trace::Instance().GetTraceID());
             })

        .def("dev_publish",
             [](HeteroClient &client, const std::vector<std::string> &keys,
                const std::vector<DeviceBlobList> &blob2dList) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<Future> futureVec;
                 Status rc = client.DevPublish(keys, blob2dList, futureVec);
                 return std::make_pair(rc, std::move(futureVec));
             })

        .def("dev_subscribe",
             [](HeteroClient &client, const std::vector<std::string> &keys,
                const std::vector<DeviceBlobList> &blob2dList) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<Future> futureVec;
                 Status rc = client.DevSubscribe(keys, blob2dList, futureVec);
                 return std::make_pair(rc, std::move(futureVec));
             })

        .def("dev_mset",
             [](HeteroClient &client, const std::vector<std::string> &keys,
                const std::vector<DeviceBlobList> &blob2dList) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.DevMSet(keys, blob2dList, failedKeys);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("dev_mget",
             [](HeteroClient &client, const std::vector<std::string> &keys, std::vector<DeviceBlobList> &blob2dList,
                uint64_t subTimeoutMs) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.DevMGet(keys, blob2dList, failedKeys, subTimeoutMs);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("dev_delete",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<std::string> failedObjectKeys;
                 auto status = client.DevDelete(objectKeys, failedObjectKeys);
                 return std::make_pair(status, std::move(failedObjectKeys));
             })

        .def("async_dev_delete",
             [](HeteroClient &client, const std::vector<std::string> &objectIds) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::shared_future<AsyncResult> future = client.AsyncDevDelete(objectIds);
                 return AsyncResultFuture(std::move(future), Trace::Instance().GetTraceID());
             })

        .def("dev_local_delete",
             [](HeteroClient &client, const std::vector<std::string> &keys) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.DevLocalDelete(keys, failedKeys);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("generate_key",
             [](HeteroClient &client, const std::string &prefix) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::string key;
                 Status rc = client.GenerateKey(prefix, key);
                 return std::make_pair(rc, std::move(key));
             })

        .def("exist",
             [](HeteroClient &client, const std::vector<std::string> &keys) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<bool> exists;
                 Status rc = client.Exist(keys, exists);
                 return std::make_pair(rc, std::move(exists));
             })

        .def("batch_is_exist",
             [](HeteroClient &client, const std::vector<std::string> &keys) {
                 std::vector<bool> exists;
                 Status rc;
                 {
                     py::gil_scoped_release release;
                     TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                     rc = client.Exist(keys, exists);
                 }
                 py::list result(exists.size());
                 for (size_t i = 0; i < exists.size(); ++i) {
                     result[i] = py::int_(exists[i] ? 1 : 0);
                 }
                 return std::make_pair(std::move(rc), std::move(result));
             })

        .def("get_meta_info", [](HeteroClient &client, const std::vector<std::string> &keys, bool isDevKey) {
            py::gil_scoped_release release;
            TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
            std::vector<std::string> failedKeys;
            std::vector<MetaInfo> metaInfos;
            Status rc = client.GetMetaInfo(keys, isDevKey, metaInfos, failedKeys);
            return std::make_pair(rc, std::make_pair(std::move(failedKeys), std::move(metaInfos)));
        });
});

PybindDefineRegisterer g_pybind_define_f_Blob("Blob", PRIORITY_LOW, [](const py::module *m) {
    py::class_<Blob>(*m, "Blob")
        .def(py::init<>())
        .def_static("build",
                    [](uint64_t pointer, uint64_t size) {
                        return Blob{ reinterpret_cast<void *>(pointer), size };
                    })
        .def("get_dev_ptr", [](Blob &blob) { return reinterpret_cast<uint64_t>(blob.pointer); })
        .def("get_size", [](Blob &blob) { return blob.size; });
});

PybindDefineRegisterer g_pybind_define_f_DeviceBlobList("DeviceBlobList", PRIORITY_LOW, [](const py::module *m) {
    py::class_<DeviceBlobList>(*m, "DeviceBlobList")
        .def(py::init<>())
        .def_static("build",
                    [](std::vector<Blob> &blobs, int32_t deviceIdx, int32_t srcOffset) {
                        return DeviceBlobList{ blobs, deviceIdx, srcOffset };
                    })
        .def("get_dev_idx", [](DeviceBlobList &blobList) { return blobList.deviceIdx; })
        .def("get_blobs", [](DeviceBlobList &blobList) { return blobList.blobs; });
});

PybindDefineRegisterer g_pybind_define_f_MetaInfos("MetaInfo", PRIORITY_LOW, [](const py::module *m) {
    py::class_<MetaInfo>(*m, "MetaInfo")
        .def(pybind11::init<>())
        .def_readwrite("blob_size_list", &MetaInfo::blobSizeList);
});

PybindDefineRegisterer g_pybind_define_f_Tensor("Tensor", PRIORITY_LOW, [](const py::module *m) {
    using namespace pybind11::literals;

    py::class_<Tensor>(*m, "Tensor")
        .def(py::init<>([](uint64_t ptr, uint32_t elemSize, const std::vector<uint64_t> &shape) {
                 return Tensor{ .ptr = ptr, .elemSize = elemSize, .shape = shape };
             }),
             "ptr"_a, "elem_size"_a, "shape"_a)
        .def_readwrite("ptr", &Tensor::ptr)
        .def_readwrite("elem_size", &Tensor::elemSize)
        .def_readwrite("shape", &Tensor::shape)
        .def("__repr__",
             [](const Tensor &self) {
                 std::ostringstream oss;
                 oss << "Tensor[ptr:" << self.ptr << ", elem_size:" << self.elemSize << ", shape:";
                 for (auto dim : self.shape) {
                     oss << dim << ",";
                 }

                 return oss.str();
             })
        .def(
            "__eq__",
            [](const Tensor &lhs, const Tensor &rhs) {
                return lhs.ptr == rhs.ptr && lhs.elemSize == rhs.elemSize
                       && std::equal(lhs.shape.begin(), lhs.shape.end(), rhs.shape.begin());
            },
            "other"_a);
});

}  // namespace datasystem
