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

#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/client/object_cache/device/page_attn_utils.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/hetero_client.h"
#include "datasystem/hetero/future.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/pybind_api/pybind_register.h"

using datasystem::ConnectOptions;
namespace datasystem {
constexpr int32_t DEFAULT_FUTURE_GET_TIMEOUT_MS = 60000;

class AsyncResultFuture {
public:
    explicit AsyncResultFuture(std::shared_future<AsyncResult> future) : future_(std::move(future))
    {
    }

    py::object Get(uint64_t timeoutMs)
    {
        AsyncResult result;
        {
            if (timeoutMs > std::numeric_limits<int64_t>::max()) {
                throw std::runtime_error("timeoutMs out of scope");
            }
            // When C++ blocks waiting for a future, release the GIL to avoid blocking Python execution.
            py::gil_scoped_release release;
            auto status = future_.wait_for(std::chrono::milliseconds(timeoutMs));
            if (status == std::future_status::timeout) {
                py::gil_scoped_acquire acquire;
                throw std::runtime_error("Future get timeout");
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
};

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
        .def(py::init([](const std::string &host, int32_t port, int32_t connectTimeoutMs,
                         const std::string &clientPublicKey, const std::string &clientPrivateKey,
                         const std::string &serverPublicKey, const std::string &accessKey, const std::string &secretKey,
                         const std::string &tenantId, const bool enableCrossNodeConnection, int32_t reqTimeoutMs) {
            ConnectOptions connectOpts{
                .host = host,
                .port = port,
                .connectTimeoutMs = connectTimeoutMs,
                .requestTimeoutMs = reqTimeoutMs,
                .clientPublicKey = clientPublicKey,
                .clientPrivateKey = clientPrivateKey,
                .serverPublicKey = serverPublicKey,
                .accessKey = accessKey,
                .secretKey = secretKey,
                .tenantId = tenantId,
                .enableCrossNodeConnection = enableCrossNodeConnection,
            };
            return std::make_unique<HeteroClient>(connectOpts);
        }))
        .def("init",
             [](HeteroClient &client) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 return client.Init();
             })

        .def("delete",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<std::string> failedObjectKeys;
                 auto status = client.Delete(objectKeys, failedObjectKeys);
                 return std::make_pair(status, std::move(failedObjectKeys));
             })

        .def("mget_h2d",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys,
                const std::vector<DeviceBlobList> &devBlobList, uint64_t subTimeoutMs) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.MGetH2D(objectKeys, devBlobList, failedKeys, subTimeoutMs);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("mset_d2h",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys,
                const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 return client.MSetD2H(objectKeys, devBlobList, setParam);
             })

        .def("async_mset_d2h",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys,
                const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::shared_future<AsyncResult> future = client.AsyncMSetD2H(objectKeys, devBlobList, setParam);
                 return AsyncResultFuture(std::move(future));
             })

        .def("async_mget_h2d",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys,
                const std::vector<DeviceBlobList> &devBlobList, uint64_t subTimeoutMs) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::shared_future<AsyncResult> future = client.AsyncMGetH2D(objectKeys, devBlobList, subTimeoutMs);
                 return AsyncResultFuture(std::move(future));
             })

        .def("dev_publish",
             [](HeteroClient &client, const std::vector<std::string> &keys,
                const std::vector<DeviceBlobList> &blob2dList) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<Future> futureVec;
                 Status rc = client.DevPublish(keys, blob2dList, futureVec);
                 return std::make_pair(rc, std::move(futureVec));
             })

        .def("dev_subscribe",
             [](HeteroClient &client, const std::vector<std::string> &keys,
                const std::vector<DeviceBlobList> &blob2dList) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<Future> futureVec;
                 Status rc = client.DevSubscribe(keys, blob2dList, futureVec);
                 return std::make_pair(rc, std::move(futureVec));
             })

        .def("dev_mset",
             [](HeteroClient &client, const std::vector<std::string> &keys,
                const std::vector<DeviceBlobList> &blob2dList) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.DevMSet(keys, blob2dList, failedKeys);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("dev_mget",
             [](HeteroClient &client, const std::vector<std::string> &keys, std::vector<DeviceBlobList> &blob2dList,
                uint64_t subTimeoutMs) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.DevMGet(keys, blob2dList, failedKeys, subTimeoutMs);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("dev_delete",
             [](HeteroClient &client, const std::vector<std::string> &objectKeys) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<std::string> failedObjectKeys;
                 auto status = client.DevDelete(objectKeys, failedObjectKeys);
                 return std::make_pair(status, std::move(failedObjectKeys));
             })

        .def("async_dev_delete",
             [](HeteroClient &client, const std::vector<std::string> &objectIds) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::shared_future<AsyncResult> future = client.AsyncDevDelete(objectIds);
                 return AsyncResultFuture(std::move(future));
             })

        .def("dev_local_delete",
             [](HeteroClient &client, const std::vector<std::string> &keys) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<std::string> failedKeys;
                 auto status = client.DevLocalDelete(keys, failedKeys);
                 return std::make_pair(status, std::move(failedKeys));
             })

        .def("generate_key",
             [](HeteroClient &client, const std::string &prefix) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::string key;
                 Status rc = client.GenerateKey(prefix, key);
                 return std::make_pair(rc, std::move(key));
             })

        .def("exist",
             [](HeteroClient &client, const std::vector<std::string> &keys) {
                 py::gil_scoped_release release;
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<bool> exists;
                 Status rc = client.Exist(keys, exists);
                 return std::make_pair(rc, std::move(exists));
             })

        .def("get_meta_info", [](HeteroClient &client, const std::vector<std::string> &keys, bool isDevKey) {
            py::gil_scoped_release release;
            TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
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

PybindDefineRegisterer g_pybind_define_f_PageAttnUtils("PageAttnUtils", PRIORITY_LOW, [](const py::module *m) {
    using namespace pybind11::literals;

    py::class_<PageAttnUtils>(*m, "PageAttnUtils")
        .def_static("blk_2_blob", &PageAttnUtils::Blk2Blob, "ptr"_a, "elem_size"_a, "num_block_elem"_a, "block_id"_a)
        .def_static("blks_2_dev_blob_list", &PageAttnUtils::Blks2DevBlobList, "device_idx"_a, "ptr"_a, "elem_size"_a,
                    "num_block_elem"_a, "block_ids"_a)
        .def_static(
            "layerwise_dev_blob_lists",
            [](int32_t deviceIdx, const std::vector<Tensor> &layerTensors, const std::vector<uint32_t> &blockIds) {
                std::vector<DeviceBlobList> outDblList;
                auto status = PageAttnUtils::LayerwiseDevBlobLists(deviceIdx, layerTensors, blockIds, outDblList);
                return std::make_pair(status, std::move(outDblList));
            },
            "device_idx"_a, "layer_tensors"_a, "block_ids"_a)
        .def_static(
            "blockwise_dev_blob_lists",
            [](int32_t deviceIdx, const std::vector<Tensor> &layerTensors, const std::vector<uint32_t> &blockIds) {
                std::vector<DeviceBlobList> outDblList;
                auto status = PageAttnUtils::BlockwiseDevBlobLists(deviceIdx, layerTensors, blockIds, outDblList);
                return std::make_pair(status, std::move(outDblList));
            },
            "device_idx"_a, "layer_tensors"_a, "block_ids"_a);
});

}  // namespace datasystem
