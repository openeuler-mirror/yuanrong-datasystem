/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
#include <exception>
#include <future>
#include <map>
#include <memory>
#include <stdexcept>
#include <vector>

#include <pybind11/detail/common.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"
#include "datasystem/object_cache/buffer.h"
#include "datasystem/object_cache/object_client.h"
#include "datasystem/object_cache/object_enum.h"
#include "datasystem/pybind_api/pybind_register.h"
#include "datasystem/utils/status.h"

using datasystem::Buffer;
using datasystem::ConnectOptions;
using datasystem::ConsistencyType;
using datasystem::CreateParam;
using datasystem::ObjectClient;
using datasystem::WriteMode;
namespace datasystem {
class ReadOnlyBuffer {
public:
    explicit ReadOnlyBuffer(std::shared_ptr<Buffer> buffer) : buffer_(std::move(buffer))
    {
    }

    ~ReadOnlyBuffer() = default;

    const uint8_t *ImmutableData()
    {
        return static_cast<const uint8_t *>(buffer_->ImmutableData());
    }

    int64_t GetSize() const
    {
        return buffer_->GetSize();
    }

private:
    std::shared_ptr<Buffer> buffer_;
};


PybindDefineRegisterer g_pybind_define_f_Client("ObjectClient", PRIORITY_LOW, [](const py::module *m) {
    py::enum_<WriteMode>(*m, "WriteMode")
        .value("NONE_L2_CACHE", WriteMode::NONE_L2_CACHE)
        .value("WRITE_THROUGH_L2_CACHE", WriteMode::WRITE_THROUGH_L2_CACHE)
        .value("WRITE_BACK_L2_CACHE", WriteMode::WRITE_BACK_L2_CACHE)
        .value("NONE_L2_CACHE_EVICT", WriteMode::NONE_L2_CACHE_EVICT)
        .export_values();

    py::enum_<ConsistencyType>(*m, "ConsistencyType")
        .value("PRAM", ConsistencyType::PRAM)
        .value("CAUSAL", ConsistencyType::CAUSAL)
        .export_values();

    py::enum_<CacheType>(*m, "CacheType")
        .value("MEMORY", CacheType::MEMORY)
        .value("DISK", CacheType::DISK)
        .export_values();

    py::enum_<DataType>(*m, "DataType")
        .value("DATA_TYPE_INT8", DataType::DATA_TYPE_INT8)
        .value("DATA_TYPE_INT16", DataType::DATA_TYPE_INT16)
        .value("DATA_TYPE_INT32", DataType::DATA_TYPE_INT32)
        .value("DATA_TYPE_FP16", DataType::DATA_TYPE_FP16)
        .value("DATA_TYPE_FP32", DataType::DATA_TYPE_FP32)
        .value("DATA_TYPE_INT64", DataType::DATA_TYPE_INT64)
        .value("DATA_TYPE_UINT64", DataType::DATA_TYPE_UINT64)
        .value("DATA_TYPE_UINT8", DataType::DATA_TYPE_UINT8)
        .value("DATA_TYPE_UINT16", DataType::DATA_TYPE_UINT16)
        .value("DATA_TYPE_UINT32", DataType::DATA_TYPE_UINT32)
        .value("DATA_TYPE_FP64", DataType::DATA_TYPE_FP64)
        .value("DATA_TYPE_BFP16", DataType::DATA_TYPE_BFP16)
        .value("DATA_TYPE_RESERVED", DataType::DATA_TYPE_RESERVED)
        .export_values();

    enum class DataType : uint8_t {
        DATA_TYPE_INT8 = 0,   /**< int8 */
        DATA_TYPE_INT16 = 1,  /**< int16 */
        DATA_TYPE_INT32 = 2,  /**< int32 */
        DATA_TYPE_FP16 = 3,   /**< fp16 */
        DATA_TYPE_FP32 = 4,   /**< fp32 */
        DATA_TYPE_INT64 = 5,  /**< int64 */
        DATA_TYPE_UINT64 = 6, /**< uint64 */
        DATA_TYPE_UINT8 = 7,  /**< uint8 */
        DATA_TYPE_UINT16 = 8, /**< uint16 */
        DATA_TYPE_UINT32 = 9, /**< uint32 */
        DATA_TYPE_FP64 = 10,  /**< fp64 */
        DATA_TYPE_BFP16 = 11, /**< bfp16 */
        DATA_TYPE_RESERVED    /**< reserved */
    };

    py::enum_<LifetimeType>(*m, "LifetimeType")
        .value("REFERENCE", LifetimeType::REFERENCE)
        .value("MOVE", LifetimeType::MOVE)
        .export_values();

    py::class_<ObjectClient, std::shared_ptr<ObjectClient>>(*m, "ObjectClient")
        .def(py::init([](const std::string &host, int32_t port, int32_t connectTimeoutMs,
                         const std::string &clientPublicKey, const std::string &clientPrivateKey,
                         const std::string &serverPublicKey, const std::string &accessKey, const std::string &secretKey,
                         const std::string &tenantId, const bool enableCrossNodeConnection) {
            ConnectOptions connectOpts{ .host = host,
                                        .port = port,
                                        .connectTimeoutMs = connectTimeoutMs,
                                        .clientPublicKey = clientPublicKey,
                                        .clientPrivateKey = clientPrivateKey,
                                        .serverPublicKey = serverPublicKey,
                                        .accessKey = accessKey,
                                        .secretKey = secretKey,
                                        .tenantId = tenantId,
                                        .enableCrossNodeConnection = enableCrossNodeConnection };
            return std::make_unique<ObjectClient>(connectOpts);
        }))

        .def("init",
             [](ObjectClient &client) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 return client.Init();
             })

        .def("create",
             [](ObjectClient &client, const std::string &objectKey, uint64_t size, WriteMode writeMode,
                ConsistencyType consistency) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::shared_ptr<Buffer> buffer;
                 CreateParam param{ .writeMode = writeMode, .consistencyType = consistency };
                 datasystem::Status status = client.Create(objectKey, size, param, buffer);
                 return std::make_pair(status, std::move(buffer));
             })

        .def("put",
             [](ObjectClient &client, const std::string &objectKey, py::buffer value, WriteMode writeMode,
                ConsistencyType consistency, const std::vector<std::string> &refIds) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 CreateParam param{ .writeMode = writeMode, .consistencyType = consistency };
                 py::buffer_info info(value.request());
                 std::unordered_set<std::string> nestedObjectKeys = { refIds.begin(), refIds.end() };
                 return client.Put(objectKey, reinterpret_cast<const uint8_t *>(info.ptr), info.size, param,
                                   nestedObjectKeys);
             })

        .def("get",
             [](ObjectClient &client, const std::vector<std::string> &objectKeys, int64_t timeoutMs) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<Optional<Buffer>> buffers;
                 Status rc = client.Get(objectKeys, timeoutMs, buffers);
                 py::list rspBuffers;
                 for (Optional<Buffer> &buffer : buffers) {
                     if (buffer) {
                         rspBuffers.append(std::move(*buffer));
                     } else {
                         rspBuffers.append(py::none());
                     }
                 }
                 return std::make_pair(rc, std::move(rspBuffers));
             })

        .def("g_increase_ref",
             [](ObjectClient &client, const std::vector<std::string> &objectKeys) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<std::string> failedObjectKeys;
                 Status rc = client.GIncreaseRef(objectKeys, failedObjectKeys);
                 return std::make_pair(rc, std::move(failedObjectKeys));
             })

        .def("g_decrease_ref",
             [](ObjectClient &client, const std::vector<std::string> &objectKeys) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::vector<std::string> failedObjectKeys;
                 Status rc = client.GDecreaseRef(objectKeys, failedObjectKeys);
                 return std::make_pair(rc, std::move(failedObjectKeys));
             })

        .def("query_global_ref_num",
             [](ObjectClient &client, const std::string &objectKey) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 return client.QueryGlobalRefNum(objectKey);
             })

        .def("generate_object_id", [](ObjectClient &client, const std::string &prefix) {
            TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
            std::string objectKey;
            Status rc = client.GenerateObjectKey(prefix, objectKey);
            return std::make_pair(rc, std::move(objectKey));
        });
});

PybindDefineRegisterer g_pybind_define_f_Buffer("Buffer", PRIORITY_LOW, [](const py::module *m) {
    py::class_<Buffer, std::shared_ptr<Buffer>>(*m, "Buffer")
        .def("wlatch",
             [](Buffer &buffer, int64_t timeoutSec) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 auto rc = buffer.WLatch(timeoutSec);
                 if (rc.IsError()) {
                     LOG(ERROR) << "Acquire write latch error: " << rc.GetMsg();
                 }
                 return rc;
             })

        .def("rlatch",
             [](Buffer &buffer, int64_t timeoutSec) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 auto rc = buffer.RLatch(timeoutSec);
                 if (rc.IsError()) {
                     LOG(ERROR) << "Acquire read latch error: " << rc.GetMsg();
                 }
                 return rc;
             })

        .def("unwlatch",
             [](Buffer &buffer) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 auto rc = buffer.UnWLatch();
                 if (rc.IsError()) {
                     LOG(ERROR) << "Release write latch error: " << rc.GetMsg();
                 }
                 return rc;
             })

        .def("unrlatch",
             [](Buffer &buffer) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 auto rc = buffer.UnRLatch();
                 if (rc.IsError()) {
                     LOG(ERROR) << "Release read latch error: " << rc.GetMsg();
                 }
                 return rc;
             })

        .def("mutable_data",
             [](Buffer &buffer) { return py::memoryview::from_memory(buffer.MutableData(), buffer.GetSize()); })

        .def("immutable_data",
             [](Buffer &buffer) { return std::make_shared<ReadOnlyBuffer>(buffer.shared_from_this()); })

        .def("memory_copy",
             [](Buffer &buffer, py::buffer value) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 py::buffer_info info(value.request());
                 return buffer.MemoryCopy(info.ptr, info.size);
             })

        .def("publish",
             [](Buffer &buffer, const std::vector<std::string> &refIds) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::unordered_set<std::string> nestedObjectKeys = { refIds.begin(), refIds.end() };
                 return buffer.Publish(nestedObjectKeys);
             })

        .def("seal",
             [](Buffer &buffer, const std::vector<std::string> &refIds) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 std::unordered_set<std::string> nestedObjectKeys = { refIds.begin(), refIds.end() };
                 return buffer.Seal(nestedObjectKeys);
             })

        .def("invalidate_buffer",
             [](Buffer &buffer) {
                 TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                 return buffer.InvalidateBuffer();
             })

        .def("is_empty", [](Buffer &buffer) { return buffer.GetSize() == 0; })

        .def("get_size", [](Buffer &buffer) { return buffer.GetSize(); });
});

PybindDefineRegisterer g_pybind_define_f_ReadOnlyBuffer(
    "ReadOnlyBuffer", PRIORITY_LOW, ([](const py::module *m) {
        (void)py::class_<ReadOnlyBuffer, std::shared_ptr<ReadOnlyBuffer>>(*m, "ReadOnlyBuffer",
                                                                          pybind11::buffer_protocol())
            .def_buffer(
            [](ReadOnlyBuffer &buffer) { return py::buffer_info(buffer.ImmutableData(), buffer.GetSize(), true); });
    }));
}  // namespace datasystem
