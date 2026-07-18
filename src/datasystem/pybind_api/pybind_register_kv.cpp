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
#include <map>
#include <memory>

#include <pybind11/numpy.h>

#include "datasystem/kv_client.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/pybind_api/pybind_register.h"

using datasystem::ConnectOptions;
using datasystem::object_cache::ObjectClientImpl;
using datasystem::object_cache::FullParam;
namespace datasystem {
namespace {
constexpr char K_MSETTX_DEPRECATED_MSG[] = "MSetTx is a deprecated API and is no longer supported.";

ConnectOptions BuildKvConnectOptions(const std::string &host, int32_t port, int32_t connectTimeoutMs,
                                     const std::string &token, const std::string &clientPublicKey,
                                     const std::string &clientPrivateKey, const std::string &serverPublicKey,
                                     const std::string &accessKey, const std::string &secretKey,
                                     const std::string &tenantId, bool enableCrossNodeConnection,
                                     int32_t reqTimeoutMs, uint64_t fastTransportMemSize,
                                     std::shared_ptr<IServiceDiscovery> serviceDiscovery)
{
    return ConnectOptions{ .host = host,
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
                           .serviceDiscovery = serviceDiscovery,
                           .fastTransportMemSize = fastTransportMemSize };
}
}  // namespace

class StateValueBuffer;
struct MemoryView {
    const uint8_t *ptr;
    int64_t size;
};
class ReadOnlyMemoryViewBuffer {
public:
    explicit ReadOnlyMemoryViewBuffer(std::shared_ptr<StateValueBuffer> valueBuffer, MemoryView memoryView,
                                      bool withLatch)
        : valBuffer_(std::move(valueBuffer)), memoryView_(memoryView), withLatch_(withLatch)
    {
    }

    ~ReadOnlyMemoryViewBuffer();

    /**
     * @brief Get a immutable data pointer, used in pybind.
     * @return A const uint8_t * to the data.
     */
    [[nodiscard]] const uint8_t *ImmutableData() const;

    /**
     * @brief Get the data size of the buffer, used in pybind.
     * @return The data size of the buffer.
     */
    [[nodiscard]] int64_t GetSize() const;

private:
    std::shared_ptr<StateValueBuffer> valBuffer_;
    MemoryView memoryView_;
    bool withLatch_;
};

class StateValueBuffer : public std::enable_shared_from_this<StateValueBuffer> {
public:
    explicit StateValueBuffer(std::shared_ptr<Buffer> buffer) : buffer_(std::move(buffer))
    {
    }

    ~StateValueBuffer() = default;

    /**
     * @brief Get a read only memory view buffer, which relates to python memoryview.
     * @param[in] withLatch Whether acquiring the latch before buffer getting.
     * @param[in] timeoutSeconds Try-lock timeoutSeconds in seconds, default value is 60 seconds.
     * @return A read only memory view buffer.
     */
    std::shared_ptr<ReadOnlyMemoryViewBuffer> ImmutableData(bool withLatch = false,
                                                            uint64_t timeoutSeconds = 60 /* default is 60s */);

    /**
     * @brief A Read lock is executed on the memory to protect the memory from concurrent writes (allow concurrent
     * reads).
     * @param[in] timeoutSeconds Try-lock timeoutSeconds in seconds, default value is 60 seconds.
     * @return Status of the result.
     */
    Status RLatch(uint64_t timeoutSeconds = 60 /* default is 60s */);

    /**
     * @brief Unlock the read latch on memory.
     * @return Status of the result.
     */
    Status UnRLatch();

    /**
     * @brief Get the data size of the buffer, used in pybind.
     * @return The data size of the buffer.
     */
    [[nodiscard]] int64_t GetSize() const;

    std::shared_ptr<Buffer> GetBuffer() {return buffer_;}
    uint8_t* MutableData(){ return static_cast<uint8_t*>(const_cast<void*>(buffer_->ImmutableData())); }

private:
    std::shared_ptr<Buffer> buffer_;
};

ReadOnlyMemoryViewBuffer::~ReadOnlyMemoryViewBuffer()
{
    if (withLatch_) {
        // UnLatch Error only on worker crash, buffer becoming deprecated, no need to unlatch anymore.
        (void)valBuffer_->UnRLatch();
    }
}

const uint8_t *ReadOnlyMemoryViewBuffer::ImmutableData() const
{
    return memoryView_.ptr;
}

int64_t ReadOnlyMemoryViewBuffer::GetSize() const
{
    return memoryView_.size;
}

std::shared_ptr<ReadOnlyMemoryViewBuffer> StateValueBuffer::ImmutableData(bool withLatch, uint64_t timeoutSeconds)
{
    if (withLatch) {
        auto status = buffer_->RLatch(timeoutSeconds);
        if (status.IsError()) {
            return nullptr;
        }
    }
    return std::make_shared<ReadOnlyMemoryViewBuffer>(
        shared_from_this(),
        MemoryView{ .ptr = static_cast<const uint8_t *>(buffer_->ImmutableData()), .size = buffer_->GetSize() },
        withLatch);
}

Status StateValueBuffer::RLatch(uint64_t timeoutSeconds)
{
    return buffer_->RLatch(timeoutSeconds);
}

Status StateValueBuffer::UnRLatch()
{
    return buffer_->UnRLatch();
}

int64_t StateValueBuffer::GetSize() const
{
    return buffer_->GetSize();
}

PybindDefineRegisterer g_pybind_define_f_StateValueBuffer(
    "StateValueBuffer", PRIORITY_LOW, ([](const py::module *m) {
        (void)py::class_<StateValueBuffer, std::shared_ptr<StateValueBuffer>>(*m, "StateValueBuffer")
            .def("ImmutableData",
                 [](StateValueBuffer &stateValBuffer, bool withLatch = false, uint64_t timeoutSeconds = 60) {
                     return stateValBuffer.ImmutableData(withLatch, timeoutSeconds);
                 })
            .def("RLatch", [](StateValueBuffer &stateValBuffer,
                             uint64_t timeoutSeconds = 60) { return stateValBuffer.RLatch(timeoutSeconds); })
            .def("UnRLatch", [](StateValueBuffer &stateValBuffer) { return stateValBuffer.UnRLatch(); })
            .def("MutableData", [](StateValueBuffer &stateValBuffer) {
                auto ptr = stateValBuffer.MutableData();
                auto size = stateValBuffer.GetSize();
                return py::memoryview::from_memory(ptr, size, false);
            })
            .def("GetSize", [](StateValueBuffer &stateValBuffer) { return stateValBuffer.GetSize(); });
    }));

PybindDefineRegisterer g_pybind_define_f_ReadOnlyMemoryViewBuffer(
    "ReadOnlyMemoryViewBuffer", PRIORITY_LOW, ([](const py::module *m) {
        (void)py::class_<ReadOnlyMemoryViewBuffer, std::shared_ptr<ReadOnlyMemoryViewBuffer>>(
            *m, "ReadOnlyMemoryViewBuffer", pybind11::buffer_protocol())
            .def_buffer([](ReadOnlyMemoryViewBuffer &memViewBuffer) {
                return py::buffer_info(memViewBuffer.ImmutableData(), memViewBuffer.GetSize(), true);
            })
            .def("GetSize", [](ReadOnlyMemoryViewBuffer &memViewBuffer) { return memViewBuffer.GetSize(); });
    }));

PybindDefineRegisterer g_pybind_define_f_KVClient("KVClient", PRIORITY_LOW, [](const py::module *m) {
    py::enum_<ExistenceOpt>(*m, "ExistenceOpt")
        .value("NONE", ExistenceOpt::NONE)
        .value("NX", ExistenceOpt::NX)
        .export_values();

    py::class_<ObjectClientImpl, std::shared_ptr<ObjectClientImpl>>(*m, "KVClient")
        .def(py::init([](const std::string &host, int32_t port, int32_t connectTimeoutMs, const std::string &token,
                         const std::string &clientPublicKey, const std::string &clientPrivateKey,
                         const std::string &serverPublicKey, const std::string &accessKey, const std::string &secretKey,
                         const std::string &tenantId, const bool enableCrossNodeConnection, int32_t reqTimeoutMs,
                         uint64_t fastTransportMemSize) {
            auto connectOpts =
                BuildKvConnectOptions(host, port, connectTimeoutMs, token, clientPublicKey, clientPrivateKey,
                                      serverPublicKey, accessKey, secretKey, tenantId, enableCrossNodeConnection,
                                      reqTimeoutMs, fastTransportMemSize, nullptr);
            return std::make_unique<ObjectClientImpl>(connectOpts);
        }))
        .def(py::init([](const std::string &host, int32_t port, int32_t connectTimeoutMs, const std::string &token,
                         const std::string &clientPublicKey, const std::string &clientPrivateKey,
                         const std::string &serverPublicKey, const std::string &accessKey, const std::string &secretKey,
                         const std::string &tenantId, const bool enableCrossNodeConnection, int32_t reqTimeoutMs,
                         uint64_t fastTransportMemSize, std::shared_ptr<IServiceDiscovery> serviceDiscovery) {
            auto connectOpts =
                BuildKvConnectOptions(host, port, connectTimeoutMs, token, clientPublicKey, clientPrivateKey,
                                      serverPublicKey, accessKey, secretKey, tenantId, enableCrossNodeConnection,
                                      reqTimeoutMs, fastTransportMemSize, serviceDiscovery);
            return std::make_unique<ObjectClientImpl>(connectOpts);
        }))
        .def("Init",
             [](ObjectClientImpl &client) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                bool needRollbackState;
                auto rc = client.Init(needRollbackState, true);
                client.CompleteHandler(rc.IsError(), needRollbackState);
                return rc;
             })
        .def("Set",
             [](ObjectClientImpl &client, const std::string &key, const py::buffer &val, WriteMode writeMode,
                uint32_t ttlSecond) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                py::buffer_info info(val.request());
                StringView strView(reinterpret_cast<const char *>(info.ptr), info.size);
                SetParam param{ .writeMode = writeMode, .ttlSecond = ttlSecond };
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
                Status rc = client.Set(key, strView, param);
                access.ObjectKeyRef(key)
                    .WriteModeProvider([writeMode] { return static_cast<int>(writeMode); })
                    .TtlSecondProvider([ttlSecond] { return ttlSecond; })
                    .Result(rc)
                    .DataSize(info.size)
                    .Record();
                return rc;
             })
        .def("SetValue",
             [](ObjectClientImpl &client, const py::buffer &val, WriteMode writeMode, uint32_t ttlSecond) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                py::buffer_info info(val.request());
                StringView strView(reinterpret_cast<const char *>(info.ptr), info.size);
                SetParam param{ .writeMode = writeMode, .ttlSecond = ttlSecond };
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
                std::string key;
                auto rc = client.Set(strView, param, key);
                access.ObjectKeyRef(key)
                    .WriteModeProvider([writeMode] { return static_cast<int>(writeMode); })
                    .TtlSecondProvider([ttlSecond] { return ttlSecond; })
                    .Result(rc)
                    .DataSize(info.size)
                    .Record();
                return key;
             })
        .def("MSet",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys, const std::vector<py::buffer> &vals,
                WriteMode writeMode, uint32_t ttlSecond, ExistenceOpt existenceOpt) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<StringView> values;
                MSetParam param{ .writeMode = writeMode, .ttlSecond = ttlSecond, .existence = existenceOpt };
                uint64_t totalSize = 0;
                for (const auto &val : vals) {
                    py::buffer_info info(val.request());
                    totalSize += info.size;
                    StringView strView(reinterpret_cast<const char *>(info.ptr), info.size);
                    values.emplace_back(strView);
                }
                std::vector<std::string> outFailedKeys;
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_MSETNX);
                auto rc = client.MSet(keys, values, param, outFailedKeys);
                access.ObjectKeysRef(keys)
                    .WriteModeProvider([writeMode] { return static_cast<int>(writeMode); })
                    .TtlSecondProvider([ttlSecond] { return ttlSecond; })
                    .Result(rc)
                    .DataSize(totalSize)
                    .Record();
                return std::make_pair(rc, std::move(outFailedKeys));
             })
        .def("MSetTx",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys, const std::vector<py::buffer> &vals,
                WriteMode writeMode, uint32_t ttlSecond, ExistenceOpt existenceOpt) {
                (void)client;
                (void)keys;
                (void)vals;
                (void)writeMode;
                (void)ttlSecond;
                (void)existenceOpt;
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                return Status(K_RUNTIME_ERROR, K_MSETTX_DEPRECATED_MSG);
             })
        .def("MCreate",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys,
                const std::vector<uint64_t> &sizes, WriteMode writeMode, uint32_t ttlSecond) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                FullParam param;
                param.writeMode = writeMode;
                param.ttlSecond = ttlSecond;

                std::vector<std::shared_ptr<Buffer>> buffers;
                uint64_t totalSize = 0;
                for (uint64_t size : sizes) {
                    totalSize += size;
                }
                auto status = client.MCreate(keys, sizes, param, buffers);
                if (status.IsError()) {
                    LOG(ERROR) << "MCreate failed:" << status.ToString();
                }

                py::list pyBuffers;
                if (status.IsOk()) {
                    for (auto &buf : buffers) {
                        pyBuffers.append(std::make_shared<StateValueBuffer>(std::move(buf)));
                    }
                }
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_MCREATE);
                access.ObjectKeysRef(keys)
                    .WriteModeProvider([writeMode] { return static_cast<int>(writeMode); })
                    .TtlSecondProvider([ttlSecond] { return ttlSecond; })
                    .Result(status)
                    .DataSize(totalSize)
                    .Record();
                return std::make_pair(status, std::move(pyBuffers));
             })
        .def("MSetBuffer",
             [](ObjectClientImpl &client, const std::vector<std::shared_ptr<StateValueBuffer>> &sv_buffers) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<std::shared_ptr<Buffer>> buffers;
                for (const auto &svb : sv_buffers) {
                    if (svb) {
                        buffers.push_back(svb->GetBuffer());
                    }
                }
                auto status = client.MSet(buffers);
                return status;
             })
        .def("MGetBuffer",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys, uint32_t timeout_ms) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<Optional<Buffer>> buffers;
                py::list vals;
                uint64_t totalSize = 0;
                Status lastRc;
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_GET);
                Raii raii([&access, &totalSize, &lastRc, &keys, timeout_ms] {
                    Status accessRc = (lastRc.GetCode() == K_NOT_FOUND) ? Status::OK() : lastRc;
                    access.ObjectKeysRef(keys).TimeoutMs(timeout_ms).Result(accessRc).DataSize(totalSize).Record();
                });

                lastRc = client.Get(keys, timeout_ms, buffers);
                if (lastRc.IsError()) {
                    return std::make_pair(lastRc, std::move(vals));
                }

                for (auto &optBuf : buffers) {
                    if (!optBuf) {
                        vals.append(py::none());
                        continue;
                    }

                    auto svb = std::make_shared<StateValueBuffer>(
                        std::make_shared<Buffer>(std::move(optBuf.value()))
                    );
                    auto view = svb->ImmutableData(true);
                    if (view) {
                        totalSize += svb->GetSize();
                        vals.append(view);
                    } else {
                        vals.append(py::none());
                    }
                }
                return std::make_pair(lastRc, std::move(vals));
             })
        .def("GetReadOnlyBuffers",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys, uint32_t timeout_ms) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<Optional<Buffer>> buffers;
                py::list pyList;
                uint64_t totalSize = 0;
                Status lastRc;
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_GET);
                Raii raii([&access, &totalSize, &lastRc, &keys, timeout_ms] {
                    Status accessRc = (lastRc.GetCode() == K_NOT_FOUND) ? Status::OK() : lastRc;
                    access.ObjectKeysRef(keys).TimeoutMs(timeout_ms).Result(accessRc).DataSize(totalSize).Record();
                });
                Status rc = client.Get(keys, timeout_ms, buffers);
                lastRc = rc;
                if (rc.IsError()) {
                    return std::make_pair(rc, std::move(pyList));
                }

                for (auto &buffer : buffers) {
                    if (!buffer) {
                        pyList.append(py::none());
                        continue;
                    }
                    std::shared_ptr<StateValueBuffer> stateValueBuffer =
                        std::make_shared<StateValueBuffer>(std::make_shared<Buffer>(std::move(buffer.value())));

                    pyList.append(py::cast(stateValueBuffer));
                    totalSize += stateValueBuffer->GetSize();
                }
                return std::make_pair(rc, std::move(pyList));
             })
        .def("Get",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys, uint32_t timeout_ms) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<Optional<Buffer>> buffers;
                py::list vals;
                uint64_t totalSize = 0;
                Status lastRc;
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_GET);
                Raii raii([&access, &totalSize, &lastRc, &keys, timeout_ms] {
                    Status accessRc = (lastRc.GetCode() == K_NOT_FOUND) ? Status::OK() : lastRc;
                    access.ObjectKeysRef(keys).TimeoutMs(timeout_ms).Result(accessRc).DataSize(totalSize).Record();
                });
                Status rc = client.Get(keys, timeout_ms, buffers);
                lastRc = rc;
                if (rc.IsError()) {
                    return std::make_pair(rc, std::move(vals));
                }
                for (auto &buffer : buffers) {
                    if (!buffer) {
                        vals.append(py::none());
                        continue;
                    }
                    // Use the SDK-internal helper so the copy still works when the buffer was
                    // returned with oc_metadata_header disabled (DisabledLock → no latch
                    // needed); other latch errors still surface as None.
                    std::string tmpData;
                    Buffer *buf = &(*buffer);
                    Status status = buf->CopyDataWithRLatch([&tmpData, buf] {
                        tmpData.assign(reinterpret_cast<const char *>(buf->ImmutableData()), buf->GetSize());
                        return Status::OK();
                    });
                    if (status.IsOk()) {
                        totalSize += tmpData.size();
                        vals.append(py::bytes(tmpData));
                    } else {
                        LOG(ERROR) << "CopyDataWithRLatch failed:" << status.ToString();
                        vals.append(py::none());
                    }
                }
                return std::make_pair(rc, std::move(vals));
             })
        .def("ReadSpecifyOffsetData",
             [](ObjectClientImpl &client, const std::vector<ReadParam> &readParams) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<Optional<Buffer>> buffers;
                py::list vals;
                uint64_t totalSize = 0;
                Status lastRc;

                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_GET);
                access.ObjectKeyProvider([&readParams] {
                    std::vector<std::string> keys;
                    keys.reserve(readParams.size());
                    for (const auto &readParam : readParams) {
                        keys.emplace_back(readParam.key);
                    }
                    return objectKeysToString(keys);
                });
                Raii raii([&access, &totalSize, &lastRc] {
                    Status accessRc = (lastRc.GetCode() == K_NOT_FOUND) ? Status::OK() : lastRc;
                    access.Result(accessRc).DataSize(totalSize).Record();
                });
                lastRc = client.Read(readParams, buffers);
                if (lastRc.IsError()) {
                    return std::make_pair(lastRc, std::move(vals));
                }

                for (auto &buffer : buffers) {
                    if (!buffer) {
                        vals.append(py::none());
                        continue;
                    }
                    std::shared_ptr<StateValueBuffer> stateValueBuffer =
                        std::make_shared<StateValueBuffer>(std::make_shared<Buffer>(std::move(buffer.value())));

                    vals.append(py::cast(stateValueBuffer));
                    totalSize += stateValueBuffer->GetSize();
                }
                return std::make_pair(lastRc, std::move(vals));
             })
        .def("Del",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<std::string> failedKeys;
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_DELETE);
                auto status = client.Delete(keys, failedKeys);
                access.ObjectKeysRef(keys).Result(status).Record();
                return std::make_pair(status, std::move(failedKeys));
             })
        .def("generate_key",
             [](ObjectClientImpl &client, const std::string &prefix) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::string key;
                Status rc = client.GenerateKey(key, prefix);
                return std::make_pair(rc, std::move(key));
             })
        .def("exist",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<bool> exists;
                auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_EXIST);
                Status rc = client.Exist(keys, exists, true, false);
                access.ObjectKeysRef(keys).Result(rc).Record();
                return std::make_pair(rc, std::move(exists));
             })
        .def("expire",
             [](ObjectClientImpl &client, const std::vector<std::string> &keys, uint32_t ttlSecond) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                std::vector<std::string> failedKeys;
                Status rc = client.Expire(keys, ttlSecond, failedKeys);
                return std::make_pair(rc, std::move(failedKeys));
             })
        .def("HealthCheck",
             [](ObjectClientImpl &client) {
                TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                ServerState state;
                Status healthState = client.HealthCheck(state);
                return healthState;
            });
});

PybindDefineRegisterer g_pybind_define_f_ReadParam("ReadParam", PRIORITY_LOW, [](const py::module *m) {
    py::class_<ReadParam>(*m, "ReadParam")
        .def(py::init<>())
        .def_static("build",
                    [](const std::string &key, uint64_t offset, uint64_t size) {
                        return ReadParam{ key, offset, size };
                    })
        .def("get_key", [](ReadParam &readParam) { return readParam.key; })
        .def("get_offset", [](ReadParam &readParam) { return readParam.offset; })
        .def("get_size", [](ReadParam &readParam) { return readParam.size; });
});

PybindDefineRegisterer g_pybind_define_f_SetParam("SetParam", PRIORITY_LOW, [](const py::module *m) {
    py::class_<SetParam>(*m, "SetParam")
        .def(py::init<>())
        .def_readwrite("write_mode", &SetParam::writeMode)
        .def_readwrite("ttl_second", &SetParam::ttlSecond)
        .def_readwrite("existence", &SetParam::existence)
        .def_readwrite("cache_type", &SetParam::cacheType);
});
}  // namespace datasystem
