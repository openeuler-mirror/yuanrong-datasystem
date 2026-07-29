/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/** Description: Shared-memory transporter for same-host workers. Uses RPC for control plane
 * (Create/Publish/GetObjectRemote). For Set/Get, when fd-passing dependencies (workerApi_ +
 * mmapManager_) are available and the worker reports a store_fd, Create does GetClientFd +
 * LookupUnitsAndMmapFd to obtain a real shm pointer for zero-copy read/write; otherwise it
 * falls back to transmitting data inline through the RPC payload (tagged AccessTransportKind::SHM
 * for routing/observability). BatchGet/MCreate use single batch RPCs (InvokeBatchGetObject /
 * InvokeMultiCreate). */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H
#define DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H

#include <algorithm>
#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/client_worker_common_api.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/metrics/kv_metrics.h"

namespace datasystem {
namespace client {
class ShmTransporter : public IDataTransporter {
    static constexpr int SHM_FALLBACK_LOG_RATE = 100;
public:
    explicit ShmTransporter(std::shared_ptr<WorkerRpcClient> rpcClient,
                           std::shared_ptr<IClientWorkerCommonApi> workerApi = nullptr,
                           std::shared_ptr<MmapManager> mmapManager = nullptr)
        : rpcClient_(std::move(rpcClient)), workerApi_(std::move(workerApi)), mmapManager_(std::move(mmapManager)) {}
    ~ShmTransporter() override = default;

    AccessTransportKind Kind() const override
    {
        return AccessTransportKind::SHM;
    }

    bool IsAlive() const override
    {
        return rpcClient_ != nullptr && rpcClient_->IsAlive();
    }

    Status Get(const DataGetRequest &input, DataGetResult &output) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
        output = DataGetResult{};
        GetObjectRemoteReqPb request;
        request.set_object_key(input.objectKey);
        request.set_data_size(input.expectedSize);
        request.set_try_lock(true);
        RETURN_IF_NOT_OK(rpcClient_->InvokeGetObject(request, output.response, output.rpcPayloads));
        Status responseStatus(static_cast<StatusCode>(output.response.error().error_code()),
                              output.response.error().error_msg());
        if (responseStatus.IsError()) {
            output.rpcPayloads.clear();
            return responseStatus;
        }
        CHECK_FAIL_RETURN_STATUS(output.response.data_source() == DataTransferSource::DATA_IN_PAYLOAD,
                                 K_RUNTIME_ERROR, "SHM GetObjectRemote returned an invalid data source");
        output.kind = AccessTransportKind::SHM;
        return Status::OK();
    }

    Status BatchGet(const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) override
    {
        outputs.clear();
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        CHECK_FAIL_RETURN_STATUS(!inputs.empty(), K_INVALID, "Batch get request is empty");
        if (inputs.size() == 1) {
            return BatchGetOne(inputs.front(), outputs);
        }

        BatchGetObjectRemoteReqPb request;
        for (const auto &input : inputs) {
            CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
            auto *itemRequest = request.add_requests();
            itemRequest->set_object_key(input.objectKey);
            itemRequest->set_data_size(input.expectedSize);
            itemRequest->set_try_lock(true);
        }
        BatchGetObjectRemoteRspPb response;
        std::vector<RpcMessage> payloads;
        RETURN_IF_NOT_OK(rpcClient_->InvokeBatchGetObject(request, response, payloads));
        CHECK_FAIL_RETURN_STATUS(response.responses_size() == static_cast<int>(inputs.size()), K_RUNTIME_ERROR,
                                 "BatchGetObjectRemote response count does not match request count");

        size_t expectedPayloadCount = 0;
        for (const auto &itemResponse : response.responses()) {
            Status itemStatus(static_cast<StatusCode>(itemResponse.error().error_code()),
                              itemResponse.error().error_msg());
            if (!itemStatus.IsOk()) {
                continue;
            }
            CHECK_FAIL_RETURN_STATUS(itemResponse.data_source() == DataTransferSource::DATA_IN_PAYLOAD,
                                     K_RUNTIME_ERROR, "SHM BatchGetObjectRemote returned an invalid data source");
            ++expectedPayloadCount;
        }
        CHECK_FAIL_RETURN_STATUS(payloads.size() == expectedPayloadCount, K_RUNTIME_ERROR,
                                 "BatchGetObjectRemote payload count does not match successful responses");

        outputs.reserve(inputs.size());
        size_t payloadIndex = 0;
        for (const auto &itemResponse : response.responses()) {
            DataGetItemResult item;
            item.status = Status(static_cast<StatusCode>(itemResponse.error().error_code()),
                                 itemResponse.error().error_msg());
            item.data.response = itemResponse;
            item.data.kind = AccessTransportKind::SHM;
            if (item.status.IsOk()) {
                item.data.rpcPayloads.emplace_back(std::move(payloads[payloadIndex++]));
            }
            outputs.emplace_back(std::move(item));
        }
        return Status::OK();
    }

    // Single-element BatchGet: delegates to Get and propagates a transport-level failure
    // (error_code==K_OK means no worker business error, i.e. the RPC itself failed) instead of
    // swallowing it into item.status — aligned with TcpTransporter (review 180849217).
    Status BatchGetOne(const DataGetRequest &input, DataGetBatchResult &outputs)
    {
        DataGetItemResult item;
        Status status = Get(input, item.data);
        // Transport-level failure (error_code==K_OK means no worker business error): return early
        // without emplacing, aligned with TcpTransporter::BatchGet size==1 path (review 181252346).
        if (status.IsError()
            && static_cast<StatusCode>(item.data.response.error().error_code()) == K_OK) {
            return status;
        }
        item.status = status;
        outputs.emplace_back(std::move(item));
        return Status::OK();
    }

    // Try to mmap the shm region for a Create response (shared by Create and MCreate).
    // On success sets info->pointer (base+offset), info->metadataSize, info->mmapEntry + AssociateShmId.
    // On failure (or no shm deps) leaves info as-is (nullptr → payload inline fallback).
    void TryMmapBuffer(const CreateRspPb &rsp, uint64_t size, ObjectBufferInfo &info)
    {
        if (workerApi_ == nullptr || mmapManager_ == nullptr || rsp.store_fd() <= 0 || !workerApi_->IsShmEnable()) {
            return;
        }
        auto shmBuf = std::make_shared<ShmUnitInfo>();
        shmBuf->fd = static_cast<int>(rsp.store_fd());
        shmBuf->mmapSize = rsp.mmap_size();
        shmBuf->offset = static_cast<ptrdiff_t>(rsp.offset());
        shmBuf->size = size;
        shmBuf->id = info.shmId;
        Status mmapRc = mmapManager_->LookupUnitsAndMmapFd("", shmBuf);
        if (!mmapRc.IsOk()) {
            LOG_EVERY_N(WARNING, SHM_FALLBACK_LOG_RATE) << "SHM mmap failed, falling back to payload: "
                                                                 << mmapRc.GetMsg();
            METRIC_INC(metrics::KvMetricId::CLIENT_SHM_MMAP_FALLBACK_TOTAL);
            return;
        }
        info.pointer = static_cast<uint8_t *>(shmBuf->pointer) + shmBuf->offset;
        info.metadataSize = rsp.metadata_size();
        if (!rsp.shm_id().empty()) {
            mmapManager_->AssociateShmId(shmBuf->fd, rsp.shm_id());
        }
        info.mmapEntry = mmapManager_->GetMmapEntryByFd(shmBuf->fd);
        if (info.mmapEntry == nullptr) {
            LOG_EVERY_N(WARNING, SHM_FALLBACK_LOG_RATE)
                << "SHM GetMmapEntryByFd returned nullptr after mmap for fd=" << shmBuf->fd;
        }
        METRIC_INC(metrics::KvMetricId::CLIENT_SHM_MMAP_SUCCESS_TOTAL);
    }

    Status Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                  const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        RETURN_IF_NOT_OK(ValidateCreateRequest(key, size, param));
        CreateReqPb createReq;
        RETURN_IF_NOT_OK(BuildCreateRequest(key, size, param, createReq));
        CreateRspPb createRsp;
        uint32_t workerVersion = 0;
        RETURN_IF_NOT_OK(rpcClient_->InvokeCreate(param.subTimeoutMs, createReq, createRsp, workerVersion));

        auto info = std::make_shared<ObjectBufferInfo>();
        info->objectKey = key;
        info->dataSize = size;
        info->metadataSize = 0;  // SHM payload-only path: metadata sent inline with data
        info->workerAddr = workerAddr;
        info->objectMode = ModeInfo(param.consistencyType, param.writeMode, param.cacheType);
        info->pointer = nullptr;  // client-side buffer allocated by ObjectBuffer for payload
        if (!createRsp.shm_id().empty()) {
            info->shmId = ShmKey::Intern(createRsp.shm_id());
        }
        info->version = workerVersion;
        // Zero-copy mmap path (shared helper). Sets info->pointer/mmapEntry on success; on failure
        // leaves them null → Set falls back to payload inline.
        TryMmapBuffer(createRsp, size, *info);
        return ObjectBufferInternal::Create(info, buffer);
    }

    Status Set(ObjectBuffer &buffer, const TransportSetParam &param) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        const ObjectBufferInfo &info = ObjectBufferInternal::GetInfo(buffer);
        PublishReqPb pubReq;
        RETURN_IF_NOT_OK(BuildSetRequest(info, param, pubReq));
        // If pointer is from shm mmap, data is already visible to worker — send empty payload.
        // Otherwise (fallback), send data inline through the RPC.
        std::vector<MemView> payloads;
        if (info.pointer != nullptr && info.mmapEntry != nullptr) {
            // Data already in shared memory, worker can see it directly. Send metadata-only publish.
        } else {
            MemView payload(info.pointer + info.metadataSize, info.dataSize);
            payloads = { payload };
        }
        PublishRspPb rsp;
        uint32_t workerVersion = 0;
        RETURN_IF_NOT_OK(rpcClient_->InvokeSet(param.subTimeoutMs, pubReq, payloads, rsp, workerVersion));
        // Observability: record whether this Set actually used the zero-copy mmap pointer or fell back to
        // inline payload (review 180849800). mmapEntry is the real zero-copy discriminator (info.pointer is
        // always malloc'd by ObjectBuffer::Init). Record(SHM) below still tags the transporter kind.
        if (info.mmapEntry != nullptr) {
            METRIC_INC(metrics::KvMetricId::CLIENT_SHM_ZERO_COPY_SET_TOTAL);
        } else {
            METRIC_INC(metrics::KvMetricId::CLIENT_SHM_PAYLOAD_FALLBACK_SET_TOTAL);
        }
        // Routed same-host Set is tagged SHM (the transporter kind for routing/observability),
        // regardless of whether the zero-copy mmap pointer or the fallback payload-inline path
        // carried the bytes — matching KVClientTransportSetTest's ExpectedTransport()=="SHM" contract.
        return SetTransportResponseStatus(rsp, AccessTransportKind::SHM, param.isSeal, param.isRetry);
    }

    Status MCreate(const HostPort &workerAddr, const std::vector<std::string> &keys,
                   const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                   std::vector<std::shared_ptr<ObjectBuffer>> &buffers) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        RETURN_IF_NOT_OK(ValidateMultiCreateRequest(keys, sizes, param));
        buffers.clear();
        MultiCreateReqPb multiReq;
        RETURN_IF_NOT_OK(BuildMultiCreateRequest(keys, sizes, param, multiReq));
        MultiCreateRspPb multiRsp;
        uint32_t workerVersion = 0;
        // Single MultiCreate RPC (1 RTT). On RPC failure the worker does not allocate; on response
        // loss the worker releases any partial allocations via the expired-fds reconciler.
        RETURN_IF_NOT_OK(rpcClient_->InvokeMultiCreate(param.subTimeoutMs, multiReq, multiRsp, workerVersion));
        if (static_cast<size_t>(multiRsp.results_size()) != keys.size()) {
            // The worker allocated some objects but returned a mismatched result count (e.g. partial
            // alloc or response loss). Release every shm_id the worker did report back so the
            // worker-side shm refs do not strand until client disconnect.
            for (const auto &r : multiRsp.results()) {
                if (!r.shm_id().empty()) {
                    (void)rpcClient_->InvokeDecreaseReference(param.requestContext, ShmKey::Intern(r.shm_id()));
                }
            }
            RETURN_STATUS(K_RUNTIME_ERROR, "ShmTransporter MCreate response count does not match request count");
        }
        for (size_t i = 0; i < keys.size(); i++) {
            auto info = std::make_shared<ObjectBufferInfo>();
            info->objectKey = keys[i];
            info->dataSize = sizes[i];
            info->metadataSize = 0;  // SHM payload-only path: metadata sent inline with data
            info->workerAddr = workerAddr;
            info->objectMode = ModeInfo(param.consistencyType, param.writeMode, param.cacheType);
            info->pointer = nullptr;  // zero-copy mmap will set this if shm deps available
            const auto &result = multiRsp.results(i);
            if (!result.shm_id().empty()) {
                info->shmId = ShmKey::Intern(result.shm_id());
            }
            info->version = workerVersion;
            // Zero-copy mmap path (same as Create). Without this MSet always falls back to payload
            // inline because info.mmapEntry stays null (review 180840879).
            TryMmapBuffer(result, sizes[i], *info);
            std::shared_ptr<ObjectBuffer> buf;
            RETURN_IF_NOT_OK(ObjectBufferInternal::Create(info, buf));
            buffers.push_back(std::move(buf));
        }
        return Status::OK();
    }

    Status MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                const TransportSetParam &param, TransportMSetResult &result) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        result.Clear();
        std::vector<bool> tcpPayload;
        tcpPayload.reserve(buffers.size());
        for (const auto &buf : buffers) {
            const auto &info = ObjectBufferInternal::GetInfo(*buf);
            tcpPayload.push_back(info.mmapEntry == nullptr);
        }
        int64_t zeroCopyCount = 0;
        for (bool isPayload : tcpPayload) {
            if (!isPayload) {
                ++zeroCopyCount;
            }
        }
        METRIC_ADD(metrics::KvMetricId::CLIENT_SHM_ZERO_COPY_SET_TOTAL, zeroCopyCount);
        METRIC_ADD(metrics::KvMetricId::CLIENT_SHM_PAYLOAD_FALLBACK_SET_TOTAL,
                   static_cast<int64_t>(buffers.size()) - zeroCopyCount);
        MultiPublishReqPb request;
        std::vector<MemView> payloads;
        RETURN_IF_NOT_OK(BuildMultiPublishRequest(buffers, tcpPayload, param, request, payloads));
        result.publishAttempted = true;
        MultiPublishRspPb response;
        uint32_t workerVersion = 0;
        RETURN_IF_NOT_OK(rpcClient_->InvokeMultiSet(param.subTimeoutMs, request, payloads, response, workerVersion));
        Status msetRc = SetMSetResponseResult(response, buffers.size(), AccessTransportKind::SHM, result);
        // SetMSetResponseResult calls result.Clear() which resets publishAttempted; re-set it like
        // TcpTransporter::MSet (tcp_transporter.cpp) so the caller's retry logic (retryUnsentPublish)
        // does not re-publish data the worker already received.
        result.publishAttempted = true;
        result.workerAutoRelease = request.auto_release_memory_ref() && msetRc.IsOk();
        return msetRc;
    }

    Status Release(const ShmKey &shmId, const TransportRequestContext &context) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        return rpcClient_->InvokeDecreaseReference(context, shmId);
    }

private:
    std::shared_ptr<WorkerRpcClient> rpcClient_;
    std::shared_ptr<IClientWorkerCommonApi> workerApi_;
    std::shared_ptr<MmapManager> mmapManager_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H
