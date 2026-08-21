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

/** Description: Shared-memory transporter for same-host workers. Routed Get/BatchGet and
 * Create/Publish all use WorkerOCService and an endpoint-scoped fd-passing session: Get mmaps a
 * worker-passed fd to read zero-copy, Create mmaps a worker-allocated region to write zero-copy
 * (fd direction is worker->client in both cases), and Set/Publish send only metadata + shm_id. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H
#define DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H

#include <algorithm>
#include <new>
#include <vector>

#include "datasystem/client/transport/data_plane/shm_connection.h"
#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rpc/brpc_status_util.h"

namespace datasystem {
namespace client {
class ShmTransporter : public IDataTransporter {
public:
    explicit ShmTransporter(HostPort workerAddr, std::shared_ptr<WorkerRpcClient> rpcClient,
                           std::weak_ptr<ThreadPool> releasePool = {})
        : rpcClient_(std::move(rpcClient)),
          shmConnection_(std::make_shared<ShmConnection>(std::move(workerAddr), rpcClient_, std::move(releasePool)))
    {
    }
    ~ShmTransporter() override = default;

    AccessTransportKind Kind() const override
    {
        return AccessTransportKind::SHM;
    }

    bool IsAlive() const override
    {
        return rpcClient_ != nullptr && rpcClient_->IsAlive() && shmConnection_ != nullptr
               && shmConnection_->IsAlive();
    }

    /**
     * @brief Acquire the endpoint shared-memory session for a routed request.
     * @param[in] context Request authentication and tenant context.
     * @param[out] session Acquired endpoint session.
     * @return K_OK on success; the error code otherwise.
     */
    Status AcquireSession(const TransportRequestContext &context, std::shared_ptr<ShmSession> &session)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(shmConnection_);
        return shmConnection_->Acquire(context, session);
    }

    /**
     * @brief Invalidate a session that returned an unusable shared-memory result.
     * @param[in] session Session to invalidate.
     */
    void InvalidateSession(const std::shared_ptr<ShmSession> &session)
    {
        if (shmConnection_ != nullptr) {
            shmConnection_->Invalidate(session);
        }
    }

    Status Get(const DataGetRequest &input, DataGetResult &output) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        RETURN_RUNTIME_ERROR_IF_NULL(shmConnection_);
        CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
        CHECK_FAIL_RETURN_STATUS(input.context != nullptr, K_INVALID, "Transport read context is missing");

        std::shared_ptr<ShmSession> session;
        RETURN_IF_NOT_OK(shmConnection_->Acquire(input.context->requestContext, session));
        GetRspPb response;
        std::vector<RpcMessage> payloads;
        const DataGetBatchRequest inputs{ input };
        Status rc = session->Get(inputs, response, payloads);
        if (rc.IsError()) {
            shmConnection_->Invalidate(session);
            return rc;
        }
        rc = ValidateShmResponse(response, payloads, 1);
        if (rc.IsError()) {
            shmConnection_->Invalidate(session);
            return rc;
        }
        const auto &info = response.objects(0);
        if (info.store_fd() <= 0) {
            Status missingStatus = MissingObjectStatus(response);
            // Stamp the business error code onto the response so BatchGetOne can distinguish a per-item
            // business error (error_code != K_OK) from a transport-level failure (default K_OK), matching
            // the multi-element BatchGet path which sets item.status = missingStatus.
            output.response.mutable_error()->set_error_code(static_cast<int>(missingStatus.GetCode()));
            return missingStatus;
        }
        rc = session->BuildResult(info, input, output);
        if (rc.IsError()) {
            shmConnection_->Invalidate(session);
        }
        return rc;
    }

    Status BatchGet(const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) override
    {
        outputs.clear();
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        RETURN_RUNTIME_ERROR_IF_NULL(shmConnection_);
        CHECK_FAIL_RETURN_STATUS(!inputs.empty(), K_INVALID, "Batch get request is empty");
        if (inputs.size() == 1) {
            return BatchGetOne(inputs.front(), outputs);
        }

        for (const auto &input : inputs) {
            CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
            CHECK_FAIL_RETURN_STATUS(input.context != nullptr, K_INVALID, "Transport read context is missing");
        }

        std::shared_ptr<ShmSession> session;
        RETURN_IF_NOT_OK(shmConnection_->Acquire(inputs.front().context->requestContext, session));
        GetRspPb response;
        std::vector<RpcMessage> payloads;
        Status rc = session->Get(inputs, response, payloads);
        if (rc.IsError()) {
            shmConnection_->Invalidate(session);
            return rc;
        }
        rc = ValidateShmResponse(response, payloads, inputs.size());
        if (rc.IsError()) {
            shmConnection_->Invalidate(session);
            return rc;
        }

        outputs.resize(inputs.size());
        const Status missingStatus = MissingObjectStatus(response);
        for (const auto &info : response.objects()) {
            const uint32_t index = info.object_index();
            auto &item = outputs[index];
            if (info.store_fd() <= 0) {
                item.status = missingStatus;
                continue;
            }
            item.status = session->BuildResult(info, inputs[index], item.data);
            if (item.status.IsError()) {
                outputs.clear();
                shmConnection_->Invalidate(session);
                return item.status;
            }
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

    void CloseDataPlane() override
    {
        if (shmConnection_ != nullptr) {
            shmConnection_->Teardown();
        }
    }

    Status Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                  const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        RETURN_RUNTIME_ERROR_IF_NULL(shmConnection_);
        RETURN_IF_NOT_OK(ValidateCreateRequest(key, size, param));
        CreateReqPb createReq;
        RETURN_IF_NOT_OK(BuildCreateRequest(key, size, param, createReq));
        CreateRspPb createRsp;
        uint32_t workerVersion = 0;
        RETURN_IF_NOT_OK(rpcClient_->InvokeCreate(param.subTimeoutMs, createReq, createRsp, workerVersion));
        // If the worker did not allocate an shm region (store_fd <= 0, e.g. an inline/small object that
        // did not meet the shm threshold), fall back to a local payload buffer published inline (TCP) —
        // there is no region to mmap. This preserves the legacy placeholder semantics for the no-fd case.
        if (createRsp.store_fd() <= 0) {
            Status localRc = BuildLocalBuffer(workerAddr, key, size, param, workerVersion, buffer);
            if (localRc.IsError()) {
                ReleaseAllocation(createRsp.shm_id(), param.requestContext,
                                  "Create allocation after local buffer setup failure");
            }
            return localRc;
        }
        // The worker allocated an shm region; acquire the endpoint-scoped fd-passing session and mmap it
        // (fd passed worker->client via GetClientFd) so the caller writes zero-copy.
        std::shared_ptr<ShmSession> session;
        Status rc = shmConnection_->Acquire(param.requestContext, session);
        if (rc.IsOk()) {
            rc = BuildShmBuffer(workerAddr, key, size, param, createRsp, workerVersion, session, buffer);
            if (rc.IsError()) {
                shmConnection_->Invalidate(session);
            }
        }
        if (rc.IsError()) {
            // fd-passing unavailable (K_NOT_SUPPORTED) or mmap failed; release the worker allocation.
            // TransportLayer escalates K_NOT_SUPPORTED to UB/TCP for the write.
            ReleaseAllocation(createRsp.shm_id(), param.requestContext,
                              "Create allocation after write-region mmap failure");
        }
        return rc;
    }

    Status Set(ObjectBuffer &buffer, const TransportSetParam &param, TransportSetResult *result = nullptr) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        const ObjectBufferInfo &info = ObjectBufferInternal::GetInfo(buffer);
        if (result != nullptr) {
            result->publishAttempted = false;
            result->publishDefinitelyNotSent = false;
        }
        // UC6: gate on session liveness for routed SHM zero-copy buffers. If the fd-passing session died
        // between Create and Set, fail fast (K_BUFFER_DEPRECATED) instead of publishing a stale shm_id.
        if (info.receiveBufferOwner != nullptr) {
            RETURN_IF_NOT_OK(info.receiveBufferOwner->CheckAlive());
        }
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
        if (result != nullptr) {
            result->publishAttempted = true;
        }
        Status invokeRc = rpcClient_->InvokeSet(param.subTimeoutMs, pubReq, payloads, rsp, workerVersion);
        if (result != nullptr) {
            result->publishDefinitelyNotSent = IsBrpcRequestDefinitelyNotSent(invokeRc);
        }
        RETURN_IF_NOT_OK(invokeRc);
        // Observability: record whether this Set actually used the zero-copy mmap pointer or fell back to
        // inline payload (review 180849800). mmapEntry is the real zero-copy discriminator (info.pointer is
        // always malloc'd by ObjectBuffer::Init).
        if (info.mmapEntry != nullptr) {
            METRIC_INC(metrics::KvMetricId::CLIENT_SHM_ZERO_COPY_SET_TOTAL);
        } else {
            METRIC_INC(metrics::KvMetricId::CLIENT_SHM_PAYLOAD_FALLBACK_SET_TOTAL);
        }
        // Zero-copy when data lives in the mmap'd worker region (mmapEntry set by Create); TCP when it
        // fell back to an inline payload. mmapEntry is the real zero-copy discriminator.
        const auto kind = (info.mmapEntry != nullptr) ? AccessTransportKind::SHM : AccessTransportKind::TCP;
        return SetTransportResponseStatus(rsp, kind, param.isSeal, param.isRetry);
    }

    Status MCreate(const HostPort &workerAddr, const std::vector<std::string> &keys,
                   const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                   std::vector<std::shared_ptr<ObjectBuffer>> &buffers) override
    {
        RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
        RETURN_RUNTIME_ERROR_IF_NULL(shmConnection_);
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
            ReleaseAllocations(multiRsp, param.requestContext,
                               "MCreate allocations after response count mismatch");
            RETURN_STATUS(K_RUNTIME_ERROR, "ShmTransporter MCreate response count does not match request count");
        }
        // Only acquire the fd-passing session if at least one result carries an shm region (store_fd > 0);
        // an all-store_fd<=0 MCreate (e.g. inline/small objects) needs no session.
        std::shared_ptr<ShmSession> session;
        Status acquireRc = AcquireSessionIfNeeded(multiRsp, param, session);
        if (acquireRc.IsError()) {
            ReleaseAllocations(multiRsp, param.requestContext, "MCreate allocations after session acquire failure");
            return acquireRc;
        }
        return BuildMCreateBuffers(workerAddr, keys, sizes, param, multiRsp, workerVersion, session, buffers);
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
            // UC6: gate on session liveness for routed SHM zero-copy buffers (fail fast with
            // K_BUFFER_DEPRECATED instead of publishing a stale shm_id after the session died).
            if (info.receiveBufferOwner != nullptr) {
                RETURN_IF_NOT_OK(info.receiveBufferOwner->CheckAlive());
            }
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
        Status invokeRc = rpcClient_->InvokeMultiSet(param.subTimeoutMs, request, payloads, response, workerVersion);
        result.publishDefinitelyNotSent = IsBrpcRequestDefinitelyNotSent(invokeRc);
        RETURN_IF_NOT_OK(invokeRc);
        // Tag SHM only when every buffer is zero-copy (data in mmap'd worker regions). A same-host
        // same-worker MSet is uniform, so this is all-or-nothing; mixed (some inline) falls back to TCP.
        const auto kind = (zeroCopyCount == static_cast<int64_t>(buffers.size()))
                              ? AccessTransportKind::SHM
                              : AccessTransportKind::TCP;
        Status msetRc = SetMSetResponseResult(response, buffers.size(), kind, result);
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
    // Acquires the endpoint-scoped fd-passing session only if some MultiCreate result has an shm region
    // (store_fd > 0); otherwise leaves session null (no fd channel needed for inline/small objects).
    Status AcquireSessionIfNeeded(const MultiCreateRspPb &multiRsp, const TransportCreateParam &param,
                                  std::shared_ptr<ShmSession> &session)
    {
        for (const auto &res : multiRsp.results()) {
            if (res.store_fd() > 0) {
                return shmConnection_->Acquire(param.requestContext, session);
            }
        }
        return Status::OK();
    }

    // Builds one buffer per MultiCreate result (per-key store_fd<=0 -> local buffer; >0 -> mmap zero-copy).
    // On per-key failure releases the unbuilt worker allocations [i, n); built buffers [0, i) release their
    // own refs via their send-side owners when the local `created` destructs. Extracted to keep MCreate
    // within the codecheck function-size limit.
    Status BuildMCreateBuffers(const HostPort &workerAddr, const std::vector<std::string> &keys,
                               const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                               const MultiCreateRspPb &multiRsp, uint32_t workerVersion,
                               const std::shared_ptr<ShmSession> &session,
                               std::vector<std::shared_ptr<ObjectBuffer>> &buffers)
    {
        std::vector<std::shared_ptr<ObjectBuffer>> created;
        try {
            created.reserve(keys.size());
        } catch (const std::bad_alloc &e) {
            ReleaseAllocations(multiRsp, param.requestContext,
                               "MCreate allocations after local result reservation failure");
            if (session != nullptr) {
                shmConnection_->Invalidate(session);
            }
            RETURN_STATUS(K_OUT_OF_MEMORY, e.what());
        }
        for (size_t i = 0; i < keys.size(); i++) {
            const auto &result = multiRsp.results(i);
            std::shared_ptr<ObjectBuffer> buf;
            Status rc = (result.store_fd() <= 0)
                            ? BuildLocalBuffer(workerAddr, keys[i], sizes[i], param, workerVersion, buf)
                            : BuildShmBuffer(workerAddr, keys[i], sizes[i], param, result, workerVersion, session,
                                             buf);
            if (rc.IsError()) {
                for (size_t j = i; j < keys.size(); j++) {
                    ReleaseAllocation(multiRsp.results(j).shm_id(), param.requestContext,
                                      "MCreate unbuilt allocation cleanup after write-region mmap failure");
                }
                if (session != nullptr) {
                    shmConnection_->Invalidate(session);
                }
                return rc;
            }
            created.push_back(std::move(buf));
        }
        buffers = std::move(created);
        return Status::OK();
    }

    // Builds a local payload buffer (no mmap) for the fallback case where the worker did not allocate an
    // shm region (store_fd <= 0). Data is later published inline (TCP). Mirrors the legacy placeholder.
    static Status BuildLocalBuffer(const HostPort &workerAddr, const std::string &key, uint64_t size,
                                   const TransportCreateParam &param, uint32_t workerVersion,
                                   std::shared_ptr<ObjectBuffer> &buffer)
    {
        INJECT_POINT("ShmTransporter.BuildLocalBuffer");
        try {
            auto info = std::make_shared<ObjectBufferInfo>();
            info->objectKey = key;
            info->dataSize = size;
            info->metadataSize = 0;  // SHM payload-only path: metadata sent inline with data
            info->workerAddr = workerAddr;
            info->objectMode = ModeInfo(param.consistencyType, param.writeMode, param.cacheType);
            info->pointer = nullptr;  // local payload buffer, allocated by ObjectBuffer::Init
            // Do not carry the worker shm_id: this is a pure inline-payload (TCP) buffer with no shm region
            // on the worker side. Sending both payload + shm_id would risk the worker dual-path attaching
            // an uninitialized region (review #7). Cleanup on failure still uses createRsp.shm_id() directly.
            info->version = workerVersion;
            return ObjectBufferInternal::Create(std::move(info), buffer);
        } catch (const std::bad_alloc &e) {
            RETURN_STATUS(K_OUT_OF_MEMORY, e.what());
        }
    }

    // Builds a routed SHM write buffer: mmaps the worker-allocated region so the caller writes zero-copy
    // and attaches the send-side owner (gates Publish on session liveness, releases the worker ref).
    static Status BuildShmBuffer(const HostPort &workerAddr, const std::string &key, uint64_t size,
                                 const TransportCreateParam &param, const CreateRspPb &createRsp,
                                 uint32_t workerVersion, const std::shared_ptr<ShmSession> &session,
                                 std::shared_ptr<ObjectBuffer> &buffer)
    {
        INJECT_POINT("ShmTransporter.BuildShmBuffer");
        try {
            auto info = std::make_shared<ObjectBufferInfo>();
            info->objectKey = key;
            info->workerAddr = workerAddr;
            info->objectMode = ModeInfo(param.consistencyType, param.writeMode, param.cacheType);
            info->version = workerVersion;
            RETURN_IF_NOT_OK(session->MmapWriteRegion(createRsp, param.requestContext, size, *info));
            return ObjectBufferInternal::Create(std::move(info), buffer);
        } catch (const std::bad_alloc &e) {
            RETURN_STATUS(K_OUT_OF_MEMORY, e.what());
        }
    }

    void ReleaseAllocation(const std::string &shmId, const TransportRequestContext &context,
                           const char *reason) const
    {
        if (rpcClient_ == nullptr || shmId.empty()) {
            return;
        }
        // Intern (tbb hash insert) can throw bad_alloc; this runs on failure-cleanup paths where OOM is
        // most likely — catch so the cleanup path never throws through to std::terminate (coredump).
        // A skipped release is reclaimed by the worker client-lost fallback.
        try {
            Status rc = rpcClient_->InvokeDecreaseReference(context, ShmKey::Intern(shmId));
            LOG_IF_ERROR(rc, reason);
        } catch (const std::bad_alloc &e) {
            LOG(WARNING) << reason << ", release skipped on OOM: " << e.what();
        }
    }

    void ReleaseAllocations(const MultiCreateRspPb &response, const TransportRequestContext &context,
                            const char *reason) const
    {
        for (const auto &result : response.results()) {
            ReleaseAllocation(result.shm_id(), context, reason);
        }
    }

    static Status MissingObjectStatus(const GetRspPb &response)
    {
        Status status(static_cast<StatusCode>(response.last_rc().error_code()), response.last_rc().error_msg());
        return status.IsError() ? status : Status(K_NOT_FOUND, "Cannot get object from worker");
    }

    static Status ValidateShmResponse(const GetRspPb &response, const std::vector<RpcMessage> &payloads,
                                      size_t requestCount)
    {
        CHECK_FAIL_RETURN_STATUS(payloads.empty() && response.payload_info().empty(), K_RUNTIME_ERROR,
                                 "SHM WorkerOCService Get unexpectedly returned RPC payload data");
        CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(response.objects_size()) == requestCount, K_RUNTIME_ERROR,
                                 "WorkerOCService Get response count does not match request count");
        if (requestCount == 1) {
            CHECK_FAIL_RETURN_STATUS(response.objects(0).object_index() == 0, K_RUNTIME_ERROR,
                                     "WorkerOCService Get returned an invalid object index");
            return Status::OK();
        }
        std::vector<bool> returnedIndexes(requestCount, false);
        for (const auto &info : response.objects()) {
            const size_t index = info.object_index();
            CHECK_FAIL_RETURN_STATUS(index < requestCount && !returnedIndexes[index],
                                     K_RUNTIME_ERROR,
                                     "WorkerOCService Get returned an invalid object index");
            returnedIndexes[index] = true;
        }
        return Status::OK();
    }

    std::shared_ptr<WorkerRpcClient> rpcClient_;
    std::shared_ptr<ShmConnection> shmConnection_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_SHM_TRANSPORTER_H
