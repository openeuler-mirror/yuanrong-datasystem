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

/** Description: Defines an endpoint-scoped client-to-worker shared-memory session. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_SHM_CONNECTION_H
#define DATASYSTEM_CLIENT_TRANSPORT_SHM_CONNECTION_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/client/mmap_manager/host_memory_pin_manager.h"
#include "datasystem/client/mmap_manager/mmap_manager.h"
#include "datasystem/client/object_cache/transport/data_plane/i_data_plane_connection.h"
#include "datasystem/client/object_cache/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/object_cache/transport/shm_fd.h"
#include "datasystem/client/object_cache/transport/transport_phase_latency_recorder.h"
#include "datasystem/common/object_cache/ireceive_buffer_owner.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/util/thread_pool.h"

// Keep bthread headers after project RPC headers so brpc logging macros are established before project overrides.
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

namespace datasystem {
namespace client {

class ShmFdChannel final : public IShmFdProvider {
public:
    ShmFdChannel(std::shared_ptr<WorkerRpcClient> rpcClient, ShmFd socketFd, bool isScmTcp,
                 std::string clientId);
    ~ShmFdChannel() override;

    Status GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds,
                       const std::string &tenantId) override;

    const std::string &ClientId() const override;

    void UpdateAuth(const TransportRequestContext &context);

    bool IsAlive() const;

    void Close();

private:
    std::shared_ptr<WorkerRpcClient> rpcClient_;
    ShmFd socketFd_;
    bool isScmTcp_;
    std::string clientId_;
    mutable bthread::Mutex mutex_;
    TransportRequestContext auth_;
    uint64_t requestId_{ 0 };
    std::atomic<int> socketNumber_{ INVALID_SHM_FD };
    std::atomic<bool> alive_{ true };
};

class ShmSession final : public std::enable_shared_from_this<ShmSession> {
public:
    static Status Create(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient,
                         const TransportRequestContext &context, std::weak_ptr<ThreadPool> releasePool,
                         std::weak_ptr<ThreadPool> maintenancePool,
                         std::shared_ptr<std::atomic<bool>> scaleInDraining,
                         const std::shared_ptr<HostMemoryPinManager> &hostMemoryPinManager,
                         std::shared_ptr<ShmSession> &session,
                         TransportPhaseLatencyRecorder *recorder = nullptr);

    ~ShmSession();

    Status Get(const DataGetBatchRequest &inputs, GetRspPb &response, std::vector<RpcMessage> &payloads);

    // Register the complete response before any owner can release a legacy per-client reference.
    Status RegisterReadReferences(const GetRspPb &response);
    Status RegisterReadReferences(const QueryAndGetRspPb &response);
    // OwnReadReference and unread cleanup consume references registered by RegisterReadReferences exactly once.
    Status OwnReadReference(const std::string &shmId, std::shared_ptr<const TransportReadContext> context,
                            std::shared_ptr<IReceiveBufferOwner> &owner);
    void ReleaseUnreadReferences(const GetRspPb &response, int begin,
                                 const std::shared_ptr<const TransportReadContext> &context) noexcept;
    void ReleaseUnreadReferences(const QueryAndGetRspPb &response, int begin,
                                 const std::shared_ptr<const TransportReadContext> &context) noexcept;

    // Both read builders require an owner returned by this session's OwnReadReference.
    Status BuildResult(const GetRspPb::ObjectInfoPb &info, const DataGetRequest &input, DataGetResult &result,
                        std::shared_ptr<IReceiveBufferOwner> owner);

    /**
     * @brief Materialize a QueryAndGet shared-memory result through this session.
     * @param[in] info Shared-memory result returned by Worker QueryAndGet.
     * @param[in] input Object identity and request context.
     * @param[out] result Materialized zero-copy data result.
     * @return K_OK on success; the error code otherwise.
     */
    Status BuildQueryAndGetResult(const QueryAndGetShmInfoPb &info, const DataGetRequest &input,
                                  DataGetResult &result, std::shared_ptr<IReceiveBufferOwner> owner);

    /** Maps the shared-memory region allocated by a routed Create into the client address space (PROT_WRITE)
     * so the caller can write zero-copy, registers the worker reference, and attaches a send-side owner that
     * gates Publish on session liveness (K_BUFFER_DEPRECATED) and releases the worker reference on buffer
     * destruction. Mirrors BuildResult for the write direction (fd still worker->client). */
    Status MmapWriteRegion(const CreateRspPb &createRsp, const TransportRequestContext &context, uint64_t size,
                           ObjectBufferInfo &info);

    bool IsAlive() const;

    const std::string &ClientId() const;

    const std::string &WorkerStartId() const;

    void Close(bool notifyWorker);

    Status DecreaseReference(const TransportRequestContext &context, const ShmKey &shmId);
    // Releases a reference registered under the request context's OWN clientId (the SDK global clientId),
    // NOT the session clientId. Used by routed Create, which registers the create-time worker reference
    // under the global clientId (req.client_id) — so the release must use the same identity or the worker
    // cannot match it and the region leaks until client-lost. (DecreaseReference above overrides the
    // clientId with the session UUID, which is correct for Get but wrong for routed Create.)
    Status DecreaseReferenceByRequestClient(const TransportRequestContext &context, const ShmKey &shmId);

private:
    ShmSession(HostPort workerAddr, std::shared_ptr<WorkerRpcClient> rpcClient,
               std::shared_ptr<ShmFdChannel> fdChannel, std::shared_ptr<MmapManager> mmapManager,
               std::string clientId, std::string workerStartId, uint32_t lockId,
               std::weak_ptr<ThreadPool> releasePool, std::weak_ptr<ThreadPool> maintenancePool,
               TransportRequestContext auth, bool supportMultiRefCount,
               std::shared_ptr<std::atomic<bool>> scaleInDraining);

    Status RegisterReference(const ShmKey &shmId);

    // Validates WorkerOCService Get response bounds (store_fd, offset/metadata/data/mmap_size, shm_id, mode).
    // Extracted from BuildResult to keep that function within the codecheck 50-line limit.
    Status ValidateObjectInfo(const GetRspPb::ObjectInfoPb &info, uint64_t &offset, uint64_t &metadataSize,
                              uint64_t &dataSize, uint64_t &mmapSize) const;

    Status StartMaintenance();

    Status ScheduleMaintenance();

    void SubmitMaintenance();

    void RunMaintenance();

    void CloseForScaleIn();

    void ScheduleDisconnect();

    HostPort workerAddr_;
    std::shared_ptr<WorkerRpcClient> rpcClient_;
    std::shared_ptr<ShmFdChannel> fdChannel_;
    std::shared_ptr<MmapManager> mmapManager_;
    std::string clientId_;
    std::string workerStartId_;
    uint32_t lockId_;
    std::weak_ptr<ThreadPool> releasePool_;
    std::weak_ptr<ThreadPool> maintenancePool_;
    mutable bthread::Mutex authMutex_;
    TransportRequestContext auth_;
    bthread::Mutex refMutex_;
    std::unordered_map<ShmKey, size_t> localRefCounts_;
    bool supportMultiRefCount_;
    std::shared_ptr<std::atomic<bool>> scaleInDraining_;
    uint64_t maintenanceIntervalMs_{ 1000 };
    std::vector<int64_t> releasedWorkerFds_;
    std::atomic<bool> alive_{ true };
    std::atomic<bool> disconnectScheduled_{ false };
};

// The response outlives this cursor. Each consumed entry is owned by an owner or has no SHM reference.
template <typename Response>
class ShmReadResponseGuard final {
public:
    ShmReadResponseGuard(const std::shared_ptr<ShmSession> &session, const Response &response,
                         const std::shared_ptr<const TransportReadContext> &context)
        : session_(session), response_(response), context_(context)
    {
    }
    ShmReadResponseGuard(const ShmReadResponseGuard &) = delete;
    ShmReadResponseGuard &operator=(const ShmReadResponseGuard &) = delete;
    ~ShmReadResponseGuard()
    {
        if (session_ != nullptr) {
            session_->ReleaseUnreadReferences(response_, consumed_, context_);
        }
    }
    void Consume()
    {
        ++consumed_;
    }

private:
    const std::shared_ptr<ShmSession> &session_;
    const Response &response_;
    const std::shared_ptr<const TransportReadContext> &context_;
    int consumed_{ 0 };
};

class ShmConnection final : public IDataPlaneConnection {
public:
    ShmConnection(HostPort workerAddr, std::shared_ptr<WorkerRpcClient> rpcClient,
                  std::weak_ptr<ThreadPool> releasePool,
                  std::shared_ptr<HostMemoryPinManager> hostMemoryPinManager = nullptr,
                  std::weak_ptr<ThreadPool> maintenancePool = {});
    ~ShmConnection() override;

    Status Establish(const HostPort &workerAddr) override;

    Status Acquire(const TransportRequestContext &context, std::shared_ptr<ShmSession> &session);

    /**
     * @brief Acquire the endpoint session without waiting for another connection attempt.
     * @param[in] context Request authentication and tenant context.
     * @param[out] session Acquired endpoint session.
     * @param[in] recorder Optional request-scoped phase recorder.
     * @return K_TRY_AGAIN while another attempt is running; K_OK on success; the error code otherwise.
     */
    Status TryAcquire(const TransportRequestContext &context, std::shared_ptr<ShmSession> &session,
                      TransportPhaseLatencyRecorder *recorder = nullptr);

    bool IsAlive() const override;

    AccessTransportKind Kind() const override;

    void Teardown() override;

private:
    // The blocking acquisition path waits for the single in-flight connection attempt, bounded by the API deadline.
    Status WaitForConnecting(std::unique_lock<bthread::Mutex> &lock);

    Status AcquireImpl(const TransportRequestContext &context, bool waitForConnecting,
                       std::shared_ptr<ShmSession> &session, TransportPhaseLatencyRecorder *recorder = nullptr);

    Status PrepareAcquire(bool waitForConnecting, uint64_t &attemptId, bool &startConnection,
                          std::shared_ptr<ShmSession> &session, TransportPhaseLatencyRecorder *recorder);

    Status WaitForExistingAttempt(std::unique_lock<bthread::Mutex> &lock, bool waitForConnecting,
                                  TransportPhaseLatencyRecorder *recorder);

    Status CompleteConnectionAttempt(uint64_t attemptId, const std::shared_ptr<ShmSession> &candidate, Status result,
                                     uint64_t connectUs, std::shared_ptr<ShmSession> &session);

    HostPort workerAddr_;
    std::shared_ptr<WorkerRpcClient> rpcClient_;
    std::weak_ptr<ThreadPool> releasePool_;
    std::weak_ptr<ThreadPool> maintenancePool_;
    std::shared_ptr<HostMemoryPinManager> hostMemoryPinManager_;
    mutable bthread::Mutex mutex_;
    bthread::ConditionVariable cv_;
    std::shared_ptr<ShmSession> session_;
    std::shared_ptr<std::atomic<bool>> scaleInDraining_{ std::make_shared<std::atomic<bool>>(false) };
    bool connecting_{ false };
    bool closed_{ false };
    uint64_t attemptId_{ 0 };
    Status lastConnectFailure_{ K_NOT_READY, "Shared-memory connection has not been attempted" };
    std::chrono::steady_clock::time_point retryAfter_;
    int64_t failureBackoffMs_{ 10 };
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_SHM_CONNECTION_H
