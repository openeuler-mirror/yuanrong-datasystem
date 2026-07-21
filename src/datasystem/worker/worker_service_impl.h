/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Interface of worker common service processing main class.
 */
#ifndef DATASYSTEM_WORKER_WORKER_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_WORKER_SERVICE_IMPL_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/protos/share_memory.irpc.pb.h"
#include "datasystem/protos/share_memory.service.rpc.pb.h"
#include "datasystem/protos/share_memory.brpc.pb.h"
#include "datasystem/server/common_server.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/runtime/worker_runtime_facade.h"

namespace datasystem {
namespace worker {
class WorkerServiceImpl : public WorkerService, public IWorkerService, public Callable {
public:
    /**
     * @brief Create a new WorkerServiceImpl object.
     * @param[in] serverAddr The address of worker.
     * @param[in] masterAddr The address of master.
     * @param[in] timeoutMult The value of timeout factor.
     * @param[in] worker The class of worker server.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] workerUuid The worker uuid.
     * @param[in] membership Read-only cluster membership view that outlives this service.
     * @param[in] localExiting Worker-owned local exit gate that outlives this service.
     */
    WorkerServiceImpl(HostPort serverAddr, HostPort masterAddr, double timeoutMult, CommonServer *worker,
                      std::shared_ptr<AkSkManager> akSkManager, std::string workerUuid,
                      const cluster::MembershipEndpointView &membership, const std::atomic<bool> &localExiting);

    ~WorkerServiceImpl() override;

    /**
     * @brief Initialize the WorkerFCServiceImpl Object.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Get FD of client.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status GetClientFd(const GetClientFdReqPb &req, GetClientFdRspPb &rsp) override;

    /**
     * @brief Get the socket path (for fd-passing)
     * @param [in] req The rpc request protobuf
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status GetSocketPath(const GetSocketPathReqPb &req, GetSocketPathRspPb &rsp) override;

    /**
     * @brief Register a client
     * @param [in] req The rpc request protobuf
     * @param [out] rsp The rpc response protobuf
     * @return Status of the call
     */
    Status RegisterClient(const RegisterClientReqPb &req, RegisterClientRspPb &rsp) override;

    /**
     * @brief Close the Fd for the client to be shutting down.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status DisconnectClient(const DisconnectClientReqPb &req, DisconnectClientRspPb &rsp) override;

    /**
     * @brief Processes heartbeat requests from clients..
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status Heartbeat(const HeartbeatReqPb &req, HeartbeatRspPb &rsp) override;

    /**
     * @brief Get the expired shm fds for a specific client (intersection of all expired fds and
     * the fds mmap'd by this client). Previously only called from Heartbeat; now extracted as a
     * reusable method so any RPC handler can attach expired fds to its response (design §4.3.3
     * path-one: RPC response attachment carries expired fds, replacing heartbeat delivery).
     * @param[in] clientId The client to query.
     * @return The set of expired fds belonging to this client.
     */
    std::set<int> GetExpiredFdsForClient(const ClientKey &clientId);

    /**
     * @brief Check the version that client sent from RegisterClientReqPb.
     * @param[in] clientVersion The version of client.
     * @return Status of the call.
     */
    Status CheckClientVersion(const std::string &clientVersion);

    /**
     * @brief Functor of this class.
     * @return Status of the call
     */
    Status operator()() override;

    /**
     * @brief Set uuid of this node.
     * @param[in] uuid Existing Worker-facing identity value.
     */
    void SetWorkerUuid(const std::string &uuid)
    {
        workerUuid_ = uuid;
    }

    /**
     * @brief Assign borrowed Worker runtime dependency.
     * @param[in] runtime Runtime facade owned by WorkerOCServer.
     */
    void SetRuntimeFacade(const worker::WorkerRuntimeFacade *runtime)
    {
        runtime_ = runtime;
    }

private:
    Status CheckRuntimeAdmission(const std::string &operation,
                                 worker::WorkerAdmissionKind kind = worker::WorkerAdmissionKind::NORMAL_WRITE) const;
    Status ValidateRegisterClientRequest(const RegisterClientReqPb &req, std::string &tenantId) const;
    Status ConsumeRegisterClientFd(const RegisterClientReqPb &req);
    Status AddRegisteringClient(const RegisterClientReqPb &req, const ClientKey &clientId, const std::string &tenantId,
                                const CompatibilityVersion &compatibilityVersion, uint32_t &lockId,
                                uint32_t &pipelineQueueId, bool &supportMultiShmRefCount);
    void PopulateRegisterClientResponse(RegisterClientRspPb &rsp, const ClientKey &clientId,
                                        const std::string &tenantId, uint32_t lockId, uint32_t pipelineQueueId,
                                        bool supportMultiShmRefCount, bool shmEnabled, int fd, uint64_t mmapSize,
                                        ptrdiff_t offset, const ShmKey &id);

    /**
     * @brief Get the standby worker address.
     * @return The address pf standby worker, return "" if standby worker not found.
     */
    const std::string GetLocalStandbyWorker();

    /**
     * @brief Close expired UnixSockFd in cache.
     */
    void CloseExpiredUnixSockFd();

    /**
     * @brief Set available worker in response.
     * @param[in/out] rsp Heartbeat response.
     */
    template <typename Protobuf>
    void SetAvailableWorkers(Protobuf &rsp)
    {
        constexpr uint32_t num = 3;
        std::vector<std::string> activeWorkers;
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        Status status = membership_.GetSnapshot(snapshot);
        if (status.IsError()) {
            const int logDuration = 30;
            LOG_EVERY_T(ERROR, logDuration) << "Get available workers failed: " << status.ToString();
            return;
        }
        for (const auto *member : snapshot->ActiveMembers()) {
            if (member->identity.address != localAddress_.ToString()) {
                activeWorkers.emplace_back(member->identity.address);
            }
            if (activeWorkers.size() == num) {
                break;
            }
        }
        for (const auto &address : activeWorkers) {
            rsp.add_available_workers(address);
        }
    }

    HostPort masterAddr_;
    CommonServer *worker_;
    std::string workerStartId_;
    double timeoutMultiplier_;
    std::string sockPath_;
    int listenFd_;
    uint32_t shmWorkerPort_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    std::string workerUuid_;
    const cluster::MembershipEndpointView &membership_;  // Read-only view owned by WorkerOCServer's Engine.
    const std::atomic<bool> &localExiting_;              // WorkerOCServer-owned local admission gate.
    const worker::WorkerRuntimeFacade *runtime_{ nullptr };

    std::shared_timed_mutex mutex_;                           // for unboundedUnixSockFds_
    std::unordered_map<int, uint64_t> unboundedUnixSockFds_;  // This is the fd that is not bound to the client.
    const size_t maxCacheUnboundedUnixSockFdsCount_;
};
}  // namespace worker
}  // namespace datasystem
#endif
