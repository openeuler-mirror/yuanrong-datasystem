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
 * Description: Interface of worker common service processing main class.
 */
#ifndef DATASYSTEM_WORKER_WORKER_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_WORKER_SERVICE_IMPL_H

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/share_memory.service.rpc.pb.h"
#include "datasystem/server/common_server.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
namespace worker {
class WorkerServiceImpl : public WorkerService, public Callable {
public:
    /**
     * @brief Create a new WorkerServiceImpl object.
     * @param[in] serverAddr The address of worker.
     * @param[in] masterAddr The address of master.
     * @param[in] timeoutMult The value of timeout factor.
     * @param[in] worker The class of worker server.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] workerUuid The worker uuid.
     */
    WorkerServiceImpl(HostPort serverAddr, HostPort masterAddr, double timeoutMult, CommonServer *worker,
                      std::shared_ptr<AkSkManager> akSkManager, std::string workerUuid = "");

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
     * @param [in] pb The rpc request protobuf
     * @param [out] rspPb The rpc response protobuf
     * @return Status of the call
     */
    Status RegisterClient(const RegisterClientReqPb &pb, RegisterClientRspPb &rspPb) override;

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
     */
    void SetWorkerUuid(const std::string &uuid)
    {
        workerUuid_ = uuid;
    }

    /**
     * @brief Setter method for assigning cluster manager
     * @param[in] cm The pointer to etcd cluster manager
     */
    void SetClusterManager(EtcdClusterManager *cm)
    {
        etcdCM_ = cm;
    }

private:
    /**
     * @brief Get the standby worker address.
     * @return The address pf standby worker, return "" if standby worker not found.
     */
    const std::string GetStandbyWorker();

    /**
     * @brief Close expired UnixSockFd in cache.
     */
    void CloseExpiredUnixSockFd();

    /**
     * @brief Set available worker in response.
     * @param[in/out] rsp Heartbeat response.
     */
    template<typename Protobuf>
    void SetAvailableWorkers(Protobuf &rsp)
    {
        if (etcdCM_ == nullptr) {
            LOG_FIRST_N(ERROR, 1) << "[Heartbeat] etcd manager is null, cannot get available workers";
            return;
        }

        constexpr uint32_t num = 3;
        std::vector<std::string> activeWorkers;
        Status status = etcdCM_->GetActiveWorkers(num, activeWorkers);
        if (status.IsError()) {
            const int logDuration = 30;
            LOG_EVERY_T(ERROR, logDuration) << "Get available workers failed: " << status.ToString();
            return;
        }
        for (const auto &addr : activeWorkers) {
            rsp.add_available_workers(addr);
        }
    }

    HostPort masterAddr_;
    CommonServer *worker_;
    std::string workerStartId_;
    double timeoutMultiplier_;
    std::string sockPath_;
    int listenFd_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    std::string workerUuid_;
    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    std::shared_timed_mutex mutex_;                           // for unboundedUnixSockFds_
    std::unordered_map<int, uint64_t> unboundedUnixSockFds_;  // This is the fd that is not bound to the client.
    const size_t maxCacheUnboundedUnixSockFdsCount_;
};
}  // namespace worker
}  // namespace datasystem
#endif
