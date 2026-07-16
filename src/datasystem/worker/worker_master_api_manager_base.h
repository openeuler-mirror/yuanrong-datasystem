/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the API manager base class for worker to master API.
 */
#ifndef DATASYSTEM_WORKER_WORKER_MASTER_API_MANAGER_BASE_H
#define DATASYSTEM_WORKER_WORKER_MASTER_API_MANAGER_BASE_H

#include <memory>
#include <string_view>
#include <utility>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/worker/metadata_route_resolver.h"

namespace datasystem {
namespace worker {
template <class ApiType>
class WorkerMasterApiManagerBase {
public:
    /**
     * @brief Construct a manager with its metadata resolver bound once.
     * @param[in] hostPort Local Worker address that outlives this manager.
     * @param[in] manager Shared authentication manager.
     * @param[in] routeResolver Metadata resolver that outlives this manager.
     */
    WorkerMasterApiManagerBase(HostPort &hostPort, std::shared_ptr<AkSkManager> manager,
                               const MetadataRouteResolver &routeResolver)
        : workerAddr_(hostPort), akSkManager_(std::move(manager)), routeResolver_(routeResolver)
    {
    }

    /**
     * @brief Release non-owned route dependencies and shared authentication state.
     */
    virtual ~WorkerMasterApiManagerBase() = default;

    /**
     * @brief Get or create a Worker-to-Master API according to a metadata key.
     * @param[in] key Binary-safe metadata placement key.
     * @param[out] api WorkerMasterApi instance; unchanged when owner resolution fails.
     * @return K_OK or metadata owner/API initialization status.
     */
    Status GetWorkerMasterApi(std::string_view key, std::shared_ptr<ApiType> &api)
    {
        HostPort owner;
        RETURN_IF_NOT_OK(routeResolver_.ResolveOwner(key, owner));
        return GetWorkerMasterApi(owner, api);
    }

    /**
     * @brief Create a worker to Master api object for masterAddress, needs to be implemented by derived class.
     * @param[in] masterAddress The remote master ip address.
     * @return The WorkerMasterApi
     */
    virtual std::shared_ptr<ApiType> CreateWorkerMasterApi(const HostPort &masterAddress) = 0;

    /**
     * @brief Get or Create a worker to Master api object for masterAddress.
     * @param[in] masterAddress The remote master ip address.
     * @param[out] api The WorkerMasterApi instance.
     * @return The status of this call.
     */
    virtual Status GetWorkerMasterApi(const HostPort &masterAddress, std::shared_ptr<ApiType> &api)
    {
        CHECK_FAIL_RETURN_STATUS(!masterAddress.Empty(), K_RUNTIME_ERROR,
                                 "masterAddr is empty, get WorkerMasterApi failed");
        // Create Master API object for first time if not exists
        Timer timer;
        auto workerMasterApi = CreateWorkerMasterApi(masterAddress);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApi->Init(),
                                         "Master addr " + masterAddress.ToString() + " workerMasterApi init failed");
        api = std::move(workerMasterApi);
        GetWorkerTimeCost().Append("Worker master api init", timer.ElapsedMilliSecond());
        return Status::OK();
    }

    /**
     * @brief Get or Create a worker to Master api object for masterAddress.
     * @param[in] masterAddress The remote master ip address.
     * @return The WorkerMasterApi
     */
    virtual std::shared_ptr<ApiType> GetWorkerMasterApi(const HostPort &masterAddress)
    {
        std::shared_ptr<ApiType> api;
        LOG_IF_ERROR(GetWorkerMasterApi(masterAddress, api), "GetWorkerMasterApi failed");
        return api;
    }

    /**
     * @brief erase failed worker master api.
     * @param[in] masterAddr failed master addr.
     */
    virtual void EraseFailedWorkerMasterApi(HostPort &masterAddr, StubType type)
    {
        if (masterAddr.Empty()) {
            return;
        }
        RpcStubCacheMgr::Instance().Remove(masterAddr, type);
    }

    template <typename F>
    static Status RetryForReplicaNotReady(int32_t timeoutMs, F &&f)
    {
        // Retry the transient RPC errors seen while a peer worker is restarting.
        // brpc POOLED channels have a short window after a worker restart where the
        // cached socket is dead but not yet revived by brpc health-check (E112
        // EHOSTDOWN -> K_RPC_UNAVAILABLE); brpc pure-timeout fallbacks surface as
        // K_RPC_CANCELLED. Like K_REPLICA_NOT_READY, these are recoverable by retry
        // once brpc revives the socket (AfterRevived). Without retrying them, the
        // first E112 breaks RetryOnError (rpc_util.h:127) and the caller waits out
        // the full deadline with 0 retries instead of recovering in ~3s.
        return RetryOnError(
            timeoutMs, [&f](int32_t) { return f(); }, []() { return Status::OK(); },
            { StatusCode::K_REPLICA_NOT_READY, StatusCode::K_RPC_UNAVAILABLE,
              StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED });
    }

protected:
    HostPort &workerAddr_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    // Required non-owning resolver used by the key-only route; its owner must outlive this manager.
    const MetadataRouteResolver &routeResolver_;
};

}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_WORKER_MASTER_API_MANAGER_BASE_H
