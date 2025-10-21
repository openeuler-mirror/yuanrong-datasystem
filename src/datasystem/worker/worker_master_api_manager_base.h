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
 * Description: Defines the API manager base class for worker to master API.
 */
#ifndef DATASYSTEM_WORKER_WORKER_MASTER_API_MANAGER_BASE_H
#define DATASYSTEM_WORKER_WORKER_MASTER_API_MANAGER_BASE_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
namespace worker {
template <class ApiType>
class WorkerMasterApiManagerBase {
public:
    WorkerMasterApiManagerBase(HostPort &hostPort, std::shared_ptr<AkSkManager> manager)
        : workerAddr_(hostPort), akSkManager_(manager){};

    /**
     * @brief Get or Create a worker to Master api object according to an identifier.
     * @param[in] id An identifier, can be an object key in OC scenario.
     * @param[in] etcdCm The cluster manager pointer to assign.
     * @param[out] api The WorkerMasterApi instance.
     * @return The status of this call.
     */
    virtual Status GetWorkerMasterApi(const std::string &id, EtcdClusterManager *etcdCm, std::shared_ptr<ApiType> &api)
    {
        CHECK_FAIL_RETURN_STATUS(etcdCm != nullptr, K_RUNTIME_ERROR,
                                 "ETCD manager is empty, get WorkerMasterApi failed");
        MetaAddrInfo metaAddrInfo;
        Timer timer;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCm->GetMetaAddress(id, metaAddrInfo), "GetMetaAddress failed");
        workerOperationTimeCost.Append("Get master address", timer.ElapsedMilliSecond());
        auto masterHostAddress = metaAddrInfo.GetAddressAndSaveDbName();

        VLOG(1) << FormatString("Get masterHostAddress:[%s] for identifier:[%s]", masterHostAddress.ToString(), id);
        return GetWorkerMasterApi(masterHostAddress, api);
    }

    /**
     * @brief Get or Create a worker to Master api object according to an identifier.
     * @param[in] id An identifier, can be an object key in OC scenario.
     * @param[in] etcdCm The cluster manager pointer to assign.
     * @return The WorkerMasterApi
     */
    virtual std::shared_ptr<ApiType> GetWorkerMasterApi(const std::string &id, EtcdClusterManager *etcdCm)
    {
        std::shared_ptr<ApiType> api;
        LOG_IF_ERROR(GetWorkerMasterApi(id, etcdCm, api), "GetWorkerMasterApi failed");
        return api;
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
        workerOperationTimeCost.Append("Worker master api init", timer.ElapsedMilliSecond());
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
     * @brief Get or Create a worker to Master api object for masterAddress.
     * @param[in] masterAddress The remote master ip address.
     * @param[in] etcdCm The cluster manager pointer to assign.
     * @param[out] api The WorkerMasterApi instance.
     * @return The status of this call.
     */
    virtual Status GetWorkerMasterApiByAddr(const std::string &masterAddress, EtcdClusterManager *etcdCm,
                                            std::shared_ptr<ApiType> &api)
    {
        CHECK_FAIL_RETURN_STATUS(etcdCm != nullptr, K_RUNTIME_ERROR,
                                 "ETCD manager is empty, get WorkerMasterApi failed");
        HostPort destAddr;
        std::string dbName;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCm->GetPrimaryReplicaLocationByAddr(masterAddress, destAddr, dbName),
                                         "GetPrimaryReplicaLocationByAddr failed");
        return GetWorkerMasterApi(destAddr, api);
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
        return RetryOnError(
            timeoutMs, [&f](int32_t) { return f(); }, []() { return Status::OK(); },
            { StatusCode::K_REPLICA_NOT_READY });
    }

protected:
    HostPort &workerAddr_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
};

}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_WORKER_MASTER_API_MANAGER_BASE_H
