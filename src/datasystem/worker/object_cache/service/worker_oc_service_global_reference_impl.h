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
 * Description: Defines the worker service processing global reference process.
 */

#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_GLOBAL_REFERENCE_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_GLOBAL_REFERENCE_IMPL_H

#include "datasystem/common/util/net_util.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

namespace datasystem {
namespace object_cache {

class WorkerOcServiceGlobalReferenceImpl : public WorkerOcServiceCrudCommonApi {
public:
    WorkerOcServiceGlobalReferenceImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                                       std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable,
                                       std::shared_ptr<AkSkManager> akSkManager, HostPort &localAddress);

    /**
     * @brief Increase the global reference count of object.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp);

    /**
     * @brief Decrease the global reference count of object.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp);

    /**
     * @brief Send request to master to decrease all objects of remote client id.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    Status ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp);

    /**
     * @brief Bypass the request of query all objs global references in the cluster to master.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp);

    /**
     * @brief Decrease the global reference count of object.
     * @param[in] objectKeys The object key list to be decreased.
     * @param[in] clientId The client making this request.
     * @param[out] failDecIds The failed object keys.
     * @return Status of the call.
     */
    Status GDecreaseRefWithLock(const std::vector<std::string> &objectKeys, const ClientKey &clientId,
                                std::vector<std::string> &failDecIds);

    /**
     * @brief Gincrease master ref when scale down.
     * @param matchFunc[in] match function to find objects
     * @param masterAddr[in] standby worker of scale down worker.
     * @return Status
     */
    Status GIncreaseMasterRefWithLock(std::function<bool(const std::string &)> matchFunc, std::string masterAddr);

    /**
     * @brief Forward nested object reference increase calls to multiple masters based on objectKeys
     * @param[in] nestedObjectKeys Vector of objectkeys
     * @return Status of network
     */
    Status IncNestedRef(const std::vector<std::string> &nestedObjectKeys);

    /**
     * @brief Forward nested object reference decrease calls to multiple masters based on objectKeys
     * @param[in] nestedObjectKeys Vector of objectkeys
     * @return Status of network
     */
    Status DecNestedRef(const std::vector<std::string> &nestedObjectKeys, std::vector<std::string> &unaliveIds);

private:
    /**
     * @brief Decrease the reference count of client with the remote client id.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GIncreaseRefWithRemoteClientId(const GIncreaseReqPb &req, GIncreaseRspPb &resp);

    /**
     * @brief Send request to master to decrease the objects with the remote client id.
     * @param[in] objectKeys The object key list to be decreased.
     * @param[in] remoteClientId The remote client id.
     * @param[out] failDecIds The failed object keys.
     * @return Status of the call.
     */
    Status GDecreaseRefWithLockWithRemoteClientId(const std::vector<std::string> &objectKeys,
                                                  const std::string &remoteClientId,
                                                  std::vector<std::string> &failDecIds);

    /**
     * @brief Send request to master to increase the objects.
     * @param[in] req The rpc request protobuf.
     * @param[in] firstIncIds Object keys
     * @param[out] failIncIds Object keys that are failed to increase
     * @return Status of the call.
     */
    Status GIncreaseMasterRef(const GIncreaseReqPb &req, const std::vector<std::string> &firstIncIds,
                              std::vector<std::string> &failIncIds);

    /**
     * @brief GIncreaseMasterRef
     * @param[in] firstIncIds first increase worker ref object keys
     * @param[in] masteraddr master address
     * @param[out] failIncIds faileld object keys
     * @return Status
     */
    Status GIncreaseMasterRef(const std::vector<std::string> &firstIncIds, const HostPort &masteraddr,
                              std::vector<std::string> &failIncIds);

    /**
     * @brief Send request to master to decrease the objects.
     * @param[in] remoteClientId The remote client id.
     * @param[in] finishDecIds Object keys
     * @param[out] unAliveIds The object keys state is unKeep and gRef is 0, can be destroy.
     * @param[out] failDecIds Object keys that are failed to update
     * @return Status of the call.
     */
    Status GDecreaseMasterRef(const std::string &remoteClientId, const std::vector<std::string> &finishDecIds,
                              std::unordered_set<std::string> &unAliveIds, std::vector<std::string> &failDecIds);

    /**
     * @brief notify master when worker references an object for first time
     * @param[in] req The rpc request protobuf
     * @param[in] firstIncIds Object keys
     * @param[out] failIncIds Object keys that are failed to update
     */
    Status UpdateMasterForFirstIds(const GIncreaseReqPb &req, const std::vector<std::string> &firstIncIds,
                                   std::vector<std::string> &failIncIds);

    /**
     * @brief notify master when all references for an object in the worker are deleted
     * @param[in] clientId clients address
     * @param[in] finishDecIds Object keys
     * @param[out] unAliveIds The object keys state is unKeep and gRef is 0, can be destroy.
     * @param[out] failDecIds Object keys that are failed to update
     */
    Status UpdateMasterForFinishedIds(const ClientKey &clientId, const std::vector<std::string> &finishDecIds,
                                      std::unordered_set<std::string> &unAliveIds,
                                      std::vector<std::string> &failDecIds);

    /**
     * @brief Batch lock global reference lock for objects via objectKeys.
     * @param[in] objectKeys Object keys that need to be lock.
     * @param[in] insertable Insert if object key does not exist.
     * @param[out] lockedEntries Locked entries map.
     */
    void BatchGRefLock(const std::vector<std::string> &objectKeys, bool insertable,
                       std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries);

    /**
     * @brief Batch lock global reference lock for objects via objectKeys.
     * @param[in] lockedEntries Locked entries map that has been holds object locks.
     */
    static void BatchGRefLock(std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries);

    /**
     * @brief Unlock a batch of objects global reference lock.
     * @param[in] lockedEntries Locked entries that need to unlock.
     */
    static void BatchGRefUnlock(const std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries);

    /**
     * @brief Wait and redirect the GInc and GDec req if refs is moving
     * @param req Request of redirect
     * @param rsp Response of redirect
     * @param workerMasterApi worker master api
     * @param fun Create GInc or GDec to master.
     * @return
     */
    template <typename Req, typename Rsp>
    Status WaitForRedirectWhenRefMoving(Req &req, Rsp &rsp, std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                        std::function<Status(Req &, Rsp &)> fun,
                                        std::function<void(Rsp &, Rsp &)> mergeFun = [](Rsp &, Rsp &) {})
    {
        while (true) {
            CHECK_FAIL_RETURN_STATUS(fun != nullptr && mergeFun !=nullptr, K_RUNTIME_ERROR, "function is nullptr");
            Timer timer;
            RETURN_IF_NOT_OK(fun(req, rsp));
            auto elapsedMs = static_cast<uint64_t>(timer.ElapsedMilliSecondAndReset());
            workerOperationTimeCost.Append("RpcToMaster", elapsedMs);
            if (rsp.infos().empty() && !rsp.ref_is_moving()) {
                return Status::OK();
            }
            if (!rsp.infos().empty() && !rsp.ref_is_moving()) {
                for (auto &info : rsp.infos()) {
                    HostPort newMetaAddr;
                    RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(info.redirect_meta_address(), newMetaAddr));
                    LOG(INFO) << "obj ref has been migrated to the new master: " << newMetaAddr.ToString();
                    workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(newMetaAddr);
                    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                             "hash master get failed, RedirectRetryWhenMetaMoving failed");
                    *req.mutable_object_keys() = { info.change_meta_ids().begin(), info.change_meta_ids().end() };
                    req.set_redirect(false);
                    Rsp redirecRsp;
                    auto status = fun(req, redirecRsp);
                    RETURN_IF_NOT_OK(status);
                    mergeFun(redirecRsp, rsp);
                }
                elapsedMs = static_cast<uint64_t>(std::round(timer.ElapsedMilliSecond()));
                workerOperationTimeCost.Append("Redirect", elapsedMs);
                return Status::OK();
            }
            static const int sleepTimeMs = 200;
            rsp.Clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        }
    }

    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable_;

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };

    HostPort &localAddress_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_GLOBAL_REFERENCE_IMPL_H
