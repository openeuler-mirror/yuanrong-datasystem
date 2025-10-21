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
 * Description: Defines the worker service processing delete process.
 */

#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_DELETE_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_DELETE_IMPL_H

#include "datasystem/utils/status.h"

#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"

namespace datasystem {
namespace object_cache {

class WorkerOcServiceDeleteImpl : public WorkerOcServiceCrudCommonApi {
public:
    WorkerOcServiceDeleteImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
        std::shared_ptr<AkSkManager> akSkManager, HostPort &localAddress,
        std::shared_ptr<WorkerOcServiceGetImpl> getProc);

    /**
     * @brief The rpc method used to delete the objects.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return Status of the call.
     */
    Status DeleteAllCopy(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp);

    Status DeleteCopyNotification(const DeleteObjectReqPb &req, DeleteObjectRspPb &rsp);

private:
    /**
     * @brief The implementation of DeleteAllCopy.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[out] deletedSize The size of  deleted objects.
     * @return Status of the call.
     */
    Status DeleteAllCopyImpl(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp, uint64_t &deletedSize);

    /**
     * @brief Delete the objects.
     * @param[in] objectKeys The object key list to be deleted.
     * @param[out] failedObjectKeys The failed object keys.
     * @param[out] deletedSize The size that has been deleted.
     * @return Status of the call.
     */
    Status DeleteAllCopyWithLock(const std::vector<std::string> &objectKeys,
        std::vector<std::string> &failedObjectKeys, uint64_t &deletedSize);

    /**
     * @brief Send request to master to delete meta and get response.
     *        Master will notify other workers to delete these objects asynchronously.
     * @param[in] needDeleteObjectKey The ids of the objects which will be delete on master.
     * @param[out] failedObjectKeys The failed object keys.
     * @return Status of the call.
     */
    Status DeleteAllCopyMetaFromMaster(const std::vector<std::string> &needDeleteObjectKey,
        std::unordered_set<std::string> &failedObjectKeys);

    /**
     * @brief Insert failed ids when delete failed.
     * @param rpcStatus The Rpc Status when delete object
     * @param recvRc The response status when delete object
     * @param failedObjectKeys The keys of failed objects.
     * @param needDeleteObjectKey The keys of need delete objects.
     * @param deleteRsp Delete response.
     * @return
     */
    Status InsertFailedId(Status &rpcStatus, Status &recvRc, std::unordered_set<std::string> &failedObjectKeys,
        const std::vector<std::string> &needDeleteObjectKey, master::DeleteAllCopyMetaRspPb &deleteRsp);

    Status DeleteObjectFromNotification(const std::string &objectKey, uint64_t version, bool async);

    /**
     * @brief Extract the cross az keys with offline master from objKeysGrpByMasterId.
     * @param[in,out] objKeysGrpByMasterId All keys group by master
     * @param[in,out] errInfos Keys that cannot find master or master is offline
     * @param[out] crossAzOfflineWorkerIdKeys The cross az keys with offline master.
     */
    void ExtractCrossAzOfflineWorkerIdKeyWithEmptyAddress(
        std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMasterId,
        std::unordered_map<std::string, Status> &errInfos,
        std::unordered_map<std::string, std::vector<std::string>> &crossAzOfflineWorkerIdKeys);

    /**
     * @brief Delete the cross az keys with offline master.
     * @param keys The cross az keys with offline master.
     */
    void DeleteCrossAzKeyWhenMasterFailed(const std::unordered_map<std::string, std::vector<std::string>> &keys);

    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };

    HostPort &localAddress_;

    std::shared_ptr<WorkerOcServiceGetImpl> getProc_{ nullptr };
};


}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_DELETE_IMPL_H