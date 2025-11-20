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
 * Description: Defines the worker service processing multi-publish process.
 */

#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_MULTI_PUBLISH_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_MULTI_PUBLISH_IMPL_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

namespace datasystem {
namespace object_cache {

class WorkerOcServiceMultiPublishImpl : public WorkerOcServiceCrudCommonApi {
public:
    /**
     * @brief Construct WorkerOcServiceMultiPublishImpl.
     * @param[in] initParam The parameter used to init WorkerOcServiceCrudCommonApi.
     * @param[in] etcdCM The cluster manager pointer to assign.
     * @param[in] memCpyThreadPool Used to copy data to memory.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] localAddress The local worker address.
     */
    WorkerOcServiceMultiPublishImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                                    std::shared_ptr<ThreadPool> memCpyThreadPool,
                                    std::shared_ptr<ThreadPool> threadPool, std::shared_ptr<AkSkManager> akSkManager,
                                    HostPort &localAddress);

    /**
     * @brief Handle multiply set request from the client.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status MultiPublish(const MultiPublishReqPb &req, MultiPublishRspPb &resp, std::vector<RpcMessage> &payloads);

private:
    using ObjGroupMap = std::unordered_map<MetaAddrInfo, std::vector<std::pair<std::string, size_t>>>;

    struct CreateMeta2PCRes {
        Status rc;
        master::CreateMultiMetaRspPb rsp;
        MetaAddrInfo metaAddrInfo;
    };

    /**
     * @brief The implementation of multiple publish.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[out] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status MultiPublishImpl(const MultiPublishReqPb &req, MultiPublishRspPb &resp, std::vector<RpcMessage> &payloads);

    /**
     * @brief Handle multiply set request from the client transaction.
     * @param[in] namespaceUri Object namespaceUri list that needs to be publish.
     * @param[in] entries Locked entry list
     * @param[in] ifInserts If the object insert to objectTable_.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status MultiPublishTx(std::vector<std::string> &namespaceUri, std::vector<std::shared_ptr<SafeObjType>> &entries,
                          std::vector<bool> &ifInserts, const MultiPublishReqPb &req,
                          std::vector<RpcMessage> &payloads);

    /**
     * @brief Handle multiply set request from the client not transaction.
     * @param[in] namespaceUri Object namespaceUri list that needs to be publish.
     * @param[in] entries Locked entry list
     * @param[in] ifInserts If the object insert to objectTable_.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status MultiPublishNtx(std::vector<std::string> &namespaceUri, std::vector<std::shared_ptr<SafeObjType>> &entries,
                           std::vector<bool> &ifInserts, const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                           std::vector<RpcMessage> &payloads);

    /**
     * @brief Publish newly objects. This function will publish entry and save data to cache.
     * @param[in] req The rpc request protobuf.
     * @param[in] objectKeys Object key list.
     * @param[out] entries The object entries.
     * @param[in] payloads Payloads for non-shared-memory cases.
     * @param[out] lastRc status of last failed object
     * @param[out] failedKeys The failed object keys.
     * @return Status of the call.
     */
    Status MultiPublishObjectNtx(const MultiPublishReqPb &req, std::vector<std::string> &objectKeys,
                                 std::vector<std::shared_ptr<SafeObjType>> &entries, std::vector<RpcMessage> &payloads,
                                 Status &lastRc, std::unordered_set<std::string> &failedKeys);

    /**
     * @brief Get the objects that need send to master.
     * @param[in] objectKeys The object keys.
     * @param[in] entries The object entries.
     * @param[in] failedKeys The failed object keys.
     * @param[in] outKeys The out keys.
     * @param[in] outEntries The out entries.
     * @return true If use outKeys and outEntries.
     */
    bool GetObjectsNeedToMaster(const std::vector<std::string> &objectKeys,
                                const std::vector<std::shared_ptr<SafeObjType>> &entries,
                                const std::unordered_set<std::string> &failedKeys, std::vector<std::string> &outKeys,
                                std::vector<std::shared_ptr<SafeObjType>> &outEntries);

    /**
     * @brief Publish newly objects. This function will publish entry and save data to cache.
     * @param[in] req The rpc request protobuf.
     * @param[in] objectKeys Object key list.
     * @param[out] entries The object entries.
     * @param[in] payloads Payloads for non-shared-memory cases.
     * @return Status of the call.
     */
    Status MultiPublishObject(const MultiPublishReqPb &req, std::vector<std::string> &objectKeys,
                              std::vector<std::shared_ptr<SafeObjType>> &entries, std::vector<RpcMessage> &payloads);

    /**
     * @brief Create or update metadata to master, object will be unlocked during requesting master.
     * @param[in] objectKeys Object key list.
     * @param[in] entries The object entries.
     * @param[in] pubReq The request of multipublish.
     * @param[out] versions The versions of objects
     * @return Status of the call.
     */
    Status CreateMultiMetaToCentralMaster(const std::vector<std::string> &objectKeys,
                                          const std::vector<std::shared_ptr<SafeObjType>> &entries,
                                          const MultiPublishReqPb &pubReq, std::vector<uint64_t> &versions);

    /**
     * @brief Construct the request info for create multiple meta.
     * @param[in] entry The object entry.
     * @param[in] pubReq The request of multipublish.
     * @param[out] req The multimeta request to construct.
     */
    void ConstructCreateReqCommon(SafeObjType &entry, const MultiPublishReqPb &pubReq,
                                  master::CreateMultiMetaReqPb &req);

    /**
     * @brief Construct the request info for create multiple meta.
     * @param[in] objectInfos Object info list.
     * @param[in] entries The object entries.
     * @param[in] pubReq The request of multipublish.
     * @param[out] req The multimeta request to construct.
     */
    void ConstructCreateReq(const std::vector<std::pair<std::string, size_t>> &objectInfos,
                            const std::vector<std::shared_ptr<SafeObjType>> &entries, const MultiPublishReqPb &pubReq,
                            master::CreateMultiMetaReqPb &req);

    /**
     * @brief Construct the request info for create multiple meta.
     * @param[in] objectKeys Object key list.
     * @param[in] entries The object entries.
     * @param[in] pubReq The request of multipublish.
     * @param[out] req The multimeta request to construct.
     */
    void ConstructCreateReq(const std::vector<std::string> &objectKeys,
                            const std::vector<std::shared_ptr<SafeObjType>> &entries, const MultiPublishReqPb &pubReq,
                            master::CreateMultiMetaReqPb &req);
    /**
     * @brief Create or update metadata to master, object will be unlocked during requesting master.
     * @param[in] objectKeys Object key list.
     * @param[in] entries The object entries.
     * @param[in] pubReq The request of multipublish.
     * @param[out] resp responese info of CreateMultiMeta
     * @param[out] versions The versions of objects.
     * @return Status of the call.
     */
    Status CreateMultiMetaToDistributedMasterNtx(const std::vector<std::string> &objectKeys,
                                                 const std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                 const MultiPublishReqPb &pubReq, master::CreateMultiMetaRspPb &resp,
                                                 std::vector<uint64_t> &versions);

    /**
     * @brief Fill multimeta request.
     * @param[in] objectKeys Object key list.
     * @param[in] entries The object entries.
     * @param[in] pubReq The request of multipublish.
     * @param[out] req The multimeta request to fill.
     */
    void FillMultiMetaReqPhaseOne(const std::vector<std::pair<std::string, size_t>> &objectKeys,
                                  const std::vector<std::shared_ptr<SafeObjType>> &entries,
                                  const MultiPublishReqPb &pubReq, master::CreateMultiMetaReqPb &req);

    /**
     * @brief Create multimeta request to master.
     * @param[in] objectKeys Object key list.
     * @param[in] entries The object entries.
     * @param[in] pubReq The request of multipublish.
     * @param[out] versions The versions of objects.
     * @return Status of the call.
     */
    Status CreateMultiMetaToDistributedMaster(const std::vector<std::string> &objectKeys,
                                              const std::vector<std::shared_ptr<SafeObjType>> &entries,
                                              const MultiPublishReqPb &pubReq, std::vector<uint64_t> &versions);

    /**
     * @brief Create multimeta request to master in parallel.
     * @param[in] masterAddrs The master addrs.
     * @param[in] reqs The CreateMultiMeta requests.
     * @param[out] respRes The response list of create.
     * @return Status of the call.
     */
    void CreateMultiMetaParallel(const std::vector<MetaAddrInfo> &masterAddrs,
                                 std::vector<master::CreateMultiMetaReqPb> &reqs,
                                 std::vector<CreateMeta2PCRes> &respRes);

    /**
     * @brief Create multimeta request to master in parallel.
     * @param[in] objGroup The group of objects.
     * @param[in] addrs The master addrs.
     * @param[in] reqs The CreateMultiMeta requests.
     * @return Status of the call.
     */
    Status CreateMultiMetaParallel(const ObjGroupMap &objGroup, const std::vector<MetaAddrInfo> &addrs,
                                   std::vector<master::CreateMultiMetaReqPb> &reqs);

    /**
     * @brief Create multimeta request to master in serial.
     * @param[in] objGroup The group of objects.
     * @param[in] addrs The master addrs.
     * @param[in] reqs The CreateMultiMeta requests.
     * @return Status of the call.
     */
    Status CreateMultiMetaSerial(const ObjGroupMap &objGroup, const std::vector<MetaAddrInfo> &addrs,
                                 std::vector<master::CreateMultiMetaReqPb> &reqs);

    /**
     * @brief Create multimeta phase one request to master.
     * @param[in] objGroup The group of objects.
     * @param[in] entries The object entries.
     * @param[in] pubReq The request of multipublish.
     * @return Status of the call.
     */
    Status CreateMultiMetaPhaseOne(const ObjGroupMap &objGroup,
                                   const std::vector<std::shared_ptr<SafeObjType>> &entries,
                                   const MultiPublishReqPb &pubReq);

    /**
     * @brief Create multimeta phase two request to master.
     * @param[in] objGroup The group of objects.
     * @param[in] pubReq The request of multipublish.
     * @param[out] versions The versions of objects.
     * @return Status of the call.
     */
    Status CreateMultiMetaPhaseTwo(const ObjGroupMap &objGroup, const MultiPublishReqPb &pubReq,
                                   std::vector<uint64_t> &versions);

    /**
     * @brief Process 2PC results.
     * @param[in] futures The 2PC request futures.
     * @param[in] objGroup The group of objects.
     * @param[out] versions The versions of objects.
     * @return Status of the call.
     */
    Status Process2PCResults(std::vector<std::future<CreateMeta2PCRes>> &futures, const ObjGroupMap &objGroup,
                             std::vector<uint64_t> &versions);

    /**
     * @brief Rollback metadata request to master.
     * @param[in] addrs The master addrs.
     * @param[in] objGroup The group of objects.
     */
    void RollbackMultiMetaReq(std::vector<MetaAddrInfo> &addrs, const ObjGroupMap &objGroup);

    /**
     * @brief Retry rollback metadata request when meta moving.
     * @param[in] metaAddrInfo The master addr.
     * @param[in] req The RollbackMultiMeta request.
     * @param[out] rsp The responese info of RollbackMultiMeta.
     * @return Status of the call.
     */
    Status RetryRollbackMultiMetaWhenMoving(const MetaAddrInfo &metaAddrInfo, master::RollbackMultiMetaReqPb &req,
                                            master::RollbackMultiMetaRspPb &rsp);

    /**
     * @brief Retry create multimeta request when meta moving.
     * @param[in] api The worker master api.
     * @param[in] req The CreateMultiMeta request.
     * @param[out] rsp The responese info of CreateMultiMeta.
     * @return Status of the call.
     */
    Status RetryCreateMultiMetaWhenMoving(std::shared_ptr<worker::WorkerMasterOCApi> api,
                                          master::CreateMultiMetaReqPb &req, master::CreateMultiMetaRspPb &rsp);

    /**
     * @brief Retry create multimeta phase two request when meta moving.
     * @param[in] api The worker master api.
     * @param[in] req The CreateMultiMeta request.
     * @param[out] rsp The responese info of CreateMultiMeta.
     * @return Status of the call.
     */
    Status RetryCreateMultiMetaPhaseTwoWhenMoving(std::shared_ptr<worker::WorkerMasterOCApi> api,
                                                  master::CreateMultiMetaPhaseTwoReqPb &req,
                                                  master::CreateMultiMetaRspPb &rsp);
    /**
     * @brief Fill the entry and save object to L2 cache if success to create meta.
     * @param[in] objectKeys Object key list.
     * @param[in] objectEntries The object entries.
     * @param[in] versions The versions of objects.
     * @param[in] failedKeys The failed object keys.
     * @return K_OK on success; the error code otherwise.
     */
    void UpdateObjectAfterCreatingMeta(const std::vector<std::string> &objectKeys,
                                       const std::vector<std::shared_ptr<SafeObjType>> &objectEntries,
                                       const std::vector<uint64_t> &versions,
                                       const std::unordered_set<std::string> &failedKeys = {});

    /**
     * @brief Publish newly objects. This function will publish entry and save data to cache.
     * @param[in] req The rpc request protobuf.
     * @param[in] objectKeys Object key list.
     * @param[in] failedKeys The failed object keys.
     * @param[out] objectEntries The object entries.
     * @param[out] lastRc status of last failed object
     * @return Status of the call.
     */
    Status SendToMasterAndUpdateObject(const MultiPublishReqPb &req, std::vector<std::string> &objectKeys,
                                       std::unordered_set<std::string> &failedKeys,
                                       std::vector<std::shared_ptr<SafeObjType>> &objectEntries, Status &lastRc);

    /**
     * @brief Batch lock for multiple set via object keys.
     * @param[in] objectKeys Object key list that needs to be locked.
     * @param[out] isInserts If the entry is newly inserted.
     * @param[out] entries Locked entry list
     * @param[out] successIndex success index of object list
     */
    Status BatchLockForSet(const std::vector<std::string> &objectKeys, std::vector<bool> &isInserts,
                           std::vector<std::shared_ptr<SafeObjType>> &entries, std::vector<size_t> &successIndex);

    /**
     * @brief Batch unlock for multiple set via lockedEntries.
     * @param[in] lockedEntries Locked entry list.
     * @param[in] successIndex success index of success object for lock
     */
    static void BatchUnlockForSet(std::vector<std::shared_ptr<SafeObjType>> &entries,
                                  const std::vector<size_t> &successIndex);

    /**
     * @brief Release memory and resource if failing to publish.
     * @param[in] objectKeys Object key list.
     * @param[in] ifInserts If the object insert to objectTable_.
     * @param[out] entries The object entries.
     */
    void BatchRollBackEntries(const std::vector<std::string> &objectKeys, const std::vector<bool> &ifInserts,
                              std::vector<std::shared_ptr<SafeObjType>> &entries);

    /**
     * @brief Release memory and resource if failing to publish.
     * @param[in] objectKeys Object key list.
     * @param[in] ifInserts If the object insert to objectTable_.
     * @param[in] entries The object entries.
     * @param[in] failedKeys The failed object keys.
     * @param[out] resp MultiPublishRspPb info.
     */
    void BatchRollBackEntries(const std::vector<std::string> &objectKeys, const std::vector<bool> &ifInserts,
                              std::vector<std::shared_ptr<SafeObjType>> &entries,
                              const std::unordered_set<std::string> &failedKeys, MultiPublishRspPb &resp);

    /**
     * @brief Release memory and resource if failing to publish.
     * @param[in] objectKey Object key.
     * @param[in] ifInsert If the object insert to objectTable_.
     * @param[out] entry The object entries.
     */
    void BatchRollBackEntriesImpl(const std::string &objectKey, bool ifInsert, std::shared_ptr<SafeObjType> &entry);

    /**
     * @brief Batch lock for multiple set via object keys.
     * @param[in] objectKeys Object key list that needs to be locked.
     * @param[in] existence object enable existence or not.
     * @param[out] isInserts If the entry is newly inserted.
     * @param[out] entries Locked entry list
     */
    Status BatchLockForSetNtx(const std::vector<std::string> &objectKeys, const ExistenceOptPb &existence,
                               std::vector<bool> &isInserts, std::vector<std::shared_ptr<SafeObjType>> &entries);
    /**
     * @brief Verify the validity of the object release.
     * @param[in] req Publish request meta.
     * @param[in] safeObj The object to be sealed.
     * @return Status of the call.
     */
    static Status VerifyObjectReleaseValidity(const MultiPublishReqPb &req, const SafeObjType &safeObj);

    /**
     * @brief Verify objects and roll back if necessary.
     * @param[in] req The rpc request protobuf.
     * @param[in] objectKeys Object key list.
     * @param[in] ifInserts If the object insert to objectTable_.
     * @param[out] entries The object entries.
     * @return K_OK on success; the error code otherwise.
     */
    Status VerifyObjectsAndRollBackIfNeedTx(const MultiPublishReqPb &req, const std::vector<std::string> &objectKeys,
                                            const std::vector<bool> &ifInserts,
                                            std::vector<std::shared_ptr<SafeObjType>> &entries);
    /**
     * @brief Verify objects in a transaction.
     * @param[in] req The rpc request protobuf.
     * @param[in] objectKeys Object key list.
     * @param[in] entries The object entries.
     * @param[out] failedKeys The failed object keys.
     */
    void VerifyObjectsNtx(const MultiPublishReqPb &req, const std::vector<std::string> &objectKeys,
                          std::vector<std::shared_ptr<SafeObjType>> &entries,
                          std::unordered_set<std::string> &failedKeys);

    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    std::shared_ptr<ThreadPool> memCpyThreadPool_{ nullptr };

    std::shared_ptr<ThreadPool> threadPool_{ nullptr };

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };

    HostPort &localAddress_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_MULTI_PUBLISH_IMPL_H