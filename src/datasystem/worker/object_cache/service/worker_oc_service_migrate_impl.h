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
 * Description: Defines the worker service processing publish process.
 */

#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_MIGRATE_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_MIGRATE_IMPL_H

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <future>

#include <google/protobuf/repeated_field.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/data_migrator/basic/migrate_data_limiter.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

namespace datasystem {
namespace object_cache {

using ObjInfoPbList = google::protobuf::RepeatedPtrField<MigrateDataReqPb::ObjectInfoPb>;
using ObjInfoPbListDirect = google::protobuf::RepeatedPtrField<MigrateDataDirectReqPb::ObjectInfoPb>;
using ObjectInfoMap = std::unordered_map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>>;
using QueryMetaMap = std::unordered_map<std::string, master::QueryMetaInfoPb>;
using LockedEntryMap = std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, uint64_t>>;
using RedirectMap =
    std::unordered_map<std::string, google::protobuf::RepeatedPtrField<master::ReplacePrimaryReqPb::ObjectInfoPb>>;

class WorkerOcServiceMigrateImpl : public WorkerOcServiceCrudCommonApi,
                                   std::enable_shared_from_this<WorkerOcServiceMigrateImpl> {
public:
    /**
     * @brief Construct WorkerOcServicePublishImpl.
     * @param[in] initParam The parameter used to init WorkerOcServiceCrudCommonApi.
     * @param[in] etcdCM The cluster manager pointer to assign.
     * @param[in] memcpyThreadPool Memory copy thread pool.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] localAddr Local worker address.
     */
    WorkerOcServiceMigrateImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                               std::shared_ptr<ThreadPool> memcpyThreadPool, std::shared_ptr<AkSkManager> akSkManager,
                               const std::string &localAddr);

    /**
     * @brief Migrate data.
     * @param[in] req Migrate data request.
     * @param[out] rsp Migrate data response.
     * @param[in] payloads Object data.
     * @return K_OK on success, the error otherwise.
     */
    Status MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp, std::vector<RpcMessage> payloads);

    /**
     * @brief Migrate data directly.
     * @param[in] req Migrate data direct request.
     * @param[out] rsp Migrate data direct response.
     * @return Status of the call.
     */
    Status MigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp);

#ifdef WITH_TESTS
public:
#else
private:
#endif
    /**
     * @brief Batch lock objects.
     * @param[in] infoList Object info list.
     * @param[out] lockedEntries Locked object list.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @param[out] needModifyPrimary Need modify primary copy list.
     */
    template <typename T>
    void BatchLockForMigrateData(const T &infoList, LockedEntryMap &lockedEntries,
                                 std::unordered_set<std::string> &successIds,
                                 std::unordered_set<std::string> &failedIds,
                                 LockedEntryMap &needModifyPrimary)
    {
        lockedEntries.clear();
        std::map<std::string, uint64_t> toLockIds;
        std::transform(infoList.begin(), infoList.end(), std::inserter(toLockIds, toLockIds.end()),
                       [](const auto &info) { return std::make_pair(info.object_key(), info.version()); });

        BatchLock(toLockIds, lockedEntries, successIds, failedIds, needModifyPrimary);
    }

    /**
     * @brief Batch lock objects.
     * @param[in] toLockIds Object key and version map.
     * @param[out] lockedEntries Locked object list.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @param[out] needModifyPrimary Need modify primary copy list.
     */
    void BatchLock(const std::map<std::string, uint64_t> &toLockIds, LockedEntryMap &lockedEntries,
                   std::unordered_set<std::string> &successIds, std::unordered_set<std::string> &failedIds,
                   LockedEntryMap &needModifyPrimary);

    /**
     * @brief Batch unlock objects.
     * @param[in] lockedEntries Locked object list.
     */
    void BatchUnlock(const LockedEntryMap &lockedEntries)
    {
        for (auto &entry : lockedEntries) {
            entry.second.first->WUnlock();
        }
    }

    /**
     * @brief Query metadat from master.
     * @param[in] objectKeys Need query object key list.
     * @param[out] queryMetas Query meta list.
     * @param[out] failedIds Failed object key list.
     * @return K_OK on success, the error otherwise.
     */
    Status QueryMasterMetadata(const std::unordered_set<std::string> &objectKeys, QueryMetaMap &queryMetas,
                               std::unordered_set<std::string> &failedIds);

    /**
     * @brief Fill objects in lock state.
     * @param[in] req Migrate data request.
     * @param[in] lockedEntries Locked object list.
     * @param[in] metas Query meta list.
     * @param[in] payloads Object datas.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @param[out] needSendMasterIds Need send master object keys.
     * @return K_OK on success, the error otherwise.
     */
    Status FillObjectsLocked(const MigrateDataReqPb &req, LockedEntryMap &lockedEntries, const QueryMetaMap &metas,
                             std::vector<RpcMessage> &payloads, std::unordered_set<std::string> &successIds,
                             std::unordered_set<std::string> &failedIds, ObjectInfoMap &needSendMasterIds);

    /**
     * @brief Fill one object data in lock state.
     * @param[in] entry Object entry.
     * @param[in] info Object info.
     * @param[in] meta Metadata query from master.
     * @param[in] payloads Data.
     * @param[in] type Migrate type.
     * @param[out] needSendMasterIds Need send master object keys.
     * @return K_OK on success, the error otherwise.
     */
    Status FillOneObjectLocked(std::shared_ptr<SafeObjType> &entry, const MigrateDataReqPb::ObjectInfoPb &info,
                               const master::QueryMetaInfoPb &meta, std::vector<RpcMessage> &payloads,
                               const MigrateType &type, ObjectInfoMap &needSendMasterIds);

    /**
     * @brief Fill metadata to object entries.
     * @param[in] lockedEntries Locked object list.
     * @param[in] metas Query meta list.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @param[out] needReadDataIds Need read data object keys.
     */
    void FillMetaToObjectEntries(LockedEntryMap &lockedEntries, const QueryMetaMap &metas,
                                 std::unordered_set<std::string> &successIds,
                                 std::unordered_set<std::string> &failedIds, ObjectInfoMap &needReadDataIds);

    /**
     * @brief Fill data to object entries.
     * @param[in] req Migrate data direct request.
     * @param[in] needReadDataIds Need read data object keys.
     * @param[out] needSendMasterIds Need send master object keys.
     * @param[out] failedIds Failed object key list.
     * @return K_OK on success, the error otherwise.
     */
    Status FillDataToObjectEntries(const MigrateDataDirectReqPb &req, const ObjectInfoMap &needReadDataIds,
                                   ObjectInfoMap &needSendMasterIds, std::unordered_set<std::string> &failedIds);

    struct ReadTask {
        std::string objectKey;
        std::vector<uint64_t> eventKeys;
        std::shared_ptr<ShmUnit> shmUnit;
        std::shared_ptr<SafeObjType> entry;
        bool isNewCreate{ false };
    };

    /**
     * @brief Start remote read tasks.
     * @param[in] req Migrate data direct request.
     * @param[in] needReadDataIds Need read data object keys.
     * @param[in] shmIndexMapping The object id to shmOwners index mapping.
     * @param[in] shmOwners The allocated shared memory chunks.
     * @param[out] tasks Remote read tasks.
     * @param[out] failedIds Failed object key list.
     * @return K_OK on success, the error otherwise.
     */
    Status StartRemoteReadTasks(const MigrateDataDirectReqPb &req, const ObjectInfoMap &needReadDataIds,
                                const std::vector<uint32_t> &shmIndexMapping,
                                const std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                std::vector<ReadTask> &tasks, std::unordered_set<std::string> &failedIds);

    /**
     * @brief Get shared memory owner by index.
     * @param[in] idx Object index.
     * @param[in] shmIndexMapping The object id to shmOwners index mapping.
     * @param[in] shmOwners The allocated shared memory chunks.
     * @return Shared memory owner or nullptr.
     */
    std::shared_ptr<ShmOwner> GetShmOwnerByIndex(int idx, const std::vector<uint32_t> &shmIndexMapping,
                                                  const std::vector<std::shared_ptr<ShmOwner>> &shmOwners) const;

    /**
     * @brief Process remote read for a single object.
     * @param[in] object Object info.
     * @param[in] needReadIt Iterator to need read data entry.
     * @param[in] shmUnit Shared memory unit.
     * @param[in] metaSize Metadata size.
     * @param[out] tasks Remote read tasks.
     * @param[out] failedIds Failed object key list.
     * @return K_OK on success, the error otherwise.
     */
    Status ProcessRemoteReadForObject(const MigrateDataDirectReqPb::ObjectInfoPb &object,
                                      ObjectInfoMap::const_iterator needReadIt,
                                      std::shared_ptr<ShmUnit> shmUnit, size_t metaSize,
                                      std::vector<ReadTask> &tasks,
                                      std::unordered_set<std::string> &failedIds);

    /**
     * @brief Wait remote read tasks.
     * @param[in,out] tasks Remote read tasks.
     * @param[out] needSendMasterIds Need send master object keys.
     * @param[out] failedIds Failed object key list.
     * @return K_OK on success, the error otherwise.
     */
    Status WaitRemoteReadTasks(std::vector<ReadTask> &tasks, ObjectInfoMap &needSendMasterIds,
                               std::unordered_set<std::string> &failedIds);

    /**
     * @brief Helper function for aggregate allocation.
     * @param[in] req Migrate data direct request.
     * @param[in] needReadDataIds Need read data object keys.
     * @param[out] shmOwners The allocated shared memory chunks.
     * @param[out] shmIndexMapping The object id to shmOwners index mapping.
     */
    Status AggregateAllocateHelper(const MigrateDataDirectReqPb &req, const ObjectInfoMap &needReadDataIds,
                                   std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                   std::vector<uint32_t> &shmIndexMapping);

    /**
     * @brief Notify master to replace object primary copy.
     * @param[in] originAddr Original primary worker address.
     * @param[in] needSendMasterIds Need replace primary copy objects.
     * @param[in] type Migrate type.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @return K_OK on success, the error otherwise.
     */
    Status ReplacePrimaryImpl(const std::string &originAddr, const ObjectInfoMap &needSendMasterIds,
                              const MigrateType &type, std::unordered_set<std::string> &successIds,
                              std::unordered_set<std::string> &failedIds);

    /**
     * @brief Calculate remain bytes according to migrate type.
     * @param[in] type Migrate type.
     * @return The remain bytes.
     */
    uint64_t CalcRemainBytes(const MigrateType &type);

    /**
     * @brief Fill migrate data response.
     * @param[in] req Migrate data request.
     * @param[in] successIds Success object key list.
     * @param[in] failedIds Failed object key list.
     * @param[in] oom Indicate is OOM or not.
     * @param[out] rsp Migrate data response.
     */
    void FillMigrateDataResponse(const MigrateDataReqPb &req, const std::unordered_set<std::string> &successIds,
                                 const std::unordered_set<std::string> &failedIds, bool oom, MigrateDataRspPb &rsp);

    /**
     * @brief Fill migrate data direct response.
     * @param[in] failedIds Failed object key list.
     * @param[in] oom Indicate is OOM or not.
     * @param[out] rsp Migrate data direct response.
     */
    void FillMigrateDataDirectResponse(const std::unordered_set<std::string> &failedIds, bool oom,
                                       MigrateDataDirectRspPb &rsp);

    /**
     * @brief For test mock purpose.
     * @param[in] api Worker master api.
     * @param[in] req Pure query meta request.
     * @param[out] rsp Pure query meta response.
     * @return K_OK on success, the error otherwise.
     */
    Status PureQueryMetaOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::PureQueryMetaReqPb &req,
                             master::PureQueryMetaRspPb &rsp);

    /**
     * @brief Pure query meta with retry.
     * @param[in] api Worker master api.
     * @param[in] req Pure query meta request.
     * @param[out] rsp Pure query meta response.
     * @return K_OK on success, the error otherwise.
     */
    Status PureQueryMetaRetry(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::PureQueryMetaReqPb &req,
                              master::PureQueryMetaRspPb &rsp);

    /**
     * @brief Pure query meta to redirect master.
     * @param[in] redirectIds Need redirect object keys.
     * @param[out] queryMetas Query metas.
     * @param[out] failedIds Failed object keys.
     * @return K_OK on success, the error otherwise.
     */
    Status PureQueryMetaToRedirectMaster(
        const std::unordered_map<std::string, std::unordered_set<std::string>> &redirectIds, QueryMetaMap &queryMetas,
        std::unordered_set<std::string> &failedIds);

    /**
     * @brief Save data to object entry in lock state.
     * @param[in] entry Object entry.
     * @param[in] info Object info.
     * @param[in] payloads Data.
     * @param[in] type Migrate type.
     * @return K_OK on success, the error otherwise.
     */
    Status SaveDataWithObjectLocked(std::shared_ptr<SafeObjType> &entry, const MigrateDataReqPb::ObjectInfoPb &info,
                                    std::vector<RpcMessage> &payloads, const MigrateType &type);

    /**
     * @brief Allocate memory and assign data to object.
     * @param[in] objectKey Object key.
     * @param[in] entry Object entry.
     * @param[in] payloads Data payload.
     * @param[in] size Data size.
     * @return K_OK on success, the error otherwise.
     */
    Status AllocateAndAssignData(const std::string &objectKey, std::shared_ptr<SafeObjType> &entry,
                                 const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads, uint64_t size);

    /**
     * @brief For test mock purpose.
     * @param[in] api Worker master api.
     * @param[in] req Replace primary request.
     * @param[out] rsp Replace primary response.
     * @return K_OK on success, the error otherwise.
     */
    Status ReplacePrimaryOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::ReplacePrimaryReqPb &req,
                              master::ReplacePrimaryRspPb &rsp);

    /**
     * @brief Replace objects primary copy with retry.
     * @param[in] api Worker master api.
     * @param[in] req Replace primary request.
     * @param[out] rsp Replace primary response.
     * @return K_OK on success, the error otherwise.
     */
    Status ReplacePrimaryRetry(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::ReplacePrimaryReqPb &req,
                               master::ReplacePrimaryRspPb &rsp);

    /**
     * @brief Process via replace primary response.
     * @param[in] rsp Replace primary response.
     * @param[in] needSendMasterIds Need replace primary copy objects.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @param[out] needRedirectIds Need redirect objects.
     */
    void ProcessReplacePrimaryRsp(master::ReplacePrimaryRspPb &rsp, const ObjectInfoMap &needSendMasterIds,
                                  std::unordered_set<std::string> &successIds,
                                  std::unordered_set<std::string> &failedIds, RedirectMap &needRedirectIds);

    /**
     * @brief Replace objects primary copy to redirect master.
     * @param[in] originAddr Original primary worker address.
     * @param[in] needRedirectIds Need redirect objects.
     * @param[in] needSendMasterIds Need replace primary copy objects.
     * @param[out] successIds Success object key list.
     * @param[out] failedIds Failed object key list.
     * @return K_OK on success, the error otherwise.
     */
    Status ReplacePrimaryToRedirectMaster(const std::string &originAddr, const RedirectMap &needRedirectIds,
                                          const ObjectInfoMap &needSendMasterIds,
                                          std::unordered_set<std::string> &successIds,
                                          std::unordered_set<std::string> &failedIds);

    /**
     * @brief Rollback the objects.
     * @param[in] objectKeys Need rollback object keys.
     * @param[in] objectInfos Object infos.
     */
    template <typename Container>
    void RollbackObjects(const Container &objectKeys, const ObjectInfoMap &objectInfos);

    /**
     * @brief Get worker master api.
     * @return Worker master api.
     */
    std::shared_ptr<worker::WorkerMasterOCApi> GetWorkerMasterApi(const HostPort &masterAddr)
    {
        return workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    }

    /**
     * @brief Indicate the object is equal to the provided version or not.
     * @param[in] entry Object entry.
     * @param[in] version Version to compare.
     * @return True if object is newer than the provided version
     */
    bool IsEqualVersion(const std::shared_ptr<SafeObjType> &entry, uint64_t version);

    /**
     * @brief Indicate the object is newer than the provided version or not.
     * @param[in] entry Object entry.
     * @param[in] version Version to compare.
     * @return True if object is newer than the provided version
     */
    bool IsNewerVersion(const std::shared_ptr<SafeObjType> &entry, uint64_t version);

    /**
     * @brief Indicate the obejct is new created object or not.
     * @param[in] entry Object entry.
     * @return True if obejct is new created.
     */
    bool IsNewCreatedObject(std::shared_ptr<SafeObjType> &entry) const;

    /**
     * @brief Indicate if memory is available.
     * @param[in] size Memory size.
     * @param[in] type Migrate type.
     * @return True if memory is available.
     */
    bool IsMemoryAvailable(uint64_t size = 0, MigrateType type = MigrateType::SCALE_DOWN) const;

    /**
     * @brief Indicate if disk space is available.
     * @param[in] size The amount of disk space needed.
     * @return True if the required disk space is available.
     */
    bool IsDiskAvailable(uint64_t size = 0) const;

    /**
     * @brief Indicate if resource space is available.
     * @param[in] type Migrate type.
     * @param[in] cacheType Cache type.
     * @param[in] size The resource size.
     * @return True if resource space is available.
     */
    bool IsResourceAvailable(const MigrateType &type, CacheType cacheType, uint64_t size) const;

    /**
     * @brief Indicate if spill disk is available.
     * @param[in] size Spill size.
     * @return True if spill disk is available.
     */
    bool IsSpillAvaialble(uint64_t size = 0) const;

    /**
     * @brief Indicate the status is OOM or No space.
     * @return True if status is OOM or No space.
     */
    bool IsNoSpace(const Status &status) const;

    /**
     * @brief Check resource before migrate data.
     * @param[in] req Migrate data request.
     * @param[out] rsp Migrate data response.
     * @return K_OK on success, the error otherwise.
     */
    Status CheckResource(const MigrateDataReqPb &req, MigrateDataRspPb &rsp);

    /**
     * @brief Galculate the limit rate can be provied for the worker.
     * @param[in] workeAddr Worker address of the request.
     * @return The limit rate for the worker.
     */
    uint64_t CalculateNewRate(const std::string &workerAddr);

    /**
     * @brief Get migrate data objects.
     * @param[in] req Migrate data request.
     * @return Object list.
     */
    template <typename Req>
    std::vector<std::string> GetObjects(const Req &req) const
    {
        std::vector<std::string> objectKeys;
        objectKeys.reserve(req.objects_size());
        const auto &infos = req.objects();
        for (const auto &info : infos) {
            objectKeys.emplace_back(info.object_key());
        }
        return objectKeys;
    }

    /**
     * @brief Prepare error response for migrate data direct.
     * @param[in] req Migrate data direct request.
     * @param[out] rsp Migrate data direct response.
     * @param[in] code Status code.
     * @param[in] message Status message.
     * @return Status of the call.
     */
    Status PrepareMigrateDataDirectError(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp,
                                         StatusCode code, const std::string &message);

    /**
     * @brief Pre-check conditions for migrate data direct.
     * @param[in] req Migrate data direct request.
     * @param[out] rsp Migrate data direct response.
     * @return Status of the call.
     */
    Status PreCheckMigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp);

    /**
     * @brief Migrate data direct implementation.
     * @param[in] req Migrate data direct request.
     * @param[out] rsp Migrate data direct response.
     * @return Status of the call.
     */
    Status MigrateDataDirectImpl(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp);

    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    std::shared_ptr<ThreadPool> memcpyThreadPool_{ nullptr };

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };

    std::shared_timed_mutex mutex_;  // protect rateMap_ and rateTimeStampMap_

    std::unordered_map<std::string, uint64_t> rateMap_;  // key is worker ip, value is last rate

    std::unordered_map<std::string, uint64_t> rateTimeStampMap_;  // key is worker ip, value is timestamp

    std::string localAddr_;

    MigrateDataRateLimiter rateLimiter_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif