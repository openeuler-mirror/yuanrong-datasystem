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
 * Description: Defines the worker service common CRUD function.
 */
#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_CRUD_COMMON_API_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_CRUD_COMMON_API_H

#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/utils/status.h"

#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/l2cache/l2_storage.h"
#include "datasystem/common/util/thread_pool.h"

#include "datasystem/worker/object_cache/async_rollback_manager.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/device/worker_device_oc_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_request_manager.h"

namespace datasystem {
namespace object_cache {
class AsyncPersistenceDelManager {
public:
    /**
     * @brief AsyncPersistenceDelManager.
     * @param[in] pool oldVerDelAsyncPool.
     * @param[in] persistenceApi persistenceApi.
     */
    AsyncPersistenceDelManager(std::shared_ptr<ThreadPool> oldVerDelAsyncPool,
                               std::shared_ptr<PersistenceApi> persistenceApi);

    ~AsyncPersistenceDelManager();

    /**
     * @brief Add need to del key with version
     */
    void Add(const std::string &objId, uint64_t version);

    /**
     * @brief Process del persistence old version obj.
     */
    void ProcessDelPersistenceOldVerSion();

private:
    std::shared_ptr<PersistenceApi> persistenceApi_{ nullptr };
    // protect persistenceDelList_
    std::mutex mutex_;
    std::unordered_map<std::string, uint64_t> persistenceDelMap_;
    std::atomic<bool> exit_{ false };
};

struct WorkerOcServiceCrudParam {
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager;
    WorkerRequestManager &workerRequestManager;
    std::shared_ptr<SharedMemoryRefTable> memoryRefTable;
    std::shared_ptr<ObjectTable> objectTable;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager;
    std::shared_ptr<WorkerDeviceOcManager> workerDevOcManager;
    std::shared_ptr<AsyncPersistenceDelManager> asyncPersistenceDelManager;
    std::shared_ptr<AsyncSendManager> asyncSendManager;
    std::shared_ptr<AsyncRollbackManager> asyncRollbackManager;
    size_t metadataSize;
    std::shared_ptr<PersistenceApi> persistenceApi;
    EtcdClusterManager *etcdCM;
};

class WorkerOcServiceCrudCommonApi {
public:
    /**
     * @brief Construct WorkerOcServiceCrudCommonApi.
     * @param[in] initParam The parameter used to init WorkerOcServiceCrudCommonApi.
     */
    WorkerOcServiceCrudCommonApi(WorkerOcServiceCrudParam &initParam);

    /**
     * @brief Save the binary object payload data to cloud storage.
     * @param[in] objectKV The object to be saved to cloud storage and its corresponding objectKey.
     * @return Status of the call.
     */
    Status SaveBinaryObjectToPersistence(ObjectKV &objectKV);

    /**
     * @brief Retry and redirect
     * @tparam Req Request to master
     * @tparam Rsp Response of master
     * @param req Request of redirect
     * @param rsp Response of redirect
     * @param workerMasterApi worker master api
     * @param fun Create update or copy meta to master.
     * @return
     */
    template <typename Req, typename Rsp>
    Status RedirectRetryWhenMetaMoving(Req &req, Rsp &rsp, std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                       std::function<Status(Req &, Rsp &)> fun)
    {
        while (true) {
            CHECK_FAIL_RETURN_STATUS(fun != nullptr, K_RUNTIME_ERROR, "function is nullptr");
            RETURN_IF_NOT_OK(fun(req, rsp));
            if (rsp.info().redirect_meta_address().empty()) {
                return Status::OK();
            } else if (!rsp.meta_is_moving()) {
                HostPort newMetaAddr;
                RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(rsp.info().redirect_meta_address(), newMetaAddr));
                LOG(INFO) << "meta has been migrated to the new master: " << newMetaAddr.ToString();
                workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(newMetaAddr);
                CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                         "hash master get failed, RedirectRetryWhenMetaMoving failed");
                req.set_redirect(false);
                auto status = fun(req, rsp);
                RETURN_IF_NOT_OK(status);
                return Status::OK();
            }
            static const int sleepTimeMs = 200;
            rsp.Clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        }
    }

    /**
     * #brief Retry when meta is moving
     * @param Rsp Response of redirect
     * @param Req Request of redirect
     * @param rsp Response of redirect
     * @param fun Query or delete request to master
     * @return
     */
    template <typename Req, typename Rsp>
    static Status RedirectRetryWhenMetasMoving(Req &req, Rsp &rsp, std::function<Status(Req &, Rsp &)> func)
    {
        while (true) {
            RETURN_IF_NOT_OK(func(req, rsp));
            RETURN_OK_IF_TRUE(MetaMovingDone(rsp));
        }
    }

    /**
     * #brief Retry when meta is moving
     * @param Rsp Response of redirect
     * @param Req Request of redirect
     * @param rsp Response of redirect
     * @param fun Query or delete request to master
     * @return
     */
    template <typename Req, typename Rsp, typename RpcMessage>
    Status RedirectRetryWhenMetasMoving(Req &req, Rsp &rsp, RpcMessage &payload,
                                        std::function<Status(Req &, Rsp &, RpcMessage &payload)> func)
    {
        while (true) {
            RETURN_IF_NOT_OK(func(req, rsp, payload));
            RETURN_OK_IF_TRUE(MetaMovingDone(rsp));
        }
    }

    /**
     * @brief Attach shmUnit to object entry
     * @param[in] shmEnabled Enable shm or not.
     * @param[in] objectKey The object key
     * @param[in] shmUnitId The shm unit id.
     * @param[in] dataSize The size of data
     * @param[out] entry The object entry
     * @return OK if attach success.
     */
    Status AttachShmUnitToObject(const bool &shmEnabled, const std::string &objectKey, const ShmKey &shmUnitId,
                                 uint64_t dataSize, SafeObjType &entry);

    /**
     * @brief Update the request if object is getting success.
     * @param[in] objectKV The key-value of the object.
     * @return OK if update success.
     */
    Status UpdateRequestForSuccess(ReadObjectKV &objectKV, const std::shared_ptr<GetRequest> &request = nullptr);

    /**
     * @brief CheckShmUnitByTenantId
     * @param tenantId request tenantId
     * @param clientId clientId
     * @param shmUnitIds shmUnitIds
     * @return Status
     */
    static Status CheckShmUnitByTenantId(const std::string &tenantId, const ClientKey &clientId,
                                         std::vector<ShmKey> &shmUnitIds,
                                         std::shared_ptr<SharedMemoryRefTable> memoryRefTable);

    /**
     * @brief Delete spilled object from disk.
     * @param[in/out] objectKV The object entry and its corresponding objectKey.
     * @return Status of the call.
     */
    static Status DeleteObjectFromDisk(ObjectKV &objectKV);

    /**
     * @brief Indicates whether the client allows shared memory.
     * @param[in] objectKV The key-value of the object.
     * @return true if client enable share memory.
     */
    static bool ClientShmEnabled(const ClientKey &clientId);

    /**
     * @brief Indicates whether the client allows shared memory.
     * @param[in] supportType The key-value of the object.
     * @param[in] writeMode The key-value of the object.
     * @return true if client enable share memory.
     */
    static Status CheckIfL2CacheNeededAndWritable(const L2StorageType &supportType, WriteMode writeMode);

    /**
     * @brief Indicates whether the worker allows transfer payload by shared memory for specific data size.
     * @param[in] size The data size.
     * @return true if allow transfer payload by share memory.
     */
    static bool CanTransferByShm(uint64_t size);

    /**
     * @brief Indicates whether the worker allows shm transfer.
     * @return true if allow allows shm transfer.
     */
    static bool ShmEnable();

    /**
     * @brief Get the metadata size for specific data size.
     * @return The metadata size
     */
    size_t GetMetadataSize() const;

    /**
     * @brief Clear object.
     * @param[in] objectKV The object to clear and its corresponding objectKey.
     * @return Status of the call.
     */
    Status ClearObject(ObjectKV &objectKV);

    /**
     * @brief Unlock a batch of objects.
     * @param[in] lockedEntries Locked entries that need to unlock.
     */
    void BatchUnlock(const std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries);

    /**
     * @brief Lock a batch of objects, may insert new fake objects into ObjectTable.
     * @param[in] objectKeys The object key list.
     * @param[out] lockedEntries Locked entries map.
     * @param[out] successIds Lock success object key list.
     * @param[out] failedIds Lock failed object key list.
     * @return Last error.
     */
    Status BatchLockWithInsert(const std::vector<std::string> &objectKeys,
                               std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries,
                               std::vector<std::string> &successIds, std::vector<std::string> &failedIds);

    /**
     * @brief Get the primary replica addr
     * @param[in] srcAddr The source address.
     * @param[out] destAddr The dest address.
     * @return Status of this call.
     */
    Status GetPrimaryReplicaAddr(const std::string &srcAddr, HostPort &destAddr);

    /**
     * @brief Remove meta location
     * @param objectKeysRemoveList The obj keys too remove
     * @param workerMasterApi Worker master api
     * @param removeCause Remove cause
     * @param version Obj version
     * @param needRedirct If need redirect or not on master
     * @param localAddress the local address
     * @param batchKeyVersions the map for the key and version
     * @param response Response of the call
     * @return Status
     */
    Status RemoveMeta(const std::list<std::string> &objectKeysRemoveList,
                      const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                      const master::RemoveMetaReqPb::Cause removeCause, const uint64_t version, bool needRedirct,
                      const std::string &localAddress,
                      const std::unordered_map<std::string, uint64_t> &batchKeyVersions,
                      master::RemoveMetaRspPb &response);

    /**
     * @brief Remove metadata redirect master
     * @param rsp Response info of redirect master
     * @param removeCause RemoveCause
     * @param localAddress the local address
     * @param batchKeyVersions the map for the key and version
     * @param failedIds Remove meta failed ids
     * @param needMigrateIds Need migrateIds.
     * @param needWaitIds Need waited Ids
     * @return Status of the call
     */
    Status RemoveMetadataFromRedirectMaster(master::RemoveMetaRspPb &rsp,
                                            const master::RemoveMetaReqPb::Cause removeCause,
                                            const std::string &localAddress,
                                            const std::unordered_map<std::string, uint64_t> &batchKeyVersions,
                                            std::vector<std::string> &failedIds,
                                            std::vector<std::string> &needMigrateIds,
                                            std::vector<std::string> &needWaitIds);

    /**
     * @brief
     * @param[in] objectKeys objects to remove meta location.
     * @param[in] workerMasterApi the worker master api.
     * @param[in] removeCase Remove meta case
     * @param[in] localAddress the local address
     * @param[in] batchKeyVersions the map for the key and version
     * @param[out] failedIds Failed Ids.
     * @param[out] needMigrateIds need to migrate ids.
     * @param[out] needWaitIds Need wait ids.
     */
    void BatchRemoveMeta(const std::vector<std::string> &objectKeys,
                         const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                         const master::RemoveMetaReqPb::Cause removeCause, const std::string &localAddress,
                         const std::unordered_map<std::string, uint64_t> &batchKeyVersions,
                         std::vector<std::string> &failedIds, std::vector<std::string> &needMigrateIds,
                         std::vector<std::string> &needWaitIds);

    /**
     * @brief GroupAndRemoveMeta
     * @param[in] objKeys ObjKeys need to remove meta
     * @param[in] removeCase Remove meta case
     * @param[in] localAddress the local address
     * @param[in] batchKeyVersions the map for the key and version
     * @param[out] failedIds Failed Ids.
     * @param[out] needMigrateIds need to migrate ids.
     * @param[out] needWaitIds Need wait ids.
     */
    void GroupAndRemoveMeta(const std::vector<std::string> &objKeys, const master::RemoveMetaReqPb::Cause &removeCase,
                            const std::string &localAddress,
                            const std::unordered_map<std::string, uint64_t> &objKeyVersions,
                            std::vector<std::string> &failedIds, std::vector<std::string> &needMigrateIds,
                            std::vector<std::string> &needWaitIds);

protected:
    /**
     * #brief Check if CheckIfNeedRetry
     * @param rsp Response of redirect
     * @return Need retry or not.
     */
    template <typename Rsp>
    static bool MetaMovingDone(Rsp &rsp)
    {
        if (rsp.info_size() == 0 || !rsp.meta_is_moving()) {
            return true;
        }
        static const int sleepTimeMs = 200;
        rsp.Clear();
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        return false;
    }

    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager_{ nullptr };

    WorkerRequestManager &workerRequestManager_;

    std::shared_ptr<PersistenceApi> persistenceApi_{ nullptr };

    std::shared_ptr<SharedMemoryRefTable> memoryRefTable_{ nullptr };

    std::shared_ptr<ObjectTable> objectTable_{ nullptr };

    std::shared_ptr<WorkerOcEvictionManager> evictionManager_{ nullptr };

    std::shared_ptr<WorkerDeviceOcManager> workerDevOcManager_{ nullptr };

    std::shared_ptr<AsyncSendManager> asyncSendManager_{ nullptr };

    std::shared_ptr<AsyncRollbackManager> asyncRollbackManager_{ nullptr };

    size_t metadataSize_{ 0 };

    L2StorageType supportL2Storage_;
    EtcdClusterManager *etcdCM_{ nullptr };

    std::shared_ptr<AsyncPersistenceDelManager> asyncPersistenceDelManager_{ nullptr };
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_CRUD_COMMON_API_H
