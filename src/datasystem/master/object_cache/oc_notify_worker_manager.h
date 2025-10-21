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
 * Description: Managing Notifications Sent to Workers.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_OC_NOTIFY_WORKER_MANAGER_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_OC_NOTIFY_WORKER_MANAGER_H

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/master/object_cache/master_worker_oc_api.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/master_worker_oc_service_impl.h"
#include "datasystem/master/object_cache/master_master_oc_api.h"

namespace datasystem {
namespace master {
using TbbNotifyWorkerOpTable = tbb::concurrent_hash_map<std::string, std::unordered_map<std::string, NotifyWorkerOp>>;

struct DeleteApiInfo {
    std::int64_t apiTag;
    std::vector<std::string> objs;
    std::string workerAddr;
};

class OCMetadataManager;
class OCNotifyWorkerManager {
public:
    /**
     * @brief Construct OCNotifyWorkerManager.
     */
    OCNotifyWorkerManager(std::shared_ptr<ObjectMetaStore> objectStore, bool backendStoreExist,
                          std::shared_ptr<AkSkManager> akSkManager, EtcdClusterManager *etcdCM,
                          OCMetadataManager *ocMetadataManager);

    ~OCNotifyWorkerManager();

    /**
     * @brief Initialization.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Shutdown the oc notify manager module.
     */
    void Shutdown();

    /**
     * @brief Synchronously notify worker which has object data to update.
     * @param[in] objectKey object key parameters.
     * @param[in] timestamp The version of object data.
     * @param[in] sourceWorker The worker initiates to update object.
     * @param[in] objectMeta The meta of object.
     * @param[in] lifeState Update the object life state to the given value.
     * @param[in] fields The secondary keys.
     * @return Status of the call.
     */
    Status SyncSendUpdateObject(const std::string &objectKey, uint64_t timestamp, const std::string &sourceWorker,
                                struct ObjectMeta &objectMeta, ObjectLifeState lifeState,
                                const std::vector<std::string> &fields);

    /**
     * @brief Asynchronously notify worker which has object data to update.
     * @param[in] objectKey object key parameters.
     * @param[in] sourceWorker The worker initiates to update object.
     * @param[in] objectMeta The meta of object.
     * @return Status of the call.
     */
    Status AsyncSendUpdateObject(const std::string &objectKey, const std::string &sourceWorker,
                                 const struct ObjectMeta &objectMeta);

    /**
     * @brief Notify the created object meta to the subscriber.
     * @param[in] objectKey object key parameters.
     * @param[in] objectMeta The created object meta.
     * @param[in] subAddress Address of the subscriber.
     * @param[in] isFromOtherAz Specifies whether subscription requests are from other AZs.
     * @return Status of the call.
     */
    Status NotifySubscribeMeta(const std::string &objectKey, const ObjectMeta &objectMeta,
                               const std::string &subAddress, bool isFromOtherAz);

    /**
     * @brief clear data without meta.
     * @param[in] range The hash range of objectKey to clear data.
     * @param[in] objsMigrateFinished The object keys in range no need to clear.
     * @return Status of the call.
     */
    Status ClearDataWithoutMeta(const worker::HashRange &range, const std::string &workerAddr,
                                const std::vector<std::string> &objsMigrateFinished,
                                const std::vector<std::string> &uuids);

    /**
     * @brief Notify worker which has object data to delete.
     * @param[in] sourceWorker The worker initiates to delete object.
     * @param[in/out] replicas2Obj Worker address and their object key, the success worker will be remove from
     * replicas2Obj
     * @param[in] isAsync Is async process mode.
     * @param[out] failedObjects The failed object list.
     * @return Status of the call.
     */
    Status DoNotifyWorkerDelete(
        const std::string &sourceWorker,
        std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas2Obj,
        bool isAsync, std::unordered_set<std::string> &failedObjects);

    /**
     * @brief Notify worker which has object data to delete.
     * @param[in] request The ptr of DeleteObjectReqPb.
     * @param[in] isAsync Is async process mode.
     * @param[in] sourceWorker The worker initiates to delete object.
     * @param[in] objectItem The failed object list.
     */

    void SetDeleteObjectReq(std::unique_ptr<DeleteObjectReqPb> &request, bool &isAsync, const std::string &sourceWorker,
                            const std::unordered_map<std::string, std::pair<int64_t, uint32_t>> &objectItem);

    /**
     * @brief Notify worker which has object data to delete.
     * @param[in] sourceWorker The worker initiates to delete object.
     * @param[in] replicas2Obj Worker address and their object key.
     * @param[in] isAsync Is async process mode.
     * @param[out] failedObjects The failed object list.
     * @param[out] api2Tag The map from request to tag.
     * @param[out] lastErr The last error status of request.
     * @return Status of the call.
     */
    Status HandleDeleteNotificationSend(
        const std::string &sourceWorker, const std::string &address,
        const std::unordered_map<std::string, std::pair<int64_t, uint32_t>> &objectItem, bool &isAsync,
        std::unordered_map<std::shared_ptr<MasterWorkerOCApi>, std::pair<int64_t, std::string>> &api2Tag,
        Status &lastErr);

    /**
     * @brief Notify worker which has object data to delete.
     * @param[in] sourceWorker The worker initiates to delete object.
     * @param[in] replicas2Obj Worker address and their object key.
     * @param[in] isAsync Is async process mode.
     * @param[out] failedObjects The failed object list.
     * @param[out] api2Tag The map from request to tag.
     * @return Status of the call.
     */
    Status DoNotifyWorkerDeleteSendRequest(
        const std::string &sourceWorker,
        std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas2Obj,
        bool isAsync, std::unordered_set<std::string> &failedObjects,
        std::unordered_map<std::shared_ptr<MasterWorkerOCApi>, std::pair<int64_t, std::string>> &api2Tag);

    /**
     * @brief Meta address change notify worker to redirect
     * @param[in] objectKey The key of object
     * @param[in] workerAddr Subscribe worker address
     * @param[in] sourceAddr New meta address
     * @param[in] timeout The subscribe timeout
     * @param[in] isFromOtherAz Specifies whether subscription requests are from other AZs.
     * @return Status of the call
     */
    Status MetaChange(const std::string &objectKey, const std::string &workerAddr, const std::string &sourceAddr,
                      const uint64_t &timeout, bool isFromOtherAz);

    /**
     * @brief Set the faulty worker.
     * @param[in] workerAddr Address of the worker.
     */
    void SetFaultWorker(const std::string &workerAddr);

    /**
     * @brief Remove the faulty worker.
     * @param[in] workerAddr Address of the worker.
     */
    void RemoveFaultWorker(const std::string &workerAddr);

    /**
     * @brief Check whether the worker status is healthy.
     * @param[in] workerAddr Address of the worker.
     * @return If it is healthy, return Status::OK().
     */
    Status CheckWorkerIsHealthy(const std::string &workerAddr);

    /**
     * @brief Insert data to async notify worker operator table.
     * @param[in] workerId The worker address of the object.
     * @param[in] objectKey The uuid of the object.
     * @param[in] op Operator to be insert.
     * @param[in] needPersist Indicates whether to persist to rocksdb.
     * @return Status of the call.
     */
    Status InsertAsyncWorkerOp(const std::string &workerId, const std::string &objectKey, const NotifyWorkerOp &op,
                               bool needPersist = true,
                               ObjectMetaStore::WriteType type = ObjectMetaStore::WriteType::ROCKS_ONLY);

    /**
     * @brief Clear data from async notify worker operator table.
     * @param[in] workerAddr The worker address of the object.
     * @return Status of the call.
     */
    Status ClearAsyncWorkerOp(const std::string &workerAddr);

    /**
     * @brief Check whether there is data from async notify worker operator table.
     * @param[in] workerId The worker address of the object.
     * @param[in] objectKey The uuid of the object.
     * @param[in] op Operator to be insert.
     * @return If the operator exists, return true.
     */
    bool CheckExistAsyncWorkerOp(const std::string &workerId, const std::string &objectKey, NotifyWorkerOpType op);

    /**
     * @brief Get async operation of the object
     * @param[in] objectKey The uuid of the object.
     * @return Some async operation.
     */
    std::vector<std::pair<std::string, NotifyWorkerOp>> GetObjectAsyncWorkerOp(const std::string &objectKey);

    /**
     * @brief Asynchronously send changes to the primary copy.
     * @param[in] toBeChanged Primary copy to be changed.
     * @param[in] ifvoluntaryScaleDown Judge whether the worker is voluntary scale down,
     * a dead worker don't need to add PRIMARY_COPY_INVALID.
     */
    void AsyncChangePrimaryCopy(const std::unordered_map<std::string, std::unordered_set<std::string>> &toBeChanged,
                                bool ifvoluntaryScaleDown = false);

    /**
     * @brief Asynchronously send push metadata to worker.
     * @param[in] workerAddr Destination worker address.
     * @param[in] timestamp timestamp of the event triggering reconciliation.
     * @param[in] isRestart Whether this function is called due to node restart.
     */
    void AsyncPushMetaToWorker(const std::string &workerAddr, int64_t timestamp, bool isRestart);

    /**
     * @brief Asynchronously sending notify operation to worker.
     * @param[in] workerAddr Destination worker address.
     * @param[in] timestamp timestamp of the event triggering reconciliation.
     */
    void AsyncNotifyOpToWorker(const std::string &workerAddr, int64_t timestamp);

    /**
     * @brief Increase reference count for objects owned by other masters.
     * @param[in] workerAddr Destination worker address.
     * @param[in] objectKeys vector of object keys
     * @return Status of the call.
     */
    Status IncNestedRefs(const std::string &workerAddr, const std::vector<std::string> &objectKeys);

    /**
     * @brief Decrease reference count for objects owned by other masters.
     * @param[in] workerAddr Destination worker address.
     * @param[in] objectKeys vector of object keys
     */
    void AsyncDecNestedRefs(const std::string &workerAddr, const std::vector<std::string> &objectKeys);

    /**
     * @brief Takes a copy of the local MasterWorkerService to allow rpc bypass when the master and worker are
     * collocated.
     * @param[in] service Pointer to the receiving side service of the MasterWorker api's
     * @param[in] masterAddr A copy of the local master host port
     */
    void AssignLocalWorker(object_cache::MasterWorkerOCServiceImpl *service, const HostPort &masterAddr);

    /**
     * @brief Requests the worker to send meta to the master in the response.
     * @param[in] masterAddr The address of the master requesting for the metadata.
     * @param[in] dbName The rocksdb name of the master requesting for the metadata.
     * @param[in] workerAddr The address of the worker to whom the request is sent.
     * @param[out] rsp The response of the call containing meta related to the master.
     * @return Status of the call.
     */
    Status RequestMetaFromWorker(const std::string &masterAddr, const std::string &dbName,
                                 const std::string &workerAddr, RequestMetaFromWorkerRspPb &rsp);

    /**
     * @brief Send push metadata to worker.
     * @param[in] workerAddr Destination worker address.
     * @param[in] timestamp timestamp of the event triggering reconciliation.
     * @param[in] isRestart Whether this function is called due to node restart.
     */
    void PushMetaToWorker(const std::string &workerAddr, int64_t timestamp, bool isRestart);

    /**
     * @brief Remove operator from async worker op table.
     * @param[in] workerId The worker address of the object.
     * @param[in] objectKeys The object keys to be remove.
     * @param[in] op The operator to be removed.
     * @param[in] isDataMigration Indicates whether the data migration scenario is used. If the value is true, data in
     * etcd is not deleted.
     * @return Status of the call.
     */
    Status RemoveAsyncWorkerOp(const std::string &workerId, const std::vector<std::string> &objectKeys,
                               NotifyWorkerOpType op, bool isDataMigration = false);

    /**
     * @brief erase master worker api.
     * @param[in] nodePort worker address.
     */
    void EraseMasterWorkerApi(HostPort &nodePort);

    /**
     * @brief Recover cache invalid and remove meta info to EtcdKeyMap
     * @param[in] cacheInvalids The vector of worker id and object key.
     */
    void RecoverCacheInvalidAndRemoveMeta2EtcdKeyMap(std::vector<std::pair<std::string, std::string>> &cacheInvalids);

    /**
     * @brief Recover cache invalid and remove meta info to cache
     * @param[in] isFromRocksdb Specifies whether to obtain data from rocksdb.
     * @param[in] workerUuids Recover the data of specified worker uuids. If the value is empty, recover the data of the
     * current worker.
     * @param[in] extraRanges Obtains the data of specified hash ranges if not empty.
     * @return Status of the call.
     */
    Status RecoverCacheInvalidAndRemoveMeta(bool isFromRocksdb, const std::vector<std::string> &workerUuids = {},
                                            const worker::HashRange &extraRanges = {});

    /**
     * @brief Send changes to the primary copy.
     * @param[in] workerAddr Primary copy to be changed.
     * @param[in] objectKeys Object key to be changed.
     * @param[out] successIds Success object keys.
     * @return Status of the call.
     */
    Status SendChangePrimaryCopy(const std::string &workerAddr, const std::unordered_set<std::string> &objectKeys,
                                 std::unordered_set<std::string> &successIds);

    /**
     * @brief Parse the notify other nodes' operations stored in L2 cache.
     * @param[in] serializedStr The msg stored in L2 cache.
     * @return The parsed struct.
     */
    static NotifyWorkerOp ParseNotifyWorkerOpFromL2Cache(const std::string &serializedStr);

    /**
     * @brief Parse the notify other nodes' operations migrated from other node.
     * @param[in] pb The msg migrated from other node.
     * @return The parsed struct.
     */
    static NotifyWorkerOp ParseNotifyWorkerOpFromMigration(const ObjectAsyncOpDetailPb &pb);

    /**
     * @brief Notify master remove meta.
     * @param[in] masterAddr The dest master address.
     * @param[in] objKeys The object key.
     * @param[out] failedObjs Failed object keys.
     * @return Status of the call.
     */
    Status NotifyMasterRemoveMeta(const HostPort &masterAddr, const std::unordered_map<std::string, int64_t> &objKeys,
                                  std::unordered_set<std::string> &failedObjs);

    /**
     * @brief When the local meta version is updated, the version of the remove meta notification needs to be updated.
     * @param[in] objKeys The object key.
     * @param[in] version The new meta version.
     */
    void UpdateRemoteMetaNotification(const std::string &objKey, int64_t version);

    /**
     * @brief Notify master delete all copy meta.
     * @param[in] masterAddr The dest master address.
     * @param[in] objKeys The object key.
     * @param[out] failedObjs Failed object keys.
     * @param[out] objsWithoutMeta If the target node has no metadata, record it here.
     * @return Status of the call.
     */
    Status NotifyMasterDeleteAllCopyMeta(const HostPort &masterAddr, const std::vector<std::string> &objKeys,
                                         std::unordered_set<std::string> &failedObjs,
                                         std::unordered_set<std::string> &objsWithoutMeta,
                                         const std::vector<std::pair<std::string, int64_t>> &objKeyWithVersion = {});

    /**
     * @brief Process changes to the primary copy.
     * @param[in] input Primary copy to be changed.
     * @param[in] ifvoluntaryScaleDown Judge whether the worker is voluntary scale down,
     * a dead worker don't need to add PRIMARY_COPY_INVALID.
     */
    void ProcessChangePrimaryCopy(const std::unordered_map<std::string, std::unordered_set<std::string>> &input,
                                  bool ifvoluntaryScaleDown);
    
    Status ProcessAsyncDeleteNotifyOpImpl();

private:
    /**
     * @brief Process async notify operation.
     */
    void ProcessAsyncNotifyOp();

    /**
     * @brief Obtain the RPC Connection from the Master to the Worker.
     * @param[in] workerAddr The worker address of the object.
     * @param[out] resultApi The api of master to worker.
     * @return Status of the call.
     */
    Status GetMasterWorkerApi(const std::string &workerAddr, std::shared_ptr<MasterWorkerOCApi> &resultApi);

    /**
     * @brief Remove cache invalid address from location table.
     * @param[in] workerId The worker address of the object.
     * @param[in] objectKeys The object keys to be remove.
     * @return Status of the call.
     */
    Status ClearAddressCacheInvalid(const std::string &workerId,
                                    const std::unordered_map<std::string, uint64_t> &objectKeys);

    /**
     * @brief Fill UpdateObjectInfoPb from meta.
     * @param[in] objectKey The object key to send cache invalid.
     * @param[out] objectInfoPb UpdateObjectInfoPb to be send.
     * @return Status of the call.
     */
    Status FillUpdateObjectInfoPb(const std::string &objectKey, UpdateObjectInfoPb *objectInfoPb);

    /**
     * @brief Process objs without target node. The request to notify master matches this situation.
     */
    void ProcessObjsWithoutTargetNode();

    /**
     * @brief Process async notify operation.
     * @return Status of the call.
     */
    Status ProcessAsyncNotifyOpImpl();

    /**
     * @brief Send cache invalid notification to worker.
     * @param[in] workerId Worker address.
     * @param[in] objectKeys Object keys.
     * @return Status of the call.
     */
    Status SendCacheInvalidToWorker(const std::string &workerId, std::unordered_set<std::string> &objectKeys);

    /**
     * @brief Decrease reference count for objects owned by other masters.
     * @param[in] workerAddr Destination worker address.
     * @param[in] objectKeys vector of object keys
     */
    void DecNestedRefs(const std::string &workerAddr, const std::vector<std::string> &objectKeys);

    /**
     * @brief Send notify operation to worker.
     * @param[in] workerAddr Destination worker address.
     * @param[in] timestamp timestamp of the event triggering reconciliation.
     */
    void NotifyOpToWorker(const std::string &workerAddr, int64_t timestamp);

    /**
     * @brief Handle worker disconnection when notify worker delete a object data.
     * @param[in] address Destination worker address.
     * @param[in] objectItem The objects tuple(objectKey, workerId, writeType) to be delete.
     * @param[out] asyncNotifyIds The objects that will send the delete request to the worker asynchronously.
     * @return Worker is healthy if true, otherwise false.
     */
    bool HandleWorkerDisconnection(
        const std::string &address, const std::unordered_map<std::string, std::pair<int64_t, uint32_t>> &objectItem,
        std::vector<std::tuple<std::string, std::string, uint32_t, uint64_t>> &asyncNotifyIds);

    /**
     * @brief Sync notify worker delete a object data.
     * @param[in] masterWorkerApi The masterWorkerApi.
     * @param[in] address Destination worker address.
     * @param[out] request The DeleteObjectReqPb req.
     * @param[out] replicas2Obj Worker address and their object key.
     * @param[out] failedObjects The failed object list.
     * @param[out] lastErr The last error record.
     * @return Status of the call.
     */
    Status SyncNotifyWorkerDelete(
        std::shared_ptr<MasterWorkerOCApi> &masterWorkerApi, const std::string &address,
        std::unique_ptr<DeleteObjectReqPb> &request,
        std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas2Obj,
        std::unordered_set<std::string> &failedObjects, Status &lastErr);

    /**
     * @brief Async notify worker delete objects.
     * @param[in] asyncNotifyIds The objects tuple(objectKey, workerId, writeType) to be delete.
     * @param[out] replicas2Obj Worker address and their object key.
     * @param[out] failedObjects The failed object list.
     * @return Status of the call.
     */
    Status AsyncNotifyWorkerDelete(
        std::vector<std::tuple<std::string, std::string, uint32_t, uint64_t>> &asyncNotifyIds,
        std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas2Obj,
        std::unordered_set<std::string> &failedObjects);

    /**
     * @brief Parse out the operation to notify worker.
     * @param[in] notifyWorkerOp Operation that requires parsing.
     * @return The operation to notify worker.
     */
    static inline NotifyWorkerOpType ClearNotifyWorkerOp(NotifyWorkerOpType notifyWorkerOp)
    {
        return static_cast<NotifyWorkerOpType>(ClearUint32EvenBits(static_cast<uint32_t>(notifyWorkerOp)));
    }

    /**
     * @brief Parse out the operation to notify master.
     * @param[in] notifyWorkerOp Operation that requires parsing.
     * @return The operation to notify master.
     */
    static inline NotifyWorkerOpType ClearNotifyMasterOp(NotifyWorkerOpType notifyWorkerOp)
    {
        return static_cast<NotifyWorkerOpType>(ClearUint32OddBits(static_cast<uint32_t>(notifyWorkerOp)));
    }

    /**
     * @brief Process objs need remove meta.
     * @param[in] objsNeedRemoveMeta The object key.
     */
    void ProcessObjsNeedRemoveMeta(const std::unordered_map<std::string, NotifyWorkerOp> &objsNeedRemoveMeta);

    /**
     * @brief Process objs need delete all copy meta.
     * @param[in] objsNeedRemoveMeta The object key.
     */
    void ProcessObjsNeedDeleteAllCopyMeta(
        const std::unordered_map<std::string, NotifyWorkerOp> &objsNeedDeleteAllCopyMeta);

    /**
     * @brief Get WriteType of object.
     * @param[in] objKey Object key.
     * @return The WriteType of object.
     */
    ObjectMetaStore::WriteType GetWriteType(const std::string &objKey);

    /**
     * @brief Remove async worker op.
     * @param[in] objectKeys <objectKey, <azNames>>.
     * @param[in] op Async notify op type.
     * @return Status of call.
     */
    Status RemoveNoTargetAsyncWorkerOp(
        const std::unordered_map<std::string, std::unordered_set<std::string>> &objectKeys, NotifyWorkerOpType op);

    std::shared_ptr<ObjectMetaStore> objectStore_;  // Metadata store for object.
    std::shared_timed_mutex notifyWorkerOpMutex_;
    TbbNotifyWorkerOpTable notifyWorkerOpTable_;  // Key is worker address, value is object keys.

    const int ASYNC_SEND_UPDATE_TIME_MS = 100;  // Time interval between two async update object.
    std::unique_ptr<Thread> thread_{ nullptr };
    WaitPost cvLock_;  // Wait for send cache update worker.
    std::atomic<bool> interruptFlag_;

    std::shared_timed_mutex faultWorkerMutex_;
    std::unordered_set<std::string> faultWorkers_;
    object_cache::MasterWorkerOCServiceImpl *masterWorkerOCService_{ nullptr };
    HostPort masterAddr_;
    const bool backendStoreExist_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    EtcdClusterManager *etcdCM_{ nullptr };
    OCMetadataManager *ocMetadataManager_{ nullptr };
    // for diffrent ocmetamanager to add and remove Subscriber.
    std::string subscriberPrefix_ = "";
    const int notifyMasterRemoveMetaTimeOutMs_ = 5'000;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_OC_NOTIFY_WORKER_MANAGER_H
