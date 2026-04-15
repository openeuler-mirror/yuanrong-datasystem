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
 * Description: Defines the worker service processing main class.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_SERVICE_IMPL_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <google/protobuf/any.h>
#include <google/protobuf/repeated_field.h>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/l2cache/l2_storage.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/worker/object_cache/async_rollback_manager.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.h"
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/common/util/queue/blocking_queue.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/util/queue/shm_circular_queue.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/master_heartbeat.pb.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/object_posix.service.rpc.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/protos/worker_object.service.rpc.pb.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/object_cache/async_rpc_request_manager.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_request_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_create_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_publish_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_multi_publish_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_delete_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_global_reference_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_expire_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_clear_data_flow.h"
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_manager.h"

namespace datasystem {
namespace master {
class MasterOCServiceImpl;
}
namespace object_cache {

using QueryMetaMap = std::unordered_map<std::string, master::QueryMetaInfoPb>;
static constexpr int MEMCOPY_THREAD_NUM = 16;
static constexpr int PARALLEL_THREAD_NUM = 8;
class MasterWorkerOCServiceImpl;
class WorkerWorkerOCServiceImpl;
class WorkerDeviceOcManager;
class WorkerRemoteWorkerOCApi;

enum LockMode { Read = 0, Write = 1 };

class WorkerOCServiceImpl : public WorkerOCService {
public:
    using AsyncTasksDoneChecker = std::function<Status(const std::string &)>;

    /**
     * @brief Construct WorkerOCServiceImpl.
     * @param[in] serverAddr The address of local worker node.
     * @param[in] masterAddr The address of the master node.
     * @param[in] objectTable The object table.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] evictionManager The eviction manager.
     * @param[in] etcdStore Pointer to EtcdStore owned by WorkerOcServer.
     * @param[in] masterOCServicer The master service.
     */
    WorkerOCServiceImpl(HostPort serverAddr, HostPort masterAddr, std::shared_ptr<ObjectTable> objectTable,
                        std::shared_ptr<AkSkManager> manager, std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                        std::shared_ptr<PersistenceApi> persistApi, EtcdStore *etcdStore,
                        master::MasterOCServiceImpl *masterOCService = nullptr);

    ~WorkerOCServiceImpl() override;

    /**
     * @brief Initialize the WorkerOCServiceApi Object(include rpc channel).
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Initialize the RPC message process class.
     * @return Status of the call.
     */
    void InitServiceImpl();

    /**
     * @brief Before calling RPC method, this method would be call to check whether this worker is doing reconciliation.
     * If it is doing reconciliation (write lock acquired), return error;
     * otherwise, acquire read lock to block reconciliation but allows other common RPCs.
     * @param[in/out] noRecon
     * @param[in] reqTimeoutMs req timeout ms
     * @return Status of the call.
     */
    Status ValidateWorkerState(ReadLock &noRecon, int reqTimeoutMs);

    /**
     * @brief GroupAndRemoveMeta
     * @param[in] objKeys ObjKeys need to remove meta
     * @param[in] removeCase Remove meta case
     * @param[out] failedIds Failed Ids.
     * @param[out] needMigrateIds need to migrate ids.
     * @param[out] needWaitIds Need wait ids.
     * @param[out] needMigrateL2CacheIds Need migrate L2 cache ids.
     */
    void GroupAndRemoveMeta(const std::vector<std::string> &objKeys, const master::RemoveMetaReqPb::Cause &removeCase,
                            std::vector<std::string> &failedIds, std::vector<std::string> &needMigrateIds,
                            std::vector<std::string> &needWaitIds,
                            std::vector<std::string> &needMigrateL2CacheIds)
    {
        INJECT_POINT("ProcessVoluntaryScaledown", [this] {
            Timer timer;
            uint64_t sleepTimeMs = 100;
            uint64_t maxSecond = 5;
            while (timer.ElapsedSecond() < maxSecond) {
                std::string key = std::string(ETCD_RING_PREFIX) + "/";
                RangeSearchResult res;
                HashRingPb newRing;
                if (etcdStore_->RawGet(key, res).IsOk() && newRing.ParseFromString(res.value)
                    && !newRing.add_node_info().empty()) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
            }
            return;
        });
        getProc_->GroupAndRemoveMeta(objKeys, removeCase, localAddress_.ToString(),
                                     std::unordered_map<std::string, uint64_t>{}, failedIds, needMigrateIds,
                                     needWaitIds, needMigrateL2CacheIds);
    }

    /**
     * @brief HealthCheck the worker health check handler
     * @param[in] req The rpc request protobuf
     * @param[out] resp The rpc response protobuf
     * @return Status of the call
     */
    Status HealthCheck(const HealthCheckRequestPb &req, HealthCheckReplyPb &resp) override;

    /**
     * @brief Create a new object, allocate memory and return the pointer. for shm use only.
     * @param[in] req The rpc request protobuf
     * @param[out] resp The rpc response protobuf
     * @return K_OK on success; the error code otherwise.
     *         K_DUPLICATED: the object already exists, no need to create.
     */
    Status Create(const CreateReqPb &req, CreateRspPb &resp) override;

    /**
     * @brief Process worker scale down.
     * @return return status of the call.
     */
    Status ProcessVoluntaryScaledown(const std::string &taskId);

    /**
     * @brief Register callback for waiting until async tasks in server are done.
     * @param[in] checker callback waits internally and returns status.
     */
    void RegisterAsyncTasksDoneChecker(AsyncTasksDoneChecker checker);

    /**
     * @brief Before migrate data process.
     * @param[out] needMigrateDataIds Need migrate data object ld list.
     * @param[out] needWaitIds Need wait finished object key list.
     */
    Status BeforeMigrateData(const std::string &taskId, std::vector<std::string> &needMigrateDataIds,
                             std::vector<std::string> &needWaitIds,
                             std::vector<std::string> &needMigrateL2CacheIds);

    /**
     * @brief Migrate data when scale down happen.
     * @param[in] req Migrate data request.
     * @param[out] rspMigrate data response.
     * @param[in] payloads Payloads.
     * @return Status of the call.
     */
    Status MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp, std::vector<RpcMessage> payloads);

    /**
     * @brief Migrate data directly.
     * @param[in] req Migrate data direct request.
     * @param[out] rsp Migrate data direct response.
     * @return Status of the call.
     */
    Status MigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp);

    /**
     * @brief Migrate data when voluntary scale down happen.
     * @param[in] objectKeys Need migrate data object key list.
     * @param[in] taskId task id of voluntary scale down task, if task id is empty, it means we
     *                   careless about the task id.
     * @return Status of the call
     */
    Status MigrateData(const std::vector<std::string> &objectKeys, const std::string &taskId);

    /**
     * @brief Migrate L2 cache data with slot-based grouping.
     * @param[in] needMigrateL2CacheIds L2 cache object keys to migrate.
     * @param[in] taskId Task ID for tracking.
     * @return Status of the call.
     */
    Status MigrateL2CacheData(const std::vector<std::string> &needMigrateL2CacheIds, const std::string &taskId);

    /**
     * @brief Handle Put/Publish/Seal request from client.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status Publish(const PublishReqPb &req, PublishRspPb &resp, std::vector<RpcMessage> payloads) override;

    /**
     * @brief Handle multiply set request from the client.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status MultiPublish(const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                        std::vector<RpcMessage> payloads) override;

    /**
     * @brief Deal with the request for object data from client.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return Status of the call.
     */
    Status Get(std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> serverApi) override;

    /**
     * @brief Decrease the reference count of client.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status DecreaseReference(const DecreaseReferenceRequest &req, DecreaseReferenceResponse &resp) override;

    /**
     * @brief Reconcile maybe expired shm references with client.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ReconcileShmRef(const ReconcileShmRefReqPb &req, ReconcileShmRefRspPb &resp) override;

    /**
     * @brief Send request to master to decrease all objects of remote client id.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    Status ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp) override;

    /**
     * @brief Increase the global reference count of object.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp) override;

    /**
     * @brief Decrease the global reference count of object.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp) override;

    /**
     * @brief Push metadata to master, for DFX purpose.
     * @return Status of the call
     */
    Status PushMetadataToMaster(const HostPort &masterAddr);

    /**
     * @brief Handles the request for meta info and populates the response pb with the meta info.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status FillRequestMetaByMaster(const RequestMetaFromWorkerReqPb &req, RequestMetaFromWorkerRspPb &rsp);

    /**
     * @brief Refresh object meta when client crashed or disconnected.
     * @param[in] clientId The id of client.
     * @return Status of the call.
     */
    Status RefreshMeta(const ClientKey &clientId);

    /**
     * @brief The rpc method used to delete the objects.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return Status of the call.
     */
    Status DeleteAllCopy(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp) override;

    /**
     * @brief Get the Primary Replica Addr object
     * @param srcAddr Src addr
     * @param destAddr Dest addr
     * @return Status of the calll
     */
    Status GetPrimaryReplicaAddr(const std::string &srcAddr, HostPort &destAddr);

    /**
     * @brief Invalidate a share memory unit.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status InvalidateBuffer(const InvalidateBufferReqPb &req, InvalidateBufferRspPb &rsp) override;

    /**
     * @brief Bypass the request of query all objs global references in the cluster to master.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp) override;

    /**
     * @brief Recovery client info and reconciliation.
     * @param[in] clientId Client ID.
     * @param[in] tenantId Tenant ID.
     * @param[in] reqToken Need construct obj uri.
     * @param[in] req Request that contains need recovery message.
     * @return K_OK on success; the error code otherwise.
     */
    Status RecoveryClient(const ClientKey &clientId, const std::string &tenantId, const std::string &reqToken,
                          const google::protobuf::RepeatedPtrField<google::protobuf::Any> &req);

    // These function of list, hashmap and seqNo need keep until completely abandoned.
    Status Lpush(const LpushRequestPb &req, LpushReplyPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Lpop(const LpopRequestPb &req, LpopReplyPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Lindex(const LindexRequestPb &req, LindexReplyPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Llen(const LlenRequestPb &req, LlenReplyPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Hget(const HgetRequestPb &req, HgetReplyPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Hset(const HsetRequestPb &req, HsetReplyPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Hdel(const HdelRequestPb &req, HdelReplyPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Hgetall(const HgetallRequestPb &req, HgetallReplyPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status IncrSeqNo(const IncrSeqNoReqPb &req, IncrSeqNoRspPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GetSeqNo(const GetSeqNoReqPb &req, GetSeqNoRspPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status DelSeqNo(const DelSeqNoReqPb &req, DelSeqNoRspPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    /**
     * @brief clear object in range.
     * @param[in] req The clear info.
     * @return Status of the call.
     */
    Status ClearObject(const ClearDataReqPb &req);

    /**
     * @brief Ask the master whether this node needs reconciliation. If needed, trigger it.
     * @return Status
     */
    Status IfNeedTriggerReconciliation();

    /**
     * @brief Get the metadata size for specific data size.
     * @return The metadata size
     */
    size_t GetMetadataSize() const;

    /**
     * @brief Setter function to assign the cluster manager back pointer.
     * @param[in] etcdCM The cluster manager pointer to assign
     */
    void SetClusterManager(EtcdClusterManager *etcdCM)
    {
        etcdCM_ = etcdCM;
        evictionManager_->SetClusterManager(etcdCM);
    }

    /**
     * @brief Check whether there are any requests for asynchronously writing to L2 cache.
     * @return True if there are unfinished async requests.
     */
    bool HaveAsyncTasksRunning();

    /**
     * @brief Check if async sender is health.
     * @return True if health.
     */
    bool AsyncTaskHealth();

    /**
     * @brief Remove object keys.
     * @param[in] objectKeys Object key list.
     */
    void RemoveAsyncTasks(const std::vector<std::string> &objectKeys);

    /**
     * @brief Stop async sender manager and get all not send to l2 objects.
     * @return Unfininsed objects.
     */
    std::vector<std::string> StopAndGetAllUnfinishedObjects();

    /**
     * @brief Check whether this node did a restart or not. If there is no restart, set health file. If there is,
     * in case of centralized master, trigger reconciliation; for distributed master, do nothing.
     * @return K_OK on success; the error code otherwise.
     */
    Status WhetherNonRestart();

    /**
     * @brief Try to give up waiting for reconciliation to be done when using distributed master. If the local worker
     * needs reconciliation from distributed masters, but did not receive expected number of reconciliations and waiting
     * time is too long (timeout), the local worker will give up waiting and set health file to process client requests.
     * @return K_OK on success; the error code otherwise.
     */
    Status GiveUpReconciliation();

    /**
     * @brief Get the total object count.
     * @return size_t The object count.
     */
    size_t GetTotalObjectCount() const
    {
        return objectTable_->GetSize();
    }

    /**
     * @brief Get the total object size.
     * @return size_t The total object size.
     */
    size_t GetTotalObjectSize() const;

    /**
     * @brief Get the hit info statistic data.
     * @return The data string.
     */
    std::string GetHitInfo() const;
    /*
     * @brief Get the usage of the asynchronous task queue of L2Cache.
     * @note currentSize: the number of tasks in the current queue.
     *       totalLimit:  the maximum queue capacity
     * @return Usage: "currentSize/totalLimit/workerL2CacheQueueUsag".
     */
    std::string GetL2CacheAsyncTasksQueueUsage()
    {
        return asyncSendManager_->GetL2CacheAsyncTasksQueueUsage();
    }

    /**
     * @brief Return a pointer to global reference table.
     */
    ObjectGlobalRefTable<ClientKey> *GetGlobalRefTable()
    {
        return globalRefTable_.get();
    }

    /**
     * @brief erase failed worker master api.
     * @param[in] masterAddr failed master addr.
     */
    void EraseFailedWorkerMasterApi(HostPort &masterAddr);

    /**
     * @brief Get the pointer information for shared memory communication
     * @param[int] lockId Get shm info by lock id.
     * @param[out] fd File descriptor of the allocated shared memory segments.
     * @param[out] mmapSize Total size of shared memory segments.
     * @param[out] offset Offset from the base of the shared memory mmap.
     * @param[out] id The id of this shmUnit.
     * @return Status of the call.
     */
    Status GetShmQueueUnit(uint32_t lockId, int &fd, uint64_t &mmapSize, ptrdiff_t &offset, ShmKey &id);

    /**
     * @brief Handle PublishDeviceObject request from the client.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status PublishDeviceObject(const PublishDeviceObjectReqPb &req, PublishDeviceObjectRspPb &resp,
                               std::vector<RpcMessage> payloads) override;

    /**
     * @brief Handle GetDeviceObject request from the client.
     * @param[in] serverApi The unary writer reader.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetDeviceObject(
        std::shared_ptr<ServerUnaryWriterReader<GetDeviceObjectRspPb, GetDeviceObjectReqPb>> serverApi) override;

    /**
     * @brief Get match object keys.
     * @param[in] matchFunc The function to match object.
     * @param[out] objKeys All object keys match.
     */
    void GetObjectsMatch(std::function<bool(const std::string &)> matchFunc,
                         std::vector<std::string> &nooL2CacheobjKeys, bool includeL2CacheIds);

    /**
     * @brief Recover metadata of the input object keys to master.
     * @param[in] objectKeys Object keys to recover.
     * @param[out] failedIds Failed object keys.
     * @param[in] standbyWorker Standby worker address.
     * @return Status of the call.
     */
    Status RecoverMetadataOfData(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedIds,
                                 std::string standbyWorker);

    /**
     * @brief Recover metadata associated with a restarted worker.
     * @param[in] workerAddr Restarted worker address.
     * @return Status of the call.
     */
    Status RecoverMetadataOfRestartedWorker(const std::string &workerAddr);

    /**
     * @brief Collect worker UUIDs whose metadata should be recovered after restart.
     * @param[in] workerAddr Restarted worker address.
     * @param[in] restartWorkerUuid Restarted worker uuid.
     * @param[in] ringPb Current hash ring protobuf.
     * @param[out] recoverUuids UUIDs to recover.
     */
    void CollectRecoveryWorkerUuidsForRestart(const std::string &workerAddr, const std::string &restartWorkerUuid,
                                              const HashRingPb &ringPb, std::vector<std::string> &recoverUuids);

    /**
     * @brief Handle worker restart event for metadata recovery.
     * @param[in] workerAddr Restarted worker address.
     * @return Status of the call.
     */
    Status HandleNodeRestartEvent(const std::string &workerAddr);

    /*
     * @brief Put p2p metadata to master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status PutP2PMeta(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp) override;

    /**
     * @brief Subscribe the p2p receiving event.
     * @param[in] serverApi The unary writer reader.
     * @return K_OK on success; the error code otherwise.
     */
    Status SubscribeReceiveEvent(
        std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi)
        override;

    /**
     * @brief Get p2p metadata from master.
     * @param[in] serverApi The unary writer reader.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetP2PMeta(std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi) override;

    /**
     * @brief Send root info to master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status SendRootInfo(const SendRootInfoReqPb &req, SendRootInfoRspPb &resp) override;

    /**
     * @brief Receive root info to master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status RecvRootInfo(
        std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi) override;

    /**
     * @brief Get metadata of object, i.e., lifecycle mode and buffer sizes.
     * @param[in, out] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetDataInfo(std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> serverApi) override;

    /**
     * @brief Acknowledging data receiving finished.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status AckRecvFinish(const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp) override;

    /**
     * @brief Remove data location in object directory.
     * @param[in, out] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return K_OK on success; the error code otherwise.
     */
    Status RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp) override;

    /**
     * @brief Waiting for WorkerOCServiceImpl to be initialized
     * @return Status of the call.
     */
    Status WaitInit();

    /**
     * @brief Get meta info of the input objects
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetObjMetaInfo(const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &resp) override;

    /**
     * @brief Get meta info of the input objects
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status QuerySize(const QuerySizeReqPb &req, QuerySizeRspPb &rsp) override;

    /**
     * @brief Check whether the keys exist in the data system.
     * @param[in] req The exist request protobuf.
     * @param[in] rsp The exist response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status Exist(const ExistReqPb &req, ExistRspPb &rsp) override;

    /**
     * @brief Send request of set expiration time for metas.
     * @param[in] req The expire request protobuf.
     * @param[in] rsp The expire response protobuf.
     * @return Status of the call.
     */
    Status Expire(const ExpireReqPb &req, ExpireRspPb &rsp) override;

    /**
     * @brief Get device meta info of the keys.
     * @param[in] req The exist request protobuf.
     * @param[in] rsp The exist response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp) override;

    /**
     * @brief Remove write back before shuhtdown
     * @return Status of the call
     */
    Status RemoveWriteBackIdsLocation();

    /**
     * @brief Create multiple objects
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return Status of the call
     */
    Status MultiCreate(const MultiCreateReqPb &req, MultiCreateRspPb &resp) override;

    /**
     * @brief Check local node is exiting or not.
     * @return True if local node is exiting
     */
    bool MigrateDataStarted()
    {
        return etcdCM_->IsDataMigrationStarted();
    }

    /**
     * @brief Create multiple objects
     * @param[in] clientId The client id.
     * @param[in] supportMultiShmRefCount Whether the client support multiple shared memory references.
     */
    void InitShmRefForClient(const ClientKey &clientId, bool supportMultiShmRefCount);

    /**
     * @brief Migrate data by triggering remote get during voluntary scale down.
     * @param[in] req rpc request.
     * @param[out] rsp rpc response.
     * @return Status of the call.
     */
    Status NotifyRemoteGet(const NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp);

private:
    friend class MasterWorkerOCServiceImpl;
    friend class WorkerWorkerOCServiceImpl;
    friend class WorkerDeviceOcManager;

    struct PublishParams {
        const ObjectLifeState lifeState;
        const std::vector<std::string> &nestedObjectKeys;
        bool isRetry = false;
        uint32_t ttlSecond;
        int existence;
    };

    /**
     * @brief Initializes resources related to the l2 cache.
     * @return Status of the call.
     */
    Status InitL2Cache();

    /**
     * @brief Reconciliation with master when worker.
     * @param[in] req
     * @return Status of the call.
     */
    Status Reconciliation(const PushMetaToWorkerReqPb &req);

    /**
     * @brief Create a shared memory communication channel.
     * @param[out] decreaseRPCQ The rpc circular queue.
     * @return K_OK on success; the error code otherwise.
     */
    Status InitShmCircularQueue(std::shared_ptr<ShmCircularQueue> &decreaseRPCQ);

    /**
     * @brief Processing decrease messages from client.
     * @param[in] element A single messages from client.
     */
    void DecreaseHandlerForShmQueue(uint8_t *element);

    /**
     * @brief Use a thread to continuously listen to the client side of the message.
     * @return K_OK on success; the error code otherwise.
     */
    Status StartDecreaseReferenceProcess();

    /**
     * @brief Decrease the reference count of client with out authenticate check.
     * @param[in] clientId The client making this request..
     * @param[in] shmIds The ids of object reference.
     * @return K_OK on success; the error code otherwise.
     */
    Status DecreaseMemoryRef(const ClientKey &clientId, const std::vector<ShmKey> &shmIds);

    /**
     * @brief Get object data from remote cache (remote worker or redis) based on object meta.
     * @param[in] readKey read key info, contain offset, size, objKey.
     * @param[in] queryMeta The object meta info contains remote address and data size.
     * @param[in] payloads  Get payloads that contains object data.
     * @return Status of the call.
     */
    Status GetObjectFromAnywhere(const ReadKey &readKey, const master::QueryMetaInfoPb &queryMeta,
                                 std::vector<RpcMessage> &payloads);

    /**
     * @brief Get data from L2Cache for primary copy.
     * @param[in] objectKey Object key.
     * @param[in] version Expected object version.
     * @param[in] safeEntry The safe object entry.
     * @return Status of the call.
     */
    Status GetDataFromL2CacheForPrimaryCopy(const std::string &objectKey, uint64_t version,
                                            std::shared_ptr<SafeObjType> &safeEntry);

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
    Status DecNestedRef(const std::vector<std::string> &nestedObjectKeys);

    /**
     * @brief Get or Create a worker to Master api object for objKey in format uuid:host:port
     * @param[in] objKey Object key that contain remote master's ip address
     * @param[out] metaAddrInfo The meta address information.
     * @return Status of the call.
     */
    Status GetMetaAddressNotCheckConnection(const std::string &objKey, MetaAddrInfo &metaAddrInfo) const;

    /**
     * @brief Update object version from worker or redis when object is expired
     * @param[in] objectKV The ObjCacheShmUnit to that we are updating and its corresponding objectKey
     * Note that the object itself is locked by the caller first.
     * @return Status of the call.
     */
    Status UpdateObjectVersion(ObjectKV &objectKV);

    /**
     * @brief Send request to master to remove meta and get response.
     * @param[in] objectKeysRemove The ids of the objects which will be removed.
     * @param[in] removeCause Cause for remove.
     * @return Status of the call.
     */
    Status RemoveMetaFromMaster(const std::list<std::string> &objectKeysRemove,
                                master::RemoveMetaReqPb::Cause removeCause);

    /**
     * @brief Delete local object.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status DeleteCopyNotification(const DeleteObjectReqPb &req, DeleteObjectRspPb &rsp);

    /**
     * @brief Delete object from L2 persistence only.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status DeletePersistenceObject(const DeletePersistenceObjectReqPb &req, DeletePersistenceObjectRspPb &rsp);

    /**
     * @brief Delete object if memory ref count is 0, otherwise set buffer invalid.
     * @param[in] objectKey The object key.
     * @param[in] version The version of the object.
     * @return Status of the call
     */
    Status DeleteObject(const std::string &objectKey, uint64_t version = 0);

    /**
     * @brief Create or update metadata to master, object will be unlocked during requesting master.
     * @param[in] objectKV Safe object entry and its corresponding objectKey.
     * @param[in] params Publishing parameters.
     * @return Status of the call.
     */
    Status RequestingToMaster(ObjectKV &objectKV, const PublishParams &params);

    /**
     * @brief Sending Asynchronous RPCs to Clear Heterogeneous Metadata.
     * @param[in] clientId the client id.
     * @return Status of the call.
     */
    Status ClearDeviceMetaData(const ClientKey &clientId);

    /**
     * @brief Async clear client metadata in master side.
     * @param[in] clientId The client id.
     * @param[in] retryTimes The retry info.
     */
    void AsyncClearClientRef(const ClientKey &clientId, uint64_t retryTimes = 0);

    /**
     * @brief Lock a batch of objects.
     * @param[in] objectKeys The object key.
     * @param[out] lockedEntries Locked entries map.
     */
    void BatchLock(const std::vector<std::string> &objectKeys,
                   std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries);

    /**
     * @brief Indicates whether the client allows shared memory.
     * @return true if client enable share memory.
     */
    static bool ClientShmEnabled(const std::string &clientId);

    /**
     * @brief Init the metadata size.
     */
    void InitMetaSize();

    Status TryUnShmQueueLatch(uint32_t lockId);

    /**
     * @brief Try unlatch the lock when client crash.
     * @param[in] pointer Shm buffer pointer.
     * @param[in] lockId Lock id for client.
     */
    static void TryUnlatch(void *pointer, int lockId);

    static const bool OUTER_LOCK = true;
    static const bool BOTH_LOCKS = true;
    /**
     * @brief Add object table data to heartbeat request
     * @param[in] req The heartbeat request extended protobuf
     * @param[in] metaAddrInfo The meta data address information
     * @return Status of the call
     */
    Status FillObjData(master::PushMetaToMasterReqPb &req, const MetaAddrInfo &metaAddrInfo);

    /**
     * @brief Add all object references belong master to heartbeat request
     * @param[in] targetMetaAddrInfo The meta data address information
     * @param[in] objectKeys The object keys.
     */
    void FillRefData(const MetaAddrInfo &targetMetaAddrInfo, std::vector<std::string> &objectKeys);

    /**
     * @brief send requests decreasing gref to masters.
     * @param[in] objKeysGrpByMaster object keys grouped by master.
     * @return Status
     */
    Status ReconciliationDecrRef(const std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMaster);

    /**
     * @brief Helper function to assign fields to the metadata protobuf
     * @param[in] metadata Metadata of the object to be filled in.
     * @param[in] objectKey The ID of teh object.
     * @param[in] obj Object entry.
     */
    static void SetObjectMetaFields(ObjectMetaPb *metadata, const std::string &objectKey, const SafeObjType &obj)
    {
        metadata->set_object_key(objectKey);
        metadata->set_data_size(obj->GetDataSize());
        metadata->set_life_state((uint64_t)obj->GetLifeState());
        metadata->set_version(obj->GetCreateTime());
        metadata->set_ttl_second(obj->GetTtlSecond());
    }

    /**
     * @brief Find object keys in currentIds but not in rsp, and add to objectKeysMayInOtherAz
     * @param[in] rsp The response after querying metadata from master.
     * @param[in] currentIds The vector of objects which query metadata from master.
     * @param[out] objectKeysMayInOtherAz Store the objects not in response.
     * @return Status
     */
    static void FindObjectKeyNotInRsp(std::vector<master::QueryMetaInfoPb> &queryMetas,
                                      std::vector<std::string> &currentIds,
                                      std::vector<std::string> &objectKeysMayInOtherAz);

    /**
     * @brief Check whether the size of the node table in EtcdClusterManager equals to the number of running workers.
     * If not, wait until they are equal or time is out.
     * @return Status
     */
    Status CheckWaitNodeTableComplete();

    /**
     * @brief Get all objectKeys from objectTable_.
     * @param[out] objectKeys All objectKeys of objectTable_.
     */
    void GetAllObjectKeys(std::vector<std::string> &objectKeys);

    /**
     * @brief Fill object metadata.
     * @param[in] objectKey The id of object.
     * @param[in] targetMetaAddrInfo The meta address information.
     * @param[out] metadata Object meta to fill.
     * @param[out] isFill The metadata is fill or not.
     */
    void FillMetadata(const std::string &objectKey, const MetaAddrInfo &targetMetaAddrInfo, ObjectMetaPb *metadata,
                      bool &isFill);

    /**
     * @brief Check node table; remove "restart" tag; set health file.
     * @param[in] req PushMetaToWorkerReqPb request.
     * @return OK if success.
     */
    Status GetReadyToWork(const PushMetaToWorkerReqPb &req);

    /**
     * @brief Check if object is in rollback progress.
     * @param[in] objectKey Object key.
     * @return True if object is in rollback progress.
     */
    bool IsInRollbackProgress(const std::string &objectKey);

    /**
     * @brief The rpc method used to delete the device objects.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return Status of the call.
     */
    Status DeleteDevObjects(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp);

    HostPort localMasterAddress_;

    // Acquire writer lock before doing reconciliation; read lock before other RPCs
    // We want to make sure each time one thread doing reconciliations
    // and every thread doing reconciliation won't go in parallel with other common RPC threads.
    // Also protects numRecon_, lastReconTime_.
    WriterPrefRWLock reconFlag_;
    uint16_t numRecon_{ 0 };                    // the number of nodes which reconciled with this node.
    int64_t lastReconTime_{ 0 };                // the last time when reconciliation was done.
    std::atomic<bool> setHealthFile_{ false };  // health file set or not.
    int64_t timestamp_{ 0 };                    // the timestamp of the event that this node is reconciling for.

    // this class manages list of all masters for our objects
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager_{ nullptr };
    std::unique_ptr<MetaDataRecoveryManager> metadataRecoveryManager_{ nullptr };
    std::unique_ptr<WorkerOcServiceClearDataFlow> clearDataFlow_{ nullptr };

    WorkerRequestManager workerRequestManager_;

    // The initOkFuture_ is used to control the synchronization between the MasterOCServiceImpl and WorkerOCServiceImpl
    // to avoid the possible asynchronous logic of the MasterOCServiceImpl, such as the ExpiredObjectManager thread,
    // which causes a core dump when accessing the local Worker while the WorkerOCServiceImpl has not been initialized.
    std::promise<Status> initOk_;
    std::shared_future<Status> initOkFuture_;
    std::atomic<bool> setValue_ = false;
    std::shared_ptr<PersistenceApi> persistenceApi_{ nullptr };
    std::shared_ptr<SharedMemoryRefTable> memoryRefTable_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable_;
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<SlotRecoveryManager> slotRecoveryManager_{ nullptr };
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    std::shared_ptr<WorkerDeviceOcManager> workerDevOcManager_{ nullptr };
    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager
    EtcdStore *etcdStore_;                   // pointer to EtcdStore in WorkerOcServer
    // Wait for client reconnect when worker crash and recovery.
    WaitPost clientReconnectPost_;
    bool waited_{ false };
    std::shared_ptr<ThreadPool> memCpyThreadPool_{ nullptr };
    // threadPool_ must be destruct before clientReconnectPost_
    std::shared_ptr<ThreadPool> threadPool_{ nullptr };
    std::unique_ptr<ThreadPool> devThreadPool_{ nullptr };
    // gcThreadPool_ must be destruct before evictionManger_
    std::unique_ptr<ThreadPool> gcThreadPool_{ nullptr };
    // the metadata size
    size_t metadataSize_{ 0 };
    // support leve2 storage type
    L2StorageType supportL2Storage_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    std::shared_ptr<AsyncRpcRequestManager> asyncRpcManager_{ nullptr };

    std::shared_ptr<std::atomic_bool> exitFlag_;
    // decThreadPool_ must be destruct before decreaseRPCQ_
    std::unique_ptr<ThreadPool> decThreadPool_{ nullptr };
    std::mutex circularQueueMutex_;  // To protect circularQueueManager_
    std::vector<std::shared_ptr<ShmCircularQueue>> circularQueueManager_;

    std::shared_timed_mutex clearIdsMutex_;                     // to protect voluntaryScaleDownClearIds_
    std::vector<std::string> voluntaryScaleDownClearIds_ = {};  // need clear ids before voluntary scaledown
    /**
     * the thread pool is only use for delete old version of object in l2cache.
     *
     * when use l2cache to persistence, it store the object by version; that is, a version is corresponds to a
     * file in l2cache; when the object is update, the old version object in l2cache need delete
     *
     */
    std::shared_ptr<ThreadPool> oldVerDelAsyncPool_{ nullptr };

    std::shared_ptr<AsyncSendManager> asyncSendManager_{ nullptr };

    std::shared_ptr<AsyncRollbackManager> asyncRollbackManager_{ nullptr };

    std::shared_ptr<AsyncPersistenceDelManager> asyncPersistenceDelManager_{ nullptr };

    std::shared_ptr<WorkerOcServiceCreateImpl> createProc_{ nullptr };

    std::shared_ptr<WorkerOcServicePublishImpl> publishProc_{ nullptr };

    std::shared_ptr<WorkerOcServiceMultiPublishImpl> multiPublishProc_{ nullptr };

    std::shared_ptr<WorkerOcServiceGetImpl> getProc_{ nullptr };

    std::shared_ptr<WorkerOcServiceDeleteImpl> deleteProc_{ nullptr };

    std::shared_ptr<WorkerOcServiceGlobalReferenceImpl> gRefProc_{ nullptr };

    std::shared_ptr<WorkerOcServiceMigrateImpl> gMigrateProc_{ nullptr };

    std::shared_ptr<WorkerOcServiceExpireImpl> expireProc_{ nullptr };
    AsyncTasksDoneChecker asyncTasksDoneChecker_{ nullptr };
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_SERVICE_IMPL_H
