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
 * Description: Implement the object cache remote services on the master.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_OBJECT_CACHE_SERVICE_IMPL_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_OBJECT_CACHE_SERVICE_IMPL_H

#include <unordered_map>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/object_cache/device/master_dev_oc_manager.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/replica_manager.h"
#include "datasystem/master/resource_manager.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/master_object.service.rpc.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
namespace master {

class MasterOCServiceImpl final : public MasterOCService {
public:
    /**
     * @brief Construct MasterOCServiceImpl.
     */
    MasterOCServiceImpl(HostPort serverAddress, std::shared_ptr<PersistenceApi> persistApi,
                        std::shared_ptr<AkSkManager> akSkManager, ReplicaManager *replicaManager,
                        master::ResourceManager *resourceManager);

    /**
     * @brief Deconstruct MasterOCServiceImpl.
     */
    ~MasterOCServiceImpl();

    /**
     * @brief shutdown master oc server impl.
     */
    void Shutdown();

    /**
     * @brief Init the service.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Create object meta in cache and rocksdb.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status CreateMeta(const CreateMetaReqPb &req, CreateMetaRspPb &rsp) override;

    /**
     * @brief Create object copy meta in cache and rocksdb.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status CreateCopyMeta(const CreateCopyMetaReqPb &req, CreateCopyMetaRspPb &rsp) override;

    /**
     * @brief Create multi object copy meta in cache and rocksdb.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status CreateMultiCopyMeta(const CreateMultiCopyMetaReqPb &req, CreateMultiCopyMetaRspPb &rsp) override;

    /**
     * @brief Query object meta from cache or rocksdb.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @param[out] payloads If the object data is also in this nodes, put the data in payload directly and send back.
     * @return Status of the call.
     */
    Status QueryMeta(const QueryMetaReqPb &req, QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads) override;

    /**
     * @brief Get device meta info of the keys.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp) override;

    /**
     * @brief Remove object meta in cache and rocksdb.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status RemoveMeta(const RemoveMetaReqPb &req, RemoveMetaRspPb &rsp) override;

    /**
     * @brief Update object meta in cache and rocksdb.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status UpdateMeta(const UpdateMetaReqPb &req, UpdateMetaRspPb &rsp) override;

    /**
     * @brief Get object locations from master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status GetObjectLocations(const GetObjectLocationsReqPb &req, GetObjectLocationsRspPb &resp) override;

    /**
     * @brief Delete metadata and notify other workers to delete these objects.
     * @param[in] serverApi The ServerUnaryWriterReader object.
     * @return Status of the call.
     */
    Status DeleteAllCopyMeta(
        std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> serverApi) override;

    /**
     * @brief Delete metadata and notify other workers to delete these objects.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status DeleteAllCopyMeta(const DeleteAllCopyMetaReqPb &req, DeleteAllCopyMetaRspPb &rsp);

    /**
     * @brief Increases nested object reference count.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status GIncNestedRef(const GIncNestedRefReqPb &req, GIncNestedRefRspPb &rsp) override;

    /**
     * @brief Decreases nested object reference count.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status GDecNestedRef(const GDecNestedRefReqPb &req, GDecNestedRefRspPb &rsp) override;

    /**
     * @brief The rpc method used to increase the remote client id ref.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GIncreaseMasterAppRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp) override;

    /**
     * @brief The rpc method used to increase the global reference count of object.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp) override;

    /**
     * @brief The rpc method used to decrease the global reference count of object.
     * @param[in] serverApi The ServerUnaryWriterReader object.
     * @return K_OK on success; the error code otherwise.
     */
    Status GDecreaseRef(std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> serverApi) override;

    /**
     * @brief The rpc method used to decrease the global reference count of object.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp);

    /**
     * @brief Process req from worker to decrease all the objects of remote client id.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp) override;

    /**
     * @brief Process req from master to decrease all the obj of remote client id.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ReleaseGRefsOfRemoteClientId(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp) override;

    /**
     * @brief Helper function to send the rpc req for querying the target worker of objs' global references.
     * @param[in] addr The worker addr.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status SendQueryGRefReq(const HostPort &addr, QueryGlobalRefNumReqPb req, QueryGlobalRefNumRspPb &rsp);

    /**
     * @brief Query all objs' global references in the cluster.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    Status QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp) override;

    /**
     * @brief Helper function to generate Pbs of QueryGlobalRefNum.
     * @param[in] object_keys Specified object keys.
     * @return std::unordered_map<std::string, QueryGlobalRefNumReqPb> key is worker HostPort str, value is the Pb.
     */
    std::unordered_map<std::string, QueryGlobalRefNumReqPb> QueryWorkerGRefReqPbGen(
        const std::unordered_set<std::string> &object_keys,
        std::shared_ptr<master::OCMetadataManager> &ocMetadataManager);

    // These function about seqNo need keep until completely abandoned.
    Status IncrSeqNoMeta(const IncrSeqNoMetaReqPb &req, IncrSeqNoMetaRspPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GetSeqNoMeta(const GetSeqNoMetaReqPb &req, GetSeqNoMetaRspPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status DelSeqNoMeta(const DelSeqNoMetaReqPb &req, DelSeqNoMetaRspPb &rsp)
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    /**
     * @brief After the worker dies clears data and process pushed data by the worker.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status PushMetaToMaster(const PushMetaToMasterReqPb &req, PushMetaToMasterRspPb &rsp) override;

    /**
     * @brief Rollback the seal req.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status RollbackSeal(const RollbackSealReqPb &req, RollbackSealRspPb &rsp) override;

    /**
     * @brief Check if the asking worker needs reconciliaton. If so, trigger reconciliation.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status IfNeedTriggerReconciliationImpl(const ReconciliationQueryPb &req, ReconciliationRspPb &rsp);

    /**
     * @brief Check if the asking worker needs reconciliaton. If so, trigger reconciliation.
     * @param[in] serverApi The ServerUnaryWriterReader object.
     * @return K_OK on success; the error code otherwise.
     */
    Status IfNeedTriggerReconciliation(
        std::shared_ptr<ServerUnaryWriterReader<ReconciliationRspPb, ReconciliationQueryPb>> serverApi) override;

    /**
     * @brief Takes a copy of the local MasterWorkerService to allow rpc bypass when the master and worker are
     * collocated.
     * @param[in] masterWorkerService Pointer to the receiving side service of the MasterWorker api's (notifications)
     */
    void AssignLocalWorker(object_cache::MasterWorkerOCServiceImpl *masterWorkerService);

    /**
     * @brief Setter method for assigning cluster manager
     * @param[in] cm The pointer to etcd cluster manager
     */
    void SetClusterManager(EtcdClusterManager *cm)
    {
        etcdCM_ = cm;
    }

    /**
     * @brief Check whether there are any requests for asynchronously writing metadata to ETCD.
     * @return True if there are unfinished async requests.
     */
    bool HaveAsyncMetaRequest();

    /**
     * @brief Get the usage of ETCD async queue.
     * @return Usage: "currentSize/totalLimit/workerL2CacheQueueUsag".
     */
    std::string GetETCDAsyncQueueUsage();

    /**
     * @brief Get the async threadpool usage of master.
     * @return Usage: "idleNum/currentTotalNum/maxThreadNum/waitingTaskNum/threadPoolUsage".
     */
    std::string GetMasterAsyncPoolUsage();

    /**
     * @brief Processing Migration Data Flows
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status MigrateMetadata(const MigrateMetadataReqPb &req, MigrateMetadataRspPb &rsp) override;

    /**
    * @brief Report the memory info.
    * @param[in] req The rpc req protobuf.
    * @param[out] rsp The rpc rsp protobuf.
    * @return K_OK on success; the error code otherwise.
    */
    Status ReportResource(const ResourceReportReqPb &req, ResourceReportRspPb &rsp) override;

    /**
     * @brief Create multi meta.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     */
    Status CreateMultiMeta(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp) override;

    /**
     * @brief Create multi meta phase two.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     */
    Status CreateMultiMetaPhaseTwo(const CreateMultiMetaPhaseTwoReqPb &req, CreateMultiMetaRspPb &rsp) override;

    /**
     * @brief Put p2p metadata from master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status PutP2PMeta(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp) override;

    /**
     * @brief Replace objects primary copy location.
     * @param[in] The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ReplacePrimary(const ReplacePrimaryReqPb &req, ReplacePrimaryRspPb &rsp) override;

    /**
     * @brief Pure query objects metadata.
     * @param[in] The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status PureQueryMeta(const PureQueryMetaReqPb &req, PureQueryMetaRspPb &rsp) override;

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
     * @brief Receive root info from master.
     * @param[in] serverApi The unary writer reader.
     * @return K_OK on success; the error code otherwise.
     */
    Status RecvRootInfo(
        std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi) override;

    std::string GetDbName();

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
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp) override;

    /**
     * @brief Remove metadata in directory.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ReleaseMetaData(const ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp) override;

    /**
     * @brief Rollback metadata in directory.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status RollbackMultiMeta(const RollbackMultiMetaReqPb &req, RollbackMultiMetaRspPb &rsp) override;

    /**
     * @brief Notify cross-az deletion
     * @param[in] objsNeedAsyncNotify The objs grouping by <objectKey, azNames> that need to notify
     */
    void AsyncNotifyCrossAzDelete(const std::unordered_map<std::string, std::vector<std::string>> &objsNeedAsyncNotify);

    /**
     * @brief Expire metadata in directory.
     * @param[in] req The rpc req protobuf.
     * @param[in] rsp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status Expire(const ExpireReqPb &req, ExpireRspPb &rsp) override;

private:
    HostPort masterAddress_;
    std::shared_ptr<PersistenceApi> persistenceApi_;
    object_cache::MasterWorkerOCServiceImpl *masterWorkerOCService_{ nullptr };
    EtcdClusterManager *etcdCM_{ nullptr };
    std::shared_ptr<AkSkManager> akSkManager_;
    EtcdStore *etcdStore_;
    std::unique_ptr<ThreadPool> reconciliationAsyncPool_;
    ReplicaManager *replicaManager_;
    ResourceManager *resourceManager_;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_OBJECT_CACHE_SERVICE_IMPL_H
