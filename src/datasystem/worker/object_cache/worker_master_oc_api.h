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
 * Description: Defines the worker client class to communicate with the worker service.
 */
#ifndef DATASYSTEM_WORKER_WORKER_MASTER_OC_API_H
#define DATASYSTEM_WORKER_WORKER_MASTER_OC_API_H

#include <list>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/object_cache/async_rpc_request_manager.h"
#include "datasystem/worker/worker_master_api_manager_base.h"

namespace datasystem {
namespace master {
class MasterOCServiceImpl;
}
namespace worker {
/**
 * @brief The WorkerMasterOCApi is an abstract class that defines the interface for interactions with the object cache
 * master service.
 */
class WorkerMasterOCApi {
public:
    /**
     * @brief Destructor
     */
    virtual ~WorkerMasterOCApi() = default;

    /**
     * @brief Initialize the WorkerMasterOCApi Object.
     * @return Status of the call.
     */
    virtual Status Init() = 0;

    /**
     * @brief Create object meta in cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status CreateMeta(master::CreateMetaReqPb &request, master::CreateMetaRspPb &response) = 0;

    /**
     * @brief Report the worker's memory left info and get all workers memory left infos in the cluster.
     * @param[in] request The rpc req protobuf.
     * @param[out] response The rpc rsp protobuf.
     * @return Status of the call.
     */
    virtual Status ReportResource(master::ResourceReportReqPb &request, master::ResourceReportRspPb &response) = 0;

    /**
     * @brief Create multiple objects' meta in cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @param[in] retry Whether to retry when an error occurs.
     * @return Status of the call.
     */
    virtual Status CreateMultiMeta(master::CreateMultiMetaReqPb &request, master::CreateMultiMetaRspPb &response,
                                   bool retry = true) = 0;

    /**
     * @brief Phase two creating meta for multiple objects.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status CreateMultiMetaPhaseTwo(master::CreateMultiMetaPhaseTwoReqPb &request,
                                           master::CreateMultiMetaRspPb &response) = 0;

    /**
     * @brief Create object copy meta in cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status CreateCopyMeta(master::CreateCopyMetaReqPb &request, master::CreateCopyMetaRspPb &response) = 0;

    /**
     * @brief Create multi object copy meta in cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status CreateMultiCopyMeta(master::CreateMultiCopyMetaReqPb &request,
                                        master::CreateMultiCopyMetaRspPb &response) = 0;

    /**
     * @brief Query object meta from cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[in] subTimeout The timeout for subscribe.
     * @param[out] response The rpc response protobuf.
     * @param[out] payloads The object data, it may exist.
     * @return Status of the call.
     */
    virtual Status QueryMeta(master::QueryMetaReqPb &request, uint64_t subTimeout, master::QueryMetaRspPb &response,
                             std::vector<RpcMessage> &payloads) = 0;

    /**
     * @brief Remove object meta in cache and rocksdb.
     * @param[in] request Req of call.
     * @param[out] response Rsp of call.
     * @return Status of the call.
     */
    virtual Status RemoveMeta(master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) = 0;

    /**
     * @brief Sends request to increment nested ref count on object or set of objects.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status GIncNestedRef(master::GIncNestedRefReqPb &request, master::GIncNestedRefRspPb &response) = 0;

    /**
     * @brief Sends request to decrement nested ref count on object or set of objects.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status GDecNestedRef(master::GDecNestedRefReqPb &request, master::GDecNestedRefRspPb &response) = 0;

    /**
     * @brief Update object meta in cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status UpdateMeta(master::UpdateMetaReqPb &request, master::UpdateMetaRspPb &response) = 0;

    /**
     * @brief Send request to master to delete meta and get response.
     *        Master will notify other workers to delete these objects asynchronously.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status DeleteAllCopyMeta(master::DeleteAllCopyMetaReqPb &request,
                                     master::DeleteAllCopyMetaRspPb &response) = 0;

    /**
     * @brief Send request to master to decrease the global reference count.
     * @param[in] finishDecIds The object keys to increase in worker.
     * @param[out] unAliveIds The object keys gRef is 0, can be destroy.
     * @param[out] failDecIds The rpc response protobuf.
     * @param[in] remoteClientId The remote client id.
     * @return Status of the call.
     */
    virtual Status GDecreaseMasterRef(const std::vector<std::string> &finishDecIds,
                                      std::unordered_set<std::string> &unAliveIds, std::vector<std::string> &failDecIds,
                                      const std::string &remoteClientId = "") = 0;

    /**
     * @brief Send request to master to decrease all objects of remote client id.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status ReleaseGRefs(master::ReleaseGRefsReqPb &request, master::ReleaseGRefsRspPb &response) = 0;

    /**
     * @brief Send request to master to increase the objects.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status GIncreaseMasterRef(master::GIncreaseReqPb &incReq, master::GIncreaseRspPb &incRsp) = 0;

    /**
     * @brief Send request to master to decrease the objects.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status GDecreaseMasterRef(master::GDecreaseReqPb &decReq, master::GDecreaseRspPb &decRsp) = 0;

    /**
     * @brief Send the request of querying all objects' global references to master.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status QueryGlobalRefNum(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp) = 0;

    /*
     * @brief Push metadata to master.
     * @param[in] request The rpc request protobuf
     * @param[out] response The rpc response protobuf
     * @return Status of the call
     */
    virtual Status PushMetadataToMaster(master::PushMetaToMasterReqPb &req, master::PushMetaToMasterRspPb &rsp) = 0;

    /*
     * @brief Rollback seal.
     * @param[in] objectKey Object key to be seal
     * @param[out] oldLifeState old life state
     * @return Status of the call
     */
    virtual Status RollbackSeal(const std::string &objectKey, uint32_t oldLifeState) = 0;

    /**
     * @brief Send request of set expiration time for metas.
     * @param[in] req The expire request protobuf.
     * @param[in] rsp The expire response protobuf.
     * @return Status of the call.
     */
    virtual Status Expire(master::ExpireReqPb &req, master::ExpireRspPb &rsp) = 0;

    /**
     * @brief A factory method to instantiate the correct derived version of the api. Remote masters will use an
     * rpc-based api, whereas local masters can be optimized for in-process pointer based api.
     * @param[in] hostPort The host port of the target master
     * @param[in] localHostPort The local worker rpc service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] service The local pointer to the master OC service implementation. If null, the created api must
     * default to the RPC-based version.
     * @return A base class pointer to the correct derived type of api.
     */
    static std::shared_ptr<WorkerMasterOCApi> CreateWorkerMasterOCApi(const HostPort &hostPort,
                                                                      const HostPort &localHostPort,
                                                                      std::shared_ptr<AkSkManager> akSkManager,
                                                                      master::MasterOCServiceImpl *service = nullptr);

    /**
     * @brief Check if the asking worker needs reconciliaton. If so, trigger reconciliation.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status IfNeedTriggerReconciliation(master::ReconciliationQueryPb &req,
                                               master::ReconciliationRspPb &rsp) = 0;

    /**
     * @brief Return the ip:port of the target node.
     * @return The ip:port of the target node.
     */
    virtual std::string GetHostPort() = 0;

    /**
     * @brief Put p2p metadata to master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status PutP2PMeta(PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp) = 0;

    /**
     * @brief Subscribe the p2p receiving event.
     * @param[in] req The rpc req protobuf.
     * @param[in] serverApi The unary writer reader.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status SubscribeReceiveEvent(
        SubscribeReceiveEventReqPb &req,
        std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi,
        std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) = 0;

    /**
     * @brief Get p2p metadata from master.
     * @param[in] req The rpc req protobuf.
     * @param[in] serverApi The unary writer reader.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status GetP2PMeta(GetP2PMetaReqPb &req,
                              std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi,
                              std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) = 0;

    /**
     * @brief Send root info to master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp) = 0;

    /**
     * @brief Receive root info to master.
     * @param[in] req The rpc req protobuf.
     * @param[in] serverApi The unary writer reader.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status RecvRootInfo(
        RecvRootInfoReqPb &req,
        std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi,
        std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) = 0;

    /**
     * @brief GetDataInfo from master.
     * @param[in] req The rpc req protobuf.
     * @param[in] serverApi The unary writer reader.
     * @param[in] subTimeoutMs The timeout for subscribe of the GetDataInfo request.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status GetDataInfo(
        GetDataInfoReqPb &req,
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi,
        const int64_t subTimeoutMs, std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) = 0;

    /**
     * @brief Acknowledging data receiving finished.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status AckRecvFinish(AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp) = 0;

    /**
     * @brief Remove data location from master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status RemoveP2PLocation(RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp) = 0;

    /**
     * @brief Get data location from master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status GetObjectLocations(master::GetObjectLocationsReqPb &req, master::GetObjectLocationsRspPb &resp) = 0;

    /**
     * @brief Send root info to master.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status ReleaseMetaData(ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp) = 0;

    virtual Status ReplacePrimary(master::ReplacePrimaryReqPb &req, master::ReplacePrimaryRspPb &rsp) = 0;

    virtual Status PureQueryMeta(master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp) = 0;

    virtual Status CheckObjectDataLocation(master::CheckObjectDataLocationReqPb &req,
                                           master::CheckObjectDataLocationRspPb &rsp) = 0;

    /**
     * @brief Rollback metadata in directory.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status RollbackMultiMeta(master::RollbackMultiMetaReqPb &req, master::RollbackMultiMetaRspPb &rsp) = 0;

    /**
     * @brief Get device meta info of the keys.
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return Status of the call.
     */
    virtual Status GetMetaInfo(GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp) = 0;

protected:
    /**
     * @brief Construct WorkerMasterOCApi object. Protected constructor enforces class instantiation through the factory
     * method CreateWorkerMasterOCApi.
     * @param[in] localHostPort The local worker service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    WorkerMasterOCApi(const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager);

    HostPort localHostPort_;  // The HostPort of the local node
    std::shared_ptr<AkSkManager> akSkManager_;
};

/**
 * @brief WorkerRemoteMasterApi is the derived remote version of the api for sending and receiving master OC requests
 * where the master is on a different host. This class will use an RPC mechanism for communication to the remote
 * location.
 * Callers will access this class naturally through base class polymorphism.
 * See the parent interface for function argument documentation.
 */
class WorkerRemoteMasterOCApi : public WorkerMasterOCApi {
public:
    /**
     * @brief Constructor for the remote version of the api
     * @param[in] hostPort The host port of the target master
     * @param[in] localHostPort The local worker rpc service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    explicit WorkerRemoteMasterOCApi(const HostPort &hostPort, const HostPort &localHostPort,
                                     std::shared_ptr<AkSkManager> akSkManager);
    ~WorkerRemoteMasterOCApi() override = default;
    Status Init() override;
    Status CreateMeta(master::CreateMetaReqPb &request, master::CreateMetaRspPb &response) override;
    Status ReportResource(master::ResourceReportReqPb &request, master::ResourceReportRspPb &response) override;
    Status CreateMultiMeta(master::CreateMultiMetaReqPb &request, master::CreateMultiMetaRspPb &response,
                           bool retry = true) override;
    Status CreateMultiMetaPhaseTwo(master::CreateMultiMetaPhaseTwoReqPb &request,
                                   master::CreateMultiMetaRspPb &response) override;
    Status CreateCopyMeta(master::CreateCopyMetaReqPb &request, master::CreateCopyMetaRspPb &response) override;
    Status CreateMultiCopyMeta(master::CreateMultiCopyMetaReqPb &request,
                                master::CreateMultiCopyMetaRspPb &response) override;
    Status QueryMeta(master::QueryMetaReqPb &request, uint64_t subTimeout, master::QueryMetaRspPb &response,
                     std::vector<RpcMessage> &payloads) override;
    Status RemoveMeta(master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) override;
    Status GIncNestedRef(master::GIncNestedRefReqPb &request, master::GIncNestedRefRspPb &response) override;
    Status GDecNestedRef(master::GDecNestedRefReqPb &request, master::GDecNestedRefRspPb &response) override;
    Status UpdateMeta(master::UpdateMetaReqPb &request, master::UpdateMetaRspPb &response) override;
    Status DeleteAllCopyMeta(master::DeleteAllCopyMetaReqPb &request,
                             master::DeleteAllCopyMetaRspPb &response) override;
    Status GDecreaseMasterRef(const std::vector<std::string> &finishDecIds, std::unordered_set<std::string> &unAliveIds,
                              std::vector<std::string> &failDecIds, const std::string &remoteClientId = "") override;
    Status ReleaseGRefs(master::ReleaseGRefsReqPb &request, master::ReleaseGRefsRspPb &response) override;
    Status GIncreaseMasterRef(master::GIncreaseReqPb &incReq, master::GIncreaseRspPb &incRsp) override;
    Status GDecreaseMasterRef(master::GDecreaseReqPb &decReq, master::GDecreaseRspPb &decRsp) override;
    Status QueryGlobalRefNum(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp) override;
    Status PushMetadataToMaster(master::PushMetaToMasterReqPb &req, master::PushMetaToMasterRspPb &rsp) override;
    Status RollbackSeal(const std::string &objectKey, uint32_t oldLifeState) override;
    Status IfNeedTriggerReconciliation(master::ReconciliationQueryPb &req, master::ReconciliationRspPb &rsp) override;
    std::string GetHostPort() override;

    Status PutP2PMeta(PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp) override;
    Status SubscribeReceiveEvent(
        SubscribeReceiveEventReqPb &req,
        std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi,
        std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) override;
    Status GetP2PMeta(GetP2PMetaReqPb &req,
                      std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi,
                      std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) override;
    Status SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp) override;
    Status RecvRootInfo(RecvRootInfoReqPb &req,
                        std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi,
                        std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) override;
    Status AckRecvFinish(AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp) override;
    Status GetDataInfo(
        GetDataInfoReqPb &req,
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi,
        const int64_t subTimeoutMs, std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) override;
    Status RemoveP2PLocation(RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp) override;
    Status GetObjectLocations(master::GetObjectLocationsReqPb &req, master::GetObjectLocationsRspPb &resp) override;
    Status ReleaseMetaData(ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp) override;
    Status ReplacePrimary(master::ReplacePrimaryReqPb &req, master::ReplacePrimaryRspPb &rsp) override;
    Status PureQueryMeta(master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp) override;
    Status CheckObjectDataLocation(master::CheckObjectDataLocationReqPb &req,
                                   master::CheckObjectDataLocationRspPb &rsp) override;
    Status RollbackMultiMeta(master::RollbackMultiMetaReqPb &req, master::RollbackMultiMetaRspPb &rsp) override;
    Status Expire(master::ExpireReqPb &req, master::ExpireRspPb &rsp) override;
    Status GetMetaInfo(GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp) override;

private:
    HostPort hostPort_;                                                    // The HostPort of the master node
    std::shared_ptr<master::MasterOCService_Stub> rpcSession_{ nullptr };  // session to the master rpc service
};

/**
 * @brief WorkerLocalMasterOCApi is the derived local version of the api for sending and receiving master OC requests
 * where the master exists in the same process as the service. This class will directly reference the service through a
 * pointer and does not use any RPC mechanism for communication.
 * Callers will access this class naturally through base class polymorphism.
 * See the parent interface for function argument documentation.
 */
class WorkerLocalMasterOCApi : public WorkerMasterOCApi {
public:
    /**
     * @brief Constructor for the local version of the api
     * @param[in] service The pointer to the master OC service implementation
     * @param[in] localHostPort The local worker service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    WorkerLocalMasterOCApi(master::MasterOCServiceImpl *service, const HostPort &localHostPort,
                           std::shared_ptr<AkSkManager> akSkManager);
    ~WorkerLocalMasterOCApi() override = default;
    Status Init() override;
    Status CreateMeta(master::CreateMetaReqPb &request, master::CreateMetaRspPb &response) override;
    Status ReportResource(master::ResourceReportReqPb &request, master::ResourceReportRspPb &response) override;
    Status CreateMultiMeta(master::CreateMultiMetaReqPb &request, master::CreateMultiMetaRspPb &response,
                           bool retry = true) override;
    Status CreateMultiMetaPhaseTwo(master::CreateMultiMetaPhaseTwoReqPb &request,
                                   master::CreateMultiMetaRspPb &response) override;
    Status CreateCopyMeta(master::CreateCopyMetaReqPb &request, master::CreateCopyMetaRspPb &response) override;
    Status CreateMultiCopyMeta(master::CreateMultiCopyMetaReqPb &request,
                                master::CreateMultiCopyMetaRspPb &response) override;
    Status QueryMeta(master::QueryMetaReqPb &request, uint64_t subTimeout, master::QueryMetaRspPb &response,
                     std::vector<RpcMessage> &payloads) override;
    Status RemoveMeta(master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) override;
    Status GIncNestedRef(master::GIncNestedRefReqPb &request, master::GIncNestedRefRspPb &response) override;
    Status GDecNestedRef(master::GDecNestedRefReqPb &request, master::GDecNestedRefRspPb &response) override;
    Status UpdateMeta(master::UpdateMetaReqPb &request, master::UpdateMetaRspPb &response) override;
    Status DeleteAllCopyMeta(master::DeleteAllCopyMetaReqPb &request,
                             master::DeleteAllCopyMetaRspPb &response) override;
    Status GDecreaseMasterRef(const std::vector<std::string> &finishDecIds, std::unordered_set<std::string> &unAliveIds,
                              std::vector<std::string> &failDecIds, const std::string &remoteClientId = "") override;
    Status ReleaseGRefs(master::ReleaseGRefsReqPb &request, master::ReleaseGRefsRspPb &response) override;
    Status GIncreaseMasterRef(master::GIncreaseReqPb &incReq, master::GIncreaseRspPb &incRsp) override;
    Status GDecreaseMasterRef(master::GDecreaseReqPb &decReq, master::GDecreaseRspPb &decRsp) override;
    Status QueryGlobalRefNum(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp) override;
    Status PushMetadataToMaster(master::PushMetaToMasterReqPb &req, master::PushMetaToMasterRspPb &rsp) override;
    Status RollbackSeal(const std::string &objectKey, uint32_t oldLifeState) override;
    Status IfNeedTriggerReconciliation(master::ReconciliationQueryPb &req, master::ReconciliationRspPb &rsp) override;
    std::string GetHostPort() override;
    Status Expire(master::ExpireReqPb &req, master::ExpireRspPb &rsp) override;
    Status GetMetaInfo(GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp) override;

    Status PutP2PMeta(PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp) override;
    Status SubscribeReceiveEvent(
        SubscribeReceiveEventReqPb &req,
        std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi,
        std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) override;
    Status GetP2PMeta(GetP2PMetaReqPb &req,
                      std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi,
                      std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) override;
    Status SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp) override;
    Status RecvRootInfo(RecvRootInfoReqPb &req,
                        std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi,
                        std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) override;
    Status AckRecvFinish(AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp) override;
    Status GetDataInfo(
        GetDataInfoReqPb &req,
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi,
        const int64_t subTimeoutMs, std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager) override;
    Status RemoveP2PLocation(RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp) override;
    Status GetObjectLocations(master::GetObjectLocationsReqPb &req, master::GetObjectLocationsRspPb &resp) override;
    Status ReleaseMetaData(ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp) override;
    Status ReplacePrimary(master::ReplacePrimaryReqPb &req, master::ReplacePrimaryRspPb &rsp) override;
    Status PureQueryMeta(master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp) override;
    Status CheckObjectDataLocation(master::CheckObjectDataLocationReqPb &req,
                                   master::CheckObjectDataLocationRspPb &rsp) override;
    Status RollbackMultiMeta(master::RollbackMultiMetaReqPb &req, master::RollbackMultiMetaRspPb &rsp) override;
    template <typename W, typename R>
    Status ReplyToClient(const std::shared_future<std::pair<W, Status>> future,
                         const std::shared_ptr<ServerUnaryWriterReader<W, R>> serverApi)
    {
        // Waiting for the asynchronous RPC response from the local master
        std::pair<W, Status> result;
        try {
            result = future.get();
        } catch (const std::exception &e) {
            return serverApi->SendStatus(
                { K_RUNTIME_ERROR, FormatString("Exception when calling future.get(): %s ", e.what()) });
        }
        // reply to client
        if (result.second.IsError()) {
            return serverApi->SendStatus(result.second);
        } else {
            return serverApi->Write(result.first);
        }
    }

    /**
     * @brief Notify cross-az deletion
     * @param[in] objsNeedAsyncNotify The objs grouping by <objectKey, azNames> that need to notify
     */
    void AsyncNotifyCrossAzDelete(const std::unordered_map<std::string, std::vector<std::string>> &objsNeedAsyncNotify);

private:
    master::MasterOCServiceImpl *masterOC_;  // A direct back-pointer to the local service
};

class WorkerMasterOcApiManager : public WorkerMasterApiManagerBase<WorkerMasterOCApi> {
public:
    WorkerMasterOcApiManager(HostPort &hostPort, std::shared_ptr<AkSkManager> manager,
                             master::MasterOCServiceImpl *masterOCService);

    /**
     * @brief Create a worker to Master api object for masterAddress
     * @param[in] masterAddress The remote master ip address
     * @return The WorkerMasterOCApi
     */
    std::shared_ptr<WorkerMasterOCApi> CreateWorkerMasterApi(const HostPort &masterAddress) override;

private:
    master::MasterOCServiceImpl *masterOCService_{ nullptr };
};

}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_WORKER_MASTER_OC_API_H
