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
 * Description: Declares the MasterWorkerOCApi class for master to communicate with the worker service.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_WORKER_OC_API_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_WORKER_OC_API_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"

namespace datasystem {

namespace object_cache {
class MasterWorkerOCServiceImpl;
}

namespace master {
static constexpr int64_t MASTER_TIMEOUT_MINUS_MILLISECOND = 5 * 1000;
static constexpr float MASTER_TIMEOUT_DESCEND_FACTOR = 0.9;
inline int64_t MasterGetRequestTimeout(int32_t timeout)
{
    return std::max(int64_t(timeout * MASTER_TIMEOUT_DESCEND_FACTOR), timeout - MASTER_TIMEOUT_MINUS_MILLISECOND);
}

/**
 * @brief The MasterWorkerOCApi is an abstract class that defines the interface for interactions with the object cache
 * worker service.
 */
class MasterWorkerOCApi {
public:
    virtual ~MasterWorkerOCApi() = default;

    /**
     * @brief Initialize the MasterWorkerOCApi Object.
     * @return Status of the call.
     */
    virtual Status Init() = 0;

    /**
     * @brief Publish already sealed object's meta data to its subscribers.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc request protobuf.
     * @return Status of the call.
     */
    virtual Status PublishMeta(PublishMetaReqPb &req, PublishMetaRspPb &resp) = 0;

    /**
     * @brief Clear data without meta.
     * @param[in] req The rpc request protobuf.
     * param[out] rsp The rpc request protobuf.
     */
    virtual Status ClearData(ClearDataReqPb &req, ClearDataRspPb &rsp) = 0;

    /**
     * @brief Notify worker to update object data.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc request protobuf.
     * @return Status of the call.
     */
    virtual Status UpdateNotification(UpdateObjectReqPb &req, UpdateObjectRspPb &rsp) = 0;

    /**
     * @brief Synchronize to notify worker to delete the object.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status DeleteNotification(std::unique_ptr<DeleteObjectReqPb> req, DeleteObjectRspPb &rsp) = 0;

    /**
     * @brief Notify worker to delete L2 persistence data only.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status DeletePersistenceObject(std::unique_ptr<DeletePersistenceObjectReqPb> req,
                                           DeletePersistenceObjectRspPb &rsp) = 0;

    /**
     * @brief Notify worker to delete object.
     * @param[in] req The rpc request protobuf.
     * @param[out] tag The flag to identify this rpc request.
     * @return Status of the call.
     */
    virtual Status DeleteNotificationSend(std::unique_ptr<DeleteObjectReqPb> req, int64_t &tag) = 0;

    /**
     * @brief Receive the delete notification response from worker.
     * @param[in] tag The flag to identify this rpc request.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status DeleteNotificationReceive(int64_t tag, DeleteObjectRspPb &rsp) = 0;

    /**
     * @brief Query the global references of all objs referred on the worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status QueryGlobalRefNumOnWorker(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspPb &rsp) = 0;

    /**
     * @brief Push metadata to the worker for reconciliation.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status PushMetaToWorker(PushMetaToWorkerReqPb &req, PushMetaToWorkerRspPb &rsp) = 0;

    /**
     * @brief Requests the worker to send meta to the master in the response.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status RequestMetaFromWorker(RequestMetaFromWorkerReqPb &req, RequestMetaFromWorkerRspPb &rsp) = 0;

    /**
     * @brief Notify the worker to change the primary copy.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status ChangePrimaryCopy(ChangePrimaryCopyReqPb &req, ChangePrimaryCopyRspPb &rsp) = 0;

    /**
     * @brief Forward nested object reference increase calls to multiple masters based on objectKeys
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status NotifyMasterIncNestedRefs(NotifyMasterIncNestedReqPb &req, NotifyMasterIncNestedResPb &rsp) = 0;

    /**
     * @brief Forward nested object reference decrease calls to multiple masters based on objectKeys
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status NotifyMasterDecNestedRefs(NotifyMasterDecNestedReqPb &req, NotifyMasterDecNestedResPb &rsp) = 0;

    /**
     * @brief A factory method to instantiate the correct derived version of the api. Remote workers will use an
     * rpc-based api, whereas local workers can be optimized for in-process pointer based api.
     * @param[in] hostPort The host port of the target worker
     * @param[in] localHostPort The local master host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] service The local pointer to the master worker OC service implementation. If null, the created api
     * must default to the RPC-based version.
     * @return A base class pointer to the correct derived type of api.
     */
    static std::shared_ptr<MasterWorkerOCApi> CreateMasterWorkerOCApi(
        const HostPort &hostPort, const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager,
        object_cache::MasterWorkerOCServiceImpl *service = nullptr);

protected:
    /**
     * @brief Constructor for MasterWorkerOCApi class. Protected constructor enforces class instantiation through the
     * factory method CreateMasterWorkerOCApi.
     * @param[in] localHostPort The local master service host port
     * @param[in] akSkManager Used to do AK/SK authenticate
     */
    explicit MasterWorkerOCApi(const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager);

    HostPort masterHostPort_;  // The HostPort of the master node.
    std::shared_ptr<AkSkManager> akSkManager_;
};

/**
 * @brief MasterRemoteWorkerApi is the derived remote version of the api for sending and receiving worker OC requests
 * where the worker is on a different host. This class will use an RPC mechanism for communication to the remote
 * location.
 * Callers will access this class naturally through base class polymorphism.
 * See the parent interface for function argument documentation.
 */
class MasterRemoteWorkerOCApi : public MasterWorkerOCApi {
public:
    /**
     * @brief Constructor for MasterRemoteWorkerOCApi class, the remote version of the api.
     * @param[in] hostPort The address of the target worker.
     * @param[in] localHostPort The local master host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    explicit MasterRemoteWorkerOCApi(const HostPort &workerHostPort, const HostPort &localHostPort,
                                     std::shared_ptr<AkSkManager> akSkManager);
    ~MasterRemoteWorkerOCApi() override = default;
    Status Init() override;
    Status PublishMeta(PublishMetaReqPb &req, PublishMetaRspPb &resp) override;
    Status ClearData(ClearDataReqPb &req, ClearDataRspPb &rsp) override;
    Status UpdateNotification(UpdateObjectReqPb &req, UpdateObjectRspPb &rsp) override;
    Status DeleteNotification(std::unique_ptr<DeleteObjectReqPb> req, DeleteObjectRspPb &rsp) override;
    Status DeletePersistenceObject(std::unique_ptr<DeletePersistenceObjectReqPb> req,
                                   DeletePersistenceObjectRspPb &rsp) override;
    Status DeleteNotificationSend(std::unique_ptr<DeleteObjectReqPb> req, int64_t &tag) override;
    Status DeleteNotificationReceive(int64_t tag, DeleteObjectRspPb &rsp) override;
    Status QueryGlobalRefNumOnWorker(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspPb &rsp) override;
    Status PushMetaToWorker(PushMetaToWorkerReqPb &req, PushMetaToWorkerRspPb &rsp) override;
    Status RequestMetaFromWorker(RequestMetaFromWorkerReqPb &req, RequestMetaFromWorkerRspPb &rsp) override;
    Status ChangePrimaryCopy(ChangePrimaryCopyReqPb &req, ChangePrimaryCopyRspPb &rsp) override;
    Status NotifyMasterIncNestedRefs(NotifyMasterIncNestedReqPb &req, NotifyMasterIncNestedResPb &rsp) override;
    Status NotifyMasterDecNestedRefs(NotifyMasterDecNestedReqPb &req, NotifyMasterDecNestedResPb &rsp) override;

private:
    HostPort workerHostPort_;                                            // The HostPort of the worker node.
    std::shared_ptr<MasterWorkerOCService_Stub> rpcSession_{ nullptr };  // Session to the worker rpc service.
};
}  // namespace master
}  // namespace datasystem

#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_WORKER_OC_API_H
