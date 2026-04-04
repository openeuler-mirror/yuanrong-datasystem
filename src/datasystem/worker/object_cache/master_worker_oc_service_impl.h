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
 * Description: Declare the worker to master service processing main class implementation.
 */
#ifndef DATASYSTEM_WORKER_OC_MASTER_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_OC_MASTER_SERVICE_IMPL_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/object_kv.h"

#include "datasystem/protos/worker_object.service.rpc.pb.h"

namespace datasystem {
namespace object_cache {
class MasterWorkerOCServiceImpl : public MasterWorkerOCService {
public:
    /**
     * @brief MasterWorkerOCServiceImpl constructor.
     * @param[in] clientSvc Pointer to worker service.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    MasterWorkerOCServiceImpl(std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc,
                              std::shared_ptr<AkSkManager> akSkManager);

    ~MasterWorkerOCServiceImpl() override;

    /**
     * @brief Initialize the WorkerOCMasterServiceApi Object including rpc channel.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Waiting for WorkerOCServiceImpl to be initialized
     * @return Status of the call.
     */
    Status WaitWorkerOCServiceImplInit();

    /**
     * @brief Process update notification from master when object data changed.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status UpdateNotification(const UpdateObjectReqPb &req, UpdateObjectRspPb &rsp) override;

    /**
     * @brief Process delete notification from master.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status DeleteNotification(const DeleteObjectReqPb &req, DeleteObjectRspPb &rsp) override;

    /**
     * @brief Process L2-persistence-only delete request from master.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status DeletePersistenceObject(const DeletePersistenceObjectReqPb &req, DeletePersistenceObjectRspPb &rsp) override;

    /**
     * @brief Publish already sealed object meta from master to worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return Status of the call.
     */
    Status PublishMeta(const PublishMetaReqPb &req, PublishMetaRspPb &resp) override;

    // Deprecated
    Status MetaChange(const MetaChangeReqPb &req, MetaChangeRspPb &rsp) override;

    /**
     * @brief Query objects' global references on worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status QueryGlobalRefNumOnWorker(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspPb &rsp) override;

    /**
     * @brief The master pushes metadata to the worker for reconciliation.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status PushMetaToWorker(const PushMetaToWorkerReqPb &req, PushMetaToWorkerRspPb &rsp) override;

    /**
     * @brief Clear data without meta.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc request protobuf.
     * @return Status of the call.
     */
    Status ClearData(const ClearDataReqPb &req, ClearDataRspPb &rsp) override;

    /**
     * @brief Retry to get object from remote.
     * @param[in] readKey read key info, contain offset, size, objKey.
     * @param[in] queryMeta QueryMeta info to get.
     * @param[out] payloads QueryMeta result may contains object data, and it would be saved here.
     */
    void RetryGetObjectFromRemote(const ReadKey &readKey, const master::QueryMetaInfoPb &queryMeta,
                                  std::vector<RpcMessage> &payloads);

    /**
     * @brief Handles the request for metadata from the worker
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status RequestMetaFromWorker(const RequestMetaFromWorkerReqPb &req, RequestMetaFromWorkerRspPb &rsp) override;

    /**
     * @brief Process change primary copy message.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status ChangePrimaryCopy(const ChangePrimaryCopyReqPb &req, ChangePrimaryCopyRspPb &rsp) override;

    /**
     * @brief Forward nested object reference increase calls to multiple masters based on objectKeys
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status NotifyMasterIncNestedRefs(const NotifyMasterIncNestedReqPb &req, NotifyMasterIncNestedResPb &rsp) override;

    /**
     * @brief Forward nested object reference decrease calls to multiple masters based on objectKeys
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status NotifyMasterDecNestedRefs(const NotifyMasterDecNestedReqPb &req, NotifyMasterDecNestedResPb &rsp) override;

private:
    /**
     * @brief Cache invalidate an object.
     * @param[in] req The rpc request protobuf.
     * @param[in,out] objectKV The object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status BinaryObjectCacheInvalidation(const UpdateObjectInfoPb &req, ObjectKV &objectKV);

    /**
     * @brief Update object information.
     * @param[in] req Object info to update.
     * @param[in] sync Indicates whether update notification is asynchronously.
     * @return Status of the call.
     */
    Status UpdateSingleNotification(const UpdateObjectInfoPb &req, bool sync = false);

    /**
     * @brief Process change primary copy
     * @param[in] objectKey object key to be changed
     * @param[in] isPrimaryCopy Specifies whether to be the primary copy
     * @return Status
     */
    Status ProcessChangePrimaryCopy(const std::string &objectKey, bool isPrimaryCopy);

    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> ocClientWorkerSvc_;
    std::shared_ptr<AkSkManager> akSkManager_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OC_MASTER_SERVICE_IMPL_H
