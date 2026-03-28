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
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_WORKER_OC_LOCAL_API_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_WORKER_OC_LOCAL_API_H

#include "datasystem/master/object_cache/master_worker_oc_api.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"

namespace datasystem {
namespace object_cache {
class MasterWorkerOCServiceImpl;
};

namespace master {
/**
 * @brief MasterLocalWorkerOCApi is the derived local version of the api for sending and receiving worker OC requests
 * where the worker exists in the same process as the service. This class will directly reference the service through a
 * pointer and does not use any RPC mechanism for communication.
 * Callers will access this class naturally through base class polymorphism.
 * See the parent interface for function argument documentation.
 */
class MasterLocalWorkerOCApi : public MasterWorkerOCApi {
public:
    /**
     * @brief Constructor for MasterLocalWorkerOCApi class, the remote version of the api.
     * @param[in] hostPort The address of the target worker.
     * @param[in] localHostPort The local master host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    explicit MasterLocalWorkerOCApi(object_cache::MasterWorkerOCServiceImpl *service, const HostPort &localHostPort,
                                    std::shared_ptr<AkSkManager> akSkManager);
    ~MasterLocalWorkerOCApi() override = default;
    Status Init() override;
    Status PublishMeta(PublishMetaReqPb &req, PublishMetaRspPb &resp) override;
    Status UpdateNotification(UpdateObjectReqPb &req, UpdateObjectRspPb &rsp) override;
    Status ClearData(ClearDataReqPb &req, ClearDataRspPb &rsp) override;
    Status DeleteNotification(std::unique_ptr<DeleteObjectReqPb> req, DeleteObjectRspPb &rsp) override;
    Status DeleteNotificationSend(std::unique_ptr<DeleteObjectReqPb> req, int64_t &tag) override;
    Status DeleteNotificationReceive(int64_t tag, DeleteObjectRspPb &rsp) override;
    Status QueryGlobalRefNumOnWorker(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspPb &rsp) override;
    Status PushMetaToWorker(PushMetaToWorkerReqPb &req, PushMetaToWorkerRspPb &rsp) override;
    Status RequestMetaFromWorker(RequestMetaFromWorkerReqPb &req, RequestMetaFromWorkerRspPb &rsp) override;
    Status ChangePrimaryCopy(ChangePrimaryCopyReqPb &req, ChangePrimaryCopyRspPb &rsp) override;
    Status NotifyMasterIncNestedRefs(NotifyMasterIncNestedReqPb &req, NotifyMasterIncNestedResPb &rsp) override;
    Status NotifyMasterDecNestedRefs(NotifyMasterDecNestedReqPb &req, NotifyMasterDecNestedResPb &rsp) override;

private:
    object_cache::MasterWorkerOCServiceImpl *workerOC_{ nullptr };
    mutable std::shared_mutex localReqMutex_; // protects localReqMap_
    // Map from local tag to pending DeleteObject request.
    std::unordered_map<int64_t, std::unique_ptr<DeleteObjectReqPb>> localReqMap_;
    // Atomic tag generator for unique local DeleteObject request identification.
    static std::atomic<int64_t> g_localTagGen_;
};

}  // namespace master
}  // namespace datasystem

#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_MASTER_WORKER_OC_API_H
