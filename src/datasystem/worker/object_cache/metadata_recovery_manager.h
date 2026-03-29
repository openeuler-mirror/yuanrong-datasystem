/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Manager for recovering object metadata from worker to master.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_MANAGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_MANAGER_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_master_api_manager_base.h"

namespace datasystem {
namespace object_cache {
class MetaDataRecoveryManager {
public:
    MetaDataRecoveryManager(
        const HostPort &localAddress, const std::shared_ptr<ObjectTable> &objectTable, EtcdClusterManager *etcdCM,
        const std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> &workerMasterApiManager);

    ~MetaDataRecoveryManager() = default;
    /**
     * @brief Recover metadata for object keys. Requests to different masters are dispatched concurrently.
     * @param[in] objectKeys Object keys to recover.
     * @param[out] failedIds Object keys failed because master is unreachable.
     * @return Status of the call.
     */
    Status RecoverMetadata(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedIds,
                           std::string stanbyMasterAddr = "");

private:
    struct DispatchResult {
        Status status = Status::OK();
        std::vector<std::string> failedIds;
    };

    bool FillRecoveredMeta(const std::string &objectKey, ObjectMetaPb &metadata) const;
    bool InitRecoverApi(const MetaAddrInfo &metaAddrInfo, const std::vector<std::string> &objectKeys, HostPort &addr,
                        std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi, DispatchResult &result) const;
    void HandleFillMetaFailure(const std::string &objectKey, DispatchResult &result) const;
    void SendRecoverBatch(const MetaAddrInfo &metaAddrInfo, const HostPort &addr,
                          const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                          master::PushMetaToMasterReqPb &req, std::vector<std::string> &batchObjectKeys,
                          DispatchResult &result) const;
    DispatchResult SendRecoverRequest(const MetaAddrInfo &metaAddrInfo,
                                      const std::vector<std::string> &objectKeys) const;

    HostPort localAddress_;
    std::shared_ptr<ObjectTable> objectTable_;
    EtcdClusterManager *etcdCM_{ nullptr };
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager_{ nullptr };
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_MANAGER_H
