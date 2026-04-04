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
 * Description: Manager for worker-side metadata recovery.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_MANAGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_MANAGER_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
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
    struct RecoverySummary {
        Status status = Status::OK();
        size_t requestedCount{ 0 };
        size_t groupedMasterCount{ 0 };
        size_t recoveredCount{ 0 };
        std::vector<std::string> failedIds;
    };

    MetaDataRecoveryManager(
        const HostPort &localAddress, const std::shared_ptr<ObjectTable> &objectTable, EtcdClusterManager *etcdCM,
        const std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> &workerMasterApiManager,
        uint64_t metadataSize = 0);

    ~MetaDataRecoveryManager() = default;

    /**
     * @brief Recover metadata for object keys and return aggregated summary for later restart-flow decisions.
     * @param[in] objectKeys Object keys to recover.
     * @return Summary of the recovery attempt.
     */
    RecoverySummary RecoverMetadataWithSummary(const std::vector<std::string> &objectKeys, std::string stanbyAddr);

    /**
     * @brief Recover metadata for explicit object metas and return aggregated summary.
     * @param[in] metas Object metas to recover.
     * @return Summary of the recovery attempt.
     */
    RecoverySummary RecoverMetadataWithSummary(const std::vector<ObjectMetaPb> &metas);

    /**
     * @brief Rebuild local object-table entries from restart/preload metadata.
     * @param[in] recoverMetas Recovery metadata produced by restart or preload flow.
     * @param[out] recoveredObjectKeys Object keys whose local entries were rebuilt.
     * @return Status of the call.
     */
    Status RecoverLocalEntries(const std::vector<ObjectMetaPb> &recoverMetas,
                               std::vector<std::string> &recoveredObjectKeys) const;

private:
    struct DispatchResult {
        Status status = Status::OK();
        size_t requestedCount{ 0 };
        size_t recoveredCount{ 0 };
        std::vector<std::string> failedIds;
    };

    using GroupedByMaster = std::unordered_map<MetaAddrInfo, std::vector<std::string>>;
    using GroupItem = GroupedByMaster::value_type;

    GroupedByMaster BuildGroupedByMaster(const std::vector<std::string> &objectKeys,
                                         const std::string &stanbyAddr) const;
    void MergeDispatchResults(const std::vector<DispatchResult> &results, RecoverySummary &summary) const;
    void BuildMetaIndex(const std::vector<ObjectMetaPb> &metas, std::vector<std::string> &objectKeys,
                        std::unordered_map<std::string, std::vector<const ObjectMetaPb *>> &metasByObjectKey,
                        RecoverySummary &summary) const;
    std::vector<ObjectMetaPb> BuildGroupedMetas(
        const std::vector<std::string> &objectKeys,
        const std::unordered_map<std::string, std::vector<const ObjectMetaPb *>> &metasByObjectKey) const;
    void LogRecoverySummary(const RecoverySummary &summary, const std::string &prefix) const;

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
    DispatchResult SendRecoverRequest(const MetaAddrInfo &metaAddrInfo, const std::vector<ObjectMetaPb> &metas) const;

    HostPort localAddress_;
    std::shared_ptr<ObjectTable> objectTable_;
    EtcdClusterManager *etcdCM_{ nullptr };
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager_{ nullptr };
    uint64_t metadataSize_{ 0 };
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_MANAGER_H
