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
 * Description: Selector for metadata recovery object keys.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_SELECTOR_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_SELECTOR_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {
namespace object_cache {
class MetadataRecoverySelector {
public:
    using MatchFunc = std::function<bool(const std::string &)>;

    struct SelectionRequest {
        worker::HashRange ranges;
        std::vector<std::string> workerUuids;
        bool includeL2CacheIds{ false };

        bool Empty() const;
        std::string ToString() const;
    };

    ~MetadataRecoverySelector() = default;

    MetadataRecoverySelector(const std::shared_ptr<ObjectTable> &objectTable, EtcdClusterManager *etcdCM);

    static SelectionRequest BuildSelectionRequest(const ClearDataReqPb &req, bool includeL2CacheIds);

    Status BuildMatchFunc(const SelectionRequest &request, MatchFunc &matchFunc) const;

    void Select(const MatchFunc &matchFunc, bool includeL2CacheIds, std::vector<std::string> &objectKeys) const;

    Status Select(const SelectionRequest &request, std::vector<std::string> &objectKeys) const;

private:
    std::shared_ptr<ObjectTable> objectTable_{ nullptr };
    EtcdClusterManager *etcdCM_{ nullptr };
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_METADATA_RECOVERY_SELECTOR_H
