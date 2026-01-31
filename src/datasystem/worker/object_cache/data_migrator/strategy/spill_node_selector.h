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
 * Description: Selection strategy for migrate data.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_SPILL_NODE_SELECTOR_H
#define DATASYSTEM_MIGRATE_DATA_SPILL_NODE_SELECTOR_H

#include "datasystem/object/object_enum.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/selection_strategy.h"

namespace datasystem {
namespace object_cache {

class SpillNodeSelector : public SelectionStrategy {
public:
    SpillNodeSelector(EtcdClusterManager *etcdCM, HostPort &localAddress)
        : etcdCM_(etcdCM), localAddress_(localAddress)
    {
        excludedNodes_.insert(localAddress_.ToString());
    }

    ~SpillNodeSelector() = default;

    /**
     * @brief Select target node for migration.
     * @param[in] originAddr Origin node address.
     * @param[in] preferNode Preferred target node.
     * @param[in] needSize Required storage size.
     * @param[out] outNode Selected target node.
     * @return Status of the call.
     */
    Status SelectNode(const std::string &originAddr, const std::string &preferNode, size_t needSize,
                      std::string &outNode) override;

    /**
     * @brief Checks whether the migration condition is met based on the response and cache type.
     * @param[in] rsp The migration response containing information about the migration status.
     * @param[in] type The cache type (e.g., memory or disk).
     * @return True if the migration condition is satisfied, false otherwise.
     */
    bool CheckCondition(const MigrateDataRspPb &rsp, const CacheType &type) override
    {
        (void)rsp;
        (void)type;
        return true;
    }

    /**
     * @brief Update strategy when migrate failed.
     * @param[in] node The failed node of migration.
     */
    void UpdateForRedirect(const std::string &currentWorker) override
    {
        excludedNodes_.insert(currentWorker);
    }

private:
    EtcdClusterManager *etcdCM_{ nullptr };
    HostPort &localAddress_;
    std::unordered_set<std::string> excludedNodes_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif