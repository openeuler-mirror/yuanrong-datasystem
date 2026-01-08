/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#ifndef DATASYSTEM_MIGRATE_DATA_SCALE_DOWN_NODE_SELECTOR_H
#define DATASYSTEM_MIGRATE_DATA_SCALE_DOWN_NODE_SELECTOR_H

#include "datasystem/object/object_enum.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/selection_strategy.h"

namespace datasystem {
namespace object_cache {

class ScaleDownNodeSelector : public SelectionStrategy {
public:
    enum class Stage {
        /**
         * @brief The first stage of migration, where the migration starts and initial checks are performed.
         * @details In this stage, the migration is allowed only if the available space ratio is above a high threshold
         *          (e.g., 50%) and the node is not leaving.
         */
        FIRST,
        /**
         * @brief The second stage of migration, where the migration continues with a lower available space threshold.
         * @details In this stage, the migration is allowed if the available space ratio is above a lower threshold
         *          (e.g., 20%) and the node is not leaving.
         */
        SECOND,
        /**
         * @brief The third stage of migration, where the migration is allowed regardless of the available space ratio.
         * @details In this stage, the migration is allowed as long as the node is not leaving.
         */
        THIRD,
        /**
         * @brief The final stage of migration, where the migration is always allowed.
         * @details In this stage, the migration is unconditionally allowed to ensure all remaining data is migrated.
         */
        FINAL
    };

    ScaleDownNodeSelector(EtcdClusterManager *etcdCM, HostPort &localAddress)
        : etcdCM_(etcdCM), localAddress_(localAddress), currentStage_(Stage::FIRST), currentDiskStage_(Stage::FIRST)
    {
    }

    ~ScaleDownNodeSelector() = default;

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
    bool CheckCondition(const MigrateDataRspPb &rsp, const CacheType &type) override;

    /**
     * @brief Checks if the current worker has been visited and upgrades the migration stage if a full cycle is
     * detected.
     * @param[in] currentWorker The address of the current worker being checked.
     */
    void UpdateForRedirect(const std::string &currentWorker) override;

private:
    /**
     * @brief Evaluates whether the migration condition for a specific stage is met.
     * @param[in] stage The migration stage to evaluate.
     * @param[in] needScaleDown Indicates whether the node is leaving the cluster.
     * @param[in] isDataMigrationStarted Indicates whether data migration has started.
     * @param[in] availableSpaceRatio The ratio of available space on the target node.
     * @return True if the migration condition for the specified stage is satisfied, false otherwise.
     */
    bool EvaluateStageCondition(Stage stage, bool needScaleDown, bool isDataMigrationStarted,
                                double availableSpaceRatio);

    /**
     * @brief Increments the migration stage if it is not already in the FINAL stage.
     * @param[in,out] stage The current migration stage to be incremented.
     */
    void IncrementStage(Stage &stage);

    std::unordered_set<std::string> visitedAddresses_;
    std::unordered_set<std::string> visitedAddressesForDisk_;

    EtcdClusterManager *etcdCM_{ nullptr };
    HostPort &localAddress_;

    Stage currentStage_;
    Stage currentDiskStage_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif