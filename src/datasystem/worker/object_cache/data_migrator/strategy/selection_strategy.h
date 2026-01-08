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
#ifndef DATASYSTEM_MIGRATE_DATA_SELECTION_STRATEGY_H
#define DATASYSTEM_MIGRATE_DATA_SELECTION_STRATEGY_H

#include <string>

#include "datasystem/object/object_enum.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class SelectionStrategy {
public:
    SelectionStrategy()
    {
    }

    virtual ~SelectionStrategy() = default;

    /**
     * @brief Select target node for migration.
     * @param[in] originAddr Origin node address.
     * @param[in] preferNode Preferred target node.
     * @param[in] needSize Required storage size.
     * @param[out] outNode Selected target node.
     * @return Status of the call.
     */
    virtual Status SelectNode(const std::string &originAddr, const std::string &preferNode, size_t needSize,
                              std::string &outNode) = 0;

    /**
     * @brief Checks whether the migration condition is met based on the response and cache type.
     * @param[in] rsp The migration response containing information about the migration status.
     * @param[in] type The cache type (e.g., memory or disk).
     * @return True if the migration condition is satisfied, false otherwise.
     */
    virtual bool CheckCondition(const MigrateDataRspPb &rsp, const CacheType &type) = 0;

    /**
     * @brief Update strategy when migrate failed.
     * @param[in] node The failed node of migration.
     */
    virtual void UpdateForRedirect(const std::string &node) = 0;
};

}  // namespace object_cache
}  // namespace datasystem

#endif
