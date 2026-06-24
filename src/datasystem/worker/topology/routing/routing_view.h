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
 * Description: R0 routing snapshot view.
 */
#ifndef DATASYSTEM_WORKER_TOPOLOGY_ROUTING_ROUTING_VIEW_H
#define DATASYSTEM_WORKER_TOPOLOGY_ROUTING_ROUTING_VIEW_H

#include <memory>
#include <shared_mutex>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/topology/routing/routing_snapshot.h"

namespace datasystem {
namespace topology {

class IRoutingView {
public:
    virtual ~IRoutingView() = default;

    /**
     * @brief Load the currently published immutable routing snapshot.
     * @param[out] snapshot Published snapshot.
     * @return K_OK if a snapshot is available, K_NOT_READY if no snapshot has been published.
     *
     * Request threads only load a local immutable snapshot. This method must not perform repository/backend IO,
     * CAS/List/Watch, task scan, migration, recovery, cleanup, or success-path logging.
     */
    virtual Status GetSnapshot(std::shared_ptr<const RoutingSnapshot> &snapshot) const = 0;
};

class RoutingView final : public IRoutingView {
public:
    RoutingView() = default;
    ~RoutingView() override = default;

    Status GetSnapshot(std::shared_ptr<const RoutingSnapshot> &snapshot) const override;

    /**
     * @brief Publish an immutable routing snapshot.
     * @param[in] snapshot Fully constructed snapshot.
     *
     * The caller must build the snapshot before calling Publish. Publish only swaps the shared pointer.
     */
    void Publish(std::shared_ptr<const RoutingSnapshot> snapshot);

private:
    mutable std::shared_timed_mutex mutex_;
    std::shared_ptr<const RoutingSnapshot> snapshot_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_TOPOLOGY_ROUTING_ROUTING_VIEW_H
