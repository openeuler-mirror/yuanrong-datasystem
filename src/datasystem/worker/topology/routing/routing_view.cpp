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
#include "datasystem/worker/topology/routing/routing_view.h"

#include <mutex>
#include <utility>

namespace datasystem {
namespace topology {

Status RoutingView::GetSnapshot(std::shared_ptr<const RoutingSnapshot> &snapshot) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    snapshot = snapshot_;
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_NOT_READY, "Routing snapshot is not published.");
    return Status::OK();
}

void RoutingView::Publish(std::shared_ptr<const RoutingSnapshot> snapshot)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    snapshot_ = std::move(snapshot);
}

}  // namespace topology
}  // namespace datasystem
