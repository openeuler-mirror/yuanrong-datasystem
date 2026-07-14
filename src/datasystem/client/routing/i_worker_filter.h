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
 * Description: IWorkerFilter - self-contained filter interface for worker routing.
 * Each filter owns its own state and rules. WorkerRouter holds only generic hash ring data.
 */
#ifndef DATASYSTEM_CLIENT_ROUTING_I_WORKER_FILTER_H
#define DATASYSTEM_CLIENT_ROUTING_I_WORKER_FILTER_H

#include <string>

#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

class IWorkerFilter {
public:
    virtual ~IWorkerFilter() = default;

    /**
     * @brief Called by WorkerRouter when traversing the filter chain.
     * @return false to skip this worker (not available for routing).
     */
    virtual bool IsAvailable(const HostPort &addr) const = 0;

    /**
     * @brief Called when WorkerRouter receives UpdateState.
     * Each filter decides whether to handle this status code.
     */
    virtual void OnWorkerStateChange(const HostPort & /* addr */, StatusCode /* status */) {}

    /**
     * @brief Called when hash ring is updated (optional, default no-op).
     */
    virtual void OnHashRingUpdated(const ::datasystem::ClusterTopologyPb & /* ring */) {}
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_I_WORKER_FILTER_H
