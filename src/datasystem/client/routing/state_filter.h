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
 * Description: StateFilter - stateless filter that reads WorkerRouter's hash ring.
 * Returns IsAvailable=true only when the worker's ring state is ACTIVE.
 */
#ifndef DATASYSTEM_CLIENT_ROUTING_STATE_FILTER_H
#define DATASYSTEM_CLIENT_ROUTING_STATE_FILTER_H

#include <memory>

#include "datasystem/client/routing/i_worker_filter.h"
#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

class StateFilter : public IWorkerFilter {
public:
    explicit StateFilter(WorkerRouter *router) : router_(router) {}
    ~StateFilter() override = default;

    bool IsAvailable(const HostPort &addr) const override;

private:
    WorkerRouter *router_;  // Non-owning: WorkerRouter owns the filter, so no cycle
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_STATE_FILTER_H
