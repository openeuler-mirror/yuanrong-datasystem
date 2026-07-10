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

/** Description: Defines the worker snapshot used to reconcile transport connections. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_WORKER_SNAPSHOT_H
#define DATASYSTEM_CLIENT_TRANSPORT_WORKER_SNAPSHOT_H

#include <vector>

#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

struct WorkerSnapshot {
    bool Empty() const
    {
        return sameHostAddrs.empty() && otherAddrs.empty();
    }

    std::vector<HostPort> sameHostAddrs;
    std::vector<HostPort> otherAddrs;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_WORKER_SNAPSHOT_H
