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
 * Description: RPC watch dispatcher implementation for coordinator server.
 */
#ifndef DATASYSTEM_COORDINATOR_WATCH_DISPATCHER_IMPL_H
#define DATASYSTEM_COORDINATOR_WATCH_DISPATCHER_IMPL_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/coordinator/watch_dispatcher.h"

namespace datasystem {
namespace coordinator {
class WatchDispatcherImpl : public WatchDispatcher {
public:
    explicit WatchDispatcherImpl(WatchRegistry *watchRegistry) : WatchDispatcher(watchRegistry)
    {
    }
    ~WatchDispatcherImpl() override = default;

protected:
    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_WATCH_DISPATCHER_IMPL_H
