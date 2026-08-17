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

#ifndef DATASYSTEM_COMMON_COORDINATOR_EVENT_NOTIFY_EXECUTOR_H
#define DATASYSTEM_COMMON_COORDINATOR_EVENT_NOTIFY_EXECUTOR_H

#include <bthread/bthread.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {
class EventNotifyExecutor {
public:
    using Worker = std::function<void(size_t)>;

    EventNotifyExecutor() = default;
    ~EventNotifyExecutor();

    EventNotifyExecutor(const EventNotifyExecutor &) = delete;
    EventNotifyExecutor &operator=(const EventNotifyExecutor &) = delete;

    Status Start(size_t workerCount, bthread_tag_t tag, Worker worker);
    void Stop();

private:
    struct WorkerContext {
        EventNotifyExecutor *executor;
        size_t workerIndex;
    };

    static void *Run(void *arg);

    Worker worker_;
    std::vector<bthread_t> workers_;
    std::vector<std::unique_ptr<WorkerContext>> workerContexts_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_COORDINATOR_EVENT_NOTIFY_EXECUTOR_H
