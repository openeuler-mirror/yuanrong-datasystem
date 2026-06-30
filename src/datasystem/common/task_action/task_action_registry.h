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
 * Description: Task action registry implementation.
 */
#ifndef DATASYSTEM_COMMON_TASK_ACTION_TASK_ACTION_REGISTRY_H
#define DATASYSTEM_COMMON_TASK_ACTION_TASK_ACTION_REGISTRY_H

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/task_action/task_action.h"
#include "datasystem/common/util/no_destructor.h"

namespace datasystem {

class TaskActionRegistry {
public:
    static TaskActionRegistry &GetInstance();

    ~TaskActionRegistry() = default;

    void AddSubscriber(TransferTaskType type, const std::string &subscriberName, TaskActionFn fn);

    void RemoveSubscriber(TransferTaskType type, const std::string &subscriberName);

    Status Dispatch(const TransferTask &task) const;

    std::string GetSubscribers(TransferTaskType type) const;

private:
    friend class NoDestructor<TaskActionRegistry>;

    TaskActionRegistry() = default;

    struct TransferTaskTypeHash {
        size_t operator()(TransferTaskType type) const
        {
            return static_cast<size_t>(type);
        }
    };

    class SubscriberSet;

    mutable std::shared_timed_mutex mutex_;
    std::unordered_map<TransferTaskType, std::shared_ptr<SubscriberSet>, TransferTaskTypeHash> subscribers_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_TASK_ACTION_TASK_ACTION_REGISTRY_H
