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

#include "datasystem/common/task_action/task_action_registry.h"

#include <algorithm>
#include <list>
#include <sstream>
#include <utility>
#include <vector>

#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

class TaskActionRegistry::SubscriberSet {
public:
    ~SubscriberSet() = default;

    struct Subscriber {
        std::string name;
        TaskActionFn fn;
    };

    void AddSubscriber(const std::string &subscriberName, TaskActionFn fn)
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        RemoveSubscriberWithoutLock(subscriberName);
        subscribers_.emplace_back(Subscriber{ subscriberName, std::move(fn) });
    }

    void RemoveSubscriber(const std::string &name)
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        RemoveSubscriberWithoutLock(name);
    }

    std::vector<Subscriber> Snapshot() const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        return std::vector<Subscriber>(subscribers_.begin(), subscribers_.end());
    }

    std::string GetSubscribers() const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        std::stringstream log;
        for (auto &i : subscribers_) {
            log << i.name << ",";
        }
        return log.str();
    }

private:
    void RemoveSubscriberWithoutLock(const std::string &name)
    {
        auto iter = std::find_if(subscribers_.begin(), subscribers_.end(),
                                 [&name](const Subscriber &subscriber) { return subscriber.name == name; });
        if (iter != subscribers_.end()) {
            subscribers_.erase(iter);
        }
    }

    mutable std::shared_timed_mutex mutex_;
    std::list<Subscriber> subscribers_;
};

TaskActionRegistry &TaskActionRegistry::GetInstance()
{
    static NoDestructor<TaskActionRegistry> instance;
    return *instance;
}

void TaskActionRegistry::AddSubscriber(TransferTaskType type, const std::string &subscriberName, TaskActionFn fn)
{
    std::shared_ptr<SubscriberSet> subscribers;
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        auto &entry = subscribers_[type];
        if (entry == nullptr) {
            entry = std::make_shared<SubscriberSet>();
        }
        subscribers = entry;
    }
    subscribers->AddSubscriber(subscriberName, std::move(fn));
}

void TaskActionRegistry::RemoveSubscriber(TransferTaskType type, const std::string &subscriberName)
{
    std::shared_ptr<SubscriberSet> subscribers;
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto iter = subscribers_.find(type);
        if (iter == subscribers_.end()) {
            return;
        }
        subscribers = iter->second;
    }
    if (subscribers == nullptr) {
        return;
    }
    subscribers->RemoveSubscriber(subscriberName);
}

Status TaskActionRegistry::Dispatch(const TransferTask &task) const
{
    std::shared_ptr<SubscriberSet> subscriberSet;
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto iter = subscribers_.find(task.type);
        CHECK_FAIL_RETURN_STATUS(
            iter != subscribers_.end() && iter->second != nullptr, K_NOT_READY,
            FormatString("Task action subscriber is not registered, type=%s", TransferTaskTypeToString(task.type)));
        subscriberSet = iter->second;
    }
    auto subscribers = subscriberSet->Snapshot();
    CHECK_FAIL_RETURN_STATUS(
        !subscribers.empty(), K_NOT_READY,
        FormatString("Task action subscriber is not registered, type=%s", TransferTaskTypeToString(task.type)));
    for (const auto &subscriber : subscribers) {
        CHECK_FAIL_RETURN_STATUS(subscriber.fn != nullptr, K_INVALID,
                                 FormatString("Task action subscriber function is null, type=%s, subscriber=%s",
                                              TransferTaskTypeToString(task.type), subscriber.name));
        auto status = subscriber.fn(task);
        if (status.IsError()) {
            return Status(status.GetCode(),
                          FormatString("Task action subscriber %s failed: %s", subscriber.name, status.ToString()));
        }
    }
    return Status::OK();
}

std::string TaskActionRegistry::GetSubscribers(TransferTaskType type) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto iter = subscribers_.find(type);
    if (iter == subscribers_.end() || iter->second == nullptr) {
        return "";
    }
    return iter->second->GetSubscribers();
}

}  // namespace datasystem
