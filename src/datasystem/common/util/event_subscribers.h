/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Event subscription and synchronize notification, an implementation of dependency inversion.
 */
#ifndef DATASYSTEM_COMMON_UTIL_EVENT_SUBSCRIBERS_H
#define DATASYSTEM_COMMON_UTIL_EVENT_SUBSCRIBERS_H

#include <shared_mutex>
#include <string>
#include <list>
#include <algorithm>
#include <mutex>

#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
template <auto eventType, typename ProcessFn, typename EventType = std::decay_t<decltype(eventType)>>
class EventSubscribers {
    using UnderlyingType = typename std::underlying_type<EventType>::type;

public:
    static EventSubscribers &GetInstance()
    {
        static EventSubscribers ins;
        return ins;
    }

    virtual ~EventSubscribers()
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        if (!subscribers_.empty()) {
            LOG(WARNING) << FormatString("Not all subscribers of event %d are removed. Remain subscribers: %s",
                                         static_cast<UnderlyingType>(eventType), GetSubscribers());
        }
    }

    void AddSubscriber(const std::string &subscriberName, ProcessFn &&fn, int priority = 0)
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        InsertWithSort(Subscriber{ priority, subscriberName, std::move(fn) });
    }

    void RemoveSubscriber(const std::string &name)
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        auto it = std::find_if(subscribers_.begin(), subscribers_.end(),
                               [&name](const Subscriber &s) { return s.name == name; });
        if (it != subscribers_.end()) {
            subscribers_.erase(it);
        }
    }

    template <typename... Args>
    auto NotifyAll(Args &&...args) const
    {
        using RetType = typename std::result_of<ProcessFn(Args...)>::type;
        static_assert(std::is_same_v<RetType, Status> || std::is_same_v<RetType, void>);
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        for (auto i = subscribers_.begin(); i != subscribers_.end(); i++) {
            if (!(i->fn)) {
                LOG(ERROR) << FormatString("Cannot find the fn of Subscriber %s with EventType %s:%d.", i->name,
                                           typeid(EventType).name(), static_cast<UnderlyingType>(eventType));
                continue;
            }
            if constexpr (std::is_same_v<RetType, Status>) {
                auto status = (i->fn)(args...);
                LOG_IF(WARNING, status.IsError())
                    << FormatString("%s process eventType %s:%d failed. Remaining unprocessed subscribers num %u",
                                    i->name, typeid(EventType).name(), static_cast<UnderlyingType>(eventType),
                                    std::distance(std::next(i), subscribers_.end()));
                RETURN_IF_NOT_OK(status);
            } else {
                (i->fn)(args...);
            }
        }
        return RetType();
    }

    std::string GetSubscribers()
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        std::stringstream log;
        for (auto &i : subscribers_) {
            log << i.name << ",";
        }
        return log.str();
    }

private:
    EventSubscribers() = default;

    struct Subscriber {
        int priority;
        std::string name;
        ProcessFn fn;

        bool operator<(const Subscriber &s) const
        {
            return this->priority < s.priority;
        }
    };

    void InsertWithSort(Subscriber &&subscriber)
    {
        auto it = std::upper_bound(subscribers_.begin(), subscribers_.end(), subscriber);
        subscribers_.insert(it, std::move(subscriber));
    }

    mutable std::shared_timed_mutex mutex_;
    std::list<Subscriber> subscribers_;
};

// concept
class EventNotifier {};
}  // namespace datasystem
#endif
