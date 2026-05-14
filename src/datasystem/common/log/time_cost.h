/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Trace class, which stores, obtains, and clears trace ID.TraceGuard class is used as the return value of
 * the SetTraceUUID method of the Trace class, which is responsible for clearing TraceID during destructor.
 */
#ifndef DATASYSTEM_COMMON_LOG_TINE_COST_H
#define DATASYSTEM_COMMON_LOG_TINE_COST_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"

namespace datasystem {
class TimeCost {
public:
    std::string GetInfo() const
    {
        if (durationList_.empty()) {
            return "";
        }
        std::string out = "exceed 3ms: {";
        for (auto iter : durationList_) {
            out += FormatString("%s: %zu ms; ", iter.first, iter.second);
        }
        out += "}";
        return out;
    }

    void Append(const char *operation, uint64_t duration)
    {
        if (duration < timeThreshold_) {
            return;
        }
        if (durationList_.size() < maxCount_) {
            durationList_.emplace_back(std::make_pair(operation, duration));
            return;
        }
        const int warningLogDuration = 60;
        LOG_EVERY_T(WARNING, warningLogDuration)
            << "The record count " << durationList_.size() << " exceed the max limit " << maxCount_
            << ", ignore the operation " << operation;
    }

    void Clear()
    {
        durationList_.clear();
    }

private:
    std::vector<std::pair<std::string, uint64_t>> durationList_;

    static const uint64_t timeThreshold_ = 3;
    static const size_t maxCount_ = 32;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_LOG_TINE_COST_H
