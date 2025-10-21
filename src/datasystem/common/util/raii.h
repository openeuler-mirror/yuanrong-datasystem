/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Raii utils.
 */
#ifndef DATASYSTEM_COMMON_UTIL_RAII_H
#define DATASYSTEM_COMMON_UTIL_RAII_H

#include <functional>

namespace datasystem {
class Raii {
public:
    explicit Raii(std::function<void(void)> function) : function_(std::move(function))
    {
    }
    ~Raii()
    {
        function_();
    }

private:
    std::function<void(void)> function_;
};

class RaiiPlus {
public:
    RaiiPlus()
    {
    }

    explicit RaiiPlus(std::function<void(void)> function)
    {
        functions_.emplace_back(std::move(function));
    }

    ~RaiiPlus()
    {
        for (const auto &func : functions_) {
            func();
        }
    }

    void AddTask(std::function<void(void)> &&task)
    {
        functions_.emplace_back(std::move(task));
    }

    void ClearAllTask()
    {
        functions_.clear();
    }

private:
    std::vector<std::function<void(void)>> functions_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_RAII_H
