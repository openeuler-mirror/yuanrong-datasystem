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
 * Description: Simple event semaphore to synchronize threads.
 */

#include "datasystem/common/util/wait_post.h"

#include <algorithm>
#include <chrono>
#include <cstdint>

namespace datasystem {
WaitPost::WaitPost() : val_(0)
{
}

void WaitPost::Wait()
{
    std::unique_lock<std::mutex> lock(mux_);
    cv_.wait(lock, [this]() { return val_ != 0; });
}

bool WaitPost::WaitFor(uint64_t timeoutMs)
{
    std::unique_lock<std::mutex> lock(mux_);
    timeoutMs = std::min<uint64_t>(timeoutMs, INT64_MAX);
    return cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this]() { return val_ != 0; });
}

bool WaitPost::WaitForNext(uint64_t timeoutMs)
{
    std::unique_lock<std::mutex> lock(mux_);
    if (val_ != 0) {
        val_ = 0;
    }
    timeoutMs = std::min<uint64_t>(timeoutMs, INT64_MAX);
    return cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this]() { return val_ != 0; });
}

void WaitPost::Set()
{
    std::unique_lock<std::mutex> lock(mux_);
    val_ = 1;
    cv_.notify_all();
}

void WaitPost::Clear()
{
    std::unique_lock<std::mutex> lock(mux_);
    val_ = 0;
}

void Barrier::Wait()
{
    std::unique_lock<std::mutex> lock(mux_);
    auto gen = generation_;
    ++currCount_;
    if (currCount_ == expectCount_) {
        ++generation_;
        currCount_ = 0;
        cv_.notify_all();
        return;
    }

    cv_.wait(lock, [this, gen] { return gen != generation_; });
}

void WaitPost::SetWithStatus(const Status &status)
{
    std::unique_lock<std::mutex> lock(mux_);
    val_ = 1;
    status_ = status;
    cv_.notify_all();
}

Status WaitPost::WaitAndGetStatus()
{
    std::unique_lock<std::mutex> lock(mux_);
    cv_.wait(lock, [this]() { return val_ != 0; });
    return status_;
}
}  // namespace datasystem
