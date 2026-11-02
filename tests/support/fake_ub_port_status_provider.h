/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

#ifndef DATASYSTEM_TESTS_FAKE_UB_PORT_STATUS_PROVIDER_H
#define DATASYSTEM_TESTS_FAKE_UB_PORT_STATUS_PROVIDER_H

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <utility>
#include <vector>

#include "datasystem/common/object_cache/ub_port_health.h"

namespace datasystem::test {
class FakeUbPortStatusProvider : public IUbPortStatusProvider {
public:
    explicit FakeUbPortStatusProvider(std::vector<UbPortStatus> ports = {}) : ports_(std::move(ports))
    {
    }

    void SetResult(Status status, std::vector<UbPortStatus> ports)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = std::move(status);
        ports_ = std::move(ports);
    }

    void Pause()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        paused_ = true;
    }

    void Resume()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            paused_ = false;
        }
        cv_.notify_all();
    }

    bool WaitForCalls(size_t expected, std::chrono::milliseconds timeout = std::chrono::seconds(2))
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this, expected] { return calls_ >= expected; });
    }

    size_t Calls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

    Status QueryPortStatus(std::vector<UbPortStatus> &portStatus) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        ++calls_;
        cv_.notify_all();
        const auto pauseTimeout = UB_PORT_HEALTH_SYNC_QUERY_WAIT_TIMEOUT + std::chrono::seconds(2);
        if (!cv_.wait_for(lock, pauseTimeout, [this] { return !paused_; })) {
            return Status(K_INTERRUPTED, "Timed out waiting to resume fake UB port query");
        }
        if (status_.IsError()) {
            return status_;
        }
        portStatus = ports_;
        return Status::OK();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    Status status_;
    std::vector<UbPortStatus> ports_;
    size_t calls_ = 0;
    bool paused_ = false;
};
}  // namespace datasystem::test
#endif
