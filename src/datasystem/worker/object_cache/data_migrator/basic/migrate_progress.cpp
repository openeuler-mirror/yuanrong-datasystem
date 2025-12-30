/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Migrate data process implementation.
 */
#include "datasystem/worker/object_cache/data_migrator/basic/migrate_progress.h"

#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace object_cache {
MigrateProgress::MigrateProgress(uint64_t count, uint64_t intervalSeconds,
                                 std::function<void(double, uint64_t, uint64_t)> callback)
    : count_(count), intervalSeconds_(intervalSeconds), callback_(callback)
{
    auto traceID = Trace::Instance().GetTraceID();
    thread_ = Thread([this, traceID]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        Process();
    });
}

MigrateProgress::~MigrateProgress()
{
    stopFlag_ = true;
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void MigrateProgress::Deal(uint64_t count)
{
    constexpr uint64_t div = 100;
    uint64_t originCount = processedCount_.fetch_add(count);
    if ((originCount + count) / div > originCount / div) {
        cv_.notify_all();
    }
}

void MigrateProgress::Process()
{
    while (!stopFlag_) {
        std::unique_lock<std::mutex> l(mutex_);
        (void)cv_.wait_for(l, std::chrono::seconds(intervalSeconds_));
        if (callback_ != nullptr) {
            callback_(timer_.ElapsedSecond(), processedCount_, count_);
        }
    }
}

}  // namespace object_cache
}  // namespace datasystem