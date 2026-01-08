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
 * Description: Migrate data process.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_MIGRATE_PROCESS_H
#define DATASYSTEM_MIGRATE_DATA_MIGRATE_PROCESS_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>

#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace object_cache {

class MigrateProgress {
public:
    MigrateProgress(uint64_t count, uint64_t intervalSeconds, std::function<void(double, uint64_t, uint64_t)> callback);

    ~MigrateProgress();

    /**
     * @brief Update processed count.
     * @param[in] count Incremental count processed.
     */
    void Deal(uint64_t count);

private:
    /**
     * @brief Process progress updates.
     */
    void Process();

    std::atomic<size_t> count_;
    std::atomic<size_t> processedCount_{ 0 };
    uint64_t intervalSeconds_;
    std::atomic<bool> stopFlag_{ false };
    Thread thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::function<void(double, uint64_t, uint64_t)> callback_;
    Timer timer_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif
