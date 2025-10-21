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
 * Description: shared disk detecter.
 */

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "datasystem/common/util/thread.h"

namespace datasystem {
namespace memory {

class SharedDiskDetecter {
public:
    SharedDiskDetecter(const std::string &path, uint64_t intervalMs = 5'000);
    
    ~SharedDiskDetecter();

    /**
     * @brief Check disk is available.
     * @return True if disk is available.
     */
    bool IsAvailable();

private:
    /**
     * @brief Run detect.
     */
    void Run();

    /**
     * @brief Detect disk.
     */
    void Detect();

    std::string path_;

    uint64_t intervalMs_;

    std::atomic_bool available_;

    std::string errMsg_;

    std::unique_ptr<Thread> thread_;

    std::atomic_bool running_;

    std::condition_variable cond_;

    // Protect nothing, just for condtion wait and notify.
    std::mutex mtx_;
};

}  // namespace memory
}  // namespace datasystem