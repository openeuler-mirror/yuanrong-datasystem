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
 * Description: Listening configuration file.
 */
#ifndef DATASYSTEM_COMMON_UTIL_FLAGS_MONITOR_H
#define DATASYSTEM_COMMON_UTIL_FLAGS_MONITOR_H

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>

#include "datasystem/common/util/gflag/flags.h"

namespace datasystem {
class FlagsMonitor {
public:
    FlagsMonitor();

    ~FlagsMonitor();

    /**
     * @brief Get FlagsMonitor single instance.
     * @return FlagsMonitor instance.
     */
    static FlagsMonitor *GetInstance();

    /**
     * @brief Start a thread to listen to configuration file changes.
     */
    void Start();

    /**
     * @brief Listen to file changes cyclically and process the changes.
     */
    void ListenConfigFile();
    
private:
    Flags flags_;
    std::thread monitorThread_;
    std::atomic<bool> stop_;
    std::atomic<bool> isStarted_;
    std::mutex mutex_;
    std::condition_variable condition_;
};

}  // namespace datasystem

#endif
