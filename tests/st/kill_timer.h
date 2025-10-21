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
 * Description: Kill Dead Locked or Time Out Process.
 */
#ifndef DATASYSTEM_TEST_ST_KILL_TIMER_H
#define DATASYSTEM_TEST_ST_KILL_TIMER_H

#include <csignal>

#include <vector>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <functional>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace st {
class KillTimer {
public:
    explicit KillTimer(size_t timeOut = 90, const std::function<void()> &func = {}) : isRunning(true)
    {
        timer = std::thread([this, timeOut, func]() {
            bool finished;
            {
                std::unique_lock<std::mutex> lck(mtx);
                finished = cv.wait_for(lck, std::chrono::seconds(timeOut), [this] { return !isRunning; });
            }
            if (!finished) {
                LOG(INFO) << "Time Out, Need to Kill";
                func();
                abort();
            }
        });
    }

    ~KillTimer()
    {
        {
            std::unique_lock<std::mutex> lck(mtx);
            isRunning = false;
            cv.notify_all();
        }
        timer.join();
    }

private:
    std::thread timer;
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic_bool isRunning;
};
}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TEST_ST_KILL_TIMER_H
