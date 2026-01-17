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
 * Description: Define signal handler function.
 */
#include "datasystem/common/signal/signal.h"

#include <csignal>
#include <chrono>
#include <ctime>

#include "datasystem/common/log/log.h"

namespace datasystem {
volatile sig_atomic_t g_exitFlag = 0;
std::condition_variable g_termSignalCv;
std::mutex g_termSignalMutex;

void SignalHandler(int signum)
{
    (void)signum;
    g_exitFlag = 1;
    g_termSignalCv.notify_all();
}

bool IsTermSignalReceived()
{
    return g_exitFlag;
}
}  // namespace datasystem
