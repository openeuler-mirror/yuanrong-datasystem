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
 * Description: deadlock util.
 */
#include "datasystem/common/util/deadlock_util.h"

#include <algorithm>

#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
static constexpr int RETRY_DELAY_MIN_MS = 5;
static constexpr int RETRY_DELAY_MAX_MS = 30;

Status RetryWhenDeadlock(const std::function<Status()> &fn)
{
    Status rc = fn();
    if (rc.GetCode() == K_WORKER_TIMEOUT) {
        int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
        while (remainingTimeMs > 0) {
            LOG(INFO) << FormatString("Remote worker timeout, try again.");
            uint64_t min = std::min<uint64_t>(remainingTimeMs, RETRY_DELAY_MIN_MS);
            uint64_t max = std::min<uint64_t>(remainingTimeMs, RETRY_DELAY_MAX_MS);
            uint64_t delayMs = RandomData().GetRandomUint64(min, max);
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
            rc = fn();
            if (rc.GetCode() != K_WORKER_TIMEOUT) {
                break;
            }
            remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
        }
    }
    return rc;
}
}  // namespace datasystem
