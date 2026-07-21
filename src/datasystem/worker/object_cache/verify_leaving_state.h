/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker-side write rejection after topology scale-in starts.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_LEAVING_STATE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_LEAVING_STATE_H

#include <atomic>

#include "datasystem/utils/status.h"

namespace datasystem::worker {

/**
 * @brief Convert the Worker lifecycle exit gate to the write-retry protocol.
 */
template <typename ExitingEvaluator>
Status VerifyLeavingStateWithEvaluator(ExitingEvaluator &&isExiting)
{
    if (!isExiting()) {
        return Status::OK();
    }
    return Status(K_SCALE_DOWN, "Worker is exiting; stop writing.");
}

/**
 * @brief Gate the leaving-state check with the compatibility flag.
 */
template <typename ExitingEvaluator>
Status VerifyLeavingStateWithEvaluator(bool enabled, ExitingEvaluator &&isExiting)
{
    if (!enabled) {
        return Status::OK();
    }
    return VerifyLeavingStateWithEvaluator(isExiting);
}

/**
 * @brief Reject writes after the local Worker starts topology scale-in draining.
 *
 * The flag is the scale-in drain gate, not a process-termination signal.
 */
inline Status VerifyLeavingState(const std::atomic<bool> *exitRequested)
{
    return VerifyLeavingStateWithEvaluator(
        [exitRequested] { return exitRequested != nullptr && exitRequested->load(std::memory_order_acquire); });
}

/**
 * @brief Reject writes on an exiting Worker when leaving interception is enabled.
 */
inline Status VerifyLeavingState(const std::atomic<bool> *exitRequested, bool enabled)
{
    return VerifyLeavingStateWithEvaluator(enabled, [exitRequested] {
        return exitRequested != nullptr && exitRequested->load(std::memory_order_acquire);
    });
}

}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_LEAVING_STATE_H
