/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/** Description: Defines deadline-aware retry mechanics shared by transport operations. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_COMMON_DEADLINE_RETRY_H
#define DATASYSTEM_CLIENT_TRANSPORT_COMMON_DEADLINE_RETRY_H

#include <cstdint>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class DeadlineRetry {
public:
    ~DeadlineRetry() = default;

    /** @brief Return whether an RPC status is safe to retry for an idempotent operation. */
    bool IsRetryableRpcError(const Status &status) const;

    /** @brief Check the shared API deadline. */
    Status CheckDeadline() const;

    /** @brief Sleep with capped exponential backoff within the shared API deadline. */
    Status Backoff(int64_t &backoffMs) const;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_COMMON_DEADLINE_RETRY_H
