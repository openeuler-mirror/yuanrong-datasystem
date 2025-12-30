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
 * Description: agc cloudstorage access token request
 */
#ifndef DATASYSTEM_COMMON_CLOUDSTORAGE_AGC_OAUTH_TOKEN_REQ_H
#define DATASYSTEM_COMMON_CLOUDSTORAGE_AGC_OAUTH_TOKEN_REQ_H

#include "datasystem/common/util/timer.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/common/eventloop/timer_queue.h"

namespace datasystem {
/**
 * because access token server's flow control threshold is 1000times/5min,
 * to prevent access token expire at the same time, we make it early expire in some random seconds,
 * early expire time random in [EARLY_EXPIRE_MIN_SEC, EARLY_EXPIRE_MAX_SEC] seconds
 */
static constexpr uint64_t EARLY_EXPIRE_MIN_SEC = 60;
static constexpr uint64_t EARLY_EXPIRE_MAX_SEC = 600;

struct AccessTokenCache {
    Timer timer;
    SensitiveValue token;
    int32_t expireSec;
};

class ClientAccessToken {
public:
    /**
     * @brief client access token manager
     * @param[in] token Token get from IAM for this tenant.
     */
    ClientAccessToken(SensitiveValue token);

    ~ClientAccessToken();

    /**
     * @brief Update Token.
     * @param[in] token New token for update.
     */
    void UpdateToken(SensitiveValue &token);

    /**
     * @brief Update the access token for client when it is expired.
     */
    Status UpdateAccessToken(SensitiveValue &token);

private:
    // Lock for the update and query of client access token.
    std::shared_timed_mutex tokenMutex_;
    std::unique_ptr<TimerQueue::TimerImpl> timer_;
    std::atomic<bool> tokenValid_{ false };
    SensitiveValue token_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_CLOUDSTORAGE_AGC_OAUTH_TOKEN_REQ_H