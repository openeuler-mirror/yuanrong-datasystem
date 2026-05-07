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
 * Description: Client access token request
 */
#include "datasystem/common/token/client_access_token.h"

namespace datasystem {
ClientAccessToken::ClientAccessToken(SensitiveValue token) : token_(std::move(token))
{
    tokenValid_ = true;
}

void ClientAccessToken::UpdateToken(SensitiveValue &token)
{
    std::unique_lock<std::shared_timed_mutex> lock(tokenMutex_);
    token_.Clear();
    token_ = std::move(token);
}

ClientAccessToken::~ClientAccessToken()
{
    if (timer_) {
        TimerQueue::GetInstance()->Cancel(*(timer_));
    }
}

Status ClientAccessToken::UpdateAccessToken(SensitiveValue &token)
{
    if (tokenValid_) {
        std::shared_lock<std::shared_timed_mutex> lock(tokenMutex_);
        token = token_;
        return Status::OK();
    } else {
        return Status(K_RUNTIME_ERROR, "token is not invalid");
    }
    return Status::OK();
}
}  // namespace datasystem