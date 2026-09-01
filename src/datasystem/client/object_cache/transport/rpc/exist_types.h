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

#ifndef DATASYSTEM_CLIENT_TRANSPORT_RPC_EXIST_TYPES_H
#define DATASYSTEM_CLIENT_TRANSPORT_RPC_EXIST_TYPES_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
namespace client {

struct TransportExistRequest {
    // objectKeys is a const reference; the caller's vector must outlive all uses of this struct.
    // The rvalue constructor is deleted to prevent binding to temporaries.
    TransportExistRequest(const std::vector<std::string> &keys, bool queryL2, bool isLocal, int64_t timeoutMs,
                          std::string clientId, std::string tenantId, SensitiveValue token)
        : objectKeys(keys),
          queryL2Cache(queryL2),
          isLocal(isLocal),
          subTimeoutMs(timeoutMs),
          clientId(std::move(clientId)),
          tenantId(std::move(tenantId)),
          token(std::move(token))
    {
    }
    TransportExistRequest(std::vector<std::string> &&keys, bool, bool, int64_t, std::string, std::string,
                          SensitiveValue) = delete;

    const std::vector<std::string> &objectKeys;
    bool queryL2Cache;
    bool isLocal;
    int64_t subTimeoutMs;
    std::string clientId;
    std::string tenantId;
    SensitiveValue token;
};

struct TransportExistResult {
    std::vector<bool> exists;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_RPC_EXIST_TYPES_H
