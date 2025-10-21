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
 * Description: RPC connection Credential
 */

#include "datasystem/common/rpc/rpc_credential.h"
#include <cstdint>
namespace datasystem {
void RpcCredential::SetAuthCurveServer(const char *secretKey)
{
    mechanism_ = RPC_AUTH_TYPE::CURVE;
    cred_.curve_.curveSecretKey_ = secretKey;
}

void RpcCredential::SetAuthCurveClient(const char *publicKey, const char *secretKey, const char *serverKey)
{
    mechanism_ = RPC_AUTH_TYPE::CURVE;
    cred_.curve_.curvePublicKey_ = publicKey;
    cred_.curve_.curveSecretKey_ = secretKey;
    cred_.curve_.curveServerKey_ = serverKey;
}
}  // namespace datasystem

/**
 * @brief To support credential as key in std::unordered_map, we need to provide two
 * functions (a) hash and (b) equal_to
 */
namespace std {
std::size_t hash<datasystem::RpcCredential>::operator()(const datasystem::RpcCredential &cred) const
{
    if (cred.NoAuthentication()) {
        return 0;
    }
    auto val1 = std::hash<int64_t>()(reinterpret_cast<intptr_t>(cred.cred_.curve_.curvePublicKey_));
    auto val2 = std::hash<int64_t>()(reinterpret_cast<intptr_t>(cred.cred_.curve_.curveSecretKey_));
    auto val3 = std::hash<int64_t>()(reinterpret_cast<intptr_t>(cred.cred_.curve_.curveServerKey_));
    return val1 ^ val2 ^ val3;
}

bool equal_to<datasystem::RpcCredential>::operator()(const datasystem::RpcCredential &lhs,
                                                     const datasystem::RpcCredential &rhs) const
{
    if (lhs.NoAuthentication() && rhs.NoAuthentication()) {
        return true;
    } else if (lhs.GetAuthMechanism() == datasystem::RPC_AUTH_TYPE::CURVE
               && rhs.GetAuthMechanism() == datasystem::RPC_AUTH_TYPE::CURVE) {
        return (lhs.cred_.curve_.curvePublicKey_ == rhs.cred_.curve_.curvePublicKey_
                && lhs.cred_.curve_.curveSecretKey_ == rhs.cred_.curve_.curveSecretKey_
                && lhs.cred_.curve_.curveServerKey_ == rhs.cred_.curve_.curveServerKey_);
    }
    return false;
}

std::size_t hash<std::pair<int, datasystem::RpcCredential>>::operator()(
    const std::pair<int, datasystem::RpcCredential> &ele) const
{
    return ele.first == 0 ? std::hash<datasystem::RpcCredential>()(ele.second) : ele.first;
}

bool equal_to<std::pair<int, datasystem::RpcCredential>>::operator()(
    const std::pair<int, datasystem::RpcCredential> &lhs, const std::pair<int, datasystem::RpcCredential> &rhs) const
{
    return (lhs.first == rhs.first) && std::equal_to<datasystem::RpcCredential>()(lhs.second, rhs.second);
}
}  // namespace std
