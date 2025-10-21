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
#ifndef DATASYSTEM_COMMON_RPC_RPC_CREDENTIAL_H
#define DATASYSTEM_COMMON_RPC_RPC_CREDENTIAL_H

#include <cstdint>
#include <cstring>
#include <functional>
namespace datasystem {
class RpcCredential;
}

/**
 * @brief Allow credential to be used as key in std::unordered_map
 */
namespace std {
template <>
struct hash<datasystem::RpcCredential> {
    size_t operator()(const datasystem::RpcCredential &cred) const;
};
template <>
struct equal_to<datasystem::RpcCredential> {
    bool operator()(const datasystem::RpcCredential &lhs, const datasystem::RpcCredential &rhs) const;
};
template <>
struct hash<std::pair<int, datasystem::RpcCredential>> {
    size_t operator()(const std::pair<int, datasystem::RpcCredential> &ele) const;
};
template <>
struct equal_to<std::pair<int, datasystem::RpcCredential>> {
    bool operator()(const std::pair<int, datasystem::RpcCredential> &lhs,
                    const std::pair<int, datasystem::RpcCredential> &rhs) const;
};
}  // namespace std

namespace datasystem {
enum class RPC_AUTH_TYPE : uint8_t { NONE = 0, CURVE = 1, TLS = 2 };
class RpcCredential {
public:
    friend struct std::hash<RpcCredential>;
    friend struct std::equal_to<RpcCredential>;
    RpcCredential() : mechanism_(RPC_AUTH_TYPE::NONE)
    {
    }
    ~RpcCredential() = default;

    bool NoAuthentication() const
    {
        return mechanism_ == RPC_AUTH_TYPE::NONE;
    }

    bool SameAuthMechanism(const RpcCredential &other) const
    {
        return mechanism_ == other.mechanism_;
    }

    auto GetAuthMechanism() const
    {
        return mechanism_;
    }

    /**
     * @brief Set the socket to act as server using CURVE authentication mechanism. See ZMQ RFC 27 for more details.
     * @param[in] secretKey Server secret key.
     */
    void SetAuthCurveServer(const char *secretKey);

    /**
     * @brief Set the socket to act as client using CURVE authentication mechanism. See ZMQ RFC 27 for more details.
     * @param[in] publicKey Client public key.
     * @param[in] secretKey Client secret key.
     * @param[in] serverKey Server public key.
     */
    void SetAuthCurveClient(const char *publicKey, const char *secretKey, const char *serverKey);

private:
    friend class ZmqSocket;
    RPC_AUTH_TYPE mechanism_;
    union {
        struct CurveCred {
            const char *curvePublicKey_;
            const char *curveSecretKey_;
            const char *curveServerKey_;
        } curve_;
        // A placeholder for TLS authentication
        struct TlsCred {
            const char *dummy_;
        } tls_;
    } cred_{};
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_CREDENTIAL_H
