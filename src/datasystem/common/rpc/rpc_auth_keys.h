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
 * Description: Struct for curve authentication key settings.
 */
#ifndef DATASYSTEM_UTILS_RPC_AUTH_KEYS
#define DATASYSTEM_UTILS_RPC_AUTH_KEYS

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const std::string WORKER_SERVER_NAME = "worker";
const std::string MASTER_SERVER_NAME = "master";
const std::unordered_set<std::string> SERVER_TYPES = { WORKER_SERVER_NAME, MASTER_SERVER_NAME };
/**
 * @brief client rpc authentication key settings.
 */
class RpcAuthKeys {
public:
    /**
     * @brief A helper function for RpcAuthKeys initialization.
     * @param[in] clientPublicKey The client's public key.
     * @param[in] clientPrivateKey The client's private key.
     * @param[in] rpcServerKeys The servers' public keys.
     * @return Status of the call.
     */
    Status SetRpcAuthKeys(const std::string &clientPublicKey, const std::string &clientPrivateKey,
                          const std::unordered_map<std::string, std::string> &rpcServerKeys);

    /**
     * @brief Getter function to access the client public key.
     * @return The client's public key.
     */
    const char *GetClientPublicKey() const;

    /**
     * @brief Setter function to access the client public key.
     * @param[in] clientPublicKey The client's public key.
     */
    Status SetClientPublicKey(std::unique_ptr<char[]> &clientPublicKey);
    Status SetClientPublicKey(const std::string &clientPublicKey);

    /**
     * @brief Getter function to access the client private key.
     * @return The client's private key.
     */
    const char *GetClientPrivateKey() const;

    /**
     * @brief Setter function to access the client private key.
     * @param[in] clientPrivateKey The client's private key.
     */
    Status SetClientPrivateKey(std::unique_ptr<char[]> &clientPrivateKey);
    Status SetClientPrivateKey(SensitiveValue clientPrivateKey);

    /**
     * @brief Getter function to access the server public key based on server name.
     * @param[in] serverName The server name.
     * @param[out] keyContent The corresponding server's key.
     * @return Status of the call.
     */
    Status GetServerKey(const std::string &serverName, const char *&keyContent) const;

    /**
     * @brief Setter function to set a particular server keys based on the given serverName.
     * @param[in] serverName The corresponding server name.
     * @param[in] keyContent The actual key contents to set.
     */
    Status SetServerKey(const std::string &serverName, std::unique_ptr<char[]> &keyContent);
    Status SetServerKey(const std::string &serverName, const std::string &keyContent);

private:
    /**
     * @brief The client's public key, default is "".
     */
    std::unique_ptr<char[]> clientPublicKey_;

    /**
     * @brief The client's private key, default is "".
     */
    std::unique_ptr<char[]> clientPrivateKey_;

    /**
     * @brief The servers' public keys.
     */
    std::unordered_map<std::string, std::unique_ptr<char[]>> serverPublicKeys_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_UTILS_RPC_AUTH_KEYS
