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
 * Description: Helper functions to load curve public/private key pair.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_AUTH_KEY_MANAGER_H
#define DATASYSTEM_COMMON_RPC_RPC_AUTH_KEY_MANAGER_H

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/rpc/rpc_auth_keys.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/rpc/zmq/zmq_auth.h"

namespace datasystem {
const std::string PUBLIC_KEY_EXT = ".key";
const std::string PRIVATE_KEY_EXT = ".key_secret";
const std::string AUTHORIZED_DIR_SUFFIX = "_authorized_clients";
const std::string SVC_MAPPING_FILE_NAME = "service.mapping";

// Naming convention of the key files: <component name>.<key extension>
// For example: worker.key is the public key for the worker component.

// Notably, the key names for the authorized keys are not restricted.
// They only need to be present in the service.mapping file.

/**
 * @brief RpcAuthKeyManager singleton class used for managing the Curve keys.
 */
class RpcAuthKeyManager {
public:
    ~RpcAuthKeyManager() noexcept = default;

    /**
     * @brief Get the Singleton RpcAuthKeyManager instance.
     * @return RpcAuthKeyManager instance.
     */
    static RpcAuthKeyManager &Instance()
    {
        static RpcAuthKeyManager keys;
        return keys;
    }

    /**
     * @brief Store all the provided keys into RpcAuthKeyManager.
     * @param[in] authKeys The keys to move.
     */
    void SetKeys(RpcAuthKeys &authKeys)
    {
        authKeys_ = std::move(authKeys);
    }

    /**
     * @brief Store all the provided keys into RpcAuthKeyManager.
     * @param[in] authorizedClients The authorized client keys to move.
     */
    void SetAuthorizedClients(std::vector<std::unique_ptr<char[]>> &authorizedClients)
    {
        authorizedClients_ = std::move(authorizedClients);
    }

    /**
     * @brief Getter function to access all the keys in RpcAuthKeyManager.
     * @return The stored keys.
     */
    const RpcAuthKeys &GetKeys() const
    {
        return authKeys_;
    }

    /**
     * @brief Load curve key files for server side.
     * @param[in] serverName The server node type.
     * @param[out] cred The authentication credentials.
     * @return Status of the call.
     */
    static Status ServerLoadKeys(const std::string &serverName, RpcCredential &cred);

    /**
     * @brief Set up the credentials using the stored keys in manager.
     * @param[in] serverName The server node type.
     * @param[out] cred The authentication credentials.
     * @return Status of the call.
     */
    static Status CreateCredentials(const std::string &serverName, RpcCredential &cred);

    /**
     * @brief Set up the client credentials using the explicitly provided keys.
     * @note The lifetime of the provided keys must exceed that of the credentials, because only the pointers are used
     * in the credentials.
     * @param[in] authKeys The explicitly provided authentication keys.
     * @param[in] serverName The server node type.
     * @param[out] cred The authentication credentials.
     * @return Status of the call.
     */
    static Status CreateClientCredentials(const RpcAuthKeys &authKeys, const std::string &serverName,
                                          RpcCredential &cred);

    /**
     * @brief Copy the curve authentication key from src to dest.
     * @param[in] src The source of the copy.
     * @param[out] dest The destination of the copy.
     * @return Status of the call.
     */
    static Status CopyCurveAuthKey(const char *src, std::unique_ptr<char[]> &dest);

    /**
     * @brief Set the Auth Handler object.
     * @param[in] authHandler Zmq authentication handler.
     */
    void SetAuthHandler(std::unique_ptr<datasystem::ZmqAuthHandler> &authHandler);
    /**
     * @brief Check if Auth Handler is set.
     * @return true if there is a Zmq authentication handler.
     */
    bool HasAuthHandler();

    /**
     * @brief Initialize authentication handler for this server to enable authentication.
     * @param ctx The ZmqContext.
     * @return Status of the call.
     */
    Status InitAuthHandler(const std::shared_ptr<datasystem::ZmqContext> &ctx);

    /**
     * @brief Start the Zmq authentication handler.
     * @param ctx The ZmqContext.
     * @return Status of the call.
     */
    Status StartAuthHandler();

    /**
     * @brief Stop the Zmq authentication handler.
     */
    void StopAuthHandler();

    /**
     * @brief Set the Svc Mapping object.
     * @param svcMapping The service name to public key mapping.
     */
    void SetSvcMapping(SvcToKeyMapping &svcMapping);

    /**
     * @brief Get the Svc Mapping object.
     * @return The service name to public key mapping.
     */
    const SvcToKeyMapping &GetSvcMapping();

private:
    RpcAuthKeyManager() = default;

    RpcAuthKeys authKeys_;
    std::vector<std::unique_ptr<char[]>> authorizedClients_;
    std::unique_ptr<ThreadPool> thrdPool_;
    std::unique_ptr<std::future<Status>> authHandlerThrd_{ nullptr };
    std::unique_ptr<datasystem::ZmqAuthHandler> authHandler_{ nullptr };
    SvcToKeyMapping svcMapping_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_AUTH_KEY_MANAGER_H
