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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_REGISTRY_IMPORT_ENDPOINT_REGISTRY_H
#define DATASYSTEM_COMMON_URMA_MOCK_REGISTRY_IMPORT_ENDPOINT_REGISTRY_H

#include <cstdint>
#include <string>

namespace datasystem {
namespace urma_mock {
/**
 * @brief Peer segment endpoint learned from a backend-specific exchange.
 * The endpoint map is keyed by token and records the peer instance, optional client id, remote wire address, and
 * wire-provided host:port used to resolve the peer UDS socket without sharing environment variables.
 */
struct ImportEndpoint {
    std::string instanceId;  ///< Peer UDS instance id.
    std::string clientId;    ///< Optional client id used to disambiguate same-token endpoints.
    uint64_t va = 0;         ///< Peer wire-visible segment address.
    uint64_t len = 0;        ///< Peer segment length.
    std::string host;        ///< Peer host parsed from a backend wire address.
    int port = -1;           ///< Peer port parsed from a backend wire address.
};

/**
 * @brief Process-local registry for imported peer endpoints.
 */
class ImportEndpointRegistry {
public:
    /**
     * @brief Get the process-wide import endpoint registry.
     * @return Import endpoint registry instance.
     */
    static ImportEndpointRegistry &Instance();

    /**
     * @brief Register an import endpoint by URMA token.
     * @param[in] token Segment token carried by URMA metadata.
     * @param[in] ep Peer endpoint to store.
     */
    void Register(uint64_t token, const ImportEndpoint &ep);

    /**
     * @brief Register an import endpoint by URMA token and client id.
     * @param[in] token Segment token carried by URMA metadata.
     * @param[in] clientId Client id used to disambiguate same-token endpoints.
     * @param[in] ep Peer endpoint to store.
     */
    void Register(uint64_t token, const std::string &clientId, const ImportEndpoint &ep);

    /**
     * @brief Lookup an import endpoint by token.
     * @param[in] token Segment token carried by URMA metadata.
     * @return Matched endpoint, or an empty endpoint if no entry exists.
     */
    ImportEndpoint Lookup(uint64_t token) const;

    /**
     * @brief Lookup an import endpoint by token and expected remote address.
     * @param[in] token Segment token carried by URMA metadata.
     * @param[in] remoteVa Expected remote segment address.
     * @return Matched endpoint, or an empty endpoint if no entry exists.
     */
    ImportEndpoint Lookup(uint64_t token, uint64_t remoteVa) const;

    /**
     * @brief Lookup an import endpoint by token and client id.
     * @param[in] token Segment token carried by URMA metadata.
     * @param[in] clientId Client id used to disambiguate same-token endpoints.
     * @return Matched endpoint, or an empty endpoint if no entry exists.
     */
    ImportEndpoint Lookup(uint64_t token, const std::string &clientId) const;

    /**
     * @brief Clear all registered import endpoints.
     */
    void Clear();

private:
    ImportEndpointRegistry() = default;
    ~ImportEndpointRegistry() = default;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_REGISTRY_IMPORT_ENDPOINT_REGISTRY_H
