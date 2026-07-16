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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_ENDPOINT_REGISTRY_H
#define DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_ENDPOINT_REGISTRY_H

#include <cstdint>
#include <string>

#include "datasystem/common/urma_mock/registry/import_endpoint_registry.h"

namespace datasystem {
namespace urma_mock {
/**
 * @brief Cross-process registry for segment owner endpoints.
 */
class SegmentEndpointRegistry {
public:
    /**
     * @brief Get the process-wide segment endpoint registry.
     * @return Segment endpoint registry instance.
     */
    static const SegmentEndpointRegistry &Instance();

    /**
     * @brief Register the endpoint that owns a segment key.
     * @param[in] key URMA-visible segment key.
     * @param[in] ep Endpoint that can serve HELLO for the segment.
     * @return true if the endpoint is stored.
     */
    bool Register(const std::string &key, const ImportEndpoint &ep) const;

    /**
     * @brief Remove one segment endpoint.
     * @param[in] key URMA-visible segment key.
     */
    void Unregister(const std::string &key) const;

    /**
     * @brief Remove segment endpoint records owned by one mock instance.
     * @param[in] instanceId Mock instance id that published the records.
     */
    void Clear(const std::string &instanceId) const;

    /**
     * @brief Lookup a registered segment endpoint.
     * @param[in] key URMA-visible segment key.
     * @param[in] containingPrefix Optional key prefix used when the exact key is not available.
     * @param[in] requestedVa Remote address that must fall inside the selected endpoint range when non-zero.
     * @return Matched endpoint, or an empty endpoint if no entry exists.
     */
    ImportEndpoint Lookup(const std::string &key, const std::string &containingPrefix, uint64_t requestedVa) const;

    /**
     * @brief Lookup a registered segment endpoint when only token and remote VA are available.
     * @param[in] token Segment token.
     * @param[in] requestedVa Remote address that must fall inside the selected endpoint range.
     * @return Matched endpoint, or an empty endpoint if no live entry exists.
     */
    ImportEndpoint Lookup(uint64_t token, uint64_t requestedVa) const;

private:
    SegmentEndpointRegistry() = default;
    ~SegmentEndpointRegistry() = default;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_ENDPOINT_REGISTRY_H
