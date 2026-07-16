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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_TRANSPORT_UDS_ENDPOINT_SERVICE_H
#define DATASYSTEM_COMMON_URMA_MOCK_TRANSPORT_UDS_ENDPOINT_SERVICE_H

#include <cstdint>
#include <string>

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#include "datasystem/common/urma_mock/registry/import_endpoint_registry.h"

namespace datasystem {
namespace urma_mock {
/**
 * @brief UDS endpoint service used by the URMA mock ABI layer.
 */
class UdsEndpointService {
public:
    /**
     * @brief Get the process-wide UDS endpoint service.
     * @return UDS endpoint service instance.
     */
    static const UdsEndpointService &Instance();

    /**
     * @brief Start the per-process UDS listener if it is not running.
     * The listener serves HELLO/HELLO_ACK frames and passes the peer segment memfd via SCM_RIGHTS. Forked children
     * rebuild listener state lazily outside the pthread_atfork handler.
     */
    void EnsureListener() const;

    /**
     * @brief Stop the per-process UDS listener and clear listener-owned state.
     */
    void ShutdownListener() const;

    /**
     * @brief Register an import endpoint keyed by URMA token.
     * @param[in] token URMA segment token.
     * @param[in] ep Peer endpoint learned from handshake.
     */
    void RegisterImportEndpoint(uint64_t token, const ImportEndpoint &ep) const;

    /**
     * @brief Lookup an import endpoint by URMA token and expected remote address.
     * @param[in] token URMA segment token.
     * @param[in] remoteVa Expected remote segment address.
     * @return Matched endpoint, or an empty endpoint when no entry exists.
     */
    ImportEndpoint LookupImportEndpoint(uint64_t token, uint64_t remoteVa) const;

    /**
     * @brief Register a segment endpoint derived from the URMA-visible segment identity.
     * @param[in] seg Segment returned by ds_urma_register_seg.
     * @param[in] token URMA segment token.
     */
    void RegisterSegmentEndpoint(const urma_seg_t &seg, uint64_t token) const;

    /**
     * @brief Remove a segment endpoint derived from the URMA-visible segment identity.
     * @param[in] seg Segment returned by ds_urma_register_seg.
     * @param[in] token URMA segment token.
     */
    void UnregisterSegmentEndpoint(const urma_seg_t &seg, uint64_t token) const;

    /**
     * @brief Lookup a segment endpoint by URMA-visible segment identity.
     * @param[in] seg Segment passed to ds_urma_import_seg.
     * @param[in] token URMA segment token.
     * @return Matched endpoint, or an empty endpoint when no entry exists.
     */
    ImportEndpoint LookupSegmentEndpoint(const urma_seg_t &seg, uint64_t token) const;

    /**
     * @brief Import a peer segment by exchanging HELLO/HELLO_ACK with a peer UDS instance.
     * @param[in] instanceId Peer UDS instance id.
     * @param[in] token URMA segment token.
     * @param[out] outVa Peer wire-visible segment address from HELLO_ACK.
     * @param[out] outLen Peer segment length from HELLO_ACK.
     * @param[out] outOffset Peer memfd file offset corresponding to outVa.
     * @param[in] requestedVa Optional expected peer virtual address.
     * @return A dup'd peer memfd fd on success; -1 on failure. The caller owns and must close the returned fd.
     */
    [[nodiscard]] int ImportSegViaUds(const std::string &instanceId, uint64_t token, uint64_t *outVa, uint64_t *outLen,
                                      uint64_t requestedVa = 0, uint64_t *outOffset = nullptr) const;

    /**
     * @brief Import a peer segment by deriving the peer UDS path from wire-provided host:port.
     * @param[in] host Peer host from URMA wire address.
     * @param[in] port Peer port from URMA wire address.
     * @param[in] token URMA segment token.
     * @param[out] outVa Peer wire-visible segment address from HELLO_ACK.
     * @param[out] outLen Peer segment length from HELLO_ACK.
     * @param[out] outOffset Peer memfd file offset corresponding to outVa.
     * @param[in] requestedVa Optional expected peer virtual address.
     * @return A dup'd peer memfd fd on success; -1 on failure. The caller owns and must close the returned fd.
     */
    [[nodiscard]] int ImportSegViaUdsForHost(const std::string &host, int port, uint64_t token, uint64_t *outVa,
                                             uint64_t *outLen, uint64_t requestedVa = 0,
                                             uint64_t *outOffset = nullptr) const;

private:
    UdsEndpointService() = default;
    ~UdsEndpointService() = default;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_TRANSPORT_UDS_ENDPOINT_SERVICE_H
