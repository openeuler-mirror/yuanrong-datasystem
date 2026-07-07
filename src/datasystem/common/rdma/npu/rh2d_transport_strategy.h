/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_RDMA_NPU_RH2D_TRANSPORT_STRATEGY_H
#define DATASYSTEM_COMMON_RDMA_NPU_RH2D_TRANSPORT_STRATEGY_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/device/ascend/p2phccl_types.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Strategy interface for RH2D transport protocols.
 *
 * Provides a unified abstraction for different P2P protocols (RoCE, HCCS).
 * Concrete implementations: RoCETransport, HCCSTransport.
 */
class RH2DTransportStrategy {
public:
    virtual ~RH2DTransportStrategy() = default;

    /**
     * @brief Initialize transport engine.
     * @param[in] deviceIds List of NPU device IDs to create per-device engines for.
     *                      Empty or single-element for single-device (RoCE ignores this).
     * @return Status of the call.
     *
     * Called by RemoteH2DManager::Init() after the ACL device is selected.
     * - RoCE: no-op (deviceIds unused).
     * - HCCS: creates one HIXL engine per device ID on distinct ports of the same
     *         local IP, so each outgoing P2P connection is bound to a specific NPU.
     */
    virtual Status Init(const std::vector<int32_t> &deviceIds) = 0;

    /**
     * @brief Get local connection identity for exchange.
     * @param[out] identity Base64(rootInfo) for RoCE, or "IP:Port" for HCCS.
     * @return Status of the call.
     *
     * Identity string is used by remote peer for connection establishment.
     */
    virtual Status GetConnectionIdentity(std::string *identity) = 0;

    /**
     * @brief Establish P2P connection.
     * @param[in] remoteIdentity Base64(rootInfo) for RoCE, "IP:Port" for HCCS.
     * @param[in] kind Direction of the connection (sender/receiver).
     * @param[in,out] heartbeatCallback Callback for heartbeat (RoCE only).
     * @return Status of the call.
     *
     * For RoCE, heartbeatCallback must point to a std::function<int()> that
     * the driver can set and that manager will invoke for heartbeats.
     * For HIXL/HCCS, calls Hixl::Connect eagerly; MEM_DEVICE destinations are registered later in ScatterBatch
     * unless they were pre-registered by the client.
     */
    virtual Status Connect(const std::string &remoteIdentity, P2pKind kind,
                           std::function<int()> *heartbeatCallback) = 0;

    /**
     * @brief Disconnect a single peer identified by remoteIdentity.
     * @param[in] remoteIdentity Base64(rootInfo) for RoCE, "IP:Port" for HCCS.
     * @return Status of the call.
     *
     * Called by the heartbeat eviction path when a peer is detected dead so that the per-connection
     * resources (e.g. RoCE P2PComm, HIXL Connect handle) are released without tearing down
     * the whole transport.
     */
    virtual Status Disconnect(const std::string &remoteIdentity) = 0;

    /**
     * @brief Disconnect all established connections.
     * @return Status of the call.
     *
     * Called during RemoteH2DManager uninitialization.
     * Required by both RoCE and HCCS.
     */
    virtual Status DisconnectAll() = 0;

    /**
     * @brief Register host memory for P2P transfer.
     * @param[in] addr Host memory address.
     * @param[in] size Size of memory region.
     * @param[out] segInfo Segment information for driver.
     * @return Status of the call.
     *
     * - RoCE: calls DSP2PRegisterHostMem.
     * - HIXL/HCCS buffer-pool: intentionally skips source host registration; segInfo is zeroed. HIXL routes
     *   unregistered remote host addresses through its internal buffer-pool relay.
     * - HIXL ROCE direct: registers the worker source host buffer with HIXL before Connect.
     */
    virtual Status RegisterMemory(void *addr, uint64_t size, P2pSegmentInfo *segInfo) = 0;

    /**
     * @brief Pre-register local device memory for HCCS/HIXL transfers.
     * @param[in] addrs Device memory addresses.
     * @param[in] sizes Sizes of memory regions.
     * @return Status of the call.
     *
     * - RoCE: no-op. RoCE registers/imports remote host segments instead.
     * - HCCS: registers MEM_DEVICE once and keeps the handle until this memory is unregistered or transport teardown.
     */
    virtual Status PreRegisterDeviceMemory(const std::vector<void *> &addrs, const std::vector<uint64_t> &sizes)
    {
        (void)addrs;
        (void)sizes;
        return Status::OK();
    }

    /**
     * @brief Release pre-registered local device memory for HCCS/HIXL transfers.
     * @param[in] addrs Starting addresses passed to PreRegisterDeviceMemory.
     * @return Status of the call.
     */
    virtual Status UnregisterDeviceMemory(const std::vector<void *> &addrs)
    {
        (void)addrs;
        return Status::OK();
    }

    /**
     * @brief Import remote memory/segment address information.
     * @param[in] remoteEndpoint Remote endpoint identifier.
     * @param[in] seg Segment info containing address/length (and protobuf name for RoCE).
     * @return Status of the call.
     *
     * - RoCE: Uses seg.name() as the segment descriptor; calls DSP2PImportHostSegment.
     * - HIXL/HCCS: no-op; the remote address is carried inline by P2pScatterEntry::ddrBuf at transfer time.
     */
    virtual Status ImportRemoteAddressInfo(const std::string &remoteEndpoint, const RemoteHostSegmentPb &seg) = 0;

    /**
     * @brief Execute scatter-gather RDMA transfer.
     * @param[in] entries Array of scatter entries.
     * @param[in] count Number of entries.
     * @param[in] remoteEndpoint Remote endpoint identifier.
     * @param[in] stream Shared pointer to ACL stream for async operations (RoCE only, HCCS ignores). May be nullptr for
     * HCCS.
     * @return Status of the call.
     *
     * Each entry describes a remote buffer (ddrBuf) split into multiple
     * destination buffers (dstBufs) with corresponding counts.
     *
     * - RoCE: calls DSP2PScatterBatchFromRemoteHostMem, uses provided stream.
     *         Implementation splits large batches to respect FFTS context limit
     *         (max 16384 blobs per batch).
     * - HCCS: uses explicitly pre-registered MEM_DEVICE ranges when available. Otherwise it temporarily registers
     *         local destinations for the transfer, then issues Hixl::TransferSync(READ) in chunks.
     */
    virtual Status ScatterBatch(P2pScatterEntry *entries, uint32_t count, const std::string &remoteEndpoint,
                                std::shared_ptr<aclrtStream> stream) = 0;

    /**
     * @brief Get link type for conditional logic.
     * @return P2P_LINK_ROCE or P2P_LINK_HCCS.
     */
    virtual P2pLink LinkType() const = 0;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_NPU_RH2D_TRANSPORT_STRATEGY_H
