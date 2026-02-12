/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_DEVICE_COMM_WRAPPER_H
#define DATASYSTEM_COMMON_DEVICE_COMM_WRAPPER_H

#include <atomic>
#include <memory>

#include "datasystem/common/device/comm_wrapper_base.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/raii.h"

namespace datasystem {

constexpr uint32_t P2P_RANK_NUM = 2;
constexpr uint32_t P2P_SEND_RANK = 0;
constexpr uint32_t P2P_RECV_RANK = 1;

class CommWrapper : public CommWrapperBase {
public:
    explicit CommWrapper(const std::string &commId, int localDeviceId, int remoteDeviceId,
                         std::shared_ptr<HcclCommMagr> &threadControl, DeviceResourceManager *resourceMgr);

    ~CommWrapper();

    void ShutDown() override;

    /**
     * @brief Init communicator. Special implementation based on this communication domain class.
     * @param[in] numRanks The number of ranks.
     * @param[in] rootInfo The root info.
     * @param[in] rank The rank in local.
     * @return Status of the call.
     */
    Status InitComm(int numRanks, CommRootInfo &rootInfo, int rank);

    /**
     * @brief Init communicator.
     * @param[in] direction own transmission direction.
     * @param[in] rootInfo The root info.
     * @return Status of the call.
     */
    Status InitCommunicator(CommRootInfo &rootInfo, const CommDirection direction, bool isSameNode) override;
    /**
     * @brief P2P send the data to the receiving side.
     * @param[in] blobs The list of the blob info.
     * @param[in] event The DeviceRtEvent wrapper.
     * @return Status of the call
     */

    Status P2PSend(const std::vector<Blob> &blobs, const std::shared_ptr<DeviceRtEventWrapper> &event,
                   aclrtStream stream) override;

    /**
     * @brief P2P recv the data from the sending side.
     * @param[in] blobs The list of the blob info.
     * @param[in] event The DeviceRtEvent wrapper.
     * @param[in] stream The stream of runtime context.
     * @return Status of the call
     */
    Status P2PRecv(const std::vector<Blob> &blobs, const std::shared_ptr<DeviceRtEventWrapper> &event,
                   aclrtStream stream) override;

    /**
     * @brief Queries whether an error occurs in the communication domain.
     * @return The status of invocation.
     */
    Status GetCommAsyncError() override;

    /**
     * @brief Warm up the communicator wrapper in the send side.
     * Attention! The communication interface has limitations.
     * Suppose thread A creates communicator a1, but does not call the send/recv interfaces.
     * Thread B also creates communicator b1 and calls the send/recv interface,
     * then communicator a1 will not work properly.
     * So we need to call send/recv immediately after creating the communicator to establish a socket,
     * and ensure that the communication domain can be used normally.
     * @param[in] eventType The p2p event type: SEND or RECV
     * @return The status of call.
     */
    Status WarmUpComm(CommDirection eventType) override;

    /**
     * @brief Creating communicator rootinfo.
     * @param[in] rootInfo Transfer a blank rootinfo, create a reference, and transfer a value.
     * @return Status of the call.
     */
    Status CreateRootInfo(CommRootInfo &rootInfo) override;

private:
    /**
     * @brief Check if the communication domain pointer is null.
     * @param[in] comm The communicator ptr.
     * @return Status of the call
     */
    Status CheckCommPtr(const void *ptr);
};
}  // namespace datasystem
#endif
