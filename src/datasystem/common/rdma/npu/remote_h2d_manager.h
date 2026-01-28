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
#ifndef DATASYSTEM_COMMON_RPC_REMOTE_H2D_MANAGER_H
#define DATASYSTEM_COMMON_RPC_REMOTE_H2D_MANAGER_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>

#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/lock_map.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {

struct HostSegment {
    std::byte *data;
    std::uint64_t dataSize;
    P2pSegmentInfo segmentInfo;
    P2pSegmentPermissions permissions;

    HostSegment() = default;

    HostSegment(std::byte *data, std::uint64_t dataSize, P2pSegmentInfo segmentInfo, P2pSegmentPermissions permissions);

    static Status Create(std::shared_ptr<HostSegment> &result, std::byte *data, std::size_t data_size,
                         P2pSegmentInfo segmentInfo, P2pSegmentPermissions permissions);

    ~HostSegment();
};

using HostSegmentMap = LockMap<uint64_t, std::unordered_map<uint64_t, std::shared_ptr<HostSegment>>>;
using RemoteSegmentMap = LockMap<std::string, std::shared_ptr<HostSegment>>;

struct RemoteH2DContext {
    ~RemoteH2DContext();

    P2PComm p2pComm = nullptr;
    HcclRootInfo rootInfo;
    enum InitState : uint32_t { UNINITIALIZED = 0, INITIALIZING, INITIALIZED };
    std::atomic<InitState> initialized = InitState::UNINITIALIZED;
    WaitPost waitPost;
    std::shared_ptr<aclrtStream> stream{ nullptr };
    std::mutex mutex;
    int32_t devId;
};

using CommunicatorMap = LockMap<std::string, std::shared_ptr<RemoteH2DContext>>;

class RemoteH2DManager {
public:
    /**
     * @brief Get the singleton instance of RemoteH2DManager.
     * @return Reference to the RemoteH2DManager.
     */
    static RemoteH2DManager &Instance();

    ~RemoteH2DManager();

    /**
     * @brief Client sets global remote h2d configurations according to connect options.
     * @param[in] enableRemoteH2D Whether to enable remote host to device data transfer.
     * @param[in] devId The NPU device id.
     */
    static void SetClientRemoteH2DConfig(bool enableRemoteH2D, uint32_t devId);

    /**
     * @brief Whether remote H2D is enabled according to FLAGS_enable_remote_h2d.
     * @return true if remote H2D is enabled.
     */
    static bool IsRemoteH2DEnabled();

    /**
     * @brief Set NPU device index according to the specified devId, or FLAGS_remote_h2d_device_ids otherwise.
     * @param[in] devId Optional. Specify the device id to execute on.
     * @return Status of the call.
     */
    Status SetDeviceIdx(std::optional<int32_t> specifiedDevId = std::nullopt);

    /**
     * @brief Get an unique identifier for connection between device and the remote host.
     * @param[out] commId The uuid in string.
     * @return Status of the call.
     */
    Status GetClientCommUuid(std::string &commId);

    /**
     * @brief Get a vector of device ids the worker is allowed to use, as specified by the flag remote_h2d_device_ids.
     * @param[out] devIds The vector of device ids.
     * @return Status of the call.
     */
    Status GetWorkerDeviceIds(std::vector<int32_t> *devIds = nullptr);

    /**
     * @brief Get the root info for communicator connection.
     * @param[in] key The client uuid for the connection.
     * @param[out] p2pRootInfo The p2p root info for communicator connection.
     * @return Status of the call.
     */
    Status P2PGetRootInfo(const std::string &key, RemoteH2DRootInfoPb *p2pRootInfo);

    /**
     * @brief Fill the segment info for device side import purposes.
     * @param[in] segLen The segment size.
     * @param[in] segDataOffset The actual data starting offset.
     * @param[in] key The segment address as the key.
     * @param[out] segmentPb The segment protobuf to be filled.
     * @param[in] devId Specify the device id to execute on.
     * @return Status of the call.
     */
    Status FillSegmentInfo(uint64_t segLen, uint64_t segDataOffset, uint64_t key, RemoteHostSegmentPb &segmentPb,
                           int32_t devId);

    /**
     * @brief Fill the data info for device side import purposes.
     * @param[in] dataPtr data info starting address
     * @param[out] dataIntoPb The data info protobuf to be filled.
     * @return Status of the call.
     */
    Status FillDataInfo(uint64_t *dataPtr, RemoteH2DDataInfoPb &dataInfoPb);

    /**
     * @brief Establish communicator connection, in blocking manner.
     * @param[in] key The key for communicator map, it is the host root info for client side, and is client uuid for
     * remote host side.
     * @param[in] p2pRootInfo The root info for the connection.
     * @param[in] kind The p2p connection direction.
     * @param[out] p2pComm The p2p comm and stream for actual data operation if applicable.
     * @param[in] devId Optional. Specify the device id to execute on.
     * @param[in] threadPool Optional. Thread pool to handle async connection.
     * @return Status of the call.
     */
    Status P2PCommInitRootInfo(const std::string &key, const RemoteH2DRootInfoPb &p2pRootInfo, P2pKind kind,
                               std::shared_ptr<RemoteH2DContext> &p2pComm, int32_t devId = -1,
                               std::shared_ptr<ThreadPool> threadPool = nullptr);

    /**
     * @brief Register host side memory (shared memory) to NPU device as segment.
     * @param[in] data The memory address.
     * @param[in] dataSize The size of the memory segment.
     * @return Status of the call.
     */
    Status RegisterHostMemory(void *data, uint64_t dataSize);

    /**
     * @brief Initialization helper.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Uninitialization helper.
     * @return Status of the call.
     */
    Status Uninit();

    /**
     * @brief Helper function to import the segment.
     * @param[in] seg The memory address.
     * @return Status of the call.
     */
    Status ImportHostSegment(const RemoteHostSegmentPb &seg);

    /**
     * @brief Batch scatter entries from host memory.
     * @param[in] entries The array of entries to scatter.
     * @param[in] size The number of entries.
     * @param[in] p2pComm The p2p communicator and stream.
     */
    Status ScatterBatch(P2pScatterEntry *entries, uint32_t size, std::shared_ptr<RemoteH2DContext> p2pComm);
    void AfterFork();

private:
    RemoteH2DManager();
    Status HandleConnection(const std::string &key, const RemoteH2DRootInfoPb &p2pRootInfo, P2pKind kind,
                            std::shared_ptr<RemoteH2DContext> &p2pComm, int32_t devId);

    // Root info string to p2p communicator mapping.
    mutable std::shared_timed_mutex communicatorMutex_;
    std::unique_ptr<CommunicatorMap> communicatorMap_;
    std::string commId_;
    mutable std::shared_timed_mutex segmentMutex_;
    // Memory address to host segment mapping.
    std::unique_ptr<HostSegmentMap> hostSegmentMap_;
    // Segment info to imported remote segment mapping.
    std::unique_ptr<RemoteSegmentMap> remoteSegmentMap_;
    // List of all device ids the worker is allowed to use
    std::vector<int32_t> workerDeviceIds_;
    // Whether or not RemoteH2D is enabled
    static bool enableRemoteH2D_;
};
}  // namespace datasystem

#endif
