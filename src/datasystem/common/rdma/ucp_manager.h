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

/**
 * Description: UCX-UCP manager for ucp context, ucp worker, ucp endpoint, etc.
 */
#ifndef DATASYSTEM_COMMON_RPC_RDMA_MANAGER_H
#define DATASYSTEM_COMMON_RPC_RDMA_MANAGER_H

#include <csignal>
#include <memory>
#include <thread>
#include <unordered_map>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/util/lock_map.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/utils/status.h"

#include "datasystem/common/rdma/ucp_segment.h"
#include "datasystem/common/rdma/ucp_dlopen_util.h"

DS_DECLARE_bool(enable_rdma);
DS_DECLARE_bool(rdma_register_whole_arena);

namespace datasystem {
class UcpWorkerPool;
using TbbInstanceMap = tbb::concurrent_hash_map<std::string, std::string>;
using EventMap = LockMap<uint64_t, std::shared_ptr<Event>>;

class UcpManager {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Reference of UcpManager
     */
    static UcpManager &Instance();

    ~UcpManager();

    /**
     * @brief Init a rdma device
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Check if rdma worker flag is set
     * @return True if flag is set, else false
     */
    static bool IsUcpEnabled();

    /**
     * @brief Check we should register whole arena upfront
     * @return True if flag is set, else false
     */
    static bool IsRegisterWholeArenaEnabled();

    /**
     * @brief Register segment
     * @param[in] segAddress Starting address of the segment
     * @param[in] segSize Size of the segment
     * @return Status of the call.
     */
    Status RegisterSegment(const uint64_t &segAddress, const uint64_t &segSize);

    /**
     * @brief Fill in ucp info for object data owner to ucp put
     * @param[in] segAddress Starting address of the segment.
     * @param[in] dataOffset The memory offset of the object
     * @param[in] srcIpAddr The ip address of remote data owner
     * @param[out] ucpInfo Ucp info that needs to be sent to remote data owner
     * @return Status of the call
     */
    Status FillUcpInfoImpl(uint64_t segAddress, uint64_t dataOffset, const std::string &srcIpAddr,
                           UcpRemoteInfoPb &ucpInfo);

    /**
     * @brief Does a RDMA write to remote worker memory location
     * 1. Registers the segment if address is not already registered
     * 2. Imports remote segment
     * 3. does a Ucp write
     * @param[in] ucpInfo Protobuf contians remote worker UCP info
     * @param[in] localObjectAddress Object address
     * @param[in] readOffset Offset in the object to read
     * @param[in] readSize Size of the object
     * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of
     * object)
     * @param[in] blocking Whether to blocking wait for the ucp_put_nbx to finish
     * @param[out] keys The new request id to wait for if not blocking
     * @return Status of the call
     */
    Status UcpPutPayload(const UcpRemoteInfoPb &ucpInfo, const uint64_t &localObjectAddress, const uint64_t &readOffset,
                         const uint64_t &readSize, const uint64_t &metaDataSize, bool blocking,
                         std::vector<uint64_t> &keys);

    /**
     * @brief Trigger UcpManager logic to import segment and write payload, without memcopy.
     * @param[in] ucpInfo Protobuf contains remote worker UCP info.
     * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of object).
     * @param[in] objInfos The local source address info of data.
     * @param[in] blocking Whether to blocking wait for the ucp_put_nbx to finish.
     * @param[out] eventKeys The new request id to wait for if not blocking.
     * @return Status of the call.
     */
    Status UcpGatherPut(const UcpRemoteInfoPb &ucpInfo, uint64_t metaDataSize,
                        const std::vector<LocalSgeInfo> &objInfos, bool blocking, std::vector<uint64_t> &eventKeys);

    /**
     * @brief Remove Remote Endpoint and all associated segments
     * @param[in] remoteAddress Remote Worker Address
     * @return Status of the call.
     */
    Status RemoveEndpoint(const HostPort &remoteAddress);

    /**
     * @brief Ucp write operation waits on the CV to check completion status
     * @param[in] requestId unique id for the rdma request
     * @param[in] timeoutMs timeout waiting for the request to end
     * @return Status of the call.
     */
    Status WaitToFinish(uint64_t requestId, int64_t timeoutMs);

    /**
     * @brief Get the network address of the receiving worker.
     * @param[in] ipAddr IP address in string format, e.g., "192.168.1.100"
     * @return string containing the network address
     */
    std::string GetRecvWorkerAddress(const std::string &ipAddr);

    /**
     * @brief Inserts a successful event.
     * @param[in] requestId a unique identifier for the request
     */
    virtual void InsertSuccessfulEvent(uint64_t requestId);

    /**
     * @brief Inserts a failed event.
     * @param[in] requestId a unique identifier for the request
     */
    virtual void InsertFailedEvent(uint64_t requestId);

    /**
     * @brief Get local transport unique instance id.
     * @param[out] instanceId The local instance uuid.
     */
    void GetLocalInstanceId(std::string &instanceId)
    {
        instanceId = uniqueInstanceId_;
    }

    /**
     * @brief Check if the connection is stable.
     * @param[in] hostAddress The dst port address.
     * @param[in] instanceId The unqiue instance uuid from dst port.
     * @return Status of the connection.
     */
    Status CheckUcpConnectionStable(const std::string &hostAddress, const std::string &instanceId = "");

private:
    UcpManager();

    /**
     * @brief Create a UCP context.
     * @return Status of the call.
     */
    Status UcpCreateContext();

    /**
     * @brief Deletes Ucp context object
     * @return Status of the call.
     */
    Status UcpDeleteContext();

    /**
     * @brief Create a UCP worker pool.
     * @return Status of the call.
     */
    Status UcpCreateWorkerPool();

    /**
     * @brief Register segment
     * @param[in] segAddress Starting address of the segment
     * @param[in] segSize Size of the segment
     * @param[out] constAccessor const accessor to Segment map
     * @return Status of the call.
     */
    Status GetOrRegisterSegment(const uint64_t &segAddress, const uint64_t &segSize,
                                UcpSegmentMap::ConstAccessor &constAccessor);

    /**
     * @brief Gets event object of request id
     * @param[in] requestId unique id of the rdma request
     * @param[out] event event object for the request
     * @return Status of the call.
     */
    Status GetEvent(uint64_t requestId, std::shared_ptr<Event> &event);

    /**
     * @brief Create Event object for the request
     * @param[in] requestId unique id for the rdma request
     * @param[out] event event object for the request
     * @return Status of the call.
     */
    Status CreateEvent(uint64_t requestId, std::shared_ptr<Event> &event);

    /**
     * @brief Deletes the Event object for the request
     * @param[in] requestId unique id for the rdma request
     * @return Status of the call.
     */
    void DeleteEvent(uint64_t requestId);

    ucp_context_h ucpContext_ = nullptr;
    std::unique_ptr<UcpWorkerPool> workerPool_;
    std::atomic<uint64_t> requestId_{ 0 };

    // Memory address to local segment mapping.
    std::unique_ptr<UcpSegmentMap> localSegmentMap_;
    mutable std::shared_timed_mutex localMapMutex_;
    mutable std::shared_timed_mutex eventMapMutex_;
    std::unique_ptr<EventMap> eventMap_;
    TbbInstanceMap instanceTable_;
    std::string uniqueInstanceId_;
};

}  // namespace datasystem
#endif
