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

#include <ucp/api/ucp.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/util/lock_map.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(enable_rdma);
DS_DECLARE_bool(rdma_register_whole_arena);

namespace datasystem {
template <typename T>
using custom_unique_ptr = std::unique_ptr<T, std::function<void(T *)>>;

template <typename T>
custom_unique_ptr<T> MakeCustomUnique(T *p, std::function<void(T *)> custom_delete)
{
    if (p) {
        return custom_unique_ptr<T>(p, custom_delete);
    } else {
        LOG(WARNING) << "Input pointer is null";
        return nullptr;
    }
}

class Event {
public:
    /**
     * @brief Create a new Event object.
     */
    explicit Event(uint64_t requestId) : requestId_(requestId), ready_(false)
    {
    }

    /**
     * @brief Wait on event until timeout or someone notify
     * @param[in] timeout time in milliseconds to wait
     * @return Status of the call.
     */
    Status waitFor(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(eventMutex_);
        bool gotNotification = cv.wait_for(lock, timeout, [this] { return ready_; });
        if (!gotNotification && !ready_) {
            // Return timeout
            RETURN_STATUS_LOG_ERROR(K_RPC_DEADLINE_EXCEEDED,
                                    FormatString("timedout waiting for request: %d", requestId_));
        }
        return Status::OK();
    }

    /**
     * @brief Notify all threads that are waiting for the event
     */
    void notifyAll()
    {
        std::unique_lock<std::mutex> lock(eventMutex_);
        ready_ = true;
        cv.notify_all();
    }

    /**
     * @brief Sets the event status as failed
     */
    void setFailed()
    {
        failed_ = true;
    }

    /**
     * @brief Checks the event status
     */
    bool isFailed()
    {
        return failed_;
    }

private:
    std::condition_variable cv;
    mutable std::mutex eventMutex_;
    uint64_t requestId_;
    bool ready_{ false };
    bool failed_{ false };
};

using EventMap = LockMap<uint64_t, std::shared_ptr<Event>>;

class UcpSegment {
public:
    /**
     * @brief Create a new UcpSegment object.
     */
    UcpSegment(){};
    ~UcpSegment(){};

    /**
     * @brief Sets UcpSegment object
     * @param[in] seg target UcpSegment object
     * @param[in] local if local no need to unimport the UcpSegment
     * @return Status of the call.
     */
    void Set(ucp_mem_h *seg, bool local);

    void Clear();

private:
    custom_unique_ptr<ucp_mem_h> segment_;
    bool local_;
};

using UcpSegmentMap = LockMap<uint64_t, UcpSegment>;

class UcpEndpoint {
public:
    /**
     * @brief Create a new UcpEndpoint object.
     */
    UcpEndpoint(){};

    ~UcpEndpoint(){};
};

using UcpEndpointMap = LockMap<std::string, UcpEndpoint>;

class UcpWorker {
public:
    /**
     * @brief Create a new UcpWorker object.
     */
    UcpWorker(){};

    ~UcpWorker();

    /**
     * @brief Get remote UcpSegment or import remote UcpSegment from the device
     * @param[in] remoteEndpoint remote ucp endpoint
     * @param[in] localSegAddress Starting address of the segment (e.g. Arena
     * start address)
     * @param[in] localSegSize Total size of the segment (e.g. Arena size)
     * @param[in] localObjectAddress Object address
     * @param[in] readOffset Offset in the object to read
     * @param[in] readSize Size of the object
     * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of
     * object)
     * @param[in] blocking Whether to blocking wait for the ucp_put_nbx to finish.
     * @return Status of the call.
     */
    Status PutPayloadToRemoteEndpoint(custom_unique_ptr<UcpEndpoint> &remoteEndpoint, const uint64_t &localSegAddress,
                                      const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                      const uint64_t &readOffset, const uint64_t &readSize,
                                      const uint64_t &metaDataSize, bool blocking);

    /**
     * @brief bind remote endpoint to the ucp worker, stored in remoteEndpointVec_
     * @param[in] remoteEndpoint ucp endpoint ptr
     * @return Status of the call.
     */
    Status BindRemoteEndpoint(custom_unique_ptr<UcpEndpoint> remoteEndpoint);

private:
    std::vector<custom_unique_ptr<UcpEndpoint>> remoteEndpointVec_;
};

class UcpManager {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Reference of UcpManager
     */
    static UcpManager &Instance();

    ~UcpManager();

    /**
     * @brief Init a Rdma device
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Check if Ucp Rdma worker flag is set
     * @return True if flag is set, else false
     */
    static bool IsUcpEnabled()
    {
        return FLAGS_enable_rdma;
    };

    /**
     * @brief Check we should register whole arena upfront
     * @return True if flag is set, else false
     */
    static bool IsRegisterWholeArenaEnabled()
    {
        return FLAGS_rdma_register_whole_arena;
    }

    /**
     * @brief Check we should use event mode for interrupts
     * @return True if flag is set, else false
     */
    static bool IsEventModeEnabled();

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
     * @brief UCP RDMA write object data to remote worker memory location
     * 1. Choose a UcpWorker to build/reuse the UcpEndpoint
     * 2. Prepare the obj data, including address offsets and data chunks
     * 3. does a ucp put
     * @param[in] ucpInfo Protobuf contians remote worker UCP RDMA info.
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
     * @brief Remove Remote Endpoint and all associated segments
     * @param[in] remoteAddress Remote Worker Address
     * @return Status of the call.
     */
    Status RemoveEndpoint(const HostPort &remoteAddress);

    /**
     * @brief Ucp write operation waits on the CV to check completion status
     * @param[in] requestId unique id for the urma request (passed as user_ctx in
     * urma_write)
     * @param[in] timeoutMs timeout waiting for the request to end
     * @return Status of the call.
     */
    Status WaitToFinish(uint64_t requestId, int64_t timeoutMs);

private:
    UcpManager();

    /**
     * @brief Initialize ucp.
     * @return Status of the call.
     */
    Status UcpInit();

    /**
     * @brief Uninitialize ucp.
     * @return Status of the call.
     */
    Status UcpUninit();

    /**
     * @brief Creates Ucp context
     * @param[in] urmaDevice local Urma device
     * @param[in] eidIndex eid index of the device
     * @return Status of the call.
     */
    Status UcpCreateContext(ucp_params_t *params, ucp_config_t *config);

    /**
     * @brief Deletes Ucp context object
     * @return Status of the call.
     */
    Status UcpDeleteContext();

    /**
     * @brief Continously running Event handler thread that polls JFC
     * @return Status of the call.
     */
    Status ServerEventHandleThreadMain();

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
     * @brief UnImport segment
     * @param[in] remoteAddress Remote worker address
     * @param[in] segmentAddress Segment start address
     * @return Status of the call
     */
    Status UnimportSegment(const HostPort &remoteAddress, uint64_t segmentAddress);

    /**
     * @brief Stops the polling thread
     * @return Status of the call.
     */
    Status Stop();

    /**
     * @brief Checks if waiting requests are completed and notifys them
     * @return Status of the call.
     */
    Status CheckAndNotify();

    /**
     * @brief Gets event object of request id
     * @param[in] requestId unique id of the Urma request
     * @param[out] event event object for the request
     * @return Status of the call.
     */
    Status GetEvent(uint64_t requestId, std::shared_ptr<Event> &event);

    /**
     * @brief Create Event object for the request
     * @param[in] requestId unique id for the Urma request
     * @param[out] event event object for the request
     * @return Status of the call.
     */
    Status CreateEvent(uint64_t requestId, std::shared_ptr<Event> &event);

    /**
     * @brief Deletes the Event object for the request
     * @param[in] requestId unique id for the Urma request
     * @return Status of the call.
     */
    void DeleteEvent(uint64_t requestId);

    // Polling thread
    std::unique_ptr<std::thread> serverEventThread_{ nullptr };

    ucp_context_h *ucpContext_ = nullptr;
    // ip -> ucp endpoint
    std::unordered_map<std::string, custom_unique_ptr<UcpEndpoint>> endPointCache_;
    std::vector<custom_unique_ptr<UcpWorker>> ucpWorkerVec_;
    std::atomic<uint64_t> requestId_{ 0 };

    std::unique_ptr<UcpSegmentMap> localSegmentMap_;
    // Eid to segment maps mapping for remote segment.
    std::unique_ptr<UcpEndpointMap> remoteEndpointMap_;
    mutable std::shared_timed_mutex eventMapMutex_;
    std::unordered_map<uint64_t, std::shared_ptr<Event>> eventMap_;
    std::unordered_set<uint64_t> finishedRequests_;
    std::unordered_set<uint64_t> failedRequests_;
    std::atomic<bool> serverStop_{ false };
};

}  // namespace datasystem
#endif
