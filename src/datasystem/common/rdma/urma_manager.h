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
 * Description: Urma manager for urma context, jfce, jfs, jfr, jfc queues, etc.
 */
#ifndef DATASYSTEM_COMMON_RPC_URMA_MANAGER_H
#define DATASYSTEM_COMMON_RPC_URMA_MANAGER_H

#include <csignal>
#include <memory>
#include <thread>
#include <unordered_map>
#include <tbb/concurrent_hash_map.h>

#include <ub/umdk/urma/urma_api.h>
#ifdef URMA_OVER_UB
#include <ub/umdk/urma/urma_ubagg.h>
#endif

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/urma_info.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/lock_map.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/utils/status.h"

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

#define MAX_POLL_JFC_TRY_CNT 10
class Segment {
public:
    /**
     * @brief Create a new Segment object.
     */
    Segment() : segment_(nullptr), local_(true) {};
    ~Segment();

    /**
     * @brief Sets segment object
     * @param[in] seg target segment object
     * @param[in] local if local no need to unimport the segment
     * @return Status of the call.
     */
    void Set(urma_target_seg_t *seg, bool local);

    /**
     * @brief Sets all Jfrs for the device
     * @param[in] jetties list of imported jfs
     * @return Status of the call.
     */
    void Clear();
    custom_unique_ptr<urma_target_seg_t> segment_;
    bool local_;
};

using SegmentMap = LockMap<uint64_t, Segment>;

class RemoteDevice {
public:
    /**
     * @brief Create a new RemoteDevice object.
     */
    RemoteDevice()
    {
        remoteSegments_ = std::make_unique<SegmentMap>();
    };

    ~RemoteDevice();

    /**
     * @brief Sets all Jfrs for the device
     * @param[in] jetties list of imported jfs
     * @return Status of the call.
     */
    void SetJfrs(std::vector<urma_target_jetty_t *> &jetties);

    /**
     * @brief Get remote segment from the device
     * @param[in] segVa The remote segment address
     * @param[out] constAccessor Accessor in segment table
     * @return Status of the call.
     */
    Status GetRemoteSeg(uint64_t segVa, SegmentMap::ConstAccessor &constAccessor);

    /**
     * @brief Import remote segment and keep record in the device
     * @param[in] urmaContext The urma context
     * @param[in] UrmaImportSegmentPb Pb with remote segment info
     * @return Status of the call.
     */
    Status ImportRemoteSeg(urma_context_t *urmaContext, const UrmaImportSegmentPb &urmaInfo);

    /**
     * @brief Unimport a remote segment
     * @param[in] segmentAddress segment address
     * @return Status of the call.
     */
    Status UnimportRemoteSeg(uint64_t segmentAddress);

    /**
     * @brief Clears all remote Jfrs
     */
    void Clear();
    UrmaJfrInfo urmaInfo_;
    std::vector<custom_unique_ptr<urma_target_jetty_t>> importJfrs_;
    std::unique_ptr<SegmentMap> remoteSegments_;
};

using RemoteDeviceMap = LockMap<std::string, RemoteDevice>;

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
    Status wait_for(std::chrono::milliseconds timeout)
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
    void notify_all()
    {
        std::unique_lock<std::mutex> lock(eventMutex_);
        ready_ = true;
        cv.notify_all();
    }

    /**
     * @brief Sets the event status as failed
     */
    void set_failed()
    {
        failed_ = true;
    }

    /**
     * @brief Checks the event status
     */
    bool is_failed()
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
using TbbEventMap = tbb::concurrent_hash_map<uint64_t, std::shared_ptr<Event>>;

class UrmaManager {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Reference of UrmaManager
     */
    static UrmaManager &Instance();

    ~UrmaManager();

    /**
     * @brief Init a Urma device
     * @param[in] hostport
     * @return Status of the call.
     */
    Status Init(const HostPort &hostport);

    /**
     * @brief Check if Urma worker flag is set
     * @return True if flag is set, else false
     */
    static bool IsUrmaEnabled()
    {
        return FLAGS_enable_urma;
    };

    /**
     * @brief Check we should register whole arena upfront
     * @return True if flag is set, else false
     */
    static bool IsRegisterWholeArenaEnabled()
    {
        return FLAGS_urma_register_whole_arena;
    }

    /**
     * @brief Check we should use event mode for interrupts
     * @return True if flag is set, else false
     */
    static bool IsEventModeEnabled();

    /**
     * @brief Get EID of the device
     * @return EID as String
     */
    std::string GetEid();

    /**
     * @brief Get uasid of the device
     * @return uasid as string
     */
    uint64_t GetUasid();

    /**
     * @brief Get ids of all JFRs
     * @return list of JFR ids
     */
    std::vector<uint32_t> GetJfrIds();

    /**
     * @brief Register segment
     * @param[in] segAddress Starting address of the segment
     * @param[in] segSize Size of the segment
     * @return Status of the call.
     */
    Status RegisterSegment(const uint64_t &segAddress, const uint64_t &segSize);

    /**
     * @brief Fill segment info into request
     * @param[out] handshakeReq The protobuf to fill with segment info
     * @return Status of the call.
     */
    Status GetSegmentInfo(UrmaHandshakeReqPb &handshakeReq);

    /**
     * @brief Import segment info from request
     * @param[in] handshakeReq The protobuf to import segment info from
     * @return Status of the call.
     */
    Status ImportRemoteInfo(const UrmaHandshakeReqPb &req);

    /**
     * @brief Does a RDMA write to remote worker memory location
     * 1. Registers the segment if address is not already registered
     * 2. Imports remote segment
     * 3. does a urma write
     * @param[in] UrmaRemoteAddrPb Protobuf contians remote host address, remote urma segment address and data offset
     * @param[in] localSegAddress Starting address of the segment (e.g. Arena start address)
     * @param[in] localSegSize Total size of the segment (e.g. Arena size)
     * @param[in] localObjectAddress Object address
     * @param[in] readOffset Offset in the object to read
     * @param[in] readSize Size of the object
     * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of object)
     * @param[in] blocking Whether to blocking wait for the urma_write to finish.
     * @param[out] keys The new request id to wait for if not blocking.
     * @return Status of the call.
     */
    Status UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                            const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                            const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                            bool blocking, std::vector<uint64_t> &keys);

    /**
     * @brief Does a RDMA read from remote worker memory location
     * 1. Registers the segment if address is not already registered
     * 2. Imports remote segment
     * 3. does a urma read
     * @param[in] UrmaRemoteAddrPb Protobuf contians remote host address, remote urma segment address and data offset
     * @param[in] localSegAddress Starting address of the segment (e.g. Arena start address)
     * @param[in] localSegSize Total size of the segment (e.g. Arena size)
     * @param[in] localObjectAddress Object address
     * @param[in] dataSize Size of the object
     * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of object)
     * @param[out] keys The new request id to wait for if not blocking.
     * @return Status of the call.
     */
    Status UrmaRead(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress, const uint64_t &localSegSize,
                    const uint64_t &localObjectAddress, const uint64_t &dataSize, const uint64_t &metaDataSize,
                    std::vector<uint64_t> &keys);

    /**
     * @brief Trigger UrmaManager logic to import segment and write payload, without memcopy.
     * @param[in] RemoteSegInfo  The remote destination address info of data.
     * @param[in] LocalSgeInfo   The local source address info of data.
     * @param[in] readOffset Offset in the object to read.
     * @param[in] blocking Whether to blocking wait for the urma_write to finish.
     * @param[out] eventKeys The new request id to wait for if not blocking.
     * @return Status of the call.
     */
    Status UrmaGatherWrite(const RemoteSegInfo &remoteInfo, const std::vector<LocalSgeInfo> &objInfos, bool blocking,
                           std::vector<uint64_t> &eventKeys);

    /**
     * @brief Remove Remote Device and all associated segments
     * @param[in] remoteAddress Remote Worker Address
     * @return Status of the call.
     */
    Status RemoveRemoteDevice(const HostPort &remoteAddress);

    /**
     * @brief Register remote jfr
     * @param[in] urmaInfo local urma device info
     * @return Status of the call
     */
    Status ImportRemoteJfr(const UrmaJfrInfo &urmaInfo);

    /**
     * @brief Import segment
     * @param[in] remoteSegment Remote segment information
     * @return Urma imported target segment
     */
    urma_target_seg_t *ImportSegment(urma_seg_t &remoteSegment);

    /**
     * @brief Urma write operation waits on the CV to check completion status
     * @param[in] requestId unique id for the urma request (passed as user_ctx in urma_write)
     * @param[in] timeoutMs timeout waiting for the request to end
     * @return Status of the call.
     */
    Status WaitToFinish(uint64_t requestId, int64_t timeoutMs);

    /**
     * @brief Converts Urma device eid to string
     * @param[in] eid Urma device eid object
     * @return String
     */
    static std::string EidToStr(const urma_eid_t &eid)
    {
        return std::string(reinterpret_cast<const char *>(eid.raw), URMA_EID_SIZE);
    }

    /**
     * @brief Converts a valid string to Urma Eid
     * @param[in] eid Urma device eid in string
     * @param[out] out Urma device eid object
     * @return String
     */
    static Status StrToEid(const std::string &eid, urma_eid_t &out);

    /**
     * @brief Exchange Jfr for URMA purposes.
     * @param[in] req Urma handshake request.
     * @param[out] rsp Urma handshake response.
     * @return Status of the call.
     */
    Status ExchangeJfr(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp);

    const UrmaJfrInfo &GetLocalUrmaInfo()
    {
        return localUrmaInfo_;
    }

    /**
     * @brief Check if the connection is stable.
     * @param[in] hostAddress The dst port address.
     * @param[in] instanceId The unqiue instance uuid from dst port.
     * @return Status of the connection.
     */
    Status CheckUrmaConnectionStable(const std::string &hostAddress, const std::string &instanceId = "");

    /**
     * @brief Get local transport unique instance id.
     * @param[out] instanceId The local instance uuid.
     */
    void GetLocalInstanceId(std::string &instanceId)
    {
        instanceId = localUrmaInfo_.uniqueInstanceId;
    }

private:
    UrmaManager();

    /**
     * @brief Initialize urma.
     * @return Status of the call.
     */
    Status UrmaInit();

    /**
     * @brief Uninitialize urma.
     * @return Status of the call.
     */
    Status UrmaUninit();

    /**
     * @brief Register log for urma.
     * @return Status of the call.
     */
    Status RegisterUrmaLog();

    /**
     * @brief UnRegister log for urma.
     * @return Status of the call.
     */
    Status UnRegisterUrmaLog();

    /**
     * @brief Gets Urma device object
     * @param[in] deviceName name of the interface in local machine
     * @param[out] urmaDevice Urma device object
     * @return Status of the call.
     */
    Status UrmaGetDeviceByName(const std::string &deviceName, urma_device_t *&urmaDevice);

    /**
     * @brief Gets device attributes into urmaDeviceAttribute_
     * @param[in] urmaDevice Urma device object
     * @return Status of the call.
     */
    Status UrmaQueryDevice(urma_device_t *&urmaDevice);

    /**
     * @brief Gets list of all Eids in local device
     * @param[in] urmaDevice local Urma device
     * @param[out] eidList list of eids
     * @param[out] eidCount eid count
     * @return Status of the call.
     */
    Status UrmaGetEidList(urma_device_t *&urmaDevice, urma_eid_info_t *&eidList, uint32_t &eidCount);

    /**
     * @brief Gets index of local eid list
     * @param[in] urmaDevice local Urma device
     * @param[out] eidIndex eid index
     * @return Status of the call.
     */
    Status GetEidIndex(urma_device_t *&urmaDevice, int &eidIndex);

    /**
     * @brief Creates Urma context
     * @param[in] urmaDevice local Urma device
     * @param[in] eidIndex eid index of the device
     * @return Status of the call.
     */
    Status UrmaCreateContext(urma_device_t *&urmaDevice, uint32_t eidIndex);

    /**
     * @brief Deletes Urma context object
     * @return Status of the call.
     */
    Status UrmaDeleteContext();

    /**
     * @brief Creates Jfce object
     * @return Status of the call.
     */
    Status UrmaCreateJfce();

    /**
     * @brief Deletes Jfce object
     * @return Status of the call.
     */
    Status UrmaDeleteJfce();

    /**
     * @brief Creates a JFC object
     * @param[out] out jfc wrapped in a unique pointer with custom deleter
     * @return Status of the call.
     */
    Status UrmaCreateJfc(custom_unique_ptr<urma_jfc_t> &out);

    /**
     * @brief Resets Event handling of Jfc
     * @param[in] jfc jfc wrapped in a unique pointer with custom deleter
     * @return Status of the call.
     */
    static Status UrmaRearmJfc(const custom_unique_ptr<urma_jfc_t> &jfc);

    /**
     * @brief Create a Jfs and sets jfc for completion queue
     * @param[in] jfc jfc wrapped in a unique pointer with custom deleter
     * @param[out] out jfs wrapped in a unique pointer with custom deleter
     * @return Status of the call
     */
    Status UrmaCreateJfs(const custom_unique_ptr<urma_jfc_t> &jfc, custom_unique_ptr<urma_jfs_t> &out);

    /**
     * @brief Create a Jfr and sets jfc for completion queue
     * @param[in] jfc jfc wrapped in a unique pointer with custom deleter
     * @param[out] out jfr wrapped in a unique pointer with custom deleter
     * @return Status of the call
     */
    Status UrmaCreateJfr(const custom_unique_ptr<urma_jfc_t> &jfc, custom_unique_ptr<urma_jfr_t> &out);

    /**
     * @brief Continously running Event handler thread that polls JFC
     * @return Status of the call.
     */
    Status ServerEventHandleThreadMain();

    /**
     * @brief Polls the Jfc(s) for maxTryCount times
     * @param[in] maxTryCount number of times to try polling
     * @param[out] successCompletedReqs list of successful request ids
     * @param[out] failedCompletedReqs list of failed request ids
     * @param[in] numPollCRS number of jfcs to poll
     * @return Status of the call.
     */
    Status PollJfcWait(const custom_unique_ptr<urma_jfc_t> &jfc, const uint64_t maxTryCount,
                       std::vector<uint64_t> &successCompletedReqs, std::vector<uint64_t> &failedCompletedReqs,
                       const uint64_t numPollCRS = 1);

    /**
     * @brief Given list of CRs, fills successful req and failed req lists
     * @param[in] completeRecords list of CRs from polling
     * @param[in] count number of CRs in the list
     * @param[out] successCompletedReqs list of successful request ids
     * @param[out] failedCompletedReqs list of failed request ids
     * @return Status of the call.
     */
    Status CheckCompletionRecordStatus(urma_cr_t completeRecords[], int count,
                                       std::vector<uint64_t> &successCompletedReqs,
                                       std::vector<uint64_t> &failedCompletedReqs);

    /**
     * @brief Register segment
     * @param[in] segAddress Starting address of the segment
     * @param[in] segSize Size of the segment
     * @param[out] constAccessor const accessor to Segment map
     * @return Status of the call.
     */
    Status GetOrRegisterSegment(const uint64_t &segAddress, const uint64_t &segSize,
                                SegmentMap::ConstAccessor &constAccessor);

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

    Status InitLocalUrmaInfo(const HostPort &hostport);

    // Polling thread
    std::unique_ptr<std::thread> serverEventThread_{ nullptr };

    urma_device_attr_t urmaDeviceAttribute_;
    urma_context_t *urmaContext_ = nullptr;
    urma_jfce_t *urmaJfce_ = nullptr;
    custom_unique_ptr<urma_jfc_t> urmaJfc_ = nullptr;
    std::vector<custom_unique_ptr<urma_jfs_t>> urmaJfsVec_;
    std::vector<custom_unique_ptr<urma_jfr_t>> urmaJfrVec_;
    urma_token_t urmaToken_ = { 0 };
    std::atomic<uint64_t> requestId_{ 0 };
    uint32_t JETTY_SIZE_ = 256;
    urma_reg_seg_flag_t registerSegmentFlag_;
    urma_import_seg_flag_t importSegmentFlag_;
    UrmaJfrInfo localUrmaInfo_;

    // protect for segment maps.
    mutable std::shared_timed_mutex localMapMutex_;
    mutable std::shared_timed_mutex remoteMapMutex_;
    // Memory address to local segment mapping.
    std::unique_ptr<SegmentMap> localSegmentMap_;
    // Eid to segment maps mapping for remote jfr and segment.
    std::unique_ptr<RemoteDeviceMap> remoteDeviceMap_;
    TbbEventMap tbbEventMap_;
    std::unordered_set<uint64_t> finishedRequests_;
    std::unordered_set<uint64_t> failedRequests_;
    std::atomic<bool> serverStop_{ false };
    urma_log_cb_t urmaLogCallback_;
};

}  // namespace datasystem
#endif
