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

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/rdma/urma_async_event_handler.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rdma/urma_info.h"
#include "datasystem/common/rdma/urma_resource.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {

using datasystem::memory::Allocator;
using datasystem::memory::AllocatorFuncRegister;
using AllocateType = datasystem::memory::CacheType;
using ArenaGroup = datasystem::memory::ArenaGroup;

const std::string DEFAULT_TENANTID = "";
const std::string UB_MAX_GET_DATA_SIZE = "UB_MAX_GET_DATA_SIZE";
const std::string UB_MAX_SET_BUFFER_SIZE = "UB_MAX_SET_BUFFER_SIZE";

#define MAX_POLL_JFC_TRY_CNT 10

using TbbUrmaConnectionMap = tbb::concurrent_hash_map<std::string, std::shared_ptr<UrmaConnection>>;
using TbbEventMap = tbb::concurrent_hash_map<uint64_t, std::shared_ptr<UrmaEvent>>;
using TbbJfrMap = tbb::concurrent_hash_map<std::string, std::unique_ptr<UrmaJfr>>;

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
     * @brief Initialize memory buffer for client side.
     * @return Status of the call.
     */
    Status InitMemoryBufferPool();

    class BufferHandle {
    public:
        BufferHandle() = default;

        BufferHandle(std::shared_ptr<ShmUnit> shmUnit, void *basePtr, uint64_t segmentSize)
            : shmUnit_(shmUnit), basePtr_(basePtr), segmentSize_(segmentSize)
        {
        }

        BufferHandle(const BufferHandle &) = delete;
        BufferHandle &operator=(const BufferHandle &) = delete;

        ~BufferHandle()
        {
            Release();
        }

        uint32_t GetOffset() const
        {
            return shmUnit_->GetOffset();
        }

        uint8_t GetNumaId() const
        {
            return shmUnit_ == nullptr ? INVALID_NUMA_ID : shmUnit_->GetNumaId();
        }

        void *GetPointer() const
        {
            return shmUnit_->GetPointer();
        }

        uint64_t GetSegmentAddress() const
        {
            return reinterpret_cast<uint64_t>(basePtr_);
        }

        uint64_t GetSegmentSize() const
        {
            return segmentSize_;
        }

    private:
        void Release()
        {
            if (shmUnit_) {
                shmUnit_.reset();
                shmUnit_ = nullptr;
            }
        }

        std::shared_ptr<ShmUnit> shmUnit_;
        void *basePtr_ = nullptr;
        uint64_t segmentSize_ = 0;
    };

    /**
     * @brief Get a self-release memory buffer handle for client side RDMA operation.
     * @param[out] handle The memory buffer handle with offset info.
     * @param[in] size The size of the memory buffer.
     * @return Status of the call.
     */
    Status GetMemoryBufferHandle(std::shared_ptr<BufferHandle> &handle, uint64_t size);

    /**
     * @brief Get client comm-buffer details for UB Get by buffer offset.
     * @param[in] bufferOffset Offset in the client memory buffer pool.
     * @param[out] bufferPtr Pointer to the beginning of this buffer slot.
     * @param[out] bufferSize Buffer slot size.
     * @param[out] urmaInfo Remote address info used by worker-side UrmaWritePayload.
     * @return Status of the call.
     */
    Status GetMemoryBufferInfo(std::shared_ptr<UrmaManager::BufferHandle> &handler, uint8_t *&bufferPtr,
                               uint64_t &bufferSize, UrmaRemoteAddrPb &urmaInfo);

    /**
     * @brief Check if Urma worker flag is set
     * @return True if flag is set, else false
     */
    static bool IsUrmaEnabled()
    {
        return FLAGS_enable_urma;
    };

    /**
     * @brief Set the fast transport mode and client id for client process (e.g. UB Put).
     * @param[in] urmaMode The transport mode, e.g. UB.
     * @param[in] transportSize The client transport mem pool size.
     */
    static void SetClientUrmaConfig(FastTransportMode urmaMode, uint64_t transportSize);

    /**
     * @brief Get client id for current process; non-empty when set by SetClientUrmaConfig (client mode).
     * @return Client id string.
     */
    const std::string &GetClientId();

    /**
     * @brief Get the ub max get data size. Default is 32MB when no value set in environment
     * @return The max get data size
     */
    uint64_t GetUBMaxGetDataSize();

    /**
     * @brief Get the ub max set buffer size. Default is 8MB when no value set in environment
     * @return The max set buffer size per request
     */
    uint64_t GetUBMaxSetBufferSize();

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
     * @brief Get or create the local JFR used for a given connection key.
     *        Reuses existing JFR when reconnecting to the same target.
     * @param[in] key Connection key (remote address or client id).
     * @param[out] jfrId The JFR ID to publish in the handshake.
     * @return Status of the call.
     */
    Status GetOrCreateLocalJfr(const std::string &key, uint32_t &jfrId);

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
     * @brief Fill local segment info into handshake response.
     */
    Status GetSegmentInfo(UrmaHandshakeRspPb &handshakeRsp);

    /**
     * @brief Import segment info from request (responder imports initiator's segments).
     * @param[in] req The handshake request containing segment info.
     * @return Status of the call.
     */
    Status ImportRemoteInfo(const UrmaHandshakeReqPb &req);

    /**
     * @brief Does a RDMA write to remote worker memory location
     * 1. Registers the segment if address is not already registered
     * 2. Imports remote segment
     * 3. Does a urma write
     * @param[in] UrmaRemoteAddrPb Protobuf contians remote host address, remote urma segment address and data offset
     * @param[in] localSegAddress Starting address of the segment (e.g. Arena start address)
     * @param[in] localSegSize Total size of the segment (e.g. Arena size)
     * @param[in] localObjectAddress Object address
     * @param[in] readOffset Offset in the object to read
     * @param[in] readSize Size of the object
     * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of object)
     * @param[in] blocking Whether to blocking wait for the urma_write to finish.
     * @param[out] eventKeys The new request id to wait for if not blocking.
     * @param[in] waiter The optional event waiter.
     * @return Status of the call.
     */
    Status UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                            const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                            const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                            uint8_t srcChipId, uint8_t dstChipId, bool blocking, std::vector<uint64_t> &eventKeys,
                            std::shared_ptr<EventWaiter> waiter = nullptr);

    /**
     * @brief Does a RDMA read from remote worker memory location
     * 1. Registers the segment if address is not already registered
     * 2. Imports remote segment
     * 3. Does a urma read
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
     * @brief Remove the connection-side state for a remote device.
     *        The local JFR is intentionally retained for worker reconnect/false-positive recovery.
     * @param[in] deviceId The device id to remove.
     * @return Status of the call.
     */
    Status RemoveRemoteDevice(const std::string &deviceId);

    /**
     * @brief Import a remote JFR as target JFR and create a per-connection JFS (responder side).
     *        Also creates or reuses the local JFR managed separately for this remote key.
     * @param[in] urmaInfo Remote JFR metadata from handshake.
     * @param[out] localJfrId The local JFR ID to include in the handshake response.
     * @return Status of the call.
     */
    Status ImportRemoteJfr(const UrmaJfrInfo &urmaInfo, uint32_t &localJfrId);

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

    /**
     * @brief Get local URMA JFR info
     * @return local UrmaJfrInfo
     */
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

    /**
     * @brief Finalize initiator-side connection from handshake response.
     * @param[in] rsp Urma handshake response containing remote JFR and segment info.
     * @return Status of the call.
     */
    Status FinalizeOutboundConnection(const UrmaHandshakeRspPb &rsp);

    /**
     * @brief Remove all URMA state associated with a client entity.
     *        Unlike RemoveRemoteDevice, this also removes the retained local JFR.
     * @param[in] clientEntityId Client entity id used for cleanup.
     * @return Status of the call.
     */
    Status RemoveRemoteClient(ClientKey clientEntityId);

    /**
     * @brief Retrieve local segment and jfr for receiving remote sender cqe.
     * @param[in] segAddress segment start address.
     * @param[in] segSize segment size.
     * @param[in] address remote sender address.
     * @param[out] targetSeg local segment.
     * @param[out] targetJfr local jfr for remote sender.
     * @return Status.
     */
    Status GetTargetSeg(uint64_t segAddress, uint64_t segSize, const std::string &address,
                        urma_target_seg_t **targetSeg, urma_jfr_t **targetJfr);

    /**
     * @return urma request id, start from 0.
     */
    uint64_t GenerateReqId();

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
     * @brief Gets Urma effective bonding device
     * @param[out] urmaDevName Effective urma device name
     * @return Status of the call.
     */
    Status UrmaGetEffectiveDevice(std::string &urmaDevName);

    /**
     * @brief Get Urma device name and eid index for a given hostport
     * @param[in] hostport HostPort of the remote device
     * @param[out] urmaDeviceName Urma device name
     * @param[out] eidIndex Eid index to use for the device
     * @return Status of the call.
     */
    Status GetUrmaDeviceName(const HostPort &hostport, std::string &urmaDeviceName, int &eidIndex);

    /**
     * @brief Gets Urma effective device from devList
     * @param[in] urmaDevName Urma device name
     * @param[in] devList Urma device list
     * @param[in] devCount Urma device list cout
     * @return Device index.
     */
    int CompareDeviceName(const std::string &urmaDevName, urma_device_t **devList, int devCount);

    /**
     * @brief Gets Urma device object
     * @param[in] deviceName name of the interface in local machine
     * @param[out] urmaDevice Urma device object
     * @return Status of the call.
     */
    Status UrmaGetDeviceByName(const std::string &deviceName, urma_device_t *&urmaDevice);

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
     * @brief Continously running Event handler thread that polls JFC
     * @return Status of the call.
     */
    Status ServerEventHandleThreadMain();

    /**
     * @brief Polls the Jfc(s) for maxTryCount times
     * @param[in] maxTryCount number of times to try polling
     * @param[out] successCompletedReqs set of successful request ids
     * @param[out] failedCompletedReqs map of failed request ids to status codes
     * @param[in] numPollCRS number of jfcs to poll
     * @return Status of the call.
     */
    Status PollJfcWait(urma_jfc_t *jfc, const uint64_t maxTryCount, std::unordered_set<uint64_t> &successCompletedReqs,
                       std::unordered_map<uint64_t, int> &failedCompletedReqs, const uint64_t numPollCRS = 1);

    /**
     * @brief Given list of CRs, fills successful req and failed req lists
     * @param[in] completeRecords list of CRs from polling
     * @param[in] count number of CRs in the list
     * @param[out] successCompletedReqs set of successful request ids
     * @param[out] failedCompletedReqs map of failed request ids to status codes
     * @return Status of the call.
     */
    Status CheckCompletionRecordStatus(urma_cr_t completeRecords[], int count,
                                       std::unordered_set<uint64_t> &successCompletedReqs,
                                       std::unordered_map<uint64_t, int> &failedCompletedReqs);

    Status HandleUrmaEvent(uint64_t requestId, const std::shared_ptr<UrmaEvent> &event);

    /**
     * @brief Get the valid JFS currently owned by a connection.
     * @param[in] connection URMA connection.
     * @param[out] jfs Valid JFS bound to the connection.
     * @return Status of the call.
     */
    Status GetJfsFromConnection(const std::shared_ptr<UrmaConnection> &connection, std::shared_ptr<UrmaJfs> &jfs);

    /**
     * @brief Import a remote JFR as a target jetty (used by both initiator and responder).
     * @param[in] remoteInfo Remote JFR metadata containing the JFR ID to import.
     * @param[out] targetJfr Imported target JFR handle.
     * @return Status of the call.
     */
    Status ImportTargetJfr(const UrmaJfrInfo &remoteInfo, std::unique_ptr<UrmaTargetJfr> &targetJfr);

    /**
     * @brief Register segment
     * @param[in] segAddress Starting address of the segment
     * @param[in] segSize Size of the segment
     * @param[out] constAccessor const accessor to Segment map
     * @return Status of the call.
     */
    Status GetOrRegisterSegment(const uint64_t &segAddress, const uint64_t &segSize,
                                UrmaLocalSegmentMap::const_accessor &constAccessor);

    /**
     * @brief Stops the polling thread
     * @return Status of the call.
     */
    Status Stop();
    Status PerfThreadMain();

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
    Status GetEvent(uint64_t requestId, std::shared_ptr<UrmaEvent> &event);

    /**
     * @brief Create UrmaEvent object for the request.
     * @param[in] requestId Unique id for the Urma request.
     * @param[in] connection The connection associated with the request.
     * @param[in] waiter Optional event waiter.
     * @return Status of the call.
     */
    Status CreateEvent(uint64_t requestId, const std::shared_ptr<UrmaConnection> &connection,
                       const std::shared_ptr<UrmaJfs> &jfs, const std::string &remoteAddress,
                       UrmaEvent::OperationType operationType, std::shared_ptr<EventWaiter> waiter = nullptr);

    struct UrmaWriteArgs {
        std::shared_ptr<UrmaConnection> connection;
        std::shared_ptr<UrmaJfs> jfs;
        std::shared_ptr<EventWaiter> waiter;
        std::string remoteAddress;
        urma_target_jetty_t *targetJfr = nullptr;
        urma_target_seg_t *remoteSeg = nullptr;
        urma_target_seg_t *localSeg = nullptr;
        uint64_t remoteDataAddress = 0;
        uint64_t localDataAddress = 0;
        uint64_t size = 0;
        uint8_t srcChipId = INVALID_CHIP_ID;
        uint8_t dstChipId = INVALID_CHIP_ID;
    };

    Status UrmaWriteImpl(const UrmaWriteArgs &args, std::vector<uint64_t> &eventKeys);

    /**
     * @brief Deletes the UrmaEvent object for the request
     * @param[in] requestId unique id for the Urma request
     * @return Status of the call.
     */
    void DeleteEvent(uint64_t requestId);

    Status InitLocalUrmaInfo(const HostPort &hostport);
    Status RemoveRemoteResources(const std::string &connectionKey, bool removeLocalJfr);

    // Polling thread
    std::unique_ptr<std::thread> serverEventThread_{ nullptr };
    std::unique_ptr<std::thread> perfThread_{ nullptr };
    UrmaAsyncEventHandler aeHandler_;
    std::unique_ptr<UrmaResource> urmaResource_;
    std::atomic<uint64_t> requestId_{ 0 };
    urma_reg_seg_flag_t registerSegmentFlag_{};
    urma_import_seg_flag_t importSegmentFlag_{};
    UrmaJfrInfo localUrmaInfo_;
    // Local JFRs keyed by remote connection ID. JFRs are created/reused per target node
    // and persist across reconnections. Not owned by UrmaConnection.
    TbbJfrMap localJfrMap_;

    // protect for segment maps.
    mutable std::shared_timed_mutex localMapMutex_;
    mutable std::shared_timed_mutex remoteMapMutex_;
    // Memory address to local segment mapping.
    std::unique_ptr<UrmaLocalSegmentMap> localSegmentMap_;
    // Eid to segment maps mapping for remote jfr and segment.
    TbbUrmaConnectionMap urmaConnectionMap_;
    TbbEventMap tbbEventMap_;
    std::unordered_set<uint64_t> finishedRequests_;
    std::unordered_map<uint64_t, int> failedRequests_;
    std::atomic<bool> serverStop_{ false };
    urma_log_cb_t urmaLogCallback_{};

    enum InitState { UNINITIALIZED = 0, INITIALIZED, DISABLED };
    std::atomic<InitState> initState_{ UNINITIALIZED };
    WaitPost waitInit_;
    std::string clientId_;
    static bool clientMode_;
    void *memoryBuffer_ = nullptr;
    std::mutex clientIdMutex_;
    std::unordered_map<ClientKey, std::string> clientIdMapping_;
    static std::atomic<uint64_t> ubTransportMemSize_;  // 128 MB
    uint64_t ubMaxGetDataSize_ = 32 * 1024 * 1024;     // 32 MB
    uint64_t ubMaxSetBufferSize_ = 8 * 1024 * 1024;    // 8 MB
};

}  // namespace datasystem
#endif
