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
#include "datasystem/common/rdma/urma_manager.h"

#include <sys/mman.h>
#include <ub/umdk/urma/urma_opcode.h>
#include <cstdint>
#include <unordered_map>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
//#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rdma/fast_transport_base.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/utils/status.h"

#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"

DS_DECLARE_uint32(urma_poll_size);
DS_DECLARE_uint32(urma_connection_size);
DS_DECLARE_bool(urma_event_mode);

namespace datasystem {
namespace {
enum class UrmaErrorHandlePolicy {
    DEFAULT,  // just report error
    RECREATE_JFS,
};

UrmaErrorHandlePolicy GetUrmaErrorHandlePolicy(int statusCode)
{
    static std::unordered_map<int, UrmaErrorHandlePolicy> urmaErrorHandlePolicyTable = {
        { 9, UrmaErrorHandlePolicy::RECREATE_JFS },
    };

    const auto iter = urmaErrorHandlePolicyTable.find(statusCode);
    if (iter == urmaErrorHandlePolicyTable.end()) {
        return UrmaErrorHandlePolicy::DEFAULT;
    }
    return iter->second;
}
}  // namespace

constexpr uint64_t MAX_STUB_CACHE_NUM = 2048;
constexpr uint64_t DEFAULT_TRANSPORT_MEM_SIZE = 128UL * 1024UL * 1024UL;
constexpr uint64_t MAX_TRANSPORT_MEM_SIZE = 2UL * 1024UL * 1024UL * 1024UL;
;
bool UrmaManager::clientMode_ = false;
std::atomic<uint64_t> UrmaManager::ubTransportMemSize_(DEFAULT_TRANSPORT_MEM_SIZE);
UrmaManager &UrmaManager::Instance()
{
    static UrmaManager manager;
    return manager;
}

UrmaManager::UrmaManager()
{
    VLOG(RPC_LOG_LEVEL) << "UrmaManager::UrmaManager()";
    registerSegmentFlag_.bs.token_policy = URMA_TOKEN_PLAIN_TEXT;
    registerSegmentFlag_.bs.token_id_valid = URMA_TOKEN_ID_INVALID;
    LOG(INFO) << "registerSegmentFlag_.token_id_valid=" << URMA_TOKEN_ID_INVALID;
    registerSegmentFlag_.bs.cacheable = URMA_NON_CACHEABLE;
    registerSegmentFlag_.bs.reserved = 0;

    importSegmentFlag_.bs.cacheable = URMA_NON_CACHEABLE;
    importSegmentFlag_.bs.mapping = URMA_SEG_NOMAP;
    importSegmentFlag_.bs.reserved = 0;

#ifdef URMA_OVER_UB
    registerSegmentFlag_.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    importSegmentFlag_.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
#else
    registerSegmentFlag_.bs.access =
        URMA_ACCESS_LOCAL_WRITE | URMA_ACCESS_REMOTE_READ | URMA_ACCESS_REMOTE_WRITE | URMA_ACCESS_REMOTE_ATOMIC;
    importSegmentFlag_.bs.access =
        URMA_ACCESS_LOCAL_WRITE | URMA_ACCESS_REMOTE_READ | URMA_ACCESS_REMOTE_WRITE | URMA_ACCESS_REMOTE_ATOMIC;
#endif
    localSegmentMap_ = std::make_unique<UrmaLocalSegmentMap>();
    urmaResource_ = std::make_unique<UrmaResource>();
}

UrmaManager::~UrmaManager()
{
    Stop();
    OsXprtPipln::UnInitOsPiplnH2DEnv();
    VLOG(RPC_LOG_LEVEL) << "UrmaManager::~UrmaManager()";
    urmaConnectionMap_.clear();
    localSegmentMap_.reset();
    tbbEventMap_.clear();
    if (urmaResource_ != nullptr) {
        urmaResource_->Clear();
    }
    UrmaUninit();
    urma_dlopen::Cleanup();
    if (memoryBuffer_ != nullptr) {
        munmap(memoryBuffer_, ubTransportMemSize_.load());
        memoryBuffer_ = nullptr;
    }
    VLOG(RPC_LOG_LEVEL) << "UrmaManager::~UrmaManager() done";
}

Status UrmaManager::Stop()
{
    serverStop_ = true;
    if (serverEventThread_ && serverEventThread_->joinable()) {
        LOG(INFO) << "Waiting for Event thread to exit";
        serverEventThread_->join();
        serverEventThread_.reset();
    }
    return Status::OK();
}

Status UrmaManager::GetUrmaDeviceName(const HostPort &hostport, std::string &urmaDeviceName, int &eidIndex)
{
    if (GetUrmaMode() == UrmaMode::IB) {
        std::string deviceName;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(GetDevNameFromLocalIp(hostport.Host(), deviceName) == 0, K_INVALID,
                                             "Invalid ip address to get device name");
        LOG(INFO) << "deviceName = " << deviceName;
        RETURN_IF_NOT_OK(EthToRdmaDevName(deviceName, urmaDeviceName));
    } else if (GetUrmaMode() == UrmaMode::UB) {
        urmaDeviceName = GetStringFromEnv(ENV_UB_DEVICE_NAME.c_str(), DEFAULT_UB_DEVICE_NAME.c_str());
        eidIndex = GetInt32FromEnv(ENV_UB_DEVICE_EID.c_str(), 0);
        if (urmaDeviceName.empty()) {
            RETURN_STATUS(K_INVALID, "env DS_URMA_DEV_NAME is empty");
        }
        RETURN_IF_NOT_OK(UrmaGetEffectiveDevice(urmaDeviceName));
    } else {
        RETURN_STATUS(K_INVALID, "Invalid Urma mode");
    }
    LOG(INFO) << "urmaDeviceName = " << urmaDeviceName;
    return Status::OK();
}

Status UrmaManager::Init(const HostPort &hostport)
{
    PerfPoint perfPoint(PerfKey::URMA_MANAGER_INIT);
    InitState expected = InitState::UNINITIALIZED;
    if (initState_.compare_exchange_strong(expected, INITIALIZED)) {
        LOG(INFO) << FormatString("UrmaManager::Init(hostport = %s)", hostport.ToString());
    } else {
        // Initialization is already in progress or done by other thread, just wait for it to be done.
        waitInit_.Wait();
        return initState_ == INITIALIZED ? Status::OK() : Status(K_URMA_ERROR, "UrmaManager initialization failed");
    }
    bool needRollback = true;
    Raii rollback([this, &needRollback]() {
        if (needRollback) {
            initState_ = DISABLED;
        }
        waitInit_.Set();
    });
    RETURN_IF_NOT_OK(UrmaInit());
    std::string urmaDeviceName;
    int eidIndex = -1;
    RETURN_IF_NOT_OK(GetUrmaDeviceName(hostport, urmaDeviceName, eidIndex));
    urma_device_t *urmaDevice = nullptr;
    RETURN_IF_NOT_OK(UrmaGetDeviceByName(urmaDeviceName, urmaDevice));
    if (eidIndex < 0) {
        RETURN_IF_NOT_OK(GetEidIndex(urmaDevice, eidIndex));
    }
    RETURN_IF_NOT_OK(urmaResource_->Init(urmaDevice, eidIndex, FLAGS_urma_connection_size));
    RETURN_IF_NOT_OK(InitLocalUrmaInfo(hostport));
    serverEventThread_ = std::make_unique<std::thread>(&UrmaManager::ServerEventHandleThreadMain, this);

    // For client mode, we need to initialize extra memory buffer pool.
    if (UrmaManager::clientMode_) {
        clientId_ = GetStringUuid();
        RETURN_IF_NOT_OK(InitMemoryBufferPool());
        RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(MAX_STUB_CACHE_NUM, hostport));
    }
    needRollback = false;
    return Status::OK();
}

static Status ParseEnvUint64(const std::string &envName, uint64_t &outVal)
{
    auto strValue = std::getenv(envName.c_str());
    RETURN_OK_IF_TRUE(strValue == nullptr);

    try {
        uint64_t ret = StrToUnsignedLong(strValue);
        if (ret == 0) {
            throw std::out_of_range("Value should not be zero.");
        }
        outVal = ret;
    } catch (std::logic_error &e) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      FormatString("Env %s value %s parse to number failed: %s", envName, strValue, e.what()));
    }
    return Status::OK();
}

Status UrmaManager::InitMemoryBufferPool()
{
    // Parse max get data size and max set buffer size from environment
    RETURN_IF_NOT_OK(ParseEnvUint64(UB_MAX_GET_DATA_SIZE, ubMaxGetDataSize_));
    RETURN_IF_NOT_OK(ParseEnvUint64(UB_MAX_SET_BUFFER_SIZE, ubMaxSetBufferSize_));

    // If transport size is not set by connection, try to get from environment
    if (ubTransportMemSize_.load() == 0) {
        uint64_t envSize = 0;
        RETURN_IF_NOT_OK(ParseEnvUint64(UB_TRANSPORT_MEM_SIZE, envSize));
        ubTransportMemSize_.store(envSize);
    }

    if (ubTransportMemSize_.load() > MAX_TRANSPORT_MEM_SIZE || ubTransportMemSize_.load() <= 0) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_INVALID,
                                FormatString("ubTransportMemSize %lu is invalid, must be between %lu and %lu",
                                             ubTransportMemSize_.load(), 0, MAX_TRANSPORT_MEM_SIZE));
    }

    AllocatorFuncRegister regFunc;
    auto hostAllocFunc = [this](void **ptr, size_t maxSize) -> Status {
        memoryBuffer_ = mmap(nullptr, maxSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        *ptr = memoryBuffer_;
        if (memoryBuffer_ == MAP_FAILED) {
            RETURN_STATUS(K_OUT_OF_MEMORY, "Failed to allocate memory buffer pool for client");
        }
        RETURN_IF_NOT_OK(RegisterSegment(reinterpret_cast<uint64_t>(*ptr), maxSize));
        return Status::OK();
    };
    auto hostdestroyFunc = [](void *ptr, size_t destroySize) -> Status {
        // unmmap in urma manager
        (void)ptr;
        (void)destroySize;
        return Status::OK();
    };

    regFunc.createFunc = hostAllocFunc;
    regFunc.destroyFunc = hostdestroyFunc;

    auto *allocator = Allocator::Instance();
    auto rc = allocator->InitWithFlexibleRegister(AllocateType::UB_TRANSPORT, ubTransportMemSize_, regFunc);
    if (rc.IsOk()) {
        // Allocate phyica memory buffer pool for client
        std::shared_ptr<ArenaGroup> arenaGroup;
        rc =
            allocator->CreateArenaGroup(DEFAULT_TENANT_ID, ubTransportMemSize_, arenaGroup, AllocateType::UB_TRANSPORT);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Failed to get arena group for client");
    }
    if (rc.IsError()) {
        rc = rc.GetCode() == K_DUPLICATED ? Status::OK() : rc;
        LOG(WARNING) << "Failed to register memory buffer pool for client, error: " << rc.ToString();
    }

    return rc;
}

Status UrmaManager::GetMemoryBufferHandle(std::shared_ptr<BufferHandle> &handle, uint64_t size)
{
    if (size == 0) {
        return Status(K_INVALID, "UB Get buffer size is 0");
    }
    std::shared_ptr<ShmUnit> unit = std::make_shared<ShmUnit>();
    RETURN_IF_NOT_OK(
        unit->AllocateMemory(DEFAULT_TENANTID, size, false, ServiceType::OBJECT, AllocateType::UB_TRANSPORT));

    handle = std::make_shared<BufferHandle>(unit, memoryBuffer_, size);
    return Status::OK();
}

Status UrmaManager::GetMemoryBufferInfo(std::shared_ptr<UrmaManager::BufferHandle> &handler, uint8_t *&bufferPtr,
                                        uint64_t &bufferSize, UrmaRemoteAddrPb &urmaInfo)
{
    RETURN_RUNTIME_ERROR_IF_NULL(memoryBuffer_);
    uint32_t bufferOffset = handler->GetOffset();
    bufferPtr = reinterpret_cast<uint8_t *>(handler->GetPointer());
    bufferSize = handler->GetSegmentSize();
    urmaInfo.set_seg_va(reinterpret_cast<uint64_t>(memoryBuffer_));
    urmaInfo.set_seg_data_offset(bufferOffset);
    auto *requestAddr = urmaInfo.mutable_request_address();
    requestAddr->set_host(localUrmaInfo_.localAddress.Host());
    requestAddr->set_port(localUrmaInfo_.localAddress.Port());
    if (!GetClientId().empty()) {
        urmaInfo.set_client_id(GetClientId());
    }
    return Status::OK();
}

Status UrmaManager::InitLocalUrmaInfo(const HostPort &hostport)
{
    localUrmaInfo_.eid = GetEid();
    localUrmaInfo_.uasid = GetUasid();
    localUrmaInfo_.jfrIds = GetJfrIds();
    localUrmaInfo_.localAddress = hostport;
    localUrmaInfo_.uniqueInstanceId = GetStringUuid();
    LOG(INFO) << "local urma info: " << localUrmaInfo_.ToString();
    return Status::OK();
}

Status UrmaManager::UrmaInit()
{
    LOG(INFO) << "UrmaManager::UrmaInit()";
    if (!datasystem::urma_dlopen::Init()) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, "Failed to initialize URMA dlopen loader");
    }
    LOG_IF_ERROR(RegisterUrmaLog(), "Failed to register urma log to datasystem, may check log in /var/log/umdk/urma");
    urma_init_attr_t urmaInitAttribute = { 0, 0 };
    urma_status_t ret = ds_urma_init(&urmaInitAttribute);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma init, ret = %d", ret));
    }
    LOG(INFO) << "urma init success";
    return Status::OK();
}

Status UrmaManager::UrmaUninit()
{
    LOG(INFO) << "UrmaManager::UrmaUninit()";
    urma_status_t ret = ds_urma_uninit();
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma uninit, ret = %d", ret));
    }
    LOG(INFO) << "urma uninit success";
    RETURN_IF_NOT_OK(UnRegisterUrmaLog());
    return Status::OK();
}

Status UrmaManager::RegisterUrmaLog()
{
    urmaLogCallback_ = [](int level, char *message) {
        if (level <= (int)URMA_VLOG_LEVEL_ERR) {
            LOG(ERROR) << message;
        } else if (level <= (int)URMA_VLOG_LEVEL_NOTICE) {
            LOG(WARNING) << message;
        } else if (level <= (int)URMA_VLOG_LEVEL_INFO) {
            VLOG(INFO) << message;
        } else if (level <= (int)URMA_VLOG_LEVEL_DEBUG) {
            VLOG(RPC_LOG_LEVEL) << message;
        } else {
            VLOG(RPC_DEBUG_LOG_LEVEL) << message;
        }
    };

    urma_status_t ret = ds_urma_register_log_func(urmaLogCallback_);
    if (ret != URMA_SUCCESS) {
        urmaLogCallback_ = nullptr;
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma register log, ret = %d", ret));
    }
    LOG(INFO) << "urma register log success";
    return Status::OK();
}

Status UrmaManager::UnRegisterUrmaLog()
{
    if (!urmaLogCallback_) {
        return Status::OK();
    }
    urma_status_t ret = ds_urma_unregister_log_func();
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma unRegister log, ret = %d", ret));
    }
    LOG(INFO) << "urma unRegister log success";
    urmaLogCallback_ = nullptr;
    return Status::OK();
}

int UrmaManager::CompareDeviceName(const std::string &urmaDevName, urma_device_t **devList, int devCount)
{
    for (int i = 0; i < devCount; i++) {
        if (devList[i] == nullptr) {
            LOG(ERROR) << FormatString("Got empty device index %d from devList.", i);
            continue;
        }
        if (strncmp(reinterpret_cast<const char *>(devList[i]->name), urmaDevName.c_str(), urmaDevName.length()) == 0) {
            return i;
        }
    }
    return -1;
}

Status UrmaManager::UrmaGetEffectiveDevice(std::string &urmaDevName)
{
    LOG(INFO) << FormatString("Start UrmaGetEffectiveDevice() with %d", urmaDevName);
    int devNums = 0;
    urma_device_t **list = nullptr;
    list = ds_urma_get_device_list(&devNums);
    if (list == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                FormatString("Got empty[%d] ub device list with errno = %d", devNums, errno));
    }
    int index = CompareDeviceName(urmaDevName, list, devNums);
    if (index >= 0) {
        return Status::OK();
    }
    std::string prefixName = "bonding";
    index = CompareDeviceName(prefixName, list, devNums);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(index >= 0, K_RUNTIME_ERROR, "Cannot get effective bonding device");
    urmaDevName = std::string(list[index]->name);
    return Status::OK();
}

Status UrmaManager::UrmaGetDeviceByName(const std::string &deviceName, urma_device_t *&urmaDevice)
{
    LOG(INFO) << "UrmaManager::UrmaGetDeviceByName()";
    urmaDevice = ds_urma_get_device_by_name(const_cast<char *>(deviceName.c_str()));
    if (urmaDevice) {
        LOG(INFO) << "urma get device by name success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma get device by name, errno = %d", errno));
}

Status UrmaManager::UrmaGetEidList(urma_device_t *&urmaDevice, urma_eid_info_t *&eidList, uint32_t &eidCount)
{
    LOG(INFO) << "UrmaManager::UrmaGetEidList()";
    eidList = ds_urma_get_eid_list(urmaDevice, &eidCount);
    if (eidList) {
        LOG(INFO) << "urma get eid list success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma get eid list, errno = %d", errno));
}

Status UrmaManager::GetEidIndex(urma_device_t *&urmaDevice, int &eidIndex)
{
    LOG(INFO) << "UrmaManager::GetEidIndex()";
    urma_eid_info_t *eidList = nullptr;
    uint32_t eidCount = 0;
    eidIndex = -1;

    RETURN_IF_NOT_OK(UrmaGetEidList(urmaDevice, eidList, eidCount));

    Raii freeEidList([&eidList]() {
        if (eidList) {
            ds_urma_free_eid_list(eidList);
        }
    });

    if (eidCount > 0) {
        eidIndex = eidList[0].eid_index;
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, "Failed to get eid index for device");
}

bool UrmaManager::IsEventModeEnabled()
{
    return FLAGS_urma_event_mode;
}

std::string UrmaManager::GetEid()
{
    return EidToStr(urmaResource_->GetContext()->eid);
};

uint64_t UrmaManager::GetUasid()
{
    return urmaResource_->GetContext()->uasid;
};

std::vector<uint32_t> UrmaManager::GetJfrIds()
{
    std::vector<uint32_t> results;
    for (const auto &urmaJfr : urmaResource_->GetJfrList()) {
        if (urmaJfr != nullptr) {
            results.emplace_back(urmaJfr->Raw()->jfr_id.id);
        }
    }
    return results;
}

Status UrmaManager::GetTargetSeg(uint64_t segAddress, uint64_t segSize, const std::string &address,
                                 urma_target_seg_t **targetSeg, urma_jfr_t **targetJfr)
{
    UrmaLocalSegmentMap::const_accessor accessor;
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetOrRegisterSegment(segAddress, segSize, accessor));
    *targetSeg = accessor->second->Raw();

    HostPort remoteSenderAddr;
    remoteSenderAddr.ParseString(address);
    LOG(ERROR) << "remoteSenderAddr.ToString() " << remoteSenderAddr.ToString();
    int idx = GetJfrIndex(remoteSenderAddr.ToString());
    *targetJfr = urmaResource_->GetJfr(idx)->Raw();
    return Status::OK();
}

Status UrmaManager::RegisterSegment(const uint64_t &segAddress, const uint64_t &segSize)
{
    UrmaLocalSegmentMap::const_accessor constAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, constAccessor));
    return Status::OK();
}

Status UrmaManager::GetSegmentInfo(UrmaHandshakeReqPb &handshakeReq)
{
    PerfPoint point(PerfKey::WORKER_URMA_GET_SEGMENT);
    // Traverse the list of local registered segments.
    std::unique_lock<std::shared_timed_mutex> l(localMapMutex_);
    for (auto iter = localSegmentMap_->begin(); iter != localSegmentMap_->end(); iter++) {
        CHECK_FAIL_RETURN_STATUS(iter->second != nullptr, K_RUNTIME_ERROR, "Local segment is null");
        auto *segInfo = handshakeReq.add_seg_infos();
        auto segPb = segInfo->mutable_seg();
        UrmaSeg::ToProto(iter->second->Raw()->seg, *segPb);
        LOG(INFO) << "local seg info: " << UrmaSeg::ToString(iter->second->Raw()->seg);
    }
    return Status::OK();
}

Status UrmaManager::GetSegmentInfo(UrmaHandshakeRspPb &handshakeRsp)
{
    PerfPoint point(PerfKey::WORKER_URMA_GET_SEGMENT);
    std::unique_lock<std::shared_timed_mutex> l(localMapMutex_);
    for (auto iter = localSegmentMap_->begin(); iter != localSegmentMap_->end(); iter++) {
        CHECK_FAIL_RETURN_STATUS(iter->second != nullptr, K_RUNTIME_ERROR, "Local segment is null");
        auto *segInfo = handshakeRsp.mutable_hand_shake()->add_seg_infos();
        auto segPb = segInfo->mutable_seg();
        UrmaSeg::ToProto(iter->second->Raw()->seg, *segPb);
        LOG(INFO) << "local seg info (rsp): " << UrmaSeg::ToString(iter->second->Raw()->seg);
    }
    return Status::OK();
}

Status UrmaManager::GetOrRegisterSegment(const uint64_t &segAddress, const uint64_t &segSize,
                                         UrmaLocalSegmentMap::const_accessor &constAccessor)
{
    std::shared_lock<std::shared_timed_mutex> l(localMapMutex_);
    if (!localSegmentMap_->find(constAccessor, segAddress)) {
        UrmaLocalSegmentMap::accessor accessor;
        if (localSegmentMap_->insert(accessor, segAddress)) {
            auto rc = UrmaLocalSegment::Register(urmaResource_->GetContext(), segAddress, segSize,
                                                 urmaResource_->GetUrmaToken(), registerSegmentFlag_, accessor->second);
            if (rc.IsError()) {
                localSegmentMap_->erase(accessor);
                return rc;
            }
        }
        accessor.release();
        // Switch to const accessor so it does not block the others.
        CHECK_FAIL_RETURN_STATUS(localSegmentMap_->find(constAccessor, segAddress), K_RUNTIME_ERROR,
                                 "Failed to operate on local segment map.");
    }
    return Status::OK();
}

Status UrmaManager::ServerEventHandleThreadMain()
{
    // Run this method until serverStop is called.
    while (!serverStop_.load()) {
        std::unordered_set<uint64_t> successCompletedReqs;
        std::unordered_map<uint64_t, int> failedCompletedReqs;
        Status rc = PollJfcWait(urmaResource_->GetJfc(), MAX_POLL_JFC_TRY_CNT, successCompletedReqs,
                                failedCompletedReqs, FLAGS_urma_poll_size);

        // push it into request set
        // we do not need lock for finishedRequests_ as its accessed only by single thread
        if (successCompletedReqs.size()) {
            finishedRequests_.insert(successCompletedReqs.begin(), successCompletedReqs.end());
        }
        if (failedCompletedReqs.size()) {
            failedRequests_.insert(failedCompletedReqs.begin(), failedCompletedReqs.end());
            for (const auto &kv : failedCompletedReqs) {
                finishedRequests_.insert(kv.first);
            }
        }
        // notify threads waiting on any finishedRequests
        CheckAndNotify();
    }
    return Status::OK();
}

Status UrmaManager::CheckAndNotify()
{
    // if no finished requests, no need to notify
    if (finishedRequests_.empty()) {
        return Status::OK();
    }

    // Iterate through the finishedRequests_ set and notify request threads
    for (auto it = finishedRequests_.begin(); it != finishedRequests_.end();) {
        auto requestId = *it;
        std::shared_ptr<UrmaEvent> event;
        // Get the event for request Id
        if (GetEvent(requestId, event).IsOk()) {
            auto failedIt = failedRequests_.find(requestId);
            if (failedIt != failedRequests_.end()) {
                event->SetFailed(failedIt->second);
                failedRequests_.erase(failedIt);
            }
            // Notify everyone who are waiting on the event
            event->NotifyAll();
            // delete the event and
            VLOG(1) << "[UrmaEventHandler] Notifying the request id: " << requestId;
            // remove request id from finishedRequests_ set
            // we dont need lock for finishedRequests_ as its accessed only by single thread
            it = finishedRequests_.erase(it);
        } else {
            ++it;
        }
    }

    return Status::OK();
}

void UrmaManager::DeleteEvent(uint64_t requestId)
{
    tbbEventMap_.erase(requestId);
}

Status UrmaManager::GetEvent(uint64_t requestId, std::shared_ptr<UrmaEvent> &event)
{
    TbbEventMap::accessor mapAccessor;
    if (tbbEventMap_.find(mapAccessor, requestId)) {
        event = mapAccessor->second;
        return Status::OK();
    }
    // Can happen if event is not yet inserted by sender thread.
    RETURN_STATUS(K_NOT_FOUND, FormatString("Request id %d doesnt exist in event map", requestId));
}

Status UrmaManager::CreateEvent(uint64_t requestId, const std::shared_ptr<UrmaConnection> &connection,
                                const std::shared_ptr<UrmaJfs> &jfs, std::shared_ptr<EventWaiter> waiter)
{
    if (!jfs->IsValid()) {
        RETURN_STATUS(K_URMA_TRY_AGAIN, "Urma jfs is invalid");
    }
    TbbEventMap::accessor mapAccessor;
    auto res = tbbEventMap_.insert(mapAccessor, requestId);
    if (!res) {
        // If this happens that means requestId is duplicated.
        RETURN_STATUS_LOG_ERROR(K_DUPLICATED, FormatString("Request id %d already exists in event map", requestId));
    } else {
        mapAccessor->second = std::make_shared<UrmaEvent>(requestId, connection, waiter);
    }
    return Status::OK();
}

Status UrmaManager::WaitToFinish(uint64_t requestId, int64_t timeoutMs)
{
    PerfPoint point(PerfKey::URMA_WAIT_TO_FINISH);
    INJECT_POINT("UrmaManager.UrmaWaitError",
                 []() { return Status(K_RPC_DEADLINE_EXCEEDED, "Injcect urma wait error"); });
    if (timeoutMs < 0) {
        RETURN_STATUS_LOG_ERROR(K_RPC_DEADLINE_EXCEEDED, FormatString("timedout waiting for request: %d", requestId_));
    }
    std::shared_ptr<UrmaEvent> event;
    RETURN_IF_NOT_OK(GetEvent(requestId, event));
    // use this unique request id as key to wait
    // wait until timeout

    Raii deleteEvent([this, &requestId]() { DeleteEvent(requestId); });

    VLOG(1) << "[UrmaEventHandler] Started waiting for the request id: " << requestId;
    PerfPoint waitPoint(PerfKey::URMA_WAIT_TIME);
    Timer timer;
    RETURN_IF_NOT_OK(event->WaitFor(std::chrono::milliseconds(timeoutMs)));
    workerOperationTimeCost.Append("Urma wait time.", timer.ElapsedMilliSecond());
    waitPoint.Record();
    RETURN_IF_NOT_OK(HandleUrmaEvent(requestId, event));
    VLOG(1) << "[UrmaEventHandler] Done waiting for the request id: " << requestId;
    return Status::OK();
}

Status UrmaManager::HandleUrmaEvent(uint64_t requestId, const std::shared_ptr<UrmaEvent> &event)
{
    RETURN_OK_IF_TRUE(!event->IsFailed());

    const auto statusCode = event->GetStatusCode();
    const auto policy = GetUrmaErrorHandlePolicy(statusCode);
    auto errMsg = FormatString("Polling failed with an error for requestId: %d, cqe status: %d", requestId, statusCode);
    if (policy == UrmaErrorHandlePolicy::RECREATE_JFS) {
        LOG(WARNING) << "Recreate JFS for requestId: " << requestId << " due to error status code: " << statusCode;
        auto connection = event->GetConnection().lock();
        if (connection != nullptr) {
            auto oldJfs = connection->GetJfs();
            std::shared_ptr<UrmaJfs> newJfs;
            LOG_IF_ERROR(urmaResource_->AsyncModifyJfsToError(oldJfs), "AsyncModifyJfsToError failed");
            LOG_IF_ERROR(urmaResource_->GetNextJfs(newJfs), "GetNextJfs failed");
            if (newJfs != nullptr) {
                LOG(INFO) << "the connection using new jfs id " << newJfs->GetJfsId();
                connection->SetJfs(std::move(newJfs));
            }
            RETURN_STATUS(K_URMA_TRY_AGAIN, errMsg);
        } else {
            LOG(WARNING) << "Event connection expired, cannot recreate JFS for requestId: " << requestId;
        }
    }

    return Status(K_URMA_ERROR, errMsg);
}

Status UrmaManager::GetJfsFromConnection(const std::shared_ptr<UrmaConnection> &connection,
                                         std::shared_ptr<UrmaJfs> &jfs)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");

    jfs = connection->GetJfs();
    if (jfs != nullptr && jfs->IsValid()) {
        VLOG(1) << "connection using jfs id " << jfs->GetJfsId();
        return Status::OK();
    }

    if (jfs != nullptr && !jfs->IsValid()) {
        INJECT_POINT("worker.GetJfsFromConnection", [jfs]() { return Status::OK(); });
    }

    RETURN_IF_NOT_OK(urmaResource_->GetNextJfs(jfs));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(jfs != nullptr, K_RUNTIME_ERROR, "Got empty jfs from urma resource.");
    connection->SetJfs(jfs);
    VLOG(1) << "connection switched to jfs id " << jfs->GetJfsId();
    return Status::OK();
}

Status UrmaManager::CheckCompletionRecordStatus(urma_cr_t completeRecords[], int count,
                                                std::unordered_set<uint64_t> &successCompletedReqs,
                                                std::unordered_map<uint64_t, int> &failedCompletedReqs)
{
    INJECT_POINT("UrmaManager.CheckCompletionRecordStatus", [&completeRecords](int index, int status) {
        if (completeRecords[index].status != URMA_CR_WR_FLUSH_ERR_DONE) {
            completeRecords[index].status = static_cast<urma_cr_status_t>(status);
        }
        return Status::OK();
    });
    for (int i = 0; i < count; i++) {
#ifdef BUILD_PIPLN_H2D
        // redirect pipeline h2d events
        if (OsXprtPipln::PiplnH2DRecvEventHook(&completeRecords[i]))
            continue;
#endif
        auto crStatus = completeRecords[i].status;
        auto userCtx = completeRecords[i].user_ctx;
        auto jfsId = completeRecords[i].local_id;
        if (crStatus == URMA_CR_WR_FLUSH_ERR_DONE) {
            LOG(INFO) << "[UrmaEventHandler] Write flush error done for request id: " << userCtx
                      << ", jfs id: " << jfsId;
            urmaResource_->AsyncDeleteJfs(jfsId);
        } else if (crStatus == URMA_CR_SUCCESS) {
            VLOG(1) << "[UrmaEventHandler] Got event with request id: " << userCtx;
            successCompletedReqs.insert(userCtx);
        } else {
            LOG(ERROR) << FormatString("Failed to poll jfc requestId: %d CR.status: %d", userCtx, crStatus);
            failedCompletedReqs[completeRecords[i].user_ctx] = crStatus;
        }
    }

    if (!failedCompletedReqs.empty()) {
        RETURN_STATUS(K_URMA_ERROR, "Failed to poll jfc");
    }
    return Status::OK();
}

Status UrmaManager::PollJfcWait(urma_jfc_t *urmaJfc, const uint64_t maxTryCount,
                                std::unordered_set<uint64_t> &successCompletedReqs,
                                std::unordered_map<uint64_t, int> &failedCompletedReqs, const uint64_t numPollCRS)
{
    urma_cr_t completeRecords[numPollCRS];
    urma_jfc_t *ev_jfc = nullptr;
    int cnt;

    if (IsEventModeEnabled()) {
        // wait for the event
        cnt = ds_urma_wait_jfc(urmaResource_->GetJfce(), 1, RPC_POLL_TIME, &ev_jfc);
        if (cnt < 0 || cnt > 1 || (cnt == 1 && urmaJfc != ev_jfc)) {
            // This is error case
            // cnt can be 0 or 1 and jfc should match
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to wait jfc, ret = %d", cnt));
        } else if (cnt == 0) {
            // not found any event, just retry
            return Status::OK();
        }

        // Got the event, now get CR for the event
        // Event mode can poll one CR at a time
        cnt = ds_urma_poll_jfc(urmaJfc, numPollCRS, &completeRecords[0]);
        INJECT_POINT("UrmaManager.CheckCompletionRecordStatus", [&completeRecords]() {
            completeRecords[0].status = URMA_CR_REM_ACCESS_ABORT_ERR;
            return Status::OK();
        });
        if (cnt < 0) {
            // this is error case
            // cnt can be 0 or 1
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to poll jfc, ret = %d, CR.status = %d", cnt,
                                                               completeRecords[0].status));
        } else if (cnt > 0) {
            // Ack the event and rearm jfc to process next event
            uint32_t ack_cnt = 1;
            ds_urma_ack_jfc((urma_jfc_t **)&ev_jfc, &ack_cnt, 1);
            auto status = ds_urma_rearm_jfc(urmaJfc, false);
            if (status != URMA_SUCCESS) {
                RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to rearm jfc, status = %d", status));
            }
            return CheckCompletionRecordStatus(completeRecords, cnt, successCompletedReqs, failedCompletedReqs);
        }
        return Status::OK();
    }

    // trys maxTryCount times to get an event
    for (uint64_t i = 0; i < maxTryCount; ++i) {
        cnt = ds_urma_poll_jfc(urmaJfc, numPollCRS, completeRecords);
        if (cnt == 0) {
            // If there is nothing to poll, just sleep.
            // Note that it takes on average 50us to wake up with usleep(0), due to OS timerslack settings.
            usleep(0);
        } else if (cnt < 0) {
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to poll jfc, ret = %d", cnt));
        } else if (cnt > 0) {
            return CheckCompletionRecordStatus(completeRecords, cnt, successCompletedReqs, failedCompletedReqs);
        }
        if (serverStop_.load()) {
            LOG(INFO) << "Worker exiting.";
            return Status::OK();
        }
    }
    RETURN_STATUS(K_TRY_AGAIN, FormatString("No Event present in JFC"));
}

Status UrmaManager::ImportRemoteJfr(const UrmaJfrInfo &jfrInfo)
{
    PerfPoint point(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    const std::string remoteConnectionId =
        jfrInfo.clientId.empty() ? jfrInfo.localAddress.ToString() : jfrInfo.clientId;
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    // Insert or update the import jfr (in case the sending worker restarts)
    TbbUrmaConnectionMap::accessor accessor;
    auto res = urmaConnectionMap_.insert(accessor, remoteConnectionId);
    if (!res && accessor->second != nullptr) {
        RETURN_OK_IF_TRUE(accessor->second->GetUrmaJfrInfo().ToString() == jfrInfo.ToString());
        // Existing entry exists, and we will update the imported jfr (assuming the sending worker restarted)
        // First of all, release the existing imported jfr
        accessor->second->Clear();
    }
    bool success = false;
    Raii raii([&success, &accessor, this]() {
        if (!success) {
            urmaConnectionMap_.erase(accessor);
        }
    });

    std::shared_ptr<UrmaJfs> jfs;
    RETURN_IF_NOT_OK(urmaResource_->GetNextJfs(jfs));
    // Now we import a new jfr
    urma_rjfr_t remoteJfr;
    urma_eid_t eid;
    RETURN_IF_NOT_OK(StrToEid(jfrInfo.eid, eid));
    remoteJfr.jfr_id.eid = eid;
    remoteJfr.jfr_id.uasid = jfrInfo.uasid;
    remoteJfr.trans_mode = URMA_TM_RM;
    remoteJfr.tp_type = URMA_CTP;

    std::unique_ptr<UrmaTargetJfr> targetJfr;
    // only import one jfr
    CHECK_FAIL_RETURN_STATUS(!jfrInfo.jfrIds.empty(), K_RUNTIME_ERROR, "empty jfrIds");
    remoteJfr.jfr_id.id = jfrInfo.jfrIds[0];
    Timer timer;
    RETURN_IF_NOT_OK_APPEND_MSG(
        UrmaTargetJfr::Import(urmaResource_->GetContext(), &remoteJfr, urmaResource_->GetUrmaToken(), targetJfr),
        FormatString("Failed to import jfr, %s", jfrInfo.ToString()));
    LOG_IF(WARNING, timer.ElapsedSecond() > 1) << "ImportRemoteJfr exceed 1s!";
    accessor->second = std::make_shared<UrmaConnection>(jfs, std::move(targetJfr), jfrInfo);
    success = true;
    (void)success;
    return Status::OK();
}

Status UrmaManager::ImportRemoteInfo(const UrmaHandshakeReqPb &req)
{
    const HostPort requestAddress(req.address().host(), req.address().port());
    const std::string remoteConnectionId = req.client_id().empty() ? requestAddress.ToString() : req.client_id();
    PerfPoint point1(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::accessor accessor;
    // The comm layer (zmq) has already exchanged the jfr, and we should be able to locate the entry.
    auto res = urmaConnectionMap_.find(accessor, remoteConnectionId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteConnectionId));
    point1.Record();
    PerfPoint point2(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
    for (int i = 0; i < req.seg_infos_size(); i++) {
        auto &segInfo = req.seg_infos(i);
        CHECK_FAIL_RETURN_STATUS(accessor->second != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
        auto rc = accessor->second->ImportRemoteSeg(segInfo, urmaResource_->GetContext(), urmaResource_->GetUrmaToken(),
                                                    importSegmentFlag_);
        if (rc.IsError()) {
            // clear import jfr and seg to reconnect next time
            urmaConnectionMap_.erase(accessor);
            return rc;
        }
    }
    point2.Record();
    return Status::OK();
}

Status UrmaManager::ImportRemoteInfo(const UrmaHandshakeRspPb &rsp)
{
    if (!rsp.has_hand_shake()) {
        return Status(StatusCode::K_INVALID, "UrmaHandshakeRspPb has no hand_shake");
    }
    const auto &handShake = rsp.hand_shake();
    const HostPort requestAddress(handShake.address().host(), handShake.address().port());
    const std::string remoteConnectionId = requestAddress.ToString();
    PerfPoint point1(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::accessor accessor;
    auto res = urmaConnectionMap_.find(accessor, remoteConnectionId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteConnectionId));
    point1.Record();
    PerfPoint point2(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
    for (int i = 0; i < handShake.seg_infos_size(); i++) {
        auto &segInfo = handShake.seg_infos(i);
        CHECK_FAIL_RETURN_STATUS(accessor->second != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
        auto rc = accessor->second->ImportRemoteSeg(segInfo, urmaResource_->GetContext(), urmaResource_->GetUrmaToken(),
                                                    importSegmentFlag_);
        if (rc.IsError()) {
            urmaConnectionMap_.erase(accessor);
            return rc;
        }
    }
    point2.Record();
    return Status::OK();
}

uint64_t UrmaManager::GenerateReqId()
{
    return requestId_.fetch_add(1);
}

Status UrmaManager::UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                     const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                     const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                     bool blocking, std::vector<uint64_t> &eventKeys,
                                     std::shared_ptr<EventWaiter> waiter)
{
    eventKeys.clear();
    PerfPoint point(PerfKey::URMA_IMPORT_AND_WRITE_PAYLOAD);
    const uint64_t segVa = urmaInfo.seg_va();
    const HostPort requestAddress(urmaInfo.request_address().host(), urmaInfo.request_address().port());
    std::string remoteConnectionId = urmaInfo.client_id().empty() ? requestAddress.ToString() : urmaInfo.client_id();
    point.RecordAndReset(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::const_accessor constAccessor;
    // The comm layer (zmq) has already exchanged the jfr, and we should be able to locate the entry.
    auto res = urmaConnectionMap_.find(constAccessor, remoteConnectionId);
    if (!res && !urmaInfo.client_id().empty()) {
        remoteConnectionId = requestAddress.ToString();
        res = urmaConnectionMap_.find(constAccessor, remoteConnectionId);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteConnectionId));
    point.RecordAndReset(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
    auto &connection = constAccessor->second;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    UrmaRemoteSegmentMap::const_accessor remoteSegAccessor;
    RETURN_IF_NOT_OK(connection->GetRemoteSeg(segVa, remoteSegAccessor));
    point.RecordAndReset(PerfKey::URMA_REGISTER_LOCAL_SEGMENT);

    UrmaLocalSegmentMap::const_accessor localSegAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(localSegAddress, localSegSize, localSegAccessor));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localSegAccessor->second != nullptr, K_RUNTIME_ERROR, "Local segment is null");
    point.RecordAndReset(PerfKey::URMA_TOTAL_WRITE);

    // Write payload
    urma_jfs_wr_flag_t flag;
    flag.value = 0;
    flag.bs.complete_enable = 1;

    // Get jfs and tjfr
    std::shared_ptr<UrmaJfs> jfs;
    RETURN_IF_NOT_OK(GetJfsFromConnection(connection, jfs));
    auto *tjfr = connection->GetTargetJfr();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(tjfr != nullptr, K_RUNTIME_ERROR, "Write got empty remote jfr.");

    if (OsXprtPipln::IsPiplnH2DRequest(urmaInfo)) {
        OsXprtPipln::PiplnSndArgs args;
        args.jfs = jfs->Raw();
        args.tjetty = tjfr;
        args.localAddr = localObjectAddress + readOffset + metaDataSize;
        args.localSeg = localSegAccessor->second->Raw();
        args.remoteAddr = segVa + urmaInfo.seg_data_offset() + readOffset;
        args.remoteSeg = remoteSegAccessor->second->Raw();
        args.len = readSize;
        args.serverKey = GenerateReqId();
        args.clientKey = urmaInfo.client_req_id();
    
        eventKeys.emplace_back(args.serverKey);
        return OsXprtPipln::StartPipelineSender(args);
    }

    PerfPoint point4(PerfKey::URMA_TOTAL_WRITE);

    uint64_t writtenSize = 0;
    uint64_t remainSize = readSize;
    Timer timer;
    while (remainSize > 0) {
        const uint64_t writeSize = std::min(remainSize, urmaResource_->GetMaxWriteSize());
        const uint64_t key = requestId_.fetch_add(1);
        PerfPoint pointWrite(PerfKey::URMA_WRITE);
        INJECT_POINT("UrmaManager.UrmaWriteError",
                     []() { return Status(K_RUNTIME_ERROR, "Injcect urma write error"); });
        urma_status_t ret =
            ds_urma_write(jfs->Raw(), tjfr, remoteSegAccessor->second->Raw(), localSegAccessor->second->Raw(),
                          segVa + urmaInfo.seg_data_offset() + readOffset + writtenSize,
                          localObjectAddress + readOffset + metaDataSize + writtenSize, writeSize, flag, key);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            ret == URMA_SUCCESS, K_RUNTIME_ERROR,
            FormatString("Failed to urma write object with key = %zu, ret = %d", key, ret));
        pointWrite.Record();
        remainSize -= writeSize;
        writtenSize += writeSize;

        RETURN_IF_NOT_OK(CreateEvent(key, connection, jfs, waiter));
        eventKeys.emplace_back(key);
    }
    workerOperationTimeCost.Append("Urma total write.", timer.ElapsedMilliSecond());
    point.Record();
    // If it is blocking wait, we will wait for the write to finish here.
    if (blocking) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitFastTransportEvent(eventKeys, remainingTime, errorHandler));
        eventKeys.clear();
    }
    return Status::OK();
}

Status UrmaManager::UrmaRead(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                             const uint64_t &localSegSize, const uint64_t &localObjectAddress, const uint64_t &dataSize,
                             const uint64_t &metaDataSize, std::vector<uint64_t> &keys)
{
    keys.clear();
    const uint64_t segVa = urmaInfo.seg_va();
    const HostPort requestAddress(urmaInfo.request_address().host(), urmaInfo.request_address().port());
    std::string remoteConnectionId = urmaInfo.client_id().empty() ? requestAddress.ToString() : urmaInfo.client_id();
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::const_accessor constAccessor;
    // The comm layer (zmq) has already exchanged the jfr, and we should be able to locate the entry.
    auto res = urmaConnectionMap_.find(constAccessor, remoteConnectionId);
    if (!res && !urmaInfo.client_id().empty()) {
        remoteConnectionId = requestAddress.ToString();
        res = urmaConnectionMap_.find(constAccessor, remoteConnectionId);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteConnectionId));
    auto &connection = constAccessor->second;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    UrmaRemoteSegmentMap::const_accessor remoteSegAccessor;
    RETURN_IF_NOT_OK(connection->GetRemoteSeg(segVa, remoteSegAccessor));

    UrmaLocalSegmentMap::const_accessor localSegAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(localSegAddress, localSegSize, localSegAccessor));

    urma_jfs_wr_flag_t flag;
    flag.value = 0;
    flag.bs.complete_enable = 1;

    std::shared_ptr<UrmaJfs> jfs;
    RETURN_IF_NOT_OK(GetJfsFromConnection(connection, jfs));
    auto *importJfr = connection->GetTargetJfr();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(importJfr != nullptr, K_RUNTIME_ERROR, "Read got empty remote jfr.");

    uint64_t readOffset = 0;
    uint64_t remainSize = dataSize;
    while (remainSize > 0) {
        const uint64_t readSize = std::min(remainSize, urmaResource_->GetMaxReadSize());
        const uint64_t key = requestId_.fetch_add(1);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localSegAccessor->second != nullptr, K_RUNTIME_ERROR,
                                             "Local segment is null");
        urma_status_t ret =
            ds_urma_read(jfs->Raw(), importJfr, localSegAccessor->second->Raw(), remoteSegAccessor->second->Raw(),
                         localObjectAddress + metaDataSize + readOffset,
                         segVa + urmaInfo.seg_data_offset() + readOffset, readSize, flag, key);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            ret == URMA_SUCCESS, K_RUNTIME_ERROR,
            FormatString("Failed to urma read object with key = %zu, ret = %d", key, ret));

        remainSize -= readSize;
        readOffset += readSize;

        RETURN_IF_NOT_OK(CreateEvent(key, connection, jfs));
        keys.emplace_back(key);
    }
    return Status::OK();
}

Status UrmaManager::UrmaGatherWrite(const RemoteSegInfo &remoteInfo, const std::vector<LocalSgeInfo> &objInfos,
                                    bool blocking, std::vector<uint64_t> &eventKeys)
{
    eventKeys.clear();
    auto segVa = remoteInfo.segAddr;
    const HostPort requestAddress(remoteInfo.host, remoteInfo.port);
    const std::string remoteConnectionId = requestAddress.ToString();
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::const_accessor constAccessor;
    auto res = urmaConnectionMap_.find(constAccessor, remoteConnectionId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteConnectionId));
    auto &connection = constAccessor->second;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    UrmaRemoteSegmentMap::const_accessor remoteSegAccessor;
    RETURN_IF_NOT_OK(connection->GetRemoteSeg(segVa, remoteSegAccessor));
    auto sgeNum = objInfos.size();
    urma_sge_t srcSgeList[sgeNum];
    const auto wrSgeMaxNum = 13U;
    auto dstSgeNum = sgeNum % wrSgeMaxNum == 0 ? sgeNum / wrSgeMaxNum : sgeNum / wrSgeMaxNum + 1;
    urma_sge_t dstSgeList[dstSgeNum];
    urma_jfs_wr_t wrList[dstSgeNum];
    urma_jfs_wr_flag_t flag = { .value = 0 };
    flag.bs.complete_enable = 1;
    flag.bs.inline_flag = 0;
    auto totalWriteSize = 0U;
    std::shared_ptr<UrmaJfs> jfs;
    RETURN_IF_NOT_OK(GetJfsFromConnection(connection, jfs));
    INJECT_POINT("UrmaManager.GatherWriteError", []() { return Status(K_RUNTIME_ERROR, "Injcect urma wait error"); });
    for (auto dstSgeIdx = 0U, srcSgeIdx = 0U; dstSgeIdx < dstSgeNum; dstSgeIdx++) {
        auto singleDstWriteSize = 0;
        urma_sg_t srcSg = { .sge = &srcSgeList[srcSgeIdx], .num_sge = static_cast<uint32_t>(srcSgeIdx) };
        while (srcSgeIdx < sgeNum) {
            auto &ele = objInfos[srcSgeIdx];
            UrmaLocalSegmentMap::const_accessor localSegAccessor;
            RETURN_IF_NOT_OK(GetOrRegisterSegment(ele.segAddr, ele.segSize, localSegAccessor));
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localSegAccessor->second != nullptr, K_RUNTIME_ERROR,
                                                 "Local segment is null");
            srcSgeList[srcSgeIdx] = urma_sge_t{ .addr = ele.sgeAddr + ele.metaDataSize + ele.readOffset,
                                                .len = static_cast<uint32_t>(ele.writeSize),
                                                .tseg = localSegAccessor->second->Raw(),
                                                .user_tseg = NULL };
            singleDstWriteSize += srcSgeList[srcSgeIdx].len;
            srcSgeIdx++;
            if (srcSgeIdx % wrSgeMaxNum == 0) {
                break;
            }
        }
        srcSg.num_sge = srcSgeIdx - srcSg.num_sge;
        dstSgeList[dstSgeIdx] = { .addr = segVa + remoteInfo.segOffset + totalWriteSize,
                                  .len = static_cast<uint32_t>(singleDstWriteSize),
                                  .tseg = remoteSegAccessor->second->Raw(),
                                  .user_tseg = nullptr };
        totalWriteSize += singleDstWriteSize;
        urma_sg_t dstSg = { .sge = &dstSgeList[dstSgeIdx], .num_sge = 1 };
        urma_rw_wr_t rw = { .src = srcSg, .dst = dstSg, .target_hint = 0, .notify_data = 0 };
        const uint64_t key = requestId_.fetch_add(1);
        auto *importJfr = connection->GetTargetJfr();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(importJfr != nullptr, K_RUNTIME_ERROR,
                                             "Gather write got empty remote jfr.");
        auto &wr = wrList[dstSgeIdx];
        wr.opcode = URMA_OPC_WRITE;
        wr.flag = flag;
        wr.tjetty = importJfr;
        wr.user_ctx = key;
        wr.rw = rw;
        wr.next = NULL;
        RETURN_IF_NOT_OK(CreateEvent(key, connection, jfs));
        eventKeys.emplace_back(key);
        if (dstSgeIdx > 0) {
            wrList[dstSgeIdx - 1].next = &wrList[dstSgeIdx];
        }
    }
    urma_jfs_wr_t *bad_wr = NULL;
    Timer timer;
    auto ret = ds_urma_post_jfs_wr(jfs->Raw(), &wrList[0], &bad_wr);
    workerOperationTimeCost.Append("Urma gather write.", timer.ElapsedMilliSecond());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == URMA_SUCCESS, K_RUNTIME_ERROR,
                                         FormatString("Failed to urma write object, ret = %d", ret));
    if (blocking) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitFastTransportEvent(eventKeys, remainingTime, errorHandler));
        eventKeys.clear();
    }
    return Status::OK();
}

Status UrmaManager::UnimportSegment(const HostPort &remoteAddress, const uint64_t segmentAddress)
{
    TbbUrmaConnectionMap::accessor accessor;
    if (urmaConnectionMap_.find(accessor, remoteAddress.ToString())) {
        CHECK_FAIL_RETURN_STATUS(accessor->second != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
        RETURN_IF_NOT_OK(accessor->second->UnimportRemoteSeg(segmentAddress));
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, "Cannot unimport jfr, jfr is not imported");
}

Status UrmaManager::RemoveRemoteDevice(const std::string &deviceId)
{
    TbbUrmaConnectionMap::accessor accessor;
    if (urmaConnectionMap_.find(accessor, deviceId)) {
        LOG(INFO) << "Remove UrmaConnection for " << deviceId;
        urmaConnectionMap_.erase(accessor);
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND,
                  FormatString("Cannot remove UrmaConnection, connection for %s does not exist", deviceId));
}

Status UrmaManager::StrToEid(const std::string &eid, urma_eid_t &out)
{
    CHECK_FAIL_RETURN_STATUS(eid.size() == URMA_EID_SIZE, K_RUNTIME_ERROR,
                             FormatString("Eid size mismatch, expected: %d, actual: %d", URMA_EID_SIZE, eid.size()));
    auto rc = memcpy_s(out.raw, URMA_EID_SIZE, eid.data(), eid.size());
    CHECK_FAIL_RETURN_STATUS(rc == EOK, K_RUNTIME_ERROR,
                             FormatString("Unable to copy %d bytes, rc = %d, errno = %d", URMA_EID_SIZE, rc, errno));
    return Status::OK();
}

Status UrmaManager::CheckUrmaConnectionStable(const std::string &hostAddress, const std::string &instanceId)
{
    TbbUrmaConnectionMap::const_accessor constAccessor;
    auto res = urmaConnectionMap_.find(constAccessor, hostAddress);
    if (!res) {
        RETURN_STATUS(K_URMA_NEED_CONNECT, "No existing connection requires creation.");
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(constAccessor->second != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    if (!instanceId.empty()) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(constAccessor->second->GetUrmaJfrInfo().uniqueInstanceId == instanceId,
                                             K_URMA_NEED_CONNECT,
                                             "Urma connect has disconnected and needs to be reconnected!");
        return Status::OK();
    }
    RETURN_STATUS(K_URMA_NEED_CONNECT, "Urma connect unstable, need to reconnect!");
}

uint32_t UrmaManager::GetJfrIndex(const std::string &senderAddr)
{
    TbbJfrMap::accessor acc;
    auto res = urmaSenderRelationtable_.insert(acc, senderAddr);
    if (!res) {
        return acc->second;
    }

    if (FLAGS_urma_connection_size <= 0) {
        return 0;
    }

    uint32_t jfrIndex = localJfrIndex_.fetch_add(1) % FLAGS_urma_connection_size;
    acc->second = jfrIndex;
    return jfrIndex;
}

Status UrmaManager::ExchangeJfr(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp)
{
    if (UrmaManager::IsUrmaEnabled()) {
        auto &mgr = UrmaManager::Instance();
        UrmaJfrInfo urmaInfo;
        RETURN_IF_NOT_OK(urmaInfo.FromProto(req));
        LOG(INFO) << "Start import remote jfr, remote urma info: " << urmaInfo.ToString()
                  << ", local address:" << localUrmaInfo_.localAddress;
        // Only import remote jfr or segment for the remote node or client.
        if (localUrmaInfo_.localAddress != urmaInfo.localAddress || !req.client_id().empty() || clientMode_) {
            LOG_IF_ERROR(mgr.ImportRemoteJfr(urmaInfo), "Error in import incoming jfr");
            LOG_IF_ERROR(mgr.ImportRemoteInfo(req), "Error in import remote segments");
        }
        // Only fill response for client->worker handshake.
        if (!req.client_id().empty()) {
            uint32_t jfrIndex = GetJfrIndex(req.client_id());
            GetLocalUrmaInfo().ToProto(*rsp.mutable_hand_shake(), jfrIndex);
            RETURN_IF_NOT_OK(GetSegmentInfo(rsp));
            // Also record the client entity id for clean up purposes.
            std::lock_guard<std::mutex> lock(clientIdMutex_);
            clientIdMapping_.emplace(ClientKey::Intern(req.client_entity_id()), req.client_id());
        }
    }
    return Status::OK();
}

void UrmaManager::SetClientUrmaConfig(FastTransportMode urmaMode, uint64_t transportSize)
{
    // Note: The parameter needs to be consistent in the same client process.
    if (urmaMode == FastTransportMode::UB) {
        FLAGS_enable_urma = true;
        FLAGS_urma_mode = "UB";
        UrmaManager::clientMode_ = true;
        uint64_t expected = DEFAULT_TRANSPORT_MEM_SIZE;
        if (UrmaManager::ubTransportMemSize_.compare_exchange_strong(expected, transportSize)) {
            LOG(INFO) << "Set client UB transport memory size to " << transportSize;
        } else {
            LOG(WARNING) << FormatString(
                "Try to set client UB transport memory size to %lu, but it is already set to %lu", transportSize,
                UrmaManager::ubTransportMemSize_);
        }
        FLAGS_urma_connection_size = 1;
#ifdef BUILD_PIPLN_H2D
        FLAGS_enable_pipeline_h2d = true;
#endif
    }
}

std::string UrmaManager::GetRemoteDevicesByClientId(ClientKey clientEntityId)
{
    std::lock_guard<std::mutex> lock(clientIdMutex_);
    auto it = clientIdMapping_.find(clientEntityId);
    if (it != clientIdMapping_.end()) {
        return it->second;
    }
    LOG(INFO) << "Cannot find remote connection for client entity id: " << clientEntityId;
    return "";
}

const std::string &UrmaManager::GetClientId()
{
    return clientId_;
}

uint64_t UrmaManager::GetUBMaxGetDataSize()
{
    return ubMaxGetDataSize_;
}

uint64_t UrmaManager::GetUBMaxSetBufferSize()
{
    return ubMaxSetBufferSize_;
}

}  // namespace datasystem
