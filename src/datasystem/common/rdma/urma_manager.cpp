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
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_base.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/numa_util.h"
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
DS_DECLARE_bool(enable_urma_perf);

namespace datasystem {
namespace {
constexpr uint32_t K_URMA_WARNING_LOG_EVERY_N = 100;
constexpr uint32_t K_URMA_ERROR_LOG_EVERY_N = 100;
constexpr const char *RECV_JETTY_KEY_PREFIX = "recv:";

enum class UrmaErrorHandlePolicy {
    DEFAULT,  // just report error
    RECREATE_JETTY,
};

UrmaErrorHandlePolicy GetUrmaErrorHandlePolicy(int statusCode)
{
    static std::unordered_map<int, UrmaErrorHandlePolicy> urmaErrorHandlePolicyTable = {
        { 9, UrmaErrorHandlePolicy::RECREATE_JETTY },
    };

    const auto iter = urmaErrorHandlePolicyTable.find(statusCode);
    if (iter == urmaErrorHandlePolicyTable.end()) {
        return UrmaErrorHandlePolicy::DEFAULT;
    }
    return iter->second;
}
Status BuildRemoteJetty(const UrmaJfrInfo &info, urma_rjetty_t &remoteJetty)
{
    urma_eid_t eid{};
    RETURN_IF_NOT_OK(UrmaManager::StrToEid(info.eid, eid));
    remoteJetty.jetty_id.eid = eid;
    remoteJetty.jetty_id.uasid = info.uasid;
    remoteJetty.jetty_id.id = info.jfrId;
    remoteJetty.trans_mode = URMA_TM_RM;
    remoteJetty.type = URMA_JETTY;
    remoteJetty.tp_type = URMA_CTP;
    remoteJetty.flag.value = 0;
    return Status::OK();
}

std::string BuildLocalJettyKey(const std::string &connectionKey, JettyType jettyType)
{
    if (jettyType == JettyType::SEND) {
        return connectionKey;
    }
    return std::string(RECV_JETTY_KEY_PREFIX) + connectionKey;
}
}  // namespace

constexpr uint64_t MAX_STUB_CACHE_NUM = 2048;
constexpr uint64_t DEFAULT_TRANSPORT_MEM_SIZE = 128UL * 1024UL * 1024UL;
constexpr uint64_t MAX_TRANSPORT_MEM_SIZE = 2UL * 1024UL * 1024UL * 1024UL;

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
    // Zero the whole flag word before setting individual bitfields.
    // Setting only .bs.xxx members leaves uninitialized bits in the rest of the
    // word (other bitfields / padding / reserved), which become heap garbage
    // under bazel -O2 and can cause URMA driver crashes. Same pattern as Issue #12.
    registerSegmentFlag_.value = 0;
    importSegmentFlag_.value = 0;
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
    localJettyMap_.clear();
    {
        std::lock_guard<std::mutex> lock(clientIdMutex_);
        clientIdMapping_.clear();
    }
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
    if (perfThread_ && perfThread_->joinable()) {
        LOG(INFO) << "Waiting for Perf thread to exit";
        perfThread_->join();
        perfThread_.reset();
    }
    if (serverEventThread_ && serverEventThread_->joinable()) {
        LOG(INFO) << "Waiting for Event thread to exit";
        serverEventThread_->join();
        serverEventThread_.reset();
    }
    aeHandler_.Stop();
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
        LOG(INFO) << "UrmaManager initializing local URMA resources"
                  << (hostport.Empty() ? "" : FormatString(", hostport = %s", hostport.ToString()));
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
    const bool isBondingDevice = urmaDeviceName.find("bonding", 0) == 0;
    urma_device_t *urmaDevice = nullptr;
    RETURN_IF_NOT_OK(UrmaGetDeviceByName(urmaDeviceName, urmaDevice));
    if (eidIndex < 0) {
        RETURN_IF_NOT_OK(GetEidIndex(urmaDevice, eidIndex));
    }
    if (FLAGS_urma_connection_size != 0) {
        LOG(WARNING) << "Flag urma_connection_size is deprecated and ignored. "
                     << "JFS/JFR are now created per-connection.";
    }
    RETURN_IF_NOT_OK(urmaResource_->Init(urmaDevice, eidIndex, isBondingDevice));
    RETURN_IF_NOT_OK(InitLocalUrmaInfo(hostport));
    serverStop_ = false;
    serverEventThread_ = std::make_unique<Thread>(&UrmaManager::ServerEventHandleThreadMain, this);
    serverEventThread_->set_name("UrmaPollJfc");
    aeHandler_.Init(urmaResource_.get());
    aeHandler_.Start(serverStop_);

    // For client mode, we need to initialize extra memory buffer pool.
    if (UrmaManager::clientMode_) {
        clientId_ = GetStringUuid();
        RETURN_IF_NOT_OK(InitMemoryBufferPool());
        RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(MAX_STUB_CACHE_NUM, hostport));
    }
    if (FLAGS_enable_urma_perf) {
        perfThread_ = std::make_unique<std::thread>(&UrmaManager::PerfThreadMain, this);
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
    if (IsUbNumaAffinityEnabled()) {
        auto chipId = NumaIdToChipId(handler->GetNumaId());
        if (chipId != INVALID_CHIP_ID) {
            urmaInfo.set_chip_id(chipId);
        }
    }
    return Status::OK();
}

Status UrmaManager::InitLocalUrmaInfo(const HostPort &hostport)
{
    localUrmaInfo_.eid = GetEid();
    localUrmaInfo_.uasid = GetUasid();
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

Status UrmaManager::GetOrCreateLocalJetty(const std::string &key, uint32_t &jettyId, JettyType jettyType)
{
    const std::string jettyCacheKey = BuildLocalJettyKey(key, jettyType);
    TbbJettyMap::accessor accessor;
    auto inserted = localJettyMap_.insert(accessor, jettyCacheKey);
    if (!inserted && accessor->second != nullptr && accessor->second->IsValid()) {
        // Reuse existing Jetty for this target node (e.g. reconnection)
        jettyId = accessor->second->GetJettyId();
        LOG(INFO) << "Reusing local Jetty id " << jettyId << " for " << jettyCacheKey;
        return Status::OK();
    }
    if (!inserted && accessor->second != nullptr) {
        LOG(WARNING) << "Discard invalid local Jetty id " << accessor->second->GetJettyId() << " for " << jettyCacheKey;
        accessor->second.reset();
    }
    std::shared_ptr<UrmaJetty> jetty;
    auto rc = urmaResource_->CreateJetty(jetty, jettyType);
    if (rc.IsError()) {
        localJettyMap_.erase(accessor);
        return rc;
    }
    jettyId = jetty->GetJettyId();
    accessor->second = std::move(jetty);
    LOG(INFO) << "Created local Jetty id " << jettyId << " for " << jettyCacheKey
              << ", jettyType=" << (jettyType == JettyType::SEND ? "SEND" : "RECV");
    return Status::OK();
}

Status UrmaManager::GetTargetSeg(uint64_t segAddress, uint64_t segSize, const std::string &address,
                                 urma_target_seg_t **targetSeg, urma_jfr_t **targetJfr)
{
    UrmaLocalSegmentMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, accessor));
    *targetSeg = accessor->second->Raw();

    HostPort remoteSenderAddr;
    remoteSenderAddr.ParseString(address);
    std::string remoteConnectionId = remoteSenderAddr.ToString();
    const std::string recvJettyKey = BuildLocalJettyKey(remoteConnectionId, JettyType::RECV);
    uint32_t jettyId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        GetOrCreateLocalJetty(remoteConnectionId, jettyId, JettyType::RECV),
        FormatString("[GetTargetSeg] GetOrCreateLocalJetty for %s failed", remoteConnectionId));
    TbbJettyMap::accessor jettyAccessor;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localJettyMap_.find(jettyAccessor, recvJettyKey), K_RUNTIME_ERROR,
                                         FormatString("[GetTargetSeg] find jetty for %s failed", remoteConnectionId));

    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(jettyAccessor->second != nullptr, K_RUNTIME_ERROR,
                                         FormatString("[GetTargetSeg] Local Jetty is null for %s", remoteConnectionId));
    *targetJfr = jettyAccessor->second->SharedJfrRaw();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(*targetJfr != nullptr, K_RUNTIME_ERROR,
                                         FormatString("[GetTargetSeg] Local Jetty shared JFR is null for %s",
                                                      remoteConnectionId));
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
    PerfPoint point(PerfKey::URMA_GET_LOCAL_SEGMENT_INFO);
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
    PerfPoint point(PerfKey::URMA_GET_LOCAL_SEGMENT_INFO);
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

Status UrmaManager::PerfThreadMain()
{
    constexpr uint32_t perfBufferLen = 16 * 1024;
    constexpr int perfIntervalMs = 10000;  // 10s
    constexpr int sleepIntervalMs = 10;

    while (!serverStop_.load()) {
        urma_status_t ret = ds_urma_start_perf();
        if (ret != URMA_SUCCESS) {
            LOG_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N) << "[URMA_PERF] Failed to start perf, ret = " << ret;
        }

        Timer timer;
        while (timer.ElapsedMilliSecond() < perfIntervalMs && !serverStop_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepIntervalMs));
        }

        if (ret != URMA_SUCCESS) {
            continue;
        }

        ret = ds_urma_stop_perf();
        if (ret != URMA_SUCCESS) {
            LOG_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N) << "[URMA_PERF] Failed to stop perf, ret = " << ret;
            continue;
        }

        uint32_t len = perfBufferLen;
        std::vector<char> buffer(len);
        ret = ds_urma_get_perf_info(buffer.data(), &len);
        if (ret != URMA_SUCCESS) {
            LOG_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N) << "[URMA_PERF] Failed to get perf info, ret = " << ret;
            continue;
        }

        std::string msg(buffer.data(), len);
        LOG(INFO) << "[URMA_PERF]:\n" << msg;
    }

    return Status::OK();
}

Status UrmaManager::ServerEventHandleThreadMain()
{
    if (!Thread::SetCurrentThreadNice(FLAGS_io_thread_nice)) {
        LOG(WARNING) << "Failed to set nice for UrmaManager server event thread, nice=" << FLAGS_io_thread_nice
                     << ", errno=" << errno;
    }
    // Run this method until serverStop is called.
    while (!serverStop_.load()) {
        std::unordered_set<uint64_t> successCompletedReqs;
        std::unordered_map<uint64_t, int> failedCompletedReqs;
        Status rc = PollJfcWait(urmaResource_->GetJfc(), MAX_POLL_JFC_TRY_CNT, successCompletedReqs,
                                failedCompletedReqs, FLAGS_urma_poll_size);
        if (rc.IsError() && rc.GetCode() != K_TRY_AGAIN) {
            LOG_FIRST_AND_EVERY_N(ERROR, K_URMA_ERROR_LOG_EVERY_N)
                << "[URMA_POLL_ERROR] PollJfcWait failed: " << rc.ToString()
                << ", successCount=" << successCompletedReqs.size() << ", failedCount=" << failedCompletedReqs.size();
        }

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
    Timer timer;
    auto count = finishedRequests_.size();
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
            LOG(INFO) << "[UrmaEventHandler] Event is missing, dropping request id: " << requestId;
            // The event may already be removed by waiter cleanup; drop this finished request id.
            failedRequests_.erase(requestId);
            it = finishedRequests_.erase(it);
        }
    }
    if (timer.ElapsedMilliSecond() > 1) {
        LOG(INFO) << "[UrmaEventHandler]: Poll jfc notify elapsed = " << timer.ElapsedMilliSecond() << "ms,"
                  << ", cpuid: " << sched_getcpu() << ", count: " << count;
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
    const auto requestIdStr = std::to_string(static_cast<uint64_t>(requestId));
    RETURN_STATUS(K_NOT_FOUND, FormatString("Request id %s doesnt exist in event map", requestIdStr.c_str()));
}

Status UrmaManager::CreateEvent(uint64_t requestId, const std::shared_ptr<UrmaConnection> &connection,
                                const std::shared_ptr<UrmaJetty> &jetty, const std::string &remoteAddress,
                                UrmaEvent::OperationType operationType, std::shared_ptr<EventWaiter> waiter)
{
    if (!jetty->IsValid()) {
        RETURN_STATUS(K_URMA_ERROR, "Urma jetty is invalid");
    }
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::URMA_INFLIGHT_WR_COUNT))
        .Observe(static_cast<int64_t>(tbbEventMap_.size()));
    TbbEventMap::accessor mapAccessor;
    auto res = tbbEventMap_.insert(mapAccessor, requestId);
    if (!res) {
        // If this happens that means requestId is duplicated.
        const auto requestIdStr = std::to_string(static_cast<uint64_t>(requestId));
        RETURN_STATUS_LOG_ERROR(K_DUPLICATED,
                                FormatString("Request id %s already exists in event map", requestIdStr.c_str()));
    } else {
        const auto requestIdStr = std::to_string(static_cast<uint64_t>(requestId));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            connection != nullptr, K_RUNTIME_ERROR,
            FormatString("Urma connection is null. requestId=%s, remoteAddress=%s, op=%s", requestIdStr.c_str(),
                         remoteAddress.c_str(), UrmaEvent::OperationTypeName(operationType)));
        mapAccessor->second = std::make_shared<UrmaEvent>(
            requestId, jetty, remoteAddress, connection->GetUrmaJfrInfo().uniqueInstanceId, operationType, waiter);
    }
    return Status::OK();
}

Status UrmaManager::WaitToFinish(uint64_t requestId, int64_t timeoutMs)
{
    PerfPoint point(PerfKey::URMA_WAIT_TO_FINISH);
    INJECT_POINT("UrmaManager.UrmaWaitError", []() { return Status(K_URMA_WAIT_TIMEOUT, "Inject urma wait error"); });
    if (timeoutMs < 0) {
        const auto requestIdStr = std::to_string(static_cast<uint64_t>(requestId));
        RETURN_STATUS_LOG_ERROR(
            K_URMA_WAIT_TIMEOUT,
            FormatString("[URMA_WAIT_TIMEOUT] timedout waiting for request: %s", requestIdStr.c_str()));
    }
    METRIC_TIMER(metrics::KvMetricId::WORKER_URMA_WAIT_LATENCY);
    std::shared_ptr<UrmaEvent> event;
    RETURN_IF_NOT_OK(GetEvent(requestId, event));
    // use this unique request id as key to wait
    // wait until timeout

    Raii deleteEvent([this, &requestId]() { DeleteEvent(requestId); });

    VLOG(1) << "[UrmaEventHandler] Started waiting for the request id: " << requestId;
    PerfPoint waitPoint(PerfKey::URMA_WAIT_TIME);
    Timer timer;
    Status waitRc = event->WaitFor(std::chrono::milliseconds(timeoutMs));
    auto elapsedMs = timer.ElapsedMilliSecond();
    auto logLevel = elapsedMs > 1 ? 0 : 1;
    VLOG(logLevel) << "[UrmaEventHandler] URMA wait elapsed " << elapsedMs << "ms, request id:" << requestId
                   << ", cpuid:" << sched_getcpu() << ", status: " << waitRc.ToString();
    if (waitRc.GetCode() == StatusCode::K_RPC_DEADLINE_EXCEEDED) {
        return Status(K_URMA_WAIT_TIMEOUT,
                      FormatString("urma write deadline exceeded: %fms, %s", elapsedMs, waitRc.GetMsg()));
    }
    RETURN_IF_NOT_OK(waitRc);
    workerOperationTimeCost.Append("Urma wait time.", static_cast<uint64_t>(elapsedMs));
    waitPoint.Record();
    RETURN_IF_NOT_OK(HandleUrmaEvent(requestId, event));
    VLOG(1) << "[UrmaEventHandler] Done waiting for the request id: " << requestId;
    return Status::OK();
}

Status UrmaManager::HandleUrmaEvent(uint64_t requestId, const std::shared_ptr<UrmaEvent> &event)
{
    RETURN_OK_IF_TRUE(!event->IsFailed());

    const auto statusCode = event->GetStatusCode();
    const auto requestIdStr = std::to_string(static_cast<uint64_t>(requestId));
    auto errMsg = FormatString("Polling failed with an error for requestId: %s, cqe status: %d", requestIdStr.c_str(),
                               statusCode);

    return Status(K_URMA_ERROR, errMsg);
}

Status UrmaManager::GetJettyFromConnection(const std::shared_ptr<UrmaConnection> &connection,
                                           std::shared_ptr<UrmaJetty> &jetty)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");

    jetty = connection->GetJetty();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(jetty != nullptr && jetty->IsValid(), K_RUNTIME_ERROR,
                                         "Connection Jetty is unavailable or invalid");
    INJECT_POINT("UrmaManager.VerifyExclusiveJetty", [this, &connection, &jetty]() {
        for (auto iter = urmaConnectionMap_.begin(); iter != urmaConnectionMap_.end(); ++iter) {
            const auto &otherConnection = iter->second;
            if (otherConnection == nullptr || otherConnection.get() == connection.get()) {
                continue;
            }
            auto otherJetty = otherConnection->GetJetty();
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(otherJetty == nullptr || otherJetty.get() != jetty.get(),
                                                 K_RUNTIME_ERROR,
                                                 FormatString("Jetty id %u is unexpectedly shared with connection %s",
                                                              jetty->GetJettyId(), iter->first.c_str()));
        }
        return Status::OK();
    });
    VLOG(1) << "connection using jetty id " << jetty->GetJettyId();
    return Status::OK();
}

Status UrmaManager::TryRecoverFailedJettyFromCompletion(uint64_t requestId, int statusCode, uint32_t jettyId)
{
    const auto policy = GetUrmaErrorHandlePolicy(statusCode);
    if (policy != UrmaErrorHandlePolicy::RECREATE_JETTY) {
        return Status::OK();
    }

    std::shared_ptr<UrmaJetty> failedJetty;
    auto lookupRc = urmaResource_->GetJettyById(jettyId, failedJetty);
    if (lookupRc.IsError() || failedJetty == nullptr) {
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JETTY_SKIP] Completion Jetty " << jettyId << " is not found, requestId=" << requestId
            << ", cqeStatus=" << statusCode << ", rc=" << lookupRc.ToString();
        return Status::OK();
    }

    auto connection = failedJetty->GetConnection().lock();
    if (connection == nullptr) {
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JETTY_SKIP] Completion Jetty " << jettyId
            << " has no bound connection, requestId=" << requestId << ", cqeStatus=" << statusCode;
        return Status::OK();
    }

    LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
        << "[URMA_RECREATE_JETTY] Trigger from completion, requestId=" << requestId << ", jettyId=" << jettyId
        << ", cqeStatus=" << statusCode;
    return connection->ReCreateJetty(*urmaResource_, failedJetty);
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
        auto jettyId = completeRecords[i].local_id;
        if (crStatus == URMA_CR_WR_FLUSH_ERR_DONE) {
            LOG(INFO) << "[UrmaEventHandler] Write flush error done for request id: " << userCtx
                      << ", jetty id: " << jettyId;
            urmaResource_->AsyncDeleteJetty(jettyId);
        } else if (crStatus == URMA_CR_SUCCESS) {
            VLOG(1) << "[UrmaEventHandler] Got event with request id: " << userCtx << ", count:" << count
                    << ", cpuid: " << sched_getcpu();
            successCompletedReqs.insert(userCtx);
        } else {
            const auto requestIdStr = std::to_string(static_cast<uint64_t>(userCtx));
            LOG(ERROR) << FormatString("Failed to poll jfc requestId: %s CR.status: %d", requestIdStr.c_str(),
                                       crStatus);
            LOG_IF_ERROR(TryRecoverFailedJettyFromCompletion(userCtx, crStatus, jettyId),
                         FormatString("[URMA_RECREATE_JETTY_FAILED] requestId=%s, jettyId=%u, cqeStatus=%d",
                                      requestIdStr.c_str(), jettyId, crStatus));
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
        Timer timer;
        cnt = ds_urma_poll_jfc(urmaJfc, numPollCRS, completeRecords);
        if (timer.ElapsedMilliSecond() > 1) {
            LOG(INFO) << "[UrmaEventHandler]: Poll jfc elapsed = " << timer.ElapsedMilliSecond() << "ms"
                      << ", cpuid: " << sched_getcpu();
        }
        if (cnt == 0) {
            // If there is nothing to poll, just sleep.
            // Note that it takes on average 50us to wake up with usleep(0), due to OS timerslack settings.
            Timer sleepTimer;
            usleep(0);
            if (sleepTimer.ElapsedMilliSecond() > 1) {
                LOG(INFO) << "[UrmaEventHandler]: Poll jfc sleep elapsed = " << sleepTimer.ElapsedMilliSecond() << "ms"
                          << ", cpuid: " << sched_getcpu();
            }
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

Status UrmaManager::ImportRemoteJetty(const UrmaJfrInfo &jfrInfo, uint32_t &localJettyId)
{
    PerfPoint point(PerfKey::URMA_SETUP_CONNECTION);
    const std::string remoteConnectionId =
        jfrInfo.clientId.empty() ? jfrInfo.localAddress.ToString() : jfrInfo.clientId;
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    // Insert or update the connection (in case the sending worker restarts)
    TbbUrmaConnectionMap::accessor accessor;
    auto res = urmaConnectionMap_.insert(accessor, remoteConnectionId);
    if (!res && accessor->second != nullptr) {
        if (accessor->second->GetUrmaJfrInfo().ToString() == jfrInfo.ToString()) {
            // Identical connection already exists, return existing local Jetty ID
            RETURN_IF_NOT_OK(GetOrCreateLocalJetty(remoteConnectionId, localJettyId, JettyType::RECV));
            return Status::OK();
        }
        accessor->second->Clear();
    }
    bool success = false;
    Raii raii([&success, &accessor, this]() {
        if (!success) {
            urmaConnectionMap_.erase(accessor);
        }
    });

    // Create per-connection JETTY and import the remote JETTY as target JETTY
    std::shared_ptr<UrmaJetty> jetty;
    RETURN_IF_NOT_OK(urmaResource_->CreateJetty(jetty));
    std::unique_ptr<UrmaTargetJetty> targetJetty;
    RETURN_IF_NOT_OK(ImportTargetJetty(jfrInfo, targetJetty, jetty->Raw()));

    // Get or create a local JETTY for this connection (reused across reconnections)
    RETURN_IF_NOT_OK(GetOrCreateLocalJetty(remoteConnectionId, localJettyId, JettyType::RECV));

    accessor->second = std::make_shared<UrmaConnection>(jetty, std::move(targetJetty), jfrInfo);
    jetty->BindConnection(accessor->second);
    success = true;
    return Status::OK();
}

Status UrmaManager::ImportRemoteInfo(const UrmaHandshakeReqPb &req)
{
    PerfPoint point(PerfKey::URMA_SETUP_CONNECTION);
    const HostPort requestAddress(req.address().host(), req.address().port());
    const std::string remoteConnectionId = req.client_id().empty() ? requestAddress.ToString() : req.client_id();
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::accessor accessor;
    // The comm layer (zmq) has already exchanged the jfr, and we should be able to locate the entry.
    auto res = urmaConnectionMap_.find(accessor, remoteConnectionId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteConnectionId));

    point.RecordAndReset(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
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
    return Status::OK();
}

Status UrmaManager::ImportTargetJetty(const UrmaJfrInfo &remoteInfo, std::unique_ptr<UrmaTargetJetty> &targetJetty,
    urma_jetty_t *localJetty)
{
    LOG(INFO) << "Begin to import target jft.";
    bondp_rjetty_t bondpRemoteJetty{};
    urma_rjetty_t remoteJetty{};
    RETURN_IF_NOT_OK(BuildRemoteJetty(remoteInfo, remoteJetty));
    bondpRemoteJetty.base = remoteJetty;
    bondpRemoteJetty.jetty = localJetty;
    bondpRemoteJetty.base.flag.bs.has_drv_ext = 1;
    Timer timer;
    METRIC_TIMER(metrics::KvMetricId::URMA_IMPORT_JFR);
    RETURN_IF_NOT_OK_APPEND_MSG(
        UrmaTargetJetty::Import(urmaResource_->GetContext(), &(bondpRemoteJetty.base), urmaResource_->GetUrmaToken(),
            targetJetty),
        FormatString("Failed to import target jetty, %s", remoteInfo.ToString()));
    if (timer.ElapsedMilliSecond() > 1) {
        LOG(INFO) << "[UrmaImportJetty] Import target jetty elapsed = " << timer.ElapsedMilliSecond() << "ms"
                  << ", cpuid: " << sched_getcpu();
    }
    return Status::OK();
}

Status UrmaManager::FinalizeOutboundConnection(const UrmaHandshakeRspPb &rsp)
{
    PerfPoint point(PerfKey::URMA_FINALIZE_OUTBOUND_CONNECTION);
    CHECK_FAIL_RETURN_STATUS(rsp.has_hand_shake(), K_INVALID, "UrmaHandshakeRspPb has no hand_shake");

    const auto &handShake = rsp.hand_shake();
    UrmaJfrInfo remoteInfo;
    RETURN_IF_NOT_OK(remoteInfo.FromProto(handShake));
    LOG(INFO) << "Start import remote jetty, remote urma info: " << remoteInfo.ToString()
              << ", local address:" << localUrmaInfo_.localAddress;

    const HostPort requestAddress(handShake.address().host(), handShake.address().port());
    const std::string remoteConnectionId = requestAddress.ToString();

    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::accessor accessor;
    auto res = urmaConnectionMap_.insert(accessor, remoteConnectionId);
    if (!res && accessor->second != nullptr) {
        RETURN_OK_IF_TRUE(accessor->second->GetUrmaJfrInfo().ToString() == remoteInfo.ToString());
        accessor->second->Clear();
    }
    bool success = false;
    Raii raii([&success, &accessor, this]() {
        if (!success) {
            urmaConnectionMap_.erase(accessor);
        }
    });

    // Create per-connection JETTY and import remote JETTY as target JETTY
    std::shared_ptr<UrmaJetty> jetty;
    RETURN_IF_NOT_OK(urmaResource_->CreateJetty(jetty));
    std::unique_ptr<UrmaTargetJetty> targetJetty;
    RETURN_IF_NOT_OK(ImportTargetJetty(remoteInfo, targetJetty, jetty->Raw()));

    accessor->second = std::make_shared<UrmaConnection>(jetty, std::move(targetJetty), remoteInfo);
    jetty->BindConnection(accessor->second);
    auto connection = accessor->second;

    // Import remote segments
    PerfPoint segPoint(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
    for (int i = 0; i < handShake.seg_infos_size(); i++) {
        auto &segInfo = handShake.seg_infos(i);
        RETURN_IF_NOT_OK_APPEND_MSG(connection->ImportRemoteSeg(segInfo, urmaResource_->GetContext(),
                                                                urmaResource_->GetUrmaToken(), importSegmentFlag_),
                                    "Failed to import remote segment in FinalizeOutboundConnection");
    }
    segPoint.Record();
    success = true;
    point.Record();
    return Status::OK();
}

uint64_t UrmaManager::GenerateReqId()
{
    return requestId_.fetch_add(1);
}

static urma_status_t PostJettyRw(urma_jetty_t *jetty, urma_opcode_t opcode, urma_target_jetty_t *targetJetty,
                                 urma_target_seg_t *remoteSeg, urma_target_seg_t *localSeg, uint64_t remoteAddress,
                                 uint64_t localAddress, uint64_t length, urma_jfs_wr_flag_t flag, uint64_t userCtx,
                                 bool useNumaAffinity, uint32_t src_chip_id, uint32_t dst_chip_id)
{
    urma_sge_t localSge{ .addr = localAddress, .len = static_cast<uint32_t>(length), .tseg = localSeg,
                         .user_tseg = nullptr };
    urma_sge_t remoteSge{ .addr = remoteAddress, .len = static_cast<uint32_t>(length), .tseg = remoteSeg,
                          .user_tseg = nullptr };

    urma_sg_t src{};
    urma_sg_t dst{};
    if (opcode == URMA_OPC_READ) {
        src = { .sge = &remoteSge, .num_sge = 1 };
        dst = { .sge = &localSge, .num_sge = 1 };
    } else {
        src = { .sge = &localSge, .num_sge = 1 };
        dst = { .sge = &remoteSge, .num_sge = 1 };
    }

    urma_jfs_wr_t *badWr = nullptr;
    if (useNumaAffinity) {
        bondp_jfs_wr_t bondp_wr{};
        urma_jfs_wr_t *base = &bondp_wr.base;
        base->opcode = opcode;
        base->flag = flag;
        base->tjetty = targetJetty;
        base->user_ctx = userCtx;
        base->rw = { .src = src, .dst = dst, .target_hint = 0, .notify_data = 0 };
        base->next = nullptr;
        bondp_wr.src_chip_id = src_chip_id;
        bondp_wr.dst_chip_id = dst_chip_id;
        
        return ds_urma_post_jetty_send_wr(jetty, base, &badWr);
    } else {
        urma_jfs_wr_t wr{};
        wr.opcode = opcode;
        wr.flag = flag;
        wr.tjetty = targetJetty;
        wr.user_ctx = userCtx;
        wr.rw = { .src = src, .dst = dst, .target_hint = 0, .notify_data = 0 };
        wr.next = nullptr;
        return ds_urma_post_jetty_send_wr(jetty, &wr, &badWr);
    }
}

Status UrmaManager::UrmaWriteImpl(const UrmaWriteArgs &args, std::vector<uint64_t> &eventKeys)
{
    urma_jfs_wr_flag_t flag{};
    flag.bs.complete_enable = 1;
    const bool useNumaAffinity =
        IsUbNumaAffinityEnabled() && args.srcChipId != INVALID_CHIP_ID && args.dstChipId != INVALID_CHIP_ID;

    uint64_t writtenSize = 0;
    uint64_t remainSize = args.size;
    Timer timer;
    while (remainSize > 0) {
        const uint64_t writeSize = std::min(remainSize, urmaResource_->GetMaxWriteSize());
        const uint64_t key = requestId_.fetch_add(1);
        const uint64_t remoteAddress = args.remoteDataAddress + writtenSize;
        const uint64_t localAddress = args.localDataAddress + writtenSize;
        PerfPoint pointWrite(PerfKey::URMA_WRITE_SINGLE);
        INJECT_POINT("UrmaManager.UrmaWriteError",
                     []() { return Status(K_RUNTIME_ERROR, "Injcect urma write error"); });
        RETURN_IF_NOT_OK(CreateEvent(key, args.connection, args.jetty, args.remoteAddress,
                                     UrmaEvent::OperationType::WRITE, args.waiter));
        urma_status_t ret;
        Timer t;
        METRIC_TIMER(metrics::KvMetricId::WORKER_URMA_WRITE_LATENCY);
        if (useNumaAffinity) {
            VLOG(1) << "URMA write numa affinity src=" << static_cast<uint32_t>(args.srcChipId)
                    << ", dst=" << static_cast<uint32_t>(args.dstChipId);
            ret = PostJettyRw(args.jetty->Raw(), URMA_OPC_WRITE, args.targetJetty, args.remoteSeg, args.localSeg,
                remoteAddress, localAddress, writeSize, flag, key, true, args.srcChipId, args.dstChipId);
        } else {
            ret = PostJettyRw(args.jetty->Raw(), URMA_OPC_WRITE, args.targetJetty, args.remoteSeg, args.localSeg,
                remoteAddress, localAddress, writeSize, flag, key, false, args.srcChipId, args.dstChipId);
        }

        if (ret != URMA_SUCCESS) {
            DeleteEvent(key);
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR,
                                    FormatString("Failed to urma write object with key = %zu, ret = %d", key, ret));
        }
        VLOG(1) << "[UrmaWrite] URMA finish write, cpuid:" << sched_getcpu() << ", elapsed:" << t.ElapsedMilliSecond()
                << ", request id:" << key;
        pointWrite.Record();
        remainSize -= writeSize;
        writtenSize += writeSize;
        eventKeys.emplace_back(key);
    }
    workerOperationTimeCost.Append("Urma total write.", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status UrmaManager::UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                     const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                     const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                     uint8_t srcChipId, uint8_t dstChipId, bool blocking,
                                     std::vector<uint64_t> &eventKeys, std::shared_ptr<EventWaiter> waiter)
{
    eventKeys.clear();
    PerfPoint point(PerfKey::URMA_WRITE_TOTAL);
    const uint64_t segVa = urmaInfo.seg_va();
    const HostPort requestAddress(urmaInfo.request_address().host(), urmaInfo.request_address().port());
    const std::string remoteAddress = requestAddress.ToString();
    std::string remoteConnectionId = urmaInfo.client_id().empty() ? requestAddress.ToString() : urmaInfo.client_id();

    point.RecordAndReset(PerfKey::URMA_WRITE_FIND_CONNECTION);
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

    point.RecordAndReset(PerfKey::URMA_WRITE_FIND_REMOTE_SEGMENT);
    auto &connection = constAccessor->second;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    UrmaRemoteSegmentMap::const_accessor remoteSegAccessor;
    RETURN_IF_NOT_OK(connection->GetRemoteSeg(segVa, remoteSegAccessor));

    point.RecordAndReset(PerfKey::URMA_WRITE_REGISTER_LOCAL_SEGMENT);
    UrmaLocalSegmentMap::const_accessor localSegAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(localSegAddress, localSegSize, localSegAccessor));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localSegAccessor->second != nullptr, K_RUNTIME_ERROR, "Local segment is null");

    // Get local and remote jettys.
    std::shared_ptr<UrmaJetty> jetty;
    RETURN_IF_NOT_OK(GetJettyFromConnection(connection, jetty));
    auto *targetJetty = connection->GetTargetJetty();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(targetJetty != nullptr, K_RUNTIME_ERROR, "Write got empty remote jetty.");

    point.RecordAndReset(PerfKey::URMA_WRITE_LOOP);

    if (OsXprtPipln::IsPiplnH2DRequest(urmaInfo)) {
        OsXprtPipln::PiplnSndArgs args;
        args.jetty = jetty->Raw();
        args.tjetty = targetJetty;
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

    UrmaWriteArgs writeLoopArgs;
    writeLoopArgs.connection = connection;
    writeLoopArgs.jetty = jetty;
    writeLoopArgs.waiter = waiter;
    writeLoopArgs.remoteAddress = remoteAddress;
    writeLoopArgs.targetJetty = targetJetty;
    writeLoopArgs.remoteSeg = remoteSegAccessor->second->Raw();
    writeLoopArgs.localSeg = localSegAccessor->second->Raw();
    writeLoopArgs.remoteDataAddress = segVa + urmaInfo.seg_data_offset() + readOffset;
    writeLoopArgs.localDataAddress = localObjectAddress + readOffset + metaDataSize;
    writeLoopArgs.size = readSize;
    writeLoopArgs.srcChipId = srcChipId;
    writeLoopArgs.dstChipId = dstChipId;
    RETURN_IF_NOT_OK(UrmaWriteImpl(writeLoopArgs, eventKeys));
    point.Record();
    // If it is blocking wait, we will wait for the write to finish here.
    if (blocking) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRemainingTime(); };
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
    const std::string remoteAddress = requestAddress.ToString();
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

    urma_jfs_wr_flag_t flag{};
    flag.bs.complete_enable = 1;

    std::shared_ptr<UrmaJetty> jetty;
    RETURN_IF_NOT_OK(GetJettyFromConnection(connection, jetty));
    auto *targetJetty = connection->GetTargetJetty();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(targetJetty != nullptr, K_RUNTIME_ERROR, "Read got empty remote jetty.");

    uint64_t readOffset = 0;
    uint64_t remainSize = dataSize;
    while (remainSize > 0) {
        const uint64_t readSize = std::min(remainSize, urmaResource_->GetMaxReadSize());
        const uint64_t key = requestId_.fetch_add(1);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localSegAccessor->second != nullptr, K_RUNTIME_ERROR,
                                             "Local segment is null");
        RETURN_IF_NOT_OK(CreateEvent(key, connection, jetty, remoteAddress, UrmaEvent::OperationType::READ));
        urma_status_t ret =
            PostJettyRw(jetty->Raw(), URMA_OPC_READ, targetJetty, remoteSegAccessor->second->Raw(),
                        localSegAccessor->second->Raw(), segVa + urmaInfo.seg_data_offset() + readOffset,
                        localObjectAddress + metaDataSize + readOffset, readSize, flag, key, false, INVALID_CHIP_ID,
                        INVALID_CHIP_ID);
        if (ret != URMA_SUCCESS) {
            DeleteEvent(key);
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR,
                                    FormatString("Failed to urma read object with key = %zu, ret = %d", key, ret));
        }

        remainSize -= readSize;
        readOffset += readSize;

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
    const std::string remoteAddress = requestAddress.ToString();
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
    std::shared_ptr<UrmaJetty> jetty;
    RETURN_IF_NOT_OK(GetJettyFromConnection(connection, jetty));
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
        auto *targetJetty = connection->GetTargetJetty();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(targetJetty != nullptr, K_RUNTIME_ERROR,
                                             "Gather write got empty remote jetty.");
        auto &wr = wrList[dstSgeIdx];
        wr.opcode = URMA_OPC_WRITE;
        wr.flag = flag;
        wr.tjetty = targetJetty;
        wr.user_ctx = key;
        wr.rw = rw;
        wr.next = NULL;
        RETURN_IF_NOT_OK(CreateEvent(key, connection, jetty, remoteAddress, UrmaEvent::OperationType::WRITE));
        eventKeys.emplace_back(key);
        if (dstSgeIdx > 0) {
            wrList[dstSgeIdx - 1].next = &wrList[dstSgeIdx];
        }
    }
    urma_jfs_wr_t *bad_wr = NULL;
    Timer timer;
    auto ret = ds_urma_post_jetty_send_wr(jetty->Raw(), &wrList[0], &bad_wr);
    workerOperationTimeCost.Append("Urma gather write.", timer.ElapsedMilliSecond());
    if (ret != URMA_SUCCESS) {
        for (auto key : eventKeys) {
            DeleteEvent(key);
        }
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma write object, ret = %d", ret));
    }
    if (blocking) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitFastTransportEvent(eventKeys, remainingTime, errorHandler));
        eventKeys.clear();
    }
    return Status::OK();
}

Status UrmaManager::RemoveRemoteResources(const std::string &connectionKey, bool removeLocalJfr)
{
    bool removed = false;

    TbbUrmaConnectionMap::accessor connectionAccessor;
    if (urmaConnectionMap_.find(connectionAccessor, connectionKey)) {
        LOG(INFO) << "Remove UrmaConnection for " << connectionKey;
        urmaConnectionMap_.erase(connectionAccessor);
        removed = true;
    }

    std::string recvKey = BuildLocalJettyKey(connectionKey, JettyType::RECV);
    if (removeLocalJfr) {
        TbbJettyMap::accessor jettyAccessor;
        if (localJettyMap_.find(jettyAccessor, recvKey)) {
            LOG(INFO) << "Remove local Jetty for " << recvKey;
            localJettyMap_.erase(jettyAccessor);
            removed = true;
        }
    }

    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        removed, K_NOT_FOUND,
        FormatString("Cannot remove URMA resources, connection key %s does not exist", connectionKey.c_str()));
    return Status::OK();
}

Status UrmaManager::RemoveRemoteDevice(const std::string &deviceId)
{
    return RemoveRemoteResources(deviceId, false);
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
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_NEED_CONNECT] No existing connection for remoteAddress: " << hostAddress
            << ", remoteInstanceId=" << (instanceId.empty() ? "UNKNOWN" : instanceId) << ", requires creation.";
        RETURN_STATUS(K_URMA_NEED_CONNECT, "No existing connection requires creation.");
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        constAccessor->second != nullptr, K_RUNTIME_ERROR,
        FormatString("Urma connection is null. remoteAddress=%s, remoteInstanceId=%s", hostAddress.c_str(),
                     instanceId.empty() ? "UNKNOWN" : instanceId.c_str()));
    if (!instanceId.empty()) {
        const auto &cachedInstanceId = constAccessor->second->GetUrmaJfrInfo().uniqueInstanceId;
        if (cachedInstanceId != instanceId) {
            LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
                << "[URMA_NEED_CONNECT] Connection stale for remoteAddress: " << hostAddress
                << ", cachedRemoteInstanceId=" << cachedInstanceId << ", requestRemoteInstanceId=" << instanceId
                << ", need reconnect.";
            RETURN_STATUS(K_URMA_NEED_CONNECT, "Urma connect has disconnected and needs to be reconnected!");
        }
        return Status::OK();
    }
    LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
        << "[URMA_NEED_CONNECT] Connection unstable for remoteAddress: " << hostAddress
        << ", remoteInstanceId=UNKNOWN, need to reconnect.";
    RETURN_STATUS(K_URMA_NEED_CONNECT, "Urma connect unstable, need to reconnect!");
}

Status UrmaManager::ExchangeJfr(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp)
{
    UrmaJfrInfo urmaInfo;
    RETURN_IF_NOT_OK(urmaInfo.FromProto(req));
    LOG(INFO) << "Start import remote jetty, remote urma info: " << urmaInfo.ToString()
              << ", local address:" << localUrmaInfo_.localAddress;
    // Only import remote jetty or segment for the remote node or client.
    if (localUrmaInfo_.localAddress != urmaInfo.localAddress || !req.client_id().empty() || clientMode_) {
        uint32_t localJettyId = 0;
        RETURN_IF_NOT_OK(ImportRemoteJetty(urmaInfo, localJettyId));
        RETURN_IF_NOT_OK(ImportRemoteInfo(req));

        // Always populate response with local Jetty ID for the reverse connection
        auto localInfo = localUrmaInfo_;
        localInfo.jfrId = localJettyId;
        localInfo.ToProto(*rsp.mutable_hand_shake());
        RETURN_IF_NOT_OK(GetSegmentInfo(rsp));
    }
    // Record the client entity id for clean up purposes.
    if (!req.client_id().empty()) {
        std::lock_guard<std::mutex> lock(clientIdMutex_);
        clientIdMapping_[ClientKey::Intern(req.client_entity_id())] = req.client_id();
    }
    return Status::OK();
}

void UrmaManager::SetClientUrmaConfig(FastTransportMode urmaMode, uint64_t transportSize)
{
    // Note: The parameter needs to be consistent in the same client process.
    if (urmaMode == FastTransportMode::UB) {
        FLAGS_enable_urma = true;
        FLAGS_urma_mode = "UB";
        FLAGS_enable_ub_numa_affinity = true;
        UrmaManager::clientMode_ = true;
        uint64_t expected = DEFAULT_TRANSPORT_MEM_SIZE;
        if (UrmaManager::ubTransportMemSize_.compare_exchange_strong(expected, transportSize)) {
            LOG(INFO) << "Set client UB transport memory size to " << transportSize;
        } else {
            LOG(WARNING) << FormatString(
                "Try to set client UB transport memory size to %lu, but it is already set to %lu", transportSize,
                UrmaManager::ubTransportMemSize_);
        }
#ifdef BUILD_PIPLN_H2D
        // pipeline h2d flag should be set for client to init pipeline env
        FLAGS_enable_pipeline_h2d = true;
        FLAGS_pipeline_h2d_thread_num = 0;
#endif
        // FLAGS_urma_connection_size is deprecated; JFS/JFR are created per-connection.
    }
}

Status UrmaManager::RemoveRemoteClient(ClientKey clientEntityId)
{
    std::string remoteConnectionId;
    {
        std::lock_guard<std::mutex> lock(clientIdMutex_);
        auto it = clientIdMapping_.find(clientEntityId);
        if (it == clientIdMapping_.end()) {
            RETURN_STATUS(K_NOT_FOUND, FormatString("Cannot find remote connection for client entity id: %s",
                                                    clientEntityId.Data()));
        }
        remoteConnectionId = it->second;
        clientIdMapping_.erase(it);
    }
    LOG(INFO) << "Remove URMA resources for client " << clientEntityId << ", connection key " << remoteConnectionId;
    return RemoveRemoteResources(remoteConnectionId, true);
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
