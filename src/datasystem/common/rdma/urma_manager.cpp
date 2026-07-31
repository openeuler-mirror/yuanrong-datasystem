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

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <vector>

#include <sys/mman.h>

#ifndef USE_URMA_MOCK
#include <ub/umdk/urma/urma_opcode.h>
#endif

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_base.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/numa_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/sched_runtime.h"
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
constexpr uint32_t K_URMA_WARNING_LOG_EVERY_N = 100;
constexpr uint32_t K_URMA_ERROR_LOG_EVERY_N = 100;
constexpr uint32_t URMA_LOG_LIMIT_MS = 1;
constexpr uint32_t URMA_LOG_LIMIT_US = 250;
constexpr uint32_t URMA_WRITE_VLOG0_LIMIT_US = 200;
constexpr size_t URMA_CHIP_INFLIGHT_TRACKED_COUNT = 10;
constexpr size_t URMA_CHIP_INFLIGHT_LOG_BUFFER_SIZE = 160;
constexpr const char *URMA_ELAPSED_TOTAL_SUGGEST =
    "check whether URMA_ELAPSED_THREAD_SHED/URMA_ELAPSED_POLL_JFC/URMA_ELAPSED_NOTIFY logs appear in the "
    "same time window; if none appear, check URMA and UDMA";
constexpr const char *URMA_ELAPSED_THREAD_SCHED_SUGGEST = "check OS scheduling overhead";
constexpr const char *URMA_ELAPSED_POLL_JFC_SUGGEST = "check URMA";
constexpr const char *URMA_ELAPSED_NOTIFY_SUGGEST = "check OS scheduling overhead";
constexpr const char *URMA_ERROR_SUGGEST = "check URMA";

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

}  // namespace

constexpr uint64_t MAX_STUB_CACHE_NUM = 2048;
constexpr uint64_t DEFAULT_TRANSPORT_MEM_SIZE = 256UL * 1024UL * 1024UL;
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

    registerSegmentFlag_.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    importSegmentFlag_.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    localSegmentMap_ = std::make_unique<UrmaLocalSegmentMap>();
    urmaResource_ = std::make_unique<UrmaResource>();
    srcChipInflightWrCounts_ = std::vector<std::atomic<int>>(URMA_CHIP_INFLIGHT_TRACKED_COUNT);
    for (auto &count : srcChipInflightWrCounts_) {
        count.store(0, std::memory_order_relaxed);
    }
}

UrmaManager::~UrmaManager()
{
    Stop();
    OsXprtPipln::UnInitOsPiplnRH2DEnv();
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
    // Close every admission gate while poll is still alive. We deliberately fail closed during
    // teardown if a provider flush cannot be observed; deleting a possibly live Jetty is unsafe.
    if (urmaResource_ != nullptr) {
        urmaResource_->BeginShutdown();
        urmaResource_->WaitForPostPermitsDrained();
    }
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

Status UrmaManager::GetUrmaDeviceName(std::string &urmaDeviceName, int &eidIndex)
{
    urmaDeviceName = GetStringFromEnv(ENV_UB_DEVICE_NAME.c_str(), DEFAULT_UB_DEVICE_NAME.c_str());
    eidIndex = GetInt32FromEnv(ENV_UB_DEVICE_EID.c_str(), 0);
    if (urmaDeviceName.empty()) {
        RETURN_STATUS(K_INVALID, "env DS_URMA_DEV_NAME is empty");
    }
    RETURN_IF_NOT_OK(UrmaGetEffectiveDevice(urmaDeviceName));
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
    RETURN_IF_NOT_OK(GetUrmaDeviceName(urmaDeviceName, eidIndex));
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
    OsXprtPipln::SetIsClientMode(clientMode_);
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
    perfThread_ = std::make_unique<std::thread>(&UrmaManager::PerfThreadMain, this);
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
    INJECT_POINT("UrmaManager.GetMemoryBufferHandle");
    std::shared_ptr<ShmUnit> unit = std::make_shared<ShmUnit>();
    RETURN_IF_NOT_OK(
        unit->AllocateMemory(DEFAULT_TENANTID, size, false, ServiceType::OBJECT, AllocateType::UB_TRANSPORT));

    handle = std::make_shared<BufferHandle>(unit, memoryBuffer_, size);
    return Status::OK();
}

Status UrmaManager::FillRemoteAddr(const BufferHandle &handle, UrmaRemoteAddrPb &urmaInfo)
{
    RETURN_RUNTIME_ERROR_IF_NULL(memoryBuffer_);
    urmaInfo.set_seg_va(reinterpret_cast<uint64_t>(memoryBuffer_));
    urmaInfo.set_seg_data_offset(handle.GetOffset());
    auto *requestAddr = urmaInfo.mutable_request_address();
    requestAddr->set_host(localUrmaInfo_.localAddress.Host());
    requestAddr->set_port(localUrmaInfo_.localAddress.Port());
    if (!GetClientId().empty()) {
        urmaInfo.set_client_id(GetClientId());
    }
    if (IsUbNumaAffinityEnabled()) {
        auto chipId = NumaIdToChipId(handle.GetNumaId());
        if (chipId != INVALID_CHIP_ID) {
            urmaInfo.set_chip_id(chipId);
        }
    }
    return Status::OK();
}

Status UrmaManager::GetMemoryBufferInfo(std::shared_ptr<UrmaManager::BufferHandle> &handler, uint8_t *&bufferPtr,
                                        uint64_t &bufferSize, UrmaRemoteAddrPb &urmaInfo)
{
    bufferPtr = reinterpret_cast<uint8_t *>(handler->GetPointer());
    bufferSize = handler->GetSegmentSize();
    return FillRemoteAddr(*handler, urmaInfo);
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
    if (jettyType == JettyType::RECV) {
        std::shared_ptr<UrmaJetty> jetty;
        RETURN_IF_NOT_OK(urmaResource_->GetOrCreateSharedRecvJetty(jetty));
        jettyId = jetty->GetJettyId();
        LOG(INFO) << "Using shared recv Jetty id " << jettyId << " for " << key;
        return Status::OK();
    }

    const std::string &jettyCacheKey = key;
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

Status UrmaManager::GetLocalJetty(const std::string &key, std::shared_ptr<UrmaJetty> &jetty, JettyType jettyType)
{
    if (jettyType == JettyType::RECV) {
        RETURN_IF_NOT_OK_APPEND_MSG(urmaResource_->GetOrCreateSharedRecvJetty(jetty),
                                    FormatString("Failed to get shared recv Jetty for %s", key.c_str()));
        return Status::OK();
    }

    const std::string &jettyCacheKey = key;
    TbbJettyMap::const_accessor accessor;
    if (!localJettyMap_.find(accessor, jettyCacheKey) || accessor->second == nullptr) {
        RETURN_STATUS(K_URMA_NEED_CONNECT, FormatString("Local jetty not found for %s", jettyCacheKey.c_str()));
    }
    if (!accessor->second->IsValid()) {
        RETURN_STATUS(K_URMA_NEED_CONNECT, FormatString("Local jetty is invalid for %s", jettyCacheKey.c_str()));
    }
    jetty = accessor->second;
    return Status::OK();
}

Status UrmaManager::GetTargetSeg(uint64_t segAddress, uint64_t segSize, const std::string &address,
                                 urma_target_seg_t **targetSeg, urma_jfr_t **targetJfr, urma_jetty_t **targetJetty)
{
    UrmaLocalSegmentMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, accessor));
    *targetSeg = accessor->second->Raw();

    HostPort remoteSenderAddr;
    remoteSenderAddr.ParseString(address);
    std::string remoteConnectionId = remoteSenderAddr.ToString();

    TbbUrmaConnectionMap::const_accessor connectionAccessor;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        urmaConnectionMap_.find(connectionAccessor, remoteConnectionId) && connectionAccessor->second != nullptr,
        K_URMA_NEED_CONNECT,
        FormatString("[GetTargetSeg] No exchanged URMA connection for %s; cannot use exchanged recv Jetty",
                     remoteConnectionId));

    std::shared_ptr<UrmaJetty> recvJetty;
    RETURN_IF_NOT_OK_APPEND_MSG(urmaResource_->GetOrCreateSharedRecvJetty(recvJetty),
                                FormatString("[GetTargetSeg] Failed to get shared recv Jetty for %s",
                                             remoteConnectionId));

    *targetJetty = recvJetty->Raw();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        *targetJetty != nullptr, K_RUNTIME_ERROR,
        FormatString("[GetTargetSeg] Shared recv Jetty raw handle is null for %s", remoteConnectionId));
    *targetJfr = recvJetty->SharedJfrRaw();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        *targetJfr != nullptr, K_RUNTIME_ERROR,
        FormatString("[GetTargetSeg] Shared recv Jetty JFR is null for %s", remoteConnectionId));
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

        urma_seg_t *segCtx = nullptr;
        uint32_t segCtxSize = 0;
        urma_status_t urmaStatus = ds_urma_get_seg_ctx(iter->second->Raw(), &segCtx, &segCtxSize);
        if (urmaStatus == URMA_SUCCESS && segCtx != nullptr && segCtxSize > 0) {
            segInfo->mutable_seg_ctx()->set_seg_blob(reinterpret_cast<const char *>(segCtx), segCtxSize);
            ds_urma_put_seg_ctx(segCtx);
            LOG(INFO) << "[URMA_CONNECT] Got delegated seg context, va=" << iter->second->Raw()->seg.ubva.va
                      << ", length=" << segCtxSize;
        } else {
            LOG(WARNING) << "[URMA_CONNECT] Failed to get delegated seg context, va="
                         << iter->second->Raw()->seg.ubva.va << ", status=" << urmaStatus;
        }
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

        urma_seg_t *segCtx = nullptr;
        uint32_t segCtxSize = 0;
        urma_status_t urmaStatus = ds_urma_get_seg_ctx(iter->second->Raw(), &segCtx, &segCtxSize);
        if (urmaStatus == URMA_SUCCESS && segCtx != nullptr && segCtxSize > 0) {
            segInfo->mutable_seg_ctx()->set_seg_blob(reinterpret_cast<const char *>(segCtx), segCtxSize);
            ds_urma_put_seg_ctx(segCtx);
            LOG(INFO) << "[URMA_CONNECT] Got delegated seg context (rsp), va=" << iter->second->Raw()->seg.ubva.va
                      << ", length=" << segCtxSize;
        } else {
            LOG(WARNING) << "[URMA_CONNECT] Failed to get delegated seg context (rsp), va="
                         << iter->second->Raw()->seg.ubva.va << ", status=" << urmaStatus;
        }
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
        if (len > 0) {
            std::string msg(buffer.data(), len - 1);
            LOG(INFO) << "[URMA_PERF]:\n" << msg;
        }
    }

    return Status::OK();
}

Status UrmaManager::ServerEventHandleThreadMain()
{
    const auto setSchedRuntimeResult = SetCurrentThreadSchedRuntime(FLAGS_enable_sched_runtime);
    if (!setSchedRuntimeResult.success && !setSchedRuntimeResult.skipped) {
        char errMsg[256] = { 0 };
#if defined(__GLIBC__) && defined(_GNU_SOURCE)
        const char *error = strerror_r(setSchedRuntimeResult.err, errMsg, sizeof(errMsg));
#else
        const char *error =
            strerror_r(setSchedRuntimeResult.err, errMsg, sizeof(errMsg)) == 0 ? errMsg : "Unknown error";
#endif
        LOG(WARNING) << FormatString("Failed to set UrmaPollJfc sched runtime to %llu ns, errno: %d, error: %s",
                                     static_cast<unsigned long long>(GetSchedRuntimeNs()), setSchedRuntimeResult.err,
                                     error);
    } else if (setSchedRuntimeResult.success) {
        LOG(INFO) << "Set UrmaPollJfc sched runtime to " << GetSchedRuntimeNs() << " ns.";
    }
    if (!Thread::SetCurrentThreadNice(FLAGS_io_thread_nice)) {
        LOG(WARNING) << "Failed to set nice for UrmaManager server event thread, nice=" << FLAGS_io_thread_nice
                     << ", errno=" << errno;
    }
    // Run this method until serverStop is called.
    while (!serverStop_.load()) {
        std::unordered_set<uint64_t> successCompletedReqs;
        std::unordered_map<uint64_t, int> failedCompletedReqs;
        UrmaWriteTrace pollTrace;
        Status rc = PollJfcWait(urmaResource_->GetJfc(), MAX_POLL_JFC_TRY_CNT, successCompletedReqs,
                                failedCompletedReqs, pollTrace, FLAGS_urma_poll_size);
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
        CheckAndNotify(pollTrace);
    }
    return Status::OK();
}

Status UrmaManager::CheckAndNotify(const UrmaWriteTrace &pollTrace)
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
            event->SetPollTrace(pollTrace);
            auto failedIt = failedRequests_.find(requestId);
            if (failedIt != failedRequests_.end()) {
                event->SetFailed(failedIt->second);
                failedRequests_.erase(failedIt);
            }
            ReleaseEventLane(event);
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
    auto elapsedMs = timer.ElapsedMilliSecond();
    LOG_IF(INFO, (elapsedMs > URMA_LOG_LIMIT_MS || FLAGS_enable_perf_trace_log))
        << "[URMA_ELAPSED_NOTIFY]: urma_poll_jfc thread notify urma_post_jetty_send_wr thread wake up cost "
        << elapsedMs << "ms, cpuid: " << sched_getcpu() << ", count: " << count
        << ", suggest: " << URMA_ELAPSED_NOTIFY_SUGGEST;

    return Status::OK();
}

void UrmaManager::DeleteEvent(uint64_t requestId)
{
    tbbEventMap_.erase(requestId);
}

void UrmaManager::ReleaseEventLane(const std::shared_ptr<UrmaEvent> &event)
{
    if (event == nullptr) {
        return;
    }
    LOG_IF_ERROR(ApplySendLaneAction(event->MarkLaneReleased(), event->GetJetty().lock()),
                 FormatString("Failed to settle URMA lane for request: %zu", event->GetRequestId()));
}

void UrmaManager::ReleaseAndDeleteEvent(uint64_t requestId)
{
    std::shared_ptr<UrmaEvent> event;
    if (GetEvent(requestId, event).IsOk()) {
        ReleaseEventLane(event);
    }
    DeleteEvent(requestId);
}

void UrmaManager::RetireAndDeleteEvent(uint64_t requestId)
{
    std::shared_ptr<UrmaEvent> event;
    if (GetEvent(requestId, event).IsOk()) {
        LOG_IF_ERROR(RetireEventLane(event), FormatString("Failed to retire URMA lane for request: %zu", requestId));
    }
    DeleteEvent(requestId);
}

Status UrmaManager::RetireEventLane(const std::shared_ptr<UrmaEvent> &event)
{
    if (event == nullptr) {
        return Status::OK();
    }
    return ApplySendLaneAction(event->MarkLaneRetired(), event->GetJetty().lock());
}

Status UrmaManager::ApplySendLaneAction(UrmaSendLaneLease::SettleAction action, const std::shared_ptr<UrmaJetty> &jetty)
{
    if (action == UrmaSendLaneLease::SettleAction::NONE) {
        return Status::OK();
    }
    if (jetty == nullptr) {
        return Status::OK();
    }
    if (action == UrmaSendLaneLease::SettleAction::RELEASE) {
        INJECT_POINT("UrmaManager.ApplySendLaneAction.Release");
        urmaResource_->ReleaseJetty(jetty);
        return Status::OK();
    }
    INJECT_POINT("UrmaManager.ApplySendLaneAction.Retire");
    return urmaResource_->RetireJetty(jetty);
}

Status UrmaManager::SealSendLaneLease(const std::shared_ptr<UrmaSendLaneLease> &laneLease)
{
    if (laneLease == nullptr) {
        return Status::OK();
    }
    return ApplySendLaneAction(laneLease->Seal(), laneLease->GetJetty());
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
                                const std::shared_ptr<UrmaSendLaneLease> &laneLease, const std::string &remoteAddress,
                                uint64_t dataSize, UrmaEvent::OperationType operationType,
                                std::atomic<int> *srcChipInflightCounter,
                                std::shared_ptr<EventWaiter> waiter, std::shared_ptr<UrmaEvent> *event)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(laneLease != nullptr, K_RUNTIME_ERROR, "URMA send lane lease is null");
    auto jetty = laneLease->GetJetty();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(jetty != nullptr, K_RUNTIME_ERROR, "URMA send lane Jetty is null");
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
        mapAccessor->second =
            std::make_shared<UrmaEvent>(requestId, laneLease, remoteAddress,
                                        connection->GetUrmaJfrInfo().uniqueInstanceId, dataSize, operationType,
                                        srcChipInflightCounter, waiter);
        if (event != nullptr) {
            *event = mapAccessor->second;
        }
    }
    return Status::OK();
}

const char *UrmaManager::GetSrcChipInflightWrCountsString() const
{
    static thread_local char buffer[URMA_CHIP_INFLIGHT_LOG_BUFFER_SIZE];
    size_t length = 0;
    buffer[length++] = '{';
    bool first = true;
    for (size_t i = 0; i < srcChipInflightWrCounts_.size(); ++i) {
        const auto count = srcChipInflightWrCounts_[i].load(std::memory_order_relaxed);
        if (count == 0) {
            continue;
        }
        const auto written = std::snprintf(buffer + length, sizeof(buffer) - length, "%s%zu:%d",
                                           first ? "" : ",", i, count);
        if (written < 0 || static_cast<size_t>(written) >= sizeof(buffer) - length) {
            break;
        }
        length += static_cast<size_t>(written);
        first = false;
    }
    std::snprintf(buffer + length, sizeof(buffer) - length, "}");
    return buffer;
}

std::atomic<int> *UrmaManager::GetSrcChipInflightWrCounter(uint8_t chipId)
{
    if (chipId == INVALID_CHIP_ID || chipId >= srcChipInflightWrCounts_.size()) {
        return nullptr;
    }
    return &srcChipInflightWrCounts_[chipId];
}

void UrmaManager::LogUrmaWaitToFinishElapsed(uint64_t requestId, const std::shared_ptr<UrmaEvent> &event,
                                             uint64_t totalElapsedUs, double totalElapsedMs, double waitElapsedMs,
                                             uint64_t wakeSchedLatencyUs, const Status &waitRc) const
{
    auto config = GetServerLatencyTraceConfig();
    const auto trace = event->GetWriteTrace();
    SLOW_LOG_IF_OR_VLOG(
        INFO, (config.rpcSlowerThanUs > 0 && totalElapsedUs >= config.rpcSlowerThanUs) || FLAGS_enable_perf_trace_log,
        1,
        "[URMA_ELAPSED_TOTAL]: Time from urma_post_jetty_send_wr to urma_write completion total cost "
            << totalElapsedMs
            << "ms, wait os sched thread finish time(std::condition_variable.wait_for): " << waitElapsedMs
            << "ms, request id:" << requestId << ", src address:" << localUrmaInfo_.localAddress.ToString()
            << ", target address:" << event->GetRemoteAddress() << ", dataSize:" << event->GetDataSize()
            << ", cpuid:" << sched_getcpu() << ", status: " << waitRc.ToString()
            << ", urma_inflight_wr_count: " << tbbEventMap_.size() << ", wakeSchedLatencyUs:" << wakeSchedLatencyUs
            << ", srcChipInflight:" << GetSrcChipInflightWrCountsString()
            << ", trace_us:{post:" << trace.postUs << ", wait:" << trace.waitUs
            << ", poll_begin:" << trace.pollBeginUs << ", sleep_start:" << trace.sleepStartUs
            << ", sleep_end:" << trace.sleepEndUs
            << ", poll_end:" << trace.pollEndUs << ", notify:" << trace.notifyUs << ", awake:" << trace.awakeUs
            << ", suggest: " << URMA_ELAPSED_TOTAL_SUGGEST);
}

Status UrmaManager::WaitToFinish(uint64_t requestId, int64_t timeoutMs)
{
    PerfPoint point(PerfKey::URMA_WAIT_TO_FINISH);
    // This legacy injection models a wait call that fails before it obtains an event.
    // Keep it before GetEvent: moving it below changes the ST from a fallback error-path
    // test into a destructive in-flight lane-retirement test.
    INJECT_POINT("UrmaManager.UrmaWaitError", []() { return Status(K_URMA_WAIT_TIMEOUT, "Inject urma wait error"); });
    std::shared_ptr<UrmaEvent> event;
    RETURN_IF_NOT_OK(GetEvent(requestId, event));
    // Unlike UrmaWaitError, this test hook models a timeout after an event owns an in-flight lane.
    INJECT_POINT("UrmaManager.UrmaWaitInFlightTimeout", [&timeoutMs](int64_t injectedTimeoutMs) {
        timeoutMs = injectedTimeoutMs;
        return Status::OK();
    });
    // use this unique request id as key to wait
    // wait until timeout

    Raii deleteEvent([this, &requestId]() { DeleteEvent(requestId); });
    if (timeoutMs < 0) {
        const auto requestIdStr = std::to_string(static_cast<uint64_t>(requestId));
        LOG_IF_ERROR(RetireEventLane(event), FormatString("Failed to retire URMA lane for request: %s",
                                                          requestIdStr.c_str()));
        const auto srcAddress = localUrmaInfo_.localAddress.ToString();
        RETURN_STATUS_LOG_ERROR(
            K_URMA_WAIT_TIMEOUT,
            FormatString("[URMA_WAIT_TIMEOUT] timedout waiting for request: %s, srcAddress=%s, targetAddress=%s, "
                         "remoteInstanceId=%s, dataSize=%zu, op=%s",
                         requestIdStr.c_str(), srcAddress.c_str(), event->GetRemoteAddress().c_str(),
                         event->GetRemoteInstanceId().c_str(), static_cast<size_t>(event->GetDataSize()),
                         UrmaEvent::OperationTypeName(event->GetOperationType())));
    }

    PerfPoint waitPoint(PerfKey::URMA_WAIT_TIME);
    Timer waitTimer;
    event->SetWriteWaitTimeUs(waitTimer.GetStartTimeStampUs());
    Status waitRc = event->WaitFor(std::chrono::milliseconds(timeoutMs));
    waitTimer.Stop();
    const auto endWaitTimeUs = waitTimer.GetEndTimeStampUs();
    constexpr double US_TO_MS = 1000.0;
    auto totalElapsedUs = endWaitTimeUs - event->GetCreateTimeUs();
    auto wakeSchedLatencyUs = event->GetWakeSchedLatencyUs();
    auto urmaElapsedUs = totalElapsedUs >= wakeSchedLatencyUs ? totalElapsedUs - wakeSchedLatencyUs : 0;
    auto totalElapsedMs = static_cast<double>(urmaElapsedUs) / US_TO_MS;
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::URMA_WAIT_LATENCY)).Observe(totalElapsedUs);
    auto waitElapsedMs = waitTimer.ElapsedMicroSecond() / US_TO_MS;
    GetWorkerTimeCost().Append("Urma wait time.", static_cast<uint64_t>(totalElapsedMs));
    if (waitRc.GetCode() == StatusCode::K_RPC_DEADLINE_EXCEEDED) {
        LOG_IF_ERROR(RetireEventLane(event), FormatString("Failed to retire URMA lane for request: %zu", requestId));
        const auto srcAddress = localUrmaInfo_.localAddress.ToString();
        return Status(K_URMA_WAIT_TIMEOUT,
                      FormatString("urma write deadline exceeded: %fms, requestId=%zu, srcAddress=%s, "
                                   "targetAddress=%s, remoteInstanceId=%s, dataSize=%zu, op=%s, %s",
                                   totalElapsedMs, requestId, srcAddress.c_str(), event->GetRemoteAddress().c_str(),
                                   event->GetRemoteInstanceId().c_str(),
                                   static_cast<size_t>(event->GetDataSize()),
                                   UrmaEvent::OperationTypeName(event->GetOperationType()), waitRc.GetMsg()));
    }
    LogUrmaWaitToFinishElapsed(requestId, event, totalElapsedUs, totalElapsedMs, waitElapsedMs, wakeSchedLatencyUs,
                               waitRc);
    RETURN_IF_NOT_OK(waitRc);
    waitPoint.Record();
    RETURN_IF_NOT_OK(HandleUrmaEvent(requestId, event));
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

Status UrmaManager::AcquireSendLaneFromConnection(const std::shared_ptr<UrmaConnection> &connection,
                                                  std::shared_ptr<UrmaJetty> &jetty,
                                                  urma_target_jetty_t *&targetJetty)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    auto rc = urmaResource_->AcquireJetty(jetty);
    if (rc.IsError()) {
        if (rc.GetCode() == K_TRY_AGAIN) {
            INJECT_POINT("UrmaManager.AcquireSendLaneFromConnection.PoolExhausted");
            const auto stats = urmaResource_->GetSendJettyPoolStats();
            const auto &jfrInfo = connection->GetUrmaJfrInfo();
            const auto srcAddress = localUrmaInfo_.localAddress.ToString();
            const auto targetAddress = jfrInfo.localAddress.ToString();
            RETURN_STATUS_LOG_ERROR(K_TRY_AGAIN,
                                    FormatString("URMA send Jetty lane pool exhausted, poolSize=%zu, idleCount=%zu, "
                                                 "inUseCount=%zu, srcAddress=%s, targetAddress=%s, "
                                                 "remoteInstanceId=%s, cause=%s",
                                                 stats.poolSize, stats.idleCount, stats.inUseCount,
                                                 srcAddress.c_str(), targetAddress.c_str(),
                                                 jfrInfo.uniqueInstanceId.c_str(), rc.ToString().c_str()));
        }
        return rc;
    }
    targetJetty = connection->GetTargetJetty();
    if (targetJetty == nullptr) {
        urmaResource_->ReleaseJetty(jetty);
        jetty.reset();
        const auto &jfrInfo = connection->GetUrmaJfrInfo();
        const auto srcAddress = localUrmaInfo_.localAddress.ToString();
        const auto targetAddress = jfrInfo.localAddress.ToString();
        RETURN_STATUS_LOG_ERROR(
            K_RUNTIME_ERROR,
            FormatString("Connection has no imported remote target Jetty, srcAddress=%s, targetAddress=%s, "
                         "remoteInstanceId=%s",
                         srcAddress.c_str(), targetAddress.c_str(), jfrInfo.uniqueInstanceId.c_str()));
    }
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

    LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
        << "[URMA_RECREATE_JETTY] Trigger from completion, requestId=" << requestId << ", jettyId=" << jettyId
        << ", cqeStatus=" << statusCode;
    return urmaResource_->ReCreateJetty(failedJetty);
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
        auto crStatus = completeRecords[i].status;
        auto userCtx = completeRecords[i].user_ctx;
        auto jettyId = completeRecords[i].local_id;
        // FLUSH_ERR_DONE is a Jetty-lifecycle event, not a request completion. It must take
        // precedence over request-specific hooks; otherwise a pipeline hook could consume the
        // sole per-Jetty flush notification and leave the retired Jetty permanently non-deletable.
        if (crStatus == URMA_CR_WR_FLUSH_ERR_DONE) {
            LOG(INFO) << "[URMA_POLL_JFC] Write flush error done for request id: " << userCtx
                      << ", jetty id: " << jettyId;
            urmaResource_->AsyncDeleteJetty(jettyId);
            continue;
        }
#ifdef BUILD_PIPLN_H2D
        // redirect pipeline h2d events
        if (OsXprtPipln::PiplnH2DRecvEventHook(&completeRecords[i])) {
            std::shared_ptr<UrmaEvent> event;
            if (GetEvent(completeRecords[i].user_ctx, event).IsOk()) {
                ReleaseEventLane(event);
                DeleteEvent(completeRecords[i].user_ctx);
            }
            continue;
        }
#endif
        if (crStatus == URMA_CR_SUCCESS) {
            VLOG(1) << "[URMA_POLL_JFC] Got event with request id: " << userCtx << ", count:" << count
                    << ", cpuid: " << sched_getcpu();
            successCompletedReqs.insert(userCtx);
        } else {
            const auto requestIdStr = std::to_string(static_cast<uint64_t>(userCtx));
            LOG(ERROR) << FormatString(
                "[URMA_POLL_JFC]: urma_poll_jfc return failed completion record, requestId: %s "
                "CR.status: %d",
                requestIdStr.c_str(), crStatus);
            LOG_IF_ERROR(TryRecoverFailedJettyFromCompletion(userCtx, crStatus, jettyId),
                         FormatString("[URMA_RECREATE_JETTY_FAILED] requestId=%s, jettyId=%u, cqeStatus=%d",
                                      requestIdStr.c_str(), jettyId, crStatus));
            failedCompletedReqs[completeRecords[i].user_ctx] = crStatus;
        }
    }

    if (!failedCompletedReqs.empty()) {
        RETURN_STATUS(K_URMA_ERROR,
                      FormatString("[URMA_POLL_JFC]: urma_poll_jfc return failed completion record, failed_count:%zu, "
                                   "suggest: %s",
                                   failedCompletedReqs.size(), URMA_ERROR_SUGGEST));
    }
    return Status::OK();
}

Status UrmaManager::PollJfcWait(urma_jfc_t *urmaJfc, const uint64_t maxTryCount,
                                std::unordered_set<uint64_t> &successCompletedReqs,
                                std::unordered_map<uint64_t, int> &failedCompletedReqs, UrmaWriteTrace &pollTrace,
                                const uint64_t numPollCRS)
{
    urma_cr_t completeRecords[numPollCRS];
    urma_jfc_t *ev_jfc = nullptr;
    int cnt;
    uint64_t sleepStartUsForNextPoll = 0;
    uint64_t sleepEndUsForNextPoll = 0;

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
            RETURN_STATUS_LOG_ERROR(
                K_URMA_ERROR,
                FormatString("[URMA_POLL_JFC]: call urma_poll_jfc failed, ret:%d, CR.status:%d, suggest: %s", cnt,
                             completeRecords[0].status, URMA_ERROR_SUGGEST));
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
        const auto pollStartUs = static_cast<uint64_t>(GetSteadyClockTimeStampUs());
        const auto pollGapAfterLastEndUs = pollLastEndUs_ == 0 ? 0 : pollStartUs - pollLastEndUs_;
        const auto pollStartIntervalUs = pollLastStartUs_ == 0 ? 0 : pollStartUs - pollLastStartUs_;
        if (pollGapAfterLastEndUs > URMA_LOG_LIMIT_US || pollStartIntervalUs > URMA_LOG_LIMIT_US) {
            LOG(INFO) << "[URMA_ELAPSED_THREAD_SHED]: urma_poll_jfc loop gap, lastPollEndToThisPollStart "
                      << pollGapAfterLastEndUs << "us, lastPollStartToThisPollStart " << pollStartIntervalUs
                      << "us, cpuid: " << sched_getcpu() << ", suggest: " << URMA_ELAPSED_THREAD_SCHED_SUGGEST;
        }
        Timer timer;
        cnt = ds_urma_poll_jfc(urmaJfc, numPollCRS, completeRecords);
        timer.Stop();
        auto pollElapsedUs = timer.ElapsedMicroSecond();
        pollLastStartUs_ = pollStartUs;
        pollLastEndUs_ = static_cast<uint64_t>(GetSteadyClockTimeStampUs());
        LOG_IF(INFO, pollElapsedUs > URMA_LOG_LIMIT_US)
            << "[URMA_ELAPSED_POLL_JFC]: urma_poll_jfc cost " << pollElapsedUs << "us, cpuid: " << sched_getcpu()
            << ", suggest: " << URMA_ELAPSED_POLL_JFC_SUGGEST;
        if (cnt == 0) {
            // If there is nothing to poll, just sleep.
            // Note that it takes on average 50us to wake up with usleep(0), due to OS timerslack settings.
            Timer sleepTimer;
            const struct timespec ts {
                0, 1000
            };
            if (!tbbEventMap_.empty()) {
                METRIC_TIMER(metrics::KvMetricId::URMA_NANOSLEEP_LATENCY);
                nanosleep(&ts, nullptr);
            } else {
                nanosleep(&ts, nullptr);
            }
            sleepTimer.Stop();
            auto sleepElapsedUs = sleepTimer.ElapsedMicroSecond();
            sleepStartUsForNextPoll = sleepTimer.GetStartTimeStampUs();
            sleepEndUsForNextPoll = sleepTimer.GetEndTimeStampUs();
            LOG_IF(INFO, sleepElapsedUs > URMA_LOG_LIMIT_US)
                << "[URMA_ELAPSED_THREAD_SHED]: urma_poll_jfc thread wake up after nanosleep(1us) cost "
                << sleepElapsedUs << "us, cpuid: " << sched_getcpu()
                << ", suggest: " << URMA_ELAPSED_THREAD_SCHED_SUGGEST;
        } else if (cnt < 0) {
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR,
                                    FormatString("[URMA_POLL_JFC]: call urma_poll_jfc failed, ret:%d, suggest: %s", cnt,
                                                 URMA_ERROR_SUGGEST));
        } else if (cnt > 0) {
            pollTrace.pollBeginUs = timer.GetStartTimeStampUs();
            pollTrace.sleepStartUs = sleepStartUsForNextPoll;
            pollTrace.sleepEndUs = sleepEndUsForNextPoll;
            pollTrace.pollEndUs = timer.GetEndTimeStampUs();
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
    LOG(INFO) << "Begin to import remote jfr.";
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
            LOG(INFO) << "Fail to import remote jfr.";
            urmaConnectionMap_.erase(accessor);
        }
    });

    // Import the remote JFR as a target Jetty (no local Jetty needed at import time).
    std::unique_ptr<UrmaTargetJetty> targetJetty;
    RETURN_IF_NOT_OK(ImportTargetJetty(jfrInfo, targetJetty, nullptr));

    // Get or create a local JETTY for this connection (reused across reconnections)
    RETURN_IF_NOT_OK(GetOrCreateLocalJetty(remoteConnectionId, localJettyId, JettyType::RECV));

    accessor->second = std::make_shared<UrmaConnection>(std::move(targetJetty), jfrInfo);
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
            LOG(ERROR) << "Failed to import remote segment, remoteConnectionId: " << remoteConnectionId
                       << ", status: " << rc.ToString();
            return rc;
        }
    }
    return Status::OK();
}

Status UrmaManager::ImportTargetJetty(const UrmaJfrInfo &remoteInfo, std::unique_ptr<UrmaTargetJetty> &targetJetty,
                                      urma_jetty_t *localJetty)
{
    LOG(INFO) << "Begin to import target jft.";
    Timer timer;
    METRIC_TIMER(metrics::KvMetricId::URMA_IMPORT_JFR);
    if (!remoteInfo.rjettyBuf.empty()) {
        if (remoteInfo.rjettyBuf.size() < sizeof(urma_rjetty_t)) {
            RETURN_STATUS(K_RUNTIME_ERROR,
                          FormatString("Invalid delegated rjetty length=%zu", remoteInfo.rjettyBuf.size()));
        }
        // Make a local mutable copy of the delegated blob. The UMDK import API takes a
        // non-const urma_rjetty_t*, and we must not modify the caller's const object.
        std::string localRjettyBuf = remoteInfo.rjettyBuf;
        auto *rjetty = reinterpret_cast<urma_rjetty_t *>(localRjettyBuf.data());
        rjetty->tp_type = URMA_CTP;
        LOG(INFO) << "[URMA_CONNECT] Import target jetty using delegated context, length=" << localRjettyBuf.size();
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            UrmaTargetJetty::Import(urmaResource_->GetContext(), rjetty, urmaResource_->GetUrmaToken(), targetJetty),
            FormatString("Failed to import target jetty, remoteInfo: %s", remoteInfo.ToString()));
    } else {
        bondp_rjetty_t bondpRemoteJetty{};
        urma_rjetty_t remoteJetty{};
        RETURN_IF_NOT_OK(BuildRemoteJetty(remoteInfo, remoteJetty));
        bondpRemoteJetty.base = remoteJetty;
        bondpRemoteJetty.jetty = localJetty;
        bondpRemoteJetty.base.flag.bs.has_drv_ext = 1;
        LOG(INFO) << "[URMA_CONNECT] Import target jetty using legacy handshake";
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            UrmaTargetJetty::Import(urmaResource_->GetContext(), &(bondpRemoteJetty.base),
                                    urmaResource_->GetUrmaToken(), targetJetty),
            FormatString("Failed to import target jetty, remoteInfo: %s", remoteInfo.ToString()));
    }
    LOG_IF(INFO, timer.ElapsedMilliSecond() > 1)
        << "[URMA_CONNECT] Import target jetty elapsed = " << timer.ElapsedMilliSecond() << "ms"
        << ", cpuid: " << sched_getcpu() << ", remoteInfo: " << remoteInfo.ToString();
    return Status::OK();
}

Status UrmaManager::FinalizeOutboundConnection(const UrmaHandshakeRspPb &rsp)
{
    METRIC_TIMER(metrics::KvMetricId::URMA_CONNECTION_SETUP_LATENCY);
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
            LOG(INFO) << "Erase outbound connection.";
            urmaConnectionMap_.erase(accessor);
        }
    });

    // Import the remote JFR as a target Jetty (no local Jetty needed at import time).
    std::unique_ptr<UrmaTargetJetty> targetJetty;
    RETURN_IF_NOT_OK(ImportTargetJetty(remoteInfo, targetJetty, nullptr));

    accessor->second = std::make_shared<UrmaConnection>(std::move(targetJetty), remoteInfo);
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

static urma_status_t PostJettyRw(const std::shared_ptr<UrmaJetty> &jetty, urma_opcode_t opcode,
                                 urma_target_jetty_t *targetJetty,
                                 urma_target_seg_t *remoteSeg, urma_target_seg_t *localSeg, uint64_t remoteAddress,
                                 uint64_t localAddress, uint64_t length, urma_jfs_wr_flag_t flag, uint64_t userCtx,
                                 bool useNumaAffinity, uint32_t src_chip_id, uint32_t dst_chip_id)
{
    auto permit = jetty == nullptr ? UrmaJetty::PostPermit{} : jetty->TryAcquirePostPermit();
    if (!permit) {
        // Real UMDK defines URMA_EAGAIN as EAGAIN. Use the errno value directly so the
        // production gate does not require changes to the repository's separate mock ABI.
        return static_cast<urma_status_t>(EAGAIN);
    }
    urma_sge_t localSge{
        .addr = localAddress, .len = static_cast<uint32_t>(length), .tseg = localSeg, .user_tseg = nullptr
    };
    urma_sge_t remoteSge{
        .addr = remoteAddress, .len = static_cast<uint32_t>(length), .tseg = remoteSeg, .user_tseg = nullptr
    };

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

        INJECT_POINT_NO_RETURN("UrmaManager.PostJettyRwWithPermit");
        return ds_urma_post_jetty_send_wr(permit.Raw(), base, &badWr);
    } else {
        urma_jfs_wr_t wr{};
        wr.opcode = opcode;
        wr.flag = flag;
        wr.tjetty = targetJetty;
        wr.user_ctx = userCtx;
        wr.rw = { .src = src, .dst = dst, .target_hint = 0, .notify_data = 0 };
        wr.next = nullptr;
        INJECT_POINT_NO_RETURN("UrmaManager.PostJettyRwWithPermit");
        return ds_urma_post_jetty_send_wr(permit.Raw(), &wr, &badWr);
    }
}

Status UrmaManager::UrmaWriteImpl(const UrmaWriteArgs &args, std::vector<uint64_t> &eventKeys,
                                  const std::shared_ptr<UrmaSendLaneLease> &externalLaneLease)
{
    if (args.size == 0) {
        return Status::OK();
    }
    urma_jfs_wr_flag_t flag{};
    flag.bs.complete_enable = 1;
    const bool useNumaAffinity =
        IsUbNumaAffinityEnabled() && args.srcChipId != INVALID_CHIP_ID && args.dstChipId != INVALID_CHIP_ID;

    uint64_t writtenSize = 0;
    uint64_t remainSize = args.size;
    Timer timer;
    std::shared_ptr<UrmaJetty> jetty;
    urma_target_jetty_t *targetJetty = nullptr;
    const bool ownsLaneLease = externalLaneLease == nullptr;
    auto laneLease = externalLaneLease;
    if (ownsLaneLease) {
        RETURN_IF_NOT_OK(AcquireSendLaneFromConnection(args.connection, jetty, targetJetty));
        laneLease = std::make_shared<UrmaSendLaneLease>(jetty);
    } else {
        laneLease = externalLaneLease;
        jetty = laneLease->GetJetty();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(jetty != nullptr, K_RUNTIME_ERROR,
                                             "Batch Get URMA send lane Jetty is null");
        targetJetty = args.connection == nullptr ? nullptr : args.connection->GetTargetJetty();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(targetJetty != nullptr, K_RUNTIME_ERROR,
                                         "Batch Get write got empty remote target Jetty");
    bool laneLeaseSealed = false;
    auto sealLaneLease = [this, &laneLease, &laneLeaseSealed]() {
        if (laneLeaseSealed) {
            return Status::OK();
        }
        laneLeaseSealed = true;
        // A Batch Get RPC owns an external lease and seals it at RPC scope.
        // Object-level writes must not settle that lease independently.
        return SealSendLaneLease(laneLease);
    };
    Raii sealOnExit([&sealLaneLease, ownsLaneLease]() {
        if (ownsLaneLease) {
            LOG_IF_ERROR(sealLaneLease(), "Failed to seal URMA write lane lease");
        }
    });
    auto cleanupSubmittedEvents = [this, &eventKeys, &sealLaneLease, ownsLaneLease]() {
        if (ownsLaneLease) {
            LOG_IF_ERROR(sealLaneLease(), "Failed to seal URMA write lane lease during cleanup");
        }
        if (eventKeys.empty()) {
            return;
        }
        auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        LOG_IF_ERROR(WaitFastTransportEvent(eventKeys, remainingTime, errorHandler),
                     "Failed to cleanup submitted URMA write events");
        eventKeys.clear();
    };
    auto *srcChipInflightCounter = GetSrcChipInflightWrCounter(args.srcChipId);
    while (remainSize > 0) {
        const uint64_t writeSize = std::min(remainSize, urmaResource_->GetMaxWriteSize());
        const uint64_t key = GenerateReqId();
        const uint64_t remoteAddress = args.remoteDataAddress + writtenSize;
        const uint64_t localAddress = args.localDataAddress + writtenSize;
        PerfPoint pointWrite(PerfKey::URMA_WRITE_SINGLE);
        auto injectRc = []() -> Status {
            INJECT_POINT("UrmaManager.UrmaWriteError",
                         []() { return Status(K_RUNTIME_ERROR, "Injcect urma write error"); });
            return Status::OK();
        }();
        if (injectRc.IsError()) {
            if (ownsLaneLease) {
                LOG_IF_ERROR(ApplySendLaneAction(laneLease->RequestRetire(), laneLease->GetJetty()),
                             "Failed to mark URMA write lane for retirement after injected write failure");
            }
            cleanupSubmittedEvents();
            return injectRc;
        }
        std::shared_ptr<UrmaEvent> event;
        auto createRc = CreateEvent(key, args.connection, laneLease, args.remoteAddress, writeSize,
                                    UrmaEvent::OperationType::WRITE, srcChipInflightCounter, args.waiter, &event);
        if (createRc.IsError()) {
            if (ownsLaneLease) {
                LOG_IF_ERROR(ApplySendLaneAction(laneLease->RequestRetire(), laneLease->GetJetty()),
                             "Failed to mark URMA write lane for retirement after event creation failure");
            }
            cleanupSubmittedEvents();
            return createRc;
        }
        urma_status_t ret;
        Timer t;
        event->SetWritePostTimeUs(t.GetStartTimeStampUs());
        METRIC_TIMER(metrics::KvMetricId::URMA_WRITE_LATENCY);
        auto jettyId = jetty->GetJettyId();
        LOG_EVERY_T(INFO, LOG_TIME_LIMIT_LEVEL1)
            << "URMA write useNumaAffinity:" << useNumaAffinity << ", src:" << static_cast<uint32_t>(args.srcChipId)
            << ", dst:" << static_cast<uint32_t>(args.dstChipId) << ", jetty id:" << jettyId
            << ", urma_inflight_wr_count:" << tbbEventMap_.size();
        if (useNumaAffinity) {
            auto numaInjectRc = []() -> Status {
                INJECT_POINT("UrmaManager.UrmaWriteNumaAffinity");
                return Status::OK();
            }();
            if (numaInjectRc.IsError()) {
                if (ownsLaneLease) {
                    RetireAndDeleteEvent(key);
                } else {
                    // A failed object WR must not retire the RPC-shared lane.
                    ReleaseAndDeleteEvent(key);
                }
                cleanupSubmittedEvents();
                return numaInjectRc;
            }
            ret = PostJettyRw(jetty, URMA_OPC_WRITE, targetJetty, args.remoteSeg, args.localSeg,
                              remoteAddress, localAddress, writeSize, flag, key, true, args.srcChipId, args.dstChipId);
        } else {
            ret = PostJettyRw(jetty, URMA_OPC_WRITE, targetJetty, args.remoteSeg, args.localSeg,
                              remoteAddress, localAddress, writeSize, flag, key, false, args.srcChipId,
                              args.dstChipId);
        }

        if (ret != URMA_SUCCESS) {
            ReleaseAndDeleteEvent(key);
            cleanupSubmittedEvents();
            const auto srcAddress = localUrmaInfo_.localAddress.ToString();
            const auto remoteInstanceId =
                args.connection == nullptr ? "" : args.connection->GetUrmaJfrInfo().uniqueInstanceId.c_str();
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR,
                                    FormatString("[URMA_WRITE]: call urma_post_jetty_send_wr failed with key: %zu, "
                                                 "ret: %d, srcAddress=%s, targetAddress=%s, remoteInstanceId=%s, "
                                                 "dataSize=%zu, srcChipId=%u, dstChipId=%u, useNumaAffinity=%s, "
                                                 "suggest: %s",
                                                 key, ret, srcAddress.c_str(), args.remoteAddress.c_str(),
                                                 remoteInstanceId, static_cast<size_t>(writeSize),
                                                 static_cast<uint32_t>(args.srcChipId),
                                                 static_cast<uint32_t>(args.dstChipId),
                                                 useNumaAffinity ? "true" : "false", URMA_ERROR_SUGGEST));
        }
        t.Stop();
        auto elapsedUs = t.ElapsedMicroSecond();
        auto vlogLevel = elapsedUs > URMA_WRITE_VLOG0_LIMIT_US ? 0 : 1;
        VLOG(vlogLevel) << "[UrmaWrite] URMA finish write, cpuid:" << sched_getcpu() << ", elapsed:" << elapsedUs
                        << " us, request id:" << key << ", jetty id:" << jettyId;
        pointWrite.Record();
        remainSize -= writeSize;
        writtenSize += writeSize;
        eventKeys.emplace_back(key);
        INJECT_POINT("UrmaManager.UrmaWriteAfterPost");
    }
    GetWorkerTimeCost().Append("Urma total write.", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status UrmaManager::AcquireSendLane(const UrmaRemoteAddrPb &urmaInfo,
                                    std::shared_ptr<UrmaSendLaneLease> &laneLease)
{
    laneLease.reset();
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
    auto connection = constAccessor->second;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    std::shared_ptr<UrmaJetty> jetty;
    urma_target_jetty_t *targetJetty = nullptr;
    RETURN_IF_NOT_OK(AcquireSendLaneFromConnection(connection, jetty, targetJetty));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(targetJetty != nullptr, K_RUNTIME_ERROR,
                                         "Batch Get got empty remote target Jetty");
    laneLease = std::make_shared<UrmaSendLaneLease>(jetty);
    return Status::OK();
}

Status UrmaManager::UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                     const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                     const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                     uint8_t srcChipId, uint8_t dstChipId, bool blocking,
                                     std::vector<uint64_t> &eventKeys, std::shared_ptr<EventWaiter> waiter)
{
    return UrmaWritePayloadImpl(urmaInfo, localSegAddress, localSegSize, localObjectAddress, readOffset, readSize,
                                metaDataSize, srcChipId, dstChipId, blocking, eventKeys, nullptr, waiter);
}

Status UrmaManager::UrmaWritePayloadWithLane(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                             const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                             const uint64_t &readOffset, const uint64_t &readSize,
                                             const uint64_t &metaDataSize, uint8_t srcChipId, uint8_t dstChipId,
                                             bool blocking, std::vector<uint64_t> &eventKeys,
                                             const std::shared_ptr<UrmaSendLaneLease> &laneLease,
                                             std::shared_ptr<EventWaiter> waiter)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(laneLease != nullptr, K_RUNTIME_ERROR,
                                         "Batch Get URMA send lane lease is null");
    return UrmaWritePayloadImpl(urmaInfo, localSegAddress, localSegSize, localObjectAddress, readOffset, readSize,
                                metaDataSize, srcChipId, dstChipId, blocking, eventKeys, laneLease, waiter);
}

Status UrmaManager::UrmaWritePayloadImpl(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                         const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                         const uint64_t &readOffset, const uint64_t &readSize,
                                         const uint64_t &metaDataSize, uint8_t srcChipId, uint8_t dstChipId,
                                         bool blocking, std::vector<uint64_t> &eventKeys,
                                         const std::shared_ptr<UrmaSendLaneLease> &externalLaneLease,
                                         std::shared_ptr<EventWaiter> waiter)
{
    eventKeys.clear();
    PerfPoint point(PerfKey::URMA_WRITE_TOTAL);
    const uint64_t segVa = urmaInfo.seg_va();
    const HostPort requestAddress(urmaInfo.request_address().host(), urmaInfo.request_address().port());
    const std::string remoteAddress = requestAddress.ToString();
    std::string remoteConnectionId = urmaInfo.client_id().empty() ? requestAddress.ToString() : urmaInfo.client_id();
    std::shared_ptr<UrmaConnection> connection;
    point.RecordAndReset(PerfKey::URMA_WRITE_FIND_CONNECTION);
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::const_accessor constAccessor;
    auto res = urmaConnectionMap_.find(constAccessor, remoteConnectionId);
    if (!res && !urmaInfo.client_id().empty()) {
        remoteConnectionId = requestAddress.ToString();
        res = urmaConnectionMap_.find(constAccessor, remoteConnectionId);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteConnectionId));
    connection = constAccessor->second;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");

    point.RecordAndReset(PerfKey::URMA_WRITE_FIND_REMOTE_SEGMENT);
    UrmaRemoteSegmentMap::const_accessor remoteSegAccessor;
    RETURN_IF_NOT_OK(connection->GetRemoteSeg(segVa, remoteSegAccessor));

    point.RecordAndReset(PerfKey::URMA_WRITE_REGISTER_LOCAL_SEGMENT);
    UrmaLocalSegmentMap::const_accessor localSegAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(localSegAddress, localSegSize, localSegAccessor));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localSegAccessor->second != nullptr, K_RUNTIME_ERROR, "Local segment is null");

    point.RecordAndReset(PerfKey::URMA_WRITE_LOOP);

    if (OsXprtPipln::IsPiplnH2DRequest(urmaInfo)) {
        std::shared_ptr<UrmaJetty> jetty;
        urma_target_jetty_t *targetJetty = nullptr;
        const bool ownsLaneLease = externalLaneLease == nullptr;
        auto laneLease = externalLaneLease;
        if (ownsLaneLease) {
            RETURN_IF_NOT_OK(AcquireSendLaneFromConnection(connection, jetty, targetJetty));
            laneLease = std::make_shared<UrmaSendLaneLease>(jetty);
        } else {
            jetty = externalLaneLease->GetJetty();
            targetJetty = connection->GetTargetJetty();
        }
        bool laneLeaseSealed = false;
        auto sealLaneLease = [this, &laneLease, &laneLeaseSealed]() {
            if (laneLeaseSealed) {
                return Status::OK();
            }
            laneLeaseSealed = true;
            return SealSendLaneLease(laneLease);
        };
        Raii sealOnExit([&sealLaneLease, ownsLaneLease]() {
            if (ownsLaneLease) {
                LOG_IF_ERROR(sealLaneLease(), "Failed to seal URMA pipeline lane lease");
            }
        });
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(jetty != nullptr, K_RUNTIME_ERROR,
                                             "Write got empty URMA send lane Jetty.");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(targetJetty != nullptr, K_RUNTIME_ERROR, "Write got empty remote jetty.");
        // The transport pipeline receives a raw handle and may issue several provider posts.
        // Keep one permit over the complete synchronous pipeline call, not merely argument setup.
        auto pipelinePermit = jetty->TryAcquirePostPermit();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pipelinePermit, K_URMA_TRY_AGAIN,
                                             "URMA pipeline Jetty is closing");
        OsXprtPipln::PiplnSndArgs args;
        args.jetty = pipelinePermit.Raw();
        args.tjetty = targetJetty;
        args.localAddr = localObjectAddress + readOffset + metaDataSize;
        args.localSeg = localSegAccessor->second->Raw();
        args.remoteAddr = segVa + urmaInfo.seg_data_offset() + readOffset;
        args.remoteSeg = remoteSegAccessor->second->Raw();
        args.len = readSize;
        args.serverKey = (uint32_t)(GenerateReqId() & URMA_REQID_MASK);
        args.clientKey = urmaInfo.pipeline_rh2d_req_id();

        RETURN_IF_NOT_OK(
            CreateEvent(args.serverKey, connection, laneLease, remoteAddress, readSize,
                        UrmaEvent::OperationType::WRITE, nullptr));
        eventKeys.emplace_back(args.serverKey);
        Status rc;
        rc = OsXprtPipln::DoPiplnStep1_StartSender(args);
        if (rc.IsError()) {
            // A failed object WR does not retire an RPC-shared lane. The RPC
            // seals the lease and lets all posted events settle through release.
            ReleaseAndDeleteEvent(args.serverKey);
            eventKeys.clear();
        }
        return rc;
    }

    UrmaWriteArgs writeLoopArgs;
    writeLoopArgs.connection = connection;
    writeLoopArgs.waiter = waiter;
    writeLoopArgs.remoteAddress = remoteAddress;
    writeLoopArgs.remoteSeg = remoteSegAccessor->second->Raw();
    writeLoopArgs.localSeg = localSegAccessor->second->Raw();
    writeLoopArgs.remoteDataAddress = segVa + urmaInfo.seg_data_offset() + readOffset;
    writeLoopArgs.localDataAddress = localObjectAddress + readOffset + metaDataSize;
    writeLoopArgs.size = readSize;
    writeLoopArgs.srcChipId = srcChipId;
    writeLoopArgs.dstChipId = dstChipId;
    RETURN_IF_NOT_OK(UrmaWriteImpl(writeLoopArgs, eventKeys, externalLaneLease));
    point.Record();
    // If it is blocking wait, we will wait for the write to finish here.
    if (blocking) {
        auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
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
    if (dataSize == 0) {
        return Status::OK();
    }
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

    uint64_t readOffset = 0;
    uint64_t remainSize = dataSize;
    std::shared_ptr<UrmaJetty> jetty;
    urma_target_jetty_t *targetJetty = nullptr;
    RETURN_IF_NOT_OK(AcquireSendLaneFromConnection(connection, jetty, targetJetty));
    auto laneLease = std::make_shared<UrmaSendLaneLease>(jetty);
    bool laneLeaseSealed = false;
    auto sealLaneLease = [this, &laneLease, &laneLeaseSealed]() {
        if (laneLeaseSealed) {
            return Status::OK();
        }
        laneLeaseSealed = true;
        return SealSendLaneLease(laneLease);
    };
    Raii sealOnExit([&sealLaneLease]() { LOG_IF_ERROR(sealLaneLease(), "Failed to seal URMA read lane lease"); });
    auto cleanupSubmittedEvents = [this, &keys, &sealLaneLease]() {
        LOG_IF_ERROR(sealLaneLease(), "Failed to seal URMA read lane lease during cleanup");
        if (keys.empty()) {
            return;
        }
        auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        LOG_IF_ERROR(WaitFastTransportEvent(keys, remainingTime, errorHandler),
                     "Failed to cleanup submitted URMA read events");
        keys.clear();
    };
    while (remainSize > 0) {
        const uint64_t readSize = std::min(remainSize, urmaResource_->GetMaxReadSize());
        const uint64_t key = GenerateReqId();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localSegAccessor->second != nullptr, K_RUNTIME_ERROR,
                                             "Local segment is null");
        auto createRc =
            CreateEvent(key, connection, laneLease, remoteAddress, readSize, UrmaEvent::OperationType::READ, nullptr);
        if (createRc.IsError()) {
            cleanupSubmittedEvents();
            return createRc;
        }
        urma_status_t ret = PostJettyRw(
            jetty, URMA_OPC_READ, targetJetty, remoteSegAccessor->second->Raw(), localSegAccessor->second->Raw(),
            segVa + urmaInfo.seg_data_offset() + readOffset, localObjectAddress + metaDataSize + readOffset, readSize,
            flag, key, false, INVALID_CHIP_ID, INVALID_CHIP_ID);
        if (ret != URMA_SUCCESS) {
            ReleaseAndDeleteEvent(key);
            cleanupSubmittedEvents();
            const auto srcAddress = localUrmaInfo_.localAddress.ToString();
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR,
                                    FormatString("[URMA_READ]: call urma_post_jetty_send_wr failed with key: %zu, "
                                                 "ret: %d, srcAddress=%s, targetAddress=%s, dataSize=%zu, "
                                                 "suggest: %s",
                                                 key, ret, srcAddress.c_str(), remoteAddress.c_str(),
                                                 static_cast<size_t>(readSize), URMA_ERROR_SUGGEST));
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
    return UrmaGatherWriteImpl(remoteInfo, objInfos, blocking, eventKeys, nullptr);
}

Status UrmaManager::UrmaGatherWriteWithLane(const RemoteSegInfo &remoteInfo,
                                            const std::vector<LocalSgeInfo> &objInfos, bool blocking,
                                            std::vector<uint64_t> &eventKeys,
                                            const std::shared_ptr<UrmaSendLaneLease> &laneLease)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(laneLease != nullptr, K_RUNTIME_ERROR,
                                         "Batch Get URMA send lane lease is null");
    return UrmaGatherWriteImpl(remoteInfo, objInfos, blocking, eventKeys, laneLease);
}

Status UrmaManager::UrmaGatherWriteImpl(const RemoteSegInfo &remoteInfo,
                                        const std::vector<LocalSgeInfo> &objInfos, bool blocking,
                                        std::vector<uint64_t> &eventKeys,
                                        const std::shared_ptr<UrmaSendLaneLease> &externalLaneLease)
{
    eventKeys.clear();
    if (objInfos.empty()) {
        return Status::OK();
    }
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
    const auto sgeNum = objInfos.size();
    const auto wrSgeMaxNum = 13U;
    const auto dstSgeNum = (sgeNum + wrSgeMaxNum - 1) / wrSgeMaxNum;
    std::vector<urma_sge_t> srcSgeList(sgeNum);
    std::vector<urma_sge_t> dstSgeList(dstSgeNum);
    std::vector<urma_jfs_wr_t> wrList(dstSgeNum);
    urma_jfs_wr_flag_t flag = { .value = 0 };
    flag.bs.complete_enable = 1;
    flag.bs.inline_flag = 0;
    auto totalWriteSize = 0U;
    INJECT_POINT("UrmaManager.GatherWriteError", []() { return Status(K_RUNTIME_ERROR, "Injcect urma wait error"); });
    std::shared_ptr<UrmaJetty> jetty;
    urma_target_jetty_t *targetJetty = nullptr;
    const bool ownsLaneLease = externalLaneLease == nullptr;
    auto laneLease = externalLaneLease;
    if (ownsLaneLease) {
        RETURN_IF_NOT_OK(AcquireSendLaneFromConnection(connection, jetty, targetJetty));
        laneLease = std::make_shared<UrmaSendLaneLease>(jetty);
    } else {
        jetty = externalLaneLease->GetJetty();
        targetJetty = connection->GetTargetJetty();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(jetty != nullptr, K_RUNTIME_ERROR,
                                         "Batch Get URMA gather send lane Jetty is null");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(targetJetty != nullptr, K_RUNTIME_ERROR,
                                         "Gather write got empty remote Jetty");
    INJECT_POINT("UrmaManager.GatherWriteAfterAcquire");
    bool laneLeaseSealed = false;
    auto sealLaneLease = [this, &laneLease, &laneLeaseSealed]() {
        if (laneLeaseSealed) {
            return Status::OK();
        }
        laneLeaseSealed = true;
        return SealSendLaneLease(laneLease);
    };
    Raii sealOnExit([&sealLaneLease, ownsLaneLease]() {
        if (ownsLaneLease) {
            LOG_IF_ERROR(sealLaneLease(), "Failed to seal URMA gather write lane lease");
        }
    });
    std::vector<uint64_t> createdEventKeys;
    createdEventKeys.reserve(dstSgeNum);
    std::vector<uint64_t> submittedEventKeys;
    submittedEventKeys.reserve(dstSgeNum);
    auto cleanupEvents = [this, &createdEventKeys, &submittedEventKeys, &sealLaneLease,
                          ownsLaneLease](size_t submittedCount) {
        submittedCount = std::min(submittedCount, createdEventKeys.size());
        for (size_t i = submittedCount; i < createdEventKeys.size(); ++i) {
            // These events are after bad_wr (or no WR was posted at all), so the provider cannot complete them.
            ReleaseAndDeleteEvent(createdEventKeys[i]);
        }
        submittedEventKeys.assign(createdEventKeys.begin(), createdEventKeys.begin() + submittedCount);
        if (ownsLaneLease) {
            LOG_IF_ERROR(sealLaneLease(), "Failed to seal URMA gather write lane lease during cleanup");
        }
        if (submittedEventKeys.empty()) {
            return;
        }
        auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        LOG_IF_ERROR(WaitFastTransportEvent(submittedEventKeys, remainingTime, errorHandler),
                     "Failed to cleanup submitted URMA gather write events");
        submittedEventKeys.clear();
    };
    for (size_t dstSgeIdx = 0, srcSgeIdx = 0; dstSgeIdx < dstSgeNum; dstSgeIdx++) {
        const auto srcSgeStart = srcSgeIdx;
        uint64_t singleDstWriteSize = 0;
        while (srcSgeIdx < sgeNum) {
            auto &ele = objInfos[srcSgeIdx];
            UrmaLocalSegmentMap::const_accessor localSegAccessor;
            auto registerRc = GetOrRegisterSegment(ele.segAddr, ele.segSize, localSegAccessor);
            if (registerRc.IsError()) {
                cleanupEvents(0);
                return registerRc;
            }
            if (localSegAccessor->second == nullptr) {
                cleanupEvents(0);
                RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Local segment is null");
            }
            srcSgeList[srcSgeIdx] =
                urma_sge_t{ .addr = ele.sgeAddr + ele.metaDataSize + ele.readOffset,
                            .len = static_cast<uint32_t>(ele.writeSize),
                            .tseg = localSegAccessor->second->Raw(),
                            .user_tseg = NULL };
            singleDstWriteSize += srcSgeList[srcSgeIdx].len;
            srcSgeIdx++;
            if (srcSgeIdx % wrSgeMaxNum == 0) {
                break;
            }
        }
        urma_sg_t srcSg = { .sge = &srcSgeList[srcSgeStart],
                            .num_sge = static_cast<uint32_t>(srcSgeIdx - srcSgeStart) };
        dstSgeList[dstSgeIdx] = { .addr = segVa + remoteInfo.segOffset + totalWriteSize,
                                  .len = static_cast<uint32_t>(singleDstWriteSize),
                                  .tseg = remoteSegAccessor->second->Raw(),
                                  .user_tseg = nullptr };
        totalWriteSize += singleDstWriteSize;
        urma_sg_t dstSg = { .sge = &dstSgeList[dstSgeIdx], .num_sge = 1 };
        urma_rw_wr_t rw = { .src = srcSg, .dst = dstSg, .target_hint = 0, .notify_data = 0 };
        const uint64_t key = GenerateReqId();
        auto &wr = wrList[dstSgeIdx];
        wr = urma_jfs_wr_t{};
        wr.opcode = URMA_OPC_WRITE;
        wr.flag = flag;
        wr.tjetty = targetJetty;
        wr.user_ctx = key;
        wr.rw = rw;
        wr.next = NULL;
        auto createRc =
            CreateEvent(key, connection, laneLease, remoteAddress, singleDstWriteSize,
                        UrmaEvent::OperationType::WRITE, nullptr);
        if (createRc.IsError()) {
            cleanupEvents(0);
            return createRc;
        }
        createdEventKeys.emplace_back(key);
        if (dstSgeIdx > 0) {
            wrList[dstSgeIdx - 1].next = &wrList[dstSgeIdx];
        }
    }

    auto gatherPermit = jetty->TryAcquirePostPermit();
    if (!gatherPermit) {
        cleanupEvents(0);
        RETURN_STATUS(K_URMA_TRY_AGAIN, "URMA gather-write Jetty is closing");
    }
    urma_jfs_wr_t *badWr = nullptr;
    Timer timer;
    auto ret = ds_urma_post_jetty_send_wr(gatherPermit.Raw(), &wrList[0], &badWr);
    GetWorkerTimeCost().Append("Urma gather write.", timer.ElapsedMilliSecond());
    if (ret != URMA_SUCCESS) {
        size_t submittedCount = createdEventKeys.size();
        if (badWr != nullptr) {
            bool badWrFound = false;
            for (size_t i = 0; i < wrList.size(); ++i) {
                if (badWr == &wrList[i]) {
                    submittedCount = i;
                    badWrFound = true;
                    break;
                }
            }
            if (!badWrFound) {
                const auto srcAddress = localUrmaInfo_.localAddress.ToString();
                LOG(WARNING) << "[URMA_WRITE]: provider returned a bad_wr outside the submitted WR chain; "
                             << "treating all gather-write events as potentially accepted, srcAddress=" << srcAddress
                             << ", targetAddress=" << remoteAddress
                             << ", dataSize=" << static_cast<size_t>(totalWriteSize)
                             << ", dstChipId=" << static_cast<uint32_t>(remoteInfo.dstChipId);
            }
        } else {
            const auto srcAddress = localUrmaInfo_.localAddress.ToString();
            LOG(WARNING) << "[URMA_WRITE]: provider post failed without bad_wr; treating all gather-write events as "
                            "potentially accepted, srcAddress="
                         << srcAddress << ", targetAddress=" << remoteAddress
                         << ", dataSize=" << static_cast<size_t>(totalWriteSize)
                         << ", dstChipId=" << static_cast<uint32_t>(remoteInfo.dstChipId);
        }
        cleanupEvents(submittedCount);
        const auto srcAddress = localUrmaInfo_.localAddress.ToString();
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR,
                                FormatString("[URMA_WRITE]: call urma_post_jetty_send_wr failed, ret: %d, "
                                             "srcAddress=%s, targetAddress=%s, dataSize=%zu, dstChipId=%u, suggest: %s",
                                             ret, srcAddress.c_str(), remoteAddress.c_str(),
                                             static_cast<size_t>(totalWriteSize),
                                             static_cast<uint32_t>(remoteInfo.dstChipId), URMA_ERROR_SUGGEST));
    }

    eventKeys = createdEventKeys;
    if (blocking) {
        auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitFastTransportEvent(eventKeys, remainingTime, errorHandler));
        eventKeys.clear();
    }
    return Status::OK();
}

Status UrmaManager::RemoveRemoteResources(const std::string &connectionKey)
{
    bool removed = false;

    TbbUrmaConnectionMap::accessor connectionAccessor;
    if (urmaConnectionMap_.find(connectionAccessor, connectionKey)) {
        LOG(INFO) << "Remove UrmaConnection for " << connectionKey;
        auto &connection = connectionAccessor->second;
        if (connection != nullptr) {
            connection->Clear();
        }
        urmaConnectionMap_.erase(connectionAccessor);
        removed = true;
        INJECT_POINT("UrmaManager.RemoveRemoteResources");
    }

    if (!removed) {
        const auto msg = FormatString(
            "Skip removing URMA resources, connection key %s not found; may be already "
            "cleaned up or not established",
            connectionKey.c_str());
        LOG(INFO) << msg;
        RETURN_STATUS(K_NOT_FOUND, msg);
    }
    return Status::OK();
}

Status UrmaManager::RemoveRemoteDevice(const std::string &deviceId)
{
    return RemoveRemoteResources(deviceId);
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
        METRIC_TIMER(metrics::KvMetricId::URMA_CONNECTION_SETUP_LATENCY);
        RETURN_IF_NOT_OK(ImportRemoteJetty(urmaInfo, localJettyId));
        RETURN_IF_NOT_OK(ImportRemoteInfo(req));

        // Always populate response with local Jetty ID for the reverse connection
        auto localInfo = localUrmaInfo_;
        localInfo.jfrId = localJettyId;

        std::shared_ptr<UrmaJetty> localRecvJetty;
        const std::string remoteConnectionId =
            urmaInfo.clientId.empty() ? urmaInfo.localAddress.ToString() : urmaInfo.clientId;
        RETURN_IF_NOT_OK(GetLocalJetty(remoteConnectionId, localRecvJetty, JettyType::RECV));
        urma_rjetty_t *rjetty = nullptr;
        uint32_t rjettyLen = 0;
        urma_status_t urmaStatus = ds_urma_get_rjetty(localRecvJetty->Raw(), &rjetty, &rjettyLen);
        if (urmaStatus == URMA_SUCCESS && rjetty != nullptr && rjettyLen > 0) {
            localInfo.rjettyBuf.assign(reinterpret_cast<const char *>(rjetty), rjettyLen);
            ds_urma_put_rjetty(rjetty);
            LOG(INFO) << "[URMA_CONNECT] Got delegated rjetty context for response, length=" << rjettyLen;
        } else {
            LOG(WARNING) << "[URMA_CONNECT] Failed to get delegated rjetty context for response, status=" << urmaStatus
                         << ", fallback to legacy handshake";
        }

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
    return RemoveRemoteResources(remoteConnectionId);
}

bool UrmaManager::HasRemoteClient(ClientKey clientEntityId)
{
    std::lock_guard<std::mutex> lock(clientIdMutex_);
    return clientIdMapping_.find(clientEntityId) != clientIdMapping_.end();
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
