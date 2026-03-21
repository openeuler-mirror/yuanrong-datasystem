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

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
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

DS_DECLARE_uint32(urma_poll_size);
DS_DECLARE_uint32(urma_connection_size);
DS_DECLARE_bool(urma_event_mode);

namespace datasystem {
constexpr uint32_t DEFAULT_TOKEN = 0xACFE;
constexpr uint64_t MAX_STUB_CACHE_NUM = 2048;
bool UrmaManager::clientMode_ = false;
UrmaManager &UrmaManager::Instance()
{
    static UrmaManager manager;
    return manager;
}

UrmaManager::UrmaManager()
{
    VLOG(RPC_LOG_LEVEL) << "UrmaManager::UrmaManager()";
    urmaToken_.token = DEFAULT_TOKEN;

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
    localSegmentMap_ = std::make_unique<SegmentMap>();
}

UrmaManager::~UrmaManager()
{
    Stop();
    VLOG(RPC_LOG_LEVEL) << "UrmaManager::~UrmaManager()";
    tbbRemoteDeviceMap_.clear();
    localSegmentMap_.reset();
    tbbEventMap_.clear();
    urmaJfrVec_.clear();
    urmaJfsVec_.clear();
    urmaJfc_.reset();
    UrmaDeleteJfce();
    UrmaDeleteContext();
    UrmaUninit();
    urma_dlopen::Cleanup();
    if (memoryBuffer_ != nullptr) {
        munmap(memoryBuffer_, ubTransportMemSize_);
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
    RETURN_IF_NOT_OK(UrmaQueryDevice(urmaDevice));
    uint8_t priority = 0;
    uint32_t sl = 0;
    bool foundPriority = GetJfsPriorityInfoForCTP(priority, sl);
    LOG(INFO) << "UrmaManager CTP priority=" << static_cast<uint32_t>(priority) << ", SL=" << sl
              << ", useDefaultPriority=" << !foundPriority;

    if (eidIndex < 0) {
        RETURN_IF_NOT_OK(GetEidIndex(urmaDevice, eidIndex));
    }
    RETURN_IF_NOT_OK(UrmaCreateContext(urmaDevice, eidIndex));
    RETURN_IF_NOT_OK(UrmaCreateJfce());
    RETURN_IF_NOT_OK(UrmaCreateJfc(urmaJfc_));
    if (IsEventModeEnabled()) {
        RETURN_IF_NOT_OK(UrmaRearmJfc(urmaJfc_));
    }
    urmaJfsVec_.resize(FLAGS_urma_connection_size);
    urmaJfrVec_.resize(FLAGS_urma_connection_size);
    for (uint i = 0; i < FLAGS_urma_connection_size; ++i) {
        RETURN_IF_NOT_OK(UrmaCreateJfs(urmaJfc_, priority, urmaJfsVec_[i]));
        RETURN_IF_NOT_OK(UrmaCreateJfr(urmaJfc_, urmaJfrVec_[i]));
    }
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
    RETURN_IF_NOT_OK(ParseEnvUint64(UB_TRANSPORT_MEM_SIZE, ubTransportMemSize_));
    RETURN_IF_NOT_OK(ParseEnvUint64(UB_MAX_GET_DATA_SIZE, ubMaxGetDataSize_));
    RETURN_IF_NOT_OK(ParseEnvUint64(UB_MAX_SET_BUFFER_SIZE, ubMaxSetBufferSize_));

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

Status UrmaManager::UrmaQueryDevice(urma_device_t *&urmaDevice)
{
    LOG(INFO) << "UrmaManager::UrmaQueryDevice()";
    urma_status_t ret = ds_urma_query_device(urmaDevice, &urmaDeviceAttribute_);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma query device, ret = %d", ret));
    }
    LOG(INFO) << "urma query device success with dev type:" << urmaDevice->type;
    return Status::OK();
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

Status UrmaManager::UrmaCreateContext(urma_device_t *&urmaDevice, uint32_t eidIndex)
{
    LOG(INFO) << "UrmaManager::UrmaCreateContext() with eidIndex:" << eidIndex;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!urmaContext_, K_DUPLICATED,
                                         "Failed to urma create context, context already exist");
    urmaContext_ = ds_urma_create_context(urmaDevice, eidIndex);
    if (urmaContext_) {
        LOG(INFO) << "urma create context success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create context, errno = %d", errno));
}

Status UrmaManager::UrmaDeleteContext()
{
    LOG(INFO) << "UrmaManager::UrmaDeleteContext()";
    if (urmaContext_) {
        urma_status_t ret = ds_urma_delete_context(urmaContext_);
        if (ret != URMA_SUCCESS) {
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma delete context, ret = %d", ret));
        }
        LOG(INFO) << "urma delete context success";
        urmaContext_ = nullptr;
    }
    return Status::OK();
}

Status UrmaManager::UrmaCreateJfce()
{
    LOG(INFO) << "UrmaManager::UrmaCreateJfce()";
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!urmaJfce_, K_DUPLICATED, "Failed to urma create jfce, jfce already exist");

    urmaJfce_ = ds_urma_create_jfce(urmaContext_);
    if (urmaJfce_) {
        LOG(INFO) << "urma create jfce success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfce, errno = %d", errno));
}

Status UrmaManager::UrmaDeleteJfce()
{
    LOG(INFO) << "UrmaManager::UrmaDeleteJfce()";
    if (urmaJfce_) {
        urma_status_t ret = ds_urma_delete_jfce(urmaJfce_);
        if (ret != URMA_SUCCESS) {
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma delete jfce, ret = %d", ret));
        }
        LOG(INFO) << "urma delete jfce success";
        urmaJfce_ = nullptr;
    }
    return Status::OK();
}

Status UrmaManager::UrmaCreateJfc(custom_unique_ptr<urma_jfc_t> &out)
{
    LOG(INFO) << "UrmaManager::UrmaCreateJfc()";
    urma_jfc_cfg_t jfcConfig;
    jfcConfig.depth = urmaDeviceAttribute_.dev_cap.max_jfc_depth;
    jfcConfig.flag.value = 0;
    if (GetUrmaMode() == UrmaMode::IB) {
        jfcConfig.jfce = urmaJfce_;
    } else if (GetUrmaMode() == UrmaMode::UB) {
        jfcConfig.jfce = nullptr;
    }
    jfcConfig.user_ctx = 0;
    jfcConfig.ceqn = 0;

    out = MakeCustomUnique<urma_jfc_t>(ds_urma_create_jfc(urmaContext_, &jfcConfig), [](urma_jfc_t *p) {
        std::stringstream oss;
        oss << "urma_delete_jfc ... ";
        auto ret = ds_urma_delete_jfc(p);
        if (ret == URMA_SUCCESS) {
            oss << "success";
        } else {
            oss << FormatString("failed. ret = %d", ret);
        }
        LOG(INFO) << oss.str();
    });
    if (out) {
        LOG(INFO) << "urma create jfc success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfc, errno = %d", errno));
}

Status UrmaManager::UrmaRearmJfc(const custom_unique_ptr<urma_jfc_t> &jfc)
{
    LOG(INFO) << "UrmaManager::UrmaRearmJfc()";
    urma_status_t ret = ds_urma_rearm_jfc(jfc.get(), false);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma rearm jfc, ret = %d", ret));
    }
    LOG(INFO) << "urma rearm jfc success";
    return Status::OK();
}

bool UrmaManager::GetJfsPriorityInfoForCTP(uint8_t &priority, uint32_t &sl) const
{
    constexpr uint8_t defaultPriorityForCTP = 6;
    constexpr uint32_t defaultSLForCTP = 6;
    urma_tp_type_en tpTypeEn;
    tpTypeEn.value = 0;
    tpTypeEn.bs.ctp = 1;

    for (uint32_t i = 0; i <= URMA_MAX_PRIORITY; ++i) {
        auto &priorityInfo = urmaDeviceAttribute_.dev_cap.priority_info[i];
        VLOG(1) << "Checking priority " << i << " with tp_type: " << priorityInfo.tp_type.value
                << " expect tp_type: " << tpTypeEn.value;
        if (priorityInfo.tp_type.value == tpTypeEn.value) {
            priority = i;
            sl = priorityInfo.SL;
            return true;
        }
    }
    // Older URMA versions may not populate priority_info, so fall back
    // to the default priority and SL for CTP.
    priority = defaultPriorityForCTP;
    sl = defaultSLForCTP;
    return false;
}

Status UrmaManager::UrmaCreateJfs(const custom_unique_ptr<urma_jfc_t> &jfc, uint8_t priority,
                                  custom_unique_ptr<urma_jfs_t> &out)
{
    LOG(INFO) << "UrmaManager::UrmaCreateJfs()";

    urma_jfs_cfg_t jfsConfig;
    jfsConfig.depth = JETTY_SIZE_;
    jfsConfig.trans_mode = URMA_TM_RM;
    jfsConfig.priority = priority;
    const auto maxSge = 13;
    jfsConfig.max_sge = maxSge;
    jfsConfig.max_inline_data = 0;
    jfsConfig.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jfsConfig.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jfsConfig.jfc = jfc.get();
    jfsConfig.user_ctx = 0;
    jfsConfig.flag.value = 0;
#ifdef URMA_OVER_UB
    if (GetUrmaMode() == UrmaMode::UB) {
        jfsConfig.flag.bs.multi_path = 1;
    }
#endif

    out = MakeCustomUnique<urma_jfs_t>(ds_urma_create_jfs(urmaContext_, &jfsConfig), [](urma_jfs_t *p) {
        std::stringstream oss;
        oss << "urma_delete_jfs ... ";
        auto ret = ds_urma_delete_jfs(p);
        if (ret == URMA_SUCCESS) {
            oss << "success";
        } else {
            oss << FormatString("failed. ret = %d", ret);
        }
        LOG(INFO) << oss.str();
    });
    if (out) {
        LOG(INFO) << "urma create jfs success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfs, errno = %d", errno));
}

Status UrmaManager::UrmaCreateJfr(const custom_unique_ptr<urma_jfc_t> &jfc, custom_unique_ptr<urma_jfr_t> &out)
{
    LOG(INFO) << "UrmaManager::UrmaCreateJfr()";

    urma_jfr_cfg_t jfrConfig;
    jfrConfig.depth = JETTY_SIZE_;
    jfrConfig.flag.value = 0;
    jfrConfig.flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
    jfrConfig.trans_mode = URMA_TM_RM;
    jfrConfig.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfrConfig.jfc = jfc.get();
    jfrConfig.token_value = urmaToken_;
    jfrConfig.id = 0;
    jfrConfig.max_sge = 1;
    jfrConfig.user_ctx = (uint64_t)NULL;

    out = MakeCustomUnique<urma_jfr_t>(ds_urma_create_jfr(urmaContext_, &jfrConfig), [](urma_jfr_t *p) {
        std::stringstream oss;
        oss << "urma_delete_jfr ... ";
        auto ret = ds_urma_delete_jfr(p);
        if (ret == URMA_SUCCESS) {
            oss << "success";
        } else {
            oss << FormatString("failed. ret = %d", ret);
        }
        LOG(INFO) << oss.str();
    });
    if (out) {
        LOG(INFO) << "urma create jfr success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfr, errno = %d", errno));
}

bool UrmaManager::IsEventModeEnabled()
{
    return FLAGS_urma_event_mode;
}

std::string UrmaManager::GetEid()
{
    return EidToStr(urmaContext_->eid);
};

uint64_t UrmaManager::GetUasid()
{
    return urmaContext_->uasid;
};

std::vector<uint32_t> UrmaManager::GetJfrIds()
{
    std::vector<uint32_t> results;
    for (auto &urmaJfr : urmaJfrVec_) {
        results.emplace_back(urmaJfr->jfr_id.id);
    }
    return results;
};

Status UrmaManager::RegisterSegment(const uint64_t &segAddress, const uint64_t &segSize)
{
    SegmentMap::ConstAccessor constAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, constAccessor));
    return Status::OK();
}

Status UrmaManager::GetSegmentInfo(UrmaHandshakeReqPb &handshakeReq)
{
    PerfPoint point(PerfKey::WORKER_URMA_GET_SEGMENT);
    // Traverse the list of local registered segments.
    std::unique_lock<std::shared_timed_mutex> l(localMapMutex_);
    for (auto iter = localSegmentMap_->begin(); iter != localSegmentMap_->end(); iter++) {
        auto *segInfo = handshakeReq.add_seg_infos();
        auto &localSegment = iter->second.data.segment_;
        auto segPb = segInfo->mutable_seg();
        UrmaSeg::ToProto(localSegment->seg, *segPb);
        LOG(INFO) << "local seg info: " << UrmaSeg::ToString(localSegment->seg);
    }
    return Status::OK();
}

Status UrmaManager::GetSegmentInfo(UrmaHandshakeRspPb &handshakeRsp)
{
    PerfPoint point(PerfKey::WORKER_URMA_GET_SEGMENT);
    std::unique_lock<std::shared_timed_mutex> l(localMapMutex_);
    for (auto iter = localSegmentMap_->begin(); iter != localSegmentMap_->end(); iter++) {
        auto *segInfo = handshakeRsp.mutable_hand_shake()->add_seg_infos();
        auto &localSegment = iter->second.data.segment_;
        auto segPb = segInfo->mutable_seg();
        UrmaSeg::ToProto(localSegment->seg, *segPb);
        LOG(INFO) << "local seg info (rsp): " << UrmaSeg::ToString(localSegment->seg);
    }
    return Status::OK();
}

Status UrmaManager::GetOrRegisterSegment(const uint64_t &segAddress, const uint64_t &segSize,
                                         SegmentMap::ConstAccessor &constAccessor)
{
    std::shared_lock<std::shared_timed_mutex> l(localMapMutex_);
    if (!localSegmentMap_->Find(constAccessor, segAddress)) {
        SegmentMap::Accessor accessor;
        if (localSegmentMap_->Insert(accessor, segAddress)) {
            urma_seg_cfg_t segmentConfig;
            segmentConfig.va = segAddress;
            segmentConfig.len = segSize;
            segmentConfig.token_value = urmaToken_;
            segmentConfig.flag = registerSegmentFlag_;
            segmentConfig.user_ctx = (uint64_t)NULL;
            segmentConfig.iova = 0;
            segmentConfig.token_id = nullptr;
            PerfPoint point(PerfKey::URMA_REGISTER_SEGMENT);
            auto *segment = ds_urma_register_seg(urmaContext_, &segmentConfig);
            point.Record();
            if (segment == nullptr) {
                localSegmentMap_->BlockingErase(accessor);
                return Status(K_RUNTIME_ERROR, FormatString("Failed to register segment, address %llu, size %llu.",
                                                            segAddress, segSize));
            }
            auto tokenId = segment->token_id != nullptr ? segment->token_id->token_id : 0;
            auto tokenId2 = segment->seg.token_id;
            LOG(INFO) << "register segment success, token_id:" << tokenId << ", token_id2:" << tokenId2;
            accessor.entry->data.Set(segment, true);
        }
        accessor.Release();
        // Switch to const accessor so it does not block the others.
        CHECK_FAIL_RETURN_STATUS(localSegmentMap_->Find(constAccessor, segAddress), K_RUNTIME_ERROR,
                                 "Failed to operate on local segment map.");
    }
    return Status::OK();
}

Status UrmaManager::ServerEventHandleThreadMain()
{
    // Run this method until serverStop is called.
    while (!serverStop_.load()) {
        std::vector<uint64_t> successCompletedReqs;
        std::vector<uint64_t> failedCompletedReqs;
        Status rc = PollJfcWait(urmaJfc_, MAX_POLL_JFC_TRY_CNT, successCompletedReqs, failedCompletedReqs,
                                FLAGS_urma_poll_size);

        // push it into request set
        // we do not need lock for finishedRequests_ as its accessed only by single thread
        if (successCompletedReqs.size()) {
            finishedRequests_.insert(successCompletedReqs.begin(), successCompletedReqs.end());
        }
        if (failedCompletedReqs.size()) {
            finishedRequests_.insert(failedCompletedReqs.begin(), failedCompletedReqs.end());
            failedRequests_.insert(failedCompletedReqs.begin(), failedCompletedReqs.end());
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
        std::shared_ptr<Event> event;
        // Get the event for request Id
        if (GetEvent(requestId, event).IsOk()) {
            if (failedRequests_.count(requestId)) {
                event->SetFailed();
                failedRequests_.erase(requestId);
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

Status UrmaManager::GetEvent(uint64_t requestId, std::shared_ptr<Event> &event)
{
    TbbEventMap::accessor mapAccessor;
    if (tbbEventMap_.find(mapAccessor, requestId)) {
        event = mapAccessor->second;
        return Status::OK();
    }
    // Can happen if event is not yet inserted by sender thread.
    RETURN_STATUS(K_NOT_FOUND, FormatString("Request id %d doesnt exist in event map", requestId));
}

Status UrmaManager::CreateEvent(uint64_t requestId, std::shared_ptr<Event> &event, std::shared_ptr<EventWaiter> waiter)
{
    (void)event;
    TbbEventMap::accessor mapAccessor;
    auto res = tbbEventMap_.insert(mapAccessor, requestId);
    if (!res) {
        // If this happens that means requestId is duplicated.
        RETURN_STATUS_LOG_ERROR(K_DUPLICATED, FormatString("Request id %d already exists in event map", requestId));
    } else {
        mapAccessor->second = std::make_shared<Event>(requestId, waiter);
    }
    return Status::OK();
}

Status UrmaManager::WaitToFinish(uint64_t requestId, int64_t timeoutMs)
{
    PerfPoint point(PerfKey::URMA_WAIT_TO_FINISH);
    if (timeoutMs < 0) {
        RETURN_STATUS_LOG_ERROR(K_RPC_DEADLINE_EXCEEDED, FormatString("timedout waiting for request: %d", requestId_));
    }
    std::shared_ptr<Event> event;
    RETURN_IF_NOT_OK(GetEvent(requestId, event));
    // use this unique request id as key to wait
    // wait until timeout

    Raii deleteEvent([this, &requestId]() { DeleteEvent(requestId); });

    VLOG(1) << "[UrmaEventHandler] Started waiting for the request id: " << requestId;
    PerfPoint waitPoint(PerfKey::URMA_WAIT_TIME);
    RETURN_IF_NOT_OK(event->WaitFor(std::chrono::milliseconds(timeoutMs)));
    waitPoint.Record();
    if (event->IsFailed()) {
        return Status(K_URMA_ERROR, FormatString("Polling failed with an error for requestId: %d", requestId));
    }
    VLOG(1) << "[UrmaEventHandler] Done waiting for the request id: " << requestId;
    return Status::OK();
}

Status UrmaManager::CheckCompletionRecordStatus(urma_cr_t completeRecords[], int count,
                                                std::vector<uint64_t> &successCompletedReqs,
                                                std::vector<uint64_t> &failedCompletedReqs)
{
    INJECT_POINT("UrmaManager.CheckCompletionRecordStatus", [&completeRecords](int index) {
        completeRecords[index].status = URMA_CR_REM_ACCESS_ABORT_ERR;
        return Status::OK();
    });
    for (int i = 0; i < count; i++) {
        if (completeRecords[i].status == URMA_CR_SUCCESS) {
            VLOG(1) << "[UrmaEventHandler] Got event with request id: " << completeRecords[i].user_ctx;
            successCompletedReqs.push_back(completeRecords[i].user_ctx);
        } else {
            LOG(ERROR) << FormatString("Failed to poll jfc requestId: %d CR.status: %d", completeRecords[i].user_ctx,
                                       completeRecords[i].status);
            failedCompletedReqs.push_back(completeRecords[i].user_ctx);
        }
    }

    if (failedCompletedReqs.size()) {
        RETURN_STATUS(K_URMA_ERROR, "Failed to poll jfc");
    }
    return Status::OK();
}

Status UrmaManager::PollJfcWait(const custom_unique_ptr<urma_jfc_t> &jfc, const uint64_t maxTryCount,
                                std::vector<uint64_t> &successCompletedReqs, std::vector<uint64_t> &failedCompletedReqs,
                                const uint64_t numPollCRS)
{
    auto urmaJfc = jfc.get();
    urma_cr_t completeRecords[numPollCRS];
    urma_jfc_t *ev_jfc = nullptr;
    int cnt;

    if (IsEventModeEnabled()) {
        // wait for the event
        cnt = ds_urma_wait_jfc(urmaJfce_, 1, RPC_POLL_TIME, &ev_jfc);
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
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Worker exiting"));
        }
    }
    RETURN_STATUS(K_TRY_AGAIN, FormatString("No Event present in JFC"));
}

Status UrmaManager::ImportRemoteJfr(const UrmaJfrInfo &urmaInfo)
{
    PerfPoint point1(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    const std::string remoteDeviceId = urmaInfo.clientId.empty() ? urmaInfo.localAddress.ToString() : urmaInfo.clientId;
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    // Insert or update the import jfr (in case the sending worker restarts)
    TbbRemoteDeviceMap::accessor accessor;
    auto res = tbbRemoteDeviceMap_.insert(accessor, remoteDeviceId);
    auto &device = accessor->second;
    if (!res) {
        RETURN_OK_IF_TRUE(device.urmaInfo_.ToString() == urmaInfo.ToString());
        // Existing entry exists, and we will update the imported jfr (assuming the sending worker restarted)
        // First of all, release the existing imported jfr
        device.Clear();
    }
    device.urmaInfo_ = urmaInfo;
    auto jfsIndex = localJfsIndex_.fetch_add(1) % FLAGS_urma_connection_size;
    device.jfsIndex_ = jfsIndex;

    // Now we import a new jfr
    urma_rjfr_t remoteJfr;
    urma_eid_t eid;
    auto rc = StrToEid(urmaInfo.eid, eid);
    if (rc.IsError()) {
        tbbRemoteDeviceMap_.erase(accessor);
        return rc;
    }
    remoteJfr.jfr_id.eid = eid;
    remoteJfr.jfr_id.uasid = urmaInfo.uasid;
    remoteJfr.trans_mode = URMA_TM_RM;
    remoteJfr.tp_type = URMA_CTP;

    std::vector<urma_target_jetty_t *> tjfrs;
    Timer timer;
    for (uint i = 0; i < urmaInfo.jfrIds.size(); ++i) {
        remoteJfr.jfr_id.id = urmaInfo.jfrIds[i];
        PerfPoint point1a(PerfKey::URMA_IMPORT_JFR);
        auto *tjfr = ds_urma_import_jfr(urmaContext_, &remoteJfr, &urmaToken_);
        point1a.Record();
        if (tjfr == nullptr) {
            tbbRemoteDeviceMap_.erase(accessor);
            return Status(K_RUNTIME_ERROR, FormatString("Failed to import jfr, %s", urmaInfo.ToString()));
        }
        PerfPoint point1b(PerfKey::URMA_ADVISE_JFR);
        auto jfs = urmaJfsVec_[jfsIndex].get();
        if (ds_urma_advise_jfr(jfs, tjfr) != URMA_SUCCESS) {
            (void)ds_urma_unimport_jfr(tjfr);
            tbbRemoteDeviceMap_.erase(accessor);
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to advise jfr"));
        }
        point1b.Record();
        tjfrs.emplace_back(tjfr);
        // only import one jfr
        break;
    }
    LOG_IF(WARNING, timer.ElapsedSecond() > 1) << "ImportRemoteJfr exceed 1s!";
    device.SetJfrs(tjfrs);
    point1.Record();
    return Status::OK();
}

Status UrmaManager::ImportRemoteInfo(const UrmaHandshakeReqPb &req)
{
    const HostPort requestAddress(req.address().host(), req.address().port());
    const std::string remoteDeviceId = req.client_id().empty() ? requestAddress.ToString() : req.client_id();
    PerfPoint point1(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbRemoteDeviceMap::accessor accessor;
    // The comm layer (zmq) has already exchanged the jfr, and we should be able to locate the entry.
    auto res = tbbRemoteDeviceMap_.find(accessor, remoteDeviceId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteDeviceId));
    point1.Record();
    PerfPoint point2(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
    for (int i = 0; i < req.seg_infos_size(); i++) {
        auto &segInfo = req.seg_infos(i);
        auto rc = accessor->second.ImportRemoteSeg(urmaContext_, segInfo);
        if (rc.IsError()) {
            // clear import jfr and seg to reconnect next time
            tbbRemoteDeviceMap_.erase(accessor);
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
    const std::string remoteDeviceId = requestAddress.ToString();
    PerfPoint point1(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbRemoteDeviceMap::accessor accessor;
    auto res = tbbRemoteDeviceMap_.find(accessor, remoteDeviceId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteDeviceId));
    point1.Record();
    PerfPoint point2(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
    for (int i = 0; i < handShake.seg_infos_size(); i++) {
        auto &segInfo = handShake.seg_infos(i);
        auto rc = accessor->second.ImportRemoteSeg(urmaContext_, segInfo);
        if (rc.IsError()) {
            tbbRemoteDeviceMap_.erase(accessor);
            return rc;
        }
    }
    point2.Record();
    return Status::OK();
}

Status UrmaManager::UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                     const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                     const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                     bool blocking, std::vector<uint64_t> &keys, std::shared_ptr<EventWaiter> waiter)
{
    // Note that the returned keys only contain the new key(s).
    keys.clear();
    PerfPoint point(PerfKey::URMA_IMPORT_AND_WRITE_PAYLOAD);
    const uint64_t segVa = urmaInfo.seg_va();
    const HostPort requestAddress(urmaInfo.request_address().host(), urmaInfo.request_address().port());
    std::string remoteDeviceId = urmaInfo.client_id().empty() ? requestAddress.ToString() : urmaInfo.client_id();
    PerfPoint point1(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbRemoteDeviceMap::const_accessor constAccessor;
    // The comm layer (zmq) has already exchanged the jfr, and we should be able to locate the entry.
    auto res = tbbRemoteDeviceMap_.find(constAccessor, remoteDeviceId);
    if (!res && !urmaInfo.client_id().empty()) {
        remoteDeviceId = requestAddress.ToString();
        res = tbbRemoteDeviceMap_.find(constAccessor, remoteDeviceId);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteDeviceId));
    point1.Record();
    PerfPoint point2(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
    SegmentMap::ConstAccessor remoteSegAccessor;
    auto &device = constAccessor->second;
    auto jfsIndex = device.jfsIndex_;
    RETURN_IF_NOT_OK(device.GetRemoteSeg(segVa, remoteSegAccessor));
    point2.Record();

    PerfPoint point3(PerfKey::URMA_REGISTER_LOCAL_SEGMENT);
    SegmentMap::ConstAccessor localSegAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(localSegAddress, localSegSize, localSegAccessor));
    point3.Record();

    // Write payload
    urma_jfs_wr_flag_t flag;
    flag.value = 0;
    flag.bs.complete_enable = 1;

    PerfPoint point4(PerfKey::URMA_TOTAL_WRITE);
    uint64_t writtenSize = 0;
    uint64_t remainSize = readSize;
    while (remainSize > 0) {
        const uint64_t writeSize = std::min(remainSize, (uint64_t)urmaDeviceAttribute_.dev_cap.max_write_size);
        const uint64_t key = requestId_.fetch_add(1);
        urma_jfs_t *urmaJfs = urmaJfsVec_[jfsIndex].get();
        const auto &remoteJfrs = device.importJfrs_;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!remoteJfrs.empty(), K_RUNTIME_ERROR, "Write got empty remote jfrs.");
        urma_target_jetty_t *importJfr = remoteJfrs[0].get();
        PerfPoint point4a(PerfKey::URMA_WRITE);
        urma_status_t ret = ds_urma_write(
            urmaJfs, importJfr, remoteSegAccessor.entry->data.segment_.get(),
            localSegAccessor.entry->data.segment_.get(), segVa + urmaInfo.seg_data_offset() + readOffset + writtenSize,
            localObjectAddress + readOffset + metaDataSize + writtenSize, writeSize, flag, key);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            ret == URMA_SUCCESS, K_RUNTIME_ERROR,
            FormatString("Failed to urma write object with key = %zu, ret = %d", key, ret));
        point4a.Record();
        keys.emplace_back(key);

        remainSize -= writeSize;
        writtenSize += writeSize;

        std::shared_ptr<Event> event;
        RETURN_IF_NOT_OK(CreateEvent(key, event, waiter));
    }
    point4.Record();
    // If it is blocking wait, we will wait for the write to finish here.
    if (blocking) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitFastTransportEvent(keys, remainingTime, errorHandler));
        keys.clear();
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
    std::string remoteDeviceId = urmaInfo.client_id().empty() ? requestAddress.ToString() : urmaInfo.client_id();
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbRemoteDeviceMap::const_accessor constAccessor;
    // The comm layer (zmq) has already exchanged the jfr, and we should be able to locate the entry.
    auto res = tbbRemoteDeviceMap_.find(constAccessor, remoteDeviceId);
    if (!res && !urmaInfo.client_id().empty()) {
        remoteDeviceId = requestAddress.ToString();
        res = tbbRemoteDeviceMap_.find(constAccessor, remoteDeviceId);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteDeviceId));
    SegmentMap::ConstAccessor remoteSegAccessor;
    auto &device = constAccessor->second;
    RETURN_IF_NOT_OK(device.GetRemoteSeg(segVa, remoteSegAccessor));

    SegmentMap::ConstAccessor localSegAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(localSegAddress, localSegSize, localSegAccessor));

    urma_jfs_wr_flag_t flag;
    flag.value = 0;
    flag.bs.complete_enable = 1;

    uint64_t readOffset = 0;
    uint64_t remainSize = dataSize;
    while (remainSize > 0) {
        const uint64_t readSize = std::min(remainSize, (uint64_t)urmaDeviceAttribute_.dev_cap.max_read_size);
        const uint64_t key = requestId_.fetch_add(1);
        urma_jfs_t *urmaJfs = urmaJfsVec_[key % FLAGS_urma_connection_size].get();
        const auto &remoteJfrs = constAccessor->second.importJfrs_;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!remoteJfrs.empty(), K_RUNTIME_ERROR, "Read got empty remote jfrs.");
        const uint64_t jfrIndex = key % remoteJfrs.size();
        urma_target_jetty_t *importJfr = remoteJfrs[jfrIndex].get();
        urma_status_t ret =
            ds_urma_read(urmaJfs, importJfr, localSegAccessor.entry->data.segment_.get(),
                         remoteSegAccessor.entry->data.segment_.get(), localObjectAddress + metaDataSize + readOffset,
                         segVa + urmaInfo.seg_data_offset() + readOffset, readSize, flag, key);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            ret == URMA_SUCCESS, K_RUNTIME_ERROR,
            FormatString("Failed to urma read object with key = %zu, ret = %d", key, ret));
        keys.emplace_back(key);

        remainSize -= readSize;
        readOffset += readSize;

        std::shared_ptr<Event> event;
        RETURN_IF_NOT_OK(CreateEvent(key, event));
    }
    return Status::OK();
}

Status UrmaManager::UrmaGatherWrite(const RemoteSegInfo &remoteInfo, const std::vector<LocalSgeInfo> &objInfos,
                                    bool blocking, std::vector<uint64_t> &eventKeys)
{
    eventKeys.clear();
    auto segVa = remoteInfo.segAddr;
    const HostPort requestAddress(remoteInfo.host, remoteInfo.port);
    const std::string remoteDeviceId = requestAddress.ToString();
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbRemoteDeviceMap::const_accessor constAccessor;
    auto res = tbbRemoteDeviceMap_.find(constAccessor, remoteDeviceId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteDeviceId));
    SegmentMap::ConstAccessor remoteSegAccessor;
    auto &device = constAccessor->second;
    RETURN_IF_NOT_OK(device.GetRemoteSeg(segVa, remoteSegAccessor));
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
    for (auto dstSgeIdx = 0U, srcSgeIdx = 0U; dstSgeIdx < dstSgeNum; dstSgeIdx++) {
        auto singleDstWriteSize = 0;
        urma_sg_t srcSg = { .sge = &srcSgeList[srcSgeIdx], .num_sge = static_cast<uint32_t>(srcSgeIdx) };
        while (srcSgeIdx < sgeNum) {
            auto &ele = objInfos[srcSgeIdx];
            SegmentMap::ConstAccessor localSegAccessor;
            RETURN_IF_NOT_OK(GetOrRegisterSegment(ele.segAddr, ele.segSize, localSegAccessor));
            srcSgeList[srcSgeIdx] = urma_sge_t{ .addr = ele.sgeAddr + ele.metaDataSize + ele.readOffset,
                                                .len = static_cast<uint32_t>(ele.writeSize),
                                                .tseg = localSegAccessor.entry->data.segment_.get(),
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
                                  .tseg = remoteSegAccessor.entry->data.segment_.get(),
                                  .user_tseg = nullptr };
        totalWriteSize += singleDstWriteSize;
        urma_sg_t dstSg = { .sge = &dstSgeList[dstSgeIdx], .num_sge = 1 };
        urma_rw_wr_t rw = { .src = srcSg, .dst = dstSg, .target_hint = 0, .notify_data = 0 };
        const uint64_t key = requestId_.fetch_add(1);
        const auto &remoteJfrs = constAccessor->second.importJfrs_;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!remoteJfrs.empty(), K_RUNTIME_ERROR,
                                             "Gather write got empty remote jfrs.");
        uint64_t index = key % remoteJfrs.size();
        urma_target_jetty_t *importJfr = remoteJfrs[index].get();
        wrList[dstSgeIdx] = {
            .opcode = URMA_OPC_WRITE, .flag = flag, .tjetty = importJfr, .user_ctx = key, .rw = rw, .next = NULL
        };
        eventKeys.emplace_back(key);
        std::shared_ptr<Event> event;
        RETURN_IF_NOT_OK(CreateEvent(key, event));
        if (dstSgeIdx > 0) {
            wrList[dstSgeIdx - 1].next = &wrList[dstSgeIdx];
        }
    }
    urma_jfs_t *urmaJfs = urmaJfsVec_[requestId_ % FLAGS_urma_connection_size].get();
    urma_jfs_wr_t *bad_wr = NULL;
    auto ret = ds_urma_post_jfs_wr(urmaJfs, &wrList[0], &bad_wr);
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

urma_target_seg_t *UrmaManager::ImportSegment(urma_seg_t &remoteSegment)
{
    return ds_urma_import_seg(urmaContext_, &remoteSegment, &urmaToken_, 0, importSegmentFlag_);
}

Status UrmaManager::UnimportSegment(const HostPort &remoteAddress, const uint64_t segmentAddress)
{
    TbbRemoteDeviceMap::accessor accessor;
    if (tbbRemoteDeviceMap_.find(accessor, remoteAddress.ToString())) {
        RETURN_IF_NOT_OK(accessor->second.UnimportRemoteSeg(segmentAddress));
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, "Cannot unimport jfr, jfr is not imported");
}

Status UrmaManager::RemoveRemoteDevice(const std::string &deviceId)
{
    TbbRemoteDeviceMap::accessor accessor;
    if (tbbRemoteDeviceMap_.find(accessor, deviceId)) {
        LOG(INFO) << "Remove RemoteDevice for " << deviceId;
        tbbRemoteDeviceMap_.erase(accessor);
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND,
                  FormatString("Cannot remove RemoteDevice, RemoteDevice for %s does not exist", deviceId));
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
    TbbRemoteDeviceMap::const_accessor constAccessor;
    auto res = tbbRemoteDeviceMap_.find(constAccessor, hostAddress);
    if (!res) {
        RETURN_STATUS(K_URMA_NEED_CONNECT, "No existing connection requires creation.");
    }
    if (!instanceId.empty()) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(constAccessor->second.urmaInfo_.uniqueInstanceId == instanceId,
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

void UrmaManager::SetClientUrmaConfig(FastTransportMode urmaMode)
{
    // Note: The parameter needs to be consistent in the same client process.
    if (urmaMode == FastTransportMode::UB) {
        FLAGS_enable_urma = true;
        FLAGS_urma_mode = "UB";
        UrmaManager::clientMode_ = true;
        FLAGS_urma_connection_size = 1;
    }
}

std::string UrmaManager::GetRemoteDevicesByClientId(ClientKey clientEntityId)
{
    std::lock_guard<std::mutex> lock(clientIdMutex_);
    auto it = clientIdMapping_.find(clientEntityId);
    if (it != clientIdMapping_.end()) {
        return it->second;
    }
    LOG(INFO) << "Cannot find remote device for client entity id: " << clientEntityId;
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

Segment::~Segment()
{
    Clear();
}

void Segment::Set(urma_target_seg_t *seg, const bool local)
{
    Clear();
    local_ = local;
    segment_ = MakeCustomUnique<urma_target_seg_t>(seg, [this](urma_target_seg_t *p) {
        if (p) {
            if (local_) {
                urma_status_t ret = ds_urma_unregister_seg(p);
                LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unregister segment, ret = " << ret;
            } else {
                urma_status_t ret = ds_urma_unimport_seg(p);
                LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unimport segment, ret = " << ret;
            }
        }
    });
}

void Segment::Clear()
{
    segment_.reset();
}

RemoteDevice::~RemoteDevice()
{
    Clear();
}

void RemoteDevice::SetJfrs(std::vector<urma_target_jetty_t *> &jetties)
{
    Clear();
    for (auto &jetty : jetties) {
        importJfrs_.emplace_back(MakeCustomUnique<urma_target_jetty_t>(jetty, [](urma_target_jetty_t *p) {
            if (p) {
                auto ret = ds_urma_unimport_jfr(p);
                LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unimport jfr, ret = " << ret;
            }
        }));
    }
}

Status RemoteDevice::GetRemoteSeg(uint64_t segVa, SegmentMap::ConstAccessor &constAccessor) const
{
    if ((*remoteSegments_).Find(constAccessor, segVa)) {
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, FormatString("Remote segment is not found, segment VA: %lu", segVa));
}

Status RemoteDevice::ImportRemoteSeg(urma_context_t *urmaContext, const UrmaImportSegmentPb &importSegmentInfo)
{
    (void)urmaContext;
    SegmentMap::Accessor accessor;
    auto segVa = importSegmentInfo.seg().va();
    auto &remoteSegments = *remoteSegments_;
    if (!remoteSegments.Find(accessor, segVa)) {
        if (remoteSegments.Insert(accessor, segVa)) {
            bool needErase = true;
            Raii eraseSegment([this, &accessor, &needErase, &remoteSegments]() {
                if (needErase) {
                    remoteSegments.BlockingErase(accessor);
                }
            });

            // Import segment
            UrmaSeg remoteSegment;
            RETURN_IF_NOT_OK(remoteSegment.FromProto(importSegmentInfo.seg()));
            LOG(INFO) << "import remote seg info: " << remoteSegment.ToString() << ", client_id:" << urmaInfo_.clientId;
            auto *segment = UrmaManager::Instance().ImportSegment(remoteSegment.raw);
            CHECK_FAIL_RETURN_STATUS(segment != nullptr, K_RUNTIME_ERROR,
                                     FormatString("Failed to import segment %s.", remoteSegment.ToString()));
            accessor.entry->data.Set(segment, false);
            needErase = false;
        }
    }
    return Status::OK();
}

void RemoteDevice::Clear()
{
    importJfrs_.clear();
    remoteSegments_.reset();
    remoteSegments_ = std::make_unique<SegmentMap>();
}

Status RemoteDevice::UnimportRemoteSeg(const uint64_t segmentAddress)
{
    SegmentMap::Accessor accessor;
    auto &remoteSegments = *remoteSegments_;
    if (remoteSegments.Find(accessor, segmentAddress)) {
        remoteSegments.BlockingErase(accessor);
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, "Cannot unimport remote segment, remote segment is not imported");
}
}  // namespace datasystem
