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
#include <array>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <vector>

#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>

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
#include "datasystem/common/rpc/bthread_utils.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/numa_util.h"
#include "datasystem/common/device/nvidia/cuda_host_memory.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/sched_runtime.h"
#include "datasystem/common/util/timer.h"
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
constexpr uint32_t K_URMA_POOL_EXHAUSTED_LOG_EVERY_N = 5000;
constexpr uint32_t URMA_LOG_LIMIT_MS = 1;
constexpr uint32_t URMA_LOG_LIMIT_US = 250;
constexpr uint32_t URMA_WRITE_VLOG0_LIMIT_US = 200;
constexpr size_t URMA_CHIP_INFLIGHT_TRACKED_COUNT = 10;
constexpr size_t URMA_CHIP_INFLIGHT_LOG_BUFFER_SIZE = 320;
constexpr uint8_t URMA_AFFINITY_SRC_CHIP_MIN = 1;
constexpr uint8_t URMA_AFFINITY_SRC_CHIP_COUNT = 2;
constexpr uint8_t URMA_AFFINITY_SRC_CHIP_MAX = URMA_AFFINITY_SRC_CHIP_MIN + URMA_AFFINITY_SRC_CHIP_COUNT - 1;
constexpr uint64_t URMA_RECOVERY_PROBE_SEGMENT_SIZE = 4096;
constexpr uint64_t URMA_FIRST_WRITE_CHUNK_INDEX = 1;
constexpr uint64_t URMA_SECOND_WRITE_CHUNK_INDEX = 2;
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
#ifdef BONDP_USER_CTL_SET_CTX_CFG
        { 9, UrmaErrorHandlePolicy::DEFAULT },
#else
        { 9, UrmaErrorHandlePolicy::RECREATE_JETTY },
#endif
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
// The 40-bit space far exceeds the supported number of concurrent in-flight requests.
// In normal operation, an earlier request is retired long before the counter wraps,
// so wraparound does not cause an active request-ID collision.
constexpr uint64_t URMA_EFFECTIVE_REQUEST_ID_WIDTH = 40;
constexpr uint64_t URMA_EFFECTIVE_REQUEST_ID_MASK = (1ULL << URMA_EFFECTIVE_REQUEST_ID_WIDTH) - 1;

std::atomic<bool> UrmaManager::clientMode_{ false };
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
    srcChipInflightWrCounts_ = std::vector<SrcChipInflightCounter>(URMA_CHIP_INFLIGHT_TRACKED_COUNT);
    for (auto &count : srcChipInflightWrCounts_) {
        count.value.store(0, std::memory_order_relaxed);
    }
    srcChipWriteCounts_ = std::vector<std::atomic<uint64_t>>(URMA_CHIP_INFLIGHT_TRACKED_COUNT);
    dstChipWriteCounts_ = std::vector<std::atomic<uint64_t>>(URMA_CHIP_INFLIGHT_TRACKED_COUNT);
    for (auto &count : srcChipWriteCounts_) {
        count.store(0, std::memory_order_relaxed);
    }
    for (auto &count : dstChipWriteCounts_) {
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
    ClearRetainedTimeoutEvents();
    tbbEventMap_.clear();
    if (urmaResource_ != nullptr) {
        urmaResource_->Clear();
    }
    if (urmaResource_ == nullptr || !urmaResource_->IsProviderCleanupDeferred()) {
        UrmaUninit();
#ifdef USE_URMA_MOCK
        // Real URMA links liburma at compile time; Cleanup() is a no-op there.
        // Mock mode needs Cleanup() to reset the mock dispatch backend.
        urma_dlopen::Cleanup();
#endif
    }
    if (memoryBuffer_ != nullptr) {
        UnregisterCudaHostMemory(memoryBuffer_);
        munmap(memoryBuffer_, ubTransportMemSize_.load());
        memoryBuffer_ = nullptr;
    }
    if (recoveryProbeBuffer_ != nullptr) {
        munmap(recoveryProbeBuffer_, URMA_RECOVERY_PROBE_SEGMENT_SIZE);
        recoveryProbeBuffer_ = nullptr;
    }
    if (recoveryProbeSourceBuffer_ != nullptr) {
        munmap(recoveryProbeSourceBuffer_, URMA_RECOVERY_PROBE_SEGMENT_SIZE);
        recoveryProbeSourceBuffer_ = nullptr;
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
    ClearRetainedTimeoutEvents();
    return Status::OK();
}

Status UrmaManager::GetUrmaDeviceName(std::vector<std::string> &candidates, int &eidIndex)
{
    std::string configuredName = GetStringFromEnv(ENV_UB_DEVICE_NAME.c_str(), DEFAULT_UB_DEVICE_NAME.c_str());
    eidIndex = GetInt32FromEnv(ENV_UB_DEVICE_EID.c_str(), 0);
    if (configuredName.empty()) {
        RETURN_STATUS(K_INVALID, "env DS_URMA_DEV_NAME is empty");
    }
    RETURN_IF_NOT_OK(UrmaGetEffectiveDevices(configuredName, candidates));
    LOG(INFO) << FormatString("Got %zu urma device candidate(s)", candidates.size());
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
    std::vector<std::string> candidates;
    int eidIndex = -1;
    RETURN_IF_NOT_OK(GetUrmaDeviceName(candidates, eidIndex));
    if (FLAGS_urma_connection_size != 0) {
        LOG(WARNING) << "Flag urma_connection_size is deprecated and ignored. "
                     << "JFS/JFR are now created per-connection.";
    }
    OsXprtPipln::SetIsClientMode(clientMode_.load(std::memory_order_acquire));
    // Try each candidate device in order. UrmaResource::Init starts with Clear() (urma_resource.cpp), so a failed
    // attempt on one device is torn down before the next candidate is tried; the first device whose Init succeeds is
    // used. This lets a bare-metal worker start when its default bonding device (EID 0) is occupied by a container.
    Status lastErr(K_RUNTIME_ERROR, "No URMA device candidate available");
    for (size_t i = 0; i < candidates.size(); i++) {
        const std::string &candidate = candidates[i];
        LOG(INFO) << FormatString("Trying URMA device candidate[%zu/%zu]: %s", i + 1, candidates.size(), candidate);
        urma_device_t *device = nullptr;
        Status s = UrmaGetDeviceByName(candidate, device);
        if (!s.IsOk()) {
            lastErr = s;
            LOG(WARNING) << FormatString("UrmaGetDeviceByName failed for candidate %s: %s", candidate, s.GetMsg());
            continue;
        }
        int candEidIndex = eidIndex;
        if (candEidIndex < 0) {
            s = GetEidIndex(device, candEidIndex);
            if (!s.IsOk()) {
                lastErr = s;
                LOG(WARNING) << FormatString("GetEidIndex failed for device %s: %s", candidate, s.GetMsg());
                continue;
            }
        }
        const bool isBondingDevice = candidate.find("bonding", 0) == 0;
        s = urmaResource_->Init(device, candEidIndex, isBondingDevice);
        if (s.IsOk()) {
            LOG(INFO) << FormatString("UrmaResource::Init succeeded with device %s (eid index %d)", candidate,
                                      candEidIndex);
            lastErr = Status::OK();
            break;
        }
        LOG(WARNING) << FormatString("UrmaResource::Init failed for device %s: %s, trying next candidate", candidate,
                                     s.GetMsg());
        lastErr = s;
    }
    RETURN_IF_NOT_OK(lastErr);
    RETURN_IF_NOT_OK(InitLocalUrmaInfo(hostport));
    serverStop_ = false;
    serverEventThread_ = std::make_unique<Thread>(&UrmaManager::ServerEventHandleThreadMain, this);
    serverEventThread_->set_name("UrmaPollJfc");
    aeHandler_.Init(urmaResource_.get());
    aeHandler_.Start(serverStop_);

    if (UrmaManager::clientMode_.load(std::memory_order_acquire)) {
        RETURN_IF_NOT_OK(InitMemoryBufferPool());
        RegisterCudaHostMemory(memoryBuffer_, ubTransportMemSize_.load());
        clientId_ = GetStringUuid();
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
    SetClientTransportArenaConfig();

    const uint32_t arenaNum = FLAGS_ub_transport_arena_num;
    const long pageSize = getpagesize();
    uint64_t poolSize = 0;
    RETURN_IF_NOT_OK(NormalizeClientTransportPoolSize(ubTransportMemSize_.load(), arenaNum,
                                                      pageSize > 0 ? static_cast<uint64_t>(pageSize) : 0, poolSize));
    ubTransportMemSize_.store(poolSize, std::memory_order_relaxed);
    AllocatorFuncRegister regFunc;
    BuildTransportRegFunc(regFunc);

    auto *allocator = Allocator::Instance();
    auto rc = allocator->InitWithFlexibleRegister(AllocateType::UB_TRANSPORT, ubTransportMemSize_, regFunc);
    if (rc.IsOk()) {
        // Allocate phyica memory buffer pool for client
        std::shared_ptr<ArenaGroup> arenaGroup;
        rc =
            allocator->CreateArenaGroup(DEFAULT_TENANT_ID, ubTransportMemSize_, arenaGroup, AllocateType::UB_TRANSPORT);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Failed to get arena group for client");
        LOG(INFO) << FormatString("UB transport memory pool initialized, poolSize=%lu, arenaNum=%u, arenaSize=%lu",
                                  poolSize, arenaNum, poolSize / arenaNum);
    }
    if (rc.IsError()) {
        rc = rc.GetCode() == K_DUPLICATED ? Status::OK() : rc;
        LOG(WARNING) << "Failed to register memory buffer pool for client, error: " << rc.ToString();
    }

    return rc;
}

Status UrmaManager::NormalizeClientTransportPoolSize(uint64_t requestedSize, uint32_t arenaNum, uint64_t pageSize,
    uint64_t &effectiveSize)
{
    CHECK_FAIL_RETURN_STATUS(requestedSize > 0 && requestedSize <= MAX_TRANSPORT_MEM_SIZE, K_INVALID,
                             FormatString("ubTransportMemSize %lu is invalid, must be between 0 and %lu", requestedSize,
                                          MAX_TRANSPORT_MEM_SIZE));
    CHECK_FAIL_RETURN_STATUS(arenaNum > 0, K_INVALID, "ub_transport_arena_num must be greater than 0");
    CHECK_FAIL_RETURN_STATUS(pageSize > 0 && pageSize <= MAX_TRANSPORT_MEM_SIZE / arenaNum, K_INVALID,
                             "System page size is invalid for the configured transport arena count");

    const uint64_t alignment = pageSize * arenaNum;
    const uint64_t remainder = requestedSize % alignment;
    const uint64_t increment = remainder == 0 ? 0 : alignment - remainder;
    CHECK_FAIL_RETURN_STATUS(
        requestedSize <= MAX_TRANSPORT_MEM_SIZE - increment, K_INVALID,
        FormatString("Aligned UB transport memory size exceeds limit %lu", MAX_TRANSPORT_MEM_SIZE));
    effectiveSize = requestedSize + increment;
    return Status::OK();
}

void UrmaManager::BuildTransportRegFunc(AllocatorFuncRegister &regFunc)
{
    const uint64_t poolSize = ubTransportMemSize_.load();
    regFunc.createFunc = [this, poolSize](void **ptr, size_t arenaSize) -> Status {
        CHECK_FAIL_RETURN_STATUS(arenaSize > 0 && poolSize % arenaSize == 0, K_RUNTIME_ERROR,
                                 "Invalid UB transport arena size");
        CHECK_FAIL_RETURN_STATUS(clientTransportSegmentIndex_ < poolSize / arenaSize, K_RUNTIME_ERROR,
                                 "UB transport arena count exceeds the mapped pool");
        if (memoryBuffer_ == nullptr) {
            memoryBuffer_ = mmap(nullptr, poolSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (memoryBuffer_ == MAP_FAILED) {
                memoryBuffer_ = nullptr;
                RETURN_STATUS(K_OUT_OF_MEMORY, "Failed to allocate memory buffer pool for client");
            }
            Status bindRc = BindClientTransportMemory(memoryBuffer_, poolSize);
            if (bindRc.IsError()) {
                (void)munmap(memoryBuffer_, poolSize);
                memoryBuffer_ = nullptr;
                return bindRc;
            }
            Status rc = RegisterSegment(reinterpret_cast<uint64_t>(memoryBuffer_), poolSize);
            if (rc.IsError()) {
                (void)munmap(memoryBuffer_, poolSize);
                memoryBuffer_ = nullptr;
                return rc;
            }
        }
        *ptr = reinterpret_cast<uint8_t *>(memoryBuffer_)
            + static_cast<uint64_t>(clientTransportSegmentIndex_) * arenaSize;
        ++clientTransportSegmentIndex_;
        return Status::OK();
    };
    regFunc.destroyFunc = [](void *, size_t) { return Status::OK(); };
}

void UrmaManager::SetClientTransportArenaConfig()
{
    const char *value = std::getenv("DATASYSTEM_UB_TRANSPORT_ARENA_NUM");
    if (value == nullptr || strlen(value) == 0) {
        return;
    }
    std::string errorMsg;
    if (SetCommandLineOption("ub_transport_arena_num", value, errorMsg)) {
        LOG(INFO) << "ub_transport_arena_num overridden by DATASYSTEM_UB_TRANSPORT_ARENA_NUM=" << value;
    } else {
        LOG(ERROR) << "Invalid DATASYSTEM_UB_TRANSPORT_ARENA_NUM: " << errorMsg;
    }
}

Status UrmaManager::BindClientTransportMemory(void *pointer, size_t size)
{
    const uint32_t arenaNum = FLAGS_ub_transport_arena_num;
    if (!FLAGS_enable_ub_numa_affinity || !FLAGS_urma_register_whole_arena || arenaNum <= 1) {
        return Status::OK();
    }

    std::vector<int> nodeIds;
    RETURN_IF_NOT_OK(GetNumaNodeIds(nodeIds));
    CHECK_FAIL_RETURN_STATUS(!nodeIds.empty(), K_RUNTIME_ERROR,
                             "No NUMA nodes available for client transport memory binding");

    Timer timer;
    std::vector<NumaBindingRange> ranges;
    RETURN_IF_NOT_OK(BuildRoundRobinNumaBindingPlan(static_cast<uint8_t *>(pointer), size, arenaNum, nodeIds, ranges));
    for (const auto &range : ranges) {
        RETURN_IF_NOT_OK(BindMemoryToNumaNode(range.pointer, range.size, range.nodeId));
    }
    LOG(INFO) << "Binding " << arenaNum << " client transport arenas to NUMA nodes took "
              << timer.ElapsedMilliSecond() << "ms";
    return Status::OK();
}

Status UrmaManager::EnsureClientPipelineH2DEnv()
{
#ifdef BUILD_PIPLN_H2D
    RETURN_RUNTIME_ERROR_IF_NULL(urmaResource_);
    RETURN_IF_NOT_OK(urmaResource_->InitPipelineH2DEnv());
#endif
    return Status::OK();
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

Status UrmaManager::UrmaGetEffectiveDevices(const std::string &configuredName, std::vector<std::string> &candidates)
{
    LOG(INFO) << FormatString("Start UrmaGetEffectiveDevices() with %s", configuredName);
    int devNums = 0;
    urma_device_t **list = nullptr;
    list = ds_urma_get_device_list(&devNums);
    if (list == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                FormatString("Got empty[%d] ub device list with errno = %d", devNums, errno));
    }
    // The configured name (env or default) is tried first to honor operator intent and preserve the previous
    // default behavior when its device is available.
    int index = CompareDeviceName(configuredName, list, devNums);
    if (index >= 0) {
        candidates.emplace_back(reinterpret_cast<const char *>(list[index]->name));
    }
    // Then collect every bonding device as a fallback for the bare-metal case where the first device is occupied
    // by a container (its EID 0 is unavailable, so UrmaResource::Init fails and the next candidate is tried).
    const std::string prefixName = "bonding";
    for (int i = 0; i < devNums; i++) {
        if (list[i] == nullptr) {
            LOG(ERROR) << FormatString("Got empty device index %d from devList.", i);
            continue;
        }
        if (strncmp(reinterpret_cast<const char *>(list[i]->name), prefixName.c_str(), prefixName.length()) == 0) {
            std::string name = reinterpret_cast<const char *>(list[i]->name);
            if (std::find(candidates.begin(), candidates.end(), name) == candidates.end()) {
                candidates.emplace_back(std::move(name));
            }
        }
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!candidates.empty(), K_RUNTIME_ERROR, "Cannot get effective bonding device");
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

Status UrmaManager::AcquireRecvTarget(uint64_t segAddress, uint64_t segSize, const std::string &address,
                                      UrmaRecvTargetLease &lease)
{
    lease.Reset();
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, lease.segmentAccessor_));

    HostPort remoteSenderAddr;
    remoteSenderAddr.ParseString(address);
    std::string remoteConnectionId = remoteSenderAddr.ToString();

    TbbUrmaConnectionMap::const_accessor connectionAccessor;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        urmaConnectionMap_.find(connectionAccessor, remoteConnectionId) && connectionAccessor->second != nullptr,
        K_URMA_NEED_CONNECT,
        FormatString("[AcquireRecvTarget] No exchanged URMA connection for %s; cannot use exchanged recv Jetty",
                     remoteConnectionId));

    std::shared_ptr<UrmaJetty> recvJetty;
    RETURN_IF_NOT_OK_APPEND_MSG(
        urmaResource_->GetOrCreateSharedRecvJetty(recvJetty),
        FormatString("[AcquireRecvTarget] Failed to get shared recv Jetty for %s", remoteConnectionId));

    lease.postPermit_ = recvJetty->TryAcquirePostPermit();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        lease.postPermit_, K_URMA_TRY_AGAIN,
        FormatString("[AcquireRecvTarget] Shared recv Jetty is closing for %s", remoteConnectionId));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        lease.TargetJetty() != nullptr, K_RUNTIME_ERROR,
        FormatString("[AcquireRecvTarget] Shared recv Jetty raw handle is null for %s", remoteConnectionId));
    lease.targetJfr_ = recvJetty->SharedJfrRaw();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        lease.TargetJfr() != nullptr, K_RUNTIME_ERROR,
        FormatString("[AcquireRecvTarget] Shared recv Jetty JFR is null for %s", remoteConnectionId));
    return Status::OK();
}

Status UrmaManager::RegisterSegment(const uint64_t &segAddress, const uint64_t &segSize)
{
    UrmaLocalSegmentMap::const_accessor constAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, constAccessor));
    return Status::OK();
}

Status UrmaManager::GetRecoveryProbeSegmentInfo(uint64_t &segmentAddress, uint64_t &dataOffset)
{
    RETURN_IF_NOT_OK(GetOrCreateRecoveryProbeBuffer(recoveryProbeBuffer_, recoveryProbeMutex_, "destination"));
    segmentAddress = reinterpret_cast<uint64_t>(recoveryProbeBuffer_);
    dataOffset = 0;
    return Status::OK();
}

Status UrmaManager::GetRecoveryProbeSourceInfo(uint64_t &segmentAddress, uint64_t &segmentSize, uint64_t &dataAddress)
{
    RETURN_IF_NOT_OK(GetOrCreateRecoveryProbeBuffer(recoveryProbeSourceBuffer_, recoveryProbeSourceMutex_, "source"));
    segmentAddress = reinterpret_cast<uint64_t>(recoveryProbeSourceBuffer_);
    segmentSize = URMA_RECOVERY_PROBE_SEGMENT_SIZE;
    dataAddress = segmentAddress;
    return Status::OK();
}

Status UrmaManager::GetOrCreateRecoveryProbeBuffer(void *&probeBuffer, std::mutex &mutex, const std::string &purpose)
{
    std::lock_guard<std::mutex> lock(mutex);
    RETURN_OK_IF_TRUE(probeBuffer != nullptr);
    void *buffer =
        mmap(nullptr, URMA_RECOVERY_PROBE_SEGMENT_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    CHECK_FAIL_RETURN_STATUS(buffer != MAP_FAILED, K_OUT_OF_MEMORY,
                             FormatString("Failed to allocate Worker URMA recovery probe %s segment", purpose));
    Status rc = RegisterSegment(reinterpret_cast<uint64_t>(buffer), URMA_RECOVERY_PROBE_SEGMENT_SIZE);
    if (rc.IsError()) {
        LOG_IF(ERROR, munmap(buffer, URMA_RECOVERY_PROBE_SEGMENT_SIZE) != 0)
            << "Failed to unmap Worker URMA recovery probe " << purpose << " segment: " << StrErr(errno);
        return rc;
    }
    probeBuffer = buffer;
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

Status UrmaManager::GetSegmentInfo(uint64_t segmentAddress, UrmaImportSegmentPb &segmentInfo)
{
    PerfPoint point(PerfKey::URMA_GET_LOCAL_SEGMENT_INFO);
    std::shared_lock<std::shared_timed_mutex> lock(localMapMutex_);
    UrmaLocalSegmentMap::const_accessor accessor;
    CHECK_FAIL_RETURN_STATUS(localSegmentMap_->find(accessor, segmentAddress), K_NOT_FOUND,
                             "Local recovery probe segment is not registered");
    CHECK_FAIL_RETURN_STATUS(accessor->second != nullptr, K_RUNTIME_ERROR, "Local segment is null");
    auto *rawSegment = accessor->second->Raw();
    UrmaSeg::ToProto(rawSegment->seg, *segmentInfo.mutable_seg());

    urma_seg_t *segmentContext = nullptr;
    uint32_t segmentContextSize = 0;
    urma_status_t urmaStatus = ds_urma_get_seg_ctx(rawSegment, &segmentContext, &segmentContextSize);
    if (urmaStatus == URMA_SUCCESS && segmentContext != nullptr && segmentContextSize > 0) {
        segmentInfo.mutable_seg_ctx()->set_seg_blob(reinterpret_cast<const char *>(segmentContext),
                                                    segmentContextSize);
        ds_urma_put_seg_ctx(segmentContext);
        LOG(INFO) << "[URMA_CONNECT] Got delegated recovery segment context, va=" << rawSegment->seg.ubva.va
                  << ", length=" << segmentContextSize;
    } else {
        LOG(WARNING) << "[URMA_CONNECT] Failed to get delegated recovery segment context, va="
                     << rawSegment->seg.ubva.va << ", status=" << urmaStatus;
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
    const auto nextPruneMs = nextRetainedTimeoutPruneMs_.load(std::memory_order_acquire);
    if (nextPruneMs != 0) {
        const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
        if (nowMs >= nextPruneMs) {
            PruneRetainedTimeoutEvents(nowMs);
        }
    }
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
            int cqeStatus = 0;
            if (failedIt != failedRequests_.end()) {
                cqeStatus = failedIt->second;
                event->SetFailed(cqeStatus);
                failedRequests_.erase(failedIt);
            }
            // Business completion is independent from lane/WR accounting, which was already
            // settled by local_id while classifying this CQE.
            const auto disposition = event->NotifyAllAndGetDisposition();
            const bool retainedTimeout = disposition == UrmaEvent::CompletionDisposition::RETAINED_TIMEOUT;
            const bool retained = retainedTimeout && ConsumeRetainedTimeoutEvent(requestId);
            if (retainedTimeout || disposition == UrmaEvent::CompletionDisposition::DISCARDED_TIMEOUT) {
                DeleteEvent(requestId);
            }
            if (retained) {
                DispatchLateCompletion(event, cqeStatus);
            }
            VLOG(1) << "[UrmaEventHandler] [urma_request_id:" << requestId << "] Notifying the request";
            // remove request id from finishedRequests_ set
            // we dont need lock for finishedRequests_ as its accessed only by single thread
            it = finishedRequests_.erase(it);
        } else {
            LOG(INFO) << "[UrmaEventHandler] [urma_request_id:" << requestId << "] Event is missing, dropping request";
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
    INJECT_POINT_NO_RETURN("UrmaManager.DeleteEvent");
}

void UrmaManager::RegisterRetainedTimeoutEvent(uint64_t requestId)
{
    const uint64_t nowMs = GetSteadyClockTimeStampMs();
    PruneRetainedTimeoutEvents(nowMs);
    std::vector<uint64_t> evicted;
    {
        std::lock_guard<std::mutex> lock(retainedTimeoutMutex_);
        while (retainedTimeoutOrder_.size() >= MAX_RETAINED_TIMEOUT_EVENTS) {
            const auto oldest = retainedTimeoutOrder_.front();
            retainedTimeoutOrder_.pop_front();
            if (retainedTimeoutDeadlines_.erase(oldest) != 0) {
                evicted.emplace_back(oldest);
            }
        }
        const uint64_t expiresAtMs = nowMs + RETAINED_TIMEOUT_EVENT_TTL_MS;
        auto [iter, inserted] = retainedTimeoutDeadlines_.insert_or_assign(requestId, expiresAtMs);
        (void)iter;
        if (inserted) {
            retainedTimeoutOrder_.push_back(requestId);
        }
        const auto nextPruneMs = nextRetainedTimeoutPruneMs_.load(std::memory_order_relaxed);
        if (nextPruneMs == 0 || expiresAtMs < nextPruneMs) {
            nextRetainedTimeoutPruneMs_.store(expiresAtMs, std::memory_order_release);
        }
    }
    for (const auto eventId : evicted) {
        DeleteEvent(eventId);
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RETAINED_EVENT_EVICTED] Evict oldest timed-out URMA Event, requestId=" << eventId
            << ", capacity=" << MAX_RETAINED_TIMEOUT_EVENTS;
    }
}

bool UrmaManager::ConsumeRetainedTimeoutEvent(uint64_t requestId)
{
    std::lock_guard<std::mutex> lock(retainedTimeoutMutex_);
    return retainedTimeoutDeadlines_.erase(requestId) != 0;
}

void UrmaManager::PruneRetainedTimeoutEvents(uint64_t nowMs)
{
    const auto nextPruneMs = nextRetainedTimeoutPruneMs_.load(std::memory_order_acquire);
    if (nextPruneMs == 0 || nowMs < nextPruneMs) {
        return;
    }
    std::vector<uint64_t> expired;
    {
        std::lock_guard<std::mutex> lock(retainedTimeoutMutex_);
        const auto lockedNextPruneMs = nextRetainedTimeoutPruneMs_.load(std::memory_order_relaxed);
        if (lockedNextPruneMs == 0 || nowMs < lockedNextPruneMs) {
            return;
        }
        while (!retainedTimeoutOrder_.empty()) {
            const auto requestId = retainedTimeoutOrder_.front();
            auto iter = retainedTimeoutDeadlines_.find(requestId);
            if (iter != retainedTimeoutDeadlines_.end() && iter->second > nowMs) {
                nextRetainedTimeoutPruneMs_.store(iter->second, std::memory_order_release);
                break;
            }
            retainedTimeoutOrder_.pop_front();
            if (iter != retainedTimeoutDeadlines_.end()) {
                expired.emplace_back(requestId);
                retainedTimeoutDeadlines_.erase(iter);
            }
        }
        if (retainedTimeoutOrder_.empty()) {
            nextRetainedTimeoutPruneMs_.store(0, std::memory_order_release);
        }
    }
    for (const auto requestId : expired) {
        DeleteEvent(requestId);
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RETAINED_EVENT_EXPIRED] Delete timed-out URMA Event without completion, requestId="
            << requestId << ", ttlMs=" << RETAINED_TIMEOUT_EVENT_TTL_MS;
    }
}

void UrmaManager::ClearRetainedTimeoutEvents()
{
    std::vector<uint64_t> retained;
    {
        std::lock_guard<std::mutex> lock(retainedTimeoutMutex_);
        retained.reserve(retainedTimeoutDeadlines_.size());
        for (const auto &[requestId, deadline] : retainedTimeoutDeadlines_) {
            (void)deadline;
            retained.emplace_back(requestId);
        }
        retainedTimeoutDeadlines_.clear();
        retainedTimeoutOrder_.clear();
        nextRetainedTimeoutPruneMs_.store(0, std::memory_order_release);
    }
    for (const auto requestId : retained) {
        DeleteEvent(requestId);
    }
}

void UrmaManager::DispatchLateCompletion(const std::shared_ptr<UrmaEvent> &event, int cqeStatus)
{
    const bool isolationCompletion = cqeStatus == URMA_PORT_UNAVAILABLE_STATUS
                                     || cqeStatus == URMA_REMOTE_ACK_TIMEOUT_STATUS;
    if (event == nullptr || !isolationCompletion
        || !event->GetLateCompletionContext().has_value()) {
        return;
    }
    const auto &context = *event->GetLateCompletionContext();
    auto observer = context.observer.lock();
    if (observer == nullptr) {
        return;
    }
    UrmaLateCompletion completion{ event->GetRequestId(), cqeStatus, event->GetRemoteAddress(),
                                   event->GetRemoteInstanceId() };
    observer->OnLateUrmaCompletion(completion, context.ownerToken, context.peerToken);
}

Status UrmaManager::SealSendLaneLease(const std::shared_ptr<UrmaSendLaneLease> &laneLease)
{
    if (laneLease == nullptr) {
        return Status::OK();
    }
    return urmaResource_->SealActiveSendLane(laneLease);
}

Status UrmaManager::GetEvent(uint64_t requestId, std::shared_ptr<UrmaEvent> &event)
{
    TbbEventMap::accessor mapAccessor;
    if (tbbEventMap_.find(mapAccessor, requestId)) {
        event = mapAccessor->second;
        return Status::OK();
    }
    // Can happen if event is not yet inserted by sender thread.
    RETURN_STATUS(K_NOT_FOUND, FormatString("[urma_request_id:%zu] doesnt exist in event map", requestId));
}

Status UrmaManager::CreateEvent(uint64_t requestId, const std::shared_ptr<UrmaConnection> &connection,
                                const std::shared_ptr<UrmaSendLaneLease> &laneLease, const std::string &remoteAddress,
                                uint64_t dataSize, UrmaEvent::OperationType operationType,
                                std::atomic<int> *srcChipInflightCounter,
                                std::shared_ptr<EventWaiter> waiter, std::shared_ptr<UrmaEvent> *event,
                                std::optional<UrmaLateCompletionContext> lateCompletionContext,
                                bool observeGatherInflightDrain)
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
        RETURN_STATUS_LOG_ERROR(K_DUPLICATED,
                                FormatString("[urma_request_id:%zu] already exists in event map", requestId));
    } else {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            connection != nullptr, K_RUNTIME_ERROR,
            FormatString("[urma_request_id:%zu] Urma connection is null, remoteAddress=%s, op=%s", requestId,
                         remoteAddress, UrmaEvent::OperationTypeName(operationType)));
        mapAccessor->second = std::make_shared<UrmaEvent>(requestId, laneLease, remoteAddress,
                                                          connection->GetUrmaJfrInfo().uniqueInstanceId, dataSize,
                                                          operationType, srcChipInflightCounter, std::move(waiter),
                                                          std::move(lateCompletionContext),
                                                          observeGatherInflightDrain);
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
        const auto count = srcChipInflightWrCounts_[i].value.load(std::memory_order_relaxed);
        if (count == 0) {
            continue;
        }
        const auto written =
            std::snprintf(buffer + length, sizeof(buffer) - length, "%s%zu:%d", first ? "" : ",", i, count);
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
    return &srcChipInflightWrCounts_[chipId].value;
}

void UrmaManager::RecordNumaWriteChipCounts(uint8_t srcChipId, uint8_t dstChipId)
{
    if (srcChipId < srcChipWriteCounts_.size() && srcChipId != INVALID_CHIP_ID) {
        srcChipWriteCounts_[srcChipId].fetch_add(1, std::memory_order_relaxed);
    }
    if (dstChipId < dstChipWriteCounts_.size() && dstChipId != INVALID_CHIP_ID) {
        dstChipWriteCounts_[dstChipId].fetch_add(1, std::memory_order_relaxed);
    }
}

void UrmaManager::RecordNumaWriteCrossChipCount(uint8_t srcChipId, uint8_t dstChipId)
{
    if (srcChipId == URMA_AFFINITY_SRC_CHIP_MIN && dstChipId == URMA_AFFINITY_SRC_CHIP_MAX) {
        src1Dst2WriteCount_.fetch_add(1, std::memory_order_relaxed);
    } else if (srcChipId == URMA_AFFINITY_SRC_CHIP_MAX && dstChipId == URMA_AFFINITY_SRC_CHIP_MIN) {
        src2Dst1WriteCount_.fetch_add(1, std::memory_order_relaxed);
    }
}

const char *UrmaManager::GetNumaWriteChipCountsString() const
{
    static thread_local char buffer[URMA_CHIP_INFLIGHT_LOG_BUFFER_SIZE];
    const auto src1 = srcChipWriteCounts_[URMA_AFFINITY_SRC_CHIP_MIN].load(std::memory_order_relaxed);
    const auto src2 = srcChipWriteCounts_[URMA_AFFINITY_SRC_CHIP_MAX].load(std::memory_order_relaxed);
    const auto dst1 = dstChipWriteCounts_[URMA_AFFINITY_SRC_CHIP_MIN].load(std::memory_order_relaxed);
    const auto dst2 = dstChipWriteCounts_[URMA_AFFINITY_SRC_CHIP_MAX].load(std::memory_order_relaxed);
    const auto src1Dst2 = src1Dst2WriteCount_.load(std::memory_order_relaxed);
    const auto src2Dst1 = src2Dst1WriteCount_.load(std::memory_order_relaxed);
    static constexpr char initialFormat[] =
        "{src1:%lu,src2:%lu,dst1:%lu,dst2:%lu,src1_dst2:%lu,"
        "src2_dst1:%lu";
    size_t length = static_cast<size_t>(
        std::snprintf(buffer, sizeof(buffer), initialFormat, src1, src2, dst1, dst2, src1Dst2, src2Dst1));
    for (size_t chipId = URMA_AFFINITY_SRC_CHIP_MAX + 1;
         chipId < srcChipWriteCounts_.size() && length < sizeof(buffer); ++chipId) {
        const auto srcCount = srcChipWriteCounts_[chipId].load(std::memory_order_relaxed);
        const auto dstCount = dstChipWriteCounts_[chipId].load(std::memory_order_relaxed);
        if (srcCount == 0 && dstCount == 0) {
            continue;
        }
        const auto written = std::snprintf(buffer + length, sizeof(buffer) - length, ",src%zu:%lu,dst%zu:%lu",
                                           chipId, srcCount, chipId, dstCount);
        if (written < 0 || static_cast<size_t>(written) >= sizeof(buffer) - length) {
            break;
        }
        length += static_cast<size_t>(written);
    }
    if (length < sizeof(buffer)) {
        std::snprintf(buffer + length, sizeof(buffer) - length, "}");
    }
    return buffer;
}

UrmaManager::SrcChipSelectionDecision UrmaManager::BuildSrcChipSelectionDecision(uint8_t transmittedChipId,
                                                                                 uint64_t logicalWriteWrCount)
{
    const uint64_t sequence = affinitySrcChipIdSequence_.fetch_add(1, std::memory_order_relaxed);
    const uint8_t candidate =
        static_cast<uint8_t>(URMA_AFFINITY_SRC_CHIP_MIN + sequence % URMA_AFFINITY_SRC_CHIP_COUNT);
    return BuildSrcChipSelectionDecisionWithCandidate(transmittedChipId, logicalWriteWrCount, candidate);
}

UrmaManager::SrcChipSelectionDecision UrmaManager::BuildSrcChipSelectionDecisionWithCandidate(
    uint8_t transmittedChipId, uint64_t logicalWriteWrCount, uint8_t candidateChipId)
{
    const uint32_t srcChipPolicy = FLAGS_ub_numa_src_chip_policy;
    if (srcChipPolicy == static_cast<uint32_t>(UbNumaSrcChipPolicy::ROUND_ROBIN)) {
#ifdef WITH_TESTS
        INJECT_POINT_NO_RETURN("UrmaManager.SrcChipPolicy.RoundRobin");
#endif
    } else {
#ifdef WITH_TESTS
        INJECT_POINT_NO_RETURN("UrmaManager.SrcChipPolicy.RoundRobinWithAffinity");
#endif
    }
    const uint32_t threshold = FLAGS_ub_numa_inflight_wr_diff_threshold;
    SrcChipSelectionDecision decision{ candidateChipId, candidateChipId, srcChipPolicy, threshold,
                                       std::max<uint64_t>(logicalWriteWrCount, 1) };
    if (threshold == 0) {
        return decision;
    }
    decision.chip1Inflight =
        std::max(srcChipInflightWrCounts_[URMA_AFFINITY_SRC_CHIP_MIN].value.load(std::memory_order_relaxed), 0);
    decision.chip2Inflight =
        std::max(srcChipInflightWrCounts_[URMA_AFFINITY_SRC_CHIP_MAX].value.load(std::memory_order_relaxed), 0);
#ifdef WITH_TESTS
    INJECT_POINT_NO_RETURN("UrmaManager.OverrideSrcChipInflightSnapshot", [&decision](int chip1, int chip2) {
        decision.chip1Inflight = std::max(chip1, 0);
        decision.chip2Inflight = std::max(chip2, 0);
    });
    auto overrideSrcChipPolicyDecision = [&decision](int chipId, int chip1, int chip2, int) {
        if (chipId >= URMA_AFFINITY_SRC_CHIP_MIN && chipId <= URMA_AFFINITY_SRC_CHIP_MAX) {
            decision.candidate = static_cast<uint8_t>(chipId);
        }
        decision.chip1Inflight = std::max(chip1, 0);
        decision.chip2Inflight = std::max(chip2, 0);
    };
    INJECT_POINT_NO_RETURN("UrmaManager.OverrideSrcChipPolicyDecision", std::move(overrideSrcChipPolicyDecision));
#endif
    decision.selected = decision.candidate;
    ApplySrcChipDepthFeedback(transmittedChipId, decision);
    return decision;
}

void UrmaManager::ApplySrcChipDepthFeedback(uint8_t transmittedChipId, SrcChipSelectionDecision &decision)
{
    decision.difference = decision.chip1Inflight >= decision.chip2Inflight
                              ? static_cast<uint64_t>(decision.chip1Inflight - decision.chip2Inflight)
                              : static_cast<uint64_t>(decision.chip2Inflight - decision.chip1Inflight);
    if (decision.difference > decision.threshold) {
        decision.selected =
            decision.chip1Inflight < decision.chip2Inflight ? URMA_AFFINITY_SRC_CHIP_MIN : URMA_AFFINITY_SRC_CHIP_MAX;
        decision.depthOverride = decision.selected != decision.candidate;
        return;
    }
    if (decision.policy != static_cast<uint32_t>(UbNumaSrcChipPolicy::ROUND_ROBIN_WITH_AFFINITY)
        || transmittedChipId < URMA_AFFINITY_SRC_CHIP_MIN || transmittedChipId > URMA_AFFINITY_SRC_CHIP_MAX
        || transmittedChipId == decision.candidate) {
        return;
    }
    const uint64_t affinityInflight = transmittedChipId == URMA_AFFINITY_SRC_CHIP_MIN
                                          ? static_cast<uint64_t>(decision.chip1Inflight)
                                          : static_cast<uint64_t>(decision.chip2Inflight);
    const uint64_t candidateInflight = decision.candidate == URMA_AFFINITY_SRC_CHIP_MIN
                                           ? static_cast<uint64_t>(decision.chip1Inflight)
                                           : static_cast<uint64_t>(decision.chip2Inflight);
    if (affinityInflight <= candidateInflight && decision.estimatedWrCount <= candidateInflight - affinityInflight) {
        decision.selected = transmittedChipId;
        decision.affinityOverride = true;
    }
}

bool UrmaManager::ShouldLogSrcChipSelection(const SrcChipSelectionDecision &decision)
{
    return decision.depthOverride;
}

void UrmaManager::ObserveSrcChipSelection(const SrcChipSelectionDecision &decision)
{
    if (ShouldLogSrcChipSelection(decision)) {
        LOG_FIRST_AND_EVERY_N(INFO, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_SRC_CHIP_BALANCE] override policy candidate " << static_cast<uint32_t>(decision.candidate)
            << " with chip " << static_cast<uint32_t>(decision.selected) << ", policy=" << decision.policy
            << ", chip1Inflight=" << decision.chip1Inflight << ", chip2Inflight=" << decision.chip2Inflight
            << ", difference=" << decision.difference << ", threshold=" << decision.threshold;
#ifdef WITH_TESTS
        INJECT_POINT_NO_RETURN("UrmaManager.SrcChipInflightBalanceOverride");
#endif
        return;
    }
#ifdef WITH_TESTS
    if (decision.affinityOverride) {
        INJECT_POINT_NO_RETURN("UrmaManager.SrcChipAffinityOverride");
    }
#endif
}

uint8_t UrmaManager::FinalizeSrcChipSelection(const SrcChipSelectionDecision &decision)
{
    ObserveSrcChipSelection(decision);
#ifdef WITH_TESTS
    if (decision.selected == URMA_AFFINITY_SRC_CHIP_MIN) {
        INJECT_POINT_NO_RETURN("UrmaManager.SrcChipSelected.1");
    } else {
        INJECT_POINT_NO_RETURN("UrmaManager.SrcChipSelected.2");
    }
#endif
    return decision.selected;
}

uint8_t UrmaManager::GetAffinitySrcChipId(uint8_t transmittedChipId, bool useNumaAffinity, uint64_t logicalWriteWrCount,
                                          uint8_t *candidateChipId)
{
    if (!useNumaAffinity || FLAGS_ub_numa_rr_type == static_cast<uint32_t>(UbNumaRrType::DISABLED)) {
        return transmittedChipId;
    }
    const auto decision = BuildSrcChipSelectionDecision(transmittedChipId, logicalWriteWrCount);
    if (candidateChipId != nullptr) {
        *candidateChipId = decision.candidate;
    }
    return FinalizeSrcChipSelection(decision);
}

uint8_t UrmaManager::GetAffinitySrcChipIdForPost(uint8_t transmittedChipId, bool useNumaAffinity, bool firstPost,
                                                 uint8_t logicalWriteChipId, uint64_t logicalWriteWrCount,
                                                 uint8_t *candidateChipId)
{
    if (!useNumaAffinity || FLAGS_ub_numa_rr_type == static_cast<uint32_t>(UbNumaRrType::DISABLED)) {
        if (candidateChipId != nullptr) {
            *candidateChipId = transmittedChipId;
        }
        return transmittedChipId;
    }
    if (FLAGS_ub_numa_rr_type == static_cast<uint32_t>(UbNumaRrType::PER_POST) || firstPost) {
        const uint64_t selectionWrCount =
            FLAGS_ub_numa_rr_type == static_cast<uint32_t>(UbNumaRrType::PER_POST) ? 1 : logicalWriteWrCount;
        return GetAffinitySrcChipId(transmittedChipId, true, selectionWrCount, candidateChipId);
    }
    return logicalWriteChipId;
}

uint8_t UrmaManager::GetAffinitySrcChipIdWithCandidate(uint8_t transmittedChipId, bool useNumaAffinity,
                                                       uint64_t logicalWriteWrCount, uint8_t candidateChipId)
{
    if (!useNumaAffinity || FLAGS_ub_numa_rr_type == static_cast<uint32_t>(UbNumaRrType::DISABLED)) {
        return transmittedChipId;
    }
    const auto decision =
        BuildSrcChipSelectionDecisionWithCandidate(transmittedChipId, logicalWriteWrCount, candidateChipId);
    return FinalizeSrcChipSelection(decision);
}

UrmaManager::UrmaNumaPostConfig UrmaManager::ResolveNumaPostConfig(uint8_t transmittedSrcChipId, uint8_t dstChipId,
                                                                   bool firstPost, uint8_t logicalWriteChipId,
                                                                   uint64_t logicalWriteWrCount,
                                                                   uint8_t candidateChipId)
{
    bool enabled = IsUbNumaAffinityEnabled() && transmittedSrcChipId != INVALID_CHIP_ID
                   && dstChipId != INVALID_CHIP_ID;
#ifdef WITH_TESTS
    // URMA Mock remaps anonymous registered memory onto memfd-backed VMAs for cross-process access. That remap
    // intentionally cannot preserve physical NUMA placement, so end-to-end tests inject a valid chip while allocator
    // and binding-plan tests independently cover placement.
    INJECT_POINT_NO_RETURN("UrmaManager.ForceNumaAffinityForMock",
                           [&enabled, &transmittedSrcChipId](bool forceEnabled) {
        if (forceEnabled) {
            enabled = true;
            if (transmittedSrcChipId < URMA_AFFINITY_SRC_CHIP_MIN
                || transmittedSrcChipId > URMA_AFFINITY_SRC_CHIP_MAX) {
                transmittedSrcChipId = URMA_AFFINITY_SRC_CHIP_MIN;
            }
        }
    });
#endif
    uint8_t resolvedCandidateChipId = candidateChipId;
    const bool reuseLogicalWriteCandidate =
        candidateChipId >= URMA_AFFINITY_SRC_CHIP_MIN && candidateChipId <= URMA_AFFINITY_SRC_CHIP_MAX
        && FLAGS_ub_numa_rr_type == static_cast<uint32_t>(UbNumaRrType::PER_LOGICAL_WRITE);
    const auto srcChipId =
        reuseLogicalWriteCandidate
            ? GetAffinitySrcChipIdWithCandidate(transmittedSrcChipId, enabled, logicalWriteWrCount, candidateChipId)
            : GetAffinitySrcChipIdForPost(transmittedSrcChipId, enabled, firstPost, logicalWriteChipId,
                                          logicalWriteWrCount, &resolvedCandidateChipId);
    return { enabled, srcChipId, resolvedCandidateChipId, GetSrcChipInflightWrCounter(srcChipId) };
}

void UrmaManager::LogUrmaWaitToFinishElapsed(uint64_t requestId, const std::shared_ptr<UrmaEvent> &event,
                                             uint64_t totalElapsedUs, double totalElapsedMs, double waitElapsedMs,
                                             uint64_t wakeSchedLatencyUs, uint64_t completionObservationLatencyUs,
                                             uint64_t eventProcessingAndWaitLatencyUs, const Status &waitRc) const
{
    auto config = GetServerLatencyTraceConfig();
    const auto trace = event->GetWriteTrace();
    const char *wakeSchedMetricName = "urmaWriteWakeSchedLatencyUs";
    if (trace.writeChunkIndex == URMA_FIRST_WRITE_CHUNK_INDEX) {
        wakeSchedMetricName = "firstUrmaWriteWakeSchedLatencyUs";
    } else if (trace.writeChunkIndex == URMA_SECOND_WRITE_CHUNK_INDEX) {
        wakeSchedMetricName = "secondUrmaWriteWakeSchedLatencyUs";
    }
    SLOW_LOG_IF_OR_VLOG(
        INFO, (config.rpcSlowerThanUs > 0 && totalElapsedUs >= config.rpcSlowerThanUs) || FLAGS_enable_perf_trace_log,
        1,
        "[URMA_ELAPSED_TOTAL]: [urma_request_id:"
            << requestId << "] Time from urma_post_jetty_send_wr to urma_write completion total cost " << totalElapsedMs
            << "ms, wait bthread completion time(bthread::ConditionVariable.wait_for): " << waitElapsedMs
            << "ms, src address:" << localUrmaInfo_.localAddress.ToString()
            << ", target address:" << event->GetRemoteAddress() << ", dataSize:" << event->GetDataSize()
            << ", writeChunkIndex:" << trace.writeChunkIndex << ", writeChunkCount:" << trace.writeChunkCount
            << ", cpuid:" << sched_getcpu() << ", status: " << waitRc.ToString()
            << ", urma_inflight_wr_count: " << tbbEventMap_.size()
            << ", " << wakeSchedMetricName << ":" << wakeSchedLatencyUs
            << ", completionObservationLatencyUs:" << completionObservationLatencyUs
            << ", urmaEventProcessingAndWaitLatencyUs:" << eventProcessingAndWaitLatencyUs
            << ", srcChipInflight:" << GetSrcChipInflightWrCountsString()
            << ", trace_us:{post:" << trace.postUs << ", wait:" << trace.waitUs
            << ", poll_begin:" << trace.pollBeginUs << ", sleep_start:" << trace.sleepStartUs
            << ", sleep_end:" << trace.sleepEndUs
            << ", poll_end:" << trace.pollEndUs << ", notify:" << trace.notifyUs << ", awake:" << trace.awakeUs
            << ", observed:" << trace.observedUs
            << ", waited_for_notification:" << trace.waitedForNotification
            << ", pre_completed_before_wait:" << trace.preCompletedBeforeWait
            << ", woken_by_previous_event:" << trace.wokenByPreviousEvent
            << ", event_processing_and_wait_latency_valid:" << trace.eventProcessingAndWaitLatencyValid
            << ", suggest: " << URMA_ELAPSED_TOTAL_SUGGEST);
}

Status UrmaManager::CreateUrmaWaitTimeoutStatus(uint64_t requestId, const std::shared_ptr<UrmaEvent> &event,
                                                double elapsedMs, const std::string &reason) const
{
    const auto srcAddress = localUrmaInfo_.localAddress.ToString();
    const auto message = FormatString(
        "[URMA_WAIT_TIMEOUT] [urma_request_id:%zu] timedout waiting, elapsedMs=%f, srcAddress=%s, "
        "targetAddress=%s, remoteInstanceId=%s, dataSize=%zu, op=%s, reason=%s",
        requestId, elapsedMs, srcAddress.c_str(), event->GetRemoteAddress().c_str(),
        event->GetRemoteInstanceId().c_str(), static_cast<size_t>(event->GetDataSize()),
        UrmaEvent::OperationTypeName(event->GetOperationType()), reason.c_str());
    LOG(WARNING) << message;
    return Status(K_URMA_WAIT_TIMEOUT, message);
}

Status UrmaManager::WaitForUrmaEvent(uint64_t requestId, int64_t timeoutMs,
                                     const std::shared_ptr<UrmaEvent> &event, UrmaWriteFailure *failure,
                                     UrmaSequentialWaitContext *waitContext)
{
    auto scheduleTimedOutLane = [this, requestId, &event]() {
        auto laneLease = event->GetLaneLease().lock();
        if (laneLease != nullptr) {
            urmaResource_->ScheduleTimedOutSendLane(laneLease, requestId, event->GetRemoteAddress(),
                                                    event->GetRemoteInstanceId());
        }
    };
    if (timeoutMs < 0) {
        event->MarkWaitTimedOut([this, requestId]() { RegisterRetainedTimeoutEvent(requestId); });
        scheduleTimedOutLane();
        return CreateUrmaWaitTimeoutStatus(requestId, event, 0, "request deadline already expired");
    }
    // Test-only: simulate a real UB link-down (URMA completion never arrives).
    // Sleep a fixed 2000ms instead of timeoutMs so teardown cannot race a long request timeout.
    // The delay exceeds the test client's 1s RPC deadline, allowing the circuit breaker to
    // observe the timeout before this injected worker-side timeout is returned.
    INJECT_POINT("UrmaManager.UrmaWaitNoCompletionHang", [this, &event]() {
        constexpr int64_t hangMs = 2000;
        SleepCurrentFor(std::chrono::milliseconds(hangMs));
        auto jetty = event->GetJetty().lock();
        if (jetty != nullptr) {
            LOG_IF_ERROR(urmaResource_->RetireActiveSendLane(jetty->GetJettyId()),
                         "Failed to retire active URMA lane for no-completion-hang inject");
        }
        return Status(K_URMA_WAIT_TIMEOUT,
                      FormatString("[URMA_WAIT_TIMEOUT] inject no-completion hang (link down): %ldms", hangMs));
    });
    PerfPoint waitPoint(PerfKey::URMA_WAIT_TIME);
    Timer waitTimer;
    event->SetWriteWaitTimeUs(waitTimer.GetStartTimeStampUs());
    Status waitRc = event->WaitFor(std::chrono::milliseconds(timeoutMs), waitContext,
                                   [this, requestId]() { RegisterRetainedTimeoutEvent(requestId); });
    waitTimer.Stop();
    const auto endWaitTimeUs = waitTimer.GetEndTimeStampUs();
    constexpr double US_TO_MS = 1000.0;
    auto totalElapsedUs = endWaitTimeUs - event->GetCreateTimeUs();
    auto wakeSchedLatencyUs = event->GetWakeSchedLatencyUs();
    auto completionObservationLatencyUs = event->GetCompletionObservationLatencyUs();
    auto eventProcessingAndWaitLatencyUs = event->GetEventProcessingAndWaitLatencyUs();
    auto urmaElapsedUs = totalElapsedUs >= completionObservationLatencyUs
                             ? totalElapsedUs - completionObservationLatencyUs
                             : 0;
    auto totalElapsedMs = static_cast<double>(urmaElapsedUs) / US_TO_MS;
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::URMA_WAIT_LATENCY)).Observe(totalElapsedUs);
    auto waitElapsedMs = waitTimer.ElapsedMicroSecond() / US_TO_MS;
    GetWorkerTimeCost().Append("Urma wait time.", static_cast<uint64_t>(totalElapsedMs));
    // UrmaEvent::WaitFor returns K_URMA_WAIT_TIMEOUT; keep K_RPC_DEADLINE_EXCEEDED for older Event paths.
    const bool isUrmaWaitTimeout =
        waitRc.GetCode() == StatusCode::K_URMA_WAIT_TIMEOUT || waitRc.GetCode() == StatusCode::K_RPC_DEADLINE_EXCEEDED;
    if (isUrmaWaitTimeout) {
        scheduleTimedOutLane();
        return CreateUrmaWaitTimeoutStatus(requestId, event, totalElapsedMs, waitRc.GetMsg());
    }
    LogUrmaWaitToFinishElapsed(requestId, event, totalElapsedUs, totalElapsedMs, waitElapsedMs, wakeSchedLatencyUs,
                               completionObservationLatencyUs, eventProcessingAndWaitLatencyUs, waitRc);
    if (event->IsFailed() && failure != nullptr) {
        const int cqeStatus = event->GetStatusCode();
        if (!failure->cqeStatus.has_value() || cqeStatus == URMA_PORT_UNAVAILABLE_STATUS) {
            failure->cqeStatus = cqeStatus;
        }
    }
    RETURN_IF_NOT_OK(waitRc);
    waitPoint.Record();
    return Status::OK();
}

Status UrmaManager::WaitToFinish(uint64_t requestId, int64_t timeoutMs, UrmaWriteFailure *failure,
                                 UrmaSequentialWaitContext *waitContext)
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

    bool retainTimedOutEvent = false;
    Raii deleteEvent([this, &requestId, &retainTimedOutEvent]() {
        if (!retainTimedOutEvent) {
            DeleteEvent(requestId);
        }
    });
    Status waitRc = WaitForUrmaEvent(requestId, timeoutMs, event, failure, waitContext);
    retainTimedOutEvent = event->IsTimedOutRetained();
    RETURN_IF_NOT_OK(waitRc);
    RETURN_IF_NOT_OK(HandleUrmaEvent(requestId, event));
    return Status::OK();
}

Status UrmaManager::HandleUrmaEvent(uint64_t requestId, const std::shared_ptr<UrmaEvent> &event)
{
    RETURN_OK_IF_TRUE(!event->IsFailed());

    const auto statusCode = event->GetStatusCode();
    auto errMsg =
        FormatString("[urma_request_id:%zu] Polling failed with an error, cqe status: %d", requestId, statusCode);

    return Status(K_URMA_ERROR, errMsg);
}

Status UrmaManager::AcquireSendLaneFromConnection(const std::shared_ptr<UrmaConnection> &connection,
                                                  std::shared_ptr<UrmaJetty> &jetty, urma_target_jetty_t *&targetJetty)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    auto rc = urmaResource_->AcquireJetty(jetty);
    if (rc.IsError()) {
        if (rc.GetCode() == K_URMA_TRY_AGAIN) {
            INJECT_POINT("UrmaManager.AcquireSendLaneFromConnection.PoolExhausted");
            const auto stats = urmaResource_->GetSendJettyPoolStats();
            const auto &jfrInfo = connection->GetUrmaJfrInfo();
            const auto srcAddress = localUrmaInfo_.localAddress.ToString();
            const auto targetAddress = jfrInfo.localAddress.ToString();
            const Status backpressure(
                K_URMA_TRY_AGAIN,
                FormatString("URMA send Jetty lane pool exhausted, poolSize=%zu, idleCount=%zu, inUseCount=%zu, "
                             "srcAddress=%s, targetAddress=%s, remoteInstanceId=%s, cause=%s",
                             stats.poolSize, stats.idleCount, stats.inUseCount, srcAddress.c_str(),
                             targetAddress.c_str(), jfrInfo.uniqueInstanceId.c_str(), rc.ToString().c_str()));
            LOG_FIRST_AND_EVERY_N(ERROR, K_URMA_POOL_EXHAUSTED_LOG_EVERY_N) << backpressure.ToString();
            return backpressure;
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

    const auto activeRetireRc = urmaResource_->RetireActiveSendLane(jettyId);
    if (activeRetireRc.IsOk()) {
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JETTY] [urma_request_id:" << requestId
            << "] Retired active lane from completion, jettyId=" << jettyId << ", cqeStatus=" << statusCode;
        return Status::OK();
    }
    // The shared JFC can report a Jetty that is not represented by the active send-lane registry,
    // or another fault path may already have removed its lane. Preserve the registry lookup so
    // retirement remains compatible and idempotent for those completions.
    std::shared_ptr<UrmaJetty> failedJetty;
    auto lookupRc = urmaResource_->GetJettyById(jettyId, failedJetty);
    if (lookupRc.IsError() || failedJetty == nullptr) {
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JETTY_SKIP] [urma_request_id:" << requestId << "] Completion Jetty " << jettyId
            << " is not found, cqeStatus=" << statusCode << ", rc=" << lookupRc.ToString();
        return Status::OK();
    }

    LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
        << "[URMA_RECREATE_JETTY] [urma_request_id:" << requestId << "] Trigger from completion, jettyId=" << jettyId
        << ", cqeStatus=" << statusCode;
    return urmaResource_->RetireJetty(failedJetty);
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
    INJECT_POINT(
        "UrmaManager.CheckCompletionRecordStatus.AfterPrimaryInject", [&completeRecords](int index, int status) {
            if (completeRecords[index].status != URMA_CR_WR_FLUSH_ERR_DONE) {
                completeRecords[index].status = static_cast<urma_cr_status_t>(status);
            }
            return Status::OK();
        });
    for (int i = 0; i < count; i++) {
        const auto crStatus = completeRecords[i].status;
        const auto userCtx = completeRecords[i].user_ctx;
        const auto jettyId = completeRecords[i].local_id;
        // FLUSH_ERR_DONE is a Jetty-lifecycle event. Its user_ctx is a fake value, so only
        // local_id may be used to advance the already-published pending-delete record.
        if (crStatus == URMA_CR_WR_FLUSH_ERR_DONE) {
            LOG(INFO) << "[URMA_POLL_JFC] Write flush error done for jetty id: " << jettyId;
            LOG_IF_ERROR(urmaResource_->HandleFlushErrDone(jettyId),
                         FormatString("[URMA_FLUSH_JETTY_FAILED] jettyId=%u", jettyId));
            continue;
        }

        // Settle transport ownership by Jetty identity and request generation before notifying
        // the business request. This prevents an old CQE from consuming a reused lane's WR count.
        if (crStatus == URMA_CR_SUCCESS
            || GetUrmaErrorHandlePolicy(crStatus) != UrmaErrorHandlePolicy::RECREATE_JETTY) {
            LOG_IF_ERROR(urmaResource_->CompleteActiveSendLane(jettyId, userCtx, crStatus),
                         FormatString("[URMA_SEND_LANE_COMPLETE_FAILED] jettyId=%u", jettyId));
        } else {
            uint64_t requestIdFloor = 0;
            if (urmaResource_->IsStaleSendCompletion(jettyId, userCtx, requestIdFloor)) {
                LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
                    << "[URMA_STALE_FATAL_CQE] [urma_request_id:" << userCtx
                    << "] A stale fatal CQE still invalidates the physical Jetty, jettyId=" << jettyId
                    << ", floor_urma_request_id=" << requestIdFloor << ", cqeStatus=" << crStatus;
            }
            LOG_IF_ERROR(TryRecoverFailedJettyFromCompletion(userCtx, crStatus, jettyId),
                         FormatString("[URMA_RECREATE_JETTY_FAILED] [urma_request_id:%zu] jettyId=%u, cqeStatus=%d",
                                      userCtx, jettyId, crStatus));
        }
#ifdef BUILD_PIPLN_H2D
        // redirect pipeline h2d events
        if (OsXprtPipln::PiplnH2DRecvEventHook(&completeRecords[i])) {
            std::shared_ptr<UrmaEvent> event;
            if (GetEvent(completeRecords[i].user_ctx, event).IsOk()) {
                // The send lane was already settled by local_id before the pipeline consumed this event.
                DeleteEvent(completeRecords[i].user_ctx);
            }
            continue;
        }
#endif

        // Classify the business result by request identity for CheckAndNotify().
        if (crStatus == URMA_CR_SUCCESS) {
            VLOG(1) << "[URMA_POLL_JFC] [urma_request_id:" << userCtx << "] Got event";
            successCompletedReqs.insert(userCtx);
        } else {
            LOG(ERROR) << FormatString(
                "[URMA_POLL_JFC]: [urma_request_id:%zu] urma_poll_jfc return failed completion record, CR.status: %d",
                userCtx, crStatus);
            failedCompletedReqs[userCtx] = crStatus;
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
            const struct timespec ts{ 0, 1000 };
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
    // The comm layer has already exchanged the jfr, and we should be able to locate the entry.
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
    return requestId_.fetch_add(1) & URMA_EFFECTIVE_REQUEST_ID_MASK;
}

static urma_status_t PostJettyRw(const std::shared_ptr<UrmaJetty> &jetty, urma_opcode_t opcode,
                                 urma_target_jetty_t *targetJetty, urma_target_seg_t *remoteSeg,
                                 urma_target_seg_t *localSeg, uint64_t remoteAddress, uint64_t localAddress,
                                 uint64_t length, urma_jfs_wr_flag_t flag, uint64_t userCtx, bool useNumaAffinity,
                                 uint32_t src_chip_id, uint32_t dst_chip_id)
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

    bondp_jfs_wr_t bondp_wr{};
    urma_jfs_wr_t *base = &bondp_wr.base;
    base->opcode = opcode;
    base->flag = flag;
    base->flag.bs.has_drv_ext = useNumaAffinity ? 1 : 0;
    base->tjetty = targetJetty;
    base->user_ctx = userCtx;
    base->rw = { .src = src, .dst = dst, .target_hint = 0, .notify_data = 0 };
    base->next = nullptr;
    bondp_wr.src_chip_id = src_chip_id;
    bondp_wr.dst_chip_id = dst_chip_id;

    urma_jfs_wr_t *badWr = nullptr;
    INJECT_POINT_NO_RETURN("UrmaManager.PostJettyRwWithPermit");
    return ds_urma_post_jetty_send_wr(permit.Raw(), base, &badWr);
}

Status UrmaManager::UrmaWriteImpl(const UrmaWriteArgs &args, std::vector<uint64_t> &eventKeys,
                                  const std::shared_ptr<UrmaSendLaneLease> &externalLaneLease,
                                  UrmaWriteFailure *failure)
{
    if (args.size == 0) {
        return Status::OK();
    }
    urma_jfs_wr_flag_t flag{};
    flag.bs.complete_enable = 1;
    uint8_t transmittedSrcChipId = args.srcChipId;
    // Type 0 keeps the transmitted chip, type 1 selects on the first post and reuses it for the logical write,
    // and type 2 selects independently for every post. Keep this decision inside GetAffinitySrcChipIdForPost so
    // tests can guard the loop granularity rather than only the underlying candidate policy.
    uint8_t srcChipId = transmittedSrcChipId;

    uint64_t writtenSize = 0;
    uint64_t remainSize = args.size;
    const uint64_t maxWriteSize = urmaResource_->GetMaxWriteSize();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(maxWriteSize > 0, K_RUNTIME_ERROR, "URMA max write size is zero");
    const uint64_t writeChunkCount = args.size / maxWriteSize + (args.size % maxWriteSize == 0 ? 0 : 1);
    uint64_t writeChunkIndex = 0;
    Timer timer;
    std::shared_ptr<UrmaJetty> jetty;
    urma_target_jetty_t *targetJetty = nullptr;
    const bool ownsLaneLease = externalLaneLease == nullptr;
    auto laneLease = externalLaneLease;
    if (ownsLaneLease) {
        RETURN_IF_NOT_OK(AcquireSendLaneFromConnection(args.connection, jetty, targetJetty));
        laneLease = std::make_shared<UrmaSendLaneLease>(jetty, requestId_.load(std::memory_order_relaxed));
        auto registerRc = urmaResource_->RegisterActiveSendLane(laneLease);
        if (registerRc.IsError()) {
            LOG_IF_ERROR(urmaResource_->RetireJetty(jetty),
                         "Failed to retire URMA send Jetty after lane registration failure");
            return registerRc;
        }
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
    auto cleanupSubmittedEvents = [this, &eventKeys, &sealLaneLease, ownsLaneLease, failure]() {
        if (ownsLaneLease) {
            LOG_IF_ERROR(sealLaneLease(), "Failed to seal URMA write lane lease during cleanup");
        }
        if (eventKeys.empty()) {
            return;
        }
        auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        LOG_IF_ERROR(WaitFastTransportEventWithFailure(eventKeys, remainingTime, errorHandler, failure),
                     "Failed to cleanup submitted URMA write events");
        eventKeys.clear();
    };
    while (remainSize > 0) {
        const auto numaConfig = ResolveNumaPostConfig(transmittedSrcChipId, args.dstChipId, writeChunkIndex == 0,
                                                      srcChipId, writeChunkCount);
        srcChipId = numaConfig.srcChipId;
        const bool useNumaAffinity = numaConfig.enabled;
        auto *srcChipInflightCounter = numaConfig.inflightCounter;
        const uint64_t writeSize = std::min(remainSize, maxWriteSize);
        ++writeChunkIndex;
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
                LOG_IF_ERROR(urmaResource_->RequestRetireActiveSendLane(laneLease),
                             "Failed to request URMA write lane retirement after injected write failure");
            }
            cleanupSubmittedEvents();
            return injectRc;
        }
        std::shared_ptr<UrmaEvent> event;
        auto createRc = CreateEvent(key, args.connection, laneLease, args.remoteAddress, writeSize,
                                    UrmaEvent::OperationType::WRITE, srcChipInflightCounter, args.waiter, &event,
                                    args.lateCompletionContext);
        if (createRc.IsError()) {
            if (ownsLaneLease) {
                LOG_IF_ERROR(urmaResource_->RequestRetireActiveSendLane(laneLease),
                             "Failed to request URMA write lane retirement after event creation failure");
            }
            cleanupSubmittedEvents();
            return createRc;
        }
        event->SetWriteChunkInfo(writeChunkIndex, writeChunkCount);
        laneLease->AddWr();
        urma_status_t ret;
        Timer t;
        event->SetWritePostTimeUs(t.GetStartTimeStampUs());
        METRIC_TIMER(metrics::KvMetricId::URMA_WRITE_LATENCY);
        auto jettyId = jetty->GetJettyId();
        RecordNumaWriteChipCounts(srcChipId, args.dstChipId);
        if (useNumaAffinity && FLAGS_ub_numa_rr_type != static_cast<uint32_t>(UbNumaRrType::DISABLED)) {
            RecordNumaWriteCrossChipCount(srcChipId, args.dstChipId);
        }
        LOG_EVERY_T(INFO, LOG_TIME_LIMIT_LEVEL1)
            << "URMA write useNumaAffinity:" << useNumaAffinity << ", src:" << static_cast<uint32_t>(srcChipId)
            << ", dst:" << static_cast<uint32_t>(args.dstChipId) << ", jetty id:" << jettyId
            << ", urma_inflight_wr_count:" << tbbEventMap_.size()
            << ", numa_write_counts:" << GetNumaWriteChipCountsString();
        if (useNumaAffinity) {
            auto numaInjectRc = []() -> Status {
                INJECT_POINT("UrmaManager.UrmaWriteNumaAffinity");
                return Status::OK();
            }();
            if (numaInjectRc.IsError()) {
                LOG_IF_ERROR(urmaResource_->CancelActiveSendLane(laneLease),
                             "Failed to cancel URMA WR after NUMA-affinity post setup failure");
                if (ownsLaneLease) {
                    LOG_IF_ERROR(urmaResource_->RequestRetireActiveSendLane(laneLease),
                                 "Failed to request URMA lane retirement after NUMA-affinity post setup failure");
                }
                // Event lifetime is independent from the owned/shared lane policy. This key has not been appended to
                // eventKeys, so cleanupSubmittedEvents() cannot remove it.
                DeleteEvent(key);
                cleanupSubmittedEvents();
                return numaInjectRc;
            }
        }
        ret = PostJettyRw(jetty, URMA_OPC_WRITE, targetJetty, args.remoteSeg, args.localSeg, remoteAddress,
                          localAddress, writeSize, flag, key, useNumaAffinity, srcChipId, args.dstChipId);
        if (ret != URMA_SUCCESS) {
            if (failure != nullptr && !failure->providerStatus.has_value()) {
                failure->providerStatus = static_cast<int>(ret);
            }
            LOG_IF_ERROR(urmaResource_->CancelActiveSendLane(laneLease), "Failed to cancel unaccepted URMA write WR");
            DeleteEvent(key);
            cleanupSubmittedEvents();
            const auto srcAddress = localUrmaInfo_.localAddress.ToString();
            const auto remoteInstanceId =
                args.connection == nullptr ? "" : args.connection->GetUrmaJfrInfo().uniqueInstanceId.c_str();
            RETURN_STATUS_LOG_ERROR(
                K_URMA_ERROR, FormatString("[URMA_WRITE]: [urma_request_id:%zu] call urma_post_jetty_send_wr failed, "
                                           "ret: %d, srcAddress=%s, targetAddress=%s, remoteInstanceId=%s, "
                                           "dataSize=%zu, srcChipId=%u, dstChipId=%u, useNumaAffinity=%s, "
                                           "suggest: %s",
                                           key, ret, srcAddress.c_str(), args.remoteAddress.c_str(), remoteInstanceId,
                                           static_cast<size_t>(writeSize), static_cast<uint32_t>(srcChipId),
                                           static_cast<uint32_t>(args.dstChipId), useNumaAffinity ? "true" : "false",
                                           URMA_ERROR_SUGGEST));
        }
        t.Stop();
        auto elapsedUs = t.ElapsedMicroSecond();
        auto vlogLevel = elapsedUs > URMA_WRITE_VLOG0_LIMIT_US ? 0 : 1;
        VLOG(vlogLevel) << "[UrmaWrite] [urma_request_id:" << key << "] URMA finish write, cpuid:" << sched_getcpu()
                        << ", elapsed:" << elapsedUs << " us, jetty id:" << jettyId;
        pointWrite.Record();
        remainSize -= writeSize;
        writtenSize += writeSize;
        eventKeys.emplace_back(key);
        INJECT_POINT("UrmaManager.UrmaWriteAfterPost");
    }
    GetWorkerTimeCost().Append("Urma total write.", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status UrmaManager::AcquireSendLane(const UrmaRemoteAddrPb &urmaInfo, std::shared_ptr<UrmaSendLaneLease> &laneLease)
{
    laneLease.reset();
    const HostPort requestAddress(urmaInfo.request_address().host(), urmaInfo.request_address().port());
    std::string remoteConnectionId = urmaInfo.client_id().empty() ? requestAddress.ToString() : urmaInfo.client_id();
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    TbbUrmaConnectionMap::const_accessor constAccessor;
    // The comm layer has already exchanged the jfr, and we should be able to locate the entry.
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
    laneLease = std::make_shared<UrmaSendLaneLease>(jetty, requestId_.load(std::memory_order_relaxed));
    auto registerRc = urmaResource_->RegisterActiveSendLane(laneLease);
    if (registerRc.IsError()) {
        LOG_IF_ERROR(urmaResource_->RetireJetty(jetty),
                     "Failed to retire URMA send Jetty after lane registration failure");
        laneLease.reset();
        return registerRc;
    }
    return Status::OK();
}

Status UrmaManager::UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                     const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                     const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                     uint8_t srcChipId, uint8_t dstChipId, bool blocking,
                                     std::vector<uint64_t> &eventKeys, std::shared_ptr<EventWaiter> waiter,
                                     UrmaWriteFailure *failure,
                                     std::optional<UrmaLateCompletionContext> lateCompletionContext)
{
    return UrmaWritePayloadImpl(urmaInfo, localSegAddress, localSegSize, localObjectAddress, readOffset, readSize,
                                metaDataSize, srcChipId, dstChipId, blocking, eventKeys, nullptr, waiter, failure,
                                std::move(lateCompletionContext));
}

Status UrmaManager::UrmaWritePayloadWithLane(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                             const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                             const uint64_t &readOffset, const uint64_t &readSize,
                                             const uint64_t &metaDataSize, uint8_t srcChipId, uint8_t dstChipId,
                                             bool blocking, std::vector<uint64_t> &eventKeys,
                                             const std::shared_ptr<UrmaSendLaneLease> &laneLease,
                                             std::shared_ptr<EventWaiter> waiter, UrmaWriteFailure *failure,
                                             std::optional<UrmaLateCompletionContext> lateCompletionContext)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(laneLease != nullptr, K_RUNTIME_ERROR,
                                         "Batch Get URMA send lane lease is null");
    return UrmaWritePayloadImpl(urmaInfo, localSegAddress, localSegSize, localObjectAddress, readOffset, readSize,
                                metaDataSize, srcChipId, dstChipId, blocking, eventKeys, laneLease, waiter, failure,
                                std::move(lateCompletionContext));
}

Status UrmaManager::UrmaWritePayloadImpl(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                         const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                         const uint64_t &readOffset, const uint64_t &readSize,
                                         const uint64_t &metaDataSize, uint8_t srcChipId, uint8_t dstChipId,
                                         bool blocking, std::vector<uint64_t> &eventKeys,
                                         const std::shared_ptr<UrmaSendLaneLease> &externalLaneLease,
                                         std::shared_ptr<EventWaiter> waiter, UrmaWriteFailure *failure,
                                         std::optional<UrmaLateCompletionContext> lateCompletionContext)
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
            laneLease = std::make_shared<UrmaSendLaneLease>(jetty, requestId_.load(std::memory_order_relaxed));
            auto registerRc = urmaResource_->RegisterActiveSendLane(laneLease);
            if (registerRc.IsError()) {
                LOG_IF_ERROR(urmaResource_->RetireJetty(jetty),
                             "Failed to retire URMA send Jetty after lane registration failure");
                return registerRc;
            }
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
        laneLease->DisableRequestIdGenerationCheck();
        // The transport pipeline receives a raw handle and may issue several provider posts.
        // Keep one permit over the complete synchronous pipeline call, not merely argument setup.
        auto pipelinePermit = jetty->TryAcquirePostPermit();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pipelinePermit, K_URMA_TRY_AGAIN, "URMA pipeline Jetty is closing");
        OsXprtPipln::PiplnSndArgs args;
        args.jetty = pipelinePermit.Raw();
        args.tjetty = targetJetty;
        args.localAddr = localObjectAddress + readOffset + metaDataSize;
        args.localSeg = localSegAccessor->second->Raw();
        args.remoteAddr = segVa + urmaInfo.seg_data_offset() + readOffset;
        args.remoteSeg = remoteSegAccessor->second->Raw();
        args.len = readSize;
        args.serverKey = GenerateReqId();
        args.clientKey = urmaInfo.pipeline_rh2d_req_id();

        RETURN_IF_NOT_OK(
            CreateEvent(args.serverKey, connection, laneLease, remoteAddress, readSize,
                        UrmaEvent::OperationType::WRITE, nullptr, nullptr, nullptr, lateCompletionContext));
        laneLease->AddWr();
        eventKeys.emplace_back(args.serverKey);
        Status rc;
        rc = OsXprtPipln::DoPiplnStep1_StartSender(args);
        if (rc.IsError()) {
            LOG_IF_ERROR(urmaResource_->CancelActiveSendLane(laneLease),
                         "Failed to cancel unaccepted URMA pipeline WR");
            // A failed object WR does not retire an RPC-shared lane.
            DeleteEvent(args.serverKey);
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
    writeLoopArgs.lateCompletionContext = std::move(lateCompletionContext);
    RETURN_IF_NOT_OK(UrmaWriteImpl(writeLoopArgs, eventKeys, externalLaneLease, failure));
    point.Record();
    // If it is blocking wait, we will wait for the write to finish here.
    if (blocking) {
        auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitFastTransportEventWithFailure(eventKeys, remainingTime, errorHandler, failure));
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
    // The comm layer has already exchanged the jfr, and we should be able to locate the entry.
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
    auto laneLease = std::make_shared<UrmaSendLaneLease>(jetty, requestId_.load(std::memory_order_relaxed));
    auto registerRc = urmaResource_->RegisterActiveSendLane(laneLease);
    if (registerRc.IsError()) {
        LOG_IF_ERROR(urmaResource_->RetireJetty(jetty),
                     "Failed to retire URMA send Jetty after lane registration failure");
        return registerRc;
    }
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
        laneLease->AddWr();
        urma_status_t ret = PostJettyRw(
            jetty, URMA_OPC_READ, targetJetty, remoteSegAccessor->second->Raw(), localSegAccessor->second->Raw(),
            segVa + urmaInfo.seg_data_offset() + readOffset, localObjectAddress + metaDataSize + readOffset, readSize,
            flag, key, false, INVALID_CHIP_ID, INVALID_CHIP_ID);
        if (ret != URMA_SUCCESS) {
            LOG_IF_ERROR(urmaResource_->CancelActiveSendLane(laneLease), "Failed to cancel unaccepted URMA read WR");
            DeleteEvent(key);
            cleanupSubmittedEvents();
            const auto srcAddress = localUrmaInfo_.localAddress.ToString();
            RETURN_STATUS_LOG_ERROR(
                K_URMA_ERROR, FormatString("[URMA_READ]: [urma_request_id:%zu] call urma_post_jetty_send_wr failed, "
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
                                    bool blocking, std::vector<uint64_t> &eventKeys,
                                    std::optional<UrmaLateCompletionContext> lateCompletionContext)
{
    return UrmaGatherWriteImpl(remoteInfo, objInfos, blocking, eventKeys, nullptr,
                               std::move(lateCompletionContext));
}

Status UrmaManager::UrmaGatherWriteWithLane(const RemoteSegInfo &remoteInfo,
                                            const std::vector<LocalSgeInfo> &objInfos, bool blocking,
                                            std::vector<uint64_t> &eventKeys,
                                            const std::shared_ptr<UrmaSendLaneLease> &laneLease,
                                            std::optional<UrmaLateCompletionContext> lateCompletionContext)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(laneLease != nullptr, K_RUNTIME_ERROR,
                                         "Batch Get URMA send lane lease is null");
    return UrmaGatherWriteImpl(remoteInfo, objInfos, blocking, eventKeys, laneLease,
                               std::move(lateCompletionContext));
}

Status UrmaManager::InitGatherWriteContext(const RemoteSegInfo &remoteInfo, size_t sgeNum,
                                           const std::shared_ptr<UrmaSendLaneLease> &externalLaneLease,
                                           UrmaGatherWriteContext &context)
{
    constexpr size_t wrSgeMaxNum = 13;
    const HostPort requestAddress(remoteInfo.host, remoteInfo.port);
    context.remoteAddress = requestAddress.ToString();
    context.remoteMapLock = std::shared_lock<std::shared_timed_mutex>(remoteMapMutex_);
    const bool found = urmaConnectionMap_.find(context.connectionAccessor, context.remoteAddress);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        found, K_RUNTIME_ERROR, FormatString("Failed to find jfr from %s", context.remoteAddress));
    context.connection = context.connectionAccessor->second;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(context.connection != nullptr, K_RUNTIME_ERROR, "Urma connection is null");
    RETURN_IF_NOT_OK(context.connection->GetRemoteSeg(remoteInfo.segAddr, context.remoteSegAccessor));

    const size_t dstSgeNum = (sgeNum + wrSgeMaxNum - 1) / wrSgeMaxNum;
    context.srcSgeList.resize(sgeNum);
    context.dstSgeList.resize(dstSgeNum);
    context.wrList.resize(dstSgeNum);
    context.createdEventKeys.reserve(dstSgeNum);
    context.submittedEventKeys.reserve(dstSgeNum);
    INJECT_POINT("UrmaManager.GatherWriteError", []() { return Status(K_RUNTIME_ERROR, "Injcect urma wait error"); });

    context.ownsLaneLease = externalLaneLease == nullptr;
    context.laneLease = externalLaneLease;
    if (context.ownsLaneLease) {
        RETURN_IF_NOT_OK(AcquireSendLaneFromConnection(context.connection, context.jetty, context.targetJetty));
        context.laneLease =
            std::make_shared<UrmaSendLaneLease>(context.jetty, requestId_.load(std::memory_order_relaxed));
        auto registerRc = urmaResource_->RegisterActiveSendLane(context.laneLease);
        if (registerRc.IsError()) {
            LOG_IF_ERROR(urmaResource_->RetireJetty(context.jetty),
                         "Failed to retire URMA send Jetty after lane registration failure");
            return registerRc;
        }
    } else {
        context.jetty = externalLaneLease->GetJetty();
        context.targetJetty = context.connection->GetTargetJetty();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(context.jetty != nullptr, K_RUNTIME_ERROR,
                                         "Batch Get URMA gather send lane Jetty is null");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(context.targetJetty != nullptr, K_RUNTIME_ERROR,
                                         "Gather write got empty remote Jetty");
    INJECT_POINT("UrmaManager.GatherWriteAfterAcquire");
    return Status::OK();
}

uint8_t UrmaManager::SelectDominantGatherSrcChipId(const std::vector<LocalSgeInfo> &objInfos, size_t begin, size_t end)
{
    if (begin >= end || end > objInfos.size()) {
        return INVALID_CHIP_ID;
    }
    std::array<uint64_t, URMA_AFFINITY_SRC_CHIP_MAX + 1> bytesByChip{};
    uint8_t dominantChipId = INVALID_CHIP_ID;
    uint64_t dominantBytes = 0;
    for (size_t i = begin; i < end; ++i) {
        const auto chipId = objInfos[i].srcChipId;
        if (chipId < URMA_AFFINITY_SRC_CHIP_MIN || chipId > URMA_AFFINITY_SRC_CHIP_MAX) {
            continue;
        }
        bytesByChip[chipId] += objInfos[i].writeSize;
        if (bytesByChip[chipId] > dominantBytes) {
            dominantChipId = chipId;
            dominantBytes = bytesByChip[chipId];
        }
    }
#ifdef WITH_TESTS
    INJECT_POINT_NO_RETURN("UrmaManager.OverrideGatherDominantSrcChip", [&dominantChipId](int chipId) {
        if (chipId >= URMA_AFFINITY_SRC_CHIP_MIN && chipId <= URMA_AFFINITY_SRC_CHIP_MAX) {
            dominantChipId = static_cast<uint8_t>(chipId);
        }
    });
#endif
    return dominantChipId;
}

Status UrmaManager::AppendGatherWriteRequest(
    const RemoteSegInfo &remoteInfo, const std::vector<LocalSgeInfo> &objInfos, size_t dstSgeIdx, size_t &srcSgeIdx,
    const std::optional<UrmaLateCompletionContext> &lateCompletionContext, UrmaGatherWriteContext &context)
{
    constexpr size_t wrSgeMaxNum = 13;
    const auto srcSgeStart = srcSgeIdx;
    uint64_t singleDstWriteSize = 0;
    while (srcSgeIdx < objInfos.size()) {
        const auto &element = objInfos[srcSgeIdx];
        UrmaLocalSegmentMap::const_accessor localSegAccessor;
        RETURN_IF_NOT_OK(GetOrRegisterSegment(element.segAddr, element.segSize, localSegAccessor));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(localSegAccessor->second != nullptr, K_RUNTIME_ERROR,
                                             "Local segment is null");
        context.srcSgeList[srcSgeIdx] = urma_sge_t{
            .addr = element.sgeAddr + element.metaDataSize + element.readOffset,
            .len = static_cast<uint32_t>(element.writeSize),
            .tseg = localSegAccessor->second->Raw(),
            .user_tseg = nullptr,
        };
        singleDstWriteSize += context.srcSgeList[srcSgeIdx].len;
        if (++srcSgeIdx % wrSgeMaxNum == 0) {
            break;
        }
    }

    urma_sg_t srcSg = { .sge = &context.srcSgeList[srcSgeStart],
                        .num_sge = static_cast<uint32_t>(srcSgeIdx - srcSgeStart) };
    context.dstSgeList[dstSgeIdx] = { .addr = remoteInfo.segAddr + remoteInfo.segOffset + context.totalWriteSize,
                                      .len = static_cast<uint32_t>(singleDstWriteSize),
                                      .tseg = context.remoteSegAccessor->second->Raw(),
                                      .user_tseg = nullptr };
    context.totalWriteSize += singleDstWriteSize;
    urma_sg_t dstSg = { .sge = &context.dstSgeList[dstSgeIdx], .num_sge = 1 };
    const auto dominantSrcChipId = SelectDominantGatherSrcChipId(objInfos, srcSgeStart, srcSgeIdx);
    return CreateGatherWriteEvent(dstSgeIdx, singleDstWriteSize, dominantSrcChipId, remoteInfo.dstChipId, srcSg, dstSg,
                                  lateCompletionContext, context);
}

Status UrmaManager::CreateGatherWriteEvent(
    size_t dstSgeIdx, uint64_t writeSize, uint8_t transmittedSrcChipId, uint8_t dstChipId,
    const urma_sg_t &srcSg, const urma_sg_t &dstSg,
    const std::optional<UrmaLateCompletionContext> &lateCompletionContext, UrmaGatherWriteContext &context)
{
    urma_jfs_wr_flag_t flag = { .value = 0 };
    flag.bs.complete_enable = 1;
    const bool firstPost = dstSgeIdx == 0;
    const uint64_t selectionWrCount = firstPost ? context.wrList.size() : 1;
    const auto numaConfig =
        ResolveNumaPostConfig(transmittedSrcChipId, dstChipId, firstPost, context.logicalWriteChipId, selectionWrCount,
                              firstPost ? INVALID_CHIP_ID : context.logicalWriteCandidateChipId);
    context.logicalWriteChipId = numaConfig.srcChipId;
    if (firstPost) {
        context.logicalWriteCandidateChipId = numaConfig.candidateChipId;
    }
    flag.bs.has_drv_ext = numaConfig.enabled ? 1 : 0;
    urma_rw_wr_t rw = { .src = srcSg, .dst = dstSg, .target_hint = 0, .notify_data = 0 };
    const uint64_t requestId = GenerateReqId();
    auto &bondpWr = context.wrList[dstSgeIdx];
    bondpWr = bondp_jfs_wr_t{};
    auto &wr = bondpWr.base;
    wr.opcode = URMA_OPC_WRITE;
    wr.flag = flag;
    wr.tjetty = context.targetJetty;
    wr.user_ctx = requestId;
    wr.rw = rw;
    wr.next = nullptr;
    bondpWr.src_chip_id = numaConfig.srcChipId;
    bondpWr.dst_chip_id = dstChipId;
    RETURN_IF_NOT_OK(CreateEvent(requestId, context.connection, context.laneLease, context.remoteAddress, writeSize,
                                 UrmaEvent::OperationType::WRITE, numaConfig.inflightCounter, nullptr, nullptr,
                                 lateCompletionContext, true));
    context.laneLease->AddWr();
    context.createdEventKeys.emplace_back(requestId);
    if (dstSgeIdx > 0) {
        context.wrList[dstSgeIdx - 1].base.next = &wr;
    }
#ifdef WITH_TESTS
    if (numaConfig.enabled && numaConfig.srcChipId == URMA_AFFINITY_SRC_CHIP_MIN) {
        INJECT_POINT_NO_RETURN("UrmaManager.GatherSrcChipSelected.1");
    } else if (numaConfig.enabled && numaConfig.srcChipId == URMA_AFFINITY_SRC_CHIP_MAX) {
        INJECT_POINT_NO_RETURN("UrmaManager.GatherSrcChipSelected.2");
    }
#endif
    return Status::OK();
}

Status UrmaManager::BuildGatherWriteRequests(
    const RemoteSegInfo &remoteInfo, const std::vector<LocalSgeInfo> &objInfos,
    const std::optional<UrmaLateCompletionContext> &lateCompletionContext, UrmaGatherWriteContext &context)
{
    size_t srcSgeIdx = 0;
    for (size_t dstSgeIdx = 0; dstSgeIdx < context.wrList.size(); ++dstSgeIdx) {
        auto rc = AppendGatherWriteRequest(remoteInfo, objInfos, dstSgeIdx, srcSgeIdx, lateCompletionContext, context);
        if (rc.IsError()) {
            CleanupGatherWriteEvents(context, 0);
            return rc;
        }
    }
    return Status::OK();
}

Status UrmaManager::SealGatherWriteLane(UrmaGatherWriteContext &context)
{
    if (context.laneLeaseSealed) {
        return Status::OK();
    }
    context.laneLeaseSealed = true;
    return SealSendLaneLease(context.laneLease);
}

void UrmaManager::CleanupGatherWriteEvents(UrmaGatherWriteContext &context, size_t submittedCount)
{
    submittedCount = std::min(submittedCount, context.createdEventKeys.size());
    for (size_t i = submittedCount; i < context.createdEventKeys.size(); ++i) {
        LOG_IF_ERROR(urmaResource_->CancelActiveSendLane(context.laneLease),
                     "Failed to cancel unaccepted URMA gather WR");
        DeleteEvent(context.createdEventKeys[i]);
    }
    context.submittedEventKeys.assign(context.createdEventKeys.begin(),
                                      context.createdEventKeys.begin() + submittedCount);
    if (context.ownsLaneLease) {
        LOG_IF_ERROR(SealGatherWriteLane(context), "Failed to seal URMA gather write lane lease during cleanup");
    }
    if (context.submittedEventKeys.empty()) {
        return;
    }
    auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
    auto errorHandler = [](Status &status) { return status; };
    LOG_IF_ERROR(WaitFastTransportEvent(context.submittedEventKeys, remainingTime, errorHandler),
                 "Failed to cleanup submitted URMA gather write events");
    context.submittedEventKeys.clear();
}

size_t UrmaManager::ResolveSubmittedGatherWriteCount(const RemoteSegInfo &remoteInfo, const urma_jfs_wr_t *badWr,
                                                     const UrmaGatherWriteContext &context) const
{
    if (badWr != nullptr) {
        for (size_t i = 0; i < context.wrList.size(); ++i) {
            if (badWr == &context.wrList[i].base) {
                return i;
            }
        }
    }
    const auto srcAddress = localUrmaInfo_.localAddress.ToString();
    if (badWr == nullptr) {
        LOG(WARNING) << "[URMA_WRITE]: provider post failed without bad_wr; treating all gather-write events as "
                        "potentially accepted, srcAddress="
                     << srcAddress << ", targetAddress=" << context.remoteAddress
                     << ", dataSize=" << static_cast<size_t>(context.totalWriteSize)
                     << ", dstChipId=" << static_cast<uint32_t>(remoteInfo.dstChipId);
    } else {
        LOG(WARNING) << "[URMA_WRITE]: provider returned a bad_wr outside the submitted WR chain; "
                     << "treating all gather-write events as potentially accepted, srcAddress=" << srcAddress
                     << ", targetAddress=" << context.remoteAddress
                     << ", dataSize=" << static_cast<size_t>(context.totalWriteSize)
                     << ", dstChipId=" << static_cast<uint32_t>(remoteInfo.dstChipId);
    }
    return context.createdEventKeys.size();
}

Status UrmaManager::PostGatherWriteRequests(const RemoteSegInfo &remoteInfo, UrmaGatherWriteContext &context)
{
    auto gatherPermit = context.jetty->TryAcquirePostPermit();
    if (!gatherPermit) {
        CleanupGatherWriteEvents(context, 0);
        RETURN_STATUS(K_URMA_TRY_AGAIN, "URMA gather-write Jetty is closing");
    }
    urma_jfs_wr_t *badWr = nullptr;
    Timer timer;
    auto ret = ds_urma_post_jetty_send_wr(gatherPermit.Raw(), &context.wrList[0].base, &badWr);
    GetWorkerTimeCost().Append("Urma gather write.", timer.ElapsedMilliSecond());
    if (ret == URMA_SUCCESS) {
        for (const auto &wr : context.wrList) {
            RecordNumaWriteChipCounts(wr.src_chip_id, wr.dst_chip_id);
            if (wr.base.flag.bs.has_drv_ext != 0) {
                RecordNumaWriteCrossChipCount(wr.src_chip_id, wr.dst_chip_id);
            }
        }
        return Status::OK();
    }
    const size_t submittedCount = ResolveSubmittedGatherWriteCount(remoteInfo, badWr, context);
    for (size_t i = 0; i < submittedCount; ++i) {
        const auto &wr = context.wrList[i];
        RecordNumaWriteChipCounts(wr.src_chip_id, wr.dst_chip_id);
        if (wr.base.flag.bs.has_drv_ext != 0) {
            RecordNumaWriteCrossChipCount(wr.src_chip_id, wr.dst_chip_id);
        }
    }
    CleanupGatherWriteEvents(context, submittedCount);
    const auto srcAddress = localUrmaInfo_.localAddress.ToString();
    RETURN_STATUS_LOG_ERROR(
        K_URMA_ERROR,
        FormatString("[URMA_WRITE]: call urma_post_jetty_send_wr failed, ret: %d, "
                     "srcAddress=%s, targetAddress=%s, dataSize=%zu, dstChipId=%u, suggest: %s",
                     ret, srcAddress.c_str(), context.remoteAddress.c_str(),
                     static_cast<size_t>(context.totalWriteSize), static_cast<uint32_t>(remoteInfo.dstChipId),
                     URMA_ERROR_SUGGEST));
}

Status UrmaManager::UrmaGatherWriteImpl(const RemoteSegInfo &remoteInfo,
                                        const std::vector<LocalSgeInfo> &objInfos, bool blocking,
                                        std::vector<uint64_t> &eventKeys,
                                        const std::shared_ptr<UrmaSendLaneLease> &externalLaneLease,
                                        std::optional<UrmaLateCompletionContext> lateCompletionContext)
{
    eventKeys.clear();
    if (objInfos.empty()) {
        return Status::OK();
    }
    UrmaGatherWriteContext context;
    RETURN_IF_NOT_OK(InitGatherWriteContext(remoteInfo, objInfos.size(), externalLaneLease, context));
    Raii sealOnExit([this, &context]() {
        if (context.ownsLaneLease) {
            LOG_IF_ERROR(SealGatherWriteLane(context), "Failed to seal URMA gather write lane lease");
        }
    });
    RETURN_IF_NOT_OK(BuildGatherWriteRequests(remoteInfo, objInfos, lateCompletionContext, context));
    RETURN_IF_NOT_OK(PostGatherWriteRequests(remoteInfo, context));

    eventKeys = context.createdEventKeys;
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

Status UrmaManager::ProcessHandshakePeer(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb *rsp)
{
    UrmaJfrInfo urmaInfo;
    RETURN_IF_NOT_OK(urmaInfo.FromProto(req));
    LOG(INFO) << "Start import remote jetty, remote urma info: " << urmaInfo.ToString()
              << ", local address:" << localUrmaInfo_.localAddress;
    if (localUrmaInfo_.localAddress != urmaInfo.localAddress || !req.client_id().empty()
        || clientMode_.load(std::memory_order_acquire)) {
        uint32_t localJettyId = 0;
        METRIC_TIMER(metrics::KvMetricId::URMA_CONNECTION_SETUP_LATENCY);
        RETURN_IF_NOT_OK(ImportRemoteJetty(urmaInfo, localJettyId));
        RETURN_IF_NOT_OK(ImportRemoteInfo(req));
        if (rsp != nullptr) {
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
                LOG(WARNING) << "[URMA_CONNECT] Failed to get delegated rjetty context for response, status="
                             << urmaStatus << ", fallback to legacy handshake";
            }
            localInfo.ToProto(*rsp->mutable_hand_shake());
            RETURN_IF_NOT_OK(GetSegmentInfo(*rsp));
        }
    }
    if (!req.client_id().empty()) {
        std::lock_guard<std::mutex> lock(clientIdMutex_);
        clientIdMapping_[ClientKey::Intern(req.client_entity_id())] = req.client_id();
    }
    return Status::OK();
}

Status UrmaManager::ImportRecoveryProbeHandshake(const UrmaHandshakeReqPb &req)
{
    return ProcessHandshakePeer(req, nullptr);
}

Status UrmaManager::ExchangeJfr(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp)
{
    return ProcessHandshakePeer(req, &rsp);
}

void UrmaManager::SetClientUrmaConfig(FastTransportMode urmaMode, uint64_t transportSize, bool enablePipelineH2D)
{
    // Note: The parameter needs to be consistent in the same client process.
    if (urmaMode == FastTransportMode::UB) {
#ifdef BUILD_PIPLN_H2D
        if (enablePipelineH2D) {
            SetClientPipelineRH2DEnabled();
        }
#else
        (void)enablePipelineH2D;
#endif
        UrmaManager::clientMode_.store(true, std::memory_order_release);
        uint64_t expected = DEFAULT_TRANSPORT_MEM_SIZE;
        if (UrmaManager::ubTransportMemSize_.compare_exchange_strong(expected, transportSize)) {
            LOG(INFO) << "Set client UB transport memory size to " << transportSize;
        } else {
            LOG(WARNING) << FormatString(
                "Try to set client UB transport memory size to %lu, but it is already set to %lu", transportSize,
                UrmaManager::ubTransportMemSize_);
        }
        RequestClientUrmaRuntime();
        // FLAGS_urma_connection_size is deprecated; JFS/JFR are created per-connection.
    }
}

uint32_t UrmaManager::NormalizeUbNumaRrType(uint32_t rrType, const std::string &configSource)
{
    constexpr auto defaultRrType = UbNumaRrType::PER_LOGICAL_WRITE;
    constexpr auto maxRrType = UbNumaRrType::PER_POST;
    if (rrType > static_cast<uint32_t>(maxRrType)) {
        LOG(WARNING) << "Worker " << configSource << " reported invalid UB NUMA rrType=" << rrType
                     << ", use default rrType=" << static_cast<uint32_t>(defaultRrType);
        return static_cast<uint32_t>(defaultRrType);
    }
    return rrType;
}

uint32_t UrmaManager::NormalizeUbNumaSrcChipPolicy(uint32_t srcChipPolicy, const std::string &configSource)
{
    constexpr auto defaultPolicy = UbNumaSrcChipPolicy::ROUND_ROBIN_WITH_AFFINITY;
    if (srcChipPolicy > static_cast<uint32_t>(defaultPolicy)) {
        LOG(WARNING) << "Worker " << configSource << " reported invalid UB NUMA srcChipPolicy=" << srcChipPolicy
                     << ", use default srcChipPolicy=" << static_cast<uint32_t>(defaultPolicy);
        return static_cast<uint32_t>(defaultPolicy);
    }
    return srcChipPolicy;
}

void UrmaManager::SetClientUbNumaConfig(bool affinityEnabled, uint32_t rrType, uint32_t srcChipPolicy,
                                        uint32_t inflightWrDiffThreshold, const std::string &configSource)
{
    rrType = NormalizeUbNumaRrType(rrType, configSource);
    srcChipPolicy = NormalizeUbNumaSrcChipPolicy(srcChipPolicy, configSource);
    bool isFirstWorker = false;
    std::call_once(Instance().clientUbNumaConfigOnce_, [&]() {
        FLAGS_enable_ub_numa_affinity = affinityEnabled;
        FLAGS_ub_numa_rr_type = rrType;
        FLAGS_ub_numa_src_chip_policy = srcChipPolicy;
        FLAGS_ub_numa_inflight_wr_diff_threshold = inflightWrDiffThreshold;
        isFirstWorker = true;
        LOG(INFO) << "Set client UB NUMA config from worker " << configSource << ", affinityEnabled=" << affinityEnabled
                  << ", rrType=" << rrType << ", srcChipPolicy=" << srcChipPolicy
                  << ", inflightWrDiffThreshold=" << inflightWrDiffThreshold;
    });
    if (!isFirstWorker
        && (FLAGS_enable_ub_numa_affinity != affinityEnabled || FLAGS_ub_numa_rr_type != rrType
            || FLAGS_ub_numa_src_chip_policy != srcChipPolicy
            || FLAGS_ub_numa_inflight_wr_diff_threshold != inflightWrDiffThreshold)) {
        LOG(WARNING) << "Worker " << configSource << " reported UB NUMA config affinityEnabled=" << affinityEnabled
                     << ", rrType=" << rrType << ", srcChipPolicy=" << srcChipPolicy
                     << ", inflightWrDiffThreshold=" << inflightWrDiffThreshold
                     << ", but the client keeps affinityEnabled=" << FLAGS_enable_ub_numa_affinity
                     << ", rrType=" << FLAGS_ub_numa_rr_type << ", srcChipPolicy=" << FLAGS_ub_numa_src_chip_policy
                     << ", inflightWrDiffThreshold=" << FLAGS_ub_numa_inflight_wr_diff_threshold;
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
