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
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/utils/status.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rdma/ucp_manager.h"
#include "datasystem/common/rdma/ucp_segment.h"
#include "datasystem/common/rdma/ucp_worker_pool.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uuid_generator.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <shared_mutex>

constexpr uint32_t DEFAULT_UCP_WORKER_NUM = 4;
constexpr uint64_t MAX_MSG_SIZE = 512 * 1024 * 1024;

namespace datasystem {
UcpManager &UcpManager::Instance()
{
    static UcpManager manager;
    return manager;
}

UcpManager::UcpManager() : localSegmentMap_(std::make_unique<UcpSegmentMap>()), eventMap_(std::make_unique<EventMap>())
{
    uniqueInstanceId_ = GetStringUuid();
    VLOG(RPC_LOG_LEVEL) << "UcpManager::UcpManager()";
}

UcpManager::~UcpManager()
{
    VLOG(RPC_LOG_LEVEL) << "UcpManager::~UcpManager()";
    workerPool_.reset();
    localSegmentMap_.reset();
    eventMap_.reset();
    UcpDeleteContext();
    ucp_dlopen::Cleanup();
    VLOG(RPC_LOG_LEVEL) << "UcpManager::~UcpManager() done";
}

Status UcpManager::Init()
{
    LOG(INFO) << "UcpManager::Init()";
    // Initialize UCP dlopen loader
    if (!datasystem::ucp_dlopen::Init()) {
        RETURN_STATUS_LOG_ERROR(K_RDMA_ERROR, "Failed to initialize UCP dlopen loader");
    }
    RETURN_IF_NOT_OK(UcpCreateContext());
    RETURN_IF_NOT_OK(UcpCreateWorkerPool());
    return Status::OK();
}

Status UcpManager::UcpCreateContext()
{
    LOG(INFO) << "UcpManager::UcpCreateContext()";
    ucp_config_t *config = nullptr;
    ucs_status_t configRet = ds_ucp_config_read(nullptr, nullptr, &config);
    if (configRet != UCS_OK) {
        RETURN_STATUS_LOG_ERROR(
            K_RDMA_ERROR, FormatString("Failed to read UCX config, ret = %d. Possible causes: "
                                       "RDMA driver or UCX dependencies are missing or "
                                       "incomplete. Set UCX_LOG_FILE and UCX_LOG_LEVEL to capture detailed UCX logs.",
                                       configRet));
    }
    ucp_params_t params;
    memset_s(&params, sizeof(params), 0, sizeof(params));
    params.field_mask = UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
    // Feature flags
    params.features = UCP_FEATURE_RMA | UCP_FEATURE_WAKEUP;
    // Multi-threaded worker shared mode
    params.mt_workers_shared = 1;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!ucpContext_, K_DUPLICATED,
                                         "Failed to ucp create context, context already exist");
    ucs_status_t contextRet = ds_ucp_init(&params, config, &ucpContext_);
    ds_ucp_config_release(config);
    if (contextRet != UCS_OK) {
        RETURN_STATUS_LOG_ERROR(
            K_RDMA_ERROR, FormatString("Failed to ucp create context, ret = %d. Possible causes: "
                                       "RDMA driver or UCX dependencies are missing or "
                                       "incomplete. Set UCX_LOG_FILE and UCX_LOG_LEVEL to capture detailed UCX logs.",
                                       contextRet));
    }
    LOG(INFO) << "ucp create context success";
    return Status::OK();
}

Status UcpManager::UcpDeleteContext()
{
    LOG(INFO) << "UcpManager::UcpDeleteContext()";
    if (ucpContext_) {
        ds_ucp_cleanup(ucpContext_);
        ucpContext_ = nullptr;
    }
    return Status::OK();
}

Status UcpManager::UcpCreateWorkerPool()
{
    LOG(INFO) << "UcpManager::UcpCreateWorkerPool()";
    workerPool_ = std::make_unique<UcpWorkerPool>(ucpContext_, this, DEFAULT_UCP_WORKER_NUM);
    Status status = workerPool_->Init();
    if (!status.IsOk()) {
        UcpDeleteContext();
        std::string detailed_msg =
            FormatString("Failed to create worker pool. Underlying error: %s", status.ToString().c_str());
        RETURN_STATUS_LOG_ERROR(K_RDMA_ERROR, detailed_msg);
    }
    LOG(INFO) << "ucp create worker pool success";
    return Status::OK();
}

Status UcpManager::RegisterSegment(const uint64_t &segAddress, const uint64_t &segSize)
{
    UcpSegmentMap::ConstAccessor constAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, constAccessor));
    return Status::OK();
}

Status UcpManager::GetOrRegisterSegment(const uint64_t &segAddress, const uint64_t &segSize,
                                        UcpSegmentMap::ConstAccessor &constAccessor)
{
    std::shared_lock<std::shared_timed_mutex> l(localMapMutex_);
    if (!localSegmentMap_->Find(constAccessor, segAddress)) {
        UcpSegmentMap::Accessor accessor;
        if (localSegmentMap_->Insert(accessor, segAddress)) {
            UcpSegment segment(segAddress, segSize, ucpContext_);
            PerfPoint point(PerfKey::RDMA_REGISTER_SEGMENT);
            Status status = segment.Init();
            point.Record();
            if (!status.IsOk()) {
                localSegmentMap_->BlockingErase(accessor);
                std::string detailed_msg =
                    FormatString("Failed to register segment, address %llu, size %llu. Underlying error: %s",
                                 segAddress, segSize, status.ToString().c_str());
                return Status(K_RUNTIME_ERROR, detailed_msg);
            }
            accessor.entry->data = std::move(segment);
        }
        accessor.Release();
        CHECK_FAIL_RETURN_STATUS(localSegmentMap_->Find(constAccessor, segAddress), K_RUNTIME_ERROR,
                                 "Failed to operate on local segment map.");
    }
    return Status::OK();
}

Status UcpManager::FillUcpInfoImpl(uint64_t segAddress, uint64_t dataOffset, const std::string &srcIpAddr,
                                   UcpRemoteInfoPb &ucpInfo)
{
    ucpInfo.set_remote_buf(segAddress + dataOffset);
    UcpSegmentMap::ConstAccessor constAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, 0, constAccessor));
    auto &segment = constAccessor.entry->data;
    ucpInfo.set_rkey(segment.GetPackedRkey());
    ucpInfo.set_remote_worker_addr(GetRecvWorkerAddress(srcIpAddr));
    return Status::OK();
}

std::string UcpManager::GetRecvWorkerAddress(const std::string &ipAddr)
{
    return workerPool_->GetOrSelRecvWorkerAddr(ipAddr);
}

bool UcpManager::IsUcpEnabled()
{
    return FLAGS_enable_rdma;
}

bool UcpManager::IsRegisterWholeArenaEnabled()
{
    return FLAGS_rdma_register_whole_arena;
}

Status UcpManager::UcpPutPayload(const UcpRemoteInfoPb &ucpInfo, const uint64_t &localObjectAddress,
                                 const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                 bool blocking, std::vector<uint64_t> &eventKeys)
{
    eventKeys.clear();
    const std::string &remoteWorkerAddr = ucpInfo.remote_worker_addr();
    const uint64_t &remoteBuf = ucpInfo.remote_buf();
    const std::string &rkey = ucpInfo.rkey();
    const std::string &remoteIpAddr =
        ucpInfo.remote_ip_addr().host() + ":" + std::to_string(ucpInfo.remote_ip_addr().port());
    LOG(INFO) << "UcpPutPayload to " << remoteIpAddr;
    PerfPoint point(PerfKey::RDMA_TOTAL_WRITE);
    uint64_t writtenSize = 0;
    uint64_t remainSize = readSize;
    while (remainSize > 0) {
        const uint64_t writeSize = std::min(remainSize, MAX_MSG_SIZE);
        const uint64_t key = requestId_.fetch_add(1);
        const uint64_t src = localObjectAddress + metaDataSize + readOffset + writtenSize;
        const uint64_t dst = remoteBuf + readOffset + writtenSize;
        std::shared_ptr<Event> event;
        RETURN_IF_NOT_OK(CreateEvent(key, event));
        Status status = workerPool_->Write(rkey, dst, remoteWorkerAddr, remoteIpAddr, src, writeSize, key);
        if (!status.IsOk()) {
            std::string detailed_msg = FormatString("Failed to ucp write object with key = %zu. Underlying error: %s",
                                                    key, status.ToString().c_str());
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, detailed_msg);
        }
        eventKeys.emplace_back(key);

        remainSize -= writeSize;
        writtenSize += writeSize;
    }
    point.Record();
    if (blocking) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitFastTransportEvent(eventKeys, remainingTime, errorHandler));
        eventKeys.clear();
    }
    return Status::OK();
}

Status UcpManager::UcpGatherPut(const UcpRemoteInfoPb &ucpInfo, uint64_t metaDataSize,
                                const std::vector<LocalSgeInfo> &objInfos, bool blocking,
                                std::vector<uint64_t> &eventKeys)
{
    eventKeys.clear();
    PerfPoint point(PerfKey::RDMA_GATHER_WRITE);
    const std::string &remoteWorkerAddr = ucpInfo.remote_worker_addr();
    const std::string remoteIpAddr =
        ucpInfo.remote_ip_addr().host() + ":" + std::to_string(ucpInfo.remote_ip_addr().port());
    const uint64_t remoteBase = ucpInfo.remote_buf();
    const std::string &rkey = ucpInfo.rkey();

    std::vector<IovSegment> segments;
    segments.reserve(objInfos.size());
    uint64_t currentOffset = 0;

    for (const auto &ele : objInfos) {
        const uint64_t srcBase = ele.sgeAddr + ele.metaDataSize + ele.readOffset;

        uint64_t writtenSize = 0;
        uint64_t remainSize = ele.writeSize;
        while (remainSize > 0) {
            const uint64_t writeSize = std::min(remainSize, MAX_MSG_SIZE);
            segments.emplace_back(IovSegment{ srcBase + writtenSize, writeSize });
            remainSize -= writeSize;
            writtenSize += writeSize;
        }
        currentOffset += ele.writeSize;
    }

    const uint64_t remoteBaseAddr = remoteBase - metaDataSize;
    const uint64_t key = requestId_.fetch_add(1);
    std::shared_ptr<Event> event;
    RETURN_IF_NOT_OK(CreateEvent(key, event));

    Status status = workerPool_->WriteN(rkey, remoteBaseAddr, remoteWorkerAddr, remoteIpAddr, segments, key);
    if (!status.IsOk()) {
        std::string detailed_msg = FormatString(
            "Failed to ucp gather write object with key = %zu. Underlying error: %s", key, status.ToString().c_str());
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, detailed_msg);
    }
    eventKeys.emplace_back(key);

    point.Record();
    if (blocking) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitFastTransportEvent(eventKeys, remainingTime, errorHandler));
        eventKeys.clear();
    }
    return Status::OK();
}

Status UcpManager::CheckUcpConnectionStable(const std::string &hostAddress, const std::string &instanceId)
{
    std::string oldInstanceId = "";
    {
        std::unique_lock<std::mutex> lock(instanceTableMutex_);
        if (instanceTable_.find(hostAddress) == instanceTable_.end()) {
            LOG(INFO) << "It's the first time to see receiver address " << hostAddress
                      << ", instance id: " << instanceId;
            instanceTable_[hostAddress] = instanceId;
            return Status::OK();
        }
        oldInstanceId = instanceTable_[hostAddress];
    }
    if (!instanceId.empty() && !oldInstanceId.empty() && oldInstanceId != instanceId) {
        LOG(WARNING) << "Ucp connection is stale; remove endpoints for " << hostAddress << "; reset instance id from "
                     << oldInstanceId << " to " << instanceId;
        HostPort remoteAddress;
        (void)remoteAddress.ParseString(hostAddress);
        (void)RemoveEndpoint(remoteAddress);
        std::unique_lock<std::mutex> lock(instanceTableMutex_);
        instanceTable_[hostAddress] = instanceId;
    } else {
        LOG(INFO) << "Successfully checked that ucp connection is stable";
    }
    return Status::OK();
}

void UcpManager::InsertSuccessfulEvent(uint64_t requestId)
{
    std::shared_ptr<Event> event;
    if (GetEvent(requestId, event).IsOk()) {
        event->NotifyAll();
        VLOG(1) << "[UcpEventHandler] Notifying successful request id: " << requestId;
    } else {
        LOG(ERROR) << "UcpManager::InsertSuccessfulEvent " << requestId << " not found in event map";
    }
}

void UcpManager::InsertFailedEvent(uint64_t requestId)
{
    std::shared_ptr<Event> event;
    if (GetEvent(requestId, event).IsOk()) {
        event->SetFailed();
        event->NotifyAll();
        VLOG(1) << "[UcpEventHandler] Notifying failed request id: " << requestId;
    } else {
        LOG(ERROR) << "UcpManager::InsertFailedEvent " << requestId << " not found in event map";
    }
}

Status UcpManager::RemoveEndpoint(const HostPort &remoteAddress)
{
    std::string addrStr = remoteAddress.ToString();
    {
        std::unique_lock<std::mutex> lock(instanceTableMutex_);
        if (instanceTable_.find(addrStr) != instanceTable_.end()) {
            LOG(INFO) << "removed instance id " << instanceTable_[addrStr] << " from instance table";
            instanceTable_.erase(addrStr);
        }
    }
    (void)workerPool_->RemoveByIp(remoteAddress.ToString());
    return Status::OK();
}

Status UcpManager::WaitToFinish(uint64_t requestId, int64_t timeoutMs)
{
    PerfPoint point(PerfKey::RDMA_WAIT_TO_FINISH);
    if (timeoutMs < 0) {
        RETURN_STATUS_LOG_ERROR(K_RPC_DEADLINE_EXCEEDED, FormatString("timedout waiting for request: %d", requestId_));
    }
    std::shared_ptr<Event> event;
    RETURN_IF_NOT_OK(GetEvent(requestId, event));
    // use this unique request id as key to wait
    // wait until timeout

    Raii deleteEvent([this, &requestId]() { DeleteEvent(requestId); });

    VLOG(1) << "[UcpEventHandler] Started waiting for the request id: " << requestId;
    RETURN_IF_NOT_OK(event->WaitFor(std::chrono::milliseconds(timeoutMs)));
    if (event->IsFailed()) {
        point.Record();
        return Status(K_RDMA_ERROR, FormatString("Polling failed with an error for requestId: %d", requestId));
    }
    VLOG(1) << "[UcpEventHandler] Done waiting for the request id: " << requestId;
    point.Record();
    return Status::OK();
}

Status UcpManager::GetEvent(uint64_t requestId, std::shared_ptr<Event> &event)
{
    std::shared_lock<std::shared_timed_mutex> lock(eventMapMutex_);
    EventMap::Accessor accessor;
    if (eventMap_->Find(accessor, requestId)) {
        event = accessor.entry->data;
        return Status::OK();
    }
    // Can happen if event is not yet inserted by sender thread.
    RETURN_STATUS(K_NOT_FOUND, FormatString("Request id %d doesnt exist in event map", requestId));
}

Status UcpManager::CreateEvent(uint64_t requestId, std::shared_ptr<Event> &event)
{
    LOG(INFO) << "UcpManager::CreateEvent()";
    std::shared_lock<std::shared_timed_mutex> lock(eventMapMutex_);
    EventMap::Accessor accessor;
    auto res = eventMap_->Insert(accessor, requestId);
    if (!res) {
        // If this happens that means requestId is duplicated.
        RETURN_STATUS_LOG_ERROR(K_DUPLICATED, FormatString("Request id %d already exists in event map", requestId));
    } else {
        event = std::make_shared<Event>(requestId);
        accessor.entry->data = event;
    }
    return Status::OK();
}

void UcpManager::DeleteEvent(uint64_t requestId)
{
    std::shared_lock<std::shared_timed_mutex> lock(eventMapMutex_);
    EventMap::Accessor accessor;
    if (eventMap_->Find(accessor, requestId)) {
        eventMap_->BlockingErase(accessor);
    }
}
}  // namespace datasystem
