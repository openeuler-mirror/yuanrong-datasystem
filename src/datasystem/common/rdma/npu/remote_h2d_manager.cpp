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

#include "datasystem/common/rdma/npu/remote_h2d_manager.h"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <sys/mman.h>
#include <tuple>
#include <vector>
#include <thread>
#include <unistd.h>

#include "re2/re2.h"

#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/object/buffer.h"
#include "datasystem/utils/status.h"

#include "datasystem/common/rdma/npu/rh2d_transport_strategy.h"
#include "datasystem/common/rdma/npu/roce_transport.h"
#ifdef ASCEND_HIXL_AVAILABLE
#include "datasystem/common/rdma/npu/hccs_transport.h"
#endif
#include "datasystem/common/ak_sk/hasher.h"

DS_DEFINE_string(remote_h2d_device_ids, "",
                 "The NPU device ids to be used for Remote H2D purposes given as a list of comma seperated values");

namespace datasystem {
bool RemoteH2DManager::enableRemoteH2D_ = false;
int32_t RemoteH2DManager::clientDeviceId_ = -1;
std::string RemoteH2DManager::hccsLocalEndpointIp_;
bool RemoteH2DManager::isClient_ = false;

namespace {
const char *InitStateToString(RemoteH2DContext::InitState state)
{
    switch (state) {
        case RemoteH2DContext::InitState::UNINITIALIZED:
            return "UNINITIALIZED";
        case RemoteH2DContext::InitState::INITIALIZING:
            return "INITIALIZING";
        case RemoteH2DContext::InitState::INITIALIZED:
            return "INITIALIZED";
        default:
            return "UNKNOWN";
    }
}

const char *P2pKindToString(P2pKind kind)
{
    switch (kind) {
        case P2P_SENDER:
            return "SENDER";
        case P2P_RECEIVER:
            return "RECEIVER";
        default:
            return "UNKNOWN";
    }
}

std::string CommKeyForLog(const std::string &key)
{
    return FormatString("len=%zu hash=%u", key.size(), MurmurHash3_32(key));
}
}  // namespace

HostSegment::HostSegment(std::byte *data, std::uint64_t dataSize, P2pSegmentInfo segmentInfo,
                         P2pSegmentPermissions permissions)
    : data{ data }, dataSize{ dataSize }, segmentInfo{ segmentInfo }, permissions{ permissions }
{
}

Status HostSegment::Create(std::shared_ptr<HostSegment> &result, std::byte *data, std::size_t dataSize,
                           P2pSegmentInfo segmentInfo, P2pSegmentPermissions permissions)
{
    result = std::make_shared<HostSegment>(data, dataSize, segmentInfo, permissions);
    return Status::OK();
}

RemoteH2DManager &RemoteH2DManager::Instance()
{
    static RemoteH2DManager manager;
    return manager;
}

Status RemoteH2DManager::SetClientRemoteH2DConfig(bool enableRemoteH2D, uint32_t devId, const std::string &localIp)
{
    // Note: The parameter needs to be consistent in the same client process.
    LOG(INFO) << "[RH2D] Client config: enable=" << enableRemoteH2D << " devId=" << devId << " localIp=" << localIp;
    isClient_ = true;
    enableRemoteH2D_ = enableRemoteH2D;
    clientDeviceId_ = static_cast<int32_t>(devId);
    std::string errMsg;
    const char *envLinkType = std::getenv("DS_RH2D_LINK_TYPE");
    if (envLinkType && strlen(envLinkType) > 0) {
        if (!SetCommandLineOption("remote_h2d_link_type", envLinkType, errMsg)) {
            RETURN_STATUS_LOG_ERROR(K_INVALID, FormatString("Invalid DS_RH2D_LINK_TYPE: %s", errMsg));
        }
        LOG(INFO) << "[RH2D] link_type overridden by DS_RH2D_LINK_TYPE=" << envLinkType;
    }
    if (FLAGS_remote_h2d_link_type == "HCCS") {
        const char *envBufferPool = std::getenv("DS_RH2D_HCCS_BUFFER_POOL");
        if (envBufferPool && strlen(envBufferPool) > 0) {
            if (!SetCommandLineOption("remote_h2d_hccs_buffer_pool", envBufferPool, errMsg)) {
                RETURN_STATUS_LOG_ERROR(K_INVALID, FormatString("Invalid DS_RH2D_HCCS_BUFFER_POOL: %s", errMsg));
            }
            LOG(INFO) << "[RH2D] hccs_buffer_pool overridden by DS_RH2D_HCCS_BUFFER_POOL=" << envBufferPool;
        }
        // Configure the HCCS local IP for this client process.
        RETURN_IF_NOT_OK(SetRH2DLocalEndpointIp(localIp));
    }
    return Status::OK();
}

bool RemoteH2DManager::IsRemoteH2DEnabled()
{
    return enableRemoteH2D_ || !FLAGS_remote_h2d_device_ids.empty();
}

Status RemoteH2DManager::SetDeviceIdx(std::optional<int32_t> specifiedDevId)
{
    VLOG(1) << "RemoteH2DManager::SetDeviceIdx enter, specified="
            << (specifiedDevId ? std::to_string(*specifiedDevId) : "<none>");
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());

    PerfPoint point(PerfKey::P2P_SET_DEV_IDX);
    int32_t devId;

    // Assume one process works with only one device index.
    if (!specifiedDevId || *specifiedDevId < 0) {
        RETURN_IF_NOT_OK(GetClientDeviceId(devId));
    } else {
        devId = *specifiedDevId;
    }

    uint32_t npuCount;
    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->aclrtGetDeviceCount(&npuCount));
    if (devId < 0 || devId >= static_cast<int32_t>(npuCount)) {
        RETURN_STATUS(StatusCode::K_INVALID,
                      "Invalid device id: " + std::to_string(devId) + ". Total NPU Count: " + std::to_string(npuCount));
    }

    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->SetDeviceIdx(devId));
    LOG(INFO) << "RemoteH2DManager::SetDeviceIdx set to " << devId;
    return Status::OK();
}

Status RemoteH2DManager::GetClientCommUuid(std::string &commId)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    // Assume client works with one device id, so only one uuid to identify the client process.
    commId = commId_;
    return Status::OK();
}

Status RemoteH2DManager::GetClientDeviceId(int32_t &devId)
{
    if (clientDeviceId_ >= 0) {
        devId = clientDeviceId_;
        return Status::OK();
    }
    try {
        devId = stoi(FLAGS_remote_h2d_device_ids);
    } catch (const std::invalid_argument &e) {
        RETURN_STATUS(StatusCode::K_INVALID, "Invalid RemoteH2D dev ids set: " + FLAGS_remote_h2d_device_ids);
    } catch (const std::out_of_range &e) {
        RETURN_STATUS(StatusCode::K_INVALID, "RemoteH2D dev id out of range: " + FLAGS_remote_h2d_device_ids);
    }
    return Status::OK();
}

Status RemoteH2DManager::GetWorkerDeviceIds(std::vector<int32_t> *devIds)
{
    // Parse gflag for worker device ids if empty
    if (workerDeviceIds_.empty()) {
        std::string devIdStr = FLAGS_remote_h2d_device_ids;
        RE2 pattern("^\\d+(,\\s*\\d+)*$");
        if (!RE2::FullMatch(devIdStr, pattern)) {
            RETURN_STATUS(K_INVALID,
                          "remote_h2d_device_ids flag has invalid form: " + devIdStr + ". Must use form \"1, 2, 3\"");
        }

        size_t start = 0;
        uint32_t npuCount;
        RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->aclrtGetDeviceCount(&npuCount));

        while (start < devIdStr.size()) {
            size_t end = devIdStr.find(',', start);
            try {
                workerDeviceIds_.push_back(std::stoi(devIdStr.substr(start, end - start)));
            } catch (const std::invalid_argument &e) {
                RETURN_STATUS(K_INVALID, "remote_h2d_device_ids flag has invalid form: " + devIdStr
                                             + ". Must use form \"1, 2, 3\"");
            } catch (const std::out_of_range &e) {
                RETURN_STATUS(K_INVALID, "remote_h2d_device_ids flag contains out of range number: " + devIdStr);
            }
            if (end == std::string::npos) {
                break;
            }
            if (workerDeviceIds_[workerDeviceIds_.size() - 1] >= int32_t(npuCount)) {
                RETURN_STATUS(K_INVALID, "remote_h2d_device_ids flag has invalid id: "
                                             + std::to_string(workerDeviceIds_[workerDeviceIds_.size() - 1])
                                             + ". Total NPU count: " + std::to_string(npuCount));
            }
            start = end + 1;
        }
    }
    if (devIds != nullptr) {
        *devIds = workerDeviceIds_;
    }
    return Status::OK();
}

Status RemoteH2DManager::P2PGetRootInfo(const std::string &key, RemoteH2DRootInfoPb *p2pRootInfo)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());

    PerfPoint point(PerfKey::P2P_GET_ROOT_INFO);
    std::shared_lock<std::shared_timed_mutex> l(communicatorMutex_);

    CommunicatorMap::ConstAccessor constAcc;
    if (communicatorMap_->Find(constAcc, key)) {
        p2pRootInfo->set_internal(constAcc.entry->data->localIdentity);
        return Status::OK();
    }

    CommunicatorMap::Accessor accessor;
    if (!communicatorMap_->Insert(accessor, key)) {
        // Insert() already returns an accessor locked on the existing entry when another thread won the race.
        // Re-finding the same key here would try to lock the same entry mutex again in the same thread.
        p2pRootInfo->set_internal(accessor.entry->data->localIdentity);
        return Status::OK();
    }

    LOG(INFO) << "RemoteH2DManager::P2PGetRootInfo create new context, key(" << CommKeyForLog(key) << ")";
    bool needRollback = true;
    Raii eraseRaii([this, &needRollback, &accessor]() {
        if (needRollback) {
            communicatorMap_->BlockingErase(accessor);
        }
    });

    auto ctx = std::make_shared<RemoteH2DContext>();
    std::string identity;
    RETURN_IF_NOT_OK(transport_->GetConnectionIdentity(&identity));
    ctx->localIdentity = identity;
    accessor.entry->data = ctx;
    needRollback = false;

    p2pRootInfo->set_internal(ctx->localIdentity);
    return Status::OK();
}

Status RemoteH2DManager::CompleteConnectionInit(const std::string &key, P2pKind kind,
                                                std::shared_ptr<RemoteH2DContext> &p2pComm, int32_t devId)
{
    std::shared_lock<std::shared_timed_mutex> l(communicatorMutex_);
    CommunicatorMap::Accessor accessor;
    CHECK_FAIL_RETURN_STATUS(communicatorMap_->Find(accessor, key), K_NOT_FOUND,
                             "Communicator context not found for key");
    p2pComm = accessor.entry->data;
    auto state = p2pComm->initialized.load(std::memory_order_acquire);
    if (state == RemoteH2DContext::InitState::INITIALIZED) {
        LOG_EVERY_N(INFO, 200) << "RemoteH2D communicator already initialized before async run, key("
                               << CommKeyForLog(key) << "), kind=" << P2pKindToString(kind) << ", devId=" << devId;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(
        state == RemoteH2DContext::InitState::INITIALIZING, K_RUNTIME_ERROR,
        FormatString("Communicator context has unexpected init state: %s", InitStateToString(state)));

    bool needRollback = true;
    bool needDisconnect = false;
    std::string remoteIdentity;
    Status rollbackStatus = Status(K_RUNTIME_ERROR, "RemoteH2D communicator init failed");
    Raii eraseRaii([this, &needRollback, &needDisconnect, &remoteIdentity, &key, &p2pComm, &rollbackStatus, kind,
                    devId]() {
        if (needRollback) {
            auto failedComm = p2pComm;
            LOG(WARNING) << "RemoteH2D comm init rollback, key(" << CommKeyForLog(key)
                         << "), kind=" << P2pKindToString(kind) << ", devId=" << devId;
            if (needDisconnect && transport_ != nullptr) {
                LOG_IF_ERROR(transport_->Disconnect(remoteIdentity), "RemoteH2D transport disconnect rollback failed");
            }
            {
                std::lock_guard<std::mutex> hbLock(heartbeatMutex_);
                clientPingFunctions_.erase(key);
                clientDisconnectChecks_.erase(key);
            }
            CommunicatorMap::Accessor accessor;
            if (communicatorMap_->Find(accessor, key)) {
                communicatorMap_->BlockingErase(accessor);
            }
            if (failedComm != nullptr) {
                failedComm->initialized.store(RemoteH2DContext::InitState::UNINITIALIZED, std::memory_order_release);
                failedComm->waitPost.SetWithStatus(rollbackStatus);
            }
            p2pComm = nullptr;
        }
    });
    LOG(INFO) << "RemoteH2D start communicator init, key(" << CommKeyForLog(key) << "), kind=" << P2pKindToString(kind)
              << ", devId=" << devId;
    remoteIdentity = p2pComm->remoteEndpoint;
    accessor.Release();
    auto p2pCallback = std::make_shared<std::function<int()>>();
    Status rc = transport_->Connect(remoteIdentity, kind, p2pCallback.get());
    if (rc.IsError()) {
        rollbackStatus = Status(K_RUNTIME_ERROR, FormatString("Failed to initialize communicator: %s", rc.ToString()));
        return rollbackStatus;
    }
    needDisconnect = true;
    p2pComm->linkType = transport_->LinkType();
    if (p2pComm->linkType == P2P_LINK_ROCE) {
        std::lock_guard<std::mutex> hbLock(heartbeatMutex_);
        if (kind == P2P_RECEIVER) {
            clientPingFunctions_.insert({ key, p2pCallback });
        } else if (kind == P2P_SENDER) {
            clientDisconnectChecks_.insert({ key, p2pCallback });
        }
    }

    if (transport_->LinkType() == P2P_LINK_ROCE) {
        p2pComm->stream = std::make_shared<aclrtStream>();
        rc = acl::AclDeviceManager::Instance()->RtCreateStream(p2pComm->stream.get());
        if (rc.IsError()) {
            rollbackStatus = rc;
            return rc;
        }
    }

    if (devId == -1) {
        rc = GetClientDeviceId(p2pComm->devId);
        if (rc.IsError()) {
            rollbackStatus = rc;
            return rc;
        }
    } else {
        p2pComm->devId = devId;
    }

    p2pComm->initialized.store(RemoteH2DContext::InitState::INITIALIZED, std::memory_order_release);
    p2pComm->waitPost.Set();
    needRollback = false;
    LOG(INFO) << "RemoteH2D communicator init done, key(" << CommKeyForLog(key) << "), kind=" << P2pKindToString(kind)
              << ", devId=" << p2pComm->devId;
    return Status::OK();
}

void RemoteH2DManager::SubmitConnectionInitTask(const std::string &key, P2pKind kind, int32_t devId,
                                                const std::shared_ptr<ThreadPool> &threadPool)
{
    auto traceId = Trace::Instance().GetTraceID();
    LOG(INFO) << "RemoteH2D enqueue async comm init, key(" << CommKeyForLog(key) << "), kind=" << P2pKindToString(kind)
              << ", devId=" << devId;
    threadPool->Execute([this, traceId, key, kind, devId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        LOG_EVERY_N(INFO, 200) << "RemoteH2D async comm init worker start, key(" << CommKeyForLog(key)
                               << "), kind=" << P2pKindToString(kind) << ", devId=" << devId;
        LOG_IF_ERROR(SetDeviceIdx(devId), "SetDeviceId failed.");
        std::shared_ptr<RemoteH2DContext> p2pComm;
        auto rc = CompleteConnectionInit(key, kind, p2pComm, devId);
        if (rc.IsError() && p2pComm != nullptr) {
            p2pComm->waitPost.SetWithStatus(rc);
        }
        LOG_IF_ERROR(rc, "Create p2p communicator connection failed in thread");
    });
}

Status RemoteH2DManager::P2PCommInitRootInfo(const std::string &key, const RemoteH2DRootInfoPb &p2pRootInfo,
                                             P2pKind kind, std::shared_ptr<RemoteH2DContext> &p2pComm, int32_t devId,
                                             std::shared_ptr<ThreadPool> threadPool)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());

    PerfPoint point(PerfKey::P2P_COMM_INIT_ROOT);

    std::shared_lock<std::shared_timed_mutex> l(communicatorMutex_);
    CommunicatorMap::Accessor accessor;
    bool inserted = communicatorMap_->Insert(accessor, key);
    if (inserted) {
        accessor.entry->data = std::make_shared<RemoteH2DContext>();
        LOG(INFO) << "RemoteH2D comm reserve new context before init, key(" << CommKeyForLog(key)
                  << "), kind=" << P2pKindToString(kind) << ", devId=" << devId;
    }
    p2pComm = accessor.entry->data;
    auto state = p2pComm->initialized.load();
    if (state != RemoteH2DContext::InitState::UNINITIALIZED) {
        LOG_EVERY_N(INFO, 500) << "RemoteH2D comm init cache hit, key(" << CommKeyForLog(key) << "), state="
                               << InitStateToString(state) << ", kind=" << P2pKindToString(kind)
                               << ", devId=" << devId;
        return Status::OK();
    }

    p2pComm->remoteEndpoint = p2pRootInfo.internal();
    CHECK_FAIL_RETURN_STATUS(!p2pComm->remoteEndpoint.empty(), K_INVALID, "RemoteH2D remote endpoint is empty");
    p2pComm->initialized.store(RemoteH2DContext::InitState::INITIALIZING, std::memory_order_release);
    p2pComm->waitPost.Clear();
    accessor.Release();

    INJECT_POINT("RemoteH2DManager.P2PCommInitRootInfo.delay");
    if (threadPool) {
        l.unlock();
        SubmitConnectionInitTask(key, kind, devId, threadPool);
    } else {
        LOG(INFO) << "RemoteH2D do sync comm init, key(" << CommKeyForLog(key) << "), kind=" << P2pKindToString(kind)
                  << ", devId=" << devId;
        RETURN_IF_NOT_OK(CompleteConnectionInit(key, kind, p2pComm, devId));
    }

    // Note: 2 threads cannot launch ops on the same comm at the same time, so client side is currently not thread safe.
    // multiple client processes should be fine, as they use different communicator connections.
    // Mutex is added before launching operation.
    return Status::OK();
}

Status RemoteH2DManager::RegisterHostMemory(void *data, uint64_t dataSize)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    LOG(INFO) << "RemoteH2DManager::RegisterHostMemory addr=" << data << " size=" << dataSize
              << " link_type=" << FLAGS_remote_h2d_link_type;

    // mlock is needed only for RoCE: DSP2PRegisterHostMem DMAs from these pages so they must be resident.
    // HCCS bufferpool mode doesn't DMA from the worker's host buffer.
    if (FLAGS_remote_h2d_link_type != "HCCS") {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(mlock(data, dataSize) == 0, K_RUNTIME_ERROR,
                                             "Failed to lock physical pages.");
        LOG(INFO) << "RemoteH2DManager::RegisterHostMemory mlock done";
    } else {
        LOG(INFO) << "RemoteH2DManager::RegisterHostMemory skipping mlock for HCCS link type";
    }

    RETURN_IF_NOT_OK(GetWorkerDeviceIds());

    std::vector<std::future<Status>> futures;
    auto pool = std::make_unique<ThreadPool>(workerDeviceIds_.size(), workerDeviceIds_.size(), "", false, 10 * 1000);

    // Register Host Memory on all allowed devices
    for (const int32_t &devId : workerDeviceIds_) {
        futures.emplace_back(pool->Submit([this, devId, data, dataSize] {
            RETURN_IF_NOT_OK(SetDeviceIdx(devId));

            uint64_t castedData = reinterpret_cast<uint64_t>(data);
            std::shared_lock<std::shared_timed_mutex> l(segmentMutex_);
            HostSegmentMap::Accessor accessor;

            // Insert devId mapping if not yet inserted
            if (!hostSegmentMap_->Find(accessor, devId)) {
                RETURN_OK_IF_TRUE(!hostSegmentMap_->Insert(accessor, devId));
            }

            if (accessor.entry->data.find(castedData) == accessor.entry->data.end()) {
                bool needRollback = true;
                Raii eraseRaii([this, &needRollback, &accessor]() {
                    if (needRollback) {
                        hostSegmentMap_->BlockingErase(accessor);
                    }
                });
                P2pSegmentInfo segmentInfo;
                P2pSegmentPermissions permissions = P2pSegmentPermissions::P2P_SEGMENT_READ_WRITE;
                RETURN_IF_NOT_OK(transport_->RegisterMemory(data, dataSize, &segmentInfo));

                // Create Host Segment
                std::shared_ptr<HostSegment> hostSegment;
                HostSegment::Create(hostSegment, reinterpret_cast<std::byte *>(data), dataSize, segmentInfo,
                                    permissions);
                accessor.entry->data[castedData] = hostSegment;
                needRollback = false;
            }

            return Status::OK();
        }));
    }
    for (auto &future : futures) {
        if (!future.get().IsOk()) {
            LOG(ERROR) << "RegisterHostMemory failed";
        }
    }
    return Status::OK();
}

Status RemoteH2DManager::FillSegmentInfo(uint64_t segLen, uint64_t segDataOffset, uint64_t key,
                                         RemoteHostSegmentPb &segmentPb, int32_t devId)
{
    PerfPoint point(PerfKey::P2P_FILL_SEG_INFO);

    HostSegmentMap::Accessor accessor;
    if (hostSegmentMap_->Find(accessor, devId)) {
        auto &segInfo = accessor.entry->data[key]->segmentInfo;
        segmentPb.set_name(std::string(std::begin(segInfo.internal), std::end(segInfo.internal)));
        segmentPb.set_seg_va(key);
        segmentPb.set_seg_len(segLen);
        segmentPb.set_seg_data_offset(segDataOffset);
        return Status::OK();
    }
    return Status(K_NOT_FOUND, FormatString("Cannot find segment for devId %d at address %zu", devId, key));
}

Status RemoteH2DManager::FillDataInfo(uint64_t *dataPtr, RemoteH2DDataInfoPb &dataInfoPb)
{
    PerfPoint point(PerfKey::P2P_FILL_DATA_INFO);

    uint64_t sz = *dataPtr;

    // Get the first offset
    dataPtr++;
    uint64_t firstOffset = *dataPtr;
    dataInfoPb.set_offset(firstOffset);

    // Calculate sizes of data
    for (uint64_t i = 0; i < sz; i++) {
        uint64_t offsetX = *dataPtr;
        dataPtr++;
        uint64_t offsetY = *dataPtr;
        dataInfoPb.add_sizes(offsetY - offsetX);
    }
    return Status::OK();
}

Status RemoteH2DManager::Init()
{
    LOG(INFO) << "RemoteH2DManager::Init enter, enabled=" << IsRemoteH2DEnabled()
              << " device_ids=" << FLAGS_remote_h2d_device_ids;
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());

    LOG(INFO) << "RemoteH2DManager::Init starting heartbeat thread";
    heartbeatThread_ = std::thread(&RemoteH2DManager::ManageHeartbeats, this);

    LOG(INFO) << "RemoteH2DManager::Init calling AclDeviceManager::aclInit";
    Status rc = acl::AclDeviceManager::Instance()->aclInit(nullptr);
    if (rc.IsError()) {
        LOG(INFO) << "RemoteH2DManager acl init failed. Repeated init with error code 100002 is acceptable. "
                  << rc.ToString();
    }
    LOG(INFO) << "RemoteH2DManager::Init aclInit returned";

    // Parse worker device IDs before initializing the transport, so that HCCS
    // can create one HIXL engine per device for multi-device load balancing.
    std::vector<int32_t> workerDevIdsForInit;
    if (!isClient_) {
        RETURN_IF_NOT_OK(GetWorkerDeviceIds(&workerDevIdsForInit));
    } else {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetDeviceIdx(), "RemoteH2DManager set device index failed.");
    }

    LOG(INFO) << "RemoteH2DManager::Init calling transport_->Init() with " << workerDevIdsForInit.size() << " devId(s)";
    RETURN_IF_NOT_OK(transport_->Init(workerDevIdsForInit));
    LOG(INFO) << "RemoteH2DManager::Init done";
    return Status::OK();
}

Status RemoteH2DManager::Uninit()
{
    interrupted_.store(true);
    if (heartbeatThread_.joinable()) {
        heartbeatThread_.join();
    }

    LOG_IF_ERROR(SetDeviceIdx(), "Set device index failed at RemoteH2DManager uninitialization");

    {
        std::lock_guard<std::shared_timed_mutex> l(segmentMutex_);
        hostSegmentMap_.reset();
        remoteSegmentMap_.reset();
    }

    {
        std::lock_guard<std::shared_timed_mutex> l(communicatorMutex_);
        communicatorMap_.reset();
    }

    commDevIdMap_.clear();

    if (transport_) {
        LOG_IF_ERROR(transport_->DisconnectAll(),
                     "Failed to disconnect all transport connections at RemoteH2DManager uninitialization");
    }

    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->aclFinalize());
    return Status::OK();
}

Status RemoteH2DManager::ImportHostSegment(const std::string &remoteEndpoint, const RemoteHostSegmentPb &seg)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());

    PerfPoint point(PerfKey::P2P_IMPORT_HOST_SEG);

    std::shared_lock<std::shared_timed_mutex> l(segmentMutex_);
    const std::string &segInternal = seg.name();
    RemoteSegmentMap::Accessor accessor;
    if (!remoteSegmentMap_->Find(accessor, segInternal)) {
        if (remoteSegmentMap_->Insert(accessor, segInternal)) {
            bool needRollback = true;
            Raii eraseRaii([this, &needRollback, &accessor]() {
                if (needRollback) {
                    remoteSegmentMap_->BlockingErase(accessor);
                }
            });
            RETURN_IF_NOT_OK(transport_->ImportRemoteAddressInfo(remoteEndpoint, seg));
            // Only for record purposes so that remote segment is not imported repeatedly,
            // the actual segment content does not need to be kept.
            accessor.entry->data = nullptr;
            needRollback = false;
        }
    }
    return Status::OK();
}

Status RemoteH2DManager::ScatterBatch(P2pScatterEntry *entries, uint32_t size,
                                      std::shared_ptr<RemoteH2DContext> p2pComm)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());

    PerfPoint point(PerfKey::P2P_SCATTER_BATCH);

    // Thread safety needs to be ensured for the p2p communicator.
    std::lock_guard<std::mutex> lock(p2pComm->mutex);
    PerfPoint waitPoint(PerfKey::P2P_COMM_INIT_WAIT_READY);
    RETURN_IF_NOT_OK(p2pComm->waitPost.WaitAndGetStatus());
    waitPoint.Record();

    RETURN_IF_NOT_OK(transport_->ScatterBatch(entries, size, p2pComm->remoteEndpoint, p2pComm->stream));
    return Status::OK();
}

Status RemoteH2DManager::GetDevIdForComm(const std::string &commId, int32_t &devId)
{
    devId = -1;
    {
        // fast path: use const accessor
        tbb::concurrent_hash_map<std::string, int32_t>::const_accessor cacc;
        if (commDevIdMap_.find(cacc, commId)) {
            devId = cacc->second;
        }
    }
    if (devId == -1) {
        // slow path
        std::vector<int32_t> devIds;
        RETURN_IF_NOT_OK(GetWorkerDeviceIds(&devIds));
        tbb::concurrent_hash_map<std::string, int32_t>::accessor acc;
        commDevIdMap_.insert(acc, commId);
        acc->second = devIds[nextDevIdIndex_.fetch_add(1, std::memory_order_relaxed) % devIds.size()];
        devId = acc->second;
    }
    return Status::OK();
}

void RemoteH2DChildAfterFork()
{
    RemoteH2DManager::Instance().AfterFork();
}

void RemoteH2DManager::AfterFork()
{
    LOG(ERROR) << "Note: fork happens with RemoteH2DManager initialized";
    LOG_IF_ERROR(Uninit(), "Uninitialize acl for Remote H2D at fork failed.");
    LOG_IF_ERROR(Init(), "Initialize acl for Remote H2D at fork failed.");
}

Status RemoteH2DManager::SetRH2DLocalEndpointIp(const std::string &localIp)
{
    if (FLAGS_remote_h2d_link_type == "HCCS") {
        if (localIp.empty()) {
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "SetRH2DLocalEndpointIp: local IP is empty.");
        }
        hccsLocalEndpointIp_ = localIp;
        LOG(INFO) << "[HCCS] Local endpoint IP configured: " << hccsLocalEndpointIp_;
    }
    return Status::OK();
}

std::unique_ptr<RH2DTransportStrategy> RemoteH2DManager::CreateTransport()
{
    const std::string &type = FLAGS_remote_h2d_link_type;
    if (type == "ROCE") {
        return std::make_unique<RoCETransport>();
    }
    if (type == "HCCS") {
#ifdef ASCEND_HIXL_AVAILABLE
        auto transport = std::make_unique<HCCSTransport>();
        if (!hccsLocalEndpointIp_.empty()) {
            transport->SetLocalEndpoint(hccsLocalEndpointIp_, isClient_);
        }
        return transport;
#else
        LOG(FATAL) << "remote_h2d_link_type=HCCS requested but datasystem was built without HIXL (cann_hixl) support";
        return nullptr;
#endif
    }
    LOG(FATAL) << "Unknown remote_h2d_link_type: " << type;
    return nullptr;
}

void RemoteH2DManager::ManageHeartbeats()
{
    INJECT_POINT("RH2D.ManageHeartbeats.heartbeat_interval_s",
                 [this](int32_t interval) { this->heartbeatIntervalS_ = interval; });
    INJECT_POINT("RH2D.ManageHeartbeats.heartbeat_timeout_s",
                 [this](int32_t timeout) { this->heartbeatTimeoutS_ = timeout; });
    while (!interrupted_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(heartbeatIntervalS_));
        DispatchPings();
        std::vector<std::string> disconnected = CheckDisconnectedClients();
        CleanupDisconnectedClients(disconnected);
    }
}

void RemoteH2DManager::DispatchPings()
{
    std::vector<std::shared_ptr<std::function<int()>>> pingFuncs;
    {
        std::lock_guard<std::mutex> lock(heartbeatMutex_);
        for (const auto &kv : clientPingFunctions_) {
            if (kv.second) {
                pingFuncs.push_back(kv.second);
            }
        }
    }
    for (auto &func : pingFuncs) {
        if (func) {
            (*func)();
        }
    }
}

std::vector<std::string> RemoteH2DManager::CheckDisconnectedClients()
{
    std::vector<std::pair<std::string, std::shared_ptr<std::function<int()>>>> disconnectChecks;
    {
        std::lock_guard<std::mutex> lock(heartbeatMutex_);
        for (const auto &kv : clientDisconnectChecks_) {
            if (kv.second) {
                disconnectChecks.emplace_back(kv.first, kv.second);
            }
        }
    }
    std::vector<std::string> disconnected;
    for (const auto &kv : disconnectChecks) {
        if (kv.second && ((*(kv.second))() > heartbeatTimeoutS_)) {
            disconnected.push_back(kv.first);
        }
    }
    return disconnected;
}

void RemoteH2DManager::CleanupDisconnectedClients(const std::vector<std::string> &disconnected)
{
    if (disconnected.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> hbLock(heartbeatMutex_);
        for (const std::string &commId : disconnected) {
            clientDisconnectChecks_.erase(commId);
        }
    }
    for (const std::string &commId : disconnected) {
        tbb::concurrent_hash_map<std::string, int32_t>::accessor acc;
        if (commDevIdMap_.find(acc, commId)) {
            commDevIdMap_.erase(acc);
        }
        CommunicatorMap::Accessor accessor;
        if (communicatorMap_->Find(accessor, commId)) {
            std::string remoteEndpoint = accessor.entry->data->remoteEndpoint;
            communicatorMap_->BlockingErase(accessor);
            if (transport_) {
                LOG_IF_ERROR(transport_->Disconnect(remoteEndpoint),
                             "Transport disconnect failed for evicted peer");
            }
        }
    }
}

RemoteH2DManager::RemoteH2DManager() : transport_(CreateTransport())
{
    LOG(INFO) << "RemoteH2DManager::Constructor start, link_type=" << FLAGS_remote_h2d_link_type;
    int rc = pthread_atfork(nullptr, nullptr, &RemoteH2DChildAfterFork);
    if (rc != 0) {
        LOG(WARNING) << "RemoteH2DManager: Constructor failure pthread_atfork.";
    }
    communicatorMap_ = std::make_unique<CommunicatorMap>();
    hostSegmentMap_ = std::make_unique<HostSegmentMap>();
    remoteSegmentMap_ = std::make_unique<RemoteSegmentMap>();
    commId_ = GetStringUuid();
    LOG(INFO) << "RemoteH2DManager::Constructor maps allocated, commId=" << commId_ << ", calling Init()";
    LOG_IF_ERROR(Init(), "Initialize acl for Remote H2D failed.");
    LOG(INFO) << "RemoteH2DManager::Constructor done";
}

RemoteH2DManager::~RemoteH2DManager()
{
    LOG_IF_ERROR(Uninit(), "Uninitialize acl for Remote H2D failed.");
}

RemoteH2DContext::~RemoteH2DContext()
{
    if (stream) {
        LOG_IF_ERROR(acl::AclDeviceManager::Instance()->SetDeviceIdx(devId),
                     "Failed to set device id to " + std::to_string(devId));
        LOG_IF_ERROR(acl::AclDeviceManager::Instance()->RtDestroyStream(*stream), "Destroy stream failed.");
    }
}

}  // namespace datasystem
