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
#include <cstdint>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <tuple>
#include <vector>
#include <thread>

#include <re2/re2.h>

#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/object/buffer.h"
#include "datasystem/utils/status.h"

DS_DEFINE_string(remote_h2d_device_ids, "",
                 "The NPU device ids to be used for Remote H2D purposes given as a list of comma seperated values");

namespace datasystem {
bool RemoteH2DManager::enableRemoteH2D_ = false;

HostSegment::HostSegment(std::byte *data, std::uint64_t dataSize, P2pSegmentInfo segmentInfo,
                         P2pSegmentPermissions permissions)
    : data{ data }, dataSize{ dataSize }, segmentInfo{ segmentInfo }, permissions{ permissions }
{
}

HostSegment::~HostSegment()
{
}

Status HostSegment::Create(std::shared_ptr<HostSegment> &result, std::byte *data, std::size_t data_size,
                           P2pSegmentInfo segmentInfo, P2pSegmentPermissions permissions)
{
    result = std::make_shared<HostSegment>(data, data_size, segmentInfo, permissions);
    return Status::OK();
}

RemoteH2DManager &RemoteH2DManager::Instance()
{
    static RemoteH2DManager manager;
    return manager;
}

void RemoteH2DManager::SetClientRemoteH2DConfig(bool enableRemoteH2D, uint32_t devId)
{
    // Note: The parameter needs to be consistent in the same client process.
    enableRemoteH2D_ = enableRemoteH2D;
    FLAGS_remote_h2d_device_ids = std::to_string(devId);
}

bool RemoteH2DManager::IsRemoteH2DEnabled()
{
    if (!enableRemoteH2D_ && !FLAGS_remote_h2d_device_ids.empty()) {
        enableRemoteH2D_ = true;
    }
    return enableRemoteH2D_;
}

Status RemoteH2DManager::SetDeviceIdx(std::optional<int32_t> specifiedDevId)
{
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
    if (devId < 0 || devId > static_cast<int32_t>(npuCount)) {
        RETURN_STATUS(StatusCode::K_INVALID, "Invalid device id: " + std::to_string(devId) +
                                             ". Total NPU Count: " + std::to_string(npuCount));
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
    try {
        devId = stoi(FLAGS_remote_h2d_device_ids);
    } catch (const std::invalid_argument& e) {
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
            RETURN_STATUS(K_INVALID, "remote_h2d_device_ids flag has invalid form: " + devIdStr +
                                     ". Must use form \"1, 2, 3\"");
        }

        size_t start = 0;
        uint32_t npuCount;
        RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->aclrtGetDeviceCount(&npuCount));

        while (start < devIdStr.size()) {
            size_t end = devIdStr.find(',', start);
            try {
                workerDeviceIds_.push_back(std::stoi(devIdStr.substr(start, end - start)));
            } catch (const std::invalid_argument& e) {
                RETURN_STATUS(K_INVALID, "remote_h2d_device_ids flag has invalid form: " + devIdStr +
                                         ". Must use form \"1, 2, 3\"");
            } catch (const std::out_of_range& e) {
                RETURN_STATUS(K_INVALID, "remote_h2d_device_ids flag contains out of range number: " + devIdStr);
            }
            if (end == std::string::npos) {
                break;
            }
            if (workerDeviceIds_[workerDeviceIds_.size() - 1] >= int32_t(npuCount)) {
                RETURN_STATUS(K_INVALID, "remote_h2d_device_ids flag has invalid id: " +
                                         std::to_string(workerDeviceIds_[workerDeviceIds_.size() - 1]) +
                                         ". Total NPU count: " + std::to_string(npuCount));
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
    CommunicatorMap::ConstAccessor constAccessor;
    if (!communicatorMap_->Find(constAccessor, key)) {
        CommunicatorMap::Accessor accessor;
        if (communicatorMap_->Insert(accessor, key)) {
            LOG(INFO) << "RemoteH2DManager::P2PGetRootInfo get new root info for client comm id " << key;
            bool needRollback = true;
            Raii eraseRaii([this, &needRollback, &accessor]() {
                if (needRollback) {
                    communicatorMap_->BlockingErase(accessor);
                }
            });
            auto p2pComm = std::make_shared<RemoteH2DContext>();
            auto &rootInfo = p2pComm->rootInfo;
            RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->DSP2PGetRootInfo(&rootInfo));
            accessor.entry->data = p2pComm;
            needRollback = false;
        }
        auto &rootInfo = accessor.entry->data->rootInfo;
        p2pRootInfo->set_internal(std::string(std::begin(rootInfo.internal), std::end(rootInfo.internal)));
    } else {
        auto &rootInfo = constAccessor.entry->data->rootInfo;
        p2pRootInfo->set_internal(std::string(std::begin(rootInfo.internal), std::end(rootInfo.internal)));
    }
    return Status::OK();
}

Status RemoteH2DManager::HandleConnection(const std::string &key, const RemoteH2DRootInfoPb &p2pRootInfo, P2pKind kind,
                                          std::shared_ptr<RemoteH2DContext> &p2pComm, int32_t devId)
{
    // If communicator already created and initialized, then do not need to initialize again.
    // Otherwise if the initialization fails, will remove the communicator from list.
    CommunicatorMap::Accessor accessor;
    if (communicatorMap_->Insert(accessor, key)) {
        accessor.entry->data = std::make_shared<RemoteH2DContext>();
    }
    p2pComm = accessor.entry->data;
    RETURN_OK_IF_TRUE(p2pComm->initialized != RemoteH2DContext::InitState::UNINITIALIZED);

    bool needRollback = true;
    Raii eraseRaii([this, &needRollback, &key, &p2pComm]() {
        if (needRollback) {
            CommunicatorMap::Accessor accessor;
            if (communicatorMap_->Find(accessor, key)) {
                communicatorMap_->BlockingErase(accessor);
            }
            p2pComm = nullptr;
        }
    });
    LOG(INFO) << "Initialize new p2p communicator";
    auto &rootInfo = p2pComm->rootInfo;
    const std::string &rootInfoStr = p2pRootInfo.internal();
    auto ret = memcpy_s(static_cast<void *>(rootInfo.internal), HCCL_ROOT_INFO_BYTES,
                        static_cast<const void *>(rootInfoStr.c_str()), HCCL_ROOT_INFO_BYTES);
    CHECK_FAIL_RETURN_STATUS(ret == EOK, K_RUNTIME_ERROR,
                             FormatString("Copy root info failed, the memcpy_s return: %d", ret));
    // Always use ROCE for RDMA, even for data transfer between 2 NPUs on the same server.
    p2pComm->initialized = RemoteH2DContext::InitState::INITIALIZING;
    p2pComm->waitPost.Clear();
    accessor.Release();
    P2pLink link = P2P_LINK_ROCE;
    std::shared_ptr<std::function<int()>> p2pCallback = std::make_shared<std::function<int()>>();
    Status rc = acl::AclDeviceManager::Instance()->DSP2PCommInitRootInfo(&rootInfo, kind, link, &p2pComm->p2pComm,
                                                                         p2pCallback.get());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc, K_RUNTIME_ERROR, FormatString("Failed to initialize communicator"));
    if (kind == P2P_RECEIVER) {
        clientPingFunctions_.insert({key, p2pCallback});
    } else if (kind == P2P_SENDER) {
        clientDisconnectChecks_.insert({key, p2pCallback});
    }
    p2pComm->stream = std::make_shared<aclrtStream>();
    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->RtCreateStream(p2pComm->stream.get()));
    if (devId == -1) {
        RETURN_IF_NOT_OK(GetClientDeviceId(p2pComm->devId));
    } else {
        p2pComm->devId = devId;
    }
    communicatorMap_->Find(accessor, key);
    p2pComm->initialized = RemoteH2DContext::InitState::INITIALIZED;
    p2pComm->waitPost.Set();
    needRollback = false;
    return Status::OK();
}

Status RemoteH2DManager::P2PCommInitRootInfo(const std::string &key, const RemoteH2DRootInfoPb &p2pRootInfo,
                                             P2pKind kind, std::shared_ptr<RemoteH2DContext> &p2pComm, int32_t devId,
                                             std::shared_ptr<ThreadPool> threadPool)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());

    PerfPoint point(PerfKey::P2P_COMM_INIT_ROOT);

    std::shared_lock<std::shared_timed_mutex> l(communicatorMutex_);
    CommunicatorMap::ConstAccessor constAccessor;
    if (communicatorMap_->Find(constAccessor, key)) {
        // Fast path: already initialized
        p2pComm = constAccessor.entry->data;
        RETURN_OK_IF_TRUE(p2pComm->initialized != RemoteH2DContext::InitState::UNINITIALIZED);
    }

    if (threadPool) {
        p2pComm = nullptr;
        auto traceId = Trace::Instance().GetTraceID();
        l.unlock();
        threadPool->Execute([this, traceId, key, p2pRootInfo, kind, devId]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            LOG_IF_ERROR(SetDeviceIdx(devId), "SetDeviceId failed.");
            std::shared_lock<std::shared_timed_mutex> l(communicatorMutex_);
            std::shared_ptr<RemoteH2DContext> p2pComm;
            LOG_IF_ERROR(HandleConnection(key, p2pRootInfo, kind, p2pComm, devId),
                         "Create p2p communicator connection failed in thread");
        });
    } else {
        RETURN_IF_NOT_OK(HandleConnection(key, p2pRootInfo, kind, p2pComm, devId));
    }

    // Note: 2 threads cannot launch ops on the same comm at the same time, so client side is currently not thread safe.
    // multiple client processes should be fine, as they use different communicator connections.
    // Mutex is added before launching operation.
    return Status::OK();
}

Status RemoteH2DManager::RegisterHostMemory(void *data, uint64_t dataSize)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    RETURN_IF_NOT_OK(SetDeviceIdx());
    // Pin the mmaped memory so it can be registered to NPU device.
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(mlock(data, dataSize) == 0, K_RUNTIME_ERROR, "Failed to lock physical pages.");
    LOG(INFO) << "RemoteH2DManager::RegisterHostMemory mlock done";
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
                RETURN_IF_NOT_OK(
                    acl::AclDeviceManager::Instance()->DSP2PRegisterHostMem(data, dataSize, &segmentInfo, permissions));

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
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    heartbeatThread_ = std::thread(&RemoteH2DManager::ManageHeartbeats, this);
    // It could have been initialized already on client side.
    Status rc = acl::AclDeviceManager::Instance()->aclInit(nullptr);
    if (rc.IsError()) {
        LOG(INFO) << "RemoteH2DManager acl init failed. Repeated init with error code 100002 is acceptable. "
                  << rc.ToString();
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetDeviceIdx(), "RemoteH2DManager set device index failed.");
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
        std::lock_guard<std::shared_timed_mutex> l(communicatorMutex_);
        communicatorMap_.reset();
    }
    {
        std::lock_guard<std::shared_timed_mutex> l(segmentMutex_);
        hostSegmentMap_.reset();
        remoteSegmentMap_.reset();
    }
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->aclFinalize());
    return Status::OK();
}

Status RemoteH2DManager::ImportHostSegment(const RemoteHostSegmentPb &seg)
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
            P2pSegmentInfo segInfo;
            auto ret = memcpy_s(static_cast<void *>(segInfo.internal), P2P_SEGMENT_INFO_BYTES,
                                static_cast<const void *>(segInternal.c_str()), P2P_SEGMENT_INFO_BYTES);
            CHECK_FAIL_RETURN_STATUS(ret == EOK, K_RUNTIME_ERROR,
                                     FormatString("Copy segment info failed, the memcpy_s return: %d", ret));
            RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->DSP2PImportHostSegment(segInfo));
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
    p2pComm->waitPost.Wait();

    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->DSP2PScatterBatchFromRemoteHostMem(
        entries, size, p2pComm->p2pComm, *(p2pComm->stream)));
    LOG_IF_ERROR(acl::AclDeviceManager::Instance()->RtSynchronizeStreamWithTimeout(*(p2pComm->stream), 5000), "");
    return Status::OK();
}

Status RemoteH2DManager::GetDevIdForComm(const std::string &commId, int32_t &devId)
{
    PerfPoint pointImpl(PerfKey::WORKER_REMOTE_GET_DEV_ID_FAST);
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
        pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_DEV_ID_SLOW);
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
    // Fixme: actually deal with client fork, and verify that.
    LOG(ERROR) << "Note: fork happens with RemoteH2DManager initialized";
    LOG_IF_ERROR(Uninit(), "Uninitialize acl for Remote H2D at fork failed.");
    LOG_IF_ERROR(Init(), "Initialize acl for Remote H2D at fork failed.");
}

void RemoteH2DManager::ManageHeartbeats()
{
    INJECT_POINT("RH2D.ManageHeartbeats.heartbeat_interval_s", [this](int32_t interval) {
        this->heartbeatIntervalS_ = interval;
    });
    INJECT_POINT("RH2D.ManageHeartbeats.heartbeat_timeout_s", [this](int32_t timeout) {
        this->heartbeatTimeoutS_ = timeout;
    });
    while (!interrupted_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(heartbeatIntervalS_));
        // Send pings
        for (const auto& [_, pingFunc] : clientPingFunctions_) {
            if (pingFunc) {
                (*pingFunc)();
            }
        }
        // Check if any clients have been disconnected
        std::vector<std::string> disconnected;
        for (const auto& [commId, callback] : clientDisconnectChecks_) {
            if (callback && ((*callback)() > heartbeatTimeoutS_)) {
                disconnected.push_back(commId);
            }
        }
        // Clear related data for disconnected clients
        for (const std::string &commId : disconnected) {
            clientDisconnectChecks_.erase(commId);
            tbb::concurrent_hash_map<std::string, int32_t>::accessor acc;
            if (commDevIdMap_.find(acc, commId)) {
                commDevIdMap_.erase(acc);
            }
            CommunicatorMap::Accessor accessor;
            if (communicatorMap_->Find(accessor, commId)) {
                communicatorMap_->BlockingErase(accessor);
            }
        }
    }
}

RemoteH2DManager::RemoteH2DManager()
{
    LOG(INFO) << "RemoteH2DManager::Constructor start";
    int rc = pthread_atfork(nullptr, nullptr, &RemoteH2DChildAfterFork);
    if (rc != 0) {
        LOG(WARNING) << "RemoteH2DManager: Constructor failure pthread_atfork.";
    }
    communicatorMap_ = std::make_unique<CommunicatorMap>();
    hostSegmentMap_ = std::make_unique<HostSegmentMap>();
    remoteSegmentMap_ = std::make_unique<RemoteSegmentMap>();
    commId_ = GetStringUuid();
    LOG_IF_ERROR(Init(), "Initialize acl for Remote H2D failed.");
    LOG(INFO) << "RemoteH2DManager::Constructor done";
}

RemoteH2DManager::~RemoteH2DManager()
{
    LOG_IF_ERROR(Uninit(), "Uninitialize acl for Remote H2D failed.");
}

RemoteH2DContext::~RemoteH2DContext()
{
    if (p2pComm) {
        LOG_IF_ERROR(acl::AclDeviceManager::Instance()->SetDeviceIdx(devId),
                     "Failed to set device id to " + std::to_string(devId));
        LOG_IF_ERROR(acl::AclDeviceManager::Instance()->DSP2PCommDestroy(p2pComm), "Destroy P2P comm failed.");
    }
    if (stream) {
        LOG_IF_ERROR(acl::AclDeviceManager::Instance()->RtDestroyStream(*stream), "Destroy stream failed.");
    }
};

}  // namespace datasystem
