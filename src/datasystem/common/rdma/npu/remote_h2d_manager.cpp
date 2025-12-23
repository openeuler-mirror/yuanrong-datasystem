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

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/object/buffer.h"
#include "datasystem/utils/status.h"

DS_DEFINE_uint32(remote_h2d_device_id, 3, "The npu device id for remote h2d purposes, default to 2.");

namespace datasystem {
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
    FLAGS_enable_remote_h2d = enableRemoteH2D;
    FLAGS_remote_h2d_device_id = devId;
}

bool RemoteH2DManager::IsRemoteH2DEnabled()
{
    return FLAGS_enable_remote_h2d;
}

Status RemoteH2DManager::SetDeviceIdx()
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    // Assume one process works with only one device index.
    auto devId = FLAGS_remote_h2d_device_id;
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

Status RemoteH2DManager::P2PGetRootInfo(const std::string &key, RemoteH2DRootInfoPb *p2pRootInfo)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    RETURN_IF_NOT_OK(SetDeviceIdx());
    {
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
    }
    return Status::OK();
}

Status RemoteH2DManager::P2PCommInitRootInfo(const std::string &key, const RemoteH2DRootInfoPb &p2pRootInfo,
                                             P2pKind kind, std::shared_ptr<RemoteH2DContext> &p2pComm)
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
    RETURN_IF_NOT_OK(SetDeviceIdx());
    std::shared_lock<std::shared_timed_mutex> l(communicatorMutex_);

    PerfPoint point(PerfKey::P2P_COMM_INIT_ROOT);
    // If communicator already created and initialized, then do not need to initialize again.
    // Otherwise if the initialization fails, will remove the communicator from list.
    CommunicatorMap::Accessor accessor;
    if (!communicatorMap_->Find(accessor, key)) {
        if (communicatorMap_->Insert(accessor, key)) {
            accessor.entry->data = std::make_shared<RemoteH2DContext>();
        }
    }
    p2pComm = accessor.entry->data;
    if (p2pComm->initialized == RemoteH2DContext::UNINITIALIZED) {
        bool needRollback = true;
        Raii eraseRaii([this, &needRollback, &accessor]() {
            if (needRollback) {
                communicatorMap_->BlockingErase(accessor);
            }
        });
        LOG(INFO) << "Initialize new p2p communicator";
        auto &rootInfo = p2pComm->rootInfo;
        const std::string &rooInfoStr = p2pRootInfo.internal();
        auto ret = memcpy_s(static_cast<void *>(rootInfo.internal), HCCL_ROOT_INFO_BYTES,
                            static_cast<const void *>(rooInfoStr.c_str()), HCCL_ROOT_INFO_BYTES);
        CHECK_FAIL_RETURN_STATUS(ret == EOK, K_RUNTIME_ERROR,
                                 FormatString("Copy root info failed, the memcpy_s return: %d", ret));
        // Always use ROCE for RDMA, even for data transfer between 2 NPUs on the same server.
        p2pComm->initialized = RemoteH2DContext::InitState::INITIALIZING;
        p2pComm->waitPost.Clear();
        accessor.Release();
        P2pLink link = P2P_LINK_ROCE;
        Status rc = acl::AclDeviceManager::Instance()->DSP2PCommInitRootInfo(&rootInfo, kind, link, &p2pComm->p2pComm);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc, K_RUNTIME_ERROR, FormatString("Failed to initialize communicator"));
        p2pComm->stream = std::make_shared<aclrtStream>();
        RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->RtCreateStream(p2pComm->stream.get()));
        communicatorMap_->Find(accessor, key);
        p2pComm->initialized = RemoteH2DContext::InitState::INITIALIZED;
        p2pComm->waitPost.Set();
        needRollback = false;
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

    uint64_t castedData = reinterpret_cast<uint64_t>(data);
    std::shared_lock<std::shared_timed_mutex> l(segmentMutex_);
    HostSegmentMap::Accessor accessor;
    if (!hostSegmentMap_->Find(accessor, castedData)) {
        if (hostSegmentMap_->Insert(accessor, castedData)) {
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
            HostSegment::Create(hostSegment, reinterpret_cast<std::byte *>(data), dataSize, segmentInfo, permissions);
            accessor.entry->data = hostSegment;
            needRollback = false;
        }
    }
    return Status::OK();
}

Status RemoteH2DManager::FillSegmentInfo(uint64_t segLen, uint64_t segDataOffset, uint64_t key,
                                         RemoteHostSegmentPb &segmentPb)
{
    HostSegmentMap::Accessor accessor;
    if (hostSegmentMap_->Find(accessor, key)) {
        auto &segInfo = accessor.entry->data->segmentInfo;
        segmentPb.set_name(std::string(std::begin(segInfo.internal), std::end(segInfo.internal)));
        segmentPb.set_seg_va(key);
        segmentPb.set_seg_len(segLen);
        segmentPb.set_seg_data_offset(segDataOffset);
        return Status::OK();
    }
    return Status(K_NOT_FOUND, FormatString("Cannot find segment for address %zu", key));
}

Status RemoteH2DManager::Init()
{
    RETURN_OK_IF_TRUE(!IsRemoteH2DEnabled());
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
    RETURN_IF_NOT_OK(SetDeviceIdx());

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
    RETURN_IF_NOT_OK(SetDeviceIdx());

    PerfPoint point(PerfKey::P2P_SCATTER_BATCH);

    // Thread safety needs to be ensured for the p2p communicator.
    std::lock_guard<std::mutex> lock(p2pComm->mutex);
    p2pComm->waitPost.Wait();

    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->DSP2PScatterBatchFromRemoteHostMem(
        entries, size, p2pComm->p2pComm, *(p2pComm->stream)));
    LOG_IF_ERROR(acl::AclDeviceManager::Instance()->RtSynchronizeStreamWithTimeout(*(p2pComm->stream), 5000), "");
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
        LOG_IF_ERROR(acl::AclDeviceManager::Instance()->DSP2PCommDestroy(p2pComm), "Destroy P2P comm failed.");
    }
    if (stream) {
        LOG_IF_ERROR(acl::AclDeviceManager::Instance()->RtDestroyStream(*stream), "Destroy stream failed.");
    }
};

}  // namespace datasystem
