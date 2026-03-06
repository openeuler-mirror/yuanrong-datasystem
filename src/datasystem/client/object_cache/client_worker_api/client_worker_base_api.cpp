/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Defines the worker client base class to communicate with the worker service.
 */
#include "datasystem/client/object_cache/client_worker_api/client_worker_base_api.h"

#include <cstdint>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"

using datasystem::client::ClientWorkerRemoteCommonApi;

namespace datasystem {
namespace object_cache {
static constexpr int DEBUG_LOG_LEVEL = 2;
static constexpr uint64_t MAX_PUB_SIZE = 256 * 1024 * 1024 * 1024uL;
static constexpr uint64_t CHUNK_SIZE = 1024 * 1024;

Status ClientWorkerBaseApi::PreparePublishReq(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isSeal,
                                              const std::unordered_set<std::string> &nestedKeys, uint32_t ttlSecond,
                                              int existence, PublishReqPb &req)
{
    LOG(INFO) << FormatString("Begin to publish object, client id: %s, worker address: %s, object key: %s", clientId_,
                              hostPort_.ToString(), bufferInfo->objectKey);
    *req.mutable_nested_keys() = { nestedKeys.begin(), nestedKeys.end() };
    req.set_client_id(clientId_);
    req.set_object_key(bufferInfo->objectKey);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when publish date.");
    req.set_ttl_second(ttlSecond);
    req.set_existence(static_cast<::datasystem::ExistenceOptPb>(existence));

    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(UINT64_MAX - bufferInfo->dataSize >= bufferInfo->metadataSize,
                                         StatusCode::K_RUNTIME_ERROR,
                                         FormatString("data size[%llu] + meta size [%llu] > UINT64_MAX",
                                                      bufferInfo->dataSize, bufferInfo->metadataSize));
    auto bufferSize = bufferInfo->dataSize + bufferInfo->metadataSize;
    CHECK_FAIL_RETURN_STATUS(
        bufferSize < MAX_PUB_SIZE, K_INVALID,
        FormatString("Buffer size should not be too large, curr: %llu, max: %llu", bufferSize, MAX_PUB_SIZE));

    req.set_data_size(bufferInfo->dataSize);
    req.set_write_mode(static_cast<uint32_t>(bufferInfo->objectMode.GetWriteMode()));
    req.set_consistency_type(static_cast<uint32_t>(bufferInfo->objectMode.GetConsistencyType()));
    req.set_cache_type(static_cast<uint32_t>(bufferInfo->objectMode.GetCacheType()));
    req.set_is_seal(isSeal);
    req.set_shm_id(bufferInfo->shmId);
    return Status::OK();
}

#ifdef USE_URMA
void ClientWorkerBaseApi::PrepareUrmaBuffer(GetReqPb &req, std::shared_ptr<UrmaManager::BufferHandle> &ubBufferHandle,
                                            uint8_t *&ubBufferPtr, uint64_t &ubBufferSize)
{
    if (IsUrmaEnabled() && !shmEnabled_) {
        Status ubRc = UrmaManager::Instance().GetMemoryBufferHandle(ubBufferHandle);
        if (ubRc.IsOk() && ubBufferHandle != nullptr) {
            UrmaRemoteAddrPb urmaInfo;
            ubRc = UrmaManager::Instance().GetMemoryBufferInfo(ubBufferHandle->GetOffset(), ubBufferPtr, ubBufferSize,
                                                               urmaInfo);
            if (ubRc.IsOk()) {
                req.set_ub_buffer_size(ubBufferSize);
                *req.mutable_urma_info() = urmaInfo;
            }
        }
        if (ubRc.IsError()) {
            LOG(WARNING) << "Prepare UB Get request failed: " << ubRc.ToString() << ", fallback to TCP/IP payload.";
            ubBufferHandle.reset();
            ubBufferPtr = nullptr;
            ubBufferSize = 0;
        }
    }
}

Status ClientWorkerBaseApi::FillUrmaBuffer(std::shared_ptr<UrmaManager::BufferHandle> &ubBufferHandle, GetRspPb &rsp,
                                           std::vector<RpcMessage> &payloads, uint8_t *ubBufferPtr,
                                           uint64_t ubBufferSize)
{
    if (ubBufferHandle != nullptr && ubBufferPtr != nullptr && rsp.payload_info_size() > 0) {
        uint64_t ubReadOffset = 0;
        for (int i = 0; i < rsp.payload_info_size(); ++i) {
            auto *payloadInfo = rsp.mutable_payload_info(i);
            if (payloadInfo->part_index_size() != 0) {
                continue;
            }
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(payloadInfo->data_size() >= 0, K_RUNTIME_ERROR,
                                                 FormatString("Invalid UB payload size for object %s: %ld",
                                                              payloadInfo->object_key(), payloadInfo->data_size()));
            uint64_t payloadSize = static_cast<uint64_t>(payloadInfo->data_size());
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                ubReadOffset <= ubBufferSize && payloadSize <= ubBufferSize - ubReadOffset, K_RUNTIME_ERROR,
                FormatString("UB payload overflow, object %s, payload size %llu, consumed %llu, buffer size %llu",
                             payloadInfo->object_key(), payloadSize, ubReadOffset, ubBufferSize));
            payloads.emplace_back();
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                payloads.back().CopyBuffer(ubBufferPtr + ubReadOffset, static_cast<size_t>(payloadSize)),
                "Build UB payload rpc message failed");
            payloadInfo->add_part_index(static_cast<uint32_t>(payloads.size() - 1));
            ubReadOffset += payloadSize;
        }
    }
    return Status::OK();
}

Status ClientWorkerBaseApi::PipelineDataTransferHelper(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                                                       const void *data, uint64_t totalSize,
                                                       std::shared_ptr<UrmaManager::BufferHandle> &bufHandle,
                                                       uint64_t slotSize)
{
    // Use credit-based pipeline for large data
    uint32_t pipelineDepth = slotSize / CHUNK_SIZE;
    VLOG(DEBUG_LOG_LEVEL) << FormatString("[UB Put Pipeline]: total_size=%llu, chunk_size=%llu, pipeline_depth=%u",
                                          totalSize, CHUNK_SIZE, pipelineDepth);

    // Free chunk queue
    std::deque<uint32_t> freeChunks(pipelineDepth);
    std::iota(freeChunks.begin(), freeChunks.end(), 0);

    // eventId -> chunkIndex
    std::unordered_map<uint64_t, uint32_t> eventToChunkMap;
    std::shared_ptr<EventWaiter> waiter = std::make_shared<EventWaiter>();

    uint64_t processedSize = 0;
    while (processedSize < totalSize || freeChunks.size() < pipelineDepth) {
        // Submit writes while there are free chunks
        while (processedSize < totalSize && !freeChunks.empty()) {
            uint32_t chunkIndex = freeChunks.front();
            freeChunks.pop_front();
            uint8_t *chunkPtr = static_cast<uint8_t *>(bufHandle->GetPointer()) + chunkIndex * CHUNK_SIZE;
            uint64_t writeSize = std::min(totalSize - processedSize, CHUNK_SIZE);
            const uint8_t *srcPtr = static_cast<const uint8_t *>(data) + processedSize;
            memcpy_s(chunkPtr, CHUNK_SIZE, srcPtr, writeSize);
            std::vector<uint64_t> eventKeys;
            Status writeStatus = UrmaWritePayload(*(bufferInfo->ubUrmaDataInfo), bufHandle->GetSegmentAddress(),
                                                  bufHandle->GetSegmentSize(), reinterpret_cast<uint64_t>(chunkPtr), 0,
                                                  writeSize, 0, false, eventKeys, waiter);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(writeStatus,
                                             FormatString("Failed to submit UrmaWritePayload, chunkIndex=%u, "
                                                          "writeSize=%llu, processedSize=%llu, totalSize=%llu",
                                                          chunkIndex, writeSize, processedSize, totalSize));

            uint64_t eventKey = eventKeys[0];
            eventToChunkMap[eventKey] = chunkIndex;
            VLOG(DEBUG_LOG_LEVEL) << FormatString("[UB Put] Submitted chunk %u, event_key=%llu, size=%llu", chunkIndex,
                                                  eventKey, writeSize);
            processedSize += writeSize;
            bufferInfo->ubUrmaDataInfo->set_seg_data_offset(bufferInfo->ubUrmaDataInfo->seg_data_offset() + writeSize);
        }

        // Wait for any event if inflight exists
        if (freeChunks.size() < pipelineDepth) {
            std::shared_ptr<Event> event = nullptr;
            Status rc = waiter->WaitAny(std::chrono::milliseconds(reqTimeoutDuration.CalcRealRemainingTime()), event);
            CHECK_FAIL_RETURN_STATUS(rc.IsOk(), K_RUNTIME_ERROR, rc.ToString());
            uint64_t eventId = event->GetRequestId();
            // Event should be ready
            RETURN_IF_NOT_OK(UrmaManager::Instance().WaitToFinish(eventId, RPC_POLL_TIME));

            auto it = eventToChunkMap.find(eventId);
            CHECK_FAIL_RETURN_STATUS(it != eventToChunkMap.end(), K_RUNTIME_ERROR,
                                     FormatString("Urma event id invalid, eventId=%llu", eventId));
            uint32_t chunkIndex = it->second;
            // Release chunk
            freeChunks.push_back(chunkIndex);
            eventToChunkMap.erase(it);
            VLOG(DEBUG_LOG_LEVEL) << FormatString("[UB Put] Completed event %llu, release chunk %u", eventId,
                                                  chunkIndex);
        }
    }
    bufferInfo->ubDataSentByMemoryCopy = true;
    VLOG(DEBUG_LOG_LEVEL) << FormatString("[UB Put Pipeline] Success, total_size=%llu", totalSize);
    return Status::OK();
}
#endif

Status ClientWorkerBaseApi::SendBufferViaUb(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data,
                                            uint64_t length)
{
    (void)bufferInfo;
    (void)data;
    (void)length;
#ifdef USE_URMA
    const uint64_t totalSize = bufferInfo->metadataSize + bufferInfo->dataSize;
    std::shared_ptr<UrmaManager::BufferHandle> bufHandle;
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferHandle(bufHandle));
    if (!bufHandle || !bufHandle->GetPointer()) {
        return Status(K_RUNTIME_ERROR, "Failed to get memory buffer handle");
    }
    // Check if data fits in a single buffer slot (fallback to old logic)
    uint64_t slotSize = bufHandle->GetSlotSize();
    if (totalSize <= slotSize) {
        std::vector<uint64_t> keys;
        void *poolBuf = bufHandle->GetPointer();
        memcpy_s(poolBuf, slotSize, data, totalSize);
        Status writeStatus = UrmaWritePayload(*(bufferInfo->ubUrmaDataInfo), bufHandle->GetSegmentAddress(),
                                              bufHandle->GetSegmentSize(), reinterpret_cast<uint64_t>(poolBuf), 0,
                                              bufferInfo->dataSize, bufferInfo->metadataSize, true, keys);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(writeStatus,
                                         FormatString("Failed to submit UrmaWritePayload, totalSize=%llu", totalSize));
        bufferInfo->ubDataSentByMemoryCopy = true;
        VLOG(DEBUG_LOG_LEVEL) << "[UB Put] UrmaWritePayload done (single buffer), dataSize = " << bufferInfo->dataSize;
        return Status::OK();
    }

    // Use pipeline transfer for large data
    RETURN_IF_NOT_OK(PipelineDataTransferHelper(bufferInfo, data, totalSize, bufHandle, slotSize));
    return Status::OK();
#else
    return Status(K_INVALID, "Failed to send buffer via UB");
#endif
}

Status ClientWorkerBaseApi::PreGet(const GetParam &getParam, int64_t subTimeoutMs, GetReqPb &req)
{
    const std::vector<std::string> &objectKeys = getParam.objectKeys;
    const std::vector<ReadParam> &readParams = getParam.readParams;
    LOG(INFO) << FormatString("Begin to get object, client id: %s, worker address: %s, object key: %s", clientId_,
                              hostPort_.ToString(), VectorToString(objectKeys));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsInNonNegativeInt32(subTimeoutMs), K_INVALID,
        FormatString("subTimeoutMs %lld is out of range, which should be between[%d, %d]", subTimeoutMs, 0, INT32_MAX));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectKeys.size() == readParams.size() || readParams.empty(), K_INVALID,
                                         FormatString("Invalid offset read param, object count %zu, params count %zu",
                                                      objectKeys.size(), readParams.size()));
    // the max count of objectKeys is 10000.
    auto size = static_cast<int>(objectKeys.size());
    if (size > 0) {
        req.mutable_object_keys()->Reserve(size);
        if (!readParams.empty()) {
            req.mutable_read_offset_list()->Reserve(size);
            req.mutable_read_size_list()->Reserve(size);
        }
    }
    for (int i = 0; i < size; i++) {
        req.add_object_keys(objectKeys[i]);
        if (!readParams.empty()) {
            req.add_read_offset_list(readParams[i].offset);
            req.add_read_size_list(readParams[i].size);
        }
    }
    req.set_no_query_l2cache(!getParam.queryL2Cache);
    req.set_sub_timeout(ClientGetRequestTimeout(subTimeoutMs));
    req.set_client_id(clientId_);
    req.set_return_object_index(true);
    // Add and fill the request with client communicator root info, if RH2D is both supported and enabled.
    if (getParam.isRH2DSupported) {
        RETURN_IF_NOT_OK(GetClientCommUuid(*req.mutable_comm_id()));
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when get data.");
    return Status::OK();
}

void ClientWorkerBaseApi::ParseGlbRefPb(
    QueryGlobalRefNumRspCollectionPb &rsp,
    std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap)
{
    gRefMap.clear();
    for (auto &workerRsp : rsp.objs_glb_refs()) {
        std::vector<GRefDistributionPb> GRefDistPb = { workerRsp.objs_glb_ref().begin(),
                                                       workerRsp.objs_glb_ref().end() };
        for (auto &dist : GRefDistPb) {
            if (dist.referred_addr_size() == 0) {
                continue;
            }
            std::string object_key = dist.object_key().data();
            std::vector<std::string> refClientUuids = { dist.referred_addr().begin(), dist.referred_addr().end() };
            std::unordered_set<std::string> rmDuplicate{ refClientUuids.begin(), refClientUuids.end() };
            gRefMap[object_key].push_back(std::move(rmDuplicate));
        }
    }
}

void ClientWorkerBaseApi::FillDevObjMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo,
                                         const std::vector<Blob> &blobs, DeviceObjectMetaPb *metaPb)
{
    metaPb->set_object_key(bufferInfo->devObjKey);
    metaPb->set_lifetime(LifetimeParamPb(static_cast<int>(bufferInfo->lifetimeType)));
    auto loc = metaPb->add_locations();
    loc->set_client_id(clientId_);
    loc->set_device_id(bufferInfo->deviceIdx);
    for (const auto &blob : blobs) {
        const auto &blobInfos = metaPb->add_data_infos();
        blobInfos->set_data_type(static_cast<int32_t>(DataType::DATA_TYPE_INT8));
        blobInfos->set_count(blob.size);
    }
    metaPb->set_src_offset(bufferInfo->srcOffset);
}

void ClientWorkerBaseApi::PostMultiCreate(bool skipCheckExistence, const MultiCreateRspPb &rsp,
                                          std::vector<MultiCreateParam> &createParams, bool &useShmTransfer,
                                          PerfPoint &point, uint32_t &version, std::vector<bool> &exists)
{
    auto checkUseShm = [this, &rsp, &skipCheckExistence]() {
        if (!shmEnabled_) {
            return false;
        }
        if (skipCheckExistence) {
            return shmEnabled_;
        }
        for (const auto &res : rsp.results()) {
            if (!res.shm_id().empty()) {
                return true;
            }
        }
        return false;
    };
    useShmTransfer = checkUseShm();
    if (!useShmTransfer) {
        return;
    }
    point.RecordAndReset(PerfKey::CLIENT_MULTI_CREATE_FILL_PARAM);
    for (auto i = 0ul; i < createParams.size(); i++) {
        if (!skipCheckExistence && exists[i]) {
            continue;
        }
        auto &shmBuf = createParams[i].shmBuf;
        auto subRsp = rsp.results()[i];
        shmBuf->fd = subRsp.store_fd();
        shmBuf->mmapSize = subRsp.mmap_size();
        shmBuf->offset = static_cast<ptrdiff_t>(subRsp.offset());
        shmBuf->id = ShmKey::Intern(subRsp.shm_id());
        createParams[i].metadataSize = subRsp.metadata_size();
    }
    version = workerVersion_.load(std::memory_order_relaxed);
}
}  // namespace object_cache
}  // namespace datasystem
