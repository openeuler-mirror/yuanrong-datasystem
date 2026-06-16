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

#include "datasystem/common/rdma/npu/roce_transport.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <openssl/evp.h>
#include <securec.h>
#include <vector>

#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/device/ascend/p2phccl_types.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "datasystem/protos/utils.pb.h"

namespace datasystem {

namespace {
constexpr size_t BASE64_ENCODED_GROUP_SIZE = 4;
constexpr size_t BASE64_DECODED_GROUP_SIZE = 3;
constexpr int ENCODED_SIZE = 2;

bool Base64Decode(const std::string &encoded, std::string &decoded)
{
    if (encoded.empty()) {
        decoded.clear();
        return true;
    }
    if (encoded.size() % BASE64_ENCODED_GROUP_SIZE != 0) {
        return false;
    }
    if (encoded.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return false;
    }
    size_t outCap = (encoded.size() / BASE64_ENCODED_GROUP_SIZE) * BASE64_DECODED_GROUP_SIZE;
    std::vector<unsigned char> encodedBytes(encoded.begin(), encoded.end());
    std::vector<unsigned char> buffer(outCap);
    int outLen = EVP_DecodeBlock(buffer.data(), encodedBytes.data(), static_cast<int>(encodedBytes.size()));
    if (outLen < 0) {
        return false;
    }
    size_t padding = 0;
    if (!encoded.empty() && encoded.back() == '=') {
        ++padding;
        if (encoded.size() > 1 && encoded[encoded.size() - ENCODED_SIZE] == '=') {
            ++padding;
        }
    }
    size_t decodedLen = static_cast<size_t>(outLen);
    if (decodedLen < padding) {
        return false;
    }
    decoded.assign(buffer.begin(), buffer.begin() + decodedLen - padding);
    return true;
}

// Maximum number of blobs (individual memory regions) per batch
// FFTS context limit: avoid exceeding driver resource constraints
constexpr uint32_t P2P_SCATTER_MAX_BATCH_BLOBS = 128 * 128;  // 16384
// Maximum length for endpoint identity in log output (truncate longer identities)
constexpr size_t MAX_ENDPOINT_LOG_LEN = 32;
constexpr int32_t WAIT_INTERVAL_MS = 5000;

}  // namespace

Status RoCETransport::Init(const std::vector<int32_t> &deviceIds)
{
    (void)deviceIds;
    // RoCE needs no special initialization beyond ACL already done
    return Status::OK();
}

Status RoCETransport::GetConnectionIdentity(std::string *identity)
{
    HcclRootInfo rootInfo;
    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->DSP2PGetRootInfo(&rootInfo));
    std::string rootInfoStr(std::begin(rootInfo.internal), std::end(rootInfo.internal));
    return Hasher::Base64Encode(rootInfoStr, *identity);
}

Status RoCETransport::Connect(const std::string &remoteIdentity, P2pKind kind, std::function<int()> *heartbeatCallback)
{
    std::lock_guard<std::mutex> lock(connMutex_);

    // Check if already connected
    if (endpointToComm_.find(remoteIdentity) != endpointToComm_.end()) {
        return Status::OK();
    }

    // Decode base64 remoteIdentity back to rootInfo bytes
    std::string rootInfoBytes;
    if (!Base64Decode(remoteIdentity, rootInfoBytes)) {
        return Status(StatusCode::K_INVALID, "Base64 decode failed for remote identity");
    }
    CHECK_FAIL_RETURN_STATUS(rootInfoBytes.size() == HCCL_ROOT_INFO_BYTES, StatusCode::K_INVALID,
                             "Invalid RoCE remote identity length");
    HcclRootInfo remoteRootInfo;
    int ret = memset_s(&remoteRootInfo, sizeof(remoteRootInfo), 0, sizeof(remoteRootInfo));
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR, "Failed to clear RoCE root info");
    ret = memcpy_s(remoteRootInfo.internal, HCCL_ROOT_INFO_BYTES, rootInfoBytes.data(), rootInfoBytes.size());
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR, "Failed to copy RoCE root info");

    // Prepare connection options with heartbeat callback
    P2PCommInitOptions p2pCommInitOptions(false, heartbeatCallback);

    int32_t devId = -1;
    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->GetDeviceIdx(devId));

    P2PComm p2pComm = nullptr;
    Status rc = acl::AclDeviceManager::Instance()->DSP2PCommInitRootInfo(&remoteRootInfo, kind, P2P_LINK_ROCE, &p2pComm,
                                                                         &p2pCommInitOptions);
    if (!rc.IsOk()) {
        return Status(StatusCode::K_RUNTIME_ERROR, "DSP2PCommInitRootInfo failed: " + rc.GetMsg());
    }

    // Track this connection for later cleanup.
    // The underlying P2PComm is destroyed when the last shared_ptr copy goes away (via ~P2PCommContext()),
    // which may happen after Disconnect if in-flight ScatterBatch calls still hold a reference.
    endpointToComm_[remoteIdentity] = std::make_shared<P2PCommContext>(p2pComm, devId);

    LOG(INFO) << "[RoCE] Connected to endpoint: " << remoteIdentity.substr(0, MAX_ENDPOINT_LOG_LEN);
    return Status::OK();
}

Status RoCETransport::Disconnect(const std::string &remoteIdentity)
{
    std::lock_guard<std::mutex> lock(connMutex_);
    endpointToComm_.erase(remoteIdentity);
    return Status::OK();
}

Status RoCETransport::DisconnectAll()
{
    std::lock_guard<std::mutex> lock(connMutex_);
    endpointToComm_.clear();
    return Status::OK();
}

Status RoCETransport::RegisterMemory(void *addr, uint64_t size, P2pSegmentInfo *segInfo)
{
    return acl::AclDeviceManager::Instance()->DSP2PRegisterHostMem(addr, size, segInfo, P2P_SEGMENT_READ_WRITE);
}

Status RoCETransport::ImportRemoteAddressInfo(const std::string &remoteEndpoint, const RemoteHostSegmentPb &seg)
{
    // RoCE uses protobuf bytes 'name' as the segment descriptor for driver import.
    // remoteEndpoint is not used for RoCE but kept for signature consistency.
    (void)remoteEndpoint;

    P2pSegmentInfo segInfo;
    int ret = memset_s(&segInfo, sizeof(segInfo), 0, sizeof(segInfo));
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR, "Failed to clear RoCE segment info");

    const std::string &nameBytes = seg.name();
    CHECK_FAIL_RETURN_STATUS(nameBytes.size() == P2P_SEGMENT_INFO_BYTES, StatusCode::K_INVALID,
                             "Invalid RoCE segment info length");
    ret = memcpy_s(segInfo.internal, P2P_SEGMENT_INFO_BYTES, nameBytes.data(), nameBytes.size());
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR, "Failed to copy RoCE segment info");

    return acl::AclDeviceManager::Instance()->DSP2PImportHostSegment(segInfo);
}

Status RoCETransport::ScatterBatch(P2pScatterEntry *entries, uint32_t count, const std::string &remoteEndpoint,
                                   std::shared_ptr<aclrtStream> stream)
{
    // Only hold connMutex_ while looking up endpointToComm_; copy the shared_ptr and release it immediately.
    // This avoids serializing all endpoints during the subsequent transfer, including the blocking
    // RtSynchronizeStreamWithTimeout calls (up to 5 seconds each).
    std::shared_ptr<P2PCommContext> ctx;
    {
        std::lock_guard<std::mutex> lock(connMutex_);
        auto it = endpointToComm_.find(remoteEndpoint);
        if (it == endpointToComm_.end()) {
            return Status(StatusCode::K_NOT_FOUND,
                          "No RoCE connection for endpoint: " + remoteEndpoint.substr(0, MAX_ENDPOINT_LOG_LEN));
        }
        ctx = it->second;
    }
    P2PComm p2pComm = ctx->comm;

    // RoCE path requires a valid stream
    RETURN_RUNTIME_ERROR_IF_NULL(stream);

    uint32_t batchStart = 0;
    uint32_t batchSize = 0;
    uint32_t batchBlobCount = 0;

    for (uint32_t i = 0; i < count; ++i) {
        uint32_t entryBlobs = entries[i].numEl;

        if (entryBlobs == 0 || entryBlobs > P2P_SCATTER_MAX_BATCH_BLOBS) {
            return Status(StatusCode::K_INVALID,
                          FormatString("RoCE ScatterBatch entry numEl out of range: %u, expected (0, %u]", entryBlobs,
                                       P2P_SCATTER_MAX_BATCH_BLOBS));
        }

        // If adding this entry exceeds limit and we have a pending batch, submit it and sync
        if (batchSize > 0 && batchBlobCount + entryBlobs > P2P_SCATTER_MAX_BATCH_BLOBS) {
            // Submit current batch
            RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->DSP2PScatterBatchFromRemoteHostMem(
                entries + batchStart, batchSize, p2pComm, *stream));
            // Must synchronize after each batch to avoid driver resource overrun / race condition
            RETURN_IF_NOT_OK(
                acl::AclDeviceManager::Instance()->RtSynchronizeStreamWithTimeout(*stream, WAIT_INTERVAL_MS));

            // Start new batch from current entry
            batchStart = i;
            batchSize = 0;
            batchBlobCount = 0;
        }

        batchSize++;
        batchBlobCount += entryBlobs;
    }

    // Submit final batch if any entries remain
    if (batchSize > 0) {
        RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->DSP2PScatterBatchFromRemoteHostMem(
            entries + batchStart, batchSize, p2pComm, *stream));
        RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->RtSynchronizeStreamWithTimeout(*stream, WAIT_INTERVAL_MS));
    }

    return Status::OK();
}

RoCETransport::P2PCommContext::~P2PCommContext()
{
    if (comm == nullptr) {
        return;
    }
    auto cleanup = [this]() {
        auto *deviceManager = acl::AclDeviceManager::Instance();
        RETURN_IF_NOT_OK(deviceManager->SetDeviceIdx(devId));
        RETURN_IF_NOT_OK(deviceManager->DSP2PCommDestroy(comm));
        return Status::OK();
    };
    LOG_IF_ERROR(cleanup(), "[RoCE] Failed to destroy P2PComm");
}

P2pLink RoCETransport::LinkType() const
{
    return P2P_LINK_ROCE;
}

}  // namespace datasystem
