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

#include "datasystem/common/rdma/urma_manager_wrapper.h"

namespace datasystem {
bool IsUrmaEnabled()
{
#ifdef USE_URMA
    return UrmaManager::IsUrmaEnabled();
#else
    return false;
#endif
}

Status InitializeUrmaManager(const std::string &host)
{
    (void)host;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().Init(host));
    }
#endif
    return Status::OK();
}

Status RemoveRemoteUrmaDevice(const HostPort &remoteAddress)
{
    (void)remoteAddress;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().RemoveRemoteDevice(remoteAddress));
    }
#endif
    return Status::OK();
}

Status RegisterUrmaMemory(void *segAddress, const uint64_t &segSize)
{
    (void)segAddress;
    (void)segSize;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled() && UrmaManager::IsRegisterWholeArenaEnabled() && segAddress != nullptr) {
        LOG(INFO) << "Doing URMA memory registration of size " << segSize;
        RETURN_IF_NOT_OK(UrmaManager::Instance().RegisterSegment(reinterpret_cast<uint64_t>(segAddress), segSize));
    }
#endif
    return Status::OK();
}

Status WaitUrmaEvent(std::vector<uint64_t> &keys, std::function<int64_t(void)> remainingTime,
                     std::function<Status(Status &)> errorHandler)
{
    (void)keys;
    (void)remainingTime;
    (void)errorHandler;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        PerfPoint point(PerfKey::URMA_TOTAL_WAIT_TO_FINISH);
        for (auto key : keys) {
            // Wait for the event until timeout
            Status rc = UrmaManager::Instance().WaitToFinish(key, remainingTime());
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(errorHandler(rc), "Failed to wait for URMA event.");
        }
    }
#endif
    return Status::OK();
}

void GetSegmentInfoFromShmUnit(std::shared_ptr<ShmUnit> shmUnit, uint64_t memoryAddress, uint64_t &segAddress,
                               uint64_t &segSize)
{
    (void)shmUnit;
    (void)memoryAddress;
    (void)segAddress;
    (void)segSize;
#ifdef USE_URMA
    // If we registered the whole arena to RDMA device,
    // then the segment address is the arena address,
    // and the segment size would be the mmaped size.
    // Otherwise the segment is for per object memory.
    if (UrmaManager::IsRegisterWholeArenaEnabled()) {
        segAddress = memoryAddress - shmUnit->GetOffset();
        segSize = shmUnit->GetMmapSize();
    } else {
        segAddress = memoryAddress;
        segSize = shmUnit->GetSize();
    }
#endif
}

Status FillUrmaInfo(std::shared_ptr<ShmUnit> shmUnit, const HostPort &localAddress, uint64_t metaSz,
                    UrmaImportSegmentPb &urmaInfo)
{
    (void)shmUnit;
    (void)localAddress;
    (void)metaSz;
    (void)urmaInfo;
#ifdef USE_URMA
    uint64_t segAddress;
    uint64_t segSize;
    GetSegmentInfoFromShmUnit(shmUnit, reinterpret_cast<uint64_t>(shmUnit->GetPointer()), segAddress, segSize);
    uint64_t segVA;
    uint64_t segLen;
    uint32_t segFlag;
    uint32_t segTokenId;
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetSegmentInfo(segAddress, segSize, segVA, segLen, segFlag, segTokenId));
    urmaInfo.set_seg_va(segVA);
    urmaInfo.set_seg_len(segLen);
    urmaInfo.set_seg_flag(segFlag);
    urmaInfo.set_seg_token_id(segTokenId);
    if (UrmaManager::IsRegisterWholeArenaEnabled()) {
        urmaInfo.set_seg_data_offset(shmUnit->GetOffset() + metaSz);
    } else {
        urmaInfo.set_seg_data_offset(metaSz);
    }
    urmaInfo.mutable_request_address()->set_host(localAddress.Host());
    urmaInfo.mutable_request_address()->set_port(localAddress.Port());
#endif
    return Status::OK();
}

Status ImportSegAndWritePayload(const UrmaImportSegmentPb &urmaInfo, const uint64_t &localSegAddress,
                                const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                bool blocking, std::vector<uint64_t> &keys)
{
    (void)urmaInfo;
    (void)localSegAddress;
    (void)localSegSize;
    (void)localObjectAddress;
    (void)readOffset;
    (void)readSize;
    (void)metaDataSize;
    (void)blocking;
    (void)keys;
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().ImportSegAndWritePayload(urmaInfo, localSegAddress, localSegSize,
                                                                      localObjectAddress, readOffset, readSize,
                                                                      metaDataSize, blocking, keys));
#endif
    return Status::OK();
}
}  // namespace datasystem