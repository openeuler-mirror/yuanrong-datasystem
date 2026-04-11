/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Slot active writer and group commit helper.
 */

#include "datasystem/common/l2cache/slot_client/slot_writer.h"

#include <chrono>

#include <fcntl.h>
#include <unistd.h>

#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {
void CloseFd(int &fd)
{
    if (fd >= 0) {
        close(fd);
        fd = -1;
    }
}
}  // namespace

SlotWriter::~SlotWriter()
{
    Close();
}

Status SlotWriter::Init(const std::string &slotPath, const SlotManifestData &manifest)
{
    Close();
    lastFlushMs_ = NowMonotonicMs();
    VLOG(1) << "Initializing slot writer, slotPath=" << slotPath << ", activeIndex=" << manifest.activeIndex
            << ", activeDataCount=" << manifest.activeData.size();
    if (!manifest.activeIndex.empty()) {
        auto indexPath = JoinPath(slotPath, manifest.activeIndex);
        RETURN_IF_NOT_OK(SlotIndexCodec::EnsureIndexFile(indexPath));
        RETURN_IF_NOT_OK(OpenFile(indexPath, O_RDWR, &activeIndexFd_));
        activeIndexSize_ = static_cast<uint64_t>(FdFileSize(activeIndexFd_));
    }
    if (!manifest.activeData.empty()) {
        auto dataFile = manifest.activeData.back();
        RETURN_IF_NOT_OK(ParseDataFileId(dataFile, activeDataFileId_));
        auto dataPath = JoinPath(slotPath, dataFile);
        RETURN_IF_NOT_OK(EnsureFile(dataPath));
        RETURN_IF_NOT_OK(OpenFile(dataPath, O_RDWR, &activeDataFd_));
        activeDataSize_ = static_cast<uint64_t>(FdFileSize(activeDataFd_));
    }
    VLOG(1) << "Initialized slot writer, slotPath=" << slotPath << ", activeIndexSize=" << activeIndexSize_
            << ", activeDataFileId=" << activeDataFileId_ << ", activeDataSize=" << activeDataSize_;
    return Status::OK();
}

void SlotWriter::Close()
{
    CloseFd(activeDataFd_);
    CloseFd(activeIndexFd_);
    activeDataFileId_ = 0;
    activeDataSize_ = 0;
    activeIndexSize_ = 0;
    bufferedBytes_ = 0;
    pendingOps_ = 0;
    lastFlushMs_ = 0;
}

Status SlotWriter::AppendData(const std::string &payload, uint64_t &offset)
{
    return AppendData(payload.data(), payload.size(), offset);
}

Status SlotWriter::AppendData(const char *buffer, size_t len, uint64_t &offset)
{
    CHECK_FAIL_RETURN_STATUS(activeDataFd_ >= 0, StatusCode::K_RUNTIME_ERROR, "Active data fd is not initialized");
    offset = activeDataSize_;
    RETURN_IF_NOT_OK(WriteFile(activeDataFd_, buffer, len, static_cast<off_t>(offset)));
    activeDataSize_ += len;
    return Status::OK();
}

Status SlotWriter::AppendIndexPayload(const std::string &payload)
{
    CHECK_FAIL_RETURN_STATUS(activeIndexFd_ >= 0, StatusCode::K_RUNTIME_ERROR, "Active index fd is not initialized");
    RETURN_IF_NOT_OK(SlotIndexCodec::AppendEncodedRecords(activeIndexFd_, activeIndexSize_, payload));
    return Status::OK();
}

void SlotWriter::RecordOperation(uint64_t bufferedBytes)
{
    bufferedBytes_ += bufferedBytes;
    ++pendingOps_;
}

Status SlotWriter::Flush()
{
    if (!HasPendingWrites()) {
        return Status::OK();
    }
    VLOG(1) << "Flushing slot writer, pendingOps=" << pendingOps_ << ", bufferedBytes=" << bufferedBytes_
            << ", activeIndexSize=" << activeIndexSize_ << ", activeDataSize=" << activeDataSize_;
    if (activeDataFd_ >= 0) {
        RETURN_IF_NOT_OK(FsyncFd(activeDataFd_));
    }
    if (activeIndexFd_ >= 0) {
        RETURN_IF_NOT_OK(FsyncFd(activeIndexFd_));
    }
    bufferedBytes_ = 0;
    pendingOps_ = 0;
    lastFlushMs_ = NowMonotonicMs();
    VLOG(1) << "Flushed slot writer successfully";
    return Status::OK();
}

bool SlotWriter::ShouldFlush(uint64_t syncIntervalMs, uint64_t batchBytes) const
{
    if (!HasPendingWrites()) {
        return false;
    }
    if (batchBytes == 0 || bufferedBytes_ >= batchBytes) {
        return true;
    }
    if (syncIntervalMs == 0) {
        return true;
    }
    return NowMonotonicMs() - lastFlushMs_ >= syncIntervalMs;
}

bool SlotWriter::HasPendingWrites() const
{
    return pendingOps_ > 0;
}

bool SlotWriter::IsInitialized() const
{
    return activeIndexFd_ >= 0 || activeDataFd_ >= 0;
}

uint32_t SlotWriter::GetActiveDataFileId() const
{
    return activeDataFileId_;
}

uint64_t SlotWriter::GetActiveDataSize() const
{
    return activeDataSize_;
}

uint64_t SlotWriter::GetActiveIndexSize() const
{
    return activeIndexSize_;
}

uint64_t SlotWriter::GetPendingOps() const
{
    return pendingOps_;
}

uint64_t SlotWriter::NowMonotonicMs() const
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
}  // namespace datasystem
