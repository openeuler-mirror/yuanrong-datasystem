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
 * Description: Client mmap table management.
 */
#include "datasystem/client/mmap_table.h"

#include <atomic>
#include <cstddef>
#include <shared_mutex>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/flags/flags.h"

namespace datasystem {
namespace client {
MmapTable::MmapTable(bool enableHugeTlb)
{
    enableHugeTlb_ = enableHugeTlb;
}
Status MmapTable::MmapAndStoreFd(const int &clientFd, const int &workerFd, const uint64_t &mmapSize)
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    auto entry = mmapTable_.find(workerFd);
    if (entry == mmapTable_.end()) {
        // Check the workerFd and clientFd whether is valid.
        if (workerFd > 0 && clientFd > 0) {
            LOG(INFO) << FormatString("Worker fd: %d, Mmap the client fd %d, mmap size is %llu", workerFd, clientFd,
                                      mmapSize);
            auto newEntry = std::make_unique<MmapTableEntry>(clientFd, mmapSize);
            RETURN_IF_NOT_OK(newEntry->Init(enableHugeTlb_));
            mmapTable_[workerFd] = std::move(newEntry);
        }
    } else {
        LOG(INFO) << FormatString("The client fd %d exists, no need to mmap again", clientFd);
    }
    return Status::OK();
}

Status MmapTable::LookupFdPointer(const int &workerFd, uint8_t **pointer)
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto entry = mmapTable_.find(workerFd);
    RETURN_RUNTIME_ERROR_IF_NULL(pointer);
    if (entry != mmapTable_.end()) {
        *pointer = const_cast<uint8_t *>(entry->second->Pointer());
    } else {
        std::string err = "Cannot find mapped file: " + std::to_string(workerFd);
        LOG(ERROR) << err;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, err);
    }
    return Status::OK();
}

bool MmapTable::FindFd(const int &workerFd)
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto entry = mmapTable_.find(workerFd);
    return entry != mmapTable_.end();
}

void MmapTable::Clear()
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    mmapTable_.clear();
}

void MmapTable::CleanInvalidMmapTable()
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    mmapTable_.clear();
}

void MmapTable::ClearExpiredFds(const std::vector<int64_t> &fds)
{
    LOG(INFO) << "Clear expired workerfds: " << VectorToString(fds);
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    for (auto fd : fds) {
        mmapTable_.erase(fd);
    }
}

std::shared_ptr<MmapTableEntry> MmapTable::GetMmapEntryByFd(int fd)
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto iter = mmapTable_.find(fd);
    return iter == mmapTable_.end() ? nullptr : iter->second;
}

MmapTableEntry::MmapTableEntry(int fd, size_t mmapSize) : fd_(fd), size_(mmapSize), pointer_(nullptr)
{
}

Status MmapTableEntry::Init(bool enableHugeTlb)
{
    std::stringstream err;
    if (size_ <= 0) {
        err << "The mmap size [" << size_ << "] is invalid for fd [" << fd_ << "]";
        LOG(ERROR) << err.str();
        RETURN_STATUS(StatusCode::K_INVALID, err.str());
    }
    INJECT_POINT("MmapTableEntry.mmap");
    // mmap fd
    uint32_t mFlag = MAP_SHARED;
    if (enableHugeTlb) {
        mFlag |= MAP_HUGETLB;
    }
    pointer_ = reinterpret_cast<uint8_t *>(mmap(nullptr, size_, PROT_READ | PROT_WRITE, mFlag, fd_, 0));
    if (pointer_ == MAP_FAILED) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Mmap [fd = %d] failed. Error no: [%s]", fd_, StrErr(errno)));
    }
    // Exclude the shared memory from core dump.
    int ret = madvise(pointer_, size_, MADV_DONTDUMP);
    if (ret != 0) {
        // Ignore and write log.
        LOG(WARNING) << "madvise DONTDUMP memory failed: " << StrErr(errno);
    }

    // Closing this fd has an effect on performance.
    RETRY_ON_EINTR(close(fd_));
    LOG(INFO) << "mmap success, fd " << fd_;
    return Status::OK();
}

MmapTableEntry::~MmapTableEntry()
{
    // munmap fd.
    if (pointer_ != nullptr && pointer_ != MAP_FAILED) {
        int ret = munmap(pointer_, size_);
        if (ret != 0) {
            LOG(ERROR) << FormatString("munmap failed, returned: [%d], errno = [%s]", ret, StrErr(errno));
        } else {
            LOG(INFO) << "munmap success, fd " << fd_;
        }
    } else {
        LOG(ERROR) << "Mmap pointer is invalid, it may be nullptr";
    }
}
}  // namespace client
}  // namespace datasystem