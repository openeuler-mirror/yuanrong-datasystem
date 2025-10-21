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
 * Description: base mmap.
 */

#include "datasystem/common/shared_memory/mmap/base_mmap.h"

#include <fcntl.h>
#ifdef __linux__
#include <linux/memfd.h>
#endif
#include <sys/mman.h>
#include <unistd.h>

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace memory {

Status BaseMmap::SetupFileMapping(size_t size, int flags, bool isSeal)
{
    // 1. Truncate it to the size what we want.
    if (ftruncate(fd_, static_cast<off_t>(size)) != 0) {
        RETRY_ON_EINTR(close(fd_));
        fd_ = -1;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "failed to ftruncate file: " + StrErr(errno));
    }

    // 2. Seal fd to avoid the file size reduce or increase, and disable seal again.
    if (isSeal && fcntl(fd_, F_ADD_SEALS, F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL) != 0) {
        RETRY_ON_EINTR(close(fd_));
        fd_ = -1;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "failed to fcntl(F_SEAL_SHRINK) file: " + StrErr(errno));
    }

    // 3. Mmap and record it to the MmapEntryTable.
    pointer_ = mmap(nullptr, size, PROT_READ | PROT_WRITE, flags, fd_, 0);
    if (pointer_ == MAP_FAILED) {
        RETRY_ON_EINTR(close(fd_));
        pointer_ = nullptr;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("failed to mmap shared %s: %s", type_, StrErr(errno)));
    }

    // 4. Exclude the shared memory from core dump.
    int ret = madvise(pointer_, size, MADV_DONTDUMP);
    if (ret != 0) {
        // Ignore and write log.
        LOG(WARNING) << "madvise DONTDUMP " << type_ << " failed: " << StrErr(errno);
    }

    mmapSize_ = size;
    curr_ = reinterpret_cast<uintptr_t>(pointer_);
    tail_ = curr_ + static_cast<uintptr_t>(mmapSize_);
    return Status::OK();
}

BaseMmap::~BaseMmap()
{
    if (pointer_ != nullptr) {
        VLOG(1) << "munmap shared " << type_ << " link, fd:" << fd_ << ", mmapSize:" << mmapSize_;
        int ret = munmap(pointer_, mmapSize_);
        if (ret != 0) {
            LOG(ERROR) << "Failed to unmap " << type_ << ": " << StrErr(errno);
        }
    }

    if (fd_ != -1) {
        RETRY_ON_EINTR(close(fd_));
    }
}

inline uintptr_t AlignCeiling(uintptr_t addr, uintptr_t alignment)
{
    return (addr + alignment - 1) & ~(alignment - 1);
}

void *BaseMmap::Allocate(size_t size, size_t alignment, bool *zero, bool *commit)
{
    (void)zero;
    (void)commit;
    uintptr_t curr = curr_.load(std::memory_order_acquire);
    void *addr = nullptr;
    while (true) {
        uintptr_t alignedAddr = AlignCeiling(curr, alignment);
        uintptr_t newCurr = alignedAddr + size;
        if (newCurr > tail_) {
            break;
        }
        // Retry if other thread changed curr_.
        if (curr_.compare_exchange_weak(curr, newCurr, std::memory_order_release, std::memory_order_acquire)) {
            addr = reinterpret_cast<void *>(alignedAddr);
            break;
        }
    }
    return addr;
}

void BaseMmap::Destroy()
{
    LOG_IF(ERROR, munmap(pointer_, mmapSize_) != 0) << "Unmap " << type_ << " failed: " << StrErr(errno);
}

bool BaseMmap::Decommit(void *addr, size_t offset, size_t length)
{
    VLOG(1) << "madvise fd:" << fd_ << ", offset:" << offset << ", length:" << length;
    // Free up a given range of pages and its associated backing store immediately.
    int ret = madvise(reinterpret_cast<void *>((uintptr_t)addr + (uintptr_t)offset), length, MADV_REMOVE);
    if (ret != 0) {
        LOG(ERROR) << "madvise REMOVE " << type_ << " failed: " << StrErr(errno);
        return true;
    }
    return false;
}

Optional<Allocation> BaseMmap::GetAllocation(void *pointer) const
{
    PerfPoint point(PerfKey::ALLOCATE_GET_MAP);
    auto *addr = reinterpret_cast<unsigned char *>(pointer);
    auto *head = reinterpret_cast<unsigned char *>(pointer_);
    auto *tail = head + static_cast<ptrdiff_t>(mmapSize_);
    // If addr is in range [head, tail), we found the mmap region.
    if (addr >= head && addr < tail) {
        auto offset = static_cast<ptrdiff_t>(addr - head);
        return Optional<Allocation>(pointer, fd_, offset, mmapSize_);
    }
    return Optional<Allocation>();
}

}  // namespace memory
}  // namespace datasystem