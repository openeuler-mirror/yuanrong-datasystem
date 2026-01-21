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
 * Description: Memory mmap instance.
 */

#include "datasystem/common/shared_memory/mmap/obmm_mmap.h"

#include <fcntl.h>
#include <optional>
#include <sstream>
#include <string>
#ifdef __linux__
#include <linux/memfd.h>
#endif
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/shared_memory/mmap/allocation.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"

namespace datasystem {
namespace memory {

ObmmMmap::ObmmMmap(int64_t memId) : memId_(memId)
{
}

Status ObmmMmap::Initialize(uint64_t size, bool populate, bool hugepage)
{
    (void)hugepage;
    // 1. open dev shm file
    std::string tmpfs = "/dev/obmm_shmdev" + std::to_string(memId_);
    fd_ = open(tmpfs.c_str(), O_RDWR | O_SYNC);
    CHECK_FAIL_RETURN_STATUS(fd_ >= 0, StatusCode::K_RUNTIME_ERROR, "open " + tmpfs + " failed: " + StrErr(errno));

    // 2. mmap
    int flags = MAP_SHARED;
    if (populate) {
        flags |= MAP_POPULATE;
    }
    pointer_ = mmap(nullptr, size, PROT_READ | PROT_WRITE, flags, fd_, 0);
    if (pointer_ == MAP_FAILED) {
        RETRY_ON_EINTR(close(fd_));
        pointer_ = nullptr;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("failed to mmap shared %s: %s", type_, StrErr(errno)));
    }
    mmapSize_ = size;
    curr_ = reinterpret_cast<uintptr_t>(pointer_);
    tail_ = curr_ + static_cast<uintptr_t>(mmapSize_);
    // If urma is enabled, register the memory.
    RETURN_IF_NOT_OK(RegisterFastTransportMemory(pointer_, mmapSize_));
    LOG(INFO) << "mmap mem id: " << memId_ << " success, mmap size: " << mmapSize_;
    return Status::OK();
}

bool ObmmMmap::InRange(void *pointer, ptrdiff_t &offset)
{
    auto alloc = GetAllocation(pointer);
    if (alloc) {
        offset = alloc->offset;
        return true;
    }
    offset = 0;
    return false;
}

bool ObmmMmap::Commit(void *addr, size_t offset, size_t length)
{
    ptrdiff_t offsetInMmap = 0;
    if (!InRange(addr, offsetInMmap)) {
        return true;
    }
    VLOG(1) << "fallocate fd:" << fd_ << ", offsetInMmap:" << offsetInMmap << ", offset:" << offset
            << ", length:" << length;
    if (offsetInMmap < 0 || length > SSIZE_MAX || offset > SSIZE_MAX
        || offsetInMmap > SSIZE_MAX - static_cast<ssize_t>(offset)) {
        LOG(ERROR) << "Invalid Commit info: offsetInMmap:" << offsetInMmap << ", offset:" << offset
                   << ", length:" << length;
        return true;
    }
    return false;
}

bool ObmmMmap::Decommit(void *addr, size_t offset, size_t length)
{
    (void)addr;
    (void)offset;
    (void)length;
    if (IsUrmaEnabled()) {
        // if urma is enabled memory is pinned
        // Decommit is a noop
        return false;
    }
    return false;
}

Optional<Allocation> ObmmMmap::GetAllocation(void *pointer) const
{
    PerfPoint point(PerfKey::ALLOCATE_GET_MAP);
    auto *addr = reinterpret_cast<unsigned char *>(pointer);
    auto *head = reinterpret_cast<unsigned char *>(pointer_);
    auto *tail = head + static_cast<ptrdiff_t>(mmapSize_);
    // If addr is in range [head, tail), we found the mmap region.
    if (addr >= head && addr < tail) {
        auto offset = static_cast<ptrdiff_t>(addr - head);
        return Optional<Allocation>(pointer, memId_, offset, mmapSize_);
    }
    return Optional<Allocation>();
}

}  // namespace memory
}  // namespace datasystem