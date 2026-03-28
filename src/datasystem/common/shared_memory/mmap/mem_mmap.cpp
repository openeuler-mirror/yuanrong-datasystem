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

#include "datasystem/common/shared_memory/mmap/mem_mmap.h"

#include <fcntl.h>
#include <optional>
#include <sstream>
#ifdef __linux__
#include <linux/memfd.h>
#endif
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <limits.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/shared_memory/mmap/allocation.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"

#ifndef DISABLE_RDMA
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#endif

DS_DEFINE_bool(enable_fallocate, true,
               "Due to Kubernetes' (k8s) resource calculation policies, "
               "shared memory is sometimes counted twice,"
               "which can lead to client crashes.To address this issue, "
               "fallocate is employed to link the client and worker nodes for shared memory,"
               "Enabling fallocate will lower the efficiency of memory allocation"
               "By default, fallocate is enabled.");

namespace datasystem {
namespace memory {

Status MemMmap::Initialize(uint64_t size, bool populate, bool hugepage)
{
    // 1. Create tmpfs file.
    std::string tmpfs = "datasystem";
    std::string hugeTlbOpenHint = "";

    unsigned int mfdFlag = MFD_ALLOW_SEALING;
    if (hugepage) {
#ifdef MFD_HUGETLB
        mfdFlag = MFD_ALLOW_SEALING | MFD_HUGETLB;
        hugeTlbOpenHint = " huge tlb is open, this probably means you have to increase /proc/sys/vm/nr_hugepages";
#else
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Huge tlb not support!");
#endif
    }

    // memfd_create is not defined in EulerOS, use syscall instead for compatibility purposes.
    fd_ = syscall(SYS_memfd_create, tmpfs.c_str(), mfdFlag);
    CHECK_FAIL_RETURN_STATUS(fd_ >= 0, StatusCode::K_RUNTIME_ERROR,
                             "memfd_create failed: " + StrErr(errno) + hugeTlbOpenHint);

    unsigned int flags = MAP_SHARED;
    if (hugepage) {
        flags = MAP_SHARED | MAP_HUGETLB;
    }
    if (populate) {
        flags |= MAP_POPULATE;
    }
    type_ = "memory";
    Status rc = SetupFileMapping(size, flags, true);
#ifndef DISABLE_RDMA
    if (rc.IsOk()) {
        // If urma or rdma is enabled, register the memory.
        RETURN_IF_NOT_OK(RegisterFastTransportMemory(pointer_, mmapSize_));
        // If Remote H2D is enabled, pin and register the npu memory
        RETURN_IF_NOT_OK(RegisterHostMemory(pointer_, mmapSize_));
    }
#endif
    return rc;
}

bool MemMmap::InRange(void *pointer, ptrdiff_t &offset)
{
    auto alloc = GetAllocation(pointer);
    if (alloc) {
        offset = alloc->offset;
        return true;
    }
    offset = 0;
    return false;
}

bool MemMmap::Commit(void *addr, size_t offset, size_t length)
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
    if (!FLAGS_enable_fallocate) {
        return false;
    }
    // retry for EINTR
    int remainRetryTimes = 3;
    while (fallocate(fd_, 0, static_cast<ssize_t>(offsetInMmap + offset), static_cast<ssize_t>(length)) != 0) {
        LOG(ERROR) << "Failed to fallocate file:" << StrErr(errno);
        remainRetryTimes--;
        if (errno != EINTR || remainRetryTimes == 0) {
            return true;
        }
    }
    return false;
}

bool MemMmap::Decommit(void *addr, size_t offset, size_t length)
{
#ifndef DISABLE_RDMA
    if (IsFastTransportEnabled() || IsRemoteH2DEnabled()) {
        // if urma/rdma/RH2D is enabled memory is pinned
        // Decommit is a noop
        return false;
    }
#endif
    if (!FLAGS_enable_fallocate) {
        return false;
    }
    return BaseMmap::Decommit(addr, offset, length);
}

}  // namespace memory
}  // namespace datasystem