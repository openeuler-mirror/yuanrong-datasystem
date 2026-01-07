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
#include "datasystem/client/mmap/shm_mmap_table_entry.h"

#include <atomic>
#include <cstddef>
#include <shared_mutex>
#include <sys/mman.h>
#include <unistd.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace client {
Status ShmMmapTableEntry::Init(bool enableHugeTlb, const std::string &tenantId)
{
    (void)tenantId;
    std::stringstream err;
    if (size_ <= 0) {
        err << "The mmap size [" << size_ << "] is invalid for fd [" << fd_ << "]";
        LOG(ERROR) << err.str();
        RETURN_STATUS(StatusCode::K_INVALID, err.str());
    }
    INJECT_POINT("IMmapTableEntry.mmap");
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

ShmMmapTableEntry::~ShmMmapTableEntry()
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
