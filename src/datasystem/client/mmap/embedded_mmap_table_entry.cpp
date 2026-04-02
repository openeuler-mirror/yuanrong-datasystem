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
 * Description: Client mmap table management.
 */
#include "datasystem/client/mmap/embedded_mmap_table_entry.h"

#include <atomic>
#include <cstddef>
#include <shared_mutex>
#include <sys/mman.h>
#include <unistd.h>

#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/arena.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
namespace client {
Status EmbeddedMmapTableEntry::Init(bool enableHugeTlb, const std::string &tenantId)
{
    (void)enableHugeTlb;
    std::stringstream err;
    if (size_ <= 0) {
        err << "The mmap size [" << size_ << "] is invalid for fd [" << fd_ << "]";
        LOG(ERROR) << err.str();
        RETURN_STATUS(StatusCode::K_INVALID, err.str());
    }

    std::string realTenantId = !tenantId.empty() ? tenantId : g_ContextTenantId;
    std::vector<std::shared_ptr<memory::ArenaGroup>> arenaGroups;
    for (int i = 0; i < static_cast<int>(memory::CacheType::DEV_HOST); i++) {
        memory::ArenaGroupKey key;
        key.tenantId = realTenantId;
        key.type = static_cast<memory::CacheType>(i);
        std::shared_ptr<memory::ArenaGroup> arenaGroup;
        auto status = memory::Allocator::Instance()->GetArenaManager()->GetArenaGroup(key, arenaGroup);
        if (status.IsOk()) {
            arenaGroups.emplace_back(arenaGroup);
        }
    }
    for (const auto &arenaGroup : arenaGroups) {
        std::pair<void *, uint64_t> ptrMmapSz;
        if (arenaGroup->FdToPointer(fd_, ptrMmapSz).IsOk()) {
            pointer_ = static_cast<uint8_t *>(ptrMmapSz.first);
            size_ = ptrMmapSz.second;
            break;
        }
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pointer_ != nullptr, K_RUNTIME_ERROR,
                                         FormatString("cant find fd: %d in arena group", fd_));
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
}  // namespace client
}  // namespace datasystem
