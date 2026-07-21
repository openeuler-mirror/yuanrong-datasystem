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
#include "datasystem/client/mmap/immap_table.h"

#include <shared_mutex>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/strings_util.h"
namespace datasystem {
namespace client {
IMmapTable::IMmapTable(bool enableHugeTlb) : enableHugeTlb_(enableHugeTlb)
{
}

Status IMmapTable::LookupFdPointer(const int &workerFd, uint8_t **pointer)
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

bool IMmapTable::FindFd(const int &workerFd)
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto entry = mmapTable_.find(workerFd);
    return entry != mmapTable_.end();
}

void IMmapTable::Clear()
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    mmapTable_.clear();
    shmIdToWorkerFd_.clear();
}

void IMmapTable::CleanInvalidMmapTable()
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    mmapTable_.clear();
    shmIdToWorkerFd_.clear();
}

void IMmapTable::ClearExpiredFds(const std::vector<int64_t> &fds)
{
    LOG(INFO) << "Clear expired workerfds: " << VectorToString(fds);
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    for (auto fd : fds) {
        // Drop the shm_id reverse entry too, if the freed fd was associated with one.
        for (auto it = shmIdToWorkerFd_.begin(); it != shmIdToWorkerFd_.end(); ++it) {
            if (it->second == fd) {
                shmIdToWorkerFd_.erase(it);
                break;
            }
        }
        mmapTable_.erase(fd);
    }
}

std::vector<int64_t> IMmapTable::GetFds()
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    std::vector<int64_t> fds;
    fds.reserve(mmapTable_.size());
    for (const auto &entry : mmapTable_) {
        fds.emplace_back(entry.first);
    }
    return fds;
}

std::shared_ptr<IMmapTableEntry> IMmapTable::GetMmapEntryByFd(int fd)
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto iter = mmapTable_.find(fd);
    return iter == mmapTable_.end() ? nullptr : iter->second;
}

void IMmapTable::AssociateShmId(int workerFd, const std::string &shmId)
{
    if (shmId.empty()) {
        return;
    }
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    auto it = mmapTable_.find(workerFd);
    if (it == mmapTable_.end()) {
        LOG(WARNING) << "AssociateShmId: worker fd " << workerFd << " not in table, skip";
        return;
    }
    it->second->SetShmId(shmId);
    shmIdToWorkerFd_[shmId] = workerFd;
}

int IMmapTable::GetWorkerFdByShmId(const std::string &shmId)
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto it = shmIdToWorkerFd_.find(shmId);
    return it == shmIdToWorkerFd_.end() ? -1 : it->second;
}

void IMmapTable::ClearExpiredByShmId(const std::string &shmId, const std::vector<int64_t> &fds)
{
    if (shmId.empty() || fds.empty()) {
        return;
    }
    // Hold the write lock across lookup + reclaim so a concurrent AssociateShmId/ClearByShmId on the
    // same shm_id cannot race (review fix #4). The fd is resolved via the shm_id reverse index, so
    // worker A's expired fds never reclaim worker B's entry (review fix #3).
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    auto it = shmIdToWorkerFd_.find(shmId);
    if (it == shmIdToWorkerFd_.end()) {
        // No mapping for this shm_id: do not fall back to a global clear — that would risk reclaiming
        // another worker's entry (the very bug UC6 forbids).
        return;
    }
    int workerFd = it->second;
    for (auto fd : fds) {
        if (fd == workerFd) {
            mmapTable_.erase(fd);
        }
    }
}

void IMmapTable::ClearByShmId(const std::string &shmId)
{
    if (shmId.empty()) {
        return;
    }
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    auto it = shmIdToWorkerFd_.find(shmId);
    if (it == shmIdToWorkerFd_.end()) {
        return; // idempotent: nothing associated with this shm_id.
    }
    int workerFd = it->second;
    mmapTable_.erase(workerFd);
    shmIdToWorkerFd_.erase(it);
}
}  // namespace client
}  // namespace datasystem
