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
#include "datasystem/client/mmap/immap_table.h"

#include <atomic>
#include <cstddef>
#include <shared_mutex>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/flags/flags.h"
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
}

void IMmapTable::CleanInvalidMmapTable()
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    mmapTable_.clear();
}

void IMmapTable::ClearExpiredFds(const std::vector<int64_t> &fds)
{
    LOG(INFO) << "Clear expired workerfds: " << VectorToString(fds);
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    for (auto fd : fds) {
        mmapTable_.erase(fd);
    }
}

std::shared_ptr<IMmapTableEntry> IMmapTable::GetMmapEntryByFd(int fd)
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto iter = mmapTable_.find(fd);
    return iter == mmapTable_.end() ? nullptr : iter->second;
}
}  // namespace client
}  // namespace datasystem
