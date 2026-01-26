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
 * Description: Client mmap management.
 */
#include "datasystem/client/mmap_manager.h"

#include "datasystem/client/mmap/embedded_mmap_table.h"
#include "datasystem/client/mmap/shm_mmap_table.h"

namespace datasystem {
namespace client {
MmapManager::MmapManager(std::shared_ptr<IClientWorkerCommonApi> clientWorker, bool enableEmbeddedClient)
    : clientWorker_(std::move(clientWorker)), enableEmbeddedClient_(enableEmbeddedClient)
{
    if (!enableEmbeddedClient) {
        mmapTable_ = std::make_unique<ShmMmapTable>(clientWorker_->enableHugeTlb_);
    } else {
        mmapTable_ = std::make_unique<EmbeddedMmapTable>(clientWorker_->enableHugeTlb_);
    }
}

MmapManager::~MmapManager()
{
}

Status MmapManager::LookupUnitsAndMmapFd(const std::string &tenantId, const std::shared_ptr<ShmUnitInfo> &unit)
{
    std::vector<std::shared_ptr<ShmUnitInfo>> units(1);
    units[0] = unit;
    return LookupUnitsAndMmapFds(tenantId, units);
}

Status MmapManager::LookupUnitsAndMmapFds(const std::string &tenantId, std::vector<std::shared_ptr<ShmUnitInfo>> &units)
{
    std::vector<int> toRecvFds;
    std::vector<int> toRecvFdInUnitIdx;
    std::vector<uint64_t> mmapSizes;
    std::vector<int> clientFds;

    // Loop all units and find the ummap fd.
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    int pageIdx = 0;
    for (auto &unit : units) {
        if (mmapTable_->FindFd(unit->fd)) {
            uint8_t *pointer = nullptr;
            RETURN_IF_NOT_OK(mmapTable_->LookupFdPointer(unit->fd, &pointer));
            CHECK_FAIL_RETURN_STATUS(pointer != nullptr, StatusCode::K_RUNTIME_ERROR,
                                     "The pointer which is looked up from mmap table is nullptr!");
            unit->pointer = (void *)pointer;
        } else {
            auto it = find(toRecvFds.begin(), toRecvFds.end(), unit->fd);
            // The unit fd is not in mmapTable and not in toRecvFds, just append to the toRecvFds.
            if (it == toRecvFds.end()) {
                toRecvFds.emplace_back(unit->fd);
                mmapSizes.emplace_back(unit->mmapSize);
            }
            // Record every unit index to toRecvFdInUnitIdx.
            toRecvFdInUnitIdx.emplace_back(pageIdx);
        }
        ++pageIdx;
    }

    if (!toRecvFds.empty()) {
        if (!enableEmbeddedClient_) {
            RETURN_IF_NOT_OK(clientWorker_->GetClientFd(toRecvFds, clientFds, tenantId));
            // Mmap the new client fd.
            for (size_t i = 0; i < clientFds.size(); i++) {
                RETURN_IF_NOT_OK(mmapTable_->MmapAndStoreFd(clientFds[i], toRecvFds[i], mmapSizes[i], tenantId));
            }
        } else {
            for (size_t i = 0; i < toRecvFds.size(); i++) {
                static const int unusedClientFd = 0;  // for embeddedclient, no need mmap client fd.
                RETURN_IF_NOT_OK(mmapTable_->MmapAndStoreFd(unusedClientFd, toRecvFds[i], mmapSizes[i], tenantId));
            }
        }
    }

    // Loop the units for needing to update pointer and fill the share memory pointer value.
    for (auto &idx : toRecvFdInUnitIdx) {
        // fill the share memory pointer in the client mmap file.
        auto unit = units[idx];
        uint8_t *pointer = nullptr;
        RETURN_IF_NOT_OK(mmapTable_->LookupFdPointer(unit->fd, &pointer));
        CHECK_FAIL_RETURN_STATUS(pointer != nullptr, StatusCode::K_RUNTIME_ERROR,
                                 "The pointer which is looked up from mmap table is nullptr!");
        unit->pointer = static_cast<void *>(pointer);
    }

    return Status::OK();
}

uint8_t *MmapManager::LookupMmappedFile(const int storeFd)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    uint8_t *pointer = nullptr;
    if (mmapTable_->FindFd(storeFd)) {
        Status rc = mmapTable_->LookupFdPointer(storeFd, &pointer);
        if (rc.IsError()) {
            LOG(ERROR) << "mmap table lookup fd pointer failed: " << rc.ToString();
            return nullptr;
        }
    }
    return pointer;
}

std::shared_ptr<IMmapTableEntry> MmapManager::GetMmapEntryByFd(int fd)
{
    return mmapTable_->GetMmapEntryByFd(fd);
}

void MmapManager::ClearExpiredFds(const std::vector<int64_t> &fds)
{
    if (fds.empty()) {
        return;
    }
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    if (mmapTable_) {
        mmapTable_->ClearExpiredFds(fds);
    }
}

void MmapManager::Clear()
{
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    if (mmapTable_) {
        mmapTable_->Clear();
    }
}

void MmapManager::CleanInvalidMmapTable()
{
    mmapTable_->CleanInvalidMmapTable();
}
}  // namespace client
}  // namespace datasystem
