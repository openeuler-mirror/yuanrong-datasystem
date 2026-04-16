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
#include "datasystem/client/mmap/shm_mmap_table.h"

#include "datasystem/client/mmap/embedded_mmap_table.h"
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
    auto classifyUnits = [&](bool fillExistingPointers) -> Status {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        int pageIdx = 0;
        for (auto &unit : units) {
            if (mmapTable_->FindFd(unit->fd)) {
                if (fillExistingPointers) {
                    uint8_t *pointer = nullptr;
                    RETURN_IF_NOT_OK(mmapTable_->LookupFdPointer(unit->fd, &pointer));
                    CHECK_FAIL_RETURN_STATUS(pointer != nullptr, StatusCode::K_RUNTIME_ERROR,
                                             "The pointer which is looked up from mmap table is nullptr!");
                    unit->pointer = static_cast<void *>(pointer);
                }
            } else {
                auto it = find(toRecvFds.begin(), toRecvFds.end(), unit->fd);
                if (it == toRecvFds.end()) {
                    toRecvFds.emplace_back(unit->fd);
                    mmapSizes.emplace_back(unit->mmapSize);
                }
                toRecvFdInUnitIdx.emplace_back(pageIdx);
            }
            ++pageIdx;
        }
        return Status::OK();
    };

    // Phase 1: fast lookup path under a shared manager lock.
    RETURN_IF_NOT_OK(classifyUnits(true));

    // Phase 2: serialize fd transfer on the shared socket path, then re-check missed fds
    // to avoid duplicate fd requests and unsafe concurrent RecvFdAfterNotify waits.
    if (!toRecvFds.empty()) {
        std::lock_guard<std::mutex> fdTransferLock(fdTransferMutex_);
        toRecvFds.clear();
        toRecvFdInUnitIdx.clear();
        mmapSizes.clear();
        RETURN_IF_NOT_OK(classifyUnits(true));

        // Notify worker to send fds and receive the client fd.
        if (!toRecvFds.empty()) {
            if (!enableEmbeddedClient_) {
                RETURN_IF_NOT_OK(clientWorker_->GetClientFd(toRecvFds, clientFds, tenantId));
                // Mmap the new client fd.
                for (size_t i = 0; i < clientFds.size(); i++) {
                    RETURN_IF_NOT_OK(mmapTable_->MmapAndStoreFd(clientFds[i], toRecvFds[i], mmapSizes[i], tenantId));
                }
            } else {
                for (size_t i = 0; i < toRecvFds.size(); i++) {
                    static const int unusedClientFd = 0; // for embeddedclient, no need mmap client fd.
                    RETURN_IF_NOT_OK(mmapTable_->MmapAndStoreFd(unusedClientFd, toRecvFds[i], mmapSizes[i], tenantId));
                }
            }

            std::shared_lock<std::shared_timed_mutex> lck(mutex_);
            for (auto &idx : toRecvFdInUnitIdx) {
                auto unit = units[idx];
                uint8_t *pointer = nullptr;
                RETURN_IF_NOT_OK(mmapTable_->LookupFdPointer(unit->fd, &pointer));
                CHECK_FAIL_RETURN_STATUS(pointer != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "The pointer which is looked up from mmap table is nullptr!");
                unit->pointer = static_cast<void *>(pointer);
            }
        }
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
