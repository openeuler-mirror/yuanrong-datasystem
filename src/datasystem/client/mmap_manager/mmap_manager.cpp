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
#include "datasystem/client/mmap_manager/mmap_manager.h"

#include <unistd.h>

#include "datasystem/client/mmap_manager/embedded_mmap_table.h"
#include "datasystem/client/mmap_manager/shm_mmap_table.h"

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

MmapManager::MmapManager(std::shared_ptr<IShmFdProvider> fdProvider, bool enableHugeTlb)
    : fdProvider_(std::move(fdProvider)), mmapTable_(std::make_unique<ShmMmapTable>(enableHugeTlb)),
      enableEmbeddedClient_(false)
{
}

MmapManager::~MmapManager()
{
}

void MmapManager::CloseFdsFrom(const std::vector<int> &clientFds, size_t fromIdx)
{
    for (size_t j = fromIdx; j < clientFds.size(); ++j) {
        if (clientFds[j] >= 0) {
            RETRY_ON_EINTR(close(clientFds[j]));
        }
    }
}

void MmapManager::CloseAllFds(const std::vector<int> &clientFds)
{
    CloseFdsFrom(clientFds, 0);
}

Status MmapManager::ReceiveAndMmapClientFds(const std::string &tenantId, const std::vector<int> &toRecvFds,
                                            const std::vector<uint64_t> &mmapSizes)
{
    if (enableEmbeddedClient_) {
        for (size_t i = 0; i < toRecvFds.size(); i++) {
            static const int unusedClientFd = 0;  // for embeddedclient, no need mmap client fd.
            RETURN_IF_NOT_OK(mmapTable_->MmapAndStoreFd(unusedClientFd, toRecvFds[i], mmapSizes[i], tenantId));
        }
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(clientWorker_ != nullptr || fdProvider_ != nullptr, K_RUNTIME_ERROR,
                             "No shared-memory fd provider is configured");
    const std::string clientId = fdProvider_ != nullptr ? fdProvider_->ClientId() : clientWorker_->clientId_;
    std::vector<int> clientFds;
    Status fdRc = fdProvider_ != nullptr ? fdProvider_->GetClientFd(toRecvFds, clientFds, tenantId)
                                        : clientWorker_->GetClientFd(toRecvFds, clientFds, tenantId);
    if (fdRc.IsError()) {
        CloseAllFds(clientFds);
        return fdRc;
    }
    if (clientFds.size() != toRecvFds.size()) {
        CloseAllFds(clientFds);
        RETURN_STATUS(K_RUNTIME_ERROR, "Received shared-memory fd count does not match request");
    }
    for (size_t i = 0; i < clientFds.size(); i++) {
        if (clientFds[i] <= 0) {
            CloseFdsFrom(clientFds, i);
            RETURN_STATUS(K_RUNTIME_ERROR, "Received an invalid shared-memory client fd");
        }
        Status mmapRc = mmapTable_->MmapAndStoreFd(clientFds[i], toRecvFds[i], mmapSizes[i], tenantId, clientId);
        if (mmapRc.IsError()) {
            // ShmMmapTable consumes the fd only after mmap succeeds. Close the failed fd and every
            // unprocessed SCM_RIGHTS fd so a partial mmap cannot leak descriptors.
            CloseFdsFrom(clientFds, i);
            return mmapRc;
        }
    }
    return Status::OK();
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
        bthread::RWLockRdGuard lck(mutex_);
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
        std::lock_guard<bthread::Mutex> fdTransferLock(fdTransferMutex_);
        toRecvFds.clear();
        toRecvFdInUnitIdx.clear();
        mmapSizes.clear();
        RETURN_IF_NOT_OK(classifyUnits(true));

        // Notify worker to send fds and receive the client fd.
        if (!toRecvFds.empty()) {
            RETURN_IF_NOT_OK(ReceiveAndMmapClientFds(tenantId, toRecvFds, mmapSizes));

            bthread::RWLockRdGuard lck(mutex_);
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
    bthread::RWLockRdGuard lck(mutex_);
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
    bthread::RWLockWrGuard lck(mutex_);
    if (mmapTable_) {
        mmapTable_->ClearExpiredFds(fds);
    }
}

void MmapManager::AssociateShmId(int workerFd, const std::string &shmId)
{
    bthread::RWLockRdGuard lck(mutex_);
    if (mmapTable_) {
        mmapTable_->AssociateShmId(workerFd, shmId);
    }
}

int MmapManager::GetWorkerFdByShmId(const std::string &shmId) const
{
    bthread::RWLockRdGuard lck(mutex_);
    return mmapTable_ ? mmapTable_->GetWorkerFdByShmId(shmId) : -1;
}

void MmapManager::ClearExpiredByShmId(const std::string &shmId, const std::vector<int64_t> &fds)
{
    bthread::RWLockRdGuard lck(mutex_);
    if (mmapTable_) {
        mmapTable_->ClearExpiredByShmId(shmId, fds);
    }
}

void MmapManager::ClearByShmId(const std::string &shmId)
{
    bthread::RWLockRdGuard lck(mutex_);
    if (mmapTable_) {
        mmapTable_->ClearByShmId(shmId);
    }
}

std::vector<int64_t> MmapManager::GetFds()
{
    bthread::RWLockRdGuard lck(mutex_);
    return mmapTable_ == nullptr ? std::vector<int64_t>{} : mmapTable_->GetFds();
}

void MmapManager::Clear()
{
    bthread::RWLockWrGuard lck(mutex_);
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
