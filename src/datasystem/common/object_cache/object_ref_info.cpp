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
 * Description: Defines object reference info class ObjectMemoryRefTable class and ObjectGlobalRefTable.
 */
#include "datasystem/common/object_cache/object_ref_info.h"

#include <algorithm>
#include <chrono>
#include <cmath>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr uint64_t SHM_REF_RECONCILE_MIN_EXPIRE_MS = 1000UL;
constexpr uint64_t SHM_REF_RECONCILE_MAX_EXPIRE_MS = RPC_TIMEOUT;
constexpr uint64_t SHM_REF_FLUSH_INTERVAL_MS = 1000UL;
constexpr size_t MAX_MAYBE_EXPIRED_SHM_IDS_PER_RECONCILE = 1024;
uint64_t GetCurrentTimeMs()
{
    INJECT_POINT("shm_ref.GetCurrentTimeMs", [](uint64_t value) { return value; });
    return static_cast<uint64_t>(GetSteadyClockTimeStampMs());
}
uint64_t GetMaybeExpiredDeadlineMs(int64_t requestTimeoutMs)
{
    auto expireMs = std::min<uint64_t>(std::max<int64_t>(requestTimeoutMs, SHM_REF_RECONCILE_MIN_EXPIRE_MS),
                                       SHM_REF_RECONCILE_MAX_EXPIRE_MS);
    return GetCurrentTimeMs() + expireMs;
}
}  // namespace

SharedMemoryRefTable::SharedMemoryRefTable()
{
    if (maybeExpiredFlushThread_ != nullptr) {
        return;
    }
    maybeExpiredFlushExit_ = false;
    maybeExpiredFlushPost_.Clear();
    maybeExpiredFlushThread_ = std::make_unique<Thread>([this]() {
        while (!maybeExpiredFlushExit_) {
            (void)maybeExpiredFlushPost_.WaitFor(SHM_REF_FLUSH_INTERVAL_MS);
            if (maybeExpiredFlushExit_) {
                break;
            }
            FlushMaybeExpiredQueue(GetCurrentTimeMs());
        }
    });
}

SharedMemoryRefTable::~SharedMemoryRefTable()
{
    if (maybeExpiredFlushThread_ == nullptr) {
        return;
    }
    maybeExpiredFlushExit_ = true;
    maybeExpiredFlushPost_.Set();
    maybeExpiredFlushThread_->join();
}

Status SharedMemoryRefTable::GetShmUnit(const ShmKey &shmId, std::shared_ptr<ShmUnit> &shmUnit)
{
    TbbMemoryObjectRefTable::const_accessor shmAccessor;
    auto found = shmRefTable_.find(shmAccessor, shmId);
    if (!found) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("Get a not found shm: %s ", shmId));
    }
    shmUnit = shmAccessor->second.first;
    return Status::OK();
}

void SharedMemoryRefTable::ClientTableGetOrInsert(const ClientKey &clientId,
                                                  TbbMemoryClientRefTable::const_accessor &accessor)
{
    // return read lock until the end of scope.
    while (!clientRefTable_.find(accessor, clientId)) {
        TbbMemoryClientRefTable::accessor writeAccessor;
        // Add write lock to avoid multiple insertions.
        if (clientRefTable_.insert(writeAccessor, clientId)) {
            auto clientInfo = std::make_shared<ObjectRefInfo<ShmKey>>();
            writeAccessor->second = std::move(clientInfo);
        }
    }
}

void SharedMemoryRefTable::InitShmRefForClient(const ClientKey &clientId, bool supportMultiShmRefCount)
{
    LOG(INFO) << "Initializing shared memory reference for client " << clientId
              << ", supportMultiShmRefCount: " << supportMultiShmRefCount;
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.insert(accessor, clientId)) {
        // Set isUniqueCnt to false if the client supports multiple shared memory reference counts,
        auto clientInfo = std::make_shared<ObjectRefInfo<ShmKey>>(!supportMultiShmRefCount);
        accessor->second = std::move(clientInfo);
    } else {
        LOG(WARNING) << "Client " << clientId << " already exists";
    }
}

void SharedMemoryRefTable::AddShmUnit(const ClientKey &clientId, std::shared_ptr<ShmUnit> &shmUnit,
                                      int64_t requestTimeoutMs)
{
    const auto &shmId = shmUnit->GetId();
    TbbMemoryClientRefTable::const_accessor clientAccessor;
    TbbMemoryObjectRefTable::accessor objectAccessor;

    ClientTableGetOrInsert(clientId, clientAccessor);
    if (clientAccessor->second->AddRef(shmId)) {
        shmUnit->IncrementRefCount();
    }
    if (!shmRefTable_.find(objectAccessor, shmId)) {
        shmRefTable_.emplace(objectAccessor, shmId, std::make_pair(shmUnit, std::unordered_set<ClientKey>()));
    }

    objectAccessor->second.second.emplace(clientId);

    if (shmUnit->GetRefCount() == 1) {
        datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(-1);
        datasystem::memory::Allocator::Instance()->ChangeRefPageCount(1);
    }
    RecordMaybeExpiredShm(clientId, shmId, requestTimeoutMs);
    VLOG(1) << "AddShmUnit for shmid: " << shmUnit->id << " client id: " << clientId;
}

void SharedMemoryRefTable::AddShmUnits(TbbMemoryClientRefTable::const_accessor &clientAccessor,
                                       std::vector<std::shared_ptr<ShmUnit>> &shmUnits, int64_t requestTimeoutMs)
{
    TbbMemoryObjectRefTable::accessor objectAccessor;
    const ClientKey &clientId = clientAccessor->first;
    for (auto &shmUnit : shmUnits) {
        if (shmUnit == nullptr) {
            continue;
        }
        const auto &shmId = shmUnit->GetId();
        if (clientAccessor->second->AddRef(shmId)) {
            shmUnit->IncrementRefCount();
        }
        if (!shmRefTable_.find(objectAccessor, shmId)) {
            shmRefTable_.emplace(objectAccessor, shmId,
                                 std::make_pair(shmUnit, std::unordered_set<ClientKey>{ clientId }));
        } else {
            objectAccessor->second.second.emplace(clientId);
        }
        objectAccessor.release();

        if (shmUnit->GetRefCount() == 1) {
            datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(-1);
            datasystem::memory::Allocator::Instance()->ChangeRefPageCount(1);
        }
        RecordMaybeExpiredShm(clientId, shmId, requestTimeoutMs);
        VLOG(1) << "AddShmUnit for shmid: " << shmUnit->id << " client id: " << clientId;
    }
}

Status SharedMemoryRefTable::RemoveShmUnit(const ClientKey &clientId, const ShmKey &shmId)
{
    TbbMemoryClientRefTable::accessor clientAccessor;
    TbbMemoryObjectRefTable::accessor shmAccessor;

    // first lock clientAccessor, then lock shmAccessor.
    auto found = clientRefTable_.find(clientAccessor, clientId);
    if (!found) {
        LOG(WARNING) << FormatString("The clientId not exists in clientRefTable_, clientId: %s, shmId: %s", clientId,
                                     shmId);
        return Status::OK();
    }
    if (!shmRefTable_.find(shmAccessor, shmId)) {
        LOG(WARNING) << FormatString("The shmId not exists in shmRefTable_, clientId: %s, shmId: %s", clientId, shmId);
        return Status::OK();
    }
    auto shmUnit = shmAccessor->second.first;
#ifdef WITH_TESTS
    INJECT_POINT("RemoveShmUnit");
#endif
    RemoveShmUnitDetail(clientId, false, shmAccessor, clientAccessor);
    ClearMaybeExpiredShmIds(clientId, { shmId });
    VLOG(1) << "RemoveShmUnit for shmid: " << shmId << " client id: " << clientId;
    return Status::OK();
}

void SharedMemoryRefTable::RemoveShmUnitDetail(const ClientKey &clientId, bool forceRemoveClient,
                                               TbbMemoryObjectRefTable::accessor &shmAccessor,
                                               TbbMemoryClientRefTable::accessor &clientAccessor)
{
    const auto &shmUnit = shmAccessor->second.first;
    const auto shmId = shmUnit->GetId();
    // Step 1: Update Client Table.
    uint32_t refCount = 0;
    if (clientAccessor->second->RemoveAndGetRefCnt(shmId, refCount)) {
        if (shmUnit->GetRefCount() > 0) {
            shmUnit->DecrementRefCount();
        } else {
            LOG(WARNING) << "RemoveShmUnit: The value of refCount is 0 and cannot be decreased. id:"
                         << BytesUuidToString(shmId.ToString());
        }
    }

    // Step 2: Update Object Table.
    if (shmUnit->GetRefCount() == 0) {
        datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(1);
        datasystem::memory::Allocator::Instance()->ChangeRefPageCount(-1);
    }

    // skip if the ref count for shmId is not zero.
    if (!forceRemoveClient && refCount > 0) {
        return;
    }
    auto &clientIds = shmAccessor->second.second;
    (void)clientIds.erase(clientId);
    if (clientIds.empty()) {
        VLOG(1) << FormatString("Erase shmId: %s", shmAccessor->first);
        if (!shmRefTable_.erase(shmAccessor)) {
            LOG(ERROR) << FormatString("Fail to erase shmId: %s", shmAccessor->first);
        }
    }
}

#ifdef WITH_TESTS
bool SharedMemoryRefTable::Contains(const ClientKey &clientId, const ShmKey &shmId) const
{
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.find(accessor, clientId)) {
        return accessor->second->Contains(shmId);
    }
    return false;
}
#endif

void SharedMemoryRefTable::GetClientRefIds(const ClientKey &clientId, std::vector<ShmKey> &shmIds) const
{
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.find(accessor, clientId)) {
        accessor->second->GetRefIds(shmIds);
    }
}

void SharedMemoryRefTable::RecordMaybeExpiredShm(const ClientKey &clientId, const ShmKey &shmId,
                                                 int64_t requestTimeoutMs)
{
    auto expireTimeMs = GetMaybeExpiredDeadlineMs(requestTimeoutMs);
    std::lock_guard<std::shared_mutex> lock(maybeExpiredShmQueueMutex_);
    maybeExpiredShmQueue_.push({ requestTimeoutMs, expireTimeMs, clientId, shmId });
    VLOG(1) << "RecordMaybeExpiredShm: clientId=" << clientId << " shmId=" << shmId
               << " requestTimeoutMs=" << requestTimeoutMs << " expireTimeMs=" << expireTimeMs;
}

void SharedMemoryRefTable::GetMaybeExpiredShmIds(const ClientKey &clientId, std::vector<ShmKey> &shmIds)
{
    shmIds.clear();
    std::shared_lock<std::shared_mutex> lock(maybeExpiredShmTableMutex_);
    auto iter = maybeExpiredShmTable_.find(clientId);
    if (iter == maybeExpiredShmTable_.end()) {
        return;
    }
    auto count = std::min(iter->second.size(), MAX_MAYBE_EXPIRED_SHM_IDS_PER_RECONCILE);
    shmIds.reserve(count);
    auto shmIter = iter->second.begin();
    for (size_t i = 0; i < count; ++i, ++shmIter) {
        shmIds.emplace_back(*shmIter);
    }
}

void SharedMemoryRefTable::ClearMaybeExpiredShmIds(const ClientKey &clientId, const std::vector<ShmKey> &shmIds)
{
    if (shmIds.empty()) {
        return;
    }
    std::lock_guard<std::shared_mutex> lock(maybeExpiredShmTableMutex_);
    auto iter = maybeExpiredShmTable_.find(clientId);
    if (iter == maybeExpiredShmTable_.end()) {
        return;
    }
    for (const auto &shmId : shmIds) {
        (void)iter->second.erase(shmId);
    }
    if (iter->second.empty()) {
        maybeExpiredShmTable_.erase(iter);
    }
}

void SharedMemoryRefTable::ReconcileClientShmRefs(const ClientKey &clientId,
                                                  const std::vector<ShmKey> &confirmedExpiredShmIds,
                                                  std::vector<ShmKey> &maybeExpiredShmIds)
{
    // clear confirmed expired shm ids.
    for (const auto &shmId : confirmedExpiredShmIds) {
        LOG(INFO) << "confirmed expired, try decreasing ref count for " << shmId << ", client: " << clientId;
        Status rc = RemoveShmUnit(clientId, shmId);
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("[ObjectKey %s] DoDecrease failed, error: %s", shmId, rc.ToString());
        }
    }

    ClearMaybeExpiredShmIds(clientId, confirmedExpiredShmIds);
    GetMaybeExpiredShmIds(clientId, maybeExpiredShmIds);
}

Status SharedMemoryRefTable::RemoveClient(const ClientKey &clientId)
{
    TbbMemoryClientRefTable::accessor clientAccessor;
    if (!clientRefTable_.find(clientAccessor, clientId)) {
        return Status::OK();
    }
    std::vector<ShmKey> shmIds;
    clientAccessor->second->GetRefIds(shmIds);
    for (const auto &shmId : shmIds) {
        TbbMemoryObjectRefTable::accessor shmAccessor;
        if (!shmRefTable_.find(shmAccessor, shmId)) {
            continue;
        }
        RemoveShmUnitDetail(clientId, true, shmAccessor, clientAccessor);
    }
    clientRefTable_.erase(clientAccessor);
    std::lock_guard<std::shared_mutex> lock(maybeExpiredShmTableMutex_);
    maybeExpiredShmTable_.erase(clientId);
    return Status::OK();
}

void SharedMemoryRefTable::FlushMaybeExpiredQueue(uint64_t nowMs)
{
    std::vector<MaybeExpiredShmItem> expiredItems;
    size_t queueSize = 0;
    {
        std::lock_guard<std::shared_mutex> lockForQueue(maybeExpiredShmQueueMutex_);
        queueSize = maybeExpiredShmQueue_.size();
        while (!maybeExpiredShmQueue_.empty() && maybeExpiredShmQueue_.top().expireTimeMs <= nowMs) {
            auto item = maybeExpiredShmQueue_.top();
            expiredItems.push_back(item);
            maybeExpiredShmQueue_.pop();
        }
    }

    constexpr int logIntervalSec = 120;
    LOG_EVERY_T(INFO, logIntervalSec) << "The size of maybeExpiredShmQueue_ is " << queueSize
                                      << ", the size of shmRefTable_ is " << shmRefTable_.size();

    std::vector<MaybeExpiredShmItem> failedItems;
    for (const auto &item : expiredItems) {
        if (!ClientContainsShm(item.clientId, item.shmId)) {
            continue;
        }
        std::lock_guard<std::shared_mutex> lockForTable(maybeExpiredShmTableMutex_);
        auto &shmIds = maybeExpiredShmTable_[item.clientId];
        if (!shmIds.emplace(item.shmId).second) {
            failedItems.emplace_back(item);
        }
    }

    // delay if already expired, re-insert into queue
    for (const auto &item : failedItems) {
        RecordMaybeExpiredShm(item.clientId, item.shmId, item.requestTimeoutMs);
    }
}

bool SharedMemoryRefTable::ClientContainsShm(const ClientKey &clientId, const ShmKey &shmId) const
{
    TbbMemoryClientRefTable::const_accessor accessor;
    if (!clientRefTable_.find(accessor, clientId)) {
        return false;
    }
    return accessor->second->Contains(shmId);
}
}  // namespace object_cache
}  // namespace datasystem
