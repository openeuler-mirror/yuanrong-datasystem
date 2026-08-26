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

#include "datasystem/worker/object_cache/rebalance_candidate_provider.h"

#include <utility>
#include <vector>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/object_cache/eviction_list.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr size_t REBALANCE_SCAN_FACTOR = 5;
}  // namespace

RebalanceCandidateProvider::RebalanceCandidateProvider(std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                                                       std::shared_ptr<ObjectTable> objectTable)
    : evictionManager_(std::move(evictionManager)), objectTable_(std::move(objectTable))
{
}

Status RebalanceCandidateProvider::TryGetObjectSize(const std::string &objectKey, uint64_t &objectSize)
{
    std::shared_ptr<SafeObjType> entry;
    RETURN_IF_NOT_OK(objectTable_->Get(objectKey, entry));
    RETURN_RUNTIME_ERROR_IF_NULL(entry);
    auto lockRc = entry->TryRLock(true);
    if (lockRc.IsError()) {
        RETURN_STATUS(K_NOT_FOUND, "Skip object whose metadata is busy");
    }
    auto *object = entry->Get();
    if (object == nullptr) {
        entry->RUnlock();
        RETURN_STATUS(K_NOT_FOUND, "Object entry is null");
    }
    if (!object->stateInfo.IsPrimaryCopy()) {
        entry->RUnlock();
        RETURN_STATUS(K_NOT_FOUND, "Skip non-primary copy");
    }
    if (object->stateInfo.IsCacheInvalid() || object->stateInfo.IsIncomplete() || object->stateInfo.IsNeedToDelete()
        || object->IsInvalid()) {
        entry->RUnlock();
        RETURN_STATUS(K_NOT_FOUND, "Skip object that is not stable for rebalance");
    }
    // Pair with EvictionTask's mark check under the object write lock. Busy objects are best-effort candidates and are
    // skipped for this bounded scan instead of stalling the entire rebalance batch behind a foreground writer.
    if (!evictionManager_->TryMarkRebalancingObject(objectKey)) {
        entry->RUnlock();
        RETURN_STATUS(K_NOT_FOUND, "Skip object that is being rebalanced");
    }
    // Size in the real-memory-usage (sallocx) unit so it matches task.max_bytes and the usage-rate denominator.
    // For standalone allocations this is the sallocx real size (fixes the payload-vs-realSize over-migration);
    // for aggregated slices GetMigratableSize returns the distributed slice size (needSize) as a proxy.
    auto shmUnit = object->GetShmUnit();
    objectSize = (shmUnit == nullptr) ? 0 : shmUnit->GetMigratableSize();
    if (objectSize == 0) {
        evictionManager_->UnmarkRebalancingObject(objectKey);
        entry->RUnlock();
        RETURN_STATUS(K_NOT_FOUND, "Skip empty object");
    }
    entry->RUnlock();
    return Status::OK();
}

Status MemoryRebalanceCandidateProvider::Select(RebalanceCandidateSession & /* session */, uint64_t targetBytes,
                                                size_t maxObjectCount,
                                                std::unordered_map<std::string, uint64_t> &candidates,
                                                ObjectHeatMap &objectHeats,
                                                const std::unordered_set<std::string> *skipKeys)
{
    candidates.clear();
    objectHeats.clear();
    CHECK_FAIL_RETURN_STATUS(evictionManager_ != nullptr && objectTable_ != nullptr, K_RUNTIME_ERROR,
                             "Rebalance candidate provider is not initialized");
    if (targetBytes == 0 || maxObjectCount == 0) {
        return Status::OK();
    }

    std::vector<EvictionList::Node> nodes;
    // Each batch needs at most maxObjectCount objects, but earlier objects can be filtered out because they are
    // non-primary, being written, already rebalancing, and so on. Scan only a bounded multiple of candidates instead
    // of the full eviction list to minimize blocking Evict.
    RETURN_IF_NOT_OK(evictionManager_->GetObjectsInfoFromOldest(maxObjectCount * REBALANCE_SCAN_FACTOR, nodes));

    uint64_t selectedBytes = 0;
    for (const auto &node : nodes) {
        const std::string objectKey = node.objectKey;
        if (skipKeys != nullptr && skipKeys->count(objectKey) > 0) {
            continue;
        }
        uint64_t objectSize = 0;
        auto rc = TryGetObjectSize(objectKey, objectSize);
        if (rc.IsError()) {
            VLOG(1) << "Skip rebalance candidate " << objectKey << ", rc: " << rc.ToString();
            continue;
        }

        // task.max_bytes is also the master's target-room and in-flight reservation. Never exceed it locally: the
        // provider cannot tell whether the budget was capped by source policy or by the target's remaining capacity.
        if (objectSize > targetBytes - selectedBytes) {
            evictionManager_->UnmarkRebalancingObject(objectKey);
            continue;
        }

        // Candidate selection is read-only here: do not decrement eviction counters or delete objects, so foreground
        // eviction behavior is not affected. TryGetObjectSize has already marked the object as rebalancing; clear the
        // mark if insertion fails because of a duplicate key.
        if (!candidates.emplace(objectKey, objectSize).second) {
            evictionManager_->UnmarkRebalancingObject(objectKey);
            continue;
        }
        selectedBytes += objectSize;
        if (selectedBytes >= targetBytes || candidates.size() >= maxObjectCount) {
            break;
        }
    }
    return Status::OK();
}

Status HeatRebalanceCandidateProvider::Select(RebalanceCandidateSession &session, uint64_t targetBytes,
                                              size_t maxObjectCount,
                                              std::unordered_map<std::string, uint64_t> &candidates,
                                              ObjectHeatMap &objectHeats,
                                              const std::unordered_set<std::string> *skipKeys)
{
    candidates.clear();
    objectHeats.clear();
    CHECK_FAIL_RETURN_STATUS(evictionManager_ != nullptr && objectTable_ != nullptr, K_RUNTIME_ERROR,
                             "Rebalance candidate provider is not initialized");
    if (targetBytes == 0 || maxObjectCount == 0) {
        return Status::OK();
    }

    if (session.nextCandidate >= session.candidateWindow.size()) {
        session.candidateWindow.clear();
        session.nextCandidate = 0;
        // Under memory pressure, migrate the lowest-heat stable primaries first. Keep one bounded immutable window for
        // the whole master task so consecutive local batches do not restart the same list scan. TryGetObjectSize below
        // still revalidates current object state and acquires the per-object rebalance mark.
        RETURN_IF_NOT_OK(
            evictionManager_->GetLowestHeatObjects(maxObjectCount * REBALANCE_SCAN_FACTOR, session.candidateWindow));
    }

    uint64_t selectedBytes = 0;
    while (session.nextCandidate < session.candidateWindow.size()) {
        const auto &node = session.candidateWindow[session.nextCandidate++];
        const std::string objectKey = node.objectKey;
        if (skipKeys != nullptr && skipKeys->count(objectKey) > 0) {
            continue;
        }
        uint64_t objectSize = 0;
        auto rc = TryGetObjectSize(objectKey, objectSize);
        if (rc.IsError()) {
            VLOG(1) << "Skip heat rebalance candidate " << objectKey << ", rc: " << rc.ToString();
            continue;
        }
        // Keep actual migration bytes within the master's target-room and in-flight reservation. An oversized object
        // needs an explicit admission protocol; selecting it here would silently overcommit the target.
        if (objectSize > targetBytes - selectedBytes) {
            evictionManager_->UnmarkRebalancingObject(objectKey);
            continue;
        }
        // Candidate selection is read-only here: do not decrement eviction counters or delete objects, so foreground
        // eviction behavior is not affected. TryGetObjectSize has already marked the object as rebalancing; clear the
        // mark if insertion fails because of a duplicate key.
        if (!candidates.emplace(objectKey, objectSize).second) {
            evictionManager_->UnmarkRebalancingObject(objectKey);
            continue;
        }
        // Refresh after the object has been marked so the transmitted value is as close as possible to migration
        // time rather than the earlier sorted-scan snapshot. Foreground hits remain lock-free and may still race
        // after this point; the wire value is intentionally a point-in-time snapshot, not a global barrier.
        EvictionList::Node heatSnapshot;
        rc = evictionManager_->GetObjectInfo(objectKey, heatSnapshot);
        if (rc.IsError()) {
            candidates.erase(objectKey);
            evictionManager_->UnmarkRebalancingObject(objectKey);
            continue;
        }
        objectHeats.emplace(objectKey, heatSnapshot.heat);
        selectedBytes += objectSize;
        if (selectedBytes >= targetBytes || candidates.size() >= maxObjectCount) {
            break;
        }
    }
    return Status::OK();
}

std::unique_ptr<RebalanceCandidateProvider> MakeRebalanceCandidateProvider(
    std::shared_ptr<WorkerOcEvictionManager> evictionManager, std::shared_ptr<ObjectTable> objectTable)
{
    if (GetRebalanceStrategy() == "heat") {
        return std::make_unique<HeatRebalanceCandidateProvider>(std::move(evictionManager), std::move(objectTable));
    }
    return std::make_unique<MemoryRebalanceCandidateProvider>(std::move(evictionManager), std::move(objectTable));
}

}  // namespace object_cache
}  // namespace datasystem
