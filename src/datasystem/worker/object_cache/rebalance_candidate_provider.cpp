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

Status RebalanceCandidateProvider::Select(uint64_t targetBytes, size_t maxObjectCount,
                                          std::unordered_map<std::string, uint64_t> &candidates)
{
    candidates.clear();
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
        uint64_t objectSize = 0;
        auto rc = TryGetObjectSize(objectKey, objectSize);
        if (rc.IsError()) {
            VLOG(1) << "Skip rebalance candidate " << objectKey << ", rc: " << rc.ToString();
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

Status RebalanceCandidateProvider::TryGetObjectSize(const std::string &objectKey, uint64_t &objectSize)
{
    std::shared_ptr<SafeObjType> entry;
    RETURN_IF_NOT_OK(objectTable_->Get(objectKey, entry));
    RETURN_RUNTIME_ERROR_IF_NULL(entry);
    RETURN_IF_NOT_OK(entry->RLock(true));
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
    // Pair with EvictionTask's mark check under the object write lock. If this read lock is acquired first, the mark is
    // visible before eviction can get the write lock; if eviction gets the write lock first, this validation waits and
    // observes the post-eviction object state.
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

}  // namespace object_cache
}  // namespace datasystem
