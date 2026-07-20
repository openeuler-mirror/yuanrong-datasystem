/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

#include "datasystem/worker/object_cache/eviction_list.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
EvictionList::EvictionList() : oldest_(list_.end())
{
}

void EvictionList::Add(const std::string &objectKey, uint8_t counter)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_ADD);
    TBBIndexMap::accessor accessor;
    bool inserted = indexTable_.insert(accessor, objectKey);
    {
        tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
        if (inserted) {
            // Append to the tail (newest end). The clock hand starts at oldest_
            // (oldest object) and advances per successful eviction, so a newly added
            // object at the tail is reached only after a full LRU sweep
            // (list_size / write_rate, ~seconds). The previous emplace(oldest_)
            // inserted the new node right before the clock hand, so the hand wrapped
            // back to it within a single EvictionTask (~ms) and evicted fresh data
            // before cross-node GETs could arrive (issue #750).
            try {
                list_.emplace_back(objectKey, counter);
                if (list_.size() == 1) {
                    oldest_ = list_.begin();
                }
                accessor->second = std::prev(list_.end());
            } catch (...) {
                indexTable_.erase(accessor);
                // Don't propagate the exception (e.g. bad_alloc from emplace_back):
                // callers (eviction task, publish, GET re-access) are not prepared
                // for Add to throw and would crash the worker. The object simply
                // stays out of the eviction list and will be added on the next access.
                return;
            }
        } else {
            // curCounter++ under the write lock: concurrent Add (GET re-access or
            // FinishPrimaryEndLifeTask readd from multiple drain workers) races on
            // the non-atomic uint8_t curCounter otherwise.
            auto &nodePtr = accessor->second;
            if (nodePtr->curCounter < nodePtr->maxCounter) {
                nodePtr->curCounter++;
            }
        }
    }
    point.Record();
}

Status EvictionList::Erase(const std::string &objectKey)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_ERASE);
    TBBIndexMap::accessor accessor;
    if (!indexTable_.find(accessor, objectKey)) {
        VLOG(1) << "Object " + objectKey + " does not exist in EvictionList";
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object " + objectKey + " does not exist in EvictionList.");
    }

    tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
    bool reassign = false;
    if (oldest_->objectKey == objectKey) {
        ++oldest_;
        if (oldest_ == list_.end()) {
            reassign = true;
        }
    }
    list_.erase(accessor->second);
    indexTable_.erase(accessor);
    if (reassign) {
        oldest_ = list_.begin();
    }
    point.Record();
    return Status::OK();
}

size_t EvictionList::Size()
{
    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    return list_.size();
}

Status EvictionList::FindEvictCandidate(std::string &candidateObjKey)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_FIND);
    tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
    CHECK_FAIL_RETURN_STATUS(!list_.empty(), StatusCode::K_RUNTIME_ERROR, "EvictionList is empty.");
    while (true) {
        if (oldest_->curCounter == 0) {
            candidateObjKey = oldest_->objectKey;
            break;
        }
        oldest_->curCounter--;
        if (++oldest_ == list_.end()) {
            oldest_ = list_.begin();
        }
    }
    point.Record();
    return Status::OK();
}

Status EvictionList::GetObjectInfo(const std::string &objectKey, Node &node)
{
    TBBIndexMap::const_accessor readAccessor;
    if (!indexTable_.find(readAccessor, objectKey)) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_NOT_FOUND, "Object " + objectKey + " does not exist");
    }
    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    node = *(readAccessor->second);
    return Status::OK();
}

Status EvictionList::GetOldestObjectInfo(Node &node)
{
    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    CHECK_FAIL_RETURN_STATUS(!list_.empty(), StatusCode::K_RUNTIME_ERROR, "EvictionList is empty.");
    node.objectKey = oldest_->objectKey;
    node.curCounter = oldest_->curCounter;
    node.maxCounter = oldest_->maxCounter;
    return Status::OK();
}

Status EvictionList::GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest)
{
    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    if (list_.empty()) {
        return Status::OK();
    }
    oldest.objectKey = oldest_->objectKey;
    oldest.curCounter = oldest_->curCounter;
    oldest.maxCounter = oldest_->maxCounter;

    auto node = oldest_;
    while (true) {
        res.emplace_back(*node);
        ++node;
        if (node == list_.end()) {
            node = list_.begin();
        }
        if (node == oldest_) {
            break;
        }
    }
    return Status::OK();
}

Status EvictionList::GetObjectsInfoFromOldest(size_t maxScanCount, std::vector<EvictionList::Node> &res)
{
    res.clear();
    if (maxScanCount == 0) {
        return Status::OK();
    }
    // Reserve outside the list read lock to avoid repeated vector growth while holding the lock, reducing the time
    // Evict can be blocked on the write lock.
    res.reserve(maxScanCount);

    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    if (list_.empty()) {
        return Status::OK();
    }

    // Rebalance only needs a candidate snapshot near the oldest position, not a full eviction-list copy.
    // Scan at most maxScanCount nodes so lock hold time changes from O(list size) to O(maxScanCount).
    auto node = oldest_;
    size_t scanned = 0;
    while (scanned < maxScanCount) {
        res.emplace_back(*node);
        ++scanned;
        ++node;
        if (node == list_.end()) {
            node = list_.begin();
        }
        if (node == oldest_) {
            break;
        }
    }
    return Status::OK();
}

bool EvictionList::Exist(const std::string &objectKey)
{
    return indexTable_.count(objectKey) > 0;
}
}  // namespace object_cache
}  // namespace datasystem
