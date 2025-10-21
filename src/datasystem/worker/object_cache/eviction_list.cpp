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
    std::lock_guard<std::shared_timed_mutex> lck(listMutex_);
    auto iter = indexTable_.find(objectKey);
    if (iter == indexTable_.end()) {
        Node node(objectKey, counter);
        auto newest = list_.insert(oldest_, node);
        if (list_.size() == 1) {
            oldest_ = newest;
        }
        (void)indexTable_.emplace(objectKey, newest);
        point.Record();
        return;
    }
    // Object exist, refresh it
    auto &nodePtr = iter->second;
    if (nodePtr->curCounter < nodePtr->maxCounter) {
        nodePtr->curCounter++;
    }
    point.Record();
}

Status EvictionList::Erase(const std::string &objectKey)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_ERASE);
    std::lock_guard<std::shared_timed_mutex> lck(listMutex_);
    auto iter = indexTable_.find(objectKey);
    if (iter == indexTable_.end()) {
        VLOG(1) << "Object " + objectKey + " does not exist in EvictionList";
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object " + objectKey + " does not exist in EvictionList.");
    }
    bool reassign = false;
    if (oldest_->objectKey == objectKey) {
        ++oldest_;
        if (oldest_ == list_.end()) {
            reassign = true;
        }
    }
    list_.erase(iter->second);
    indexTable_.erase(objectKey);
    if (reassign) {
        oldest_ = list_.begin();
    }
    point.Record();
    return Status::OK();
}

size_t EvictionList::Size()
{
    std::shared_lock<std::shared_timed_mutex> lck(listMutex_);
    return list_.size();
}

Status EvictionList::FindEvictCandidate(std::string &candidateObjKey)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_FIND);
    std::lock_guard<std::shared_timed_mutex> lck(listMutex_);
    CHECK_FAIL_RETURN_STATUS(!list_.empty(), StatusCode::K_RUNTIME_ERROR, "EvictionList is empty.");
    while (true) {
        if (oldest_->curCounter == 0) {
            candidateObjKey = oldest_->objectKey;
            break;
        } else {
            oldest_->curCounter--;
        }
        if (++oldest_ == list_.end()) {
            oldest_ = list_.begin();
        }
    }
    point.Record();
    return Status::OK();
}

Status EvictionList::GetObjectInfo(const std::string &objectKey, Node &node)
{
    std::lock_guard<std::shared_timed_mutex> lck(listMutex_);
    if (indexTable_.find(objectKey) == indexTable_.end()) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_NOT_FOUND, "Object " + objectKey + " does not exist");
    }
    node = *(indexTable_[objectKey]);
    return Status::OK();
}

Status EvictionList::GetOldestObjectInfo(Node &node)
{
    std::lock_guard<std::shared_timed_mutex> lck(listMutex_);
    CHECK_FAIL_RETURN_STATUS(!list_.empty(), StatusCode::K_RUNTIME_ERROR, "EvictionList is empty.");
    node.objectKey = oldest_->objectKey;
    node.curCounter = oldest_->curCounter;
    node.maxCounter = oldest_->maxCounter;
    return Status::OK();
}

Status EvictionList::GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest)
{
    std::lock_guard<std::shared_timed_mutex> lck(listMutex_);
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

bool EvictionList::Exist(const std::string &objectKey)
{
    std::shared_lock<std::shared_timed_mutex> lck(listMutex_);
    return indexTable_.count(objectKey) > 0;
}
}  // namespace object_cache
}  // namespace datasystem