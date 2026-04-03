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
 * Description: Selector for metadata recovery object keys.
 */
#include "datasystem/worker/object_cache/metadata_recovery_selector.h"

#include <sstream>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr size_t GET_MATCH_OBJECT_BATCH = 500;
}

bool MetadataRecoverySelector::SelectionRequest::Empty() const
{
    return ranges.empty() && workerUuids.empty();
}

std::string MetadataRecoverySelector::SelectionRequest::ToString() const
{
    std::stringstream ss;
    ss << "ranges: ";
    for (const auto &range : ranges) {
        ss << "[" << range.first << ", " << range.second << "],";
    }
    ss << " uuids: " << VectorToString(workerUuids);
    ss << " includeL2CacheIds: " << includeL2CacheIds;
    return ss.str();
}

MetadataRecoverySelector::MetadataRecoverySelector(const std::shared_ptr<ObjectTable> &objectTable,
                                                   EtcdClusterManager *etcdCM)
    : objectTable_(objectTable), etcdCM_(etcdCM)
{
}

MetadataRecoverySelector::SelectionRequest MetadataRecoverySelector::BuildSelectionRequest(const ClearDataReqPb &req,
                                                                                           bool includeL2CacheIds)
{
    SelectionRequest request;
    request.includeL2CacheIds = includeL2CacheIds;
    for (const auto &range : req.ranges()) {
        request.ranges.emplace_back(range.from(), range.end());
    }
    request.workerUuids.assign(req.worker_ids().begin(), req.worker_ids().end());
    return request;
}

Status MetadataRecoverySelector::BuildMatchFunc(const SelectionRequest &request, MatchFunc &matchFunc) const
{
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, K_RUNTIME_ERROR, "etcdCM is null");
    if (request.workerUuids.empty()) {
        matchFunc = [this, ranges = request.ranges](const std::string &objKey) {
            return etcdCM_->IsInRange(ranges, objKey, "");
        };
        return Status::OK();
    }

    matchFunc = [this, ranges = request.ranges, workerUuids = request.workerUuids](const std::string &objKey) {
        return etcdCM_->NeedToClear(objKey, ranges, workerUuids);
    };
    return Status::OK();
}

void MetadataRecoverySelector::Select(const MatchFunc &matchFunc, bool includeL2CacheIds,
                                      std::vector<std::string> &objectKeys) const
{
    LOG(INFO) << "MetadataRecoverySelector.Select begin, includeL2CacheIds: " << includeL2CacheIds;
    Timer timer;
    std::vector<std::string> allObjectKeys;
    allObjectKeys.reserve(objectTable_->GetSize());
    for (const auto &it : *objectTable_) {
        allObjectKeys.emplace_back(it.first);
    }
    LOG(INFO) << "MetadataRecoverySelector.Select get all object keys elapsed ms: " << timer.ElapsedMilliSecond()
              << ", object size: " << allObjectKeys.size();
    timer.Reset();

    auto batchFun = [this, &matchFunc, &objectKeys, includeL2CacheIds](std::vector<std::string> &batchObjectKeys) {
        for (const auto &objectKey : batchObjectKeys) {
            if (!matchFunc(objectKey)) {
                continue;
            }
            if (includeL2CacheIds) {
                objectKeys.emplace_back(objectKey);
                continue;
            }
            std::shared_ptr<SafeObjType> entry;
            if (objectTable_->Get(objectKey, entry).IsError()) {
                continue;
            }
            if (entry->TryRLock().IsError()) {
                continue;
            }
            Raii entryUnlock([&entry] { entry->RUnlock(); });
            if (!(*entry)->HasL2Cache()) {
                objectKeys.emplace_back(objectKey);
            }
        }
    };

    std::vector<std::string> tmpObjectKeys;
    for (const auto &objectKey : allObjectKeys) {
        tmpObjectKeys.emplace_back(objectKey);
        if (tmpObjectKeys.size() < GET_MATCH_OBJECT_BATCH) {
            continue;
        }
        batchFun(tmpObjectKeys);
        tmpObjectKeys.clear();
    }
    if (!tmpObjectKeys.empty()) {
        batchFun(tmpObjectKeys);
    }
    LOG(INFO) << "MetadataRecoverySelector.Select finish, objectKeys size: " << objectKeys.size()
              << ", includeL2CacheIds: " << includeL2CacheIds
              << ", elapsed ms: " << timer.ElapsedMilliSecond();
}

Status MetadataRecoverySelector::Select(const SelectionRequest &request, std::vector<std::string> &objectKeys) const
{
    MatchFunc matchFunc;
    RETURN_IF_NOT_OK(BuildMatchFunc(request, matchFunc));
    Select(matchFunc, request.includeL2CacheIds, objectKeys);
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
