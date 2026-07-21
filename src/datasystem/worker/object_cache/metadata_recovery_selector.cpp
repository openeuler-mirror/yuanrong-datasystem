/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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

#include <algorithm>
#include <sstream>
#include <thread>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr size_t GET_MATCH_OBJECT_BATCH = 500;
constexpr char URMA_WARMUP_KEY_PREFIX[] = "_urma_";

bool RangeContains(const Range &range, uint32_t value)
{
    if (range.first < range.second) {
        return value > range.first && value <= range.second;
    }
    return value > range.first || value <= range.second;
}
}  // namespace

bool MetadataRecoverySelector::SelectionRequest::Empty() const
{
    return ranges.empty();
}

std::string MetadataRecoverySelector::SelectionRequest::ToString() const
{
    std::stringstream ss;
    ss << "ranges: ";
    for (const auto &range : ranges) {
        ss << "[" << range.first << ", " << range.second << "],";
    }
    ss << " includeL2CacheIds: " << includeL2CacheIds;
    return ss.str();
}

MetadataRecoverySelector::MetadataRecoverySelector(const std::shared_ptr<ObjectTable> &objectTable)
    : MetadataRecoverySelector(objectTable, GET_MATCH_OBJECT_BATCH, [] { std::this_thread::yield(); })
{
}

MetadataRecoverySelector::MetadataRecoverySelector(const std::shared_ptr<ObjectTable> &objectTable, size_t visitBudget,
                                                   std::function<void()> batchYield)
    : objectTable_(objectTable), visitBudget_(visitBudget), batchYield_(std::move(batchYield))
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
    return request;
}

Status MetadataRecoverySelector::BuildMatchFunc(const SelectionRequest &request, MatchFunc &matchFunc) const
{
    CHECK_FAIL_RETURN_STATUS(!request.ranges.empty(), K_INVALID, "metadata recovery selection ranges are empty");
    matchFunc = [ranges = request.ranges](const std::string &objKey) {
        const uint32_t objectHash = MurmurHash3_32(objKey);
        return std::any_of(ranges.begin(), ranges.end(),
                           [objectHash](const Range &range) { return RangeContains(range, objectHash); });
    };
    return Status::OK();
}

void MetadataRecoverySelector::Select(const MatchFunc &matchFunc, bool includeL2CacheIds,
                                      std::vector<std::string> &objectKeys) const
{
    LOG(INFO) << "MetadataRecoverySelector.Select begin, includeL2CacheIds: " << includeL2CacheIds;
    Timer timer;
    auto cursor = objectTable_->BeginRecoverySnapshot();
    auto batchFun = [this, &cursor, &matchFunc, &objectKeys,
                     includeL2CacheIds](std::vector<std::string> &batchObjectKeys) {
        for (const auto &objectKey : batchObjectKeys) {
            if (!matchFunc(objectKey)) {
                continue;
            }
            // Skip key for URMA warmup.
            if (objectKey.rfind(URMA_WARMUP_KEY_PREFIX, 0) == 0) {
                continue;
            }
            std::shared_ptr<SafeObjType> entry;
            if (objectTable_->GetRecoverySnapshotObject(cursor, objectKey, entry).IsError()) {
                continue;
            }
            if (includeL2CacheIds) {
                objectKeys.emplace_back(objectKey);
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

    bool done = false;
    size_t visitedObjectCount = 0;
    while (!done) {
        std::vector<std::string> batchObjectKeys;
        auto status = objectTable_->NextRecoverySnapshotBatch(cursor, visitBudget_, batchObjectKeys, done);
        if (status.IsError()) {
            LOG(ERROR) << "MetadataRecoverySelector.Select snapshot failed: " << status.ToString();
            return;
        }
        visitedObjectCount += batchObjectKeys.size();
        batchFun(batchObjectKeys);
        if (!done) {
            batchYield_();
        }
    }
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RECOVERY_CANDIDATE_COUNT))
        .Observe(objectKeys.size());
    LOG(INFO) << "MetadataRecoverySelector.Select finish, objectKeys size: " << objectKeys.size()
              << ", includeL2CacheIds: " << includeL2CacheIds << ", snapshot object size: " << visitedObjectCount
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
