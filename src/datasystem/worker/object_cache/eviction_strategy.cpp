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

#include "datasystem/worker/object_cache/eviction_strategy.h"

#include <algorithm>

#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace object_cache {

// ---- ClockEvictionStrategy: wraps the existing clock/second-chance algorithm ----

uint8_t ComputeClockAddCounter(
    const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &globalRefTable, const std::string &objectKey)
{
    return globalRefTable->GetRefWorkerCount(objectKey) == 0 ? Q1 : Q2;
}

void ClockEvictionStrategy::OnAdd(const std::string &objectKey)
{
    list_.Add(objectKey, ComputeClockAddCounter(globalRefTable_, objectKey));
}

void ClockEvictionStrategy::OnCacheHit(const std::string &objectKey, uint64_t /* migratableSize */)
{
    // The pre-strategy Get path refilled the node via Add on every memory hit, so a
    // cache hit is identical to an add for the clock strategy.
    OnAdd(objectKey);
}

void ClockEvictionStrategy::OnRefill(const std::string &objectKey, uint64_t migratableSize)
{
    OnCacheHit(objectKey, migratableSize);
}

Status ClockEvictionStrategy::SelectCandidate(EvictionRoundState & /* round */, EvictionCandidate &candidate)
{
    candidate = {};
    candidate.policy = EvictionPolicy::CLOCK;
    return list_.FindEvictCandidate(candidate.objectKey);
}

void ClockEvictionStrategy::ReaddCandidate(const EvictionCandidate &candidate, uint8_t counter)
{
    // Use the carried counter from EvictFailedList: K_TRY_AGAIN spill failures carry Q1(1)
    // for fast retry; other failures carry READD_COUNTER(5). This restores the pre-strategy
    // behavior where ReleaseSpillFutures selected the counter based on the spill return code.
    list_.Add(candidate.objectKey, counter);
}

// ---- HeatEvictionStrategy: periodically-decayed heat counter ----

void HeatEvictionStrategy::OnAdd(const std::string &objectKey)
{
    // Fresh inserts start at the (low) initial heat rather than the cap, so they are not counted as hot data by
    // the heat rebalance strategy (heat > rebalance_heat_hot_counter_threshold). They stay at or above the
    // eviction threshold so they are not first-round eviction candidates either; GetHeatCandidates keeps recent
    // objects behind otherwise equivalent older candidates.
    list_.AddHeatNode(objectKey, config_.initialCounter, GetSteadyClockTimeStampMs());
}

void HeatEvictionStrategy::OnCacheHit(const std::string &objectKey, uint64_t migratableSize)
{
    const double credit = migratableSize == 0
                              ? 1.0
                              : std::min(1.0, static_cast<double>(HEAT_ACCESS_UNIT_BYTES) / migratableSize);
    list_.IncrementHeat(objectKey, credit, config_.maxCounter, GetSteadyClockTimeStampMs());
}

void HeatEvictionStrategy::OnRefill(const std::string &objectKey, uint64_t migratableSize)
{
    // A refill is both an admission and a successful Get. Publish the new node with
    // its access credit already applied, so eviction cannot observe an intermediate
    // global-minimum initial heat between separate Add and hit operations.
    const double credit = migratableSize == 0
                              ? 1.0
                              : std::min(1.0, static_cast<double>(HEAT_ACCESS_UNIT_BYTES) / migratableSize);
    list_.RefillHeatNode(objectKey, config_.initialCounter, credit, config_.maxCounter,
                         GetSteadyClockTimeStampMs());
}

bool HeatEvictionStrategy::TryResolveCandidateSize(const std::string &objectKey, uint64_t &migratableSize) const
{
    migratableSize = 0;
    std::shared_ptr<SafeObjType> entry;
    if (objectTable_ == nullptr || objectTable_->Get(objectKey, entry).IsError() || entry == nullptr
        || entry->TryRLock(true).IsError()) {
        return false;
    }
    Raii unlock([&entry]() { entry->RUnlock(); });
    auto *object = entry->Get();
    if (object == nullptr || object->stateInfo.IsCacheInvalid() || object->stateInfo.IsIncomplete()
        || object->stateInfo.IsNeedToDelete() || object->IsInvalid()) {
        return false;
    }
    auto shmUnit = object->GetShmUnit();
    if (shmUnit == nullptr) {
        return false;
    }
    migratableSize = shmUnit->GetMigratableSize();
    return migratableSize > 0;
}

EvictionList::HeatNodeMetadata HeatEvictionStrategy::TryResolveCopyType(const std::string &objectKey) const
{
    EvictionList::HeatNodeMetadata metadata;
    std::shared_ptr<SafeObjType> entry;
    if (objectTable_ == nullptr || objectTable_->Get(objectKey, entry).IsError() || entry == nullptr
        || entry->TryRLock(true).IsError()) {
        return metadata;
    }
    Raii unlock([&entry]() { entry->RUnlock(); });
    auto *object = entry->Get();
    if (object != nullptr) {
        metadata.resolved = true;
        metadata.isPrimary = object->stateInfo.IsPrimaryCopy();
    }
    return metadata;
}

Status HeatEvictionStrategy::SelectCandidate(EvictionRoundState &round, EvictionCandidate &candidate)
{
    for (;;) {
        if (round.nextCandidate >= round.candidateBatch.size()) {
            round.ClearCandidateBatch();
            EvictionList::HeatCandidateOptions options;
            options.recentAccessProtectionMs = HEAT_RECENT_ACCESS_PROTECTION_MS;
            options.immediateEvictionThreshold = HEAT_IMMEDIATE_EVICTION_THRESHOLD;
            options.exactRankingThreshold = HEAT_EXACT_RANKING_THRESHOLD;
            options.sizeResolver = [this](const std::string &objectKey, uint64_t &migratableSize) {
                return TryResolveCandidateSize(objectKey, migratableSize);
            };
            RETURN_IF_NOT_OK(
                list_.GetHeatCandidates(config_.threshold, HEAT_CANDIDATE_BATCH_SIZE, round.candidateBatch, options));
        }
        const auto &snapshot = round.candidateBatch[round.nextCandidate++];
        if (!list_.IsHeatSnapshotCurrent(snapshot)) {
            // A hit only invalidates this key's snapshot. The remaining snapshots carry independent generation and
            // update-sequence guards, so keep the bounded batch and validate each entry when it is selected. Dropping
            // all 256 entries here turns a multi-key hit burst into repeated scan/size-resolution/sort work.
            continue;
        }
        round.selectedCandidate = snapshot;
        candidate.objectKey = snapshot.objectKey;
        candidate.policy = EvictionPolicy::HEAT;
        candidate.heat = snapshot.heat;
        candidate.generation = snapshot.generation;
        candidate.heatUpdateSeq = snapshot.heatUpdateSeq;
        return Status::OK();
    }
}

bool HeatEvictionStrategy::ValidateCandidate(EvictionRoundState &round, const EvictionCandidate &candidate)
{
    if (!round.selectedCandidate.has_value() || round.selectedCandidate->objectKey != candidate.objectKey
        || round.selectedCandidate->generation != candidate.generation
        || round.selectedCandidate->heatUpdateSeq != candidate.heatUpdateSeq
        || !list_.IsHeatSnapshotCurrent(*round.selectedCandidate)) {
        // The selected key changed while its object write lock was pending. Skip only that key: every remaining
        // snapshot is revalidated independently by SelectCandidate before it can be returned.
        round.selectedCandidate.reset();
        return false;
    }
    round.selectedCandidate.reset();
    return true;
}

void HeatEvictionStrategy::ReaddCandidate(const EvictionCandidate &candidate, uint8_t /* counter */)
{
    // The immutable token crosses async retry boundaries, so no strategy-global key->heat side map is required.
    // A token selected by another policy has no meaningful Heat value and falls back to normal admission heat.
    const double heat = candidate.policy == EvictionPolicy::HEAT ? candidate.heat : config_.initialCounter;
    list_.ReinsertHot(candidate.objectKey, std::min(heat, config_.maxCounter), GetSteadyClockTimeStampMs());
}

std::shared_ptr<EvictionStrategy> MakeEvictionStrategy(
    EvictionPolicy policy, EvictionList &list, const std::shared_ptr<ObjectTable> &objectTable,
    const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &gRefTable, const HeatPolicyConfig &heatConfig)
{
    if (policy == EvictionPolicy::HEAT) {
        return std::make_shared<HeatEvictionStrategy>(list, objectTable, heatConfig);
    }
    return std::make_shared<ClockEvictionStrategy>(list, gRefTable);
}

HeatPolicyConfig GetCurrentHeatPolicyConfig()
{
    const auto &config = GetEvictionHeatConfig();
    return HeatPolicyConfig{ config.halfLifePrimaryS, config.halfLifeLocalS, config.threshold, config.initialCounter,
                             static_cast<double>(config.maxCounter) };
}

}  // namespace object_cache
}  // namespace datasystem
