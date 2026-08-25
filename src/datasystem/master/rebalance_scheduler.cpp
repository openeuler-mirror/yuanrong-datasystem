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

/** Description: Lifecycle bookkeeping shared by memory- and heat-driven rebalance schedulers. */

#include "datasystem/master/rebalance_scheduler.h"

#include <algorithm>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/util/math_util.h"

namespace datasystem {
namespace master {

bool RebalanceScheduler::IsTerminalStatus(master::RebalanceTaskStatusPb status)
{
    return status == master::REBALANCE_TASK_SUCCEEDED || status == master::REBALANCE_TASK_FAILED
           || status == master::REBALANCE_TASK_EXPIRED;
}

bool RebalanceScheduler::IsWorkerActiveInTopology(const std::string &worker,
                                                  const cluster::TopologySnapshot *topologySnapshot)
{
    if (topologySnapshot == nullptr) {
        return true;
    }
    const cluster::Member *member = nullptr;
    auto rc = topologySnapshot->FindMemberByAddress(worker, member);
    return rc.IsOk() && member != nullptr && member->state == cluster::MemberState::ACTIVE;
}

uint64_t RebalanceScheduler::GetTargetInflightBytesLocked(const std::string &targetWorker) const
{
    auto it = futureView_.find(targetWorker);
    return it == futureView_.end() ? 0 : it->second.inflightBytes;
}

void RebalanceScheduler::IncreaseTargetInflightLocked(const std::string &targetWorker, uint64_t bytes)
{
    auto &delta = futureView_[targetWorker];
    delta.inflightBytes = SaturatingAdd(delta.inflightBytes, bytes);
}

void RebalanceScheduler::DecreaseInflightLocked(const std::string &targetWorker, uint64_t bytes)
{
    auto it = futureView_.find(targetWorker);
    if (it == futureView_.end()) {
        return;
    }
    it->second.inflightBytes = it->second.inflightBytes > bytes ? it->second.inflightBytes - bytes : 0;
    if (it->second.inflightBytes == 0) {
        futureView_.erase(it);
    }
}

RebalanceScheduler::FutureDelta &RebalanceScheduler::HoldTargetInflightLocked(const std::string &targetWorker,
                                                                              uint64_t bytes, uint64_t nowMs,
                                                                              uint64_t minimumObservedUsedMemory)
{
    auto &delta = futureView_[targetWorker];
    delta.heldBytes = SaturatingAdd(delta.heldBytes, bytes);
    delta.holdSinceMs = std::max(delta.holdSinceMs, nowMs);
    delta.minimumObservedUsedMemory = std::max(delta.minimumObservedUsedMemory, minimumObservedUsedMemory);
    return delta;
}

void RebalanceScheduler::ReleaseHeldLocked(const std::string &targetWorker, uint64_t heldBytes)
{
    DecreaseInflightLocked(targetWorker, heldBytes);
    auto current = futureView_.find(targetWorker);
    if (current != futureView_.end()) {
        current->second.heldBytes = 0;
        current->second.holdSinceMs = 0;
        current->second.minimumObservedUsedMemory = 0;
    }
}

bool RebalanceScheduler::IsInCooldownLocked(const std::string &worker, uint64_t nowMs) const
{
    auto it = cooldownUntilMs_.find(worker);
    return it != cooldownUntilMs_.end() && it->second > nowMs;
}

void RebalanceScheduler::SetCooldownUntilLocked(const std::string &worker, uint64_t deadlineMs)
{
    if (!worker.empty()) {
        cooldownUntilMs_[worker] = deadlineMs;
    }
}

void RebalanceScheduler::ExpireWorkerCooldownsLocked(uint64_t nowMs)
{
    for (auto it = cooldownUntilMs_.begin(); it != cooldownUntilMs_.end();) {
        if (it->second <= nowMs) {
            it = cooldownUntilMs_.erase(it);
        } else {
            ++it;
        }
    }
}

std::vector<std::string> RebalanceScheduler::CollectExpiredTaskSourcesLocked(uint64_t nowMs) const
{
    std::vector<std::string> expiredSources;
    expiredSources.reserve(activeTasksBySource_.size());
    for (const auto &[source, runningTask] : activeTasksBySource_) {
        if (runningTask.task.deadline_ms() <= nowMs) {
            expiredSources.emplace_back(source);
        }
    }
    return expiredSources;
}

std::vector<std::pair<std::string, uint64_t>> RebalanceScheduler::CollectExpiredHoldsLocked(uint64_t nowMs,
                                                                                            uint64_t holdTtlMs) const
{
    std::vector<std::pair<std::string, uint64_t>> expired;
    for (const auto &[target, delta] : futureView_) {
        if (delta.heldBytes > 0 && nowMs > delta.holdSinceMs && nowMs - delta.holdSinceMs > holdTtlMs) {
            expired.emplace_back(target, delta.heldBytes);
        }
    }
    return expired;
}

std::vector<std::pair<std::string, uint64_t>> RebalanceScheduler::CollectReleasableHoldsLocked(
    const std::unordered_map<std::string, NodeInfo> &snapshot) const
{
    std::vector<std::pair<std::string, uint64_t>> releasable;
    releasable.reserve(futureView_.size());
    for (const auto &[target, delta] : futureView_) {
        auto node = snapshot.find(target);
        if (delta.heldBytes > 0 && node != snapshot.end() && node->second.timestamp > delta.holdSinceMs
            && node->second.usedMemory >= delta.minimumObservedUsedMemory) {
            releasable.emplace_back(target, delta.heldBytes);
        }
    }
    return releasable;
}

const RebalanceScheduler::FutureDelta *RebalanceScheduler::GetReleasableReporterHoldLocked(
    const NodeInfo &reportingNode) const
{
    auto hold = futureView_.find(reportingNode.nodeId);
    if (hold == futureView_.end() || hold->second.heldBytes == 0 || reportingNode.timestamp <= hold->second.holdSinceMs
        || reportingNode.usedMemory < hold->second.minimumObservedUsedMemory) {
        return nullptr;
    }
    return &hold->second;
}

}  // namespace master
}  // namespace datasystem
