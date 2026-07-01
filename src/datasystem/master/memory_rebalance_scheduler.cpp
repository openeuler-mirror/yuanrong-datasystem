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
 * Description: In-memory scheduler for object-cache memory rebalance tasks.
 */

#include "datasystem/master/memory_rebalance_scheduler.h"

#include <algorithm>
#include <limits>
#include <vector>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DECLARE_uint32(data_migrate_rate_limit_mb);

namespace datasystem {
namespace master {
namespace {
constexpr uint64_t PERCENT_BASE = 100;
constexpr uint64_t MS_PER_SECOND = 1'000;
constexpr size_t MIN_REBALANCE_WORKER_COUNT = 2;
constexpr uint64_t TRANSFER_TIME_MULTIPLIER = 2;

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs)
{
    return std::numeric_limits<uint64_t>::max() - lhs < rhs ? std::numeric_limits<uint64_t>::max() : lhs + rhs;
}

uint64_t SubOrZero(uint64_t lhs, uint64_t rhs)
{
    return lhs > rhs ? lhs - rhs : 0;
}

void DecreaseCounter(std::unordered_map<std::string, uint64_t> &counter, const std::string &key, uint64_t value)
{
    auto iter = counter.find(key);
    if (iter == counter.end()) {
        return;
    }
    iter->second = iter->second > value ? iter->second - value : 0;
    if (iter->second == 0) {
        counter.erase(iter);
    }
}
}  // namespace

bool MemoryRebalanceScheduler::IsTerminalStatus(master::RebalanceTaskStatusPb status)
{
    return status == master::REBALANCE_TASK_SUCCEEDED || status == master::REBALANCE_TASK_FAILED
           || status == master::REBALANCE_TASK_EXPIRED;
}

bool MemoryRebalanceScheduler::IsFailedStatus(master::RebalanceTaskStatusPb status)
{
    return status == master::REBALANCE_TASK_FAILED || status == master::REBALANCE_TASK_EXPIRED;
}

uint64_t MemoryRebalanceScheduler::CalculateUsageRate(uint64_t usedMemory, uint64_t memoryCapacity)
{
    if (memoryCapacity == 0) {
        return PERCENT_BASE;
    }
    if (usedMemory > std::numeric_limits<uint64_t>::max() / PERCENT_BASE) {
        return PERCENT_BASE;
    }
    return std::min<uint64_t>(PERCENT_BASE, usedMemory * PERCENT_BASE / memoryCapacity);
}

uint64_t MemoryRebalanceScheduler::CalculateUsageRate(const NodeInfo &node)
{
    return CalculateUsageRate(node.usedMemory, node.memoryCapacity);
}

Status MemoryRebalanceScheduler::Schedule(const master::ResourceReportReqPb &req,
                                          const std::unordered_map<std::string, NodeInfo> &snapshot,
                                          master::ResourceReportRspPb &rsp)
{
    RETURN_OK_IF_TRUE(!FLAGS_enable_memory_rebalance);
    const std::string &reportingWorker = req.stat().address();
    RETURN_OK_IF_TRUE(reportingWorker.empty());

    uint64_t nowMs = GetSteadyClockTimeStampMs();
    std::lock_guard<std::mutex> lock(mutex_);
    ExpireTimeoutTasksLocked(nowMs, reportingWorker);

    auto activeTask = activeTasksBySource_.find(reportingWorker);
    if (activeTask != activeTasksBySource_.end()) {
        MarkTaskDispatchedLocked(activeTask->second);
        *rsp.mutable_rebalance_task() = activeTask->second.task;
        return Status::OK();
    }

    master::RebalanceTaskPb task;
    Status rc = TryBuildTaskLocked(snapshot, reportingWorker, nowMs, task);
    RETURN_OK_IF_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
    RETURN_IF_NOT_OK(rc);
    RETURN_OK_IF_TRUE(task.source_worker() != reportingWorker);

    RunningTask runningTask{ .task = task };
    activeTasksBySource_.emplace(task.source_worker(), runningTask);
    targetInflightBytes_[task.target_worker()] =
        SaturatingAdd(targetInflightBytes_[task.target_worker()], task.max_bytes());

    LOG(INFO) << FormatString(
        "[MemoryRebalance] assign task %s source=%s target=%s max_bytes=%lu deadline_ms=%lu", task.task_id(),
        task.source_worker(), task.target_worker(), task.max_bytes(), task.deadline_ms());
    INJECT_POINT_NO_RETURN("MemoryRebalanceScheduler.AssignTask");

    auto newTask = activeTasksBySource_.find(reportingWorker);
    if (newTask != activeTasksBySource_.end()) {
        MarkTaskDispatchedLocked(newTask->second);
        *rsp.mutable_rebalance_task() = newTask->second.task;
    }
    return Status::OK();
}

bool MemoryRebalanceScheduler::NeedSnapshotForSchedule(const master::ResourceReportReqPb &req,
                                                       const NodeInfo &reportingNode,
                                                       master::ResourceReportRspPb &rsp)
{
    if (!FLAGS_enable_memory_rebalance) {
        return false;
    }
    const std::string &reportingWorker = req.stat().address();
    if (reportingWorker.empty() || reportingNode.nodeId != reportingWorker) {
        return false;
    }

    uint64_t nowMs = GetSteadyClockTimeStampMs();
    std::lock_guard<std::mutex> lock(mutex_);
    ExpireTimeoutTasksLocked(nowMs, reportingWorker);

    auto activeTask = activeTasksBySource_.find(reportingWorker);
    if (activeTask != activeTasksBySource_.end()) {
        MarkTaskDispatchedLocked(activeTask->second);
        *rsp.mutable_rebalance_task() = activeTask->second.task;
        return false;
    }
    return IsSourceCandidateLocked(reportingNode, nowMs);
}

Status MemoryRebalanceScheduler::ReportResult(const master::ReportRebalanceResultReqPb &req,
                                              master::ReportRebalanceResultRspPb &rsp)
{
    (void)rsp;
    CHECK_FAIL_RETURN_STATUS(!req.task_id().empty(), K_INVALID, "The rebalance task id can not be empty");
    CHECK_FAIL_RETURN_STATUS(!req.source_worker().empty(), K_INVALID, "The rebalance source worker can not be empty");
    CHECK_FAIL_RETURN_STATUS(IsTerminalStatus(req.status()), K_INVALID, "The rebalance task status is not terminal");

    uint64_t nowMs = GetSteadyClockTimeStampMs();
    std::lock_guard<std::mutex> lock(mutex_);
    ExpireTimeoutTasksLocked(nowMs, req.source_worker());

    auto taskIt = activeTasksBySource_.find(req.source_worker());
    if (taskIt == activeTasksBySource_.end()) {
        LOG(INFO) << FormatString("[MemoryRebalance] ignore stale result task=%s source=%s target=%s status=%d",
                                  req.task_id(), req.source_worker(), req.target_worker(),
                                  static_cast<int>(req.status()));
        return Status::OK();
    }
    const auto &task = taskIt->second.task;
    CHECK_FAIL_RETURN_STATUS(task.source_worker() == req.source_worker(), K_RUNTIME_ERROR,
                             "Rebalance task source index is inconsistent");
    if (task.task_id() != req.task_id()) {
        LOG(INFO) << FormatString(
            "[MemoryRebalance] ignore stale result task=%s source=%s target=%s status=%d, active_task=%s",
            req.task_id(), req.source_worker(), req.target_worker(), static_cast<int>(req.status()), task.task_id());
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(task.target_worker() == req.target_worker(), K_INVALID,
                             "The rebalance result does not match the active task");

    LOG(INFO) << FormatString(
        "[MemoryRebalance] finish task %s source=%s target=%s status=%d migrated_bytes=%lu migrated_objects=%lu "
        "failed_objects=%lu reason=%s",
        req.task_id(), req.source_worker(), req.target_worker(), static_cast<int>(req.status()), req.migrated_bytes(),
        req.migrated_objects(), req.failed_objects(), req.failed_reason());
    RemoveTaskLocked(req.source_worker(), nowMs, IsFailedStatus(req.status()));
    return Status::OK();
}

void MemoryRebalanceScheduler::ExpireTimeoutTasksLocked(uint64_t nowMs, const std::string &activeWorker)
{
    std::vector<std::string> expiredSources;
    expiredSources.reserve(activeTasksBySource_.size());
    for (const auto &[source, runningTask] : activeTasksBySource_) {
        if (runningTask.task.deadline_ms() <= nowMs) {
            expiredSources.emplace_back(source);
        }
    }
    for (const auto &source : expiredSources) {
        auto taskIt = activeTasksBySource_.find(source);
        if (taskIt == activeTasksBySource_.end()) {
            continue;
        }
        LOG(WARNING) << FormatString("[MemoryRebalance] expire task %s", taskIt->second.task.task_id());
        INJECT_POINT_NO_RETURN("MemoryRebalanceScheduler.ExpireTask");
        RemoveTaskLocked(source, nowMs, true);
    }
    for (auto iter = cooldownUntilMs_.begin(); iter != cooldownUntilMs_.end();) {
        if (iter->second <= nowMs || iter->first == activeWorker) {
            iter = cooldownUntilMs_.erase(iter);
        } else {
            ++iter;
        }
    }
}

bool MemoryRebalanceScheduler::IsInCooldownLocked(const std::string &worker, uint64_t nowMs) const
{
    auto iter = cooldownUntilMs_.find(worker);
    return iter != cooldownUntilMs_.end() && iter->second > nowMs;
}

void MemoryRebalanceScheduler::AddCooldownLocked(const std::string &worker, uint64_t nowMs)
{
    if (worker.empty()) {
        return;
    }
    cooldownUntilMs_[worker] = nowMs + static_cast<uint64_t>(FLAGS_rebalance_cooldown_s) * MS_PER_SECOND;
}

void MemoryRebalanceScheduler::RemoveTaskLocked(const std::string &sourceWorker, uint64_t nowMs, bool addCooldown)
{
    auto taskIt = activeTasksBySource_.find(sourceWorker);
    if (taskIt != activeTasksBySource_.end()) {
        const auto &task = taskIt->second.task;
        DecreaseCounter(targetInflightBytes_, task.target_worker(), task.max_bytes());
        if (addCooldown) {
            AddCooldownLocked(task.source_worker(), nowMs);
            AddCooldownLocked(task.target_worker(), nowMs);
        }
        activeTasksBySource_.erase(taskIt);
    }
}

void MemoryRebalanceScheduler::MarkTaskDispatchedLocked(RunningTask &runningTask)
{
    if (runningTask.dispatched) {
        return;
    }
    runningTask.dispatched = true;
}

uint64_t MemoryRebalanceScheduler::GetTargetInflightBytesLocked(const std::string &targetWorker) const
{
    auto inFlightIt = targetInflightBytes_.find(targetWorker);
    if (inFlightIt != targetInflightBytes_.end()) {
        return inFlightIt->second;
    }
    return 0;
}

bool MemoryRebalanceScheduler::IsSourceCandidateLocked(const NodeInfo &node, uint64_t nowMs) const
{
    uint64_t targetInflightBytes = GetTargetInflightBytesLocked(node.nodeId);
    bool hasInboundTask = targetInflightBytes > 0;
    return node.isReady && node.memoryCapacity > 0
           && CalculateUsageRate(node) >= FLAGS_rebalance_source_usage_percent
           && activeTasksBySource_.find(node.nodeId) == activeTasksBySource_.end() && !hasInboundTask
           && !IsInCooldownLocked(node.nodeId, nowMs);
}

void MemoryRebalanceScheduler::CollectWorkerCandidatesLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                                             const std::string &sourceWorker, uint64_t nowMs,
                                                             std::vector<const NodeInfo *> &sources,
                                                             std::vector<const NodeInfo *> &targets) const
{
    sources.reserve(1);
    targets.reserve(snapshot.size());
    for (const auto &[worker, node] : snapshot) {
        (void)worker;
        if (node.nodeId == sourceWorker && IsSourceCandidateLocked(node, nowMs)) {
            sources.emplace_back(&node);
        }
        if (node.isReady && node.memoryCapacity > 0 && !IsInCooldownLocked(node.nodeId, nowMs)) {
            targets.emplace_back(&node);
        }
    }
}

void MemoryRebalanceScheduler::CollectCandidatePairsLocked(const std::vector<const NodeInfo *> &sources,
                                                           const std::vector<const NodeInfo *> &targets,
                                                           std::vector<CandidatePair> &targetPairs) const
{
    targetPairs.reserve(sources.size() * targets.size());
    for (const auto *source : sources) {
        for (const auto *target : targets) {
            if (source->nodeId == target->nodeId) {
                continue;
            }
            uint64_t targetInflightBytes = GetTargetInflightBytesLocked(target->nodeId);
            uint64_t maxBytes = CalculateTaskBytesLocked(*source, *target, targetInflightBytes);
            if (maxBytes == 0) {
                continue;
            }
            const uint64_t sourceUsage = CalculateUsageRate(*source);
            const uint64_t targetUsage = CalculateUsageRate(*target);
            const uint64_t usageGap = sourceUsage > targetUsage ? sourceUsage - targetUsage : 0;
            if (usageGap < FLAGS_rebalance_usage_gap_percent) {
                continue;
            }

            CandidatePair pair;
            pair.source = source;
            pair.target = target;
            pair.maxBytes = maxBytes;
            pair.targetAvailableAfterInFlight = SubOrZero(target->availableMemory, targetInflightBytes);
            pair.usageGapRate = static_cast<uint32_t>(usageGap);
            pair.projectedTargetUsageRate =
                static_cast<uint32_t>(CalculateProjectedTargetUsageRate(*target, targetInflightBytes, maxBytes));
            targetPairs.emplace_back(pair);
        }
    }
}

void MemoryRebalanceScheduler::FillTaskFromPairLocked(const CandidatePair &bestPair, uint64_t nowMs,
                                                      master::RebalanceTaskPb &task) const
{
    uint64_t createTimeMs = nowMs;
    task.set_task_id("memory-rebalance-" + GetStringUuid());
    task.set_source_worker(bestPair.source->nodeId);
    task.set_target_worker(bestPair.target->nodeId);
    task.set_max_bytes(bestPair.maxBytes);
    task.set_create_time_ms(createTimeMs);
    auto rate_bytes_per_sec = static_cast<uint64_t>(FLAGS_data_migrate_rate_limit_mb) * 1024 * 1024;
    auto estimated_transfer_ms = ((bestPair.maxBytes + rate_bytes_per_sec - 1) / rate_bytes_per_sec) * MS_PER_SECOND;
    task.set_deadline_ms(createTimeMs + estimated_transfer_ms * TRANSFER_TIME_MULTIPLIER +
                         static_cast<uint64_t>(FLAGS_rebalance_task_report_grace_ms));
}

Status MemoryRebalanceScheduler::TryBuildTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                                    const std::string &sourceWorker, uint64_t nowMs,
                                                    master::RebalanceTaskPb &task)
{
    CHECK_FAIL_RETURN_STATUS(snapshot.size() >= MIN_REBALANCE_WORKER_COUNT, K_NOT_FOUND,
                             "No enough workers for memory rebalance");

    std::vector<const NodeInfo *> sources;
    std::vector<const NodeInfo *> targets;
    CollectWorkerCandidatesLocked(snapshot, sourceWorker, nowMs, sources, targets);
    CHECK_FAIL_RETURN_STATUS(!sources.empty() && !targets.empty(), K_NOT_FOUND,
                             "No source or target worker is suitable for memory rebalance");

    std::vector<CandidatePair> targetPairs;
    CollectCandidatePairsLocked(sources, targets, targetPairs);
    std::sort(targetPairs.begin(), targetPairs.end(), [](const CandidatePair &lhs, const CandidatePair &rhs) {
        if (lhs.usageGapRate != rhs.usageGapRate) {
            return lhs.usageGapRate > rhs.usageGapRate;
        }
        if (lhs.projectedTargetUsageRate != rhs.projectedTargetUsageRate) {
            return lhs.projectedTargetUsageRate < rhs.projectedTargetUsageRate;
        }
        if (lhs.targetAvailableAfterInFlight != rhs.targetAvailableAfterInFlight) {
            return lhs.targetAvailableAfterInFlight > rhs.targetAvailableAfterInFlight;
        }
        if (lhs.source->nodeId != rhs.source->nodeId) {
            return lhs.source->nodeId < rhs.source->nodeId;
        }
        return lhs.target->nodeId < rhs.target->nodeId;
    });
    CHECK_FAIL_RETURN_STATUS(!targetPairs.empty(), K_NOT_FOUND,
                             "No source target pair is suitable for memory rebalance");
    const CandidatePair &bestPair = targetPairs.front();

    FillTaskFromPairLocked(bestPair, nowMs, task);
    LOG(INFO) << FormatString(
        "[MemoryRebalance] select source=%s(%lu%%) target=%s(%lu%% -> %u%%), max_bytes=%lu, "
        "target_available_after_in_flight=%lu, usage_gap=%u%%",
        bestPair.source->nodeId, CalculateUsageRate(*bestPair.source), bestPair.target->nodeId,
        CalculateUsageRate(*bestPair.target), bestPair.projectedTargetUsageRate, bestPair.maxBytes,
        bestPair.targetAvailableAfterInFlight, bestPair.usageGapRate);
    return Status::OK();
}

uint64_t MemoryRebalanceScheduler::CalculateTaskBytesLocked(const NodeInfo &source, const NodeInfo &target,
                                                            uint64_t targetInflightBytes) const
{
    if (source.usedMemory <= target.usedMemory) {
        return 0;
    }
    const uint64_t usageGapBytes = (source.usedMemory - target.usedMemory) / 2;
    const uint64_t targetAvailableAfterInFlight = SubOrZero(target.availableMemory, targetInflightBytes);
    return std::min({ usageGapBytes, targetAvailableAfterInFlight, FLAGS_rebalance_max_migrate_bytes_per_round });
}

uint64_t MemoryRebalanceScheduler::CalculateProjectedTargetUsageRate(const NodeInfo &target,
                                                                     uint64_t targetInflightBytes,
                                                                     uint64_t maxBytes) const
{
    uint64_t projectedUsed = target.usedMemory;
    projectedUsed = SaturatingAdd(projectedUsed, targetInflightBytes);
    projectedUsed = SaturatingAdd(projectedUsed, maxBytes);
    return CalculateUsageRate(projectedUsed, target.memoryCapacity);
}

}  // namespace master
}  // namespace datasystem
