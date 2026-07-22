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

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/math_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace master {
namespace {
constexpr uint64_t PERCENT_BASE = 100;
constexpr uint64_t MS_PER_SECOND = 1'000;
constexpr size_t MIN_REBALANCE_WORKER_COUNT = 2;
constexpr uint64_t TRANSFER_TIME_MULTIPLIER = 2;
// Floor for the held in-flight GC TTL. The TTL tracks node_dead_timeout_s but a high-perf
// cluster may shrink that flag (e.g. to 5s); this floor keeps the TTL above one ~30s worker
// report cycle (+ margin) so a merely-slow (still alive) target is not released before its
// own report and re-picked on a stale snapshot. GC only -- alive targets release on their
// own report (<=30s) well before this fires.
constexpr uint64_t HOLD_TTL_MIN_S = 60;

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

void MemoryRebalanceScheduler::SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership)
{
    std::lock_guard<std::mutex> lock(mutex_);
    topologyMembership_ = topologyMembership;
}

bool MemoryRebalanceScheduler::IsTerminalStatus(master::RebalanceTaskStatusPb status)
{
    return status == master::REBALANCE_TASK_SUCCEEDED || status == master::REBALANCE_TASK_FAILED
           || status == master::REBALANCE_TASK_EXPIRED;
}

bool MemoryRebalanceScheduler::IsFailedStatus(master::RebalanceTaskStatusPb status)
{
    return status == master::REBALANCE_TASK_FAILED || status == master::REBALANCE_TASK_EXPIRED;
}

uint64_t MemoryRebalanceScheduler::CalculateUsageRate(uint64_t usedMemory, uint64_t memoryLimit)
{
    if (memoryLimit == 0) {
        // Unknown memory limit is treated as full usage so it cannot look like a low-usage target.
        // Callers must still guard memoryLimit > 0 before adding a worker to rebalance candidates.
        return PERCENT_BASE;
    }
    if (usedMemory > std::numeric_limits<uint64_t>::max() / PERCENT_BASE) {
        return PERCENT_BASE;
    }
    return std::min<uint64_t>(PERCENT_BASE, usedMemory * PERCENT_BASE / memoryLimit);
}

uint64_t MemoryRebalanceScheduler::CalculateUsageRate(const NodeInfo &node)
{
    return CalculateUsageRate(node.usedMemory, node.memoryLimit);
}

Status MemoryRebalanceScheduler::Schedule(const master::ResourceReportReqPb &req,
                                          const std::unordered_map<std::string, NodeInfo> &snapshot,
                                          master::ResourceReportRspPb &rsp)
{
    RETURN_OK_IF_TRUE(!FLAGS_enable_memory_rebalance);
    const std::string &reportingWorker = req.stat().address();
    RETURN_OK_IF_TRUE(reportingWorker.empty());

    auto topologySnapshot = GetTopologySnapshot();
    uint64_t nowMs = GetSteadyClockTimeStampMs();
    std::lock_guard<std::mutex> lock(mutex_);
    ExpireTimeoutTasksLocked(nowMs, reportingWorker);
    ReleaseSnapshotHoldsLocked(snapshot);

    auto activeTask = activeTasksBySource_.find(reportingWorker);
    if (activeTask != activeTasksBySource_.end()) {
        MarkTaskDispatchedLocked(activeTask->second);
        *rsp.mutable_rebalance_task() = activeTask->second.task;
        return Status::OK();
    }

    master::RebalanceTaskPb task;
    Status rc = TryBuildTaskLocked(snapshot, reportingWorker, nowMs, topologySnapshot.get(), task);
    RETURN_OK_IF_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
    RETURN_IF_NOT_OK(rc);
    RETURN_OK_IF_TRUE(task.source_worker() != reportingWorker);

    RunningTask runningTask{ .task = task };
    activeTasksBySource_.emplace(task.source_worker(), runningTask);
    targetInflightBytes_[task.target_worker()] =
        SaturatingAdd(targetInflightBytes_[task.target_worker()], task.max_bytes());

    LOG(INFO) << FormatString(
        "[MemoryRebalance] assign task %s source=%s target=%s max_bytes=%lu timeout_ms=%lu deadline_ms=%lu",
        task.task_id(), task.source_worker(), task.target_worker(), task.max_bytes(), task.timeout_ms(),
        task.deadline_ms());
    INJECT_POINT_NO_RETURN("MemoryRebalanceScheduler.AssignTask");

    auto newTask = activeTasksBySource_.find(reportingWorker);
    if (newTask != activeTasksBySource_.end()) {
        MarkTaskDispatchedLocked(newTask->second);
        *rsp.mutable_rebalance_task() = newTask->second.task;
    }
    return Status::OK();
}

bool MemoryRebalanceScheduler::NeedSnapshotForSchedule(const master::ResourceReportReqPb &req,
                                                       const NodeInfo &reportingNode, master::ResourceReportRspPb &rsp)
{
    if (!FLAGS_enable_memory_rebalance) {
        return false;
    }
    const std::string &reportingWorker = req.stat().address();
    if (reportingWorker.empty() || reportingNode.nodeId != reportingWorker) {
        return false;
    }

    auto topologySnapshot = GetTopologySnapshot();
    uint64_t nowMs = GetSteadyClockTimeStampMs();
    std::lock_guard<std::mutex> lock(mutex_);
    ExpireTimeoutTasksLocked(nowMs, reportingWorker);
    ReleaseReporterHoldsLocked(reportingWorker, reportingNode.timestamp);

    auto activeTask = activeTasksBySource_.find(reportingWorker);
    if (activeTask != activeTasksBySource_.end()) {
        MarkTaskDispatchedLocked(activeTask->second);
        *rsp.mutable_rebalance_task() = activeTask->second.task;
        return false;
    }
    return IsSourceCandidateLocked(reportingNode, nowMs, topologySnapshot.get());
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
    RemoveTaskLocked(req.source_worker(), nowMs, !IsFailedStatus(req.status()));
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
        RemoveTaskLocked(source, nowMs, false);
    }
    for (auto iter = cooldownUntilMs_.begin(); iter != cooldownUntilMs_.end();) {
        if (iter->second <= nowMs || iter->first == activeWorker) {
            iter = cooldownUntilMs_.erase(iter);
        } else {
            ++iter;
        }
    }
    // issue #685: GC held in-flight charges whose target never reported back. Kept in a helper so
    // ExpireTimeoutTasksLocked stays under the 50-line limit; the helper also sweeps orphan
    // holdSinceMs_ entries for defensive consistency (the two maps are always updated together,
    // so an orphan indicates a logic bug worth a WARNING).
    GcHeldInflightLocked(nowMs);
}

void MemoryRebalanceScheduler::GcHeldInflightLocked(uint64_t nowMs)
{
    const uint64_t holdTtlMs =
        std::max(static_cast<uint64_t>(FLAGS_node_dead_timeout_s), HOLD_TTL_MIN_S) * MS_PER_SECOND;
    for (auto it = pendingReleaseBytes_.begin(); it != pendingReleaseBytes_.end();) {
        auto hIt = holdSinceMs_.find(it->first);
        if (hIt == holdSinceMs_.end()) {
            // Defensive: pendingRelease without holdSinceMs is inconsistent (the two are always
            // updated together). Release the charge so targetInflightBytes_ cannot get stuck high
            // and permanently reject the target.
            LOG(WARNING) << FormatString(
                "[MemoryRebalance] orphan pendingRelease target=%s bytes=%lu (no holdSinceMs); releasing", it->first,
                it->second);
            DecreaseCounter(targetInflightBytes_, it->first, it->second);
            it = pendingReleaseBytes_.erase(it);
            continue;
        }
        if (nowMs - hIt->second > holdTtlMs) {
            LOG(WARNING) << FormatString(
                "[MemoryRebalance] release held in-flight target=%s after TTL %lus (hold_age=%lums, bytes=%lu)",
                it->first, holdTtlMs / MS_PER_SECOND, nowMs - hIt->second, it->second);
            DecreaseCounter(targetInflightBytes_, it->first, it->second);
            holdSinceMs_.erase(hIt);
            it = pendingReleaseBytes_.erase(it);
        } else {
            ++it;
        }
    }
    // Defensive: clean orphan holdSinceMs entries not backed by pendingReleaseBytes_.
    for (auto hIt = holdSinceMs_.begin(); hIt != holdSinceMs_.end();) {
        if (pendingReleaseBytes_.find(hIt->first) == pendingReleaseBytes_.end()) {
            LOG(WARNING) << FormatString("[MemoryRebalance] orphan holdSinceMs target=%s (no pendingRelease); erasing",
                                         hIt->first);
            hIt = holdSinceMs_.erase(hIt);
        } else {
            ++hIt;
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

void MemoryRebalanceScheduler::RemoveTaskLocked(const std::string &sourceWorker, uint64_t nowMs, bool success)
{
    auto taskIt = activeTasksBySource_.find(sourceWorker);
    if (taskIt != activeTasksBySource_.end()) {
        const auto &task = taskIt->second.task;
        if (success) {
            // issue #685: do NOT clear the in-flight charge on success. The target's snapshot
            // still shows stale-low usage until it reports again, so clearing now would let the
            // next source re-pick the just-received target. Hold the charge; it is released by
            // ReleaseReporterHoldsLocked / ReleaseSnapshotHoldsLocked when the target reports.
            pendingReleaseBytes_[task.target_worker()] =
                SaturatingAdd(pendingReleaseBytes_[task.target_worker()], task.max_bytes());
            auto holdIt = holdSinceMs_.find(task.target_worker());
            if (holdIt == holdSinceMs_.end() || nowMs > holdIt->second) {
                holdSinceMs_[task.target_worker()] = nowMs;  // keep the latest completion time
            }
            LOG(INFO) << FormatString(
                "[MemoryRebalance] hold in-flight target=%s bytes=%lu pending=%lu until target reports",
                task.target_worker(), task.max_bytes(), pendingReleaseBytes_[task.target_worker()]);
        } else {
            // Failure/expire: data did not land on the target, so the in-flight charge is bogus.
            DecreaseCounter(targetInflightBytes_, task.target_worker(), task.max_bytes());
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

void MemoryRebalanceScheduler::ReleaseReporterHoldsLocked(const std::string &worker, uint64_t reportTimestamp)
{
    auto it = pendingReleaseBytes_.find(worker);
    if (it == pendingReleaseBytes_.end()) {
        return;  // no held charge for this worker
    }
    auto holdIt = holdSinceMs_.find(worker);
    if (holdIt == holdSinceMs_.end() || reportTimestamp <= holdIt->second) {
        return;  // worker has not reported since its latest held completion
    }
    // The worker just reported (reportTimestamp is fresh), so its snapshot now reflects the
    // post-receive memory. Drop the held charge so the worker can be re-evaluated on its merits.
    LOG(INFO) << FormatString(
        "[MemoryRebalance] release held in-flight target=%s bytes=%lu hold_age=%lums via reporter", worker, it->second,
        reportTimestamp - holdIt->second);
    DecreaseCounter(targetInflightBytes_, worker, it->second);
    pendingReleaseBytes_.erase(it);
    holdSinceMs_.erase(holdIt);
}

void MemoryRebalanceScheduler::ReleaseSnapshotHoldsLocked(const std::unordered_map<std::string, NodeInfo> &snapshot)
{
    // Backup release path for non-reporting targets: a target whose snapshot timestamp advanced
    // past its held completion time has had its memory re-published by the master snapshot swap.
    // The swap runs every FLAGS_master_snapshot_swap_interval_s (~10s), so a non-reporting
    // target's hold may persist up to ~10s after its snapshot is actually fresh -- within that
    // window CollectCandidatePairsLocked may over-estimate its projected usage and briefly skip
    // it. Acceptable: the reporter path (ReleaseReporterHoldsLocked) is the primary release.
    std::vector<std::string> toRelease;
    for (const auto &[worker, bytes] : pendingReleaseBytes_) {
        auto nit = snapshot.find(worker);
        if (nit == snapshot.end()) {
            continue;  // worker no longer in the snapshot (down / scaled away)
        }
        auto holdIt = holdSinceMs_.find(worker);
        if (holdIt == holdSinceMs_.end() || nit->second.timestamp <= holdIt->second) {
            continue;  // snapshot timestamp still predates the held completion
        }
        LOG(INFO) << FormatString(
            "[MemoryRebalance] release held in-flight target=%s bytes=%lu hold_age=%lums via snapshot-swap", worker,
            bytes, nit->second.timestamp - holdIt->second);
        DecreaseCounter(targetInflightBytes_, worker, bytes);
        toRelease.emplace_back(worker);
    }
    for (const auto &worker : toRelease) {
        pendingReleaseBytes_.erase(worker);
        holdSinceMs_.erase(worker);
    }
}

std::shared_ptr<const cluster::TopologySnapshot> MemoryRebalanceScheduler::GetTopologySnapshot()
{
    const cluster::MembershipEndpointView *topologyMembership = nullptr;
    {
        // Copy the non-owning view under the scheduler lock, then invoke it without the lock. SetTopologyMembership is
        // an initialization operation; WorkerOCServer also destroys this scheduler before the topology engine.
        std::lock_guard<std::mutex> lock(mutex_);
        topologyMembership = topologyMembership_;
    }
    if (topologyMembership == nullptr) {
        return nullptr;
    }
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    auto rc = topologyMembership->GetSnapshot(snapshot);
    if (rc.IsError() || snapshot == nullptr) {
        LOG_FIRST_N(WARNING, 1) << "[MemoryRebalance] topology snapshot is not ready; "
                                   "falling back to the resource snapshot: "
                                << rc.ToString();
        return nullptr;
    }
    return snapshot;
}

bool MemoryRebalanceScheduler::IsWorkerActiveInTopology(
    const std::string &worker, const cluster::TopologySnapshot *topologySnapshot) const
{
    if (topologySnapshot == nullptr) {
        return true;
    }
    const cluster::Member *member = nullptr;
    auto rc = topologySnapshot->FindMemberByAddress(worker, member);
    return rc.IsOk() && member != nullptr && member->state == cluster::MemberState::ACTIVE;
}

bool MemoryRebalanceScheduler::IsSourceCandidateLocked(
    const NodeInfo &node, uint64_t nowMs, const cluster::TopologySnapshot *topologySnapshot) const
{
    uint64_t targetInflightBytes = GetTargetInflightBytesLocked(node.nodeId);
    bool hasInboundTask = targetInflightBytes > 0;
    return node.isReady && IsWorkerActiveInTopology(node.nodeId, topologySnapshot) && node.memoryLimit > 0
           && CalculateUsageRate(node) >= FLAGS_rebalance_source_usage_percent
           && activeTasksBySource_.find(node.nodeId) == activeTasksBySource_.end() && !hasInboundTask
           && !IsInCooldownLocked(node.nodeId, nowMs);
}

void MemoryRebalanceScheduler::CollectWorkerCandidatesLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                                             const std::string &sourceWorker, uint64_t nowMs,
                                                             const cluster::TopologySnapshot *topologySnapshot,
                                                             std::vector<const NodeInfo *> &sources,
                                                             std::vector<const NodeInfo *> &targets) const
{
    sources.reserve(1);
    targets.reserve(snapshot.size());
    for (const auto &[worker, node] : snapshot) {
        (void)worker;
        if (node.nodeId == sourceWorker && IsSourceCandidateLocked(node, nowMs, topologySnapshot)) {
            sources.emplace_back(&node);
        }
        if (node.isReady && IsWorkerActiveInTopology(node.nodeId, topologySnapshot) && node.memoryLimit > 0
            && !IsInCooldownLocked(node.nodeId, nowMs)) {
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
            // issue #685 H3: do not pick a target whose projected post-migration usage would
            // reach the source threshold -- migrating to it would just create a future source
            // (logs685 W2: master projected .103 "37% -> 85%" and still dispatched). Projection
            // is salcx-unit-accurate only with the #1346 migrated_bytes fix; without it the
            // executor moves ~1.25x max_bytes so a [~64%,70%) projected band still overshoots.
            if (pair.projectedTargetUsageRate >= FLAGS_rebalance_source_usage_percent) {
                continue;
            }
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
    auto transferTimeoutMs = SaturatingMultiply(estimated_transfer_ms, TRANSFER_TIME_MULTIPLIER);
    auto timeoutMs = SaturatingAdd(transferTimeoutMs, static_cast<uint64_t>(FLAGS_rebalance_task_report_grace_ms));
    task.set_timeout_ms(timeoutMs);
    task.set_deadline_ms(SaturatingAdd(createTimeMs, timeoutMs));
}

Status MemoryRebalanceScheduler::TryBuildTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                                    const std::string &sourceWorker, uint64_t nowMs,
                                                    const cluster::TopologySnapshot *topologySnapshot,
                                                    master::RebalanceTaskPb &task)
{
    CHECK_FAIL_RETURN_STATUS(snapshot.size() >= MIN_REBALANCE_WORKER_COUNT, K_NOT_FOUND,
                             "No enough workers for memory rebalance");

    std::vector<const NodeInfo *> sources;
    std::vector<const NodeInfo *> targets;
    CollectWorkerCandidatesLocked(snapshot, sourceWorker, nowMs, topologySnapshot, sources, targets);
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
    return CalculateUsageRate(projectedUsed, target.memoryLimit);
}

}  // namespace master
}  // namespace datasystem
