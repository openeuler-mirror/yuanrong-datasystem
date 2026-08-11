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
// Previously gflags rebalance_cooldown_s / rebalance_max_migrate_bytes_per_round; the knobs were
// removed but the cooldown behavior is retained as a constant. CooldownSeconds inject below lets
// tests shorten the cooldown without re-introducing a flag.
constexpr uint32_t REBALANCE_COOLDOWN_S = 60;
// Per-task (per-batch) migration cap. The old per-round 1GB cap was removed so the rebalance can
// keep issuing batches until the usage gap converges to the midpoint; the 1GB cap is replaced by
// a 300MB per-task batch so master gets fresh target memory feedback between batches.
constexpr uint64_t REBALANCE_MAX_BYTES_PER_TASK = 300 * 1024ul * 1024ul;
// Wall-clock budget for one chain of 300MB batches. Matches the default ResourceReport cycle: a
// chain started in cycle N must end before cycle N+1's snapshot is taken, otherwise it competes
// with the next cycle's fresh budget and foreground work. The flag-driven migration rate limit
// (~40 MiB/s default) bounds throughput; this bounds wall-clock duty cycle.
constexpr uint64_t REBALANCE_EPOCH_BUDGET_MS = 30 * MS_PER_SECOND;

uint64_t SubOrZero(uint64_t lhs, uint64_t rhs)
{
    return lhs > rhs ? lhs - rhs : 0;
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
    ExpireTimeoutTasksLocked(nowMs);
    ReleaseSnapshotHoldsLocked(snapshot);

    auto activeTask = activeTasksBySource_.find(reportingWorker);
    if (activeTask != activeTasksBySource_.end()) {
        MarkTaskDispatchedLocked(activeTask->second);
        *rsp.mutable_rebalance_task() = activeTask->second.task;
        return Status::OK();
    }

    RunningTask runningTask;
    Status rc = TryBuildTaskLocked(snapshot, reportingWorker, nowMs, topologySnapshot.get(), runningTask);
    RETURN_OK_IF_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
    RETURN_IF_NOT_OK(rc);
    RETURN_OK_IF_TRUE(runningTask.task.source_worker() != reportingWorker);

    const auto &task = runningTask.task;
    activeTasksBySource_.emplace(task.source_worker(), runningTask);
    futureView_[task.target_worker()].inflightBytes =
        SaturatingAdd(futureView_[task.target_worker()].inflightBytes, task.max_bytes());

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
    ExpireTimeoutTasksLocked(nowMs);
    ReleaseReporterHoldsLocked(reportingWorker, reportingNode.timestamp);

    auto activeTask = activeTasksBySource_.find(reportingWorker);
    if (activeTask != activeTasksBySource_.end()) {
        MarkTaskDispatchedLocked(activeTask->second);
        *rsp.mutable_rebalance_task() = activeTask->second.task;
        return false;
    }
    return IsSourceCandidateLocked(reportingNode, nowMs, topologySnapshot.get());
}

Status MemoryRebalanceScheduler::ReplayOrIgnoreStaleLocked(
    std::unordered_map<std::string, RunningTask>::iterator taskIt,
    const master::ReportRebalanceResultReqPb &req, master::ReportRebalanceResultRspPb &rsp)
{
    // Predecessor's response was lost in flight; worker retries it. If the active entry's
    // immediatePredecessorTaskId matches, replay the successor so the chain continues.
    if (!taskIt->second.immediatePredecessorTaskId.empty()
        && taskIt->second.immediatePredecessorTaskId == req.task_id()) {
        *rsp.mutable_next_rebalance_task() = taskIt->second.task;
        LOG(INFO) << FormatString(
            "[MemoryRebalance] replay cached successor for stale predecessor task=%s source=%s "
            "active_task=%s",
            req.task_id(), req.source_worker(), taskIt->second.task.task_id());
        return Status::OK();
    }
    LOG(INFO) << FormatString(
        "[MemoryRebalance] ignore stale result task=%s source=%s target=%s status=%d, active_task=%s",
        req.task_id(), req.source_worker(), req.target_worker(), static_cast<int>(req.status()),
        taskIt->second.task.task_id());
    return Status::OK();
}

Status MemoryRebalanceScheduler::ReportResult(const master::ReportRebalanceResultReqPb &req,
                                              master::ReportRebalanceResultRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(!req.task_id().empty(), K_INVALID, "The rebalance task id can not be empty");
    CHECK_FAIL_RETURN_STATUS(!req.source_worker().empty(), K_INVALID, "The rebalance source worker can not be empty");
    CHECK_FAIL_RETURN_STATUS(IsTerminalStatus(req.status()), K_INVALID, "The rebalance task status is not terminal");

    uint64_t nowMs = GetSteadyClockTimeStampMs();
    std::lock_guard<std::mutex> lock(mutex_);
    ExpireTimeoutTasksLocked(nowMs);

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
        return ReplayOrIgnoreStaleLocked(taskIt, req, rsp);
    }
    CHECK_FAIL_RETURN_STATUS(task.target_worker() == req.target_worker(), K_INVALID,
                             "The rebalance result does not match the active task");

    LOG(INFO) << FormatString(
        "[MemoryRebalance] finish task %s source=%s target=%s status=%d failure_side=%d migrated_bytes=%lu "
        "migrated_objects=%lu failed_objects=%lu reason=%s",
        req.task_id(), req.source_worker(), req.target_worker(), static_cast<int>(req.status()),
        static_cast<int>(req.failure_side()), req.migrated_bytes(), req.migrated_objects(), req.failed_objects(),
        req.failed_reason());
    RunningTask prevTask = taskIt->second;
    const bool succeeded = !IsFailedStatus(req.status());
    RemoveTaskLocked(req.source_worker(), nowMs, succeeded, req.failure_side());

    ProcessFreshFeedbackLocked(prevTask, req, nowMs, rsp);
    return Status::OK();
}

void MemoryRebalanceScheduler::ProcessFreshFeedbackLocked(RunningTask &prevTask,
                                                          const master::ReportRebalanceResultReqPb &req,
                                                          uint64_t nowMs,
                                                          master::ReportRebalanceResultRspPb &rsp)
{
    // proto3 scalar uint64 has no presence; treat 0 (wire default, old worker) and UINT64_MAX
    // (new worker sentinel) both as "no fresh signal". Chain ends; genuine remain=0 also lands
    // here -- the watermark stop in BuildNextBatchTaskLocked would have fired anyway.
    if (IsFailedStatus(req.status()) || req.target_remain_bytes() == 0
        || req.target_remain_bytes() == UINT64_MAX) {
        return;
    }
    prevTask.cumulativeMigrated += req.migrated_bytes();
    // Don't continue the chain if the batch was partial (candidates exhausted, didn't reach
    // max_bytes); the next batch would immediately fail with NO_CANDIDATE and trigger a cooldown.
    if (req.migrated_bytes() < prevTask.task.max_bytes()) {
        return;
    }
    master::RebalanceTaskPb nextTask;
    if (BuildNextBatchTaskLocked(prevTask, req.target_remain_bytes(), nowMs, nextTask)) {
        // Release the held charge only when a next batch is built. If the chain ends, keep the
        // held so the #685 stale-snapshot guard protects the target until its own ResourceReport
        // arrives. BuildNextBatchTaskLocked already excluded held from its inflight projection.
        auto deltaIt = futureView_.find(req.target_worker());
        if (deltaIt != futureView_.end() && deltaIt->second.heldBytes > 0) {
            ReleaseHeldLocked(req.target_worker(), deltaIt->second.heldBytes);
        }
        RunningTask nextRunningTask;
        nextRunningTask.task = nextTask;
        nextRunningTask.targetMemoryLimit = prevTask.targetMemoryLimit;
        nextRunningTask.targetMemoryCapacity = prevTask.targetMemoryCapacity;
        nextRunningTask.totalBudget = prevTask.totalBudget;
        nextRunningTask.cumulativeMigrated = prevTask.cumulativeMigrated;
        // Propagate the chain's epoch start so BuildNextBatchTaskLocked can enforce the
        // wall-clock budget across batches; and record this predecessor so a lost-response retry
        // of the predecessor replays this successor (idempotent ReportResult).
        nextRunningTask.epochStartMs = prevTask.epochStartMs;
        nextRunningTask.immediatePredecessorTaskId = prevTask.task.task_id();
        activeTasksBySource_.emplace(nextTask.source_worker(), nextRunningTask);
        futureView_[nextTask.target_worker()].inflightBytes =
            SaturatingAdd(futureView_[nextTask.target_worker()].inflightBytes, nextTask.max_bytes());
        *rsp.mutable_next_rebalance_task() = nextTask;
        INJECT_POINT_NO_RETURN("MemoryRebalanceScheduler.ReportResult.AssignNextTask");
    }
}

void MemoryRebalanceScheduler::ExpireTimeoutTasksLocked(uint64_t nowMs)
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
        RemoveTaskLocked(source, nowMs, false, master::REBALANCE_FAILURE_UNKNOWN);
    }
    ExpireCooldownsLocked(nowMs);
    // issue #685: GC held in-flight charges whose target never reported back.
    GcHeldInflightLocked(nowMs);
}

void MemoryRebalanceScheduler::ExpireCooldownsLocked(uint64_t nowMs)
{
    for (auto iter = cooldownUntilMs_.begin(); iter != cooldownUntilMs_.end();) {
        if (iter->second <= nowMs) {
            iter = cooldownUntilMs_.erase(iter);
        } else {
            ++iter;
        }
    }
    for (auto source = pairCooldownUntilMs_.begin(); source != pairCooldownUntilMs_.end();) {
        for (auto target = source->second.begin(); target != source->second.end();) {
            if (target->second <= nowMs) {
                target = source->second.erase(target);
            } else {
                ++target;
            }
        }
        if (source->second.empty()) {
            source = pairCooldownUntilMs_.erase(source);
        } else {
            ++source;
        }
    }
}

void MemoryRebalanceScheduler::GcHeldInflightLocked(uint64_t nowMs)
{
    const uint64_t holdTtlMs =
        std::max(static_cast<uint64_t>(FLAGS_node_dead_timeout_s), HOLD_TTL_MIN_S) * MS_PER_SECOND;
    // Collect targets whose held charge outlived the TTL, then release outside the iteration so
    // DecreaseInflightLocked's erase-on-zero cannot invalidate the range iterator.
    std::vector<std::pair<std::string, uint64_t>> expired;
    for (const auto &[worker, delta] : futureView_) {
        if (delta.heldBytes == 0) {
            continue;  // no held charge on this target
        }
        // heldBytes > 0 implies holdSinceMs was set (the two are updated together on success), so
        // there is no orphan case to defend here -- the single FutureDelta entry keeps them paired.
        if (nowMs - delta.holdSinceMs > holdTtlMs) {
            expired.emplace_back(worker, delta.heldBytes);
        }
    }
    for (const auto &[worker, heldBytes] : expired) {
        LOG(WARNING) << FormatString(
            "[MemoryRebalance] release held in-flight target=%s after TTL %lus (hold_age=%lums, bytes=%lu)",
            worker, holdTtlMs / MS_PER_SECOND, nowMs - futureView_.at(worker).holdSinceMs, heldBytes);
        ReleaseHeldLocked(worker, heldBytes);
    }
}

bool MemoryRebalanceScheduler::IsInCooldownLocked(const std::string &worker, uint64_t nowMs) const
{
    auto iter = cooldownUntilMs_.find(worker);
    return iter != cooldownUntilMs_.end() && iter->second > nowMs;
}

bool MemoryRebalanceScheduler::IsPairInCooldownLocked(const std::string &source, const std::string &target,
                                                      uint64_t nowMs) const
{
    auto sourceIt = pairCooldownUntilMs_.find(source);
    if (sourceIt == pairCooldownUntilMs_.end()) {
        return false;
    }
    auto targetIt = sourceIt->second.find(target);
    return targetIt != sourceIt->second.end() && targetIt->second > nowMs;
}

uint64_t MemoryRebalanceScheduler::CalculateCooldownDeadlineMs(uint64_t nowMs)
{
    uint32_t cooldownS = REBALANCE_COOLDOWN_S;
    INJECT_POINT_NO_RETURN("MemoryRebalanceScheduler.CooldownSeconds",
                           [&cooldownS](uint32_t seconds) { cooldownS = seconds; });
    return nowMs + static_cast<uint64_t>(cooldownS) * MS_PER_SECOND;
}

void MemoryRebalanceScheduler::AddCooldownLocked(const std::string &worker, uint64_t nowMs)
{
    if (worker.empty()) {
        return;
    }
    cooldownUntilMs_[worker] = CalculateCooldownDeadlineMs(nowMs);
}

void MemoryRebalanceScheduler::AddPairCooldownLocked(const std::string &source, const std::string &target,
                                                     uint64_t nowMs)
{
    if (source.empty() || target.empty()) {
        return;
    }
    pairCooldownUntilMs_[source][target] = CalculateCooldownDeadlineMs(nowMs);
}

void MemoryRebalanceScheduler::ApplyFailureCooldownLocked(const master::RebalanceTaskPb &task,
                                                          master::RebalanceFailureSidePb failureSide,
                                                          uint64_t nowMs)
{
    switch (failureSide) {
        case master::REBALANCE_FAILURE_SOURCE:
        case master::REBALANCE_FAILURE_NO_CANDIDATE:
            AddCooldownLocked(task.source_worker(), nowMs);
            break;
        case master::REBALANCE_FAILURE_TARGET:
            AddCooldownLocked(task.target_worker(), nowMs);
            break;
        case master::REBALANCE_FAILURE_CONTROL_PLANE:
            break;
        case master::REBALANCE_FAILURE_UNKNOWN:
        default:
            AddPairCooldownLocked(task.source_worker(), task.target_worker(), nowMs);
            break;
    }
}

void MemoryRebalanceScheduler::RemoveTaskLocked(const std::string &sourceWorker, uint64_t nowMs, bool success,
                                                master::RebalanceFailureSidePb failureSide)
{
    auto taskIt = activeTasksBySource_.find(sourceWorker);
    if (taskIt != activeTasksBySource_.end()) {
        const auto &task = taskIt->second.task;
        if (success) {
            // issue #685: do NOT clear the in-flight charge on success. The target's snapshot
            // still shows stale-low usage until it reports again, so clearing now would let the
            // next source re-pick the just-received target. Hold the charge; it is released by
            // ReleaseReporterHoldsLocked / ReleaseSnapshotHoldsLocked when the target reports.
            auto &delta = futureView_[task.target_worker()];
            delta.heldBytes = SaturatingAdd(delta.heldBytes, task.max_bytes());
            if (nowMs > delta.holdSinceMs) {
                delta.holdSinceMs = nowMs;  // keep the latest completion time
            }
            LOG(INFO) << FormatString(
                "[MemoryRebalance] hold in-flight target=%s bytes=%lu pending=%lu until target reports",
                task.target_worker(), task.max_bytes(), delta.heldBytes);
        } else {
            // Failure/expire: data did not land on the target, so the in-flight charge is bogus.
            DecreaseInflightLocked(task.target_worker(), task.max_bytes());
            ApplyFailureCooldownLocked(task, failureSide, nowMs);
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
    auto inFlightIt = futureView_.find(targetWorker);
    if (inFlightIt != futureView_.end()) {
        return inFlightIt->second.inflightBytes;
    }
    return 0;
}

void MemoryRebalanceScheduler::DecreaseInflightLocked(const std::string &targetWorker, uint64_t bytes)
{
    auto it = futureView_.find(targetWorker);
    if (it == futureView_.end()) {
        return;
    }
    it->second.inflightBytes = it->second.inflightBytes > bytes ? it->second.inflightBytes - bytes : 0;
    if (it->second.inflightBytes == 0) {
        // No inflight charge remains on this target. Since heldBytes is always a subset of
        // inflightBytes (invariant), the held charge was part of the inflight just released,
        // so erasing the entry cannot drop a still-held charge.
        futureView_.erase(it);
    }
}

void MemoryRebalanceScheduler::ReleaseHeldLocked(const std::string &worker, uint64_t heldBytes)
{
    DecreaseInflightLocked(worker, heldBytes);
    auto cur = futureView_.find(worker);
    if (cur != futureView_.end()) {
        cur->second.heldBytes = 0;
        cur->second.holdSinceMs = 0;
    }
}

void MemoryRebalanceScheduler::ReleaseReporterHoldsLocked(const std::string &worker, uint64_t reportTimestamp)
{
    auto it = futureView_.find(worker);
    if (it == futureView_.end() || it->second.heldBytes == 0) {
        return;  // no held charge for this worker
    }
    if (reportTimestamp <= it->second.holdSinceMs) {
        return;  // worker has not reported since its latest held completion
    }
    // The worker just reported (reportTimestamp is fresh), so its snapshot now reflects the
    // post-receive memory. Drop the held charge so the worker can be re-evaluated on its merits.
    const uint64_t heldBytes = it->second.heldBytes;
    LOG(INFO) << FormatString(
        "[MemoryRebalance] release held in-flight target=%s bytes=%lu hold_age=%lums via reporter", worker, heldBytes,
        reportTimestamp - it->second.holdSinceMs);
    ReleaseHeldLocked(worker, heldBytes);
}

void MemoryRebalanceScheduler::ReleaseSnapshotHoldsLocked(const std::unordered_map<std::string, NodeInfo> &snapshot)
{
    // Backup release path for non-reporting targets: a target whose snapshot timestamp advanced
    // past its held completion time has had its memory re-published by the master snapshot swap.
    // The swap runs every FLAGS_master_snapshot_swap_interval_s (~10s), so a non-reporting
    // target's hold may persist up to ~10s after its snapshot is actually fresh -- within that
    // window CollectCandidatePairsLocked may over-estimate its projected usage and briefly skip
    // it. Acceptable: the reporter path (ReleaseReporterHoldsLocked) is the primary release.
    std::vector<std::pair<std::string, uint64_t>> toRelease;
    for (const auto &[worker, delta] : futureView_) {
        if (delta.heldBytes == 0) {
            continue;
        }
        auto nit = snapshot.find(worker);
        if (nit == snapshot.end()) {
            continue;  // worker no longer in the snapshot (down / scaled away)
        }
        if (nit->second.timestamp <= delta.holdSinceMs) {
            continue;  // snapshot timestamp still predates the held completion
        }
        LOG(INFO) << FormatString(
            "[MemoryRebalance] release held in-flight target=%s bytes=%lu hold_age=%lums via snapshot-swap", worker,
            delta.heldBytes, nit->second.timestamp - delta.holdSinceMs);
        toRelease.emplace_back(worker, delta.heldBytes);
    }
    for (const auto &[worker, heldBytes] : toRelease) {
        ReleaseHeldLocked(worker, heldBytes);
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
                                                           uint64_t nowMs,
                                                           std::vector<CandidatePair> &targetPairs) const
{
    targetPairs.reserve(sources.size() * targets.size());
    for (const auto *source : sources) {
        for (const auto *target : targets) {
            if (source->nodeId == target->nodeId
                || IsPairInCooldownLocked(source->nodeId, target->nodeId, nowMs)) {
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

void MemoryRebalanceScheduler::FillTaskProtoLocked(const std::string &sourceWorker,
                                                   const std::string &targetWorker, uint64_t maxBytes,
                                                   uint64_t nowMs, master::RebalanceTaskPb &task) const
{
    task.set_task_id("memory-rebalance-" + GetStringUuid());
    task.set_source_worker(sourceWorker);
    task.set_target_worker(targetWorker);
    task.set_max_bytes(maxBytes);
    task.set_create_time_ms(nowMs);
    auto rateBytesPerSec = static_cast<uint64_t>(FLAGS_data_migrate_rate_limit_mb) * 1024 * 1024;
    auto estimatedTransferMs = ((maxBytes + rateBytesPerSec - 1) / rateBytesPerSec) * MS_PER_SECOND;
    auto transferTimeoutMs = SaturatingMultiply(estimatedTransferMs, TRANSFER_TIME_MULTIPLIER);
    auto timeoutMs = SaturatingAdd(transferTimeoutMs, static_cast<uint64_t>(FLAGS_rebalance_task_report_grace_ms));
    task.set_timeout_ms(timeoutMs);
    task.set_deadline_ms(SaturatingAdd(nowMs, timeoutMs));
}

void MemoryRebalanceScheduler::FillTaskFromPairLocked(const CandidatePair &bestPair, uint64_t nowMs,
                                                      RunningTask &runningTask) const
{
    FillTaskProtoLocked(bestPair.source->nodeId, bestPair.target->nodeId, bestPair.maxBytes, nowMs,
                        runningTask.task);
    runningTask.targetMemoryLimit = bestPair.target->memoryLimit;
    runningTask.targetMemoryCapacity = bestPair.target->memoryCapacity;
    // Fixed total migration budget for this 30s cycle: the midpoint gap (usageGap/2) capped by
    // the target's watermark headroom and eviction headroom. Per-batch 300MB caps only each
    // batch (bestPair.maxBytes already applies it for the first batch); totalBudget is uncapped
    // so the loop can issue multiple batches until the planned convergence amount is migrated.
    const uint64_t usageGapBytes = (bestPair.source->usedMemory > bestPair.target->usedMemory)
        ? (bestPair.source->usedMemory - bestPair.target->usedMemory) / 2 : 0;
    const uint64_t watermarkBytes =
        bestPair.target->memoryLimit * FLAGS_rebalance_source_usage_percent / PERCENT_BASE;
    const uint64_t headroomToWatermark = SubOrZero(watermarkBytes, bestPair.target->usedMemory);
    runningTask.totalBudget = std::min({ usageGapBytes, headroomToWatermark, bestPair.targetAvailableAfterInFlight });
    runningTask.cumulativeMigrated = 0;
    runningTask.epochStartMs = nowMs;
}

Status MemoryRebalanceScheduler::TryBuildTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                                    const std::string &sourceWorker, uint64_t nowMs,
                                                    const cluster::TopologySnapshot *topologySnapshot,
                                                    RunningTask &runningTask)
{
    CHECK_FAIL_RETURN_STATUS(snapshot.size() >= MIN_REBALANCE_WORKER_COUNT, K_NOT_FOUND,
                             "No enough workers for memory rebalance");

    std::vector<const NodeInfo *> sources;
    std::vector<const NodeInfo *> targets;
    CollectWorkerCandidatesLocked(snapshot, sourceWorker, nowMs, topologySnapshot, sources, targets);
    CHECK_FAIL_RETURN_STATUS(!sources.empty() && !targets.empty(), K_NOT_FOUND,
                             "No source or target worker is suitable for memory rebalance");

    std::vector<CandidatePair> targetPairs;
    CollectCandidatePairsLocked(sources, targets, nowMs, targetPairs);
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

    FillTaskFromPairLocked(bestPair, nowMs, runningTask);
    LOG(INFO) << FormatString(
        "[MemoryRebalance] select source=%s(%lu%%) target=%s(%lu%% -> %u%%), max_bytes=%lu, "
        "target_available_after_in_flight=%lu, usage_gap=%u%%",
        bestPair.source->nodeId, CalculateUsageRate(*bestPair.source), bestPair.target->nodeId,
        CalculateUsageRate(*bestPair.target), bestPair.projectedTargetUsageRate, bestPair.maxBytes,
        bestPair.targetAvailableAfterInFlight, bestPair.usageGapRate);
    return Status::OK();
}

bool MemoryRebalanceScheduler::ShouldStopChainEarlyLocked(const RunningTask &prevTask,
                                                          uint64_t targetRemainBytes,
                                                          uint64_t nowMs) const
{
    if (prevTask.targetMemoryCapacity == 0) {
        return true;  // no captured target memory view; cannot make a fresh decision
    }
    // Guard against an impossible "remain > capacity" signal: SubOrZero would silently reverse
    // to freshTargetUsed=0 (target looks empty). Treat as signal corrupt: stop the chain.
    if (targetRemainBytes > prevTask.targetMemoryCapacity) {
        LOG(INFO) << FormatString("[MemoryRebalance] stop loop source=%s target=%s: fresh remain %lu "
                                  "> capacity %lu (signal corrupt)",
                                  prevTask.task.source_worker(), prevTask.task.target_worker(),
                                  targetRemainBytes, prevTask.targetMemoryCapacity);
        return true;
    }
    // Epoch wall-clock budget: the chain must end before the next 30s ResourceReport cycle.
    if (prevTask.epochStartMs != 0
        && nowMs - prevTask.epochStartMs >= REBALANCE_EPOCH_BUDGET_MS) {
        LOG(INFO) << FormatString("[MemoryRebalance] stop loop source=%s target=%s: epoch budget "
                                  "exhausted (elapsed=%lums, budget=%lums)",
                                  prevTask.task.source_worker(), prevTask.task.target_worker(),
                                  nowMs - prevTask.epochStartMs, REBALANCE_EPOCH_BUDGET_MS);
        return true;
    }
    return false;
}

bool MemoryRebalanceScheduler::BuildNextBatchTaskLocked(const RunningTask &prevTask, uint64_t targetRemainBytes,
                                                        uint64_t nowMs, master::RebalanceTaskPb &nextTask) const
{
    if (ShouldStopChainEarlyLocked(prevTask, targetRemainBytes, nowMs)) {
        return false;
    }
    // Budget exhausted: the fixed migration amount for this 30s cycle has been moved.
    const uint64_t remaining = SubOrZero(prevTask.totalBudget, prevTask.cumulativeMigrated);
    if (remaining == 0) {
        LOG(INFO) << FormatString("[MemoryRebalance] stop loop source=%s target=%s: budget exhausted "
                                  "(total=%lu, migrated=%lu)",
                                  prevTask.task.source_worker(), prevTask.task.target_worker(),
                                  prevTask.totalBudget, prevTask.cumulativeMigrated);
        return false;
    }
    // Fresh target safety: stop if the target reached the rebalance watermark.
    const uint64_t freshTargetUsed = SubOrZero(prevTask.targetMemoryCapacity, targetRemainBytes);
    const uint64_t freshTargetRate = CalculateUsageRate(freshTargetUsed, prevTask.targetMemoryLimit);
    if (freshTargetRate >= FLAGS_rebalance_source_usage_percent) {
        LOG(INFO) << FormatString("[MemoryRebalance] stop loop source=%s target=%s: target reached "
                                  "watermark (usage_rate=%lu%%)",
                                  prevTask.task.source_worker(), prevTask.task.target_worker(), freshTargetRate);
        return false;
    }
    const uint64_t watermarkBytes =
        prevTask.targetMemoryLimit * FLAGS_rebalance_source_usage_percent / PERCENT_BASE;
    const uint64_t targetInflight = GetTargetInflightBytesLocked(prevTask.task.target_worker());
    uint64_t heldForTarget = 0;
    auto deltaIt = futureView_.find(prevTask.task.target_worker());
    if (deltaIt != futureView_.end()) {
        heldForTarget = deltaIt->second.heldBytes;
    }
    // Exclude held from inflight to avoid double-counting the just-landed data.
    const uint64_t inflightExcludingHeld = SubOrZero(targetInflight, heldForTarget);
    const uint64_t projectedUsed = SaturatingAdd(freshTargetUsed, inflightExcludingHeld);
    const uint64_t headroomToWatermark = SubOrZero(watermarkBytes, projectedUsed);
    const uint64_t targetAvailableAfterInFlight = SubOrZero(targetRemainBytes, inflightExcludingHeld);
    const uint64_t nextBytes = std::min({ REBALANCE_MAX_BYTES_PER_TASK, remaining,
                                          headroomToWatermark, targetAvailableAfterInFlight });
    if (nextBytes == 0) {
        LOG(INFO) << FormatString("[MemoryRebalance] stop loop source=%s target=%s: next batch is 0 "
                                  "(remaining=%lu, headroom_to_watermark=%lu, target_avail_after_inflight=%lu)",
                                  prevTask.task.source_worker(), prevTask.task.target_worker(), remaining,
                                  headroomToWatermark, targetAvailableAfterInFlight);
        return false;
    }
    FillTaskProtoLocked(prevTask.task.source_worker(), prevTask.task.target_worker(), nextBytes, nowMs, nextTask);
    LOG(INFO) << FormatString("[MemoryRebalance] next batch source=%s target=%s max_bytes=%lu "
                              "(remaining_budget=%lu, target_usage_rate=%lu%%)",
                              nextTask.source_worker(), nextTask.target_worker(), nextBytes, remaining,
                              freshTargetRate);
    return true;
}

uint64_t MemoryRebalanceScheduler::CalculateTaskBytesLocked(const NodeInfo &source, const NodeInfo &target,
                                                            uint64_t targetInflightBytes) const
{
    if (source.usedMemory <= target.usedMemory) {
        return 0;
    }
    // Rebalance goal: converge source and target toward the usage midpoint (keep the existing
    // gap/2 logic). This is the migration amount, NOT a drain-to-watermark.
    const uint64_t usageGapBytes = (source.usedMemory - target.usedMemory) / 2;
    // Target ceiling: do not push the target past the rebalance watermark (source_usage_percent).
    // memoryCapacity == highWater line, so headroomToWatermark is how much the target can still
    // absorb before hitting the watermark usage rate.
    const uint64_t watermarkBytes = target.memoryLimit * FLAGS_rebalance_source_usage_percent / PERCENT_BASE;
    // Account for other sources' in-flight bytes so concurrent tasks don't each reuse the same
    // watermark headroom. projectedUsed = already-landed + not-yet-landed (in-flight).
    const uint64_t projectedUsed = SaturatingAdd(target.usedMemory, targetInflightBytes);
    const uint64_t headroomToWatermark = SubOrZero(watermarkBytes, projectedUsed);
    const uint64_t targetAvailableAfterInFlight = SubOrZero(target.availableMemory, targetInflightBytes);
    return std::min({ usageGapBytes, headroomToWatermark, targetAvailableAfterInFlight,
                      REBALANCE_MAX_BYTES_PER_TASK });
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
