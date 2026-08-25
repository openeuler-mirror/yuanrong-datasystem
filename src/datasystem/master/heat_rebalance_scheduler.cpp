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
 * Description: Master-side scheduler for the heat-driven rebalance strategy.
 *
 * RebalanceScheduler owns shared task/in-flight/hold/cooldown bookkeeping. This file keeps Heat-specific completion
 * policy and selection. Key differences from MemoryRebalanceScheduler:
 * - Source trigger has two OR paths: high usage with any primary data (memory-pressure fallback), or moderate usage
 *   plus high hot-primary bytes.
 * - Migration bytes are a fixed 10% of the source's memory capacity, capped by target available space.
 * - Cooldown is added only on failure/timeout, not on success.
 * See heat_rebalance_scheduler.h for the selection rules.
 */

#include "datasystem/master/heat_rebalance_scheduler.h"

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
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
constexpr uint64_t HOLD_TTL_MIN_S = 60;
constexpr uint32_t REBALANCE_COOLDOWN_S = 60;
constexpr size_t EXPECTED_SCHEDULER_EVENT_COUNT = 4;
constexpr uint64_t REBALANCE_MAX_MIGRATE_BYTES_PER_ROUND = 1024ul * 1024ul * 1024ul;

uint64_t SubOrZero(uint64_t lhs, uint64_t rhs)
{
    return lhs > rhs ? lhs - rhs : 0;
}
}  // namespace

void HeatRebalanceScheduler::SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership)
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    topologyMembership_ = topologyMembership;
}

std::shared_ptr<const cluster::TopologySnapshot> HeatRebalanceScheduler::GetTopologySnapshot(bool &topologyConfigured)
{
    const cluster::MembershipEndpointView *topologyMembership = nullptr;
    {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        topologyMembership = topologyMembership_;
    }
    topologyConfigured = topologyMembership != nullptr;
    if (topologyMembership == nullptr) {
        return nullptr;
    }
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    auto rc = topologyMembership->GetSnapshot(snapshot);
    if (rc.IsError()) {
        LOG_FIRST_N(WARNING, 1) << "[HeatRebalance] topology snapshot is not ready; skip new scheduling: "
                                << rc.ToString();
        return nullptr;
    }
    return snapshot;
}

#ifdef WITH_TESTS
void HeatRebalanceScheduler::SetActiveTaskDeadlineForTest(const std::string &sourceWorker, uint64_t deadlineMs)
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto task = activeTasksBySource_.find(sourceWorker);
    if (task != activeTasksBySource_.end()) {
        task->second.task.set_deadline_ms(deadlineMs);
    }
}
#endif

uint64_t HeatRebalanceScheduler::CalculateUsageRate(const NodeInfo &node)
{
    if (node.memoryCapacity == 0) {
        return PERCENT_BASE;
    }
    if (node.usedMemory > std::numeric_limits<uint64_t>::max() / PERCENT_BASE) {
        return PERCENT_BASE;
    }
    return std::min<uint64_t>(PERCENT_BASE, node.usedMemory * PERCENT_BASE / node.memoryCapacity);
}

Status HeatRebalanceScheduler::Schedule(const master::ResourceReportReqPb &req,
                                        const std::unordered_map<std::string, NodeInfo> &snapshot,
                                        master::ResourceReportRspPb &rsp)
{
    RETURN_OK_IF_TRUE(!FLAGS_enable_memory_rebalance);
    const std::string &reportingWorker = req.stat().address();
    RETURN_OK_IF_TRUE(reportingWorker.empty());

    bool topologyConfigured = false;
    auto topologySnapshot = GetTopologySnapshot(topologyConfigured);
    RETURN_OK_IF_TRUE(topologyConfigured && topologySnapshot == nullptr);
    uint64_t nowMs = GetSteadyClockTimeStampMs();
    // UUID generation can allocate and use internal synchronization. Generate it before taking the scheduler state
    // lock; wasting an id when no task is selected is cheaper than serializing concurrent resource reports.
    const std::string taskId = "heat-rebalance-" + GetStringUuid();
    std::vector<SchedulerEvent> events;
    events.reserve(snapshot.size() + 1);
    std::unique_lock<bthread::Mutex> lock(mutex_);
    ExpireTimeoutTasksLocked(nowMs, events);
    ReleaseSnapshotHoldsLocked(snapshot, events);

    auto activeTask = activeTasksBySource_.find(reportingWorker);
    if (activeTask != activeTasksBySource_.end()) {
        *rsp.mutable_rebalance_task() = activeTask->second.task;
        lock.unlock();
        EmitSchedulerEvents(events);
        return Status::OK();
    }

    master::RebalanceTaskPb task;
    Status rc = TryBuildTaskLocked(snapshot, reportingWorker, taskId, nowMs, topologySnapshot.get(), task);
    if (rc.GetCode() == StatusCode::K_NOT_FOUND) {
        lock.unlock();
        EmitSchedulerEvents(events);
        return Status::OK();
    }
    if (rc.IsError()) {
        lock.unlock();
        EmitSchedulerEvents(events);
        return rc;
    }
    if (task.source_worker() != reportingWorker) {
        lock.unlock();
        EmitSchedulerEvents(events);
        return Status::OK();
    }

    return ActivateTaskLocked(snapshot, reportingWorker, task, rsp, events, lock);
}

Status HeatRebalanceScheduler::ActivateTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                                  const std::string &reportingWorker,
                                                  const master::RebalanceTaskPb &task,
                                                  master::ResourceReportRspPb &rsp,
                                                  std::vector<SchedulerEvent> &events,
                                                  std::unique_lock<bthread::Mutex> &lock)
{
    auto targetNode = snapshot.find(task.target_worker());
    if (targetNode == snapshot.end()) {
        lock.unlock();
        EmitSchedulerEvents(events);
        RETURN_STATUS(K_NOT_FOUND, "Selected heat target left the snapshot");
    }
    RunningTask runningTask;
    runningTask.task = task;
    runningTask.targetUsedBeforeDispatch = targetNode->second.usedMemory;
    activeTasksBySource_.emplace(task.source_worker(), std::move(runningTask));
    IncreaseTargetInflightLocked(task.target_worker(), task.max_bytes());
    auto newTask = activeTasksBySource_.find(reportingWorker);
    if (newTask != activeTasksBySource_.end()) {
        *rsp.mutable_rebalance_task() = newTask->second.task;
    }
    lock.unlock();
    EmitSchedulerEvents(events);

    auto sourceNode = snapshot.find(task.source_worker());
    if (sourceNode != snapshot.end()) {
        LOG(INFO) << FormatString(
            "[HeatRebalance] select source=%s(usage=%lu%% hotBytesRatio=%u%%) "
            "target=%s(usage=%lu%% hotBytesRatio=%u%%), max_bytes=%lu",
            task.source_worker(), CalculateUsageRate(sourceNode->second),
            sourceNode->second.GetHotPrimaryBytesRatioPercent(), task.target_worker(),
            CalculateUsageRate(targetNode->second), targetNode->second.GetHotPrimaryBytesRatioPercent(),
            task.max_bytes());
    }
    LOG(INFO) << FormatString("[HeatRebalance] assign task %s source=%s target=%s max_bytes=%lu deadline_ms=%lu",
                              task.task_id(), task.source_worker(), task.target_worker(), task.max_bytes(),
                              task.deadline_ms());
    INJECT_POINT_NO_RETURN("HeatRebalanceScheduler.AssignTask");
    return Status::OK();
}

bool HeatRebalanceScheduler::NeedSnapshotForSchedule(const master::ResourceReportReqPb &req,
    const NodeInfo &reportingNode, master::ResourceReportRspPb &rsp)
{
    if (!FLAGS_enable_memory_rebalance) {
        return false;
    }
    const std::string &reportingWorker = req.stat().address();
    if (reportingWorker.empty() || reportingNode.nodeId != reportingWorker) {
        return false;
    }

    bool topologyConfigured = false;
    auto topologySnapshot = GetTopologySnapshot(topologyConfigured);
    if (topologyConfigured && topologySnapshot == nullptr) {
        return false;
    }
    uint64_t nowMs = GetSteadyClockTimeStampMs();
    std::vector<SchedulerEvent> events;
    events.reserve(EXPECTED_SCHEDULER_EVENT_COUNT);
    std::unique_lock<bthread::Mutex> lock(mutex_);
    ExpireTimeoutTasksLocked(nowMs, events);
    ReleaseReporterHoldsLocked(reportingNode, events);

    auto activeTask = activeTasksBySource_.find(reportingWorker);
    if (activeTask != activeTasksBySource_.end()) {
        *rsp.mutable_rebalance_task() = activeTask->second.task;
        lock.unlock();
        EmitSchedulerEvents(events);
        return false;
    }
    const bool isCandidate = IsSourceCandidateLocked(reportingNode, nowMs, topologySnapshot.get());
    lock.unlock();
    EmitSchedulerEvents(events);
    return isCandidate;
}

Status HeatRebalanceScheduler::ReportResult(const master::ReportRebalanceResultReqPb &req,
                                            master::ReportRebalanceResultRspPb &rsp)
{
    (void)rsp;
    CHECK_FAIL_RETURN_STATUS(!req.task_id().empty(), K_INVALID, "The rebalance task id can not be empty");
    CHECK_FAIL_RETURN_STATUS(!req.source_worker().empty(), K_INVALID, "The rebalance source worker can not be empty");
    CHECK_FAIL_RETURN_STATUS(IsTerminalStatus(req.status()), K_INVALID, "The rebalance task status is not terminal");

    uint64_t nowMs = GetSteadyClockTimeStampMs();
    std::vector<SchedulerEvent> events;
    events.reserve(1);
    std::unique_lock<bthread::Mutex> lock(mutex_);
    auto taskIt = activeTasksBySource_.find(req.source_worker());
    if (taskIt == activeTasksBySource_.end()) {
        lock.unlock();
        LOG(INFO) << FormatString("[HeatRebalance] ignore stale result task=%s source=%s target=%s status=%d",
                                  req.task_id(), req.source_worker(), req.target_worker(),
                                  static_cast<int>(req.status()));
        return Status::OK();
    }
    const auto &task = taskIt->second.task;
    CHECK_FAIL_RETURN_STATUS(task.source_worker() == req.source_worker(), K_RUNTIME_ERROR,
                             "Rebalance task source index is inconsistent");
    if (task.task_id() != req.task_id()) {
        const std::string activeTaskId = task.task_id();
        lock.unlock();
        LOG(INFO) << FormatString(
            "[HeatRebalance] ignore stale result task=%s source=%s target=%s status=%d, active_task=%s",
            req.task_id(), req.source_worker(), req.target_worker(), static_cast<int>(req.status()), activeTaskId);
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(task.target_worker() == req.target_worker(), K_INVALID,
                             "The rebalance result does not match the active task");

    // A terminal result racing the deadline wins for the matching active task. Expiring first would discard a
    // successful transfer and release its target capacity hold based only on scheduling delay/transport latency.
    // Cooldown only on failure/timeout, not on success. Successful target bytes remain charged until a newer target
    // report makes its post-migration memory visible.
    RemoveTaskLocked(req.source_worker(), nowMs, req.status() == master::REBALANCE_TASK_SUCCEEDED,
                     events, req.migrated_bytes());
    lock.unlock();
    EmitSchedulerEvents(events);
    LOG(INFO) << FormatString(
        "[HeatRebalance] finish task %s source=%s target=%s status=%d migrated_bytes=%lu migrated_objects=%lu "
        "failed_objects=%lu reason=%s",
        req.task_id(), req.source_worker(), req.target_worker(), static_cast<int>(req.status()), req.migrated_bytes(),
        req.migrated_objects(), req.failed_objects(), req.failed_reason());
    return Status::OK();
}

void HeatRebalanceScheduler::EmitSchedulerEvents(const std::vector<SchedulerEvent> &events)
{
    for (const auto &event : events) {
        switch (event.type) {
            case SchedulerEventType::EXPIRE:
                LOG(WARNING) << FormatString("[HeatRebalance] expire task %s", event.taskId);
                INJECT_POINT_NO_RETURN("HeatRebalanceScheduler.ExpireTask");
                break;
            case SchedulerEventType::HOLD:
                LOG(INFO) << FormatString(
                    "[HeatRebalance] hold in-flight target=%s bytes=%lu pending=%lu minimum_used=%lu until target "
                    "reports",
                    event.target, event.bytes, event.pendingBytes, event.minimumObservedUsedMemory);
                break;
            case SchedulerEventType::RELEASE_REPORTER:
                LOG(INFO) << FormatString("[HeatRebalance] release held in-flight target=%s bytes=%lu via reporter",
                                          event.target, event.bytes);
                break;
            case SchedulerEventType::RELEASE_SNAPSHOT:
                LOG(INFO) << FormatString("[HeatRebalance] release held in-flight target=%s bytes=%lu via snapshot",
                                          event.target, event.bytes);
                break;
            case SchedulerEventType::RELEASE_TTL:
                LOG(WARNING) << FormatString(
                    "[HeatRebalance] release held in-flight target=%s after TTL %lus, bytes=%lu", event.target,
                    event.ttlSeconds, event.bytes);
                break;
        }
    }
}

void HeatRebalanceScheduler::ExpireTimeoutTasksLocked(uint64_t nowMs, std::vector<SchedulerEvent> &events)
{
    for (const auto &source : CollectExpiredTaskSourcesLocked(nowMs)) {
        auto taskIt = activeTasksBySource_.find(source);
        if (taskIt == activeTasksBySource_.end()) {
            continue;
        }
        SchedulerEvent event(SchedulerEventType::EXPIRE);
        event.taskId = taskIt->second.task.task_id();
        events.emplace_back(std::move(event));
        RemoveTaskLocked(source, nowMs, false, events);
    }
    ExpireWorkerCooldownsLocked(nowMs);
    GcHeldInflightLocked(nowMs, events);
}

void HeatRebalanceScheduler::GcHeldInflightLocked(uint64_t nowMs, std::vector<SchedulerEvent> &events)
{
    const uint64_t holdTtlMs =
        std::max(static_cast<uint64_t>(FLAGS_node_dead_timeout_s), HOLD_TTL_MIN_S) * MS_PER_SECOND;
    for (const auto &[target, heldBytes] : CollectExpiredHoldsLocked(nowMs, holdTtlMs)) {
        SchedulerEvent event(SchedulerEventType::RELEASE_TTL);
        event.target = target;
        event.bytes = heldBytes;
        event.ttlSeconds = holdTtlMs / MS_PER_SECOND;
        events.emplace_back(std::move(event));
        ReleaseHeldLocked(target, heldBytes);
    }
}

void HeatRebalanceScheduler::AddCooldownLocked(const std::string &worker, uint64_t nowMs)
{
    SetCooldownUntilLocked(worker, nowMs + static_cast<uint64_t>(REBALANCE_COOLDOWN_S) * MS_PER_SECOND);
}

void HeatRebalanceScheduler::RemoveTaskLocked(const std::string &sourceWorker, uint64_t nowMs, bool success,
                                              std::vector<SchedulerEvent> &events, uint64_t migratedBytes)
{
    auto taskIt = activeTasksBySource_.find(sourceWorker);
    if (taskIt != activeTasksBySource_.end()) {
        const auto &task = taskIt->second.task;
        if (success) {
            const uint64_t heldBytes = std::min(task.max_bytes(), migratedBytes);
            DecreaseInflightLocked(task.target_worker(), task.max_bytes() - heldBytes);
            if (heldBytes > 0) {
                auto &hold =
                    HoldTargetInflightLocked(task.target_worker(), heldBytes, nowMs,
                                             SaturatingAdd(taskIt->second.targetUsedBeforeDispatch, heldBytes));
                SchedulerEvent event(SchedulerEventType::HOLD);
                event.target = task.target_worker();
                event.bytes = heldBytes;
                event.pendingBytes = hold.heldBytes;
                event.minimumObservedUsedMemory = hold.minimumObservedUsedMemory;
                events.emplace_back(std::move(event));
            }
        } else {
            DecreaseInflightLocked(task.target_worker(), task.max_bytes());
            AddCooldownLocked(task.source_worker(), nowMs);
            AddCooldownLocked(task.target_worker(), nowMs);
        }
        activeTasksBySource_.erase(taskIt);
    }
}

void HeatRebalanceScheduler::ReleaseReporterHoldsLocked(const NodeInfo &reportingNode,
                                                        std::vector<SchedulerEvent> &events)
{
    const auto *hold = GetReleasableReporterHoldLocked(reportingNode);
    if (hold == nullptr) {
        return;
    }
    const uint64_t heldBytes = hold->heldBytes;
    SchedulerEvent event(SchedulerEventType::RELEASE_REPORTER);
    event.target = reportingNode.nodeId;
    event.bytes = heldBytes;
    events.emplace_back(std::move(event));
    ReleaseHeldLocked(reportingNode.nodeId, heldBytes);
}

void HeatRebalanceScheduler::ReleaseSnapshotHoldsLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                                        std::vector<SchedulerEvent> &events)
{
    for (const auto &[target, bytes] : CollectReleasableHoldsLocked(snapshot)) {
        SchedulerEvent event(SchedulerEventType::RELEASE_SNAPSHOT);
        event.target = target;
        event.bytes = bytes;
        events.emplace_back(std::move(event));
        ReleaseHeldLocked(target, bytes);
    }
}

bool HeatRebalanceScheduler::IsSourceCandidateLocked(const NodeInfo &node, uint64_t nowMs,
                                                     const cluster::TopologySnapshot *topologySnapshot) const
{
    uint64_t targetInflightBytes = GetTargetInflightBytesLocked(node.nodeId);
    bool hasInboundTask = targetInflightBytes > 0;
    if (!node.isReady || !IsWorkerActiveInTopology(node.nodeId, topologySnapshot) || node.memoryCapacity == 0
        || hasInboundTask || activeTasksBySource_.find(node.nodeId) != activeTasksBySource_.end()
        || IsInCooldownLocked(node.nodeId, nowMs)) {
        return false;
    }
    const uint64_t usageRate = CalculateUsageRate(node);
    const uint32_t hotBytesRatio = node.GetHotPrimaryBytesRatioPercent();
    const auto &config = GetRebalanceHeatConfig();
    // Path A is the memory-pressure safety path. It must not depend on hot bytes: with keep-local enabled, moving hot
    // primaries demotes them to evictable local copies, but the remaining cold primaries would otherwise strand the
    // source at full memory. Path B retains the heat-only trigger at moderate usage.
    const bool pathA = usageRate > config.sourceUsagePercent && node.totalPrimaryCopyCount > 0;
    const bool pathB = usageRate > config.sourceUsagePercentLow && hotBytesRatio > config.sourceHotRatioPercent;
    return pathA || pathB;
}

bool HeatRebalanceScheduler::IsTargetCandidateLocked(const NodeInfo &node, uint64_t nowMs,
                                                     const cluster::TopologySnapshot *topologySnapshot) const
{
    if (!node.isReady || !IsWorkerActiveInTopology(node.nodeId, topologySnapshot) || node.memoryCapacity == 0
        || IsInCooldownLocked(node.nodeId, nowMs)) {
        return false;
    }
    // Both dimensions are admission guards. The former OR rule admitted workers at 85-90% usage merely because
    // their hot ratio was low; the memory-pressure fallback then circulated cold primaries between nearly-full
    // workers and triggered remote-Get storms. Stop assigning once either target watermark is reached.
    const auto &config = GetRebalanceHeatConfig();
    return CalculateUsageRate(node) < config.targetUsagePercent
           && node.GetHotPrimaryBytesRatioPercent() < config.targetHotRatioPercent;
}

uint64_t HeatRebalanceScheduler::CalculateTaskBytesLocked(const NodeInfo &source, const NodeInfo &target,
                                                          uint64_t targetInflightBytes) const
{
    // Fixed migration budget: 10% of the source's memory capacity per round.
    const uint64_t migrateBudget = source.memoryCapacity / 10;
    // Guard against target overflow: don't exceed the target's remaining available space (after in-flight).
    const uint64_t targetRoomAfterInflight = SubOrZero(target.availableMemory, targetInflightBytes);
    return std::min({ migrateBudget, targetRoomAfterInflight, REBALANCE_MAX_MIGRATE_BYTES_PER_ROUND });
}

void HeatRebalanceScheduler::FillTaskFromPairLocked(const CandidatePair &bestPair, const std::string &taskId,
                                                    uint64_t nowMs, master::RebalanceTaskPb &task) const
{
    uint64_t createTimeMs = nowMs;
    task.set_task_id(taskId);
    task.set_source_worker(bestPair.source->nodeId);
    task.set_target_worker(bestPair.target->nodeId);
    task.set_max_bytes(bestPair.maxBytes);
    task.set_create_time_ms(createTimeMs);
    auto rate_bytes_per_sec = static_cast<uint64_t>(FLAGS_data_migrate_rate_limit_mb) * 1024 * 1024;
    auto estimated_transfer_ms = ((bestPair.maxBytes + rate_bytes_per_sec - 1) / rate_bytes_per_sec) * MS_PER_SECOND;
    // master's RebalanceExecutor converts timeout_ms to a local steady-clock deadline; without it the task
    // expires immediately. timeout_ms is the relative timeout; deadline_ms is the absolute master-side cutoff.
    uint64_t timeoutMs = estimated_transfer_ms * TRANSFER_TIME_MULTIPLIER
                          + static_cast<uint64_t>(FLAGS_rebalance_task_report_grace_ms);
    task.set_timeout_ms(timeoutMs);
    task.set_deadline_ms(SaturatingAdd(createTimeMs, timeoutMs));
    task.set_source_eviction_policy(static_cast<master::EvictionPolicyPb>(bestPair.source->evictionPolicy));
    task.set_source_eviction_policy_epoch(bestPair.source->evictionPolicyEpoch);
    task.set_target_eviction_policy(static_cast<master::EvictionPolicyPb>(bestPair.target->evictionPolicy));
    task.set_target_eviction_policy_epoch(bestPair.target->evictionPolicyEpoch);
    task.set_has_eviction_policy_fence(true);
}

Status HeatRebalanceScheduler::TryBuildTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
    const std::string &sourceWorker, const std::string &taskId, uint64_t nowMs,
    const cluster::TopologySnapshot *topologySnapshot, master::RebalanceTaskPb &task)
{
    CHECK_FAIL_RETURN_STATUS(snapshot.size() >= MIN_REBALANCE_WORKER_COUNT, K_NOT_FOUND,
                             "No enough workers for heat rebalance");

    const NodeInfo *source = nullptr;
    for (const auto &[worker, node] : snapshot) {
        (void)worker;
        if (node.evictionPolicy == master::EVICTION_POLICY_HEAT && node.nodeId == sourceWorker
            && IsSourceCandidateLocked(node, nowMs, topologySnapshot)) {
            source = &node;
            break;
        }
    }
    CHECK_FAIL_RETURN_STATUS(source != nullptr, K_NOT_FOUND, "No source worker is suitable for heat rebalance");

    CandidatePair bestPair;
    bool foundPair = false;
    // Preserve the previous ordering without allocating and sorting every eligible pair while mutex_ is held:
    // usage low first, then space-after-inflight large first, then target nodeId ascending.
    auto isBetter = [](const CandidatePair &lhs, const CandidatePair &rhs) {
        if (lhs.usageRate != rhs.usageRate) {
            return lhs.usageRate < rhs.usageRate;
        }
        if (lhs.targetAvailAfterInflight != rhs.targetAvailAfterInflight) {
            return lhs.targetAvailAfterInflight > rhs.targetAvailAfterInflight;
        }
        return lhs.target->nodeId < rhs.target->nodeId;
    };
    for (const auto &[worker, target] : snapshot) {
        (void)worker;
        if (target.evictionPolicy != master::EVICTION_POLICY_HEAT || target.nodeId == source->nodeId
            || !IsTargetCandidateLocked(target, nowMs, topologySnapshot)) {
            continue;
        }
        const uint64_t targetInflightBytes = GetTargetInflightBytesLocked(target.nodeId);
        CandidatePair pair;
        pair.source = source;
        pair.target = &target;
        pair.maxBytes = CalculateTaskBytesLocked(*source, target, targetInflightBytes);
        if (pair.maxBytes == 0) {
            continue;
        }
        pair.targetAvailAfterInflight = SubOrZero(target.availableMemory, targetInflightBytes);
        pair.usageRate = static_cast<uint32_t>(CalculateUsageRate(target));
        if (!foundPair || isBetter(pair, bestPair)) {
            bestPair = pair;
            foundPair = true;
        }
    }
    CHECK_FAIL_RETURN_STATUS(foundPair, K_NOT_FOUND, "No source target pair is suitable for heat rebalance");

    FillTaskFromPairLocked(bestPair, taskId, nowMs, task);
    return Status::OK();
}

}  // namespace master
}  // namespace datasystem
