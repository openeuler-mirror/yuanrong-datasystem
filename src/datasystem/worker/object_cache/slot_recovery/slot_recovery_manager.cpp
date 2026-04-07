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
 * Description: Slot recovery coordination manager.
 */
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_manager.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <unordered_map>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_uint32(distributed_disk_slot_num);

namespace datasystem {
namespace object_cache {
namespace {
constexpr size_t SLOT_PREVIEW_LIMIT = 6;

const char *TaskStatusName(RecoveryTaskPb_TaskStatus status)
{
    switch (status) {
        case RecoveryTaskPb_TaskStatus_PENDING:
            return "PENDING";
        case RecoveryTaskPb_TaskStatus_IN_PROGRESS:
            return "IN_PROGRESS";
        case RecoveryTaskPb_TaskStatus_COMPLETED:
            return "COMPLETED";
        case RecoveryTaskPb_TaskStatus_FAILED:
            return "FAILED";
        default:
            return "UNKNOWN";
    }
}

std::string BuildSortedSlotsSummary(const std::vector<uint32_t> &sortedSlots)
{
    const auto count = sortedSlots.size();
    if (count == 0) {
        return "count=0";
    }
    const size_t previewSize = std::min(count, SLOT_PREVIEW_LIMIT);
    const auto minSlot = sortedSlots.front();
    const auto maxSlot = sortedSlots.back();
    std::vector<uint32_t> preview(
        sortedSlots.begin(), sortedSlots.begin() + static_cast<std::vector<uint32_t>::difference_type>(previewSize));
    return FormatString("count=%zu min=%u max=%u head=%s%s", count, minSlot, maxSlot, VectorToString(preview),
                        count > preview.size() ? "..." : "");
}

std::string SlotsSummary(const std::set<uint32_t> &slots)
{
    if (slots.empty()) {
        return "count=0";
    }
    const auto count = slots.size();
    std::vector<uint32_t> preview;
    preview.reserve(std::min(count, SLOT_PREVIEW_LIMIT));
    for (auto it = slots.begin(); it != slots.end() && preview.size() < SLOT_PREVIEW_LIMIT; ++it) {
        preview.emplace_back(*it);
    }
    return FormatString("count=%zu min=%u max=%u head=%s%s", count, *slots.begin(), *slots.rbegin(),
                        VectorToString(preview), count > preview.size() ? "..." : "");
}

std::string SlotsSummary(const google::protobuf::RepeatedField<uint32_t> &slots)
{
    std::vector<uint32_t> sortedSlots(slots.begin(), slots.end());
    std::sort(sortedSlots.begin(), sortedSlots.end());
    return BuildSortedSlotsSummary(sortedSlots);
}

std::string IncidentSummary(const SlotRecoveryInfoPb &info)
{
    return FormatString("tasks=%d total=%u completed=%u failed=%u", info.recovery_tasks_size(), info.total_slots(),
                        info.completed_slots(), info.failed_slots());
}

std::string TaskSummary(const RecoveryTaskPb &task)
{
    return FormatString("failed_worker=%s owner_worker=%s source_worker=%s task_status=%s slots_summary={%s}",
                        task.failed_worker(), task.owner_worker(), task.source_worker(),
                        TaskStatusName(task.task_status()), SlotsSummary(task.slots()));
}

bool IsSameTaskIdentity(const RecoveryTaskPb &lhs, const RecoveryTaskPb &rhs)
{
    return lhs.failed_worker() == rhs.failed_worker() && lhs.owner_worker() == rhs.owner_worker()
           && lhs.source_worker() == rhs.source_worker();
}

std::string GetTaskSourceWorker(const RecoveryTaskPb &task)
{
    if (!task.source_worker().empty()) {
        return task.source_worker();
    }
    if (task.task_status() == RecoveryTaskPb_TaskStatus_COMPLETED
        || task.task_status() == RecoveryTaskPb_TaskStatus_IN_PROGRESS) {
        return task.owner_worker();
    }
    return task.failed_worker();
}

void AddPlannedSlots(std::unordered_map<std::string, std::set<uint32_t>> &plannedSlotsBySource,
                     const std::string &sourceWorker, const google::protobuf::RepeatedField<uint32_t> &slots)
{
    auto &plannedSlots = plannedSlotsBySource[sourceWorker];
    plannedSlots.insert(slots.begin(), slots.end());
}

bool ShouldRetainTerminalIncidentForRestart(const std::string &incidentKey, const SlotRecoveryInfoPb &info)
{
    for (const auto &task : info.recovery_tasks()) {
        if (task.failed_worker() != incidentKey) {
            continue;
        }
        if (task.task_status() == RecoveryTaskPb_TaskStatus_COMPLETED && task.owner_worker() != incidentKey) {
            return true;
        }
    }
    return false;
}

ObjectMetaPb BuildRecoveredMetadata(const SlotPreloadMeta &meta, const std::string &localWorker)
{
    ObjectMetaPb objectMeta;
    objectMeta.set_object_key(meta.objectKey);
    objectMeta.set_data_size(meta.size);
    objectMeta.set_version(meta.version);
    objectMeta.set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_PUBLISHED));
    objectMeta.set_primary_address(localWorker);
    objectMeta.set_is_recovered(true);
    auto *config = objectMeta.mutable_config();
    config->set_write_mode(static_cast<uint32_t>(meta.writeMode));
    config->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    return objectMeta;
}
}  // namespace

bool SlotRecoveryIncidentState::IsTaskTerminal(const RecoveryTaskPb &task)
{
    return task.task_status() == RecoveryTaskPb_TaskStatus_COMPLETED
           || task.task_status() == RecoveryTaskPb_TaskStatus_FAILED;
}

bool SlotRecoveryIncidentState::IsFullyTerminal(const SlotRecoveryInfoPb &info)
{
    return info.total_slots() != 0 && info.completed_slots() + info.failed_slots() == info.total_slots();
}

Status SlotRecoveryIncidentState::MarkTasksFailedByOwner(const std::string &failedWorker, SlotRecoveryInfoPb &info)
{
    bool changed = false;
    for (auto &task : *info.mutable_recovery_tasks()) {
        if (task.owner_worker() != failedWorker || IsTaskTerminal(task)) {
            continue;
        }
        task.set_task_status(RecoveryTaskPb_TaskStatus_FAILED);
        changed = true;
    }
    if (changed) {
        RefreshCounters(info);
    }
    return Status::OK();
}

void SlotRecoveryIncidentState::RefreshCounters(SlotRecoveryInfoPb &info)
{
    uint32_t totalSlots = 0;
    uint32_t completedSlots = 0;
    uint32_t failedSlots = 0;
    for (const auto &task : info.recovery_tasks()) {
        const auto slotCount = static_cast<uint32_t>(task.slots_size());
        totalSlots += slotCount;
        if (task.task_status() == RecoveryTaskPb_TaskStatus_COMPLETED) {
            completedSlots += slotCount;
        } else if (task.task_status() == RecoveryTaskPb_TaskStatus_FAILED) {
            failedSlots += slotCount;
        }
    }
    info.set_total_slots(totalSlots);
    info.set_completed_slots(completedSlots);
    info.set_failed_slots(failedSlots);
}

Status SlotRecoveryPlanner::BuildInitialTasks(const std::string &failedWorker, uint32_t totalSlots,
                                              const std::vector<std::string> &activeWorkers, SlotRecoveryInfoPb &info)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!activeWorkers.empty(), K_INVALID, "No active workers for slot recovery.");
    std::unordered_map<std::string, int> tasksByOwner;
    tasksByOwner.reserve(activeWorkers.size());
    for (uint32_t slot = 0; slot < totalSlots; ++slot) {
        const auto &owner = activeWorkers[slot % activeWorkers.size()];
        auto found = tasksByOwner.find(owner);
        if (found == tasksByOwner.end()) {
            auto *task = info.add_recovery_tasks();
            task->set_failed_worker(failedWorker);
            task->set_owner_worker(owner);
            task->set_source_worker(failedWorker);
            task->set_task_status(RecoveryTaskPb_TaskStatus_PENDING);
            found = tasksByOwner.emplace(owner, info.recovery_tasks_size() - 1).first;
        }
        info.mutable_recovery_tasks(found->second)->add_slots(slot);
    }
    SlotRecoveryIncidentState::RefreshCounters(info);
    VLOG(1) << FormatString("action=build_initial_tasks failed_worker=%s summary={%s}", failedWorker,
                            IncidentSummary(info));
    return Status::OK();
}

Status SlotRecoveryPlanner::CollectInheritedTasks(const std::string &failedWorker, const SlotRecoveryInfoPb &sourceInfo,
                                                  std::vector<RecoveryTaskPb> &tasks)
{
    tasks.clear();
    for (const auto &task : sourceInfo.recovery_tasks()) {
        if (task.owner_worker() != failedWorker) {
            continue;
        }
        if (SlotRecoveryIncidentState::IsTaskTerminal(task)) {
            continue;
        }
        tasks.emplace_back(task);
        if (tasks.back().source_worker().empty()) {
            tasks.back().set_source_worker(task.failed_worker());
        }
    }
    if (!tasks.empty()) {
        VLOG(1) << FormatString("action=collect_inherited_raw_tasks owner_worker=%s task_count=%zu", failedWorker,
                                tasks.size());
    }
    return Status::OK();
}

Status SlotRecoveryPlanner::ReassignInheritedTasks(const std::vector<RecoveryTaskPb> &rawTasks,
                                                   const std::vector<std::string> &activeWorkers,
                                                   std::vector<RecoveryTaskPb> &tasks)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!activeWorkers.empty(), K_INVALID, "No active workers for inherited tasks.");
    tasks.clear();
    size_t workerIdx = 0;
    for (const auto &rawTask : rawTasks) {
        if (rawTask.slots_size() == 0) {
            continue;
        }
        size_t ownerIdx = workerIdx % activeWorkers.size();
        bool foundOwner = false;
        for (size_t shift = 0; shift < activeWorkers.size(); ++shift) {
            const size_t idx = (ownerIdx + shift) % activeWorkers.size();
            if (activeWorkers[idx] == rawTask.failed_worker()) {
                continue;
            }
            ownerIdx = idx;
            foundOwner = true;
            break;
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            foundOwner, K_RUNTIME_ERROR,
            FormatString("No eligible inherited-task owner for failed_worker=%s. activeWorkers=%s",
                         rawTask.failed_worker(), VectorToString(activeWorkers)));
        const auto &owner = activeWorkers[ownerIdx];
        RecoveryTaskPb *inheritedTask = nullptr;
        for (auto &current : tasks) {
            if (current.failed_worker() == rawTask.failed_worker() && current.owner_worker() == owner
                && current.task_status() == RecoveryTaskPb_TaskStatus_PENDING) {
                inheritedTask = &current;
                break;
            }
        }
        if (inheritedTask == nullptr) {
            tasks.emplace_back();
            inheritedTask = &tasks.back();
            inheritedTask->set_failed_worker(rawTask.failed_worker());
            inheritedTask->set_owner_worker(owner);
            inheritedTask->set_source_worker(rawTask.owner_worker());
            inheritedTask->set_task_status(RecoveryTaskPb_TaskStatus_PENDING);
        }
        for (const auto slot : rawTask.slots()) {
            inheritedTask->add_slots(slot);
        }
        workerIdx = ownerIdx + 1;
    }
    return Status::OK();
}

Status SlotRecoveryPlanner::AppendRecoveryTasks(std::vector<RecoveryTaskPb> &&tasks, SlotRecoveryInfoPb &info)
{
    auto *recoveryTasks = info.mutable_recovery_tasks();
    recoveryTasks->Reserve(recoveryTasks->size() + static_cast<int>(tasks.size()));
    for (auto &task : tasks) {
        auto *newTask = recoveryTasks->Add();
        *newTask = std::move(task);
    }
    SlotRecoveryIncidentState::RefreshCounters(info);
    return Status::OK();
}

SlotRecoveryManager::~SlotRecoveryManager()
{
    Shutdown();
}

Status SlotRecoveryManager::Init(
    const HostPort &localAddress, EtcdClusterManager *etcdCM, std::shared_ptr<PersistenceApi> persistApi,
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager,
    datasystem::EtcdStore *etcdStore, MetaDataRecoveryManager *metadataRecoveryManager)
{
    localAddress_ = localAddress;
    etcdCM_ = etcdCM;
    persistenceApi_ = std::move(persistApi);
    workerMasterApiManager_ = std::move(apiManager);
    metadataRecoveryManager_ = metadataRecoveryManager;
    store_ = CreateStore(etcdStore);
    if (!IsFeatureEnabled()) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(store_ != nullptr, K_INVALID, "slot recovery store is null");
    RETURN_IF_NOT_OK(store_->Init());
    constexpr uint32_t maxTaskThreadNum = 4;
    recoveryTaskThreadPool_ = std::make_shared<ThreadPool>(0, maxTaskThreadNum, "SlotRecoveryTask");
    SlotRecoveryFailedWorkersEvent::GetInstance().AddSubscriber(
        "SLOT_RECOVERY_MANAGER",
        [this](const std::vector<HostPort> &failedWorkers) { return HandleFailedWorkers(failedWorkers); });
    return Status::OK();
}

void SlotRecoveryManager::Shutdown()
{
    SlotRecoveryFailedWorkersEvent::GetInstance().RemoveSubscriber("SLOT_RECOVERY_MANAGER");
    recoveryTaskThreadPool_ = nullptr;
}

std::vector<std::string> SlotRecoveryManager::GetStableActiveWorkers() const
{
    std::vector<std::string> activeWorkers;
    if (etcdCM_ == nullptr) {
        return activeWorkers;
    }
    auto excludedWorkers = etcdCM_->GetFailedWorkers();
    const auto workers = etcdCM_->GetValidWorkersInHashRing();
    for (const auto &worker : workers) {
        if (excludedWorkers.find(worker) != excludedWorkers.end()) {
            continue;
        }
        activeWorkers.emplace_back(worker);
    }
    std::sort(activeWorkers.begin(), activeWorkers.end());
    return activeWorkers;
}

std::vector<std::string> SlotRecoveryManager::PickProcessWorkers(const std::vector<std::string> &failedWorkers,
                                                                 const std::vector<std::string> &activeWorkers,
                                                                 size_t maxWorkers)
{
    if (failedWorkers.empty() || activeWorkers.empty() || maxWorkers == 0) {
        return {};
    }
    std::vector<std::string> sortedFailedWorkers = failedWorkers;
    std::sort(sortedFailedWorkers.begin(), sortedFailedWorkers.end());
    std::vector<std::string> processWorkers;
    processWorkers.reserve(std::min(maxWorkers, activeWorkers.size()));
    const auto &anchorWorker = sortedFailedWorkers.front();
    auto it = std::lower_bound(activeWorkers.begin(), activeWorkers.end(), anchorWorker);
    const size_t startIndex = it == activeWorkers.end() ? 0 : static_cast<size_t>(it - activeWorkers.begin());
    for (size_t i = 0; i < std::min(maxWorkers, activeWorkers.size()); ++i) {
        processWorkers.emplace_back(activeWorkers[(startIndex + i) % activeWorkers.size()]);
    }
    return processWorkers;
}

Status SlotRecoveryManager::HandleFailedWorkers(const std::vector<HostPort> &failedWorkers)
{
    RETURN_OK_IF_TRUE(!IsFeatureEnabled());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    std::vector<std::string> shuffledFailedWorkers;
    shuffledFailedWorkers.reserve(failedWorkers.size());
    for (const auto &failedWorker : failedWorkers) {
        shuffledFailedWorkers.emplace_back(failedWorker.ToString());
    }
    static thread_local std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
    std::shuffle(shuffledFailedWorkers.begin(), shuffledFailedWorkers.end(), gen);
    const auto activeWorkers = GetStableActiveWorkers();
    const auto processWorkers = PickProcessWorkers(shuffledFailedWorkers, activeWorkers, 5);
    const bool isProcessWorker =
        std::find(processWorkers.begin(), processWorkers.end(), localAddress_.ToString()) != processWorkers.end();
    LOG(INFO) << FormatString(
        "action=handle_failed_workers local_worker=%s failed_workers=%zu active_workers=%zu process_workers=%zu "
        "selected_as_process_worker=%d",
        localAddress_.ToString(), shuffledFailedWorkers.size(), activeWorkers.size(), processWorkers.size(),
        isProcessWorker);
    for (const auto &failedWorker : shuffledFailedWorkers) {
        SlotRecoveryInfoPb info;
        // Always read the incident first. If a planner has already published the recovery plan, every worker can
        // directly reuse that snapshot and proceed to local execution without another cluster-wide scan.
        auto rc = store_->GetIncident(failedWorker, info);
        if (rc.GetCode() == K_NOT_FOUND) {
            std::vector<std::pair<std::string, SlotRecoveryInfoPb>> incidents;
            RETURN_IF_NOT_OK(store_->ListIncidents(incidents));
            RETURN_IF_NOT_OK(PlanIncident(failedWorker, activeWorkers, incidents));
            auto getPlannedRc = store_->GetIncident(failedWorker, info);
            if (getPlannedRc.GetCode() == K_NOT_FOUND) {
                VLOG(1) << FormatString("action=skip_schedule_after_plan failed_worker=%s reason=incident_absent",
                                        failedWorker);
                continue;
            }
            RETURN_IF_NOT_OK(getPlannedRc);
            INJECT_POINT_NO_RETURN("SlotRecoveryManager.HandleFailedWorkers.AfterPlanIncident");
            if (isProcessWorker) {
                RETURN_IF_NOT_OK(MarkTasksFailedInOtherIncidents(failedWorker, incidents));
            }
        } else {
            RETURN_IF_NOT_OK(rc);
            if (isProcessWorker) {
                std::vector<std::pair<std::string, SlotRecoveryInfoPb>> incidents;
                RETURN_IF_NOT_OK(store_->ListIncidents(incidents));
                // processWorkers only handle cross-incident cleanup. Planning itself is open to every worker and is
                // serialized by the incident CAS path above.
                RETURN_IF_NOT_OK(MarkTasksFailedInOtherIncidents(failedWorker, incidents));
            }
        }
        RETURN_IF_NOT_OK(ScheduleLocalTasks(failedWorker, info));
    }
    return Status::OK();
}

Status SlotRecoveryManager::PlanIncident(const std::string &failedWorker, const std::vector<std::string> &activeWorkers,
                                         std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents)
{
    LOG(INFO) << FormatString("action=plan_incident_begin failed_worker=%s active_workers=%zu", failedWorker,
                              activeWorkers.size());
    if (activeWorkers.empty()) {
        LOG(WARNING) << FormatString("action=plan_incident_skip failed_worker=%s reason=no_active_workers",
                                     failedWorker);
        return Status::OK();
    }
    std::vector<RecoveryTaskPb> inheritedTasks;
    RETURN_IF_NOT_OK(CollectInheritedTasks(failedWorker, activeWorkers, incidents, inheritedTasks));
    SlotRecoveryInfoPb plannedInfo;
    RETURN_IF_NOT_OK(SlotRecoveryPlanner::BuildInitialTasks(failedWorker, FLAGS_distributed_disk_slot_num,
                                                            activeWorkers, plannedInfo));
    const auto initialTaskCount = plannedInfo.recovery_tasks_size();
    RETURN_IF_NOT_OK(SlotRecoveryPlanner::AppendRecoveryTasks(std::move(inheritedTasks), plannedInfo));
    RETURN_IF_NOT_OK(store_->CASIncident(failedWorker, [failedWorker, &plannedInfo](SlotRecoveryInfoPb &info,
                                                                                    bool &exists, bool &writeBack) {
        if (exists) {
            // Another worker has already published the incident plan. Reuse that plan instead of appending
            // tasks again, so only one planner wins and the rest of the workers fall back to execution.
            writeBack = false;
            return Status::OK();
        }
        if (plannedInfo.recovery_tasks_size() == 0) {
            writeBack = false;
            VLOG(1) << FormatString("action=plan_incident_skip_write failed_worker=%s reason=empty_plan", failedWorker);
            return Status::OK();
        }
        info = plannedInfo;
        writeBack = true;
        LOG(INFO) << FormatString("action=plan_incident_publish failed_worker=%s cas_result=updated summary={%s}",
                                  failedWorker, IncidentSummary(plannedInfo));
        return Status::OK();
    }));
    LOG(INFO) << FormatString(
        "action=plan_incident_finish failed_worker=%s initial_tasks=%d inherited_tasks=%d summary={%s}", failedWorker,
        initialTaskCount, plannedInfo.recovery_tasks_size() - initialTaskCount, IncidentSummary(plannedInfo));
    return Status::OK();
}

Status SlotRecoveryManager::CollectInheritedTasks(
    const std::string &failedWorker, const std::vector<std::string> &activeWorkers,
    const std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents,
    std::vector<RecoveryTaskPb> &inheritedTasks)
{
    std::vector<RecoveryTaskPb> rawInheritedTasks;
    inheritedTasks.clear();
    for (const auto &incident : incidents) {
        if (incident.first == failedWorker) {
            continue;
        }
        std::vector<RecoveryTaskPb> currentRawTasks;
        // The new failed worker must inherit unfinished tasks before the old incidents invalidate them. This preserves
        // the "create new incident first, then fail old task" ordering required by the design.
        RETURN_IF_NOT_OK(SlotRecoveryPlanner::CollectInheritedTasks(failedWorker, incident.second, currentRawTasks));
        rawInheritedTasks.insert(rawInheritedTasks.end(), std::make_move_iterator(currentRawTasks.begin()),
                                 std::make_move_iterator(currentRawTasks.end()));
    }
    RETURN_IF_NOT_OK(SlotRecoveryPlanner::ReassignInheritedTasks(rawInheritedTasks, activeWorkers, inheritedTasks));
    return Status::OK();
}

Status SlotRecoveryManager::MarkTasksFailedInOtherIncidents(
    const std::string &failedWorker, const std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents)
{
    for (const auto &incident : incidents) {
        if (incident.first == failedWorker) {
            continue;
        }
        bool shouldDelete = false;
        // This is the second half of the reassignment flow. By the time we reach this step, the successor incident has
        // already received the inherited tasks, so the old owner can now be safely marked FAILED.
        RETURN_IF_NOT_OK(store_->CASIncident(incident.first, [&failedWorker, &incident, &shouldDelete](
                                                                 SlotRecoveryInfoPb &info, bool &exists,
                                                                 bool &writeBack) {
            if (!exists) {
                writeBack = false;
                shouldDelete = false;
                return Status::OK();
            }
            uint32_t oldFailedSlots = info.failed_slots();
            RETURN_IF_NOT_OK(SlotRecoveryIncidentState::MarkTasksFailedByOwner(failedWorker, info));
            writeBack = oldFailedSlots != info.failed_slots();
            shouldDelete = writeBack && SlotRecoveryIncidentState::IsFullyTerminal(info);
            if (writeBack) {
                LOG(INFO) << FormatString(
                    "action=mark_tasks_failed incident_key=%s owner_worker=%s task_status=FAILED cas_result=updated "
                    "summary={%s}",
                    incident.first, failedWorker, IncidentSummary(info));
            }
            return Status::OK();
        }));
        if (shouldDelete) {
            auto deleteRc = store_->DeleteIncident(incident.first);
            if (deleteRc.IsError() && deleteRc.GetCode() != K_NOT_FOUND) {
                return deleteRc;
            }
            LOG(INFO) << FormatString("action=delete_incident_after_failover incident_key=%s reason=terminal",
                                      incident.first);
        }
    }
    return Status::OK();
}

Status SlotRecoveryManager::ScheduleLocalTasks(const std::string &incidentKey, const SlotRecoveryInfoPb &info)
{
    std::string localWorker = localAddress_.ToString();
    // A local snapshot may be stale. Execute only after a CAS claim verifies that the task owner is still local and
    // the task can transition from PENDING to IN_PROGRESS.
    for (const auto &task : info.recovery_tasks()) {
        if (task.owner_worker() != localWorker || SlotRecoveryIncidentState::IsTaskTerminal(task)) {
            continue;
        }
        const auto asyncTraceId = GetStringUuid().substr(0, SHORT_TRACEID_SIZE);
        LOG(INFO) << FormatString(
            "action=schedule_local_task incident_key=%s failed_worker=%s owner_worker=%s trace=%s task_status=%s",
            incidentKey, task.failed_worker(), task.owner_worker(), asyncTraceId, TaskStatusName(task.task_status()));
        recoveryTaskThreadPool_->Execute([this, incidentKey, task, asyncTraceId]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(asyncTraceId);
            bool claimed = false;
            auto claimRc = ClaimLocalTask(incidentKey, task, claimed);
            if (claimRc.IsError()) {
                LOG(ERROR) << FormatString(
                    "action=claim_local_task incident_key=%s failed_worker=%s owner_worker=%s "
                    "task_status=%s reason=cas_error err=%s",
                    incidentKey, task.failed_worker(), task.owner_worker(), TaskStatusName(task.task_status()),
                    claimRc.ToString());
                return;
            }
            if (!claimed) {
                LOG(INFO) << FormatString(
                    "action=claim_local_task incident_key=%s failed_worker=%s owner_worker=%s "
                    "cas_result=unchanged reason=task_not_claimable task_status=%s slots_summary={%s}",
                    incidentKey, task.failed_worker(), task.owner_worker(), TaskStatusName(task.task_status()),
                    SlotsSummary(task.slots()));
                return;
            }
            auto executeRc = ExecuteRecoveryTask(task);
            if (executeRc.IsError()) {
                LOG(WARNING) << FormatString(
                    "action=execute_local_task incident_key=%s failed_worker=%s owner_worker=%s "
                    "task_status=%s reason=execute_error_continue_completion err=%s",
                    incidentKey, task.failed_worker(), task.owner_worker(), TaskStatusName(task.task_status()),
                    executeRc.ToString());
            }
            LOG_IF_ERROR(CompleteLocalTask(incidentKey, task),
                         FormatString("Async completion failed for recovery task of %s.", task.failed_worker()));
        });
    }
    return Status::OK();
}

Status SlotRecoveryManager::ClaimLocalTask(const std::string &incidentKey, const RecoveryTaskPb &task, bool &claimed)
{
    claimed = false;
    const std::string localWorker = localAddress_.ToString();
    return store_->CASIncident(incidentKey, [&incidentKey, &task, &localWorker, &claimed](
                                                SlotRecoveryInfoPb &info, bool &exists, bool &writeBack) {
        if (!exists) {
            writeBack = false;
            return Status::OK();
        }
        for (auto &current : *info.mutable_recovery_tasks()) {
            if (!IsSameTaskIdentity(current, task)) {
                continue;
            }
            if (current.owner_worker() != localWorker || SlotRecoveryIncidentState::IsTaskTerminal(current)) {
                continue;
            }
            if (current.task_status() != RecoveryTaskPb_TaskStatus_PENDING) {
                continue;
            }
            current.set_task_status(RecoveryTaskPb_TaskStatus_IN_PROGRESS);
            claimed = true;
            LOG(INFO) << FormatString("action=claim_local_task incident_key=%s task={%s} cas_result=updated",
                                      incidentKey, TaskSummary(current));
            writeBack = true;
            return Status::OK();
        }
        writeBack = false;
        return Status::OK();
    });
}

Status SlotRecoveryManager::CompleteLocalTask(const std::string &incidentKey, const RecoveryTaskPb &task)
{
    bool shouldDelete = false;
    bool taskCompleted = false;
    const std::string localWorker = localAddress_.ToString();
    // The completion write still uses CAS because multiple workers may read the same incident concurrently, and only
    // the owner that still sees a non-terminal task should publish the terminal transition.
    auto rc = store_->CASIncident(incidentKey, [&task, &shouldDelete, &taskCompleted, &localWorker, &incidentKey](
                                                   SlotRecoveryInfoPb &info, bool &exists, bool &writeBack) {
        if (!exists) {
            writeBack = false;
            return Status::OK();
        }
        writeBack = false;
        for (auto &current : *info.mutable_recovery_tasks()) {
            if (!IsSameTaskIdentity(current, task)) {
                continue;
            }
            if (current.owner_worker() != localWorker || SlotRecoveryIncidentState::IsTaskTerminal(current)) {
                continue;
            }
            if (current.task_status() != RecoveryTaskPb_TaskStatus_PENDING
                && current.task_status() != RecoveryTaskPb_TaskStatus_IN_PROGRESS) {
                continue;
            }
            current.set_task_status(RecoveryTaskPb_TaskStatus_COMPLETED);
            current.set_source_worker(current.owner_worker());
            SlotRecoveryIncidentState::RefreshCounters(info);
            shouldDelete = SlotRecoveryIncidentState::IsFullyTerminal(info)
                           && !ShouldRetainTerminalIncidentForRestart(incidentKey, info);
            taskCompleted = true;
            writeBack = true;
            return Status::OK();
        }
        return Status::OK();
    });
    if (rc.IsOk()) {
        LOG(INFO) << FormatString(
            "action=complete_local_task incident_key=%s failed_worker=%s owner_worker=%s "
            "cas_result=%s delete_incident=%d slots_summary={%s}",
            incidentKey, task.failed_worker(), localWorker, taskCompleted ? "updated" : "unchanged", shouldDelete,
            SlotsSummary(task.slots()));
        if (shouldDelete) {
            auto deleteRc = store_->DeleteIncident(incidentKey);
            if (deleteRc.IsError() && deleteRc.GetCode() != K_NOT_FOUND) {
                return deleteRc;
            }
            LOG(INFO) << FormatString("action=delete_incident_after_completion incident_key=%s reason=terminal",
                                      incidentKey);
        }
    }
    return rc;
}

Status SlotRecoveryManager::HandleLocalRestart()
{
    RETURN_OK_IF_TRUE(!IsFeatureEnabled());
    const std::string localWorker = localAddress_.ToString();
    LocalRestartPlan restartPlan;
    RETURN_IF_NOT_OK(CollectLocalRestartPlan(localWorker, restartPlan));
    LOG(INFO) << FormatString(
        "action=restart_begin local_worker=%s local_incident_exists=%d planned_sources=%zu "
        "source_incidents=%zu",
        localWorker, restartPlan.localIncidentExists, restartPlan.plannedSlotsBySource.size(),
        restartPlan.sourceIncidentKeys.size());

    // Stage 1: mark source PENDING tasks as FAILED so ownership transfer is serialized by source-incident CAS.
    std::unordered_map<std::string, std::set<uint32_t>> takenSlotsBySource;
    std::set<uint32_t> blockedSlotSet;
    for (const auto &incidentKey : restartPlan.sourceIncidentKeys) {
        std::unordered_map<std::string, std::set<uint32_t>> takenFromIncident;
        std::vector<uint32_t> blockedSlots;
        bool shouldDeleteSource = false;
        RETURN_IF_NOT_OK(TakeOverPendingFromSourceIncident(incidentKey, localWorker, takenFromIncident, blockedSlots,
                                                           shouldDeleteSource));
        for (const auto &it : takenFromIncident) {
            auto &slots = takenSlotsBySource[it.first];
            slots.insert(it.second.begin(), it.second.end());
        }
        blockedSlotSet.insert(blockedSlots.begin(), blockedSlots.end());
        LOG(INFO) << FormatString(
            "action=restart_takeover_source incident_key=%s local_worker=%s taken_sources=%zu blocked_slots=%zu "
            "delete_source_incident=%d",
            incidentKey, localWorker, takenFromIncident.size(), blockedSlots.size(), shouldDeleteSource);
        if (shouldDeleteSource) {
            auto deleteRc = store_->DeleteIncident(incidentKey);
            if (deleteRc.IsError() && deleteRc.GetCode() != K_NOT_FOUND) {
                return deleteRc;
            }
            LOG(INFO) << FormatString(
                "action=restart_delete_source_incident incident_key=%s local_worker=%s reason=terminal", incidentKey,
                localWorker);
        }
    }

    // Stage 2: compute the single canonical local task that restart should own after takeover.
    std::vector<RecoveryTaskPb> plannedLocalTasks;
    RETURN_IF_NOT_OK(
        BuildPlannedLocalRestartTasks(localWorker, restartPlan, takenSlotsBySource, blockedSlotSet, plannedLocalTasks));
    VLOG(1) << FormatString(
        "action=restart_plan_local_slots local_worker=%s taken_source_count=%zu blocked_slots={%s} "
        "planned_task_count=%zu",
        localWorker, takenSlotsBySource.size(), SlotsSummary(blockedSlotSet), plannedLocalTasks.size());

    // Stage 3: rewrite the local incident into one canonical local task while preserving foreign in-flight work.
    RETURN_IF_NOT_OK(RebuildLocalRestartIncident(localWorker, plannedLocalTasks));
    LOG(INFO) << FormatString("action=restart_rebuild_local_incident local_worker=%s planned_task_count=%zu",
                              localWorker, plannedLocalTasks.size());

    // Stage 4: schedule the canonical local task through the normal claim/execute/complete flow.
    return ScheduleLocalRestartTasks(localWorker);
}

Status SlotRecoveryManager::CollectLocalRestartPlan(const std::string &localWorker, LocalRestartPlan &restartPlan)
{
    restartPlan = LocalRestartPlan{};
    std::vector<std::pair<std::string, SlotRecoveryInfoPb>> incidents;
    RETURN_IF_NOT_OK(store_->ListIncidents(incidents));
    for (const auto &incident : incidents) {
        const bool isLocalIncident = incident.first == localWorker;
        bool hasRelevantSourceTask = false;
        for (const auto &task : incident.second.recovery_tasks()) {
            if (task.failed_worker() != localWorker) {
                continue;
            }
            const auto sourceWorker = GetTaskSourceWorker(task);
            if (isLocalIncident) {
                restartPlan.localIncidentExists = true;
                if (task.owner_worker() == localWorker
                    && (task.task_status() == RecoveryTaskPb_TaskStatus_PENDING
                        || task.task_status() == RecoveryTaskPb_TaskStatus_IN_PROGRESS)) {
                    AddPlannedSlots(restartPlan.plannedSlotsBySource, sourceWorker, task.slots());
                } else if (task.task_status() == RecoveryTaskPb_TaskStatus_COMPLETED) {
                    AddPlannedSlots(restartPlan.plannedSlotsBySource, sourceWorker, task.slots());
                } else if (task.task_status() == RecoveryTaskPb_TaskStatus_PENDING) {
                    AddPlannedSlots(restartPlan.plannedSlotsBySource, sourceWorker, task.slots());
                }
                continue;
            }

            // Source incidents still matter to restart as long as they retain a non-FAILED recovery fact for the
            // local worker. PENDING may be taken over, IN_PROGRESS must stay blocked, and COMPLETED remains a valid
            // preload source for rebuilding the restarted worker.
            if (task.task_status() != RecoveryTaskPb_TaskStatus_FAILED) {
                hasRelevantSourceTask = true;
                if (task.task_status() == RecoveryTaskPb_TaskStatus_COMPLETED) {
                    AddPlannedSlots(restartPlan.plannedSlotsBySource, sourceWorker, task.slots());
                }
            }
        }
        if (hasRelevantSourceTask) {
            restartPlan.sourceIncidentKeys.emplace_back(incident.first);
        }
    }
    return Status::OK();
}

Status SlotRecoveryManager::TakeOverPendingFromSourceIncident(
    const std::string &sourceIncidentKey, const std::string &localWorker,
    std::unordered_map<std::string, std::set<uint32_t>> &takenSlotsBySource, std::vector<uint32_t> &blockedSlots,
    bool &shouldDeleteSource)
{
    shouldDeleteSource = false;
    takenSlotsBySource.clear();
    std::set<uint32_t> blockedSlotSet;
    RETURN_IF_NOT_OK(store_->CASIncident(
        sourceIncidentKey, [&localWorker, &takenSlotsBySource, &blockedSlotSet, &shouldDeleteSource,
                            &sourceIncidentKey](SlotRecoveryInfoPb &info, bool &exists, bool &writeBack) {
            if (!exists) {
                writeBack = false;
                return Status::OK();
            }

            for (auto &task : *info.mutable_recovery_tasks()) {
                if (task.failed_worker() != localWorker || task.task_status() == RecoveryTaskPb_TaskStatus_FAILED) {
                    continue;
                }

                if (task.task_status() == RecoveryTaskPb_TaskStatus_PENDING) {
                    task.set_task_status(RecoveryTaskPb_TaskStatus_FAILED);
                    auto &takenSlots = takenSlotsBySource[GetTaskSourceWorker(task)];
                    takenSlots.insert(task.slots().begin(), task.slots().end());
                    writeBack = true;
                    continue;
                }

                // Restart must not race with an owner that is still actively transferring slots. COMPLETED tasks stay
                // available as preload source for the local worker.
                if (task.task_status() == RecoveryTaskPb_TaskStatus_IN_PROGRESS) {
                    blockedSlotSet.insert(task.slots().begin(), task.slots().end());
                }
            }

            if (writeBack) {
                SlotRecoveryIncidentState::RefreshCounters(info);
            }
            shouldDeleteSource = SlotRecoveryIncidentState::IsFullyTerminal(info)
                                 && !ShouldRetainTerminalIncidentForRestart(sourceIncidentKey, info);
            VLOG(1) << FormatString(
                "action=restart_source_takeover_cas incident_key=%s local_worker=%s cas_result=%s summary={%s}",
                sourceIncidentKey, localWorker, writeBack ? "updated" : "unchanged", IncidentSummary(info));
            return Status::OK();
        }));
    blockedSlots.assign(blockedSlotSet.begin(), blockedSlotSet.end());
    return Status::OK();
}

Status SlotRecoveryManager::BuildPlannedLocalRestartTasks(
    const std::string &localWorker, const LocalRestartPlan &restartPlan,
    const std::unordered_map<std::string, std::set<uint32_t>> &takenSlotsBySource,
    const std::set<uint32_t> &blockedSlots, std::vector<RecoveryTaskPb> &plannedLocalTasks)
{
    std::unordered_map<std::string, std::set<uint32_t>> plannedSlotsBySource = restartPlan.plannedSlotsBySource;
    for (const auto &it : takenSlotsBySource) {
        auto &slots = plannedSlotsBySource[it.first];
        slots.insert(it.second.begin(), it.second.end());
    }
    if (!restartPlan.localIncidentExists) {
        std::set<uint32_t> assignedSlots;
        for (const auto &it : plannedSlotsBySource) {
            assignedSlots.insert(it.second.begin(), it.second.end());
        }
        auto &selfSlots = plannedSlotsBySource[localWorker];
        for (uint32_t slot = 0; slot < FLAGS_distributed_disk_slot_num; ++slot) {
            if (blockedSlots.find(slot) == blockedSlots.end() && assignedSlots.find(slot) == assignedSlots.end()) {
                selfSlots.insert(slot);
            }
        }
    }

    plannedLocalTasks.clear();
    for (auto &[sourceWorker, slots] : plannedSlotsBySource) {
        for (auto it = slots.begin(); it != slots.end();) {
            if (blockedSlots.find(*it) != blockedSlots.end()) {
                it = slots.erase(it);
            } else {
                ++it;
            }
        }
        if (slots.empty()) {
            continue;
        }
        RecoveryTaskPb task;
        task.set_failed_worker(localWorker);
        task.set_owner_worker(localWorker);
        task.set_source_worker(sourceWorker);
        task.set_task_status(RecoveryTaskPb_TaskStatus_PENDING);
        for (const auto slot : slots) {
            task.add_slots(slot);
        }
        plannedLocalTasks.emplace_back(std::move(task));
    }
    std::sort(plannedLocalTasks.begin(), plannedLocalTasks.end(),
              [](const RecoveryTaskPb &lhs, const RecoveryTaskPb &rhs) {
                  if (lhs.source_worker() != rhs.source_worker()) {
                      return lhs.source_worker() < rhs.source_worker();
                  }
                  return lhs.slots_size() < rhs.slots_size();
              });
    return Status::OK();
}

Status SlotRecoveryManager::RebuildLocalRestartIncident(const std::string &localWorker,
                                                        const std::vector<RecoveryTaskPb> &plannedLocalTasks)
{
    return store_->CASIncident(
        localWorker, [&localWorker, &plannedLocalTasks](SlotRecoveryInfoPb &info, bool &exists, bool &writeBack) {
            SlotRecoveryInfoPb newInfo;
            bool localChanged = !exists;
            for (const auto &task : info.recovery_tasks()) {
                // Keep finished local tasks, because they are already terminal and part of the incident history.
                const bool isLocalFailedWorker = task.failed_worker() == localWorker;
                const bool keepCompleted = isLocalFailedWorker && task.owner_worker() == localWorker
                                           && task.task_status() == RecoveryTaskPb_TaskStatus_COMPLETED;
                // Keep foreign in-flight local tasks, because restart is not allowed to rewrite another worker's
                // IN_PROGRESS ownership.
                const bool keepForeignInProgress = isLocalFailedWorker && task.owner_worker() != localWorker
                                                   && task.task_status() == RecoveryTaskPb_TaskStatus_IN_PROGRESS;
                // Keep unrelated failed-worker chains that happen to share this incident key.
                const bool keepOtherFailedWorker = !isLocalFailedWorker;
                if (keepCompleted || keepForeignInProgress || keepOtherFailedWorker) {
                    *newInfo.add_recovery_tasks() = task;
                } else {
                    localChanged = true;
                }
            }

            for (const auto &task : plannedLocalTasks) {
                auto *canonicalTask = newInfo.add_recovery_tasks();
                *canonicalTask = task;
                localChanged = true;
            }

            SlotRecoveryIncidentState::RefreshCounters(newInfo);
            info = std::move(newInfo);
            writeBack = localChanged;
            return Status::OK();
        });
}

Status SlotRecoveryManager::ScheduleLocalRestartTasks(const std::string &localWorker)
{
    SlotRecoveryInfoPb info;
    auto rc = store_->GetIncident(localWorker, info);
    if (rc.GetCode() == K_NOT_FOUND) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    return ScheduleLocalTasks(localWorker, info);
}

Status SlotRecoveryManager::ExecuteRecoveryTask(const RecoveryTaskPb &task)
{
    LOG(INFO) << FormatString("action=execute_recovery_task local_worker=%s task={%s}", localAddress_.ToString(),
                              TaskSummary(task));
    INJECT_POINT_NO_RETURN("SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecover");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(persistenceApi_ != nullptr, K_RUNTIME_ERROR, "PersistenceApi is null");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(metadataRecoveryManager_ != nullptr, K_RUNTIME_ERROR,
                                         "MetaDataRecoveryManager is null");
    std::vector<ObjectMetaPb> recoveredMetas;
    const auto sourceWorker = task.source_worker().empty() ? task.failed_worker() : task.source_worker();
    SlotPreloadCallback callback = [this, &recoveredMetas](
                                       const SlotPreloadMeta &meta, const std::shared_ptr<std::stringstream> &content) {
        auto recoveredMeta = BuildRecoveredMetadata(meta, localAddress_.ToString());
        recoveredMetas.emplace_back(recoveredMeta);

        std::unordered_map<std::string, std::shared_ptr<std::stringstream>> recoveredContents;
        if (content != nullptr) {
            recoveredContents.emplace(meta.objectKey, content);
        }
        std::vector<std::string> recoveredObjectKeys;
        return metadataRecoveryManager_->RecoverLocalEntries({ std::move(recoveredMeta) }, recoveredContents,
                                                             recoveredObjectKeys);
    };
    for (const auto slotId : task.slots()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(persistenceApi_->PreloadSlot(sourceWorker, slotId, callback),
                                         FormatString("Preload slot %u from %s failed", slotId, sourceWorker));
    }
    std::vector<std::string> failedIds;
    auto recoverRc = metadataRecoveryManager_->RecoverMetadata(recoveredMetas, failedIds);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(recoverRc.IsOk() && failedIds.empty(),
                                         recoverRc.IsOk() ? K_RUNTIME_ERROR : recoverRc.GetCode(),
                                         FormatString("Recover metadata failed. status=%s failedIds=%s",
                                                      recoverRc.ToString(), VectorToString(failedIds)));
    return Status::OK();
}

std::shared_ptr<SlotRecoveryStore> SlotRecoveryManager::CreateStore(datasystem::EtcdStore *etcdStore) const
{
    if (etcdStore == nullptr) {
        return nullptr;
    }
    return std::make_shared<SlotRecoveryStore>(etcdStore);
}

bool SlotRecoveryManager::IsFeatureEnabled() const
{
    return FLAGS_l2_cache_type == "distributed_disk" && store_ != nullptr;
}

}  // namespace object_cache
}  // namespace datasystem
