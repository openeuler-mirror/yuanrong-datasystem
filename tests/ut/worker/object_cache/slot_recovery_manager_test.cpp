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
 * Description: Test slot recovery manager coordination helpers.
 */

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../../common/binmock/binmock.h"
#include "common.h"
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_manager.h"
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_uint32(distributed_disk_slot_num);

namespace datasystem {
namespace ut {
using datasystem::RecoveryTaskPb;
using datasystem::SlotRecoveryInfoPb;
using datasystem::object_cache::SlotRecoveryIncidentState;
using datasystem::object_cache::SlotRecoveryManager;
using datasystem::object_cache::MetaDataRecoveryManager;
using datasystem::object_cache::SlotRecoveryPlanner;
using datasystem::object_cache::SlotRecoveryStore;
using namespace ::testing;

namespace {
class FakePersistenceApi : public PersistenceApi {
public:
    using PreloadHandler = std::function<Status(const std::string &, uint32_t, const SlotPreloadCallback &)>;

    explicit FakePersistenceApi(PreloadHandler handler) : handler_(std::move(handler))
    {
    }

    Status Init() override
    {
        return Status::OK();
    }

    Status Save(const std::string &, uint64_t, int64_t, const std::shared_ptr<std::iostream> &, uint64_t,
                WriteMode) override
    {
        return Status::OK();
    }

    Status Get(const std::string &, uint64_t, int64_t, std::shared_ptr<std::stringstream> &) override
    {
        return Status::OK();
    }

    Status GetWithoutVersion(const std::string &, int64_t, uint64_t, std::shared_ptr<std::stringstream> &) override
    {
        return Status::OK();
    }

    Status Del(const std::string &, uint64_t, bool, uint64_t, const uint64_t *const, bool) override
    {
        return Status::OK();
    }

    Status PreloadSlot(const std::string &sourceWorkerAddress, uint32_t slotId,
                       const SlotPreloadCallback &callback) override
    {
        return handler_(sourceWorkerAddress, slotId, callback);
    }

    Status MergeSlot(const std::string &, uint32_t) override
    {
        return Status::OK();
    }

    std::string GetL2CacheRequestSuccessRate() const override
    {
        return "";
    }

private:
    PreloadHandler handler_;
};
}  // namespace

TEST(SlotRecoveryIncidentStateTest, DetectsTerminalStates)
{
    LOG(INFO) << "Scenario: detect terminal task and incident states.";
    RecoveryTaskPb pendingTask;
    pendingTask.set_task_status(RecoveryTaskPb::PENDING);
    EXPECT_FALSE(SlotRecoveryIncidentState::IsTaskTerminal(pendingTask));

    RecoveryTaskPb completedTask;
    completedTask.set_task_status(RecoveryTaskPb::COMPLETED);
    EXPECT_TRUE(SlotRecoveryIncidentState::IsTaskTerminal(completedTask));

    // Mix completed and failed slots so the incident is only terminal after both are accounted for.
    SlotRecoveryInfoPb info;
    info.set_total_slots(4);
    info.set_completed_slots(2);
    info.set_failed_slots(1);
    EXPECT_FALSE(SlotRecoveryIncidentState::IsFullyTerminal(info));

    info.set_failed_slots(2);
    EXPECT_TRUE(SlotRecoveryIncidentState::IsFullyTerminal(info));
}

TEST(SlotRecoveryIncidentStateTest, RefreshesCounters)
{
    LOG(INFO) << "Scenario: refresh incident counters from task states.";
    SlotRecoveryInfoPb info;

    // Build a mixed incident so each aggregate counter has a distinct contribution.
    auto *completedTask = info.add_recovery_tasks();
    completedTask->set_task_status(RecoveryTaskPb::COMPLETED);
    completedTask->add_slots(0);
    completedTask->add_slots(1);

    auto *failedTask = info.add_recovery_tasks();
    failedTask->set_task_status(RecoveryTaskPb::FAILED);
    failedTask->add_slots(2);

    auto *pendingTask = info.add_recovery_tasks();
    pendingTask->set_task_status(RecoveryTaskPb::PENDING);
    pendingTask->add_slots(3);

    // Recompute counters from task details instead of trusting any stored aggregates.
    SlotRecoveryIncidentState::RefreshCounters(info);

    EXPECT_EQ(info.completed_slots(), 2);
    EXPECT_EQ(info.failed_slots(), 1);
    EXPECT_EQ(info.total_slots(), 4);
}

TEST(SlotRecoveryIncidentStateTest, RecomputesTotalSlots)
{
    LOG(INFO) << "Scenario: recompute total slots from task coverage.";
    SlotRecoveryInfoPb info;
    info.set_total_slots(99);

    // Seed a stale total_slots value and overlapping task layouts to verify the refresh path wins.
    auto *pendingTask = info.add_recovery_tasks();
    pendingTask->set_failed_worker("worker1");
    pendingTask->set_task_status(RecoveryTaskPb::PENDING);
    pendingTask->add_slots(0);
    pendingTask->add_slots(2);

    auto *inProgressTask = info.add_recovery_tasks();
    inProgressTask->set_failed_worker("worker2");
    inProgressTask->set_task_status(RecoveryTaskPb::IN_PROGRESS);
    inProgressTask->add_slots(0);
    inProgressTask->add_slots(1);

    auto *completedTask = info.add_recovery_tasks();
    completedTask->set_failed_worker("worker3");
    completedTask->set_task_status(RecoveryTaskPb::COMPLETED);
    completedTask->add_slots(3);

    SlotRecoveryIncidentState::RefreshCounters(info);

    EXPECT_EQ(info.completed_slots(), 1);
    EXPECT_EQ(info.failed_slots(), 0);
    EXPECT_EQ(info.total_slots(), 5);
}

TEST(SlotRecoveryIncidentStateTest, MarksOwnerTasksFailed)
{
    LOG(INFO) << "Scenario: fail unfinished tasks owned by a lost worker.";
    SlotRecoveryInfoPb info;
    // Only unfinished tasks owned by worker2 should flip to FAILED.
    auto *pendingTask = info.add_recovery_tasks();
    pendingTask->set_failed_worker("worker1");
    pendingTask->set_owner_worker("worker2");
    pendingTask->set_task_status(RecoveryTaskPb::PENDING);
    pendingTask->add_slots(0);

    auto *inProgressTask = info.add_recovery_tasks();
    inProgressTask->set_failed_worker("worker1");
    inProgressTask->set_owner_worker("worker2");
    inProgressTask->set_task_status(RecoveryTaskPb::IN_PROGRESS);
    inProgressTask->add_slots(1);
    inProgressTask->add_slots(2);

    auto *completedTask = info.add_recovery_tasks();
    completedTask->set_failed_worker("worker1");
    completedTask->set_owner_worker("worker2");
    completedTask->set_task_status(RecoveryTaskPb::COMPLETED);
    completedTask->add_slots(3);

    auto *otherOwnerTask = info.add_recovery_tasks();
    otherOwnerTask->set_failed_worker("worker1");
    otherOwnerTask->set_owner_worker("worker3");
    otherOwnerTask->set_task_status(RecoveryTaskPb::PENDING);
    otherOwnerTask->add_slots(4);

    auto rc = SlotRecoveryIncidentState::MarkTasksFailedByOwner("worker2", info);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(info.recovery_tasks(0).task_status(), RecoveryTaskPb::FAILED);
    EXPECT_EQ(info.recovery_tasks(1).task_status(), RecoveryTaskPb::FAILED);
    EXPECT_EQ(info.recovery_tasks(2).task_status(), RecoveryTaskPb::COMPLETED);
    EXPECT_EQ(info.recovery_tasks(3).task_status(), RecoveryTaskPb::PENDING);
    EXPECT_EQ(info.completed_slots(), 1);
    EXPECT_EQ(info.failed_slots(), 3);
}

TEST(SlotRecoveryPlannerTest, BuildsRoundRobinPlan)
{
    LOG(INFO) << "Scenario: build a stable round-robin recovery plan.";
    SlotRecoveryInfoPb info;
    std::vector<std::string> activeWorkers{ "worker2", "worker3" };

    // Initial planning should split slots deterministically across active workers.
    auto rc = SlotRecoveryPlanner::BuildInitialTasks("worker1", 4, activeWorkers, info);

    ASSERT_TRUE(rc.IsOk());
    ASSERT_EQ(info.recovery_tasks_size(), 2);
    ASSERT_EQ(info.recovery_tasks(0).failed_worker(), "worker1");
    ASSERT_EQ(info.recovery_tasks(0).owner_worker(), "worker2");
    ASSERT_EQ(info.recovery_tasks(0).source_worker(), "worker1");
    ASSERT_EQ(info.recovery_tasks(0).slots_size(), 2);
    EXPECT_EQ(info.recovery_tasks(0).slots(0), 0);
    EXPECT_EQ(info.recovery_tasks(0).slots(1), 2);
    ASSERT_EQ(info.recovery_tasks(1).owner_worker(), "worker3");
    EXPECT_EQ(info.recovery_tasks(1).slots(0), 1);
    EXPECT_EQ(info.recovery_tasks(1).slots(1), 3);
}

TEST(SlotRecoveryPlannerTest, ReassignsFailedOwnerTasks)
{
    LOG(INFO) << "Scenario: collect and reassign tasks from a failed owner.";
    SlotRecoveryInfoPb info;
    // Only the unfinished task owned by worker2 should be inherited.
    auto *task0 = info.add_recovery_tasks();
    task0->set_failed_worker("worker1");
    task0->set_owner_worker("worker2");
    task0->add_slots(0);
    task0->add_slots(2);
    task0->set_task_status(RecoveryTaskPb::IN_PROGRESS);

    auto *task1 = info.add_recovery_tasks();
    task1->set_failed_worker("worker1");
    task1->set_owner_worker("worker3");
    task1->add_slots(1);
    task1->add_slots(3);
    task1->set_task_status(RecoveryTaskPb::COMPLETED);

    std::vector<RecoveryTaskPb> inheritedRawTasks;
    auto rc = SlotRecoveryPlanner::CollectInheritedTasks("worker2", info, inheritedRawTasks);

    ASSERT_TRUE(rc.IsOk());
    ASSERT_EQ(inheritedRawTasks.size(), 1);
    EXPECT_EQ(inheritedRawTasks[0].failed_worker(), "worker1");
    EXPECT_EQ(inheritedRawTasks[0].owner_worker(), "worker2");
    EXPECT_EQ(inheritedRawTasks[0].source_worker(), "worker1");
    EXPECT_EQ(inheritedRawTasks[0].task_status(), RecoveryTaskPb::IN_PROGRESS);
    ASSERT_EQ(inheritedRawTasks[0].slots_size(), 2);
    EXPECT_EQ(inheritedRawTasks[0].slots(0), 0);
    EXPECT_EQ(inheritedRawTasks[0].slots(1), 2);

    // Reassignment should preserve the slot set while resetting ownership and status.
    std::vector<std::string> activeWorkers{ "worker3", "worker4" };
    std::vector<RecoveryTaskPb> inheritedTasks;
    rc = SlotRecoveryPlanner::ReassignInheritedTasks(inheritedRawTasks, activeWorkers, inheritedTasks);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_EQ(inheritedTasks.size(), 1);
    EXPECT_EQ(inheritedTasks[0].failed_worker(), "worker1");
    EXPECT_EQ(inheritedTasks[0].owner_worker(), "worker3");
    EXPECT_EQ(inheritedTasks[0].source_worker(), "worker2");
    EXPECT_EQ(inheritedTasks[0].task_status(), RecoveryTaskPb::PENDING);
    ASSERT_EQ(inheritedTasks[0].slots_size(), 2);
    EXPECT_EQ(inheritedTasks[0].slots(0), 0);
    EXPECT_EQ(inheritedTasks[0].slots(1), 2);

    // The original incident still needs to record that worker2's unfinished work has failed.
    rc = SlotRecoveryIncidentState::MarkTasksFailedByOwner("worker2", info);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_EQ(info.recovery_tasks_size(), 2);
    EXPECT_EQ(info.recovery_tasks(0).task_status(), RecoveryTaskPb::FAILED);
    EXPECT_EQ(info.failed_slots(), 2);
}

namespace {
constexpr int WAIT_TIMEOUT_MS = 3000;
constexpr int WAIT_INTERVAL_MS = 10;

RecoveryTaskPb MakeTask(const std::string &failedWorker, const std::string &ownerWorker,
                        RecoveryTaskPb::TaskStatus status, std::initializer_list<uint32_t> slots,
                        const std::string &sourceWorker = "")
{
    RecoveryTaskPb task;
    task.set_failed_worker(failedWorker);
    task.set_owner_worker(ownerWorker);
    task.set_task_status(status);
    task.set_source_worker(sourceWorker.empty() ? failedWorker : sourceWorker);
    for (auto slot : slots) {
        task.add_slots(slot);
    }
    return task;
}

SlotRecoveryInfoPb BuildIncident(std::initializer_list<RecoveryTaskPb> tasks, uint32_t totalSlots = 0)
{
    SlotRecoveryInfoPb info;
    for (const auto &task : tasks) {
        *info.add_recovery_tasks() = task;
    }
    if (totalSlots != 0) {
        info.set_total_slots(totalSlots);
    }
    SlotRecoveryIncidentState::RefreshCounters(info);
    return info;
}

std::set<uint32_t> CollectSlots(const RecoveryTaskPb &task)
{
    return std::set<uint32_t>(task.slots().begin(), task.slots().end());
}

std::set<uint32_t> CollectAllSlots(const SlotRecoveryInfoPb &info)
{
    std::set<uint32_t> slots;
    for (const auto &task : info.recovery_tasks()) {
        for (auto slot : task.slots()) {
            auto inserted = slots.insert(slot);
            EXPECT_TRUE(inserted.second);
        }
    }
    return slots;
}

bool WaitUntil(const std::function<bool()> &predicate, int timeoutMs = WAIT_TIMEOUT_MS)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_INTERVAL_MS));
    }
    return predicate();
}
}  // namespace

class FakeSlotRecoveryStore : public SlotRecoveryStore {
public:
    FakeSlotRecoveryStore() : SlotRecoveryStore(nullptr)
    {
    }

    Status Init() override
    {
        return Status::OK();
    }

    Status GetIncident(const std::string &failedWorker, SlotRecoveryInfoPb &info) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = incidents_.find(failedWorker);
        if (it == incidents_.end()) {
            return Status(K_NOT_FOUND, __LINE__, __FILE__, "incident not found");
        }
        info = it->second;
        return Status::OK();
    }

    Status ListIncidents(std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        incidents.clear();
        for (const auto &entry : incidents_) {
            incidents.emplace_back(entry.first, entry.second);
        }
        return Status::OK();
    }

    Status DeleteIncident(const std::string &failedWorker) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = incidents_.find(failedWorker);
        if (it == incidents_.end()) {
            return Status(K_NOT_FOUND, __LINE__, __FILE__, "incident not found");
        }
        deletedSnapshots_[failedWorker] = it->second;
        incidents_.erase(it);
        return Status::OK();
    }

    Status UpdateIncident(const std::string &failedWorker, const SlotRecoveryInfoPb &info) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        incidents_[failedWorker] = info;
        histories_[failedWorker].emplace_back(info);
        return Status::OK();
    }

    Status CASIncident(const std::string &failedWorker, const IncidentMutator &mutator) override
    {
        for (int attempt = 0; attempt < 16; ++attempt) {
            SlotRecoveryInfoPb info;
            uint64_t observedVersion = 0;
            bool exists = false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                observedVersion = versions_[failedWorker];
                auto it = incidents_.find(failedWorker);
                exists = it != incidents_.end();
                if (exists) {
                    info = it->second;
                }
            }

            bool writeBack = false;
            auto rc = mutator(info, exists, writeBack);
            if (rc.IsError() || !writeBack) {
                return rc;
            }

            std::lock_guard<std::mutex> lock(mutex_);
            if (versions_[failedWorker] != observedVersion) {
                continue;
            }
            bool existedBefore = incidents_.find(failedWorker) != incidents_.end();
            incidents_[failedWorker] = info;
            histories_[failedWorker].emplace_back(info);
            ++versions_[failedWorker];
            if (!existedBefore) {
                ++createCount_[failedWorker];
            }
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "CAS exceeded retry budget");
    }

    void SeedIncident(const std::string &failedWorker, const SlotRecoveryInfoPb &info)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        incidents_[failedWorker] = info;
        histories_[failedWorker].emplace_back(info);
        ++versions_[failedWorker];
    }

    bool HasDeletedSnapshot(const std::string &failedWorker)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return deletedSnapshots_.find(failedWorker) != deletedSnapshots_.end();
    }

    SlotRecoveryInfoPb GetDeletedSnapshot(const std::string &failedWorker)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return deletedSnapshots_.at(failedWorker);
    }

    SlotRecoveryInfoPb GetCurrentIncident(const std::string &failedWorker)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return incidents_.at(failedWorker);
    }

    std::vector<SlotRecoveryInfoPb> GetHistory(const std::string &failedWorker)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = histories_.find(failedWorker);
        if (it == histories_.end()) {
            return {};
        }
        return it->second;
    }

    int GetCreateCount(const std::string &failedWorker)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return createCount_[failedWorker];
    }

private:
    std::mutex mutex_;
    std::map<std::string, SlotRecoveryInfoPb> incidents_;
    std::unordered_map<std::string, uint64_t> versions_;
    std::unordered_map<std::string, std::vector<SlotRecoveryInfoPb>> histories_;
    std::unordered_map<std::string, SlotRecoveryInfoPb> deletedSnapshots_;
    std::unordered_map<std::string, int> createCount_;
};

class SlotRecoveryManagerTestHelper : public SlotRecoveryManager {
public:
    using RestartPlan = SlotRecoveryManager::LocalRestartPlan;

    SlotRecoveryManagerTestHelper(const HostPort &localAddress, const std::shared_ptr<SlotRecoveryStore> &store)
        : localAddress_(localAddress), store_(store)
    {
    }

    using SlotRecoveryManager::BuildPlannedLocalRestartTasks;
    using SlotRecoveryManager::ClaimLocalTask;
    using SlotRecoveryManager::CollectInheritedTasks;
    using SlotRecoveryManager::CollectLocalRestartPlan;
    using SlotRecoveryManager::CompleteLocalTask;
    using SlotRecoveryManager::ExecuteRecoveryTask;
    using SlotRecoveryManager::GetStableActiveWorkers;
    using SlotRecoveryManager::HandleFailedWorkers;
    using SlotRecoveryManager::HandleLocalRestart;
    using SlotRecoveryManager::Init;
    using SlotRecoveryManager::MarkTasksFailedInOtherIncidents;
    using SlotRecoveryManager::PickProcessWorkers;
    using SlotRecoveryManager::PlanIncident;
    using SlotRecoveryManager::ScheduleLocalTasks;
    using SlotRecoveryManager::Shutdown;
    using SlotRecoveryManager::TakeOverPendingFromSourceIncident;

    Status InitForTest()
    {
        return SlotRecoveryManager::Init(localAddress_, nullptr, nullptr, nullptr, nullptr);
    }

    void SetActiveWorkers(const std::vector<std::string> &workers)
    {
        activeWorkers_ = workers;
        BINEXPECT_CALL(&SlotRecoveryManagerTestHelper::GetStableActiveWorkers, ()).WillRepeatedly(Invoke([this]() {
            return activeWorkers_;
        }));
    }

    std::vector<RecoveryTaskPb> GetExecutedTasks() const
    {
        std::lock_guard<std::mutex> lock(executedTasksMutex_);
        return executedTasks_;
    }

    void SetExecutionHook(std::function<Status(const RecoveryTaskPb &)> hook)
    {
        executionHook_ = std::move(hook);
        BINEXPECT_CALL(&SlotRecoveryManagerTestHelper::ExecuteRecoveryTask, (_))
            .WillRepeatedly(Invoke([this](const RecoveryTaskPb &task) {
                {
                    std::lock_guard<std::mutex> lock(executedTasksMutex_);
                    executedTasks_.emplace_back(task);
                }
                if (executionHook_) {
                    return executionHook_(task);
                }
                return Status::OK();
            }));
    }

private:
    std::shared_ptr<SlotRecoveryStore> CreateStore(datasystem::EtcdStore *etcdStore) const override
    {
        (void)etcdStore;
        return store_;
    }

    HostPort localAddress_;
    std::shared_ptr<SlotRecoveryStore> store_;
    std::vector<std::string> activeWorkers_;
    std::function<Status(const RecoveryTaskPb &)> executionHook_;
    mutable std::mutex executedTasksMutex_;
    std::vector<RecoveryTaskPb> executedTasks_;
};

class SlotRecoveryTest : public testing::Test {
public:
    void SetUp() override
    {
        FLAGS_l2_cache_type = "distributed_disk";
        FLAGS_distributed_disk_slot_num = 4;
    }

    void TearDown() override
    {
        RELEASE_STUBS
    }
};

TEST_F(SlotRecoveryTest, LocalFailureCompletes)
{
    LOG(INFO) << "Scenario: local execution failure still completes recovery cleanup.";
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    const std::string failedWorker = "127.0.0.1:4001";
    const std::string localWorker = "127.0.0.1:4002";

    // Inject an execution failure so the manager must take the 'log error but still complete' branch.
    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 4002), store);
    manager.SetActiveWorkers({ localWorker });
    manager.SetExecutionHook([](const RecoveryTaskPb &) {
        return Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "injected local recovery failure");
    });

    DS_ASSERT_OK(manager.InitForTest());
    DS_ASSERT_OK(manager.HandleFailedWorkers({ HostPort("127.0.0.1", 4001) }));

    // Even after execution failure, the incident must converge through COMPLETED and then be cleaned up.
    ASSERT_TRUE(WaitUntil([&store, &failedWorker]() { return store->HasDeletedSnapshot(failedWorker); }));
    auto deleted = store->GetDeletedSnapshot(failedWorker);
    ASSERT_EQ(deleted.total_slots(), 4);
    ASSERT_EQ(deleted.completed_slots(), 4);
    ASSERT_EQ(deleted.failed_slots(), 0);
    for (const auto &task : deleted.recovery_tasks()) {
        EXPECT_EQ(task.owner_worker(), localWorker);
        EXPECT_EQ(task.source_worker(), localWorker);
        EXPECT_EQ(task.task_status(), RecoveryTaskPb::COMPLETED);
    }
    EXPECT_FALSE(manager.GetExecutedTasks().empty());

    manager.Shutdown();
}

TEST_F(SlotRecoveryTest, MultiFailureIsolation)
{
    LOG(INFO) << "Scenario: multiple failures stay isolated from each other.";
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    const std::string failedWorker1 = "127.0.0.1:5001";
    const std::string failedWorker2 = "127.0.0.1:5002";
    const std::string localWorker = "127.0.0.1:5003";

    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 5003), store);
    manager.SetActiveWorkers({ localWorker });

    DS_ASSERT_OK(manager.InitForTest());
    DS_ASSERT_OK(manager.HandleFailedWorkers({ HostPort("127.0.0.1", 5001), HostPort("127.0.0.1", 5002) }));

    // Incidents are deleted once terminal; validate isolation from deleted snapshots.
    ASSERT_TRUE(WaitUntil([&store, &failedWorker1]() { return store->HasDeletedSnapshot(failedWorker1); }));
    ASSERT_TRUE(WaitUntil([&store, &failedWorker2]() { return store->HasDeletedSnapshot(failedWorker2); }));

    auto deleted1 = store->GetDeletedSnapshot(failedWorker1);
    auto deleted2 = store->GetDeletedSnapshot(failedWorker2);
    ASSERT_EQ(deleted1.completed_slots(), 4);
    ASSERT_EQ(deleted2.completed_slots(), 4);
    for (const auto &task : deleted1.recovery_tasks()) {
        EXPECT_EQ(task.failed_worker(), failedWorker1);
        EXPECT_EQ(task.owner_worker(), localWorker);
        EXPECT_EQ(task.source_worker(), localWorker);
    }
    for (const auto &task : deleted2.recovery_tasks()) {
        EXPECT_EQ(task.failed_worker(), failedWorker2);
        EXPECT_EQ(task.owner_worker(), localWorker);
        EXPECT_EQ(task.source_worker(), localWorker);
    }
    EXPECT_EQ(store->GetCreateCount(failedWorker1), 1);
    EXPECT_EQ(store->GetCreateCount(failedWorker2), 1);

    manager.Shutdown();
}

TEST_F(SlotRecoveryTest, CreateIdempotent)
{
    LOG(INFO) << "Scenario: concurrent planning creates only one incident.";
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    const std::string failedWorker = "127.0.0.1:6001";
    const std::string worker2 = "127.0.0.1:6002";
    const std::string worker3 = "127.0.0.1:6003";
    std::vector<std::string> activeWorkers{ worker2, worker3 };

    SlotRecoveryManagerTestHelper manager2(HostPort("127.0.0.1", 6002), store);
    SlotRecoveryManagerTestHelper manager3(HostPort("127.0.0.1", 6003), store);
    manager2.SetActiveWorkers(activeWorkers);
    manager3.SetActiveWorkers(activeWorkers);
    DS_ASSERT_OK(manager2.InitForTest());
    DS_ASSERT_OK(manager3.InitForTest());

    // Plan the same failure from two managers to verify create-once behavior.
    std::vector<std::pair<std::string, SlotRecoveryInfoPb>> incidents;
    DS_ASSERT_OK(store->ListIncidents(incidents));
    DS_ASSERT_OK(manager2.PlanIncident(failedWorker, activeWorkers, incidents));
    DS_ASSERT_OK(manager3.PlanIncident(failedWorker, activeWorkers, incidents));

    // The resulting incident must contain exactly one stable plan with no duplicated slot coverage.
    auto planned = store->GetCurrentIncident(failedWorker);
    ASSERT_EQ(store->GetCreateCount(failedWorker), 1);
    ASSERT_EQ(planned.total_slots(), 4);
    ASSERT_EQ(planned.recovery_tasks_size(), 2);
    EXPECT_EQ(CollectAllSlots(planned), (std::set<uint32_t>{ 0, 1, 2, 3 }));
}

TEST_F(SlotRecoveryTest, CreatesLocalIncidentOnRestart)
{
    LOG(INFO) << "Scenario: restart rebuilds a missing local incident.";
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 7001), store);
    manager.SetActiveWorkers({ "127.0.0.1:7001" });
    DS_ASSERT_OK(manager.InitForTest());

    // With no local incident persisted, restart should synthesize and complete the canonical one.
    DS_ASSERT_OK(manager.HandleLocalRestart());

    ASSERT_TRUE(WaitUntil([&store]() { return store->HasDeletedSnapshot("127.0.0.1:7001"); }));
    auto deleted = store->GetDeletedSnapshot("127.0.0.1:7001");
    ASSERT_EQ(deleted.total_slots(), 4);
    ASSERT_EQ(deleted.failed_slots(), 0);
    ASSERT_EQ(deleted.completed_slots(), 4);
    ASSERT_EQ(deleted.recovery_tasks_size(), 1);
    EXPECT_EQ(deleted.recovery_tasks(0).failed_worker(), "127.0.0.1:7001");
    EXPECT_EQ(deleted.recovery_tasks(0).owner_worker(), "127.0.0.1:7001");
    EXPECT_EQ(deleted.recovery_tasks(0).task_status(), RecoveryTaskPb::COMPLETED);
    EXPECT_EQ(CollectSlots(deleted.recovery_tasks(0)), (std::set<uint32_t>{ 0, 1, 2, 3 }));
}

TEST_F(SlotRecoveryTest, ExcludesBlockedSlotsOnRestart)
{
    LOG(INFO) << "Scenario: restart excludes slots already blocked elsewhere.";
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 7451), store);
    manager.SetActiveWorkers({ "127.0.0.1:7451", "127.0.0.1:7452" });
    DS_ASSERT_OK(manager.InitForTest());

    // Seed another incident that already owns part of the restarting worker's slot space.
    store->SeedIncident(
        "127.0.0.1:7452",
        BuildIncident(
            { MakeTask("127.0.0.1:7451", "127.0.0.1:7453", RecoveryTaskPb::PENDING, { 0 }),
              MakeTask("127.0.0.1:7451", "127.0.0.1:7454", RecoveryTaskPb::IN_PROGRESS, { 1 }),
              MakeTask("127.0.0.1:7451", "127.0.0.1:7455", RecoveryTaskPb::COMPLETED, { 2 }, "127.0.0.1:7455") },
            3));

    DS_ASSERT_OK(manager.HandleLocalRestart());

    // History lets us observe the transient canonical pending task before async execution finishes.
    auto history = store->GetHistory("127.0.0.1:7451");
    ASSERT_FALSE(history.empty());
    bool sawTakenSourceTask = false;
    bool sawCompletedSourceTask = false;
    bool sawSelfFillTask = false;
    for (const auto &snapshot : history) {
        for (const auto &task : snapshot.recovery_tasks()) {
            if (task.failed_worker() == "127.0.0.1:7451" && task.owner_worker() == "127.0.0.1:7451"
                && task.task_status() == RecoveryTaskPb::PENDING) {
                if (task.source_worker() == "127.0.0.1:7451" && CollectSlots(task) == (std::set<uint32_t>{ 0, 3 })) {
                    sawSelfFillTask = true;
                } else if (task.source_worker() == "127.0.0.1:7455"
                           && CollectSlots(task) == (std::set<uint32_t>{ 2 })) {
                    sawCompletedSourceTask = true;
                }
            }
        }
    }
    sawTakenSourceTask = sawSelfFillTask;
    EXPECT_TRUE(sawTakenSourceTask);
    EXPECT_TRUE(sawCompletedSourceTask);

    // The source incident should only fail the task that restart has taken over.
    auto sourceIncident = store->GetCurrentIncident("127.0.0.1:7452");
    ASSERT_EQ(sourceIncident.recovery_tasks_size(), 3);
    EXPECT_EQ(sourceIncident.recovery_tasks(0).task_status(), RecoveryTaskPb::FAILED);
    EXPECT_EQ(sourceIncident.recovery_tasks(1).task_status(), RecoveryTaskPb::IN_PROGRESS);
    EXPECT_EQ(sourceIncident.recovery_tasks(2).task_status(), RecoveryTaskPb::COMPLETED);
}

TEST_F(SlotRecoveryTest, ExcludesNewlyBlockedSlotsOnRestart)
{
    LOG(INFO) << "Scenario: restart excludes slots claimed during takeover race.";
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 7461), store);
    manager.SetActiveWorkers({ "127.0.0.1:7461", "127.0.0.1:7462" });
    DS_ASSERT_OK(manager.InitForTest());

    store->SeedIncident(
        "127.0.0.1:7462",
        BuildIncident({ MakeTask("127.0.0.1:7461", "127.0.0.1:7463", RecoveryTaskPb::PENDING, { 1, 2 }) }, 2));

    // Capture the restart plan before the source incident changes under us.
    SlotRecoveryManagerTestHelper::RestartPlan restartPlan;
    DS_ASSERT_OK(manager.CollectLocalRestartPlan("127.0.0.1:7461", restartPlan));
    EXPECT_FALSE(restartPlan.localIncidentExists);
    EXPECT_TRUE(restartPlan.plannedSlotsBySource.empty());
    ASSERT_EQ(restartPlan.sourceIncidentKeys.size(), 1);
    EXPECT_EQ(restartPlan.sourceIncidentKeys[0], "127.0.0.1:7462");

    // The source owner wins the claim after restart scans ETCD but before restart tries the takeover CAS.
    store->UpdateIncident(
        "127.0.0.1:7462",
        BuildIncident({ MakeTask("127.0.0.1:7461", "127.0.0.1:7463", RecoveryTaskPb::IN_PROGRESS, { 1, 2 }) }, 2));

    std::unordered_map<std::string, std::set<uint32_t>> takenSlotsBySource;
    std::vector<uint32_t> blockedSlots;
    bool shouldDeleteSource = false;
    DS_ASSERT_OK(manager.TakeOverPendingFromSourceIncident("127.0.0.1:7462", "127.0.0.1:7461", takenSlotsBySource,
                                                           blockedSlots, shouldDeleteSource));

    EXPECT_TRUE(takenSlotsBySource.empty());
    EXPECT_FALSE(shouldDeleteSource);
    EXPECT_EQ(std::set<uint32_t>(blockedSlots.begin(), blockedSlots.end()), (std::set<uint32_t>{ 1, 2 }));

    // Rebuild the local plan with the newly blocked slots excluded.
    std::vector<RecoveryTaskPb> plannedLocalTasks;
    DS_ASSERT_OK(manager.BuildPlannedLocalRestartTasks("127.0.0.1:7461", restartPlan, {},
                                                       std::set<uint32_t>(blockedSlots.begin(), blockedSlots.end()),
                                                       plannedLocalTasks));
    ASSERT_EQ(plannedLocalTasks.size(), 1u);
    EXPECT_EQ(plannedLocalTasks[0].source_worker(), "127.0.0.1:7461");
    EXPECT_EQ(CollectSlots(plannedLocalTasks[0]), (std::set<uint32_t>{ 0, 3 }));
}

TEST_F(SlotRecoveryTest, PreservesTakenSourceOnRestart)
{
    LOG(INFO) << "Scenario: restart keeps the original preload source for taken pending tasks.";
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 7469), store);
    manager.SetActiveWorkers({ "127.0.0.1:7469", "127.0.0.1:7470" });
    DS_ASSERT_OK(manager.InitForTest());

    store->SeedIncident(
        "127.0.0.1:7470",
        BuildIncident(
            { MakeTask("127.0.0.1:7469", "127.0.0.1:7471", RecoveryTaskPb::PENDING, { 1, 2 }, "127.0.0.1:7472") }, 2));

    SlotRecoveryManagerTestHelper::RestartPlan restartPlan;
    DS_ASSERT_OK(manager.CollectLocalRestartPlan("127.0.0.1:7469", restartPlan));

    std::unordered_map<std::string, std::set<uint32_t>> takenSlotsBySource;
    std::vector<uint32_t> blockedSlots;
    bool shouldDeleteSource = false;
    DS_ASSERT_OK(manager.TakeOverPendingFromSourceIncident("127.0.0.1:7470", "127.0.0.1:7469", takenSlotsBySource,
                                                           blockedSlots, shouldDeleteSource));

    ASSERT_EQ(takenSlotsBySource.size(), 1u);
    EXPECT_EQ(takenSlotsBySource.begin()->first, "127.0.0.1:7472");
    EXPECT_EQ(takenSlotsBySource.begin()->second, (std::set<uint32_t>{ 1, 2 }));

    std::vector<RecoveryTaskPb> plannedLocalTasks;
    DS_ASSERT_OK(manager.BuildPlannedLocalRestartTasks("127.0.0.1:7469", restartPlan, takenSlotsBySource, {},
                                                       plannedLocalTasks));
    ASSERT_EQ(plannedLocalTasks.size(), 2u);
    EXPECT_EQ(plannedLocalTasks[0].source_worker(), "127.0.0.1:7469");
    EXPECT_EQ(CollectSlots(plannedLocalTasks[0]), (std::set<uint32_t>{ 0, 3 }));
    EXPECT_EQ(plannedLocalTasks[1].source_worker(), "127.0.0.1:7472");
    EXPECT_EQ(CollectSlots(plannedLocalTasks[1]), (std::set<uint32_t>{ 1, 2 }));
}

TEST_F(SlotRecoveryTest, ExcludesSourceBlockedSlotsOnRestart)
{
    LOG(INFO) << "Scenario: restart excludes slots already blocked in the source incident.";
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 7465), store);
    manager.SetActiveWorkers({ "127.0.0.1:7465", "127.0.0.1:7466" });
    DS_ASSERT_OK(manager.InitForTest());

    store->SeedIncident(
        "127.0.0.1:7466",
        BuildIncident(
            { MakeTask("127.0.0.1:7465", "127.0.0.1:7467", RecoveryTaskPb::IN_PROGRESS, { 1, 2 }),
              MakeTask("127.0.0.1:7465", "127.0.0.1:7468", RecoveryTaskPb::COMPLETED, { 3 }, "127.0.0.1:7468") },
            3));

    // The restart plan should discover the source incident but not claim its blocked slots.
    SlotRecoveryManagerTestHelper::RestartPlan restartPlan;
    DS_ASSERT_OK(manager.CollectLocalRestartPlan("127.0.0.1:7465", restartPlan));
    EXPECT_FALSE(restartPlan.localIncidentExists);
    ASSERT_EQ(restartPlan.sourceIncidentKeys.size(), 1);
    EXPECT_EQ(restartPlan.sourceIncidentKeys[0], "127.0.0.1:7466");

    std::unordered_map<std::string, std::set<uint32_t>> takenSlotsBySource;
    std::vector<uint32_t> blockedSlots;
    bool shouldDeleteSource = false;
    DS_ASSERT_OK(manager.TakeOverPendingFromSourceIncident("127.0.0.1:7466", "127.0.0.1:7465", takenSlotsBySource,
                                                           blockedSlots, shouldDeleteSource));

    EXPECT_TRUE(takenSlotsBySource.empty());
    EXPECT_FALSE(shouldDeleteSource);
    EXPECT_EQ(std::set<uint32_t>(blockedSlots.begin(), blockedSlots.end()), (std::set<uint32_t>{ 1, 2 }));

    std::vector<RecoveryTaskPb> plannedLocalTasks;
    DS_ASSERT_OK(manager.BuildPlannedLocalRestartTasks("127.0.0.1:7465", restartPlan, {},
                                                       std::set<uint32_t>(blockedSlots.begin(), blockedSlots.end()),
                                                       plannedLocalTasks));
    ASSERT_EQ(plannedLocalTasks.size(), 2u);
    EXPECT_EQ(plannedLocalTasks[0].source_worker(), "127.0.0.1:7465");
    EXPECT_EQ(CollectSlots(plannedLocalTasks[0]), (std::set<uint32_t>{ 0 }));
    EXPECT_EQ(plannedLocalTasks[1].source_worker(), "127.0.0.1:7468");
    EXPECT_EQ(CollectSlots(plannedLocalTasks[1]), (std::set<uint32_t>{ 3 }));
}

TEST_F(SlotRecoveryTest, ExecuteRecoveryTaskShouldRecoverEntriesObjectByObjectDuringPreload)
{
    LOG(INFO) << "Scenario: preload callback recovers local entries one object at a time.";
    using RecoverLocalEntriesMethod = Status (MetaDataRecoveryManager::*)(
        const std::vector<ObjectMetaPb> &,
        const std::unordered_map<std::string, std::shared_ptr<std::stringstream>> &,
        std::vector<std::string> &) const;
    using RecoverMetadataMethod =
        Status (MetaDataRecoveryManager::*)(const std::vector<ObjectMetaPb> &, std::vector<std::string> &,
                                            std::string);
    auto store = std::make_shared<FakeSlotRecoveryStore>();
    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 7101), store);
    auto persistApi = std::make_shared<FakePersistenceApi>(
        [](const std::string &sourceWorkerAddress, uint32_t slotId, const SlotPreloadCallback &callback) {
            EXPECT_EQ(sourceWorkerAddress, "127.0.0.1:7100");
            EXPECT_EQ(slotId, 3U);

            SlotPreloadMeta meta1{ "tenant/object_a", 1, WriteMode::WRITE_THROUGH_L2_CACHE, 11 };
            auto content1 = std::make_shared<std::stringstream>();
            (*content1) << "v1";
            RETURN_IF_NOT_OK(callback(meta1, content1));

            SlotPreloadMeta meta2{ "tenant/object_a", 2, WriteMode::WRITE_THROUGH_L2_CACHE, 22 };
            auto content2 = std::make_shared<std::stringstream>();
            (*content2) << "v2";
            RETURN_IF_NOT_OK(callback(meta2, content2));

            SlotPreloadMeta meta3{ "tenant/object_b", 3, WriteMode::WRITE_THROUGH_L2_CACHE, 33 };
            auto content3 = std::make_shared<std::stringstream>();
            (*content3) << "v3";
            return callback(meta3, content3);
        });
    MetaDataRecoveryManager metadataManager(HostPort("127.0.0.1", 7101), nullptr, nullptr, nullptr);

    std::vector<std::vector<std::string>> localRecoverKeys;
    BINEXPECT_CALL((RecoverLocalEntriesMethod) & MetaDataRecoveryManager::RecoverLocalEntries, (_, _, _))
        .Times(3)
        .WillRepeatedly(Invoke([&localRecoverKeys](const std::vector<ObjectMetaPb> &recoverMetas,
                                                   const std::unordered_map<std::string,
                                                                            std::shared_ptr<std::stringstream>>
                                                       &recoveredContents,
                                                   std::vector<std::string> &recoveredObjectKeys) {
            EXPECT_EQ(recoverMetas.size(), 1U);
            if (recoverMetas.size() != 1U || recoveredContents.size() != 1U) {
                ADD_FAILURE() << "RecoverLocalEntries should receive exactly one object per callback.";
                return Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "invalid recover inputs");
            }
            const auto &meta = recoverMetas.front();
            auto found = recoveredContents.find(meta.object_key());
            if (found == recoveredContents.end() || found->second == nullptr) {
                ADD_FAILURE() << "Recovered content should exist for each callback object.";
                return Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "missing recovered content");
            }
            recoveredObjectKeys.emplace_back(meta.object_key());
            localRecoverKeys.emplace_back(recoveredObjectKeys);
            return Status::OK();
        }));

    std::vector<ObjectMetaPb> finalRecoveredMetas;
    BINEXPECT_CALL((RecoverMetadataMethod) & MetaDataRecoveryManager::RecoverMetadata, (_, _, _))
        .Times(1)
        .WillOnce(Invoke([&finalRecoveredMetas](const std::vector<ObjectMetaPb> &metas,
                                                std::vector<std::string> &failedIds, std::string standbyMasterAddr) {
            EXPECT_TRUE(failedIds.empty());
            EXPECT_TRUE(standbyMasterAddr.empty());
            finalRecoveredMetas = metas;
            return Status::OK();
        }));

    RecoveryTaskPb task;
    task.set_failed_worker("127.0.0.1:7100");
    task.add_slots(3);

    DS_ASSERT_OK(manager.Init(HostPort("127.0.0.1", 7101), nullptr, persistApi, nullptr, nullptr, &metadataManager));
    DS_ASSERT_OK(manager.ExecuteRecoveryTask(task));

    ASSERT_EQ(localRecoverKeys.size(), 3U);
    EXPECT_THAT(localRecoverKeys[0], ElementsAre("tenant/object_a"));
    EXPECT_THAT(localRecoverKeys[1], ElementsAre("tenant/object_a"));
    EXPECT_THAT(localRecoverKeys[2], ElementsAre("tenant/object_b"));

    ASSERT_EQ(finalRecoveredMetas.size(), 3U);
    EXPECT_EQ(finalRecoveredMetas[0].object_key(), "tenant/object_a");
    EXPECT_EQ(finalRecoveredMetas[0].version(), 1U);
    EXPECT_EQ(finalRecoveredMetas[1].object_key(), "tenant/object_a");
    EXPECT_EQ(finalRecoveredMetas[1].version(), 2U);
    EXPECT_EQ(finalRecoveredMetas[2].object_key(), "tenant/object_b");
    EXPECT_EQ(finalRecoveredMetas[2].version(), 3U);
}

}  // namespace ut
}  // namespace datasystem
