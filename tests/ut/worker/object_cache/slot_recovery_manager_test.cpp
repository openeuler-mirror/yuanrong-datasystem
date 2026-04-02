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
DS_DECLARE_uint32(l2_cache_slot_num);

namespace datasystem {
namespace ut {
using datasystem::RecoveryTaskPb;
using datasystem::SlotRecoveryInfoPb;
using datasystem::object_cache::SlotRecoveryIncidentState;
using datasystem::object_cache::SlotRecoveryManager;
using datasystem::object_cache::SlotRecoveryPlanner;
using datasystem::object_cache::SlotRecoveryStore;
using namespace ::testing;

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
    SlotRecoveryManagerTestHelper(const HostPort &localAddress, const std::shared_ptr<SlotRecoveryStore> &store)
        : localAddress_(localAddress), store_(store)
    {
    }

    using SlotRecoveryManager::ClaimLocalTask;
    using SlotRecoveryManager::CollectInheritedTasks;
    using SlotRecoveryManager::CompleteLocalTask;
    using SlotRecoveryManager::ExecuteRecoveryTask;
    using SlotRecoveryManager::GetStableActiveWorkers;
    using SlotRecoveryManager::HandleFailedWorkers;
    using SlotRecoveryManager::Init;
    using SlotRecoveryManager::MarkTasksFailedInOtherIncidents;
    using SlotRecoveryManager::PickProcessWorkers;
    using SlotRecoveryManager::PlanIncident;
    using SlotRecoveryManager::ScheduleLocalTasks;
    using SlotRecoveryManager::Shutdown;

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
        FLAGS_l2_cache_slot_num = 4;
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

    // Even after execution failure, the incident must converge through COMPLETED and then delete.
    ASSERT_TRUE(WaitUntil([&store, &failedWorker]() { return store->HasDeletedSnapshot(failedWorker); }));
    auto deleted = store->GetDeletedSnapshot(failedWorker);
    ASSERT_EQ(deleted.total_slots(), 4);
    ASSERT_EQ(deleted.completed_slots(), 4);
    ASSERT_EQ(deleted.failed_slots(), 0);
    for (const auto &task : deleted.recovery_tasks()) {
        EXPECT_EQ(task.owner_worker(), localWorker);
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

    // Both incidents must converge independently without cross-mixing failed_worker identities.
    ASSERT_TRUE(WaitUntil([&store, &failedWorker1]() { return store->HasDeletedSnapshot(failedWorker1); }));
    ASSERT_TRUE(WaitUntil([&store, &failedWorker2]() { return store->HasDeletedSnapshot(failedWorker2); }));

    auto deleted1 = store->GetDeletedSnapshot(failedWorker1);
    auto deleted2 = store->GetDeletedSnapshot(failedWorker2);
    for (const auto &task : deleted1.recovery_tasks()) {
        EXPECT_EQ(task.failed_worker(), failedWorker1);
        EXPECT_EQ(task.owner_worker(), localWorker);
    }
    for (const auto &task : deleted2.recovery_tasks()) {
        EXPECT_EQ(task.failed_worker(), failedWorker2);
        EXPECT_EQ(task.owner_worker(), localWorker);
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

}  // namespace ut
}  // namespace datasystem
