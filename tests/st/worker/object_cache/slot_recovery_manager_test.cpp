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
 * Description: Real ETCD slot recovery manager tests.
 */

#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <gmock/gmock.h>

#include "../../../common/binmock/binmock.h"
#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_manager.h"
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_uint32(distributed_disk_slot_num);
DS_DECLARE_string(etcd_address);

namespace datasystem {
namespace st {
using datasystem::RecoveryTaskPb;
using datasystem::SlotRecoveryInfoPb;
using datasystem::object_cache::SlotRecoveryManager;
using datasystem::object_cache::SlotRecoveryStore;
using namespace ::testing;

namespace {
constexpr int WAIT_TIMEOUT_MS = 3000;
constexpr int WAIT_INTERVAL_MS = 10;

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

std::set<uint32_t> CollectSlots(const RecoveryTaskPb &task)
{
    return std::set<uint32_t>(task.slots().begin(), task.slots().end());
}

RecoveryTaskPb *FindTaskByOwner(SlotRecoveryInfoPb &info, const std::string &ownerWorker)
{
    for (auto &task : *info.mutable_recovery_tasks()) {
        if (task.owner_worker() == ownerWorker) {
            return &task;
        }
    }
    return nullptr;
}

const RecoveryTaskPb *FindTaskByFailedWorker(const SlotRecoveryInfoPb &info, const std::string &failedWorker)
{
    for (const auto &task : info.recovery_tasks()) {
        if (task.failed_worker() == failedWorker) {
            return &task;
        }
    }
    return nullptr;
}

class SlotRecoveryManagerTestHelper : public SlotRecoveryManager {
public:
    SlotRecoveryManagerTestHelper(const HostPort &localAddress, const std::shared_ptr<SlotRecoveryStore> &store)
        : localAddress_(localAddress), store_(store)
    {
    }

    using SlotRecoveryManager::ExecuteRecoveryTask;
    using SlotRecoveryManager::GetStableActiveWorkers;
    using SlotRecoveryManager::HandleFailedWorkers;
    using SlotRecoveryManager::HandleLocalRestart;
    using SlotRecoveryManager::Init;
    using SlotRecoveryManager::Shutdown;

    Status InitForTest()
    {
        BINEXPECT_CALL(&SlotRecoveryManagerTestHelper::ExecuteRecoveryTask, (_))
            .WillRepeatedly(Invoke([this](const RecoveryTaskPb &task) {
                if (executionHook_) {
                    return executionHook_(task);
                }
                return Status::OK();
            }));
        return SlotRecoveryManager::Init(localAddress_, nullptr, nullptr, nullptr, nullptr);
    }

    void SetActiveWorkers(const std::vector<std::string> &workers)
    {
        activeWorkers_ = workers;
        BINEXPECT_CALL(&SlotRecoveryManagerTestHelper::GetStableActiveWorkers, ()).WillRepeatedly(Invoke([this]() {
            return activeWorkers_;
        }));
    }

    void SetExecutionHook(std::function<Status(const RecoveryTaskPb &)> hook)
    {
        executionHook_ = std::move(hook);
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
};
}  // namespace

class SlotRecoveryEtcdTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 0;
        opts.numWorkers = 0;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        FLAGS_l2_cache_type = "distributed_disk";
        FLAGS_distributed_disk_slot_num = 4;

        FLAGS_etcd_address = cluster_->GetEtcdAddrs();
        LOG(INFO) << "Real ETCD test uses endpoint: " << FLAGS_etcd_address;
        etcdStore_ = std::make_unique<EtcdStore>(FLAGS_etcd_address);
        DS_ASSERT_OK(etcdStore_->Init());
        store_ = std::make_shared<SlotRecoveryStore>(etcdStore_.get());
        DS_ASSERT_OK(store_->Init());
    }

    void TearDown() override
    {
        RELEASE_STUBS
        datasystem::inject::ClearAll();
        CleanupIncident("127.0.0.1:8001");
        CleanupIncident("127.0.0.1:8002");
        CleanupIncident("127.0.0.1:8003");
        ExternalClusterTest::TearDown();
    }

protected:
    void CleanupIncident(const std::string &failedWorker)
    {
        auto rc = store_->DeleteIncident(failedWorker);
        if (rc.IsError()) {
            EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
        }
    }

    SlotRecoveryInfoPb LoadIncidentOrFail(const std::string &failedWorker)
    {
        SlotRecoveryInfoPb info;
        auto rc = store_->GetIncident(failedWorker, info);
        EXPECT_TRUE(rc.IsOk()) << rc.ToString();
        return info;
    }

    void ExpectIncidentDeleted(const std::string &failedWorker)
    {
        ASSERT_TRUE(WaitUntil([&]() {
            SlotRecoveryInfoPb current;
            return store_->GetIncident(failedWorker, current).GetCode() == K_NOT_FOUND;
        }));
    }

    std::unique_ptr<EtcdStore> etcdStore_;
    std::shared_ptr<SlotRecoveryStore> store_;
};

TEST_F(SlotRecoveryEtcdTest, HandlesSingleFailure)
{
    LOG(INFO) << "Scenario: recover a single failed worker through ETCD.";
    const std::string failedWorker = "127.0.0.1:8001";
    const std::string worker2 = "127.0.0.1:8002";
    const std::string worker3 = "127.0.0.1:8003";

    SlotRecoveryManagerTestHelper manager2(HostPort("127.0.0.1", 8002), store_);
    SlotRecoveryManagerTestHelper manager3(HostPort("127.0.0.1", 8003), store_);
    manager2.SetActiveWorkers({ worker2, worker3 });
    manager3.SetActiveWorkers({ worker2, worker3 });
    manager2.SetExecutionHook([](const RecoveryTaskPb &) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        return Status::OK();
    });
    manager3.SetExecutionHook([](const RecoveryTaskPb &) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        return Status::OK();
    });

    // Start two managers so they race on the same ETCD-backed incident.
    DS_ASSERT_OK(manager2.InitForTest());
    DS_ASSERT_OK(manager3.InitForTest());
    DS_ASSERT_OK(manager2.HandleFailedWorkers({ HostPort("127.0.0.1", 8001) }));
    DS_ASSERT_OK(manager3.HandleFailedWorkers({ HostPort("127.0.0.1", 8001) }));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // While both executions are sleeping, the incident must still exist in ETCD and both tasks should already
    // be claimed as IN_PROGRESS.
    auto info = LoadIncidentOrFail(failedWorker);
    ASSERT_EQ(info.recovery_tasks_size(), 2);
    EXPECT_EQ(info.total_slots(), 4);
    EXPECT_EQ(info.completed_slots(), 0);
    EXPECT_EQ(info.failed_slots(), 0);
    size_t inProgressTasks = 0;
    for (const auto &task : info.recovery_tasks()) {
        EXPECT_NE(task.task_status(), RecoveryTaskPb::COMPLETED);
        EXPECT_NE(task.task_status(), RecoveryTaskPb::FAILED);
        if (task.task_status() == RecoveryTaskPb::IN_PROGRESS) {
            ++inProgressTasks;
        }
    }
    EXPECT_GE(inProgressTasks, 1U);

    ASSERT_TRUE(WaitUntil([&]() {
        auto current = LoadIncidentOrFail(failedWorker);
        return current.completed_slots() == 4 && current.failed_slots() == 0;
    }));
    auto current = LoadIncidentOrFail(failedWorker);
    EXPECT_EQ(current.completed_slots(), 4);
    for (const auto &task : current.recovery_tasks()) {
        EXPECT_EQ(task.task_status(), RecoveryTaskPb::COMPLETED);
        EXPECT_EQ(task.source_worker(), task.owner_worker());
    }

    manager2.Shutdown();
    manager3.Shutdown();
}

TEST_F(SlotRecoveryEtcdTest, PreservesOwnerFailoverOrder)
{
    LOG(INFO) << "Scenario: publish successor incident before failing the old owner.";
    const std::string worker1 = "127.0.0.1:8001";
    const std::string worker2 = "127.0.0.1:8002";
    const std::string worker3 = "127.0.0.1:8003";

    // Seed an existing incident where worker2 owns unfinished work for worker1.
    SlotRecoveryInfoPb oldIncident;
    auto *ownerTask = oldIncident.add_recovery_tasks();
    ownerTask->set_failed_worker(worker1);
    ownerTask->set_owner_worker(worker2);
    ownerTask->set_task_status(RecoveryTaskPb::IN_PROGRESS);
    ownerTask->add_slots(0);
    ownerTask->add_slots(2);

    auto *completedTask = oldIncident.add_recovery_tasks();
    completedTask->set_failed_worker(worker1);
    completedTask->set_owner_worker(worker3);
    completedTask->set_task_status(RecoveryTaskPb::COMPLETED);
    completedTask->add_slots(1);
    completedTask->add_slots(3);
    oldIncident.set_total_slots(4);
    oldIncident.set_completed_slots(2);
    oldIncident.set_failed_slots(0);
    DS_ASSERT_OK(store_->UpdateIncident(worker1, oldIncident));

    SlotRecoveryManagerTestHelper manager3(HostPort("127.0.0.1", 8003), store_);
    manager3.SetActiveWorkers({ worker3 });
    DS_ASSERT_OK(manager3.InitForTest());

    // Pause after successor planning so we can inspect the transition state in ETCD.
    DS_ASSERT_OK(datasystem::inject::Set("SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecover", "1*return(K_OK)"));
    DS_ASSERT_OK(datasystem::inject::Set("SlotRecoveryManager.HandleFailedWorkers.AfterPlanIncident", "1*sleep(1000)"));
    std::thread thread([&]() { DS_ASSERT_OK(manager3.HandleFailedWorkers({ HostPort("127.0.0.1", 8002) })); });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // At this point the successor incident has been published, but the old incident has not yet been failed.
    auto successorIncident = LoadIncidentOrFail(worker2);
    ASSERT_EQ(successorIncident.recovery_tasks_size(), 2);
    EXPECT_EQ(successorIncident.total_slots(), 6);
    EXPECT_EQ(successorIncident.completed_slots(), 0);
    EXPECT_EQ(successorIncident.failed_slots(), 0);

    const auto *inheritedTask = FindTaskByFailedWorker(successorIncident, worker1);
    const auto *ownTask = FindTaskByFailedWorker(successorIncident, worker2);
    ASSERT_NE(inheritedTask, nullptr);
    ASSERT_NE(ownTask, nullptr);
    EXPECT_EQ(inheritedTask->owner_worker(), worker3);
    EXPECT_EQ(ownTask->owner_worker(), worker3);
    EXPECT_EQ(inheritedTask->task_status(), RecoveryTaskPb::PENDING);
    EXPECT_EQ(ownTask->task_status(), RecoveryTaskPb::PENDING);

    auto oldBeforeFail = LoadIncidentOrFail(worker1);
    ASSERT_EQ(oldBeforeFail.recovery_tasks_size(), 2);
    auto *ownerTaskBeforeFail = FindTaskByOwner(oldBeforeFail, worker2);
    auto *completedTaskBeforeFail = FindTaskByOwner(oldBeforeFail, worker3);
    ASSERT_NE(ownerTaskBeforeFail, nullptr);
    ASSERT_NE(completedTaskBeforeFail, nullptr);
    EXPECT_EQ(ownerTaskBeforeFail->task_status(), RecoveryTaskPb::IN_PROGRESS);
    EXPECT_EQ(completedTaskBeforeFail->task_status(), RecoveryTaskPb::COMPLETED);

    thread.join();

    ExpectIncidentDeleted(worker1);
    ASSERT_TRUE(WaitUntil([&]() {
        auto current = LoadIncidentOrFail(worker2);
        return current.completed_slots() == 6 && current.failed_slots() == 0;
    }));
    auto retainedWorker2 = LoadIncidentOrFail(worker2);
    EXPECT_EQ(retainedWorker2.completed_slots(), 6);
    EXPECT_EQ(retainedWorker2.failed_slots(), 0);
    const auto *retainedInheritedTask = FindTaskByFailedWorker(retainedWorker2, worker1);
    const auto *retainedOwnTask = FindTaskByFailedWorker(retainedWorker2, worker2);
    ASSERT_NE(retainedInheritedTask, nullptr);
    ASSERT_NE(retainedOwnTask, nullptr);
    EXPECT_EQ(retainedInheritedTask->task_status(), RecoveryTaskPb::COMPLETED);
    EXPECT_EQ(retainedOwnTask->task_status(), RecoveryTaskPb::COMPLETED);
    EXPECT_EQ(retainedInheritedTask->source_worker(), worker3);
    EXPECT_EQ(retainedOwnTask->source_worker(), worker3);

    manager3.Shutdown();
}

TEST_F(SlotRecoveryEtcdTest, TakesOverRestartSlots)
{
    LOG(INFO) << "Scenario: restart takes over pending local slots from another incident.";
    const std::string localWorker = "127.0.0.1:8001";
    const std::string sourceIncident = "127.0.0.1:8002";
    DS_ASSERT_OK(datasystem::inject::Set("SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecover", "1*sleep(1000)"));

    // Seed the local incident so restart has a canonical target to merge into.
    SlotRecoveryInfoPb localInfo;
    auto *localTask = localInfo.add_recovery_tasks();
    localTask->set_failed_worker(localWorker);
    localTask->set_owner_worker(localWorker);
    localTask->set_task_status(RecoveryTaskPb::PENDING);
    localTask->add_slots(0);
    localInfo.set_total_slots(1);
    localInfo.set_completed_slots(0);
    localInfo.set_failed_slots(0);
    DS_ASSERT_OK(store_->UpdateIncident(localWorker, localInfo));

    // Seed a separate source incident that still owns pending work for the local worker.
    SlotRecoveryInfoPb sourceInfo;
    auto *pendingTask = sourceInfo.add_recovery_tasks();
    pendingTask->set_failed_worker(localWorker);
    pendingTask->set_owner_worker("127.0.0.1:8003");
    pendingTask->set_task_status(RecoveryTaskPb::PENDING);
    pendingTask->add_slots(1);
    pendingTask->add_slots(2);
    auto *inProgressTask = sourceInfo.add_recovery_tasks();
    inProgressTask->set_failed_worker(localWorker);
    inProgressTask->set_owner_worker("127.0.0.1:8004");
    inProgressTask->set_task_status(RecoveryTaskPb::IN_PROGRESS);
    inProgressTask->add_slots(3);
    sourceInfo.set_total_slots(4);
    sourceInfo.set_completed_slots(0);
    sourceInfo.set_failed_slots(0);
    DS_ASSERT_OK(store_->UpdateIncident(sourceIncident, sourceInfo));

    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 8001), store_);
    manager.SetActiveWorkers({ localWorker, "127.0.0.1:8003" });
    DS_ASSERT_OK(manager.InitForTest());
    DS_ASSERT_OK(manager.HandleLocalRestart());

    // The source incident should fail only the task that restart took over.
    auto updatedSource = LoadIncidentOrFail(sourceIncident);
    ASSERT_EQ(updatedSource.recovery_tasks_size(), 2);
    EXPECT_EQ(updatedSource.recovery_tasks(0).task_status(), RecoveryTaskPb::FAILED);
    EXPECT_EQ(updatedSource.recovery_tasks(1).task_status(), RecoveryTaskPb::IN_PROGRESS);

    // Wait until the rebuilt local incident visibly claims the inherited slots.
    ASSERT_TRUE(WaitUntil([&]() {
        SlotRecoveryInfoPb current;
        auto rc = store_->GetIncident(localWorker, current);
        if (rc.IsError()) {
            return false;
        }
        bool hasTakenSlotsClaimed = false;
        for (const auto &task : current.recovery_tasks()) {
            if (task.owner_worker() == localWorker && task.task_status() == RecoveryTaskPb::IN_PROGRESS) {
                for (auto slot : task.slots()) {
                    if (slot == 1 || slot == 2) {
                        hasTakenSlotsClaimed = true;
                    }
                }
            }
        }
        return hasTakenSlotsClaimed;
    }));

    ExpectIncidentDeleted(localWorker);
    manager.Shutdown();
}

TEST_F(SlotRecoveryEtcdTest, ResumesLocalInProgressTask)
{
    LOG(INFO) << "Scenario: restart resumes the local in-progress task.";
    const std::string localWorker = "127.0.0.1:8001";

    // Seed the local incident with one resumable task and one already completed peer task.
    SlotRecoveryInfoPb localInfo;
    auto *localTask = localInfo.add_recovery_tasks();
    localTask->set_failed_worker(localWorker);
    localTask->set_owner_worker(localWorker);
    localTask->set_task_status(RecoveryTaskPb::IN_PROGRESS);
    localTask->add_slots(0);
    localTask->add_slots(1);
    auto *completedTask = localInfo.add_recovery_tasks();
    completedTask->set_failed_worker(localWorker);
    completedTask->set_owner_worker("127.0.0.1:8002");
    completedTask->set_task_status(RecoveryTaskPb::COMPLETED);
    completedTask->add_slots(2);
    completedTask->add_slots(3);
    localInfo.set_total_slots(4);
    localInfo.set_completed_slots(2);
    localInfo.set_failed_slots(0);
    DS_ASSERT_OK(store_->UpdateIncident(localWorker, localInfo));

    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 8001), store_);
    manager.SetActiveWorkers({ localWorker, "127.0.0.1:8002" });
    DS_ASSERT_OK(manager.InitForTest());
    DS_ASSERT_OK(manager.HandleLocalRestart());

    // Once the resumed task finishes, the local incident should be cleaned up.
    ExpectIncidentDeleted(localWorker);

    manager.Shutdown();
}

TEST_F(SlotRecoveryEtcdTest, RebuildsLocalIncidentFirst)
{
    LOG(INFO) << "Scenario: restart rebuilds the local incident before deleting a terminal source.";
    const std::string localWorker = "127.0.0.1:8001";
    const std::string sourceIncident = "127.0.0.1:8002";
    DS_ASSERT_OK(datasystem::inject::Set("SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecover", "1*sleep(1000)"));

    // Seed a terminal source incident whose pending task belongs to the restarting worker.
    SlotRecoveryInfoPb sourceInfo;
    auto *pendingTask = sourceInfo.add_recovery_tasks();
    pendingTask->set_failed_worker(localWorker);
    pendingTask->set_owner_worker("127.0.0.1:8003");
    pendingTask->set_task_status(RecoveryTaskPb::PENDING);
    pendingTask->add_slots(1);
    pendingTask->add_slots(2);
    auto *otherCompletedTask = sourceInfo.add_recovery_tasks();
    otherCompletedTask->set_failed_worker(sourceIncident);
    otherCompletedTask->set_owner_worker("127.0.0.1:8003");
    otherCompletedTask->set_task_status(RecoveryTaskPb::COMPLETED);
    otherCompletedTask->add_slots(0);
    sourceInfo.set_total_slots(3);
    sourceInfo.set_completed_slots(1);
    sourceInfo.set_failed_slots(0);
    DS_ASSERT_OK(store_->UpdateIncident(sourceIncident, sourceInfo));

    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 8001), store_);
    manager.SetActiveWorkers({ localWorker, "127.0.0.1:8002" });
    DS_ASSERT_OK(manager.InitForTest());
    DS_ASSERT_OK(manager.HandleLocalRestart());

    // Restart rebuilds the local incident first, then claims the canonical task asynchronously.
    ASSERT_TRUE(WaitUntil([&]() {
        auto localCurrent = SlotRecoveryInfoPb{};
        auto rc = store_->GetIncident(localWorker, localCurrent);
        if (rc.IsError()) {
            return false;
        }
        if (localCurrent.recovery_tasks_size() != 1) {
            return false;
        }
        const auto &task = localCurrent.recovery_tasks(0);
        return task.failed_worker() == localWorker && task.owner_worker() == localWorker
               && task.task_status() == RecoveryTaskPb::IN_PROGRESS
               && CollectSlots(task) == (std::set<uint32_t>{ 0, 1, 2, 3 });
    }));

    auto retainedSource = LoadIncidentOrFail(sourceIncident);
    EXPECT_EQ(retainedSource.completed_slots(), 1);
    EXPECT_EQ(retainedSource.failed_slots(), 2);
    ExpectIncidentDeleted(localWorker);

    manager.Shutdown();
}

TEST_F(SlotRecoveryEtcdTest, CanonicalizesLocalPendingTasks)
{
    LOG(INFO) << "Scenario: restart canonicalizes local pending tasks while preserving foreign progress.";
    const std::string localWorker = "127.0.0.1:8001";
    DS_ASSERT_OK(datasystem::inject::Set("SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecover", "1*sleep(1000)"));

    // Seed a local incident with split ownership so restart must rebuild the canonical local task.
    SlotRecoveryInfoPb localInfo;
    auto *foreignPendingTask = localInfo.add_recovery_tasks();
    foreignPendingTask->set_failed_worker(localWorker);
    foreignPendingTask->set_owner_worker("127.0.0.1:8002");
    foreignPendingTask->set_task_status(RecoveryTaskPb::PENDING);
    foreignPendingTask->add_slots(0);
    foreignPendingTask->add_slots(1);
    auto *localPendingTask = localInfo.add_recovery_tasks();
    localPendingTask->set_failed_worker(localWorker);
    localPendingTask->set_owner_worker(localWorker);
    localPendingTask->set_task_status(RecoveryTaskPb::PENDING);
    localPendingTask->add_slots(2);
    localPendingTask->add_slots(3);
    auto *foreignInProgressTask = localInfo.add_recovery_tasks();
    foreignInProgressTask->set_failed_worker(localWorker);
    foreignInProgressTask->set_owner_worker("127.0.0.1:8003");
    foreignInProgressTask->set_task_status(RecoveryTaskPb::IN_PROGRESS);
    foreignInProgressTask->add_slots(4);
    localInfo.set_total_slots(5);
    localInfo.set_completed_slots(0);
    localInfo.set_failed_slots(0);
    DS_ASSERT_OK(store_->UpdateIncident(localWorker, localInfo));

    SlotRecoveryManagerTestHelper manager(HostPort("127.0.0.1", 8001), store_);
    manager.SetActiveWorkers({ localWorker, "127.0.0.1:8002" });
    DS_ASSERT_OK(manager.InitForTest());
    DS_ASSERT_OK(manager.HandleLocalRestart());

    // The rebuilt canonical local task is claimed asynchronously; wait until the claimed state is visible
    // while the foreign IN_PROGRESS task is still preserved.
    ASSERT_TRUE(WaitUntil([&]() {
        SlotRecoveryInfoPb current;
        auto rc = store_->GetIncident(localWorker, current);
        if (rc.IsError()) {
            return false;
        }
        int localTaskCount = 0;
        bool foreignInProgressKept = false;
        for (const auto &task : current.recovery_tasks()) {
            if (task.failed_worker() == localWorker && task.owner_worker() == localWorker
                && task.task_status() == RecoveryTaskPb::IN_PROGRESS
                && CollectSlots(task) == (std::set<uint32_t>{ 0, 1, 2, 3 })) {
                ++localTaskCount;
            }
            if (task.failed_worker() == localWorker && task.owner_worker() == "127.0.0.1:8003") {
                foreignInProgressKept = task.task_status() == RecoveryTaskPb::IN_PROGRESS
                                        && CollectSlots(task) == (std::set<uint32_t>{ 4 });
            }
        }
        return localTaskCount == 1 && foreignInProgressKept;
    }));

    manager.Shutdown();
}

}  // namespace st
}  // namespace datasystem
