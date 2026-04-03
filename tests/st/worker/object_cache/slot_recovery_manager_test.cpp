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
DS_DECLARE_uint32(l2_cache_slot_num);
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

    using SlotRecoveryManager::GetStableActiveWorkers;
    using SlotRecoveryManager::HandleFailedWorkers;
    using SlotRecoveryManager::Init;
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

private:
    std::shared_ptr<SlotRecoveryStore> CreateStore(datasystem::EtcdStore *etcdStore) const override
    {
        (void)etcdStore;
        return store_;
    }

    HostPort localAddress_;
    std::shared_ptr<SlotRecoveryStore> store_;
    std::vector<std::string> activeWorkers_;
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
        FLAGS_l2_cache_slot_num = 4;

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
    DS_ASSERT_OK(datasystem::inject::Set("SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecover", "2*sleep(1000)"));

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
    EXPECT_EQ(info.recovery_tasks(0).task_status(), RecoveryTaskPb::IN_PROGRESS);
    EXPECT_EQ(info.recovery_tasks(1).task_status(), RecoveryTaskPb::IN_PROGRESS);

    ExpectIncidentDeleted(failedWorker);

    auto rc = store_->GetIncident(failedWorker, info);
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);

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
    ExpectIncidentDeleted(worker2);

    manager3.Shutdown();
}

}  // namespace st
}  // namespace datasystem
