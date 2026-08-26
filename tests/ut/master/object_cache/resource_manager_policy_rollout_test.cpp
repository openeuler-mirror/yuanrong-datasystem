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
 * Description: Test durable eviction-policy rollout coordination.
 */

#include "datasystem/master/resource_manager.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/object_cache/eviction_policy_common.h"
#include "ut/common.h"

DS_DECLARE_bool(enable_memory_rebalance);

namespace datasystem {
namespace ut {
namespace {
class InMemoryRolloutStore {
public:
    ~InMemoryRolloutStore() = default;

    Status Load(std::string &value)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (value_.empty()) {
            return Status(K_NOT_FOUND, "rollout is absent");
        }
        value = value_;
        return Status::OK();
    }

    Status Cas(const master::ResourceManager::StoreProcessFunction &process)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::unique_ptr<std::string> newValue;
        bool retry = true;
        RETURN_IF_NOT_OK(process(value_, newValue, retry));
        if (newValue != nullptr) {
            value_ = *newValue;
        }
        return Status::OK();
    }

    master::EvictionPolicyRolloutPb ReadRollout()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        master::EvictionPolicyRolloutPb rollout;
        EXPECT_TRUE(rollout.ParseFromString(value_));
        return rollout;
    }

private:
    std::mutex mutex_;
    std::string value_;
};

master::ResourceReportReqPb MakeReport(const std::string &address)
{
    master::ResourceReportReqPb req;
    auto *stat = req.mutable_stat();
    stat->set_address(address);
    stat->set_is_ready(true);
    stat->set_eviction_policy(master::EVICTION_POLICY_CLOCK);
    stat->set_eviction_policy_epoch(0);
    stat->set_eviction_policy_update_phase(master::EVICTION_POLICY_STABLE);
    return req;
}

void BindStore(master::ResourceManager &manager, InMemoryRolloutStore &store)
{
    DS_ASSERT_OK(manager.InitEvictionPolicyRolloutStore(
        [&store](std::string &value) { return store.Load(value); },
        [&store](const master::ResourceManager::StoreProcessFunction &process) { return store.Cas(process); }));
}

Status SetPolicyUpdate(master::ResourceManager &manager, master::EvictionPolicyPb targetPolicy, uint64_t epoch,
                       uint32_t migrationBatchSize, uint32_t cohortPercent,
                       master::EvictionPolicyCommandPb command = master::EVICTION_POLICY_COMMIT_CONVERT)
{
    master::EvictionPolicyUpdatePb update;
    update.set_epoch(epoch);
    update.set_target_policy(targetPolicy);
    update.set_migration_batch_size(migrationBatchSize);
    update.set_command(command);
    return manager.SetEvictionPolicyUpdate(update, cohortPercent);
}
}  // namespace

class ResourceManagerPolicyRolloutTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        oldEnableMemoryRebalance_ = FLAGS_enable_memory_rebalance;
        FLAGS_enable_memory_rebalance = false;
    }

    void TearDown() override
    {
        FLAGS_enable_memory_rebalance = oldEnableMemoryRebalance_;
        CommonTest::TearDown();
    }

private:
    bool oldEnableMemoryRebalance_{ false };
};

TEST_F(ResourceManagerPolicyRolloutTest, PersistBeforePublishingAndRecoverAfterRestart)
{
    InMemoryRolloutStore store;
    {
        master::ResourceManager manager;
        BindStore(manager, store);
        DS_ASSERT_OK(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 7, 128, 100));

        master::ResourceReportRspPb rsp;
        DS_ASSERT_OK(manager.ReportResource(MakeReport("127.0.0.1:9001"), rsp));
        ASSERT_TRUE(rsp.has_eviction_policy_update());
        EXPECT_EQ(rsp.eviction_policy_update().epoch(), 7);
        EXPECT_EQ(rsp.eviction_policy_update().target_policy(), master::EVICTION_POLICY_HEAT);
    }

    master::ResourceManager restarted;
    BindStore(restarted, store);
    master::ResourceReportRspPb rsp;
    DS_ASSERT_OK(restarted.ReportResource(MakeReport("127.0.0.1:9002"), rsp));
    ASSERT_TRUE(rsp.has_eviction_policy_update());
    EXPECT_EQ(rsp.eviction_policy_update().epoch(), 7);
}

TEST_F(ResourceManagerPolicyRolloutTest, IndependentMasterRefreshesSharedIntent)
{
    InMemoryRolloutStore store;
    master::ResourceManager firstMaster;
    master::ResourceManager secondMaster;
    BindStore(firstMaster, store);
    BindStore(secondMaster, store);
    DS_ASSERT_OK(SetPolicyUpdate(firstMaster, master::EVICTION_POLICY_HEAT, 9, 64, 100));
    DS_ASSERT_OK(secondMaster.RefreshEvictionPolicyRolloutForTest());

    master::ResourceReportRspPb rsp;
    DS_ASSERT_OK(secondMaster.ReportResource(MakeReport("127.0.0.1:9009"), rsp));
    ASSERT_TRUE(rsp.has_eviction_policy_update());
    EXPECT_EQ(rsp.eviction_policy_update().epoch(), 9);
}

TEST_F(ResourceManagerPolicyRolloutTest, SameEpochOnlyAllowsIdempotencyOrCohortExpansion)
{
    InMemoryRolloutStore store;
    master::ResourceManager manager;
    BindStore(manager, store);
    DS_ASSERT_OK(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 11, 64, 10));
    DS_ASSERT_OK(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 11, 64, 50));

    auto rollout = store.ReadRollout();
    EXPECT_EQ(rollout.update().epoch(), 11);
    EXPECT_EQ(rollout.cohort_percent(), 50);
    EXPECT_EQ(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 11, 64, 25).GetCode(), K_INVALID);
    EXPECT_EQ(SetPolicyUpdate(manager, master::EVICTION_POLICY_CLOCK, 11, 64, 50).GetCode(), K_INVALID);
    EXPECT_EQ(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 10, 64, 100).GetCode(), K_INVALID);
    EXPECT_EQ(
        SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 12, EVICTION_POLICY_MAX_MIGRATION_BATCH_SIZE + 1, 100)
            .GetCode(),
        K_INVALID);
}

TEST_F(ResourceManagerPolicyRolloutTest, RejectsUnspecifiedControlEnums)
{
    InMemoryRolloutStore store;
    master::ResourceManager manager;
    BindStore(manager, store);

    master::EvictionPolicyUpdatePb update;
    update.set_epoch(1);
    update.set_migration_batch_size(1);
    update.set_command(master::EVICTION_POLICY_COMMIT_CONVERT);
    EXPECT_EQ(manager.SetEvictionPolicyUpdate(update, 100).GetCode(), K_INVALID);

    update.set_target_policy(master::EVICTION_POLICY_HEAT);
    update.set_command(master::EVICTION_POLICY_COMMAND_UNSPECIFIED);
    EXPECT_EQ(manager.SetEvictionPolicyUpdate(update, 100).GetCode(), K_INVALID);
}

TEST_F(ResourceManagerPolicyRolloutTest, PrecheckAcknowledgementIsObservableBeforeCommit)
{
    InMemoryRolloutStore store;
    master::ResourceManager manager;
    BindStore(manager, store);
    DS_ASSERT_OK(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 12, 64, 100, master::EVICTION_POLICY_PRECHECK));

    auto report = MakeReport("127.0.0.1:9012");
    master::ResourceReportRspPb firstRsp;
    DS_ASSERT_OK(manager.ReportResource(report, firstRsp));
    ASSERT_TRUE(firstRsp.has_eviction_policy_update());
    EXPECT_EQ(firstRsp.eviction_policy_update().command(), master::EVICTION_POLICY_PRECHECK);

    report.mutable_stat()->set_eviction_policy_control_epoch(12);
    report.mutable_stat()->set_eviction_policy_worker_status(master::EVICTION_POLICY_WORKER_READY);
    report.mutable_stat()->set_eviction_policy_total_objects(1'000);
    master::ResourceReportRspPb readyRsp;
    DS_ASSERT_OK(manager.ReportResource(report, readyRsp));
    EXPECT_FALSE(readyRsp.has_eviction_policy_update());

    master::GetEvictionPolicyUpdateProgressRspPb progress;
    DS_ASSERT_OK(manager.GetEvictionPolicyUpdateProgress(12, progress));
    EXPECT_EQ(progress.selected_workers(), 1);
    EXPECT_EQ(progress.ready_workers(), 1);
    ASSERT_EQ(progress.workers_size(), 1);
    EXPECT_EQ(progress.workers(0).total_objects(), 1'000);

    DS_ASSERT_OK(
        SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 12, 64, 100, master::EVICTION_POLICY_COMMIT_CONVERT));
    master::ResourceReportRspPb commitRsp;
    DS_ASSERT_OK(manager.ReportResource(report, commitRsp));
    ASSERT_TRUE(commitRsp.has_eviction_policy_update());
    EXPECT_EQ(commitRsp.eviction_policy_update().command(), master::EVICTION_POLICY_COMMIT_CONVERT);
    ASSERT_EQ(commitRsp.stats_size(), 1);
    EXPECT_FALSE(commitRsp.stats(0).is_ready());

    report.mutable_stat()->set_eviction_policy(master::EVICTION_POLICY_HEAT);
    report.mutable_stat()->set_eviction_policy_epoch(12);
    report.mutable_stat()->set_eviction_policy_update_phase(master::EVICTION_POLICY_STABLE);
    report.mutable_stat()->set_eviction_policy_worker_status(master::EVICTION_POLICY_WORKER_ACTIVE);
    report.mutable_stat()->set_eviction_policy_migrated_objects(1'000);
    master::ResourceReportRspPb activeRsp;
    DS_ASSERT_OK(manager.ReportResource(report, activeRsp));
    EXPECT_FALSE(activeRsp.has_eviction_policy_update());
    progress.Clear();
    DS_ASSERT_OK(manager.GetEvictionPolicyUpdateProgress(12, progress));
    EXPECT_EQ(progress.active_workers(), 1);
    EXPECT_EQ(progress.workers(0).migrated_objects(), 1'000);

    EXPECT_EQ(
        SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 12, 64, 100, master::EVICTION_POLICY_PRECHECK).GetCode(),
        K_INVALID);
}

TEST_F(ResourceManagerPolicyRolloutTest, StaleFailureIsNotCountedForNewEpoch)
{
    InMemoryRolloutStore store;
    master::ResourceManager manager;
    BindStore(manager, store);
    constexpr uint64_t staleEpoch = 20;
    constexpr uint64_t currentEpoch = 21;
    DS_ASSERT_OK(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, currentEpoch, 64, 100,
                                 master::EVICTION_POLICY_PRECHECK));

    auto report = MakeReport("127.0.0.1:9021");
    report.mutable_stat()->set_eviction_policy_control_epoch(staleEpoch);
    report.mutable_stat()->set_eviction_policy_worker_status(master::EVICTION_POLICY_WORKER_FAILED);
    report.mutable_stat()->set_eviction_policy_failure_code(K_NO_SPACE);
    report.mutable_stat()->set_eviction_policy_failure_reason("stale admission failure");
    master::ResourceReportRspPb rsp;
    DS_ASSERT_OK(manager.ReportResource(report, rsp));
    ASSERT_TRUE(rsp.has_eviction_policy_update());
    EXPECT_EQ(rsp.eviction_policy_update().epoch(), currentEpoch);

    master::GetEvictionPolicyUpdateProgressRspPb progress;
    DS_ASSERT_OK(manager.GetEvictionPolicyUpdateProgress(currentEpoch, progress));
    EXPECT_EQ(progress.selected_workers(), 1);
    EXPECT_EQ(progress.failed_workers(), 0);
    EXPECT_EQ(progress.ready_workers(), 0);
    ASSERT_EQ(progress.workers_size(), 1);
    EXPECT_EQ(progress.workers(0).epoch(), staleEpoch);
    EXPECT_EQ(progress.workers(0).status(), master::EVICTION_POLICY_WORKER_FAILED);

    report.mutable_stat()->set_eviction_policy_control_epoch(currentEpoch);
    report.mutable_stat()->set_eviction_policy_worker_status(master::EVICTION_POLICY_WORKER_READY);
    report.mutable_stat()->clear_eviction_policy_failure_code();
    report.mutable_stat()->clear_eviction_policy_failure_reason();
    rsp.Clear();
    DS_ASSERT_OK(manager.ReportResource(report, rsp));
    progress.Clear();
    DS_ASSERT_OK(manager.GetEvictionPolicyUpdateProgress(currentEpoch, progress));
    EXPECT_EQ(progress.ready_workers(), 1);
    EXPECT_EQ(progress.failed_workers(), 0);
}

TEST_F(ResourceManagerPolicyRolloutTest, FailedStoreCommitDoesNotPublishIntent)
{
    master::ResourceManager manager;
    DS_ASSERT_OK(manager.InitEvictionPolicyRolloutStore(
        [](std::string &) { return Status(K_NOT_FOUND, "rollout is absent"); },
        [](const master::ResourceManager::StoreProcessFunction &) {
            return Status(K_KVSTORE_ERROR, "injected store failure");
        }));

    EXPECT_EQ(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 3, 32, 100).GetCode(), K_KVSTORE_ERROR);
    master::ResourceReportRspPb rsp;
    DS_ASSERT_OK(manager.ReportResource(MakeReport("127.0.0.1:9003"), rsp));
    EXPECT_FALSE(rsp.has_eviction_policy_update());
}

TEST_F(ResourceManagerPolicyRolloutTest, LegacyWorkerWithoutPolicyCapabilityIsNotSelected)
{
    InMemoryRolloutStore store;
    master::ResourceManager manager;
    BindStore(manager, store);
    DS_ASSERT_OK(SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, 3, 32, 100));

    auto legacyReport = MakeReport("127.0.0.1:9004");
    legacyReport.mutable_stat()->clear_eviction_policy();
    master::ResourceReportRspPb rsp;
    DS_ASSERT_OK(manager.ReportResource(legacyReport, rsp));

    EXPECT_FALSE(rsp.has_eviction_policy_update());
    ASSERT_GT(rsp.stats_size(), 0);
    EXPECT_TRUE(rsp.stats(0).is_ready());
}

TEST_F(ResourceManagerPolicyRolloutTest, ConcurrentReportsAndEpochAdvanceRemainCoherent)
{
    InMemoryRolloutStore store;
    master::ResourceManager manager;
    BindStore(manager, store);
    std::atomic<bool> start{ false };
    std::atomic<uint64_t> failures{ 0 };
    auto waitForStart = [&start]() {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    };

    std::vector<std::thread> reporters;
    for (uint64_t worker = 0; worker < 4; ++worker) {
        reporters.emplace_back([&, worker]() {
            waitForStart();
            auto report = MakeReport("127.0.0.1:" + std::to_string(9'100 + worker));
            for (uint64_t i = 0; i < 500; ++i) {
                master::ResourceReportRspPb rsp;
                if (manager.ReportResource(report, rsp).IsError()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    std::thread updater([&]() {
        waitForStart();
        for (uint64_t epoch = 1; epoch <= 100; ++epoch) {
            if (SetPolicyUpdate(manager, master::EVICTION_POLICY_HEAT, epoch, 32, 100).IsError()) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
            master::GetEvictionPolicyUpdateProgressRspPb progress;
            if (manager.GetEvictionPolicyUpdateProgress(0, progress).IsError()
                || progress.rollout().update().epoch() != epoch) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
    start.store(true, std::memory_order_release);
    for (auto &reporter : reporters) {
        reporter.join();
    }
    updater.join();
    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0u);
}
}  // namespace ut
}  // namespace datasystem
