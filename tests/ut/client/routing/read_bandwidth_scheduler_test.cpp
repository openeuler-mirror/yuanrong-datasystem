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
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <gtest/gtest.h>
#include "datasystem/client/object_cache/routing/client_read_bandwidth_scheduler.h"
#include "datasystem/client/object_cache/routing/ub_routing_health.h"
#include "datasystem/client/object_cache/routing/i_worker_filter.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace ut {
namespace {

using client::IWorkerFilter;
using client::UbRoutingHealthSnapshot;
using client::ClientReadBandwidthScheduler;

// ===========================================================================
// Shared helpers
// ===========================================================================
void SetHealth(UbPortHealthSummary &summary, uint32_t total, uint32_t failed) {
    summary.valid = true;
    summary.healthEpoch = UB_PORT_HEALTH_FIRST_EPOCH;
    summary.verificationPending = false;
    summary.totalPortCount = total;
    summary.badPortCount = failed;
}

void PopulateHealth(const std::vector<HostPort> &workers, UbRoutingHealthSnapshot &snapshot,
                    uint32_t totalPorts = 4, uint32_t failedPorts = 0) {
    for (const HostPort &worker : workers) {
        SetHealth(snapshot.workers[worker].portHealth, totalPorts, failedPorts);
    }
}

class AllowAllFilter final : public IWorkerFilter {
public:
    bool IsAvailable(const HostPort &, client::WorkerAccessAction action) const override
    {
        (void)action;
        return true;
    }
};

class TrackingFilter final : public IWorkerFilter {
public:
    explicit TrackingFilter(bool available) : available_(available) {}
    bool IsAvailable(const HostPort &, client::WorkerAccessAction action) const override
    {
        (void)action;
        calls_.fetch_add(1, std::memory_order_relaxed);
        return available_;
    }
    uint32_t Calls() const noexcept
    {
        return calls_.load(std::memory_order_relaxed);
    }
private:
    bool available_;
    mutable std::atomic<uint32_t> calls_{0};
};

// ---------------------------------------------------------------------------
// Keep time-based eligibility changes out of ordinary selection tests.
// ---------------------------------------------------------------------------
ClientReadBandwidthScheduler::Config StableConfig() {
    ClientReadBandwidthScheduler::Config config;
    config.enabled = true;
    config.clientSalt = "client-scheduler-test";
    config.latencyCandidateRefreshMs = 60000;
    config.latencyClientStaleMs = 60000;
    config.latencyWorkerRecycleCheckMs = 60000;
    config.latencyWorkerInactiveRecycleMs = 60000;
    config.latencyClientTableSize = 256;
    config.latencyStarvationProtectMs = 60000;
    return config;
}

ClientReadBandwidthScheduler::Config StableWeightedConfig() {
    ClientReadBandwidthScheduler::Config config = StableConfig();
    config.latencyNonAffinityPenaltyUs = 0;
    return config;
}

// Reference latency = 1 us → very small absolute weights.
ClientReadBandwidthScheduler::Config LowWeightConfig() {
    ClientReadBandwidthScheduler::Config config = StableWeightedConfig();
    config.latencyWeightReferenceUs = 1;
    return config;
}

// Reference latency = 100 s gives acceptance probability close to one.
ClientReadBandwidthScheduler::Config HighWeightConfig() {
    ClientReadBandwidthScheduler::Config config = StableWeightedConfig();
    config.latencyWeightReferenceUs = 100000000;
    return config;
}

}  // namespace

// ===========================================================================
// Part A — Fallback semantics
// ===========================================================================
class ClientReadBandwidthSchedulerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        scheduler_ = std::make_unique<ClientReadBandwidthScheduler>(StableConfig());
        health_ = std::make_shared<UbRoutingHealthSnapshot>();
        taskId_ = 0;
    }

    void TearDown() override
    {
        scheduler_.reset();
    }

    void ObserveWorker(const HostPort &worker, uint32_t p50Ns, uint32_t p99Ns, uint64_t version = 1)
    {
        scheduler_->Observe(worker, p50Ns, p99Ns, version, "test");
    }

    void RefreshWith(const std::vector<HostPort> &workers)
    {
        scheduler_->RefreshCandidates(workers);
    }

    void RegisterWorkers(const std::vector<HostPort> &workers,
                         uint32_t p50Ns = 1000000, uint32_t p99Ns = 1200000)
    {
        for (const HostPort &w : workers) {
            ObserveWorker(w, p50Ns, p99Ns, 1);
        }
        RefreshWith(workers);
    }

    std::unique_ptr<ClientReadBandwidthScheduler> scheduler_;
    std::shared_ptr<UbRoutingHealthSnapshot> health_;
    uint64_t taskId_{0};
    const HostPort preferred_{"10.0.0.1", 1000};
    const HostPort candidate_{"10.0.0.2", 1000};
    const HostPort candidate2_{"10.0.0.3", 1000};
};

// --- 1. Fallback contract -------------------------------------------------
TEST_F(ClientReadBandwidthSchedulerTest, FallbackReturnsPreferredWorkerWithTrue) {
    const std::vector<HostPort> workers{preferred_, candidate_};
    RegisterWorkers(workers);
    PopulateHealth(workers, *health_, 2, 2);
    HostPort selected;
    auto filters = std::make_shared<AllowAllFilter>();
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {preferred_}, preferred_, {filters}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
}

TEST_F(ClientReadBandwidthSchedulerTest, FallbackWithEmptyCandidateTable) {
    HostPort selected;
    auto filters = std::make_shared<AllowAllFilter>();
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {}, preferred_, {filters}, nullptr, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
}

TEST_F(ClientReadBandwidthSchedulerTest, FallbackWithFiltersAllRejecting) {
    const std::vector<HostPort> workers{preferred_, candidate_};
    RegisterWorkers(workers);
    PopulateHealth(workers, *health_, 4, 0);
    HostPort selected;
    auto rejecting = std::make_shared<TrackingFilter>(false);
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {}, preferred_, {rejecting}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
    EXPECT_LE(rejecting->Calls(), 4U);
}

// --- 2. Exclude + fallback -------------------------------------------------
TEST_F(ClientReadBandwidthSchedulerTest, FallbackBypassesExcludeForPreferredWorker) {
    const std::vector<HostPort> workers{preferred_, candidate_};
    RegisterWorkers(workers);
    PopulateHealth(workers, *health_, 2, 2);
    HostPort selected;
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {preferred_}, preferred_, {}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
}

TEST_F(ClientReadBandwidthSchedulerTest, ExcludeAppliedDuringWeightedSelection) {
    const std::vector<HostPort> workers{preferred_, candidate_};
    RegisterWorkers(workers);
    PopulateHealth(workers, *health_, 4, 0);
    HostPort selected;
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {preferred_}, preferred_, {}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, candidate_);
}

TEST_F(ClientReadBandwidthSchedulerTest, ExcludeAppliesAfterPickWeightedCandidate) {
    const std::vector<HostPort> workers{preferred_, candidate_};
    RegisterWorkers(workers);
    PopulateHealth(workers, *health_, 4, 0);
    HostPort selected;
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {preferred_, candidate_}, preferred_, {}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
}

// --- 4. Disabled / edge ----------------------------------------------------
TEST_F(ClientReadBandwidthSchedulerTest, ReturnsFalseWhenDisabled) {
    ClientReadBandwidthScheduler::Config disabledConfig;
    disabledConfig.enabled = false;
    ClientReadBandwidthScheduler disabledScheduler(disabledConfig);
    HostPort selected;
    uint64_t tid = 0;
    EXPECT_FALSE(disabledScheduler.SelectWorkerFast("key", {}, HostPort("a", 1), {}, nullptr, tid, selected, client::WorkerAccessAction::CONTROL));
}

TEST_F(ClientReadBandwidthSchedulerTest, FallsBackWhenNoStateExistsForPreferred) {
    RegisterWorkers({candidate_});
    PopulateHealth({candidate_}, *health_, 4, 0);
    HostPort selected;
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {candidate_}, preferred_, {}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
}

// --- 5. Weighted path success cases ---------------------------------------
TEST_F(ClientReadBandwidthSchedulerTest, WeightedPathSelectsHealthyCandidate) {
    const std::vector<HostPort> workers{preferred_, candidate_};
    RegisterWorkers(workers);
    PopulateHealth(workers, *health_, 4, 0);
    HostPort selected;
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {}, candidate2_, {}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_TRUE(selected == candidate_ || selected == preferred_);
}

TEST_F(ClientReadBandwidthSchedulerTest, WeightedPathRejectsCandidateAboveCutInGuard) {
    const std::vector<HostPort> workers{preferred_, candidate_};
    ObserveWorker(preferred_, 20000000, 25000000, 1);
    ObserveWorker(candidate_, 1000000, 1200000, 1);
    RefreshWith(workers);
    PopulateHealth(workers, *health_, 4, 0);
    HostPort selected;
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {}, preferred_, {}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, candidate_);
}

TEST_F(ClientReadBandwidthSchedulerTest, WeightedPathRejectsAllPortsFailed) {
    const std::vector<HostPort> workers{preferred_, candidate_};
    RegisterWorkers(workers);
    PopulateHealth(workers, *health_, 3, 3);
    HostPort selected;
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {}, preferred_, {}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
}

TEST_F(ClientReadBandwidthSchedulerTest, RefreshedCandidateTableAfterRefresh) {
    auto config = HighWeightConfig();
    config.latencyCandidateRefreshMs = 1;
    scheduler_ = std::make_unique<ClientReadBandwidthScheduler>(config);
    RegisterWorkers({preferred_, candidate_});
    const std::vector<HostPort> newSet{preferred_};
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    RefreshWith(newSet);
    PopulateHealth(newSet, *health_, 4, 0);
    HostPort selected;
    EXPECT_TRUE(scheduler_->SelectWorkerFast("key", {}, candidate2_, {}, health_, taskId_, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
    ClientReadBandwidthScheduler::WorkerStatus removed;
    scheduler_->GetWorkerStatus(candidate_, health_, removed);
    EXPECT_FALSE(removed.currentCandidate);
}

TEST_F(ClientReadBandwidthSchedulerTest, ConcurrentRefreshAndSelectionReturnsKnownWorker) {
    auto config = HighWeightConfig();
    config.latencyCandidateRefreshMs = 1;
    scheduler_ = std::make_unique<ClientReadBandwidthScheduler>(config);
    std::vector<HostPort> workers;
    for (int index = 0; index < 30; ++index) {
        workers.emplace_back("10.3.0." + std::to_string(index + 1), 1000);
    }
    for (const HostPort &w : workers) {
        ObserveWorker(w, 1000000, 1200000, 1);
    }
    RefreshWith(workers);
    const std::vector<HostPort> first(workers.begin(), workers.begin() + 20);
    const std::vector<HostPort> second(workers.begin() + 10, workers.end());
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth(workers, *health);
    auto filter = std::make_shared<AllowAllFilter>();
    std::atomic<bool> start{false};
    std::atomic<uint64_t> totalCalls{0};
    std::atomic<uint64_t> candidateSelections{0};
    std::vector<std::thread> threads;
    for (int ti = 0; ti < 4; ++ti) {
        threads.emplace_back([&, ti] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (uint64_t v = 2; v < 100; ++v) {
                const size_t idx = (v + ti) % workers.size();
                HostPort selected;
                bool ok = scheduler_->SelectWorkerFast("concurrent", {workers.front()}, candidate2_,
                                                       {filter}, health, v, selected, client::WorkerAccessAction::CONTROL);
                totalCalls.fetch_add(1, std::memory_order_relaxed);
                EXPECT_TRUE(ok);
                const bool isCandidate = std::find(workers.begin(), workers.end(), selected) != workers.end();
                EXPECT_TRUE(isCandidate || selected == candidate2_);
                if (isCandidate) {
                    candidateSelections.fetch_add(1, std::memory_order_relaxed);
                }
                EXPECT_NE(selected, workers.front());
                if (v % 7 == 0) {
                    scheduler_->Observe(workers[(idx + 1) % workers.size()], 1000000, 1200000, v, "concurrent");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }
    threads.emplace_back([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int i = 0; i < 20; ++i) {
            scheduler_->RefreshCandidates(i % 2 == 0 ? first : second);
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    });
    start.store(true, std::memory_order_release);
    for (std::thread &t : threads) {
        t.join();
    }
    EXPECT_EQ(totalCalls.load(std::memory_order_relaxed), 4U * 98U);
    EXPECT_GT(candidateSelections.load(std::memory_order_relaxed), 0U);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    RefreshWith(second);
    ClientReadBandwidthScheduler::WorkerStatus removed;
    scheduler_->GetWorkerStatus(workers.front(), health, removed);
    EXPECT_FALSE(removed.currentCandidate);
}

// --- 8. GetWorkerStatus ----------------------------------------------------
TEST_F(ClientReadBandwidthSchedulerTest, GetWorkerStatusReturnsZeroWeightForAllPortsFailed) {
    RegisterWorkers({candidate_});
    PopulateHealth({candidate_}, *health_, 2, 2);
    ClientReadBandwidthScheduler::WorkerStatus status;
    scheduler_->GetWorkerStatus(candidate_, health_, status);
    EXPECT_TRUE(status.exists);
    EXPECT_EQ(status.affinityWeight, 0U);
    EXPECT_EQ(status.nonAffinityWeight, 0U);
    EXPECT_TRUE(status.currentCandidate);
}

TEST_F(ClientReadBandwidthSchedulerTest, GetWorkerStatusReflectsHealthyWeight) {
    RegisterWorkers({candidate_});
    PopulateHealth({candidate_}, *health_, 4, 0);
    ClientReadBandwidthScheduler::WorkerStatus status;
    scheduler_->GetWorkerStatus(candidate_, health_, status);
    EXPECT_TRUE(status.exists);
    EXPECT_GT(status.affinityWeight, 0U);
    EXPECT_GT(status.nonAffinityWeight, 0U);
    EXPECT_TRUE(status.currentCandidate);
}

// ===========================================================================
// Part B — Weighted-selection behaviour
// ===========================================================================
class WeightedSelectionTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        taskId_ = 0;
    }

    void ObserveWorker(ClientReadBandwidthScheduler &scheduler,
                       const HostPort &worker, uint32_t p50Ns, uint32_t p99Ns,
                       uint64_t version = 1)
    {
        scheduler.Observe(worker, p50Ns, p99Ns, version, "test");
    }

    void RegisterWorkers(ClientReadBandwidthScheduler &scheduler,
                         const std::vector<HostPort> &workers,
                         uint32_t p50Ns = 1000000, uint32_t p99Ns = 1200000)
    {
        for (const HostPort &w : workers) {
            ObserveWorker(scheduler, w, p50Ns, p99Ns, 1);
        }
        scheduler.RefreshCandidates(workers);
    }

    uint64_t taskId_{0};
    const HostPort preferred_{"10.1.0.1", 1000};
    const HostPort candidate_{"10.1.0.2", 1000};
};

// --- 1. Deterministic weight via GetWorkerStatus --------------------------
TEST_F(WeightedSelectionTest, ComputeWeightForLowLatency) {
    ClientReadBandwidthScheduler scheduler(StableWeightedConfig());
    RegisterWorkers(scheduler, {candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    ClientReadBandwidthScheduler::WorkerStatus status;
    EXPECT_TRUE(scheduler.GetWorkerStatus(candidate_, health, status));
    EXPECT_TRUE(status.exists);
    EXPECT_GT(status.affinityWeight, 0U);
    EXPECT_GT(status.nonAffinityWeight, 0U);
}

TEST_F(WeightedSelectionTest, ComputeWeightReturnsZeroForAllPortsFailed) {
    ClientReadBandwidthScheduler scheduler(StableWeightedConfig());
    RegisterWorkers(scheduler, {candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 2, 2);
    ClientReadBandwidthScheduler::WorkerStatus status;
    EXPECT_TRUE(scheduler.GetWorkerStatus(candidate_, health, status));
    EXPECT_TRUE(status.exists);
    EXPECT_EQ(status.affinityWeight, 0U);
    EXPECT_EQ(status.nonAffinityWeight, 0U);
}

TEST_F(WeightedSelectionTest, ComputeWeightZeroForCutInGuardExceeded) {
    ClientReadBandwidthScheduler scheduler(StableWeightedConfig());
    scheduler.SetCutInGuardNs(12);
    ObserveWorker(scheduler, candidate_, 8000000, 8000000, 1);
    scheduler.RefreshCandidates({candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    ClientReadBandwidthScheduler::WorkerStatus status;
    EXPECT_TRUE(scheduler.GetWorkerStatus(candidate_, health, status));
    EXPECT_TRUE(status.exists);
    EXPECT_EQ(status.affinityWeight, 0U);
}

TEST_F(WeightedSelectionTest, LowPositiveWeightFromGetWorkerStatus) {
    ClientReadBandwidthScheduler scheduler(LowWeightConfig());
    ObserveWorker(scheduler, candidate_, 11000000, 11000000, 1);
    scheduler.RefreshCandidates({candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    ClientReadBandwidthScheduler::WorkerStatus status;
    EXPECT_TRUE(scheduler.GetWorkerStatus(candidate_, health, status));
    EXPECT_TRUE(status.exists);
    EXPECT_GE(status.affinityWeight, 85U);
    EXPECT_LE(status.affinityWeight, 95U);
}

TEST_F(WeightedSelectionTest, HighLatencyProducesLowWeight) {
    ClientReadBandwidthScheduler scheduler(LowWeightConfig());
    scheduler.SetCutInGuardNs(1000);
    ObserveWorker(scheduler, candidate_, 100000000, 120000000, 1);
    scheduler.RefreshCandidates({candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    ClientReadBandwidthScheduler::WorkerStatus status;
    EXPECT_TRUE(scheduler.GetWorkerStatus(candidate_, health, status));
    EXPECT_TRUE(status.exists);
    EXPECT_GE(status.affinityWeight, 1U);
    EXPECT_LT(status.affinityWeight, 10U);
}

// --- 2. Statistical selection ----------------------------------------------
TEST_F(WeightedSelectionTest, LowWeightCanStillBeSelectedInMultipleTries) {
    ClientReadBandwidthScheduler scheduler(LowWeightConfig());
    ObserveWorker(scheduler, candidate_, 11000000, 11000000, 1);
    scheduler.RefreshCandidates({candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    uint32_t successes = 0;
    constexpr uint32_t kTrials = 10000;
    for (uint32_t i = 0; i < kTrials; ++i) {
        HostPort selected;
        std::string key = "trial-" + std::to_string(i);
        ASSERT_TRUE(scheduler.SelectWorkerFast(key, {}, preferred_, {}, health, taskId_++, selected, client::WorkerAccessAction::CONTROL));
        ASSERT_TRUE(selected == candidate_ || selected == preferred_);
        if (selected == candidate_) {
            ++successes;
        }
    }
    // With 100 draws per call and p = 1 / 11001 per draw, expect about 90 hits.
    EXPECT_GT(successes, 0U);
    EXPECT_LT(successes, 200U);
}

TEST_F(WeightedSelectionTest, HighWeightSelectsCandidate) {
    ClientReadBandwidthScheduler scheduler(HighWeightConfig());
    ObserveWorker(scheduler, candidate_, 1000000, 1200000, 1);
    scheduler.RefreshCandidates({candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    HostPort selected;
    ASSERT_TRUE(scheduler.SelectWorkerFast("high-weight-key", {}, preferred_, {}, health, taskId_++, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_TRUE(selected == candidate_ || selected == preferred_);
}

TEST_F(WeightedSelectionTest, ZeroWeightNeverSelected) {
    ClientReadBandwidthScheduler scheduler(StableWeightedConfig());
    RegisterWorkers(scheduler, {candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 2, 2);
    HostPort selected;
    EXPECT_TRUE(scheduler.SelectWorkerFast("zero-weight", {}, preferred_, {}, health, taskId_++, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
}

// --- 3. Acceptance boundary ------------------------------------------------
TEST_F(WeightedSelectionTest, NearMaximumWeightSelectsCandidate) {
    ClientReadBandwidthScheduler::Config config = StableWeightedConfig();
    config.latencyWeightReferenceUs = 100000000;
    ClientReadBandwidthScheduler scheduler(config);
    ObserveWorker(scheduler, candidate_, 100, 200, 1);
    scheduler.RefreshCandidates({candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    HostPort selected;
    ASSERT_TRUE(scheduler.SelectWorkerFast("max-weight", {}, preferred_, {}, health, taskId_++, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_TRUE(selected == candidate_ || selected == preferred_);
}

// --- 4. Starvation override -----------------------------------------------
TEST_F(WeightedSelectionTest, StarvationDoesNotBypassAllPortsFailed) {
    ClientReadBandwidthScheduler::Config config = StableWeightedConfig();
    config.latencyStarvationProtectMs = 0;
    ClientReadBandwidthScheduler scheduler(config);
    RegisterWorkers(scheduler, {candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 2, 2);
    HostPort selected;
    EXPECT_TRUE(scheduler.SelectWorkerFast("starve-key", {}, preferred_, {}, health, taskId_++, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
}

TEST_F(WeightedSelectionTest, StarvationSelectsHealthyLowWeightCandidate) {
    ClientReadBandwidthScheduler::Config config = LowWeightConfig();
    config.latencyStarvationProtectMs = 0;
    ClientReadBandwidthScheduler scheduler(config);
    RegisterWorkers(scheduler, {candidate_}, 11000000, 11000000);
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    for (uint32_t i = 0; i < 32; ++i) {
        HostPort selected;
        ASSERT_TRUE(scheduler.SelectWorkerFast("starved", {}, preferred_, {}, health, taskId_++, selected, client::WorkerAccessAction::CONTROL));
        EXPECT_EQ(selected, candidate_);
    }
}

// --- 5. Filter + weighted path --------------------------------------------
TEST_F(WeightedSelectionTest, FilterRejectionFallsBackToSeparatePreferredWorker) {
    ClientReadBandwidthScheduler scheduler(HighWeightConfig());
    const HostPort healthy("10.2.0.1", 1000);
    const HostPort dead("10.2.0.2", 1000);
    RegisterWorkers(scheduler, {healthy, dead});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({healthy}, *health, 4, 0);
    PopulateHealth({dead}, *health, 2, 2);
    auto filter = std::make_shared<TrackingFilter>(false);
    HostPort selected;
    EXPECT_TRUE(scheduler.SelectWorkerFast("filter-healthy", {},
                                          preferred_, {filter}, health, taskId_++, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(selected, preferred_);
    EXPECT_EQ(filter->Calls(), 1U);
}

// --- 6. NonAffinity penalty -----------------------------------------------
TEST_F(WeightedSelectionTest, NonAffinityWeightIsLowerThanAffinityWeight) {
    ClientReadBandwidthScheduler::Config config = StableWeightedConfig();
    config.latencyNonAffinityPenaltyUs = 200;
    ClientReadBandwidthScheduler scheduler(config);
    RegisterWorkers(scheduler, {candidate_}, 500, 600);
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({candidate_}, *health, 4, 0);
    ClientReadBandwidthScheduler::WorkerStatus status;
    EXPECT_TRUE(scheduler.GetWorkerStatus(candidate_, health, status));
    EXPECT_GT(status.affinityWeight, status.nonAffinityWeight);
}

// --- 7. Deterministic reproducibility --------------------------------------
TEST_F(WeightedSelectionTest, DeterministicSelectionWithKnownKey) {
    ClientReadBandwidthScheduler scheduler(StableWeightedConfig());
    RegisterWorkers(scheduler, {preferred_, candidate_});
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({preferred_, candidate_}, *health, 4, 0);
    HostPort first;
    ASSERT_TRUE(scheduler.SelectWorkerFast("deterministic", {}, preferred_, {}, health, taskId_++, first, client::WorkerAccessAction::CONTROL));
    // Fresh scheduler with same state → same key gives same behaviour.
    ClientReadBandwidthScheduler scheduler2(StableWeightedConfig());
    RegisterWorkers(scheduler2, {preferred_, candidate_});
    auto health2 = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth({preferred_, candidate_}, *health2, 4, 0);
    HostPort freshFirst;
    uint64_t freshTaskId = 0;
    ASSERT_TRUE(scheduler2.SelectWorkerFast("deterministic", {}, preferred_, {}, health2, freshTaskId, freshFirst, client::WorkerAccessAction::CONTROL));
    EXPECT_EQ(freshFirst, first);
    EXPECT_TRUE(freshFirst == preferred_ || freshFirst == candidate_);
}

// --- 8. Large table -------------------------------------------------------
TEST_F(WeightedSelectionTest, SelectFromLargeTable) {
    ClientReadBandwidthScheduler::Config config = HighWeightConfig();
    config.latencyClientTableSize = 512;
    ClientReadBandwidthScheduler scheduler(config);
    std::vector<HostPort> workers;
    for (int i = 0; i < 200; ++i) {
        workers.emplace_back("10.4.0." + std::to_string(i + 1), 1000);
        ObserveWorker(scheduler, workers.back(), 1000000, 1200000, 1);
    }
    scheduler.RefreshCandidates(workers);
    auto health = std::make_shared<UbRoutingHealthSnapshot>();
    PopulateHealth(workers, *health, 4, 0);
    HostPort selected;
    ASSERT_TRUE(scheduler.SelectWorkerFast("large-table", {}, preferred_, {}, health, taskId_++, selected, client::WorkerAccessAction::CONTROL));
    EXPECT_TRUE(selected == preferred_ || std::find(workers.begin(), workers.end(), selected) != workers.end());
}

}  // namespace ut
}  // namespace datasystem
