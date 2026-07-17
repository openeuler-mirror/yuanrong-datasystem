/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Coordinator-backed watch session and CoordinatorId fence tests.
 */
#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace datasystem::cluster {
namespace {

constexpr char COORDINATOR_A[] = "coordinator-a";
constexpr char COORDINATOR_B[] = "coordinator-b";
constexpr char WATCHER_ADDRESS[] = "127.0.0.1:31501";

class DeterministicCoordinatorProxy final : public ICoordinatorServiceProxy {
public:
    struct WatchStep {
        Status status;
        std::string coordinatorId;
        std::vector<KeyValueEntry> initialKvs;
    };

    struct WatchCall {
        std::string key;
        std::string rangeEnd;
        int64_t watchId;
    };

    struct CancelCall {
        std::vector<int64_t> watchIds;
        std::string expectedCoordinatorId;
    };

    ~DeterministicCoordinatorProxy() override = default;

    Status Put(const std::string &, const std::string &, int64_t, int64_t, int64_t &version, int64_t &revision,
               int32_t, std::string *coordinatorId, const std::string &expectedCoordinatorId) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        lastExpectedCoordinatorId_ = expectedCoordinatorId;
        ++version;
        ++revision;
        observedCoordinatorId_ = putCoordinatorId_;
        if (coordinatorId != nullptr) {
            *coordinatorId = putCoordinatorId_;
        }
        return Status::OK();
    }

    Status Range(const std::string &, const std::string &, std::vector<KeyValueEntry> &entries, int64_t &revision,
                 int32_t, std::string *coordinatorId) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        entries.clear();
        revision = 0;
        observedCoordinatorId_ = putCoordinatorId_;
        if (coordinatorId != nullptr) {
            *coordinatorId = putCoordinatorId_;
        }
        return Status::OK();
    }

    Status DeleteRange(const std::string &, const std::string &, int64_t &, int64_t &, int32_t) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake DeleteRange");
    }

    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &,
                      const std::string &, int64_t &watchId, std::vector<KeyValueEntry> &initialKvs, int32_t,
                      std::string *coordinatorId) override
    {
        WatchStep step{ Status::OK(), COORDINATOR_A, {} };
        std::function<void()> hook;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            watchEntered_ = true;
            watchCv_.notify_all();
            watchCv_.wait(lock, [this] { return !blockWatch_; });
            if (nextWatchStep_ < watchSteps_.size()) {
                step = watchSteps_[nextWatchStep_++];
            }
            watchId = nextWatchId_++;
            watchCalls_.push_back({ key, rangeEnd, watchId });
            hook = beforeWatchReturn_;
            if (step.status.IsOk()) {
                observedCoordinatorId_ = step.coordinatorId;
                initialKvs = step.initialKvs;
                if (coordinatorId != nullptr) {
                    *coordinatorId = step.coordinatorId;
                }
            }
        }
        if (hook != nullptr) {
            hook();
        }
        return step.status;
    }

    Status CancelWatch(const std::string &, const std::vector<int64_t> &watchIds,
                       const std::string &expectedCoordinatorId, int32_t) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cancelCalls_.push_back({ watchIds, expectedCoordinatorId });
        return Status::OK();
    }

    Status KeepAlive(const std::string &, int64_t &, int64_t &, int32_t, std::string *) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake KeepAlive");
    }

    Status CAS(const std::string &, const CasProcessFunc &, int64_t &, int64_t &) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake CAS");
    }

    Status GetCoordinatorId(std::string &coordinatorId, int32_t) override
    {
        GetObservedCoordinatorId(coordinatorId);
        return Status::OK();
    }

    Status ReportTopologyRecoveryCandidate(const coordinator::ReportTopologyRecoveryCandidateReqPb &,
                                           coordinator::ReportTopologyRecoveryCandidateRspPb &, int32_t) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake recovery report");
    }

    void GetObservedCoordinatorId(std::string &coordinatorId) const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        coordinatorId = observedCoordinatorId_;
    }

    void AddWatchStep(Status status, std::string coordinatorId,
                      std::vector<KeyValueEntry> initialKvs = {})
    {
        std::lock_guard<std::mutex> lock(mutex_);
        watchSteps_.push_back({ std::move(status), std::move(coordinatorId), std::move(initialKvs) });
    }

    void SetPutCoordinatorId(std::string coordinatorId)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        putCoordinatorId_ = std::move(coordinatorId);
    }

    void SetBeforeWatchReturn(std::function<void()> hook)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        beforeWatchReturn_ = std::move(hook);
    }

    void BlockNextWatch()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        watchEntered_ = false;
        blockWatch_ = true;
    }

    void WaitForBlockedWatch()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        watchCv_.wait(lock, [this] { return watchEntered_; });
    }

    void ReleaseBlockedWatch()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        blockWatch_ = false;
        watchCv_.notify_all();
    }

    std::vector<WatchCall> WatchCalls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return watchCalls_;
    }

    std::vector<CancelCall> CancelCalls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return cancelCalls_;
    }

    std::string LastExpectedCoordinatorId() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastExpectedCoordinatorId_;
    }

private:
    // Protects every fake response, observation and deterministic watch barrier below.
    mutable std::mutex mutex_;
    std::condition_variable watchCv_;
    std::vector<WatchStep> watchSteps_;
    size_t nextWatchStep_{ 0 };
    int64_t nextWatchId_{ 1 };
    std::vector<WatchCall> watchCalls_;
    std::vector<CancelCall> cancelCalls_;
    std::string observedCoordinatorId_;
    std::string lastExpectedCoordinatorId_;
    std::string putCoordinatorId_{ COORDINATOR_A };
    std::function<void()> beforeWatchReturn_;
    bool blockWatch_{ false };
    bool watchEntered_{ false };
};

std::vector<WatchKey> TwoWatchPlan()
{
    return { { "/datasystem/c/topology", "", 0 }, { "/datasystem/c/notify", WATCHER_ADDRESS, 0 } };
}

void AddSuccessfulBatch(DeterministicCoordinatorProxy &proxy, const std::string &coordinatorId)
{
    proxy.AddWatchStep(Status::OK(), coordinatorId);
    proxy.AddWatchStep(Status::OK(), coordinatorId);
}

TEST(DsCoordinationBackendSessionTest, CommitsOnlyCompleteSameCoordinatorBatch)
{
    DeterministicCoordinatorProxy proxy;
    std::atomic<int> eventCount{ 0 };
    KeyValueEntry topology{ "/datasystem/c/topology/", "topology", 1, 1 };
    KeyValueEntry notification{ "/datasystem/c/notify/127.0.0.1:31501", "notify", 1, 2 };
    proxy.AddWatchStep(Status::OK(), COORDINATOR_A, { topology });
    proxy.AddWatchStep(Status::OK(), COORDINATOR_A, { notification });
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    proxy.SetBeforeWatchReturn([&backend, &eventCount] {
        EXPECT_EQ(eventCount.load(), 0);
        EXPECT_FALSE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
        EXPECT_TRUE(backend.IsWatchRegistrationInProgress());
    });
    backend.SetEventHandler([&eventCount](CoordinationEvent &&) { ++eventCount; });

    ASSERT_TRUE(backend.WatchEvents(TwoWatchPlan()).IsOk());

    EXPECT_EQ(eventCount.load(), 3);
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 2));
    EXPECT_FALSE(backend.OwnsWatchIdentity(COORDINATOR_B, 1));
}

TEST(DsCoordinationBackendSessionTest, RawValueCasCarriesRangeCoordinatorIdFence)
{
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);

    ASSERT_TRUE(backend.CAS("/election", "master", "", "127.0.0.1:31502").IsOk());

    EXPECT_EQ(proxy.LastExpectedCoordinatorId(), COORDINATOR_A);
}

TEST(DsCoordinationBackendSessionTest, CrossCoordinatorBatchRollsBackAndPreservesOldBatch)
{
    DeterministicCoordinatorProxy proxy;
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.WatchEvents(TwoWatchPlan()).IsOk());
    proxy.AddWatchStep(Status::OK(), COORDINATOR_A);
    proxy.AddWatchStep(Status::OK(), COORDINATOR_B);

    EXPECT_EQ(backend.WatchEvents(TwoWatchPlan()).GetCode(), K_TRY_AGAIN);

    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 2));
    const auto cancellations = proxy.CancelCalls();
    ASSERT_EQ(cancellations.size(), 2UL);
    const auto oldGeneration = std::find_if(cancellations.begin(), cancellations.end(), [](const auto &call) {
        return call.expectedCoordinatorId == COORDINATOR_A;
    });
    const auto newGeneration = std::find_if(cancellations.begin(), cancellations.end(), [](const auto &call) {
        return call.expectedCoordinatorId == COORDINATOR_B;
    });
    ASSERT_NE(oldGeneration, cancellations.end());
    ASSERT_NE(newGeneration, cancellations.end());
    EXPECT_EQ(oldGeneration->watchIds, (std::vector<int64_t>{ 3 }));
    EXPECT_EQ(newGeneration->watchIds, (std::vector<int64_t>{ 4 }));
}

TEST(DsCoordinationBackendSessionTest, PartialRegistrationFailureRollsBackAndPreservesOldBatch)
{
    DeterministicCoordinatorProxy proxy;
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.WatchEvents(TwoWatchPlan()).IsOk());
    proxy.AddWatchStep(Status::OK(), COORDINATOR_A);
    proxy.AddWatchStep(Status(K_RPC_UNAVAILABLE, "injected registration failure"), COORDINATOR_A);

    EXPECT_EQ(backend.WatchEvents(TwoWatchPlan()).GetCode(), K_RPC_UNAVAILABLE);

    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 2));
    const auto cancellations = proxy.CancelCalls();
    ASSERT_EQ(cancellations.size(), 1UL);
    EXPECT_EQ(cancellations[0].watchIds, (std::vector<int64_t>{ 3 }));
    EXPECT_EQ(cancellations[0].expectedCoordinatorId, COORDINATOR_A);
}

TEST(DsCoordinationBackendSessionTest, ResetReplacesTheWholeCommittedBatch)
{
    DeterministicCoordinatorProxy proxy;
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    int resetCount = 0;
    backend.SetEventHandler([&resetCount](CoordinationEvent &&event) {
        if (event.type == CoordinationEventType::RESET) {
            ++resetCount;
        }
    });
    ASSERT_TRUE(backend.WatchEvents(TwoWatchPlan()).IsOk());

    CoordinationEvent reset{ CoordinationEventType::RESET, "", "", 0, 0 };
    backend.HandleWatchEvent(COORDINATOR_A, 1, std::move(reset));

    EXPECT_EQ(resetCount, 2);
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 2));
    EXPECT_EQ(proxy.WatchCalls().size(), 2UL);
    proxy.SetPutCoordinatorId(COORDINATOR_A);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    EXPECT_EQ(proxy.WatchCalls().size(), 2UL);
    std::vector<std::pair<std::string, std::string>> entries;
    ASSERT_TRUE(backend.GetAll("/datasystem/c/topology", entries).IsOk());
    EXPECT_EQ(resetCount, 3);
    EXPECT_FALSE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
    EXPECT_FALSE(backend.OwnsWatchIdentity(COORDINATOR_A, 2));
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 3));
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 4));
    const auto cancellations = proxy.CancelCalls();
    ASSERT_EQ(cancellations.size(), 1UL);
    EXPECT_EQ(cancellations[0].watchIds, (std::vector<int64_t>{ 1, 2 }));
    EXPECT_EQ(cancellations[0].expectedCoordinatorId, COORDINATOR_A);
}

TEST(DsCoordinationBackendSessionTest, MembershipIdentityChangeRewatchesAndDropsOldCallbacks)
{
    DeterministicCoordinatorProxy proxy;
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    AddSuccessfulBatch(proxy, COORDINATOR_B);
    proxy.SetPutCoordinatorId(COORDINATOR_B);
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    std::vector<std::string> membershipIds;
    int eventCount = 0;
    backend.SetMembershipReadyHandler(
        [&membershipIds](const std::string &coordinatorId, bool) { membershipIds.push_back(coordinatorId); });
    backend.SetEventHandler([&eventCount](CoordinationEvent &&event) {
        if (event.type != CoordinationEventType::RESET) {
            ++eventCount;
        }
    });
    ASSERT_TRUE(backend.WatchEvents(TwoWatchPlan()).IsOk());

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());

    ASSERT_EQ(membershipIds.size(), 1UL);
    EXPECT_EQ(membershipIds[0], COORDINATOR_B);
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
    std::vector<std::pair<std::string, std::string>> entries;
    ASSERT_TRUE(backend.GetAll("/datasystem/c/topology", entries).IsOk());
    EXPECT_FALSE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
    EXPECT_TRUE(backend.OwnsWatchIdentity(COORDINATOR_B, 3));
    CoordinationEvent stale{ CoordinationEventType::PUT, "/datasystem/c/topology/", "old", 1, 3 };
    backend.HandleWatchEvent(COORDINATOR_A, 1, std::move(stale));
    EXPECT_EQ(eventCount, 0);
    CoordinationEvent current{ CoordinationEventType::PUT, "/datasystem/c/topology/", "new", 1, 4 };
    backend.HandleWatchEvent(COORDINATOR_B, 3, std::move(current));
    EXPECT_EQ(eventCount, 1);
}

TEST(DsCoordinationBackendSessionTest, InitialLeaseFactSurvivesReadyLifecycleTransition)
{
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    EXPECT_TRUE(backend.IsFirstKeepAliveSent());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_TRUE(backend.IsFirstKeepAliveSent());
}

TEST(DsCoordinationBackendSessionTest, InvalidatedPlansRebuildOnTheNextExactRead)
{
    DeterministicCoordinatorProxy proxy;
    for (int i = 0; i < 4; ++i) {
        AddSuccessfulBatch(proxy, COORDINATOR_A);
    }
    DsCoordinationBackend workerBackend(&proxy, WATCHER_ADDRESS);
    DsCoordinationBackend controllerBackend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(workerBackend.WatchEvents(TwoWatchPlan()).IsOk());
    ASSERT_TRUE(controllerBackend.WatchEvents(TwoWatchPlan()).IsOk());

    workerBackend.InvalidateWatches();
    controllerBackend.InvalidateWatches();
    std::vector<std::pair<std::string, std::string>> entries;
    ASSERT_TRUE(workerBackend.GetAll("/datasystem/c/topology", entries).IsOk());
    ASSERT_TRUE(controllerBackend.GetAll("/datasystem/c/topology", entries).IsOk());

    EXPECT_FALSE(workerBackend.OwnsWatchIdentity(COORDINATOR_A, 1));
    EXPECT_FALSE(controllerBackend.OwnsWatchIdentity(COORDINATOR_A, 3));
    EXPECT_TRUE(workerBackend.OwnsWatchIdentity(COORDINATOR_A, 5));
    EXPECT_TRUE(controllerBackend.OwnsWatchIdentity(COORDINATOR_A, 7));
}

TEST(DsCoordinationBackendSessionTest, ShutdownWaitsForInProgressRegistrationThenRejectsNewPlans)
{
    DeterministicCoordinatorProxy proxy;
    proxy.AddWatchStep(Status::OK(), COORDINATOR_A);
    proxy.BlockNextWatch();
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    const std::vector<WatchKey> oneWatchPlan{ { "/datasystem/c/topology", "", 0 } };
    EXPECT_EQ(backend.WatchEvents({}).GetCode(), K_NOT_READY);
    EXPECT_TRUE(proxy.WatchCalls().empty());
    Status watchStatus = Status::OK();
    Status shutdownStatus = Status::OK();
    std::mutex startMutex;
    std::condition_variable startCv;
    bool shutdownStarted = false;
    std::thread watchThread([&] { watchStatus = backend.WatchEvents(oneWatchPlan); });
    proxy.WaitForBlockedWatch();
    std::thread shutdownThread([&] {
        {
            std::lock_guard<std::mutex> lock(startMutex);
            shutdownStarted = true;
        }
        startCv.notify_all();
        shutdownStatus = backend.Shutdown();
    });
    {
        std::unique_lock<std::mutex> lock(startMutex);
        startCv.wait(lock, [&shutdownStarted] { return shutdownStarted; });
    }
    proxy.ReleaseBlockedWatch();
    watchThread.join();
    shutdownThread.join();

    EXPECT_TRUE(watchStatus.IsOk());
    EXPECT_TRUE(shutdownStatus.IsOk());
    EXPECT_FALSE(backend.OwnsWatchIdentity(COORDINATOR_A, 1));
    const auto watchCallsBeforeRejectedPlans = proxy.WatchCalls().size();
    EXPECT_EQ(backend.WatchEvents({}).GetCode(), K_NOT_READY);
    EXPECT_EQ(backend.WatchEvents(oneWatchPlan).GetCode(), K_NOT_READY);
    EXPECT_EQ(proxy.WatchCalls().size(), watchCallsBeforeRejectedPlans);
}

}  // namespace
}  // namespace datasystem::cluster
