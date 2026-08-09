// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Description: Unit tests for the standalone Coordinator membership manager.
 */

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/coordinator/raft/coordinator_raft_node.h"
#define private public
#include "datasystem/coordinator/raft/coordinator_membership_manager.h"
#undef private

namespace datasystem::coordinator {
namespace {
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

constexpr size_t kExpectedMemberCount = 3;
constexpr int64_t kObservedTerm = 7;
constexpr int64_t kNextTerm = 8;
constexpr int64_t kConfigurationIndex = 11;
constexpr std::chrono::milliseconds kHealthCheckInterval{ 20 };
constexpr std::chrono::milliseconds kMemberFailureGrace{ 40 };
constexpr std::chrono::milliseconds kDiscoveryRetryInterval{ 20 };
constexpr std::chrono::milliseconds kZeroDuration{ 0 };
constexpr std::chrono::milliseconds kNegativeDuration{ -1 };
constexpr std::chrono::seconds kLongHealthCheckInterval{ 5 };
constexpr std::chrono::seconds kLongMemberFailureGrace{ 10 };
constexpr std::chrono::seconds kLifecycleDeadline{ 2 };
constexpr std::chrono::milliseconds kManualClockStart{ 1'000 };
constexpr int kSuspectedFailureErrors = kCoordinatorFollowerFailureErrorThreshold + 1;
constexpr char kUnsafeOverTargetMarker[] =
    "Coordinator membership over-target configuration has no safe removal target";
constexpr char kPeer1[] = "10.0.0.1:1001";
constexpr char kPeer2[] = "10.0.0.2:1002";
constexpr char kPeer3[] = "10.0.0.3:1003";
constexpr char kCandidate4[] = "10.0.0.4:1004";
constexpr char kCandidate5[] = "10.0.0.5:1005";
constexpr char kMalformedCandidate[] = "not-an-address";
constexpr char kDomainCandidate[] = "coordinator.example:1004";
constexpr char kIpv6Candidate[] = "[::1]:1004";
constexpr char kWildcardCandidate[] = "0.0.0.0:1004";

static_assert(!std::is_copy_constructible_v<CoordinatorMembershipManager>);
static_assert(!std::is_copy_assignable_v<CoordinatorMembershipManager>);

class ManualNow {
public:
    ManualNow() : now_(TimePoint(kManualClockStart))
    {
    }

    TimePoint Get() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return now_;
    }

    void Advance(std::chrono::milliseconds duration)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        now_ += duration;
    }

private:
    mutable std::mutex mutex_;
    TimePoint now_;
};

class ThreadSafeCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++calls_;
        serviceList = candidates_;
        return result_;
    }

    void SetCandidates(std::vector<std::string> candidates)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        candidates_ = std::move(candidates);
        result_ = Status::OK();
    }

    void SetResult(Status result)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = std::move(result);
    }

    int Calls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::string> candidates_;
    Status result_;
    int calls_{ 0 };
};

class BlockingCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    BlockingCoordinatorDiscovery() : releaseFuture_(releasePromise_.get_future().share())
    {
    }

    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        std::call_once(enteredOnce_, [this] { enteredPromise_.set_value(); });
        if (releaseFuture_.wait_until(Clock::now() + kLifecycleDeadline) != std::future_status::ready) {
            return Status(K_RUNTIME_ERROR, "Blocked Coordinator Discovery test dependency timed out");
        }
        serviceList = { kCandidate4 };
        return Status::OK();
    }

    std::future<void> GetEnteredFuture()
    {
        return enteredPromise_.get_future();
    }

    void Release()
    {
        std::call_once(releaseOnce_, [this] { releasePromise_.set_value(); });
    }

private:
    std::once_flag enteredOnce_;
    std::promise<void> enteredPromise_;
    std::once_flag releaseOnce_;
    std::promise<void> releasePromise_;
    std::shared_future<void> releaseFuture_;
};

class ThreadSafeMembershipDependencies {
public:
    using GetStatusAction = std::function<Status(CoordinatorRaftMembershipStatus &, int)>;
    using PeerAction = std::function<Status(const std::string &,
                                            const CoordinatorMembershipManager::MembershipOperationCallback &)>;

    CoordinatorMembershipManager::Dependencies Make()
    {
        return CoordinatorMembershipManager::Dependencies{
            [this] { return HasInFlightMembershipOperation(); },
            [this](CoordinatorRaftMembershipStatus &status) { return GetStatus(status); },
            [this](const std::string &peer,
                   const CoordinatorMembershipManager::MembershipOperationCallback &callback) {
                return AddPeer(peer, callback);
            },
            [this](const std::string &peer,
                   const CoordinatorMembershipManager::MembershipOperationCallback &callback) {
                return RemovePeer(peer, callback);
            }
        };
    }

    void SetStatus(CoordinatorRaftMembershipStatus status)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = std::move(status);
    }

    void SetInFlightMembershipOperation(bool inFlight)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        hasInFlightMembershipOperation_ = inFlight;
    }

    void SetGetStatusAction(GetStatusAction action)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        getStatusAction_ = std::move(action);
    }

    void SetAddSubmissionResult(Status result)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        addSubmissionResult_ = std::move(result);
    }

    void SetRemoveSubmissionResult(Status result)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        removeSubmissionResult_ = std::move(result);
    }

    void SetAddPeerAction(PeerAction action)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        addPeerAction_ = std::move(action);
    }

    bool CompleteLastAdd(const Status &result) const
    {
        auto callback = LastAddCallback();
        if (!callback) {
            return false;
        }
        callback(result);
        return true;
    }

    CoordinatorMembershipManager::MembershipOperationCallback LastAddCallback() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return addCallback_;
    }

    CoordinatorMembershipManager::MembershipOperationCallback LastRemoveCallback() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return removeCallback_;
    }

    std::future<void> GetFirstStatusFuture()
    {
        return firstGetStatus_.get_future();
    }

    bool WaitForAddPeerCalls(int expectedCalls, TimePoint deadline)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return callsCv_.wait_until(lock, deadline, [this, expectedCalls] { return addPeerCalls_ >= expectedCalls; });
    }

    int GetStatusCalls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return getStatusCalls_;
    }

    int AddPeerCalls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return addPeerCalls_;
    }

    int RemovePeerCalls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return removePeerCalls_;
    }

    std::string LastAddedPeer() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return addedPeers_.empty() ? std::string() : addedPeers_.back();
    }

    std::vector<std::string> AddedPeers() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return addedPeers_;
    }

    std::string LastRemovedPeer() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return removedPeers_.empty() ? std::string() : removedPeers_.back();
    }

private:
    bool HasInFlightMembershipOperation() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return hasInFlightMembershipOperation_;
    }

    Status GetStatus(CoordinatorRaftMembershipStatus &status)
    {
        GetStatusAction action;
        Status result;
        int callNumber = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            status = status_;
            result = getStatusResult_;
            action = getStatusAction_;
            callNumber = ++getStatusCalls_;
            if (!firstGetStatusSignaled_) {
                firstGetStatusSignaled_ = true;
                firstGetStatus_.set_value();
            }
        }
        callsCv_.notify_all();
        return action ? action(status, callNumber) : result;
    }

    Status AddPeer(const std::string &peer,
                   const CoordinatorMembershipManager::MembershipOperationCallback &callback)
    {
        Status result;
        PeerAction action;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++addPeerCalls_;
            addedPeers_.emplace_back(peer);
            addCallback_ = callback;
            result = addSubmissionResult_;
            action = addPeerAction_;
        }
        callsCv_.notify_all();
        return action ? action(peer, callback) : result;
    }

    Status RemovePeer(const std::string &peer,
                      const CoordinatorMembershipManager::MembershipOperationCallback &callback)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++removePeerCalls_;
        removedPeers_.emplace_back(peer);
        removeCallback_ = callback;
        return removeSubmissionResult_;
    }

    mutable std::mutex mutex_;
    std::condition_variable callsCv_;
    CoordinatorRaftMembershipStatus status_;
    Status getStatusResult_;
    GetStatusAction getStatusAction_;
    PeerAction addPeerAction_;
    Status addSubmissionResult_;
    Status removeSubmissionResult_;
    CoordinatorMembershipManager::MembershipOperationCallback addCallback_;
    CoordinatorMembershipManager::MembershipOperationCallback removeCallback_;
    std::vector<std::string> addedPeers_;
    std::vector<std::string> removedPeers_;
    int getStatusCalls_{ 0 };
    int addPeerCalls_{ 0 };
    int removePeerCalls_{ 0 };
    bool hasInFlightMembershipOperation_{ false };
    bool firstGetStatusSignaled_{ false };
    std::promise<void> firstGetStatus_;
};

CoordinatorFollowerStatus Follower(std::string peer, bool valid, int errors)
{
    return CoordinatorFollowerStatus{ std::move(peer), valid, errors };
}

CoordinatorRaftMembershipStatus LeaderStatus(std::vector<std::string> peers,
                                             std::vector<CoordinatorFollowerStatus> followers,
                                             int64_t index = kConfigurationIndex,
                                             int64_t term = kObservedTerm)
{
    return CoordinatorRaftMembershipStatus{ true, term, index, std::move(peers), std::move(followers) };
}

CoordinatorRaftMembershipStatus HealthyFullStatus(int follower2Errors = 0)
{
    return LeaderStatus({ kPeer1, kPeer2, kPeer3 },
                        { Follower(kPeer2, true, follower2Errors), Follower(kPeer3, true, 0) });
}

CoordinatorRaftMembershipStatus VacancyStatus()
{
    return LeaderStatus({ kPeer1, kPeer2 }, { Follower(kPeer2, true, 0) });
}

CoordinatorRaftMembershipStatus FailedFullStatus(int64_t index = kConfigurationIndex, int64_t term = kObservedTerm)
{
    return LeaderStatus({ kPeer1, kPeer2, kPeer3 },
                        { Follower(kPeer2, false, kSuspectedFailureErrors), Follower(kPeer3, true, 0) }, index, term);
}

CoordinatorRaftMembershipStatus ReplacementCommittedStatus(CoordinatorFollowerStatus failedPeerObservation,
                                                            int64_t index = kConfigurationIndex + 1,
                                                            int64_t term = kObservedTerm,
                                                            const std::string &candidate = kCandidate4)
{
    return LeaderStatus({ kPeer1, kPeer2, kPeer3, candidate },
                        { std::move(failedPeerObservation), Follower(kPeer3, true, 0), Follower(candidate, true, 0) },
                        index, term);
}

CoordinatorRaftMembershipStatus ReplacementCompletedStatus(int64_t index = kConfigurationIndex + 2)
{
    return LeaderStatus({ kPeer1, kPeer3, kCandidate4 },
                        { Follower(kPeer3, true, 0), Follower(kCandidate4, true, 0) }, index);
}

CoordinatorMembershipOptions ValidOptions(size_t expectedMemberCount = kExpectedMemberCount)
{
    return CoordinatorMembershipOptions{ expectedMemberCount, kHealthCheckInterval, kMemberFailureGrace,
                                         kDiscoveryRetryInterval };
}

CoordinatorMembershipOptions LongWaitOptions()
{
    auto options = ValidOptions();
    options.healthCheckInterval = kLongHealthCheckInterval;
    options.memberFailureGrace = kLongMemberFailureGrace;
    options.discoveryRetryInterval = kLongHealthCheckInterval;
    return options;
}

CoordinatorMembershipManager MakeManager(CoordinatorMembershipOptions options,
                                         ThreadSafeMembershipDependencies &dependencies,
                                         std::shared_ptr<ICoordinatorDiscovery> discovery, ManualNow &manualNow)
{
    return CoordinatorMembershipManager(options, dependencies.Make(), std::move(discovery),
                                        [&manualNow] { return manualNow.Get(); });
}

CoordinatorMembershipManager MakeManager(CoordinatorMembershipOptions options,
                                         ThreadSafeMembershipDependencies &dependencies,
                                         std::shared_ptr<ICoordinatorDiscovery> discovery)
{
    return CoordinatorMembershipManager(options, dependencies.Make(), std::move(discovery), [] { return Clock::now(); });
}

void ExpectReconcileOk(CoordinatorMembershipManager &manager)
{
    const auto status = manager.ReconcileOnce();
    EXPECT_TRUE(status.IsOk()) << status.ToString();
}

TEST(CoordinatorMembershipManagerTest, RejectsInvalidOptions)
{
    struct InvalidCase {
        const char *diagnostic;
        std::function<void(CoordinatorMembershipOptions &)> mutate;
    };
    const std::vector<InvalidCase> cases{
        { "expectedMemberCount must be positive", [](auto &options) { options.expectedMemberCount = 0; } },
        { "healthCheckInterval must be positive", [](auto &options) { options.healthCheckInterval = kZeroDuration; } },
        { "healthCheckInterval must be positive", [](auto &options) { options.healthCheckInterval = kNegativeDuration; } },
        { "memberFailureGrace must be positive", [](auto &options) { options.memberFailureGrace = kZeroDuration; } },
        { "discoveryRetryInterval must be positive",
          [](auto &options) { options.discoveryRetryInterval = kZeroDuration; } },
        { "healthCheckInterval must be less than memberFailureGrace",
          [](auto &options) { options.memberFailureGrace = options.healthCheckInterval; } },
    };

    for (const auto &testCase : cases) {
        ThreadSafeMembershipDependencies dependencies;
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        auto options = ValidOptions();
        testCase.mutate(options);
        auto manager = MakeManager(options, dependencies, discovery);
        const auto status = manager.Start();
        EXPECT_EQ(status.GetCode(), K_INVALID);
        EXPECT_NE(status.ToString().find(testCase.diagnostic), std::string::npos);
    }
}

TEST(CoordinatorMembershipManagerTest, AcceptsDiscoveryRetryBelowHealthCheckInterval)
{
    auto options = ValidOptions();
    options.discoveryRetryInterval = options.healthCheckInterval / 2;

    EXPECT_TRUE(options.IsValid());
}

TEST(CoordinatorMembershipManagerTest, RejectsNullDiscovery)
{
    ThreadSafeMembershipDependencies dependencies;
    auto manager = MakeManager(ValidOptions(), dependencies, nullptr);
    EXPECT_EQ(manager.Start().GetCode(), K_INVALID);
}

TEST(CoordinatorMembershipManagerTest, StartsOnceAndShutsDownIdempotently)
{
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(HealthyFullStatus());
    auto firstStatus = dependencies.GetFirstStatusFuture();
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery);

    ASSERT_TRUE(manager.Start().IsOk());
    EXPECT_EQ(manager.Start().GetCode(), K_INVALID);
    ASSERT_EQ(firstStatus.wait_for(kLifecycleDeadline), std::future_status::ready);
    EXPECT_TRUE(manager.Shutdown().IsOk());
    EXPECT_TRUE(manager.Shutdown().IsOk());
}

TEST(CoordinatorMembershipManagerTest, ShutdownInterruptsLongHealthWait)
{
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(HealthyFullStatus());
    auto firstStatus = dependencies.GetFirstStatusFuture();
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(firstStatus.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    ASSERT_EQ(shutdown.wait_for(kLifecycleDeadline), std::future_status::ready);
    EXPECT_TRUE(shutdown.get().IsOk());
}

TEST(CoordinatorMembershipManagerTest, ReentrantShutdownFromReconciliationThreadIsRejected)
{
    std::promise<Status> resultPromise;
    auto result = resultPromise.get_future();
    std::once_flag once;
    CoordinatorMembershipManager *managerPtr = nullptr;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(HealthyFullStatus());
    dependencies.SetGetStatusAction([&](CoordinatorRaftMembershipStatus &, int) {
        std::call_once(once, [&] { resultPromise.set_value(managerPtr->Shutdown()); });
        return Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery);
    managerPtr = &manager;

    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(result.wait_for(kLifecycleDeadline), std::future_status::ready);
    EXPECT_EQ(result.get().GetCode(), K_INVALID);
    EXPECT_TRUE(manager.Shutdown().IsOk());
}

TEST(CoordinatorMembershipManagerTest, FollowerAndHealthyFullMembershipDoNotCallDiscovery)
{
    for (const bool leader : { false, true }) {
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        auto status = HealthyFullStatus(kCoordinatorFollowerFailureErrorThreshold);
        status.isLeader = leader;
        dependencies.SetStatus(status);
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        EXPECT_EQ(discovery->Calls(), 0);
        EXPECT_EQ(dependencies.AddPeerCalls(), 0);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    }
}

TEST(CoordinatorMembershipManagerTest, UnknownQuorumPreventsDiscoveryAndMutation)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus({ kPeer1, kPeer2 }, { Follower(kPeer2, false, 0) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 0);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, DiscoveryRetryIntervalBoundsVacancyAttempts)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kDiscoveryRetryInterval - std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 1);
    now.Advance(std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 2);
}

TEST(CoordinatorMembershipManagerTest, CandidateFilteringIsDeterministic)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate5, kPeer1, kMalformedCandidate, kDomainCandidate, kIpv6Candidate,
                               kWildcardCandidate, kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate4);
}

TEST(CoordinatorMembershipManagerTest, InFlightOperationDefersReconciliationUntilCommittedStateIsPublished)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetInFlightMembershipOperation(true);
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.GetStatusCalls(), 0);
    EXPECT_EQ(discovery->Calls(), 0);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);

    dependencies.SetStatus(HealthyFullStatus());
    dependencies.SetInFlightMembershipOperation(false);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.GetStatusCalls(), 1);
    EXPECT_EQ(discovery->Calls(), 0);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, AsynchronousAddFailureDoesNotControlCandidateRotation)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(dependencies.CompleteLastAdd(Status(K_RUNTIME_ERROR, "scripted asynchronous failure")));
    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate5);
}

TEST(CoordinatorMembershipManagerTest, AttemptHistoryIsPrunedToCurrentDiscoveryCandidates)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_EQ(manager.candidateLastAttemptAt_.count(kCandidate4), 1U);
    discovery->SetCandidates({ kCandidate5 });
    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    EXPECT_EQ(manager.candidateLastAttemptAt_.count(kCandidate4), 0U);
    EXPECT_EQ(manager.candidateLastAttemptAt_.count(kCandidate5), 1U);
}

TEST(CoordinatorMembershipManagerTest, SynchronousFailureStillRotatesToNextCandidate)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetAddSubmissionResult(Status(K_RUNTIME_ERROR, "scripted submission failure"));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    EXPECT_TRUE(manager.ReconcileOnce().IsError());
    now.Advance(kDiscoveryRetryInterval);
    EXPECT_TRUE(manager.ReconcileOnce().IsError());
    EXPECT_EQ(dependencies.AddedPeers(), (std::vector<std::string>{ kCandidate4, kCandidate5 }));
}

TEST(CoordinatorMembershipManagerTest, DiscoveryFailureAndEmptyResultNeverRemove)
{
    for (const bool failDiscovery : { false, true }) {
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(FailedFullStatus());
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        if (failDiscovery) {
            discovery->SetResult(Status(K_RUNTIME_ERROR, "scripted discovery failure"));
        }
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        EXPECT_EQ(dependencies.AddPeerCalls(), 0);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    }
}

TEST(CoordinatorMembershipManagerTest, SubmissionSnapshotRejectsStaleAdd)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetGetStatusAction([](CoordinatorRaftMembershipStatus &status, int callNumber) {
        if (callNumber == 2) {
            status = HealthyFullStatus();
        }
        return Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    EXPECT_EQ(manager.ReconcileOnce().GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, FailureGraceRequiresContinuousErrorsAndRestartsOnNewTerm)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    auto recovered = HealthyFullStatus();
    dependencies.SetStatus(recovered);
    ExpectReconcileOk(manager);
    dependencies.SetStatus(FailedFullStatus(kConfigurationIndex, kNextTerm));
    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace - std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    now.Advance(std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, VacancyIsAlwaysAddOnly)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer2, kPeer3 },
        { Follower(kPeer2, true, 0), Follower(kPeer3, false, kSuspectedFailureErrors) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(4), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, ReplacementCommitsNPlusOneBeforeRemovingFailedPeer)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate4);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);

    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);

    dependencies.SetStatus(ReplacementCompletedStatus());
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.replacementIntent_.has_value());
}

TEST(CoordinatorMembershipManagerTest, RollbackUsesActuallyCommittedCandidateNotLastAttempt)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate5);

    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, true, 0), kConfigurationIndex + 1,
                                                       kObservedTerm, kCandidate4));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kCandidate4);
}

TEST(CoordinatorMembershipManagerTest, RecoveryBeforeDelayedCommitRollsBackAttemptedCandidate)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.replacementIntent_.has_value());
    EXPECT_EQ(manager.replacementIntent_->attemptedCandidates.count(kCandidate4), 1U);

    dependencies.SetStatus(HealthyFullStatus());
    ExpectReconcileOk(manager);
    EXPECT_TRUE(manager.replacementIntent_.has_value());

    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, true, 0)));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kCandidate4);
}

TEST(CoordinatorMembershipManagerTest, RejectedReplacementDoesNotOwnExternallyCommittedCandidate)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    dependencies.SetAddSubmissionResult(Status(K_RUNTIME_ERROR, "scripted rejection"));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    EXPECT_TRUE(manager.ReconcileOnce().IsError());
    ASSERT_TRUE(manager.replacementIntent_.has_value());
    EXPECT_TRUE(manager.replacementIntent_->attemptedCandidates.empty());

    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, true, 0)));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, RejectedRetryPreservesPreviouslyAcceptedReplacementOwnership)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.replacementIntent_.has_value());
    EXPECT_EQ(manager.replacementIntent_->attemptedCandidates,
              (std::set<std::string>{ kCandidate4 }));

    dependencies.SetAddSubmissionResult(Status(K_RUNTIME_ERROR, "scripted retry rejection"));
    now.Advance(kDiscoveryRetryInterval);
    EXPECT_TRUE(manager.ReconcileOnce().IsError());
    EXPECT_EQ(manager.replacementIntent_->attemptedCandidates,
              (std::set<std::string>{ kCandidate4 }));

    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, true, 0)));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kCandidate4);
}

TEST(CoordinatorMembershipManagerTest, RecoveredPeerWithoutProvenCandidateFailsClosed)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer2, kPeer3, kCandidate4 },
        { Follower(kPeer2, true, 0), Follower(kPeer3, true, 0), Follower(kCandidate4, true, 0) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    testing::internal::CaptureStderr();
    // The marker is logged via LOG_FIRST_AND_EVERY_N(ERROR, 100): its per-line counter is process-global, and
    // earlier tests in this binary (e.g. RejectedReplacementDoesNotOwnExternallyCommittedCandidate) may advance
    // it, so a single ReconcileOnce is not guaranteed to emit. Replaying up to 100 times always hits an
    // emission (counter == 1 or counter % 100 == 0) regardless of the counter position.
    for (int attempt = 0; attempt < 100; ++attempt) {
        ExpectReconcileOk(manager);
    }
    const auto logs = testing::internal::GetCapturedStderr();
    EXPECT_NE(logs.find(kUnsafeOverTargetMarker), std::string::npos);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, PreExistingSpareRemovesConfirmedFailedPeerWithoutDiscovery)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
    EXPECT_EQ(discovery->Calls(), 0);
}

TEST(CoordinatorMembershipManagerTest, FreshQuorumLossRejectsFailedPeerRemoval)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));
    dependencies.SetGetStatusAction([](CoordinatorRaftMembershipStatus &status, int callNumber) {
        if (callNumber == 3) {
            status = LeaderStatus(
                { kPeer1, kPeer2, kPeer3, kCandidate4 },
                { Follower(kPeer2, false, kSuspectedFailureErrors), Follower(kPeer3, false, 0),
                  Follower(kCandidate4, false, 0) },
                kConfigurationIndex + 1);
        }
        return Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    EXPECT_EQ(manager.ReconcileOnce().GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, FreshRecoveryChangeRejectsCandidateRollback)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, true, 0)));
    dependencies.SetGetStatusAction([](CoordinatorRaftMembershipStatus &status, int callNumber) {
        if (callNumber == 2) {
            status = ReplacementCommittedStatus(Follower(kPeer2, false, 0));
        }
        return Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);
    manager.observingLeader_ = true;
    manager.observedTerm_ = kObservedTerm;
    manager.replacementIntent_.emplace(CoordinatorMembershipManager::ReplacementIntent{
        kPeer2, { kPeer1, kPeer2, kPeer3 }, { kCandidate4 } });

    EXPECT_EQ(manager.ReconcileOnce().GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, RemoveRetriesAreBoundedByDiscoveryInterval)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_EQ(dependencies.RemovePeerCalls(), 1);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 2);
}

TEST(CoordinatorMembershipManagerTest, ShutdownWhileStatusBlockedPreventsNewDiscovery)
{
    std::promise<void> statusEnteredPromise;
    auto statusEntered = statusEnteredPromise.get_future();
    std::promise<void> releaseStatusPromise;
    auto releaseStatus = releaseStatusPromise.get_future().share();
    std::once_flag statusEnteredOnce;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetGetStatusAction([&](CoordinatorRaftMembershipStatus &, int) {
        std::call_once(statusEnteredOnce, [&] { statusEnteredPromise.set_value(); });
        if (releaseStatus.wait_until(Clock::now() + kLifecycleDeadline) != std::future_status::ready) {
            return Status(K_RUNTIME_ERROR, "blocked status test timed out");
        }
        return Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(statusEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    bool stopping = false;
    {
        std::unique_lock<std::mutex> lock(manager.lifecycleMutex_);
        stopping = manager.lifecycleCv_.wait_for(lock, kLifecycleDeadline, [&manager] {
            return manager.state_ == CoordinatorMembershipManager::LifecycleState::STOPPING;
        });
    }
    releaseStatusPromise.set_value();
    ASSERT_TRUE(stopping);
    ASSERT_EQ(shutdown.wait_for(kLifecycleDeadline), std::future_status::ready);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_EQ(discovery->Calls(), 0);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, ShutdownDuringBlockedDiscoveryPreventsAdd)
{
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<BlockingCoordinatorDiscovery>();
    auto entered = discovery->GetEnteredFuture();
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(entered.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    {
        std::unique_lock<std::mutex> lock(manager.lifecycleMutex_);
        ASSERT_TRUE(manager.lifecycleCv_.wait_for(lock, kLifecycleDeadline, [&manager] {
            return manager.state_ == CoordinatorMembershipManager::LifecycleState::STOPPING;
        }));
    }
    discovery->Release();
    ASSERT_EQ(shutdown.wait_for(kLifecycleDeadline), std::future_status::ready);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, AddAndRemoveCallbacksRemainSafeAfterManagerDestruction)
{
    CoordinatorMembershipManager::MembershipOperationCallback addCompletion;
    CoordinatorMembershipManager::MembershipOperationCallback removeCompletion;
    ThreadSafeMembershipDependencies addDependencies;
    ThreadSafeMembershipDependencies removeDependencies;

    {
        addDependencies.SetStatus(VacancyStatus());
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), addDependencies, discovery);
        ExpectReconcileOk(manager);
        addCompletion = addDependencies.LastAddCallback();
        ASSERT_TRUE(addCompletion);
        ASSERT_TRUE(manager.Shutdown().IsOk());
    }

    {
        ManualNow now;
        removeDependencies.SetStatus(
            ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        auto manager = MakeManager(ValidOptions(), removeDependencies, discovery, now);
        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        removeCompletion = removeDependencies.LastRemoveCallback();
        ASSERT_TRUE(removeCompletion);
        ASSERT_TRUE(manager.Shutdown().IsOk());
    }

    const auto addCalls = addDependencies.AddPeerCalls();
    const auto removeCalls = removeDependencies.RemovePeerCalls();
    EXPECT_NO_THROW(addCompletion(Status(K_RUNTIME_ERROR, "late Add completion")));
    EXPECT_NO_THROW(removeCompletion(Status(K_RUNTIME_ERROR, "late Remove completion")));
    EXPECT_EQ(addDependencies.AddPeerCalls(), addCalls);
    EXPECT_EQ(removeDependencies.RemovePeerCalls(), removeCalls);
}

TEST(CoordinatorMembershipManagerTest, ReplacementSubmissionExceptionRollsBackOnlyNewCandidateOwnership)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.replacementIntent_.has_value());
    EXPECT_EQ(manager.replacementIntent_->attemptedCandidates,
              (std::set<std::string>{ kCandidate4 }));

    dependencies.SetAddPeerAction([](const std::string &,
                                     const CoordinatorMembershipManager::MembershipOperationCallback &) -> Status {
        throw std::runtime_error("scripted submission exception");
    });
    now.Advance(kDiscoveryRetryInterval);
    EXPECT_TRUE(manager.ReconcileOnce().IsError());
    EXPECT_EQ(dependencies.AddedPeers(), (std::vector<std::string>{ kCandidate4, kCandidate5 }));
    EXPECT_EQ(manager.replacementIntent_->attemptedCandidates,
              (std::set<std::string>{ kCandidate4 }));
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

}  // namespace
}  // namespace datasystem::coordinator
