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
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/util/thread.h"
#include "datasystem/coordinator/raft/coordinator_raft_node.h"
#include "datasystem/utils/service_discovery.h"
#define private public
#include "datasystem/coordinator/raft/coordinator_membership_manager.h"
#undef private

namespace datasystem::coordinator {
namespace {
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

constexpr size_t kExpectedMemberCount = 3;
constexpr int64_t kObservedFollowerTerm = 7;
constexpr int64_t kNextObservedTerm = 8;
constexpr int64_t kObservedConfigurationIndex = 11;
constexpr std::chrono::milliseconds kHealthCheckInterval{ 20 };
constexpr std::chrono::milliseconds kMemberFailureGrace{ 40 };
constexpr std::chrono::milliseconds kDiscoveryRetryInterval{ 20 };
constexpr std::chrono::milliseconds kOperationWarningTimeout{ 100 };
constexpr std::chrono::milliseconds kCandidateRetryCooldown{ 100 };
constexpr std::chrono::milliseconds kZeroDuration{ 0 };
constexpr std::chrono::milliseconds kNegativeDuration{ -1 };
constexpr std::chrono::seconds kLongHealthCheckInterval{ 5 };
constexpr std::chrono::seconds kLongMemberFailureGrace{ 10 };
constexpr std::chrono::seconds kLifecycleDeadline{ 2 };
constexpr std::chrono::seconds kBlockingGetStatusWaitBudget{ 2 };
constexpr std::chrono::milliseconds kBlockedObservationWindow{ 200 };
constexpr std::chrono::milliseconds kManualClockStart{ 1'000 };
constexpr int kRuntimeExceptionCall = 1;
constexpr int kNonStandardExceptionCall = 2;
constexpr int kSuccessfulCallAfterExceptions = 3;
constexpr int kRemovalPolicyRevalidationCall = 5;
constexpr int kSuspectedFailureErrors = kCoordinatorFollowerFailureErrorThreshold + 1;
constexpr size_t kPolicyDiagnosticRateLimitInterval = 100;
constexpr size_t kMaximumDiagnosticEmissionsPerFullInterval = 2;
constexpr char kReconcileExceptionMarker[] = "Coordinator membership reconciliation exception";
constexpr char kInvalidCandidateMarker[] = "Coordinator membership discovery returned an invalid candidate";
constexpr char kOperationWarningMarker[] = "Coordinator membership operation remains uncertain";
constexpr char kUnsafeOverTargetMarker[] =
    "Coordinator membership over-target configuration has no safe removal target";
constexpr char kSensitiveExceptionText[] = "injected-sensitive-reconciliation-detail";
constexpr char kScriptedSubmissionFailure[] = "scripted membership submission failure";
constexpr char kPeer1[] = "10.0.0.1:1001";
constexpr char kPeer2[] = "10.0.0.2:1002";
constexpr char kPeer3[] = "10.0.0.3:1003";
constexpr char kCandidate4[] = "10.0.0.4:1004";
constexpr char kCandidate5[] = "10.0.0.5:1005";
constexpr char kCandidate6[] = "10.0.0.6:1006";
constexpr char kMalformedCandidate[] = "not-an-address";
constexpr char kDomainCandidate[] = "coordinator.example:1004";
constexpr char kIpv6Candidate[] = "[::1]:1004";
constexpr char kWildcardCandidate[] = "0.0.0.0:1004";

static_assert(!std::is_copy_constructible_v<CoordinatorMembershipManager>);
static_assert(!std::is_copy_assignable_v<CoordinatorMembershipManager>);

enum class NonStandardReconcileException : uint8_t { SENTINEL };

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
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++calls_;
        }
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

    int Calls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

private:
    mutable std::mutex mutex_;
    int calls_{ 0 };
    std::once_flag enteredOnce_;
    std::promise<void> enteredPromise_;
    std::once_flag releaseOnce_;
    std::promise<void> releasePromise_;
    std::shared_future<void> releaseFuture_;
};

class ScopedBlockingDiscoveryRelease {
public:
    explicit ScopedBlockingDiscoveryRelease(BlockingCoordinatorDiscovery &discovery) : discovery_(discovery)
    {
    }

    ~ScopedBlockingDiscoveryRelease()
    {
        discovery_.Release();
    }

private:
    BlockingCoordinatorDiscovery &discovery_;
};

class ThreadSafeMembershipDependencies {
public:
    using GetStatusAction = std::function<Status(CoordinatorRaftMembershipStatus &, int)>;
    using PeerAction = std::function<Status(const std::string &,
                                            const CoordinatorMembershipManager::MembershipOperationCallback &)>;

    CoordinatorMembershipManager::Dependencies Make()
    {
        return CoordinatorMembershipManager::Dependencies{
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

    void SetGetStatusAction(GetStatusAction action)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        getStatusAction_ = std::move(action);
    }

    void SetAddSubmissionResult(Status result)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        addSubmissionResult_ = std::move(result);
        inlineAddResult_.reset();
        addPeerAction_ = nullptr;
    }

    void SetAddPeerAction(PeerAction action)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        addSubmissionResult_ = Status::OK();
        inlineAddResult_.reset();
        addPeerAction_ = std::move(action);
    }

    void SetInlineAddResult(Status result)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        addSubmissionResult_ = Status::OK();
        inlineAddResult_ = std::move(result);
        addPeerAction_ = nullptr;
    }

    void SetRemoveSubmissionResult(Status result)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        removeSubmissionResult_ = std::move(result);
        inlineRemoveResult_.reset();
        removePeerAction_ = nullptr;
    }

    void SetRemovePeerAction(PeerAction action)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        removeSubmissionResult_ = Status::OK();
        inlineRemoveResult_.reset();
        removePeerAction_ = std::move(action);
    }

    void SetInlineRemoveResult(Status result)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        removeSubmissionResult_ = Status::OK();
        inlineRemoveResult_ = std::move(result);
        removePeerAction_ = nullptr;
    }

    bool CompleteAdd(const Status &result, int repetitions = 1)
    {
        CoordinatorMembershipManager::MembershipOperationCallback callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback = addCallback_;
        }
        if (!callback) {
            return false;
        }
        for (int call = 0; call < repetitions; ++call) {
            callback(result);
        }
        return true;
    }

    bool CompleteRemove(const Status &result, int repetitions = 1)
    {
        CoordinatorMembershipManager::MembershipOperationCallback callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback = removeCallback_;
        }
        if (!callback) {
            return false;
        }
        for (int call = 0; call < repetitions; ++call) {
            callback(result);
        }
        return true;
    }

    std::future<void> GetFirstStatusFuture()
    {
        return firstGetStatus_.get_future();
    }

    bool WaitForGetStatusCalls(int expectedCalls, TimePoint deadline)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return callsCv_.wait_until(lock, deadline, [this, expectedCalls] { return getStatusCalls_ >= expectedCalls; });
    }

    bool WaitForAddPeerCalls(int expectedCalls, TimePoint deadline)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return callsCv_.wait_until(lock, deadline, [this, expectedCalls] { return addPeerCalls_ >= expectedCalls; });
    }

    bool WaitForRemovePeerCalls(int expectedCalls, TimePoint deadline)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return callsCv_.wait_until(lock, deadline,
                                   [this, expectedCalls] { return removePeerCalls_ >= expectedCalls; });
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

    std::string LastRemovedPeer() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return removedPeers_.empty() ? std::string() : removedPeers_.back();
    }

private:
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
        if (action) {
            return action(status, callNumber);
        }
        return result;
    }

    Status AddPeer(const std::string &peer,
                   const CoordinatorMembershipManager::MembershipOperationCallback &callback)
    {
        Status submissionResult;
        std::optional<Status> inlineResult;
        PeerAction action;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++addPeerCalls_;
            addedPeers_.emplace_back(peer);
            submissionResult = addSubmissionResult_;
            inlineResult = inlineAddResult_;
            action = addPeerAction_;
            if (submissionResult.IsOk() && !action) {
                addCallback_ = callback;
            }
        }
        callsCv_.notify_all();
        if (action) {
            return action(peer, callback);
        }
        if (inlineResult.has_value()) {
            callback(*inlineResult);
        }
        return submissionResult;
    }

    Status RemovePeer(const std::string &peer,
                      const CoordinatorMembershipManager::MembershipOperationCallback &callback)
    {
        Status submissionResult;
        std::optional<Status> inlineResult;
        PeerAction action;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++removePeerCalls_;
            removedPeers_.emplace_back(peer);
            submissionResult = removeSubmissionResult_;
            inlineResult = inlineRemoveResult_;
            action = removePeerAction_;
            if (submissionResult.IsOk() && !action) {
                removeCallback_ = callback;
            }
        }
        callsCv_.notify_all();
        if (action) {
            return action(peer, callback);
        }
        if (inlineResult.has_value()) {
            callback(*inlineResult);
        }
        return submissionResult;
    }

    mutable std::mutex mutex_;
    std::condition_variable callsCv_;
    CoordinatorRaftMembershipStatus status_;
    Status getStatusResult_;
    GetStatusAction getStatusAction_;
    PeerAction addPeerAction_;
    PeerAction removePeerAction_;
    Status addSubmissionResult_;
    std::optional<Status> inlineAddResult_;
    Status removeSubmissionResult_;
    std::optional<Status> inlineRemoveResult_;
    CoordinatorMembershipManager::MembershipOperationCallback addCallback_;
    CoordinatorMembershipManager::MembershipOperationCallback removeCallback_;
    std::vector<std::string> addedPeers_;
    std::vector<std::string> removedPeers_;
    int getStatusCalls_{ 0 };
    int addPeerCalls_{ 0 };
    int removePeerCalls_{ 0 };
    bool firstGetStatusSignaled_{ false };
    std::promise<void> firstGetStatus_;
};

class BlockingGetStatusState {
public:
    BlockingGetStatusState() : releaseFuture_(releasePromise_.get_future().share())
    {
    }

    Status Block()
    {
        std::call_once(enteredOnce_, [this] { enteredPromise_.set_value(); });
        const auto deadline = Clock::now() + kBlockingGetStatusWaitBudget;
        if (releaseFuture_.wait_until(deadline) != std::future_status::ready) {
            return Status(K_RUNTIME_ERROR, "Blocked Coordinator membership test dependency timed out");
        }
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

class ScopedBlockingGetStatusRelease {
public:
    explicit ScopedBlockingGetStatusRelease(BlockingGetStatusState &state) : state_(state)
    {
    }

    ~ScopedBlockingGetStatusRelease()
    {
        state_.Release();
    }

private:
    BlockingGetStatusState &state_;
};

CoordinatorFollowerStatus Follower(std::string peer, bool valid, int errors)
{
    return CoordinatorFollowerStatus{ std::move(peer), valid, errors };
}

CoordinatorRaftMembershipStatus LeaderStatus(std::vector<std::string> peers,
                                             std::vector<CoordinatorFollowerStatus> followers,
                                             int64_t index = kObservedConfigurationIndex,
                                             int64_t term = kObservedFollowerTerm)
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

CoordinatorRaftMembershipStatus FailedFullStatus(int64_t index = kObservedConfigurationIndex,
                                                 int64_t term = kObservedFollowerTerm)
{
    return LeaderStatus({ kPeer1, kPeer2, kPeer3 },
                        { Follower(kPeer2, false, kSuspectedFailureErrors), Follower(kPeer3, true, 0) }, index, term);
}

CoordinatorRaftMembershipStatus ReplacementCommittedStatus(CoordinatorFollowerStatus failedPeerObservation,
                                                            int64_t index = kObservedConfigurationIndex + 1,
                                                            int64_t term = kObservedFollowerTerm)
{
    return LeaderStatus({ kPeer1, kPeer2, kPeer3, kCandidate4 },
                        { std::move(failedPeerObservation), Follower(kPeer3, true, 0),
                          Follower(kCandidate4, true, 0) },
                        index, term);
}

CoordinatorRaftMembershipStatus ReplacementCompletedStatus(int64_t index = kObservedConfigurationIndex + 2,
                                                            int64_t term = kObservedFollowerTerm)
{
    return LeaderStatus({ kPeer1, kPeer3, kCandidate4 },
                        { Follower(kPeer3, true, 0), Follower(kCandidate4, true, 0) }, index, term);
}

CoordinatorMembershipOptions ValidOptions(size_t expectedMemberCount = kExpectedMemberCount)
{
    return CoordinatorMembershipOptions{ expectedMemberCount,
                                         kHealthCheckInterval,
                                         kMemberFailureGrace,
                                         kDiscoveryRetryInterval,
                                         kOperationWarningTimeout,
                                         kCandidateRetryCooldown };
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

size_t CountOccurrences(const std::string &text, const std::string &needle)
{
    size_t count = 0;
    size_t position = 0;
    while ((position = text.find(needle, position)) != std::string::npos) {
        ++count;
        position += needle.size();
    }
    return count;
}

void ExpectNoMembershipCalls(const ThreadSafeMembershipDependencies &dependencies,
                             const ThreadSafeCoordinatorDiscovery &discovery)
{
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_EQ(discovery.Calls(), 0);
}

void ExpectReconcileOk(CoordinatorMembershipManager &manager)
{
    const auto status = manager.ReconcileOnce();
    EXPECT_TRUE(status.IsOk()) << status.ToString();
}

TEST(CoordinatorMembershipManagerTest, RejectsEveryInvalidOptionFieldAndRelation)
{
    struct InvalidOptionsCase {
        const char *name;
        const char *expectedDiagnostic;
        std::function<void(CoordinatorMembershipOptions &)> invalidate;
    };
    const std::vector<InvalidOptionsCase> cases{
        { "zero expected member count", "expectedMemberCount must be positive",
          [](auto &options) { options.expectedMemberCount = 0; } },
        { "zero health interval", "healthCheckInterval must be positive",
          [](auto &options) { options.healthCheckInterval = kZeroDuration; } },
        { "negative health interval", "healthCheckInterval must be positive",
          [](auto &options) { options.healthCheckInterval = kNegativeDuration; } },
        { "zero failure grace", "memberFailureGrace must be positive",
          [](auto &options) { options.memberFailureGrace = kZeroDuration; } },
        { "negative failure grace", "memberFailureGrace must be positive",
          [](auto &options) { options.memberFailureGrace = kNegativeDuration; } },
        { "zero discovery retry", "discoveryRetryInterval must be positive",
          [](auto &options) { options.discoveryRetryInterval = kZeroDuration; } },
        { "negative discovery retry", "discoveryRetryInterval must be positive",
          [](auto &options) { options.discoveryRetryInterval = kNegativeDuration; } },
        { "zero operation warning", "operationWarningTimeout must be positive",
          [](auto &options) { options.operationWarningTimeout = kZeroDuration; } },
        { "negative operation warning", "operationWarningTimeout must be positive",
          [](auto &options) { options.operationWarningTimeout = kNegativeDuration; } },
        { "zero candidate cooldown", "candidateRetryCooldown must be positive",
          [](auto &options) { options.candidateRetryCooldown = kZeroDuration; } },
        { "negative candidate cooldown", "candidateRetryCooldown must be positive",
          [](auto &options) { options.candidateRetryCooldown = kNegativeDuration; } },
        { "health equals failure grace", "healthCheckInterval must be less than memberFailureGrace",
          [](auto &options) { options.memberFailureGrace = options.healthCheckInterval; } },
        { "health exceeds failure grace", "healthCheckInterval must be less than memberFailureGrace",
          [](auto &options) { options.memberFailureGrace = kHealthCheckInterval / 2; } },
        { "health exceeds discovery retry", "healthCheckInterval must not exceed discoveryRetryInterval",
          [](auto &options) { options.discoveryRetryInterval = kHealthCheckInterval / 2; } },
    };

    for (const auto &testCase : cases) {
        SCOPED_TRACE(testCase.name);
        auto options = ValidOptions();
        testCase.invalidate(options);
        EXPECT_FALSE(options.IsValid());

        ThreadSafeMembershipDependencies dependencies;
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        auto manager = MakeManager(options, dependencies, discovery);
        const auto startStatus = manager.Start();
        EXPECT_EQ(startStatus.GetCode(), K_INVALID);
        EXPECT_NE(startStatus.GetMsg().find(testCase.expectedDiagnostic), std::string::npos);
        EXPECT_EQ(dependencies.GetStatusCalls(), 0);
        ExpectNoMembershipCalls(dependencies, *discovery);
    }
}

TEST(CoordinatorMembershipManagerTest, RejectsNullDiscovery)
{
    auto options = ValidOptions();
    ASSERT_TRUE(options.IsValid());
    ThreadSafeMembershipDependencies dependencies;
    auto manager = MakeManager(options, dependencies, nullptr);

    EXPECT_EQ(manager.Start().GetCode(), K_INVALID);
    EXPECT_EQ(dependencies.GetStatusCalls(), 0);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, StartsOnceAndShutsDownIdempotentlyWithoutMembershipCalls)
{
    ThreadSafeMembershipDependencies dependencies;
    auto firstStatus = dependencies.GetFirstStatusFuture();
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery);

    ASSERT_TRUE(manager.Start().IsOk());
    EXPECT_EQ(manager.Start().GetCode(), K_INVALID);
    ASSERT_EQ(firstStatus.wait_for(kLifecycleDeadline), std::future_status::ready);
    EXPECT_TRUE(manager.Shutdown().IsOk());
    EXPECT_TRUE(manager.Shutdown().IsOk());
    EXPECT_EQ(manager.Start().GetCode(), K_INVALID);
    EXPECT_GE(dependencies.GetStatusCalls(), 1);
    ExpectNoMembershipCalls(dependencies, *discovery);
}

TEST(CoordinatorMembershipManagerTest, ShutdownInterruptsLongHealthWaitAndJoinsWithinDeadline)
{
    auto options = LongWaitOptions();
    ASSERT_TRUE(options.IsValid());

    ThreadSafeMembershipDependencies dependencies;
    auto firstStatus = dependencies.GetFirstStatusFuture();
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(options, dependencies, discovery);
    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(firstStatus.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    ASSERT_EQ(shutdown.wait_for(kLifecycleDeadline), std::future_status::ready);
    EXPECT_TRUE(shutdown.get().IsOk());
    ExpectNoMembershipCalls(dependencies, *discovery);
}

TEST(CoordinatorMembershipManagerTest, ReconcileExceptionsAreContainedAndStandardDetailsAreLogged)
{
    std::promise<void> successfulCallPromise;
    auto successfulCall = successfulCallPromise.get_future();
    std::once_flag successfulCallOnce;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetGetStatusAction(
        [&successfulCallPromise, &successfulCallOnce](CoordinatorRaftMembershipStatus &, int callNumber) {
            if (callNumber == kRuntimeExceptionCall) {
                throw std::runtime_error(kSensitiveExceptionText);
            }
            if (callNumber == kNonStandardExceptionCall) {
                throw NonStandardReconcileException::SENTINEL;
            }
            if (callNumber == kSuccessfulCallAfterExceptions) {
                std::call_once(successfulCallOnce, [&successfulCallPromise] { successfulCallPromise.set_value(); });
            }
            return Status::OK();
        });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery);

    testing::internal::CaptureStderr();
    const auto startStatus = manager.Start();
    const auto successfulCallWait = successfulCall.wait_for(kLifecycleDeadline);
    const auto shutdownStatus = manager.Shutdown();
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    EXPECT_TRUE(startStatus.IsOk()) << startStatus.ToString();
    EXPECT_EQ(successfulCallWait, std::future_status::ready);
    EXPECT_TRUE(shutdownStatus.IsOk()) << shutdownStatus.ToString();
    EXPECT_GE(dependencies.GetStatusCalls(), kSuccessfulCallAfterExceptions);
    EXPECT_EQ(CountOccurrences(capturedStderr, kReconcileExceptionMarker), 2U);
    EXPECT_NE(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
    EXPECT_EQ(startStatus.ToString().find(kSensitiveExceptionText), std::string::npos);
    EXPECT_EQ(shutdownStatus.ToString().find(kSensitiveExceptionText), std::string::npos);
    ExpectNoMembershipCalls(dependencies, *discovery);
}

TEST(CoordinatorMembershipManagerTest, ReentrantShutdownFromReconciliationThreadLeavesManagerRunning)
{
    std::promise<Status> reentrantShutdownPromise;
    auto reentrantShutdown = reentrantShutdownPromise.get_future();
    std::once_flag reentrantShutdownOnce;
    CoordinatorMembershipManager *managerPtr = nullptr;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetGetStatusAction(
        [&reentrantShutdownPromise, &reentrantShutdownOnce, &managerPtr](CoordinatorRaftMembershipStatus &, int) {
            std::call_once(reentrantShutdownOnce, [&reentrantShutdownPromise, &managerPtr] {
                reentrantShutdownPromise.set_value(managerPtr->Shutdown());
            });
            return Status::OK();
        });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
    managerPtr = &manager;

    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(reentrantShutdown.wait_for(kLifecycleDeadline), std::future_status::ready);
    const auto reentrantStatus = reentrantShutdown.get();

    bool mailboxStopping = true;
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        mailboxStopping = manager.completionMailbox_->stopping;
    }
    bool isRunning = false;
    bool ownsJoinableThread = false;
    bool threadIdPublished = false;
    {
        std::lock_guard<std::mutex> lock(manager.lifecycleMutex_);
        isRunning = manager.state_ == CoordinatorMembershipManager::LifecycleState::RUNNING;
        ownsJoinableThread = manager.thread_ != nullptr && manager.thread_->joinable();
        threadIdPublished = manager.reconciliationThreadId_ != std::thread::id{}
                            && manager.thread_ != nullptr
                            && manager.reconciliationThreadId_ == manager.thread_->get_id();
    }

    EXPECT_EQ(reentrantStatus.GetCode(), K_INVALID);
    EXPECT_NE(reentrantStatus.GetMsg().find("reconciliation thread"), std::string::npos);
    EXPECT_FALSE(mailboxStopping);
    EXPECT_TRUE(isRunning);
    EXPECT_TRUE(ownsJoinableThread);
    EXPECT_TRUE(threadIdPublished);

    EXPECT_TRUE(manager.Shutdown().IsOk());
    bool isStopped = false;
    bool threadIdCleared = false;
    {
        std::lock_guard<std::mutex> lock(manager.lifecycleMutex_);
        isStopped = manager.state_ == CoordinatorMembershipManager::LifecycleState::STOPPED;
        threadIdCleared = manager.reconciliationThreadId_ == std::thread::id{};
    }
    EXPECT_TRUE(isStopped);
    EXPECT_TRUE(threadIdCleared);
    EXPECT_TRUE(manager.Shutdown().IsOk());
    ExpectNoMembershipCalls(dependencies, *discovery);
}

TEST(CoordinatorMembershipManagerTest, ReentrantShutdownRejectsWhileExternalShutdownOwnsJoin)
{
    BlockingGetStatusState blockingState;
    auto getStatusEntered = blockingState.GetEnteredFuture();
    std::promise<Status> reentrantShutdownPromise;
    auto reentrantShutdown = reentrantShutdownPromise.get_future();
    std::once_flag reentrantShutdownOnce;
    CoordinatorMembershipManager *managerPtr = nullptr;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetGetStatusAction(
        [&blockingState, &reentrantShutdownPromise, &reentrantShutdownOnce,
         &managerPtr](CoordinatorRaftMembershipStatus &, int) {
            const auto blockStatus = blockingState.Block();
            if (blockStatus.IsError()) {
                return blockStatus;
            }
            std::call_once(reentrantShutdownOnce, [&reentrantShutdownPromise, &managerPtr] {
                reentrantShutdownPromise.set_value(managerPtr->Shutdown());
            });
            return Status::OK();
        });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
    managerPtr = &manager;
    ScopedBlockingGetStatusRelease releaseOnExit(blockingState);

    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(getStatusEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto externalShutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    const auto stoppingDeadline = Clock::now() + kLifecycleDeadline;
    bool externalCallerOwnsJoin = false;
    bool threadIdPreserved = false;
    {
        std::unique_lock<std::mutex> lock(manager.lifecycleMutex_);
        externalCallerOwnsJoin = manager.lifecycleCv_.wait_until(lock, stoppingDeadline, [&manager] {
            return manager.state_ == CoordinatorMembershipManager::LifecycleState::STOPPING
                   && manager.thread_ == nullptr;
        });
        threadIdPreserved = manager.reconciliationThreadId_ != std::thread::id{};
    }

    blockingState.Release();
    const auto reentrantCompletionWait = reentrantShutdown.wait_for(kLifecycleDeadline);
    const auto externalCompletionWait = externalShutdown.wait_for(kLifecycleDeadline);

    EXPECT_TRUE(externalCallerOwnsJoin);
    EXPECT_TRUE(threadIdPreserved);
    ASSERT_EQ(reentrantCompletionWait, std::future_status::ready);
    ASSERT_EQ(externalCompletionWait, std::future_status::ready);
    const auto reentrantStatus = reentrantShutdown.get();
    EXPECT_EQ(reentrantStatus.GetCode(), K_INVALID);
    EXPECT_NE(reentrantStatus.GetMsg().find("reconciliation thread"), std::string::npos);
    EXPECT_TRUE(externalShutdown.get().IsOk());

    bool isStopped = false;
    bool threadIdCleared = false;
    {
        std::lock_guard<std::mutex> lock(manager.lifecycleMutex_);
        isStopped = manager.state_ == CoordinatorMembershipManager::LifecycleState::STOPPED;
        threadIdCleared = manager.reconciliationThreadId_ == std::thread::id{};
    }
    EXPECT_TRUE(isStopped);
    EXPECT_TRUE(threadIdCleared);
    EXPECT_TRUE(manager.Shutdown().IsOk());
    ExpectNoMembershipCalls(dependencies, *discovery);
}

TEST(CoordinatorMembershipManagerTest, ConcurrentShutdownWaitsForBlockedReconcileAndSharesSingleJoin)
{
    BlockingGetStatusState blockingState;
    auto getStatusEntered = blockingState.GetEnteredFuture();
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetGetStatusAction(
        [&blockingState](CoordinatorRaftMembershipStatus &, int) { return blockingState.Block(); });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
    ScopedBlockingGetStatusRelease releaseOnExit(blockingState);

    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(getStatusEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto firstShutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    const auto stoppingDeadline = Clock::now() + kLifecycleDeadline;
    bool firstCallerOwnsJoin = false;
    {
        std::unique_lock<std::mutex> lock(manager.lifecycleMutex_);
        firstCallerOwnsJoin = manager.lifecycleCv_.wait_until(lock, stoppingDeadline, [&manager] {
            return manager.state_ == CoordinatorMembershipManager::LifecycleState::STOPPING
                   && manager.thread_ == nullptr;
        });
    }

    auto secondCallerEnteredPromise = std::make_shared<std::promise<void>>();
    auto secondCallerEntered = secondCallerEnteredPromise->get_future();
    auto secondShutdown = std::async(std::launch::async, [&manager, secondCallerEnteredPromise] {
        secondCallerEnteredPromise->set_value();
        return manager.Shutdown();
    });
    const auto secondCallerEnteredWait = secondCallerEntered.wait_for(kLifecycleDeadline);
    const auto firstBlockedWait = firstShutdown.wait_for(kBlockedObservationWindow);
    const auto secondBlockedWait = secondShutdown.wait_for(kBlockedObservationWindow);

    blockingState.Release();
    const auto firstCompletionWait = firstShutdown.wait_for(kLifecycleDeadline);
    const auto secondCompletionWait = secondShutdown.wait_for(kLifecycleDeadline);

    EXPECT_TRUE(firstCallerOwnsJoin);
    EXPECT_EQ(secondCallerEnteredWait, std::future_status::ready);
    EXPECT_EQ(firstBlockedWait, std::future_status::timeout);
    EXPECT_EQ(secondBlockedWait, std::future_status::timeout);
    ASSERT_EQ(firstCompletionWait, std::future_status::ready);
    ASSERT_EQ(secondCompletionWait, std::future_status::ready);
    EXPECT_EQ(firstShutdown.get().GetCode(), K_OK);
    EXPECT_EQ(secondShutdown.get().GetCode(), K_OK);
    ExpectNoMembershipCalls(dependencies, *discovery);
}

TEST(CoordinatorMembershipManagerTest, DestructorDrainsRunningThreadWithinDeadline)
{
    auto scopeExit = std::async(std::launch::async, [] {
        ThreadSafeMembershipDependencies dependencies;
        auto firstStatus = dependencies.GetFirstStatusFuture();
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        {
            auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
            auto startStatus = manager.Start();
            if (startStatus.IsError()) {
                return startStatus;
            }
            if (firstStatus.wait_for(kLifecycleDeadline) != std::future_status::ready) {
                return Status(K_RUNTIME_ERROR, "Coordinator membership destructor test did not observe reconciliation");
            }
        }
        return Status::OK();
    });

    ASSERT_EQ(scopeExit.wait_for(kLifecycleDeadline), std::future_status::ready);
    EXPECT_EQ(scopeExit.get().GetCode(), K_OK);
}

TEST(CoordinatorMembershipManagerTest, CopyOperationsAreDeleted)
{
    EXPECT_FALSE(std::is_copy_constructible_v<CoordinatorMembershipManager>);
    EXPECT_FALSE(std::is_copy_assignable_v<CoordinatorMembershipManager>);
}

TEST(CoordinatorMembershipManagerTest, FollowerNeverCallsDiscoveryOrChangesMembership)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    auto status = HealthyFullStatus();
    status.isLeader = false;
    dependencies.SetStatus(status);
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ExpectNoMembershipCalls(dependencies, *discovery);
}

TEST(CoordinatorMembershipManagerTest, HealthyFullMembershipAndErrorExactlyThresholdDoNotCallDiscovery)
{
    for (const int errors : { 0, kCoordinatorFollowerFailureErrorThreshold }) {
        SCOPED_TRACE(errors);
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(HealthyFullStatus(errors));
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        ExpectNoMembershipCalls(dependencies, *discovery);
        EXPECT_TRUE(manager.failureSince_.empty());
    }
}

TEST(CoordinatorMembershipManagerTest, UnknownFollowerDoesNotCountHealthyOrStartFailureGrace)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus({ kPeer1, kPeer2, kPeer3 },
                                        { Follower(kPeer2, true, 0), Follower(kPeer3, false, 0) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ExpectNoMembershipCalls(dependencies, *discovery);
    EXPECT_TRUE(manager.failureSince_.empty());
}

TEST(CoordinatorMembershipManagerTest, NoKnownQuorumPreventsDiscovery)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus({ kPeer1, kPeer2, kPeer3 },
                                        { Follower(kPeer2, false, 0), Follower(kPeer3, false, 0) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(4), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ExpectNoMembershipCalls(dependencies, *discovery);
}

TEST(CoordinatorMembershipManagerTest, ActiveOperationPreventsAdditionalDiscoveryAndSubmission)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, DiscoveryRetryDeadlinePreventsEarlyRetry)
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
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);

    now.Advance(std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 2);
}

TEST(CoordinatorMembershipManagerTest, FailureGraceRequiresContinuousErrorsAboveThreshold)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer2, kPeer3 },
        { Follower(kPeer2, false, kSuspectedFailureErrors), Follower(kPeer3, true, 0) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    EXPECT_EQ(manager.failureSince_.count(kPeer2), 1U);
    EXPECT_EQ(discovery->Calls(), 0);

    now.Advance(kMemberFailureGrace - std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 0);

    now.Advance(std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ADDING_REPLACEMENT);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, RecoveryAndUnknownObservationEachResetFullFailureGrace)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    auto failed = LeaderStatus(
        { kPeer1, kPeer2, kPeer3 },
        { Follower(kPeer2, true, kSuspectedFailureErrors), Follower(kPeer3, true, 0) });
    dependencies.SetStatus(failed);
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace - std::chrono::milliseconds(1));
    dependencies.SetStatus(HealthyFullStatus());
    ExpectReconcileOk(manager);
    EXPECT_TRUE(manager.failureSince_.empty());

    dependencies.SetStatus(failed);
    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace - std::chrono::milliseconds(1));
    dependencies.SetStatus(
        LeaderStatus({ kPeer1, kPeer2, kPeer3 },
                     { Follower(kPeer2, false, kCoordinatorFollowerFailureErrorThreshold),
                       Follower(kPeer3, true, 0) }));
    ExpectReconcileOk(manager);
    EXPECT_TRUE(manager.failureSince_.empty());

    dependencies.SetStatus(failed);
    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace - std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 0);
    now.Advance(std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 1);
}

TEST(CoordinatorMembershipManagerTest, NewLeaderTermRestartsFailureGrace)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    auto failed = LeaderStatus(
        { kPeer1, kPeer2, kPeer3 },
        { Follower(kPeer2, true, kSuspectedFailureErrors), Follower(kPeer3, true, 0) });
    dependencies.SetStatus(failed);
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    failed.term = kNextObservedTerm;
    dependencies.SetStatus(failed);
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 0);

    now.Advance(kMemberFailureGrace - std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 0);
    now.Advance(std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 1);
}

TEST(CoordinatorMembershipManagerTest, CandidateFilteringSkipsInvalidMembersDuplicatesAndCooldownWithoutPayloadLogs)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate5, kPeer1, kMalformedCandidate, kDomainCandidate, kIpv6Candidate,
                               kWildcardCandidate, kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);
    manager.candidateRetryAfter_[kCandidate4] = now.Get() + kCandidateRetryCooldown;

    testing::internal::CaptureStderr();
    ExpectReconcileOk(manager);
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate5);
    EXPECT_NE(capturedStderr.find(kInvalidCandidateMarker), std::string::npos);
    EXPECT_EQ(capturedStderr.find(kMalformedCandidate), std::string::npos);
    EXPECT_EQ(capturedStderr.find(kDomainCandidate), std::string::npos);
    EXPECT_EQ(capturedStderr.find(kIpv6Candidate), std::string::npos);
    EXPECT_EQ(capturedStderr.find(kWildcardCandidate), std::string::npos);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, CandidateChoiceIsLexicographicIndependentOfDiscoveryOrder)
{
    for (const auto &candidates : { std::vector<std::string>{ kCandidate5, kCandidate4 },
                                    std::vector<std::string>{ kCandidate4, kCandidate5 } }) {
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(VacancyStatus());
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates(candidates);
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate4);
    }
}

TEST(CoordinatorMembershipManagerTest, DiscoveryErrorAndEmptyResultDoNotCreateOperation)
{
    for (const bool returnError : { false, true }) {
        SCOPED_TRACE(returnError);
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(VacancyStatus());
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        if (returnError) {
            discovery->SetResult(Status(K_RUNTIME_ERROR, "scripted discovery failure"));
        }
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        EXPECT_FALSE(manager.activeOperation_.has_value());
        EXPECT_EQ(dependencies.AddPeerCalls(), 0);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
        EXPECT_EQ(discovery->Calls(), 1);
    }
}

TEST(CoordinatorMembershipManagerTest, DiscoveryResultCannotSubmitAfterObservedMembershipAdvances)
{
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<BlockingCoordinatorDiscovery>();
    auto discoveryEntered = discovery->GetEnteredFuture();
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
    ScopedBlockingDiscoveryRelease releaseOnExit(*discovery);

    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(discoveryEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto current = HealthyFullStatus();
    current.term = kNextObservedTerm;
    current.configurationIndex = kObservedConfigurationIndex + 1;
    dependencies.SetStatus(std::move(current));
    discovery->Release();

    EXPECT_TRUE(dependencies.WaitForGetStatusCalls(2, Clock::now() + kLifecycleDeadline));
    EXPECT_TRUE(manager.Shutdown().IsOk());
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
}

TEST(CoordinatorMembershipManagerTest, DiscoveryResultCannotAddAfterFailedFollowerRecovers)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    auto discovery = std::make_shared<BlockingCoordinatorDiscovery>();
    auto discoveryEntered = discovery->GetEnteredFuture();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    std::future<Status> reconciliation;
    ScopedBlockingDiscoveryRelease releaseOnExit(*discovery);
    reconciliation = std::async(std::launch::async, [&manager] { return manager.ReconcileOnce(); });
    ASSERT_EQ(discoveryEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    dependencies.SetStatus(HealthyFullStatus());
    discovery->Release();
    ASSERT_EQ(reconciliation.wait_for(kLifecycleDeadline), std::future_status::ready);
    const auto reconcileStatus = reconciliation.get();

    EXPECT_EQ(reconcileStatus.GetCode(), K_TRY_AGAIN) << reconcileStatus.ToString();
    EXPECT_NE(reconcileStatus.GetMsg().find("submission policy changed"), std::string::npos);
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
}

TEST(CoordinatorMembershipManagerTest, DiscoveryResultCannotAddAfterKnownQuorumIsLost)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<BlockingCoordinatorDiscovery>();
    auto discoveryEntered = discovery->GetEnteredFuture();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    std::future<Status> reconciliation;
    ScopedBlockingDiscoveryRelease releaseOnExit(*discovery);
    reconciliation = std::async(std::launch::async, [&manager] { return manager.ReconcileOnce(); });
    ASSERT_EQ(discoveryEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    dependencies.SetStatus(LeaderStatus({ kPeer1, kPeer2 }, { Follower(kPeer2, false, 0) }));
    discovery->Release();
    ASSERT_EQ(reconciliation.wait_for(kLifecycleDeadline), std::future_status::ready);
    const auto reconcileStatus = reconciliation.get();

    EXPECT_EQ(reconcileStatus.GetCode(), K_TRY_AGAIN) << reconcileStatus.ToString();
    EXPECT_NE(reconcileStatus.GetMsg().find("submission policy changed"), std::string::npos);
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
}

TEST(CoordinatorMembershipManagerTest, SubmissionSnapshotRejectsEveryStaleAddDimension)
{
    struct StaleSnapshotCase {
        const char *name;
        std::function<void(CoordinatorRaftMembershipStatus &)> change;
    };
    const std::vector<StaleSnapshotCase> cases{
        { "leadership", [](auto &status) { status.isLeader = false; } },
        { "term", [](auto &status) { status.term = kNextObservedTerm; } },
        { "configuration index", [](auto &status) { ++status.configurationIndex; } },
        { "committed peers", [](auto &status) { status.committedPeers = { kPeer1, kPeer3 }; } },
    };

    for (const auto &testCase : cases) {
        SCOPED_TRACE(testCase.name);
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(VacancyStatus());
        dependencies.SetGetStatusAction([&testCase](CoordinatorRaftMembershipStatus &status, int callNumber) {
            if (callNumber == 2) {
                testCase.change(status);
            }
            return Status::OK();
        });
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        const auto reconcileStatus = manager.ReconcileOnce();
        EXPECT_EQ(reconcileStatus.GetCode(), K_TRY_AGAIN) << reconcileStatus.ToString();
        EXPECT_NE(reconcileStatus.GetMsg().find("submission decision is stale"), std::string::npos);
        EXPECT_EQ(dependencies.GetStatusCalls(), 2);
        EXPECT_EQ(discovery->Calls(), 1);
        EXPECT_EQ(dependencies.AddPeerCalls(), 0);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
        EXPECT_FALSE(manager.activeOperation_.has_value());
        EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
    }
}

TEST(CoordinatorMembershipManagerTest, StaleSnapshotBeforeRemoveClearsIntentAndDoesNotSubmit)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    const auto expected = ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors));
    dependencies.SetStatus(expected);
    dependencies.SetGetStatusAction([](CoordinatorRaftMembershipStatus &status, int callNumber) {
        if (callNumber == 1) {
            status.committedPeers = { kPeer1, kPeer3, kCandidate4 };
        }
        return Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);
    manager.ownedReplacementIntent_.emplace(
        CoordinatorMembershipManager::OwnedReplacementIntent{ kCandidate4, kPeer2 });

    const auto submissionStatus = manager.SubmitFailedPeerRemoval(
        CoordinatorMembershipManager::CaptureSubmissionSnapshot(expected), kPeer2, now.Get());

    EXPECT_EQ(submissionStatus.GetCode(), K_TRY_AGAIN) << submissionStatus.ToString();
    EXPECT_EQ(dependencies.GetStatusCalls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
}

TEST(CoordinatorMembershipManagerTest, StaleTermOrConfigurationIndexPreservesProvenIntentForRollback)
{
    for (const bool changeTerm : { false, true }) {
        SCOPED_TRACE(changeTerm ? "term" : "configuration index");
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(FailedFullStatus());
        dependencies.SetGetStatusAction([changeTerm](CoordinatorRaftMembershipStatus &status, int callNumber) {
            if (callNumber == kRemovalPolicyRevalidationCall) {
                if (changeTerm) {
                    status.term = kNextObservedTerm;
                } else {
                    ++status.configurationIndex;
                }
            }
            return Status::OK();
        });
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        ASSERT_EQ(dependencies.AddPeerCalls(), 1);
        ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());

        dependencies.SetStatus(
            ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));
        const auto staleSubmission = manager.ReconcileOnce();

        EXPECT_EQ(staleSubmission.GetCode(), K_TRY_AGAIN) << staleSubmission.ToString();
        EXPECT_NE(staleSubmission.GetMsg().find("submission decision is stale"), std::string::npos);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
        EXPECT_FALSE(manager.activeOperation_.has_value());
        ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());
        EXPECT_EQ(manager.ownedReplacementIntent_->candidate, kCandidate4);
        EXPECT_EQ(manager.ownedReplacementIntent_->failedPeer, kPeer2);

        dependencies.SetStatus(ReplacementCommittedStatus(
            Follower(kPeer2, true, 0),
            changeTerm ? kObservedConfigurationIndex + 1 : kObservedConfigurationIndex + 2,
            changeTerm ? kNextObservedTerm : kObservedFollowerTerm));
        ExpectReconcileOk(manager);

        ASSERT_TRUE(manager.activeOperation_.has_value());
        EXPECT_EQ(manager.activeOperation_->submittedStage,
                  CoordinatorMembershipManager::OperationStage::ROLLING_BACK);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
        EXPECT_EQ(dependencies.LastRemovedPeer(), kCandidate4);
        EXPECT_TRUE(manager.ownedReplacementIntent_.has_value());
    }
}

TEST(CoordinatorMembershipManagerTest, FreshHealthInvalidatesFailedRemovalAndRollbackBeforeSubmission)
{
    for (const bool rollback : { false, true }) {
        SCOPED_TRACE(rollback ? "rollback" : "failed removal");
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(FailedFullStatus());
        dependencies.SetGetStatusAction([rollback](CoordinatorRaftMembershipStatus &status, int callNumber) {
            if (callNumber == kRemovalPolicyRevalidationCall) {
                status = ReplacementCommittedStatus(
                    rollback ? Follower(kPeer2, false, 0) : Follower(kPeer2, true, 0));
            }
            return Status::OK();
        });
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        ASSERT_EQ(dependencies.AddPeerCalls(), 1);
        ASSERT_TRUE(manager.activeOperation_.has_value());
        ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());

        dependencies.SetStatus(ReplacementCommittedStatus(
            rollback ? Follower(kPeer2, true, 0) : Follower(kPeer2, false, kSuspectedFailureErrors)));
        const auto reconcileStatus = manager.ReconcileOnce();

        EXPECT_EQ(reconcileStatus.GetCode(), K_TRY_AGAIN) << reconcileStatus.ToString();
        EXPECT_NE(reconcileStatus.GetMsg().find("submission policy changed"), std::string::npos);
        EXPECT_EQ(dependencies.GetStatusCalls(), kRemovalPolicyRevalidationCall);
        EXPECT_EQ(dependencies.AddPeerCalls(), 1);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
        EXPECT_FALSE(manager.activeOperation_.has_value());
        EXPECT_TRUE(manager.ownedReplacementIntent_.has_value());
    }
}

TEST(CoordinatorMembershipManagerTest, VacancyAddsExactlyOneCandidateAndNeverRemoves)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate4);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->stage, CoordinatorMembershipManager::OperationStage::ADDING_VACANCY);
}

TEST(CoordinatorMembershipManagerTest, VacancyRemainsAddOnlyWithConfirmedFailedCommittedPeer)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(
        LeaderStatus({ kPeer1, kPeer2, kPeer3 },
                     { Follower(kPeer2, true, 0), Follower(kPeer3, true, kSuspectedFailureErrors) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(4), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    EXPECT_EQ(discovery->Calls(), 1);
    discovery->SetCandidates({ kCandidate4 });
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);

    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate4);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, InlineSuccessWaitsForNewerCommittedConfiguration)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetInlineAddResult(Status::OK());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    ASSERT_TRUE(manager.activeOperation_->completionStatus.has_value());
    EXPECT_TRUE(manager.activeOperation_->completionStatus->IsOk());
    EXPECT_EQ(manager.activeOperation_->completionOrigin,
              CoordinatorMembershipManager::CompletionOrigin::CALLBACK);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);

    dependencies.SetStatus(LeaderStatus({ kPeer1, kPeer2, kCandidate4 },
                                        { Follower(kPeer2, true, 0), Follower(kCandidate4, true, 0) },
                                        kObservedConfigurationIndex + 1));
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
}

TEST(CoordinatorMembershipManagerTest, DeferredDuplicateCallbackPublishesOnlyFirstResult)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(dependencies.CompleteAdd(Status::OK(), 2));
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    ASSERT_TRUE(manager.activeOperation_->completionStatus.has_value());
    EXPECT_TRUE(manager.activeOperation_->completionStatus->IsOk());
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "late duplicate")));
    ExpectReconcileOk(manager);
    EXPECT_TRUE(manager.activeOperation_.has_value());
    EXPECT_TRUE(manager.activeOperation_->completionStatus->IsOk());
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, CallbackPublishedAfterStatusSnapshotWaitsForNextFreshStatus)
{
    ManualNow now;
    BlockingGetStatusState blockingState;
    auto statusSnapshotTaken = blockingState.GetEnteredFuture();
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetGetStatusAction([&blockingState](CoordinatorRaftMembershipStatus &, int callNumber) {
        return callNumber == 3 ? blockingState.Block() : Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);
    ScopedBlockingGetStatusRelease releaseOnExit(blockingState);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());

    auto staleStatusPass = std::async(std::launch::async, [&manager] { return manager.ReconcileOnce(); });
    const auto snapshotWait = statusSnapshotTaken.wait_for(kLifecycleDeadline);
    bool callbackPublished = false;
    if (snapshotWait == std::future_status::ready) {
        callbackPublished = dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "post-snapshot callback"));
    }
    blockingState.Release();
    const auto stalePassWait = staleStatusPass.wait_for(kLifecycleDeadline);

    EXPECT_EQ(snapshotWait, std::future_status::ready);
    EXPECT_TRUE(callbackPublished);
    ASSERT_EQ(stalePassWait, std::future_status::ready);
    EXPECT_TRUE(staleStatusPass.get().IsOk());
    EXPECT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 0U);
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_TRUE(manager.completionMailbox_->result.has_value());
    }

    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 1U);
    EXPECT_EQ(dependencies.GetStatusCalls(), 4);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, ClaimedCompletionSurvivesOneStatusFailure)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetGetStatusAction([](CoordinatorRaftMembershipStatus &, int callNumber) {
        return callNumber == 3 ? Status(K_RUNTIME_ERROR, "scripted status failure") : Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "claimed callback")));

    const auto failedPass = manager.ReconcileOnce();
    EXPECT_EQ(failedPass.GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 0U);
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        ASSERT_TRUE(manager.completionMailbox_->result.has_value());
        EXPECT_EQ(manager.completionMailbox_->result->generation, manager.activeOperation_->generation);
    }

    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 1U);
    EXPECT_EQ(dependencies.GetStatusCalls(), 4);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, ClaimedCompletionSurvivesNowFunctionException)
{
    ManualNow now;
    bool throwNextNow = false;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    CoordinatorMembershipManager manager(
        ValidOptions(), dependencies.Make(), discovery, [&now, &throwNextNow]() -> TimePoint {
            if (throwNextNow) {
                throwNextNow = false;
                throw std::runtime_error("scripted NowFunction failure");
            }
            return now.Get();
        });

    ExpectReconcileOk(manager);
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "claimed Add callback failure")));
    throwNextNow = true;
    EXPECT_THROW(static_cast<void>(manager.ReconcileOnce()), std::runtime_error);

    ASSERT_TRUE(manager.activeOperation_.has_value());
    ASSERT_TRUE(manager.activeOperation_->completionStatus.has_value());
    EXPECT_TRUE(manager.activeOperation_->completionStatus->IsError());
    EXPECT_EQ(manager.activeOperation_->completionOrigin,
              CoordinatorMembershipManager::CompletionOrigin::CALLBACK);
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }

    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 1U);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, SynchronousAddSubmissionFailureUsesMailboxAndCoolsCandidate)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetAddSubmissionResult(Status(K_RUNTIME_ERROR, "synchronous add failure"));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        ASSERT_TRUE(manager.completionMailbox_->result.has_value());
        EXPECT_EQ(manager.completionMailbox_->result->origin,
                  CoordinatorMembershipManager::CompletionOrigin::SUBMISSION_REJECTED);
    }
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 1U);
    EXPECT_EQ(dependencies.GetStatusCalls(), 3);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest,
     VacancySubmissionRejectedReleasesAgainstAdvancedDifferentConfigurationAndWaitsForRetryDeadline)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetAddSubmissionResult(Status(K_RUNTIME_ERROR, "synchronous add failure"));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    dependencies.SetStatus(LeaderStatus({ kPeer1, kPeer3 }, { Follower(kPeer3, true, 0) },
                                        kObservedConfigurationIndex + 1));
    ExpectReconcileOk(manager);

    EXPECT_FALSE(manager.activeOperation_.has_value());
    ASSERT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 1U);
    EXPECT_EQ(manager.candidateRetryAfter_.at(kCandidate4), now.Get() + kCandidateRetryCooldown);
    EXPECT_EQ(manager.nextDiscoveryAt_, now.Get() + kDiscoveryRetryInterval);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);

    now.Advance(kDiscoveryRetryInterval - std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(discovery->Calls(), 1);

    dependencies.SetAddSubmissionResult(Status::OK());
    now.Advance(std::chrono::milliseconds(1));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.AddPeerCalls(), 2);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate5);
    EXPECT_EQ(discovery->Calls(), 2);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, CallbackErrorAcceptsNewerCommittedAddAsAuthority)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "callback error after commit")));
    dependencies.SetStatus(LeaderStatus({ kPeer1, kPeer2, kCandidate4 },
                                        { Follower(kPeer2, true, 0), Follower(kCandidate4, true, 0) },
                                        kObservedConfigurationIndex + 1));
    ExpectReconcileOk(manager);

    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 0U);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, CallbackErrorWithUnchangedConfigurationCoolsCandidateAndSchedulesRetry)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "add failed")));
    ExpectReconcileOk(manager);

    EXPECT_FALSE(manager.activeOperation_.has_value());
    ASSERT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 1U);
    EXPECT_EQ(manager.nextDiscoveryAt_, now.Get() + kDiscoveryRetryInterval);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, CandidatePresenceWithoutConfigurationIndexAdvanceDoesNotCompleteAdd)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetInlineAddResult(Status::OK());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    dependencies.SetStatus(LeaderStatus({ kPeer1, kPeer2, kCandidate4 },
                                        { Follower(kPeer2, true, 0), Follower(kCandidate4, true, 0) },
                                        kObservedConfigurationIndex));
    ExpectReconcileOk(manager);
    EXPECT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, CandidateCooldownAllowsSecondCandidateOnLaterRetry)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate4);
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "first candidate failed")));
    ExpectReconcileOk(manager);
    ASSERT_FALSE(manager.activeOperation_.has_value());

    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.AddPeerCalls(), 2);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate5);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, OperationWarningTimeoutEntersUncertainWithoutReleasingSingleFlight)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kOperationWarningTimeout);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->stage, CoordinatorMembershipManager::OperationStage::UNCERTAIN);
    EXPECT_TRUE(manager.activeOperation_->warningEmitted);

    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    EXPECT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, TwoUncertainOperationsEachLogOneStructuredWarning)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    ASSERT_EQ(manager.activeOperation_->generation, 1U);

    testing::internal::CaptureStderr();
    now.Advance(kOperationWarningTimeout);
    ExpectReconcileOk(manager);
    ExpectReconcileOk(manager);
    const bool firstCallbackPublished =
        dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "first uncertain operation failed"));
    ExpectReconcileOk(manager);
    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    now.Advance(kOperationWarningTimeout);
    ExpectReconcileOk(manager);
    ExpectReconcileOk(manager);
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    EXPECT_TRUE(firstCallbackPublished);
    EXPECT_EQ(CountOccurrences(capturedStderr, kOperationWarningMarker), 2U);
    EXPECT_EQ(CountOccurrences(capturedStderr, "group=datasystem-coordinator"), 2U);
    EXPECT_EQ(CountOccurrences(capturedStderr, "stage=ADDING_VACANCY"), 2U);
    EXPECT_EQ(CountOccurrences(capturedStderr, "failed_peer=none"), 2U);
    EXPECT_EQ(CountOccurrences(capturedStderr, "term=7"), 2U);
    EXPECT_EQ(CountOccurrences(capturedStderr, "starting_configuration_index=11"), 2U);
    EXPECT_EQ(CountOccurrences(capturedStderr, "generation=1"), 1U);
    EXPECT_EQ(CountOccurrences(capturedStderr, "generation=2"), 1U);
    EXPECT_NE(capturedStderr.find(std::string("candidate=") + kCandidate4), std::string::npos);
    EXPECT_NE(capturedStderr.find(std::string("candidate=") + kCandidate5), std::string::npos);
    EXPECT_EQ(capturedStderr.find("first uncertain operation failed"), std::string::npos);
    EXPECT_EQ(dependencies.AddPeerCalls(), 2);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, CallbackWakesRunThreadBeforeLongHealthInterval)
{
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);

    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_TRUE(dependencies.WaitForAddPeerCalls(1, Clock::now() + kLifecycleDeadline));
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "wake reconciliation")));
    EXPECT_TRUE(dependencies.WaitForGetStatusCalls(2, Clock::now() + kLifecycleDeadline));
    EXPECT_TRUE(manager.Shutdown().IsOk());
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, ShutdownWhileStatusBlockedPreventsNewDiscovery)
{
    BlockingGetStatusState blockingState;
    auto secondStatusBlocked = blockingState.GetEnteredFuture();
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    dependencies.SetGetStatusAction([&blockingState](CoordinatorRaftMembershipStatus &status, int callNumber) {
        if (callNumber == 1) {
            status = HealthyFullStatus();
            return Status::OK();
        }
        return callNumber == 2 ? blockingState.Block() : Status::OK();
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery);
    ScopedBlockingGetStatusRelease releaseOnExit(blockingState);

    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(secondStatusBlocked.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    bool mailboxStopping = false;
    {
        std::unique_lock<std::mutex> lock(manager.completionMailbox_->mutex);
        mailboxStopping = manager.completionMailbox_->cv.wait_until(
            lock, Clock::now() + kLifecycleDeadline, [&manager] { return manager.completionMailbox_->stopping; });
    }
    blockingState.Release();
    const auto shutdownWait = shutdown.wait_for(kLifecycleDeadline);

    EXPECT_TRUE(mailboxStopping);
    ASSERT_EQ(shutdownWait, std::future_status::ready);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_GE(dependencies.GetStatusCalls(), 2);
    EXPECT_EQ(discovery->Calls(), 0);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, ShutdownBeforeMailboxReservationPreventsAddAfterBlockedDiscovery)
{
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<BlockingCoordinatorDiscovery>();
    auto discoveryEntered = discovery->GetEnteredFuture();
    auto manager = MakeManager(LongWaitOptions(), dependencies, discovery);
    ScopedBlockingDiscoveryRelease releaseOnExit(*discovery);

    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_EQ(discoveryEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    const auto stoppingDeadline = Clock::now() + kLifecycleDeadline;
    bool mailboxStopping = false;
    {
        std::unique_lock<std::mutex> lock(manager.completionMailbox_->mutex);
        mailboxStopping = manager.completionMailbox_->cv.wait_until(
            lock, stoppingDeadline, [&manager] { return manager.completionMailbox_->stopping; });
    }
    bool lifecycleStopping = false;
    {
        std::unique_lock<std::mutex> lock(manager.lifecycleMutex_);
        lifecycleStopping = manager.lifecycleCv_.wait_until(lock, stoppingDeadline, [&manager] {
            return manager.state_ == CoordinatorMembershipManager::LifecycleState::STOPPING
                   && manager.thread_ == nullptr;
        });
    }

    discovery->Release();
    const auto shutdownWait = shutdown.wait_for(kLifecycleDeadline);

    EXPECT_TRUE(lifecycleStopping);
    EXPECT_TRUE(mailboxStopping);
    ASSERT_EQ(shutdownWait, std::future_status::ready);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_EQ(manager.completionMailbox_->activeGeneration, 0U);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }
}

TEST(CoordinatorMembershipManagerTest, TermChangeInvalidatesOldOperationAndLateCallback)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    const auto oldGeneration = manager.activeOperation_->generation;

    auto newTermStatus = HealthyFullStatus();
    newTermStatus.term = kNextObservedTerm;
    dependencies.SetStatus(newTermStatus);
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    ASSERT_TRUE(manager.observedTerm_.has_value());
    EXPECT_EQ(*manager.observedTerm_, kNextObservedTerm);

    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "old generation callback")));
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_NE(manager.completionMailbox_->activeGeneration, oldGeneration);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, LateCallbackAfterShutdownCannotMutateMailboxOrChainOperation)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(VacancyStatus());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    ASSERT_TRUE(manager.Shutdown().IsOk());
    EXPECT_FALSE(manager.activeOperation_.has_value());
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "late callback")));

    EXPECT_FALSE(manager.activeOperation_.has_value());
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_TRUE(manager.completionMailbox_->stopping);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, FullReplacementCommitsSpareBeforeExactFailedRemovalAndFinalCompletion)
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
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ADDING_REPLACEMENT);
    EXPECT_EQ(manager.activeOperation_->failedPeer, kPeer2);
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);

    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    ASSERT_TRUE(dependencies.CompleteAdd(Status(K_RUNTIME_ERROR, "callback error after committed add")));
    dependencies.SetStatus(
        ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::REMOVING_FAILED);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    EXPECT_EQ(manager.activeOperation_->candidate, kCandidate4);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_EQ(manager.activeOperation_->targetPeer, kPeer2);
    EXPECT_EQ(manager.activeOperation_->generation, 2U);
    EXPECT_FALSE(manager.activeOperation_->completionStatus.has_value());
    ASSERT_TRUE(dependencies.CompleteAdd(Status::OK()));
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_->completionStatus.has_value());

    ASSERT_TRUE(dependencies.CompleteRemove(Status::OK()));
    ExpectReconcileOk(manager);
    EXPECT_TRUE(manager.activeOperation_.has_value());
    dependencies.SetStatus(ReplacementCompletedStatus());
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, ReplacementDiscoveryAndAddFailuresNeverRemove)
{
    for (const int failureMode : { 0, 1, 2 }) {
        SCOPED_TRACE(failureMode);
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(FailedFullStatus());
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        if (failureMode == 1) {
            discovery->SetResult(Status(K_RUNTIME_ERROR, "scripted replacement discovery failure"));
        } else if (failureMode == 2) {
            discovery->SetCandidates({ kCandidate4 });
            dependencies.SetAddSubmissionResult(Status(K_RUNTIME_ERROR, "scripted replacement add failure"));
        }
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        if (failureMode == 2) {
            ASSERT_TRUE(manager.activeOperation_.has_value());
            ExpectReconcileOk(manager);
            EXPECT_FALSE(manager.activeOperation_.has_value());
            EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
            EXPECT_EQ(manager.candidateRetryAfter_.count(kCandidate4), 1U);
        }
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    }
}

TEST(CoordinatorMembershipManagerTest, ReplacementSubmissionRejectedNeverOwnsConcurrentHealthyCandidate)
{
    for (const bool changeTerm : { false, true }) {
        SCOPED_TRACE(changeTerm);
        ManualNow now;
        CoordinatorMembershipManager::MembershipOperationCallback lateAddCallback;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(FailedFullStatus());
        dependencies.SetAddPeerAction(
            [&lateAddCallback](const std::string &,
                               const CoordinatorMembershipManager::MembershipOperationCallback &callback) {
                lateAddCallback = callback;
                return Status(K_RUNTIME_ERROR, kScriptedSubmissionFailure);
            });
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        ASSERT_TRUE(manager.activeOperation_.has_value());
        ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());
        ASSERT_TRUE(static_cast<bool>(lateAddCallback));

        dependencies.SetStatus(ReplacementCommittedStatus(
            Follower(kPeer2, true, 0), kObservedConfigurationIndex + 1,
            changeTerm ? kNextObservedTerm : kObservedFollowerTerm));
        ExpectReconcileOk(manager);

        EXPECT_FALSE(manager.activeOperation_.has_value());
        EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
        EXPECT_EQ(dependencies.AddPeerCalls(), 1);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
        lateAddCallback(Status::OK());
        ExpectReconcileOk(manager);
        EXPECT_FALSE(manager.activeOperation_.has_value());
        EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
        {
            std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
            EXPECT_FALSE(manager.completionMailbox_->result.has_value());
        }
    }
}

TEST(CoordinatorMembershipManagerTest, ReplacementSubmissionRejectedDefersToLaterGenericFailedRemoval)
{
    ManualNow now;
    CoordinatorMembershipManager::MembershipOperationCallback lateAddCallback;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    dependencies.SetAddPeerAction(
        [&lateAddCallback](const std::string &,
                           const CoordinatorMembershipManager::MembershipOperationCallback &callback) {
            lateAddCallback = callback;
            return Status(K_RUNTIME_ERROR, kScriptedSubmissionFailure);
        });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());
    ASSERT_TRUE(static_cast<bool>(lateAddCallback));

    dependencies.SetStatus(
        ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);

    lateAddCallback(Status::OK());
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::REMOVING_FAILED);
    EXPECT_TRUE(manager.activeOperation_->candidate.empty());
    EXPECT_EQ(manager.activeOperation_->targetPeer, kPeer2);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, ReplacementCommittedWithoutQuorumCannotRemove)
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
    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer2, kPeer3, kCandidate4 },
        { Follower(kPeer2, false, kSuspectedFailureErrors), Follower(kPeer3, false, 0),
          Follower(kCandidate4, false, 0) },
        kObservedConfigurationIndex + 1));
    ASSERT_TRUE(dependencies.CompleteAdd(Status::OK()));
    ExpectReconcileOk(manager);

    EXPECT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ADDING_REPLACEMENT);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
}

TEST(CoordinatorMembershipManagerTest, ExplicitOldPeerRecoveryRollsBackOnlyOwnedCandidate)
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
    dependencies.SetStatus(ReplacementCommittedStatus(Follower(kPeer2, true, 0)));
    ExpectReconcileOk(manager);

    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ROLLING_BACK);
    EXPECT_EQ(manager.activeOperation_->failedPeer, kPeer2);
    EXPECT_EQ(manager.activeOperation_->candidate, kCandidate4);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->candidate);
    EXPECT_NE(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kCandidate4);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, UnknownOrNewlySuspectedOldPeerAfterCandidateCommitFailsClosed)
{
    for (const auto &observation : { Follower(kPeer2, false, 0),
                                     Follower(kPeer2, false, kSuspectedFailureErrors) }) {
        SCOPED_TRACE(observation.consecutiveErrorTimes);
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(FailedFullStatus());
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        dependencies.SetStatus(LeaderStatus(
            { kPeer1, kPeer2, kPeer3 }, { Follower(kPeer2, false, 0), Follower(kPeer3, true, 0) }));
        ExpectReconcileOk(manager);
        EXPECT_EQ(manager.failureSince_.count(kPeer2), 0U);

        dependencies.SetStatus(ReplacementCommittedStatus(observation));
        ExpectReconcileOk(manager);
        ASSERT_TRUE(manager.activeOperation_.has_value());
        EXPECT_EQ(manager.activeOperation_->submittedStage,
                  CoordinatorMembershipManager::OperationStage::ADDING_REPLACEMENT);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    }
}

TEST(CoordinatorMembershipManagerTest, VacancyWithFailureNeedsSecondCommittedSpareBeforeRemoval)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer2, kPeer3 },
        { Follower(kPeer2, true, 0), Follower(kPeer3, false, kSuspectedFailureErrors) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4, kCandidate5 });
    auto manager = MakeManager(ValidOptions(4), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ADDING_VACANCY);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate4);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);

    now.Advance(kMemberFailureGrace);
    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer2, kPeer3, kCandidate4 },
        { Follower(kPeer2, true, 0), Follower(kPeer3, false, kSuspectedFailureErrors),
          Follower(kCandidate4, true, 0) },
        kObservedConfigurationIndex + 1));
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);

    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ADDING_REPLACEMENT);
    EXPECT_EQ(dependencies.LastAddedPeer(), kCandidate5);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);

    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer2, kPeer3, kCandidate4, kCandidate5 },
        { Follower(kPeer2, true, 0), Follower(kPeer3, false, kSuspectedFailureErrors),
          Follower(kCandidate4, true, 0), Follower(kCandidate5, true, 0) },
        kObservedConfigurationIndex + 2));
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer3);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, PreExistingCommittedSpareRemovesExactConfirmedFailureWithoutDiscoveryOrAdd)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(
        ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors),
                                   kObservedConfigurationIndex));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate5 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);

    EXPECT_EQ(discovery->Calls(), 0);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::REMOVING_FAILED);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_TRUE(manager.activeOperation_->candidate.empty());
}

TEST(CoordinatorMembershipManagerTest, UnexplainedHealthyOrUnknownOverTargetMembershipFailsClosedWithDiagnostic)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus(
        { kCandidate4, kPeer3, kPeer1, kPeer2 },
        { Follower(kPeer2, false, 0), Follower(kPeer3, true, 0), Follower(kCandidate4, true, 0) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    testing::internal::CaptureStderr();
    for (size_t reconcileCount = 0; reconcileCount < kPolicyDiagnosticRateLimitInterval; ++reconcileCount) {
        ExpectReconcileOk(manager);
    }
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_EQ(discovery->Calls(), 0);
    const auto markerCount = CountOccurrences(capturedStderr, kUnsafeOverTargetMarker);
    EXPECT_GE(markerCount, 1U);
    EXPECT_LE(markerCount, kMaximumDiagnosticEmissionsPerFullInterval);
    EXPECT_NE(capturedStderr.find("group=datasystem-coordinator"), std::string::npos);
    EXPECT_NE(capturedStderr.find("committed_size=4"), std::string::npos);
    EXPECT_NE(capturedStderr.find("expected_size=3"), std::string::npos);
    EXPECT_NE(capturedStderr.find("term=7"), std::string::npos);
    EXPECT_NE(capturedStderr.find("configuration_index=11"), std::string::npos);
    EXPECT_NE(capturedStderr.find(std::string(kPeer1) + ", " + kPeer2 + ", " + kPeer3 + ", " + kCandidate4),
              std::string::npos);
}

TEST(CoordinatorMembershipManagerTest, MultipleFailuresUseEarliestTimerThenPeerTieBreakAndRemainSingleFlight)
{
    {
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(LeaderStatus(
            { kPeer1, kPeer2, kPeer3, kCandidate4, kCandidate5 },
            { Follower(kPeer2, true, 0), Follower(kPeer3, false, kSuspectedFailureErrors),
              Follower(kCandidate4, true, 0), Follower(kCandidate5, true, 0) }));
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kHealthCheckInterval);
        dependencies.SetStatus(LeaderStatus(
            { kPeer1, kPeer2, kPeer3, kCandidate4, kCandidate5 },
            { Follower(kPeer2, false, kSuspectedFailureErrors),
              Follower(kPeer3, false, kSuspectedFailureErrors), Follower(kCandidate4, true, 0),
              Follower(kCandidate5, true, 0) }));
        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer3);
        ExpectReconcileOk(manager);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    }

    {
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(LeaderStatus(
            { kPeer1, kPeer2, kPeer3, kCandidate4, kCandidate5 },
            { Follower(kPeer2, false, kSuspectedFailureErrors),
              Follower(kPeer3, false, kSuspectedFailureErrors), Follower(kCandidate4, true, 0),
              Follower(kCandidate5, true, 0) }));
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    }
}

TEST(CoordinatorMembershipManagerTest, LostLeadershipOrNewTermDuringReplacementAddInvalidatesOldCallback)
{
    for (const bool loseLeadership : { false, true }) {
        SCOPED_TRACE(loseLeadership);
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        dependencies.SetStatus(FailedFullStatus());
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        discovery->SetCandidates({ kCandidate4 });
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        ASSERT_TRUE(manager.activeOperation_.has_value());
        const auto oldGeneration = manager.activeOperation_->generation;

        auto changedStatus = ReplacementCommittedStatus(
            Follower(kPeer2, false, kSuspectedFailureErrors), kObservedConfigurationIndex + 1,
            loseLeadership ? kObservedFollowerTerm : kNextObservedTerm);
        changedStatus.isLeader = !loseLeadership;
        dependencies.SetStatus(changedStatus);
        ExpectReconcileOk(manager);
        EXPECT_FALSE(manager.activeOperation_.has_value());
        ASSERT_TRUE(dependencies.CompleteAdd(Status::OK()));
        ExpectReconcileOk(manager);

        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
        {
            std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
            EXPECT_NE(manager.completionMailbox_->activeGeneration, oldGeneration);
            EXPECT_FALSE(manager.completionMailbox_->result.has_value());
        }
        if (!loseLeadership) {
            ASSERT_TRUE(manager.observedTerm_.has_value());
            EXPECT_EQ(*manager.observedTerm_, kNextObservedTerm);
            EXPECT_EQ(manager.failureSince_.count(kPeer2), 1U);
        }
    }
}

TEST(CoordinatorMembershipManagerTest, NewTermUsesCommittedOwnedIntentOnlyForExactHealthyRollback)
{
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
        ASSERT_TRUE(manager.activeOperation_.has_value());
        const auto oldGeneration = manager.activeOperation_->generation;
        ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());

        dependencies.SetStatus(ReplacementCommittedStatus(
            Follower(kPeer2, true, 0), kObservedConfigurationIndex + 1, kNextObservedTerm));
        ExpectReconcileOk(manager);
        ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());
        ASSERT_TRUE(manager.activeOperation_.has_value());
        EXPECT_EQ(manager.activeOperation_->submittedStage,
                  CoordinatorMembershipManager::OperationStage::ROLLING_BACK);
        EXPECT_EQ(manager.activeOperation_->term, kNextObservedTerm);
        EXPECT_EQ(manager.activeOperation_->targetPeer, kCandidate4);
        EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->candidate);
        EXPECT_EQ(dependencies.LastRemovedPeer(), kCandidate4);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

        ASSERT_TRUE(dependencies.CompleteAdd(Status::OK()));
        ExpectReconcileOk(manager);
        ASSERT_TRUE(manager.activeOperation_.has_value());
        EXPECT_FALSE(manager.activeOperation_->completionStatus.has_value());
        {
            std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
            EXPECT_NE(manager.completionMailbox_->activeGeneration, oldGeneration);
            EXPECT_FALSE(manager.completionMailbox_->result.has_value());
        }
    }

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
        ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());

        auto newTermWithoutCandidate = HealthyFullStatus();
        newTermWithoutCandidate.term = kNextObservedTerm;
        dependencies.SetStatus(newTermWithoutCandidate);
        ExpectReconcileOk(manager);
        EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
        EXPECT_FALSE(manager.activeOperation_.has_value());
        ASSERT_TRUE(dependencies.CompleteAdd(Status::OK()));
        ExpectReconcileOk(manager);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    }
}

TEST(CoordinatorMembershipManagerTest, NewTermDuringRemoveIgnoresOldCallbackAndRestartsFailureGrace)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(
        ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors),
                                   kObservedConfigurationIndex));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_EQ(dependencies.RemovePeerCalls(), 1);

    dependencies.SetStatus(ReplacementCommittedStatus(
        Follower(kPeer2, false, kSuspectedFailureErrors), kObservedConfigurationIndex + 1,
        kNextObservedTerm));
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.failureSince_.count(kPeer2), 1U);
    ASSERT_TRUE(dependencies.CompleteRemove(Status(K_RUNTIME_ERROR, "old remove callback")));
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 2);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
}

TEST(CoordinatorMembershipManagerTest, RemoveCallbackResultsNeverCompleteBeforeCommittedExactExclusion)
{
    for (const bool callbackFails : { false, true }) {
        SCOPED_TRACE(callbackFails);
        ManualNow now;
        ThreadSafeMembershipDependencies dependencies;
        const auto overTarget = ReplacementCommittedStatus(
            Follower(kPeer2, false, kSuspectedFailureErrors), kObservedConfigurationIndex);
        dependencies.SetStatus(overTarget);
        auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
        auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

        ExpectReconcileOk(manager);
        now.Advance(kMemberFailureGrace);
        ExpectReconcileOk(manager);
        ASSERT_EQ(dependencies.RemovePeerCalls(), 1);
        ASSERT_TRUE(manager.activeOperation_.has_value());
        EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
        ASSERT_TRUE(dependencies.CompleteRemove(
            callbackFails ? Status(K_RUNTIME_ERROR, "scripted remove callback failure") : Status::OK()));
        ExpectReconcileOk(manager);
        ASSERT_TRUE(manager.activeOperation_.has_value());
        EXPECT_EQ(manager.activeOperation_->completionOrigin,
                  CoordinatorMembershipManager::CompletionOrigin::CALLBACK);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

        dependencies.SetStatus(ReplacementCompletedStatus(kObservedConfigurationIndex + 1));
        ExpectReconcileOk(manager);
        EXPECT_FALSE(manager.activeOperation_.has_value());
        EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    }
}

TEST(CoordinatorMembershipManagerTest, SynchronousRemoveSubmissionRejectionUsesBoundedRetry)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(
        ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors),
                                   kObservedConfigurationIndex));
    dependencies.SetRemoveSubmissionResult(Status(K_RUNTIME_ERROR, "synchronous remove failure"));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_EQ(dependencies.RemovePeerCalls(), 1);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);

    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.nextMembershipSubmissionAt_, now.Get() + kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 2);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
}

TEST(CoordinatorMembershipManagerTest, RollbackCallbackErrorWaitsForCommittedCandidateExclusion)
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
    const auto candidateCommitted = ReplacementCommittedStatus(Follower(kPeer2, true, 0));
    dependencies.SetStatus(candidateCommitted);
    ExpectReconcileOk(manager);
    ASSERT_EQ(dependencies.LastRemovedPeer(), kCandidate4);
    ASSERT_TRUE(dependencies.CompleteRemove(Status(K_RUNTIME_ERROR, "rollback callback failure")));
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ROLLING_BACK);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

    auto rolledBack = HealthyFullStatus();
    rolledBack.configurationIndex = kObservedConfigurationIndex + 2;
    dependencies.SetStatus(rolledBack);
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, RollbackSubmissionRejectionRetriesOnlyOwnedCandidateAfterDeadline)
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
    dependencies.SetRemoveSubmissionResult(Status(K_RUNTIME_ERROR, "rollback submission rejected"));
    const auto candidateCommitted = ReplacementCommittedStatus(Follower(kPeer2, true, 0));
    dependencies.SetStatus(candidateCommitted);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ROLLING_BACK);
    EXPECT_EQ(manager.activeOperation_->targetPeer, kCandidate4);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->candidate);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    ASSERT_TRUE(manager.ownedReplacementIntent_.has_value());
    EXPECT_EQ(manager.nextMembershipSubmissionAt_, now.Get() + kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->submittedStage,
              CoordinatorMembershipManager::OperationStage::ROLLING_BACK);
    EXPECT_EQ(manager.activeOperation_->targetPeer, kCandidate4);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->candidate);
    EXPECT_NE(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kCandidate4);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 2);
}

TEST(CoordinatorMembershipManagerTest, SubmissionExceptionsIncludeDetailsAndRemainFailClosed)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    dependencies.SetAddPeerAction([](const std::string &,
                                     const CoordinatorMembershipManager::MembershipOperationCallback &) -> Status {
        throw std::runtime_error(kSensitiveExceptionText);
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    testing::internal::CaptureStderr();
    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        ASSERT_TRUE(manager.completionMailbox_->result.has_value());
        EXPECT_EQ(manager.completionMailbox_->result->origin,
                  CoordinatorMembershipManager::CompletionOrigin::SUBMISSION_REJECTED);
        EXPECT_NE(manager.completionMailbox_->result->status.GetMsg().find("Coordinator membership AddPeer submission exception"),
                  std::string::npos);
        EXPECT_NE(manager.completionMailbox_->result->status.GetMsg().find(kSensitiveExceptionText), std::string::npos);
    }
    ExpectReconcileOk(manager);
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_NE(capturedStderr.find("Coordinator membership AddPeer submission exception"), std::string::npos);
    EXPECT_NE(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
}

TEST(CoordinatorMembershipManagerTest, RemoveSubmissionExceptionIncludesDetailsAndUsesBoundedRetry)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(
        ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors),
                                   kObservedConfigurationIndex));
    dependencies.SetRemovePeerAction([](const std::string &,
                                        const CoordinatorMembershipManager::MembershipOperationCallback &) -> Status {
        throw std::runtime_error(kSensitiveExceptionText);
    });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    testing::internal::CaptureStderr();
    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        ASSERT_TRUE(manager.completionMailbox_->result.has_value());
        EXPECT_EQ(manager.completionMailbox_->result->origin,
                  CoordinatorMembershipManager::CompletionOrigin::SUBMISSION_REJECTED);
        EXPECT_NE(manager.completionMailbox_->result->status.GetMsg().find("Coordinator membership RemovePeer submission exception"),
                  std::string::npos);
        EXPECT_NE(manager.completionMailbox_->result->status.GetMsg().find(kSensitiveExceptionText), std::string::npos);
    }
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    ExpectReconcileOk(manager);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    now.Advance(kDiscoveryRetryInterval);
    ExpectReconcileOk(manager);
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 2);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_NE(capturedStderr.find("Coordinator membership RemovePeer submission exception"), std::string::npos);
    EXPECT_NE(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
}

TEST(CoordinatorMembershipManagerTest, PreExistingNPlusTwoResolvesOneExactRemovalWithoutRepeatedRemove)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer2, kPeer3, kCandidate4, kCandidate5 },
        { Follower(kPeer2, false, kSuspectedFailureErrors), Follower(kPeer3, true, 0),
          Follower(kCandidate4, true, 0), Follower(kCandidate5, true, 0) }));
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate6 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->startingCommittedPeers.size(), kExpectedMemberCount + 2);
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(discovery->Calls(), 0);

    dependencies.SetStatus(LeaderStatus(
        { kPeer1, kPeer3, kCandidate4, kCandidate5 },
        { Follower(kPeer3, true, 0), Follower(kCandidate4, true, 0), Follower(kCandidate5, true, 0) },
        kObservedConfigurationIndex + 1));
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(discovery->Calls(), 0);
}

TEST(CoordinatorMembershipManagerTest, ReplacementAddAcceptsOnlyExactAlreadyCompletedConfiguration)
{
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
        dependencies.SetStatus(ReplacementCompletedStatus(kObservedConfigurationIndex + 1));
        ExpectReconcileOk(manager);

        EXPECT_FALSE(manager.activeOperation_.has_value());
        EXPECT_FALSE(manager.ownedReplacementIntent_.has_value());
        EXPECT_EQ(dependencies.AddPeerCalls(), 1);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    }

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
        dependencies.SetStatus(LeaderStatus(
            { kPeer1, kPeer2, kCandidate4 },
            { Follower(kPeer2, false, kSuspectedFailureErrors), Follower(kCandidate4, true, 0) },
            kObservedConfigurationIndex + 1));
        ExpectReconcileOk(manager);

        ASSERT_TRUE(manager.activeOperation_.has_value());
        EXPECT_EQ(manager.activeOperation_->submittedStage,
                  CoordinatorMembershipManager::OperationStage::ADDING_REPLACEMENT);
        EXPECT_EQ(manager.activeOperation_->failedPeer, kPeer2);
        EXPECT_EQ(dependencies.AddPeerCalls(), 1);
        EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    }
}

TEST(CoordinatorMembershipManagerTest, LostLeadershipDuringRemoveInvalidatesOperationAndOldCallback)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    auto overTarget = ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors),
                                                 kObservedConfigurationIndex);
    dependencies.SetStatus(overTarget);
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    const auto oldGeneration = manager.activeOperation_->generation;
    ASSERT_EQ(dependencies.RemovePeerCalls(), 1);

    overTarget.isLeader = false;
    dependencies.SetStatus(overTarget);
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    ASSERT_TRUE(dependencies.CompleteRemove(Status::OK()));
    ExpectReconcileOk(manager);

    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(discovery->Calls(), 0);
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_NE(manager.completionMailbox_->activeGeneration, oldGeneration);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }
}

TEST(CoordinatorMembershipManagerTest, InlineRemoveCompletionWaitsForNewerExactCommittedExclusion)
{
    ManualNow now;
    ThreadSafeMembershipDependencies dependencies;
    const auto overTarget = ReplacementCommittedStatus(
        Follower(kPeer2, false, kSuspectedFailureErrors), kObservedConfigurationIndex);
    dependencies.SetStatus(overTarget);
    dependencies.SetInlineRemoveResult(Status::OK());
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    EXPECT_EQ(manager.activeOperation_->targetPeer, manager.activeOperation_->failedPeer);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    ExpectReconcileOk(manager);
    ASSERT_TRUE(manager.activeOperation_.has_value());
    ASSERT_TRUE(manager.activeOperation_->completionStatus.has_value());
    EXPECT_TRUE(manager.activeOperation_->completionStatus->IsOk());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);

    dependencies.SetStatus(ReplacementCompletedStatus(kObservedConfigurationIndex + 1));
    ExpectReconcileOk(manager);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
}

TEST(CoordinatorMembershipManagerTest, ShutdownDuringBlockedReplacementAddSubmissionFailureDoesNotChain)
{
    ManualNow now;
    BlockingGetStatusState submissionBlock;
    auto submissionEntered = submissionBlock.GetEnteredFuture();
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    dependencies.SetAddPeerAction(
        [&submissionBlock](const std::string &,
                           const CoordinatorMembershipManager::MembershipOperationCallback &) {
            const auto blockStatus = submissionBlock.Block();
            if (blockStatus.IsError()) {
                return blockStatus;
            }
            return Status(K_RUNTIME_ERROR, kScriptedSubmissionFailure);
        });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);
    ScopedBlockingGetStatusRelease releaseOnExit(submissionBlock);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_TRUE(dependencies.WaitForAddPeerCalls(1, Clock::now() + kLifecycleDeadline));
    ASSERT_EQ(submissionEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    bool mailboxStopping = false;
    {
        std::unique_lock<std::mutex> lock(manager.completionMailbox_->mutex);
        mailboxStopping = manager.completionMailbox_->cv.wait_until(
            lock, Clock::now() + kLifecycleDeadline, [&manager] { return manager.completionMailbox_->stopping; });
    }
    submissionBlock.Release();
    ASSERT_EQ(shutdown.wait_for(kLifecycleDeadline), std::future_status::ready);

    EXPECT_TRUE(mailboxStopping);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_TRUE(manager.completionMailbox_->stopping);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }
}

TEST(CoordinatorMembershipManagerTest, ShutdownDuringBlockedDirectRemoveSubmissionFailureDoesNotRetry)
{
    ManualNow now;
    BlockingGetStatusState submissionBlock;
    auto submissionEntered = submissionBlock.GetEnteredFuture();
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(ReplacementCommittedStatus(
        Follower(kPeer2, false, kSuspectedFailureErrors), kObservedConfigurationIndex));
    dependencies.SetRemovePeerAction(
        [&submissionBlock](const std::string &,
                           const CoordinatorMembershipManager::MembershipOperationCallback &) {
            const auto blockStatus = submissionBlock.Block();
            if (blockStatus.IsError()) {
                return blockStatus;
            }
            return Status(K_RUNTIME_ERROR, kScriptedSubmissionFailure);
        });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);
    ScopedBlockingGetStatusRelease releaseOnExit(submissionBlock);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_TRUE(dependencies.WaitForRemovePeerCalls(1, Clock::now() + kLifecycleDeadline));
    ASSERT_EQ(submissionEntered.wait_for(kLifecycleDeadline), std::future_status::ready);

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    bool mailboxStopping = false;
    {
        std::unique_lock<std::mutex> lock(manager.completionMailbox_->mutex);
        mailboxStopping = manager.completionMailbox_->cv.wait_until(
            lock, Clock::now() + kLifecycleDeadline, [&manager] { return manager.completionMailbox_->stopping; });
    }
    submissionBlock.Release();
    ASSERT_EQ(shutdown.wait_for(kLifecycleDeadline), std::future_status::ready);

    EXPECT_TRUE(mailboxStopping);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    EXPECT_EQ(dependencies.LastRemovedPeer(), kPeer2);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(discovery->Calls(), 0);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_TRUE(manager.completionMailbox_->stopping);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }
}

TEST(CoordinatorMembershipManagerTest, ShutdownDuringBlockedRemoveSubmissionMakesCallbackInertAndJoins)
{
    ManualNow now;
    BlockingGetStatusState submissionBlock;
    auto submissionEntered = submissionBlock.GetEnteredFuture();
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(ReplacementCommittedStatus(
        Follower(kPeer2, false, kSuspectedFailureErrors), kObservedConfigurationIndex));
    dependencies.SetRemovePeerAction(
        [&submissionBlock](const std::string &,
                           const CoordinatorMembershipManager::MembershipOperationCallback &callback) {
            const auto blockStatus = submissionBlock.Block();
            callback(Status::OK());
            return blockStatus;
        });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);
    ScopedBlockingGetStatusRelease releaseOnExit(submissionBlock);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_TRUE(dependencies.WaitForRemovePeerCalls(1, Clock::now() + kLifecycleDeadline));
    ASSERT_EQ(submissionEntered.wait_for(kLifecycleDeadline), std::future_status::ready);
    dependencies.SetStatus(ReplacementCompletedStatus(kObservedConfigurationIndex + 1));

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    bool mailboxStopping = false;
    {
        std::unique_lock<std::mutex> lock(manager.completionMailbox_->mutex);
        mailboxStopping = manager.completionMailbox_->cv.wait_until(
            lock, Clock::now() + kLifecycleDeadline, [&manager] { return manager.completionMailbox_->stopping; });
    }
    submissionBlock.Release();
    ASSERT_EQ(shutdown.wait_for(kLifecycleDeadline), std::future_status::ready);

    EXPECT_TRUE(mailboxStopping);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_EQ(dependencies.RemovePeerCalls(), 1);
    EXPECT_EQ(dependencies.AddPeerCalls(), 0);
    EXPECT_EQ(discovery->Calls(), 0);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_TRUE(manager.completionMailbox_->stopping);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }
}

TEST(CoordinatorMembershipManagerTest, ShutdownDuringBlockedReplacementSubmissionMakesInlineCallbackInertAndNeverRemoves)
{
    ManualNow now;
    BlockingGetStatusState submissionBlock;
    auto submissionEntered = submissionBlock.GetEnteredFuture();
    ThreadSafeMembershipDependencies dependencies;
    dependencies.SetStatus(FailedFullStatus());
    dependencies.SetAddPeerAction(
        [&submissionBlock](const std::string &,
                           const CoordinatorMembershipManager::MembershipOperationCallback &callback) {
            const auto blockStatus = submissionBlock.Block();
            callback(Status::OK());
            return blockStatus;
        });
    auto discovery = std::make_shared<ThreadSafeCoordinatorDiscovery>();
    discovery->SetCandidates({ kCandidate4 });
    auto manager = MakeManager(ValidOptions(), dependencies, discovery, now);
    ScopedBlockingGetStatusRelease releaseOnExit(submissionBlock);

    ExpectReconcileOk(manager);
    now.Advance(kMemberFailureGrace);
    ASSERT_TRUE(manager.Start().IsOk());
    ASSERT_TRUE(dependencies.WaitForAddPeerCalls(1, Clock::now() + kLifecycleDeadline));
    ASSERT_EQ(submissionEntered.wait_for(kLifecycleDeadline), std::future_status::ready);
    dependencies.SetStatus(
        ReplacementCommittedStatus(Follower(kPeer2, false, kSuspectedFailureErrors)));

    auto shutdown = std::async(std::launch::async, [&manager] { return manager.Shutdown(); });
    bool mailboxStopping = false;
    {
        std::unique_lock<std::mutex> lock(manager.completionMailbox_->mutex);
        mailboxStopping = manager.completionMailbox_->cv.wait_until(
            lock, Clock::now() + kLifecycleDeadline, [&manager] { return manager.completionMailbox_->stopping; });
    }
    submissionBlock.Release();
    ASSERT_EQ(shutdown.wait_for(kLifecycleDeadline), std::future_status::ready);

    EXPECT_TRUE(mailboxStopping);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_EQ(dependencies.AddPeerCalls(), 1);
    EXPECT_EQ(dependencies.RemovePeerCalls(), 0);
    EXPECT_FALSE(manager.activeOperation_.has_value());
    {
        std::lock_guard<std::mutex> lock(manager.completionMailbox_->mutex);
        EXPECT_TRUE(manager.completionMailbox_->stopping);
        EXPECT_FALSE(manager.completionMailbox_->result.has_value());
    }
}

}  // namespace
}  // namespace datasystem::coordinator
