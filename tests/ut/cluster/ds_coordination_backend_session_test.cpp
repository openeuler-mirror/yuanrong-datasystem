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
#include "datasystem/cluster/membership/membership_value_codec.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/raii.h"

#include "gtest/gtest.h"

DS_DECLARE_uint32(node_dead_timeout_s);

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

    Status Init() override
    {
        return Status::OK();
    }

    Status Put(const std::string &, const std::string &value, int64_t ttlMs, int64_t, int64_t &version,
               int64_t &revision,
               int32_t timeoutMs, std::string *coordinatorId, const std::string &expectedCoordinatorId,
               int64_t expectedModRevision) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++putCalls_;
        lastPutTimeoutMs_ = timeoutMs;
        lastPutTtlMs_ = ttlMs;
        if (!putStatuses_.empty()) {
            auto status = std::move(putStatuses_.front());
            putStatuses_.erase(putStatuses_.begin());
            if (status.IsError()) {
                return status;
            }
        }
        if (putStatus_.IsError()) {
            return putStatus_;
        }
        lastExpectedCoordinatorId_ = expectedCoordinatorId;
        lastExpectedModRevision_ = expectedModRevision;
        if (expectedModRevision != COORDINATOR_NO_MOD_REVISION_CHECK && expectedModRevision != putRevision_) {
            return Status(K_TRY_AGAIN, "membership incarnation changed");
        }
        version = ++putVersion_;
        revision = ++putRevision_;
        lastPutValue_ = value;
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
        entries = rangeEntries_;
        revision = putRevision_;
        observedCoordinatorId_ = putCoordinatorId_;
        if (coordinatorId != nullptr) {
            *coordinatorId = putCoordinatorId_;
        }
        return Status::OK();
    }

    Status DeleteRange(const std::string &, const std::string &, int64_t &deleted, int64_t &, int32_t,
                       int64_t expectedModRevision) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        lastDeleteModRevision_ = expectedModRevision;
        if (putRevision_ != 0 && expectedModRevision != COORDINATOR_NO_MOD_REVISION_CHECK
            && expectedModRevision != putRevision_) {
            return Status(K_TRY_AGAIN, "membership incarnation changed");
        }
        deleted = putRevision_ == 0 ? 0 : 1;
        putRevision_ = 0;
        return Status::OK();
    }

    Status DeleteMembership(const std::string &key, int64_t &deleted, int64_t &revision, int32_t timeoutMs,
                            int64_t expectedModRevision) override
    {
        auto status = DeleteRange(key, "", deleted, revision, timeoutMs, expectedModRevision);
        std::lock_guard<std::mutex> lock(mutex_);
        membershipDeleteUsed_ = true;
        return status;
    }

    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &, const std::string &,
                      int64_t &watchId, std::vector<KeyValueEntry> &initialKvs, int32_t,
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

    Status KeepAlive(const std::string &, int64_t &ttlMs, int64_t &remainingTtlMs, int32_t, std::string *coordinatorId,
                     const std::string &expectedCoordinatorId, int64_t expectedModRevision,
                     const std::vector<std::string> &failedTargets = {}) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        lastKeepAliveCoordinatorId_ = expectedCoordinatorId;
        lastKeepAliveModRevision_ = expectedModRevision;
        lastFailedTargets_ = failedTargets;
        ++keepAliveCalls_;
        watchCv_.notify_all();
        if (keepAliveStatus_.IsError()) {
            return keepAliveStatus_;
        }
        if (putRevision_ == 0) {
            return Status(K_NOT_FOUND, "membership does not exist");
        }
        if (expectedModRevision != COORDINATOR_NO_MOD_REVISION_CHECK && expectedModRevision != putRevision_) {
            return Status(K_TRY_AGAIN, "membership incarnation changed");
        }
        remainingTtlMs = ttlMs;
        observedCoordinatorId_ = putCoordinatorId_;
        if (coordinatorId != nullptr) {
            *coordinatorId = putCoordinatorId_;
        }
        return Status::OK();
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

    Status GetClusterRawSnapshot(const coordinator::GetClusterRawSnapshotReqPb &,
                                 coordinator::GetClusterRawSnapshotRspPb &, int32_t) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake GetClusterRawSnapshot");
    }

    void GetObservedCoordinatorId(std::string &coordinatorId) const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        coordinatorId = observedCoordinatorId_;
    }

    void AddWatchStep(Status status, std::string coordinatorId, std::vector<KeyValueEntry> initialKvs = {})
    {
        std::lock_guard<std::mutex> lock(mutex_);
        watchSteps_.push_back({ std::move(status), std::move(coordinatorId), std::move(initialKvs) });
    }

    void SetPutCoordinatorId(std::string coordinatorId)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        putCoordinatorId_ = std::move(coordinatorId);
    }

    void SetPutStatus(Status status)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        putStatus_ = std::move(status);
    }

    void AddPutStatus(Status status)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        putStatuses_.push_back(std::move(status));
    }

    void SetKeepAliveStatus(Status status)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        keepAliveStatus_ = std::move(status);
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

    int64_t LastExpectedModRevision() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastExpectedModRevision_;
    }

    int32_t LastPutTimeoutMs() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastPutTimeoutMs_;
    }

    std::string LastPutValue() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastPutValue_;
    }

    int64_t LastPutTtlMs() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastPutTtlMs_;
    }

    int64_t LastKeepAliveModRevision() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastKeepAliveModRevision_;
    }

    bool WaitForKeepAliveRevision(int64_t expectedRevision, size_t minimumCalls, std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return watchCv_.wait_for(lock, timeout, [this, expectedRevision, minimumCalls] {
            return keepAliveCalls_ >= minimumCalls && lastKeepAliveModRevision_ == expectedRevision;
        });
    }

    size_t KeepAliveCalls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return keepAliveCalls_;
    }

    size_t PutCalls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return putCalls_;
    }

    int64_t LastDeleteModRevision() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastDeleteModRevision_;
    }

    bool MembershipDeleteUsed() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return membershipDeleteUsed_;
    }

    std::vector<std::string> LastFailedTargets() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastFailedTargets_;
    }

    void ReplaceMembershipIncarnation()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++putVersion_;
        ++putRevision_;
    }

    void SetMembershipRevision(int64_t revision)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        putRevision_ = revision;
    }

    void SetRangeEntries(std::vector<KeyValueEntry> entries)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        rangeEntries_ = std::move(entries);
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
    std::string lastKeepAliveCoordinatorId_;
    int64_t lastExpectedModRevision_{ 0 };
    int32_t lastPutTimeoutMs_{ 0 };
    int64_t lastPutTtlMs_{ 0 };
    int64_t lastKeepAliveModRevision_{ 0 };
    int64_t lastDeleteModRevision_{ 0 };
    bool membershipDeleteUsed_{ false };
    size_t putCalls_{ 0 };
    size_t keepAliveCalls_{ 0 };
    std::vector<std::string> lastFailedTargets_;
    int64_t putVersion_{ 0 };
    int64_t putRevision_{ 0 };
    std::vector<KeyValueEntry> rangeEntries_;
    std::string lastPutValue_;
    std::string putCoordinatorId_{ COORDINATOR_A };
    Status putStatus_{ Status::OK() };
    std::vector<Status> putStatuses_;
    Status keepAliveStatus_{ Status(K_RUNTIME_ERROR, "unused fake KeepAlive") };
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

Status ReadRenewalValue(const DsCoordinationBackend &backend, MembershipValue &value)
{
    DsCoordinationBackend::MembershipRenewalPayload payload;
    RETURN_IF_NOT_OK(backend.GetMembershipRenewalPayload(payload));
    return MembershipValueCodec::Decode(payload.encodedValue, value);
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
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    EXPECT_TRUE(backend.IsFirstKeepAliveSent());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_TRUE(backend.IsFirstKeepAliveSent());
}

TEST(DsCoordinationBackendSessionTest, InitialSuccessfulLeaseDoesNotReconcileBeforeReady)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    int reconcileCalls = 0;
    backend.SetMembershipReconcileHandler([&reconcileCalls](bool) {
        ++reconcileCalls;
        return Status::OK();
    });

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    EXPECT_EQ(reconcileCalls, 0);
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 1);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, InitialKeepAliveRetriesRoutingDeadlineExceeded)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    proxy.AddPutStatus(Status(K_RPC_DEADLINE_EXCEEDED, "leader not ready within route deadline"));
    proxy.AddPutStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    EXPECT_EQ(proxy.PutCalls(), 2U);
}

TEST(DsCoordinationBackendSessionTest, RecreatedMembershipIsBlockedUntilCleanupGatePasses)
{
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    std::atomic<size_t> gateCalls{ 0 };
    backend.SetMembershipRecreateGate([&gateCalls] {
        ++gateCalls;
        return Status(K_NOT_READY, "rejoin cleanup pending");
    });

    auto rc = backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true);

    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_EQ(gateCalls.load(), 1U);
}

TEST(DsCoordinationBackendSessionTest, RecreatedMembershipInvalidatesWatchesAfterCleanupGatePasses)
{
    DeterministicCoordinatorProxy proxy;
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    size_t resetCount = 0;
    backend.SetMembershipRecreateGate([] { return Status::OK(); });
    backend.SetEventHandler([&resetCount](CoordinationEvent &&event) {
        if (event.type == CoordinationEventType::RESET) {
            ++resetCount;
        }
    });
    ASSERT_TRUE(backend.WatchEvents(TwoWatchPlan()).IsOk());
    ASSERT_EQ(resetCount, 1U);

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());

    EXPECT_EQ(resetCount, 2U);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, EnsuredMembershipIsBlockedUntilCleanupGatePasses)
{
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    std::atomic<size_t> gateCalls{ 0 };
    backend.SetMembershipRecreateGate([&gateCalls] {
        ++gateCalls;
        return Status(K_NOT_READY, "rejoin cleanup pending");
    });

    const auto rc = backend.OnMembershipEnsured(COORDINATOR_A, 17);

    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_EQ(gateCalls.load(), 1U);
    EXPECT_FALSE(backend.IsFirstKeepAliveSent());
}

TEST(DsCoordinationBackendSessionTest, PeerRpcFailuresNeedCountAndWindowBeforeReporting)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;
    Raii restore([savedNodeTimeout] { FLAGS_node_timeout_s = savedNodeTimeout; });
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    const HostPort target("127.0.0.1", 12002);
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));

    backend.RecordPeerRpcFailure(target, start);
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(100));
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(1'499));
    EXPECT_TRUE(backend.GetFailedTargets(start + std::chrono::milliseconds(1'499)).empty());

    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(1'500));
    const auto failedTargets = backend.GetFailedTargets(start + std::chrono::milliseconds(1'500));
    ASSERT_EQ(failedTargets.size(), 1);
    EXPECT_EQ(failedTargets.front(), "127.0.0.1:12002");
    EXPECT_TRUE(backend.ConsumeImmediateReportSignal());
    EXPECT_FALSE(backend.ConsumeImmediateReportSignal());
}

TEST(DsCoordinationBackendSessionTest, PeerRpcSuccessClearsFailureSummary)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;
    Raii restore([savedNodeTimeout] { FLAGS_node_timeout_s = savedNodeTimeout; });
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    const HostPort target("127.0.0.1", 12002);
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));

    backend.RecordPeerRpcFailure(target, start);
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(750));
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(1'500));
    ASSERT_FALSE(backend.GetFailedTargets(start + std::chrono::milliseconds(1'500)).empty());

    backend.RecordPeerRpcSuccess(target);

    EXPECT_TRUE(backend.GetFailedTargets(start + std::chrono::milliseconds(1'501)).empty());
    EXPECT_FALSE(backend.ConsumeImmediateReportSignal());
}

TEST(DsCoordinationBackendSessionTest, ClearPeerRpcFailureObservationsDropsAllPreExitEvidence)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;
    Raii restore([savedNodeTimeout] { FLAGS_node_timeout_s = savedNodeTimeout; });
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    const HostPort firstTarget("127.0.0.1", 12002);
    const HostPort secondTarget("127.0.0.1", 12003);
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));

    for (const auto &target : { firstTarget, secondTarget }) {
        backend.RecordPeerRpcFailure(target, start);
        backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(750));
        backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(1'500));
    }
    ASSERT_EQ(backend.GetFailedTargets(start + std::chrono::milliseconds(1'500)).size(), 2U);

    backend.ClearPeerRpcFailureObservations();

    EXPECT_TRUE(backend.GetFailedTargets(start + std::chrono::milliseconds(1'501)).empty());
    EXPECT_FALSE(backend.ConsumeImmediateReportSignal());
}

TEST(DsCoordinationBackendSessionTest, PeerRpcFailureSummaryExpiresWithoutNewFailures)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;
    Raii restore([savedNodeTimeout] { FLAGS_node_timeout_s = savedNodeTimeout; });
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    const HostPort target("127.0.0.1", 12002);
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));

    backend.RecordPeerRpcFailure(target, start);
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(750));
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(1'500));
    const auto failedTargets = backend.GetFailedTargets(start + std::chrono::milliseconds(1'500));
    ASSERT_EQ(failedTargets.size(), 1);
    EXPECT_EQ(failedTargets.front(), "127.0.0.1:12002");

    EXPECT_EQ(backend.GetFailedTargets(start + std::chrono::milliseconds(4'501)),
              (std::vector<std::string>{ "127.0.0.1:12002" }));
    EXPECT_TRUE(backend.ConsumeImmediateReportSignal());

    EXPECT_TRUE(backend.GetFailedTargets(start + std::chrono::milliseconds(7'501)).empty());
    EXPECT_FALSE(backend.ConsumeImmediateReportSignal());
}

TEST(DsCoordinationBackendSessionTest, SparsePeerRpcFailuresDoNotAccumulateAcrossFailureWindow)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;
    Raii restore([savedNodeTimeout] { FLAGS_node_timeout_s = savedNodeTimeout; });
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    const HostPort target("127.0.0.1", 12002);
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));

    backend.RecordPeerRpcFailure(target, start);
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(2'000));
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(4'000));

    EXPECT_TRUE(backend.GetFailedTargets(start + std::chrono::milliseconds(4'000)).empty());
    EXPECT_FALSE(backend.ConsumeImmediateReportSignal());
}

TEST(DsCoordinationBackendSessionTest, ContinuousPeerRpcFailuresReportAcrossFailureWindow)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;
    Raii restore([savedNodeTimeout] { FLAGS_node_timeout_s = savedNodeTimeout; });
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    const HostPort target("127.0.0.1", 12002);
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));

    backend.RecordPeerRpcFailure(target, start);
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(700));
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(1'400));
    backend.RecordPeerRpcFailure(target, start + std::chrono::milliseconds(1'600));

    EXPECT_EQ(backend.GetFailedTargets(start + std::chrono::milliseconds(1'600)),
              (std::vector<std::string>{ "127.0.0.1:12002" }));
    EXPECT_TRUE(backend.ConsumeImmediateReportSignal());
}

TEST(DsCoordinationBackendSessionTest, CoordinatorKeepAliveLeaseTtlUsesNodeTimeoutBudget)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    const auto savedNodeDeadTimeout = FLAGS_node_dead_timeout_s;
    FLAGS_node_timeout_s = 3;
    FLAGS_node_dead_timeout_s = 30;
    Raii restore([savedNodeTimeout, savedNodeDeadTimeout] {
        FLAGS_node_timeout_s = savedNodeTimeout;
        FLAGS_node_dead_timeout_s = savedNodeDeadTimeout;
    });
    DeterministicCoordinatorProxy proxy;
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());

    EXPECT_EQ(proxy.LastPutTtlMs(), 3'000);
}

TEST(DsCoordinationBackendSessionTest, MembershipWritesCarryObservedCoordinatorFence)
{
    DeterministicCoordinatorProxy proxy;
    AddSuccessfulBatch(proxy, COORDINATOR_A);
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.WatchEvents(TwoWatchPlan()).IsOk());

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    EXPECT_EQ(proxy.LastExpectedCoordinatorId(), COORDINATOR_A);
    EXPECT_EQ(proxy.LastExpectedModRevision(), COORDINATOR_NO_MOD_REVISION_CHECK);
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_EQ(proxy.LastExpectedCoordinatorId(), COORDINATOR_A);
    EXPECT_EQ(proxy.LastExpectedModRevision(), 1);
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::EXITING).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 2);
}

TEST(DsCoordinationBackendSessionTest, FailedExitRequestRemainsTerminalForMembershipPayload)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());

    proxy.SetPutStatus(Status(K_RPC_UNAVAILABLE, "injected exit failure"));
    EXPECT_TRUE(backend.UpdateNodeState(MemberLifecycleState::EXITING).IsError());
    proxy.SetPutStatus(Status::OK());
    EXPECT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());

    DsCoordinationBackend::MembershipRenewalPayload payload;
    ASSERT_TRUE(backend.GetMembershipRenewalPayload(payload).IsOk());
    MembershipValue value;
    ASSERT_TRUE(MembershipValueCodec::Decode(payload.encodedValue, value).IsOk());
    EXPECT_EQ(value.lifecycleState, MemberLifecycleState::EXITING);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, ExitingPublicationUsesCallerTimeoutBudget)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());

    ASSERT_TRUE(backend.UpdateNodeStateWithTimeout(MemberLifecycleState::EXITING, 17).IsOk());
    EXPECT_GT(proxy.LastPutTimeoutMs(), 0);
    EXPECT_LE(proxy.LastPutTimeoutMs(), 17);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, StaleMembershipIncarnationCannotOverwriteReplacement)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    ASSERT_TRUE(backend.ShutdownEventSources().IsOk());

    proxy.ReplaceMembershipIncarnation();
    EXPECT_EQ(backend.UpdateNodeState(MemberLifecycleState::EXITING).GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(proxy.LastExpectedModRevision(), 2);
}

TEST(DsCoordinationBackendSessionTest, StaleMembershipIncarnationCannotDeleteReplacement)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());

    proxy.ReplaceMembershipIncarnation();
    EXPECT_EQ(backend.Delete("/datasystem/c/cluster", WATCHER_ADDRESS).GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(proxy.LastDeleteModRevision(), 2);
}

TEST(DsCoordinationBackendSessionTest, StartupRollbackDeletesOnlyPublishedMembership)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend unpublished(&proxy, WATCHER_ADDRESS);
    EXPECT_FALSE(unpublished.IsFirstKeepAliveSent());

    DsCoordinationBackend published(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(published.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    ASSERT_TRUE(published.ShutdownEventSources().IsOk());
    ASSERT_TRUE(published.IsFirstKeepAliveSent());
    EXPECT_TRUE(published.Delete("/datasystem/c/cluster", WATCHER_ADDRESS).IsOk());
    EXPECT_EQ(proxy.LastDeleteModRevision(), 1);
    EXPECT_TRUE(proxy.MembershipDeleteUsed());
}

TEST(DsCoordinationBackendSessionTest, RecoveringLeaderInitialLeaseUsesReconcileHandlerBeforeStartingRenewal)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetPutStatus(Status(K_NOT_READY, "leader recovering"));
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    int reconciled = 0;
    backend.SetMembershipReconcileHandler([&backend, &proxy, &reconciled](bool waitForCompletion) {
        EXPECT_TRUE(waitForCompletion);
        ++reconciled;
        proxy.SetMembershipRevision(17);
        RETURN_IF_NOT_OK(backend.OnMembershipEnsured(COORDINATOR_A, 17));
        return Status::OK();
    });

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    EXPECT_EQ(reconciled, 1);
    EXPECT_TRUE(backend.IsFirstKeepAliveSent());
    proxy.SetPutStatus(Status::OK());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 17);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, EnsuredMembershipClearsEarlierRenewalFailure)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status(K_RPC_UNAVAILABLE, "injected renewal failure"));
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    for (int retry = 0; retry < 100 && !backend.IsKeepAliveTimeout(); ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(backend.IsKeepAliveTimeout());

    proxy.SetKeepAliveStatus(Status::OK());
    proxy.SetMembershipRevision(17);
    ASSERT_TRUE(backend.OnMembershipEnsured(COORDINATOR_A, 17).IsOk());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 17);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, EnsuredMembershipWakesRenewalBeforeLeaseExpires)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    ASSERT_TRUE(proxy.WaitForKeepAliveRevision(1, 1, std::chrono::milliseconds(200)));
    const size_t callsBeforeEnsure = proxy.KeepAliveCalls();

    proxy.SetMembershipRevision(17);
    ASSERT_TRUE(backend.OnMembershipEnsured(COORDINATOR_A, 17).IsOk());

    EXPECT_TRUE(proxy.WaitForKeepAliveRevision(17, callsBeforeEnsure + 1, std::chrono::milliseconds(200)));
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, StaleEnsuredRevisionCannotRollbackNewerMembershipMutation)
{
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());

    proxy.SetMembershipRevision(17);
    ASSERT_TRUE(backend.OnMembershipEnsured(COORDINATOR_A, 17).IsOk());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 17);

    // A delayed Ensure response must not replace the revision produced by the newer state mutation.
    ASSERT_TRUE(backend.OnMembershipEnsured(COORDINATOR_A, 17).IsOk());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 18);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, NewCoordinatorLifetimeAcceptsLowerEnsuredRevision)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    // This test exercises revision installation synchronously; quiesce the independent renewal loop so it cannot
    // legitimately recreate membership between the installation and the assertion below.
    ASSERT_TRUE(backend.ShutdownEventSources().IsOk());

    proxy.SetMembershipRevision(17);
    ASSERT_TRUE(backend.OnMembershipEnsured(COORDINATOR_A, 17).IsOk());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 17);

    proxy.SetPutCoordinatorId(COORDINATOR_B);
    proxy.SetMembershipRevision(2);
    ASSERT_TRUE(backend.OnMembershipEnsured(COORDINATOR_B, 2).IsOk());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 2);
}

TEST(DsCoordinationBackendSessionTest, RenewalPayloadDoesNotExposePhysicalMembershipKey)
{
    DeterministicCoordinatorProxy proxy;
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());

    DsCoordinationBackend::MembershipRenewalPayload payload;
    ASSERT_TRUE(backend.GetMembershipRenewalPayload(payload).IsOk());
    EXPECT_EQ(payload.reporterAddress, WATCHER_ADDRESS);
    EXPECT_GT(payload.ttlMs, 0);
    EXPECT_FALSE(payload.encodedValue.empty());
    EXPECT_EQ(payload.reporterAddress.find("/datasystem/"), std::string::npos);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, LocalReconciliationReadyUpdatesRenewalPayload)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, true, true).IsOk());

    MembershipValue renewal;
    ASSERT_TRUE(ReadRenewalValue(backend, renewal).IsOk());
    ASSERT_EQ(renewal.lifecycleState, MemberLifecycleState::RECOVERING);
    MembershipValue stored{ 123, MemberLifecycleState::RECOVERING, "host-a", "v1" };
    std::string encoded;
    ASSERT_TRUE(MembershipValueCodec::Encode(stored, encoded).IsOk());
    proxy.SetRangeEntries({ { "/datasystem/c/cluster/" + std::string(WATCHER_ADDRESS), encoded, 1, 1 } });
    HostPort localAddress;
    ASSERT_TRUE(localAddress.ParseString(WATCHER_ADDRESS).IsOk());

    ASSERT_TRUE(backend.InformReconciliationDone(localAddress).IsOk());

    ASSERT_TRUE(ReadRenewalValue(backend, renewal).IsOk());
    EXPECT_EQ(renewal.lifecycleState, MemberLifecycleState::READY);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, EnsureTransactionCannotReplayRecoveringAfterReady)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, true, true).IsOk());
    MembershipValue recovering{ 123, MemberLifecycleState::RECOVERING, "host-a", "v1" };
    std::string encoded;
    ASSERT_TRUE(MembershipValueCodec::Encode(recovering, encoded).IsOk());
    proxy.SetRangeEntries({ { "/datasystem/c/cluster/" + std::string(WATCHER_ADDRESS), encoded, 1, 1 } });
    HostPort localAddress;
    ASSERT_TRUE(localAddress.ParseString(WATCHER_ADDRESS).IsOk());

    std::mutex barrierMutex;
    std::condition_variable barrierCv;
    bool payloadCaptured = false;
    bool releaseEnsure = false;
    auto ensure = std::async(std::launch::async, [&] {
        return backend.EnsureMembership(
            COORDINATOR_A,
            [&](const DsCoordinationBackend::MembershipRenewalPayload &payload, int64_t &membershipModRevision) {
                MembershipValue captured;
                RETURN_IF_NOT_OK(MembershipValueCodec::Decode(payload.encodedValue, captured));
                CHECK_FAIL_RETURN_STATUS(captured.lifecycleState == MemberLifecycleState::RECOVERING, K_INVALID,
                                         "Ensure did not capture RECOVERING membership");
                std::unique_lock<std::mutex> lock(barrierMutex);
                payloadCaptured = true;
                barrierCv.notify_all();
                barrierCv.wait(lock, [&] { return releaseEnsure; });
                membershipModRevision = 1;
                return Status::OK();
            });
    });
    bool reachedBarrier = false;
    {
        std::unique_lock<std::mutex> lock(barrierMutex);
        reachedBarrier = barrierCv.wait_for(lock, std::chrono::seconds(2), [&] { return payloadCaptured; });
        if (!reachedBarrier) {
            releaseEnsure = true;
            barrierCv.notify_all();
        }
    }
    ASSERT_TRUE(reachedBarrier);
    auto reconciliation =
        std::async(std::launch::async, [&] { return backend.InformReconciliationDone(localAddress); });
    {
        std::lock_guard<std::mutex> lock(barrierMutex);
        releaseEnsure = true;
        barrierCv.notify_all();
    }
    ASSERT_TRUE(ensure.get().IsOk());
    ASSERT_TRUE(reconciliation.get().IsOk());

    MembershipValue renewal;
    ASSERT_TRUE(ReadRenewalValue(backend, renewal).IsOk());
    EXPECT_EQ(renewal.lifecycleState, MemberLifecycleState::READY);
    MembershipValue stored;
    ASSERT_TRUE(MembershipValueCodec::Decode(proxy.LastPutValue(), stored).IsOk());
    EXPECT_EQ(stored.lifecycleState, MemberLifecycleState::READY);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, FailedExitIntentFencesReconciliationAndEnsurePayload)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, false, true).IsOk());
    proxy.SetPutStatus(Status(K_RPC_UNAVAILABLE, "injected exit failure"));
    EXPECT_EQ(backend.UpdateNodeState(MemberLifecycleState::EXITING).GetCode(), K_RPC_UNAVAILABLE);
    proxy.SetPutStatus(Status::OK());
    EXPECT_TRUE(backend.UpdateNodeState(MemberLifecycleState::READY).IsOk());
    HostPort localAddress;
    ASSERT_TRUE(localAddress.ParseString(WATCHER_ADDRESS).IsOk());
    EXPECT_TRUE(backend.InformReconciliationDone(localAddress).IsOk());

    ASSERT_TRUE(backend
                    .EnsureMembership(
                        COORDINATOR_A,
                        [](const DsCoordinationBackend::MembershipRenewalPayload &payload,
                           int64_t &membershipModRevision) {
                            MembershipValue captured;
                            RETURN_IF_NOT_OK(MembershipValueCodec::Decode(payload.encodedValue, captured));
                            CHECK_FAIL_RETURN_STATUS(captured.lifecycleState == MemberLifecycleState::EXITING,
                                                     K_INVALID,
                                                     "Ensure did not preserve EXITING membership");
                            membershipModRevision = 17;
                            return Status::OK();
                        })
                    .IsOk());
    MembershipValue renewal;
    ASSERT_TRUE(ReadRenewalValue(backend, renewal).IsOk());
    EXPECT_EQ(renewal.lifecycleState, MemberLifecycleState::EXITING);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, FailedLocalReconciliationDoesNotAdvanceRenewalPayload)
{
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, true, true).IsOk());
    MembershipValue stored{ 123, MemberLifecycleState::RECOVERING, "host-a", "v1" };
    std::string encoded;
    ASSERT_TRUE(MembershipValueCodec::Encode(stored, encoded).IsOk());
    proxy.SetRangeEntries({ { "/datasystem/c/cluster/" + std::string(WATCHER_ADDRESS), encoded, 1, 1 } });
    proxy.SetPutStatus(Status(K_RPC_UNAVAILABLE, "injected reconciliation failure"));
    HostPort localAddress;
    ASSERT_TRUE(localAddress.ParseString(WATCHER_ADDRESS).IsOk());

    EXPECT_EQ(backend.InformReconciliationDone(localAddress).GetCode(), K_RPC_UNAVAILABLE);

    MembershipValue renewal;
    ASSERT_TRUE(ReadRenewalValue(backend, renewal).IsOk());
    EXPECT_EQ(renewal.lifecycleState, MemberLifecycleState::RECOVERING);
    proxy.SetPutStatus(Status::OK());
    ASSERT_TRUE(backend.UpdateNodeState(MemberLifecycleState::EXITING).IsOk());
    EXPECT_EQ(proxy.LastExpectedModRevision(), 1);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(DsCoordinationBackendSessionTest, RemoteReconciliationDoesNotOverwriteLocalRenewalPayload)
{
    constexpr char REMOTE_ADDRESS[] = "127.0.0.1:31502";
    DeterministicCoordinatorProxy proxy;
    proxy.SetKeepAliveStatus(Status::OK());
    DsCoordinationBackend backend(&proxy, WATCHER_ADDRESS);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/c/cluster", WATCHER_ADDRESS, true, true).IsOk());
    MembershipValue stored{ 123, MemberLifecycleState::RECOVERING, "host-b", "v1" };
    std::string encoded;
    ASSERT_TRUE(MembershipValueCodec::Encode(stored, encoded).IsOk());
    proxy.SetRangeEntries({ { "/datasystem/c/cluster/" + std::string(REMOTE_ADDRESS), encoded, 1, 1 } });
    HostPort remoteAddress;
    ASSERT_TRUE(remoteAddress.ParseString(REMOTE_ADDRESS).IsOk());

    ASSERT_TRUE(backend.InformReconciliationDone(remoteAddress).IsOk());

    MembershipValue renewal;
    ASSERT_TRUE(ReadRenewalValue(backend, renewal).IsOk());
    EXPECT_EQ(renewal.lifecycleState, MemberLifecycleState::RECOVERING);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
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
