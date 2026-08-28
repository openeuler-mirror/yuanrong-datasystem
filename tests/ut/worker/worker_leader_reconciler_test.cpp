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

#include "datasystem/cluster/coordination_backend/worker_leader_reconciler.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <google/protobuf/descriptor.h>
#include "gtest/gtest.h"

#include "datasystem/cluster/membership/membership_value_codec.h"

namespace datasystem::cluster {
namespace {
using namespace std::chrono_literals;

constexpr char kWorkerAddress[] = "127.0.0.1:31501";
constexpr char kClusterName[] = "cluster-a";
constexpr char kCoordinatorId[] = "0123456789abcdef";
constexpr char kNextCoordinatorId[] = "fedcba9876543210";

TEST(WorkerWorkerOCServiceProtocolTest, KeepsLegacyMethodIndexesStable)
{
    const auto *service =
        google::protobuf::DescriptorPool::generated_pool()->FindServiceByName("datasystem.WorkerWorkerOCService");
    ASSERT_NE(service, nullptr);
    const auto *batchGetObjectRemote = service->FindMethodByName("BatchGetObjectRemote");
    const auto *migrateDataDirect = service->FindMethodByName("MigrateDataDirect");
    const auto *notifyRemoteGet = service->FindMethodByName("NotifyRemoteGet");
    const auto *getPeerHashRing = service->FindMethodByName("GetPeerHashRing");
    ASSERT_NE(batchGetObjectRemote, nullptr);
    ASSERT_NE(migrateDataDirect, nullptr);
    ASSERT_NE(notifyRemoteGet, nullptr);
    ASSERT_NE(getPeerHashRing, nullptr);
    EXPECT_EQ(batchGetObjectRemote->index(), 4);
    EXPECT_EQ(migrateDataDirect->index(), 5);
    EXPECT_EQ(notifyRemoteGet->index(), 6);
    EXPECT_EQ(getPeerHashRing->index(), 7);
}

class FakeRoutes final : public ICoordinatorLeaderRouteProvider {
public:
    class Token final : public Subscription {
    public:
        explicit Token(FakeRoutes &owner) : owner_(owner) {}
        ~Token() override { owner_.Clear(); }
    private:
        FakeRoutes &owner_;
    };

    CoordinatorLeaderIdentity GetLeaderCache() const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return identity_;
    }

    void Set(CoordinatorLeaderIdentity identity)
    {
        std::function<void(const CoordinatorLeaderIdentity &)> callback;
        std::lock_guard<std::mutex> lock(mutex_);
        identity_ = std::move(identity);
        callback = callback_;
        if (callback != nullptr) {
            callback(identity_);
        }
    }
    void SetCacheWithoutCallback(CoordinatorLeaderIdentity identity)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        identity_ = std::move(identity);
    }
    void SetAndWaitBeforeCallback(CoordinatorLeaderIdentity identity, std::mutex &waitMutex,
                                  std::condition_variable &waitCv, bool &ready, bool &resume)
    {
        std::function<void(const CoordinatorLeaderIdentity &)> callback;
        CoordinatorLeaderIdentity callbackIdentity;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            identity_ = std::move(identity);
            callback = callback_;
            callbackIdentity = identity_;
        }
        {
            std::unique_lock<std::mutex> lock(waitMutex);
            ready = true;
            waitCv.notify_all();
            waitCv.wait(lock, [&] { return resume; });
        }
        if (callback != nullptr) {
            callback(callbackIdentity);
        }
    }

    std::unique_ptr<Subscription> SubscribeLeaderChanges(
        std::function<void(const CoordinatorLeaderIdentity &)> callback) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        callback_ = std::move(callback);
        return std::make_unique<Token>(*this);
    }

    void Clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        callback_ = nullptr;
    }

private:
    mutable std::mutex mutex_;
    CoordinatorLeaderIdentity identity_;
    std::function<void(const CoordinatorLeaderIdentity &)> callback_;
};

class FakeProxy final : public ICoordinatorServiceProxy {
public:
    Status Init() override { return Status::OK(); }
    Status Put(const std::string &key, const std::string &value, int64_t, int64_t, int64_t &version,
               int64_t &revision, int32_t, std::string *coordinatorId, const std::string &, int64_t) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        version = ++membershipVersion_;
        revision = ++membershipRevision_;
        remoteMembershipKey_ = key;
        remoteMembershipValue_ = value;
        if (coordinatorId != nullptr) {
            *coordinatorId = kCoordinatorId;
        }
        cv_.notify_all();
        return Status::OK();
    }
    Status Range(const std::string &key, const std::string &, std::vector<KeyValueEntry> &entries, int64_t &revision,
                 int32_t, std::string *coordinatorId) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        entries.clear();
        if (!remoteMembershipValue_.empty() && key == remoteMembershipKey_) {
            entries.push_back({ remoteMembershipKey_, remoteMembershipValue_, membershipVersion_,
                                membershipRevision_ });
        }
        revision = membershipRevision_;
        if (coordinatorId != nullptr) {
            *coordinatorId = kCoordinatorId;
        }
        return Status::OK();
    }
    Status DeleteRange(const std::string &, const std::string &, int64_t &, int64_t &, int32_t, int64_t) override
    {
        return Unused();
    }
    Status WatchRange(const std::string &, const std::string &, const std::string &, const std::string &, int64_t &,
                      std::vector<KeyValueEntry> &, int32_t, std::string *) override { return Unused(); }
    Status CancelWatch(const std::string &, const std::vector<int64_t> &, const std::string &, int32_t) override
    {
        return Unused();
    }
    Status KeepAlive(const std::string &, int64_t &ttlMs, int64_t &remainingTtlMs, int32_t,
                     std::string *coordinatorId, const std::string &, int64_t,
                     const std::vector<std::string> & = {}) override
    {
        if (keepAliveSucceeds_) {
            remainingTtlMs = ttlMs;
            if (coordinatorId != nullptr) {
                *coordinatorId = kCoordinatorId;
            }
            return Status::OK();
        }
        return Unused();
    }
    Status CAS(const std::string &, const CasProcessFunc &, int64_t &, int64_t &) override { return Unused(); }
    Status GetCoordinatorId(std::string &, int32_t) override { return Unused(); }
    Status GetClusterRawSnapshot(const coordinator::GetClusterRawSnapshotReqPb &,
                                 coordinator::GetClusterRawSnapshotRspPb &, int32_t) override
    {
        return Unused();
    }
    Status ReportTopologyRecoveryCandidate(const coordinator::ReportTopologyRecoveryCandidateReqPb &request,
                                           coordinator::ReportTopologyRecoveryCandidateRspPb &response,
                                           int32_t) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        reportRequests_.push_back(request);
        response.set_result(coordinator::ReportTopologyRecoveryCandidateRspPb::ACCEPTED);
        response.set_recovery_state(coordinator::COORDINATOR_READY);
        cv_.notify_all();
        return Status::OK();
    }
    Status EnsureLeaderMembership(const coordinator::EnsureLeaderMembershipReqPb &request,
                                  coordinator::EnsureLeaderMembershipRspPb &response, int32_t) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        ensureRequests_.push_back(request);
        cv_.notify_all();
        cv_.wait(lock, [this] { return !blockEnsure_ || releaseEnsure_; });
        remoteMembershipValue_ = request.membership_value();
        ++membershipVersion_;
        ensureMembershipModRevision_ = ++membershipRevision_;
        response.set_result(ensureResult_);
        response.set_membership_mod_revision(ensureMembershipModRevision_);
        ++ensureCompletions_;
        cv_.notify_all();
        return ensureStatus_;
    }
    ICoordinatorLeaderRouteProvider *GetLeaderRouteProvider() override { return &routes_; }
    void GetObservedCoordinatorId(std::string &id) const override { id.clear(); }

    FakeRoutes routes_;
    coordinator::EnsureLeaderMembershipRspPb::ResultPb ensureResult_{
        coordinator::EnsureLeaderMembershipRspPb::ACCEPTED };
    Status ensureStatus_{ Status::OK() };
    int64_t ensureMembershipModRevision_{ 1 };

    bool WaitForEnsures(size_t count) const
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, 2s, [&] { return ensureRequests_.size() >= count; });
    }
    bool WaitForEnsureCompletions(size_t count) const
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, 2s, [&] { return ensureCompletions_ >= count; });
    }
    bool WaitForReports(size_t count) const
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, 2s, [&] { return reportRequests_.size() >= count; });
    }
    coordinator::EnsureLeaderMembershipReqPb EnsureAt(size_t index) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return ensureRequests_.at(index);
    }
    coordinator::ReportTopologyRecoveryCandidateReqPb ReportAt(size_t index) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return reportRequests_.at(index);
    }
    size_t ReportCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return reportRequests_.size();
    }
    size_t EnsureCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return ensureRequests_.size();
    }
    void BlockEnsure()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        blockEnsure_ = true;
        releaseEnsure_ = false;
    }
    void ReleaseEnsure()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        releaseEnsure_ = true;
        cv_.notify_all();
    }

    void SetKeepAliveSucceeds()
    {
        keepAliveSucceeds_ = true;
    }

    std::string RemoteMembershipValue() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return remoteMembershipValue_;
    }

    bool WaitForMembershipState(MemberLifecycleState state) const
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, 2s, [&] {
            MembershipValue value;
            return MembershipValueCodec::Decode(remoteMembershipValue_, value).IsOk()
                   && value.lifecycleState == state;
        });
    }

private:
    static Status Unused() { return Status(K_RUNTIME_ERROR, "unused fake RPC"); }
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    std::vector<coordinator::EnsureLeaderMembershipReqPb> ensureRequests_;
    std::vector<coordinator::ReportTopologyRecoveryCandidateReqPb> reportRequests_;
    size_t ensureCompletions_{ 0 };
    std::string remoteMembershipKey_;
    std::string remoteMembershipValue_;
    int64_t membershipVersion_{ 0 };
    int64_t membershipRevision_{ 0 };
    bool blockEnsure_{ false };
    bool releaseEnsure_{ false };
    bool keepAliveSucceeds_{ false };
};

TopologyRecoveryReporterOptions ReporterOptions()
{
    TopologyRecoveryReporterOptions options;
    options.maxInitialJitter = 0ms;
    options.minRetryBackoff = 1ms;
    options.maxRetryBackoff = 1ms;
    return options;
}

CoordinatorLeaderIdentity Identity(uint64_t term, uint64_t epoch,
                                   const std::string &coordinatorId = kCoordinatorId)
{
    return { HostPort("127.0.0.1", 30001), coordinatorId, term, epoch, true };
}

TEST(WorkerLeaderReconcilerTest, EnsureAcceptancePrecedesReporterAndUsesObservedLeaderIdentity)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);

    ASSERT_TRUE(proxy.WaitForEnsures(1));
    ASSERT_TRUE(proxy.WaitForReports(1));
    const auto ensure = proxy.EnsureAt(0);
    EXPECT_EQ(ensure.coordinator_id(), kCoordinatorId);
    EXPECT_EQ(ensure.leader_term(), 9UL);
    EXPECT_EQ(ensure.cluster_name(), kClusterName);
    EXPECT_EQ(ensure.reporter_address(), kWorkerAddress);
    EXPECT_EQ(proxy.ReportCount(), 1UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, RejectedEnsureNeverWakesReporter)
{
    FakeProxy proxy;
    proxy.ensureResult_ = coordinator::EnsureLeaderMembershipRspPb::STALE_EPOCH;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);

    ASSERT_TRUE(proxy.WaitForEnsures(1));
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(proxy.ReportCount(), 0UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, NewRouteEpochDiscardsOldEnsureCompletion)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.BlockEnsure();
    proxy.routes_.Set(Identity(9, 1));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    ASSERT_TRUE(proxy.WaitForEnsures(1));
    proxy.routes_.Set(Identity(10, 2));
    proxy.ReleaseEnsure();

    ASSERT_TRUE(proxy.WaitForEnsures(2));
    ASSERT_TRUE(proxy.WaitForReports(1));
    EXPECT_EQ(proxy.EnsureAt(0).leader_term(), 9UL);
    EXPECT_EQ(proxy.EnsureAt(1).leader_term(), 10UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, RouteChangeDuringRecreateGateRetriesWithLatestIdentity)
{
    constexpr uint64_t OLD_LEADER_TERM = 9;
    constexpr uint64_t OLD_ROUTE_EPOCH = 1;
    constexpr uint64_t NEW_LEADER_TERM = 10;
    constexpr uint64_t NEW_ROUTE_EPOCH = 2;
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    TopologyRecoveryReporter reporter(
        proxy, kClusterName, kWorkerAddress,
        [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); }, ReporterOptions());
    proxy.routes_.Set(Identity(OLD_LEADER_TERM, OLD_ROUTE_EPOCH));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool gateEntered = false;
    bool releaseGate = false;
    bool routeCacheUpdated = false;
    bool resumeRouteCallback = false;
    backend.SetMembershipRecreateGate([&] {
        std::unique_lock<std::mutex> lock(gateMutex);
        gateEntered = true;
        gateCv.notify_all();
        gateCv.wait(lock, [&] { return releaseGate; });
        return Status::OK();
    });
    ASSERT_TRUE(reconciler.Rejoin().IsOk());
    {
        std::unique_lock<std::mutex> lock(gateMutex);
        ASSERT_TRUE(gateCv.wait_for(lock, 2s, [&] { return gateEntered; }));
    }

    std::thread routeChange([&] {
        proxy.routes_.SetAndWaitBeforeCallback(Identity(NEW_LEADER_TERM, NEW_ROUTE_EPOCH), gateMutex, gateCv,
                                               routeCacheUpdated, resumeRouteCallback);
    });
    {
        std::unique_lock<std::mutex> lock(gateMutex);
        ASSERT_TRUE(gateCv.wait_for(lock, 2s, [&] { return routeCacheUpdated; }));
    }
    {
        std::lock_guard<std::mutex> lock(gateMutex);
        releaseGate = true;
    }
    gateCv.notify_all();

    // The first Rejoin pass fails with K_TRY_AGAIN after the recreate gate because the Leader changed; the queued
    // route callback installs the successor identity so the retry publishes the latest one.
    {
        std::lock_guard<std::mutex> lock(gateMutex);
        resumeRouteCallback = true;
    }
    gateCv.notify_all();
    routeChange.join();

    ASSERT_TRUE(proxy.WaitForEnsures(1));
    EXPECT_EQ(proxy.EnsureAt(0).leader_term(), NEW_LEADER_TERM);
    EXPECT_EQ(proxy.ReportCount(), 0UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, SameLeaderIdentityDoesNotSubmitSecondEnsure)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    ASSERT_TRUE(proxy.WaitForEnsures(1));
    proxy.routes_.Set(Identity(9, 2));
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(proxy.EnsureCount(), 1UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, ExplicitMembershipLossResubmitsEnsureForSameLeader)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    ASSERT_TRUE(proxy.WaitForEnsures(1));
    ASSERT_TRUE(proxy.WaitForReports(1));

    ASSERT_TRUE(reconciler.Reconcile(false).IsOk());
    ASSERT_TRUE(proxy.WaitForEnsures(2));
    EXPECT_EQ(proxy.EnsureAt(1).leader_term(), 9UL);
    MembershipValue payload;
    ASSERT_TRUE(MembershipValueCodec::Decode(proxy.EnsureAt(1).membership_value(), payload).IsOk());
    EXPECT_NE(payload.lifecycleState, MemberLifecycleState::RESTARTING);

    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, SynchronousForceEnsureDoesNotPublishRestarting)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    proxy.routes_.SetCacheWithoutCallback(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);

    ASSERT_TRUE(reconciler.Reconcile(true).IsOk());
    ASSERT_EQ(proxy.EnsureCount(), 1UL);
    MembershipValue payload;
    ASSERT_TRUE(MembershipValueCodec::Decode(proxy.EnsureAt(0).membership_value(), payload).IsOk());
    EXPECT_NE(payload.lifecycleState, MemberLifecycleState::RESTARTING);

    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, AsyncRejoinCompletesMembershipReadyAfterEnsure)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    ASSERT_TRUE(proxy.WaitForEnsures(1));
    ASSERT_TRUE(proxy.WaitForReports(1));

    ASSERT_TRUE(reconciler.Rejoin().IsOk());

    ASSERT_TRUE(proxy.WaitForEnsures(2));
    ASSERT_TRUE(proxy.WaitForMembershipState(MemberLifecycleState::READY));

    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, InflightReconcileDefersQueuedRejoinCleanupToRestartingRound)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    proxy.routes_.SetCacheWithoutCallback(Identity(9, 2));
    proxy.BlockEnsure();
    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool releaseGate = false;
    int gateInvocations = 0;
    backend.SetMembershipRecreateGate([&] {
        std::unique_lock<std::mutex> lock(gateMutex);
        ++gateInvocations;
        gateCv.notify_all();
        gateCv.wait(lock, [&] { return releaseGate; });
        return Status::OK();
    });

    // The Reconcile Ensure is already drained and in flight when the confirmed Rejoin arrives, so the Rejoin can only
    // win the next round, which is the only round allowed to run the destructive recreate gate.
    ASSERT_TRUE(reconciler.Reconcile(false).IsOk());
    ASSERT_TRUE(proxy.WaitForEnsures(1));
    ASSERT_TRUE(reconciler.Rejoin().IsOk());
    {
        std::lock_guard<std::mutex> lock(gateMutex);
        EXPECT_EQ(gateInvocations, 0);
    }
    proxy.ReleaseEnsure();

    {
        std::unique_lock<std::mutex> lock(gateMutex);
        ASSERT_TRUE(gateCv.wait_for(lock, 2s, [&] { return gateInvocations > 0; }));
    }
    // The drained Reconcile pass completed without the gate and without a RESTARTING payload; only the queued Rejoin
    // round entered the destructive gate, and it blocks before publishing its Ensure.
    ASSERT_EQ(proxy.EnsureCount(), 1UL);
    MembershipValue payload;
    ASSERT_TRUE(MembershipValueCodec::Decode(proxy.EnsureAt(0).membership_value(), payload).IsOk());
    EXPECT_NE(payload.lifecycleState, MemberLifecycleState::RESTARTING);
    {
        std::lock_guard<std::mutex> lock(gateMutex);
        releaseGate = true;
    }
    gateCv.notify_all();

    ASSERT_TRUE(proxy.WaitForEnsures(2));
    ASSERT_TRUE(proxy.WaitForMembershipState(MemberLifecycleState::READY));
    ASSERT_TRUE(MembershipValueCodec::Decode(proxy.EnsureAt(1).membership_value(), payload).IsOk());
    EXPECT_EQ(payload.lifecycleState, MemberLifecycleState::RESTARTING);
    {
        std::lock_guard<std::mutex> lock(gateMutex);
        EXPECT_EQ(gateInvocations, 1);
    }

    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, ExplicitMembershipLossDuringInflightEnsureResubmitsForSameLeader)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.BlockEnsure();
    proxy.routes_.Set(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    ASSERT_TRUE(proxy.WaitForEnsures(1));

    ASSERT_TRUE(reconciler.Reconcile(false).IsOk());
    proxy.ReleaseEnsure();
    ASSERT_TRUE(proxy.WaitForEnsures(2));
    EXPECT_EQ(proxy.EnsureAt(1).leader_term(), 9UL);

    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, ExitingMembershipWinsAgainstInflightLeaderEnsure)
{
    FakeProxy proxy;
    proxy.SetKeepAliveSucceeds();
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    proxy.BlockEnsure();
    proxy.routes_.Set(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    ASSERT_TRUE(proxy.WaitForEnsures(1));

    Status exitStatus;
    std::thread exitThread([&] { exitStatus = backend.UpdateNodeState(MemberLifecycleState::EXITING); });
    proxy.ReleaseEnsure();
    exitThread.join();

    ASSERT_TRUE(exitStatus.IsOk()) << exitStatus.ToString();
    MembershipValue remoteValue;
    ASSERT_TRUE(MembershipValueCodec::Decode(proxy.RemoteMembershipValue(), remoteValue).IsOk());
    EXPECT_EQ(remoteValue.lifecycleState, MemberLifecycleState::EXITING);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, ExitingMembershipDeadlineBoundsInflightLeaderEnsure)
{
    constexpr int32_t exitTimeoutMs = 200;
    constexpr auto promptCompletionTimeout = std::chrono::seconds(1);
    FakeProxy proxy;
    proxy.SetKeepAliveSucceeds();
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    proxy.BlockEnsure();
    proxy.routes_.Set(Identity(9, 2));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    ASSERT_TRUE(proxy.WaitForEnsures(1));

    const auto start = std::chrono::steady_clock::now();
    const auto status = backend.UpdateNodeStateWithTimeout(MemberLifecycleState::EXITING, exitTimeoutMs);
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_LT(elapsed, promptCompletionTimeout);
    MembershipValue remoteValue;
    const auto initialDecodeStatus = MembershipValueCodec::Decode(proxy.RemoteMembershipValue(), remoteValue);
    EXPECT_TRUE(initialDecodeStatus.IsOk()) << initialDecodeStatus.ToString();
    if (initialDecodeStatus.IsOk()) {
        EXPECT_EQ(remoteValue.lifecycleState, MemberLifecycleState::EXITING);
    }

    proxy.ReleaseEnsure();
    ASSERT_TRUE(proxy.WaitForEnsureCompletions(1));
    ASSERT_TRUE(proxy.WaitForMembershipState(MemberLifecycleState::EXITING));
    ASSERT_TRUE(MembershipValueCodec::Decode(proxy.RemoteMembershipValue(), remoteValue).IsOk());
    EXPECT_EQ(remoteValue.lifecycleState, MemberLifecycleState::EXITING);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, InitialMembershipPublicationDoesNotTriggerEnsure)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);

    // The first successful membership Put owns startup; only a recovering-gate rejection may synchronously Ensure.
    proxy.routes_.Set(Identity(9, 1));
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(proxy.EnsureCount(), 0UL);

    proxy.routes_.Set(Identity(10, 2));
    ASSERT_TRUE(proxy.WaitForEnsures(1));
    EXPECT_EQ(proxy.EnsureAt(0).leader_term(), 10UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, TermfulLeaderInitialMembershipWakesReporterWithoutEnsure)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(9, 1));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    backend.SetMembershipReadyHandler(
        [&reconciler](const std::string &coordinatorId, bool) { reconciler.NotifyMembershipReady(coordinatorId); });

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());

    ASSERT_TRUE(proxy.WaitForReports(1));
    EXPECT_EQ(proxy.EnsureCount(), 0UL);
    const auto report = proxy.ReportAt(0);
    EXPECT_EQ(report.coordinator_id(), kCoordinatorId);
    EXPECT_EQ(report.leader_term(), 9UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, MembershipSuccessForOldLifetimeEnsuresCurrentLeaderBeforeReporting)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(10, 2, kNextCoordinatorId));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    backend.SetMembershipReadyHandler(
        [&reconciler](const std::string &coordinatorId, bool) { reconciler.NotifyMembershipReady(coordinatorId); });

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());

    ASSERT_TRUE(proxy.WaitForEnsures(1));
    ASSERT_TRUE(proxy.WaitForReports(1));
    EXPECT_EQ(proxy.EnsureAt(0).coordinator_id(), kNextCoordinatorId);
    EXPECT_EQ(proxy.EnsureAt(0).leader_term(), 10UL);
    EXPECT_EQ(proxy.ReportAt(0).coordinator_id(), kNextCoordinatorId);
    EXPECT_EQ(proxy.ReportAt(0).leader_term(), 10UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, SynchronousMembershipReconcileConvergesAfterSuccessorCallback)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    backend.SetMembershipReadyHandler([&](const std::string &coordinatorId, bool) {
        if (coordinatorId == kCoordinatorId) {
            // Reproduce a successor Router observation whose callback was delayed or dropped by the startup gate.
            proxy.routes_.SetCacheWithoutCallback(Identity(10, 2, kNextCoordinatorId));
        }
        reconciler.NotifyMembershipReady(coordinatorId);
    });
    proxy.routes_.SetCacheWithoutCallback(Identity(9, 1));

    const auto reconcileStatus = reconciler.Reconcile(true);
    ASSERT_TRUE(reconcileStatus.IsOk()) << reconcileStatus.ToString();

    ASSERT_TRUE(proxy.WaitForEnsures(2));
    ASSERT_TRUE(proxy.WaitForReports(1));
    EXPECT_EQ(proxy.EnsureAt(0).coordinator_id(), kCoordinatorId);
    EXPECT_EQ(proxy.EnsureAt(1).coordinator_id(), kNextCoordinatorId);
    EXPECT_EQ(proxy.ReportAt(0).coordinator_id(), kNextCoordinatorId);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, TermZeroMembershipForOldLifetimeEnsuresCurrentCoordinator)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(0, 1, kNextCoordinatorId));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    backend.SetMembershipReadyHandler(
        [&reconciler](const std::string &coordinatorId, bool) { reconciler.NotifyMembershipReady(coordinatorId); });

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());

    ASSERT_TRUE(proxy.WaitForEnsures(1));
    ASSERT_TRUE(proxy.WaitForReports(1));
    EXPECT_EQ(proxy.EnsureAt(0).coordinator_id(), kNextCoordinatorId);
    EXPECT_EQ(proxy.EnsureAt(0).leader_term(), 0UL);
    EXPECT_EQ(proxy.ReportAt(0).coordinator_id(), kNextCoordinatorId);
    EXPECT_EQ(proxy.ReportAt(0).leader_term(), 0UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}

TEST(WorkerLeaderReconcilerTest, TermZeroLeaderInitialMembershipWakesReporterWithoutEnsure)
{
    FakeProxy proxy;
    DsCoordinationBackend backend(&proxy, kWorkerAddress);
    TopologyRecoveryReporter reporter(proxy, kClusterName, kWorkerAddress,
                                      [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no snapshot"); },
                                      ReporterOptions());
    reporter.NotifyRuntimeReady();
    proxy.routes_.Set(Identity(0, 1));
    WorkerLeaderReconciler reconciler(proxy, backend, reporter, kClusterName);
    backend.SetMembershipReadyHandler(
        [&reconciler](const std::string &coordinatorId, bool) { reconciler.NotifyMembershipReady(coordinatorId); });

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());

    ASSERT_TRUE(proxy.WaitForReports(1));
    EXPECT_EQ(proxy.EnsureCount(), 0UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}
}  // namespace
}  // namespace datasystem::cluster
