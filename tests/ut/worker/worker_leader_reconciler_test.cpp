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

namespace datasystem::cluster {
namespace {
using namespace std::chrono_literals;

constexpr char kWorkerAddress[] = "127.0.0.1:31501";
constexpr char kClusterName[] = "cluster-a";
constexpr char kCoordinatorId[] = "0123456789abcdef";

TEST(WorkerWorkerOCServiceProtocolTest, KeepsLegacyZmqMethodIndexesStable)
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
    Status Put(const std::string &, const std::string &, int64_t, int64_t, int64_t &version, int64_t &revision,
               int32_t, std::string *coordinatorId, const std::string &, int64_t) override
    {
        ++version;
        ++revision;
        if (coordinatorId != nullptr) {
            *coordinatorId = kCoordinatorId;
        }
        return Status::OK();
    }
    Status Range(const std::string &, const std::string &, std::vector<KeyValueEntry> &entries, int64_t &revision,
                 int32_t, std::string *coordinatorId) override
    {
        entries.clear();
        revision = 0;
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
    Status CancelWatch(const std::string &, const std::vector<int64_t> &, const std::string &, int32_t) override { return Unused(); }
    Status KeepAlive(const std::string &, int64_t &, int64_t &, int32_t, std::string *, const std::string &,
                     int64_t) override
    {
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
        response.set_result(ensureResult_);
        response.set_membership_mod_revision(ensureMembershipModRevision_);
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

private:
    static Status Unused() { return Status(K_RUNTIME_ERROR, "unused fake RPC"); }
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    std::vector<coordinator::EnsureLeaderMembershipReqPb> ensureRequests_;
    std::vector<coordinator::ReportTopologyRecoveryCandidateReqPb> reportRequests_;
    bool blockEnsure_{ false };
    bool releaseEnsure_{ false };
};

TopologyRecoveryReporterOptions ReporterOptions()
{
    TopologyRecoveryReporterOptions options;
    options.maxInitialJitter = 0ms;
    options.minRetryBackoff = 1ms;
    options.maxRetryBackoff = 1ms;
    return options;
}

CoordinatorLeaderIdentity Identity(uint64_t term, uint64_t epoch)
{
    return { HostPort("127.0.0.1", 30001), kCoordinatorId, term, epoch, true };
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

TEST(WorkerLeaderReconcilerTest, RouteChangeDuringRecreateGateDiscardsOldEnsureCompletion)
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
    Status reconcileStatus;
    std::thread reconcileThread([&] { reconcileStatus = reconciler.Reconcile(true); });
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

    reconcileThread.join();
    EXPECT_EQ(reconcileStatus.GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(proxy.EnsureCount(), 0UL);
    EXPECT_EQ(proxy.ReportCount(), 0UL);
    {
        std::lock_guard<std::mutex> lock(gateMutex);
        resumeRouteCallback = true;
    }
    gateCv.notify_all();
    routeChange.join();
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

TEST(WorkerLeaderReconcilerTest, LegacyLeaderInitialMembershipWakesReporterWithoutEnsure)
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
        [&reconciler](const std::string &coordinatorId, bool) { reconciler.NotifyLegacyMembershipReady(coordinatorId); });

    ASSERT_TRUE(backend.InitKeepAlive("/datasystem/cluster/cluster-a", kWorkerAddress, false, true).IsOk());

    ASSERT_TRUE(proxy.WaitForReports(1));
    EXPECT_EQ(proxy.EnsureCount(), 0UL);
    reconciler.Shutdown();
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
}
}  // namespace
}  // namespace datasystem::cluster
