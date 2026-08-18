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
 * Description: Tests WorkerOCServer topology snapshot handling.
 */

#include "datasystem/worker/worker_oc_server.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/worker/cluster_event_type.h"
#include "ut/common.h"

namespace datasystem::ut {
namespace {
constexpr char SUBSCRIBER_NAME[] = "WorkerOCServerTest";

cluster::Member MakeMember(char idByte, std::string address, cluster::MemberState state, uint32_t token)
{
    return cluster::Member{ { std::string(16, idByte), std::move(address) }, state, { token } };
}

Status MakeSnapshot(cluster::MemberState localState, cluster::MemberState targetState, uint64_t version,
                    std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    cluster::TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    state.members = { MakeMember('a', "127.0.0.1:31501", localState, 10),
                      MakeMember('b', "127.0.0.1:31502", targetState, 20) };
    if (targetState == cluster::MemberState::FAILED) {
        state.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::FAILURE, version };
    }
    return cluster::TopologySnapshot::Create(std::move(state), version, std::string(64, 'a'), snapshot);
}

Status MakeSnapshot(cluster::MemberState targetState, uint64_t version,
                    std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    return MakeSnapshot(cluster::MemberState::ACTIVE, targetState, version, snapshot);
}

Status MakeLocalScaleInSnapshot(uint64_t version, std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    cluster::TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    state.members = { MakeMember('a', "127.0.0.1:31501", cluster::MemberState::LEAVING, 10),
                      MakeMember('b', "127.0.0.1:31502", cluster::MemberState::ACTIVE, 20) };
    state.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, version };
    return cluster::TopologySnapshot::Create(std::move(state), version, std::string(64, 'a'), snapshot);
}

Status MakeSnapshotWithoutLocal(uint64_t version, std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    cluster::TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    state.members = { MakeMember('b', "127.0.0.1:31502", cluster::MemberState::ACTIVE, 20) };
    return cluster::TopologySnapshot::Create(std::move(state), version, std::string(64, 'a'), snapshot);
}

Status MakeMultiWorkerSnapshotWithoutLocal(uint64_t version,
                                           std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    cluster::TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    state.members = { MakeMember('x', "127.0.0.1:31598", cluster::MemberState::ACTIVE, 10),
                      MakeMember('y', "127.0.0.1:31502", cluster::MemberState::ACTIVE, 20) };
    return cluster::TopologySnapshot::Create(std::move(state), version, std::string(64, 'a'), snapshot);
}

Status MakeSingleWorkerSnapshot(std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    cluster::TopologyState state;
    state.clusterHasInit = true;
    state.version = 1;
    state.members = { MakeMember('a', "127.0.0.1:31501", cluster::MemberState::ACTIVE, 10) };
    return cluster::TopologySnapshot::Create(std::move(state), 1, std::string(64, 'a'), snapshot);
}
}  // namespace

class WorkerOCServerTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(RpcStubCacheMgr::Instance().Init(100));
        server_ = std::make_unique<worker::WorkerOCServer>(HostPort("127.0.0.1", 31501),
                                                          HostPort("127.0.0.1", 31501), HostPort(), nullptr);
        server_->SetScaleInShutdownRequester([this] { ++shutdownRequestCount_; });
        RemoveDeadWorkerEvent::GetInstance().AddSubscriber(SUBSCRIBER_NAME, [this](const std::string &address) {
            recoveredAddress_ = address;
            ++recoveryCount_;
        });
    }

    void TearDown() override
    {
        RemoveDeadWorkerEvent::GetInstance().RemoveSubscriber(SUBSCRIBER_NAME);
        server_.reset();
        CommonTest::TearDown();
    }

    void Publish(std::shared_ptr<const cluster::TopologySnapshot> snapshot)
    {
        server_->HandleTopologySnapshotPublished(std::move(snapshot));
    }

    Status RestoreScaleInPreparation(const cluster::TopologySnapshot &snapshot)
    {
        return server_->RestoreScaleInPreparation(snapshot);
    }

    bool HasScaleInPreparationWorkers() const
    {
        return server_->checkAsyncTasksThread_ != nullptr && server_->clientsExitChecker_ != nullptr;
    }

    bool HasCheckAsyncTasksThread() const
    {
        return server_->checkAsyncTasksThread_ != nullptr;
    }

    bool HasClientsExitChecker() const
    {
        return server_->clientsExitChecker_ != nullptr;
    }

    bool IsTopologyExitRequested() const
    {
        return server_->topologyExitRequested_.load();
    }

    std::pair<std::thread::id, std::thread::id> ScaleInPreparationWorkerIds() const
    {
        return { server_->checkAsyncTasksThread_->get_id(), server_->clientsExitChecker_->get_id() };
    }

    Status StartScaleInPreparation()
    {
        return server_->StartPreShutdownWorkers(true, Trace::Instance().GetTraceID());
    }

    bool ShouldPublishReadyMembership(bool needsRestartReconciliation) const
    {
        return server_->ShouldPublishReadyMembership(needsRestartReconciliation);
    }

    void SetScaleInExitPublishFn(std::function<Status(int32_t)> publish)
    {
        server_->scaleInExitPublishFn_ = std::move(publish);
    }

    bool WaitForScaleInExitPublished(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(server_->scaleInExitPublisherMutex_);
        return server_->scaleInExitPublisherCv_.wait_for(
            lock, timeout, [this] { return server_->scaleInExitPublicationPublished_; });
    }

    void MarkTopologyRuntimeStarted()
    {
        server_->topologyRuntimeStarted_.store(true, std::memory_order_release);
        server_->scaleInExitPublisherCv_.notify_all();
    }

    void StopScaleInExitPublisher()
    {
        server_->StopScaleInExitPublisher();
    }

    void StopPreShutdownWorkers()
    {
        server_->StopPreShutdownWorkers();
    }

    Status ScheduleScaleInExitPublication()
    {
        return server_->ScheduleScaleInExitPublication();
    }

    std::chrono::milliseconds ScaleInExitRetryDelay(const std::string &address, size_t consecutiveFailures) const
    {
        return worker::WorkerOCServer::ComputeScaleInExitRetryDelay(address, consecutiveFailures);
    }

    bool IsLocalUbVerificationEligible(const cluster::TopologySnapshot &snapshot) const
    {
        return server_->IsLocalUbVerificationEligible(snapshot);
    }

    Status WaitForExitingRemoval(std::chrono::steady_clock::time_point deadline,
                                 const std::function<Status(int32_t)> &publish,
                                 const std::function<Status()> &observe,
                                 std::chrono::milliseconds retryInterval)
    {
        return server_->WaitForExitingMembershipAndTopologyRemoval(deadline, publish, observe, retryInterval);
    }

    Status WaitForStartupHealth(const std::function<Status()> &refresh, const std::function<bool()> &healthy,
                                const std::function<Status()> &probe, const std::function<bool()> &interrupted)
    {
        return server_->WaitForStartupHealth([] { return Status::OK(); }, refresh, healthy, probe, interrupted,
                                             std::chrono::milliseconds(1));
    }

    Status WaitForStartupHealth(const std::function<Status()> &reconcile, const std::function<Status()> &refresh,
                                const std::function<bool()> &healthy, const std::function<Status()> &probe,
                                const std::function<bool()> &interrupted)
    {
        return server_->WaitForStartupHealth(reconcile, refresh, healthy, probe, interrupted,
                                             std::chrono::milliseconds(1));
    }

protected:
    std::unique_ptr<worker::WorkerOCServer> server_;
    std::string recoveredAddress_;
    size_t recoveryCount_{ 0 };
    size_t shutdownRequestCount_{ 0 };
};

TEST_F(WorkerOCServerTest, ActiveAddressRemovesPreviouslyFailedWorker)
{
    std::shared_ptr<const cluster::TopologySnapshot> failed;
    std::shared_ptr<const cluster::TopologySnapshot> active;
    DS_ASSERT_OK(MakeSnapshot(cluster::MemberState::FAILED, 1, failed));
    DS_ASSERT_OK(MakeSnapshot(cluster::MemberState::ACTIVE, 2, active));

    Publish(failed);
    EXPECT_EQ(recoveryCount_, 0);
    Publish(active);
    Publish(active);

    EXPECT_EQ(recoveryCount_, 1);
    EXPECT_EQ(recoveredAddress_, "127.0.0.1:31502");
}

TEST_F(WorkerOCServerTest, ActiveAddressWithoutFailureDoesNotPublishRecovery)
{
    std::shared_ptr<const cluster::TopologySnapshot> active;
    DS_ASSERT_OK(MakeSnapshot(cluster::MemberState::ACTIVE, 1, active));

    Publish(active);

    EXPECT_EQ(recoveryCount_, 0);
}

TEST_F(WorkerOCServerTest, LeavingSnapshotDoesNotWaitForRecoveredExitingPublication)
{
    std::shared_ptr<const cluster::TopologySnapshot> leaving;
    DS_ASSERT_OK(MakeLocalScaleInSnapshot(3, leaving));
    WaitPost publishEntered;
    WaitPost releasePublish;
    WaitPost snapshotReturned;
    SetScaleInExitPublishFn([&](int32_t) {
        publishEntered.Set();
        releasePublish.Wait();
        return Status::OK();
    });

    std::thread snapshotThread([&] {
        Publish(leaving);
        snapshotReturned.Set();
    });
    const bool entered = publishEntered.WaitFor(1'000);
    const bool returnedBeforeRelease = entered && snapshotReturned.WaitFor(1'000);
    releasePublish.Set();
    snapshotThread.join();

    EXPECT_TRUE(entered);
    EXPECT_TRUE(returnedBeforeRelease);
}

TEST_F(WorkerOCServerTest, LeavingLocalMemberRetriesRecoveredExitAndRequestsShutdownOnce)
{
    std::shared_ptr<const cluster::TopologySnapshot> leaving;
    std::shared_ptr<const cluster::TopologySnapshot> removed;
    DS_ASSERT_OK(MakeLocalScaleInSnapshot(3, leaving));
    DS_ASSERT_OK(MakeSnapshotWithoutLocal(4, removed));
    std::atomic<size_t> publishCount{ 0 };
    std::atomic<int32_t> publishTimeoutMs{ 0 };
    WaitPost firstAttempt;
    WaitPost releaseFirstAttempt;
    SetScaleInExitPublishFn([&](int32_t timeoutMs) {
        publishTimeoutMs.store(timeoutMs);
        const auto attempt = publishCount.fetch_add(1) + 1;
        if (attempt == 1) {
            firstAttempt.Set();
            releaseFirstAttempt.Wait();
            return Status(K_NOT_READY, "injected first publication failure");
        }
        return Status::OK();
    });

    EXPECT_TRUE(ShouldPublishReadyMembership(false));
    EXPECT_FALSE(ShouldPublishReadyMembership(true));
    Publish(leaving);
    const bool firstAttemptEntered = firstAttempt.WaitFor(1'000);
    MarkTopologyRuntimeStarted();
    releaseFirstAttempt.Set();
    ASSERT_TRUE(firstAttemptEntered);

    ASSERT_TRUE(WaitForScaleInExitPublished(std::chrono::milliseconds(1'000)));
    EXPECT_EQ(publishCount.load(), 2UL);
    EXPECT_GT(publishTimeoutMs.load(), 0);
    EXPECT_LE(publishTimeoutMs.load(), 1'000);
    EXPECT_TRUE(IsTopologyExitRequested());
    EXPECT_FALSE(ShouldPublishReadyMembership(false));
    ASSERT_TRUE(HasScaleInPreparationWorkers());
    const auto workerIds = ScaleInPreparationWorkerIds();

    Publish(leaving);

    ASSERT_TRUE(HasScaleInPreparationWorkers());
    EXPECT_EQ(ScaleInPreparationWorkerIds(), workerIds);
    EXPECT_EQ(publishCount.load(), 2UL);

    Publish(removed);
    Publish(removed);

    EXPECT_EQ(shutdownRequestCount_, 1UL);
}

TEST_F(WorkerOCServerTest, LeavingRecoveryFailsClosedAndRetriesAfterFirstWorkerCreationFailure)
{
    constexpr char injectPoint[] = "worker.RestoreScaleIn.beforeCheckAsyncTasksThreadCreation";
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    DS_ASSERT_OK(inject::Set(injectPoint, "1*return(K_RUNTIME_ERROR)"));
    std::shared_ptr<const cluster::TopologySnapshot> leaving;
    DS_ASSERT_OK(MakeLocalScaleInSnapshot(3, leaving));
    std::atomic<size_t> publishCount{ 0 };
    SetScaleInExitPublishFn([&](int32_t) {
        publishCount.fetch_add(1);
        return Status::OK();
    });

    EXPECT_EQ(RestoreScaleInPreparation(*leaving).GetCode(), K_RUNTIME_ERROR);

    EXPECT_TRUE(IsTopologyExitRequested());
    EXPECT_FALSE(ShouldPublishReadyMembership(false));
    EXPECT_FALSE(HasCheckAsyncTasksThread());
    EXPECT_FALSE(HasClientsExitChecker());
    EXPECT_EQ(publishCount.load(), 0UL);

    DS_ASSERT_OK(inject::Clear(injectPoint));
    DS_ASSERT_OK(RestoreScaleInPreparation(*leaving));

    ASSERT_TRUE(WaitForScaleInExitPublished(std::chrono::milliseconds(1'000)));
    EXPECT_TRUE(HasScaleInPreparationWorkers());
    EXPECT_EQ(publishCount.load(), 1UL);
}

TEST_F(WorkerOCServerTest, LeavingRecoveryKeepsPartialWorkersAndRetriesSecondWorkerCreation)
{
    constexpr char injectPoint[] = "worker.RestoreScaleIn.beforeClientsExitCheckerCreation";
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    DS_ASSERT_OK(inject::Set(injectPoint, "1*return(K_RUNTIME_ERROR)"));
    std::shared_ptr<const cluster::TopologySnapshot> leaving;
    DS_ASSERT_OK(MakeLocalScaleInSnapshot(3, leaving));
    std::atomic<size_t> publishCount{ 0 };
    SetScaleInExitPublishFn([&](int32_t) {
        publishCount.fetch_add(1);
        return Status::OK();
    });

    EXPECT_EQ(RestoreScaleInPreparation(*leaving).GetCode(), K_RUNTIME_ERROR);

    EXPECT_TRUE(IsTopologyExitRequested());
    EXPECT_FALSE(ShouldPublishReadyMembership(false));
    EXPECT_TRUE(HasCheckAsyncTasksThread());
    EXPECT_FALSE(HasClientsExitChecker());
    EXPECT_EQ(publishCount.load(), 0UL);

    DS_ASSERT_OK(inject::Clear(injectPoint));
    DS_ASSERT_OK(RestoreScaleInPreparation(*leaving));

    ASSERT_TRUE(WaitForScaleInExitPublished(std::chrono::milliseconds(1'000)));
    EXPECT_TRUE(HasScaleInPreparationWorkers());
    EXPECT_EQ(publishCount.load(), 1UL);
}

TEST_F(WorkerOCServerTest, ScaleInPreparationFailsClosedWhenFirstWorkerCreationFails)
{
    constexpr char injectPoint[] = "worker.RestoreScaleIn.beforeCheckAsyncTasksThreadCreation";
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    DS_ASSERT_OK(inject::Set(injectPoint, "1*return(K_RUNTIME_ERROR)"));

    EXPECT_EQ(StartScaleInPreparation().GetCode(), K_RUNTIME_ERROR);

    EXPECT_TRUE(IsTopologyExitRequested());
    EXPECT_FALSE(ShouldPublishReadyMembership(false));
    EXPECT_FALSE(HasCheckAsyncTasksThread());
    EXPECT_FALSE(HasClientsExitChecker());
}

TEST_F(WorkerOCServerTest, ScaleInPreparationFailsClosedWhenSecondWorkerCreationFails)
{
    constexpr char injectPoint[] = "worker.RestoreScaleIn.beforeClientsExitCheckerCreation";
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    DS_ASSERT_OK(inject::Set(injectPoint, "1*return(K_RUNTIME_ERROR)"));

    EXPECT_EQ(StartScaleInPreparation().GetCode(), K_RUNTIME_ERROR);

    EXPECT_TRUE(IsTopologyExitRequested());
    EXPECT_FALSE(ShouldPublishReadyMembership(false));
    EXPECT_TRUE(HasCheckAsyncTasksThread());
    EXPECT_FALSE(HasClientsExitChecker());

    StopPreShutdownWorkers();
    EXPECT_FALSE(HasCheckAsyncTasksThread());
    EXPECT_FALSE(HasClientsExitChecker());
}

TEST_F(WorkerOCServerTest, TopologyRemovalWithoutRequesterMonotonicallyCancelsOlderLeavingPublication)
{
    std::shared_ptr<const cluster::TopologySnapshot> leaving;
    std::shared_ptr<const cluster::TopologySnapshot> removed;
    DS_ASSERT_OK(MakeLocalScaleInSnapshot(3, leaving));
    DS_ASSERT_OK(MakeSnapshotWithoutLocal(4, removed));
    WaitPost firstPublishEntered;
    WaitPost releaseFirstPublish;
    std::atomic<size_t> publishCount{ 0 };
    server_->SetScaleInShutdownRequester(nullptr);
    SetScaleInExitPublishFn([&](int32_t) {
        publishCount.fetch_add(1);
        firstPublishEntered.Set();
        releaseFirstPublish.Wait();
        return Status(K_NOT_READY, "injected in-flight publication");
    });

    Publish(leaving);
    ASSERT_TRUE(firstPublishEntered.WaitFor(1'000));
    Publish(removed);
    releaseFirstPublish.Set();

    // Simulate the older LEAVING callback resuming after the newer removal callback.
    Publish(leaving);

    EXPECT_EQ(ScheduleScaleInExitPublication().GetCode(), K_NOT_READY);
    EXPECT_EQ(publishCount.load(), 1UL);
    EXPECT_EQ(shutdownRequestCount_, 0UL);
}

TEST_F(WorkerOCServerTest, RecoveredExitRetryUsesBoundedBackoffAndDistributesLargeBatches)
{
    constexpr size_t workerCounts[] = { 100, 1'000, 2'500 };
    for (const auto workerCount : workerCounts) {
        size_t retryBuckets[6] = {};
        for (size_t i = 0; i < workerCount; ++i) {
            const auto delay = ScaleInExitRetryDelay("127.0.0.1:" + std::to_string(31'501 + i), 1);
            EXPECT_GE(delay, std::chrono::seconds(1));
            EXPECT_LE(delay, std::chrono::seconds(6));
            const auto bucket = static_cast<size_t>((delay - std::chrono::seconds(1)).count() / 1'000);
            ++retryBuckets[std::min(bucket, std::size(retryBuckets) - 1)];
        }
        EXPECT_LE(*std::max_element(std::begin(retryBuckets), std::end(retryBuckets)), (workerCount * 3 + 9) / 10);
    }

    for (size_t consecutiveFailures = 1; consecutiveFailures <= 64; ++consecutiveFailures) {
        const auto delay = ScaleInExitRetryDelay("127.0.0.1:31501", consecutiveFailures);
        EXPECT_GE(delay, std::chrono::seconds(1));
        EXPECT_LE(delay, std::chrono::seconds(30));
    }
}

TEST_F(WorkerOCServerTest, RecoveredExitPublisherStopsDuringRetryBackoff)
{
    std::shared_ptr<const cluster::TopologySnapshot> leaving;
    DS_ASSERT_OK(MakeLocalScaleInSnapshot(3, leaving));
    WaitPost firstAttempt;
    SetScaleInExitPublishFn([&](int32_t) {
        firstAttempt.Set();
        return Status(K_NOT_READY, "injected persistent publication failure");
    });
    Publish(leaving);
    ASSERT_TRUE(firstAttempt.WaitFor(1'000));

    const auto start = std::chrono::steady_clock::now();
    StopScaleInExitPublisher();

    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(500));
}

TEST_F(WorkerOCServerTest, ExistingScaleInPreparationIsNotClaimedAsRestartRecovery)
{
    std::shared_ptr<const cluster::TopologySnapshot> leaving;
    std::shared_ptr<const cluster::TopologySnapshot> removed;
    DS_ASSERT_OK(MakeLocalScaleInSnapshot(3, leaving));
    DS_ASSERT_OK(MakeSnapshotWithoutLocal(4, removed));
    std::atomic<size_t> publishCount{ 0 };
    SetScaleInExitPublishFn([&](int32_t) {
        publishCount.fetch_add(1);
        return Status::OK();
    });
    DS_ASSERT_OK(StartScaleInPreparation());

    Publish(leaving);
    Publish(removed);

    EXPECT_EQ(publishCount.load(), 0UL);
    EXPECT_EQ(shutdownRequestCount_, 0UL);
}

TEST_F(WorkerOCServerTest, LocalUbVerificationRequiresActiveMultiWorkerTopology)
{
    std::shared_ptr<const cluster::TopologySnapshot> active;
    std::shared_ptr<const cluster::TopologySnapshot> single;
    std::shared_ptr<const cluster::TopologySnapshot> localAbsent;
    DS_ASSERT_OK(MakeSnapshot(cluster::MemberState::ACTIVE, cluster::MemberState::ACTIVE, 3, active));
    DS_ASSERT_OK(MakeSingleWorkerSnapshot(single));
    DS_ASSERT_OK(MakeMultiWorkerSnapshotWithoutLocal(4, localAbsent));

    EXPECT_TRUE(IsLocalUbVerificationEligible(*active));
    EXPECT_FALSE(IsLocalUbVerificationEligible(*single));
    EXPECT_FALSE(IsLocalUbVerificationEligible(*localAbsent));
}

TEST_F(WorkerOCServerTest, FailedProbeResultAttachesObservationOnlyToErrorOutcome)
{
    const auto peer = MakeMember('a', "127.0.0.1:1", cluster::MemberState::ACTIVE, 0).identity;

    const auto peerDead = server_->BuildFailedProbeResultForTest(peer, Status(K_RPC_PEER_DEAD, "peer dead"));
    EXPECT_EQ(peerDead.outcome, cluster::ControlBackendProbeOutcome::UNAVAILABLE);
    EXPECT_FALSE(peerDead.observation.has_value());

    const auto applicationError =
        server_->BuildFailedProbeResultForTest(peer, Status(K_RUNTIME_ERROR, "application error"));
    EXPECT_EQ(applicationError.outcome, cluster::ControlBackendProbeOutcome::ERROR);
    EXPECT_TRUE(applicationError.observation.has_value());

    const auto networkBlip =
        server_->BuildFailedProbeResultForTest(peer, Status(K_RPC_NETWORK_BLIP, "network blip"));
    EXPECT_EQ(networkBlip.outcome, cluster::ControlBackendProbeOutcome::ERROR);
    EXPECT_TRUE(networkBlip.observation.has_value());
}

TEST_F(WorkerOCServerTest, SuccessfulExitingPublicationIsNotRepeatedAndSleepHonorsDeadline)
{
    size_t publishCount = 0;
    size_t observeCount = 0;
    int32_t observedBudgetMs = 0;
    const auto start = std::chrono::steady_clock::now();
    const auto status = WaitForExitingRemoval(
        start + std::chrono::milliseconds(30),
        [&](int32_t timeoutMs) {
            ++publishCount;
            observedBudgetMs = timeoutMs;
            return Status::OK();
        },
        [&] {
            ++observeCount;
            return Status(K_NOT_READY, "local member is still present");
        },
        std::chrono::seconds(1));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(publishCount, 1UL);
    EXPECT_EQ(observeCount, 1UL);
    EXPECT_GT(observedBudgetMs, 0);
    EXPECT_LE(observedBudgetMs, 30);
    EXPECT_LT(elapsed, std::chrono::milliseconds(200));
}

TEST_F(WorkerOCServerTest, FailedExitingPublicationRetriesUntilSuccess)
{
    size_t publishCount = 0;
    size_t observeCount = 0;
    const auto status = WaitForExitingRemoval(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(200),
        [&](int32_t) {
            ++publishCount;
            return publishCount == 1 ? Status(K_RPC_UNAVAILABLE, "injected publish failure") : Status::OK();
        },
        [&] {
            ++observeCount;
            return observeCount == 1 ? Status(K_NOT_READY, "local member is still present") : Status::OK();
        },
        std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(publishCount, 2UL);
    EXPECT_EQ(observeCount, 2UL);
}

TEST_F(WorkerOCServerTest, StartupHealthPublicationFailureIsRetried)
{
    size_t refreshCount = 0;
    size_t probeCount = 0;
    const auto status = WaitForStartupHealth(
        [&] {
            ++refreshCount;
            return refreshCount == 1 ? Status(K_IO_ERROR, "injected health publication failure") : Status::OK();
        },
        [] { return true; },
        [&] {
            ++probeCount;
            return Status::OK();
        },
        [] { return false; });

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(refreshCount, 2UL);
    EXPECT_EQ(probeCount, 1UL);
}

TEST_F(WorkerOCServerTest, StartupHealthWaitReevaluatesReconciliationBeforeEachRefresh)
{
    size_t reconciliationCount = 0;
    size_t refreshCount = 0;
    bool reconciliationReady = false;
    bool healthy = false;
    const auto status = WaitForStartupHealth(
        [&] {
            reconciliationReady = ++reconciliationCount >= 2;
            return Status::OK();
        },
        [&] {
            ++refreshCount;
            healthy = reconciliationReady;
            return Status::OK();
        },
        [&] { return healthy; }, [] { return Status::OK(); }, [] { return false; });

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(reconciliationCount, 2UL);
    EXPECT_EQ(refreshCount, 2UL);
}

TEST_F(WorkerOCServerTest, StartupHealthWaitStopsCleanlyOnTermination)
{
    std::atomic<size_t> refreshCount{ 0 };
    const auto start = std::chrono::steady_clock::now();
    const auto status = WaitForStartupHealth(
        [&] {
            refreshCount.fetch_add(1, std::memory_order_relaxed);
            return Status(K_IO_ERROR, "persistent health publication failure");
        },
        [] { return false; }, [] { return Status::OK(); },
        [&] { return refreshCount.load(std::memory_order_relaxed) >= 2; });
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(refreshCount.load(std::memory_order_relaxed), 2UL);
    EXPECT_LT(elapsed, std::chrono::milliseconds(100));
}
}  // namespace datasystem::ut
