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
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "tests/support/fake_ub_port_status_provider.h"
#include "common/binmock/binmock.h"
#include "datasystem/common/eventloop/timer_queue.h"

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "ut/common.h"
#include <bthread/countdown_event.h>
#define private public
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#undef private
#include "tests/ut/worker/object_cache/test_metadata_route.h"
#include "ut/common.h"

namespace datasystem::object_cache {
namespace {
const HostPort SELF("127.0.0.1", 18481);
const HostPort PEER("127.0.0.1", 18482);
constexpr char INCARNATION[] = "self-incarn-0001";
constexpr char PEER_INCARNATION[] = "peer-incarn-0001";

using FakeUbPortStatusProvider = ::datasystem::test::FakeUbPortStatusProvider;

template <typename Predicate>
bool WaitUntil(Predicate &&predicate, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}

void PublishSelf(cluster::TopologySnapshotState &snapshots)
{
    cluster::TopologyState state;
    state.version = 1;
    state.members = {
        cluster::Member{ { INCARNATION, SELF.ToString() }, cluster::MemberState::ACTIVE, { 1 } },
        cluster::Member{ { PEER_INCARNATION, PEER.ToString() }, cluster::MemberState::ACTIVE, { 2 } }
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    ASSERT_TRUE(cluster::TopologySnapshot::Create(
                    std::move(state), 1, std::string(64, 'a'), snapshot)
                    .IsOk());
    cluster::SnapshotUpdateOutcome outcome;
    ASSERT_TRUE(snapshots.Publish(std::move(snapshot), outcome).IsOk());
}

class QueryUbPortHealthTest : public ::datasystem::ut::CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        PublishSelf(snapshots_);
        objectTable_ = std::make_shared<ObjectTable>();
        auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
            objectTable_, SELF, SELF, ::datasystem::ut::GetTestMetadataRoute(), nullptr);
        service_ = std::make_shared<WorkerOCServiceImpl>(
            SELF, SELF, objectTable_, nullptr, evictionManager, nullptr, nullptr, nullptr,
            nullptr, ::datasystem::ut::GetTestMetadataRoute(), membership_, &exitRequested_, false, false);
        provider_ = std::make_shared<FakeUbPortStatusProvider>(
            std::vector<UbPortStatus>{ { 0, UbPortState::GOOD }, { 1, UbPortState::BAD },
                                       { 2, UbPortState::GOOD }, { 3, UbPortState::GOOD } });
    }

    void ConfigurePortHealth()
    {
        std::weak_ptr<IUbPortHealthObserver> observer(service_);
        monitor_ = UbPortHealthMonitor::CreateForTest(provider_, std::chrono::milliseconds(10));
        DS_ASSERT_OK(service_->ConfigureSelfPortHealth(monitor_, observer));
        DS_ASSERT_OK(monitor_->Start());
        (void)monitor_->EnsureFresh(std::chrono::milliseconds(10));
        ASSERT_TRUE(monitor_->GetSummary().has_value());
    }

    void ConfigureQueryDriver()
    {
        ASSERT_TRUE(TimerQueue::GetInstance()->Initialize());
        WorkerOcServiceCrudParam param{
            .workerRequestManager = service_->workerRequestManager_,
            .objectTable = objectTable_,
            .metadataRouteResolver = &::datasystem::ut::GetTestMetadataRoute(),
            .endpointPolicy = &service_->endpointPolicy_,
            .exitRequested = &exitRequested_
        };
        service_->threadPool_ = std::make_shared<ThreadPool>(0, 1, "test-ub-driver");
        service_->getProc_ = std::make_shared<WorkerOcServiceGetImpl>(
            param, nullptr, nullptr, nullptr, SELF, nullptr, service_->ubAdmission_);
    }

protected:
    cluster::TopologySnapshotState snapshots_;
    cluster::MembershipEndpointView membership_{ snapshots_ };
    std::atomic<bool> exitRequested_{ false };
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<FakeUbPortStatusProvider> provider_;
    std::shared_ptr<UbPortHealthMonitor> monitor_;
    std::shared_ptr<WorkerOCServiceImpl> service_;
};
}  // namespace

TEST_F(QueryUbPortHealthTest, QueryDriverRefillsFreedSlotWithoutWaitingForSlowestPeer)
{
    ConfigureQueryDriver();
    std::mutex mutex;
    std::condition_variable changed;
    size_t started = 0;
    bool releaseAll = false;
    HostPort allowed;
    std::vector<HostPort> peers;
    using testing::_;
    BINEXPECT_CALL(&WorkerOcServiceGetImpl::QueryPeerUbPortHealth, (_, _, _, _))
        .WillRepeatedly(testing::Invoke([&](const HostPort &peer, const std::string &incarnation,
                                          int32_t, UbHealthSummary &summary) {
            std::unique_lock<std::mutex> lock(mutex);
            ++started;
            changed.notify_all();
            changed.wait(lock, [&] { return releaseAll || peer == allowed; });
            summary.worker = peer;
            summary.incarnation = incarnation;
            summary.portHealth = { true, 4, 0, 1, false };
            return Status::OK();
        }));
    Raii cleanup([&] {
        {
            std::lock_guard<std::mutex> lock(mutex);
            releaseAll = true;
            changed.notify_all();
        }
        service_->ubHealthCallbackState_->Detach();
        service_->threadPool_.reset();
        service_->remoteUbPortHealthQueryPool_.reset();
        RELEASE_STUBS
    });
    for (int i = 0; i < 5; ++i) {
        peers.emplace_back("127.0.0.1", 18482 + i);
        ASSERT_TRUE(service_->remoteUbPortHealthVerifier_->RequestVerification(peers.back(), PEER_INCARNATION, 0));
    }
    service_->SchedulePeerUbPortHealthVerification();
    auto sentinel = service_->threadPool_->Submit([] {});
    EXPECT_EQ(sentinel.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(changed.wait_for(lock, std::chrono::seconds(2), [&] { return started == 4; }));
        EXPECT_EQ(service_->remoteUbQueriesInFlight_->load(), 4u);
        allowed = peers[1];
        changed.notify_all();
        EXPECT_TRUE(changed.wait_for(lock, std::chrono::seconds(2), [&] { return started == 5; }));
    }
    EXPECT_EQ(service_->remoteUbQueriesInFlight_->load(), 4u);
    auto detach = std::async(std::launch::async, [&] { service_->ubHealthCallbackState_->Detach(); });
    EXPECT_EQ(detach.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(mutex);
        releaseAll = true;
        changed.notify_all();
    }
    detach.get();
    EXPECT_FALSE(service_->ubHealthCallbackState_->IsAttached());
}

TEST_F(QueryUbPortHealthTest, QuerySubmissionFailureReturnsSlotAndRetainsRetry)
{
    ConfigureQueryDriver();
    Raii cleanup([&] {
        (void)inject::Clear("WorkerOCServiceImpl.DispatchUbPortHealthQuery");
        service_->ubHealthCallbackState_->Detach();
        service_->remoteUbPortHealthQueryPool_.reset();
    });
    DS_ASSERT_OK(inject::Set("WorkerOCServiceImpl.DispatchUbPortHealthQuery", "1*call()"));
    service_->RequestPeerUbPortHealthVerification(PEER);
    ASSERT_TRUE(WaitUntil([&] {
        return service_->remoteUbQueriesInFlight_->load() == 0u
               && service_->remoteUbPortHealthVerifier_->NextQueryDeadlineMs().has_value();
    }, std::chrono::seconds(2)));
    EXPECT_EQ(service_->remoteUbQueriesInFlight_->load(), 0u);
    EXPECT_TRUE(service_->remoteUbPortHealthVerifier_->NextQueryDeadlineMs().has_value());
}

TEST_F(QueryUbPortHealthTest, CallbackLeaseAllowsConcurrentWorkAndDetachDrainsExistingUsers)
{
    auto callback = service_->ubHealthCallbackState_;
    std::future<void> detach;
    {
        auto lease = callback->Acquire();
        ASSERT_TRUE(static_cast<bool>(lease));
        auto other = std::async(std::launch::async, [&] {
            cluster::RemoteUbQueryTicket ticket{ PEER, PEER_INCARNATION, 1 };
            return callback->ApplyVerifiedPortHealth(ticket, UbPortHealthSummary{ true, 4, 4, 1, false });
        });
        EXPECT_EQ(other.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        EXPECT_TRUE(other.get());
        detach = std::async(std::launch::async, [&] { callback->Detach(); });
        EXPECT_EQ(detach.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    }
    detach.get();
    EXPECT_FALSE(callback->IsAttached());
    EXPECT_FALSE(callback->ApplyVerifiedPortHealth({ PEER, PEER_INCARNATION, 2 },
                                                   UbPortHealthSummary{ true, 4, 0, 2, false }));
    service_.reset();
    callback->RequestVerification(PEER);
    callback->ScheduleVerification();
}

TEST_F(QueryUbPortHealthTest, BthreadLeasesRejectNewUsersAndDrainAfterSpuriousWake)
{
    constexpr size_t users = 8;
    struct Context {
        std::shared_ptr<WorkerOCServiceImpl::UbHealthCallbackState> callback;
        bthread::CountdownEvent entered{ users };
        bthread::CountdownEvent release{ 1 };
        std::atomic<size_t> rejected{ 0 };
    } context{ service_->ubHealthCallbackState_ };
    std::future<void> detach;
    {
        auto lastLease = context.callback->Acquire();
        ASSERT_TRUE(static_cast<bool>(lastLease));
        std::vector<bthread_t> threads;
        bool released = false;
        Raii cleanup([&] {
            if (!released) {
                context.release.signal();
            }
            for (const auto thread : threads) {
                EXPECT_EQ(bthread_join(thread, nullptr), 0);
            }
        });
        for (size_t i = 0; i < users; ++i) {
            bthread_t thread;
            auto run = [](void *arg) -> void * {
                auto &shared = *static_cast<Context *>(arg);
                auto lease = shared.callback->Acquire();
                if (!lease) {
                    ++shared.rejected;
                }
                shared.entered.signal();
                shared.release.wait();
                return nullptr;
            };
            ASSERT_EQ(bthread_start_background(&thread, nullptr, run, &context), 0);
            threads.push_back(thread);
        }
        ASSERT_EQ(context.entered.timed_wait(butil::seconds_from_now(2)), 0);
        EXPECT_EQ(context.rejected.load(), 0u);
        detach = std::async(std::launch::async, [&] { context.callback->Detach(); });
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (context.callback->IsAttached() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        EXPECT_FALSE(context.callback->IsAttached());
        EXPECT_FALSE(static_cast<bool>(context.callback->Acquire()));
        context.callback->drained_.notify_all();
        EXPECT_EQ(detach.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
        context.release.signal();
        released = true;
        for (const auto thread : threads) {
            EXPECT_EQ(bthread_join(thread, nullptr), 0);
        }
        threads.clear();
        EXPECT_EQ(detach.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    }
    EXPECT_EQ(detach.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    detach.get();
    EXPECT_EQ(context.callback->activeCallbacks_, 0u);
    service_.reset();
    EXPECT_FALSE(static_cast<bool>(context.callback->Acquire()));
}

TEST_F(QueryUbPortHealthTest, ReturnsCachedAggregateWithCurrentWorkerIdentity)
{
    ConfigurePortHealth();
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation(INCARNATION);
    QueryUbPortHealthRspPb response;

    DS_ASSERT_OK(service_->QuerySelfUbPortHealth(request, response));

    EXPECT_GE(provider_->Calls(), 1u);
    ASSERT_TRUE(response.has_health_summary());
    UbHealthSummary summary;
    DS_ASSERT_OK(DecodeUbHealthSummary(response.health_summary(), summary));
    EXPECT_EQ(summary.worker, SELF);
    EXPECT_EQ(summary.incarnation, INCARNATION);
    ASSERT_TRUE(summary.portHealth.has_value());
    EXPECT_EQ(summary.portHealth->totalPortCount, 4u);
    EXPECT_EQ(summary.portHealth->badPortCount, 1u);
    EXPECT_EQ(summary.portHealth->healthEpoch, UB_PORT_HEALTH_FIRST_EPOCH);
}

TEST_F(QueryUbPortHealthTest, RejectsStaleIncarnationBeforeCallingQuery)
{
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation("retired-incarnation");
    QueryUbPortHealthRspPb response;

    auto status = service_->QuerySelfUbPortHealth(request, response);

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(provider_->Calls(), 0u);
    EXPECT_FALSE(response.has_health_summary());
}

TEST_F(QueryUbPortHealthTest, QueryFailurePublishesExplicitUnknownWithoutSyntheticCounts)
{
    provider_->SetResult(Status(K_RPC_DEADLINE_EXCEEDED, "cached health refresh pending"), {});
    ConfigurePortHealth();
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation(INCARNATION);
    QueryUbPortHealthRspPb response;

    DS_ASSERT_OK(service_->QuerySelfUbPortHealth(request, response));
    EXPECT_GE(provider_->Calls(), 1u);
    ASSERT_TRUE(response.has_health_summary());
    UbHealthSummary summary;
    DS_ASSERT_OK(DecodeUbHealthSummary(response.health_summary(), summary));
    ASSERT_TRUE(summary.portHealth.has_value());
    EXPECT_FALSE(summary.portHealth->valid);
    EXPECT_TRUE(summary.portHealth->verificationPending);
    EXPECT_EQ(summary.portHealth->totalPortCount, 0u);
    EXPECT_EQ(summary.portHealth->badPortCount, 0u);
}

TEST_F(QueryUbPortHealthTest, PendingCacheIsReturnedWithoutInventingNewCounts)
{
    provider_->SetResult(Status::OK(), { { 0, UbPortState::GOOD }, { 1, UbPortState::BAD },
                                         { 2, UbPortState::BAD }, { 3, UbPortState::GOOD } });
    ConfigurePortHealth();
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation(INCARNATION);
    QueryUbPortHealthRspPb response;
    DS_ASSERT_OK(service_->QuerySelfUbPortHealth(request, response));
    const auto completed = provider_->Calls();
    provider_->SetResult(Status(K_RPC_DEADLINE_EXCEEDED, "refresh failed"), {});
    UbOpOutcome localFailure(SELF, UbOperationKind::CLIENT_PUT,
                             Status(K_URMA_ERROR, "CQE status 4"));
    localFailure.cqeStatus = URMA_PORT_UNAVAILABLE_STATUS;
    service_->GetUbAdmission()->ReportOutcome(localFailure);
    ASSERT_TRUE(provider_->WaitForCalls(completed + 1, std::chrono::seconds(1)));
    response.Clear();

    DS_ASSERT_OK(service_->QuerySelfUbPortHealth(request, response));

    UbHealthSummary summary;
    DS_ASSERT_OK(DecodeUbHealthSummary(response.health_summary(), summary));
    ASSERT_TRUE(summary.portHealth.has_value());
    EXPECT_EQ(summary.portHealth->totalPortCount, 4u);
    EXPECT_EQ(summary.portHealth->badPortCount, 2u);
    EXPECT_EQ(summary.portHealth->healthEpoch, UB_PORT_HEALTH_FIRST_EPOCH);
    EXPECT_TRUE(summary.portHealth->verificationPending);
}

TEST_F(QueryUbPortHealthTest, MonitorObserverPublishesWholeAggregateWithoutPorts)
{
    provider_->SetResult(Status::OK(), { { 0, UbPortState::GOOD }, { 1, UbPortState::BAD },
                                         { 2, UbPortState::BAD }, { 3, UbPortState::GOOD },
                                         { 4, UbPortState::GOOD }, { 5, UbPortState::GOOD } });
    ConfigurePortHealth();
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation(INCARNATION);
    QueryUbPortHealthRspPb response;
    DS_ASSERT_OK(service_->QuerySelfUbPortHealth(request, response));
    auto summary = service_->BuildSelfUbHealthSummary();

    ASSERT_TRUE(summary.portHealth.has_value());
    EXPECT_EQ(summary.portHealth->totalPortCount, 6u);
    EXPECT_EQ(summary.portHealth->badPortCount, 2u);
    EXPECT_EQ(summary.portHealth->healthEpoch, UB_PORT_HEALTH_FIRST_EPOCH);
    auto published = service_->GetPublishedSelfUbHealthSummary();
    ASSERT_TRUE(published.has_value());
    ASSERT_TRUE(published->portHealth.has_value());
    EXPECT_EQ(published->portHealth->totalPortCount, 6u);
    EXPECT_EQ(published->portHealth->badPortCount, 2u);
    EXPECT_EQ(published->portHealth->healthEpoch, UB_PORT_HEALTH_FIRST_EPOCH);
}

TEST_F(QueryUbPortHealthTest, BroadcastDoesNotCreateQueriesForUnusedPeers)
{
    UbHealthSummary broadcast;
    broadcast.worker = PEER;
    broadcast.incarnation = PEER_INCARNATION;
    broadcast.portHealth = UbPortHealthSummary{ true, 4, 4, 1, false };

    service_->ReplaceGlobalUbHealthSummaries({ broadcast });

    EXPECT_FALSE(service_->remoteUbPortHealthVerifier_->NextQueryDeadlineMs().has_value());
    service_->RequestPeerUbPortHealthVerification(PEER);
    EXPECT_TRUE(service_->remoteUbPortHealthVerifier_->NextQueryDeadlineMs().has_value());
}

TEST_F(QueryUbPortHealthTest, QueryApplicationRechecksMembershipAndDetachedOwner)
{
    auto callback = std::make_shared<WorkerOCServiceImpl::UbHealthCallbackState>(service_.get());
    cluster::RemoteUbQueryTicket ticket{ PEER, "retired-peer", 1 };
    const UbPortHealthSummary allDown{ true, 4, 4, 1, false };
    EXPECT_FALSE(callback->ApplyVerifiedPortHealth(ticket, allDown));
    EXPECT_FALSE(service_->GetUbAdmission()->GetState(PEER).has_value());
    ticket.incarnation = PEER_INCARNATION;
    ASSERT_TRUE(callback->ApplyVerifiedPortHealth(ticket, allDown));
    EXPECT_EQ(service_->GetUbAdmission()->GetState(PEER)->state, UbAdmissionState::UNAVAILABLE);
    callback->Detach();
    EXPECT_FALSE(callback->ApplyVerifiedPortHealth(ticket, UbPortHealthSummary{ true, 4, 3, 2, false }));
    EXPECT_EQ(service_->GetUbAdmission()->GetState(PEER)->state, UbAdmissionState::UNAVAILABLE);
}

TEST_F(QueryUbPortHealthTest, StalePeerSummaryCannotEnablePortHealthVerification)
{
    UbHealthSummary stale;
    stale.worker = PEER;
    stale.incarnation = "retired-peer-incarnation";
    stale.portHealth = UbPortHealthSummary{ true, 4, 3, 1, false };
    service_->ObservePeerUbHealthSummary(stale);
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "remote ACK timed out"));
    outcome.cqeStatus = URMA_REMOTE_ACK_TIMEOUT_STATUS;

    service_->GetUbAdmission()->ReportOutcome(outcome);

    auto state = service_->GetUbAdmission()->GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::UNAVAILABLE);
}

TEST_F(QueryUbPortHealthTest, CurrentPeerSummaryEnablesIndependentVerification)
{
    UbHealthSummary current;
    current.worker = PEER;
    current.incarnation = PEER_INCARNATION;
    current.portHealth = UbPortHealthSummary{ true, 4, 3, 1, false };
    service_->ObservePeerUbHealthSummary(current);
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "remote ACK timed out"));
    outcome.cqeStatus = URMA_REMOTE_ACK_TIMEOUT_STATUS;

    service_->GetUbAdmission()->ReportOutcome(outcome);

    auto state = service_->GetUbAdmission()->GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
}
TEST_F(QueryUbPortHealthTest, QueryDoesNotWaitForBlockedProvider)
{
    provider_->Pause();
    monitor_ = UbPortHealthMonitor::CreateForTest(provider_, std::chrono::seconds(1));
    DS_ASSERT_OK(service_->ConfigureSelfPortHealth(monitor_));
    DS_ASSERT_OK(monitor_->Start());
    ASSERT_TRUE(provider_->WaitForCalls(1));
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation(INCARNATION);
    QueryUbPortHealthRspPb response;
    auto query = std::async(std::launch::async, [&] { return service_->QuerySelfUbPortHealth(request, response); });
    const auto ready = query.wait_for(std::chrono::milliseconds(200));
    provider_->Resume();
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_EQ(query.get().GetCode(), K_NOT_READY);
    EXPECT_FALSE(response.has_health_summary());
}

}  // namespace datasystem::object_cache
