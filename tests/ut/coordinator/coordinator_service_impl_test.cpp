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
 * Description: Unit tests for Coordinator leader serving gates.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ut/common.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/rpc/bthread_utils.h"
#define private public
#include "datasystem/coordinator/coordinator_service_impl.h"
#include "datasystem/coordinator/topology_control_host.h"
#undef private

DS_DECLARE_bool(use_brpc);
DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace ut {
namespace {
constexpr uint16_t TEST_COORDINATOR_PORT = 18501;
constexpr uint64_t FOLLOWER_TERM = 7;
constexpr uint64_t RECOVERING_TERM = 8;
constexpr char RECOVERING_LEADER_ADDRESS[] = "127.0.0.1:18501";
constexpr char VALID_CLUSTER_NAME[] = "cluster-a";

class FakeCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &addresses) override
    {
        addresses = { "127.0.0.1:18501", "127.0.0.1:18502" };
        return Status::OK();
    }
};

class SlowProbeWatchDispatcher final : public coordinator::WatchDispatcherImpl {
public:
    explicit SlowProbeWatchDispatcher(WatchRegistry *registry, std::function<void()> onFirstProbe = {})
        : WatchDispatcherImpl(registry, "test-coordinator", WatchDispatcher::DEFAULT_DISPATCH_THREAD_COUNT),
          onFirstProbe_(std::move(onFirstProbe))
    {
    }

    coordinator::WorkerReachabilityProbeResult ProbeWorkerReachable(
        const std::string &, std::chrono::steady_clock::time_point deadline) override
    {
        if (!firstProbeCalled_.exchange(true) && onFirstProbe_) {
            onFirstProbe_();
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return { Status(K_RPC_DEADLINE_EXCEEDED, "probe deadline expired"), false };
        }
        SleepCurrentFor(std::min(deadline, now + std::chrono::milliseconds(150)) - now);
        return { Status(K_RPC_DEADLINE_EXCEEDED, "probe timed out"), true };
    }

private:
    std::function<void()> onFirstProbe_;
    std::atomic<bool> firstProbeCalled_{ false };
};

class CoordinatorServiceImplTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        savedUseBrpc_ = FLAGS_use_brpc;
        FLAGS_use_brpc = true;
        coordinator::CoordinatorRaftFlags raftFlags;
        raftFlags.localAddress = "127.0.0.1:18501";
        service_ = std::make_unique<coordinator::CoordinatorServiceImpl>(
            HostPort("127.0.0.1", TEST_COORDINATOR_PORT), std::make_shared<FakeCoordinatorDiscovery>(), 2,
            std::move(raftFlags));
        DS_ASSERT_OK(service_->Init(true));
    }

    void TearDown() override
    {
        if (service_ != nullptr) {
            DS_EXPECT_OK(service_->Shutdown());
        }
        FLAGS_use_brpc = savedUseBrpc_;
        CommonTest::TearDown();
    }

protected:
    std::string ValidMembershipValue() const
    {
        cluster::MembershipValue value{ 1, cluster::MemberLifecycleState::READY, "host-a", "v1" };
        std::string bytes;
        EXPECT_TRUE(cluster::MembershipValueCodec::Encode(value, bytes).IsOk());
        return bytes;
    }

    std::string MembershipKey(std::string_view workerAddress) const
    {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        EXPECT_TRUE(cluster::TopologyKeyHelper::Create(VALID_CLUSTER_NAME, keys).IsOk());
        return keys->MembershipTable() + "/" + std::string(workerAddress);
    }

    void ExpectHeader(const coordinator::ResponseHeader &header, bool isLeader,
                      coordinator::ResponseHeader::ServingStatePb servingState, uint64_t term,
                      std::string_view leaderAddress) const
    {
        EXPECT_EQ(header.is_leader(), isLeader);
        EXPECT_EQ(header.serving_state(), servingState);
        EXPECT_EQ(header.leader_term(), term);
        EXPECT_EQ(header.leader_address(), leaderAddress);
        EXPECT_FALSE(header.coordinator_id().empty());
    }

    void EnterRecoveringLeader()
    {
        service_->OnLeaderStart(RECOVERING_TERM);
        // These cases exercise recovery-control RPCs after recovery work has been discovered.
        service_->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::LEADER_RECOVERING,
                                      std::memory_order_release);
    }

    std::unique_ptr<coordinator::CoordinatorServiceImpl> service_;
    bool savedUseBrpc_{ false };
};

TEST_F(CoordinatorServiceImplTest, RecoveringLeaderAcceptsOnlyEnsureAndExistingRecoveryReport)
{
    EnterRecoveringLeader();

    coordinator::RangeReqPb rangeRequest;
    rangeRequest.set_key("/coordinator/recovering-fence");
    coordinator::RangeRspPb rangeResponse;
    // A recovering Leader returns a normal routing envelope but does not touch the local Store.
    DS_ASSERT_OK(service_->Range(rangeRequest, rangeResponse));
    EXPECT_TRUE(rangeResponse.kvs().empty());
    ExpectHeader(rangeResponse.header(), false, coordinator::ResponseHeader::LEADER_RECOVERING, RECOVERING_TERM,
                 RECOVERING_LEADER_ADDRESS);

    coordinator::EnsureLeaderMembershipReqPb ensureRequest;
    ensureRequest.set_leader_term(RECOVERING_TERM);
    ensureRequest.set_coordinator_id(rangeResponse.header().coordinator_id());
    ensureRequest.set_cluster_name(VALID_CLUSTER_NAME);
    ensureRequest.set_reporter_address("127.0.0.1:31501");
    ensureRequest.set_membership_value(ValidMembershipValue());
    ensureRequest.set_ttl_ms(10'000);
    coordinator::EnsureLeaderMembershipRspPb ensureResponse;
    DS_ASSERT_OK(service_->EnsureLeaderMembership(ensureRequest, ensureResponse));
    EXPECT_EQ(ensureResponse.result(), coordinator::EnsureLeaderMembershipRspPb::ACCEPTED);
    EXPECT_GT(ensureResponse.membership_mod_revision(), 0);
    ExpectHeader(ensureResponse.header(), false, coordinator::ResponseHeader::LEADER_RECOVERING,
                 RECOVERING_TERM, RECOVERING_LEADER_ADDRESS);
    {
        std::lock_guard<std::mutex> lock(service_->topologyControlHost_->mutex_);
        const auto entry = service_->topologyControlHost_->entries_.find(VALID_CLUSTER_NAME);
        ASSERT_NE(entry, service_->topologyControlHost_->entries_.end());
        EXPECT_EQ(entry->second->state, coordinator::TopologyControlHost::EntryState::WAITING_RECOVERY);
        EXPECT_TRUE(entry->second->hasCommittedMembership);
        EXPECT_EQ(entry->second->pendingMembershipPuts, 0UL);
    }

    coordinator::KeepAliveReqPb keepAliveRequest;
    keepAliveRequest.set_key(MembershipKey(ensureRequest.reporter_address()));
    keepAliveRequest.set_expected_coordinator_id(ensureResponse.header().coordinator_id());
    keepAliveRequest.set_expected_mod_revision(ensureResponse.membership_mod_revision());
    service_->topologyControlHost_->RecordWorkerFailureSummaries(VALID_CLUSTER_NAME, ensureRequest.reporter_address(),
                                                                 { "127.0.0.1:31502" });
    coordinator::KeepAliveRspPb keepAliveResponse;
    DS_ASSERT_OK(service_->KeepAlive(keepAliveRequest, keepAliveResponse));
    EXPECT_GT(keepAliveResponse.ttl(), 0);
    EXPECT_GT(keepAliveResponse.remaining_ttl(), 0);
    ExpectHeader(keepAliveResponse.header(), false, coordinator::ResponseHeader::LEADER_RECOVERING, RECOVERING_TERM,
                 RECOVERING_LEADER_ADDRESS);
    {
        std::lock_guard<std::mutex> lock(service_->topologyControlHost_->failureReportMutex_);
        const auto cluster = service_->topologyControlHost_->failureReportsByCluster_.find(VALID_CLUSTER_NAME);
        EXPECT_TRUE(cluster == service_->topologyControlHost_->failureReportsByCluster_.end()
                    || cluster->second.empty());
    }

    coordinator::ReportTopologyRecoveryCandidateReqPb reportRequest;
    reportRequest.set_cluster_name(VALID_CLUSTER_NAME);
    reportRequest.set_reporter_address("127.0.0.1:31501");
    reportRequest.set_result(coordinator::TOPOLOGY_RECOVERY_NO_SNAPSHOT);
    reportRequest.set_leader_term(RECOVERING_TERM);
    reportRequest.set_coordinator_id(rangeResponse.header().coordinator_id());
    coordinator::ReportTopologyRecoveryCandidateRspPb reportResponse;
    DS_ASSERT_OK(service_->ReportTopologyRecoveryCandidate(reportRequest, reportResponse));
    EXPECT_EQ(reportResponse.result(), coordinator::ReportTopologyRecoveryCandidateRspPb::ACCEPTED);
    ExpectHeader(reportResponse.header(), false, coordinator::ResponseHeader::LEADER_RECOVERING,
                 RECOVERING_TERM, RECOVERING_LEADER_ADDRESS);
}

TEST_F(CoordinatorServiceImplTest, ProbesMultipleFailedMembersWithinSharedDeadline)
{
    service_->coordinatorDiscovery_.reset();
    service_->expectedMemberCount_ = 0;
    service_->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING,
                                  std::memory_order_release);
    service_->watchDispatcher_ = std::make_shared<SlowProbeWatchDispatcher>(service_->watchRegistry_.get());
    std::vector<cluster::MemberIdentity> targets;
    for (size_t index = 0; index < 10; ++index) {
        targets.push_back({ "member-" + std::to_string(index), "127.0.0.1:" + std::to_string(31501 + index) });
    }

    const auto start = std::chrono::steady_clock::now();
    const auto results = service_->ProbeMembersLiveness(targets, start + std::chrono::milliseconds(250));
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

    ASSERT_EQ(results.size(), targets.size());
    EXPECT_TRUE(std::all_of(results.begin(), results.end(), [](const auto &result) {
        return result.outcome == cluster::ControlBackendProbeOutcome::DEADLINE_EXCEEDED;
    }));
    EXPECT_LT(elapsed, std::chrono::milliseconds(220));
}

TEST_F(CoordinatorServiceImplTest, DiscardsProbeResultsWhenControlEpochChanges)
{
    service_->coordinatorDiscovery_.reset();
    service_->expectedMemberCount_ = 0;
    service_->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING,
                                  std::memory_order_release);
    service_->watchDispatcher_ = std::make_shared<SlowProbeWatchDispatcher>(service_->watchRegistry_.get(), [this] {
        service_->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::STOPPING,
                                      std::memory_order_release);
    });
    const std::vector<cluster::MemberIdentity> targets{ { "member-0", "127.0.0.1:31501" },
                                                        { "member-1", "127.0.0.1:31502" } };

    const auto results =
        service_->ProbeMembersLiveness(targets, std::chrono::steady_clock::now() + std::chrono::milliseconds(250));
    service_->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING,
                                  std::memory_order_release);

    ASSERT_EQ(results.size(), targets.size());
    EXPECT_TRUE(std::all_of(results.begin(), results.end(), [](const auto &result) {
        return result.outcome == cluster::ControlBackendProbeOutcome::CANCELLED;
    }));
}

TEST_F(CoordinatorServiceImplTest, DisablesActiveFailureDirectProbeForZmq)
{
    FLAGS_use_brpc = false;
    coordinator::TopologyControlHost::Options options;

    service_->ConfigureTopologyHostOptions(options);

    EXPECT_FALSE(options.controller.memberLivenessProbe);
}

TEST_F(CoordinatorServiceImplTest, StaleTermEnsureDoesNotCreateMembership)
{
    EnterRecoveringLeader();

    coordinator::GetCoordinatorIdReqPb idRequest;
    coordinator::GetCoordinatorIdRspPb idResponse;
    DS_ASSERT_OK(service_->GetCoordinatorId(idRequest, idResponse));

    coordinator::EnsureLeaderMembershipReqPb request;
    request.set_cluster_name(VALID_CLUSTER_NAME);
    request.set_reporter_address("127.0.0.1:31501");
    request.set_coordinator_id(idResponse.header().coordinator_id());
    request.set_leader_term(FOLLOWER_TERM);
    request.set_membership_value(ValidMembershipValue());
    request.set_ttl_ms(10'000);
    coordinator::EnsureLeaderMembershipRspPb response;

    DS_ASSERT_OK(service_->EnsureLeaderMembership(request, response));
    EXPECT_EQ(response.result(), coordinator::EnsureLeaderMembershipRspPb::STALE_EPOCH);
    ExpectHeader(response.header(), false, coordinator::ResponseHeader::LEADER_RECOVERING, RECOVERING_TERM,
                 RECOVERING_LEADER_ADDRESS);

    // Recovery control remains available while business reads are gated. A current-round
    // report proves the rejected Ensure did not admit this Worker's membership.
    coordinator::ReportTopologyRecoveryCandidateReqPb reportRequest;
    reportRequest.set_cluster_name(VALID_CLUSTER_NAME);
    reportRequest.set_reporter_address("127.0.0.1:31501");
    reportRequest.set_result(coordinator::TOPOLOGY_RECOVERY_NO_SNAPSHOT);
    reportRequest.set_coordinator_id(idResponse.header().coordinator_id());
    reportRequest.set_leader_term(RECOVERING_TERM);
    coordinator::ReportTopologyRecoveryCandidateRspPb reportResponse;
    DS_ASSERT_OK(service_->ReportTopologyRecoveryCandidate(reportRequest, reportResponse));
    EXPECT_EQ(reportResponse.result(), coordinator::ReportTopologyRecoveryCandidateRspPb::MEMBERSHIP_NOT_READY);
}

TEST_F(CoordinatorServiceImplTest, MembershipDeleteCompletesRecoveringLeaderGate)
{
    EnterRecoveringLeader();
    const std::string workerAddress = "127.0.0.1:31501";
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(service_->store_->Put(MembershipKey(workerAddress), ValidMembershipValue(), 10'000,
                                      COORDINATOR_KEY_NOT_EXISTS_VERSION, version, revision));
    ASSERT_EQ(service_->topologyRecoveryManager_->GetRoundSummary().recoveringCount, 1U);

    coordinator::DeleteRangeReqPb request;
    request.set_key(MembershipKey(workerAddress));
    coordinator::DeleteRangeRspPb response;
    DS_ASSERT_OK(service_->DeleteRange(request, response));
    EXPECT_EQ(response.deleted(), 0);
    EXPECT_EQ(service_->topologyRecoveryManager_->GetRoundSummary().recoveringCount, 1U);

    request.set_expected_coordinator_id(service_->coordinatorId_);
    request.set_expected_mod_revision(revision);
    response.Clear();
    DS_ASSERT_OK(service_->DeleteRange(request, response));

    EXPECT_EQ(response.deleted(), 1);
    EXPECT_TRUE(response.header().is_leader());
    EXPECT_EQ(service_->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING);

    service_->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::LEADER_RECOVERING,
                                  std::memory_order_release);
    response.Clear();
    DS_ASSERT_OK(service_->DeleteRange(request, response));
    EXPECT_EQ(response.deleted(), 0);
    EXPECT_TRUE(response.header().is_leader());
}

TEST_F(CoordinatorServiceImplTest, CoordinatorIdProbeRejectsUnknownLeaderButReturnsRecoveringLeader)
{
    coordinator::GetCoordinatorIdReqPb request;
    coordinator::GetCoordinatorIdRspPb response;

    EXPECT_EQ(service_->GetCoordinatorId(request, response).GetCode(), K_NOT_READY);

    EnterRecoveringLeader();
    response.Clear();
    DS_ASSERT_OK(service_->GetCoordinatorId(request, response));
    ExpectHeader(response.header(), false, coordinator::ResponseHeader::LEADER_RECOVERING, RECOVERING_TERM,
                 RECOVERING_LEADER_ADDRESS);

    service_->OnLeaderStop(Status(K_RUNTIME_ERROR, "leadership lost"));
    response.Clear();
    EXPECT_EQ(service_->GetCoordinatorId(request, response).GetCode(), K_NOT_READY);
}

TEST_F(CoordinatorServiceImplTest, LeaderStopImmediatelyRevokesBusinessServing)
{
    EnterRecoveringLeader();
    service_->OnLeaderStop(Status(K_RUNTIME_ERROR, "leadership lost"));

    coordinator::GetCoordinatorIdReqPb request;
    coordinator::GetCoordinatorIdRspPb response;
    EXPECT_EQ(service_->GetCoordinatorId(request, response).GetCode(), K_NOT_READY);

    coordinator::RangeReqPb rangeRequest;
    rangeRequest.set_key("/coordinator/revoked-leader");
    coordinator::RangeRspPb rangeResponse;
    EXPECT_EQ(service_->Range(rangeRequest, rangeResponse).GetCode(), K_NOT_READY);
}

TEST_F(CoordinatorServiceImplTest, RecoveryGateRestoresLeaderRoundTrace)
{
    constexpr char EXPECTED_TRACE_ID[] = "CoordinatorBootstrap;recovery-gate-test";
    std::promise<std::string> observedTracePromise;
    auto observedTrace = observedTracePromise.get_future();
    service_->recoveryWindowTraceHook_ = [this, &observedTracePromise] {
        if (std::this_thread::get_id() == service_->recoveryGateThread_.get_id()) {
            observedTracePromise.set_value(Trace::Instance().GetTraceID());
        }
    };
    const auto savedNodeDeadTimeout = FLAGS_node_dead_timeout_s;
    Raii restoreTestState([this, savedNodeDeadTimeout] {
        FLAGS_node_dead_timeout_s = savedNodeDeadTimeout;
        std::unique_lock<std::shared_mutex> operationLock(service_->leaderOperationMutex_);
        service_->recoveryWindowTraceHook_ = {};
    });

    {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(EXPECTED_TRACE_ID);
        service_->OnLeaderStart(RECOVERING_TERM);
    }
    ASSERT_TRUE(Trace::Instance().GetTraceID().empty());
    service_->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::LEADER_RECOVERING,
                                  std::memory_order_release);
    FLAGS_node_dead_timeout_s = 0;
    service_->recoveryGateCv_.notify_all();

    ASSERT_EQ(observedTrace.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(observedTrace.get(), EXPECTED_TRACE_ID);
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
