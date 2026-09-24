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
 * Description: Unit tests for Coordinator service lifecycle state.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <google/protobuf/arena.h>

#include "ut/common.h"
#include "ut/bthread_test_helper.h"

#include <array>
#include <unordered_map>
#include <utility>

#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/rpc/bthread_utils.h"
#define private public
#include "datasystem/coordinator/topology_recovery_manager.h"
#include "datasystem/coordinator/coordinator_service_impl.h"
#undef private
#include "datasystem/coordinator/topology_control_host.h"

DS_DECLARE_bool(use_brpc);
DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace ut {
namespace {
constexpr uint16_t TEST_COORDINATOR_PORT = 18501;
constexpr uint64_t LEADER_TERM = 8;
constexpr size_t ELECTION_MEMBER_COUNT = 3;
constexpr char COORDINATOR_ID[] = "coordinator-id";
constexpr char MISMATCHED_COORDINATOR_ID[] = "mismatched-coordinator-id";
constexpr char LEADER_ADDRESS[] = "127.0.0.1:18502";
constexpr char CLUSTER_NAME[] = "cluster-a";
constexpr char MEMBER_ADDRESS[] = "127.0.0.1:31501";
constexpr int64_t MEMBERSHIP_TTL_MS = 60'000;

std::unique_ptr<coordinator::CoordinatorServiceImpl> MakeService()
{
    return std::make_unique<coordinator::CoordinatorServiceImpl>(HostPort("127.0.0.1", TEST_COORDINATOR_PORT), nullptr,
                                                                  0);
}

void SetRunning(coordinator::CoordinatorServiceImpl &service)
{
    service.coordinatorId_ = COORDINATOR_ID;
    service.lifecycleState_.store(coordinator::CoordinatorServiceImpl::LifecycleState::RUNNING,
                                  std::memory_order_release);
}

Status InitializeRunning(coordinator::CoordinatorServiceImpl &service)
{
    RETURN_IF_NOT_OK(service.Init());
    service.lifecycleState_.store(coordinator::CoordinatorServiceImpl::LifecycleState::RUNNING,
                                  std::memory_order_release);
    return Status::OK();
}

void ExpectHeaderState(const coordinator::ResponseHeader &header, coordinator::ResponseHeader::StatePb expected,
                       const std::string &coordinatorId, uint64_t expectedTerm = 0)
{
    EXPECT_EQ(header.state(), expected);
    EXPECT_EQ(header.coordinator_id(), coordinatorId);
    EXPECT_EQ(header.leader_term(), expectedTerm);
    EXPECT_TRUE(header.leader_address().empty());
}

std::string EncodeMembershipValue()
{
    cluster::MembershipValue membership;
    membership.timestamp = 1;
    membership.lifecycleState = cluster::MemberLifecycleState::READY;
    membership.hostId = "host-a";
    std::string encoded;
    EXPECT_TRUE(cluster::MembershipValueCodec::Encode(membership, encoded).IsOk());
    return encoded;
}

class TestCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        serviceList.clear();
        return Status::OK();
    }
};

class CoordinatorServiceMetricsTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        service_ = MakeService();
        DS_ASSERT_OK(service_->Init());
        metrics::ResetKvMetricsForTest();
        DS_ASSERT_OK(metrics::InitKvMetrics());
    }

    void TearDown() override
    {
        DS_EXPECT_OK(service_->Shutdown());
        metrics::ResetKvMetricsForTest();
        CommonTest::TearDown();
    }

protected:
    std::unique_ptr<coordinator::CoordinatorServiceImpl> service_;
};

TEST_F(CoordinatorServiceMetricsTest, ExpiredWatchProbeDoesNotRecordNotificationRpcMetrics)
{
    auto result =
        service_->watchDispatcher_->ProbeWorkerReachable("127.0.0.1:1", std::chrono::steady_clock::now());
    EXPECT_EQ(result.status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_FALSE(result.rpcDispatched);
    const auto summaries = metrics::DumpSummariesForTest();
    ASSERT_EQ(summaries.size(), 1U);
    EXPECT_NE(summaries.front().find("\"metrics\":[]"), std::string::npos);
}

TEST_F(CoordinatorServiceMetricsTest, EveryRpcHandlerIncrementsOnlyItsRequestCounter)
{
    const auto expectSingleIncrement = [](const char *name, const auto &call) {
        call();
        std::string summary;
        for (const auto &part : metrics::DumpSummariesForTest()) {
            summary += part;
        }
        const std::string metric = "\"name\":\"" + std::string(name) + "\",\"total\":1,\"delta\":1";
        EXPECT_NE(summary.find(metric), std::string::npos) << name;
        size_t changedCounterCount = 0;
        size_t offset = 0;
        while ((offset = summary.find("\"delta\":1", offset)) != std::string::npos) {
            ++changedCounterCount;
            ++offset;
        }
        EXPECT_EQ(changedCounterCount, 1U) << name;
    };

    coordinator::PutReqPb putReq;
    coordinator::PutRspPb putRsp;
    expectSingleIncrement("coordinator_rpc_put_request_total", [&] { (void)service_->Put(putReq, putRsp); });
    coordinator::RangeReqPb rangeReq;
    coordinator::RangeRspPb rangeRsp;
    expectSingleIncrement("coordinator_rpc_range_request_total", [&] { (void)service_->Range(rangeReq, rangeRsp); });
    coordinator::DeleteRangeReqPb deleteReq;
    coordinator::DeleteRangeRspPb deleteRsp;
    expectSingleIncrement("coordinator_rpc_delete_range_request_total",
                          [&] { (void)service_->DeleteRange(deleteReq, deleteRsp); });
    coordinator::WatchRangeReqPb watchReq;
    coordinator::WatchRangeRspPb watchRsp;
    expectSingleIncrement("coordinator_rpc_watch_range_request_total",
                          [&] { (void)service_->WatchRange(watchReq, watchRsp); });
    coordinator::CancelWatchReqPb cancelReq;
    coordinator::CancelWatchRspPb cancelRsp;
    expectSingleIncrement("coordinator_rpc_cancel_watch_request_total",
                          [&] { (void)service_->CancelWatch(cancelReq, cancelRsp); });
    coordinator::KeepAliveReqPb keepAliveReq;
    coordinator::KeepAliveRspPb keepAliveRsp;
    expectSingleIncrement("coordinator_rpc_keep_alive_request_total",
                          [&] { (void)service_->KeepAlive(keepAliveReq, keepAliveRsp); });
    coordinator::GetCoordinatorIdReqPb coordinatorIdReq;
    coordinator::GetCoordinatorIdRspPb coordinatorIdRsp;
    expectSingleIncrement("coordinator_rpc_get_coordinator_id_request_total",
                          [&] { (void)service_->GetCoordinatorId(coordinatorIdReq, coordinatorIdRsp); });
    coordinator::ReportTopologyRecoveryCandidateReqPb recoveryReq;
    coordinator::ReportTopologyRecoveryCandidateRspPb recoveryRsp;
    expectSingleIncrement("coordinator_rpc_report_topology_recovery_candidate_request_total",
                          [&] { (void)service_->ReportTopologyRecoveryCandidate(recoveryReq, recoveryRsp); });
    coordinator::GetClusterRawSnapshotReqPb snapshotReq;
    coordinator::GetClusterRawSnapshotRspPb snapshotRsp;
    expectSingleIncrement("coordinator_rpc_get_cluster_raw_snapshot_request_total",
                          [&] { (void)service_->GetClusterRawSnapshot(snapshotReq, snapshotRsp); });
    coordinator::RaftBootstrapObservationPb bootstrapReq;
    coordinator::RaftBootstrapObservationPb bootstrapRsp;
    expectSingleIncrement("coordinator_rpc_exchange_bootstrap_observation_request_total",
                          [&] { (void)service_->ExchangeBootstrapObservation(bootstrapReq, bootstrapRsp); });
    coordinator::EnsureLeaderMembershipReqPb ensureReq;
    coordinator::EnsureLeaderMembershipRspPb ensureRsp;
    expectSingleIncrement("coordinator_rpc_ensure_leader_membership_request_total",
                          [&] { (void)service_->EnsureLeaderMembership(ensureReq, ensureRsp); });
    coordinator::ReportWorkerLivenessReqPb livenessReq;
    coordinator::ReportWorkerLivenessRspPb livenessRsp;
    expectSingleIncrement("coordinator_rpc_report_worker_liveness_request_total",
                          [&] { (void)service_->ReportWorkerLiveness(livenessReq, livenessRsp); });
}

void EnableElection(coordinator::CoordinatorServiceImpl &service)
{
    service.coordinatorDiscovery_ = std::make_shared<TestCoordinatorDiscovery>();
    service.expectedMemberCount_ = ELECTION_MEMBER_COUNT;
    service.leadershipSnapshotProvider_ = [](coordinator::CoordinatorLeadershipSnapshot &snapshot) {
        snapshot = { true, LEADER_ADDRESS, LEADER_TERM };
        return Status::OK();
    };
    service.OnLeaderStart(LEADER_TERM);
}

class CoordinatorServiceImplTest : public CommonTest {};



TEST_F(CoordinatorServiceImplTest, MembershipWatchContentionPreservesBthreadProgress)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    service->recoveryStateProvider_ = [](const std::string &) { return coordinator::TopologyRecoveryState::READY; };
    coordinator::PutReqPb put;
    put.set_key("/datasystem/contention/cluster/" + std::string(MEMBER_ADDRESS));
    put.set_expected_version(-1);
    put.set_value(EncodeMembershipValue());
    coordinator::PutRspPb putResponse;
    DS_ASSERT_OK(service->Put(put, putResponse));
    coordinator::WatchRangeReqPb watch;
    watch.set_key("/datasystem/contention/topology/");
    watch.set_watcher_addr(MEMBER_ADDRESS);
    watch.set_registration_id("contention-watch");
    std::unique_lock membershipLock(service->membershipWatchMutex_);
    ExpectBthreadProgressWhileBlocked(
        [&](size_t index) {
            constexpr size_t operationKinds = 2;
            if (index % operationKinds == 0) {
                coordinator::WatchRangeRspPb response;
                EXPECT_TRUE(service->WatchRange(watch, response).IsOk());
            } else {
                coordinator::PutRspPb response;
                EXPECT_TRUE(service->Put(put, response).IsOk());
            }
        },
        [&] { membershipLock.unlock(); });
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, LifecycleContentionPreservesBthreadProgress)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    std::unique_lock lifecycleLock(service->lifecycleMutex_);
    ExpectBthreadProgressWhileBlocked(
        [&](size_t) {
            coordinator::GetCoordinatorIdReqPb request;
            coordinator::GetCoordinatorIdRspPb response;
            EXPECT_TRUE(service->GetCoordinatorId(request, response).IsOk());
        },
        [&] { lifecycleLock.unlock(); });
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, RpcDependenciesContentionPreservesBthreadProgress)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    const auto check = [&](const char *name, auto &mutex, const auto &operation) {
        SCOPED_TRACE(name);
        std::unique_lock lock(mutex);
        ExpectBthreadProgressWhileBlocked(operation, [&] { lock.unlock(); });
    };
    check("recovery", service->topologyRecoveryManager_->mutex_, [&](size_t) {
        (void)service->topologyRecoveryManager_->GetState("contention");
    });
    check("control host", service->topologyControlHost_->mutex_, [&](size_t) {
        (void)service->topologyControlHost_->IsStopped();
    });
    check("memory store", service->store_->memKvStore_->mutex_, [&](size_t) {
        std::vector<KeyValueEntry> entries;
        int64_t revision = 0;
        EXPECT_TRUE(service->store_->Range("key", "", entries, revision).IsOk());
    });
    check("watch registry", service->watchRegistry_->mutex_, [&](size_t) {
        std::vector<std::shared_ptr<WatcherEntry>> matched;
        service->watchRegistry_->MatchWatchers("key", matched);
    });
    check("TTL", service->ttlManager_->mutex_, [&](size_t) {
        EXPECT_TRUE(service->ttlManager_->Schedule("key", 1000, 1, 1).IsOk());
    });
    check("watch pending queue", service->watchDispatcher_->pendingMutex_, [&](size_t) {
        service->watchDispatcher_->Enqueue(std::make_shared<WatchEvent>());
    });
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, LeaderStopContentionPreservesBthreadProgress)
{
    auto service = MakeService();
    SetRunning(*service);
    service->OnLeaderStart(LEADER_TERM);
    std::shared_lock leaderLock(service->leaderOperationMutex_);
    ExpectBthreadProgressWhileBlocked(
        [&](size_t) { service->OnLeaderStop(Status(K_RUNTIME_ERROR, "test leadership lost")); },
        [&] { leaderLock.unlock(); });
    EXPECT_EQ(service->leaderTerm_.load(), 0);
}

TEST_F(CoordinatorServiceImplTest, WatchChannelContentionPreservesBthreadProgress)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    constexpr int64_t watchId = 1;
    service->watchDispatcher_->AddChannel(watchId, MEMBER_ADDRESS);
    auto channel = service->watchDispatcher_->channels_.at(watchId);
    std::unique_lock channelLock(channel->mutex);
    ExpectBthreadProgressWhileBlocked(
        [&](size_t) { service->watchDispatcher_->SetSnapshotRevision(watchId, 1); },
        [&] { channelLock.unlock(); });
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, WatchMapContentionPreservesBthreadProgress)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    std::unique_lock channelsLock(service->watchDispatcher_->channelsMutex_);
    ExpectBthreadProgressWhileBlocked(
        [&](size_t index) { service->watchDispatcher_->AddChannel(static_cast<int64_t>(index) + 1, MEMBER_ADDRESS); },
        [&] { channelsLock.unlock(); });
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, CoordinatorServiceConstructAndInitRemainCreated)
{
    auto service = MakeService();
    EXPECT_EQ(service->lifecycleState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::LifecycleState::CREATED);

    DS_ASSERT_OK(service->Init());

    EXPECT_TRUE(service->initialized_);
    EXPECT_FALSE(service->rpcStarted_);
    EXPECT_EQ(service->lifecycleState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::LifecycleState::CREATED);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, CoordinatorLeaderCallbacksDoNotChangeRunningLifecycle)
{
    auto service = MakeService();
    DS_ASSERT_OK(service->Init());
    service->lifecycleState_.store(coordinator::CoordinatorServiceImpl::LifecycleState::RUNNING,
                                   std::memory_order_release);

    service->OnLeaderStart(LEADER_TERM);

    EXPECT_EQ(service->leaderTerm_.load(std::memory_order_acquire), LEADER_TERM);
    EXPECT_EQ(service->lifecycleState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::LifecycleState::RUNNING);

    service->OnLeaderStop(Status(K_RUNTIME_ERROR, "leadership lost"));

    EXPECT_EQ(service->leaderTerm_.load(std::memory_order_acquire), 0U);
    EXPECT_EQ(service->lifecycleState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::LifecycleState::RUNNING);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, PrepareResponseHeaderRejectsNullAndLifecycleErrors)
{
    auto service = MakeService();
    coordinator::ResponseHeader header;

    EXPECT_EQ(service->PrepareResponseHeader(nullptr).GetCode(), K_INVALID);
    EXPECT_EQ(service->PrepareResponseHeader(&header).GetCode(), K_NOT_READY);

    service->lifecycleState_.store(coordinator::CoordinatorServiceImpl::LifecycleState::STOPPED,
                                   std::memory_order_release);
    EXPECT_EQ(service->PrepareResponseHeader(&header).GetCode(), K_SHUTTING_DOWN);
}

TEST_F(CoordinatorServiceImplTest, PrepareResponseHeaderNoElectionIsServingAtTermZero)
{
    auto service = MakeService();
    SetRunning(*service);
    coordinator::ResponseHeader header;
    header.set_state(coordinator::ResponseHeader::NOT_LEADER);
    header.set_leader_address("stale-leader");
    header.set_coordinator_id("stale-id");
    header.set_leader_term(LEADER_TERM);

    DS_ASSERT_OK(service->PrepareResponseHeader(&header));

    EXPECT_EQ(header.state(), coordinator::ResponseHeader::SERVING);
    EXPECT_TRUE(header.leader_address().empty());
    EXPECT_EQ(header.coordinator_id(), COORDINATOR_ID);
    EXPECT_EQ(header.leader_term(), 0U);
}

TEST_F(CoordinatorServiceImplTest, PrepareClusterResponseHeaderFollowersSkipRecoveryState)
{
    auto service = MakeService();
    SetRunning(*service);
    int leadershipCalls = 0;
    int recoveryStateCalls = 0;
    coordinator::CoordinatorLeadershipSnapshot observed;
    service->leadershipSnapshotProvider_ = [&](coordinator::CoordinatorLeadershipSnapshot &snapshot) {
        ++leadershipCalls;
        snapshot = observed;
        return Status::OK();
    };
    service->recoveryStateProvider_ = [&](const std::string &) {
        ++recoveryStateCalls;
        return coordinator::TopologyRecoveryState::READY;
    };

    for (const auto &leaderAddress : std::array<std::string, 2>{ LEADER_ADDRESS, "" }) {
        observed = { false, leaderAddress, LEADER_TERM };
        coordinator::ResponseHeader header;
        DS_ASSERT_OK(service->PrepareResponseHeader(CLUSTER_NAME, &header));
        EXPECT_EQ(header.state(), coordinator::ResponseHeader::NOT_LEADER);
        EXPECT_EQ(header.leader_address(), leaderAddress);
        EXPECT_EQ(header.coordinator_id(), COORDINATOR_ID);
        EXPECT_EQ(header.leader_term(), LEADER_TERM);
    }
    EXPECT_EQ(leadershipCalls, 2);
    EXPECT_EQ(recoveryStateCalls, 0);
}

TEST_F(CoordinatorServiceImplTest, PrepareClusterResponseHeaderMapsOneRecoveryStateRead)
{
    auto service = MakeService();
    SetRunning(*service);
    EnableElection(*service);
    int leadershipCalls = 0;
    int recoveryStateCalls = 0;
    auto recoveryState = coordinator::TopologyRecoveryState::RECOVERING;
    service->leadershipSnapshotProvider_ = [&](coordinator::CoordinatorLeadershipSnapshot &snapshot) {
        ++leadershipCalls;
        snapshot = { true, LEADER_ADDRESS, LEADER_TERM };
        return Status::OK();
    };
    service->recoveryStateProvider_ = [&](const std::string &clusterName) {
        ++recoveryStateCalls;
        EXPECT_EQ(clusterName, CLUSTER_NAME);
        return recoveryState;
    };
    const std::array<std::pair<coordinator::TopologyRecoveryState, coordinator::ResponseHeader::StatePb>, 4> cases{ {
        { coordinator::TopologyRecoveryState::READY, coordinator::ResponseHeader::SERVING },
        { coordinator::TopologyRecoveryState::RECOVERING, coordinator::ResponseHeader::RECOVERING },
        { coordinator::TopologyRecoveryState::INSTALLING, coordinator::ResponseHeader::RECOVERING },
        { coordinator::TopologyRecoveryState::BLOCKED, coordinator::ResponseHeader::RECOVERING },
    } };

    for (const auto &[state, expectedHeaderState] : cases) {
        leadershipCalls = 0;
        recoveryStateCalls = 0;
        recoveryState = state;
        coordinator::ResponseHeader header;
        header.set_leader_address("stale-leader");
        DS_ASSERT_OK(service->PrepareResponseHeader(CLUSTER_NAME, &header));
        EXPECT_EQ(header.state(), expectedHeaderState);
        EXPECT_TRUE(header.leader_address().empty());
        EXPECT_EQ(header.coordinator_id(), COORDINATOR_ID);
        EXPECT_EQ(header.leader_term(), LEADER_TERM);
        EXPECT_EQ(leadershipCalls, 1);
        EXPECT_EQ(recoveryStateCalls, 1);
    }
}

TEST_F(CoordinatorServiceImplTest, RangeUsesPreparedRecoveryAdmission)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    EnableElection(*service);
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &clusterName) {
        ++recoveryStateCalls;
        EXPECT_EQ(clusterName, "single-read");
        return coordinator::TopologyRecoveryState::READY;
    };
    const std::string key = "/datasystem/single-read/topology/";
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(service->store_->Put(key, "topology", 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, version, revision));

    coordinator::RangeReqPb request;
    request.set_key(key);
    coordinator::RangeRspPb response;
    DS_ASSERT_OK(service->Range(request, response));

    EXPECT_EQ(recoveryStateCalls, 1);
    ASSERT_EQ(response.kvs_size(), 1);
    EXPECT_EQ(response.kvs(0).value(), "topology");
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, NoElectionRecoveringClusterUsesTypedAdmission)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &clusterName) {
        ++recoveryStateCalls;
        EXPECT_EQ(clusterName, "standalone-recovery");
        return coordinator::TopologyRecoveryState::RECOVERING;
    };
    auto store = std::move(service->store_);

    coordinator::RangeReqPb request;
    request.set_key("/datasystem/standalone-recovery/topology/");
    coordinator::RangeRspPb response;
    DS_ASSERT_OK(service->Range(request, response));

    ExpectHeaderState(response.header(), coordinator::ResponseHeader::RECOVERING, service->coordinatorId_, 0);
    EXPECT_EQ(recoveryStateCalls, 1);
    service->store_ = std::move(store);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, NoElectionReadyClusterServesOrdinaryRpc)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &clusterName) {
        ++recoveryStateCalls;
        EXPECT_EQ(clusterName, "standalone-ready");
        return coordinator::TopologyRecoveryState::READY;
    };
    const std::string key = "/datasystem/standalone-ready/topology/";
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(service->store_->Put(key, "topology", 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, version, revision));

    coordinator::RangeReqPb request;
    request.set_key(key);
    coordinator::RangeRspPb response;
    DS_ASSERT_OK(service->Range(request, response));

    ExpectHeaderState(response.header(), coordinator::ResponseHeader::SERVING, service->coordinatorId_, 0);
    EXPECT_EQ(recoveryStateCalls, 1);
    ASSERT_EQ(response.kvs_size(), 1);
    EXPECT_EQ(response.kvs(0).value(), "topology");
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, NoElectionRecoveryControlRpcExecutesWithRecoveringHeader)
{
    const std::string clusterName = "standalone-recovery-control";
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &observedClusterName) {
        ++recoveryStateCalls;
        EXPECT_EQ(observedClusterName, clusterName);
        return coordinator::TopologyRecoveryState::RECOVERING;
    };

    coordinator::EnsureLeaderMembershipReqPb request;
    request.set_cluster_name(clusterName);
    request.set_reporter_address(MEMBER_ADDRESS);
    request.set_coordinator_id(service->coordinatorId_);
    request.set_leader_term(0);
    request.set_membership_value(EncodeMembershipValue());
    request.set_ttl_ms(MEMBERSHIP_TTL_MS);
    coordinator::EnsureLeaderMembershipRspPb response;
    DS_ASSERT_OK(service->EnsureLeaderMembership(request, response));

    ExpectHeaderState(response.header(), coordinator::ResponseHeader::RECOVERING, service->coordinatorId_, 0);
    EXPECT_EQ(response.result(), coordinator::EnsureLeaderMembershipRspPb::ACCEPTED);
    EXPECT_GT(response.membership_mod_revision(), 0);
    EXPECT_EQ(recoveryStateCalls, 1);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, RangeResponsesOwnBinaryValuesWithoutChangingStore)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    service->recoveryStateProvider_ = [](const std::string &) {
        return coordinator::TopologyRecoveryState::READY;
    };
    const std::string prefix = "/datasystem/range-owned/cluster/";
    std::vector<KeyValueEntry> entries{
        { prefix + "127.0.0.1:31501", std::string(4096, '\0') },
        { prefix + "127.0.0.1:31502", std::string(4096, '\xff') },
        { prefix + "127.0.0.1:31503", "" },
    };
    for (auto &entry : entries) {
        DS_ASSERT_OK(service->store_->Put(entry.key, entry.value, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION,
                                          entry.version, entry.modRevision));
    }
    coordinator::RangeReqPb request;
    request.set_key(prefix);
    request.set_range_end("/datasystem/range-owned/cluster0");
    coordinator::RangeRspPb response;
    google::protobuf::Arena arena;
    auto *arenaResponse = google::protobuf::Arena::CreateMessage<coordinator::RangeRspPb>(&arena);
    for (auto *result : { &response, arenaResponse }) {
        DS_ASSERT_OK(service->Range(request, *result));
        ASSERT_EQ(result->kvs_size(), static_cast<int>(entries.size()));
        EXPECT_EQ(result->revision(), entries.back().modRevision);
        EXPECT_FALSE(result->unchanged());
    }
    for (const auto &entry : entries) {
        coordinator::RangeReqPb exact;
        exact.set_key(entry.key);
        coordinator::RangeRspPb exactResponse;
        DS_ASSERT_OK(service->Range(exact, exactResponse));
        ASSERT_EQ(exactResponse.kvs_size(), 1);
        EXPECT_EQ(exactResponse.kvs(0).value(), entry.value);
        exact.set_known_mod_revision(entry.modRevision);
        coordinator::RangeRspPb unchangedResponse;
        DS_ASSERT_OK(service->Range(exact, unchangedResponse));
        EXPECT_TRUE(unchangedResponse.unchanged());
        EXPECT_EQ(unchangedResponse.kvs_size(), 0);
    }
    int64_t deleted = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(service->store_->DeleteRange(prefix, request.range_end(), deleted, revision));
    EXPECT_EQ(deleted, static_cast<int64_t>(entries.size()));
    coordinator::RangeRspPb emptyResponse;
    DS_ASSERT_OK(service->Range(request, emptyResponse));
    EXPECT_EQ(emptyResponse.kvs_size(), 0);
    DS_ASSERT_OK(service->Shutdown());
    service.reset();
    for (auto *result : { &response, arenaResponse }) {
        coordinator::RangeRspPb decoded;
        ASSERT_TRUE(decoded.ParseFromString(result->SerializeAsString()));
        ASSERT_EQ(decoded.kvs_size(), static_cast<int>(entries.size()));
        for (size_t i = 0; i < entries.size(); ++i) {
            const auto &actual = decoded.kvs(static_cast<int>(i));
            EXPECT_EQ(actual.key(), entries[i].key);
            EXPECT_EQ(actual.value(), entries[i].value);
            EXPECT_EQ(actual.version(), entries[i].version);
            EXPECT_EQ(actual.mod_revision(), entries[i].modRevision);
        }
    }
}

TEST_F(CoordinatorServiceImplTest, PrepareClusterResponseHeaderRejectsMissingRecoveryManager)
{
    auto service = MakeService();
    SetRunning(*service);
    EnableElection(*service);
    coordinator::ResponseHeader header;

    EXPECT_EQ(service->PrepareResponseHeader(CLUSTER_NAME, &header).GetCode(), K_NOT_READY);
}

TEST_F(CoordinatorServiceImplTest, LeaderHeadersWaitForMatchingCallbackTermBeforeAnySideEffect)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    service->coordinatorDiscovery_ = std::make_shared<TestCoordinatorDiscovery>();
    service->expectedMemberCount_ = ELECTION_MEMBER_COUNT;
    int leadershipCalls = 0;
    service->leadershipSnapshotProvider_ = [&](coordinator::CoordinatorLeadershipSnapshot &snapshot) {
        ++leadershipCalls;
        snapshot = { true, LEADER_ADDRESS, LEADER_TERM };
        return Status::OK();
    };
    service->recoveryStateProvider_ = [](const std::string &) { return coordinator::TopologyRecoveryState::READY; };
    const auto revisionBefore = service->memStore_->CurrentRevision();

    coordinator::PutReqPb put;
    put.set_key("/datasystem/callback-gap/notify/" + std::string(MEMBER_ADDRESS));
    put.set_value("notify");
    coordinator::PutRspPb putBeforeCallback;
    EXPECT_EQ(service->Put(put, putBeforeCallback).GetCode(), K_NOT_READY);
    EXPECT_EQ(putBeforeCallback.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

    coordinator::EnsureLeaderMembershipReqPb ensure;
    ensure.set_cluster_name("callback-gap");
    ensure.set_reporter_address(MEMBER_ADDRESS);
    ensure.set_coordinator_id(service->coordinatorId_);
    ensure.set_leader_term(LEADER_TERM);
    ensure.set_membership_value(EncodeMembershipValue());
    ensure.set_ttl_ms(MEMBERSHIP_TTL_MS);
    coordinator::EnsureLeaderMembershipRspPb ensureBeforeCallback;
    EXPECT_EQ(service->EnsureLeaderMembership(ensure, ensureBeforeCallback).GetCode(), K_NOT_READY);
    EXPECT_EQ(ensureBeforeCallback.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);
    EXPECT_EQ(service->memStore_->CurrentRevision(), revisionBefore);

    service->OnLeaderStart(LEADER_TERM);

    coordinator::PutRspPb putAfterCallback;
    DS_ASSERT_OK(service->Put(put, putAfterCallback));
    EXPECT_EQ(putAfterCallback.header().state(), coordinator::ResponseHeader::SERVING);
    EXPECT_EQ(putAfterCallback.header().leader_term(), LEADER_TERM);
    coordinator::EnsureLeaderMembershipRspPb ensureAfterCallback;
    DS_ASSERT_OK(service->EnsureLeaderMembership(ensure, ensureAfterCallback));
    EXPECT_EQ(ensureAfterCallback.header().state(), coordinator::ResponseHeader::SERVING);
    EXPECT_EQ(ensureAfterCallback.header().leader_term(), LEADER_TERM);
    EXPECT_EQ(ensureAfterCallback.result(), coordinator::EnsureLeaderMembershipRspPb::ACCEPTED);
    EXPECT_GT(service->memStore_->CurrentRevision(), revisionBefore);
    EXPECT_EQ(leadershipCalls, 4);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, TypedAdmissionAllowsOnlyRecoveryControlDuringRecovery)
{
    auto service = MakeService();
    coordinator::ResponseHeader header;
    const auto verify = [&](coordinator::ResponseHeader::StatePb state, bool allowBusiness, bool allowControl) {
        header.set_state(state);
        EXPECT_EQ(service->AllowContinue<coordinator::PutReqPb>(header), allowBusiness);
        EXPECT_EQ(service->AllowContinue<coordinator::KeepAliveReqPb>(header), allowControl);
        EXPECT_EQ(service->AllowContinue<coordinator::EnsureLeaderMembershipReqPb>(header), allowControl);
        EXPECT_EQ(service->AllowContinue<coordinator::ReportTopologyRecoveryCandidateReqPb>(header), allowControl);
    };

    verify(coordinator::ResponseHeader::STATE_UNSPECIFIED, false, false);
    verify(coordinator::ResponseHeader::NOT_LEADER, false, false);
    verify(coordinator::ResponseHeader::RECOVERING, false, true);
    verify(coordinator::ResponseHeader::SERVING, true, true);
}

TEST_F(CoordinatorServiceImplTest, RecoveringOrdinaryClusterRpcsReturnHeaderWithoutSideEffects)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    EnableElection(*service);
    const std::string coordinatorId = service->coordinatorId_;
    auto recoveryState = coordinator::TopologyRecoveryState::READY;
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &clusterName) {
        EXPECT_EQ(clusterName, "ordinary");
        ++recoveryStateCalls;
        return recoveryState;
    };

    const std::string membershipKey = "/datasystem/ordinary/cluster/" + std::string(MEMBER_ADDRESS);
    coordinator::PutReqPb setup;
    setup.set_key(membershipKey);
    setup.set_value(EncodeMembershipValue());
    setup.set_ttl(MEMBERSHIP_TTL_MS);
    coordinator::PutRspPb setupResponse;
    DS_ASSERT_OK(service->Put(setup, setupResponse));

    recoveryState = coordinator::TopologyRecoveryState::RECOVERING;
    recoveryStateCalls = 0;
    const auto revisionBefore = service->memStore_->CurrentRevision();
    const auto nextWatchIdBefore = service->watchRegistry_->nextWatchId_.load(std::memory_order_acquire);
    auto store = service->store_;
    service->store_.reset();
    auto controlHost = std::move(service->topologyControlHost_);

    coordinator::PutReqPb put;
    put.set_key(membershipKey);
    put.set_value("blocked");
    put.set_expected_coordinator_id(MISMATCHED_COORDINATOR_ID);
    coordinator::PutRspPb putResponse;
    DS_ASSERT_OK(service->Put(put, putResponse));
    ExpectHeaderState(putResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);

    coordinator::RangeReqPb range;
    range.set_key(membershipKey);
    coordinator::RangeRspPb rangeResponse;
    DS_ASSERT_OK(service->Range(range, rangeResponse));
    ExpectHeaderState(rangeResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);
    EXPECT_TRUE(rangeResponse.kvs().empty());

    coordinator::DeleteRangeReqPb deleteRange;
    deleteRange.set_key(membershipKey);
    deleteRange.set_expected_coordinator_id(MISMATCHED_COORDINATOR_ID);
    coordinator::DeleteRangeRspPb deleteResponse;
    DS_ASSERT_OK(service->DeleteRange(deleteRange, deleteResponse));
    ExpectHeaderState(deleteResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);
    EXPECT_EQ(deleteResponse.deleted(), 0);

    coordinator::WatchRangeReqPb watch;
    watch.set_key("/datasystem/ordinary/topology/");
    watch.set_watcher_addr(MEMBER_ADDRESS);
    watch.set_registration_id("recovering-watch");
    coordinator::WatchRangeRspPb watchResponse;
    DS_ASSERT_OK(service->WatchRange(watch, watchResponse));
    ExpectHeaderState(watchResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);
    EXPECT_EQ(watchResponse.watch_id(), 0);

    coordinator::ReportWorkerLivenessReqPb liveness;
    liveness.set_cluster_name("ordinary");
    liveness.set_witness_address(MEMBER_ADDRESS);
    liveness.set_target_address("127.0.0.1:31502");
    liveness.set_target_member_id("target-member");
    liveness.set_probe_round(1);
    liveness.set_result(coordinator::WORKER_REACHABLE);
    liveness.set_coordinator_id(coordinatorId);
    coordinator::ReportWorkerLivenessRspPb livenessResponse;
    DS_ASSERT_OK(service->ReportWorkerLiveness(liveness, livenessResponse));
    ExpectHeaderState(livenessResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);

    coordinator::GetClusterRawSnapshotReqPb snapshot;
    snapshot.set_cluster_name("ordinary");
    coordinator::GetClusterRawSnapshotRspPb snapshotResponse;
    DS_ASSERT_OK(service->GetClusterRawSnapshot(snapshot, snapshotResponse));
    ExpectHeaderState(snapshotResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);
    EXPECT_TRUE(snapshotResponse.topology_kvs().empty());
    EXPECT_TRUE(snapshotResponse.membership_kvs().empty());

    EXPECT_EQ(recoveryStateCalls, 6);
    EXPECT_EQ(service->memStore_->CurrentRevision(), revisionBefore);
    EXPECT_EQ(service->watchRegistry_->nextWatchId_.load(std::memory_order_acquire), nextWatchIdBefore);

    service->store_ = std::move(store);
    service->topologyControlHost_ = std::move(controlHost);
    std::vector<KeyValueEntry> members;
    int64_t revision = 0;
    DS_ASSERT_OK(service->store_->Range(membershipKey, "", members, revision));
    ASSERT_EQ(members.size(), 1U);
    EXPECT_EQ(members.front().value, setup.value());
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, RecoveryControlRpcsExecuteWithRecoveringHeader)
{
    const std::string clusterName = "recovery-control";
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    EnableElection(*service);
    const std::string coordinatorId = service->coordinatorId_;
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &observedClusterName) {
        EXPECT_EQ(observedClusterName, clusterName);
        ++recoveryStateCalls;
        return coordinator::TopologyRecoveryState::RECOVERING;
    };

    coordinator::EnsureLeaderMembershipReqPb ensure;
    ensure.set_cluster_name(clusterName);
    ensure.set_reporter_address(MEMBER_ADDRESS);
    ensure.set_coordinator_id(coordinatorId);
    ensure.set_leader_term(LEADER_TERM);
    ensure.set_membership_value(EncodeMembershipValue());
    ensure.set_ttl_ms(MEMBERSHIP_TTL_MS);
    coordinator::EnsureLeaderMembershipRspPb ensureResponse;
    DS_ASSERT_OK(service->EnsureLeaderMembership(ensure, ensureResponse));
    ExpectHeaderState(ensureResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);
    EXPECT_EQ(ensureResponse.result(), coordinator::EnsureLeaderMembershipRspPb::ACCEPTED);
    EXPECT_GT(ensureResponse.membership_mod_revision(), 0);

    coordinator::KeepAliveReqPb keepAlive;
    keepAlive.set_key("/datasystem/" + clusterName + "/cluster/" + MEMBER_ADDRESS);
    keepAlive.set_expected_coordinator_id(coordinatorId);
    keepAlive.set_expected_mod_revision(ensureResponse.membership_mod_revision());
    coordinator::KeepAliveRspPb keepAliveResponse;
    DS_ASSERT_OK(service->KeepAlive(keepAlive, keepAliveResponse));
    ExpectHeaderState(keepAliveResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);
    EXPECT_EQ(keepAliveResponse.ttl(), MEMBERSHIP_TTL_MS);
    EXPECT_GT(keepAliveResponse.remaining_ttl(), 0);

    coordinator::ReportTopologyRecoveryCandidateReqPb report;
    report.set_cluster_name(clusterName);
    report.set_coordinator_id(coordinatorId);
    report.set_leader_term(LEADER_TERM);
    report.set_reporter_address(MEMBER_ADDRESS);
    report.set_result(coordinator::TOPOLOGY_RECOVERY_NO_SNAPSHOT);
    coordinator::ReportTopologyRecoveryCandidateRspPb reportResponse;
    DS_ASSERT_OK(service->ReportTopologyRecoveryCandidate(report, reportResponse));
    ExpectHeaderState(reportResponse.header(), coordinator::ResponseHeader::RECOVERING, coordinatorId, LEADER_TERM);
    EXPECT_EQ(reportResponse.result(), coordinator::ReportTopologyRecoveryCandidateRspPb::ACCEPTED);
    EXPECT_EQ(reportResponse.recovery_state(), coordinator::COORDINATOR_RECOVERING);
    EXPECT_EQ(recoveryStateCalls, 3);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, InvalidEnsureMembershipRetainsAdmittedHeaderWithoutSideEffects)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    EnableElection(*service);
    auto recoveryState = coordinator::TopologyRecoveryState::RECOVERING;
    service->recoveryStateProvider_ = [&](const std::string &clusterName) {
        EXPECT_EQ(clusterName, CLUSTER_NAME);
        return recoveryState;
    };
    const auto revisionBefore = service->memStore_->CurrentRevision();
    auto store = service->store_;
    service->store_.reset();
    auto controlHost = std::move(service->topologyControlHost_);

    coordinator::EnsureLeaderMembershipReqPb ensure;
    ensure.set_cluster_name(CLUSTER_NAME);
    ensure.set_reporter_address("invalid-address");
    ensure.set_coordinator_id(service->coordinatorId_);
    ensure.set_leader_term(LEADER_TERM);
    ensure.set_membership_value(EncodeMembershipValue());
    ensure.set_ttl_ms(MEMBERSHIP_TTL_MS);
    for (const auto expectedState : { coordinator::ResponseHeader::RECOVERING,
                                      coordinator::ResponseHeader::SERVING }) {
        recoveryState = expectedState == coordinator::ResponseHeader::RECOVERING
                            ? coordinator::TopologyRecoveryState::RECOVERING
                            : coordinator::TopologyRecoveryState::READY;
        coordinator::EnsureLeaderMembershipRspPb response;
        DS_ASSERT_OK(service->EnsureLeaderMembership(ensure, response));
        ExpectHeaderState(response.header(), expectedState, service->coordinatorId_, LEADER_TERM);
        EXPECT_EQ(response.result(), coordinator::EnsureLeaderMembershipRspPb::INVALID_MEMBERSHIP);
        EXPECT_EQ(service->memStore_->CurrentRevision(), revisionBefore);
    }

    service->store_ = std::move(store);
    service->topologyControlHost_ = std::move(controlHost);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, CollectionRootsCannotBePutOrExactlyDeleted)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    service->recoveryStateProvider_ = [](const std::string &) { return coordinator::TopologyRecoveryState::READY; };
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    DS_ASSERT_OK(cluster::TopologyKeyHelper::Create("mutation-root", keys));
    const std::array<std::string, 7> collectionRoots{
        keys->MigrateTaskTable() + "/", keys->DeleteTaskTable() + "/", keys->NotifyTable() + "/",
        keys->ProbeTable() + "/",       keys->MembershipTable() + "/", keys->UbHealthTable() + "/",
        keys->ScaleInMetadataDoneTable() + "/"
    };
    const auto revisionBefore = service->memStore_->CurrentRevision();
    auto store = service->store_;
    service->store_.reset();
    auto controlHost = std::move(service->topologyControlHost_);

    for (const auto &root : collectionRoots) {
        SCOPED_TRACE(root);
        coordinator::PutReqPb put;
        put.set_key(root);
        put.set_value("invalid-root-mutation");
        coordinator::PutRspPb putResponse;
        EXPECT_EQ(service->Put(put, putResponse).GetCode(), K_INVALID);
        EXPECT_EQ(putResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

        coordinator::DeleteRangeReqPb deleteRange;
        deleteRange.set_key(root);
        coordinator::DeleteRangeRspPb deleteResponse;
        EXPECT_EQ(service->DeleteRange(deleteRange, deleteResponse).GetCode(), K_INVALID);
        EXPECT_EQ(deleteResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);
    }
    EXPECT_EQ(service->memStore_->CurrentRevision(), revisionBefore);

    service->store_ = std::move(store);
    service->topologyControlHost_ = std::move(controlHost);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, UbHealthSupportsTtlPutExactAndTableRangeAsOrdinaryClusterData)
{
    constexpr char clusterName[] = "ub-ready";
    constexpr char peerAddress[] = "127.0.0.1:31502";
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    EnableElection(*service);
    auto recoveryState = coordinator::TopologyRecoveryState::READY;
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &observedClusterName) {
        EXPECT_EQ(observedClusterName, clusterName);
        ++recoveryStateCalls;
        return recoveryState;
    };
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    DS_ASSERT_OK(cluster::TopologyKeyHelper::Create(clusterName, keys));
    const std::string tablePrefix = keys->UbHealthTable() + "/";
    const std::string localKey = tablePrefix + MEMBER_ADDRESS;
    const std::string peerKey = tablePrefix + peerAddress;

    coordinator::PutReqPb put;
    put.set_key(localKey);
    put.set_value("local-health");
    put.set_ttl(MEMBERSHIP_TTL_MS);
    coordinator::PutRspPb putResponse;
    DS_ASSERT_OK(service->Put(put, putResponse));
    EXPECT_EQ(putResponse.header().state(), coordinator::ResponseHeader::SERVING);

    put.set_key(peerKey);
    put.set_value("peer-health");
    coordinator::PutRspPb peerPutResponse;
    DS_ASSERT_OK(service->Put(put, peerPutResponse));

    coordinator::RangeReqPb exactRange;
    exactRange.set_key(localKey);
    coordinator::RangeRspPb exactResponse;
    DS_ASSERT_OK(service->Range(exactRange, exactResponse));
    ASSERT_EQ(exactResponse.kvs_size(), 1);
    EXPECT_EQ(exactResponse.kvs(0).value(), "local-health");

    coordinator::RangeReqPb tableRange;
    tableRange.set_key(tablePrefix);
    tableRange.set_range_end(keys->UbHealthTable() + "0");
    coordinator::RangeRspPb tableResponse;
    DS_ASSERT_OK(service->Range(tableRange, tableResponse));
    EXPECT_EQ(tableResponse.kvs_size(), 2);

    coordinator::KeepAliveReqPb keepAlive;
    keepAlive.set_key(localKey);
    coordinator::KeepAliveRspPb keepAliveResponse;
    EXPECT_EQ(service->KeepAlive(keepAlive, keepAliveResponse).GetCode(), K_INVALID);

    recoveryState = coordinator::TopologyRecoveryState::RECOVERING;
    put.set_key(tablePrefix + "127.0.0.1:31503");
    put.set_value("blocked-health");
    coordinator::PutRspPb recoveringResponse;
    DS_ASSERT_OK(service->Put(put, recoveringResponse));
    EXPECT_EQ(recoveringResponse.header().state(), coordinator::ResponseHeader::RECOVERING);
    std::vector<KeyValueEntry> blockedEntries;
    int64_t revision = 0;
    DS_ASSERT_OK(service->store_->Range(put.key(), "", blockedEntries, revision));
    EXPECT_TRUE(blockedEntries.empty());
    EXPECT_EQ(recoveryStateCalls, 5);

    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, UbHealthRejectsInvalidKeysRootMutationAndCrossTableRange)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    service->recoveryStateProvider_ = [](const std::string &) { return coordinator::TopologyRecoveryState::READY; };
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    DS_ASSERT_OK(cluster::TopologyKeyHelper::Create("ub-boundary", keys));
    const std::string tablePrefix = keys->UbHealthTable() + "/";

    for (const auto &key : { tablePrefix + "not-an-address",
                             std::string("/datasystem_ub_health/datasystem/cluster/") + MEMBER_ADDRESS }) {
        coordinator::PutReqPb put;
        put.set_key(key);
        put.set_value("invalid");
        coordinator::PutRspPb response;
        EXPECT_EQ(service->Put(put, response).GetCode(), K_INVALID);
    }

    coordinator::PutReqPb rootPut;
    rootPut.set_key(tablePrefix);
    rootPut.set_value("invalid-root");
    coordinator::PutRspPb rootResponse;
    EXPECT_EQ(service->Put(rootPut, rootResponse).GetCode(), K_INVALID);

    coordinator::RangeReqPb crossTableRange;
    crossTableRange.set_key(tablePrefix);
    crossTableRange.set_range_end(keys->MembershipTable() + "0");
    coordinator::RangeRspPb crossTableResponse;
    EXPECT_EQ(service->Range(crossTableRange, crossTableResponse).GetCode(), K_INVALID);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, InvalidPhysicalKeysFailBeforeHeaderOrStoreAccess)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &) {
        ++recoveryStateCalls;
        return coordinator::TopologyRecoveryState::READY;
    };
    const auto revisionBefore = service->memStore_->CurrentRevision();
    auto store = service->store_;
    service->store_.reset();

    coordinator::PutReqPb put;
    put.set_key("/outside/coordinator");
    coordinator::PutRspPb putResponse;
    EXPECT_EQ(service->Put(put, putResponse).GetCode(), K_INVALID);
    EXPECT_EQ(putResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

    coordinator::RangeReqPb range;
    range.set_key("/outside/coordinator");
    coordinator::RangeRspPb rangeResponse;
    EXPECT_EQ(service->Range(range, rangeResponse).GetCode(), K_INVALID);
    EXPECT_EQ(rangeResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

    coordinator::DeleteRangeReqPb deleteRange;
    deleteRange.set_key("/outside/coordinator");
    coordinator::DeleteRangeRspPb deleteResponse;
    EXPECT_EQ(service->DeleteRange(deleteRange, deleteResponse).GetCode(), K_INVALID);
    EXPECT_EQ(deleteResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

    coordinator::WatchRangeReqPb watch;
    watch.set_key("/outside/coordinator");
    coordinator::WatchRangeRspPb watchResponse;
    EXPECT_EQ(service->WatchRange(watch, watchResponse).GetCode(), K_INVALID);
    EXPECT_EQ(watchResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

    coordinator::KeepAliveReqPb keepAlive;
    keepAlive.set_key("/outside/coordinator");
    coordinator::KeepAliveRspPb keepAliveResponse;
    EXPECT_EQ(service->KeepAlive(keepAlive, keepAliveResponse).GetCode(), K_INVALID);
    EXPECT_EQ(keepAliveResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

    EXPECT_EQ(recoveryStateCalls, 0);
    EXPECT_EQ(service->memStore_->CurrentRevision(), revisionBefore);
    service->store_ = std::move(store);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, MasterAddressSingletonUsesLeadershipOnlyAdmission)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &) {
        ++recoveryStateCalls;
        return coordinator::TopologyRecoveryState::RECOVERING;
    };
    const std::string key = std::string(COORDINATION_MASTER_ADDRESS_TABLE) + "/" + COORDINATION_MASTER_ADDRESS_KEY;

    coordinator::PutReqPb put;
    put.set_key(key);
    put.set_value("127.0.0.1:31501");
    coordinator::PutRspPb putResponse;
    DS_ASSERT_OK(service->Put(put, putResponse));

    coordinator::RangeReqPb range;
    range.set_key(key);
    coordinator::RangeRspPb rangeResponse;
    DS_ASSERT_OK(service->Range(range, rangeResponse));
    ASSERT_EQ(rangeResponse.kvs_size(), 1);
    EXPECT_EQ(rangeResponse.kvs(0).value(), put.value());
    EXPECT_EQ(recoveryStateCalls, 0);

    range.set_range_end(key + "0");
    coordinator::RangeRspPb invalidRangeResponse;
    EXPECT_EQ(service->Range(range, invalidRangeResponse).GetCode(), K_INVALID);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, ReadyAndRecoveringClustersAreAdmittedIndependently)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    EnableElection(*service);
    std::unordered_map<std::string, int> calls;
    service->recoveryStateProvider_ = [&](const std::string &clusterName) {
        ++calls[clusterName];
        return clusterName == "a" ? coordinator::TopologyRecoveryState::READY
                                  : coordinator::TopologyRecoveryState::RECOVERING;
    };

    coordinator::PutReqPb readyPut;
    readyPut.set_key("/datasystem/a/cluster/127.0.0.1:31511");
    readyPut.set_value(EncodeMembershipValue());
    coordinator::PutRspPb readyResponse;
    DS_ASSERT_OK(service->Put(readyPut, readyResponse));
    ExpectHeaderState(readyResponse.header(), coordinator::ResponseHeader::SERVING, service->coordinatorId_,
                      LEADER_TERM);
    EXPECT_GT(readyResponse.revision(), 0);

    coordinator::PutReqPb recoveringPut;
    recoveringPut.set_key("/datasystem/b/cluster/127.0.0.1:31512");
    recoveringPut.set_value(EncodeMembershipValue());
    coordinator::PutRspPb recoveringResponse;
    DS_ASSERT_OK(service->Put(recoveringPut, recoveringResponse));
    ExpectHeaderState(recoveringResponse.header(), coordinator::ResponseHeader::RECOVERING, service->coordinatorId_,
                      LEADER_TERM);
    EXPECT_EQ(recoveringResponse.revision(), 0);

    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(service->store_->Range(readyPut.key(), "", entries, revision));
    EXPECT_EQ(entries.size(), 1U);
    entries.clear();
    DS_ASSERT_OK(service->store_->Range(recoveringPut.key(), "", entries, revision));
    EXPECT_TRUE(entries.empty());
    EXPECT_EQ(calls["a"], 1);
    EXPECT_EQ(calls["b"], 1);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, InvalidRangesFailBeforeClusterHeader)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    EnableElection(*service);
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &) {
        ++recoveryStateCalls;
        return coordinator::TopologyRecoveryState::READY;
    };

    coordinator::RangeReqPb validRange;
    validRange.set_key("/datasystem/a/cluster/");
    validRange.set_range_end("/datasystem/a/cluster0");
    coordinator::RangeRspPb validResponse;
    DS_ASSERT_OK(service->Range(validRange, validResponse));
    ExpectHeaderState(validResponse.header(), coordinator::ResponseHeader::SERVING, service->coordinatorId_,
                      LEADER_TERM);

    coordinator::RangeReqPb crossCluster;
    crossCluster.set_key("/datasystem/a/cluster/");
    crossCluster.set_range_end("/datasystem/b/cluster0");
    coordinator::RangeRspPb crossClusterResponse;
    EXPECT_EQ(service->Range(crossCluster, crossClusterResponse).GetCode(), K_INVALID);
    EXPECT_EQ(crossClusterResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

    coordinator::DeleteRangeReqPb deleteRange;
    deleteRange.set_key("/datasystem/a/cluster/");
    deleteRange.set_range_end("/datasystem/a/cluster0");
    coordinator::DeleteRangeRspPb deleteResponse;
    EXPECT_EQ(service->DeleteRange(deleteRange, deleteResponse).GetCode(), K_INVALID);
    EXPECT_EQ(deleteResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);

    coordinator::WatchRangeReqPb watch;
    watch.set_key("/datasystem/a/topology/");
    watch.set_range_end("/datasystem/b/topology0");
    watch.set_registration_id("cross-cluster");
    coordinator::WatchRangeRspPb watchResponse;
    EXPECT_EQ(service->WatchRange(watch, watchResponse).GetCode(), K_INVALID);
    EXPECT_EQ(watchResponse.header().state(), coordinator::ResponseHeader::STATE_UNSPECIFIED);
    EXPECT_EQ(recoveryStateCalls, 1);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, FollowerAdmissionPrecedesRequestDetailValidation)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &) {
        ++recoveryStateCalls;
        return coordinator::TopologyRecoveryState::READY;
    };
    service->leadershipSnapshotProvider_ = [](coordinator::CoordinatorLeadershipSnapshot &snapshot) {
        snapshot = { false, LEADER_ADDRESS, LEADER_TERM };
        return Status::OK();
    };

    coordinator::WatchRangeReqPb watch;
    watch.set_key("/datasystem/follower/topology/");
    coordinator::WatchRangeRspPb watchResponse;
    DS_ASSERT_OK(service->WatchRange(watch, watchResponse));
    EXPECT_EQ(watchResponse.header().state(), coordinator::ResponseHeader::NOT_LEADER);
    EXPECT_EQ(watchResponse.header().leader_address(), LEADER_ADDRESS);

    coordinator::ReportTopologyRecoveryCandidateReqPb report;
    report.set_cluster_name("follower");
    coordinator::ReportTopologyRecoveryCandidateRspPb reportResponse;
    DS_ASSERT_OK(service->ReportTopologyRecoveryCandidate(report, reportResponse));
    EXPECT_EQ(reportResponse.header().state(), coordinator::ResponseHeader::NOT_LEADER);
    EXPECT_EQ(reportResponse.header().leader_address(), LEADER_ADDRESS);
    EXPECT_EQ(recoveryStateCalls, 0);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, FollowerUnscopedRpcsReturnHeaderWithoutCancelSideEffect)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    int recoveryStateCalls = 0;
    service->recoveryStateProvider_ = [&](const std::string &) {
        ++recoveryStateCalls;
        return coordinator::TopologyRecoveryState::READY;
    };
    const std::string coordinatorId = service->coordinatorId_;
    const std::string membershipKey = "/datasystem/follower/cluster/" + std::string(MEMBER_ADDRESS);
    coordinator::PutReqPb put;
    put.set_key(membershipKey);
    put.set_value(EncodeMembershipValue());
    coordinator::PutRspPb putResponse;
    DS_ASSERT_OK(service->Put(put, putResponse));

    const std::string watchKey = "/datasystem/follower/topology/";
    coordinator::WatchRangeReqPb watch;
    watch.set_key(watchKey);
    watch.set_watcher_addr(MEMBER_ADDRESS);
    watch.set_registration_id("follower-cancel");
    coordinator::WatchRangeRspPb watchResponse;
    DS_ASSERT_OK(service->WatchRange(watch, watchResponse));
    ASSERT_GT(watchResponse.watch_id(), 0);

    service->leadershipSnapshotProvider_ = [](coordinator::CoordinatorLeadershipSnapshot &snapshot) {
        snapshot = { false, LEADER_ADDRESS, LEADER_TERM };
        return Status::OK();
    };
    recoveryStateCalls = 0;
    coordinator::CancelWatchReqPb cancel;
    cancel.set_watcher_addr(MEMBER_ADDRESS);
    cancel.add_watch_ids(watchResponse.watch_id());
    cancel.set_expected_coordinator_id(coordinatorId);
    coordinator::CancelWatchRspPb cancelResponse;
    DS_ASSERT_OK(service->CancelWatch(cancel, cancelResponse));
    EXPECT_EQ(cancelResponse.header().state(), coordinator::ResponseHeader::NOT_LEADER);
    EXPECT_EQ(cancelResponse.header().leader_address(), LEADER_ADDRESS);
    EXPECT_EQ(cancelResponse.header().coordinator_id(), coordinatorId);
    EXPECT_EQ(cancelResponse.header().leader_term(), LEADER_TERM);

    std::vector<std::shared_ptr<WatcherEntry>> matched;
    service->watchRegistry_->MatchWatchers(watchKey, matched);
    ASSERT_EQ(matched.size(), 1U);
    EXPECT_EQ(matched.front()->watchId, watchResponse.watch_id());

    coordinator::GetCoordinatorIdReqPb idRequest;
    coordinator::GetCoordinatorIdRspPb idResponse;
    DS_ASSERT_OK(service->GetCoordinatorId(idRequest, idResponse));
    EXPECT_EQ(idResponse.header().state(), coordinator::ResponseHeader::NOT_LEADER);
    EXPECT_EQ(idResponse.header().leader_address(), LEADER_ADDRESS);
    EXPECT_EQ(idResponse.header().coordinator_id(), coordinatorId);
    EXPECT_EQ(idResponse.header().leader_term(), LEADER_TERM);
    EXPECT_EQ(recoveryStateCalls, 0);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorServiceImplTest, FencedMembershipDeleteExecutesDuringRecovery)
{
    auto service = MakeService();
    DS_ASSERT_OK(InitializeRunning(*service));
    EnableElection(*service);
    service->recoveryStateProvider_ = [](const std::string &clusterName) {
        EXPECT_EQ(clusterName, CLUSTER_NAME);
        return coordinator::TopologyRecoveryState::RECOVERING;
    };

    const std::string membershipKey = "/datasystem/" + std::string(CLUSTER_NAME) + "/cluster/" + MEMBER_ADDRESS;
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(service->store_->Put(membershipKey, EncodeMembershipValue(), MEMBERSHIP_TTL_MS,
                                     COORDINATOR_KEY_NOT_EXISTS_VERSION, version, revision));

    coordinator::DeleteRangeReqPb request;
    request.set_key(membershipKey);
    coordinator::DeleteRangeRspPb response;
    DS_ASSERT_OK(service->DeleteRange(request, response));
    ExpectHeaderState(response.header(), coordinator::ResponseHeader::RECOVERING, service->coordinatorId_, LEADER_TERM);
    EXPECT_EQ(response.deleted(), 0);

    request.set_expected_coordinator_id(service->coordinatorId_);
    request.set_expected_mod_revision(revision);
    response.Clear();
    DS_ASSERT_OK(service->DeleteRange(request, response));
    ExpectHeaderState(response.header(), coordinator::ResponseHeader::RECOVERING, service->coordinatorId_, LEADER_TERM);
    EXPECT_EQ(response.deleted(), 1);

    std::vector<KeyValueEntry> entries;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(service->store_->Range(membershipKey, "", entries, rangeRevision));
    EXPECT_TRUE(entries.empty());

    response.Clear();
    DS_ASSERT_OK(service->DeleteRange(request, response));
    ExpectHeaderState(response.header(), coordinator::ResponseHeader::RECOVERING, service->coordinatorId_, LEADER_TERM);
    EXPECT_EQ(response.deleted(), 0);
    DS_ASSERT_OK(service->Shutdown());
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
