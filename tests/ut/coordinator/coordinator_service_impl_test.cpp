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

#include <string>
#include <vector>

#include "ut/common.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#define private public
#include "datasystem/coordinator/coordinator_service_impl.h"
#undef private

namespace datasystem {
namespace ut {
namespace {
DS_DECLARE_bool(use_brpc);
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
}  // namespace
}  // namespace ut
}  // namespace datasystem
