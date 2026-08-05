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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/worker/cluster_event_type.h"
#include "ut/common.h"

namespace datasystem::ut {
namespace {
constexpr char SUBSCRIBER_NAME[] = "WorkerOCServerTest";

cluster::Member MakeMember(char idByte, std::string address, cluster::MemberState state, uint32_t token)
{
    return cluster::Member{ { std::string(16, idByte), std::move(address) }, state, { token } };
}

Status MakeSnapshot(cluster::MemberState targetState, uint64_t version,
                    std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    cluster::TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    state.members = { MakeMember('a', "127.0.0.1:31501", cluster::MemberState::ACTIVE, 10),
                      MakeMember('b', "127.0.0.1:31502", targetState, 20) };
    if (targetState == cluster::MemberState::FAILED) {
        state.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::FAILURE, version };
    }
    return cluster::TopologySnapshot::Create(std::move(state), version, std::string(64, 'a'), snapshot);
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

protected:
    std::unique_ptr<worker::WorkerOCServer> server_;
    std::string recoveredAddress_;
    size_t recoveryCount_{ 0 };
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
}  // namespace datasystem::ut
