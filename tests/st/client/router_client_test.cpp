/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: rpc util test.
 */
 
#include <algorithm>
#include <memory>
#include <set>
#include <vector>
 
#include "common.h"
#include "datasystem/client/cluster_query/cluster_query_client.h"
#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/coordinator/static_coordinator_discovery.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object_client.h"
#include "datasystem/router_client.h"

namespace datasystem {
namespace st {
constexpr size_t TOTAL_WORKER_NUM = 6;
constexpr uint32_t TOKENS_PER_MEMBER = 4;
const std::string AZ1 = "AZ1";
const uint32_t NODE_TIMEOUT_SECOND = 5;
const uint32_t EXTRA_WAIT_SECOND = 10;

std::vector<uint32_t> BuildMemberTokens(const std::string &address)
{
    std::vector<uint32_t> tokens;
    tokens.reserve(TOKENS_PER_MEMBER);
    for (uint32_t index = 0; index < TOKENS_PER_MEMBER; ++index) {
        tokens.emplace_back(cluster::HashAlgorithm::MakeToken(address, index, 0));
    }
    return tokens;
}
 
class RouterClientTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 0;
        opts.numCoordinators = 1;
        opts.numWorkers = TOTAL_WORKER_NUM;
        opts.workerGflagParams = "-node_timeout_s=" + std::to_string(NODE_TIMEOUT_SECOND);
        opts.coordinatorGflagParams = "-node_timeout_s=3 -node_dead_timeout_s=6";
        opts.waitWorkerReady = true;
        opts.waitAfterStart = false;
    }
 
    void SetUp() override
    {
        OCClientCommon::SetUp();
        auto &external = static_cast<ExternalCluster &>(*cluster_);
        for (const auto &addr : external.GetExternalOptions().coordinatorConfigs) {
            if (!coordinatorAddress_.empty()) {
                coordinatorAddress_ += ",";
            }
            coordinatorAddress_ += addr.ToString();
        }
        if (cluster_->GetWorkerNum() > 0) {
            HostPort addr;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(0, addr));
            localNodeAddress_ = addr.Host();
        }
    }

    std::string GetLocalNodeAddress()
    {
        return localNodeAddress_;
    }
 
    void InitTestClient(const std::string &workerHostPortStr, std::shared_ptr<ObjectClient> &client)
    {
        HostPort hostPort;
        DS_ASSERT_OK(hostPort.ParseString(workerHostPortStr));
        int32_t timeoutMs = 60000;
        ConnectOptions connectOptions = { .host = hostPort.Host(),
                                          .port = hostPort.Port(),
                                          .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }
 
    void SelectWorkerAndConnect(std::string azName, bool specifyNode, std::string *outAddr = nullptr)
    {
        RouterClient routerClient(azName, coordinatorAddress_);
        DS_ASSERT_OK(routerClient.Init());
        std::vector<std::string> candidates;
        DS_ASSERT_OK(cluster_->WaitForExpectedResult(
            [&routerClient, &candidates, this, specifyNode]() {
                return routerClient.GetWorkerCandidates(specifyNode ? localNodeAddress_ : "", candidates);
            },
            EXTRA_WAIT_SECOND, K_OK));
        ASSERT_FALSE(candidates.empty());
        const std::string oneAzWorkerAddr = candidates.front();

        if (outAddr != nullptr) {
            *outAddr = oneAzWorkerAddr;
        }
 
        std::shared_ptr<ObjectClient> objectClient;
        InitTestClient(oneAzWorkerAddr, objectClient);
    }
 
protected:
    std::string coordinatorAddress_;
    std::string localNodeAddress_;
};

class MembershipRecoveryQueryTest : public RouterClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        RouterClientTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 2;
        opts.numCoordinators = 3;
        opts.enableCoordinatorElection = true;
    }

protected:
    int GetTestCaseTimeoutSecs() const override
    {
        return 180;
    }

    Status QueryExpectedWorkersHealthy(client::cluster_query::ClusterQueryClient &queryClient,
                                       const std::set<std::string> &expectedWorkers)
    {
        client::cluster_query::ClusterQueryResult result;
        RETURN_IF_NOT_OK(queryClient.QueryCluster(result));
        if (result.nodes.size() != expectedWorkers.size()) {
            return Status(K_NOT_READY, "Cluster query has not converged to the expected worker count");
        }
        for (const auto &node : result.nodes) {
            if (expectedWorkers.count(node.workerAddress) == 0
                || node.health != client::cluster_query::ClusterNodeHealth::HEALTHY) {
                return Status(K_NOT_READY, "Cluster query still contains a non-healthy or unexpected worker");
            }
        }
        return Status::OK();
    }

    void WaitForExpectedWorkersHealthy(client::cluster_query::ClusterQueryClient &queryClient,
                                       const std::set<std::string> &expectedWorkers)
    {
        DS_ASSERT_OK(cluster_->WaitForExpectedResult(
            [this, &queryClient, &expectedWorkers] {
                return QueryExpectedWorkersHealthy(queryClient, expectedWorkers);
            },
            30, K_OK));
    }
};

TEST_F(MembershipRecoveryQueryTest, RoutesToRecoveredWorkerAfterCoordinatorFailover)
{
    HostPort worker0;
    HostPort worker1;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1));
    const std::set<std::string> expectedWorkers{ worker0.ToString(), worker1.ToString() };

    client::cluster_query::ClusterQueryOptions queryOptions;
    queryOptions.clusterName = GetTestClusterName();
    queryOptions.coordinatorAddress = coordinatorAddress_;
    client::cluster_query::ClusterQueryClient queryClient(std::move(queryOptions));
    DS_ASSERT_OK(queryClient.Init());
    WaitForExpectedWorkersHealthy(queryClient, expectedWorkers);

    RouterClient routerClient(GetTestClusterName(), coordinatorAddress_);
    DS_ASSERT_OK(routerClient.Init());

    auto &external = static_cast<ExternalCluster &>(*cluster_);
    HostPort leader;
    CoordinatorServiceProxyBrpcImpl probe(std::make_shared<StaticCoordinatorDiscovery>(coordinatorAddress_));
    DS_ASSERT_OK(probe.Init());
    DS_ASSERT_OK(probe.SetLeaderChangeHandler([&leader](const auto &identity) { leader = identity.address; }));
    std::string originalId;
    DS_ASSERT_OK(probe.GetCoordinatorId(originalId, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
    const auto coordinatorCount = external.GetExternalOptions().numCoordinators;
    size_t leaderIndex = 0;
    for (; leaderIndex < coordinatorCount; ++leaderIndex) {
        HostPort address;
        DS_ASSERT_OK(external.GetCoordinatorAddr(leaderIndex, address));
        if (address == leader) {
            break;
        }
    }
    ASSERT_LT(leaderIndex, coordinatorCount);
    DS_ASSERT_OK(cluster_->ShutdownNode(COORDINATOR, leaderIndex));
    WaitForExpectedWorkersHealthy(queryClient, expectedWorkers);
    std::string electedId;
    DS_ASSERT_OK(probe.GetCoordinatorId(electedId, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
    ASSERT_NE(electedId, originalId);
    DS_ASSERT_OK(external.RestartWorkerAndWaitReadyOneByOne({ 1 }));
    WaitForExpectedWorkersHealthy(queryClient, expectedWorkers);

    HostPort added(worker0.Host(), GetFreePort());
    DS_ASSERT_OK(external.AddNode(added.ToString()));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    std::vector<std::string> candidates;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&routerClient, &candidates, &added, &worker1] {
            RETURN_IF_NOT_OK(routerClient.GetWorkerCandidates("", candidates));
            const std::set<std::string> active(candidates.begin(), candidates.end());
            return active.count(added.ToString()) && active.count(worker1.ToString())
                       ? Status::OK()
                       : Status(K_NOT_READY, "Router has not observed the recovered and newly added workers");
        },
        30, K_OK));

    std::shared_ptr<ObjectClient> objectClient;
    InitTestClient(worker1.ToString(), objectClient);
    const std::string objectKey = "membership_recovery_smoke";
    const std::string value = "healthy_after_coordinator_failover";
    DS_ASSERT_OK(objectClient->Put(objectKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(),
                                   CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(objectClient->Get({ objectKey }, 0, buffers));
    ASSERT_EQ(buffers.size(), 1);
    ASSERT_TRUE(buffers.front());
    AssertBufferEqual(*buffers.front(), value);
}

TEST_F(RouterClientTest, RetriesAfterDiscoveryThreadStartFailure)
{
    RouterClient client(GetTestClusterName(), coordinatorAddress_);
    constexpr char point[] = "CoordinatorDiscoveryCache.StartThread";
    DS_ASSERT_OK(inject::Set(point, "1*call()"));
    Raii clear([&] { (void)inject::Clear(point); });
    ASSERT_EQ(client.Init().GetCode(), K_RUNTIME_ERROR);
    DS_ASSERT_OK(client.Init());
}

TEST_F(RouterClientTest, TestInitInvalidAz)
{
    std::string azName = "AZ3";
    RouterClient client(azName, coordinatorAddress_);
    DS_ASSERT_OK(cluster_->WaitForExpectedResult([&client] { return client.Init(); }, EXTRA_WAIT_SECOND, K_NOT_FOUND));
}
 
TEST_F(RouterClientTest, TestSelectOneLocalNodeWorker)
{
    bool specifyNode = true;
    SelectWorkerAndConnect(GetTestClusterName(), specifyNode);
}
 
TEST_F(RouterClientTest, TestRandomSelectWorker)
{
    bool specifyNode = false;
    SelectWorkerAndConnect(GetTestClusterName(), specifyNode);
}
 
TEST_F(RouterClientTest, DISABLED_LEVEL1_TestWorkerShutDownInitRouteAndRecovery)
{
    const std::vector<int> stopped{ 0, 2, 4 };
    std::set<std::string> stoppedAddresses;
    for (auto index : stopped) {
        HostPort address;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(index, address));
        stoppedAddresses.insert(address.ToString());
        DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, index));
    }
    RouterClient client(GetTestClusterName(), coordinatorAddress_);
    DS_ASSERT_OK(client.Init());
    std::vector<std::string> candidates;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&] {
            RETURN_IF_NOT_OK(client.GetWorkerCandidates(GetLocalNodeAddress(), candidates));
            return std::none_of(candidates.begin(), candidates.end(), [&](const std::string &address) {
                return stoppedAddresses.count(address) != 0;
            }) ? Status::OK() : Status(K_NOT_READY, "Stopped worker is still a routing candidate");
        },
        30, K_OK));

    for (auto index : stopped) {
        DS_ASSERT_OK(cluster_->StartNode(WORKER, index, ""));
    }
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&] {
            RETURN_IF_NOT_OK(client.GetWorkerCandidates(GetLocalNodeAddress(), candidates));
            return candidates.size() == TOTAL_WORKER_NUM ? Status::OK()
                                                         : Status(K_NOT_READY, "Waiting for restarted workers");
        },
        30, K_OK));
    std::shared_ptr<ObjectClient> objectClient;
    InitTestClient(candidates.front(), objectClient);
}
 
TEST_F(RouterClientTest, DISABLED_LEVEL1_TestInitRouteAndWorkerShutDownAndRecovery)
{
    std::string selectedWorkerAddr;
    RouterClient client(GetTestClusterName(), coordinatorAddress_);
    DS_ASSERT_OK(client.Init());
    std::vector<std::string> candidates;
    DS_ASSERT_OK(client.GetWorkerCandidates(GetLocalNodeAddress(), candidates));
    selectedWorkerAddr = candidates.front();
    std::shared_ptr<ObjectClient> objectClient1;
    InitTestClient(selectedWorkerAddr, objectClient1);
    objectClient1.reset();
 
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 0, 2, 4 }));
 
    candidates.clear();
    DS_ASSERT_OK(client.GetWorkerCandidates(GetLocalNodeAddress(), candidates));
    selectedWorkerAddr = candidates.front();
    std::shared_ptr<ObjectClient> objectClient2;
    InitTestClient(selectedWorkerAddr, objectClient2);
}
 
class RouterClientIpv6Test : public RouterClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        RouterClientTest::SetClusterSetupOptions(opts);

        // set the worker's address to IPv6
        opts.workerConfigs.clear();
        const std::string IPV6_LOCALHOST = "::1";
        uint16_t startPort = 8888;
        for (size_t i = 0; i < TOTAL_WORKER_NUM; i++) {
            HostPort ipv6HostPort;
            std::string ipv6Addr = "[" + IPV6_LOCALHOST + "]:" + std::to_string(startPort + i);
            DS_ASSERT_OK(ipv6HostPort.ParseString(ipv6Addr));
            opts.workerConfigs.push_back(ipv6HostPort);
        }

    }
};

TEST_F(RouterClientIpv6Test, DISABLED_TestSelectSpeccifyLocalNodeWorker)
{
    bool specifyNode = true;

    HostPort az1WorkerHostPort;
    cluster_->GetWorkerAddr(0, az1WorkerHostPort);
    std::string expectedAz1Ip = az1WorkerHostPort.Host();

    std::string selectedAz1Addr;
    SelectWorkerAndConnect(GetTestClusterName(), specifyNode, &selectedAz1Addr);
    std::string expectedAz1Prefix = "[" + expectedAz1Ip + "]:";
    ASSERT_TRUE(selectedAz1Addr.find(expectedAz1Prefix) == 0)
        << "AZ1: Selected worker addr [" << selectedAz1Addr
        << "] does not match expected IP prefix [" << expectedAz1Prefix << "]";
}

class SimpleRouterClientTest : public RouterClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        RouterClientTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 0;
    }
 
    void SetUp() override
    {
        RouterClientTest::SetUp();
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        DS_ASSERT_OK(cluster::TopologyKeyHelper::Create(AZ1, keys));
        topologyKey_ = keys->TopologyTable() + "/" + keys->TopologyKey();
        writer_ = std::make_unique<CoordinatorServiceProxyBrpcImpl>(
            std::make_shared<StaticCoordinatorDiscovery>(coordinatorAddress_));
        DS_ASSERT_OK(writer_->Init());
        RouterClient missing(AZ1, coordinatorAddress_);
        DS_ASSERT_OK(cluster_->WaitForExpectedResult([&missing] { return missing.Init(); }, EXTRA_WAIT_SECOND,
                                                     K_NOT_FOUND));
    }

    Status PutTopology(const cluster::TopologyState &topology)
    {
        std::string bytes;
        RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(topology, bytes));
        int64_t version = 0;
        int64_t revision = 0;
        return writer_->Put(topologyKey_, bytes, 0, COORDINATOR_NO_VERSION_CHECK, version, revision);
    }

private:
    std::string topologyKey_;
    std::unique_ptr<ICoordinatorServiceProxy> writer_;
};

TEST_F(SimpleRouterClientTest, ReturnsOrderedTopologyCandidatesAndBinaryIdResolution)
{
    RouterClient invalid(AZ1, "");
    EXPECT_EQ(invalid.Init().GetCode(), K_INVALID);
    RouterClient client(AZ1, coordinatorAddress_);
    EXPECT_EQ(client.Init().GetCode(), K_NOT_FOUND);
    cluster::TopologyState topology;
    topology.version = 1;
    topology.clusterHasInit = true;
    topology.tokensPerMember = TOKENS_PER_MEMBER;
    topology.members = {
        cluster::Member{ { std::string(16, 'a'), "127.0.0.1:3001" }, cluster::MemberState::ACTIVE,
                         BuildMemberTokens("127.0.0.1:3001") },
        cluster::Member{ { std::string(16, 'b'), "127.0.0.2:3002" }, cluster::MemberState::ACTIVE,
                         BuildMemberTokens("127.0.0.2:3002") },
        cluster::Member{ { std::string(16, 'c'), "127.0.0.3:3003" }, cluster::MemberState::INITIAL, {} },
    };
    DS_ASSERT_OK(PutTopology(topology));
    DS_ASSERT_OK(client.Init());
    DS_ASSERT_OK(client.Init());
    std::vector<std::string> candidates;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&client, &candidates]() { return client.GetWorkerCandidates("127.0.0.2", candidates); },
        EXTRA_WAIT_SECOND, K_OK));
    ASSERT_EQ(candidates.size(), 2);
    EXPECT_EQ(candidates.front(), "127.0.0.2:3002");
    std::vector<std::string> addresses;
    DS_ASSERT_OK(client.GetWorkerAddrByWorkerId({ std::string(16, 'a'), std::string(16, 'x') }, addresses));
    ASSERT_EQ(addresses.size(), 2);
    EXPECT_EQ(addresses.front(), "127.0.0.1:3001");
    EXPECT_TRUE(addresses.back().empty());
    DS_ASSERT_OK(client.GetWorkerAddrByWorkerId({ std::string(16, 'c') }, addresses));
    EXPECT_EQ(addresses, std::vector<std::string>({ "127.0.0.3:3003" }));
    EXPECT_EQ(client.GetWorkerAddrByWorkerId({ "short" }, addresses).GetCode(), K_INVALID);
    EXPECT_EQ(client.GetWorkerAddrByWorkerId({}, addresses).GetCode(), K_INVALID);
    DS_ASSERT_OK(client.GetWorkerAddrByWorkerId(std::vector<std::string>(100, std::string(16, 'a')), addresses));
    EXPECT_EQ(client.GetWorkerAddrByWorkerId(std::vector<std::string>(101, std::string(16, 'a')), addresses).GetCode(),
              K_INVALID);
    ++topology.version;
    topology.members.clear();
    DS_ASSERT_OK(PutTopology(topology));
    DS_ASSERT_OK(cluster_->WaitForExpectedResult([&] { return client.GetWorkerCandidates("", candidates); },
                                                 EXTRA_WAIT_SECOND, K_NOT_FOUND));
    EXPECT_TRUE(candidates.empty());
}
 
TEST_F(SimpleRouterClientTest, NormalGetWorker)
{
    auto alreadyExist = std::make_pair("127.0.0.1:1", std::string(16, 'a'));
    const std::string unknownId(16, 'x');
    cluster::TopologyState topology;
    topology.version = 1;
    topology.tokensPerMember = TOKENS_PER_MEMBER;
    topology.members.push_back({ { alreadyExist.second, alreadyExist.first }, cluster::MemberState::ACTIVE,
                                 BuildMemberTokens(alreadyExist.first) });
    DS_ASSERT_OK(PutTopology(topology));
    RouterClient routerClient(AZ1, coordinatorAddress_);
    DS_ASSERT_OK(routerClient.Init());

    // not exist
    std::vector<std::string> result;
    DS_ASSERT_NOT_OK(routerClient.GetWorkerAddrByWorkerId({ unknownId }, result));
    ASSERT_TRUE(result.empty());
 
    // already exist
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&routerClient, &alreadyExist, &result]() {
            result.clear();
            return routerClient.GetWorkerAddrByWorkerId({ alreadyExist.second }, result);
        },
        EXTRA_WAIT_SECOND, K_OK));
    ASSERT_TRUE(result.size() == 1);
    ASSERT_TRUE(result[0] == alreadyExist.first);
 
    // newly-added
    auto newlyAdd = std::make_pair("127.0.0.1:2", std::string(16, 'b'));
    ++topology.version;
    topology.members.push_back({ { newlyAdd.second, newlyAdd.first }, cluster::MemberState::ACTIVE,
                                 BuildMemberTokens(newlyAdd.first) });
    DS_ASSERT_OK(PutTopology(topology));
    result.clear();
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&routerClient, &newlyAdd, &result]() {
            return routerClient.GetWorkerAddrByWorkerId({ newlyAdd.second }, result);
        },
        EXTRA_WAIT_SECOND, K_OK));
    ASSERT_TRUE(result.size() == 1);
    ASSERT_TRUE(result[0] == newlyAdd.first);
 
    // multiple get and all success
    result.clear();
    std::vector<std::string> input = { alreadyExist.second, newlyAdd.second };
    DS_ASSERT_OK(routerClient.GetWorkerAddrByWorkerId(input, result));
    ASSERT_TRUE(result.size() == input.size());
    ASSERT_TRUE(result[0] == alreadyExist.first);
    ASSERT_TRUE(result[1] == newlyAdd.first);
 
    // multiple get and partly success
    result.clear();
    input = { alreadyExist.second, unknownId, newlyAdd.second };
    DS_ASSERT_OK(routerClient.GetWorkerAddrByWorkerId(input, result));
    ASSERT_TRUE(result.size() == input.size());
    auto i = 0;
    ASSERT_TRUE(result[i++] == alreadyExist.first);
    ASSERT_TRUE(result[i++] == "");
    ASSERT_TRUE(result[i++] == newlyAdd.first);
 
    // cannot be found after delete
    ++topology.version;
    topology.members.erase(topology.members.begin());
    DS_ASSERT_OK(PutTopology(topology));
    result.clear();
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&routerClient, &alreadyExist, &result]() {
            result.clear();
            return routerClient.GetWorkerAddrByWorkerId({ alreadyExist.second }, result);
        },
        EXTRA_WAIT_SECOND, K_NOT_FOUND));
    ASSERT_TRUE(result.size() == 0);
 
    // multiple get and all fail
    result.clear();
    DS_ASSERT_NOT_OK(routerClient.GetWorkerAddrByWorkerId({ alreadyExist.second, unknownId }, result));
    ASSERT_TRUE(result.size() == 0);
}
}  // namespace st
}  // namespace datasystem
