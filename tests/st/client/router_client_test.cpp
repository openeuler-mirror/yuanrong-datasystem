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
 
#include <memory>
#include <vector>
 
#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/object_client.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/router_client.h"
 
DS_DECLARE_string(cluster_name);
DS_DECLARE_bool(enable_etcd_auth);
DS_DECLARE_string(encrypt_kit);
DS_DECLARE_string(secret_key1);
DS_DECLARE_string(secret_key2);
DS_DECLARE_string(secret_key3);
DS_DECLARE_string(secret_salt);
DS_DECLARE_string(etcd_address);
 
namespace datasystem {
namespace st {
constexpr size_t TOTAL_WORKER_NUM = 6;
constexpr size_t ETCD_NUM = 2;
constexpr uint32_t TOKENS_PER_MEMBER = 4;
const std::string AZ1 = "AZ1";
const uint32_t ETCD_TIME_OUT_SECOND = 5;
const uint32_t EXTRA_WAIT_SECOND = 3;
 
class RouterClientTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = ETCD_NUM;
        opts.numWorkers = TOTAL_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        std::string workerGflags =
            "-v=1 add_node_wait_time_s=5 -node_timeout_s=" + std::to_string(ETCD_TIME_OUT_SECOND);
        opts.workerGflagParams = workerGflags;
 
    }
 
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < TOTAL_WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        InitEtcdAddress();
        InitNodeAddress();
    }
 
    void InitEtcdAddress()
    {
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress_.empty()) {
                etcdAddress_ += ",";
            }
            etcdAddress_ += addrs.first.ToString();
        }
    }
 
    void InitNodeAddress()
    {
        HostPort workerHostPort;
        cluster_->GetWorkerAddr(0, workerHostPort);
        localNodeAddress_ = workerHostPort.Host();
    }
 
    std::string GetEtcdAddress()
    {
        return etcdAddress_;
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
        RouterClient routerClient(azName, etcdAddress_, "", "", "", "");
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
    std::string etcdAddress_;
    std::string localNodeAddress_;
};
 
TEST_F(RouterClientTest, TestInitInvalidAz)
{
    std::string azName = "AZ3";
    RouterClient client(azName, GetEtcdAddress(), "", "", "", "");
    EXPECT_EQ(client.Init().GetCode(), K_NOT_FOUND);
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
    std::vector<int> az1Pos{ 0, 2, 4 };
    for (auto &pos : az1Pos) {
        DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, pos));
    }
    sleep(ETCD_TIME_OUT_SECOND + EXTRA_WAIT_SECOND);
 
    std::string selectedWorkerAddr;
    RouterClient client(GetTestClusterName(), GetEtcdAddress(), "", "", "", "");
    DS_ASSERT_OK(client.Init());
    std::vector<std::string> candidates;
    DS_ASSERT_NOT_OK(client.GetWorkerCandidates(GetLocalNodeAddress(), candidates));
 
    // restart az1
    for (auto &pos : az1Pos) {
        DS_ASSERT_OK(cluster_->StartNode(WORKER, pos, ""));
    }
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
 
    DS_ASSERT_OK(client.GetWorkerCandidates(GetLocalNodeAddress(), candidates));
    selectedWorkerAddr = candidates.front();
    std::shared_ptr<ObjectClient> objectClient;
    InitTestClient(selectedWorkerAddr, objectClient);
}
 
TEST_F(RouterClientTest, DISABLED_LEVEL1_TestInitRouteAndWorkerShutDownAndRecovery)
{
    std::string selectedWorkerAddr;
    RouterClient client(GetTestClusterName(), GetEtcdAddress(), "", "", "", "");
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
        opts.numEtcd = ETCD_NUM;
        opts.numWorkers = TOTAL_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        std::string workerGflags =
            "-v=1 add_node_wait_time_s=5 -node_timeout_s=" + std::to_string(ETCD_TIME_OUT_SECOND);
        opts.workerGflagParams = workerGflags;

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
        opts.numEtcd = ETCD_NUM;
        opts.numWorkers = 0;
    }
 
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitEtcdAddress();
    }
};
 
TEST_F(SimpleRouterClientTest, DestrutClientWhenTimerExists)
{
    FLAGS_log_monitor = false;
    ASSERT_TRUE(TimerQueue::GetInstance()->Initialize());
    auto etcdStore = std::make_shared<EtcdStore>(etcdAddress_);
    DS_ASSERT_OK(etcdStore->Init());
 
    DS_ASSERT_OK(RegisterTopologyTables(*etcdStore, AZ1));
    ClusterTopologyPb topology;
    topology.set_schema_version("1");
    topology.set_cluster_has_init(true);
    topology.set_version(1);
    auto &member = (*topology.mutable_members())["127.0.0.1:3000"];
    member.set_id(std::string(16, 'a'));
    member.set_state(MembershipPb::ACTIVE);
    member.add_tokens(1);
    DS_ASSERT_OK(etcdStore->Put(GetTopologyTableName(AZ1), "", topology.SerializeAsString()));
    DS_ASSERT_OK(inject::Set("worker.GenerateFakePutEventIfNeeded.timeout", "call(2000)"));
    DS_ASSERT_OK(inject::Set("EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly", "call(100)"));
    {
        RouterClient client(AZ1, GetEtcdAddress(), "", "", "", "");
        DS_ASSERT_OK(client.Init());
        // generate event
        topology.set_version(2);
        DS_ASSERT_OK(etcdStore->Put(GetTopologyTableName(AZ1), "", topology.SerializeAsString()));
        int waitShutdown = 1000;
        std::this_thread::sleep_for(std::chrono::milliseconds(waitShutdown));
    }
    int waitTimerExecute = 2000;
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimerExecute));
}

TEST_F(SimpleRouterClientTest, ReturnsOrderedTopologyCandidatesAndBinaryIdResolution)
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    DS_ASSERT_OK(cluster::TopologyKeyHelper::Create(AZ1, keys));
    auto store = std::make_shared<EtcdStore>(etcdAddress_);
    DS_ASSERT_OK(store->Init());
    DS_ASSERT_OK(store->CreateTableWithExactPrefix(keys->TopologyTable(), keys->TopologyTable()));
    cluster::TopologyState topology;
    topology.version = 1;
    topology.clusterHasInit = true;
    topology.members = {
        cluster::Member{ { std::string(16, 'a'), "127.0.0.1:3001" }, cluster::MemberState::ACTIVE, { 1 } },
        cluster::Member{ { std::string(16, 'b'), "127.0.0.2:3002" }, cluster::MemberState::ACTIVE, { 2 } },
        cluster::Member{ { std::string(16, 'c'), "127.0.0.3:3003" }, cluster::MemberState::INITIAL, {} },
    };
    std::string bytes;
    DS_ASSERT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(topology, bytes));
    DS_ASSERT_OK(store->Put(keys->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey(), bytes));
    RouterClient client(AZ1, GetEtcdAddress(), "", "", "", "");
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
}
 
class RouterClientGetWorkerIdTest : public RouterClientTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
    }
 
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        FLAGS_v = 1;
        InitTestEtcdInstance();
    }
 
    void InitTestEtcdInstance()
    {
        if (db_ != nullptr) {
            return;
        }
        InitEtcdAddress();
        FLAGS_etcd_address = etcdAddress_;
        db_ = std::make_unique<EtcdStore>(etcdAddress_);
        DS_ASSERT_OK(db_->Init());
        (void)RegisterTopologyTables(*db_, AZ1);
        ClusterTopologyPb ring;
        ring.set_schema_version("1");
        ring.set_version(1);
        DS_ASSERT_OK(db_->Put(GetTopologyTableName(AZ1), "", ring.SerializeAsString()));
    }
 
protected:
    void InsertWorkerToDb(std::pair<std::string, std::string> p)
    {
        std::string hashRingStr;
        DS_ASSERT_OK(db_->Get(GetTopologyTableName(AZ1), "", hashRingStr));
        ClusterTopologyPb ring;
        ASSERT_TRUE(ring.ParseFromString(hashRingStr));
        MembershipPb wpb;
        wpb.set_id(p.second);
        wpb.set_state(MembershipPb::ACTIVE);
        const auto tokenBase = static_cast<uint32_t>(ring.members_size()) * TOKENS_PER_MEMBER;
        for (uint32_t offset = 0; offset < TOKENS_PER_MEMBER; ++offset) {
            wpb.add_tokens(tokenBase + offset);
        }
        ring.mutable_members()->insert({ p.first, wpb });
        ring.set_version(ring.version() + 1);
        DS_ASSERT_OK(db_->Put(GetTopologyTableName(AZ1), "", ring.SerializeAsString()));
    };
 
    void DelWorkerFromDb(std::string workerAddr)
    {
        std::string hashRingStr;
        DS_ASSERT_OK(db_->Get(GetTopologyTableName(AZ1), "", hashRingStr));
        ClusterTopologyPb ring;
        ASSERT_TRUE(ring.ParseFromString(hashRingStr));
        auto it = ring.mutable_members()->find(workerAddr);
        ring.mutable_members()->erase(it);
        ring.set_version(ring.version() + 1);
        int64_t version{};
        DS_ASSERT_OK(db_->Put(GetTopologyTableName(AZ1), "", ring.SerializeAsString(), &version));
    };
 
    std::unique_ptr<EtcdStore> db_;
};
 
TEST_F(RouterClientGetWorkerIdTest, NormalGetWorker)
{
    auto alreadyExist = std::make_pair("127.0.0.1:1", std::string(16, 'a'));
    const std::string unknownId(16, 'x');
    InsertWorkerToDb(alreadyExist);
 
    RouterClient routerClient(AZ1, etcdAddress_, "", "", "", "");
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
    InsertWorkerToDb(newlyAdd);
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
    DelWorkerFromDb(alreadyExist.first);
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
