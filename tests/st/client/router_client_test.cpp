/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: rpc util test.
 */
 
#include <memory>
#include <vector>
 
#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/object_client.h"
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
constexpr size_t EVERY_AZ_WORKER_NUM = 3;
constexpr size_t AZ_NUM = 2;
constexpr size_t ETCD_NUM = 2;
const std::string AZ1 = "AZ1";
const std::string AZ2 = "AZ2";
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
 
        // specify az for every worker
        for (size_t i = 0; i < TOTAL_WORKER_NUM; i++) {
            std::string param;
            if (i % AZ_NUM == 0) {
                param = "-cluster_name=" + AZ1 + " -other_cluster_names=" + AZ2;
            } else {
                param = "-cluster_name=" + AZ2 + " -other_cluster_names=" + AZ1;
            }
            opts.workerSpecifyGflagParams[i] = param;
        }
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
        std::string oneAzWorkerAddr;
        if (specifyNode) {
            DS_ASSERT_OK(routerClient.SelectWorker(localNodeAddress_, oneAzWorkerAddr));
        } else {
            DS_ASSERT_OK(routerClient.SelectWorker("", oneAzWorkerAddr));
        }

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
    std::string getWorkerAddr;
    RouterClient client(azName, GetEtcdAddress(), "", "", "", "");
    DS_ASSERT_OK(client.Init());
    DS_ASSERT_NOT_OK(client.SelectWorker(GetLocalNodeAddress(), getWorkerAddr));
}
 
TEST_F(RouterClientTest, TestSelectOneLocalNodeWorker)
{
    bool specifyNode = true;
    SelectWorkerAndConnect(AZ1, specifyNode);
    SelectWorkerAndConnect(AZ2, specifyNode);
}
 
TEST_F(RouterClientTest, TestRandomSelectWorker)
{
    bool specifyNode = false;
    SelectWorkerAndConnect(AZ1, specifyNode);
    SelectWorkerAndConnect(AZ2, specifyNode);
}
 
TEST_F(RouterClientTest, DISABLED_LEVEL1_TestWorkerShutDownInitRouteAndRecovery)
{
    std::vector<int> az1Pos{ 0, 2, 4 };
    for (auto &pos : az1Pos) {
        DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, pos));
    }
    sleep(ETCD_TIME_OUT_SECOND + EXTRA_WAIT_SECOND);
 
    std::string selectedWorkerAddr;
    RouterClient client(AZ1, GetEtcdAddress(), "", "", "", "");
    DS_ASSERT_OK(client.Init());
    DS_ASSERT_NOT_OK(client.SelectWorker(GetLocalNodeAddress(), selectedWorkerAddr));
 
    // restart az1
    for (auto &pos : az1Pos) {
        DS_ASSERT_OK(cluster_->StartNode(WORKER, pos, ""));
    }
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
 
    DS_ASSERT_OK(client.SelectWorker(GetLocalNodeAddress(), selectedWorkerAddr));
    std::shared_ptr<ObjectClient> objectClient;
    InitTestClient(selectedWorkerAddr, objectClient);
}
 
TEST_F(RouterClientTest, DISABLED_LEVEL1_TestInitRouteAndWorkerShutDownAndRecovery)
{
    std::string selectedWorkerAddr;
    RouterClient client(AZ1, GetEtcdAddress(), "", "", "", "");
    DS_ASSERT_OK(client.Init());
    DS_ASSERT_OK(client.SelectWorker(GetLocalNodeAddress(), selectedWorkerAddr));
    std::shared_ptr<ObjectClient> objectClient1;
    InitTestClient(selectedWorkerAddr, objectClient1);
    objectClient1.reset();
 
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 0, 2, 4 }));
 
    DS_ASSERT_OK(client.SelectWorker(GetLocalNodeAddress(), selectedWorkerAddr));
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

        // specify az for every worker
        for (size_t i = 0; i < TOTAL_WORKER_NUM; i++) {
            std::string param;
            if (i % AZ_NUM == 0) {
                param = "-cluster_name=" + AZ1 + " -other_cluster_names=" + AZ2;
            } else {
                param = "-cluster_name=" + AZ2 + " -other_cluster_names=" + AZ1;
            }
            opts.workerSpecifyGflagParams[i] = param;
        }
    }
};

TEST_F(RouterClientIpv6Test, TestSelectSpeccifyLocalNodeWorker)
{
    bool specifyNode = true;

    HostPort az1WorkerHostPort;
    cluster_->GetWorkerAddr(0, az1WorkerHostPort);
    std::string expectedAz1Ip = az1WorkerHostPort.Host();

    std::string selectedAz1Addr;
    SelectWorkerAndConnect(AZ1, specifyNode, &selectedAz1Addr);
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
 
    std::string etcdTablePrefix = "/" + AZ1;
    DS_ASSERT_OK(etcdStore->CreateTable(ETCD_CLUSTER_TABLE, etcdTablePrefix + "/" + std::string(ETCD_CLUSTER_TABLE)));
    // data already exists in etcd.
    std::atomic<int64_t> leaseId;
    int ttlInSec = 60;
    DS_ASSERT_OK(etcdStore->GetLeaseID(ttlInSec, leaseId));
    DS_ASSERT_OK(etcdStore->PutWithLeaseId(ETCD_CLUSTER_TABLE, "127.0.0.1:3000", "fake1", leaseId));
    DS_ASSERT_OK(inject::Set("worker.GenerateFakePutEventIfNeeded.timeout", "call(2000)"));
    DS_ASSERT_OK(inject::Set("EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly", "call(100)"));
    {
        RouterClient client(AZ1, GetEtcdAddress(), "", "", "", "");
        DS_ASSERT_OK(client.Init());
        // generate event
        DS_ASSERT_OK(etcdStore->PutWithLeaseId(ETCD_CLUSTER_TABLE, "127.0.0.1:3000", "fake2", leaseId));
        int waitShutdown = 1000;
        std::this_thread::sleep_for(std::chrono::milliseconds(waitShutdown));
    }
    int waitTimerExecute = 2000;
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimerExecute));
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
        (void)db_->CreateTable(ETCD_RING_PREFIX, "/" + AZ1 + ETCD_RING_PREFIX);
        (void)db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + AZ1 + "/" + std::string(ETCD_CLUSTER_TABLE));
        DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ""));
    }
 
protected:
    void InsertWorkerToDb(std::pair<std::string, std::string> p)
    {
        std::string hashRingStr;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", hashRingStr));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(hashRingStr));
        WorkerPb wpb;
        wpb.set_worker_uuid(p.second);
        ring.mutable_workers()->insert({ p.first, wpb });
        DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));
    };
 
    void DelWorkerFromDb(std::string workerAddr)
    {
        std::string hashRingStr;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", hashRingStr));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(hashRingStr));
        auto it = ring.mutable_workers()->find(workerAddr);
        ring.mutable_workers()->erase(it);
        int64_t version{};
        DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString(), &version));
    };
 
    std::unique_ptr<EtcdStore> db_;
};
 
TEST_F(RouterClientGetWorkerIdTest, NormalGetWorker)
{
    auto alreadyExist = std::make_pair("127.0.0.1:1", "workerid1");
    InsertWorkerToDb(alreadyExist);
 
    RouterClient routerClient(AZ1, etcdAddress_, "", "", "", "");
    DS_ASSERT_OK(routerClient.Init());
 
    // not exist
    std::vector<std::string> result;
    DS_ASSERT_NOT_OK(routerClient.GetWorkerAddrByWorkerId({ "not-exist-workerid" }, result));
    ASSERT_TRUE(result.empty());
 
    // already exist
    DS_ASSERT_OK(routerClient.GetWorkerAddrByWorkerId({ alreadyExist.second }, result));
    ASSERT_TRUE(result.size() == 1);
    ASSERT_TRUE(result[0] == alreadyExist.first);
 
    // newly-added
    auto newlyAdd = std::make_pair("127.0.0.1:2", "workerid2");
    InsertWorkerToDb(newlyAdd);
    const int receiveEventDelay = 1;
    std::this_thread::sleep_for(std::chrono::seconds(receiveEventDelay));  // wait for the reach of event
    result.clear();
    DS_ASSERT_OK(routerClient.GetWorkerAddrByWorkerId({ newlyAdd.second }, result));
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
    input = { alreadyExist.second, "not-exist-workerid", newlyAdd.second };
    DS_ASSERT_OK(routerClient.GetWorkerAddrByWorkerId(input, result));
    ASSERT_TRUE(result.size() == input.size());
    auto i = 0;
    ASSERT_TRUE(result[i++] == alreadyExist.first);
    ASSERT_TRUE(result[i++] == "");
    ASSERT_TRUE(result[i++] == newlyAdd.first);
 
    // cannot be found after delete
    DelWorkerFromDb(alreadyExist.first);
    std::this_thread::sleep_for(std::chrono::milliseconds(receiveEventDelay));  // wait for the reach of event
    result.clear();
    DS_ASSERT_NOT_OK(routerClient.GetWorkerAddrByWorkerId({ alreadyExist.second }, result));
    ASSERT_TRUE(result.size() == 0);
 
    // multiple get and all fail
    result.clear();
    DS_ASSERT_NOT_OK(routerClient.GetWorkerAddrByWorkerId({ alreadyExist.second, "not-exist-workerid" }, result));
    ASSERT_TRUE(result.size() == 0);
}
}  // namespace st
}  // namespace datasystem
