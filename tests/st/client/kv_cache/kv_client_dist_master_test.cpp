/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: State client tests.
 */
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/kv_cache/kv_client.h"

#include <unistd.h>
#include <atomic>
#include <csignal>
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/object_cache/object_enum.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "client/object_cache/oc_client_common.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);

namespace datasystem {
namespace st {
namespace {
const std::string HOST_IP = "127.0.0.1";
}  // namespace

class STCClientDistMasterTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.numOBS = 1;
        opts.workerGflagParams = "-node_timeout_s=5 -node_dead_timeout_s=8";
        opts.enableDistributedMaster = "true";
        opts.waitWorkerReady = false;
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitTestEtcdInstance();
        DS_ASSERT_OK(inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)"));
        DS_ASSERT_OK(inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)"));
        DS_ASSERT_OK(inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)"));
    }

    void TearDown() override
    {
        client_.reset();
        client1_.reset();
        client2_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTestEtcdInstance();

    void InitClients(int32_t timeoutMs = 60000);

    void StartClustersAndWaitReady(int32_t timeoutMs = 60000);

    void ShutdownEraseRocksDBAndRestartNodes();

    void GetWorkerUuids();

    std::unique_ptr<EtcdStore> db_;
    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    std::shared_ptr<KVClient> client2_;
    std::unordered_map<HostPort, std::string> uuidMap_;
};

void STCClientDistMasterTest::InitTestEtcdInstance()
{
    std::string etcdAddress;
    for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
        std::pair<HostPort, HostPort> addrs;
        cluster_->GetEtcdAddrs(i, addrs);
        if (!etcdAddress.empty()) {
            etcdAddress += ",";
        }
        etcdAddress += addrs.first.ToString();
    }
    FLAGS_etcd_address = etcdAddress;
    LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
    db_ = std::make_unique<EtcdStore>(etcdAddress);
    if ((db_ != nullptr) && (db_->Init().IsOk())) {
        db_->DropTable(ETCD_RING_PREFIX);
        // We don't check rc here. If table to drop does not exist, it's fine.
        (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
    }
}

void STCClientDistMasterTest::StartClustersAndWaitReady(int32_t timeoutMs)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    DS_ASSERT_OK(cluster_->StartWorkers());
    DS_ASSERT_OK(cluster_->WaitUntilClusterReadyOrTimeout(30));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    InitClients(timeoutMs);
}

void STCClientDistMasterTest::ShutdownEraseRocksDBAndRestartNodes()
{
    for (size_t i = 0; i < 3ul; ++i) {
        DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(i));
        auto dir = "/worker" + std::to_string(i) + "/rocksdb";
        DS_ASSERT_OK(RemoveAll(cluster_->GetRootDir() + dir));
        DS_ASSERT_OK(cluster_->StartNode(WORKER, i, " -client_reconnect_wait_s=1"));
    }
    for (size_t i = 0; i < 3ul; ++i) {
        Status rc = cluster_->WaitNodeReady(WORKER, i);
        if (rc.IsError()) {
            kill(cluster_->GetWorkerPid(0), SIGKILL);
            kill(cluster_->GetWorkerPid(1), SIGKILL);
            kill(cluster_->GetWorkerPid(2), SIGKILL);
        }
        DS_ASSERT_OK(rc);
    }
    const int sleepTime = 1000;  // 1s;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
}

void STCClientDistMasterTest::InitClients(int32_t timeoutMs)
{
    InitTestKVClient(0, client_, timeoutMs);
    InitTestKVClient(1, client1_, timeoutMs);
    InitTestKVClient(2, client2_, timeoutMs);
}

void STCClientDistMasterTest::GetWorkerUuids()
{
    std::string value;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
    HashRingPb ring;
    ring.ParseFromString(value);
    for (auto worker : ring.workers()) {
        HostPort workerAddr;
        DS_ASSERT_OK(workerAddr.ParseString(worker.first));
        uuidMap_.emplace(std::move(workerAddr), worker.second.worker_uuid());
    }
}

TEST_F(STCClientDistMasterTest, TestPrimaryKey)
{
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKey();
    std::string value = "value1";
    ASSERT_EQ(client_->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    ASSERT_EQ(client_->Del(key), Status::OK());
}

TEST_F(STCClientDistMasterTest, TestSetValue)
{
    StartClustersAndWaitReady();
    std::string value = "value1";
    auto key = client_->Set(value);
    std::string valueGet;
    ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    ASSERT_EQ(client_->Del(key), Status::OK());
}

TEST_F(STCClientDistMasterTest, DISABLED_TestHashInitFailed)
{
    StartClustersAndWaitReady();
    std::string value = "value1";
    auto key = GetStringUuid();
    DS_ASSERT_NOT_OK(client_->Set(key, value));
    DS_ASSERT_NOT_OK(client1_->Set(key, value));
    DS_ASSERT_NOT_OK(client2_->Set(key, value));
}

TEST_F(STCClientDistMasterTest, TestHashKey)
{
    StartClustersAndWaitReady();
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    std::string value = "value1";
    for (int i = 0; i < 10; i++) {
        auto key = GetStringUuid();
        DS_ASSERT_OK(client->Set(key, value));
        std::string valueGet;
        std::string valueGet1;
        std::string valueGet2;
        ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
        ASSERT_EQ(client1_->Get(key, valueGet1), Status::OK());
        ASSERT_EQ(client2_->Get(key, valueGet2), Status::OK());
        ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
        ASSERT_EQ(client->Del(key), Status::OK());
    }
}

TEST_F(STCClientDistMasterTest, DISABLED_LEVEL1_TestRestartWorkerWithError)
{
    StartClustersAndWaitReady();

    std::string key1;
    std::string key2;
    (void)client_->GenerateKey("", key1);
    (void)client_->GenerateKey("", key2);
    DS_ASSERT_OK(client1_->Set(key1, "val"));
    DS_ASSERT_OK(client2_->Set(key2, "val"));

    // let w0 failed on w1
    cluster_->ShutdownNode(WORKER, 0);
    int sleep = 5;  // > node_dead_timeout_s - node_timeout_s
    std::this_thread::sleep_for(std::chrono::seconds(sleep));
    // w1 can not send meta to w0
    cluster_->SetInjectAction(WORKER, 1, "WorkerRemote.PushMetadataToMaster", "return(K_RPC_UNAVAILABLE)");
    // restart w0
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    // check if w1 can get new data from w0
    std::string key;
    (void)client2_->GenerateKey("", key);
    DS_ASSERT_OK(client_->Set(key, "val"));
    std::string outVal;
    DS_ASSERT_OK(client1_->Get(key, outVal));
    DS_ASSERT_OK(client2_->Get(key, outVal));
}

TEST_F(STCClientDistMasterTest, DISABLED_LEVEL1_TestRestartWorkerWithMixOp)
{
    StartClustersAndWaitReady();
    std::string key = "Warrior131415926";
    std::string value = "The world is not black or white, heh; but a delicious shade of gray.";
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client_->Set(key, value, param));
    std::string value1;
    std::string value2;
    DS_ASSERT_OK(client1_->Get(key, value1));
    DS_ASSERT_OK(client2_->Get(key, value2));
    ASSERT_EQ(value, value1);
    ASSERT_EQ(value, value2);
    DS_ASSERT_OK(client1_->Del(key));
    DS_ASSERT_OK(client2_->Set(key, value, param));

    ShutdownEraseRocksDBAndRestartNodes();

    std::string value3;
    DS_ASSERT_OK(client_->Get(key, value3));
    ASSERT_EQ(value, value3);
}

TEST_F(STCClientDistMasterTest, DISABLED_LEVEL1_TestRestartWorkerWithSpecKeys)
{
    StartClustersAndWaitReady();
    std::string value = "The world is not black or white, heh; but a delicious shade of gray.";
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::string> keys;
    for (size_t i = 0; i < 10; ++i) {
        StringView view(value.c_str(), value.size());
        std::string key = client_->Set(view, param);
        ASSERT_NE(key, "");
        keys.emplace_back(std::move(key));
    }
    for (size_t i = 0; i < 10; ++i) {
        StringView view(value.c_str(), value.size());
        std::string key = client1_->Set(view, param);
        ASSERT_NE(key, "");
        keys.emplace_back(std::move(key));
    }
    for (size_t i = 0; i < 10; ++i) {
        StringView view(value.c_str(), value.size());
        std::string key = client2_->Set(view, param);
        ASSERT_NE(key, "");
        keys.emplace_back(std::move(key));
    }

    ShutdownEraseRocksDBAndRestartNodes();

    std::string getValue;
    for (const auto &key : keys) {
        DS_ASSERT_OK(client_->Get(key, getValue));
        ASSERT_EQ(getValue, value);
    }
}

TEST_F(STCClientDistMasterTest, TestMetaIsMoving)
{
    const int addIndex = 3;
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string value = "value1";
    std::string key = "wwwwwww";
    HostPort workerAddr(HOST_IP, GetFreePort());
    HostPort masterAddr;
    std::stringstream call;
    call << "10*return(" << workerAddr.ToString() << ")";
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, masterAddr));
    DS_ASSERT_OK(cluster_->AddNode(masterAddr, workerAddr.ToString(), GetFreePort()));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, addIndex));
    for (int i = 0; i < 3; i++) {
        cluster_->SetInjectAction(WORKER, i, "MurmurHash3.redirect", call.str());
        cluster_->SetInjectAction(WORKER, i, "meta.moving", "1*call()");
        cluster_->SetInjectAction(WORKER, i, "metas.moving", "1*call()");
    }
    DS_ASSERT_OK(client1_->Set(key, value));
    std::string valueGet;
    for (int i = 0; i < 3; i++) {
        cluster_->SetInjectAction(WORKER, i, "meta.moving", "1*call()");
        cluster_->SetInjectAction(WORKER, i, "metas.moving", "1*call()");
    }
    DS_ASSERT_OK(client2_->Get(key, valueGet));
    for (int i = 0; i < 3; i++) {
        cluster_->SetInjectAction(WORKER, i, "meta.moving", "1*call()");
        cluster_->SetInjectAction(WORKER, i, "metas.moving", "1*call()");
    }
    DS_ASSERT_OK(client1_->Del(key));
}

TEST_F(STCClientDistMasterTest, DISABLED_TestAddNodeSet)
{
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string value = "value1";
    std::vector<std::string> keys;
    HostPort workerAddr(HOST_IP, GetFreePort());
    HostPort masterAddr;
    for (int i = 0; i < 3; i++) {
        cluster_->SetInjectAction(WORKER, i, "add.node.redirect", "10000*return()");
    }
    cluster_->GetWorkerAddr(0, masterAddr);
    cluster_->AddNode(masterAddr, workerAddr.ToString(), GetFreePort());
    for (int i = 0; i < 500; i++) {
        auto key = "redirect_test_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value));
    }
}

TEST_F(STCClientDistMasterTest, DISABLED_TestAddNodeUpdate)
{
    FLAGS_v = 1;
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string value = "value1";
    std::string value1 = "value2";
    std::vector<std::string> keys;
    for (int i = 0; i < 3; i++) {
        cluster_->SetInjectAction(WORKER, i, "add.node.redirect", "10000*return()");
    }
    for (int i = 0; i < 1000; i++) {
        auto key = "redirect_test_" + std::to_string(i);
        keys.emplace_back(key);
        DS_ASSERT_OK(client_->Set(key, value));
    }
    HostPort workerAddr(HOST_IP, GetFreePort());
    HostPort masterAddr;
    cluster_->GetWorkerAddr(0, masterAddr);
    cluster_->AddNode(masterAddr, workerAddr.ToString(), GetFreePort());
    for (int i = 0; i < 1000; i++) {
        auto key = "redirect_test_" + std::to_string(i);
        keys.emplace_back(key);
        DS_ASSERT_OK(client_->Set(key, value));
    }
}

TEST_F(STCClientDistMasterTest, DISABLED_TestAddNodeGet)
{
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string value = "value1";
    std::vector<std::string> keys;
    HostPort workerAddr(HOST_IP, GetFreePort());
    HostPort masterAddr;
    for (int i = 0; i < 500; i++) {
        auto key = "redirect_test_" + std::to_string(i);
        keys.emplace_back(key);
    }
    for (int i = 0; i < 3; i++) {
        cluster_->SetInjectAction(WORKER, i, "add.node.redirect", "10000*return()");
    }
    std::function<void()> func = [this, &keys, &value]() {
        for (auto &key : keys) {
            std::string valueGet;
            std::string valueGet1;
            std::string valueGet2;
            ASSERT_EQ(client_->Get(key, valueGet, 50000), Status::OK());
            ASSERT_EQ(client1_->Get(key, valueGet1, 50000), Status::OK());
            ASSERT_EQ(client2_->Get(key, valueGet2, 50000), Status::OK());
            ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
        }
    };
    std::thread thread(func);
    cluster_->GetWorkerAddr(0, masterAddr);
    DS_ASSERT_OK(cluster_->AddNode(masterAddr, workerAddr.ToString(), GetFreePort()));
    for (auto &key : keys) {
        DS_ASSERT_OK(client_->Set(key, value));
    }
    thread.join();
}

TEST_F(STCClientDistMasterTest, TestRedirectSet)
{
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKeyWithOwner(0, uuidMap_);
    std::string value = "value1";
    std::string valueGet;
    HostPort redirectAddr;
    cluster_->GetWorkerAddr(1, redirectAddr);
    std::stringstream call;
    call << "call(" << redirectAddr.ToString() << ")";
    cluster_->SetInjectAction(WORKER, 0, "redirect.create.update.copy.meta", call.str());
    ASSERT_EQ(client_->Set(key, value), Status::OK());
    cluster_->SetInjectAction(WORKER, 0, "redirect.create.update.copy.meta", call.str());
    cluster_->SetInjectAction(WORKER, 0, "redirect.query.delete", call.str());
    DS_ASSERT_OK(client2_->Get(key, valueGet));
    cluster_->SetInjectAction(WORKER, 0, "redirect.query.delete", call.str());
    ASSERT_EQ(client2_->Del(key), Status::OK());
}

TEST_F(STCClientDistMasterTest, TestRedirectSetKeys)
{
    FLAGS_v = 1;
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::vector<std::string> keys;
    keys.emplace_back(ObjectKeyWithOwner(0, uuidMap_));
    keys.emplace_back(ObjectKeyWithOwner(1, uuidMap_));
    keys.emplace_back(ObjectKeyWithOwner(1, uuidMap_));

    std::string value = "value1";
    std::vector<std::string> valueGets;
    HostPort redirectAddr;
    cluster_->GetWorkerAddr(1, redirectAddr);
    std::stringstream call;
    call << "call(" << redirectAddr.ToString() << ")";
    cluster_->SetInjectAction(WORKER, 0, "redirect.create.update.copy.meta", call.str());
    for (auto key : keys) {
        ASSERT_EQ(client_->Set(key, value), Status::OK());
    }
    cluster_->SetInjectAction(WORKER, 0, "redirect.create.update.copy.meta", call.str());
    cluster_->SetInjectAction(WORKER, 0, "redirect.query.delete", call.str());
    DS_ASSERT_OK(client2_->Get(keys, valueGets));
    cluster_->SetInjectAction(WORKER, 0, "redirect.query.delete", call.str());
    std::vector<std::string> failedKeys;
    ASSERT_EQ(client2_->Del(keys, failedKeys), Status::OK());
}

TEST_F(STCClientDistMasterTest, TestGetAndDelSameKeyConcurrency)
{
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKeyWithOwner(0, uuidMap_);
    std::string val = RandomData().GetRandomString(1024ul);
    DS_ASSERT_OK(client_->Set(key, val));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.after_query_meta", "1*sleep(500)"));

    std::thread t1([this, &key, &val]() {
        std::string getVal;
        DS_EXPECT_OK(client1_->Get(key, getVal));
        EXPECT_EQ(getVal, val);
    });

    std::thread t2([this, &key]() {
        sleep(1);
        DS_EXPECT_OK(client_->Del(key));
    });

    t1.join();
    t2.join();

    std::string getVal;
    ASSERT_EQ(client_->Get(key, getVal).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(STCClientDistMasterTest, TestMockRemoteGetConcurrency)
{
    LOG(INFO) << "Test multi-worker remote get a key concurrency";
    // During the remote get operation, the location is updated when the QueryMeta request is sent tp master. However,
    // worker has no data at this time and still getting. There is a situation that can cause a potential deadlock:
    // 1. w0 set the key.
    // 2. w1 and w2 get the key concurrently.
    // 3. w0 tell w1 the location is w2, and tell w2 the location is w1.
    // 4. In present implementation, deadlock happen.
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKeyWithOwner(0, uuidMap_);
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);
    DS_ASSERT_OK(client_->Set(key, val));
    std::atomic<int> succCount{ 0 };

    HostPort w1Addr;
    HostPort w2Addr;
    cluster_->GetWorkerAddr(1, w1Addr);
    cluster_->GetWorkerAddr(2, w2Addr);
    Timer timer;
    std::thread t1([this, &key, &val, &w2Addr, &succCount]() {
        std::string getVal;
        std::stringstream call;
        call << "return(" << w2Addr.ToString() << ")";
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.select_location", call.str()));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.GetObjectFromAnywhere", "sleep(1500)"));
        DS_ASSERT_OK(client1_->Get(key, getVal));
        ASSERT_EQ(getVal, val);
        succCount += 1;
    });

    std::thread t2([this, &key, &val, &w1Addr, &succCount]() {
        sleep(1);
        std::string getVal;
        std::stringstream call;
        call << "return(" << w1Addr.ToString() << ")";
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.select_location", call.str()));
        DS_ASSERT_OK(client2_->Get(key, getVal));
        ASSERT_EQ(getVal, val);
        succCount += 1;
    });

    t1.join();
    t2.join();

    ASSERT_EQ(succCount, 2);
    ASSERT_LE(timer.ElapsedMilliSecond(), 10'000);
}

TEST_F(STCClientDistMasterTest, TestLocationConcurrencyScenario)
{
    LOG(INFO) << "Test remote get add and remove location concurrency";
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKeyWithOwner(0, uuidMap_);
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);
    DS_ASSERT_OK(client_->Set(key, val));

    // Inject the faults let the first remote get failed.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.remote_get_failed", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.remove_location", "1*return(K_RPC_UNAVAILABLE)"));
    std::string getVal;
    DS_ASSERT_NOT_OK(client1_->Get(key, getVal));

    HostPort w1Addr;
    cluster_->GetWorkerAddr(1, w1Addr);
    std::stringstream call;
    call << "return(" << w1Addr.ToString() << ")";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.select_location", call.str()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.remove_meta", "1*sleep(2000)"));
    std::thread t1([this, key, w1Addr]() {
        std::string getVal;
        DS_ASSERT_OK(client2_->Get(key, getVal));
    });

    std::thread t2([this, key]() {
        std::string getVal;
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.before_query_meta", "1*sleep(1000)"));
        DS_ASSERT_OK(client1_->Get(key, getVal));
    });

    t1.join();
    t2.join();

    DS_ASSERT_OK(client_->Del(key));
    ASSERT_EQ(client1_->Get(key, getVal).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(STCClientDistMasterTest, TestRemoteGetSameObjectsConcurrently)
{
    LOG(INFO) << "Test remote get same objects concurrently";
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::vector<std::string> objectKeys{ "Zed",  "Vi",      "Morgana",  "AShe",   "Aatrox", "Annie",     "Akali",
                                        "Bard", "Camille", "Darius",   "Ekko",   "Garen",  "Gangplank", "Jax",
                                        "Jinx", "Jax",     "Kassadin", "Kennen", "Lillia" };
    std::string val = "Welcome to Wild Rift";

    std::vector<std::string> partOfIds1;
    std::vector<std::string> partOfIds2;
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        DS_ASSERT_OK(client_->Set(objectKeys[i], val));
        if (i <= objectKeys.size() / 2) {
            partOfIds1.emplace_back(objectKeys[i]);
        } else {
            partOfIds2.emplace_back(objectKeys[i]);
        }
    }

    size_t num = 20;
    std::vector<std::thread> threads(num);
    std::vector<std::string> absentIds1 = partOfIds1;
    std::vector<std::string> absentIds2;
    for (size_t i = 0; i < 2; ++i) {
        absentIds1.emplace_back("not_exist_id" + std::to_string(i));
        absentIds2.emplace_back("not_exist_id" + std::to_string(i));
    }

    for (size_t i = 0; i < num; ++i) {
        threads[i] = std::thread([this, i, &val, &objectKeys, &partOfIds1, &partOfIds2, &absentIds1, &absentIds2]() {
            std::shared_ptr<KVClient> client;
            if (i % 2 == 0) {
                client = client1_;
            } else {
                client = client2_;
            }
            std::vector<std::string> getObjects;
            if (i < 4) {
                getObjects = objectKeys;
            } else if (i < 8) {
                getObjects = partOfIds1;
            } else if (i < 12) {
                getObjects = partOfIds2;
            } else if (i < 16) {
                getObjects = absentIds1;
            } else {
                getObjects = absentIds2;
            }

            std::vector<std::string> getVals;
            DS_ASSERT_OK(client->Get(getObjects, getVals, 20'000));
            ASSERT_EQ(getVals.size(), getObjects.size());
            for (size_t k = 0; k < getObjects.size(); ++k) {
                ASSERT_EQ(getVals[k], val);
            }
        });
    }

    DS_ASSERT_OK(client1_->Set("not_exist_id0", val));
    DS_ASSERT_OK(client_->Set("not_exist_id1", val));
    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(STCClientDistMasterTest, TestRemoteGetSameSubscribeObjectsConcurrently)
{
    LOG(INFO) << "Test remote get and subscribe happen currently";
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::vector<std::string> objectKeys{ "Zed",  "Vi",      "Morgana",  "AShe",   "Aatrox", "Annie",     "Akali",
                                        "Bard", "Camille", "Darius",   "Ekko",   "Garen",  "Gangplank", "Jax",
                                        "Jinx", "Jax",     "Kassadin", "Kennen", "Lillia", "Lucian",    "Pyke" };
    std::string val = "Welcome to Wild Rift";
    DS_ASSERT_OK(client_->Set(objectKeys.back(), val));

    size_t num = 25;
    std::vector<std::thread> threads(num);
    for (size_t i = 0; i < num; ++i) {
        threads[i] = std::thread([this, &objectKeys, &val, i]() {
            if (i < 20) {
                std::vector<std::string> getVals;
                DS_ASSERT_OK(client1_->Get(objectKeys, getVals, 20'000));
                ASSERT_EQ(getVals.size(), objectKeys.size());
                for (size_t k = 0; k < objectKeys.size(); ++k) {
                    ASSERT_EQ(getVals[k], val);
                }
            } else {
                size_t index = i - 20;
                for (size_t k = index * 4; k < index * 4 + 4; ++k) {
                    if (index % 3 == 0) {
                        DS_ASSERT_OK(client_->Set(objectKeys[k], val));
                    } else if (index % 3 == 1) {
                        DS_ASSERT_OK(client1_->Set(objectKeys[k], val));
                    } else {
                        DS_ASSERT_OK(client2_->Set(objectKeys[k], val));
                    }
                }
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(STCClientDistMasterTest, DISABLED_TestRemoteGetConcurrentlyMeetsQueryMetaError)
{
    LOG(INFO) << "Test remote get same objects concurrently meets query meta rpc error";
    StartClustersAndWaitReady(5'000);
    GetWorkerUuids();
    std::shared_ptr<KVClient> client2;
    int32_t sleepMs = 1000;
    int32_t workerIndex = 2;
    InitTestKVClient(workerIndex, client2, sleepMs);
    std::vector<std::string> objectKeys{ "Zed",  "Vi",      "Morgana",  "AShe",   "Aatrox", "Annie",     "Akali",
                                        "Bard", "Camille", "Darius",   "Ekko",   "Garen",  "Gangplank", "Jax",
                                        "Jinx", "Jax",     "Kassadin", "Kennen", "Lillia", "Lucian",    "Pyke" };
    std::string val = "Welcome to Wild Rift";

    for (size_t i = 0; i < objectKeys.size(); ++i) {
        if (i % 2 == 0) {
            DS_ASSERT_OK(client_->Set(objectKeys[i], val));
        } else {
            DS_ASSERT_OK(client1_->Set(objectKeys[i], val));
        }
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "worker.remote_query_meta", "1*return(K_RPC_UNAVAILABLE)"));
    // Ensure that the RetryOnError function is executed only once.
    DS_ASSERT_OK(inject::Set("Get.RetryOnError.retry_on_error_after_func", "sleep(1000)"));
    size_t num = 20;
    std::atomic<int> succCount{ 0 };
    std::atomic<int> failCount{ 0 };
    std::vector<std::thread> threads(num);
    for (size_t i = 0; i < num; ++i) {
        threads[i] = std::thread([&client2, &objectKeys, &val, &succCount, &failCount]() {
            std::vector<std::string> getVals;
            Status rc = client2->Get(objectKeys, getVals, 0);
            if (rc.IsError()) {
                failCount.fetch_add(1);
                return;
            }
            succCount.fetch_add(1);
            ASSERT_EQ(getVals.size(), objectKeys.size());
            for (size_t k = 0; k < objectKeys.size(); ++k) {
                EXPECT_EQ(getVals[k], val);
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }

    EXPECT_EQ(succCount, 19);
    EXPECT_EQ(failCount, 1);
}

TEST_F(STCClientDistMasterTest, TestRemoteGetAndSetConcurrency)
{
    LOG(INFO) << "Test remote get and set concurrency cause cache invalid scenario";
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKeyWithOwner(0, uuidMap_);
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);

    DS_ASSERT_OK(client_->Set(key, val));
    HostPort worker1Addr;
    cluster_->GetWorkerAddr(1, worker1Addr);
    std::stringstream ss;
    ss << "1*call(" << worker1Addr.ToString() << ")";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.send_cache_invalid", ss.str()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.worker_worker_remote_get_failure", "1*sleep(1000)"));

    std::thread setThr([this, &key, &val]() {
        usleep(100'000);
        DS_ASSERT_OK(client2_->Set(key, val));
    });

    std::thread getThr([this, &key, &val]() {
        std::string getVal;
        DS_ASSERT_OK(client1_->Get(key, getVal));
        ASSERT_EQ(getVal, val);
    });

    setThr.join();
    getThr.join();
}

TEST_F(STCClientDistMasterTest, TestRemoteGetAndDelConcurrencyNotFoundScenario)
{
    LOG(INFO) << "Test remote get and del concurrency cause not found scenario";
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKeyWithOwner(0, uuidMap_);
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);

    DS_ASSERT_OK(client_->Set(key, val));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.worker_worker_remote_get_failure", "1*sleep(1000)"));

    std::thread delThr([this, &key]() {
        usleep(100'000);
        DS_ASSERT_OK(client2_->Del(key));
    });

    std::thread getThr([this, &key]() {
        std::string getVal;
        Timer timer;
        Status status = client1_->Get(key, getVal, 5000);
        EXPECT_EQ(status.GetCode(), StatusCode::K_NOT_FOUND);
        auto elsaped = timer.ElapsedMilliSecond();
        EXPECT_GE(elsaped, 4000.0);
        EXPECT_LE(elsaped, 6000.0);
    });

    delThr.join();
    getThr.join();
}

TEST_F(STCClientDistMasterTest, TestRemoteGetAndDelConcurrencyRemoteSetScenario)
{
    LOG(INFO) << "Test remote get and del concurrency cause not found scenario";
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKeyWithOwner(0, uuidMap_);
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);

    DS_ASSERT_OK(client_->Set(key, val));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.worker_worker_remote_get_failure", "1*sleep(1000)"));

    std::thread delThr([this, &key]() {
        usleep(100'000);
        DS_ASSERT_OK(client2_->Del(key));
    });

    std::thread getThr([this, &key, &val]() {
        std::string getVal;
        DS_ASSERT_OK(client1_->Get(key, getVal, 5000));
        ASSERT_EQ(getVal, val);
    });

    delThr.join();
    DS_ASSERT_OK(client2_->Set(key, val));
    getThr.join();
}

TEST_F(STCClientDistMasterTest, TestRemoteGetAndDelConcurrencyLocalSetScenario)
{
    LOG(INFO) << "Test remote get and del concurrency cause not found scenario";
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::string key = ObjectKeyWithOwner(0, uuidMap_);
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);

    DS_ASSERT_OK(client_->Set(key, val));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.worker_worker_remote_get_failure", "1*sleep(1000)"));

    std::thread delThr([this, &key]() {
        usleep(100'000);
        DS_ASSERT_OK(client2_->Del(key));
    });

    std::thread getThr([this, &key, &val]() {
        std::string getVal;
        DS_ASSERT_OK(client1_->Get(key, getVal, 5000));
        ASSERT_EQ(getVal, val);
    });

    delThr.join();
    DS_ASSERT_OK(client_->Set(key, val));
    getThr.join();
}

TEST_F(STCClientDistMasterTest, TestSetAndDelOneKeyConcurrency2)
{
    LOG(INFO) << "Test set and del the same key in different workers";
    StartClustersAndWaitReady();
    std::string objectKey = client_->Set("Malaysia is a beautiful place");
    ASSERT_FALSE(objectKey.empty());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.before_delete_metadata", "1*sleep(2000)"));
    std::thread t1([this, &objectKey]() { DS_ASSERT_OK(client1_->Del(objectKey)); });

    std::thread t2([this, &objectKey]() {
        sleep(1);
        DS_ASSERT_OK(client_->Set(objectKey, "Malaysia is a beautiful place"));
    });

    t1.join();
    t2.join();

    DS_ASSERT_OK(client_->Set(objectKey, "Malaysia is a beautiful place"));
}

TEST_F(STCClientDistMasterTest, TestMutiSetGet)
{
    StartClustersAndWaitReady();
    GetWorkerUuids();
    std::vector<std::string> keys;
    std::string value = "value1";
    for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
            keys.emplace_back(ObjectKeyWithOwner(0, uuidMap_));
        } else {
            keys.emplace_back(ObjectKeyWithOwner(1, uuidMap_));
        }
    }
    ThreadPool threadPool(2);
    auto fut = threadPool.Submit([this, &keys, &value]() {
        for (int i = 0; i < 5; i++) {
            DS_ASSERT_OK(client_->Set(keys[i], value));
        }
    });

    auto fut2 = threadPool.Submit([this, &keys, &value]() {
        for (int i = 5; i < 10; ++i) {
            DS_ASSERT_OK(client2_->Set(keys[i], value));
        }
    });

    fut.get();
    fut2.get();

    for (int i = 0; i < 10; i++) {
        std::string valueGet1;
        std::string valueGet2;
        std::string valueGet3;
        DS_ASSERT_OK(client_->Get(keys[i], valueGet1));
        DS_ASSERT_OK(client1_->Get(keys[i], valueGet2));
        DS_ASSERT_OK(client2_->Get(keys[i], valueGet3));
        ASSERT_EQ(value, std::string(valueGet1.data(), valueGet1.size()));
        ASSERT_EQ(value, std::string(valueGet2.data(), valueGet2.size()));
        ASSERT_EQ(value, std::string(valueGet3.data(), valueGet3.size()));
    }
}

TEST_F(STCClientDistMasterTest, DISABLED_LEVEL1_TestSetSpecialKey)
{
    StartClustersAndWaitReady();
    std::vector<std::string> keys = { "/home/Freljord/AShe/Tryndamere/The_Frost_Archer.png",
                                      "/home/Freljord;AShe/Tryndamere;The_Frost_Archer;png",
                                      "////;;;;/;/;/;/;/;////;;;;" };
    std::string value = "Take a good look. It's the last you're going to get.";
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (const auto &key : keys) {
        DS_ASSERT_OK(client_->Set(key, value, param));
    }

    ShutdownEraseRocksDBAndRestartNodes();

    std::string getValue;
    for (const auto &key : keys) {
        DS_ASSERT_OK(client1_->Get(key, getValue));
        ASSERT_EQ(getValue, value);
    }
}

TEST_F(STCClientDistMasterTest, TestSetAndDelOneKeyConcurrency)
{
    StartClustersAndWaitReady();
    std::string objectKey = client_->Set("Malaysia is a beautiful place");
    ASSERT_FALSE(objectKey.empty());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.before_delete_metadata", "1*sleep(2000)"));
    std::thread t1([this, &objectKey]() { DS_ASSERT_OK(client1_->Del(objectKey)); });

    std::thread t2([this, &objectKey]() {
        sleep(1);
        DS_ASSERT_OK(client1_->Set(objectKey, "Malaysia is a beautiful place"));
    });

    t1.join();
    t2.join();

    std::string val;
    DS_ASSERT_OK(client2_->Get(objectKey, val));
    ASSERT_EQ(val, "Malaysia is a beautiful place");
}

TEST_F(STCClientDistMasterTest, LEVEL2_TestSyncToEtcdWithMultiThreads)
{
    StartClustersAndWaitReady();

    uint32_t threadSize = 4;
    uint32_t objPerThr = 100;
    std::vector<std::thread> setThreads(threadSize);
    std::vector<std::vector<std::string>> keyWithWorkerIds(threadSize);
    for (auto &vec : keyWithWorkerIds) {
        vec.resize(objPerThr);
    }
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::string data = "Hello World";
    std::string data1 = "Hello World2";
    for (uint32_t i = 0; i < threadSize; ++i) {
        setThreads[i] = std::thread([this, i, threadSize, objPerThr, param, &keyWithWorkerIds, data, data1]() {
            for (uint32_t k = 0; k < objPerThr; ++k) {
                if (i > threadSize / 2) {
                    std::string key = "Ginnungagap" + std::to_string(i) + "_" + std::to_string(k);
                    DS_ASSERT_OK(client_->Set(key, data, param));
                    DS_ASSERT_OK(client_->Set(key, data1, param));
                } else {
                    std::string key = client_->Set(data, param);
                    ASSERT_FALSE(key.empty());
                    keyWithWorkerIds[i][k] = key;
                    DS_ASSERT_OK(client_->Set(key, data1, param));
                }
            }
        });
    }

    for (auto &t : setThreads) {
        t.join();
    }

    ShutdownEraseRocksDBAndRestartNodes();

    sleep(10);

    std::vector<std::thread> getThreads(threadSize);
    for (uint32_t i = 0; i < threadSize; ++i) {
        getThreads[i] = std::thread([this, i, threadSize, objPerThr, keyWithWorkerIds, data1]() {
            for (uint32_t k = 0; k < objPerThr; ++k) {
                std::string key = (i > threadSize / 2) ? "Ginnungagap" + std::to_string(i) + "_" + std::to_string(k)
                                                       : keyWithWorkerIds[i][k];
                std::string getData;
                DS_ASSERT_OK(client2_->Get(key, getData));
                ASSERT_EQ(getData, data1);
            }
        });
    }

    for (auto &t : getThreads) {
        t.join();
    }
}

TEST_F(STCClientDistMasterTest, EXCLUSIVE_LEVEL2_TestASyncToEtcdWithMultiThreads)
{
    StartClustersAndWaitReady();

    uint32_t threadSize = 5;
    uint32_t objPerThr = 50;
    std::vector<std::thread> setThreads(threadSize);
    std::vector<std::vector<std::string>> keyWithWorkerIds(threadSize);
    for (auto &vec : keyWithWorkerIds) {
        vec.resize(objPerThr);
    }
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    std::string data = "Hello World";
    std::string data1 = "Hello World2";
    for (uint32_t i = 0; i < threadSize; ++i) {
        setThreads[i] = std::thread([this, i, threadSize, objPerThr, param, &keyWithWorkerIds, data, data1]() {
            for (uint32_t k = 0; k < objPerThr; ++k) {
                if (i > threadSize / 2) {
                    std::string key = "Ginnungagap" + std::to_string(i) + "_" + std::to_string(k);
                    DS_ASSERT_OK(client_->Set(key, data, param));
                    DS_ASSERT_OK(client_->Set(key, data1, param));
                } else {
                    std::string key = client_->Set(data, param);
                    ASSERT_FALSE(key.empty());
                    keyWithWorkerIds[i][k] = key;
                    DS_ASSERT_OK(client_->Set(key, data1, param));
                }
            }
        });
    }

    for (auto &t : setThreads) {
        t.join();
    }

    sleep(10);

    ShutdownEraseRocksDBAndRestartNodes();

    sleep(10);
    std::atomic<uint32_t> failedCount{ 0 };
    std::vector<std::thread> getThreads(threadSize);
    for (uint32_t i = 0; i < threadSize; ++i) {
        getThreads[i] = std::thread([this, i, threadSize, objPerThr, keyWithWorkerIds, data, data1, &failedCount]() {
            for (uint32_t k = 0; k < objPerThr; ++k) {
                std::string key = (i > threadSize / 2) ? "Ginnungagap" + std::to_string(i) + "_" + std::to_string(k)
                                                       : keyWithWorkerIds[i][k];
                std::string getData;
                Status rc = client2_->Get(key, getData);
                std::string msg = rc.GetMsg();
                ASSERT_TRUE(rc.IsOk() || rc.GetCode() == StatusCode::K_NOT_FOUND
                            || (rc.GetCode() == StatusCode::K_RUNTIME_ERROR))
                    << rc.ToString();
                if (rc.IsError()) {
                    failedCount.fetch_add(1);
                }
                if (rc.IsOk()) {
                    ASSERT_TRUE(getData == data1 || getData == data) << getData;
                }
            }
        });
    }

    for (auto &t : getThreads) {
        t.join();
    }
    LOG(INFO) << "Failed count: " << failedCount;
}

TEST_F(STCClientDistMasterTest, DISABLED_LEVEL1_TestBoundaryValue)
{
    StartClustersAndWaitReady();
    std::vector<std::string> keys = { "a_key_hash_to_4294967291", "a_key_hash_to_" + std::to_string(UINT32_MAX) };
    std::string value = "Take a good look. It's the last you're going to get.";

    for (size_t i = 0; i < 3ul; ++i) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "MurmurHash3", "10*return()"));
    }

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (const auto &key : keys) {
        DS_ASSERT_OK(client_->Set(key, value, param));
    }

    ShutdownEraseRocksDBAndRestartNodes();

    for (size_t i = 0; i < 3ul; ++i) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "MurmurHash3", "10*return()"));
    }

    std::string getValue;
    for (const auto &key : keys) {
        DS_ASSERT_OK(client1_->Get(key, getValue));
        ASSERT_EQ(getValue, value);
    }
}

TEST_F(STCClientDistMasterTest, LEVEL1_ProcessRequestBeforeReady)
{
    ExternalCluster *externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(inject::Clear("ListenWorker.CheckHeartbeat.heartbeat_interval_ms"));
    DS_ASSERT_OK(externalCluster_->StartForkWorkerProcess());

    StartClustersAndWaitReady(5000);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    auto key = client_->Set("anyValue", param);

    std::atomic<bool> quit(false);
    auto keepSending = [&](KVClient *client) {
        while (!quit) {
            (void) client->GenerateKey("", key);
            if (key.empty()) {
                LOG(INFO) << "tolerable error, continue. empty key.";
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            Status status = client->Set(key, "value");
            if (status.GetCode() == K_RPC_UNAVAILABLE || status.GetCode() == K_NOT_READY) {
                LOG(INFO) << "tolerable error, continue. code=" << status.GetCode();
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            std::string errMsg, checkStr = "";
            if (status.GetCode() == K_RUNTIME_ERROR) {
                errMsg = status.ToString();
                checkStr = "service/handler is not registered";
            }
            if (errMsg.find(checkStr) != std::string::npos) {
                // Two if to avoid code_check huge depth and tolerable error.(server may not start yet)
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            DS_ASSERT_OK(status);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    };

    std::thread sendOnW0(keepSending, client_.get());
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    Raii end([&] {
        quit = true;
        sendOnW0.join();
    });

    DS_ASSERT_OK(externalCluster_->StartWorkerByForkProcess(0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0, 20));
    sleep(1);
}

TEST_F(STCClientDistMasterTest, TestGetReqAndObjectLock)
{
    const int timeout = 3000;
    StartClustersAndWaitReady(timeout);
    std::string key = "key0001";
    std::string val = "v";
    std::vector<std::thread> threads;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.AddEntryToGetResponse", "call()"));
    std::atomic<bool> stop{ false };
    threads.emplace_back([this, &stop, &key, &val] {
        while (!stop) {
            (void)client1_->Set(key, val);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    const int getThreadCnt = 4;
    for (int n = 0; n < getThreadCnt; n++) {
        threads.emplace_back([this, &stop, timeout, &key] {
            while (!stop) {
                std::string v1;
                Timer timer;
                (void)client_->Get(key, v1);
                ASSERT_LE(timer.ElapsedMilliSecond(), timeout);
            }
        });
    }
    const int testTime = 10000;  // 10s
    std::this_thread::sleep_for(std::chrono::milliseconds(testTime));
    stop = true;
    for (auto &t : threads) {
        t.join();
    }
}

class STCClientDistMasterDfxTest : public STCClientDistMasterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        STCClientDistMasterTest::SetClusterSetupOptions(opts);
        opts.disableRocksDB = false;
    }

protected:
};

TEST_F(STCClientDistMasterDfxTest, LEVEL1_TestReStartWorker)
{
    StartClustersAndWaitReady();
    std::string value = "value1";
    DS_ASSERT_OK(client_->Set("userDataKey", value));  // murmurhash(userDataKey) = 2350663102, fall into worker0
    std::string valueGet;
    DS_ASSERT_OK(client1_->Get("userDataKey", valueGet));
    cluster_->ShutdownNode(WORKER, 0);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    std::string valueGet1;
    DS_ASSERT_OK(client_->Get("userDataKey", valueGet1));
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    ASSERT_EQ(client_->Del("userDataKey"), Status::OK());
}

TEST_F(STCClientDistMasterDfxTest, LEVEL1_TestRestartWorkerAndRecoveryFromEtcd)
{
    StartClustersAndWaitReady();
    std::string value = "The world is not black or white, heh; but a delicious shade of gray.";
    for (size_t i = 0; i < 20; ++i) {
        std::string key = "Camille" + std::to_string(i);
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        DS_ASSERT_OK(client_->Set(key, value, param));
    }

    ShutdownEraseRocksDBAndRestartNodes();

    for (size_t i = 0; i < 20; ++i) {
        std::string key = "Camille" + std::to_string(i);
        DS_ASSERT_OK(client_->Get(key, value));
    }
}

TEST_F(STCClientDistMasterDfxTest, LEVEL2_RestartClusterNotReady)
{
    StartClustersAndWaitReady();
    for (size_t i = 0; i < 3ul; ++i) {
        DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, i));
    }

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    sleep(2);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    sleep(2);

    // cannot accept any request if nodes not complete.
    for (auto i = 0; i < 10; i++) {
        ASSERT_EQ(client_->Set(GetStringUuid(), "value").GetCode(), K_NOT_READY);
    }

    // after ready, request is ok.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    for (size_t i = 0; i < 3ul; ++i) {
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
    }
    for (auto i = 0; i < 10; i++) {
        ASSERT_EQ(client_->Set(GetStringUuid(), "value").GetCode(), K_OK);
    }
}

class STCClientDistMasterOOMTest : public STCClientDistMasterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.numOBS = 1;
        opts.workerGflagParams = " -v=1 -shared_memory_size_mb=64";
        opts.enableDistributedMaster = "true";
        opts.waitWorkerReady = false;
    }
};

TEST_F(STCClientDistMasterOOMTest, LEVEL2_TestRemoteGetAndSetConcurrencyMeetsOOMScenario)
{
    LOG(INFO) << "Test remote get and set concurrency cause cache invalid scenario and meets OOM scenario, but part of "
                 "objects success";
    int timeoutMs = 30'000;
    StartClustersAndWaitReady(timeoutMs);
    GetWorkerUuids();

    std::string worker0key = "RUOK;" + GetWorkerUuid(0, uuidMap_);
    std::string worker1key = "ALICE;" + GetWorkerUuid(0, uuidMap_);
    std::string worker2key = "BANK;" + GetWorkerUuid(2, uuidMap_);
    std::string worker2key1 = "CATHY;" + GetWorkerUuid(2, uuidMap_);
    std::string val0 = RandomData().GetRandomString(4 * 1024ul * 1024ul);
    std::string val1 = RandomData().GetRandomString(8 * 1024ul * 1024ul);
    std::string val2 = RandomData().GetRandomString(27 * 1024ul * 1024ul);

    DS_ASSERT_OK(client_->Set(worker0key, val1));
    DS_ASSERT_OK(client1_->Set(worker1key, val1));
    DS_ASSERT_OK(client2_->Set(worker2key, val2));
    DS_ASSERT_OK(client2_->Set(worker2key1, val2));

    HostPort worker1Addr;
    cluster_->GetWorkerAddr(1, worker1Addr);
    std::stringstream ss;
    ss << "1*call(" << worker1Addr.ToString() << ")";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.send_cache_invalid", ss.str()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.worker_worker_remote_get_failure", "1*sleep(1000)"));

    std::thread setThr([this, &worker0key]() {
        std::string newVal = "You are strong, but I am stronger.";
        usleep(100'000);
        DS_ASSERT_OK(client2_->Set(worker0key, newVal));
    });

    std::thread getThr([this, &worker0key, &worker2key, &worker2key1, &val2]() {
        std::vector<std::string> getVals;
        DS_ASSERT_OK(client1_->Get({ worker0key, worker2key, worker2key1 }, getVals));
        ASSERT_EQ(getVals.size(), size_t(3));
        ASSERT_TRUE(getVals[0].empty());
        ASSERT_TRUE(getVals[1].empty() ^ getVals[2].empty());
        if (getVals[1].empty()) {
            ASSERT_EQ(getVals[2], val2);
        } else {
            ASSERT_EQ(getVals[1], val2);
        }
    });

    setThr.join();
    getThr.join();
}
}  // namespace st
}  // namespace datasystem
