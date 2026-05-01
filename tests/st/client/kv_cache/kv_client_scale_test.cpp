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
 * Description: State client scale tests.
 */

#include <unistd.h>
#include <chrono>
#include <csignal>
#include <thread>

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "client/kv_cache/kv_client_scale_common.h"
#include "common.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_string(cluster_name);

namespace datasystem {
namespace st {
class STCScaleUpTest : public STCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 5;
        opts.numOBS = 1;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3 ";
        opts.waitWorkerReady = false;
        opts.disableRocksDB = false;
    }

    void TriggerRedirect(uint32_t workerIdxRouteFrom, uint32_t workerIdxScaleUp, std::function<void()> funcNeedRedirect)
    {
        // Redirection occurs when the hash ring's version of the worker is lower than that of the master.
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIdxRouteFrom, "HashRing.UpdateRing.sleep", "sleep(2000)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(
            WORKER, workerIdxRouteFrom,
            "EtcdClusterManager.GroupObjKeysByMasterHostPortWithStatus.PreFetchDestAddrFromAnywhere", "sleep(100)"));
        std::thread trd(funcNeedRedirect);
        StartWorkerAndWaitReady({ workerIdxScaleUp });
        trd.join();
    }
};

TEST_F(STCScaleUpTest, TestRedirectExpire)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);

    std::vector<std::string> keys;
    std::string keyPrefix = "test_expire_redirect";
    std::string value = "any_val";
    size_t keyNum = 50;
    for (size_t i = 0; i < keyNum; ++i) {
        keys.emplace_back(keyPrefix + std::to_string(i));
        DS_ASSERT_OK(client_->Set(keys.back(), value));
    }

    auto expireFunc = [this, &keys]() {
        uint64_t ttlSecond = 1;
        std::vector<std::string> failedKeys;
        DS_ASSERT_OK(client1_->Expire(keys, ttlSecond, failedKeys));
    };
    TriggerRedirect(1, 2, std::move(expireFunc));  // 2 is worker index.

    std::this_thread::sleep_for(
        std::chrono::milliseconds(2500));  // Sleep for 2500 milliseconds, waiting for expiration.
    std::vector<std::string> getValue;
    auto rc = client_->Get(keys, getValue);
    ASSERT_EQ(rc.GetCode(), K_NOT_FOUND);
}

TEST_F(STCScaleUpTest, TestEtcdFailedWhenWorkerTimeout)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    auto key = client_->GenerateKey();
    SetParam param;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    DS_ASSERT_OK(client1_->Set(key, "aaaaaaa", param));
    client1_.reset();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "PutToEtcdStore.failed", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(2); // wait worker timeout 2 s
    client_.reset();
    StartWorkerAndWaitReady({ 1 });
    RestartWorkerAndWaitReady({ 0 });
    InitTestKVClient(0, client_);
    std::string val;
    DS_ASSERT_OK(client_->Get(key, val));
}

TEST_F(STCScaleUpTest, DISABLED_TestRedirectSubscribe)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);

    std::vector<std::string> keys;
    std::string keyPrefix = "test_sub_redirect";
    size_t keyNum = 5000;
    for (size_t i = 0; i < keyNum; ++i) {
        keys.emplace_back(keyPrefix + std::to_string(i));
    }
    std::string value = "any_val";

    std::vector<std::thread> trds;

    auto subFunc = [this, &keys, &value, keyNum]() {
        std::vector<std::string> valsToGet;
        int subTimeoutMs = 60'000;
        DS_ASSERT_OK(client_->Get(keys, valsToGet, subTimeoutMs));
        ASSERT_EQ(valsToGet.size(), keyNum);
        std::for_each(valsToGet.begin(), valsToGet.end(), [&value](const std::string &s) { ASSERT_EQ(s, value); });
    };
    trds.emplace_back(subFunc);

    auto pubFunc = [this, &keys, &value]() {
        const int postSubTimeMs = 100;
        std::this_thread::sleep_for(std::chrono::milliseconds(postSubTimeMs));
        std::for_each(keys.begin(), keys.end(),
                      [this, &value](const auto &key) { DS_ASSERT_OK(client1_->Set(key, value)); });
    };
    trds.emplace_back(pubFunc);

    // Redirection occurs only when the hash ring's version of the worker is lower than that of the master.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "HashRing.UpdateRing.sleep", "sleep(2000)"));
    StartWorkerAndWaitReady({ 2 });  // 2 is worker index.

    for (auto &trd : trds) {
        trd.join();
    }
}

TEST_F(STCScaleUpTest, ScaleUpTTl)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    std::string value = "Level0_Ttl_001";
    uint32_t ttl = 6;  // should be greater than addNodeWaitTime_

    std::vector<std::string> keys;
    int objnum = 10;
    // set before scaleup with l2 cache
    {
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = ttl };
        std::string key_name = "Level0_Ttl_001";
        for (int i = 0; i < objnum; i++) {
            std::string key = key_name + std::to_string(i);
            keys.emplace_back(key);
            DS_ASSERT_OK(client_->Set(key, value, param));
        }
    }
    // set before scaleup with no l2 cache
    {
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = ttl };
        std::string key_name = "Level0_Ttl_002";
        for (int i = 0; i < objnum; i++) {
            std::string key = key_name + std::to_string(i);
            keys.emplace_back(key);
            DS_ASSERT_OK(client_->Set(key, value, param));
        }
    }

    // wait for scale up finish, and keys' expiration.
    StartWorkerAndWaitReady({ 2 });
    sleep(ttl + 1);

    // keys not found in worker and etcd
    std::string valueGet;
    std::stringstream table1;
    table1 << ETCD_META_TABLE_PREFIX << "/" << ETCD_HASH_SUFFIX << "/";
    auto cmd = "etcdctl --endpoints " + FLAGS_etcd_address + " get --prefix /";
    auto m = system(cmd.c_str());
    LOG(INFO) << cmd << "-----" << m;
    for (auto &key : keys) {
        DS_ASSERT_NOT_OK(client1_->Get(key, valueGet));
        RangeSearchResult res;
        DS_ASSERT_NOT_OK(db_->RawGet(table1.str() + key, res));
    }
}

TEST_F(STCScaleUpTest, ScaleUpQuerySize)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    std::string value = "Level0_Ttl_001";
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::string key_name = "Level0_Ttl_001";
    DS_ASSERT_OK(client_->Set(key_name, value, param));

    // wait for scale up finish, and keys' expiration.
    StartWorkerAndWaitReady({ 2 });

    // keys not found in worker and etcd
    std::vector<uint64_t> outSizes;
    DS_ASSERT_OK(client1_->QuerySize({ key_name }, outSizes));
    ASSERT_EQ(outSizes.size(), 1);
    ASSERT_EQ(outSizes[0], value.length());
}

TEST_F(STCScaleUpTest, DISABLED_ScaleUpNodeReady)
{
    StartWorkerAndWaitReady({ 0, 1 });

    // set migration task duration
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "HashRing.SubmitScaleUpTask.skip", "return(5)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "HashRing.SubmitScaleUpTask.skip", "return(5)"));

    StartWorkerAndWaitReady({ 2 });

    // the master feature of newly-added worker is normal after ready
    InitTestKVClient(2, client2_);
    for (auto i = 0; i < 10; i++) {
        DS_ASSERT_OK(client2_->Set(GetStringUuid(), GenPartRandomString()));
        ASSERT_TRUE(!client2_->Set(GenPartRandomString()).empty());
    }
    // the master feature of newly-added worker is normal during migration
    sleep(5);
    for (auto i = 0; i < 10; i++) {
        DS_ASSERT_OK(client2_->Set(GetStringUuid(), GenPartRandomString()));
        ASSERT_TRUE(!client2_->Set(GenPartRandomString()).empty());
    }
}

TEST_F(STCScaleUpTest, InsertDuringNodeAddition)
{
    std::string value{ "anyValue" };
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(2, client2_);

    // for newly inserted data, it is expected to write to new node directly via redirect request, so it should be
    // workable with migration disabled
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "HashRing.SubmitScaleUpTask.skip", "return(2)"));

    StartWorkerAndWaitReady({ 3 });
    sleep(3);  // scale-up begin
    for (int i = 0; i < 500; i++) {
        auto key = "redirect_test_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value));
    }
    sleep(2);  // scale-up should be finished.

    std::string outValue;
    for (int i = 0; i < 500; i++) {
        auto key = "redirect_test_" + std::to_string(i);
        DS_ASSERT_OK(client2_->Get(key, outValue));
        ASSERT_EQ(value, outValue);
        DS_ASSERT_OK(client_->Get(key, outValue));
        ASSERT_EQ(value, outValue);
    }
}

TEST_F(STCScaleUpTest, TestTtlMigrate)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    std::string value{ "anyValue" };
    SetParam param;
    param = { .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 4 };
    for (auto i = 0; i < 50; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value, param));
    }
    StartWorkerAndWaitReady({ 2, 3 });
    sleep(8);
    InitTestKVClient(2, client2_);
    std::string outValue;
    for (int i = 0; i < 50; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_NOT_OK(client2_->Get(key, outValue));
    }
}

TEST_F(STCScaleUpTest, UpdateDuringNodeAddition)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);

    std::string value{ "anyValue" };
    for (auto i = 0; i < 100; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value));
    }

    StartWorkerAndWaitReady({ 2, 3 });
    std::shared_ptr<KVClient> client3;
    InitTestKVClient(3, client3);
    InitTestKVClient(2, client2_);
    sleep(3);  // scale-up begin

    std::string value2{ "modifiedValue" };
    // set data through different clients
    for (int i = 0; i < 50; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value2));
    }
    for (int i = 50; i < 100; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_OK(client2_->Set(key, value2));
    }
    std::string outValue;
    for (int i = 0; i < 100; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_OK(client3->Get(key, outValue));
        ASSERT_EQ(outValue, value2);
    }
}

TEST_F(STCScaleUpTest, ScaleUpInBatch)
{
    bool quit = false;
    int putNum = 0;
    std::mutex m;
    auto keepSending = [&](KVClient *client) {
        while (!quit) {
            {
                std::unique_lock<std::mutex> l(m);
                DS_ASSERT_OK(client->Set("KEYYYY_" + std::to_string(putNum), "anyValue"));
                putNum++;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    };

    DS_ASSERT_OK(externalCluster_->StartForkWorkerProcess());

    StartForkWorkerAndWaitReady({ 0, 1 });
    // ensure the first scale-up does not end before the last node is added by setting a longer migration time.
    cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "call(3)");
    InitTestKVClient(0, client_);
    std::thread sendOnW0(keepSending, client_.get());

    // add nodes per two seconds
    std::this_thread::sleep_for(std::chrono::seconds(2));
    StartForkWorkerAndWaitReady({ 2 });
    std::this_thread::sleep_for(std::chrono::seconds(2));
    StartForkWorkerAndWaitReady({ 3 });
    std::shared_ptr<KVClient> client3;
    InitTestKVClient(3, client3);
    std::thread sendOnW3(keepSending, client3.get());

    std::this_thread::sleep_for(std::chrono::seconds(2));
    StartForkWorkerAndWaitReady({ 4 });
    std::shared_ptr<KVClient> client4;
    InitTestKVClient(4, client4);
    std::thread sendOnW4(keepSending, client4.get());

    // wait for scale-up end.
    sleep(6);
    // all nodes have been joined.
    AssertAllNodesJoinIntoHashRing(5);

    quit = true;
    sendOnW0.join();
    sendOnW3.join();
    sendOnW4.join();

    std::string outValue;
    InitTestKVClient(0, client_);
    for (int i = 0; i < putNum; i++) {
        std::string key = "KEYYYY_" + std::to_string(i);
        DS_ASSERT_OK(client_->Get(key, outValue));
    }
}

TEST_F(STCScaleUpTest, LEVEL1_RestartNodeAfterScaleUp)
{
    DS_ASSERT_OK(cluster_->StartOBS());

    // init cluster
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);

    // put test data
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::string> keys;
    for (auto i = 0; i < 5; i++) {
        keys.emplace_back(client_->Set("any_value", param));
    }
    for (auto i = 0; i < 5; i++) {
        keys.emplace_back("w0_beforescaleup_" + std::to_string(i));
        DS_ASSERT_OK(client1_->Set(keys.back(), "any_value", param));
    }

    StartWorkerAndWaitReady({ 2 });
    sleep(3);  // scaleup begin
    for (auto i = 0; i < 5; i++) {
        keys.emplace_back(client1_->Set("any_value", param));
    }
    for (auto i = 0; i < 5; i++) {
        keys.emplace_back("w1_afterscaleup_" + std::to_string(i));
        DS_ASSERT_OK(client1_->Set(keys.back(), "any_value", param));
    }

    RestartWorkerAndWaitReady({ 1 });

    // get value one by one
    std::string outValue;
    for (auto &key : keys) {
        DS_ASSERT_OK(client_->Get(key, outValue));
    }
}

TEST_F(STCScaleUpTest, DISABLED_LEVEL1_RestartClusterAfterScaleUp)
{
    DS_ASSERT_OK(cluster_->StartOBS());

    // init cluster
    StartWorkerAndWaitReady({ 0, 1 });
    // put test data
    std::vector<std::string> keys;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    InitTestKVClient(0, client_);
    for (auto i = 0; i < 50; i++) {
        keys.emplace_back("w0_beforescaleup_" + std::to_string(i));
        DS_ASSERT_OK(client_->Set(keys.back(), "any_value", param));
    }

    // scale up
    StartWorkerAndWaitReady({ 2, 3 });
    sleep(5);
    // put test data
    for (auto i = 0; i < 50; i++) {
        keys.emplace_back("w0_afterscaleup_" + std::to_string(i));
        DS_ASSERT_OK(client_->Set(keys.back(), "any_value", param));
    }
    InitTestKVClient(2, client2_);
    for (auto i = 0; i < 50; i++) {
        keys.emplace_back("w2_" + GenPartRandomString());
        DS_ASSERT_OK(client2_->Set(keys.back(), "any_value", param));
    }

    // restart the cluster
    RestartWorkerAndWaitReady({ 0, 1, 2, 3 });

    // get value
    InitTestKVClient(2, client2_);
    std::string outValue;
    for (auto &key : keys) {
        DS_ASSERT_OK(client2_->Get(key, outValue));
        ASSERT_EQ("any_value", outValue);
    }
}

TEST_F(STCScaleUpTest, LEVEL1_RestartSourceNodeDuringScaleUp)
{
    // init cluster
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    std::string value{ "anyValue" };
    for (auto i = 0; i < 100; i++) {
        auto key = "Origin_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value));  // data on w0 (will not be restarted)
    }

    cluster_->SetInjectAction(WORKER, 1, "BatchMigrateMetadata.delay", "call(5)");

    StartWorkerAndWaitReady({ 2 });
    sleep(5);  // scale up begin, and migrating

    // restart source node
    RestartWorkerAndWaitReady({ 1 }, SIGKILL);  // SIGTEM will wait for migration task complete, use SIGKILL here.
    sleep(10);                                  // wait for migration finished.

    // all nodes have been joined.
    AssertAllNodesJoinIntoHashRing(3);

    // no data loss
    InitTestKVClient(1, client1_);
    std::string outValue;
    for (auto i = 0; i < 100; i++) {
        auto key = "Origin_" + std::to_string(i);
        DS_ASSERT_OK(client1_->Get(key, outValue));
        ASSERT_EQ(outValue, value);
    }

    ResetClients();
    (void)cluster_->ShutdownNodes(ClusterNodeType::WORKER);
}

TEST_F(STCScaleUpTest, LEVEL1_RestartJoiningNodeDuringScaleUp)
{
    // init cluster
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    std::string value{ "anyValue" };
    for (auto i = 0; i < 100; i++) {
        auto key = "Origin_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value));  // data on w0 (will not be restarted)
    }

    cluster_->SetInjectAction(WORKER, 1, "BatchMigrateMetadata.delay", "call(5)");

    StartWorkerAndWaitReady({ 2 });
    sleep(5);  // scale up begin, and migrating

    // restart joining node
    RestartWorkerAndWaitReady({ 2 }, SIGKILL);  // SIGTEM will wait for migration task complete, use SIGKILL here.
    WaitAllNodesJoinIntoHashRing(3); // Worker num is 3.

    // no data loss
    InitTestKVClient(1, client1_);
    std::string outValue;
    for (auto i = 0; i < 100; i++) {
        auto key = "Origin_" + std::to_string(i);
        DS_ASSERT_OK(client1_->Get(key, outValue));
        ASSERT_EQ(outValue, value);
    }
}

TEST_F(STCScaleUpTest, LEVEL2_JoiningNodeNetworkRecoveryDuringScaleUp)
{
    // init cluster
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    std::string value{ "anyValue" };
    for (auto i = 0; i < 100; i++) {
        auto key = "Origin_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value));  // data on w0 (will not be restarted)
    }

    cluster_->SetInjectAction(WORKER, 1, "BatchMigrateMetadata.delay", "call(5)");

    StartWorkerAndWaitReady({ 2 });

    sleep(5);  // scale up begin, and migrating
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "EtcdClusterManager.HandleFailedNodeToActive.sleep", "1*sleep(2)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "EtcdClusterManager.HandleFailedNodeToActive.sleep", "1*sleep(2)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "heartbeat.sleep", "1*sleep(5000)"));
    sleep(20);  // wait for migration finished.

    // all nodes have been joined.
    AssertAllNodesJoinIntoHashRing(3);

    // no data loss
    InitTestKVClient(1, client1_);
    std::string outValue;
    for (auto i = 0; i < 100; i++) {
        auto key = "Origin_" + std::to_string(i);
        DS_ASSERT_OK(client1_->Get(key, outValue));
        ASSERT_EQ(outValue, value);
    }

    (void)cluster_->ShutdownNodes(ClusterNodeType::WORKER);
}

TEST_F(STCScaleUpTest, ForceJoin)
{
    // 1. init 2 nodes
    StartWorkerAndWaitReady({ 0, 1 });

    // 2. add node without scale-up process
    InitTestEtcdInstance();
    std::string value;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
    HashRingPb ring;
    ASSERT_TRUE(ring.ParseFromString(value));

    HostPort w2 = externalCluster_->GetExternalOptions().workerConfigs[2];
    WorkerPb wpb;
    wpb.set_state(WorkerPb::ACTIVE);
    wpb.set_worker_uuid(GetStringUuid());
    auto tokens = { 295278997, 832149912, 1369020823, 1905891734 };  // an example of tokens splitting
    for (auto token : tokens) {
        wpb.mutable_hash_tokens()->Add(token);
    }
    ring.mutable_workers()->insert({ w2.ToString(), wpb });
    DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));

    // 3. check if can hash to w2
    StartWorkerAndWaitReady({ 2 });
    CheckMaster({ 0, 1, 2 });
}

TEST_F(STCScaleUpTest, TaskRetry)
{
    // init 2 nodes
    StartWorkerAndWaitReady({ 0, 1 });
    // simulate an etcd put failure to test if the scale-up task can retry
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashring.finishaddnodeinfo", "1*return(K_RUNTIME_ERROR)"));

    // add node
    StartWorkerAndWaitReady({ 2 });
    for (auto i = 0; i < 3; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "worker.HashRingHealthCheck", "1*call(3000)"));
        // do not abort when fail.
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "Hashring.Scaletask.Fail"));
    }

    WaitAllNodesJoinIntoHashRing(3);
}

TEST_F(STCScaleUpTest, TestMarkChangeRangeFinishFailed)
{
    // init 2 nodes
    StartWorkerAndWaitReady({ 0, 1 });
    int currWorkerNum = 2;
    for (auto i = 0; i < currWorkerNum; i++) {
        // simulate an etcd put failure to test if the scale-up task can retry
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "hashring.finishaddnodeinfo", "1*return(K_TRY_AGAIN)"));
        // do not abort when fail.
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "Hashring.Scaletask.Fail"));
        // Ensure that the routine HashRingHealthCheck will not be executed during the test case execution.
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "worker.HashRingHealthCheck", "call(3000000)"));
    }

    // add node
    StartWorkerAndWaitReady({ 2 });
    currWorkerNum += 1;

    int waitScaleUpTimeSec = 5;
    WaitAllNodesJoinIntoHashRing(currWorkerNum, waitScaleUpTimeSec);
}

TEST_F(STCScaleUpTest, TestScaleUpWhenExistsPendingLeavingNode)
{
    // The hashring exists pending leaving node.
    HashRingPb ring;
    WorkerPb workerPb;
    workerPb.set_worker_uuid("5d941f6a-dd77-43ee-b3cf-40a1cf77489e");
    workerPb.set_state(WorkerPb::LEAVING);
    workerPb.set_need_scale_down(true);
    std::vector<uint32_t> tokens = { 703003027, 1320750813, 2986027838, 1537794891 };
    for (auto token : tokens) {
        workerPb.mutable_hash_tokens()->Add(token);
    }
    ring.mutable_workers()->insert({ "127.0.0.1:9999", workerPb });
    ring.set_cluster_has_init(true);
    DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));

    int timeoutSec = 20;
    StartWorkerAndWaitReady({ 0, 1 }, timeoutSec, false,
                            "-inject_actions=worker.GetFailedWorkers:call(127.0.0.1:9999)");
}

class STCDefaultScaleUpTest : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.addNodeTime = 60;  // the TOTAL_WAIT_NODE_TABLE_TIME_SEC in etcdClusterManager is 60
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 ";
        opts.waitWorkerReady = false;
    }
};

TEST_F(STCDefaultScaleUpTest, LEVEL1_InitCluster)
{
    ASSERT_TRUE(externalCluster_->StartWorker(0, HostPort()).IsOk());
    // during the first period 0~60s, a new node added.
    std::this_thread::sleep_for(std::chrono::seconds(10));
    ASSERT_TRUE(externalCluster_->StartWorker(1, HostPort()).IsOk());

    for (auto i : { 0, 1 }) {
        ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i).IsOk());
    }
}

class STCScaleDownTest : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 5;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        opts.workerGflagParams =
            FormatString(" -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true", nodeTimeout_,
                         nodeDeadTimeout_);
        opts.waitWorkerReady = false;
        opts.disableRocksDB = false;
    }

protected:
    const int nodeTimeout_ = 6;
    const int nodeDeadTimeout_ = 8;
};

class STCScaleDownRecoverMetaTest : public STCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true "
            "--enable_metadata_recovery=true ",
            nodeTimeout_, nodeDeadTimeout_);
        opts.disableRocksDB = true;
    }

protected:
    int nodeTimeout_ = 6;
    int nodeDeadTimeout_ = 8;
};

TEST_F(STCScaleDownRecoverMetaTest, WorkerScaleDown)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    InitTestKVClient(2, client2_);
    std::vector<std::string> keys;
    for (int i = 0; i < 10; i++) {
        keys.emplace_back(client_->GenerateKey());
        DS_ASSERT_OK(client1_->Set(keys.back(), "any_value"));
    }
    client_.reset();
    kill(cluster_->GetWorkerPid(0), SIGTERM);  // worker index 0
    WaitAllNodesJoinIntoHashRing(2);
    for (const auto &key : keys) {
        std::string value;
        DS_ASSERT_OK(client2_->Get(key, value));
        ASSERT_EQ(value, "any_value");
    }
}

class STCRestartRecoverMetaTest : public STCScaleDownRecoverMetaTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) {
        nodeTimeout_ = 60;
        nodeDeadTimeout_ = 80;
        STCScaleDownRecoverMetaTest::SetClusterSetupOptions(opts);
    }
};

TEST_F(STCRestartRecoverMetaTest, WorkerRestartRecoverTest)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    InitTestKVClient(2, client2_);
    std::vector<std::string> keys;
    for (int i = 0; i < 10; i++) {
        keys.emplace_back(client_->GenerateKey());
        DS_ASSERT_OK(client1_->Set(keys.back(), "any_value"));
    }
    client_.reset();
    kill(cluster_->GetWorkerPid(0), SIGTERM);  // worker index 0
    const int interval = 2000;  // wait 10000ms for clean map;
    std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    StartWorkerAndWaitReady({ 0 });
    for (const auto &key : keys) {
        std::string value;
        DS_ASSERT_OK(client2_->Get(key, value));
        ASSERT_EQ(value, "any_value");
    }
}

TEST_F(STCScaleDownTest, LEVEL1_TestStandbyNodeFailedRestart)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2, 3 });
    std::vector<std::string> keys;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    InitTestKVClient(0, client_);
    for (auto i = 0; i < 20; i++) {  // obj num is 20
        keys.emplace_back(client_->Set("data_on_worker0", param));
        keys.emplace_back(GetStringUuid());
        DS_ASSERT_OK(client_->Set(keys.back(), "data_on_worker0", param));
    }

    kill(cluster_->GetWorkerPid(0), SIGTERM);  // worker index 0
    kill(cluster_->GetWorkerPid(1), SIGTERM);  // worker index 1
    kill(cluster_->GetWorkerPid(2), SIGTERM);  // worker index 2
    WaitAllNodesJoinIntoHashRing(1);           // After w0, w1, w2 scale down, there is 1 node w3 left.

    StartWorkerAndWaitReady({ 0, 1, 2 }, 100);                 // wait all node finished is 100
    WaitAllNodesJoinIntoHashRing(4, SCALE_DOWN_ADD_TIME + 1);  // After w0, w1, w2 scale up, there is 4 nodes total.
}

TEST_F(STCScaleDownTest, LEVEL2_KeyWithL2Cache)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });

    InitTestKVClient(0, client_);

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::string> keys;
    for (auto i = 0; i < 5; i++) {
        keys.emplace_back(client_->Set("data_on_worker0", param));
        keys.emplace_back(GetStringUuid());
        DS_ASSERT_OK(client_->Set(keys.back(), "data_on_worker0", param));
    }

    // shutdown and wait for scale down finished.
    client_.reset();
    externalCluster_->ShutdownNode(WORKER, 0);
    WaitAllNodesJoinIntoHashRing(2, nodeDeadTimeout_ + 1);

    // after scale down, other worker can get key from etcd and modify
    InitTestKVClient(1, client1_);
    std::string value;
    for (auto &key : keys) {
        DS_ASSERT_OK(client1_->Get(key, value));
        ASSERT_TRUE(value == "data_on_worker0");
        DS_ASSERT_OK(client1_->Set(key, "data_on_worker1", param));
    }

    // restart the substitute worker, everything is ok
    client1_.reset();
    RestartWorkerAndWaitReady({ 1 });
    InitTestKVClient(1, client1_);
    for (auto &key : keys) {
        DS_ASSERT_OK(client1_->Get(key, value));
        EXPECT_TRUE(value == "data_on_worker1");
    }

    // faulty worker restart, join as a new node
    StartWorkerAndWaitReady({ 0 });
    WaitAllNodesJoinIntoHashRing(3, SCALE_DOWN_ADD_TIME + 1);

    // as a new node, it will not recover from rocksdb to prevent fetching old version data.
    InitTestKVClient(0, client_);
    for (auto &key : keys) {
        DS_ASSERT_OK(client_->Get(key, value));
        EXPECT_TRUE(value == "data_on_worker1");
    }
}

TEST_F(STCScaleDownTest, GetKeyWithL2CacheDuringScaleDown)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "SubmitScaleDownTask.skip", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "SubmitScaleDownTask.skip", "return()"));  // worker index is 2.

    InitTestKVClient(0, client_);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::string> keys;
    int keyCount = 10;
    for (auto i = 0; i < keyCount; i++) {
        keys.emplace_back(client_->Set("data_on_worker0", param));
        keys.emplace_back(GetStringUuid());
        DS_ASSERT_OK(client_->Set(keys.back(), "data_on_worker0", param));
    }

    // shutdown and wait for scale down finished.
    client_.reset();
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, 0));
    sleep(nodeDeadTimeout_ - nodeTimeout_ + 1);

    // after scale down, other worker can get key from etcd and modify
    InitTestKVClient(1, client1_);
    std::string value;
    for (auto &key : keys) {
        DS_ASSERT_OK(client1_->Get(key, value));
        ASSERT_TRUE(value == "data_on_worker0");
    }
}

TEST_F(STCScaleDownTest, ScaleDownQuerySize)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });

    InitTestKVClient(0, client_);
    std::string value = "data_on_worker0";
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::string key_name = "data_on_worker0";
    DS_ASSERT_OK(client_->Set(key_name, value, param));
    client_.reset();
    externalCluster_->ShutdownNode(WORKER, 0);
    int scaleDownWorkerNum = 2;
    WaitAllNodesJoinIntoHashRing(scaleDownWorkerNum, nodeDeadTimeout_ + 1);
    InitTestKVClient(1, client1_);
    std::vector<uint64_t> outSizes;
    DS_ASSERT_OK(client1_->QuerySize({ key_name }, outSizes));
    ASSERT_EQ(outSizes.size(), 1);
    ASSERT_EQ(outSizes[0], value.length());
}

TEST_F(STCScaleDownTest, ClearDataWithUuid)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    std::vector<std::string> keys;
    for (int i = 0; i < 20; i++) {
        auto key_with_worker0 = client_->Set("data_on_worker0");
        keys.emplace_back(key_with_worker0);
    }
    std::string valGet;
    for (const auto &key : keys) {
        DS_ASSERT_OK(client1_->Get(key, valGet));
    }
    externalCluster_->ShutdownNode(WORKER, 0);
    WaitAllNodesJoinIntoHashRing(2, nodeDeadTimeout_ + 1);

    for (const auto &key : keys) {
        DS_ASSERT_NOT_OK(client1_->Get(key, valGet));
    }
}

TEST_F(STCScaleDownTest, LEVEL2_CleanKeyWithWorkerIdMetaMap)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    std::string value = "testValue";
    StartWorkerAndWaitReady({ 0, 1, 2, 3 }, " -inject_actions=Worker.cleanMapIntervalMs:call()");

    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    InitTestKVClient(2, client2_);  //  init client2 with workeId 2
    auto uuidKey = client_->GenerateKey();
    auto UuidKey1 = client1_->GenerateKey();
    DS_ASSERT_OK(client_->Set(uuidKey, value, param));
    DS_ASSERT_OK(client1_->Set(UuidKey1, value, param));

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    int validWorkerNum = 2;
    WaitAllNodesJoinIntoHashRing(validWorkerNum, nodeDeadTimeout_ + SCALE_DOWN_ADD_TIME);

    std::string newValue = "newValue";
    std::thread thread([&]() {
        DS_ASSERT_OK(client2_->Del(uuidKey));
        DS_ASSERT_OK(client2_->Set(uuidKey, newValue));
        DS_ASSERT_OK(client2_->Set(UuidKey1, value + "1", param));
        DS_ASSERT_OK(client2_->Del(uuidKey));
        const int interval = 10000;  // wait 10000ms for clean map;
        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        DS_ASSERT_NOT_OK(client2_->Set(uuidKey, newValue, param));
        DS_ASSERT_OK(client2_->Set(UuidKey1, newValue, param));
    });
    thread.join();
}

TEST_F(STCScaleDownTest, LEVEL1_AddNodeWhenOldNodeFailed)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    std::string value{ "anyValue" };
    std::vector<std::string> keys;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 20; i++) {
        auto key = "add_node_old_failed_" + std::to_string(i);
        keys.emplace_back(key);
        DS_ASSERT_OK(client_->Set(key, value, param));
    }
    client_.reset();
    externalCluster_->QuicklyShutdownWorker(0);
    StartWorkerAndWaitReady({ 3 });
    WaitAllNodesJoinIntoHashRing(3, nodeDeadTimeout_ + 1);

    std::string valueGet;
    for (auto key : keys) {
        DS_ASSERT_OK(client1_->Get(key, valueGet));
    }
}

TEST_F(STCScaleDownTest, TestOldNodeFailedClearData)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    std::string value{ "anyValue" };
    std::vector<std::string> keys;
    for (auto i = 0; i < 50; i++) {
        auto key = "add_node_old_failed_" + std::to_string(i);
        keys.emplace_back(key);
        DS_ASSERT_OK(client_->Set(key, value));
    }
    for (auto key : keys) {
        std::string valueGet;
        DS_ASSERT_OK(client1_->Get(key, valueGet));
    }
    client_.reset();
    externalCluster_->ShutdownNode(WORKER, 0);
    StartWorkerAndWaitReady({ 3 });
    WaitAllNodesJoinIntoHashRing(3, nodeDeadTimeout_ + SCALE_DOWN_ADD_TIME);

    InitTestKVClient(3, client2_);
    std::string valueGet;
    for (auto key : keys) {
        if (client2_->Get(key, valueGet).GetCode() == K_NOT_FOUND) {
            std::string valueGet1;
            DS_ASSERT_NOT_OK(client1_->Get(key, valueGet1));
        } else {
            ASSERT_EQ(valueGet, "anyValue");
        }
    }
}

TEST_F(STCScaleDownTest, LEVEL1_AddNodeWhenOldNodeFailedKeyWithUuid)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    std::string value{ "anyValue" };
    std::vector<std::string> keys;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 10; i++) {
        auto key = client_->Set(value, param);
        ASSERT_NE("", key);
        keys.emplace_back(key);
    }
    StartWorkerAndWaitReady({ 3 });
    externalCluster_->ShutdownNode(WORKER, 0);
    sleep(10);
    AssertAllNodesJoinIntoHashRing(3);
    std::string valueGet;
    for (auto key : keys) {
        DS_ASSERT_OK(client1_->Get(key, valueGet));
    }
}

TEST_F(STCScaleDownTest, DISABLED_TestClearDataWriteThtough)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    std::string value{ "anyValue" };
    std::vector<std::string> keys;
    std::vector<std::string> throughKeys;
    for (auto i = 0; i < 50; i++) {
        auto key = "add_node_old_failed_" + std::to_string(i);
        keys.emplace_back(key);
        std::string valueGet;
        DS_ASSERT_OK(client_->Set(key, value));
        DS_ASSERT_OK(client1_->Get(key, valueGet));
    }
    SetParam param = { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 50; i++) {
        auto key = "add_node_old_failed_through_" + std::to_string(i);
        keys.emplace_back(key);
        DS_ASSERT_OK(client_->Set(key, value, param));
        std::string valueGet;
        DS_ASSERT_OK(client1_->Get(key, valueGet));
    }

    externalCluster_->ShutdownNode(WORKER, 0);
    client_.reset();
    StartWorkerAndWaitReady({ 3 });
    sleep(20);
    std::vector<std::string> unfinishKeys;
    AssertAllNodesJoinIntoHashRing(3);
    InitTestKVClient(3, client2_);
    std::string valueGet;
    for (auto key : keys) {
        if (client2_->Get(key, valueGet).GetCode() == K_NOT_FOUND) {
            std::string valueGet1;
            DS_ASSERT_NOT_OK(client1_->Get(key, valueGet1));
        } else {
            ASSERT_EQ(valueGet, "anyValue");
        }
    }
}

TEST_F(STCScaleDownTest, LEVEL1_AddNodeNetDelay)
{
    StartWorkerAndWaitReady({ 0, 1, 3 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client2_);
    std::vector<std::string> keys;
    for (auto i = 0; i < 10; i++) {
        keys.emplace_back("key_scale_test" + std::to_string(i));
    }
    std::for_each(keys.begin(), keys.end(),
                  [this](const std::string &key) { DS_ASSERT_OK(client_->Set(key, "another_data_on_worker0")); });
    StartWorkerAndWaitReady({ 2 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "HashRing.UpdateRing.sleep", "sleep(8000)"));
    sleep(7);
    std::string dbValue;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", dbValue));
    HashRingPb ring;
    ASSERT_TRUE(ring.ParseFromString(dbValue));
    ASSERT_EQ(ring.workers_size(), 4);
    HostPort workerAddr1;
    cluster_->GetWorkerAddr(2, workerAddr1);
    for (auto &worker : ring.workers()) {
        if (worker.first == workerAddr1.ToString()) {
            ASSERT_TRUE(worker.second.state() == WorkerPb::JOINING) << ring.ShortDebugString();
        } else {
            ASSERT_TRUE(worker.second.state() == WorkerPb::ACTIVE);
        }
    }
    sleep(10);
    AssertAllNodesJoinIntoHashRing(4);
    std::string value;
    std::for_each(keys.begin(), keys.end(), [this, &value](const std::string &key) {
        DS_ASSERT_OK(client2_->Get(key, value));
        ASSERT_EQ(value, "another_data_on_worker0");
    });
}

TEST_F(STCScaleDownTest, DISABLED_LEVEL1_RestartAfterScaleDown)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });

    InitTestKVClient(0, client_);

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    auto key_with_worker0 = client_->Set("data_on_worker0", param);
    std::string value;

    // shutdown and wait for scale down finished.
    client_.reset();
    externalCluster_->ShutdownNode(WORKER, 0);
    sleep(15);
    AssertAllNodesJoinIntoHashRing(2);

    // restart worker1
    client1_.reset();
    RestartWorkerAndWaitReady({ 1 });

    InitTestKVClient(1, client1_);
    DS_ASSERT_OK(client1_->Get(key_with_worker0, value));
    EXPECT_TRUE(value == "data_on_worker0");
}

TEST_F(STCScaleDownTest, DISABLED_LEVEL1_CutNodeWhenRestart)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::string> keys;
    for (auto i = 0; i < 50; i++) {
        keys.emplace_back(client_->Set("data_on_worker0", param));
        keys.emplace_back(client2_->Set("data_on_worker2", param));
        keys.emplace_back("Hash_" + std::to_string(i));
        DS_ASSERT_OK(client_->Set(keys.back(), "HashKeyValue", param));
    }
    ResetClients();

    // close the cluster
    kill(cluster_->GetWorkerPid(0), SIGTERM);
    kill(cluster_->GetWorkerPid(1), SIGTERM);
    kill(cluster_->GetWorkerPid(2), SIGTERM);
    // sleep more than node_timeout_s therefore the clusterTable in etcd becomes empty.
    sleep(7);
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    DS_ASSERT_OK(db_->GetAll(ETCD_CLUSTER_TABLE, outKeyValues));
    ASSERT_TRUE(outKeyValues.empty());

    StartWorkerAndWaitReady({ 0, 1 }, 100, true);
    sleep(5);  // if scale down needed.
    AssertAllNodesJoinIntoHashRing(2);

    // new cluster running
    InitTestKVClient(1, client1_);
    // successfully get the data of previous cluster
    std::string outValue;
    for (auto &key : keys) {
        DS_ASSERT_OK(client1_->Get(key, outValue));
    }
    // successfully set the data into current cluster
    std::string value{ "any" };
    for (auto i = 0; i < 50; i++) {
        auto key = "new_" + std::to_string(i);
        DS_ASSERT_OK(client1_->Set(key, value));
    }
}

TEST_F(STCScaleDownTest, ZombieNodeInHashRing)
{
    StartWorkerAndWaitReady({ 0, 1 });

    std::string dbValue;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", dbValue));
    HashRingPb ring;
    ASSERT_TRUE(ring.ParseFromString(dbValue));
    WorkerPb zombie;
    zombie.set_worker_uuid("zombie_worker");
    ring.mutable_workers()->insert({ "127.0.0.1:0", zombie });
    ASSERT_EQ(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()), Status::OK());

    WaitAllNodesJoinIntoHashRing(2);  // finally, the zombie node in hash ring can be removed.
}

TEST_F(STCScaleDownTest, ChangeCluster1)
{
    std::string hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {
                "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {
                "hashTokens": [590558001, 1664299825, 2738041648, 3811783471],
                "workerUuid": "dXVpZDE=", "state": "ACTIVE"}
        },
    })";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok()) << rc.ToString();
    ASSERT_EQ(db_->Put(ETCD_RING_PREFIX, "", hashRing.SerializeAsString()), Status::OK());

    // start new cluster
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0 }, { { 0, " -inject_actions=EtcdClusterManager.CheckWaitNodeTableComplete.waitTime:call(2)" } }));
    WaitAllNodesJoinIntoHashRing(1);
}

TEST_F(STCScaleDownTest, ChangeCluster2)
{
    std::string hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {
                "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {
                "hashTokens": [590558001, 1664299825, 2738041648, 3811783471],
                "workerUuid": "dXVpZDE=", "state": "JOINING"}
        },
        "addNodeInfo": {
            "127.0.0.1:1": {"changedRanges": [
                {"workerId": "127.0.0.1:0", "from": 4294967292, "end": 590558001},
                {"workerId": "127.0.0.1:0", "from": 1073741823, "end": 1664299825},
                {"workerId": "127.0.0.1:0", "from": 2147483646, "end": 2738041648},
                {"workerId": "127.0.0.1:0", "from": 3221225469, "end": 3811783471}]}
        }
    })";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok()) << rc.ToString();
    ASSERT_EQ(db_->Put(ETCD_RING_PREFIX, "", hashRing.SerializeAsString()), Status::OK());

    // start new cluster
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0 }, { { 0, " -inject_actions=EtcdClusterManager.CheckWaitNodeTableComplete.waitTime:call(2)" } }));
    WaitAllNodesJoinIntoHashRing(1);
}

TEST_F(STCScaleDownTest, LEVEL2_KeyWithoutL2Cache)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);

    SetParam param{};
    auto key = "keyWithoutL2_" + GenPartRandomString();
    DS_ASSERT_OK(client_->Set(key, "data_on_worker0", param));
    client_.reset();

    // shutdown and wait for scale down finished.
    externalCluster_->ShutdownNode(WORKER, 0);
    sleep(15);
    AssertAllNodesJoinIntoHashRing(2);

    // after scale down, other worker can not get key if it has no coplia.
    InitTestKVClient(1, client1_);
    std::string value;
    DS_ASSERT_NOT_OK(client1_->Get(key, value));

    // after scale down, the hash ranges of faulty worker is redistributed.
    for (auto i = 0; i < 100; i++) {
        DS_ASSERT_OK(client1_->Set(GetStringUuid(), GenPartRandomString()));
    }

    // faulty worker restart, join as a new node
    StartWorkerAndWaitReady({ 0 });
    sleep(6);
    AssertAllNodesJoinIntoHashRing(3);

    // as a new node, it will not recover from rocksdb to prevent fetching old version data.
    InitTestKVClient(0, client_);
    DS_ASSERT_NOT_OK(client_->Get(key, value));
}

TEST_F(STCScaleDownTest, LEVEL1_JoiningNodeScaleDown)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 }, 20, false,  // wait time is 20 s
                            " -heartbeat_interval_ms=2000 ");
    cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "call(3)");

    std::string value{ "any" };
    InitTestKVClient(0, client_);
    // the number of objects should be bigger than the "objBatch" in MigrateMetadataForScaleout
    for (auto i = 0; i < 10; i++) {
        auto key = "Origin_NonL2_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value));
    }
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 5; i++) {
        auto key = "Origin_L2_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, value, param));
    }
    std::vector<std::string> keys_with_id;
    for (auto i = 0; i < 5; i++) {
        keys_with_id.emplace_back(client_->Set(value, param));
    }

    StartWorkerAndWaitReady({ 3, 4 });
    sleep(10);  // node is joining and the first-batch migration finished

    // wait for scale down and then scale up finished.
    externalCluster_->KillWorker(3);
    sleep(10);

    AssertAllNodesJoinIntoHashRing(4);

    InitTestKVClient(2, client2_);
    std::string outValue;
    for (auto i = 0; i < 5; i++) {
        auto key = "Origin_L2_" + std::to_string(i);
        DS_ASSERT_OK(client2_->Get(key, outValue));
    }
    for (auto &key : keys_with_id) {
        DS_ASSERT_OK(client2_->Get(key, outValue));
    }

    uint32_t successNum{ 0 };
    uint32_t failNum{ 0 };
    for (auto i = 0; i < 50; i++) {
        auto key = "Origin_NonL2_" + std::to_string(i);
        if (client_->Get(key, outValue).IsOk()) {
            ASSERT_EQ(outValue, value);
            successNum++;
        } else {
            failNum++;
        }
    }
    // the migrated meta will be lost, so the failNum should be greater than 0.
    ASSERT_TRUE(successNum != 0);
    ASSERT_TRUE(failNum != 0);
}

// Test scenario: the process worker of first scale down node is faulty
TEST_F(STCScaleDownTest, LEVEL1_ScaleDownAtSameTime)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::string> keys_with_workerid;
    for (auto i = 0; i < 5; i++) {
        keys_with_workerid.emplace_back(client_->Set("data_on_worker0", param));
    }
    for (auto i = 0; i < 5; i++) {
        keys_with_workerid.emplace_back(client2_->Set("data_on_worker2", param));
    }
    for (auto i = 0; i < 5; i++) {
        auto key = "Hash_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, "HashKeyValue", param));
    }

    // shutdown and wait for scale down finished.
    client_.reset();
    client2_.reset();
    kill(cluster_->GetWorkerPid(0), SIGTERM);  // Shutdown worker 0
    kill(cluster_->GetWorkerPid(2), SIGTERM);  // Shutdown worker 2
    sleep(20);

    AssertAllNodesJoinIntoHashRing(1);

    // check if data of scale-down nodes is ok
    std::string outValue;
    for (auto &key : keys_with_workerid) {
        DS_ASSERT_OK(client1_->Get(key, outValue));
    }
    for (auto i = 0; i < 5; i++) {
        auto key = "Hash_" + std::to_string(i);
        DS_ASSERT_OK(client1_->Get(key, outValue));
        ASSERT_EQ("HashKeyValue", outValue);
    }
}

TEST_F(STCScaleDownTest, DISABLED_LEVEL1_TestDelMetaInEtcd)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::string key_with_workerid;
    key_with_workerid = client2_->Set("data_on_worker2", param);

    // To confirm whether the metadata can be obtained from etcd after Set.
    std::stringstream table;
    table << ETCD_META_TABLE_PREFIX << ETCD_WORKER_SUFFIX << "/";
    RangeSearchResult res;
    DS_ASSERT_OK(db_->RawGet(std::string(ETCD_RING_PREFIX) + "/" + "", res));
    HashRingPb ring;
    ring.ParseFromString(res.value);

    HostPort w2;
    cluster_->GetWorkerAddr(2, w2);
    std::string worker2Uuid;
    for (auto worker : ring.workers()) {
        HostPort workerAddr;
        DS_ASSERT_OK(workerAddr.ParseString(worker.first));
        if (workerAddr == w2) {
            worker2Uuid = worker.second.worker_uuid();
        }
    }
    uint32_t hash = MurmurHash3_32(worker2Uuid);
    DS_ASSERT_OK(db_->RawGet(table.str() + master::Hash2Str(hash) + "/" + key_with_workerid, res));

    // shutdown and wait for scale down finished.
    client2_.reset();
    externalCluster_->ShutdownNode(WORKER, 2);
    sleep(20);

    AssertAllNodesJoinIntoHashRing(2);

    DS_ASSERT_OK(client1_->Del(key_with_workerid));

    // To confirm that the metadata in etcd has been deleted.
    DS_ASSERT_NOT_OK(db_->RawGet(table.str() + master::Hash2Str(hash) + "/" + key_with_workerid, res));
}

// Test scenario: scale down a node with del_node_info task.
TEST_F(STCScaleDownTest, LEVEL2_ScaleDownInOneBatch)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();
    cluster_->SetInjectAction(WORKER, 1, "SubmitScaleDownTask.delay", "call(5)");
    cluster_->SetInjectAction(WORKER, 2, "SubmitScaleDownTask.delay", "call(5)");

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::string> keys_with_workerid;
    int objNum = 5;
    for (auto i = 0; i < objNum; i++) {
        keys_with_workerid.emplace_back(client_->Set("data_on_worker0", param));
    }
    for (auto i = 0; i < objNum; i++) {
        keys_with_workerid.emplace_back(client2_->Set("data_on_worker2", param));
    }
    for (auto i = 0; i < objNum; i++) {
        auto key = "Hash_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, "HashKeyValue", param));
    }

    // shutdown and wait for scale down finished.
    client_.reset();
    client2_.reset();
    kill(cluster_->GetWorkerPid(0), SIGTERM);  // Shutdown worker 0
    sleep(3);                                  // less than node_dead_timeout and SubmitScaleDownTask.delay
    kill(cluster_->GetWorkerPid(2), SIGTERM);  // Shutdown worker 2
    sleep(25);

    AssertAllNodesJoinIntoHashRing(1);

    // check if data of scale-down nodes is ok
    std::string outValue;
    for (auto &key : keys_with_workerid) {
        DS_ASSERT_OK(client1_->Get(key, outValue));
    }
    for (auto i = 0; i < objNum; i++) {
        auto key = "Hash_" + std::to_string(i);
        DS_ASSERT_OK(client1_->Get(key, outValue));
        ASSERT_EQ("HashKeyValue", outValue);
    }
}

TEST_F(STCScaleDownTest, LEVEL1_ScaleDownNewWorker)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0 });
    AssertAllNodesJoinIntoHashRing(1);

    StartWorkerAndWaitReady({ 1 });
    InitTestKVClient(1, client1_);
    std::vector<std::string> keys;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 1; i++) {
        auto key = client1_->Set("anyVal", param);  // set when node is at INIT (before reach add_node_wait_time)
        ASSERT_TRUE(!key.empty());
        keys.emplace_back(std::move(key));
    }
    client1_.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(10);
    AssertAllNodesJoinIntoHashRing(1);

    // check the recovery of meta by modification
    InitTestKVClient(0, client_);
    std::string outVal;
    for (auto &key : keys) {
        DS_ASSERT_OK(client_->Get(key, outVal));
        ASSERT_EQ(outVal, "anyVal");
        DS_ASSERT_OK(client_->Set(key, "newVal", param));
    }
}

TEST_F(STCScaleDownTest, LEVEL1_ScaleDownNewWorkerAndRecoverMetaFail)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0 });
    AssertAllNodesJoinIntoHashRing(1);

    StartWorkerAndWaitReady({ 1 });
    InitTestKVClient(1, client1_);
    std::vector<std::string> keys;
    std::map<std::string, std::string> kv;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 15; i++) {
        std::string value = GenRandomString(1000);
        auto key = client1_->Set(value, param);
        ASSERT_TRUE(!key.empty());
        keys.emplace_back(key);
        kv.emplace(key, value);
    }
    client1_.reset();
    // The scale-in is performed after worker0 is shut down.
    // The metadata of object key0 is expected to be migrated to worker1.
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(10);
    AssertAllNodesJoinIntoHashRing(1);
    InitTestKVClient(0, client_);
    // When object key0 is got, metadata is expected to be queried on the w1 master node.
    // If the etcd query is performed, an error is returned.
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.QueryMetaDataFromEtcd_failure", "1000*return(K_NOT_FOUND)"));
    std::string outVal;
    for (auto &key : keys) {
        DS_ASSERT_OK(client_->Get(key, outVal));
        ASSERT_EQ(outVal, kv[key]);
    }
}

TEST_F(STCScaleDownTest, DISABLED_ScaleDownNewWorkerAndTestObjectTTL)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0 });
    AssertAllNodesJoinIntoHashRing(1);
    StartWorkerAndWaitReady({ 1 });
    InitTestKVClient(1, client1_);
    int keyNum = 10;
    std::vector<std::string> keys;
    std::map<std::string, std::string> kv;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 100 };
    for (auto i = 0; i < keyNum; i++) {
        std::string value = GenRandomString(1000);
        auto key = client1_->Set(value, param);
        ASSERT_TRUE(!key.empty());
        keys.emplace_back(key);
        kv.emplace(key, value);
    }
    client1_.reset();

    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.LoadMeta.steadyClockIsDifferent", "100*call()"));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(10);
    AssertAllNodesJoinIntoHashRing(1);
    InitTestKVClient(0, client_);

    std::string outVal;
    for (auto &key : keys) {
        DS_ASSERT_OK(client_->Get(key, outVal));
    }
}

TEST_F(STCScaleDownTest, LEVEL2_RestoreScaleDown)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    AssertAllNodesJoinIntoHashRing(2);
    cluster_->SetInjectAction(WORKER, 0, "SubmitScaleDownTask.skip", "return()");
    InitTestKVClient(1, client1_);
    std::vector<std::string> keys;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 5; i++) {
        auto key = client1_->Set("anyVal", param);
        ASSERT_TRUE(!key.empty());
        keys.emplace_back(std::move(key));
        keys.emplace_back("KEYYY_" + std::to_string(i));
        DS_ASSERT_OK(client1_->Set(keys.back(), "anyVal", param));
    }
    client1_.reset();
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(1));
    sleep(nodeTimeout_);
    AssertDelNodeInfoExist();

    RestartWorkerAndWaitReady({ 0 });
    sleep(1);
    AssertAllNodesJoinIntoHashRing(1);

    // check the recovery of meta by modification later
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    std::string valueToGet;
    std::for_each(keys.begin(), keys.end(), [&client, &valueToGet, &param](const std::string &key) {
        DS_ASSERT_OK(client->Get(key, valueToGet));
        ASSERT_TRUE(valueToGet == "anyVal");
        DS_ASSERT_OK(client->Set(key, "newVal", param));
    });
    client_.reset();
}

TEST_F(STCScaleDownTest, ShutdownWorkerAndDelKeyInEtcdTest)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0 });
    HostPort w0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, w0));
    std::string key = FLAGS_cluster_name + "/" + ETCD_CLUSTER_TABLE + "/" + w0.ToString();
    RangeSearchResult res;
    DS_ASSERT_OK(db_->RawGet(key, res));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_NOT_OK(db_->RawGet(key, res));
}

// During the scaling down period, the corresponding scaled-down node cannot be started.
TEST_F(STCScaleDownTest, LEVEL1_TestStartWorkerDuringScaleDown1)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    int nodeTimeoutS = 2;
    int nodeDeadTimeoutS = 5;
    StartWorkerAndWaitReady(
        { 0, 1 }, FormatString(" -node_timeout_s=%d -node_dead_timeout_s=%d", nodeTimeoutS, nodeDeadTimeoutS));

    InitTestKVClient(0, client_);
    std::string value{ "anyValue" };
    std::vector<std::string> keys;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 10; i++) {
        auto key = client_->Set(value, param);
        ASSERT_NE("", key);
        keys.emplace_back(key);
    }
    client_.reset();

    // During the scaling down period, the corresponding scaled-down node cannot be started.
    int submitScaleDownTaskWaitTimeS = 10;
    int S2Ms = 1000;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "HashRingTaskExecutor.SubmitScaleDownTask.ProcessSlowly",
                                           FormatString("1*sleep(%d)", submitScaleDownTaskWaitTimeS * S2Ms)));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    // To make sure hashring has been changed and "SubmitScaleDownTask" is not finished.
    sleep(nodeDeadTimeoutS - nodeTimeoutS + 1);
    DS_ASSERT_OK(externalCluster_->StartWorker(
        0, HostPort(),
        " -inject_actions=HashRing.InitRing.CheckQuickly:call(10);HashRing.InitWithEtcd.MotifyWaitTimeMs:call(5000)"));
    sleep(submitScaleDownTaskWaitTimeS);  // wait scaling down finish.
    AssertAllNodesJoinIntoHashRing(1);

    // Anything is ok.
    InitTestKVClient(1, client1_);
    std::string valueToGet;
    std::for_each(keys.begin(), keys.end(), [this, &valueToGet, &param](const std::string &key) {
        DS_ASSERT_OK(client1_->Get(key, valueToGet));
        ASSERT_TRUE(valueToGet == "anyValue");
        DS_ASSERT_OK(client1_->Set(key, "newVal", param));
    });
    client1_.reset();
}

// Before scaling down, the corresponding scaling down node can be started, but after successful startup, it will kill
// itself because it will receive its own delete event.
TEST_F(STCScaleDownTest, DISABLED_TestStartWorkerDuringScaleDown2)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    int nodeTimeoutS = 2;
    int nodeDeadTimeoutS = 5;
    StartWorkerAndWaitReady(
        { 0, 1 }, FormatString(" -node_timeout_s=%d -node_dead_timeout_s=%d -heartbeat_interval_ms=500", nodeTimeoutS,
                               nodeDeadTimeoutS));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "SkipPushMetaToWorker", "return(K_OK)"));
    InitTestKVClient(0, client_);
    std::string value{ "anyValue" };
    std::vector<std::string> keys;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (auto i = 0; i < 10; i++) {
        auto key = client_->Set(value, param);
        ASSERT_NE("", key);
        keys.emplace_back(key);
    }
    client_.reset();
    // During the scaling down period, the corresponding scaled-down node cannot be started.
    int removeWorkerWaitTimeS = 5;
    int S2Ms = 1000;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "hashRing.removeWorker",
                                           FormatString("1*sleep(%d)", removeWorkerWaitTimeS * S2Ms)));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "SubmitScaleDownTask.skip", "1*sleep(5000)"));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    sleep(nodeDeadTimeoutS - nodeTimeoutS + 1);  // To make sure worker0 becomes a scaled-down node.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "HandleNodeRemoveEvent.delay", "1*sleep(10000)"));
    if (!db_) {
        InitTestEtcdInstance();
    }
    Timer timer;
    while (timer.ElapsedSecond() < 10) {  // wait time is 10 s
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        if (ring.workers_size() == 1) {
            break;
        }
        auto t100ms = 100;
        std::this_thread::sleep_for(std::chrono::milliseconds(t100ms));  // interval is 500 ms
    }
    AssertAllNodesJoinIntoHashRing(1);
    // Anything is ok.
    InitTestKVClient(1, client1_);
    std::string valueToGet;
    std::for_each(keys.begin(), keys.end(), [this, &valueToGet, &param](const std::string &key) {
        DS_ASSERT_OK(client1_->Get(key, valueToGet));
        ASSERT_TRUE(valueToGet == "anyValue");
        DS_ASSERT_OK(client1_->Set(key, "newVal", param));
    });
    client1_.reset();
    cluster_->KillWorker(1);
}

TEST_F(STCScaleDownTest, NoStandbyWorker)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    std::initializer_list<uint32_t> workerlist = { 0, 1, 2 };  // init 3 worker
    StartWorkerAndWaitReady(workerlist);
    InitTestKVClient(0, client_);
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    // all workers timeout due to etcd fault
    for (auto idx : workerlist) {
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, idx, "heartbeat.sleep", "1*sleep(6000)"));  // Set injection to worker 0
        std::this_thread::sleep_for(std::chrono::milliseconds(100));                      // Wait 100 milliseconds.
        cluster_->SetInjectAction(ClusterNodeType::WORKER, idx, "worker.PreShutDown.skip", "return(K_OK)");
    }
    sleep(nodeDeadTimeout_ + 1);

    // check if worker can exit
    // it's possible that worker will exit with SIGKILL, so shutdown here to prevent assertion failed in tear down.
    client_.reset();
    for (auto i : workerlist) {
        (void)cluster_->ShutdownNode(WORKER, i);
    }
}

TEST_F(STCScaleDownTest, LEVEL1_ScaleDownWhenScaleUpStart)
{
    StartWorkerAndWaitReady({ 0, 1 });
    const int nodeCount = 2;
    AssertAllNodesJoinIntoHashRing(nodeCount);

    // start worker 2.
    const int newWorkerIndex = 2;
    DS_ASSERT_OK(
        externalCluster_->StartWorker(newWorkerIndex, HostPort(), " -inject_actions=worker.HashRing.TryAdd:return()"));

    // Wait until worker 2 join into hashring
    HostPort workerAddress2;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(newWorkerIndex, workerAddress2));
    WaitHashRingChange([&workerAddress2](const HashRingPb &hashRing) {
        auto iter = hashRing.workers().find(workerAddress2.ToString());
        return iter != hashRing.workers().end() && iter->second.state() == WorkerPb::INITIAL;
    });

    LOG(INFO) << "kill term worker 0";
    kill(cluster_->GetWorkerPid(0), SIGTERM);

    // wait until worker 0 scale down.
    HostPort workerAddress0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress0));
    WaitHashRingChange([&workerAddress0](const HashRingPb &hashRing) {
        return hashRing.workers().count(workerAddress0.ToString()) == 0;
    });

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, newWorkerIndex, "worker.HashRing.TryAdd"));
    // wait until worker 2 to active.
    WaitHashRingChange([&workerAddress2](const HashRingPb &hashRing) {
        auto iter = hashRing.workers().find(workerAddress2.ToString());
        return iter != hashRing.workers().end() && iter->second.state() == WorkerPb::ACTIVE;
    });

    // restart worker 0 again.
    StartWorkerAndWaitReady({ 0 });
    WaitHashRingChange([&workerAddress0](const HashRingPb &hashRing) {
        auto iter = hashRing.workers().find(workerAddress0.ToString());
        return iter != hashRing.workers().end() && iter->second.state() == WorkerPb::ACTIVE;
    });
    const int totalNodeCount = 3;
    AssertAllNodesJoinIntoHashRing(totalNodeCount);
}

TEST_F(STCScaleDownTest, LEVEL2_StartWhenScalingDown)
{
    StartWorkerAndWaitReady({ 0, 1 });     // init 2 workers at first time
    std::this_thread::sleep_for(std::chrono::seconds(SCALE_DOWN_ADD_TIME + 1));
    StartWorkerAndWaitReady({ 2, 3, 4 });  // init 3 workers to trigger scale up
    std::this_thread::sleep_for(std::chrono::seconds(SCALE_DOWN_ADD_TIME + 1));
    AssertAllNodesJoinIntoHashRing(5);  // total 5 workers is ready
    LOG(INFO) << "----all workers join.";

    cluster_->SetInjectAction(WORKER, 0, "SubmitScaleDownTask.skip",
                              "sleep(5000)");  // blocking the scale down task for worker 0
    const int w2 = 2;                          // the index of worker2
    cluster_->SetInjectAction(WORKER, w2, "SubmitScaleDownTask.skip",
                              "sleep(6000)");  // blocking the scale down task for worker 2
    kill(cluster_->GetWorkerPid(1), SIGKILL);
    WaitHashRingChange([&](const HashRingPb &hashRing) { return hashRing.del_node_info_size() > 0; });
    LOG(INFO) << "----worker1 is scaling down, restart";
    StartWorkerAndWaitReady({ 1 });
    std::this_thread::sleep_for(std::chrono::seconds(SCALE_DOWN_ADD_TIME + 1));
    AssertAllNodesJoinIntoHashRing(5);
    (void)cluster_->ShutdownNodes(WORKER);
    (void)cluster_->ShutdownNodes(ETCD);
}

TEST_F(STCScaleDownTest, LEVEL1_ForceRemove)
{
    // 1. init 3 nodes
    StartWorkerAndWaitReady({ 0, 1, 2 });
    const int w2Index = 2;
    // 2. create object that meta not on w2 but data on w2
    InitClients();
    std::vector<std::string> keys1;
    std::string outVal;
    // meta on w0, primary is w2, location is w1
    for (auto i = 0; i < 10; i++) {
        std::string key;
        (void)client_->GenerateKey("", key);
        keys1.emplace_back(key);
        DS_ASSERT_OK(client2_->Set(keys1.back(), "val"));
        DS_ASSERT_OK(client1_->Get(keys1.back(), outVal));
    }
    // meta on w0, primary is w1, location is w2
    std::vector<std::string> keys2;
    for (auto i = 0; i < 10; i++) {
        std::string key;
        (void)client_->GenerateKey("", key);
        keys2.emplace_back(key);
        DS_ASSERT_OK(client1_->Set(keys2.back(), "val"));
        DS_ASSERT_OK(client2_->Get(keys2.back(), outVal));
    }
    // meta on w0, primary is w1, no locations
    for (auto i = 0; i < 10; i++) {
        std::string key;
        (void)client_->GenerateKey("", key);
        keys2.emplace_back(key);
        DS_ASSERT_OK(client1_->Set(keys2.back(), "val"));
    }

    // 3. del node without scale-down process
    InitTestEtcdInstance();
    std::string value;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
    HashRingPb ring;
    ASSERT_TRUE(ring.ParseFromString(value));

    HostPort w2 = externalCluster_->GetExternalOptions().workerConfigs[w2Index];
    ring.mutable_workers()->erase(w2.ToString());
    DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));

    // 4. w2 will exit because it's not in ring, now the ring is occupied b y w0 and w1
    const int receiveEventDelay = 2;
    std::this_thread::sleep_for(std::chrono::seconds(receiveEventDelay));
    ASSERT_FALSE(cluster_->CheckWorkerProcess(w2Index));
    CheckMaster({ 0, 1 });

    // 5. w2 will be removed from the memory on w0 and w1
    sleep(nodeTimeout_);
    // primary address w2 can be changed if there is location.
    for (auto &key : keys1) {
        DS_ASSERT_OK(client_->Get(key, outVal));
        ASSERT_EQ(outVal, "val");
    }
    // location of w2 is removed
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    for (auto &key : keys2) {
        // no location exist, meta is removed. object nou found
        ASSERT_EQ(client_->Get(key, outVal).GetCode(), K_RUNTIME_ERROR);
    }

    // shut down here and ignore the error code to prevent assertion failure in TearDown because w2 exited by SIGKILL
    (void)cluster_->ShutdownNodes(WORKER);
}

TEST_F(STCScaleDownTest, TaskRetry)
{
    // init 2 nodes
    StartWorkerAndWaitReady({ 0, 1 });
    // simulate an etcd put failure to test if the scale-down task can retry
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "SubmitScaleDownTask.skip", "1*return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.HashRingHealthCheck", "1*call(3000)"));

    // del node
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, 1));
    WaitAllNodesJoinIntoHashRing(1);
}

TEST_F(STCScaleDownTest, LEVEL1_ScaleDownAllNodeThenScaleup)
{
    // 1. init 3 nodes
    const int initNodeCount = 3;
    StartWorkerAndWaitReady({ 0, 1, 2 });
    AssertAllNodesJoinIntoHashRing(initNodeCount);

    InitClients();
    std::vector<std::shared_ptr<KVClient>> clients{ client_, client1_, client2_ };
    int countPerClient = 50;
    for (auto client : clients) {
        for (int i = 0; i < countPerClient; i++) {
            std::string key1 = GetStringUuid();
            std::string value1 = GenPartRandomString();
            DS_ASSERT_OK(client->Set(key1, value1));
            std::string value2 = GenPartRandomString();
            std::string key2 = client->Set(value2);
            ASSERT_TRUE(!key2.empty());
        }
    }

    // 2. Stop all node
    for (int i = 0; i < initNodeCount; i++) {
        kill(cluster_->GetWorkerPid(i), SIGKILL);
    }

    // 3. Scale up 2 nodes
    std::initializer_list<uint32_t> newNodes = { 3, 4 };
    StartWorkerAndWaitReady(newNodes);
    for (auto index : newNodes) {
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(index, workerAddress));
        WaitHashRingChange([&workerAddress](const HashRingPb &hashRing) {
            auto iter = hashRing.workers().find(workerAddress.ToString());
            return iter != hashRing.workers().end() && iter->second.state() == WorkerPb::ACTIVE;
        });
    }
    WaitAllNodesJoinIntoHashRing(newNodes.size());
}

TEST_F(STCScaleDownTest, LEVEL1_NodeTimeoutWhenScaleDownTaskRunning)
{
    // 1. init 3 nodes
    const int initNodeCount = 3;
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    AssertAllNodesJoinIntoHashRing(initNodeCount);

    InitClients();
    std::vector<std::shared_ptr<KVClient>> clients{ client_, client1_, client2_ };
    int countPerClient = 5;
    SetParam param;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    for (auto client : clients) {
        for (int i = 0; i < countPerClient; i++) {
            std::string key1 = GetStringUuid();
            std::string value1 = GenPartRandomString();
            DS_ASSERT_OK(client->Set(key1, value1, param));
            std::string value2 = GenPartRandomString();
            std::string key2 = client->Set(value2, param);
            ASSERT_TRUE(!key2.empty());
        }
        client.reset();
    }

    for (int i = 1; i < initNodeCount; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "master.SelectPrimaryCopy", "sleep(1000)"));
    }
    // 2. Stop node 0
    kill(cluster_->GetWorkerPid(0), SIGKILL);

    // waiting scale down task running.
    sleep(nodeDeadTimeout_ + 1);

    for (int i = 1; i < initNodeCount; i++) {
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(i, workerAddress));
        DS_ASSERT_OK(db_->Delete(ETCD_CLUSTER_TABLE, workerAddress.ToString()));
    }

    for (int i = 1; i < initNodeCount; i++) {
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "master.SelectPrimaryCopy"));
    }

    // shut down here and ignore the error code to prevent assertion failure in TearDown because w2 exited by SIGKILL
    (void)cluster_->ShutdownNodes(WORKER);
}

TEST_F(STCScaleDownTest, TestScaleDownDuringScaleUp)
{
    const int initNodeCount = 3;
    for (int i = 0; i < initNodeCount; i++) {
        DS_ASSERT_OK(externalCluster_->StartWorker(i, HostPort(), " -inject_actions="));
    }
    int waitTimeSec = 2;
    // Make sure the cluster event is received, but the cluster init flag has not been set (set after 5 seconds by
    // default). Therefore, the hash ring is not ready at this time and will not process cluster events.
    sleep(waitTimeSec);
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, 1));
    sleep(nodeDeadTimeout_);
    int remainingNodesCount = 2;
    AssertAllNodesJoinIntoHashRing(remainingNodesCount);
}

class STCScaleDownLivenessTest : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 4; // worker num is 4
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.enableLivenessProbe = true;
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true -liveness_probe_timeout_s=3",
            nodeTimeout_, nodeDeadTimeout_);
        opts.waitWorkerReady = false;
    }

protected:
    const int nodeTimeout_ = 3;
    const int nodeDeadTimeout_ = 5;
};

TEST_F(STCScaleDownLivenessTest, LEVEL2_TestScaleDownDuringScaleUp)
{
    const int initNodeCount = 3;
    StartWorkerAndWaitReady({ 0, 1, 2 });
    AssertAllNodesJoinIntoHashRing(initNodeCount);
    InitClients();
    std::vector<std::shared_ptr<KVClient>> clients{ client_, client1_, client2_ };
    int countPerClient = 50;
    for (auto client : clients) {
        for (int i = 0; i < countPerClient; i++) {
            std::string key1 = GetStringUuid();
            std::string value1 = GenPartRandomString();
            DS_ASSERT_OK(client->Set(key1, value1));
            std::string value2 = GenPartRandomString();
            std::string key2 = client->Set(value2);
            ASSERT_TRUE(!key2.empty());
        }
    }
    ResetClients();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "SubmitScaleDownTask.skip", "return()"));  // worker index is 2
    DS_ASSERT_OK(externalCluster_->KillWorker(0));
    DS_ASSERT_OK(externalCluster_->KillWorker(1));
    sleep(7); // wait 7 s for worker scale down
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "worker.LivenessProbe", "return()")); // worker index is 2
    StartWorkerAndWaitReady({ 3 });
    WaitAllNodesJoinIntoHashRing(1, 60); // wait for 60 s.
}

TEST_F(STCScaleDownLivenessTest, LEVEL1_TestScaleDownScaleUptest)
{
    const int initNodeCount = 3;
    StartWorkerAndWaitReady({ 0, 1, 2 });
    AssertAllNodesJoinIntoHashRing(initNodeCount);
    InitClients();
    std::vector<std::shared_ptr<KVClient>> clients{ client_, client1_, client2_ };
    int countPerClient = 50;
    for (auto client : clients) {
        for (int i = 0; i < countPerClient; i++) {
            std::string key1 = GetStringUuid();
            std::string value1 = GenPartRandomString();
            DS_ASSERT_OK(client->Set(key1, value1));
            std::string value2 = GenPartRandomString();
            std::string key2 = client->Set(value2);
            ASSERT_TRUE(!key2.empty());
        }
    }
    ResetClients();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "SubmitScaleDownTask.skip", "return()"));  // worker index is 2
    DS_ASSERT_OK(externalCluster_->KillWorker(0));
    DS_ASSERT_OK(externalCluster_->KillWorker(1));
    sleep(7);  // wait 7 s for worker scale down
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2,                   // worker index is 2
                                           "WorkerLivenessCheck.GetServicesName.failed",
                                           "return(K_WORKER_ABNORMAL)"));
    StartWorkerAndWaitReady({ 3 });
    WaitAllNodesJoinIntoHashRing(1, 60);  // wait for 60 s.
}

class STCLivenessStartTest : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 4; // worker num is 4
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        opts.enableLivenessProbe = true;
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true -liveness_probe_timeout_s=150",
            nodeTimeout_, nodeDeadTimeout_);
        opts.waitWorkerReady = false;
    }

protected:
    const int nodeTimeout_ = 6;
    const int nodeDeadTimeout_ = 8;
};

TEST_F(STCLivenessStartTest, TestScaleDownScaleUptest)
{
    StartWorkerAndWaitReady({ 0 });
    sleep(nodeDeadTimeout_ + nodeTimeout_);
    InitTestKVClient(0, client_, 2000); // client timeout ms is 2000
}

constexpr int WORKER_NUM = 3;
class STCStandByTest : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.numOBS = 1;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -v=2 -node_timeout_s=1 -node_dead_timeout_s=3 -heartbeat_interval_ms=500"
            " -auto_del_dead_node=true -add_node_wait_time_s=0 -client_reconnect_wait_s=1";
        opts.waitWorkerReady = false;
    }
};

TEST_F(STCStandByTest, LEVEL1_TestSwitchTwoWorker)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    std::shared_ptr<KVClient> kvClient0;
    std::shared_ptr<KVClient> kvClient1;
    InitTestKVClient(0, kvClient0, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    InitTestKVClient(1, kvClient1, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    std::string key = "ooooo";
    std::string key1 = "ppppp";
    std::string value = "Level0_Ttl_001";
    std::string valGet1;
    std::string valGet2;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(kvClient0->Set(key, value, param));
    DS_ASSERT_OK(kvClient0->Set(key1, value, param));
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)");
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    inject::Set("client.switch_worker_end", "call()");
    auto waitSwitch = []() {
        auto sec5s = 5;
        Timer t;
        do {
            if (inject::GetExecuteCount("client.switch_worker_end") == 1) {
                inject::Clear("client.switch_worker_end");
                return;
            }
            auto ms100 = 100;
            std::this_thread::sleep_for(std::chrono::milliseconds(ms100));
        } while (t.ElapsedSecond() < sec5s);
        LOG(ERROR) << "wait switch timeout";
    };
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(0));
    waitSwitch();
    DS_ASSERT_OK(kvClient0->Get(key1, valGet1));
    DS_ASSERT_OK(kvClient1->Get(key, valGet1));
    inject::Set("client.switch_worker_end", "call()");
    StartWorkerAndWaitReady({ 0 });
    waitSwitch();
    DS_ASSERT_OK(kvClient0->Get(key1, valGet1));
    DS_ASSERT_OK(kvClient1->Get(key, valGet1));
    inject::Set("client.switch_worker_end", "call()");
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(1));
    WaitAllNodesJoinIntoHashRing(1);
    AssertAllNodesJoinIntoHashRing(1);
    waitSwitch();
}

TEST_F(STCStandByTest, TestSwitchWorker)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    std::shared_ptr<KVClient> kvClient0;
    std::shared_ptr<KVClient> kvClient1;
    InitTestKVClient(0, kvClient0, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    InitTestKVClient(1, kvClient1, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    std::string key = "ooooo";
    std::string key1 = "ppppp";
    std::string value = "Level0_Ttl_001";
    std::string valGet1;
    std::string valGet2;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(kvClient0->Set(key, value, param));
    DS_ASSERT_OK(kvClient0->Set(key1, value, param));
    externalCluster_->ShutdownNode(WORKER, 0);
    sleep(7);
    DS_ASSERT_OK(kvClient0->Get(key1, valGet1));
    DS_ASSERT_OK(kvClient0->Get(key, valGet1));
}

TEST_F(STCStandByTest, TestSetShmBufferAfterSwitchWorkerReturnsDeprecated)
{
    constexpr int64_t SHM_SIZE = 500 * 1024;
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    std::shared_ptr<KVClient> kvClient0;
    InitTestKVClient(0, kvClient0, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });

    SetParam param;
    std::shared_ptr<Buffer> buffer;
    auto data = GenRandomString(SHM_SIZE);
    DS_ASSERT_OK(kvClient0->Create("shm_buffer_after_switch", data.size(), param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)"));
    inject::Set("client.switch_worker_end", "call()");
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(0));
    Timer timer;
    while (timer.ElapsedSecond() < 5 && inject::GetExecuteCount("client.switch_worker_end") == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    auto switchCount = inject::GetExecuteCount("client.switch_worker_end");
    inject::Clear("client.switch_worker_end");
    ASSERT_GT(switchCount, 0);
    ASSERT_EQ(kvClient0->Set(buffer).GetCode(), K_BUFFER_DEPRECATED);
}

TEST_F(STCStandByTest, TestFunctionAfterSwitchWorker)
{
    constexpr int64_t NON_SHM_SIZE = 50 * 1024;
    constexpr int64_t SHM_SIZE = 500 * 1024;
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    std::shared_ptr<KVClient> kvClient0;
    std::shared_ptr<KVClient> kvClient1;
    InitTestKVClient(0, kvClient0, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        opts.tenantId = "tenant1";
    });
    InitTestKVClient(1, kvClient1, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        opts.tenantId = "tenant1";
    });
    std::string key = "ooooo";
    std::string key1 = "ppppp";
    std::string value = "Level0_Ttl_001";
    std::string valGet1;
    std::string valGet2;
    SetParam param;
    DS_ASSERT_OK(kvClient0->Set(key, value, param));
    DS_ASSERT_OK(kvClient0->Get(key, value));
    std::string keySet = kvClient0->Set("value0", param);
    externalCluster_->ShutdownNode(WORKER, 0);
    sleep(7);
    DS_ASSERT_OK(kvClient0->Set(key1, value, param));
    DS_ASSERT_OK(kvClient0->HealthCheck());
    std::vector<std::string> failedKeys;
    {
        // Create
        std::string keyCreate = "key1_create";
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(kvClient0->Create(keyCreate, NON_SHM_SIZE, param, buffer));
        DS_ASSERT_OK(kvClient0->Set(buffer));
        Optional<Buffer> getBuffer;
        DS_ASSERT_OK(kvClient0->Get(keyCreate, getBuffer));
        Optional<ReadOnlyBuffer> getBuffer1;
        DS_ASSERT_OK(kvClient0->Get(keyCreate, getBuffer1));
    }
    {
        // MCreate
        std::string key1 = "key1";
        std::string key2 = "key2";
        std::vector<std::string> keys{ key1, key2 };
        std::vector<uint64_t> sizes{ NON_SHM_SIZE, SHM_SIZE };
        std::vector<std::shared_ptr<Buffer>> buffers;
        DS_ASSERT_OK(kvClient0->MCreate(keys, sizes, param, buffers));
        DS_ASSERT_OK(kvClient0->MSet(buffers));
        std::vector<std::string> valsToGet;
        DS_ASSERT_OK(kvClient0->Get(keys, valsToGet));
        std::vector<Optional<Buffer>> getBuffer;
        DS_ASSERT_OK(kvClient0->Get(keys, getBuffer));
        std::vector<Optional<ReadOnlyBuffer>> getBuffer1;
        DS_ASSERT_OK(kvClient0->Get(keys, getBuffer1));
        DS_ASSERT_OK(kvClient0->Read({ ReadParam{ .key = key1} }, getBuffer1));
        DS_ASSERT_OK(kvClient0->Del(keys, failedKeys));
    }
    {
        std::vector<std::string> keys;
        std::vector<StringView> values;
        size_t bigSize = 1024UL;
        size_t batchSize = 10'000UL;
        std::vector<std::string> vals;
        for (size_t i = 0; i < batchSize; ++i) {
            keys.emplace_back("key" + std::to_string(i));
            vals.emplace_back(GenRandomString(bigSize));
            values.emplace_back(vals.back());
        }
        DS_ASSERT_OK(kvClient0->MSet(keys, values, failedKeys));
    }
    DS_ASSERT_OK(kvClient0->Set(key, value, param));
    std::vector<uint64_t> outSizes;
    DS_ASSERT_OK(kvClient0->QuerySize({ key }, outSizes));
    std::vector<bool> exists;
    DS_ASSERT_OK(kvClient0->Exist({ key }, exists));
    uint64_t ttlTimeout = 2;
    DS_ASSERT_OK(kvClient0->Expire( { key }, ttlTimeout, failedKeys));
    DS_ASSERT_OK(kvClient0->Del(key));

    std::string accessKey = "accesskey";
    std::string secretKey_ = "secretkey";
    SensitiveValue secretKey(secretKey_);
    DS_ASSERT_OK(kvClient0->UpdateAkSk(accessKey, secretKey));
    std::string token = "qqqqqq";
    SensitiveValue token0(token);
    DS_ASSERT_OK(kvClient0->UpdateToken(token0));
}

constexpr int SCALE_RESTART_ADD_TIME = 3;
class STCRestartTest : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_RESTART_ADD_TIME;
        opts.workerGflagParams =
            " -v=2 -node_timeout_s=2 -node_dead_timeout_s=300"
            " -auto_del_dead_node=true";
        opts.waitWorkerReady = false;
    }
};

TEST_F(STCRestartTest, LEVEL1_RestartDelEtcd)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::string> keys;
    for (int i = 0; i < 10; i++) {
        auto key = "key0_" + std::to_string(i);
        std::string val1;
        keys.emplace_back(key);
        client_->Set(key, "aaaa", param);
        DS_ASSERT_OK(client1_->Get(key, val1));
    }
    HostPort worker1;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1));
    DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                  std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_HASH_SUFFIX));
    externalCluster_->ShutdownNode(WORKER, 1);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    std::vector<std::string> keys1;
    std::stringstream table, table2;
    RangeSearchResult res;
    std::string valueGet;
    table << ETCD_META_TABLE_PREFIX << ETCD_HASH_SUFFIX << "/";
    table2 << ETCD_LOCATION_TABLE_PREFIX << ETCD_HASH_SUFFIX;
    for (auto key : keys) {
        uint32_t hash = MurmurHash3_32(key);
        auto etcdKey = table.str() + master::Hash2Str(hash) + "/" + key;
        DS_ASSERT_OK(db_->RawGet(etcdKey, res));
    }
    for (auto key : keys) {
        DS_ASSERT_OK(client1_->Get(key, valueGet));
    }
    for (auto key : keys) {
        DS_ASSERT_OK(client_->Del(key));
    }
    for (auto key : keys) {
        uint32_t hash = MurmurHash3_32(key);
        auto etcdKey = table.str() + master::Hash2Str(hash) + "/" + key;
        auto locationKey1 = master::Hash2Str(hash) + "/" + worker1.ToString() + "_" + key;
        DS_ASSERT_NOT_OK(db_->RawGet(etcdKey, res));
        DS_ASSERT_NOT_OK(db_->Delete(table2.str(), locationKey1));
    }
}

TEST_F(STCRestartTest, LEVEL1_DeleteWhenMasterTimeout)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1 });
    int timeoutMs = 5000;
    InitTestKVClient(0, client_, timeoutMs);
    InitTestKVClient(1, client1_, timeoutMs);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    // key1: data in worker0, meta in worker0
    // key2: data in worker0, meta in worker1
    std::string key1;
    (void)client_->GenerateKey("", key1);
    std::string key2;
    (void)client1_->GenerateKey("", key2);
    DS_ASSERT_OK(client_->Set(key1, "hello1", param));
    DS_ASSERT_OK(client_->Set(key2, "hello2", param));

    kill(cluster_->GetWorkerPid(1), SIGKILL);

    // waiting scale down task running.
    int nodeTimeoutSec = 3;
    sleep(nodeTimeoutSec);
    DS_ASSERT_NOT_OK(client_->Del(key2));

    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client_->Del({ key1, key2 }, failedIds));
    ASSERT_EQ(failedIds.size(), 1ul);
    ASSERT_EQ(failedIds[0], key2);
}

class STCNetCrashTest : public STCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        rootDir_ = opts.rootDir;
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.numOBS = 1;
        opts.enableSpill = true;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=1 -node_dead_timeout_s=3 -heartbeat_interval_ms=500"
            " -auto_del_dead_node=true"
            " -shared_memory_size_mb=20 ");
        opts.waitWorkerReady = false;
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    }

protected:
    std::string rootDir_;
};

TEST_F(STCNetCrashTest, LEVEL1_ClearWorkerWhenEtcdWorkerNetCrash)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    DS_ASSERT_OK(client_->Set("key", "aaaaa"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(9000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.etcd.netdelay", "1*call(5)"));
    sleep(15);
    AssertAllNodesJoinIntoHashRing(2);
    sleep(5);
    ASSERT_EQ(cluster_->CheckWorkerProcess(0), false);
}

class STCScaleUpDelayTest : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 5;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3";
        opts.waitWorkerReady = false;
    }
};

TEST_F(STCScaleUpDelayTest, LEVEL2_DestNodeNetDelay)
{
    std::string value{ "anyValue" };
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client2_);
    std::vector<std::string> keys;

    for (auto i = 0; i < 100; i++) {
        keys.emplace_back(GetStringUuid());
    }
    std::for_each(keys.begin(), keys.end(),
                  [this](const std::string &key) { DS_ASSERT_OK(client_->Set(key, "another_data_on_worker0")); });

    StartWorkerAndWaitReady({ 3 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 3, "HashRing.NotReceiveUpdate", "10*call(10)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 3, "heartbeat.sleep", "1*sleep(2000)"));
    std::string dbValue;
    auto waitTime = 6;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", dbValue));
    HashRingPb ring;
    ASSERT_TRUE(ring.ParseFromString(dbValue));
    ASSERT_EQ(ring.workers_size(), 4);
    HostPort workerAddr;
    cluster_->GetWorkerAddr(3, workerAddr);
    for (auto &worker : ring.workers()) {
        if (worker.first == workerAddr.ToString()) {
            ASSERT_TRUE(worker.second.state() == WorkerPb::JOINING) << ring.DebugString();
        } else {
            ASSERT_TRUE(worker.second.state() == WorkerPb::ACTIVE);
        }
    }

    sleep(10);
    AssertAllNodesJoinIntoHashRing(4);
    std::for_each(keys.begin(), keys.end(), [this, &value](const std::string &key) {
        DS_ASSERT_OK(client2_->Get(key, value));
        ASSERT_TRUE(value == "another_data_on_worker0");
    });
}

TEST_F(STCScaleUpDelayTest, LEVEL1_RedirectWhenMigrationFailed)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);

    std::vector<std::string> keys;
    size_t objNum = 1000;
    for (size_t i = 0; i < objNum; i++) {
        keys.emplace_back(GetStringUuid());
        DS_ASSERT_OK(client_->Set(keys.back(), "value"));
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.rpc.error", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.HandleFailed.before", "1*sleep(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.HandleFailed.after", "1*sleep(2000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "CheckNeedToRedirectOrNot.delay", "1*sleep(2000)"));
    ASSERT_TRUE(externalCluster_->StartWorker(2, HostPort()).IsOk());  // Scale up worker 2
    sleep(2);                                                          // wait 2s for start worker
    for (size_t i = 0; i < objNum; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        std::this_thread::sleep_for(std::chrono::milliseconds(5));  // Interval is 5 ms
    }
}

class STCRocksdbTest : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3 -node_dead_timeout_s=60";
        opts.waitWorkerReady = false;
    }
};

TEST_F(STCRocksdbTest, RestartAndUpdateMeta)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    std::string value = "anyValue";
    StartWorkerAndWaitReady({ 0, 1, 2 });

    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    std::string key;
    (void)client_->GenerateKey("", key);
    ASSERT_NE(key, "");
    DS_ASSERT_OK(client1_->Set(key, value, param));

    // Trigger write operation to async_worker_op_table through del request.
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));

    // remove rocksdb dir.
    std::string backendStoreDir = GetTestCaseDataDir() + "/worker0/rocksdb";
    std::string backendStoreDir1 = GetTestCaseDataDir() + "/worker0/rocksdb1";
    ASSERT_EQ(rename(backendStoreDir.c_str(), backendStoreDir1.c_str()), 0);
    StartWorkerAndWaitReady({ 0 });
    DS_ASSERT_OK(client1_->Set(key, "qqqqqqq"));
}

class STCCentralMasterrScaleTest : public STCScaleUpTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams =
            FormatString(" -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d", timeoutS_, deadTimeoutS_);
        opts.waitWorkerReady = false;
        std::string hostIp = "127.0.0.1";
        for (uint32_t i = 0; i < opts.numWorkers; i++) {
            int port = GetFreePort();
            opts.workerConfigs.emplace_back(hostIp, port);
        }
        worker2Addr = opts.workerConfigs[2].ToString();  // worker 2 address.
    }

protected:
    int timeoutS_ = 3;
    int deadTimeoutS_ = 8;
    std::string worker2Addr;
};

TEST_F(STCCentralMasterrScaleTest, RetryWhenEtcdWatchDelay)
{
    StartWorkerAndWaitReady({ 0, 1 });
    int worker0Index = 0, worker2Index = 2;
    std::shared_ptr<KVClient> client1, client2;
    InitTestKVClient(worker0Index, client1);
    std::string exec = "10*call(" + worker2Addr + ")";

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdClusterManager.HandleNodeAdditionEvent.delay", exec));
    StartWorkerAndWaitReady({ 2 });
    InitTestKVClient(worker2Index, client2);
    auto key = "qqqq";
    DS_ASSERT_OK(client2->Set(key, "mmmm"));
    std::string val;
    DS_ASSERT_OK(client1->Get(key, val));
}

class STCTestNotWait : public STCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.numOBS = 1;
        opts.addNodeTime = 60;  // add node wait time is 60
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -v=2 -node_timeout_s=1 -node_dead_timeout_s=3 -heartbeat_interval_ms=500"
            " -auto_del_dead_node=true  -inject_actions=test.start.notWait:call(3)";
        opts.waitWorkerReady = false;
    }
};

TEST_F(STCTestNotWait, StartWorkerNotWait)
{
    StartWorkerAndWaitReady({ 0, 1 }, 6);  // worker index is 0, 1, wait time is 6s
    int worker0Index = 0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(worker0Index, client1);
    auto key = "qqqq";
    DS_ASSERT_OK(client1->Set(key, "mmmm"));
    std::string val;
    DS_ASSERT_OK(client1->Get(key, val));
}

class STCWorkerStartFail : public STCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=2 -node_timeout_s=120 -node_dead_timeout_s=1800";
        opts.waitWorkerReady = false;
        std::string hostIp = "127.0.0.1";
        const int redisPort = GetFreePort(55'535);  // Redis port number must be lower than 55535.
        opts.workerConfigs.emplace_back(hostIp, redisPort);
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
    }

    void SetUp() override
    {
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

protected:
    const int workerNum_ = 2;
};

TEST_F(STCWorkerStartFail, AddressAlreadyInUse)
{
    DS_ASSERT_OK(externalCluster_->StartWorker(0, HostPort()));
    DS_ASSERT_OK(externalCluster_->StartWorker(1, HostPort()));
    DS_ASSERT_OK(externalCluster_->WaitNodeReady(WORKER, 1, 20));  // Wait 20s for w1 ready.
    std::shared_ptr<KVClient> client;
    InitTestKVClient(1, client, 2000);  // Init client1 to w1 with 2000ms timeout
    int cnt = 100;
    std::vector<std::string> keys;
    for (int i = 0; i < cnt; i++) {
        keys.emplace_back(GenRandomString());
        DS_ASSERT_OK(client->Set(keys.back(), GenRandomString()));
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client->Del(keys, failedKeys));
    ASSERT_TRUE(failedKeys.empty());
}

class STCScaleUpRedirectTest : public STCScaleUpTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3; // worker num is 3
        opts.addNodeTime = 0;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            FormatString(" -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d", timeoutS_, deadTimeoutS_);
        opts.waitWorkerReady = false;
        std::string hostIp = "127.0.0.1";
        for (uint32_t i = 0; i < opts.numWorkers; i++) {
            int port = GetFreePort();
            opts.workerConfigs.emplace_back(hostIp, port);
        }
        worker2Addr = opts.workerConfigs[2].ToString();  // worker 2 address.
    }

protected:
    int timeoutS_ = 3;
    int deadTimeoutS_ = 8;
    std::string worker2Addr;
};


TEST_F(STCScaleUpRedirectTest, ScaleUpDelayRedirect)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    InitTestKVClient(1, client1_);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "HashRing.UpdateRing.sleep", "sleep(2000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "BatchMigrateMetadata.delay", "sleep(2000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "sleep(2000)"));
    StartWorkerAndWaitReady({ 2 });
    Timer timer;
    while (timer.ElapsedSecond() < 3) {  // set 3 s for scale up
        auto key = GetStringUuid();
        DS_ASSERT_OK(client1_->Set(key, "oooooo"));
        std::this_thread::sleep_for(std::chrono::milliseconds(10));  // wait interval is 10 ms
    }
}

TEST_F(STCScaleUpTest, MSetNtx_RequestTimeoutMs_WorkerToMasterUnavailable)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });

    const int32_t requestTimeoutMs = 10 * 1000;
    const int32_t connectTimeoutMs = 20 * 1000;
    InitTestKVClient(0, client_, [&requestTimeoutMs](ConnectOptions &opts) {
        opts.connectTimeoutMs = connectTimeoutMs;
        opts.requestTimeoutMs = requestTimeoutMs;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    InitTestKVClient(1, client1_, [&requestTimeoutMs](ConnectOptions &opts) {
        opts.connectTimeoutMs = connectTimeoutMs;
        opts.requestTimeoutMs = requestTimeoutMs;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });

    constexpr size_t kBatchSize = 32;
    constexpr int kValSize = 10 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> vals;
    std::vector<StringView> values;
    vals.reserve(kBatchSize);
    for (size_t i = 0; i < kBatchSize; ++i) {
        vals.emplace_back(GenRandomString(kValSize));
        values.emplace_back(vals[i]);
        keys.emplace_back(client_->GenerateKey());
    }

    const std::string injectAction = "3000*return(K_RPC_UNAVAILABLE)";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.CreateMultiMeta.begin", injectAction));

    MSetParam msetParam{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::vector<std::string> outFailedKeys;

    auto start = std::chrono::high_resolution_clock::now();
    Status setRc = client1_->MSet(keys, values, outFailedKeys, msetParam);
    auto end = std::chrono::high_resolution_clock::now();
    const auto durationSec = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

    ASSERT_NE(setRc.ToString().find("RPC unavailable"), std::string::npos);
    ASSERT_GE(durationSec, 8);
    ASSERT_LE(durationSec, 11);
}
}  // namespace st
}  // namespace datasystem
