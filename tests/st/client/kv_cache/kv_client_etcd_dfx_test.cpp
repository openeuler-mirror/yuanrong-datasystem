/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: State client etcd dfx tests.
 */

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include "client/kv_cache/kv_client_scale_common.h"
#include "cluster/base_cluster.h"
#include "common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/kv_cache/kv_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {
class KVClientEtcdDfxTest : public KVClientScaleCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerNum_;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.enableDistributedMaster = enableDistributedMaster_;
        opts.workerGflagParams =
            FormatString(" -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d", timeoutS_, deadTimeoutS_);
        opts.injectActions = "worker.PreShutDown.skip:return(K_OK)";
        opts.waitWorkerReady = false;
    }

protected:
    void BasicPutGetDel(const std::shared_ptr<KVClient> &client)
    {
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
        std::string keyPrefix = "Level0", value = "test", modifyValue = "test2";
        std::vector<std::string> keys;
        size_t keyNum = 100;
        for (size_t i = 0; i < keyNum; i++) {
            std::string key = keyPrefix + std::to_string(i);
            keys.emplace_back(key);
            DS_ASSERT_OK(client->Set(key, value, param));
        }

        std::vector<std::string> valsToGet;
        DS_ASSERT_OK(client->Get(keys, valsToGet));
        ASSERT_EQ(valsToGet.size(), keyNum);
        for (auto valToGet : valsToGet) {
            ASSERT_EQ(valToGet, value);
        }

        for (auto &key : keys) {
            DS_ASSERT_OK(client->Set(key, modifyValue));
            std::string valToGet;
            DS_ASSERT_OK(client->Get(key, valToGet));
            ASSERT_EQ(valToGet, modifyValue);
        }

        std::vector<std::string> failedKeys;
        DS_ASSERT_OK(client->Del(keys, failedKeys));
        ASSERT_EQ(failedKeys.size(), 0);
    }

    int timeoutS_ = 2;
    int deadTimeoutS_ = 3;
    ExternalCluster *externalCluster_ = nullptr;
    const int maxWaitTimeSec_ = 20;
    const size_t workerNum_ = 5;
    std::string enableDistributedMaster_ = "true";
};

TEST_F(KVClientEtcdDfxTest, LEVEL1_TestEtcdRestart)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    sleep(deadTimeoutS_ + 1);

    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string key_name = "Level0", value = "test", modifyValue = "test2";
    std::vector<std::string> keys;
    size_t keyNum = 100;
    for (size_t i = 0; i < keyNum; i++) {
        std::string key = key_name + std::to_string(i);
        keys.emplace_back(key);
        DS_ASSERT_OK(client->Set(key, value, param));
    }

    std::vector<std::string> valsToGet;
    DS_ASSERT_OK(client->Get(keys, valsToGet));
    ASSERT_EQ(valsToGet.size(), keyNum);
    for (auto valToGet : valsToGet) {
        ASSERT_EQ(valToGet, value);
    }

    for (auto key : keys) {
        DS_ASSERT_OK(client->Set(key, modifyValue));
        std::string valToGet;
        DS_ASSERT_OK(client->Get(key, valToGet));
        ASSERT_EQ(valToGet, modifyValue);
    }

    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());

    DS_ASSERT_OK(client->Get(keys, valsToGet));
    ASSERT_EQ(valsToGet.size(), keyNum);
    for (auto valToGet : valsToGet) {
        ASSERT_EQ(valToGet, modifyValue);
    }
    client.reset();

    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 2 }));

    int workerIndex2 = 2;
    InitTestKVClient(workerIndex2, client);
    DS_ASSERT_OK(client->Get(keys, valsToGet));
    ASSERT_EQ(valsToGet.size(), keyNum);
    for (auto valToGet : valsToGet) {
        ASSERT_EQ(valToGet, modifyValue);
    }
    client.reset();
}

TEST_F(KVClientEtcdDfxTest, DISABLED_TestWatchEventLost)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1 }, " -inject_actions=EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly:call(100)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdWatch.StoreEvents.IgnoreEvent", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "EtcdWatch.StoreEvents.IgnoreEvent", "return()"));

    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 2 }));

    int waitTime = SCALE_UP_ADD_TIME + 2;
    int workerNum = 3;
    WaitAllNodesJoinIntoHashRing(workerNum, waitTime);
    CheckMaster({ 0, 1, 2 });
}

TEST_F(KVClientEtcdDfxTest, TestEtcdCommitFailed)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({1}));
    DS_ASSERT_NOT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0 },
        " -inject_actions=etcd.txn.commit:return(K_TRY_AGAIN);HashRing.InitWithEtcd.MotifyWaitTimeMs:call(3000)"));
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 2 }, " -inject_actions=etcd.txn.commit:30*return(K_TRY_AGAIN) "));
}

TEST_F(KVClientEtcdDfxTest, DISABLED_TestErrorMsgWhenEtcdFailed)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::string key_name = "Level0", value = "test";
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    auto status = client->Set(key_name, value, param);
    ASSERT_TRUE(status.ToString().find("Put::etcd_kv_Put") != std::string::npos);
}

TEST_F(KVClientEtcdDfxTest, DISABLED_LEVEL1_TestRestartWorkerDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    DS_ASSERT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 0 }));

    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    BasicPutGetDel(client);
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
}

// Test the scenario where reconciliation cannot be completed during the restart process
TEST_F(KVClientEtcdDfxTest, LEVEL1_TestRestartWorkerDuringEtcdCrash2)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    DS_ASSERT_NOT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 0 }));
}

TEST_F(KVClientEtcdDfxTest, LEVEL1_TestScaleUpWorkerDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    int maxWaitTimeSec = 10;
    DS_ASSERT_NOT_OK(externalCluster_->StartWorkerAndWaitReady({ 3 }, maxWaitTimeSec));
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
}

class KVClientEtcdDfxTestAdjustNodeTimeout : public KVClientEtcdDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        timeoutS_ = 60;  // 60s
        deadTimeoutS_ = 300;  // 300s
        KVClientEtcdDfxTest::SetClusterSetupOptions(opts);
        opts.disableRocksDB = false;
        opts.injectActions = "GetLeaseRenewIntervalMs:return(1000)";
    }
};

TEST_F(KVClientEtcdDfxTestAdjustNodeTimeout, TestSetHealthProbe)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(externalCluster_->StartWorker(1, HostPort(),
                                               " -inject_actions=worker.RunKeepAliveTask:return(K_RPC_UNAVAILABLE)"));
    int waitReconciliationSec = 5;
    DS_ASSERT_NOT_OK(externalCluster_->WaitNodeReady(WORKER, 1, waitReconciliationSec));
   
    DS_ASSERT_OK(externalCluster_->SetInjectAction(WORKER, 1, "WorkerOCServiceImpl.Reconciliation.SkipWait", "call()"));
    DS_ASSERT_OK(externalCluster_->ClearInjectAction(WORKER, 1, "worker.RunKeepAliveTask"));
    DS_ASSERT_OK(externalCluster_->WaitNodeReady(WORKER, 1, waitReconciliationSec));
}

TEST_F(KVClientEtcdDfxTestAdjustNodeTimeout, TestRestartDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());

    auto val = "val";

    std::vector<std::string> keysPerRestart;
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);
    keysPerRestart.emplace_back(client0->GenerateKey());
    ASSERT_NE(keysPerRestart.back(), "");
    keysPerRestart.emplace_back("per_restart");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);
    for (const auto &key : keysPerRestart) {
        DS_ASSERT_OK(client1->Set(key, val));
    }
    client1.reset();

    DS_ASSERT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 1 }));

    InitTestKVClient(1, client1);

    std::vector<std::string> keysPostRestart;
    keysPostRestart.emplace_back(client0->GenerateKey());
    ASSERT_NE(keysPostRestart.back(), "");
    keysPostRestart.emplace_back("post_restart");
    for (const auto &key : keysPostRestart) {
        DS_ASSERT_OK(client1->Set(key, val));
    }

    DS_ASSERT_OK(externalCluster_->SetInjectAction(WORKER, 1, "WorkerOCServiceImpl.Reconciliation.SkipWait", "call()"));
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());

    int waitReconciliationSec = 5;
    sleep(waitReconciliationSec);

    for (const auto &key : keysPerRestart) {
        std::string valToGet;
        DS_ASSERT_NOT_OK(client0->Get(key, valToGet));
    }

    for (const auto &key : keysPostRestart) {
        std::string valToGet;
        // For the keys set after the restart, it is expected that they can be obtained, but this logic is not yet
        // implemented
        DS_ASSERT_NOT_OK(client0->Get(key, valToGet));
    }
}

class KVClientEtcdDfxCentralizedMasterTest : public KVClientEtcdDfxTest {
public:
    void SetUp() override
    {
        KVClientEtcdDfxTest::enableDistributedMaster_ = "false";
        KVClientEtcdDfxTest::SetUp();
    }
};

TEST_F(KVClientEtcdDfxCentralizedMasterTest, LEVEL1_TestRestartWorkerDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    int maxWaitTimeSec = 10;
    DS_ASSERT_NOT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 0 }, maxWaitTimeSec));
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
}

class KVClientEtcdDfxCrossAzTest : public KVClientEtcdDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerNum_;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.enableDistributedMaster = enableDistributedMaster_;
        opts.disableRocksDB = false;
        opts.workerGflagParams = FormatString(
            " -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d -other_az_names=AZ1,AZ2 "
            "-cross_az_get_meta_from_worker=true",
            timeoutS_, deadTimeoutS_);
        for (size_t i = 0; i < workerNum_; i++) {
            std::string param = "-az_name=" + azNames_[i % azNames_.size()];
            opts.workerSpecifyGflagParams[i] += param;
        }
    }
private:
    std::vector<std::string> azNames_ = {"AZ1", "AZ2"};
};

TEST_F(KVClientEtcdDfxCrossAzTest, LEVEL1_TestRestartWorkerDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);
    auto keyWithWorkerId = client1->GenerateKey("key");
    ASSERT_TRUE(!keyWithWorkerId.empty());
    std::string val = "val";
    DS_ASSERT_OK(client1->Set(keyWithWorkerId, val));
    client1.reset();

    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());

    int maxWaitTimeSec = 10;
    DS_ASSERT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 0 }, maxWaitTimeSec));
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);
    std::string valToGet;
    DS_ASSERT_OK(client0->Get(keyWithWorkerId, valToGet));
    ASSERT_EQ(valToGet, val);
    client0.reset();

    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
}

class KVClientEtcdDfxKeepAliveTest : public KVClientEtcdDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerNum_;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.enableDistributedMaster = enableDistributedMaster_;
        opts.workerGflagParams = FormatString(
            " -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d -other_az_names=AZ1,AZ2 "
            "-cross_az_get_meta_from_worker=true",
            timeoutS_, deadTimeoutS_);
        for (size_t i = 0; i < workerNum_; i++) {
            std::string param = "-az_name=" + azNames_[i % azNames_.size()];
            opts.workerSpecifyGflagParams[i] += param;
        }
    }

private:
    std::vector<std::string> azNames_ = {"AZ1", "AZ2"};
    int timeoutS_ = 20;
    int deadTimeoutS_ = 60;
    size_t workerNum_ = 3;
};

TEST_F(KVClientEtcdDfxKeepAliveTest, LEVEL1_TestEtcdKeepAlive)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.KeepAlive.send", "return(10)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "GetLeaseIDWithReconnectIfError.Error", "return(K_RPC_UNAVAILABLE)"));
    sleep(15);  // wait 15 s worker not timeout
    auto keyWithWorkerId = client1->GenerateKey();
    DS_ASSERT_OK(client1->Set(keyWithWorkerId, "aaaaa"));
    sleep(10);  // wait 10 for timeout
    DS_ASSERT_NOT_OK(client1->Set(keyWithWorkerId, "bbbbbb"));
}
}  // st
}  // datasystem