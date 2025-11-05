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
 * Description: State client multi replica tests.
 */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>

#include <gtest/gtest.h>

#include "common.h"
#include "common_distributed_ext.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv_client.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
class STCReplicaTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3 -enable_meta_replica=true";
        opts.waitWorkerReady = false;
        opts.disableRocksDB = false;
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void TearDown() override
    {
        ResetClients();
        ExternalClusterTest::TearDown();
    }

    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    void VoluntaryScaleDownInject(int workerIdx)
    {
        std::string checkFilePath = FLAGS_log_dir.c_str();
        std::string client = "client";
        checkFilePath = checkFilePath.substr(0, checkFilePath.length() - client.length()) + "/worker"
                        + std::to_string(workerIdx) + "/log/worker-status";
        std::ofstream ofs(checkFilePath);
        if (!ofs.is_open()) {
            LOG(ERROR) << "Can not open worker status file in " << checkFilePath
                       << ", voluntary scale in will not start, errno: " << errno;
        } else {
            ofs << "voluntary scale in\n";
        }
        ofs.close();
        kill(cluster_->GetWorkerPid(workerIdx), SIGTERM);
    }

    void StartWorkerAndWaitReady(std::initializer_list<int> indexes,
                                 const std::unordered_map<int, std::string> &workerFlags = {}, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            std::string flags;
            auto iter = workerFlags.find(i);
            if (iter != workerFlags.end()) {
                flags = " " + iter->second;
            }
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort(), flags).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
        for (auto i : indexes) {
            // When the scale-in scenario is tested, the scale-in failure may not be determined correctly.
            // Therefore, the scale-in failure is directly exited.
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "Hashring.Scaletask.Fail", "abort()"));
        }
        InitWorkersInfoMap(indexes);
    }

    void StartWorkerAndWaitReady(std::initializer_list<int> indexes, const std::string &flags, int maxWaitTimeSec = 20)
    {
        std::unordered_map<int, std::string> workerFlags;
        for (auto i : indexes) {
            workerFlags.emplace(i, flags);
        }
        StartWorkerAndWaitReady(indexes, workerFlags, maxWaitTimeSec);
    }

    void RestartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int signal = SIGTERM)
    {
        for (auto i : indexes) {
            if (signal == SIGTERM) {
                ASSERT_TRUE(cluster_->ShutdownNode(WORKER, i).IsOk()) << i;
            } else {
                ASSERT_TRUE(externalCluster_->KillWorker(i).IsOk()) << i;
            }
        }

        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->StartNode(WORKER, i, "").IsOk()) << i;
        }
        int maxWaitTimeSec = 40;  // need reconliation
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
    }

    void InitClients(int count)
    {
        for (int i = 0; i < count; i++) {
            std::shared_ptr<KVClient> client;
            InitTestKVClient(i, client);
            clients_.emplace_back(std::move(client));
        }
    }

    void InitClients(std::vector<int> indexs)
    {
        for (auto index : indexs) {
            std::shared_ptr<KVClient> client;
            InitTestKVClient(index, client);
            clients_.emplace_back(std::move(client));
        }
    }

    void ResetClients()
    {
        clients_.clear();
    }

    template <typename Func>
    void UntilTrueOrTimeout(Func &&func, uint64_t timeoutMs = RETRY_TIMEOUT_MS)
    {
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < timeOut) {
            std::string value;
            if (func()) {
                return;
            }
            const int interval = 1000;  // 1000ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        ASSERT_TRUE(false) << "Timeout";
    }

    std::vector<std::vector<std::string>> RandomGroup(std::vector<std::string> keys, int count)
    {
        if (count == 0) {
            return {};
        }
        std::vector<std::vector<std::string>> groupKeys;
        std::mt19937 gen(std::random_device{}());
        std::shuffle(std::begin(keys), std::end(keys), gen);
        size_t groupCount = keys.size() / count;
        if (keys.size() % count != 0) {
            groupCount++;
        }
        groupKeys.resize(groupCount);
        for (size_t i = 0; i < keys.size(); i++) {
            const auto &key = keys[i];
            groupKeys[i / count].emplace_back(key);
        }
        return groupKeys;
    }

    void BasicPut(int count, std::unordered_map<std::string, std::string> &keyValues, const std::string &keyPrefix = "")
    {
        SetParam param;
        param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
        auto prefix = keyPrefix.empty() ? "" : keyPrefix + "-";
        for (auto &client : clients_) {
            for (auto i = 0; i < count; i++) {
                std::string key1 = prefix + GetStringUuid();
                std::string value1 = GenPartRandomString();
                DS_ASSERT_OK(client->Set(key1, value1, param));
                std::string value2 = GenPartRandomString();
                std::string key2;
                (void)client->GenerateKey("", key2);
                ASSERT_TRUE(!key2.empty());
                key2 = prefix + key2;
                DS_ASSERT_OK(client->Set(key2, value2, param));
                keyValues.emplace(key1, value1);
                keyValues.emplace(key2, value2);
            }
        }
    }

    void BasicGet(int batchNo, std::unordered_map<std::string, std::string> &keyValues)
    {
        std::vector<std::string> keys;
        for (const auto &kv : keyValues) {
            keys.emplace_back(kv.first);
        }

        auto groupKeys = RandomGroup(keys, batchNo);
        size_t index = 0;
        for (const auto &keys : groupKeys) {
            auto client = clients_[index % clients_.size()];
            std::vector<std::string> vals;
            DS_ASSERT_OK(client->Get(keys, vals));
            for (size_t k = 0; k < keys.size(); k++) {
                const auto &key = keys[k];
                ASSERT_EQ(vals[k], keyValues[key]) << "key:" << key;
            }
            index++;
        }
    }

    void BasicDel(int batchNo, std::unordered_map<std::string, std::string> &keyValues)
    {
        std::vector<std::string> keys;
        for (const auto &kv : keyValues) {
            keys.emplace_back(kv.first);
        }

        auto groupKeys = RandomGroup(keys, batchNo);
        size_t index = 0;
        for (const auto &keys : groupKeys) {
            auto client = clients_[index % clients_.size()];
            std::vector<std::string> failedIds;
            DS_ASSERT_OK(client->Del(keys, failedIds));
            ASSERT_EQ(0ul, failedIds.size());
            index++;
        }
    }

    void BasicPutGetDel(int batchNo)
    {
        const int countPerClient = 5;
        std::unordered_map<std::string, std::string> keyValues;
        BasicPut(countPerClient, keyValues);
        BasicGet(batchNo, keyValues);
        BasicDel(batchNo, keyValues);
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::vector<std::shared_ptr<KVClient>> clients_;
};

class STCReplicaRouterTest : public STCReplicaTest {};

TEST_F(STCReplicaRouterTest, DISABLED_BasicPutGetDelTest)
{
    // replica layout:
    // worker0: { primary: [worker0], backup: [worker1] }
    // worker1: { primary: [worker1, worker2], backup: []}
    // worker2: { primary: [], backup: [worker0, worker2]}
    StartWorkerAndWaitReady({ 0, 1, 2 }, { { 2, " -inject_actions=worker.ClusterInitFinish:return()" } });
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);
    // remove sleep after add retry logic.
    int timeout = 3;
    sleep(timeout);
    const int clientCount = 3;
    InitClients(clientCount);

    int batchNo = 1;
    BasicPutGetDel(batchNo++);
    BasicPutGetDel(batchNo++);
    BasicPutGetDel(batchNo++);
}

class STCReplicaSwitchTest : public STCReplicaTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        STCReplicaTest::SetClusterSetupOptions(opts);
        opts.injectActions = "GetLeaseRenewIntervalMs:return(1000)";
    }
};

TEST_F(STCReplicaSwitchTest, DISABLED_LEVEL1_TestWorkerRestart)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, "-node_timeout_s=60");
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);
    // restart worker0
    RestartWorkerAndWaitReady({ 0 });
    // waiting replica switch.
    WaitReplicaNotInCurrentNode(0);
    InitClients({ 0, 1, 2 });
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, DISABLED_LEVEL1_TestWorkerCrashAndStartBeforeSwitch)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, "-node_timeout_s=60");
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);
    // worker0 crash and start.
    RestartWorkerAndWaitReady({ 0 }, SIGILL);
    // replica not switch.
    WaitReplicaInCurrentNode(0);
    InitClients({ 0, 1, 2 });
    BasicPutGetDel(1);

    (void)cluster_->ShutdownNodes(ClusterNodeType::WORKER);
}

TEST_F(STCReplicaSwitchTest, DISABLED_LEVEL1_TestWorkerCrashAndStartAfterSwitchThenSwitchBack)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, "-node_timeout_s=5");
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);
    // worker0 crash and start.
    DS_ASSERT_OK(externalCluster_->KillWorker(0));
    // wait replica switch.
    WaitReplicaNotInCurrentNode(0);
    int delaySec = 3;
    sleep(delaySec);
    // start worker0.
    const int nodeReadyTimeout = 30;  // 30s.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0, nodeReadyTimeout));

    std::vector<int> workerIndexes = { 0, 1, 2 };
    InitClients(workerIndexes);
    BasicPutGetDel(1);

    InjectSyncCap(workerIndexes, 1, 1);
    WaitReplicaInCurrentNode(0);

    (void)cluster_->ShutdownNodes(ClusterNodeType::WORKER);
}

TEST_F(STCReplicaSwitchTest, LEVEL1_TestWorkerNetworkRecoveryBeforeSwitch)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, "-node_timeout_s=60");

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RunKeepAliveTask", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.KeepAlive.send", "return(3000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetLeaseExpiredMs", "return(3000)"));
    InitClients({ 0, 1, 2 });

    // worker0 switch replica to backup.
    UntilTrueOrTimeout([this] {
        std::string key;
        (void)clients_[0]->GenerateKey("", key);
        Status rc = clients_[1]->Set(key, "hi");
        return rc.IsError();
    });

    // network recovery
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.RunKeepAliveTask"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.KeepAlive.send"));

    // worker0 switch replica to primary.
    UntilTrueOrTimeout([this] {
        std::string key;
        (void)clients_[0]->GenerateKey("", key);
        Status rc = clients_[1]->Set(key, "hi");
        return rc.IsOk();
    });

    WaitReplicaInCurrentNode(0);
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, LEVEL2_TestWorkerNetworkRecoveryAfterSwitch)
{
    const int nodeTimeout = 5;
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString("-node_timeout_s=%d", nodeTimeout));
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RunKeepAliveTask", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.KeepAlive.send", "return(3000)"));

    // wait replica switch.
    WaitReplicaNotInCurrentNode(0);

    // network recovery
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.RunKeepAliveTask"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.KeepAlive.send"));

    sleep(nodeTimeout);
    InitClients({ 0, 1, 2 });
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestWorkerVoluntaryScaleDown)
{
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    StartWorkerAndWaitReady({ 0, 1, 2 },
                            FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d", nodeTimeout, nodeDeadTimeoutS));
    InitClients({ 0, 1, 2 });
    std::vector<std::string> objKeys;
    auto value = "datta";
    for (int i = 0; i < 30; i++) {  // obj num is 30
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        DS_ASSERT_OK(clients_[0]->Set(key, value));
    }
    // wait replica switch.
    clients_[0].reset();
    VoluntaryScaleDownInject(0);
    WaitChangeReplicaInCurrentNode(1, workersInfo_[0].uuid);  // worker index is 1
    WaitChangeReplicaInCurrentNode(2, workersInfo_[0].uuid);  // worker index is 2
    std::string etcdValue;
    DS_ASSERT_NOT_OK(etcd_->Get(ETCD_REPLICA_GROUP_TABLE, workersInfo_[0].uuid, etcdValue));
    for (const auto &id : objKeys) {
        std::string data;
        DS_ASSERT_OK(clients_[1]->Get(id, data));
        ASSERT_EQ(data, value);
    }
    clients_.erase(clients_.begin());
    sleep(nodeTimeout);
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestWorkerVoluntaryScaleDownAfterSwitch)
{
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    StartWorkerAndWaitReady(
        { 0, 1, 2 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    InitClients({ 0, 1, 2 });
    RestartWorkerAndWaitReady({ 0 });
    // waiting replica switch.
    WaitReplicaNotInCurrentNode(0);
    std::vector<std::string> objKeys;
    auto value = "datta";
    for (int i = 0; i < 30; i++) {  // obj num is 30
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        DS_ASSERT_OK(clients_[0]->Set(key, value));
    }
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "ReplicaManager.locationCheckInterval", "call()"));  // worker index is 1
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 2, "ReplicaManager.locationCheckInterval", "call()"));  // worker index is 2
                                                                                                  // worker index is 1
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "ReplicaManager.CheckBeforeSwitchReplica", "return(K_OK)"));
    // worker index is 2
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "ReplicaManager.CheckBeforeSwitchReplica", "return(K_OK)"));
    clients_[0].reset();
    VoluntaryScaleDownInject(0);
    WaitChangeReplicaInCurrentNode(1, workersInfo_[0].uuid);  // worker index is 1
    WaitChangeReplicaInCurrentNode(2, workersInfo_[0].uuid);  // worker index is 2
    std::string etcdValue;
    DS_ASSERT_NOT_OK(etcd_->Get(ETCD_REPLICA_GROUP_TABLE, workersInfo_[0].uuid, etcdValue));
    clients_.erase(clients_.begin());
    sleep(nodeTimeout);
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestWorkerScaleDown)
{
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    StartWorkerAndWaitReady(
        { 0, 1, 2 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    WaitReplicaLocationMatch({ 0, 1, 2 });
    InitClients({ 0, 1, 2 });
    std::vector<std::string> objKeys;
    auto value = "datta";
    for (int i = 0; i < 30; i++) {  // obj num is 30
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        DS_ASSERT_OK(clients_[2]->Set(key, value));  // client index is 2
    }
    // wait replica switch.
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    WaitChangeReplicaInCurrentNode(1, workersInfo_[0].uuid);  // worker index is 1
    WaitChangeReplicaInCurrentNode(2, workersInfo_[0].uuid);  // worker index is 2
    std::string etcdValue;
    DS_ASSERT_NOT_OK(etcd_->Get(ETCD_REPLICA_GROUP_TABLE, workersInfo_[0].uuid, etcdValue));
    clients_.erase(clients_.begin());
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestWorkersScaleDown)
{
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    StartWorkerAndWaitReady(
        { 0, 1, 2, 3 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    WaitReplicaLocationMatch({ 0, 1, 2 });
    InitClients({ 0, 1, 2, 3 });
    std::vector<std::string> objKeys;
    auto value = "datta";
    for (int i = 0; i < 30; i++) {  // obj num is 30
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        DS_ASSERT_OK(clients_[0]->Set(key, value));
    }
    // wait replica switch.
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    WaitChangeReplicaInCurrentNode(2, workersInfo_[0].uuid);  // worker index is 2
    WaitChangeReplicaInCurrentNode(3, workersInfo_[0].uuid);  // worker index is 3
    clients_.erase(clients_.begin());
    clients_.erase(clients_.begin());
    sleep(nodeTimeout);
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestOneWorkerScaleDownAfterSwithReplica)
{
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    StartWorkerAndWaitReady(
        { 0, 1, 2 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    InitClients({ 0, 1, 2 });
    RestartWorkerAndWaitReady({ 0 });
    // waiting replica switch.
    WaitReplicaNotInCurrentNode(0);
    std::vector<std::string> objKeys;
    auto value = "datta";
    for (int i = 0; i < 30; i++) {  // obj num is 30
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        DS_ASSERT_OK(clients_[0]->Set(key, value));
    }
    // wait replica switch.
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    WaitChangeReplicaInCurrentNode(1, workersInfo_[0].uuid);  // worker index is 1
    WaitChangeReplicaInCurrentNode(2, workersInfo_[0].uuid);  // worker index is 2
    std::string etcdValue;
    DS_ASSERT_NOT_OK(etcd_->Get(ETCD_REPLICA_GROUP_TABLE, workersInfo_[0].uuid, etcdValue));
    clients_.erase(clients_.begin());
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, TestAfterScaleupAdjustReplicaFromSingleNode)
{
    StartWorkerAndWaitReady({ 0 });
    LOG(INFO) << "check after cluster init.";
    StartWorkerAndWaitReady({ 1 });
    LOG(INFO) << "check after scale up finish.";
    WaitReplicaLocationMatch({ 0, 1 });
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestWorkerScaleUp)
{
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    StartWorkerAndWaitReady(
        { 0, 1, 2 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    InitClients({ 0, 1, 2 });
    std::vector<std::string> objKeys;
    auto value = "datta";
    for (int i = 0; i < 40; i++) {  // obj num is 40
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        DS_ASSERT_OK(clients_[0]->Set(key, value));
    }
    // wait replica switch.
    StartWorkerAndWaitReady(
        { 3 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    WaitReplicaLocationMatch({ 0, 1, 2, 3 });
    std::vector<int> index = { 3 };
    InitClients(index);
    for (const auto &objKey : objKeys) {
        std::string data;
        DS_ASSERT_OK(clients_[1]->Get(objKey, data));
        ASSERT_EQ(data, value);
    }
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestOneWorkerScaleUpAfterSwithReplica)
{
    const int nodeTimeout = 5;
    const int nodeDeadTimeoutS = 8;
    StartWorkerAndWaitReady(
        { 0, 1 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    WaitReplicaLocationMatch({ 0, 1 });
    // restart worker0
    RestartWorkerAndWaitReady({ 0 });
    // waiting replica switch.
    WaitReplicaNotInCurrentNode(0);
    InitClients({ 0, 1 });
    std::vector<std::string> objKeys;
    auto value = "datta";
    for (int i = 0; i < 10; i++) {  // obj num is 10
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        DS_ASSERT_OK(clients_[0]->Set(key, value));
    }
    // wait replica switch.
    StartWorkerAndWaitReady(
        { 2 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    WaitReplicaLocationMatch({ 1, 2 });
    for (const auto &objKey : objKeys) {
        std::string data;
        DS_ASSERT_OK(clients_[1]->Get(objKey, data));
        ASSERT_EQ(data, value);
    }
    BasicPutGetDel(1);
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestReplicasCrashCurrent)
{
    const int nodeTimeout = 2;
    const int nodeDeadTimeoutS = 4;
    StartWorkerAndWaitReady(
        { 0, 1, 2 }, FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d -v=2", nodeTimeout, nodeDeadTimeoutS));
    WaitReplicaLocationMatch({ 0, 1, 2 });
    for (auto i = 0; i < 3; i++) {  // worker num is 3
        // do not abort when fail.
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "Hashring.Scaletask.Fail"));
    }

    for (const auto &info : workersInfo_) {
        if (info.second.uuid == workersInfo_[0].uuid || info.second.uuid == workersInfo_[0].nextUuid) {
            DS_ASSERT_OK(externalCluster_->KillWorker(info.first));
        }
    }
    WaitAllNodesJoinIntoHashRing(1);
}

TEST_F(STCReplicaSwitchTest, TestAfterScaleupAdjustReplicaFromMultiNode)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    LOG(INFO) << "check after cluster init.";
    WaitReplicaLocationMatch({ 0, 1, 2 });
    StartWorkerAndWaitReady({ 3 });
    LOG(INFO) << "check after scale up finish.";
    WaitReplicaLocationMatch({ 0, 1, 2, 3 });
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestAfterScaleupMultiNodeAdjustReplica)
{
    StartWorkerAndWaitReady({ 0, 1 });
    LOG(INFO) << "check after cluster init.";
    WaitReplicaLocationMatch({ 0, 1 });
    StartWorkerAndWaitReady({ 2, 3 });
    LOG(INFO) << "check after scale up finish.";
    WaitReplicaLocationMatch({ 0, 1, 2, 3 });
}

TEST_F(STCReplicaSwitchTest, DISABLED_LEVEL1_TestStartFullSync)
{
    const int delaySec = 3;
    const int clientCount = 3;
    StartWorkerAndWaitReady({ 0, 1, 2 }, { { 0, " -inject_actions=worker.ClusterInitFinish:return()" } });
    // diable replica switch.
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);
    sleep(delaySec);
    InitClients(clientCount);
    const int countPerClient = 50;
    std::unordered_map<std::string, std::string> keyValues;
    // write data.
    BasicPut(countPerClient, keyValues);
    // waiting async finish.
    sleep(delaySec);

    // force full sync
    for (int index = 1; index < clientCount; index++) {
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, index, "master.Replication.TryPSync", "1*return(K_RUNTIME_ERROR)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, index, "worker.HandleFetchFile.fetchSize", "call(10240)"));
    }

    // worker0 crash and start.
    RestartWorkerAndWaitReady({ 0 });
    sleep(delaySec);
    // enable replica switch.
    ResetSyncCap({ 0, 1, 2 });
    WaitReplicaInCurrentNode(0);
    sleep(delaySec);
    BasicGet(1, keyValues);
}

TEST_F(STCReplicaSwitchTest, DISABLED_ScaleDownPrimaryAfterSwitchTest)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);
    int indexForNextWorker0 = workersInfo_[0].nextIndex;
    std::string uuidForWorker0 = workersInfo_[0].uuid;
    std::string uuidForNextWorker0 = workersInfo_[indexForNextWorker0].uuid;
    std::string uuidForPreWorker0 = workersInfo_[indexForNextWorker0].nextUuid;
    LOG(INFO) << "backup replica for worker 0 in worker " << indexForNextWorker0;
    // worker0 crash and start.
    DS_ASSERT_OK(externalCluster_->KillWorker(0));
    // wait replica switch.
    WaitReplicaNotInCurrentNode(0);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, "-inject_actions=worker.CheckReplicaLocation:call(1000);"));
    VoluntaryScaleDownInject(indexForNextWorker0);
    LOG(INFO) << "expect replica in " << uuidForWorker0 << ", " << uuidForPreWorker0
              << " primary replica location not in " << uuidForNextWorker0;
    UntilTrueOrTimeout([this, uuidForWorker0, uuidForPreWorker0, uuidForNextWorker0] {
        std::string replicaGroupStr;
        auto rc = etcd_->Get(ETCD_REPLICA_GROUP_TABLE, uuidForWorker0, replicaGroupStr);
        if (rc.IsError()) {
            LOG(ERROR) << "Get ETCD_REPLICA_GROUP_TABLE faild.";
            return false;
        }

        ReplicaGroupPb replicaGroup;
        if (!replicaGroup.ParseFromString(replicaGroupStr)) {
            return false;
        }

        LOG(INFO) << "ReplicaGroupPb for " << uuidForWorker0 << ":" << replicaGroup.ShortDebugString();

        std::set<std::string> expectReplicaLocations{ uuidForWorker0, uuidForPreWorker0 };
        std::set<std::string> replicaLocations;
        for (const auto &item : replicaGroup.replicas()) {
            const auto &workerUuid = item.worker_id();
            replicaLocations.emplace(workerUuid);
        }
        return replicaLocations == expectReplicaLocations && replicaGroup.primary_id() != uuidForNextWorker0;
    });
    etcd_.reset();
    cluster_->KillWorker(indexForNextWorker0);
}

TEST_F(STCReplicaSwitchTest, DISABLED_TestPrimaryReplicaNotInHashRing)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InjectSyncCap({ 0, 1, 2 }, UINT16_MAX, 1);
    int indexForNextWorker0 = workersInfo_[0].nextIndex;
    std::string uuidForWorker0 = workersInfo_[0].uuid;
    std::string uuidForNextWorker0 = workersInfo_[indexForNextWorker0].uuid;
    std::string uuidForPreWorker0 = workersInfo_[indexForNextWorker0].nextUuid;
    LOG(INFO) << "backup replica for worker 0 in worker " << indexForNextWorker0;
    // worker0 crash and start.
    DS_ASSERT_OK(externalCluster_->KillWorker(0));
    // wait replica switch.
    WaitReplicaNotInCurrentNode(0);
    int delaySec = 3;
    sleep(delaySec);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, "-inject_actions=worker.CheckReplicaLocation:call(1000)"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.ReplicaManager.HandleNodeTimeout", "call()"));
    // 5s
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.ReplicaManager::CheckPrimaryReplicaLocation", "call(5000000)"));

    VoluntaryScaleDownInject(indexForNextWorker0);

    LOG(INFO) << "expect replica in " << uuidForWorker0 << ", " << uuidForPreWorker0
              << " primary replica location not in " << uuidForNextWorker0;
    UntilTrueOrTimeout([this, uuidForWorker0, uuidForPreWorker0, uuidForNextWorker0] {
        std::string replicaGroupStr;
        auto rc = etcd_->Get(ETCD_REPLICA_GROUP_TABLE, uuidForWorker0, replicaGroupStr);
        if (rc.IsError()) {
            LOG(ERROR) << "Get ETCD_REPLICA_GROUP_TABLE faild.";
            return false;
        }

        ReplicaGroupPb replicaGroup;
        if (!replicaGroup.ParseFromString(replicaGroupStr)) {
            return false;
        }

        LOG(INFO) << "ReplicaGroupPb for " << uuidForWorker0 << ":" << replicaGroup.ShortDebugString();

        std::set<std::string> expectReplicaLocations{ uuidForWorker0, uuidForPreWorker0 };
        std::set<std::string> replicaLocations;
        for (const auto &item : replicaGroup.replicas()) {
            const auto &workerUuid = item.worker_id();
            replicaLocations.emplace(workerUuid);
        }
        return replicaLocations == expectReplicaLocations && replicaGroup.primary_id() != uuidForNextWorker0;
    });
}

class STCReplicaCrossAzTest : public STCReplicaTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.enableDistributedMaster = "true";
        std::string gflag =
            " -v=1 -shared_memory_size_mb=5120 -node_timeout_s=3 -node_dead_timeout_s=8 -auto_del_dead_node=true "
            "-other_az_names=AZ1,AZ2,AZ3 -cross_az_get_meta_from_worker=true -enable_meta_replica=true "
            "-oc_io_from_l2cache_need_metadata=true";

        opts.workerGflagParams = gflag;

        for (size_t i = 0; i < WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back("127.0.0.1", GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
            std::string param = "-az_name=";
            param.append(otherAzNames_[i % otherAzNames_.size()]);
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
        for (size_t i = 0; i < WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        clients_.resize(WORKER_NUM);
        int32_t timeoutMs = 2000;
        for (size_t i = 0; i < WORKER_NUM; i++) {
            InitTestKVClient(i, clients_[i], timeoutMs);
        }
    }

    enum KeyType { HASH = 0, WITH_WORKER_ID = 1 };

    void TestSetGetModify(KeyType keyType)
    {
        std::string keyPrefix = "object";
        std::string val1 = "val1";
        std::string val2 = "val2";
        int32_t timeoutMs = 2000;
        int loopTime = 1000;
        std::vector<std::string> keys;
        for (int i = 0; i < loopTime; i++) {
            std::string key;
            switch (keyType) {
                case HASH:
                    key = keyPrefix + std::to_string(i);
                    break;
                case WITH_WORKER_ID:
                    (void)clients_[i % otherAzNames_.size()]->GenerateKey(keyPrefix + std::to_string(i), key);
                    break;
            }
            // Cross-AZ set is not supported.
            DS_ASSERT_OK(clients_[i % otherAzNames_.size()]->Set(key, val1));
            keys.emplace_back(key);
        }

        // Disorder the order of the client and expect to cover all scenarios.
        std::shared_ptr<KVClient> client1;
        InitTestKVClient(0, client1, timeoutMs);
        clients_.emplace_back(client1);
        for (int i = 0; i < loopTime; i++) {
            std::string valToGet;
            DS_ASSERT_OK(clients_[i % clients_.size()]->Get(keys[i], valToGet));
            ASSERT_EQ(valToGet, val1);
        }

        // Test modify. Restore the client sequence because set operations cannot be performed across AZs.
        clients_.pop_back();
        for (int i = 0; i < loopTime; i++) {
            DS_ASSERT_OK(clients_[i % otherAzNames_.size()]->Set(keys[i], val2));
        }

        // Disorder the order of the client and expect to cover all scenarios.
        std::shared_ptr<KVClient> client2;
        InitTestKVClient(0, client2, timeoutMs);
        clients_.emplace_back(client2);
        for (int i = 0; i < loopTime; i++) {
            std::string valToGet;
            DS_ASSERT_OK(clients_[i % clients_.size()]->Get(keys[i], valToGet));
            ASSERT_EQ(valToGet, val2);
        }
    }

protected:
    const size_t WORKER_NUM = 6;
    const std::vector<std::string> otherAzNames_ = { "AZ1", "AZ2", "AZ3" };
    std::vector<std::string> workerAddress_;
    std::vector<std::shared_ptr<KVClient>> clients_;
};

TEST_F(STCReplicaCrossAzTest, DISABLED_TestHashObjKeySetGetModify)
{
    TestSetGetModify(KeyType::HASH);
}

TEST_F(STCReplicaCrossAzTest, DISABLED_TestObjKeyWithUuidSetGetModify)
{
    TestSetGetModify(KeyType::WITH_WORKER_ID);
}

class STCUpdateToReplicaTest : public STCReplicaSwitchTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3 ";
        opts.waitWorkerReady = false;
        opts.disableRocksDB = false;
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }
};

TEST_F(STCUpdateToReplicaTest, LEVEL1_TestUpdateToReplicaEnable)
{
    StartWorkerAndWaitReady({ 0, 1 });
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::vector<std::string> objs;
    std::string data = randomData_.GetRandomString(513 * 1024ul);
    SetParam param = { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (int i = 0; i < 20; i++) {  // obj num is 20
        std::string key = "League_of_Legends_" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key, data, param));
        objs.emplace_back(key);
    }
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, "-enable_meta_replica=true"));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, "-enable_meta_replica=true"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    WaitReplicaLocationMatch({ 0, 1 });
    for (const std::string &key : objs) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(key, getValue));
        ASSERT_EQ(data, getValue);
        getValue.clear();
        DS_ASSERT_OK(client2->Get(key, getValue));
        ASSERT_EQ(data, getValue);
    }
}

TEST_F(STCUpdateToReplicaTest, LEVEL1_TestUpdateToReplicaEnableWithoutRocks)
{
    StartWorkerAndWaitReady({ 0, 1 });
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    // Remove rockdb from filesystem.
    auto baseDir = GetTestCaseDataDir();
    for (auto index : { 0, 1 }) {
        std::string metaDir = baseDir + "/worker" + std::to_string(index) + "/rocksdb/object_metadata";
        DS_ASSERT_OK(RemoveAll(metaDir));
    }
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, "-enable_meta_replica=true"));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, "-enable_meta_replica=true"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    WaitReplicaLocationMatch({ 0, 1 });
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::vector<std::string> objs;
    std::string data = randomData_.GetRandomString(513 * 1024ul);
    SetParam param = { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    const int count = 20;
    for (int i = 0; i < count; i++) {  // obj num is 20
        std::string key1 = "League_of_Legends_a_" + std::to_string(i);
        std::string key2 = "League_of_Legends_b_" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key1, data, param));
        DS_ASSERT_OK(client2->Set(key2, data, param));
    }
}
}  // namespace st
}  // namespace datasystem
