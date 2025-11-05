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

#include <cstdint>
#include <map>
#include <string>
#include <thread>
#include <vector>
 
#include <gtest/gtest.h>
 
#include "common.h"
#include "common_distributed_ext.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/log/log.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

DS_DECLARE_string(etcd_address);
namespace datasystem {
namespace st {
class STCReplicaScaleTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3 -enable_meta_replica=true";
        opts.waitWorkerReady = false;
        opts.numOBS = 1;
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
 
    void BasicPutGetDel(int batchNo)
    {
        const int objCountPerClient = 10;
        std::vector<std::string> keys;
        std::unordered_map<std::string, std::string> keyValues;
        for (auto &client : clients_) {
            for (auto i = 0; i < objCountPerClient; i++) {
                std::string key1 = GetStringUuid();
                std::string value1 = GenPartRandomString();
                DS_ASSERT_OK(client->Set(key1, value1));
                std::string value2 = GenPartRandomString();
                std::string key2 = client->Set(value2);
                ASSERT_TRUE(!key2.empty());
                keys.emplace_back(key1);
                keys.emplace_back(key2);
                keyValues.emplace(key1, value1);
                keyValues.emplace(key2, value2);
            }
        }
 
        auto groupKeys1 = RandomGroup(keys, batchNo);
        size_t index1 = 0;
        for (const auto &keys : groupKeys1) {
            auto client = clients_[index1 % clients_.size()];
            std::vector<std::string> vals;
            DS_ASSERT_OK(client->Get(keys, vals));
            for (size_t k = 0; k < keys.size(); k++) {
                const auto &key = keys[k];
                ASSERT_EQ(vals[k], keyValues[key]) << "key:" << key;
            }
            index1++;
        }
 
        auto groupKeys2 = RandomGroup(keys, batchNo);
        size_t index2 = 0;
        for (const auto &keys : groupKeys2) {
            auto client = clients_[index2 % clients_.size()];
            std::vector<std::string> failedIds;
            DS_ASSERT_OK(client->Del(keys, failedIds));
            ASSERT_EQ(0ul, failedIds.size());
            index2++;
        }
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
 
protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::vector<std::shared_ptr<KVClient>> clients_;
    int idx0 = 0;
    int idx1 = 1;
    int idx2 = 2;
    int objNum = 50;
};

class STCReplicaScaleDownTest : public STCReplicaScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=2 -enable_meta_replica=true -node_dead_timeout_s=4";
        opts.waitWorkerReady = false;
        opts.addNodeTime = 2;  // add node time is 2 s
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

TEST_F(STCReplicaScaleDownTest, LEVEL2_ScaleDownTest)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients({ 0, 1, 2 });
    std::vector<std::string> objKeys, values;
    SetParam param;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    std::this_thread::sleep_for(std::chrono::seconds(1));  // sleep for waiting replica update.
    for (int i = 0; i < 10; i++) {  // obj num is 10
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        auto value = GenRandomString(10);
        DS_ASSERT_OK(clients_[idx0]->Set(key, value, param));
        std::string uuidKey;
        (void)clients_[i % 3]->GenerateKey("", uuidKey);              // client num is 3
        DS_ASSERT_OK(clients_[i % 3]->Set(uuidKey, value, param));  // client num is 3
        objKeys.emplace_back(uuidKey);
        values.emplace_back(value);
        values.emplace_back(value);
    }
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    WaitChangeReplicaInCurrentNode(1, workersInfo_[idx0].uuid);     // worker index is 1
    WaitChangeReplicaInCurrentNode(idx2, workersInfo_[idx0].uuid);  // worker index is 2
    std::this_thread::sleep_for(std::chrono::seconds(1));  // sleep for waiting replica update.
    for (size_t i = 0; i < objKeys.size(); i++) {
        std::string data;
        DS_ASSERT_OK(clients_[idx1]->Get(objKeys[i], data));
        ASSERT_EQ(data, values[i]);
    }
}

TEST_F(STCReplicaScaleDownTest, DISABLED_ScaleDownAfterSwitchTest)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients({ 0, 1, 2 });
    std::vector<std::string> objKeys, values;
    SetParam param;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    for (int i = 0; i < 30; i++) {  // obj num is 30
        auto key = "kv_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        auto value = GenRandomString(10);
        DS_ASSERT_OK(clients_[idx0]->Set(key, value, param));
        std::string uuidKey;
        (void)clients_[i % 3]->GenerateKey("", uuidKey);              // client num is 3
        DS_ASSERT_OK(clients_[i % 3]->Set(uuidKey, value, param));  // client num is 3
        objKeys.emplace_back(uuidKey);
        values.emplace_back(value);
        values.emplace_back(value);
    }
    RestartWorkerAndWaitReady({ 0 });
    WaitReplicaNotInCurrentNode(0);
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    WaitChangeReplicaInCurrentNode(1, workersInfo_[idx0].uuid);     // worker index is 1
    WaitChangeReplicaInCurrentNode(idx2, workersInfo_[idx0].uuid);  // worker index is 2
    for (size_t i = 0; i < objKeys.size(); i++) {
        std::string data;
        DS_ASSERT_OK(clients_[idx1]->Get(objKeys[i], data));
        ASSERT_EQ(data, values[i]);
    }
}

TEST_F(STCReplicaScaleDownTest, DISABLED_ScaleDownCleanMapTest)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, " -inject_actions=Worker.cleanMapIntervalMs:call()");
    InitClients({ 0, 1, 2 });
    SetParam param;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    auto value = GenRandomString(10);
    auto uuidKey = clients_[idx0]->GenerateKey();
    auto uuidKey1 = clients_[idx1]->GenerateKey();
    DS_ASSERT_OK(clients_[idx0]->Set(uuidKey, value, param));
    DS_ASSERT_OK(clients_[idx0]->Set(uuidKey1, value, param));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    WaitChangeReplicaInCurrentNode(1, workersInfo_[idx0].uuid);
    WaitChangeReplicaInCurrentNode(idx2, workersInfo_[idx0].uuid);
    std::string data;
    std::thread thread([&]() {
        DS_ASSERT_OK(clients_[idx1]->Get(uuidKey, data));
        ASSERT_EQ(data, value);
        DS_ASSERT_OK(clients_[idx2]->Del(uuidKey));
        DS_ASSERT_OK(clients_[idx1]->Set(uuidKey, value, param));
        DS_ASSERT_OK(clients_[idx1]->Set(uuidKey1, value, param));
        DS_ASSERT_OK(clients_[idx2]->Del(uuidKey));
        const int interval = 10000;  // 10000ms;
        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        DS_ASSERT_NOT_OK(clients_[idx1]->Set(uuidKey, "test_data", param));
        DS_ASSERT_OK(clients_[idx1]->Set(uuidKey1, "test_data", param));
        });
    thread.join();
}

class STCReplicaScaleUpTest : public STCReplicaScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=2 -enable_meta_replica=true";
        opts.waitWorkerReady = false;
        opts.addNodeTime = 2;  // add node time is 2 s
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

TEST_F(STCReplicaScaleUpTest, TestWritebackScaleUp)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients({ 0, 1, 2 });
    std::vector<std::string> objKeys, values;
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    for (int i = 0; i < 30; i++) {  // obj num is 30
        auto key = "state_client_key_test_" + std::to_string(i);
        objKeys.emplace_back(key);
        auto value = GenRandomString(10);
        DS_ASSERT_OK(clients_[idx0]->Set(key, value, param));
        auto uuidKey = clients_[i % 3]->GenerateKey();              // client num is 3
        DS_ASSERT_OK(clients_[i % 3]->Set(uuidKey, value, param));  // client num is 3
        objKeys.emplace_back(uuidKey);
        values.emplace_back(value);
        values.emplace_back(value);
    }
    StartWorkerAndWaitReady({3});
    for (size_t i = 0; i < objKeys.size(); i++) {
        std::string data;
        DS_ASSERT_OK(clients_[idx1]->Get(objKeys[i], data));
        ASSERT_EQ(data, values[i]);
    }
}
}  // namespace st
}  // namespace datasystem
