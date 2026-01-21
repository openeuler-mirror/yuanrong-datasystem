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
 * Description: Spill to remote worker via TCP test.
 */

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "cluster/external_cluster.h"
#include "common.h"
#include "datasystem/kv_client.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

class KVClientSpillRemoteTcpTest : public OCClientCommon {
public:
    KVClientSpillRemoteTcpTest() = default;
    ~KVClientSpillRemoteTcpTest() = default;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;  // 3 workers to test spill to remote worker
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=64 -log_monitor=true -spill_to_remote_worker=true";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(0, client_);
        InitTestKVClient(1, client1_);
    }

    void TearDown() override
    {
        client_.reset();
        client1_.reset();
    }

    void PrepareHotKeys(const std::shared_ptr<KVClient> &client, const std::string &prefix, size_t hotKeyNum,
                        std::vector<std::string> &hotKeys, std::vector<std::string> &hotVals)
    {
        hotKeys.clear();
        hotVals.clear();
        hotKeys.reserve(hotKeyNum);
        hotVals.reserve(hotKeyNum);
        for (size_t i = 0; i < hotKeyNum; ++i) {
            hotKeys.emplace_back(prefix + std::to_string(i));
            hotVals.emplace_back(std::string(valueSize_, 'a'));
        }
        for (size_t i = 0; i < hotKeys.size(); ++i) {
            DS_ASSERT_OK(client->Set(hotKeys[i], hotVals[i]));
        }
    }

    void AddPressureData(const std::shared_ptr<KVClient> &client, const std::string &prefix, int pressureNum)
    {
        for (int i = 0; i < pressureNum; ++i) {
            std::string key = prefix + std::to_string(i);
            std::string value = GenRandomString(valueSize_);
            DS_ASSERT_OK(client->Set(key, value));
        }
    }

    void RunConcurrentSetGetDel(const std::shared_ptr<KVClient> &client, const std::vector<std::string> &hotKeys,
                                const std::vector<std::string> &hotVals)
    {
        auto getAndDel = [&client](const std::string &key, const std::string &originVal) {
            std::string getVal;
            DS_ASSERT_OK(client->Get(key, getVal));
            ASSERT_EQ(getVal, originVal);
            DS_ASSERT_OK(client->Del(key));
            ASSERT_EQ(client->Get(key, getVal).GetCode(), StatusCode::K_NOT_FOUND);
        };

        auto setAndGet = [&client](const std::string &key) {
            std::string newVal(valueSize_, 'x');
            DS_ASSERT_OK(client->Set(key, newVal));
            std::string getVal;
            DS_ASSERT_OK(client->Get(key, getVal));
            ASSERT_EQ(getVal, newVal);
        };

        ThreadPool pool(static_cast<int>(hotKeys.size()));
        std::vector<std::future<void>> futures;
        const auto mod = 2;
        for (size_t i = 0; i < hotKeys.size(); ++i) {
            futures.emplace_back(pool.Submit([&, i]() {
                if (i % mod == 0) {
                    setAndGet(hotKeys[i]);
                } else {
                    getAndDel(hotKeys[i], hotVals[i]);
                }
            }));
        }
        for (auto &f : futures) {
            f.get();
        }
    }

protected:
    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    static constexpr size_t valueSize_ = 5 * 1024UL * 1024UL;  // 5MB
};

TEST_F(KVClientSpillRemoteTcpTest, SingleWorkerMigrateTest)
{
    // shared_memory_size_mb is 64MB, set 9 objects of 5MB(Align to 6MB) to trigger eviction, total allocate
    // size is 54MB.
    const size_t valueSize = valueSize_;
    const int objectNum = 9;

    for (int i = 0; i < objectNum; ++i) {
        std::string key = "object_" + std::to_string(i);
        std::string value = GenRandomString(valueSize);
        DS_ASSERT_OK(client_->Set(key, value));
    }

    const int objectNumAdditional = 4;  // worker1 requires 24MB of free memory
    for (int i = 0; i < objectNumAdditional; ++i) {
        std::string key = "object_additional_" + std::to_string(i);
        std::string value = GenRandomString(valueSize);
        DS_ASSERT_OK(client1_->Set(key, value));
    }
}

TEST_F(KVClientSpillRemoteTcpTest, MultiWorkerMigrateTest)
{
    // shared_memory_size_mb is 64MB, set 9 objects of 5MB(Align to 6MB) to trigger eviction, total allocate
    // size is 54MB.
    const size_t valueSize = valueSize_;
    const int objectNum = 9;

    HostPort addr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(2, addr));  // worker index is 2
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "SpillNodeSelector.SelectNode.force", "call(" + addr.ToString() + ")"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "SpillNodeSelector.SelectNode.force", "call(" + addr.ToString() + ")"));

    for (int i = 0; i < objectNum; ++i) {
        std::string key = "object_" + std::to_string(i);
        std::string value = GenRandomString(valueSize);
        DS_ASSERT_OK(client_->Set(key, value));
        key = "object1_" + std::to_string(i);
        DS_ASSERT_OK(client1_->Set(key, value));
    }

    const int objectNumAdditional = 2;  // worker0 and worker1 require 12MB of free memory
    for (int i = 0; i < objectNumAdditional; ++i) {
        std::string key = "object_additional_" + std::to_string(i);
        std::string value = GenRandomString(valueSize);
        DS_ASSERT_OK(client_->Set(key, value));
        key = "object_additional1_" + std::to_string(i);
        DS_ASSERT_OK(client1_->Set(key, value));
    }
}

TEST_F(KVClientSpillRemoteTcpTest, LocalConcurrentSetGetDelDuringMigration)
{
    // 1. Inject a migration delay on the worker0 for concurrency.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "TcpMigrateTransport.MigrateDataToRemote.delay", "sleep(3000)"));

    // 2. Prepare 3 hot keys first.
    std::vector<std::string> hotKeys;
    std::vector<std::string> hotVals;
    PrepareHotKeys(client_, "hot_migrate_", 2, hotKeys, hotVals);  // 2 hot keys

    // 3. Add pressure to trigger eviction -> spill to remote worker migration (TCP).
    const int pressureNum = 7;
    AddPressureData(client_, "pressure_", pressureNum);
    std::vector<std::string> pressureKeys;
    for (int i = 0; i < pressureNum; ++i) {
        pressureKeys.emplace_back("pressure_" + std::to_string(i));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // sleep 100 ms wait for migration start
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client_->Del(pressureKeys, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    // 4. Concurrent Set/Get/Del on hot keys locally.
    RunConcurrentSetGetDel(client_, hotKeys, hotVals);
}

TEST_F(KVClientSpillRemoteTcpTest, RemoteConcurrentSetGetDelDuringMigration)
{
    // 1. Inject a migration delay on the worker0 for concurrency.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "TcpMigrateTransport.MigrateDataToRemote.delay", "sleep(3000)"));

    // 2. Prepare 3 hot keys first.
    std::vector<std::string> hotKeys;
    std::vector<std::string> hotVals;
    PrepareHotKeys(client_, "hot_migrate_", 2, hotKeys, hotVals);  // 2 hot keys

    // 3. Add pressure to trigger eviction -> spill to remote worker migration (TCP).
    const int pressureNum = 7;
    AddPressureData(client_, "pressure_", pressureNum);

    // 4. Concurrent Set/Get/Del on hot keys remotely.
    RunConcurrentSetGetDel(client1_, hotKeys, hotVals);
}

class KVClientSpillRemoteTcpDfxTest : public OCClientCommon {
public:
    KVClientSpillRemoteTcpDfxTest() = default;
    ~KVClientSpillRemoteTcpDfxTest() = default;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;  // 2 workers to test spill to remote worker
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=64 -log_monitor=true -spill_to_remote_worker=true";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(0, client_);
        InitTestKVClient(1, client1_);
    }

    void TearDown() override
    {
        client_.reset();
        client1_.reset();
    }

protected:
    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    static constexpr size_t valueSize_ = 5 * 1024UL * 1024UL;  // 5MB
};

TEST_F(KVClientSpillRemoteTcpDfxTest, MigrateDataWithDestFail)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.migrate_service.return", "return(K_RPC_UNAVAILABLE)"));

    const size_t valueSize = valueSize_;
    const int objectNum = 10;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(objectNum);
    values.reserve(objectNum);

    for (int i = 0; i < objectNum; i++) {
        std::string key = "object_" + std::to_string(i);
        std::string value = GenRandomString(valueSize);
        DS_ASSERT_OK(client_->Set(key, value));
        keys.emplace_back(key);
        values.emplace_back(value);
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        std::string getVal;
        DS_ASSERT_OK(client_->Get(keys[i], getVal));
        ASSERT_EQ(getVal, values[i]);
    }
}

}  // namespace st
}  // namespace datasystem
