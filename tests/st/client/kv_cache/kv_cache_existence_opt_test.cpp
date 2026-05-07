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
 * Description: test ExistenceOpt.
 */
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>

#include "common.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "client/object_cache/oc_client_common.h"

namespace datasystem {
namespace st {
constexpr uint8_t WORKER_NUM = 3;
class KVCacheExistenceOptTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numOBS = 1;
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-node_timeout_s=5 -shared_memory_size_mb=2048 -v=1";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(client1Index, client1);
        InitTestKVClient(client2Index, client2);
        InitTestKVClient(client3Index, client3);
    }

    void TearDown() override
    {
        client1.reset();
        client2.reset();
        client3.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    std::shared_ptr<KVClient> client3;

private:
    const uint32_t client1Index = 0;
    const uint32_t client2Index = 1;
    const uint32_t client3Index = 2;
};

TEST_F(KVCacheExistenceOptTest, TestNXSet)
{
    std::string key = "test";
    std::string value = "value1";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(client2->Set(key, value, param));
}

TEST_F(KVCacheExistenceOptTest, TestNXSetAndDel)
{
    std::string key = "test";
    std::string value = "value1";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(client2->Set(key, value, param));
    DS_ASSERT_OK(client1->Del(key));
    DS_ASSERT_OK(client2->Set(key, value, param));
    std::string valToGet;
    DS_ASSERT_OK(client1->Get(key, valToGet));
    ASSERT_EQ(valToGet, value);
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(client2->Set(key, value, param));
}

TEST_F(KVCacheExistenceOptTest, TestNXSetAfterNoneSet)
{
    std::string key = "test";
    std::string value = "value1";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(key, value));
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(client2->Set(key, value, param));
}

TEST_F(KVCacheExistenceOptTest, TestNoneSetAfterNXSet)
{
    std::string key = "test";
    std::string value = "value1";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(client1->Set(key, value));
    DS_ASSERT_OK(client2->Set(key, value));
}

TEST_F(KVCacheExistenceOptTest, DISABLED_TestMutiNXSetNoneSet)
{
    std::string key = "test";
    std::string value = "value1";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
 
    std::atomic<int> successNum {0};
    auto numThreads = 5;
    ThreadPool threadPool(numThreads);
    const auto numRepeat = 10;
    auto fut1 = threadPool.Submit([this, &key, &value, &param, &successNum]() {
        for (int i = 0; i < numRepeat; i++) {
            if (client1->Set(key, value, param).IsOk()) {
                successNum++;
            };
        }
    });

    auto fut2 = threadPool.Submit([this, &key, &value]() {
        for (int i = 0; i < numRepeat; ++i) {
            DS_ASSERT_OK(client1->Set(key, value));
        }
    });

    auto fut3 = threadPool.Submit([this, &key, &value, &param, &successNum]() {
        for (int i = 0; i < numRepeat; ++i) {
            if (client2->Set(key, value, param).IsOk()) {
                successNum++;
            };
        }
    });

    fut1.get();
    fut2.get();
    fut3.get();
    ASSERT_GE(successNum, 1);
}

TEST_F(KVCacheExistenceOptTest, DISABLED_TestMutiNXSetAndDel)
{
    std::string key = "test";
    std::string value = "value1";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };

    ThreadPool threadPool(5);
    auto fut = threadPool.Submit([this, &key, &value, &param]() {
        for (int i = 0; i < 10; i++) {
            client1->Set(key, value, param);
        }
    });

    auto fut2 = threadPool.Submit([this, &key]() {
        for (int i = 0; i < 10; ++i) {
            DS_ASSERT_OK(client1->Del(key));
        }
    });

    auto fut3 = threadPool.Submit([this, &key]() {
        for (int i = 0; i < 20; ++i) {
            DS_ASSERT_OK(client2->Del(key));
        }
    });

    auto fut4 = threadPool.Submit([this, &key, &value]() {
        for (int i = 0; i < 10; ++i) {
            DS_ASSERT_OK(client1->Set(key, value));
        }
    });

    auto fut5 = threadPool.Submit([this, &key, &value, &param]() {
        for (int i = 0; i < 10; ++i) {
            client2->Set(key, value, param);
        }
    });

    fut.get();
    fut2.get();
    fut3.get();
    fut4.get();
    fut5.get();
}

TEST_F(KVCacheExistenceOptTest, TestDelDeadLock)
{
    std::string key = "test";
    std::string value = "value";
    SetParam param { .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    auto fun = [](std::string &key, std::string &value, SetParam &param, std::shared_ptr<KVClient> &client) {
        auto rc = client->Set(key, value, param);
        if (rc.IsError()) {
            std::string valToGet;
            client->Get(key, valToGet);
            DS_ASSERT_OK(client->Del(key));
        }
    };
    ThreadPool threadPool(3);
    threadPool.Execute(fun, key, value, param, client1);
    threadPool.Execute(fun, key, value, param, client2);
    threadPool.Execute(fun, key, value, param, client3);
}
 
TEST_F(KVCacheExistenceOptTest, TestDelDeadLock2)
{
    std::string key = "test";
    std::string value = "value";
    client1->Set(key, value);
    std::string valToGet1;
    client2->Get(key, valToGet1);
    std::string valToGet2;
    client3->Get(key, valToGet2);
    auto fun = [](std::string &key, std::shared_ptr<KVClient> &client) {
        client->Del(key);
    };
    ThreadPool threadPool(2);
    threadPool.Execute(fun, key, client2);
    threadPool.Execute(fun, key, client3);
}

TEST_F(KVCacheExistenceOptTest, LEVEL1_TestDfxNoneL2NXSet)
{
    std::string key = "test";
    std::string value = "value1";
    SetParam param { .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(key, value, param));
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 0, 1, 2 }));
    std::string val;
    DS_ASSERT_OK(client1->Set(key, value, param));
}
 
TEST_F(KVCacheExistenceOptTest, LEVEL1_TestDfxNoneL2NXSetAfterGet)
{
    std::string key = "test";
    std::string value = "value1";
    SetParam param { .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(key, value, param));
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 0, 1, 2 }));
    std::string val;
    DS_ASSERT_NOT_OK(client1->Get(key, val));
    DS_ASSERT_OK(client1->Set(key, value, param));
}
 
TEST_F(KVCacheExistenceOptTest, LEVEL1_TestDfxL2NXSet)
{
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    std::string key = "test";
    std::string value = "value1";
    SetParam param { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({1}));
    std::string val;
    DS_ASSERT_OK(client1->Set(key, value, param));
}

TEST_F(KVCacheExistenceOptTest, TestSetGetConcurrency)
{
    std::string key = "test";
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(key, value, param));
    DS_ASSERT_OK(client1->Set(key, value, param));
 
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "local.get.sleep", "sleep(2)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "publish.sleep", "sleep(1)"));
 
    std::thread t1([this, key, value, param]() {
        DS_ASSERT_OK(client2->Set(key, value, param));
    });
 
    std::thread t2([this, key, value]() {
        std::string val;
        DS_ASSERT_OK(client2->Get(key, val));
        ASSERT_EQ(val, value);
    });
 
    t1.join();
    t2.join();
}

class KVCacheNXCentralMasterTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        constexpr int WORKER_NUM = 4;
        opts.numOBS = 1;
        opts.numWorkers = WORKER_NUM;
        opts.masterIdx = 0;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams = "-node_timeout_s=5 -shared_memory_size_mb=2048 -v=1";
    }
};

TEST_F(KVCacheNXCentralMasterTest, GetAfterPutFailed)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    std::shared_ptr<KVClient> client3;
    InitTestKVClient(1, client1);
    InitTestKVClient(2, client2);
    InitTestKVClient(3, client3);
    std::string objectKey = NewObjectKey();
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client1->Set(objectKey, data, param));
    std::vector<std::string> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey }, dataList));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.worker_worker_remote_get_failure", "sleep(1500)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 2, "worker.worker_worker_remote_get_failure", "1*return(K_NOT_FOUND)"));

    ThreadPool pool(3);
    pool.Execute([&client1, &objectKey, data, &param] {
        sleep(1);
        Status rc = client1->Set(objectKey, data, param);
    });
    pool.Execute([&client2, &objectKey, data, &param] {
        DS_ASSERT_OK(client2->Del(objectKey));
        DS_ASSERT_OK(client2->Set(objectKey, data, param));
    });
    pool.Execute([&client3, &objectKey, data] {
        std::vector<std::string> dataList;
        Status rc = client3->Get({ objectKey }, dataList);
        ASSERT_EQ(rc.GetMsg().find("Invalid parameter"), std::string::npos);
    });
}

TEST_F(KVCacheExistenceOptTest, DISABLED_TestMCreateNXConcurrentClients)
{
    // Test: two clients concurrently MCreate NX on pre-existing keys.
    // Both should get placeholder buffers (GetSize == 0), and MemoryCopy
    // should safely return K_INVALID instead of crashing on null pointer.
    // Both clients connect to the same worker: MCreate sends to the
    // connected worker (not hash-routed), so both must see the same
    // object table state for deterministic NX rejection.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(0, client2);

    constexpr int keyCount = 3;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (int i = 0; i < keyCount; i++) {
        keys.emplace_back("mcreate_nx_concurrent_" + std::to_string(i) + "_" + client1->GenerateKey());
        values.emplace_back("value_" + std::to_string(i));
    }

    // Pre-create all keys so NX will reject both clients
    for (int i = 0; i < keyCount; i++) {
        DS_ASSERT_OK(client1->Set(keys[i], values[i]));
    }

    std::vector<uint64_t> sizes;
    for (const auto &val : values) {
        sizes.emplace_back(val.size());
    }

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.existence = ExistenceOpt::NX;

    std::vector<std::shared_ptr<Buffer>> buffers1;
    std::vector<std::shared_ptr<Buffer>> buffers2;

    // Both clients MCreate with NX concurrently
    std::promise<void> ready;
    std::shared_future<void> start(ready.get_future());
    ThreadPool pool(2);
    auto fut1 = pool.Submit([&]() {
        start.wait();
        DS_ASSERT_OK(client1->MCreate(keys, sizes, param, buffers1));
    });
    auto fut2 = pool.Submit([&]() {
        start.wait();
        DS_ASSERT_OK(client2->MCreate(keys, sizes, param, buffers2));
    });
    ready.set_value();
    fut1.get();
    fut2.get();

    // Both clients should get placeholder buffers for all keys
    ASSERT_EQ(buffers1.size(), static_cast<size_t>(keyCount));
    ASSERT_EQ(buffers2.size(), static_cast<size_t>(keyCount));
    for (int i = 0; i < keyCount; i++) {
        ASSERT_NE(buffers1[i], nullptr) << "Client1 buffer should be placeholder, not null";
        ASSERT_EQ(buffers1[i]->GetSize(), 0) << "Client1 buffer for existing key should have zero size";
        ASSERT_NE(buffers2[i], nullptr) << "Client2 buffer should be placeholder, not null";
        ASSERT_EQ(buffers2[i]->GetSize(), 0) << "Client2 buffer for existing key should have zero size";

        // Core assertion: MemoryCopy on NX-rejected buffer returns error, does not crash
        std::string testData = "test_data";
        Status rc1 = buffers1[i]->MemoryCopy(testData.data(), testData.size());
        ASSERT_FALSE(rc1.IsOk()) << "MemoryCopy on NX-rejected buffer should return error";
        Status rc2 = buffers2[i]->MemoryCopy(testData.data(), testData.size());
        ASSERT_FALSE(rc2.IsOk()) << "MemoryCopy on NX-rejected buffer should return error";
    }

    // Verify original values are unchanged
    for (int i = 0; i < keyCount; i++) {
        std::string val;
        DS_ASSERT_OK(client1->Get(keys[i], val));
        ASSERT_EQ(val, values[i]);
    }
}

TEST_F(KVCacheExistenceOptTest, TestMCreateNXNullBufferMemoryCopySafe)
{
    std::shared_ptr<KVClient> testClient;
    InitTestKVClient(0, testClient);

    std::string key = "mcreate_nx_null_buf_" + testClient->GenerateKey();
    std::string originalValue = "original_value";

    // Pre-create the key so NX will return nullptr buffer
    DS_ASSERT_OK(testClient->Set(key, originalValue));

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.existence = ExistenceOpt::NX;

    std::vector<std::string> keys{ key };
    std::vector<uint64_t> sizes{ originalValue.size() };
    std::vector<std::shared_ptr<Buffer>> buffers;
    DS_ASSERT_OK(testClient->MCreate(keys, sizes, param, buffers));

    ASSERT_EQ(buffers.size(), 1u);
    ASSERT_NE(buffers[0], nullptr) << "Buffer should be a valid placeholder for existing key with NX";
    ASSERT_EQ(buffers[0]->GetSize(), 0) << "NX-rejected buffer should have zero size";

    // Verify that calling MemoryCopy on NX-rejected buffer returns error rather than crashing
    std::string testData = "test_data";
    Status rc = buffers[0]->MemoryCopy(testData.data(), testData.size());
    ASSERT_FALSE(rc.IsOk()) << "MemoryCopy on NX-rejected buffer should return error";
    ASSERT_EQ(rc.GetCode(), K_INVALID) << "MemoryCopy on NX-rejected buffer should return K_INVALID";

    // Verify original value is unchanged
    std::string val;
    DS_ASSERT_OK(testClient->Get(key, val));
    ASSERT_EQ(val, originalValue);
}

TEST_F(KVCacheExistenceOptTest, TestMCreateNXPartialNullBuffers)
{
    std::shared_ptr<KVClient> testClient;
    InitTestKVClient(0, testClient);

    std::string existingKey1 = "mcreate_nx_exist1_" + testClient->GenerateKey();
    std::string existingKey2 = "mcreate_nx_exist2_" + testClient->GenerateKey();
    std::string newKey1 = "mcreate_nx_new1_" + testClient->GenerateKey();
    std::string newKey2 = "mcreate_nx_new2_" + testClient->GenerateKey();

    std::string existVal1 = "existing_val_1";
    std::string existVal2 = "existing_val_2";
    std::string newVal1 = "new_val_1";
    std::string newVal2 = "new_val_2";

    // Pre-create some keys
    DS_ASSERT_OK(testClient->Set(existingKey1, existVal1));
    DS_ASSERT_OK(testClient->Set(existingKey2, existVal2));

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.existence = ExistenceOpt::NX;

    std::vector<std::string> keys{ existingKey1, newKey1, existingKey2, newKey2 };
    std::vector<uint64_t> sizes{ existVal1.size(), newVal1.size(), existVal2.size(), newVal2.size() };
    std::vector<std::shared_ptr<Buffer>> buffers;
    DS_ASSERT_OK(testClient->MCreate(keys, sizes, param, buffers));

    ASSERT_EQ(buffers.size(), keys.size());

    // Existing key buffers should have zero size (NX-rejected), new key buffers should have valid size
    ASSERT_EQ(buffers[0]->GetSize(), 0) << "Buffer for existing key1 should have zero size (NX-rejected)";
    ASSERT_GT(buffers[1]->GetSize(), 0) << "Buffer for new key1 should have valid size";
    ASSERT_EQ(buffers[2]->GetSize(), 0) << "Buffer for existing key2 should have zero size (NX-rejected)";
    ASSERT_GT(buffers[3]->GetSize(), 0) << "Buffer for new key2 should have valid size";

    // Only do MemoryCopy + MSet on valid buffers
    DS_ASSERT_OK(buffers[1]->MemoryCopy(newVal1.data(), newVal1.size()));
    DS_ASSERT_OK(buffers[3]->MemoryCopy(newVal2.data(), newVal2.size()));
    std::vector<std::shared_ptr<Buffer>> validBuffers{ buffers[1], buffers[3] };
    DS_ASSERT_OK(testClient->MSet(validBuffers));

    // Verify new keys have correct values
    std::string val;
    DS_ASSERT_OK(testClient->Get(newKey1, val));
    ASSERT_EQ(val, newVal1);
    DS_ASSERT_OK(testClient->Get(newKey2, val));
    ASSERT_EQ(val, newVal2);

    // Verify existing keys are unchanged
    DS_ASSERT_OK(testClient->Get(existingKey1, val));
    ASSERT_EQ(val, existVal1);
    DS_ASSERT_OK(testClient->Get(existingKey2, val));
    ASSERT_EQ(val, existVal2);
}

TEST_F(KVCacheExistenceOptTest, TestMCreateNXMSetWithPlaceholder)
{
    // Regression test for Issue #494: concurrent MCreate NX + MSet with unfiltered
    // placeholder buffers should not produce "Object already sealed" error.
    // Before fix: MCreate NX placeholder had isSeal=true, MSet rejected it with
    // K_OC_ALREADY_SEALED. After fix: placeholder has isSeal=false, MSet skips
    // dataSize==0 buffers.
    std::shared_ptr<KVClient> testClient;
    InitTestKVClient(0, testClient);

    std::string existingKey = "issue494_exist_" + testClient->GenerateKey();
    std::string newKey = "issue494_new_" + testClient->GenerateKey();
    std::string existVal = "existing_value";
    std::string newVal = "new_value";

    // Pre-create one key so NX will reject it
    DS_ASSERT_OK(testClient->Set(existingKey, existVal));

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.existence = ExistenceOpt::NX;

    std::vector<std::string> keys{ existingKey, newKey };
    std::vector<uint64_t> sizes{ existVal.size(), newVal.size() };
    std::vector<std::shared_ptr<Buffer>> buffers;
    DS_ASSERT_OK(testClient->MCreate(keys, sizes, param, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    ASSERT_EQ(buffers[0]->GetSize(), 0) << "Existing key buffer should be NX placeholder";
    ASSERT_GT(buffers[1]->GetSize(), 0) << "New key buffer should have valid size";

    // MemoryCopy only to valid buffer
    DS_ASSERT_OK(buffers[1]->MemoryCopy(newVal.data(), newVal.size()));

    // Core assertion: MSet with ALL buffers (including placeholder) must succeed.
    // Before fix this would fail with K_OC_ALREADY_SEALED.
    DS_ASSERT_OK(testClient->MSet(buffers));

    // Verify new key was published correctly
    std::string val;
    DS_ASSERT_OK(testClient->Get(newKey, val));
    ASSERT_EQ(val, newVal);

    // Verify existing key is unchanged
    DS_ASSERT_OK(testClient->Get(existingKey, val));
    ASSERT_EQ(val, existVal);
}

TEST_F(KVCacheExistenceOptTest, TestMCreateNXMSetAllPlaceholders)
{
    // Edge case: MSet called with ALL placeholder buffers (all keys already exist).
    // Should return OK without publishing anything.
    std::shared_ptr<KVClient> testClient;
    InitTestKVClient(0, testClient);

    std::string key1 = "issue494_allph_1_" + testClient->GenerateKey();
    std::string key2 = "issue494_allph_2_" + testClient->GenerateKey();
    std::string val1 = "val1";
    std::string val2 = "val2";

    // Pre-create all keys
    DS_ASSERT_OK(testClient->Set(key1, val1));
    DS_ASSERT_OK(testClient->Set(key2, val2));

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.existence = ExistenceOpt::NX;

    std::vector<std::string> keys{ key1, key2 };
    std::vector<uint64_t> sizes{ val1.size(), val2.size() };
    std::vector<std::shared_ptr<Buffer>> buffers;
    DS_ASSERT_OK(testClient->MCreate(keys, sizes, param, buffers));

    ASSERT_EQ(buffers[0]->GetSize(), 0);
    ASSERT_EQ(buffers[1]->GetSize(), 0);

    // MSet with all placeholders should succeed (nothing to publish)
    DS_ASSERT_OK(testClient->MSet(buffers));

    // Verify original values unchanged
    std::string val;
    DS_ASSERT_OK(testClient->Get(key1, val));
    ASSERT_EQ(val, val1);
    DS_ASSERT_OK(testClient->Get(key2, val));
    ASSERT_EQ(val, val2);
}

TEST_F(KVCacheExistenceOptTest, TestMCreateNXMultiThreadRace)
{
    // Test: multiple threads race to MCreate NX the same key.
    // Verifies no crash occurs. At least one thread should complete
    // the full MCreate+MemoryCopy+MSet flow. Others get NX-rejected
    // placeholders where MemoryCopy safely returns error.
    std::shared_ptr<KVClient> raceClient;
    InitTestKVClient(0, raceClient);

    std::string key = "mcreate_nx_race_" + raceClient->GenerateKey();
    std::string value = "race_value";

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.existence = ExistenceOpt::NX;

    constexpr int numThreads = 4;
    std::atomic<int> msetSuccessCount(0);

    std::promise<void> ready;
    std::shared_future<void> start(ready.get_future());
    ThreadPool pool(numThreads);

    std::vector<std::future<void>> futures;
    futures.reserve(numThreads);
    for (int t = 0; t < numThreads; t++) {
        futures.emplace_back(pool.Submit([&, t]() {
            start.wait();
            std::vector<std::string> keys{ key };
            std::vector<uint64_t> sizes{ value.size() };
            std::vector<std::shared_ptr<Buffer>> buffers;

            Status rc = raceClient->MCreate(keys, sizes, param, buffers);
            if (!rc.IsOk() || buffers.empty()) {
                return;
            }
            if (buffers[0]->GetSize() > 0) {
                // This thread won the NX race - valid buffer
                Status copyRc = buffers[0]->MemoryCopy(value.data(), value.size());
                if (!copyRc.IsOk()) {
                    return;
                }
                Status msetRc = raceClient->MSet(buffers);
                if (msetRc.IsOk()) {
                    msetSuccessCount++;
                }
            } else {
                // NX-rejected: MemoryCopy should return error, not crash
                std::string testData = "x";
                buffers[0]->MemoryCopy(testData.data(), testData.size());
            }
        }));
    }

    ready.set_value();
    for (auto &f : futures) {
        f.get();
    }

    // At least one thread should complete MSet
    ASSERT_GE(msetSuccessCount.load(), 1) << "At least one thread should complete MCreate+MSet";

    // Verify final value
    std::string val;
    DS_ASSERT_OK(raceClient->Get(key, val));
    ASSERT_EQ(val, value);
}

TEST_F(KVCacheExistenceOptTest, TestMCreateNXMultiThreadBatchOverlap)
{
    // Reproduce Issue #494: Concurrent MSet with NX existence on overlapping key
    // batches, shared KVClient. Pre-existing keys create placeholder buffers that
    // were incorrectly marked isSeal=true before the fix, causing MSet to fail with
    // K_OC_ALREADY_SEALED ("Object already sealed error" at object_client_impl.cpp).
    //
    // Scenario (per issue #494):
    //   2 nodes, 3 workers/node, 1 client/worker
    //   Each client randomly picks keys, checks existence (NX), MSet on miss
    //   20 ops/sec, keys 1-150000, multi-threaded shared KVClient
    //
    // This test simulates: 4 threads sharing one KVClient, each with its own
    // batch of keys where some overlap across threads and some keys pre-exist.
    std::shared_ptr<KVClient> sharedClient;
    InitTestKVClient(0, sharedClient);

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.existence = ExistenceOpt::NX;

    // Pre-create overlapping keys that some threads will hit as "already exist"
    std::vector<std::string> preExistingKeys;
    constexpr int numPreExisting = 4;
    for (int i = 0; i < numPreExisting; i++) {
        preExistingKeys.push_back(
            "issue494_preexist_" + sharedClient->GenerateKey() + "_" + std::to_string(i));
        DS_ASSERT_OK(sharedClient->Set(preExistingKeys[i], "pre_existing_value_" + std::to_string(i)));
    }

    constexpr int numThreads = 4;
    constexpr int keysPerThread = 6;
    constexpr int sharedKeyPoolSize = 8;
    constexpr int kTestDataSize = 64;
    constexpr int kKeyStride = 2;
    // Shared key pool: some new, some from preExistingKeys
    std::vector<std::string> sharedKeyPool;
    for (int i = 0; i < sharedKeyPoolSize; i++) {
        if (i < numPreExisting) {
            sharedKeyPool.push_back(preExistingKeys[i]);
        } else {
            sharedKeyPool.push_back(
                "issue494_new_" + sharedClient->GenerateKey() + "_" + std::to_string(i));
        }
    }

    std::atomic<int> msetOkCount(0);
    std::atomic<int> sealedErrorCount(0);
    std::atomic<int> totalOps(0);

    std::promise<void> ready;
    std::shared_future<void> start(ready.get_future());
    ThreadPool pool(numThreads);

    std::vector<std::future<void>> futures;
    futures.reserve(numThreads);
    for (int t = 0; t < numThreads; t++) {
        futures.emplace_back(pool.Submit([&, t]() {
            start.wait();
            // Each thread picks keysPerThread keys, rotated by thread index
            // to create overlapping access patterns
            std::vector<std::string> keys;
            std::vector<uint64_t> sizes;
            for (int j = 0; j < keysPerThread; j++) {
                int poolIdx = (t * kKeyStride + j) % sharedKeyPoolSize;
                keys.push_back(sharedKeyPool[poolIdx]);
                sizes.push_back(kTestDataSize);
            }

            std::vector<std::shared_ptr<Buffer>> buffers;
            Status rc = sharedClient->MCreate(keys, sizes, param, buffers);
            if (!rc.IsOk()) {
                if (rc.GetCode() == StatusCode::K_OC_ALREADY_SEALED) {
                    sealedErrorCount++;
                }
                totalOps++;
                return;
            }

            // MemoryCopy data into non-placeholder buffers, then MSet all
            for (size_t i = 0; i < keys.size(); i++) {
                if (buffers[i]->GetSize() > 0) {
                    std::string data = "data_" + std::to_string(t) + "_" + std::to_string(i);
                    data.resize(kTestDataSize);
                    buffers[i]->MemoryCopy(data.data(), data.size());
                }
            }
            rc = sharedClient->MSet(buffers);
            if (!rc.IsOk()) {
                if (rc.GetCode() == StatusCode::K_OC_ALREADY_SEALED) {
                    sealedErrorCount++;
                }
            } else {
                msetOkCount++;
            }
            totalOps++;
        }));
    }

    ready.set_value();
    for (auto &f : futures) {
        f.get();
    }

    // Core assertion: zero K_OC_ALREADY_SEALED errors.
    // Before fix: placeholder buffers from NX-rejected keys had isSeal=true,
    // causing MSet to reject them with K_OC_ALREADY_SEALED.
    ASSERT_EQ(sealedErrorCount.load(), 0)
        << "K_OC_ALREADY_SEALED should not occur. Placeholder buffers must have isSeal=false";

    // All threads should have completed operations
    ASSERT_EQ(totalOps.load(), numThreads);

    // Verify keys can be read back
    for (size_t i = numPreExisting; i < sharedKeyPool.size(); i++) {
        std::string val;
        Status rc = sharedClient->Get(sharedKeyPool[i], val);
        if (rc.IsOk()) {
            ASSERT_FALSE(val.empty());
        }
    }
}

}  // namespace st
}  // namespace datasystem
