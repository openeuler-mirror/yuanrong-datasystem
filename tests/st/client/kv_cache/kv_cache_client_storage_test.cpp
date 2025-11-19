/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: state cache save to cloud storage test
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include <chrono>
#include <csignal>
#include <thread>

#include "client/kv_cache/kv_client_common.h"
#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace st {
const uint32_t ASYNC_DELETE_TIME_MS = 200;

class KVCacheClientStorageTest : virtual public OCClientCommon, public KVClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        constexpr uint8_t WORKER_NUM = 1;
        opts.workerGflagParams = "-check_async_queue_empty_time_s=15";
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = WORKER_NUM;
        opts.enableDistributedMaster = "true";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void InitConnectOptW(uint32_t workerIndex, ConnectOptions &connectOptions, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        connectOptions.tenantId = "tanfasd";
    }

    void InitTestKVClientWithTenant(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions connectOptions;
        InitConnectOptW(workerIndex, connectOptions);
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
};

TEST_F(KVCacheClientStorageTest, TestSetWriteBackDel)
{
    std::shared_ptr<KVClient> client, client1;
    InitTestKVClient(0, client);
    std::string key = "key";
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    ASSERT_EQ(client->Set(key, value, param), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ObjectMetaStore.AsyncMetaOpToEtcdStorageHandler.Delay.MetaTable",
                                           "call(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "global_cache_delete.delete_objects", "sleep(5000)"));
    ASSERT_EQ(client->Del(key), Status::OK());
    sleep(1);
    DS_ASSERT_NOT_OK(client->Get(key, valueGet));
}

TEST_F(KVCacheClientStorageTest, TestSetStorageRepeatly)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key1 = "key1";
    std::string value1 = "value1";
    std::string value2 = "value2";
    SetParam param1{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.before_pop_from_queue", "100*sleep(5)"));
    Status setRes = client->Set(key1, value1, param1);
    ASSERT_TRUE(setRes.IsOk());
    setRes = client->Set(key1, value2, param1);
    ASSERT_TRUE(setRes.IsOk());
}

TEST_F(KVCacheClientStorageTest, TestSetStorageNotExit)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.del", "sleep(1000)"));
    std::string value1 = "value1";
    SetParam param1{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (int i = 0; i < 5; i++) { // obj num is 5
        auto key1 = GetStringUuid();
        DS_ASSERT_OK(client->Set(key1, value1, param1));
    }
}

TEST_F(KVCacheClientStorageTest, LEVEL2_TestSetStorageWhenWorkershuttingdown)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    ThreadPool threadPool(1);
    threadPool.Execute([this, &client]() {
        std::string key = "key1";
        std::string value = "value1";
        SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.save", "100*return(K_OK)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.before_pop_from_queue", "100*sleep(5)"));
        int requestNum = 10;
        int intervalTimeS = 2;
        for (int i = 0; i < requestNum; i++) {
            DS_ASSERT_OK(client->Set(key, value, param));
            std::this_thread::sleep_for(std::chrono::seconds(intervalTimeS));
        }
        client.reset();
    });
    externalCluster_->ShutdownNode(WORKER, 0);
}

TEST_F(KVCacheClientStorageTest, LEVEL1_TestSetStorageMutable)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key1 = "key1";
    std::string value1 = "value1";
    SetParam param1{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    Status setRes = client->Set(key1, value1, param1);
    ASSERT_TRUE(setRes.IsOk());

    Status setAgainRes = client->Set(key1, value1, param1);
    ASSERT_TRUE(setAgainRes.IsOk());
}

TEST_F(KVCacheClientStorageTest, TestDelStorageObjNeedDelay)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key1 = "key1";
    std::string value1 = "value1";
    SetParam param1{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    Status setRes = client->Set(key1, value1, param1);
    ASSERT_TRUE(setRes.IsOk());

    Status delRes = client->Del(key1);
    ASSERT_TRUE(delRes.IsOk());
}

class KVCacheSpillTest : public KVCacheClientStorageTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        KVCacheClientStorageTest::SetClusterSetupOptions(opts);
        const int workrCount = 2;
        opts.numWorkers = workrCount;
        opts.enableSpill = true;
        opts.numEtcd = 1;
        opts.workerGflagParams += " -shared_memory_size_mb=10 -v=1 -spill_size_limit=104857600";
        opts.injectActions = "worker.Spill.Sync:return()";
    }
};

TEST_F(KVCacheSpillTest, LEVEL1_TestSpillThenSpill)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    std::string value(1020 * 1024, 'a');
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.async_send.before_send", "8*sleep(1000)"));

    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    std::vector<std::string> objects;
    const int loopCnt = 100;
    for (int i = 0; i < loopCnt; i++) {
        std::string key = "key-" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key, value, param));
        objects.emplace_back(std::move(key));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    // try again.
    for (int i = 0; i < loopCnt; i++) {
        std::string key = "again-" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key, value, param));
        objects.emplace_back(std::move(key));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    for (size_t i = 0; i < objects.size(); i++) {
        std::string id = objects[i];
        std::string val;
        if (i % 2 == 0) {
            // 1. object evict again after delete spill data, will get from obs(mock).
            // 2. object still in memory.
            DS_ASSERT_OK(client1->Get(id, val));
        } else {
            DS_ASSERT_OK(client2->Get(id, val));
        }
        ASSERT_TRUE(val == value);
    }

    for (const auto &id : objects) {
        DS_ASSERT_OK(client1->Del(id));
    }
}

TEST_F(KVCacheSpillTest, LEVEL1_TestWorkerShutdown)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    const size_t dataSize = 1020 * 1024;  // 1MB
    std::string value(dataSize, 'a');
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.save", "return(K_OK)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.ProcessDeleteObjects", "sleep(5000)"));
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    std::vector<std::string> objects;
    const int loopCnt = 100;
    for (int i = 0; i < loopCnt; i++) {
        std::string key = "key-" + std::to_string(i);
        DS_ASSERT_OK(client->Set(key, value, param));
        objects.emplace_back(std::move(key));
    }

    for (const auto &id : objects) {
        DS_ASSERT_OK(client->Del(id));
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
}

TEST_F(KVCacheSpillTest, LEVEL1_TestSpillSpaceFull)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const size_t testSize = 1024 * 1024;
    std::string value(testSize, 'a');

    std::vector<std::string> objects;
    const int loopCnt = 50;
    const int threads = 5;
    ThreadPool pool(threads);
    for (int i = 0; i < threads; i++) {
        pool.Execute([i, &client, value] {
            for (int j = 0; j < loopCnt; j++) {
                std::string key = "key-" + std::to_string(i) + std::to_string(j);
                SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
                DS_ASSERT_OK(client->Set(key, value, param));
            }
        });
    }
}

class KVCacheNoMetaClientStorageTest : public KVCacheClientStorageTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams = "-check_async_queue_empty_time_s=15 -oc_io_from_l2cache_need_metadata=false";
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
    }
    uint8_t WORKER_NUM = 2;
};

TEST_F(KVCacheNoMetaClientStorageTest, LEVEL2_TestDeleteThenPutAndGetAfterRestart)
{
    std::shared_ptr<KVClient> client;
    int timeOutMs = 5000;
    InitTestKVClient(0, client, timeOutMs);  // timeout is 5000 ms

    std::string key = "key";
    std::string targetPath = GetTestCaseDataDir() + "/OBS/test/" + key;
    DS_ASSERT_OK(CreateDir(targetPath, true));
    std::ofstream outfile(targetPath + "/100");
    ASSERT_TRUE(outfile.is_open());
    outfile << "hello";
    outfile.close();

    for (int i = 0; i < WORKER_NUM; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "worker.DelPersistence.delay", "call(3)"));
    }

    DS_ASSERT_OK(client->Del(key));
    sleep(1);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client->Set(key, "value", param));

    const int sleepTime = 5;  // 5s;
    sleep(sleepTime);
    for (int i = 0; i < WORKER_NUM; i++) {
        DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(i));
        DS_ASSERT_OK(externalCluster_->StartWorker(i, HostPort(), " -client_reconnect_wait_s=1"));
        DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, i));
    }
    std::string val;
    auto func = [&client, &key, &val] { return client->Get(key, val); };
    auto waitTime = 15;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(func, waitTime, K_OK));
    ASSERT_EQ(val, "value");
}

TEST_F(KVCacheNoMetaClientStorageTest, DISABLED_TestDeleteVersionBetweenPutAndDel)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key = "key";
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client->Set(key, "data az1", param));

    // simulate other az put data to L2 cache.
    std::string targetPath = GetTestCaseDataDir() + "/OBS/test/" + key;
    DS_ASSERT_OK(CreateDir(targetPath, true));
    uint64_t version = GetSystemClockTimeStampUs();
    std::ofstream outfile(targetPath + "/" + std::to_string(version));
    ASSERT_TRUE(outfile.is_open());
    outfile << "data az2";
    outfile.close();

    DS_ASSERT_OK(client->Del(key));
    std::string val;
    DS_ASSERT_NOT_OK(client->Get(key, val));
}

TEST_F(KVCacheNoMetaClientStorageTest, LEVEL2_TestDeleteL2AfterWorkerRestart)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    int keyCount = 32;
    std::vector<std::string> keys;
    for (int i = 0; i < keyCount; i++) {
        auto val = "data-" + std::to_string(i);
        auto key = client->Set(val, param);
        ASSERT_TRUE(!key.empty());
        keys.emplace_back(key);
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.DelPersistenceObj.beforeDel", "return(K_TRY_AGAIN)"));
    for (auto &key : keys) {
        DS_ASSERT_OK(client->Del(key));
    }
    DS_ASSERT_OK(cluster_->ShutdownNode(ClusterNodeType::WORKER, 0));
    DS_ASSERT_OK(externalCluster_->StartWorker(0, HostPort()));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    int delayMs = 3000;
    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    for (auto &key : keys) {
        std::string val;
        DS_ASSERT_NOT_OK(client->Get(key, val));
    }
}

class KVCacheClientStorageTestMultiNode : public KVCacheClientStorageTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVCacheClientStorageTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -object_del_retry_delay_sec=1";
        opts.numEtcd = 1;
        opts.numWorkers = 3;  // start 3 nodes.
        opts.enableDistributedMaster = "true";
        opts.disableRocksDB = false;
    }
};

TEST_F(KVCacheClientStorageTestMultiNode, TestGlobalCacheTableNotLeakAfterRestart)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    auto key = client->Set(value, param);
    ASSERT_NE(key, "");
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 0, "ObjectMetaStore.AsyncMetaOpToEtcdStorageHandler.Delay.GlobalCacheTable.PassAdd", "call(10000)"));
    DS_ASSERT_OK(client->Del(key));
    int waitTimeSec = 2;
    sleep(waitTimeSec);  // Now the data in L2 cache has been deleted, but the GlobalCacheTable has not been deleted.
    DS_ASSERT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne(
        { 0 }, SIGKILL));  // Do not wait for asynchronous tasks to complete

    InitTestEtcdInstance();
    std::string tableName = ETCD_GLOBAL_CACHE_TABLE_PREFIX;
    tableName.pop_back();
    DS_ASSERT_OK(db_->CreateTable(tableName, tableName));
    const int maxWaitTimeSec = 5;
    Timer timer;
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    while (timer.ElapsedMilliSecond() < maxWaitTimeSec * SECS_TO_MS) {
        outKeyValues.clear();
        DS_ASSERT_OK(db_->GetAll(tableName, outKeyValues));
        if (outKeyValues.empty()) {
            break;
        }
        const int intervalMs = 100;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    ASSERT_TRUE(outKeyValues.empty());
}

}  // namespace st
}  // namespace datasystem
