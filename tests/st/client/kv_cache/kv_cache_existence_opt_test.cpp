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
}  // namespace st
}  // namespace datasystem