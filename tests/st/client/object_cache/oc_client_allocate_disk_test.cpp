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
 * Description: ObjInterface test.
 */
#include <gtest/gtest.h>
#include <unistd.h>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_size_limit);

namespace datasystem {
namespace st {
namespace {
constexpr int64_t SHM_SIZE = 1024 * 1024;
constexpr int64_t K_SIZE = 1024;
constexpr size_t TOTAL_WORKER_NUM = 4;
}  // namespace
class OCClientAllocateDiskTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = TOTAL_WORKER_NUM;
        opts.numEtcd = 1;
        opts.numOcThreadNum = getThreadNum_;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = FormatString(" -shared_memory_size_mb=500 -v=2 -log_monitor=true");
        for (size_t i = 0; i < opts.numWorkers; i++) {
            std::string dir = GetTestCaseDataDir() + "/worker" + std::to_string(i) + "/shared_disk";
            int size = 32;
            opts.workerSpecifyGflagParams[i] +=
                FormatString(" -shared_disk_directory=%s -shared_disk_size_mb=%d", dir, size);
        }
    }

    void GenerateKeyValues(std::vector<std::string> &keys, std::vector<std::string> &values, int num, int valSize)
    {
        auto keySize = 20;
        for (int i = 0; i < num; i++) {
            keys.emplace_back(randomData_.GetRandomString(keySize));
            values.emplace_back(randomData_.GetRandomString(valSize));
        }
    }

protected:
    const int getThreadNum_ = 8;
};

TEST_F(OCClientAllocateDiskTest, Create)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    std::string objKey = NewObjectKey();
    std::string data(SHM_SIZE, 'x');
    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(client->Create(objKey, data.size(), param, buffer));
}

TEST_F(OCClientAllocateDiskTest, TestDiskFull)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    int sleepSecs = 5;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "disk_detecter.free_bytes", "1*call()"));
    sleep(sleepSecs);
    std::string objKey = NewObjectKey();
    std::string data(SHM_SIZE, 'x');
    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    param.cacheType = CacheType::DISK;
    auto status = client->Create(objKey, data.size(), param, buffer);
    ASSERT_EQ(status.GetCode(), StatusCode::K_RUNTIME_ERROR);

    sleep(sleepSecs);
    DS_ASSERT_OK(client->Create(objKey, data.size(), param, buffer));
}

TEST_F(OCClientAllocateDiskTest, PublishThroughShm)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    std::string objKey = NewObjectKey();
    std::string data(SHM_SIZE, 'x');
    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(client->Create(objKey, data.size(), param, buffer));
    buffer->WLatch();
    buffer->MemoryCopy((void *)data.data(), data.size());
    DS_ASSERT_OK(buffer->Publish());
    buffer->UnWLatch();
}

TEST_F(OCClientAllocateDiskTest, PublishNotThroughShm)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    std::string objKey = NewObjectKey();
    std::string data(K_SIZE, 'x');
    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(client->Create(objKey, data.size(), param, buffer));
    buffer->WLatch();
    buffer->MemoryCopy((void *)data.data(), data.size());
    DS_ASSERT_OK(buffer->Publish());
    buffer->UnWLatch();
}

TEST_F(OCClientAllocateDiskTest, PublishOOM)
{
    FLAGS_spill_directory = "./spill_PublishOOM";
    constexpr size_t limit = 100;
    FLAGS_spill_size_limit = limit;

    std::shared_ptr<ObjectClient> client;
    const int32_t kDefaultClientTimeout = 5000;
    InitTestClient(0, client, kDefaultClientTimeout);

    std::string objKey = NewObjectKey();
    std::string data(SHM_SIZE * K_SIZE, 'x');
    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_NOT_OK(client->Create(objKey, data.size(), param, buffer));
}

TEST_F(OCClientAllocateDiskTest, Set)
{
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);

    SetParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(client1->Set("key1", "value", param));
}

TEST_F(OCClientAllocateDiskTest, SetCacheTypeChange)
{
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);

    std::shared_ptr<KVClient> client2;
    InitTestKVClient(1, client2);
    SetParam param;
    DS_ASSERT_OK(client1->Set("key1", "value1", param));

    param.cacheType = CacheType::DISK;
    DS_ASSERT_NOT_OK(client2->Set("key1", "value2", param));
    DS_ASSERT_OK(client2->Set("key2", "value2", param));

    param.cacheType = CacheType::MEMORY;
    DS_ASSERT_NOT_OK(client2->Set("key2", "value1", param));

    DS_ASSERT_OK(client1->Del("key1"));
    DS_ASSERT_OK(client2->Del("key2"));

    DS_ASSERT_OK(client1->Set("key2", "value1", param));

    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(client2->Set("key1", "value2", param));
}

TEST_F(OCClientAllocateDiskTest, MSet)
{
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);

    MSetParam param;
    param.cacheType = CacheType::DISK;

    std::vector<std::string> keys, vals;
    std::vector<StringView> values;

    size_t maxElementSize = 8;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1->MSet(keys, values, failedKeys, param));
}

TEST_F(OCClientAllocateDiskTest, MSetCacheTypeChange)
{
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);

    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    
    size_t maxElementSize = 20;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }

    size_t index = 3;
    SetParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(client1->Set(keys[index], "qqqqqq", param));

    std::vector<std::string> failedKeys;
    MSetParam param1;
    param1.cacheType = CacheType::MEMORY;
    DS_ASSERT_OK(client1->MSet(keys, values, failedKeys, param1));

    size_t failedSize = 1;
    ASSERT_EQ(failedKeys.size(), failedSize);

    std::string val1;
    DS_ASSERT_OK(client1->Get(keys[index], val1, 0));
    ASSERT_EQ(val1, "qqqqqq");
}

TEST_F(OCClientAllocateDiskTest, MSetTxCacheTypeChange)
{
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);

    std::vector<std::string> keys, vals;
    std::vector<StringView> values;

    size_t maxElementSize = 20;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }

    size_t index = 3;
    SetParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(client1->Set(keys[index], "qqqqqq", param));

    MSetParam param1;
    param1.cacheType = CacheType::MEMORY;
    DS_ASSERT_NOT_OK(client1->MSetTx(keys, values, param1));

    std::string val1;
    DS_ASSERT_OK(client1->Get(keys[index], val1, 0));
    ASSERT_EQ(val1, "qqqqqq");
}

TEST_F(OCClientAllocateDiskTest, MSetTx)
{
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);

    MSetParam param;
    param.cacheType = CacheType::DISK;
    param.existence = ExistenceOpt::NX;
    uint32_t ttl = 3;
    param.ttlSecond = ttl;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;

    size_t maxElementSize = 8;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }

    DS_ASSERT_OK(client1->MSetTx(keys, values, param));
}

TEST_F(OCClientAllocateDiskTest, LocalGet)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);

    std::string obj1Id = NewObjectKey();
    std::string data1 = GenRandomString(SHM_SIZE);

    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(cliLocal->Create(obj1Id, data1.size(), param, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data1.data(), data1.size()));
    DS_ASSERT_OK(buffer->Seal({}));
    DS_ASSERT_OK(buffer->UnWLatch());

    std::vector<std::string> getObjList = { obj1Id };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliLocal->Get(getObjList, 0, dataList));
}

TEST_F(OCClientAllocateDiskTest, RemoteGet)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cliRemote1;
    InitTestClient(1, cliRemote1);

    std::string obj1Id = NewObjectKey();
    std::string data1 = GenRandomString(SHM_SIZE);

    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    param.cacheType = CacheType::DISK;
    DS_ASSERT_OK(cliRemote1->Create(obj1Id, data1.size(), param, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data1.data(), data1.size()));
    DS_ASSERT_OK(buffer->Seal({}));
    DS_ASSERT_OK(buffer->UnWLatch());

    std::vector<std::string> getObjList = { obj1Id };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliLocal->Get(getObjList, 0, dataList));
}

TEST_F(OCClientAllocateDiskTest, RemoteGetDelay)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cliRemote1;
    InitTestClient(1, cliRemote1);

    std::string obj1Id = NewObjectKey();
    std::string data1 = GenRandomString(SHM_SIZE);

    const size_t minThreadNum = 2;
    ThreadPool threadPool(minThreadNum);
    std::vector<std::string> getObjList = { obj1Id };
    // get data from local to multiple remote
    auto fut1 = threadPool.Submit([getObjList, cliLocal, data1]() {
        LOG(INFO) << "cliLocal starts to get object at first time.";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_NOT_OK(cliLocal->Get(getObjList, 0, dataList));
        LOG(INFO) << "cliLocal ends to get object at first time.";

        LOG(INFO) << "cliLocal starts to get object at second time.";
        Timer timer;
        dataList.clear();
        DS_ASSERT_OK(cliLocal->Get(getObjList, 1500, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cost: " << timeCost;
        ASSERT_TRUE(timeCost < 1500);
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], data1);
        LOG(INFO) << "cliLocal ends to get object at second time.";
    });

    // create data in remote node after 1000 milliseconds
    auto fut2 = threadPool.Submit([getObjList, obj1Id, &data1, cliRemote1, this]() {
        LOG(INFO) << "cliRemote1 starts to create object";
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        std::shared_ptr<Buffer> buffer;
        CreateParam param;
        param.cacheType = CacheType::DISK;
        DS_ASSERT_OK(cliRemote1->Create(obj1Id, data1.size(), param, buffer));
        DS_ASSERT_OK(buffer->WLatch());
        DS_ASSERT_OK(buffer->MemoryCopy((void *)data1.data(), data1.size()));
        DS_ASSERT_OK(buffer->Seal({}));
        DS_ASSERT_OK(buffer->UnWLatch());

        LOG(INFO) << "cliRemote1 ends to create object";
    });

    fut1.get();
    fut2.get();
}

TEST_F(OCClientAllocateDiskTest, TestSameKeyParallel)
{
    std::shared_ptr<KVClient> client;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client);
    InitTestKVClient(0, client1);
    std::string key = "key";
    SetParam param;
    param.cacheType = CacheType::DISK;

    auto testFun = [&](int num) {
        std::string value = "value" + std::to_string(num);
        DS_ASSERT_OK(client->Set(key, value, param));
        std::string getValue;
        DS_ASSERT_OK(client->Get(key, getValue));
        DS_ASSERT_OK(client1->Get(key, getValue));
    };

    int threadNum = 100;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(threadPool.Submit(testFun, i));
    }
    for (auto &future : futures) {
        future.get();
    }
}

TEST_F(OCClientAllocateDiskTest, TestDifferentKeyParallel)
{
    std::shared_ptr<KVClient> client;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client);
    InitTestKVClient(0, client1);
    SetParam param;
    param.cacheType = CacheType::DISK;
    auto testFun = [&](std::string value) {
        std::vector<std::string> keys, vals;
        size_t maxElementSize = 5;
        auto dataSize = 20;
        GenerateKeyValues(keys, vals, maxElementSize, dataSize);

        for (size_t i = 0; i < keys.size(); i++) {
            DS_ASSERT_OK(client->Set(keys[i], vals[i], param));
        }
        size_t index = 2;
        DS_ASSERT_OK(client->Set(keys[index], value, param));
        std::string getValue;
        DS_ASSERT_OK(client->Get(keys[index], getValue));
        DS_ASSERT_OK(client1->Get(keys[index], getValue));
    };

    int threadNum = 30;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < threadNum; i++) {
        std::string value = "value" + std::to_string(i);
        futures.emplace_back(threadPool.Submit(testFun, value));
    }
    for (auto &future : futures) {
        future.get();
    }
}

}  // namespace st
}  // namespace datasystem
