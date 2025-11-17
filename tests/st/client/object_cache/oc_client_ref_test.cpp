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
 * Description: Client increase/decrease reference test
 */
#include "oc_client_common.h"

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"

DS_DECLARE_string(etcd_address);
namespace datasystem {
namespace st {
static constexpr int64_t CLIENT_DEFAULT_TIMEOUT = 80000L;

class OCClientRefTest2 : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -enable_multi_stubs=true -v=1";
    }
};

TEST_F(OCClientRefTest2, GIncGDecTest)
{
    int clientNum = 32;
    std::vector<std::shared_ptr<ObjectClient>> clients(clientNum);
    for (size_t i = 0; i < clients.size(); ++i) {
        InitTestClient(0, clients[i]);
    }

    ThreadPool threadPool(clientNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < clientNum; i++) {
        auto &client = clients[i];
        futures.emplace_back(threadPool.Submit([&client]() {
            for (int loop = 0; loop < 50; loop++) {
                std::vector<std::string> objectKeys(100);
                for (size_t j = 0; j < objectKeys.size(); j++) {
                    objectKeys[j] = GetStringUuid();
                }
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
                ASSERT_TRUE(failObjects.empty());
                DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }
        }));
    }
    for (auto &future : futures) {
        future.get();
    }
}

class OCClientRefTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams = FormatString(" -shared_memory_size_mb=3000 ");
    }

    int CalcGRefNumOnWorker(std::string objectKey, std::shared_ptr<ObjectClient> client)
    {
        return client->QueryGlobalRefNum(objectKey);
    }
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
};

TEST_F(OCClientRefTest, GRefEmptyIdVectorTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey };

    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    ASSERT_GE(client->QueryGlobalRefNum(objectKey), 0);
}

TEST_F(OCClientRefTest, GDecreaseRefTestCore)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, 2000); // timeout is 2000 ms
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey };

    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "entry.setNull", "call()"));
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
}

TEST_F(OCClientRefTest, GRefIncDecTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey };

    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    ASSERT_EQ(CalcGRefNumOnWorker(objectKey, client), 1);

    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
}

TEST_F(OCClientRefTest, GRefDecPartNotExistTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::vector<std::string> failObjects;

    DS_ASSERT_OK(client->GIncreaseRef({ objectKey1, objectKey2 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey2 }, failObjects));
    ASSERT_TRUE(failObjects.empty());

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1, objectKey2 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
}

TEST_F(OCClientRefTest, GRefPubGetTest)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };

    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey };

    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    CreateAndSealObject(client1, objectKey, data);

    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    ASSERT_EQ(CalcGRefNumOnWorker(objectKey, client1), 0);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey, client2), 0);

    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
}

TEST_F(OCClientRefTest, TestClientDisconnectFailed)
{
    cluster_->StartOBS();
    std::shared_ptr<ObjectClient> client1, client2;
    InitTestClient(0, client1);  // the worker index is 2 // the worker index is 2
    InitTestClient(1, client2);  // the worker index is 1
    int timeout = 10;
    std::string data = "111111111";
    CreateParam param{};
    std::string testcasename = "trouble_ticket_";
    std::vector<std::string> failObjects;
    int objNum = 10;
    for (int j = 0; j < objNum; ++j) {
        std::shared_ptr<Buffer> buffer;
        std::vector<Optional<Buffer>> buffers;
        std::string objectKey = testcasename + std::to_string(j);
        DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failObjects));
        ASSERT_EQ(static_cast<uint64_t>(failObjects.size()), static_cast<uint64_t>(0));
        DS_ASSERT_OK(client1->Create(objectKey, data.size(), param, buffer));
        ASSERT_NE(buffer, nullptr);
        ASSERT_EQ(data.size(), static_cast<uint64_t>(buffer->GetSize()));
        DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
        ASSERT_EQ(data.size(), static_cast<uint64_t>(buffer->GetSize()));
        DS_ASSERT_OK(buffer->Publish());
        auto m = client2->QueryGlobalRefNum({ objectKey });
        ASSERT_EQ(m, 1);
    }

    // w3到etcd网络故障 NODE_TIMEOUT_S: 5, the inject worker index is 2
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "etcd.failed.add.deleteobjecttoetcd", "2*return()"));
    client1.reset();

    sleep(10);  // sleep 10 to wait for async decrease

    for (int j = 0; j < objNum; ++j) {
        std::shared_ptr<Buffer> buffer;
        std::vector<Optional<Buffer>> buffers;
        std::string objectKey = testcasename + std::to_string(j);
        auto m = client2->QueryGlobalRefNum({ objectKey });
        ASSERT_EQ(m, 0);
        DS_ASSERT_NOT_OK(client2->Get({ objectKey }, timeout, buffers));
    }
}

TEST_F(OCClientRefTest, GRefAsyncIncrease)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    std::vector<std::string> objectKeys = { objectKey };
    CreateAndSealObject(client1, objectKey, data);

    std::vector<Optional<Buffer>> dataList;

    DS_ASSERT_OK(client2->Get(objectKeys, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    ThreadPool threadPool(10);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
            futures.emplace_back(threadPool.Submit([&client1, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }));
        } else {
            futures.emplace_back(threadPool.Submit([&client2, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }));
        }
    }
    for (auto &future : futures) {
        future.get();
    }
    std::vector<std::string> failObjects;
    for (int i = 0; i < 5; i++) {
        DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
        ASSERT_TRUE(failObjects.empty());
        DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
        ASSERT_TRUE(failObjects.empty());
    }

    ASSERT_EQ(CalcGRefNumOnWorker(objectKey, client1), 0);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey, client2), 0);

    ASSERT_TRUE(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    ASSERT_TRUE(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
}

TEST_F(OCClientRefTest, GRefAsyncIncDec)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    for (int i = 0; i < 5; i++) {
        std::vector<std::string> failObjects;
        DS_ASSERT_OK(client->GIncreaseRef({ objectKey1 }, failObjects));
        ASSERT_TRUE(failObjects.empty());
    }
    ThreadPool threadPool(10);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
            futures.emplace_back(threadPool.Submit([&client, &objectKey2]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client->GIncreaseRef({ objectKey2 }, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }));
        } else {
            futures.emplace_back(threadPool.Submit([&client, &objectKey1]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }));
        }
    }
    for (auto &future : futures) {
        future.get();
    }
    for (int i = 0; i < 5; i++) {
        std::vector<std::string> failObjects;
        DS_ASSERT_OK(client->GDecreaseRef({ objectKey2 }, failObjects));
        ASSERT_TRUE(failObjects.empty());
    }

    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1, client), 0);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2, client), 0);

    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey1, objectKey2 };
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
}

TEST_F(OCClientRefTest, GRefAndQueryTest)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client2);
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(1, client3);

    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::string objectKey3 = NewObjectKey();
    std::string objectKey4 = NewObjectKey();

    auto incFunc = [](std::vector<std::string> &ids, std::shared_ptr<ObjectClient> &client, int times) {
        std::vector<std::string> failObjects;
        for (int i = 0; i < times; i++) {
            DS_ASSERT_OK(client->GIncreaseRef(ids, failObjects));
            ASSERT_TRUE(failObjects.empty());
        }
    };
    auto decFunc = [](std::vector<std::string> &ids, std::shared_ptr<ObjectClient> &client, int times) {
        std::vector<std::string> failObjects;
        for (int i = 0; i < times; i++) {
            DS_ASSERT_OK(client->GDecreaseRef(ids, failObjects));
            ASSERT_TRUE(failObjects.empty());
        }
    };
    std::vector<std::string> ids = { objectKey1, objectKey3 };
    incFunc(ids, client1, 3);
    ids = { objectKey2, objectKey3 };
    incFunc(ids, client2, 2);
    ids = { objectKey1, objectKey3, objectKey4 };
    incFunc(ids, client3, 4);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1, client1), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2, client1), 1);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey3, client1), 3);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey4, client1), 1);

    ids = { objectKey3 };
    decFunc(ids, client2, 2);
    ids = { objectKey1 };
    decFunc(ids, client1, 3);
    ids = { objectKey1 };
    decFunc(ids, client3, 4);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1, client1), 0);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2, client1), 1);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey3, client1), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey4, client1), 1);
}

TEST_F(OCClientRefTest, GRefExistAliveTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey };

    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(client1, objectKey, data);

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    DS_ASSERT_NOT_OK(client2->Get({ objectKey }, 0, dataList));
    // Get will failed
}

TEST_F(OCClientRefTest, OneNodeIncOtherDealData)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey };
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(client2, objectKey, data);
    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_NOT_OK(client2->Get({ objectKey }, 0, dataList));
}

TEST_F(OCClientRefTest, NestedNormalTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::string objectKey3 = NewObjectKey();
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    std::vector<uint8_t> data2 = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(client, objectKey2, data2);
    std::vector<uint8_t> data3 = { 55, 56, 57, 58, 59, 60 };
    CreateAndSealObject(client, objectKey3, data3);
    std::vector<uint8_t> data1 = { 45, 46, 47, 48, 49, 50 };
    CreateAndSealObject(client, objectKey1, data1, { objectKey2, objectKey3 });

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, dataList));
    ASSERT_EQ(dataList.size(), size_t(3));
    ASSERT_TRUE(NotExistsNone(dataList));

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    dataList.clear();
    DS_ASSERT_OK(client->Get({ objectKey2, objectKey3 }, 0, dataList));
    ASSERT_EQ(dataList.size(), size_t(2));
    ASSERT_TRUE(NotExistsNone(dataList));

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    dataList.clear();
    DS_ASSERT_NOT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, dataList));
}

TEST_F(OCClientRefTest, TestNoShmNested)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    CreateParam param{};
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::string objectKey3 = NewObjectKey();
    char data[] = { '1', '2', '3' };
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client->Create(objectKey1, sizeof(data), param, buffer1));
    buffer1->MemoryCopy(reinterpret_cast<uint8_t *>(data), sizeof(data));

    std::shared_ptr<Buffer> buffer2;
    DS_ASSERT_OK(client->Create(objectKey2, sizeof(data), param, buffer2));
    buffer2->MemoryCopy(reinterpret_cast<uint8_t *>(data), sizeof(data));
    DS_ASSERT_OK(buffer2->Seal());

    std::shared_ptr<Buffer> buffer3;
    DS_ASSERT_OK(client->Create(objectKey3, sizeof(data), param, buffer3));
    buffer3->MemoryCopy(reinterpret_cast<uint8_t *>(data), sizeof(data));
    DS_ASSERT_OK(buffer3->Seal());

    DS_ASSERT_OK(buffer1->Seal({ objectKey2, objectKey3 }));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 10000, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_TRUE(buffers.size() == 3);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    buffers.clear();
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 10000, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_TRUE(buffers.size() == 3);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    buffers.clear();
    DS_ASSERT_NOT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 3000, buffers));
}

TEST_F(OCClientRefTest, TestPutNested)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    CreateParam param{};
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::string objectKey3 = NewObjectKey();
    std::string data = GenRandomString();

    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    DS_ASSERT_OK(
        client->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));

    DS_ASSERT_OK(
        client->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));

    DS_ASSERT_OK(client->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                             param, { objectKey2, objectKey3 }));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 10000, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_TRUE(buffers.size() == 3);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    buffers.clear();
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 10000, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_TRUE(buffers.size() == 3);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    buffers.clear();
    DS_ASSERT_NOT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 3000, buffers));
}

TEST_F(OCClientRefTest, TestPutNested2)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    CreateParam param{};
    std::string objectKey1 = "key_1";
    std::string objectKey2 = "key_2";
    std::string objectKey3 = "key_3";
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    std::string data = GenRandomString();

    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));

    // Test put
    DS_ASSERT_OK(
        client->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(
        client->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));

    DS_ASSERT_OK(
        client->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param, {}));
    DS_ASSERT_OK(client->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                             param, { objectKey2, objectKey3 }));
    DS_ASSERT_NOT_OK(client->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                                 param, { "key_4" }));
    DS_ASSERT_OK(client->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                             param, { objectKey2, objectKey3 }));

    // Test publish and seal
    std::vector<Optional<Buffer>> b1;
    DS_ASSERT_OK(client->Get({ objectKey1 }, 0, b1));
    ASSERT_TRUE(b1[0]);
    DS_ASSERT_NOT_OK(b1[0]->Publish());
    DS_ASSERT_NOT_OK(b1[0]->Seal({ objectKey2 }));
    DS_ASSERT_OK(b1[0]->Seal({ objectKey2, objectKey3 }));

    // Get verify
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && buffers[0] && buffers[1] && buffers[2]);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    DS_ASSERT_OK(client->Get({ objectKey1 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 1 && buffers[0]);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failObjects));
    DS_ASSERT_NOT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
}

TEST_F(OCClientRefTest, TestPutNested3)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client);
    InitTestClient(1, client2);
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    std::string objectKey1 = "key_1";
    std::string objectKey2 = "key_2";
    std::string objectKey3 = "key_3";
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    std::string data = GenRandomString();

    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    // Test put
    DS_ASSERT_OK(
        client->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(
        client->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(client->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                             param, { objectKey2, objectKey3 }));
    DS_ASSERT_OK(
        client2->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(
        client2->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_NOT_OK(
        client2->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param, {}));
    DS_ASSERT_NOT_OK(client2->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                                  param, { "key4" }));
    DS_ASSERT_OK(client2->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              param, { objectKey2, objectKey3 }));

    // client2 seal success
    std::vector<Optional<Buffer>> b2;
    DS_ASSERT_OK(client2->Get({ objectKey1 }, 0, b2));
    ASSERT_TRUE(b2[0]);
    DS_ASSERT_OK(b2[0]->Seal({ objectKey2, objectKey3 }));

    // client1 seal failed
    std::vector<Optional<Buffer>> b1;
    DS_ASSERT_OK(client->Get({ objectKey1 }, 0, b1));
    ASSERT_TRUE(b1[0]);
    DS_ASSERT_NOT_OK(b1[0]->Seal({ objectKey2, objectKey3 }));

    // Get verify
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && buffers[0] && buffers[1] && buffers[2]);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && buffers[0] && buffers[1] && buffers[2]);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_NOT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
}

TEST_F(OCClientRefTest, TestPutNested4)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client);
    InitTestClient(1, client2);
    CreateParam param{};
    std::string objectKey1 = "key_1";
    std::string objectKey2 = "key_2";
    std::string objectKey3 = "key_3";
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    std::string data = GenRandomString();

    DS_ASSERT_OK(
        client->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param, {}));

    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(
        client2->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(
        client2->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(client2->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              param, { objectKey2, objectKey3 }));

    // Get verify
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && buffers[0] && buffers[1] && buffers[2]);

    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    DS_ASSERT_OK(client2->Get({ objectKey1 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 1 && buffers[0]);

    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey1 }, failObjects));
    DS_ASSERT_NOT_OK(client2->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && !buffers[0] && !buffers[1] && !buffers[2]);
}

TEST_F(OCClientRefTest, TestPublishNested1)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    CreateParam param{};
    std::string objectKey1 = "key_1";
    std::string objectKey2 = "key_2";
    std::string objectKey3 = "key_3";
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    std::string data = GenRandomString();

    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));

    // Test put
    DS_ASSERT_OK(
        client->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(
        client->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));

    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey1, data.size(), param, buffer));
    buffer->MemoryCopy(reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Publish({ objectKey2, objectKey3 }));
    DS_ASSERT_NOT_OK(buffer->Publish({ "key_4" }));
    DS_ASSERT_OK(buffer->Publish({ objectKey2, objectKey3 }));
    DS_ASSERT_NOT_OK(buffer->Seal({ objectKey2 }));
    DS_ASSERT_OK(buffer->Seal({ objectKey2, objectKey3 }));

    // Get verify
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && buffers[0] && buffers[1] && buffers[2]);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    DS_ASSERT_OK(client->Get({ objectKey1 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 1 && buffers[0]);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failObjects));
    DS_ASSERT_NOT_OK(client->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && !buffers[0] && !buffers[1] && !buffers[2]);
}

TEST_F(OCClientRefTest, TestPublishNested2)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client);
    InitTestClient(1, client2);
    CreateParam param{};
    std::string objectKey1 = "key_1";
    std::string objectKey2 = "key_2";
    std::string objectKey3 = "key_3";
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    std::string data = GenRandomString();

    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));

    DS_ASSERT_OK(
        client->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(
        client->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(client->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                             param, { objectKey2, objectKey3 }));
    DS_ASSERT_OK(
        client2->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(
        client2->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));

    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client2->Create(objectKey1, data.size(), param, buffer));
    buffer->MemoryCopy(reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size());
    DS_ASSERT_NOT_OK(buffer->Publish());
    DS_ASSERT_NOT_OK(buffer->Publish({ "key_4" }));
    DS_ASSERT_OK(buffer->Publish({ objectKey2, objectKey3 }));
    DS_ASSERT_NOT_OK(buffer->Seal({ "key_4" }));
    DS_ASSERT_OK(buffer->Seal({ objectKey2, objectKey3 }));

    // client1 seal failed
    std::vector<Optional<Buffer>> b1;
    DS_ASSERT_OK(client->Get({ objectKey1 }, 0, b1));
    ASSERT_TRUE(b1[0]);
    DS_ASSERT_NOT_OK(b1[0]->Seal({ objectKey2, objectKey3 }));

    // Get verify
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && buffers[0] && buffers[1] && buffers[2]);

    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && buffers[0] && buffers[1] && buffers[2]);

    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey1 }, failObjects));
    DS_ASSERT_NOT_OK(client2->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && !buffers[0] && !buffers[1] && !buffers[2]);
}

TEST_F(OCClientRefTest, TestPublishNested3)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client);
    InitTestClient(1, client2);
    CreateParam param{};
    std::string objectKey1 = "key_1";
    std::string objectKey2 = "key_2";
    std::string objectKey3 = "key_3";
    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    std::string data = GenRandomString();

    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client->Create(objectKey1, data.size(), param, buffer1));
    buffer1->MemoryCopy(reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size());
    DS_ASSERT_OK(buffer1->Publish());

    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(
        client2->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    DS_ASSERT_OK(
        client2->Put(objectKey3, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param));
    std::shared_ptr<Buffer> buffer2;
    DS_ASSERT_OK(client2->Create(objectKey1, data.size(), param, buffer2));
    buffer2->MemoryCopy(reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size());
    DS_ASSERT_OK(buffer2->Publish({ objectKey2, objectKey3 }));

    // Get verify
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && buffers[0] && buffers[1] && buffers[2]);

    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    DS_ASSERT_OK(client2->Get({ objectKey1 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 1 && buffers[0]);

    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey1 }, failObjects));
    DS_ASSERT_NOT_OK(client2->Get({ objectKey1, objectKey2, objectKey3 }, 0, buffers));
    ASSERT_TRUE(buffers.size() == 3 && !buffers[0] && !buffers[1] && !buffers[2]);
}

TEST_F(OCClientRefTest, NestedChildTest)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(2, client3);
    CreateParam param1{};
    CreateParam param2{};
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();

    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };

    std::shared_ptr<Buffer> buffer1;
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1 }, failObjects));
    ASSERT_TRUE(failObjects.empty());

    DS_ASSERT_OK(client1->Create(objectKey1, data.size(), param1, buffer1));
    DS_ASSERT_OK(buffer1->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer1->Seal({ objectKey2 }));

    std::shared_ptr<Buffer> buffer2;
    DS_ASSERT_OK(client2->Create(objectKey2, data.size(), param2, buffer2));
    DS_ASSERT_OK(buffer2->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey2 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(buffer2->Seal());

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client3->GIncreaseRef({ objectKey1, objectKey2 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client3->Get({ objectKey1, objectKey2 }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1 }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey2 }, failObjects));
    ASSERT_TRUE(failObjects.empty());

    DS_ASSERT_OK(client3->Get({ objectKey1, objectKey2 }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    DS_ASSERT_OK(client3->GDecreaseRef({ objectKey1, objectKey2 }, failObjects));
    ASSERT_TRUE(failObjects.empty());

    DS_ASSERT_NOT_OK(client3->Get({ objectKey1, objectKey2 }, 0, dataList));
}

TEST_F(OCClientRefTest, NestedComplexTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    std::string parentObjKey = NewObjectKey();
    std::string firstId = parentObjKey;
    std::string lastId;

    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    DS_ASSERT_OK(client->Create(parentObjKey, data.size(), param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));

    LOG(INFO) << "First id is : " << firstId;
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client->GIncreaseRef({ firstId }, failObjects));
    ASSERT_TRUE(failObjects.empty());

    // Relationship : First -> Obj_1 -> ... -> Obj_x -> Last
    for (int times = 0; times < 100; times++) {
        std::string childObjKey = NewObjectKey();
        std::shared_ptr<Buffer> buffer1;
        DS_ASSERT_OK(client->Create(childObjKey, data.size(), param, buffer1));
        DS_ASSERT_OK(buffer1->MemoryCopy(data.data(), data.size()));
        buffer->Seal({ childObjKey });
        parentObjKey = childObjKey;
        buffer = buffer1;
        if (times == 99) {
            lastId = parentObjKey;
            LOG(INFO) << "Seal last id is : " << lastId;
            buffer->Seal();
        }
    }

    LOG(INFO) << FormatString("After seal first object key is : %s, last object key is %s", firstId, lastId);
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client->Get({ firstId, lastId }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    DS_ASSERT_OK(client->GDecreaseRef({ firstId }, failObjects));
    ASSERT_TRUE(failObjects.empty());

    dataList.clear();
    DS_ASSERT_NOT_OK(client->Get({ firstId, lastId }, 0, dataList));
}

TEST_F(OCClientRefTest, NestedAsyncBySameClient)
{
    int asyncNum = 5;
    std::vector<std::string> objectKeys;
    objectKeys.reserve(asyncNum + 1);
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    for (int i = 0; i <= asyncNum; i++) {
        objectKeys.emplace_back(NewObjectKey());
    }

    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    std::vector<uint8_t> data = { 45, 46, 47, 48, 49, 50 };
    std::vector<std::shared_ptr<Buffer>> buffers;
    CreateParam param;
    for (int i = 0; i <= asyncNum; i++) {
        std::shared_ptr<Buffer> buffer;
        client->Create(objectKeys[i], data.size(), param, buffer);
        buffer->MemoryCopy(data.data(), data.size());
        buffers.emplace_back(buffer);
        if (i == asyncNum) {
            buffer->Seal();
        }
    }

    ThreadPool threadPool(asyncNum);
    std::vector<std::future<void>> futures;
    futures.reserve(asyncNum);

    for (int i = 0; i < asyncNum; i++) {
        auto &nestedKey = objectKeys.back();
        auto &buffer = buffers[i];
        futures.emplace_back(threadPool.Submit([&nestedKey, &buffer]() { buffer->Seal({ nestedKey }); }));
    }
    for (auto &future : futures) {
        future.get();
    }

    DS_ASSERT_OK(client->GDecreaseRef({ objectKeys.back() }, failObjects));
    ASSERT_TRUE(failObjects.empty());

    std::vector<Optional<Buffer>> dataList;
    dataList.reserve(asyncNum + 1);
    DS_ASSERT_OK(client->Get(objectKeys, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    ASSERT_EQ(dataList.size(), objectKeys.size());
    dataList.clear();
    failObjects.clear();
    objectKeys.pop_back();
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    LOG(INFO) << "Start test get all failed.";
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, dataList));
}

TEST_F(OCClientRefTest, NestedAsyncByDiffWork)
{
    int asyncNum = 3;
    std::vector<std::string> objectKeys;
    objectKeys.reserve(asyncNum + 1);
    std::vector<std::shared_ptr<ObjectClient>> clients(asyncNum);

    for (int i = 0; i < asyncNum; i++) {
        InitTestClient(i, clients[i]);
        objectKeys.emplace_back(NewObjectKey());
    }
    objectKeys.emplace_back(NewObjectKey());

    std::vector<uint8_t> data = { 45, 46, 47, 48, 49, 50 };
    std::vector<std::shared_ptr<Buffer>> buffers;
    CreateParam param;
    for (int i = 0; i < asyncNum; i++) {
        std::vector<std::string> failObjects;
        DS_ASSERT_OK(clients[i]->GIncreaseRef({ objectKeys[i], objectKeys.back() }, failObjects));
        ASSERT_TRUE(failObjects.empty());
        std::shared_ptr<Buffer> buffer;
        clients[i]->Create(objectKeys[i], data.size(), param, buffer);
        buffer->MemoryCopy(data.data(), data.size());
        buffers.emplace_back(buffer);
    }
    std::shared_ptr<Buffer> buffer;
    clients[0]->Create(objectKeys.back(), data.size(), param, buffer);
    buffer->MemoryCopy(data.data(), data.size());
    buffers.emplace_back(buffer);
    buffer->Seal();

    ThreadPool threadPool(asyncNum);
    std::vector<std::future<void>> futures;
    futures.reserve(asyncNum);
    for (int i = 0; i < asyncNum; i++) {
        auto &nestedKey = objectKeys.back();
        auto &buf = buffers[i];
        futures.emplace_back(threadPool.Submit([&buf, &nestedKey]() { buf->Seal({ nestedKey }); }));
    }
    for (auto &future : futures) {
        future.get();
    }

    for (int i = 0; i < asyncNum; i++) {
        std::vector<std::string> failObjects;
        DS_ASSERT_OK(clients[i]->GDecreaseRef({ objectKeys.back() }, failObjects));
        ASSERT_TRUE(failObjects.empty());
    }

    std::vector<Optional<Buffer>> dataList;
    dataList.reserve(asyncNum + 1);
    DS_ASSERT_OK(clients[2]->Get(objectKeys, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    LOG(INFO) << "Start clear all objects";
    for (int i = 0; i < asyncNum; i++) {
        std::vector<std::string> failObjects;
        DS_ASSERT_OK(clients[i]->GDecreaseRef({ objectKeys[i] }, failObjects));
        ASSERT_TRUE(failObjects.empty());
    }
    dataList.clear();
    DS_ASSERT_NOT_OK(clients[1]->Get(objectKeys, 0, dataList));
    dataList.clear();
    DS_ASSERT_NOT_OK(clients[0]->Get({ objectKeys.back() }, 0, dataList));
}

TEST_F(OCClientRefTest, GIncDuplicateIdsAndGDecDuplicateIds)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::vector<std::string> objectKeys;
    const int maxSize = 5;
    int randomSize = 5;
    for (int i = 0; i < maxSize; i++) {
        objectKeys.emplace_back(NewObjectKey());
    }
    randomSize--;
    while (randomSize > 0) {
        for (int i = 0; i < randomSize; i++) {
            objectKeys.emplace_back(objectKeys[i]);
        }
        randomSize--;
    }  // gRefCnt are 5 4 3 2 1;
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    // pop object[0]
    objectKeys.pop_back();
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());

    randomSize = 0;
    const int leftTimes = 1;
    ASSERT_EQ(client->QueryGlobalRefNum(objectKeys[randomSize++]), leftTimes);
    for (; randomSize < 5; randomSize++) {
        ASSERT_EQ(client->QueryGlobalRefNum(objectKeys[randomSize]), 0);
        DS_ASSERT_OK(client->GDecreaseRef({ objectKeys[randomSize] }, failObjects));
        ASSERT_TRUE(failObjects.empty());
    }
}

TEST_F(OCClientRefTest, GRefAsyncIncDecDiffObjectKey)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    const int loopNum = 5;
    std::vector<std::string> objectKeys;
    for (int i = 0; i < loopNum; i++) {
        objectKeys.emplace_back(NewObjectKey());
    }
    ThreadPool threadPool(loopNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < loopNum; i++) {
        futures.emplace_back(threadPool.Submit([&client, &objectKeys, i]() {
            for (int times = 0; times < loopNum; times++) {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client->GIncreaseRef({ objectKeys[i] }, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }
            for (int times = 0; times < loopNum; times++) {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client->GDecreaseRef({ objectKeys[i] }, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }
        }));
    }
    for (auto &future : futures) {
        future.get();
    }
    for (int i = 0; i < loopNum; i++) {
        ASSERT_EQ(client->QueryGlobalRefNum(objectKeys[i]), 0);
    }
}

TEST_F(OCClientRefTest, LEVEL1_GDecDeadlockTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestClient(2, client3);
    uint64_t size = 1024;
    std::string data = GenRandomString(size);
    size_t test_cnt = 100;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);
    for (int k = 0; k < 10; k++) {
        std::vector<std::string> needDelete;
        for (size_t i = 0; i < test_cnt; i++) {
            std::string objectKey = "key" + std::to_string(i);
            std::vector<std::string> failedObjectKeys;
            DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
            ASSERT_TRUE(failedObjectKeys.empty());
            DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failedObjectKeys));
            ASSERT_TRUE(failedObjectKeys.empty());
            DS_ASSERT_OK(client3->GIncreaseRef({ objectKey }, failedObjectKeys));
            ASSERT_TRUE(failedObjectKeys.empty());

            needDelete.emplace_back(objectKey);
        }

        for (size_t i = 0; i < test_cnt; i++) {
            std::string objectKey = "key" + std::to_string(i);
            DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size,
                                      CreateParam{}));
            std::vector<Optional<Buffer>> buffers;
            std::vector<std::string> objectKeys{ objectKey };
            DS_ASSERT_OK(client2->Get(objectKeys, 0, buffers));
            ASSERT_TRUE(NotExistsNone(buffers));
            AssertBufferEqual(*buffers[0], data);
            DS_ASSERT_OK(client3->Get(objectKeys, 0, buffers));
            ASSERT_TRUE(NotExistsNone(buffers));
            AssertBufferEqual(*buffers[0], data);
        }

        ThreadPool threadPool(3);
        auto fut = threadPool.Submit([&needDelete, client1]() {
            std::vector<std::string> res;
            Status rc = client1->GDecreaseRef(needDelete, res);
            EXPECT_TRUE(res.empty());
            return rc;
        });
        auto fut2 = threadPool.Submit([&needDelete, client2]() {
            std::vector<std::string> res;
            Status rc = client2->GDecreaseRef(needDelete, res);
            EXPECT_TRUE(res.empty());
            return rc;
        });
        auto fut3 = threadPool.Submit([&needDelete, client3]() {
            std::vector<std::string> res;
            Status rc = client3->GDecreaseRef(needDelete, res);
            EXPECT_TRUE(res.empty());
            return rc;
        });
        ASSERT_EQ(fut.get(), Status::OK());
        ASSERT_EQ(fut2.get(), Status::OK());
        ASSERT_EQ(fut3.get(), Status::OK());

        for (size_t i = 0; i < test_cnt; i++) {
            std::string objectKey = "key" + std::to_string(i);
            std::vector<Optional<Buffer>> buffers;
            DS_ASSERT_NOT_OK(client1->Get({ objectKey }, 0, buffers));
        }
    }
}

TEST_F(OCClientRefTest, LEVEL1_GDecDeadlockTest2)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    std::shared_ptr<ObjectClient> client4;
    std::shared_ptr<ObjectClient> client5;
    std::shared_ptr<ObjectClient> client6;
    InitTestClient(0, client1);
    InitTestClient(0, client2);
    InitTestClient(0, client3);
    InitTestClient(0, client4);
    InitTestClient(0, client5);
    InitTestClient(1, client6);
    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    std::string objectKey = NewObjectKey();
    DS_ASSERT_OK(
        client6->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    std::promise<void> promise;
    std::shared_future<void> result = promise.get_future();

    ThreadPool threadPool(5);
    auto fut = threadPool.Submit([&client1, &objectKey, &promise]() {
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers));
        promise.set_value();
        ASSERT_TRUE(NotExistsNone(buffers));

        DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    });
    auto fut2 = threadPool.Submit([&client2, &objectKey, &result]() {
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        result.get();
        DS_ASSERT_OK(client2->GDecreaseRef({ objectKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    });
    auto fut3 = threadPool.Submit([&client3, &objectKey, &result]() {
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client3->GIncreaseRef({ objectKey }, failedObjectKeys));
        result.get();
        ASSERT_TRUE(failedObjectKeys.empty());
        DS_ASSERT_OK(client3->GDecreaseRef({ objectKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    });
    auto fut4 = threadPool.Submit([&client4, &objectKey, &result]() {
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client4->GIncreaseRef({ objectKey }, failedObjectKeys));
        result.get();
        ASSERT_TRUE(failedObjectKeys.empty());
        DS_ASSERT_OK(client4->GDecreaseRef({ objectKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    });
    auto fut5 = threadPool.Submit([&client5, &objectKey, &result]() {
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client5->GIncreaseRef({ objectKey }, failedObjectKeys));
        result.get();
        ASSERT_TRUE(failedObjectKeys.empty());
        DS_ASSERT_OK(client5->GDecreaseRef({ objectKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    });
    fut.get();
    fut2.get();
    fut3.get();
    fut4.get();
    fut5.get();
}

TEST_F(OCClientRefTest, GDecNotificationRetry)
{
    LOG(INFO) << "Test GDecrease, master retry DeleteNotification";
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    std::vector<std::string> needDelete;
    std::string objectKey = "key0";
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failedObjectKeys));
    needDelete.emplace_back(objectKey);

    DS_ASSERT_OK(
        client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_OK(client2->Get(objectKeys, 0, buffers));
    AssertBufferEqual(*buffers[0], data);

    std::vector<std::string> res;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "MasterWorkerOCServiceImpl.DeleteNotification.retry",
                                           "1*return(K_RPC_CANCELLED)"));
    client1->GDecreaseRef(needDelete, res);
    client2->GDecreaseRef(needDelete, res);

    DS_ASSERT_NOT_OK(client1->Get({ objectKey }, 0, buffers));
}

TEST_F(OCClientRefTest, GDecNotificationPartiallyObjKeyFail)
{
    LOG(INFO) << "Test GDecrease, master DeleteNotification to worker, worker process objKeys partially fail";
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    std::string objectKey1 = "key0";
    std::string objectKey2 = "key1";
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));

    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));
    DS_ASSERT_OK(
        client1->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2 }, 0, buffers));
    AssertBufferEqual(*buffers[0], data);

    std::vector<std::string> res;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.DeleteObjectWithTryLock.before", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1, objectKey2 }, res));
    ASSERT_TRUE(res.empty());

    std::vector<std::string> failedIds;
    // master process fail will return status ok and failed objectKey record in resp body.
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey1, objectKey2 }, failedIds));
    ASSERT_TRUE(failedIds.size() == 1);
    ASSERT_TRUE(failedIds[0] == objectKey1);
}

TEST_F(OCClientRefTest, GDecNotificationWorkerUnHealthy)
{
    LOG(INFO)
        << "Test GDecrease, master send DeleteNotification, but the worker unhealthy, and insert async task success";
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    std::string objectKey1 = "key0";
    std::string objectKey2 = "key1";
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));

    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));
    DS_ASSERT_OK(
        client1->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2 }, 0, buffers));
    AssertBufferEqual(*buffers[0], data);

    std::vector<std::string> res;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.CheckWorkerIsHealth.worker.unhealthy",
                                           "1*return(K_WORKER_ABNORMAL)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1, objectKey2 }, res));
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey1, objectKey2 }, failedIds));
    ASSERT_TRUE(failedIds.empty());
}

TEST_F(OCClientRefTest, GDecNotificationWorkerUnHealthyFail)
{
    LOG(INFO) << "Test GDecrease, master send DeleteNotification, but the worker unhealthy, and insert async task fail";
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    std::string objectKey1 = "key0";
    std::string objectKey2 = "key1";
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));

    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));
    DS_ASSERT_OK(
        client1->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2 }, 0, buffers));
    AssertBufferEqual(*buffers[0], data);

    std::vector<std::string> res;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.CheckWorkerIsHealth.worker.unhealthy",
                                           "return(K_WORKER_ABNORMAL)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.InsertAsyncWorkerOp.Fail",
                                           "1*return(K_KVSTORE_ERROR)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1, objectKey2 }, res));
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey1, objectKey2 }, failedIds));
    ASSERT_TRUE(failedIds.size() == 1);
    DS_ASSERT_OK(client2->GDecreaseRef(failedIds, res));
}

TEST_F(OCClientRefTest, GDecNotificationMutipleWorker)
{
    LOG(INFO) << "Test GDecrease, master retry DeleteNotification to two worker, all success.";
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestClient(2, client3);

    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    std::string objectKey1 = "key0";
    std::string objectKey2 = "key1";
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey1 }, failedObjectKeys));
    DS_ASSERT_OK(client3->GIncreaseRef({ objectKey2 }, failedObjectKeys));

    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));
    DS_ASSERT_OK(
        client1->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get({ objectKey1 }, 0, buffers1));
    AssertBufferEqual(*buffers1[0], data);

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client3->Get({ objectKey2 }, 0, buffers2));
    AssertBufferEqual(*buffers2[0], data);

    std::vector<std::string> res;
    client2->GDecreaseRef({ objectKey1 }, res);
    client3->GDecreaseRef({ objectKey2 }, res);

    std::vector<std::string> lastRes;
    // master process fail will return status ok and failed objectKey record in resp body.
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1, objectKey2 }, lastRes));
    ASSERT_TRUE(lastRes.empty());
}

TEST_F(OCClientRefTest, LEVEL1_GDecNotificationPartiallyFail)
{
    LOG(INFO) << "Test GDecrease, master retry DeleteNotification to two worker, one success, one "
                 "failed;DeleteNotification sync send.";
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    int32_t timeoutMs = 10000;
    InitTestClient(0, client1, timeoutMs);
    InitTestClient(1, client2, timeoutMs);
    InitTestClient(2, client3, timeoutMs);

    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    std::string objectKey1 = "key0";
    std::string objectKey2 = "key1";
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey1 }, failedObjectKeys));
    DS_ASSERT_OK(client3->GIncreaseRef({ objectKey2 }, failedObjectKeys));

    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));
    DS_ASSERT_OK(
        client1->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get({ objectKey1 }, 0, buffers1));
    AssertBufferEqual(*buffers1[0], data);

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client3->Get({ objectKey2 }, 0, buffers2));
    AssertBufferEqual(*buffers2[0], data);

    std::vector<std::string> res;
    client2->GDecreaseRef({ objectKey1 }, res);
    client3->GDecreaseRef({ objectKey2 }, res);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "MasterWorkerOCServiceImpl.DeleteNotification.retry",
                                           "1*return(K_RPC_CANCELLED)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "MasterWorkerOCServiceImpl.DeleteNotification.retry",
                                           "return(K_RPC_CANCELLED)"));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.NotifyWorkerDelete.timeoutMs", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.GDecreaseMasterRef.timeoutMs", "call(100)"));
    std::vector<std::string> lastRes;

    // master process fail will return status ok and failed objectKey record in resp body.
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1, objectKey2 }, lastRes));
    ASSERT_TRUE(lastRes.size() == 1);
    ASSERT_TRUE(lastRes[0] == objectKey2);
}

TEST_F(OCClientRefTest, GIncreaseRefPartiallySuccess)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey1 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    // master rpc failed for objectKey2
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.GIncrease_ref_failure",
                                           "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), 1ul);
    ASSERT_EQ(failedObjectKeys[0], objectKey2);
    failedObjectKeys.clear();

    // master rpc success but objectKey2 failed
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.rocksdb.AddGlobalRef",
                                           "1*return(K_KVSTORE_ERROR)"));
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), 1ul);
    ASSERT_EQ(failedObjectKeys[0], objectKey2);
    failedObjectKeys.clear();

    // worker rpc failed for objectKey2
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.GIncrease_ref_failure",
                                           "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), 1ul);
    ASSERT_EQ(failedObjectKeys[0], objectKey2);
    failedObjectKeys.clear();

    // verify
    ASSERT_EQ(client->QueryGlobalRefNum(objectKey1), 1);
    ASSERT_EQ(client->QueryGlobalRefNum(objectKey2), 0);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1, objectKey1, objectKey1 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(client->QueryGlobalRefNum(objectKey1), 1);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(client->QueryGlobalRefNum(objectKey1), 0);

    std::string objectKey3 = NewObjectKey();
    // Failed for Master RPC
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.GIncrease_ref_failure",
                                           "2*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_NOT_OK(client->GIncreaseRef({ objectKey3 }, failedObjectKeys));
    failedObjectKeys.clear();

    // Failed for worker RPC
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.GIncrease_ref_failure",
                                           "2*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_NOT_OK(client->GIncreaseRef({ objectKey3 }, failedObjectKeys));
    failedObjectKeys.clear();
}

TEST_F(OCClientRefTest, GDecreaseRefPartiallySuccess)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::string objectKey3 = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey1, objectKey1, objectKey1, objectKey1, objectKey2, objectKey3 },
                                      failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    // master rpc failed for objectKey2
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.GDecreaseRef.before",
                                           "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), 1ul);
    ASSERT_EQ(failedObjectKeys[0], objectKey2);
    failedObjectKeys.clear();

    // master rpc success but objectKey2 failed
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.rocksdb.RemoveGlobalRef",
                                           "1*return(K_KVSTORE_ERROR)"));
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), 1ul);
    ASSERT_EQ(failedObjectKeys[0], objectKey2);
    failedObjectKeys.clear();

    // worker rpc failed for objectKey2
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.GDecreaseRef.before",
                                           "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), 1ul);
    ASSERT_EQ(failedObjectKeys[0], objectKey2);
    failedObjectKeys.clear();

    // verify
    ASSERT_EQ(client->QueryGlobalRefNum(objectKey1), 1);
    ASSERT_EQ(client->QueryGlobalRefNum(objectKey2), 0);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey1 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(client->QueryGlobalRefNum(objectKey1), 0);

    std::string objectKey4 = NewObjectKey();
    // Failed for Master RPC
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.GDecreaseRef.before",
                                           "2*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey3, objectKey4 }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), 1ul);
    ASSERT_EQ(failedObjectKeys[0], objectKey3);
    failedObjectKeys.clear();
    DS_ASSERT_NOT_OK(client->GDecreaseRef({ objectKey3 }, failedObjectKeys));
    failedObjectKeys.clear();

    // Failed for worker RPC
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.GDecreaseRef.before",
                                           "2*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey3, objectKey4 }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), 1ul);
    ASSERT_EQ(failedObjectKeys[0], objectKey3);
    failedObjectKeys.clear();
    DS_ASSERT_NOT_OK(client->GDecreaseRef({ objectKey3 }, failedObjectKeys));
}

TEST_F(OCClientRefTest, GDecreaseRefTryAgain)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();

    uint64_t size = 1024;
    std::string data = GenRandomString(size);
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    DS_ASSERT_OK(
        client1->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2 }, 0, dataList));
    ASSERT_TRUE(dataList.size() == 2 && dataList[0] && dataList[1]);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.DeleteObjectWithTryLock.before",
                                           "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_TRUE(!failedObjectKeys.empty());

    std::vector<std::string> failedObjectKeys2;
    DS_ASSERT_OK(client1->GDecreaseRef(failedObjectKeys, failedObjectKeys2));
    ASSERT_TRUE(failedObjectKeys2.empty());

    DS_ASSERT_NOT_OK(client2->Get({ objectKey1, objectKey2 }, 0, dataList));
    DS_ASSERT_NOT_OK(client1->Get({ objectKey1, objectKey2 }, 0, dataList));
}

TEST_F(OCClientRefTest, GDecreaseRefDeadLock)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();

    uint64_t size = 1024;
    std::string data = GenRandomString(size);
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    DS_ASSERT_OK(
        client1->Put(objectKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2 }, 0, dataList));
    ASSERT_TRUE(dataList.size() == 2 && dataList[0] && dataList[1]);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.DeleteObjectWithTryLock.before",
                                           "1*return(K_WORKER_DEADLOCK)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_NOT_OK(client2->Get({ objectKey1, objectKey2 }, 0, dataList));
    DS_ASSERT_NOT_OK(client1->Get({ objectKey1, objectKey2 }, 0, dataList));
}

TEST_F(OCClientRefTest, AsyncShutdownAndRetryFailedDec)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::string objectKey3 = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey1, objectKey1, objectKey1, objectKey1, objectKey2, objectKey3 },
                                      failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.rocksdb.RemoveGlobalRef",
                                           "1*return(K_KVSTORE_ERROR)"));

    DS_ASSERT_OK(client->ShutDown());

    // verify
    std::shared_ptr<ObjectClient> clientVerify;
    InitTestClient(0, clientVerify);
    const int timeout = 30000;  // 30s;
    bool flag = false;
    Timer timer;
    while (timer.ElapsedMilliSecondAndReset() < timeout) {
        if (clientVerify->QueryGlobalRefNum(objectKey1) == 0 && clientVerify->QueryGlobalRefNum(objectKey2) == 0
            && clientVerify->QueryGlobalRefNum(objectKey3) == 0) {
            flag = true;
            break;
        }
        const int interval = 10;  // ms
        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }
    ASSERT_TRUE(flag);
}

class OCClientRefTestWithAsyncDelete : public OCClientRefTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        OCClientRefTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams = "-async_delete=true -v=1";
    }

    int CalcGRefNumOnWorker(std::string objectKey, std::shared_ptr<ObjectClient> client)
    {
        return client->QueryGlobalRefNum(objectKey);
    }
};

TEST_F(OCClientRefTestWithAsyncDelete, GDecreaseRefDeleteAsync)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    auto times = 10;
    for (auto i = 0; i < times; i++) {
        std::string objectKey = NewObjectKey();
        std::vector<std::string> failObjects;
        std::vector<std::string> objectKeys = { objectKey };
        std::string data = "value";
        CreateAndSealObject(client0, objectKey, data);
        std::vector<Optional<Buffer>> buffers;
        client0->Get(objectKeys, 0, buffers);
        client1->Get(objectKeys, 0, buffers);
        DS_ASSERT_OK(client0->GIncreaseRef(objectKeys, failObjects));

        ASSERT_EQ(CalcGRefNumOnWorker(objectKey, client0), 1);
        ASSERT_EQ(CalcGRefNumOnWorker(objectKey, client1), 1);

        DS_ASSERT_OK(client0->GDecreaseRef(objectKeys, failObjects));
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        DS_ASSERT_NOT_OK(client0->Get(objectKeys, 0, buffers));
        DS_ASSERT_NOT_OK(client1->Get(objectKeys, 0, buffers));
    }
}

class OCClientDistributedRefTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numOBS = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = " -v=1";
        opts.waitWorkerReady = false;
        opts.enableDistributedMaster = "true";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitTestEtcdInstance();
        StartClustersAndWaitReady();
        GetWorkerUuids();
    }

    void TearDown() override
    {
        db_.reset();
        ExternalClusterTest::TearDown();
    }

    void GetWorkerUuids()
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

    void InitTestEtcdInstance()
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

    void StartClustersAndWaitReady()
    {
        DS_ASSERT_OK(cluster_->StartWorkers());
        DS_ASSERT_OK(cluster_->WaitUntilClusterReadyOrTimeout(30));
        int maxWaitTimeSec = 10;
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0, maxWaitTimeSec));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1, maxWaitTimeSec));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2, maxWaitTimeSec));
    }

    int CalcGRefNumOnWorker(std::string objectKey, std::shared_ptr<ObjectClient> client)
    {
        return client->QueryGlobalRefNum(objectKey);
    }

protected:
    std::unique_ptr<EtcdStore> db_;
    std::unordered_map<HostPort, std::string> uuidMap_;
};

TEST_F(OCClientDistributedRefTest, GRefAsyncDecrease)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);

    std::string objectKey1 = ObjectKeyWithOwner(0, uuidMap_);
    std::string objectKey2 = ObjectKeyWithOwner(0, uuidMap_);
    std::string objectKey3 = ObjectKeyWithOwner(1, uuidMap_);

    std::vector<std::string> objectKeys = { objectKey1, objectKey2, objectKey3 };
    ThreadPool threadPool(10);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
            futures.emplace_back(threadPool.Submit([&client1, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }));
        } else {
            futures.emplace_back(threadPool.Submit([&client2, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }));
        }
    }
    for (auto &future : futures) {
        future.get();
    }

    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1, client1), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2, client1), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey3, client1), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2, client2), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey3, client2), 2);
    futures.clear();
    for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
            futures.emplace_back(threadPool.Submit([&client1, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }));
        } else {
            futures.emplace_back(threadPool.Submit([&client2, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
                ASSERT_TRUE(failObjects.empty());
            }));
        }
    }
    for (auto &future : futures) {
        future.get();
    }

    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1, client1), 0);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2, client1), 0);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey3, client1), 0);

    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_TRUE(failObjects.empty());
}

class OCClientShmRefHighConcurrentTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams = " -shared_memory_size_mb=2048 -enable_multi_stubs=true -v=1 -max_client_num=800";
    }
};

TEST_F(OCClientShmRefHighConcurrentTest, MutiClientCreate)
{
    // Simulating high concurrency with object Point
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.clientManager.getLockId", "call(100)"));
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
}

TEST_F(OCClientShmRefHighConcurrentTest, LEVEL2_MaxNumOfClientTest)
{
    int numClient = 800;
    uint64_t dataSize = 600 * 1024;
    std::string data = GenPartRandomString(dataSize);

    ThreadPool threadPool(numClient);
    int loopTimes = 10;
    std::vector<std::future<void>> futureVec;
    for (size_t i = 0; i < (size_t)numClient; ++i) {
        auto fut = threadPool.Submit([&data, dataSize, loopTimes, this]() {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
            CreateParam param;
            std::vector<std::string> failedObjectKeys;
            std::vector<Optional<Buffer>> buffers;
            for (int index = 0; index < loopTimes; index++) {
                std::string objectKey = NewObjectKey();
                std::shared_ptr<Buffer> buffer;
                DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
                DS_ASSERT_OK(client->Create(objectKey, dataSize, param, buffer));
                DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
                DS_ASSERT_OK(buffer->Seal());
                DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
                buffer.reset();
            }
        });
        futureVec.push_back(std::move(fut));
    }
    for (auto &future : futureVec) {
        future.get();
    }
}

class OCClientShmRefTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams = " -shared_memory_size_mb=10240 -enable_multi_stubs=true -v=1";
    }

    void InitNumsClient(std::vector<std::shared_ptr<ObjectClient>> &clientVec, int index = 0)
    {
        for (auto &client : clientVec) {
            if (index == 0) {
                InitTestClient(index, client);
                index++;
            } else {
                InitTestClient(index, client);
            }
        }
    }

    void ExecNormalDataHanle(std::shared_ptr<ObjectClient> client, std::string data, uint64_t dataSize,
                             int iterNum = 1000)
    {
        CreateParam param;
        std::vector<std::string> failedObjectKeys;
        std::vector<Optional<Buffer>> buffers;
        for (int index = 0; index < iterNum; index++) {
            std::string objectKey = NewObjectKey();
            std::shared_ptr<Buffer> buffer;
            DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
            DS_ASSERT_OK(client->Create(objectKey, dataSize, param, buffer));
            DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
            DS_ASSERT_OK(buffer->Seal());
            buffer.reset();
            DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
            DS_ASSERT_NOT_OK(client->Get({ objectKey }, 0, buffers));
        }
    }
};

TEST_F(OCClientShmRefTest, LEVEL1_MultipleClientDecreaeObjectTest)
{
    int numClient = 16;
    std::vector<std::shared_ptr<ObjectClient>> clientVec(numClient);
    InitNumsClient(clientVec);
    uint64_t dataSize = 600 * 1024;
    std::string data = GenPartRandomString(dataSize);

    ThreadPool threadPool(numClient);
    std::vector<std::future<void>> futureVec;
    for (size_t i = 0; i < clientVec.size(); ++i) {
        auto fut = threadPool.Submit([&clientVec, &data, dataSize, i, this]() {
            int iterNum = 500;
            ExecNormalDataHanle(clientVec[i], data, dataSize, iterNum);
        });
        futureVec.push_back(std::move(fut));
    }
    for (auto &future : futureVec) {
        future.get();
    }
    PerfManager *perfManager = PerfManager::Instance();
    perfManager->PrintPerfLog();
    sleep(1);  // wait for collect perf log.
}

TEST_F(OCClientShmRefTest, LEVEL2_MultipleThreadDecreaseTest)
{
    int numThread = 10;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    uint64_t dataSize = 600 * 1024;
    CreateParam param;
    std::string data = GenPartRandomString(dataSize);
    int dataNum = numThread * 1000;
    std::vector<std::string> objectKeys;
    objectKeys.reserve(dataNum);
    for (int i = 0; i < dataNum; i++) {
        objectKeys.emplace_back(NewObjectKey());
    }

    std::vector<std::shared_ptr<Buffer>> buffers;
    buffers.reserve(dataNum);
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    for (int i = 0; i < dataNum; i++) {
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(objectKeys[i], dataSize, param, buffer));
        buffers.emplace_back(buffer);
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());
    }

    ThreadPool threadPool(numThread);
    std::vector<std::future<void>> futureVec;
    auto startTick = std::chrono::steady_clock::now();
    for (int i = 0; i < numThread; ++i) {
        auto fut = threadPool.Submit([&client, &buffers, &objectKeys, i]() {
            std::vector<std::string> failedObjectKeys;
            for (int index = 0; index < 1000; index++) {
                auto pos = i * 1000 + index;
                client->GDecreaseRef({ objectKeys[pos] }, failedObjectKeys);
                std::shared_ptr<Buffer> bufferNew;
                buffers[pos] = bufferNew;  // Decrease before buffer
            }
        });
        futureVec.push_back(std::move(fut));
    }
    for (auto &future : futureVec) {
        future.get();
    }
    auto endTick = std::chrono::steady_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count();
    std::vector<Optional<Buffer>> buffersGet;
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, buffersGet));
    LOG(INFO) << "elapsedTime is : " << elapsedTime << "us";
}

TEST_F(OCClientShmRefTest, LEVEL2_ClientDecreaseSuccessTest)
{
    int testNum = 3000;
    std::string objectKey = NewObjectKey();
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    int32_t size = 500 * 1024;
    std::string data = GenRandomString(size);
    for (int i = 0; i < testNum; i++) {
        LOG(INFO) << "Start to test client decrease.";
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
        std::vector<std::string> objectKeys{ objectKey };
        std::vector<std::string> failedObjectKeys;
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
        buffer->MemoryCopy((void *)data.data(), size);
        buffer->Publish();
        buffer.reset();
        DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
        ASSERT_EQ(buffers.size(), (size_t)1);
        buffers.clear();
        DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, buffers));
    }
}

TEST_F(OCClientShmRefTest, LEVEL1_TestWorkerLostInDeadLockAndReconnect)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    int32_t size = 600 * 1024;
    std::string data = GenRandomString(size);

    datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(400)");
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(400)");
    datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(400)");
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Decrease_Reference_Deadlock", "return(K_RUNTIME_ERROR)"));

    ThreadPool threadPool(1);
    std::vector<std::future<void>> futureVec;
    futureVec.push_back(threadPool.Submit([&client1, data, this] {
        std::string objectKey = NewObjectKey();
        CreateParam param{};
        // Put operate is waiting for worker resp decrease, when reconnect to worker, auto wake up old queue and
        // update the queue.
        auto status =
            client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), param);
        ASSERT_EQ(status.GetCode(), StatusCode::K_RUNTIME_ERROR);
        ASSERT_TRUE(status.GetMsg().find("ShmQueue is destroyed.") != std::string::npos);
    }));

    sleep(2);
    cluster_->ShutdownNodes(WORKER);
    cluster_->StartWorkers();  // restart quickly.
    for (auto &future : futureVec) {
        future.get();
    }
}

TEST_F(OCClientShmRefTest, LEVEL1_TestWorkerLostWhenDeadLock)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(0, client2);
    int32_t size = 600 * 1024;
    std::string data = GenRandomString(size);

    datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(400)");
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(400)");
    datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(400)");
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Decrease_Reference_Deadlock", "return(K_RUNTIME_ERROR)"));

    ThreadPool threadPool(2);
    std::vector<std::future<void>> futureVec;
    for (int i = 0; i < 2; ++i) {
        futureVec.push_back(threadPool.Submit([&client1, &client2, data, i, this] {
            auto client = i == 0 ? client1 : client2;
            std::string objectKey = NewObjectKey();
            CreateParam param{};
            // Put wait for worker resp, when reconnect to worker, auto wake up last queue, and update.
            DS_ASSERT_NOT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())),
                                         data.size(), param));
        }));
    }
    sleep(2);
    cluster_->ShutdownNodes(WORKER);
    // wait put finish.
    sleep(10);
    cluster_->StartWorkers();
    for (auto &future : futureVec) {
        future.get();
    }
}

TEST_F(OCClientShmRefTest, TestClientLostWhenClientLocked)
{
    int numClient = 49;
    std::vector<std::shared_ptr<ObjectClient>> clientVec(numClient);
    InitNumsClient(clientVec);
    int32_t size = 600 * 1024;
    std::string data = GenRandomString(size);
    datasystem::inject::Set("ClientManager.Init.heartbeatInterval", "call(500)");
    datasystem::inject::Set("ClientManager.IsClientLost.heartbeatThreshold", "call(1)");
    datasystem::inject::Set("ClientWorkerApi.DecreaseWorkerRefByShm.ClientDeadlock", "48*return(K_RUNTIME_ERROR)");
    ThreadPool threadPool(numClient - 1);
    std::vector<std::future<void>> futureVec;
    for (int i = 0; i < numClient - 1; ++i) {
        futureVec.push_back(threadPool.Submit([&clientVec, data, i, this] {
            auto client = clientVec[i];
            std::string objectKey = NewObjectKey();
            CreateParam param{};

            std::shared_ptr<Buffer> buffer;
            // Put wait for worker resp, when reconnect to worker, auto wake up last queue, and update.
            DS_ASSERT_OK(client->Create(objectKey, data.size(), param, buffer));
            buffer.reset();
        }));
    }

    for (auto &future : futureVec) {
        future.get();
    }
    for (int i = 0; i < numClient - 1; ++i) {
        clientVec[i].reset();
    }

    std::string objectKey = NewObjectKey();
    CreateParam param{};
    // Put wait for worker resp, when reconnect to worker, auto wake up last queue, and update.
    // If the execution fails, the worker side may not be able to get the write-lock.
    DS_ASSERT_OK(clientVec[numClient - 1]->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())),
                                               data.size(), param));
}

class OCRemoteClientIdDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numOBS = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = " -node_timeout_s=3";
        opts.enableDistributedMaster = "true";
    }

    const int timeoutMs_ = 5000;  // 5s timeout
};

TEST_F(OCRemoteClientIdDfxTest, MasterTimeout)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0, timeoutMs_);
    InitTestClient(1, client1, timeoutMs_);

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "EtcdClusterManager.checkConnection", "return(K_MASTER_TIMEOUT)"));
    {
        // fail if no success objects
        std::string objectKey = "obj";
        std::vector<std::string> failObjects;
        auto status = (client1->GIncreaseRef({ objectKey }, failObjects));
        LOG(INFO) << "--status: " << status;
        EXPECT_NE(status.GetCode(), K_OK) << status;
        EXPECT_TRUE(!failObjects.empty()) << VectorToString(failObjects);
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "EtcdClusterManager.checkConnection",
                                           "1*return(K_MASTER_TIMEOUT)->1*return(K_OK)"));
    {
        // success if partly fail, and return the fail ids
        std::string objectKey1;
        DS_ASSERT_OK(client1->GenerateObjectKey("obj1", objectKey1));
        std::string objectKey0;
        DS_ASSERT_OK(client0->GenerateObjectKey("obj0", objectKey0));

        std::vector<std::string> failObjects;
        auto status = (client1->GIncreaseRef({ objectKey0, objectKey1 }, failObjects));
        LOG(INFO) << "--status: " << status;  // ignore the verification of status code.
        EXPECT_EQ(failObjects.size(), 1) << VectorToString(failObjects);
    }
}

class OCRemoteClientIdRefMigrateTest : public OCClientCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }
    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        opts.workerGflagParams = FormatString(" -shared_memory_size_mb=3000  -v=1");
    }

    int CalcGRefNumOnWorker(std::string objectKey, std::shared_ptr<ObjectClient> client)
    {
        return client->QueryGlobalRefNum(objectKey);
    }

    void WaitWorkerReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort()).IsOk()) << i;
        }
        WaitWorkerReady(indexes, maxWaitTimeSec);
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

        sleep(1);
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->StartNode(WORKER, i, "").IsOk()) << i;
        }
        int maxWaitTimeSec = 40;  // need reconliation
        WaitWorkerReady(indexes, maxWaitTimeSec);
    }

    ExternalCluster *externalCluster_ = nullptr;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
};

TEST_F(OCRemoteClientIdRefMigrateTest, DISABLED_TestRemoteClientObjRefMigrate2)
{
    std::shared_ptr<ObjectClient> client0, client1, client2;
    std::vector<std::string> failObjects;
    StartWorkerAndWaitReady({ 0, 2 });
    InitTestClient(0, client0);
    InitTestClient(2, client2);
    int size = 1024;
    std::vector<uint8_t> data(size, 0);
    int objNum = 200;
    CreateParam param = CreateParam{};
    for (auto i = 0; i < objNum; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
        DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
        DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
        DS_ASSERT_OK(client0->Put(key, data.data(), size, param));
    }
    StartWorkerAndWaitReady({ 1 });
    sleep(20);
    InitTestClient(1, client1);
    std::vector<Optional<Buffer>> dataList;
    for (int i = 0; i < objNum; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_OK(client1->Get({ key }, 0, dataList));
        DS_ASSERT_OK(client2->Get({ key }, 0, dataList));
    }
    DS_ASSERT_OK(client2->ReleaseGRefs("remoteId1"));
    DS_ASSERT_OK(client2->ReleaseGRefs("remoteId2"));
    for (int i = 0; i < objNum; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_OK(client1->Get({ key }, 0, dataList));
        DS_ASSERT_OK(client2->Get({ key }, 0, dataList));
    }
    DS_ASSERT_OK(client2->ReleaseGRefs("remoteId3"));
    for (auto i = 0; i < objNum; i++) {
        auto key = "Update_" + std::to_string(i);
        DS_ASSERT_NOT_OK(client0->Get({ key }, 0, dataList));
        DS_ASSERT_NOT_OK(client1->Get({ key }, 0, dataList));
        DS_ASSERT_NOT_OK(client2->Get({ key }, 0, dataList));
    }
}

TEST_F(OCRemoteClientIdRefMigrateTest, DISABLED_TestRemoteClientObjRefDfx2)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    std::shared_ptr<ObjectClient> client0, client1, client2;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    InitTestClient(2, client2);
    std::vector<std::string> failObjects;
    uint64_t size = 1024;
    std::vector<uint8_t> data(size, 0);
    std::vector<Optional<Buffer>> dataList;
    CreateParam param = CreateParam{};

    DS_ASSERT_OK(client0->GIncreaseRef({ "key2", "key3", "key4" }, failObjects));
    DS_ASSERT_OK(client1->GIncreaseRef({ "key2", "key3", "key4" }, failObjects));
    RestartWorkerAndWaitReady({ 0 });
    DS_ASSERT_OK(client0->Put("key2", data.data(), size, param));
    DS_ASSERT_OK(client1->Get({ "key2" }, 0, dataList));
    DS_ASSERT_OK(client1->GDecreaseRef({ "key2", "key3", "key4" }, failObjects));
    DS_ASSERT_OK(client1->GDecreaseRef({ "key2", "key3", "key4" }, failObjects));
    DS_ASSERT_NOT_OK(client0->Get({ "key2" }, 0, dataList));
    DS_ASSERT_NOT_OK(client1->Get({ "key2" }, 0, dataList));

    DS_ASSERT_OK(client0->GIncreaseRef({ "key5" }, failObjects));
    DS_ASSERT_OK(client1->GIncreaseRef({ "key6" }, failObjects));
    RestartWorkerAndWaitReady({ 0 });
    DS_ASSERT_OK(client0->Put("key5", data.data(), size, param));
    DS_ASSERT_OK(client0->Put("key6", data.data(), size, param));
    DS_ASSERT_OK(client1->Get({ "key5", "key6" }, 0, dataList));
    DS_ASSERT_OK(client2->ReleaseGRefs("remoteId4"));
    DS_ASSERT_NOT_OK(client0->Get({ "key5", "key6" }, 0, dataList));
    DS_ASSERT_NOT_OK(client1->Get({ "key5", "key6" }, 0, dataList));
}

TEST_F(OCRemoteClientIdRefMigrateTest, TestRemoteClientObjRefRedirect1)
{
    std::shared_ptr<ObjectClient> client0, client1, client2;
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(externalCluster_->StartForkWorkerProcess());
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t size = 1024;
    std::vector<uint8_t> data(size, 0);
    int objNum = 400;

    ThreadPool threadPool(1);
    std::vector<std::future<void>> futureVec;
    futureVec.push_back(threadPool.Submit([&client0, &client1, &failObjects, data, size, objNum] {
        CreateParam param = CreateParam{};
        std::vector<Optional<Buffer>> dataList;
        for (auto i = 0; i < objNum; i++) {
            auto key = "remoteObj_" + std::to_string(i);
            DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
            DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
            DS_ASSERT_OK(client0->Put(key, data.data(), size, param));
        }
        for (auto i = 0; i < objNum; i++) {
            auto key = "remoteObj_" + std::to_string(i);
            DS_ASSERT_OK(client1->GDecreaseRef({ key }, failObjects));
            DS_ASSERT_OK(client1->GDecreaseRef({ key }, failObjects));
        }
    }));
    // Migrate and Redirect
    sleep(1);
    const int index = 2;
    DS_ASSERT_OK(externalCluster_->StartWorkerByForkProcess(index));
    WaitWorkerReady({ index });
    InitTestClient(index, client2);
    for (auto &future : futureVec) {
        future.get();
    }
}

TEST_F(OCRemoteClientIdRefMigrateTest, TestRemoteClientObjRefRedirect2)
{
    std::shared_ptr<ObjectClient> client0, client1, client2;
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(externalCluster_->StartForkWorkerProcess());
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t size = 1024;
    std::vector<uint8_t> data(size, 0);
    int objNum = 400;

    ThreadPool threadPool(1);
    std::vector<std::future<void>> futureVec;
    futureVec.push_back(threadPool.Submit([&client0, &client1, &failObjects, data, size, objNum] {
        CreateParam param = CreateParam{};
        std::vector<Optional<Buffer>> dataList;
        for (auto i = 0; i < objNum; i++) {
            auto key = "remoteObj_" + std::to_string(i);
            DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
            DS_ASSERT_OK(client0->Put(key, data.data(), size, param));
        }
        DS_ASSERT_OK(client1->ReleaseGRefs("remoteId1"));
    }));
    // Migrate and Redirect
    const int index = 2;
    DS_ASSERT_OK(externalCluster_->StartWorkerByForkProcess(index));
    WaitWorkerReady({ index });
    InitTestClient(index, client2);
    for (auto &future : futureVec) {
        future.get();
    }
}
}  // namespace st
}  // namespace datasystem
