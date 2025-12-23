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
 * Description: This is used to test the ObjectClient class.
 */
#include <memory>
#include <gtest/gtest.h>

#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/uuid_generator.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
namespace {
const char *HOST_IP = "127.0.0.1";
constexpr int64_t NON_SHM_SIZE = 499 * 1024;
constexpr int64_t SHM_SIZE = 500 * 1024;
}  // namespace
class ObjectClientWithTokenTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -shared_memory_size_mb=5120 -v=2 -authorization_enable=true";
    }

    void InitTestTokenClient(uint32_t workerIndex, std::shared_ptr<ObjectClient> &client,
                             const std::string &token = "token1")
    {
        ConnectOptions connectOptions;
        InitConnectTokenOpt(workerIndex, connectOptions, token);
        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void InitConnectTokenOpt(uint32_t workerIndex, ConnectOptions &connectOptions, const std::string &token = "token1",
                             int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.enableExclusiveConnection = true;
        connectOptions.token = token;
    }

    void EndToEndSuccess(int64_t size, bool isSeal);

    void EndToEndRemoteGet(int64_t size, bool isSeal);

    void CreateBufferSuccess(int64_t size, CreateParam &param);

    void GetMultiObjectSuccess(int64_t size);

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->SetInjectAction(
            WORKER, 0, "worker.auth", "1*return(token1,tenant1)->1*return(token2,tenant2)->1*return(token3,tenant3)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(
            WORKER, 1, "worker.auth", "1*return(token1,tenant1)->1*return(token2,tenant2)->1*return(token3,tenant3)"));
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

// Test create->GIncreaseRef->WLatch->Write->Publish->UnWLatch->Get->RLatch->MutableData->UnRLatch->GDecreaseRef
void ObjectClientWithTokenTest::EndToEndSuccess(int64_t size, bool isSeal)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    InitTestTokenClient(0, client, "token1");
    std::string data = GenRandomString(size);
    DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    buffer->WLatch();
    buffer->MemoryCopy((void *)data.data(), size);
    if (isSeal) {
        buffer->Seal();
    } else {
        buffer->Publish();
    }
    buffer->UnWLatch();

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, buffers));
}

void ObjectClientWithTokenTest::EndToEndRemoteGet(int64_t size, bool isSeal)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    InitTestTokenClient(0, client);
    InitTestTokenClient(1, client1);
    std::string data = GenRandomString(size);

    DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    buffer->WLatch();
    buffer->MemoryCopy(data.data(), size);
    if (isSeal) {
        buffer->Seal();
    } else {
        buffer->Publish();
    }
    buffer->UnWLatch();

    std::vector<Optional<Buffer>> buffers;
    std::vector<Optional<Buffer>> buffer1;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();

    DS_ASSERT_OK(client1->Get(objectKeys, 0, buffer1));
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_EQ(buffer1.size(), objectKeys.size());
    ASSERT_EQ(buffer1[0]->GetSize(), size);
    buffer1[0]->RLatch();
    AssertBufferEqual(*buffer1[0], data);
    buffer1[0]->UnRLatch();
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));

    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

void ObjectClientWithTokenTest::CreateBufferSuccess(int64_t size, CreateParam &param)
{
    std::string objectKey = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<Buffer> buffer2;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestTokenClient(0, client, "token1");
    InitTestTokenClient(0, client2, "token2");
    DS_ASSERT_OK(client->Create(objectKey, size, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer->GetSize());

    DS_ASSERT_OK(client2->Create(objectKey2, size, param, buffer2));
    ASSERT_NE(buffer2, nullptr);
    ASSERT_EQ(size, buffer2->GetSize());
}

void ObjectClientWithTokenTest::GetMultiObjectSuccess(int64_t size)
{
    std::shared_ptr<ObjectClient> client;
    const int timeout = 3;
    InitTestTokenClient(0, client);

    std::string data = GenRandomString(size);
    std::string objectKey1 = NewObjectKey();
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client->Create(objectKey1, data.size(), CreateParam{}, buffer1));
    buffer1->Publish();

    std::string objectKey2 = NewObjectKey();
    std::shared_ptr<Buffer> buffer2;
    DS_ASSERT_OK(client->Create(objectKey2, data.size(), CreateParam{}, buffer2));
    buffer2->Publish();

    std::vector<std::string> objectKeys{ objectKey1, objectKey2 };
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get(objectKeys, timeout, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
}

TEST_F(ObjectClientWithTokenTest, CreateShmBufferSuccess)
{
    // Shared memory, non-Keep scenario
    CreateParam param{};
    CreateBufferSuccess(SHM_SIZE, param);
}

TEST_F(ObjectClientWithTokenTest, RepeatedCreateTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestTokenClient(0, client, "token1");
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> data1;
    std::shared_ptr<Buffer> data2;
    std::shared_ptr<Buffer> data3;
    std::shared_ptr<Buffer> data4;

    // When repeated creation happens, delete the object and then re-create in the case of non-shm
    DS_ASSERT_OK(client->Create(objectKey, NON_SHM_SIZE, CreateParam{}, data1));
    DS_ASSERT_OK(client->Create(objectKey, NON_SHM_SIZE, CreateParam{}, data2));

    // When repeated creation happens, return error in the case of shm
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, data3));
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, data4));
}

TEST_F(ObjectClientWithTokenTest, PutGetDeleteShmTest)
{
    FLAGS_v = 1;
    std::shared_ptr<ObjectClient> client;
    InitTestTokenClient(0, client, "token1");
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(
        client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), SHM_SIZE, CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(ObjectClientWithTokenTest, TestConcurrentPublish)
{
    LOG(INFO) << "Start to TestConsistencyCausal from OC client";
    std::shared_ptr<ObjectClient> client;
    InitTestTokenClient(0, client, "token1");

    // client create obj and  publish
    std::string objectKey = NewObjectKey();
    char data[] = { '1', '2', '3' };
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, sizeof(data), CreateParam(), buffer));
    buffer->MemoryCopy(data, sizeof(data));
    DS_ASSERT_OK(buffer->Publish());

    int threadNum = 2;
    std::vector<std::thread> clientThreads(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        clientThreads[i] = std::thread([i, objectKey, this]() {
            std::shared_ptr<ObjectClient> client;
            InitTestTokenClient(i, client, "token1");
            char newdata[] = { '4', '5', '6' };
            std::vector<Optional<Buffer>> buffers;
            int64_t timeOut = 5;
            Status status;
            do {
                status = client->Get({ objectKey }, timeOut, buffers);
            } while (status.IsError());
            buffers[0]->MemoryCopy(newdata, sizeof(newdata));
            int loopNum = 50;
            for (int j = 0; j < loopNum; ++j) {
                DS_ASSERT_OK(buffers[0]->Publish());
            }
        });
    }

    for (auto &t : clientThreads) {
        t.join();
    }
}

TEST_F(ObjectClientWithTokenTest, QueryMetaAndCreateMetaConcurrently)
{
    uint64_t size = 1024 * 1024;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestTokenClient(0, client1, "token1");
    InitTestTokenClient(1, client2, "token1");
    std::string data = GenRandomString(size);
    std::string objectKey = "key_123";

    ThreadPool threadPool(2);
    auto fut = threadPool.Submit([&client1, &objectKey, &data]() {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                                  CreateParam{}));
    });
    auto fut2 = threadPool.Submit([&client2, &objectKey, &data, this]() {
        std::vector<Optional<Buffer>> buffers;
        std::vector<std::string> objectKeys{ objectKey };
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.slow_query_meta", "1*sleep(2000)"));
        DS_ASSERT_OK(client2->Get(objectKeys, 10000, buffers));
        AssertBufferEqual(*buffers[0], data);
    });
    fut.get();
    fut2.get();
}

TEST_F(ObjectClientWithTokenTest, ConnectAndDisconnectConcurrently)
{
    ThreadPool threadPool(5);
    std::shared_ptr<ObjectClient> client;
    InitTestTokenClient(0, client, "token1");
    InitTestTokenClient(0, client, "token2");
    InitTestTokenClient(0, client, "token3");
    auto fut = threadPool.Submit([this]() {
        for (int i = 0; i < 100; i++) {
            std::shared_ptr<ObjectClient> client1;
            InitTestTokenClient(0, client1, "token1");
        }
    });
    auto fut2 = threadPool.Submit([this]() {
        for (int i = 0; i < 100; i++) {
            std::shared_ptr<ObjectClient> client2;
            InitTestTokenClient(0, client2, "token2");
        }
    });
    auto fut3 = threadPool.Submit([this]() {
        for (int i = 0; i < 100; i++) {
            std::shared_ptr<ObjectClient> client3;
            InitTestTokenClient(0, client3, "token3");
        }
    });
    fut.get();
    fut2.get();
    fut3.get();
}

TEST_F(ObjectClientWithTokenTest, RemoteGetBig)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestTokenClient(0, client1, "token1");
    InitTestTokenClient(1, client2, "token1");
    const int64_t size = 1024ul * 1024ul * 1024ul * 2 + 1024;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));

    ASSERT_EQ(buffers1[0]->GetSize(), size);
    ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
}

TEST_F(ObjectClientWithTokenTest, TenantArenaReleaseTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestTokenClient(0, client1, "token1");
    InitTestTokenClient(1, client2, "token1");
    const int64_t size = 1024ul * 1024ul * 1024ul * 2 + 1024;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));

    ASSERT_EQ(buffers1[0]->GetSize(), size);
    ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);

    client1.reset();

    std::this_thread::sleep_for(std::chrono::seconds(7));
    InitTestTokenClient(0, client1, "token1");
    const int64_t sizeNew = 1024ul * 1024ul + 1024;
    std::string objectKeyNew = NewObjectKey();
    std::string dataNew(sizeNew, 'a');
    DS_ASSERT_OK(client1->Put(objectKeyNew, reinterpret_cast<uint8_t *>(const_cast<char *>(dataNew.data())),
                              dataNew.size(), CreateParam{}));
}
}  // namespace st
}  // namespace datasystem
