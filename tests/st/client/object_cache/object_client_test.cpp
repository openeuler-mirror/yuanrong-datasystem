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
 * Description: This is used to test the ObjectClient class.
 */
#include <dirent.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/uuid_generator.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
namespace {
const char *HOST_IP = "127.0.0.1";
const char *HOST_IPV6 = "::1";
constexpr int WORKER_NUM = 3;
constexpr int64_t NON_SHM_SIZE = 499 * 1024;
constexpr int64_t SHM_SIZE = 500 * 1024;
constexpr int64_t BIG_STR_SIZE = 50 * 1024 * 1024;
constexpr int64_t DEFAULT_TIMEOUT_MS = 1000;
}  // namespace
class ObjectClientTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = FormatString(
            " -shared_memory_size_mb=5120 -v=2 -payload_nocopy_threshold=1000000 -max_client_num=%d", maxClientNum);
    }

    void EndToEndSuccess(int64_t size, bool isSeal);

    void EndToEndRemoteGet(int64_t size, bool isSeal);

    void CreateBufferSuccess(int64_t size, CreateParam &param);

    void GetMultiObjectSuccess(int64_t size);

    void SetUp() override
    {
        ImmutableStringPool::Instance().Init();
        intern::StringPool::InitAll();
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

protected:
    int maxClientNum = 200;
};

// Test create->GIncreaseRef->WLatch->Write->Publish->UnWLatch->Get->RLatch->MutableData->UnRLatch->GDecreaseRef
void ObjectClientTest::EndToEndSuccess(int64_t size, bool isSeal)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
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
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, buffers));
}

void ObjectClientTest::EndToEndRemoteGet(int64_t size, bool isSeal)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);
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
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();

    DS_ASSERT_OK(client1->Get(objectKeys, 0, buffer1));
    ASSERT_TRUE(NotExistsNone(buffer1));
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(buffer1.size(), objectKeys.size());
    ASSERT_EQ(buffer1[0]->GetSize(), size);
    buffer1[0]->RLatch();
    AssertBufferEqual(*buffer1[0], data);
    buffer1[0]->UnRLatch();
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
}

void ObjectClientTest::CreateBufferSuccess(int64_t size, CreateParam &param)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    DS_ASSERT_OK(client->Create(objectKey, size, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer->GetSize());
}

void ObjectClientTest::GetMultiObjectSuccess(int64_t size)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

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
    DS_ASSERT_OK(client->Get(objectKeys, 3, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
}

TEST_F(ObjectClientTest, DISABLED_LEVEL1_TestAsynPublishAndShutdown)
{
    auto func1 = [](std::shared_ptr<ObjectClient> client, std::string objectKey, std::shared_ptr<Buffer> buffer,
                    std::string data, int size) {
        auto rc = client->Create(objectKey, size, CreateParam{}, buffer);
        if (rc.IsOk()) {
            std::vector<std::string> objectKeys{ objectKey };
            std::vector<std::string> failedObjectKeys;
            client->GIncreaseRef(objectKeys, failedObjectKeys);
            buffer->WLatch();
            buffer->MemoryCopy((void *)data.data(), size);
            buffer->Publish();
            buffer->UnWLatch();
        }
    };

    auto func2 = [](std::shared_ptr<ObjectClient> client, std::string objectKey, std::string data, int size) {
        std::vector<std::string> failedObjectKeys;
        client->GIncreaseRef({ objectKey }, failedObjectKeys);
        client->Put(objectKey, (uint8_t *)data.c_str(), size, CreateParam{});
    };

    auto func3 = [](std::shared_ptr<ObjectClient> client) { DS_ASSERT_OK(client->ShutDown()); };

    int loopNum = 1000;
    for (int i = 0; i < loopNum; i++) {
        int size = 10;
        std::string objectKey1 = "key1" + std::to_string(i);
        std::string objectKey2 = "key2" + std::to_string(i);
        std::shared_ptr<Buffer> buffer;
        std::shared_ptr<ObjectClient> client;
        InitTestClient(0, client);
        std::string data = "val";

        std::vector<std::thread> clientThreads;
        clientThreads.emplace_back(std::thread(func1, client, objectKey1, buffer, data, size));
        clientThreads.emplace_back(std::thread(func2, client, objectKey2, data, size));
        clientThreads.emplace_back(std::thread(func3, client));
        for (auto &t : clientThreads) {
            t.join();
        }

        std::shared_ptr<ObjectClient> client1;
        InitTestClient(0, client1);
        std::vector<Optional<Buffer>> buffers;
        auto num = client1->QueryGlobalRefNum(objectKey1);
        ASSERT_EQ(num, 0) << "objectKey1: " << objectKey1 << " " << num;
        EXPECT_EQ(client1->Get({ objectKey1 }, 0, buffers).GetCode(), StatusCode::K_NOT_FOUND);
        num = client1->QueryGlobalRefNum(objectKey2);
        ASSERT_EQ(num, 0) << "objectKey2: " << objectKey2 << " " << num;
        EXPECT_EQ(client1->Get({ objectKey2 }, 0, buffers).GetCode(), StatusCode::K_NOT_FOUND);
    }
}

TEST_F(ObjectClientTest, CreateShmBufferSuccess)
{
    // Shared memory, non-Keep scenario
    CreateParam param{};
    CreateBufferSuccess(SHM_SIZE, param);
}

TEST_F(ObjectClientTest, CreateNonShmBufferSuccess)
{
    // Non-shared memory, Keep scenario
    CreateParam param{};
    CreateBufferSuccess(NON_SHM_SIZE, param);
}

TEST_F(ObjectClientTest, RepeatedCreateTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
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

TEST_F(ObjectClientTest, PutGetDeleteShmTest)
{
    FLAGS_v = 1;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(
        client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), SHM_SIZE, CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(ObjectClientTest, PutGetDeleteNonShmTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(NON_SHM_SIZE);

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), NON_SHM_SIZE,
                             CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers[0]).ImmutableData()), (*buffers[0]).GetSize()));
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(ObjectClientTest, DISABLED_RepeatPutTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(NON_SHM_SIZE);
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), NON_SHM_SIZE,
                             CreateParam{}));
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), NON_SHM_SIZE,
                             CreateParam{}));
    objectKey = NewObjectKey();
    std::string dataShm = GenRandomString(SHM_SIZE);
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(dataShm.data())), SHM_SIZE,
                             CreateParam{}));
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(dataShm.data())), SHM_SIZE,
                             CreateParam{}));
}

TEST_F(ObjectClientTest, EndToEndSuccessWithShmSealSuccess)
{
    EndToEndSuccess(SHM_SIZE, true);
}

TEST_F(ObjectClientTest, EndToEndSuccessWithShmPubSuccess)
{
    EndToEndSuccess(SHM_SIZE, false);
}

TEST_F(ObjectClientTest, EndToEndSuccessWithNonShmSealSuccess)
{
    EndToEndSuccess(NON_SHM_SIZE, true);
}

TEST_F(ObjectClientTest, EndToEndSuccessWithNonShmPubSuccess)
{
    EndToEndSuccess(NON_SHM_SIZE, false);
}

TEST_F(ObjectClientTest, EndToEndRemoteGetWithShmSealSuccess)
{
    EndToEndRemoteGet(SHM_SIZE, true);
}

TEST_F(ObjectClientTest, EndToEndRemoteGetWithShmPubSuccess)
{
    EndToEndRemoteGet(SHM_SIZE, false);
}

TEST_F(ObjectClientTest, EndToEndRemoteGetWithNonShmSealSuccess)
{
    EndToEndRemoteGet(NON_SHM_SIZE, true);
}

TEST_F(ObjectClientTest, LEVEL1_EndToEndRemoteGetWithNonShmPubSuccess)
{
    EndToEndRemoteGet(NON_SHM_SIZE, false);
}

TEST_F(ObjectClientTest, EndToEndWithVectorSuccess)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    int vecSize = NON_SHM_SIZE / sizeof(uint64_t);
    std::vector<uint64_t> data = GenRandomVector(vecSize);
    int64_t dataSize = data.size() * sizeof(uint64_t);
    DS_ASSERT_OK(client->Create(objectKey, dataSize, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);

    buffer->MemoryCopy((void *)data.data(), dataSize);
    buffer->Publish();

    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), dataSize);
    AssertBufferEqual(*buffers[0], reinterpret_cast<uint8_t *>(data.data()), dataSize);
}

TEST_F(ObjectClientTest, LEVEL1_InvalidateBufferAndRePublishSuccess)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    const int32_t timeoutMs = 1000;
    InitTestClient(0, client, timeoutMs);
    int dataSize = SHM_SIZE;
    CreateParam param{};
    DS_ASSERT_OK(client->Create(objectKey, dataSize, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(dataSize, buffer->GetSize());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->InvalidateBuffer());

    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, buffers));

    // Test the function of republishing an object.
    DS_ASSERT_OK(buffer->Publish());
}

TEST_F(ObjectClientTest, LEVEL2_InvalidateBufferAndRemoteGetFailed)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    int dataSize = SHM_SIZE;
    CreateParam param{};
    DS_ASSERT_OK(client->Create(objectKey, dataSize, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(dataSize, buffer->GetSize());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->InvalidateBuffer());

    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);

    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_NOT_OK(client1->Get(objectKeys, 0, buffers));
}

TEST_F(ObjectClientTest, InvalidateBufferAfterRemoteGet)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };

    int size = 10;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, size, param, buffer));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(buffer->InvalidateBuffer());
    DS_ASSERT_OK(buffer->Publish());

    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
}

TEST_F(ObjectClientTest, GetMultiObjectWithShmSuccess)
{
    GetMultiObjectSuccess(SHM_SIZE);
}

TEST_F(ObjectClientTest, GetMultiObjectWithNonShmSuccess)
{
    GetMultiObjectSuccess(NON_SHM_SIZE);
}

TEST_F(ObjectClientTest, GetAndModifySuccess)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;

    std::string data = GenRandomString(SHM_SIZE);
    int64_t dataSize = data.size();
    CreateParam param;
    DS_ASSERT_OK(client->Create(objectKey, dataSize, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(dataSize, buffer->GetSize());

    buffer->MemoryCopy((void *)data.data(), dataSize);
    buffer->Publish();

    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), dataSize);
    auto &bufferGet = buffers[0];
    ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet->ImmutableData()), bufferGet->GetSize()));

    std::string newData = GenRandomString(SHM_SIZE);
    DS_ASSERT_OK(bufferGet->MemoryCopy((void *)newData.data(), newData.size()));
    DS_ASSERT_OK(bufferGet->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    auto &bufferGet1 = buffers2[0];
    ASSERT_EQ(newData, std::string(reinterpret_cast<const char *>(bufferGet1->ImmutableData()), bufferGet1->GetSize()));
}

TEST_F(ObjectClientTest, ErrorGetTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 5, buffers));
}

TEST_F(ObjectClientTest, RepeatedPublishTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();

    std::string data = GenRandomString(SHM_SIZE);
    int64_t dataSize = data.size();
    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    DS_ASSERT_OK(client->Create(objectKey, dataSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Publish());  // Publish again to update
}

/**
 * @brief Two clients create the same Object, both can success.
 */
TEST_F(ObjectClientTest, MultiplyClientCreateOkTest)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client);
    InitTestClient(0, client2);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> data1;
    std::shared_ptr<Buffer> data2;

    ThreadPool threadPool(2);
    auto fut = threadPool.Submit([objectKey, &data1, client]() {
        LOG(INFO) << "client starts to create object";
        CreateParam param;
        auto rc = client->Create(objectKey, SHM_SIZE, param, data1);
        LOG(INFO) << "client ends to create object";
        return rc;
    });
    auto fut2 = threadPool.Submit([objectKey, &data2, client2]() {
        LOG(INFO) << "client2 starts to create object";
        CreateParam param;
        auto rc = client2->Create(objectKey, SHM_SIZE, param, data2);
        LOG(INFO) << "client2 ends to create object";
        return rc;
    });
    ASSERT_EQ(fut.get(), Status::OK());
    ASSERT_EQ(fut2.get(), Status::OK());
    DS_ASSERT_OK(data1->Publish());
    DS_ASSERT_OK(data2->Seal());
}

std::string MockGetClientId()
{
    return "123456";
}

TEST_F(ObjectClientTest, ImmutableObjectTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Seal());
    ASSERT_NE(buffer, nullptr);
    DS_ASSERT_NOT_OK(buffer->Seal());
}

TEST_F(ObjectClientTest, PublishAfterSealTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Seal());
    ASSERT_NE(buffer, nullptr);
    DS_ASSERT_NOT_OK(buffer->Publish());
}

TEST_F(ObjectClientTest, SealAfterPublishTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Publish());
    ASSERT_NE(buffer, nullptr);
    DS_ASSERT_OK(buffer->Seal());
}

TEST_F(ObjectClientTest, LatestObjectGetTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> workerOBuffer;
    int bufferSize = SHM_SIZE;
    // Causal consistency triggers synchronous invalidation.
    CreateParam createParam = { .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client1->Create(objectKey, bufferSize, createParam, workerOBuffer));
    std::string worker0_data = GenRandomString(bufferSize);
    DS_ASSERT_OK(workerOBuffer->MemoryCopy(const_cast<char *>(worker0_data.data()), worker0_data.length()));
    DS_ASSERT_OK(workerOBuffer->Publish());

    std::hash<std::string> hash;
    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    std::string worker1Result0(reinterpret_cast<const char *>(dataList[0]->ImmutableData()), worker0_data.length());
    ASSERT_EQ(hash(worker1Result0), hash(worker0_data));

    std::string worker1_data = GenRandomString(bufferSize);
    DS_ASSERT_OK(dataList[0]->MemoryCopy(const_cast<char *>(worker1_data.data()), worker1_data.length()));
    DS_ASSERT_OK(dataList[0]->Publish());

    DS_ASSERT_OK(client1->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    std::string worker0Result1(reinterpret_cast<const char *>(dataList[0]->ImmutableData()), worker1_data.length());
    ASSERT_EQ(hash(worker0Result1), hash(worker1_data));
}

TEST_F(ObjectClientTest, ClientStateSync)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Seal());

    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    AssertStatusCode(dataList[0]->Seal(), K_OC_ALREADY_SEALED);
}

TEST_F(ObjectClientTest, RemoteSealTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, 20, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    DS_ASSERT_OK(dataList[0]->Seal());
    DS_ASSERT_NOT_OK(buffer->Publish());
}

TEST_F(ObjectClientTest, ExpireObjectUpdate)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> workerOBuffer;
    int bufferSize = SHM_SIZE;

    // Causal consistency triggers synchronous invalidation.
    CreateParam createParam = { .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client1->Create(objectKey, bufferSize, createParam, workerOBuffer));
    std::string worker0_data = GenRandomString(SHM_SIZE);
    DS_ASSERT_OK(workerOBuffer->MemoryCopy(const_cast<char *>(worker0_data.data()), worker0_data.length()));
    DS_ASSERT_OK(workerOBuffer->Publish());

    std::hash<std::string> hash;
    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    std::string worker1_data = GenRandomString(SHM_SIZE);
    DS_ASSERT_OK(dataList[0]->MemoryCopy(const_cast<char *>(worker1_data.data()), worker1_data.length()));
    DS_ASSERT_OK(dataList[0]->Publish());

    std::string worker0_data2 = GenRandomString(SHM_SIZE);
    DS_ASSERT_OK(workerOBuffer->MemoryCopy(const_cast<char *>(worker0_data2.data()), worker0_data2.length()));
    DS_ASSERT_OK(workerOBuffer->Publish());
    dataList.clear();
    DS_ASSERT_OK(client1->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    std::string worker0Result(reinterpret_cast<const char *>(dataList[0]->ImmutableData()), worker0_data2.length());
    ASSERT_EQ(hash(worker0Result), hash(worker0_data2));
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    std::string worker1Result(reinterpret_cast<const char *>(dataList[0]->ImmutableData()), worker0_data2.length());
    ASSERT_EQ(hash(worker1Result), hash(worker0_data2));
}

TEST_F(ObjectClientTest, AsynPublishSealTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    ThreadPool threadPool(2);
    Status rc1;
    Status rc2;
    auto fut1 = threadPool.Submit([&buffer, &rc1]() { rc1 = buffer->Seal(); });
    auto fut2 = threadPool.Submit([&dataList, &rc2]() { rc2 = dataList[0]->Publish(); });
    fut1.get();
    fut2.get();
    ASSERT_TRUE((rc1.IsOk() || rc2.IsOk()));
}

TEST_F(ObjectClientTest, AsynPublishTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    ThreadPool threadPool(2);
    Status rc1;
    Status rc2;
    auto fut1 = threadPool.Submit([&buffer, &rc1]() { rc1 = buffer->Publish(); });
    auto fut2 = threadPool.Submit([&dataList, &rc2]() { rc2 = dataList[0]->Publish(); });
    fut1.get();
    fut2.get();
    ASSERT_TRUE((rc1.IsOk() && rc2.IsOk()));
}

TEST_F(ObjectClientTest, PublishCreateMetaRetry)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "MasterOCServiceImpl.CreateMeta.idempotence",
                                           "1*return(K_RPC_CANCELLED)"));
    DS_ASSERT_OK(buffer->Publish());
}

TEST_F(ObjectClientTest, SealCreateMetaNotRetry)
{
    std::shared_ptr<ObjectClient> client1;
    int timeoutMs = 3000;
    InitTestClient(0, client1, timeoutMs);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "MasterOCServiceImpl.CreateMeta.idempotence",
                                           "1*return(K_RPC_CANCELLED)"));
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "1*sleep(3000)");
    // seal create meta is not idempotence
    ASSERT_EQ(buffer->Seal().GetCode(), StatusCode::K_RPC_CANCELLED);
}

TEST_F(ObjectClientTest, PublishUpdateMetaRetry)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Publish());

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.update_meta_failure", "1*return(K_RPC_CANCELLED)"));
    DS_ASSERT_OK(buffer->Publish());
}

TEST_F(ObjectClientTest, SealUpdateMetaNotRetry)
{
    std::shared_ptr<ObjectClient> client1;
    int timeoutMs = 3000;
    InitTestClient(0, client1, timeoutMs);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Publish());

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.update_meta_failure", "1*return(K_RPC_CANCELLED)"));
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "1*sleep(3000)");
    // seal create meta is not idempotence
    ASSERT_EQ(buffer->Seal().GetCode(), StatusCode::K_RPC_CANCELLED);
}

TEST_F(ObjectClientTest, PublishMaster2WorkerUpdateNotificationRetry)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    CreateParam createParam;
    createParam.consistencyType = ConsistencyType::CAUSAL;
    DS_ASSERT_OK(client1->Create(objectKey, SHM_SIZE, createParam, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    client2->Get(getObjList, 0, dataList);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "MasterWorkerOCServiceImpl.UpdateNotification.retry",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(buffer->Publish());
}

TEST_F(ObjectClientTest, DISABLED_SuccessPutGetBigDataTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(BIG_STR_SIZE);
    std::shared_ptr<Buffer> dataBuffer;
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(client->Create(objectKey, data.size(), CreateParam{}, dataBuffer));
    DS_ASSERT_OK(dataBuffer->MemoryCopy(const_cast<char *>(data.data()), data.size()));
    DS_ASSERT_OK(dataBuffer->Seal());
    std::vector<Optional<Buffer>> objectBuffer;
    DS_ASSERT_OK(client->Get({ objectKey }, 5, objectBuffer));
    ASSERT_TRUE(NotExistsNone(objectBuffer));
    ASSERT_EQ(objectBuffer.size(), size_t(1));
    AssertBufferEqual(*objectBuffer[0], data);
}

TEST_F(ObjectClientTest, TestConsistencyPRAM)
{
    LOG(INFO) << "Start to TestConsistencyPram from OC client";
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    // create obj
    CreateParam param{ .consistencyType = ConsistencyType::PRAM };
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, data.size(), param, buffer));
    buffer->MemoryCopy(const_cast<char *>(data.data()), data.size());
    DS_ASSERT_OK(buffer->Publish());

    // update obj
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "Asyncsend.cacheinvalid", "1000*sleep(2000)"));
    std::string newData = GenRandomString(SHM_SIZE);
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get({ objectKey }, 5, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    buffers[0]->MemoryCopy(const_cast<char *>(newData.data()), newData.size());
    DS_ASSERT_OK(buffers[0]->Publish());

    // get obj and check version
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client->Get({ objectKey }, 5, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    auto &bufferGet = *buffers1[0];
    if (data != std::string(reinterpret_cast<const char *>(bufferGet.ImmutableData()), bufferGet.GetSize())) {
        LOG(INFO) << std::string(reinterpret_cast<const char *>(bufferGet.ImmutableData()), bufferGet.GetSize())
                         .substr(0, 10);     // subSize is 10
        LOG(INFO) << data.substr(0, 10);     // subSize is 10
        LOG(INFO) << newData.substr(0, 10);  // subSize is 10
        ASSERT_FALSE(true);
    };

    // There is no guarantee about how long it will take for the publish that was issued to worker 1 to complete
    // all the updates to worker 0 via invalidations etc.
    // If it runs quickly, its possible that the new data can be fetched from worker 0 right away.
    // Wait a few seconds and then check. Timings in testcase like this might contribute toward intermittent testcase
    // failures, but hopefully the probability with a 2 second sleep here is enough to get to the new data from worker
    // 0.
    // wait worker to update and check version
    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::function<Status()> func2 = [&] {
        std::vector<Optional<Buffer>> buffers2;
        auto timeout = 5;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(client->Get({ objectKey }, timeout, buffers2), "get failed");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(NotExistsNone(buffers2), K_RUNTIME_ERROR, "object not exist");
        auto &bufferGet1 = *buffers2[0];
        if (newData == std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize())) {
            return Status::OK();
        } else {
            return Status(K_RUNTIME_ERROR, "data not equal");
        }
        return Status::OK();
    };
    auto waitTime = 10;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(func2, waitTime, K_OK));
}

TEST_F(ObjectClientTest, TestConsistencyCAUSAL)
{
    LOG(INFO) << "Start to TestConsistencyCausal from OC client";
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    // client create obj and  publish
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, data.size(), param, buffer));
    buffer->MemoryCopy(const_cast<char *>(data.data()), data.size());
    DS_ASSERT_OK(buffer->Publish());

    // client1 update obj and publish
    std::string newData = GenRandomString(SHM_SIZE);
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get({ objectKey }, 5, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    buffers[0]->MemoryCopy(const_cast<char *>(newData.data()), newData.size());
    DS_ASSERT_OK(buffers[0]->Publish());

    // client2 get and create obj1
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client2);
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get({ objectKey }, 5, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_EQ(newData,
              std::string(reinterpret_cast<const char *>((*buffers1[0]).ImmutableData()), (*buffers1[0]).GetSize()));

    std::string objectKey1 = NewObjectKey();
    std::string data1 = GenRandomString(SHM_SIZE);
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client2->Create(objectKey1, data1.size(), param, buffer1));
    buffer1->MemoryCopy(const_cast<char *>(data1.data()), data1.size());
    DS_ASSERT_OK(buffer1->Publish());

    // check objs version
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client->Get({ objectKey, objectKey1 }, 50, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    auto &bufferGet1 = *buffers2[0];
    auto &bufferGet2 = *buffers2[1];
    ASSERT_EQ(newData, std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize()));
    ASSERT_EQ(data1, std::string(reinterpret_cast<const char *>(bufferGet2.ImmutableData()), bufferGet2.GetSize()));
}

TEST_F(ObjectClientTest, TestObjectsPRAM)
{
    LOG(INFO) << "Start to TestObjectsPram from OC client";
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    // create obj
    CreateParam param{ .consistencyType = ConsistencyType::PRAM };
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, data.size(), param, buffer));
    buffer->MemoryCopy(const_cast<char *>(data.data()), data.size());
    DS_ASSERT_OK(buffer->Publish());

    std::string objectKey1 = NewObjectKey();
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client->Create(objectKey1, data.size(), param, buffer1));
    buffer1->MemoryCopy(const_cast<char *>(data.data()), data.size());
    DS_ASSERT_OK(buffer1->Publish());

    // update obj
    std::string newData = GenRandomString(SHM_SIZE);
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get({ objectKey, objectKey1 }, 5, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers[0]).ImmutableData()), (*buffers[0]).GetSize()));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers[1]).ImmutableData()), (*buffers[1]).GetSize()));
    buffers[0]->MemoryCopy(const_cast<char *>(newData.data()), newData.size());
    buffers[1]->MemoryCopy(const_cast<char *>(newData.data()), newData.size());
    DS_ASSERT_OK(buffers[0]->Publish());
    DS_ASSERT_OK(buffers[1]->Publish());

    // get obj and check version
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client->Get({ objectKey, objectKey1 }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers1[0]).ImmutableData()), (*buffers[0]).GetSize()));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers1[1]).ImmutableData()), (*buffers[1]).GetSize()));

    // There is no guarantee about how long it will take for the publish that was issued to worker 1 to complete
    // all the updates to worker 0 via invalidations etc.
    // If it runs quickly, its possible that the new data can be fetched from worker 0 right away.
    // Wait a few seconds and then check. Timings in testcase like this might contribute toward intermittent testcase
    // failures, but hopefully the probability with a 2 second sleep here is enough to get to the new data from worker
    // 0.
    // wait and check version
    std::this_thread::sleep_for(std::chrono::seconds(4));
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client->Get({ objectKey, objectKey1 }, 5, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    auto &bufferGet1 = *buffers2[0];
    auto &bufferGet2 = *buffers2[1];
    ASSERT_EQ(newData, std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize()));
    ASSERT_EQ(newData, std::string(reinterpret_cast<const char *>(bufferGet2.ImmutableData()), bufferGet2.GetSize()));
}

TEST_F(ObjectClientTest, DISABLED_TestTwoObjectsRRAM)
{
    LOG(INFO) << "Start to TestObjectsPram1 from OC client";
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    // client create obj1
    CreateParam param{ .consistencyType = ConsistencyType::PRAM };
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, data.size(), param, buffer));
    buffer->MemoryCopy(const_cast<char *>(data.data()), data.size());
    DS_ASSERT_OK(buffer->Publish());

    // client1 get and update obj1
    std::string newData = GenRandomString(SHM_SIZE);
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get({ objectKey }, 5, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    buffers[0]->MemoryCopy(const_cast<char *>(newData.data()), newData.size());
    DS_ASSERT_OK(buffers[0]->Publish());

    // client get obj1 check version and create obj2
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers1[0]).ImmutableData()), (*buffers1[0]).GetSize()));
    std::string objectKey1 = NewObjectKey();
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client->Create(objectKey1, data.size(), param, buffer1));
    buffer1->MemoryCopy(const_cast<char *>(data.data()), data.size());
    DS_ASSERT_OK(buffer1->Publish());

    // client1 get obj2 and update
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client1->Get({ objectKey1 }, 5, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    buffers2[0]->MemoryCopy(const_cast<char *>(newData.data()), newData.size());
    DS_ASSERT_OK(buffers2[0]->Publish());

    // client get obj2 and check version
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(client->Get({ objectKey1 }, 0, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers3[0]).ImmutableData()), (*buffers3[0]).GetSize()));

    // wait update and check version
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(client->Get({ objectKey, objectKey1 }, 0, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(newData,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));
    ASSERT_EQ(newData,
              std::string(reinterpret_cast<const char *>((*buffers4[1]).ImmutableData()), (*buffers4[1]).GetSize()));
}

TEST_F(ObjectClientTest, TestObjectAlternatingPublish)
{
    LOG(INFO) << "Start to TestObjectsPram from OC client";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(2, client3);

    // client1 create obj
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    std::string objectKey = NewObjectKey();
    std::string data1 = GenRandomString(SHM_SIZE);
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client1->Create(objectKey, data1.size(), param, buffer1));
    buffer1->MemoryCopy((void *)data1.c_str(), data1.size());
    DS_ASSERT_OK(buffer1->Publish());

    // client2 get and update object
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client2->Get({ objectKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_EQ(data1,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));
    std::string data2 = GenRandomString(SHM_SIZE);
    buffers2[0]->MemoryCopy((void *)data2.c_str(), data2.size());
    DS_ASSERT_OK(buffers2[0]->Publish());

    // client3 get
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(client3->Get({ objectKey }, 5000, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_EQ(buffers3.size(), size_t(1));
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers3[0]).ImmutableData()), (*buffers3[0]).GetSize()));

    // client1 update
    std::string data3 = GenRandomString(SHM_SIZE);
    buffer1->MemoryCopy((void *)data3.c_str(), data3.size());
    DS_ASSERT_OK(buffer1->Publish());

    // client2 update
    std::string data4 = GenRandomString(SHM_SIZE);
    buffers2[0]->MemoryCopy((void *)data4.c_str(), data4.size());
    DS_ASSERT_OK(buffers2[0]->Publish());

    // client3 get
    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(client3->Get({ objectKey }, 5000, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(buffers4.size(), size_t(1));
    ASSERT_EQ(data4,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));
}

TEST_F(ObjectClientTest, TestObjectAlternatingPublish2)
{
    LOG(INFO) << "Start to TestObjectsPram from OC client";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(2, client3);

    // client1 create obj
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    std::string objectKey = NewObjectKey();
    std::string data1 = GenRandomString(SHM_SIZE);
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client1->Create(objectKey, data1.size(), param, buffer1));
    buffer1->MemoryCopy((void *)data1.c_str(), data1.size());
    DS_ASSERT_OK(buffer1->Publish());

    // client2 get and update object
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client2->Get({ objectKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_EQ(data1,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));
    std::string data2 = GenRandomString(SHM_SIZE);
    buffers2[0]->MemoryCopy((void *)data2.c_str(), data2.size());
    DS_ASSERT_OK(buffers2[0]->Publish());

    // client3 get
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(client3->Get({ objectKey }, 5000, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_EQ(buffers3.size(), size_t(1));
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers3[0]).ImmutableData()), (*buffers3[0]).GetSize()));

    // client1 update
    std::string data3 = GenRandomString(SHM_SIZE);
    buffer1->MemoryCopy((void *)data3.c_str(), data3.size());
    DS_ASSERT_OK(buffer1->Publish());

    // client3 get
    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(client3->Get({ objectKey }, 5000, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(buffers4.size(), size_t(1));
    ASSERT_EQ(data3,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));

    // client2 update
    std::string data4 = GenRandomString(SHM_SIZE);
    buffers2[0]->MemoryCopy((void *)data4.c_str(), data4.size());
    DS_ASSERT_OK(buffers2[0]->Publish());

    // client3 get
    std::vector<Optional<Buffer>> buffers5;
    DS_ASSERT_OK(client3->Get({ objectKey }, 5000, buffers5));
    ASSERT_TRUE(NotExistsNone(buffers5));
    ASSERT_EQ(buffers5.size(), size_t(1));
    ASSERT_EQ(data4,
              std::string(reinterpret_cast<const char *>((*buffers5[0]).ImmutableData()), (*buffers5[0]).GetSize()));
}

TEST_F(ObjectClientTest, TestConcurrentPublish)
{
    LOG(INFO) << "Start to TestConsistencyCausal from OC client";
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

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
            InitTestClient(i, client);
            char newdata[] = { '4', '5', '6' };
            std::vector<Optional<Buffer>> buffers;
            int64_t timeOut = 5;
            Status status;
            do {
                status = client->Get({ objectKey }, timeOut, buffers);
            } while (status.IsOk() && ExistsNone(buffers));
            ASSERT_TRUE(NotExistsNone(buffers));
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

TEST_F(ObjectClientTest, AsyncGetAndDelete)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    const int loopTimes = 50;
    for (int i = 0; i < loopTimes; i++) {
        std::string objectKey = NewObjectKey();
        CreateParam param{};
        char data[] = { '1', '2', '3' };
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client1->Create(objectKey, sizeof(data), param, buffer));
        buffer->MemoryCopy(reinterpret_cast<uint8_t *>(data), sizeof(data));
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
        DS_ASSERT_OK(buffer->Publish());
        DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failedObjectKeys));
        DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
        ASSERT_EQ(failedObjectKeys.size(), size_t(0));

        std::vector<std::future<void>> futures;
        ThreadPool threadPool(2);
        futures.emplace_back(threadPool.Submit([&client2, &objectKey]() {
            std::vector<Optional<Buffer>> dataList;
            client2->Get({ objectKey }, 0, dataList);
        }));
        futures.emplace_back(threadPool.Submit([&client2, &objectKey]() {
            std::vector<std::string> failedObjectKeys;
            WaitPost wait;
            wait.WaitFor(1);
            DS_ASSERT_OK(client2->GDecreaseRef({ objectKey }, failedObjectKeys));
            ASSERT_EQ(failedObjectKeys.size(), size_t(0));
        }));
        for (auto &future : futures) {
            future.get();
        }
    }
}

TEST_F(ObjectClientTest, TestDeleteAfterCreate)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    CreateParam param{};
    std::string objectKey = NewObjectKey();
    std::string data = "123";
    std::shared_ptr<Buffer> buffer;
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(client->Create(objectKey, data.size(), param, buffer));
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(client->Create(objectKey, data.size(), param, buffer));
}

TEST_F(ObjectClientTest, GRefAsyncPublishAndDelete)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    const int loopTimes = 100;
    for (int i = 0; i < loopTimes; i++) {
        std::string objectKey = NewObjectKey();
        CreateParam param{};
        char data[] = { '1', '2', '3' };
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(objectKey, sizeof(data), param, buffer));
        buffer->MemoryCopy(reinterpret_cast<uint8_t *>(data), sizeof(data));
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));

        std::vector<std::future<void>> futures;
        ThreadPool threadPool(2);
        futures.emplace_back(threadPool.Submit([&buffer]() { DS_ASSERT_OK(buffer->Publish()); }));
        futures.emplace_back(threadPool.Submit([&client, &objectKey]() {
            std::vector<std::string> failedObjectKeys;
            std::vector<std::string> objectKeys;
            objectKeys.emplace_back(objectKey);
            WaitPost wait;
            wait.WaitFor(1);
            DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
            ASSERT_EQ(failedObjectKeys.size(), size_t(0));
        }));
        for (auto &future : futures) {
            future.get();
        }
    }
}

TEST_F(ObjectClientTest, GRefAsyncGet)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);

    std::vector<std::string> objKeys;
    const int loopTimes = 20;
    for (int i = 0; i < loopTimes; i++) {
        std::string objectKey = NewObjectKey();
        objKeys.emplace_back(objectKey);
        std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
        std::shared_ptr<Buffer> buffer;
        CreateAndSealObject(client2, objectKey, data);
    }
    std::vector<std::string> rObjKeys(objKeys.rbegin(), objKeys.rend());

    for (int i = 0; i < loopTimes; i++) {
        std::vector<std::future<void>> futures;
        ThreadPool threadPool(2);
        futures.emplace_back(threadPool.Submit([&client1, &objKeys]() {
            std::vector<Optional<Buffer>> dataList;
            DS_ASSERT_OK(client1->Get(objKeys, 0, dataList));
            ASSERT_TRUE(NotExistsNone(dataList));
        }));
        futures.emplace_back(threadPool.Submit([&client1, &rObjKeys]() {
            std::vector<Optional<Buffer>> dataList;
            DS_ASSERT_OK(client1->Get(rObjKeys, 0, dataList));
            ASSERT_TRUE(NotExistsNone(dataList));
        }));
        for (auto &future : futures) {
            future.get();
        }
    }
}

TEST_F(ObjectClientTest, BufferDestructTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer1;
    std::shared_ptr<Buffer> buffer2;
    std::shared_ptr<Buffer> buffer3;
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer1));
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer2));

    // After calling std::move(), the resources that buffer1 already holds should be released
    // by implicitly calling its release method, Hence re-create objectKey should succeed.
    buffer1 = std::move(buffer2);
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer3));
}

TEST_F(ObjectClientTest, PutAndRemoteGetTest)
{
    uint64_t size = 1024 * 1024;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string data = GenRandomString(size);
    size_t test_cnt = 100;
    for (size_t i = 0; i < test_cnt; i++) {
        std::string objectKey = NewObjectKey();
        std::vector<std::string> objectKeys{ objectKey };
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
        DS_ASSERT_OK(
            client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));
        std::vector<Optional<Buffer>> buffers;

        DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failedObjectKeys));
        DS_ASSERT_OK(client2->Get(objectKeys, 0, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        AssertBufferEqual(*buffers[0], data);
        DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    }
}

TEST_F(ObjectClientTest, TestPramMultiClientPublishGet)
{
    // Sort worker addresses.
    std::map<std::string, size_t> workerAddrMap;
    for (size_t i = 0; i < cluster_->GetWorkerNum(); ++i) {
        HostPort addr;
        cluster_->GetWorkerAddr(i, addr);
        workerAddrMap.emplace(addr.ToString(), i);
    }

    // Create client
    std::vector<std::shared_ptr<ObjectClient>> clientVec;
    for (const auto &it : workerAddrMap) {
        std::shared_ptr<ObjectClient> client;
        InitTestClient(it.second, client);
        clientVec.push_back(client);
    }

    // client1 create obj
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    std::string objectKey = NewObjectKey();
    std::string data1 = GenRandomString(SHM_SIZE);
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(clientVec[0]->Create(objectKey, data1.size(), param, buffer1));
    buffer1->MemoryCopy((void *)data1.c_str(), data1.size());
    DS_ASSERT_OK(buffer1->Publish());

    // client2 get and update object
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(clientVec[1]->Get({ objectKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), 1ul);
    ASSERT_EQ(data1,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));
    std::string data2 = GenRandomString(SHM_SIZE);
    buffers2[0]->MemoryCopy((void *)data2.c_str(), data2.size());
    DS_ASSERT_OK(buffers2[0]->Publish());

    // client3 get
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(clientVec[2]->Get({ objectKey }, 5000, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_EQ(buffers3.size(), 1ul);
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers3[0]).ImmutableData()), (*buffers3[0]).GetSize()));
}

TEST_F(ObjectClientTest, LEVEL1_HugeMemoryCopyTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    const std::string &objKey = NewObjectKey();

    RandomData rand;
    const int64_t dataSize = rand.GetRandomUint64(0xC800000, 0x1f400000);  // [200MB, 500MB]
    const std::string &data = GenRandomString(dataSize);
    LOG(INFO) << "data size is " << dataSize / 1024 / 1024 << " MB";

    DS_ASSERT_OK(datasystem::inject::Set("memcopy.GetMemChunkLimit", "call(10485760)"));  // 10MB

    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objKey, dataSize, CreateParam{}, buffer));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objKey }, failedObjectKeys));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers[0]->GetSize(), dataSize);
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(client->GDecreaseRef({ objKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(ObjectClientTest, InvalidTimeoutGetTest)
{
    std::shared_ptr<ObjectClient> normalClient;
    InitTestClient(0, normalClient);
    std::string objectKey = NewObjectKey();

    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    std::shared_ptr<Buffer> buffer;
    CreateAndSealObject(normalClient, objectKey, data);

    int64_t maxTimeout = INT32_MAX;
    std::vector<std::string> getObjList{ objectKey };
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(normalClient->Get(getObjList, maxTimeout, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    int32_t invalidTimeout = -1024;
    DS_ASSERT_NOT_OK(normalClient->Get(getObjList, invalidTimeout, dataList));
}

TEST_F(ObjectClientTest, LEVEL1_HugeMemoryCopyTestCreateThreadFailed)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    const std::string &objKey = NewObjectKey();

    RandomData rand;
    const int64_t dataSize = rand.GetRandomUint64(0xC800000, 0x1f400000);  // [200MB, 500MB]
    const std::string &data = GenRandomString(dataSize);
    LOG(INFO) << "data size is " << dataSize / 1024 / 1024 << " MB";

    DS_ASSERT_OK(datasystem::inject::Set("buffer.CreateThreadPool", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(datasystem::inject::Set("memcopy.GetMemChunkLimit", "call(10485760)"));  // 10MB

    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objKey, dataSize, CreateParam{}, buffer));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objKey }, failedObjectKeys));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers[0]->GetSize(), dataSize);
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(client->GDecreaseRef({ objKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(ObjectClientTest, DeleteDeadlockTest)
{
    uint64_t size = 1024 * 1024;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string data = GenRandomString(size);
    size_t test_cnt = 100;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);
    std::vector<std::string> failedObjectKeys;
    for (size_t i = 0; i < test_cnt; i++) {
        std::string objectKey = "key" + std::to_string(i);
        std::vector<std::string> objectKeys{ objectKey };
        DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
        DS_ASSERT_OK(
            client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), size, CreateParam{}));
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failedObjectKeys));
        DS_ASSERT_OK(client2->Get(objectKeys, 0, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        AssertBufferEqual(*buffers[0], data);
    }

    std::vector<std::string> needDelete;
    for (size_t i = 0; i < test_cnt; i++) {
        std::string objectKey = "key" + std::to_string(i);
        needDelete.emplace_back(objectKey);
    }

    ThreadPool threadPool(2);
    auto fut = threadPool.Submit([&needDelete, client1, &failedObjectKeys]() {
        DS_ASSERT_OK(client1->GDecreaseRef(needDelete, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    });
    auto fut2 = threadPool.Submit([&needDelete, client2, &failedObjectKeys]() {
        DS_ASSERT_OK(client2->GDecreaseRef(needDelete, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    });
    fut.get();
    fut2.get();
}

TEST_F(ObjectClientTest, QueryMetaAndCreateMetaConcurrently)
{
    uint64_t size = 1024 * 1024;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
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

TEST_F(ObjectClientTest, TestInvalidKey)
{
    uint64_t size = 1024;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::string data = GenRandomString(size);
    std::string objectKey = "192.168.0.0";

    auto rc = client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{});
    DS_ASSERT_NOT_OK(rc);
    ASSERT_TRUE(rc.GetMsg().find(objectKey) != std::string::npos);
}

TEST_F(ObjectClientTest, DISABLED_ConnectAndDisconnectConcurrently)
{
    ThreadPool threadPool(5);
    auto fut = threadPool.Submit([this]() {
        for (int i = 0; i < 1000; i++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
        }
    });
    auto fut2 = threadPool.Submit([this]() {
        for (int i = 0; i < 1000; i++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
        }
    });
    auto fut3 = threadPool.Submit([this]() {
        for (int i = 0; i < 1000; i++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
        }
    });
    auto fut4 = threadPool.Submit([this]() {
        for (int i = 0; i < 1000; i++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
        }
    });
    auto fut5 = threadPool.Submit([this]() {
        for (int i = 0; i < 1000; i++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
        }
    });
    fut.get();
    fut2.get();
    fut3.get();
    fut4.get();
    fut5.get();
}

TEST_F(ObjectClientTest, LEVEL1_TestMultiThreadsClientInit)
{
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    auto client = std::make_shared<ObjectClient>(connectOptions);
    size_t threadNum = 100;
    ThreadPool threadPool(threadNum);

    for (size_t i = 0; i < threadNum; ++i) {
        threadPool.Execute([&client] { DS_ASSERT_OK(client->Init()); });
    }
}

TEST_F(ObjectClientTest, DISABLED_RemoteGetAndModify)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const uint32_t size = 1024 * 1024;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              CreateParam{}));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.LoadObjectData.AddPayload", "sleep(1000)"));

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers2));
    std::thread t([&buffers2] {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        auto &buffer = *buffers2[0];
        DS_ASSERT_OK(buffer.WLatch());
        DS_ASSERT_OK(buffer.MemoryCopy("b", 1));
        DS_ASSERT_OK(buffer.UnWLatch());
    });

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    t.join();
    AssertBufferEqual(*buffers1[0], data);
}

TEST_F(ObjectClientTest, PutDifferentMetaSize)
{
    uint64_t size1 = 1024;
    uint64_t size2 = 1024 * 1024;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string data1 = GenRandomString(size1);
    std::string data2 = GenRandomString(size2);
    std::string objectKey = GetStringUuid();
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(
        client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data1.data())), data1.size(), param));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers));
    AssertBufferEqual(*buffers[0], data1);

    DS_ASSERT_OK(
        client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data2.data())), data2.size(), param));

    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers));
    AssertBufferEqual(*buffers[0], data2);

    DS_ASSERT_OK(
        client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data1.data())), data1.size(), param));

    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers));
    AssertBufferEqual(*buffers[0], data1);

    DS_ASSERT_OK(
        client2->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data2.data())), data2.size(), param));

    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers));
    AssertBufferEqual(*buffers[0], data2);
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers));
    AssertBufferEqual(*buffers[0], data2);
}

TEST_F(ObjectClientTest, TestInitDFXWithGetSocket)
{
    std::shared_ptr<ObjectClient> client;
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    client = std::make_shared<ObjectClient>(connectOptions);
    DS_ASSERT_OK(datasystem::inject::Set("client.get_sock.fail", "1*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(client->Init());
}

TEST_F(ObjectClientTest, TestInitDFXWithRegister)
{
    std::shared_ptr<ObjectClient> client;
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    client = std::make_shared<ObjectClient>(connectOptions);
    DS_ASSERT_OK(datasystem::inject::Set("client.register.fail", "1*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(client->Init());
}

TEST_F(ObjectClientTest, TestFdNotLeakWhenRegisterFailed)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerServiceImpl.RegisterClient.AboveAddClient",
                                           "return(K_RUNTIME_ERROR)"));
    std::shared_ptr<ObjectClient> client;
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    client = std::make_shared<ObjectClient>(connectOptions);
    int loopNum = 10000;
    for (int i = 0; i < loopNum; i++) {
        DS_ASSERT_NOT_OK(client->Init());
    }

    DIR *dir = opendir(FormatString("/proc/%d/fd", cluster_->GetWorkerPid(0)).c_str());
    ASSERT_NE(dir, nullptr);
    int w0FdCount = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        w0FdCount++;
    }
    closedir(dir);

    // There are only two workers here, and no client has registered successfully. The threshold of 500 is already very
    // loose.
    int maxFdCount = 500;
    ASSERT_LE(w0FdCount, maxFdCount);
}

TEST_F(ObjectClientTest, TestWorkerCacheUdsSockFdUpperLimit)
{
    std::shared_ptr<ObjectClient> client;
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    client = std::make_shared<ObjectClient>(connectOptions);
    DS_ASSERT_OK(datasystem::inject::Set("ClientWorkerCommonApi.Connect.MustUds", "call()"));
    // Worker will cache fd and client will not consume these fds.
    DS_ASSERT_OK(datasystem::inject::Set("ClientWorkerCommonApi.CreateHandShakeFunc", "return(K_RUNTIME_ERROR)"));
    int loopNum = maxClientNum * 10;  // The maximum number of cached fds in a worker is "max_client_num * 10".
    for (int i = 0; i < loopNum; i++) {
        DS_ASSERT_NOT_OK(client->Init());
    }
    DS_ASSERT_OK(datasystem::inject::Clear("ClientWorkerCommonApi.CreateHandShakeFunc"));
    // The client will fail to register because the worker cache FD has reached the upper limit.
    DS_ASSERT_NOT_OK(client->Init());
}

TEST_F(ObjectClientTest, TestWorkerSendFdSlowly)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerServiceImpl.AcceptFdLoop.AfterSend", "sleep(5000)"));
    std::shared_ptr<ObjectClient> client;
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    client = std::make_shared<ObjectClient>(connectOptions);
    DS_ASSERT_OK(datasystem::inject::Set("ClientWorkerCommonApi.Connect.MustUds", "call()"));
    DS_ASSERT_OK(client->Init());
}

TEST_F(ObjectClientTest, TestFdReleasedDueToTimeout)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerServiceImpl.RegisterClient.BlowAuth",
                                           "1*return(K_SERVER_FD_CLOSED)"));
    std::shared_ptr<ObjectClient> client;
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    client = std::make_shared<ObjectClient>(connectOptions);
    ASSERT_EQ(client->Init().GetCode(), K_TRY_AGAIN);
}

TEST_F(ObjectClientTest, RemoteGetBig)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
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

TEST_F(ObjectClientTest, EXCLUSIVE_TestParallelPut)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::vector<int64_t> mixedSizeVec = {
        2ul * 1024ul * 1024ul * 1024 + 2ul * 1024ul,
        260ul * 1024ul * 1024ul,
        20ul * 1024ul * 1024ul,
        2ul * 1024ul * 1024ul,
        10ul * 1024ul,
    };
    size_t objectsNum = mixedSizeVec.size();
    std::vector<std::pair<std::string, std::string>> dataVec;
    dataVec.reserve(objectsNum);
    for (auto size : mixedSizeVec) {
        dataVec.emplace_back(NewObjectKey(), GenPartRandomString(size));
    }

    auto pool = std::make_unique<ThreadPool>(objectsNum);
    std::vector<std::future<Status>> futs;
    futs.reserve(objectsNum);
    for (auto &it : dataVec) {
        futs.emplace_back(pool->Submit([&client2, &it] {
            return client2->Put(it.first, reinterpret_cast<uint8_t *>(const_cast<char *>(it.second.data())),
                                it.second.size(), CreateParam{});
        }));
    }
    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }

    for (auto &it : dataVec) {
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client1->Get({ it.first }, 0, buffers));
        ASSERT_EQ(buffers[0]->GetSize(), static_cast<int64_t>(it.second.size()));
        ASSERT_EQ(memcmp(it.second.data(), buffers[0]->MutableData(), it.second.size()), 0);
    }
}

TEST_F(ObjectClientTest, TestTraceDestructorHeapUseAfterFree)
{
    // The order in which objects are destructed is that threadload static precedes static-modified objects.
    static std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
}

TEST_F(ObjectClientTest, TestClientWorkerVersionDiff)
{
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "WorkerServiceImpl.CheckClientVersion.workerVersion", "call(123)"));
    // The order in which objects are destructed is that threadload static precedes static-modified objects.
    static std::shared_ptr<ObjectClient> client;
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    client = std::make_shared<ObjectClient>(connectOptions);
    DS_ASSERT_OK(client->Init());
}

// Test Worker early etcd shutdown.
TEST_F(ObjectClientTest, LEVEL1_TestEtcdShutdown)
{
    // Provide some startup time to allow the init to settle.
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Shut down etcd server. This will cause the keep alive threading in the worker to start failing.
    // There isn't anything to check here in the testcase, but it will drive the keep alive failure handling and retry
    // logic that will continually try to re-establish connection with the etcd server.
    cluster_->ShutdownNodes(ETCD);

    // Give it some time in this state where etcd is down/missing so that error paths of the worker server are executed
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // Restart it, which will allow keep alive to re-establish lease connections to etcd
    cluster_->StartEtcdCluster();
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

TEST_F(ObjectClientTest, LEVEL2_TestClientTimeoutInterval)
{
    FLAGS_v = 1;
    // Set same check time with zmq.
    // Avoid heartbeat occupy the session lock to create session
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(30000)");
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(30000)");
    ConnectOptions connectOptions;
    // 1.Test invalid timeout configuration, timeout interval should greater than or equal to 500;
    InitConnectOpt(0, connectOptions, 400);
    std::shared_ptr<ObjectClient> client1 = std::make_shared<ObjectClient>(connectOptions);
    ASSERT_EQ(client1->Init().GetCode(), StatusCode::K_INVALID);

    // 2.Construct worker unavailable while GET, calculate the timeout interval meets the expectation
    InitConnectOpt(0, connectOptions, 5000);
    std::shared_ptr<ObjectClient> client2 = std::make_shared<ObjectClient>(connectOptions);
    DS_ASSERT_OK(client2->Init());
    std::shared_ptr<Buffer> buffer1;
    std::string objectKey = NewObjectKey();
    DS_ASSERT_OK(client2->Create(objectKey, SHM_SIZE, CreateParam{}, buffer1));
    DS_ASSERT_OK(buffer1->Publish());
    // Requests sent after the worker is shut down will time out for 3s.
    cluster_->ShutdownNode(WORKER, 0);
    Timer timer;
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(client2->Get({ objectKey }, 5000, buffers));
    auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cast: " << timeCost;
    uint64_t expectedTime = 3000 + 50 + 1950;  // 3000 is rpc timeout, 50 is retry interval, 1950 is the remaining time
    uint64_t maxExpectedTime = expectedTime + 50;  // The actual running time should be 5001 ms.
    ASSERT_TRUE(timeCost >= expectedTime && timeCost <= maxExpectedTime);

    // 3.Construct worker unavailable while PUBLISH, calculate the timeout interval meets the expectation
    timer.Reset();
    DS_ASSERT_NOT_OK(buffer1->Publish());
    timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cast: " << timeCost;
    ASSERT_TRUE(timeCost >= expectedTime && timeCost <= maxExpectedTime);

    // 4.Construct worker unavailable while DELETE, calculate the timeout interval meets the expectation
    timer.Reset();
    DS_ASSERT_NOT_OK(buffer1->InvalidateBuffer());
    timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cast: " << timeCost;
    ASSERT_TRUE(timeCost >= 2500 && timeCost <= 3500);
    cluster_->StartNode(WORKER, 0, "");
}

TEST_F(ObjectClientTest, SetAndCacheInvalidConcurrency)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    InitTestClient(2, client2);

    CreateParam param;
    param.consistencyType = ConsistencyType::PRAM;

    std::string data = "value1";
    std::string newData = "value2";
    std::string outValue;
    std::string objectKey = "wrewr";

    // let w0 insert a cache invalid op of w1
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), param));
    DS_ASSERT_OK(client0->Put(objectKey, reinterpret_cast<const uint8_t *>(newData.data()), newData.size(), param));

    // let w1 become primary again, delay RequestToMaster to wait for w0 sending cache invalidation.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.publish.before_request_to_master", "sleep(500)"));
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), param));

    // check if can get data
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get({ objectKey }, DEFAULT_TIMEOUT_MS, buffers));
}

TEST_F(ObjectClientTest, TestObjectClientDestructor)
{
    // Force extra logging to ensure we don't log anything on program exit
    const int logLevel = 3;
    FLAGS_v = logLevel;
    static std::shared_ptr<ObjectClient> staticClient_;  // For testing exit handler
    InitTestClient(0, staticClient_);
    // Let the destructor goes out of scope
}

TEST_F(ObjectClientTest, MaxClientCreateAndCloseTest)
{
    const int maxClientDefaultNum = 200;
    int loopTimes = 5;
    for (int i = 0; i < loopTimes; i++) {
        std::vector<std::shared_ptr<ObjectClient>> clientVec;
        clientVec.reserve(maxClientDefaultNum);
        for (int j = 0; j < maxClientDefaultNum; j++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
            clientVec.emplace_back(client);
        }
        std::shared_ptr<ObjectClient> client;
        ConnectOptions connectOptions;
        const int timeoutMs = 60000;
        InitConnectOpt(0, connectOptions, timeoutMs);
        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_NOT_OK(client->Init());
        clientVec.clear();
    }
}

TEST_F(ObjectClientTest, LEVEL1_GetClientFdRpcError)
{
    FLAGS_v = 1;
    int32_t timeout = 5000;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, timeout);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.before_GetClientFd", "sleep(10000)"));
    std::shared_ptr<Buffer> buffer1;
    std::string objectKey = NewObjectKey();
    Timer time;
    Status status = client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer1);
    DS_ASSERT_TRUE(status.GetCode(), K_RUNTIME_ERROR);
    LOG(INFO) << "use time: " << time.ElapsedMilliSecond();
    int32_t expectTime = timeout + 50;  // actual value 5001
    ASSERT_LE(time.ElapsedMilliSecond(), expectTime);
}

TEST_F(ObjectClientTest, GetClientFdRecvPageNotify)
{
    int32_t timeout = 5000;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, timeout);
    std::shared_ptr<Buffer> buffer1;
    std::string objectKey = NewObjectKey();
    datasystem::inject::Set("ClientWorkerCommonApi.RecvPageFd", "call(10000)");
    Timer time;
    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer1));
    int32_t expectTime = timeout + 50;  // actual value 5001
    LOG(INFO) << "use time: " << time.ElapsedMilliSecond();
    ASSERT_LE(time.ElapsedMilliSecond(), expectTime);
}

TEST_F(ObjectClientTest, TestObjectKeyWithWorkerUuid)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::shared_ptr<Buffer> buffer;

    std::string objectKey;
    DS_ASSERT_OK(client->GenerateObjectKey("test", objectKey));

    int64_t size = SHM_SIZE;
    std::string data = GenRandomString(size);
    DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    buffer->WLatch();
    buffer->MemoryCopy((void *)data.data(), size);
    buffer->Seal();
    buffer->UnWLatch();

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, buffers));
}

TEST_F(ObjectClientTest, InvalidObjKeyTest)
{
    std::string invalidId = "112233????";

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::shared_ptr<Buffer> buffer;

    int64_t size = SHM_SIZE;
    std::string data = GenRandomString(size);

    DS_ASSERT_NOT_OK(client->Create(invalidId, size, CreateParam{}, buffer));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_NOT_OK(client->GIncreaseRef({ invalidId }, failedObjectKeys));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(client->Get({ invalidId }, 0, buffers));
}

TEST_F(ObjectClientTest, GetNotExitObj)
{
    std::string notExistId = "12332";
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(client->Get({ notExistId }, 0, buffers));
}

TEST_F(ObjectClientTest, TestMisuseGenerateKey)
{
    // case 1: invalid Id
    std::string invalidId = "112233????";
    std::string validId = "112233";
    std::string objectKey;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    DS_ASSERT_NOT_OK(client->GenerateObjectKey(invalidId, objectKey));

    // case 2: empty Id
    DS_ASSERT_OK(client->GenerateObjectKey("", objectKey));

    // case 2: cleint not init
    std::shared_ptr<ObjectClient> client2;
    ConnectOptions connectOptions;
    auto timeoutMs = 60000;
    InitConnectOpt(0, connectOptions, timeoutMs);
    client2 = std::make_shared<ObjectClient>(connectOptions);
    DS_ASSERT_NOT_OK(client2->GenerateObjectKey(validId, objectKey));
}

TEST_F(ObjectClientTest, TestCreateBufferFailed)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    auto func = [&client](const std::string keyPrefix, size_t dataSize, bool isShm) {
        std::string obj1 = keyPrefix + "-1";
        std::string obj2 = keyPrefix + "-2";
        std::string obj3 = keyPrefix + "-3";
        std::string data(dataSize, 'a');
        DS_ASSERT_OK(inject::Clear("buffer.init"));

        DS_ASSERT_OK(client->Put(obj1, reinterpret_cast<const uint8_t *>(data.data()), data.size(), {}));

        DS_ASSERT_OK(inject::Set("buffer.init", "return(K_RUNTIME_ERROR)"));

        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_NOT_OK(client->Get({ obj1 }, 0, buffers));

        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_NOT_OK(client->Create(obj2, dataSize, CreateParam{}, buffer));

        // Put not call Buffer::CreateBuffer for no shm.
        if (isShm) {
            DS_ASSERT_NOT_OK(client->Put(obj3, reinterpret_cast<const uint8_t *>(data.data()), data.size(), {}));
        } else {
            DS_ASSERT_OK(client->Put(obj3, reinterpret_cast<const uint8_t *>(data.data()), data.size(), {}));
        }
    };
    size_t noShmSize = 100;
    size_t shmSize = 600 * 1024;
    func("object1", noShmSize, false);
    func("object2", shmSize, true);
}

TEST_F(ObjectClientTest, TestHealthCheck)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    DS_ASSERT_OK(client->HealthCheck());
}

TEST_F(ObjectClientTest, TestHealthCheckFailed)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    cluster_->ShutdownNode(WORKER, 0);
    int waitWorkerShutDown = 2;
    sleep(waitWorkerShutDown);
    DS_ASSERT_NOT_OK(client->HealthCheck());
}

class ObjectClientTest2 : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.disableRocksDB = false;
    }
};

TEST_F(ObjectClientTest2, TestStoreError)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(NON_SHM_SIZE);
    std::vector<std::string> failedObjectKeys;

    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.rocksdb.put", "2*return(K_KVSTORE_ERROR)"));
    EXPECT_EQ(
        client
            ->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), NON_SHM_SIZE, CreateParam{})
            .GetCode(),
        StatusCode::K_KVSTORE_ERROR);

    std::string data1 = GenRandomString(SHM_SIZE);
    EXPECT_EQ(
        client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data1.data())), SHM_SIZE, CreateParam{})
            .GetCode(),
        StatusCode::K_KVSTORE_ERROR);

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.rocksdb.put", "1*return(K_KVSTORE_ERROR)"));
    EXPECT_EQ(client->GIncreaseRef({ objectKey }, failedObjectKeys).GetCode(), StatusCode::K_KVSTORE_ERROR);
    failedObjectKeys.clear();
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.rocksdb.delete", "1*return(K_KVSTORE_ERROR)"));
    EXPECT_EQ(client->GDecreaseRef({ objectKey }, failedObjectKeys).GetCode(), StatusCode::K_KVSTORE_ERROR);
    failedObjectKeys.clear();
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
}

class ObjectClientTestIPv6 : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.disableRocksDB = false;

        opts.workerConfigs.emplace_back(HOST_IPV6, GetFreePort());
        opts.workerConfigs.emplace_back(HOST_IPV6, GetFreePort());

        // etcd setup IPv6 addrs
        opts.etcdIpAddrs.clear();
        opts.etcdIpAddrs.emplace_back(
            std::make_pair(HostPort(HOST_IPV6, GetFreePort()), HostPort(HOST_IPV6, GetFreePort())));

        std::string workers;
        for (const auto &worker : opts.workerConfigs) {
            workers += worker.ToString() + " ";
        }
        std::string etcdPairs;
        for (const auto &etcdPair : opts.etcdIpAddrs) {
            etcdPairs += etcdPair.first.ToString() + "," + etcdPair.second.ToString() + " ";
        }
        LOG(INFO) << "ObjClientTestIPv6 cluster configs.\nWorkers: " << workers << "\netcdPairs:\n" << etcdPairs;
    }
};

TEST_F(ObjectClientTestIPv6, DISABLED_IPv6_PutGet)
{
    // Simple put and get test, ensuring rpcs are sent okay given worker started with IPv6 format
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(NON_SHM_SIZE);

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), NON_SHM_SIZE,
                             CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers[0]).ImmutableData()), (*buffers[0]).GetSize()));
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(ObjectClientTestIPv6, DISABLED_IPv6_PutGetRemote)
{
    // This is a basically a copy of the EndToEndRemoteGet from the ObjectClientTest version of the testcase.
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    int64_t size = NON_SHM_SIZE;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    std::string data = GenRandomString(size);

    DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    buffer->WLatch();
    buffer->MemoryCopy(data.data(), size);
    buffer->Seal();
    buffer->UnWLatch();

    std::vector<Optional<Buffer>> buffers;
    std::vector<Optional<Buffer>> buffer1;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();

    DS_ASSERT_OK(client1->Get(objectKeys, 0, buffer1));
    ASSERT_TRUE(NotExistsNone(buffer1));
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(buffer1.size(), objectKeys.size());
    ASSERT_EQ(buffer1[0]->GetSize(), size);
    buffer1[0]->RLatch();
    AssertBufferEqual(*buffer1[0], data);
    buffer1[0]->UnRLatch();
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
}
}  // namespace st
}  // namespace datasystem
