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
#include <gtest/gtest.h>
#include <memory>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/inject/inject_point.h"
#include "oc_client_common.h"
#include "zmq_curve_test_common.h"

namespace datasystem {
namespace st {
namespace {
const char *HOST_IP = "127.0.0.1";
constexpr int WORKER_NUM = 2;
constexpr int64_t NON_SHM_SIZE = 499 * 1024;
constexpr int64_t SHM_SIZE = 500 * 1024;
constexpr int64_t BIG_STR_SIZE = 50 * 1024 * 1024;
}  // namespace
class ObjectClientWithTCPTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -shared_memory_size_mb=5120 -v=2 -ipc_through_shared_memory=false";
    }

    void EndToEndSuccess(int64_t size, bool isSeal);

    void EndToEndRemoteGet(int64_t size, bool isSeal);

    void CreateBufferSuccess(int64_t size, CreateParam &param);

    void GetMultiObjectSuccess(int64_t size);

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

// Test create->GIncreaseRef->WLatch->Write->Publish->UnWLatch->Get->RLatch->MutableData->UnRLatch->GDecreaseRef
void ObjectClientWithTCPTest::EndToEndSuccess(int64_t size, bool isSeal)
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
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, buffers));
}

void ObjectClientWithTCPTest::EndToEndRemoteGet(int64_t size, bool isSeal)
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

void ObjectClientWithTCPTest::CreateBufferSuccess(int64_t size, CreateParam &param)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    DS_ASSERT_OK(client->Create(objectKey, size, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer->GetSize());
}

TEST_F(ObjectClientWithTCPTest, EndToEndSuccessWithNonShmSealSuccess)
{
    EndToEndSuccess(NON_SHM_SIZE, true);
}

TEST_F(ObjectClientWithTCPTest, LEVEL1_EndToEndRemoteGetWithNonShmPubSuccess)
{
    EndToEndRemoteGet(NON_SHM_SIZE, false);
}

TEST_F(ObjectClientWithTCPTest, GetAndModifySuccess)
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
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), dataSize);
    auto &bufferGet = buffers[0];
    ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet->ImmutableData()), bufferGet->GetSize()));

    std::string newData = GenRandomString(SHM_SIZE);
    DS_ASSERT_OK(bufferGet->MemoryCopy((void *)newData.data(), newData.size()));
    DS_ASSERT_OK(bufferGet->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers2));
    auto &bufferGet1 = buffers2[0];
    ASSERT_EQ(newData, std::string(reinterpret_cast<const char *>(bufferGet1->ImmutableData()), bufferGet1->GetSize()));
}

/**
 * @brief Two clients create the same Object, both can success.
 */
TEST_F(ObjectClientWithTCPTest, MultiplyClientCreateOkTest)
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
}  // namespace st
}  // namespace datasystem