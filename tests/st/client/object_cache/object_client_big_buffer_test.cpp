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
#include <gtest/gtest.h>
#include <memory>
#include <thread>

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/util/uuid_generator.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
class ObjectClientBigBufferTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        int numWorkers = 2;
        opts.numWorkers = numWorkers;
        opts.workerGflagParams = " -shared_memory_size_mb=8192 -v=2 -ipc_through_shared_memory=false";
        opts.masterGflagParams = " -v=1";
        opts.numEtcd = 1;
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

TEST_F(ObjectClientBigBufferTest, EXCLUSIVE_TestPutAndLocalGetBigData)
{
    int64_t size = (int64_t)1024 * 1024 * 1024 * 2;
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string data = GenPartRandomString(size);
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
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    ASSERT_EQ(memcmp(data.data(), buffers[0]->MutableData(), size), 0);
    buffers[0]->UnRLatch();
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_NOT_OK(client->Get(objectKeys, 0, buffers));
}

TEST_F(ObjectClientBigBufferTest, EXCLUSIVE_TestPutAndRemoteGetBigData)
{
    int64_t size = (int64_t)1024 * 1024 * 1024 * 2;
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    std::string data = GenPartRandomString(size);

    DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    buffer->WLatch();
    buffer->MemoryCopy(data.data(), size);
    buffer->UnWLatch();
    buffer->Publish();

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
    ASSERT_EQ(memcmp(data.data(), buffer1[0]->MutableData(), size), 0);
    buffer1[0]->UnRLatch();
}

TEST_F(ObjectClientBigBufferTest, DISABLED_TestPutAndGetBigDatas)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    // small data
    int64_t size1 = 1024;
    std::string data1 = GenPartRandomString(size1);
    std::string objectKey1 = NewObjectKey();
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client->Create(objectKey1, size1, CreateParam{}, buffer1));
    ASSERT_NE(buffer1, nullptr);
    ASSERT_EQ(size1, buffer1.get()->GetSize());
    std::vector<std::string> failedObjectKeys1;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey1 }, failedObjectKeys1));
    buffer1->WLatch();
    buffer1->MemoryCopy((void *)data1.data(), size1);
    buffer1->UnWLatch();
    buffer1->Publish();
    // big data
    int64_t size2 = (int64_t)1024 * 1024 * 1024 * 2;
    std::string data2 = GenPartRandomString(size2);
    std::string objectKey2 = NewObjectKey();
    std::shared_ptr<Buffer> buffer2;
    DS_ASSERT_OK(client->Create(objectKey2, size2, CreateParam{}, buffer2));
    ASSERT_NE(buffer2, nullptr);
    ASSERT_EQ(size2, buffer2.get()->GetSize());
    std::vector<std::string> failedObjectKeys2;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey2 }, failedObjectKeys2));
    buffer2->WLatch();
    buffer2->MemoryCopy((void *)data2.data(), size2);
    buffer2->UnWLatch();
    buffer2->Publish();
    // get all
    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> objectKeys{ objectKey1, objectKey2 };
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data1);
    ASSERT_EQ(memcmp(data2.data(), buffers[1]->MutableData(), size2), 0);
    buffers[0]->UnRLatch();
}
}  // namespace st
}  // namespace datasystem
