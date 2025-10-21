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
 * Description: Ds client test.
 */

#include "common.h"

#include <memory>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/datasystem.h"

namespace datasystem {
namespace st {
class DsClientTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        uint32_t workerNum = 2;
        opts.numWorkers = workerNum;
        opts.numEtcd = 1;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestDsClient(dsClient1_, 0);
        InitTestDsClient(dsClient2_, 1);
    }

    void InitTestDsClient(std::shared_ptr<DsClient> &dsClient, uint32_t workerIndex, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        ConnectOptions opts;
        opts.host = workerAddress.Host();
        opts.port = workerAddress.Port();
        opts.connectTimeoutMs = timeoutMs;
        opts.accessKey = accessKey_;
        opts.secretKey = secretKey_;
        dsClient = std::make_shared<DsClient>(opts);
        DS_ASSERT_OK(dsClient->Init());
    }

    void TearDown() override
    {
        DS_ASSERT_OK(dsClient1_->ShutDown());
        DS_ASSERT_OK(dsClient2_->ShutDown());
        dsClient1_.reset();
        dsClient2_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<DsClient> dsClient1_;
    std::shared_ptr<DsClient> dsClient2_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(DsClientTest, TestObjectClient)
{
    std::shared_ptr<ObjectClient> client1 = dsClient1_->Object();
    std::shared_ptr<ObjectClient> client2 = dsClient2_->Object();

    // client1 Publish
    std::shared_ptr<Buffer> buffer;
    std::string objectKey = "TestObj";
    std::string value = "TestValue";
    std::vector<std::string> failedObjectKeys;
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_OK(client1->Create(objectKey, value.size(), CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy((void *)value.data(), value.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());

    // client2 Get
    std::vector<Optional<Buffer>> getBuffers;
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_OK(client2->Get(objectKeys, 0, getBuffers));
    ASSERT_EQ(getBuffers.size(), objectKeys.size());
    DS_ASSERT_OK(getBuffers[0]->RLatch());
    std::string getValue = std::string(reinterpret_cast<const char *>(getBuffers[0]->ImmutableData()));
    ASSERT_EQ(getValue, value);
    DS_ASSERT_OK(getBuffers[0]->UnRLatch());

    // client1 and client2 release
    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
    getBuffers.clear();
    DS_ASSERT_NOT_OK(client2->Get(objectKeys, 0, getBuffers));
}

TEST_F(DsClientTest, TestKVClient)
{
    std::shared_ptr<KVClient> client1 = dsClient1_->KV();
    std::shared_ptr<KVClient> client2 = dsClient2_->KV();

    std::string setKey = "testKey";
    std::string setValue = "testValue";
    DS_ASSERT_OK(client1->Set(setKey, setValue));

    std::string getValue;
    DS_ASSERT_OK(client2->Get(setKey, getValue));
    ASSERT_EQ(getValue, setValue);

    DS_ASSERT_OK(client2->Del(setKey));
    DS_ASSERT_NOT_OK(client1->Get(setKey, getValue));
}

TEST_F(DsClientTest, TestHeteroClient)
{
    std::shared_ptr<HeteroClient> client1 = dsClient1_->Hetero();
    std::shared_ptr<HeteroClient> client2 = dsClient2_->Hetero();
}
}  // namespace st
}  // namespace datasystem