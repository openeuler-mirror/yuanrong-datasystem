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
 * Description: This is used to test the ObjectClient class when RDMA is enabled.
 */
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <chrono>

#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/uuid_generator.h"
#include "oc_client_common.h"
#include "zmq_curve_test_common.h"

namespace datasystem {
namespace st {
namespace {
const char *HOST_IP = "127.0.0.1";
constexpr int WORKER_NUM = 3;
const int K_2 = 2, K_5 = 5, K_10 = 10, K_100 = 100;
constexpr int64_t SHM_SIZE = 500 * 1024;
}  // namespace
class RdmaObjectClientTest : public OCClientCommon {
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
        // Enable worker->worker batch get by default.
        opts.workerGflagParams =
            " -shared_memory_size_mb=5120 -v=2 -payload_nocopy_threshold=1000000 -enable_worker_worker_batch_get=true"
            " -batch_get_threshold_mb=20";
#ifdef USE_RDMA
        opts.workerGflagParams += " -arena_per_tenant=1 -enable_rdma=true ";
#else
        opts.workerGflagParams += " -arena_per_tenant=1 -enable_rdma=false ";
#endif
    }

    void SetUp() override
    {
        ImmutableStringPool::Instance().Init();
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

class RdmaObjectClientAuthorizationTest : public RdmaObjectClientTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        RdmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -authorization_enable=true";
        opts.systemAccessKey = accessKey_;
        opts.systemSecretKey = secretKey_;
    }

protected:
    std::string tenantId1_ = "tenant1";
    std::string tenantId2_ = "tenant2";
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

#ifdef USE_RDMA
TEST_F(RdmaObjectClientTest, RdmaPutGetDeleteShmTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), SHM_SIZE, CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(RdmaObjectClientTest, DISABLED_RdmaReconnectTest)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 1024;
    const uint64_t objectSize = 8 * 1024;
    std::vector<std::string> keys(numKV);
    std::vector<StringView> values(numKV);
    std::vector<std::string> valuesForVer(numKV);
    for (int i = 0; i < numKV; i++) {
        keys[i] = "keys_" + std::to_string(i);
        valuesForVer[i] = GenRandomString(objectSize);
        values[i] = StringView(valuesForVer[i]);
    }

    auto checkFunc = [&valuesForVer, &keys](std::vector<std::string> getData) {
        for (size_t i = 0; i < keys.size(); i++) {
            ASSERT_EQ(valuesForVer[i].size(), getData[i].size());
            ASSERT_EQ(valuesForVer[i], getData[i]);
        }
    };

    std::vector<std::string> valuesGet;
    valuesGet.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client1->Set(keys[i], values[i]));
    }
    DS_ASSERT_OK(client2->Get(keys, valuesGet));
    checkFunc(valuesGet);

    cluster_->ShutdownNode(WORKER, 1);
    int32_t sleepSec = 5;
    sleep(sleepSec);
    cluster_->StartNode(WORKER, 1, "");
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    valuesGet.clear();
    valuesGet.reserve(keys.size());
    DS_ASSERT_OK(client2->Get(keys, valuesGet));
    checkFunc(valuesGet);
}

TEST_F(RdmaObjectClientTest, TestRemoteGet)
{
    // Test specifically batch get for 8KB * 1024, so it needs multiple batches when allocating in RDMA case.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 1024;
    const uint64_t objectSize = 8 * 1024;
    std::vector<std::string> keys(numKV);
    std::vector<StringView> values(numKV);
    std::vector<std::string> valuesForVer(numKV);
    for (int i = 0; i < numKV; i++) {
        keys[i] = "keys_" + std::to_string(i);
        valuesForVer[i] = GenRandomString(objectSize);
        values[i] = StringView(valuesForVer[i]);
    }

    std::vector<std::string> valuesGet(keys.size());
    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
        DS_ASSERT_OK(client1->Get(keys[i], valuesGet[i]));
        ASSERT_EQ(valuesForVer[i].size(), valuesGet[i].size());
        ASSERT_EQ(valuesForVer[i], valuesGet[i]);
    }
}

TEST_F(RdmaObjectClientTest, TestBatchRemoteGet1)
{
    // Test that the batch get path in rdma case is working as expected.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 100;
    std::vector<std::string> keys(numKV);
    std::vector<std::string> values(numKV);
    for (int i = 0; i < numKV; i++) {
        keys[i] = "keys_" + std::to_string(i);
        values[i] = "values_" + std::to_string(i);
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(RdmaObjectClientTest, DISABLED_TestBatchRemoteGet2)
{
    // Test specifically batch get for 8KB * 1024, so it needs multiple batches when allocating in RDMA case.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 1024;
    const uint64_t objectSize = 8 * 1024;
    std::vector<std::string> keys(numKV);
    std::vector<StringView> values(numKV);
    std::vector<std::string> valuesForVer(numKV);
    for (int i = 0; i < numKV; i++) {
        keys[i] = "keys_" + std::to_string(i);
        valuesForVer[i] = GenRandomString(objectSize);
        values[i] = StringView(valuesForVer[i]);
    }

    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client2->MSet(keys, values, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(valuesForVer[i], valuesGet[i]);
        ASSERT_EQ(valuesForVer[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(RdmaObjectClientTest, DISABLED_TestBatchRemoteGet3)
{
    // Test that with big objects (>= 1M), the logic batches all the other small objects,
    // and allocates memory separate for the big objects.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 1024;
    const uint64_t smallObjectSize = 8 * 1024;
    std::vector<std::string> keys(numKV);
    std::vector<std::string> values(numKV);
    std::vector<std::string> valuesForVer(numKV);
    const uint64_t bigObjectSize = 1024 * 1024;
    const int bigIndex = 200;
    for (int i = 0; i < numKV; i++) {
        if (i % bigIndex == 0) {
            keys[i] = ("big_keys_" + std::to_string(i));
            values[i] = GenRandomString(bigObjectSize);
        } else {
            keys[i] = ("small_keys_" + std::to_string(i));
            values[i] = GenRandomString(smallObjectSize);
        }
        valuesForVer[i] = values[i];
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(valuesForVer[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(valuesForVer[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(RdmaObjectClientAuthorizationTest, TestBatchRemoteGet4)
{
    // Test that with tenant authorization enabled,
    // the logic still batches the allocation, and the tenant id is selected correctly.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1, [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId2_); });
    InitTestKVClient(1, client2, [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId2_); });

    const int numKV = 1024;
    const uint64_t objectSize = 8 * 1024;
    std::vector<std::string> keys(numKV);
    std::vector<std::string> values(numKV);
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys[i] = "keys_" + std::to_string(i);
        values[i] = GenRandomString(objectSize);
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(RdmaObjectClientTest, TestBatchRemoteGetErrorCode1)
{
    // Test the error handling in rdma batch get logic.
    const int32_t timeout = 1000;
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeout);
    InitTestKVClient(1, client1, timeout);

    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string key3 = "key3";
    std::string key4 = "key4";
    const int64_t size1 = (int64_t)1024;
    std::string value1 = GenPartRandomString(size1);
    std::string value4 = GenPartRandomString(size1);
    std::string valueGet;
    std::vector<std::string> valsGet;
    ASSERT_EQ(client1->Set(key1, value1), Status::OK());
    ASSERT_EQ(client1->Set(key4, value4), Status::OK());

    // TestCase1: single-key scenario, key successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key1 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase2: single-key scenario, key failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key2 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);

    // TestCase3: multi-key scenario, Some keys are successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key2, key4 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase4: multi-key scenario, All keys are failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key2, key3 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(RdmaObjectClientTest, TestBatchRemoteGetErrorCode2)
{
    // Test the error handling in rdma batch get logic.
    // In this case, the worker->worker get is really batched, and error code is injected.
    const int32_t timeout = 1000;
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeout);
    InitTestKVClient(1, client1, timeout);

    std::string key0 = "key0";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string key3 = "key3";
    std::string key4 = "key4";
    const int64_t size1 = (int64_t)1024;
    std::string value0 = GenPartRandomString(size1);
    std::string value1 = GenPartRandomString(size1);
    std::string value2 = GenPartRandomString(size1);
    std::string value3 = GenPartRandomString(size1);
    std::string value4 = GenPartRandomString(size1);
    std::string valueGet;
    std::vector<std::string> valsGet;
    ASSERT_EQ(client1->Set(key0, value0), Status::OK());
    ASSERT_EQ(client1->Set(key1, value1), Status::OK());
    ASSERT_EQ(client1->Set(key2, value2), Status::OK());
    ASSERT_EQ(client1->Set(key3, value3), Status::OK());
    ASSERT_EQ(client1->Set(key4, value4), Status::OK());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.batch_get_failure_for_keys", "return"));

    // TestCase0: single-key scenario, key fails with OOM
    ASSERT_EQ(client0->Get({ key0 }, valsGet).GetCode(), StatusCode::K_OUT_OF_MEMORY);

    // TestCase0.1: multi-key scenario, Some keys are successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key1, key0 }, valsGet).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(client0->Get({ key0, key1 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase1: multi-key scenario, Some keys are successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key1, key2 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase2: Same as case 1, but the order changes.
    ASSERT_EQ(client0->Get({ key2, key4 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase3: multi-key scenario, All keys are failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key2, key3 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);

    // TestCase4: multi-key scenario, All keys are failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key3, key2 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(RdmaObjectClientTest, TestBatchGetSplitPayload)
{
    std::shared_ptr<ObjectClient> client1, client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const uint64_t KB = 1024;
    const uint64_t MB_10 = K_10 * KB * KB;
    std::string data(MB_10, 'a');
    std::vector<std::string> objectKeys;
    for (int i = 0; i < K_100; i++) {
        const std::string objectKey = NewObjectKey();
        DS_ASSERT_OK(
            client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        objectKeys.push_back(objectKey);
    }

    std::vector<Optional<Buffer>> buffer;
    // Remote Get
    DS_ASSERT_OK(client2->Get(objectKeys, 0, buffer));
    ASSERT_EQ(buffer[0]->GetSize(), MB_10);
    ASSERT_EQ(memcmp(data.data(), buffer[0]->MutableData(), MB_10), 0);
    // Local Get
    buffer.clear();
    DS_ASSERT_OK(client2->Get(objectKeys, 0, buffer));
    ASSERT_EQ(buffer[0]->GetSize(), MB_10);
    ASSERT_EQ(memcmp(data.data(), buffer[0]->MutableData(), MB_10), 0);
}

TEST_F(RdmaObjectClientTest, RdmaRemoteGetSmall)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    const int64_t num_objects = 20;
    for (int i = 0; i < num_objects; i++) {
        std::string objectKey = NewObjectKey();
        std::string data(size, 'a');

        DS_ASSERT_OK(
            client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        auto start_time = std::chrono::high_resolution_clock::now();
        DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Time take " << duration.count() << " ms";
        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    }
    LOG(INFO) << "Test case RdmaRemoteGetSmall success";
}

TEST_F(RdmaObjectClientTest, RdmaRemoteGetBig)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const uint64_t KB = 1024;
    const uint64_t GB = KB * KB * KB;
    const uint64_t size = 2 * GB + 1 * KB;
    const std::string objectKey = NewObjectKey();
    const std::string data = GenRandomString(size);

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));

    ASSERT_EQ(buffers1[0]->GetSize(), size);
    ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
}

TEST_F(RdmaObjectClientTest, RdmaPutAndRemoteGetTest)
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
        DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), size, CreateParam{}));
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

TEST_F(RdmaObjectClientTest, RdmaRemoteGetTwoSmallParallel)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    const int K_2 = 2;
    ThreadPool threadPool(K_2);
    auto fut = threadPool.Submit([client1, client2, objectKey1]() {
        const int64_t size = 1024;
        std::string data(size, 'a');
        DS_ASSERT_OK(
            client2->Put(objectKey1, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(client1->Get({ objectKey1 }, 0, buffers1));

        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    });
    auto fut2 = threadPool.Submit([client1, client2, objectKey2]() {
        const int64_t size = 1024;
        std::string data(size, 'b');
        DS_ASSERT_OK(
            client2->Put(objectKey2, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(client1->Get({ objectKey2 }, 0, buffers1));

        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    });
    fut.get();
    fut2.get();
    LOG(INFO) << "Test case RdmaRemoteGetTwoSmallParallel success";
}

TEST_F(RdmaObjectClientTest, RdmaRemoteGetSizeChanged)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOcServiceGetImpl.PrepareGetRequestHelper.changeSize",
                                           "1*call(1023)"));
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));

    ASSERT_EQ(buffers1[0]->GetSize(), size);
    ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    LOG(INFO) << "Test case RdmaRemoteGetSmall success";
}

TEST_F(RdmaObjectClientTest, RdmaRemoteBatchGetSizeChanged)
{
    // Test that with batch get, a batch of failure due to size change can be retried automatically.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOcServiceGetImpl.PrepareGetRequestHelper.changeSize",
                                           "10*call(1023)"));
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 100;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back("values_" + std::to_string(i));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(RdmaObjectClientTest, RdmaRemoteGetSizeChangedInvalid)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOcServiceGetImpl.PrepareGetRequestHelper.changeSize",
                                           "1*call(1023)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerWorkerOCServiceImpl.GetObjectRemoteImpl.changeDataSize",
                                           "1*call(0)"));
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_NOT_OK(client1->Get({ objectKey }, 0, buffers1));

    LOG(INFO) << "Test case RdmaRemoteGetSizeChangedInvalid success";
}

TEST_F(RdmaObjectClientTest, RdmaParallelWrite)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    const int64_t num_objects = 100;
    std::vector<std::string> objectKeys;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');
    for (int i = 0; i < num_objects; i++) {
        DS_ASSERT_OK(
            client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        objectKeys.push_back(objectKey);
    }

    std::vector<std::string> batch;
    std::vector<Optional<Buffer>> buf;
    const int batchSize = 10;
    auto total_time = std::chrono::high_resolution_clock::duration::zero();
    for (int i = 0; i < num_objects; i++) {
        batch.push_back(objectKeys[i]);
        if (((i + 1) % batchSize) == 0) {
            auto start_time = std::chrono::high_resolution_clock::now();
            DS_ASSERT_OK(client1->Get(batch, 0, buf));
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = end_time - start_time;
            total_time += duration;
            for (size_t j = 0; j < batch.size(); ++j) {
                CHECK_EQ(data, std::string(reinterpret_cast<const char *>(buf[j]->ImmutableData()), buf[j]->GetSize()));
            }
        }
    }
    auto total_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(total_time);
    LOG(INFO) << "Average Time per Get req: " << total_time_ms.count() / batchSize << " ms";
}

TEST_F(RdmaObjectClientTest, RdmaGetMultipleWorkers)
{
    std::shared_ptr<ObjectClient> client1, client2, client3;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestClient(K_2, client3);
    const int64_t size = 1024;
    const int64_t num_objects = 5;
    std::string data(size, 'a');
    std::vector<std::string> objectIds;
    for (int i = 0; i < num_objects; i++) {
        std::string objectId = NewObjectKey();
        std::string objectId1 = NewObjectKey();
        DS_ASSERT_OK(
            client2->Put(objectId, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        DS_ASSERT_OK(
            client3->Put(objectId1, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        objectIds.push_back(objectId);
        objectIds.push_back(objectId1);
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectIds }, 0, buffers1));
    for (uint64_t i = 0; i < buffers1.size(); i++) {
        ASSERT_EQ(buffers1[i]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[i]->MutableData(), size), 0);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG(INFO) << "Time take " << duration.count() << " ms";
    LOG(INFO) << "Testcase RdmaGetMultipleWorkers Finished";
}

class RdmaObjectClientTestMismatch : public RdmaObjectClientTest {
public:
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
        // Enable worker->worker batch get by default.
        opts.workerGflagParams =
            " -shared_memory_size_mb=5120 -v=2 -payload_nocopy_threshold=1000000 -enable_worker_worker_batch_get=true "
            "-arena_per_tenant=1";
        // Specify mismatching enable_rdma setting for the workers.
        opts.workerSpecifyGflagParams[0] += " -enable_rdma=true ";
        opts.workerSpecifyGflagParams[1] += " -enable_rdma=false ";
    }
};

TEST_F(RdmaObjectClientTestMismatch, RdmaRemoteGetDirection1)
{
    // Test that with mismatching enable_rdma setting, remote get can still go through.
    // Worker1 with enable_rdma=true tries to remote get from worker2 with enable_rdma=false,
    // worker1 will handshake to exchange jfr, pre-allocate memory and prepare Rdma info,
    // but the data will be sent back at payload, and all are fine.
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 10 * 1024 * 1024;
    const int64_t num_objects = 20;
    for (int i = 0; i < num_objects; i++) {
        std::string objectKey = NewObjectKey();
        std::string data(size, 'a');

        DS_ASSERT_OK(
            client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        auto start_time = std::chrono::high_resolution_clock::now();
        DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Time take " << duration.count() << " ms";
        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    }
}

TEST_F(RdmaObjectClientTestMismatch, RdmaRemoteGetDirection2)
{
    // Test that with mismatching enable_rdma setting, remote get can still go through.
    // Worker2 with enable_rdma=false tries to remote get from worker1 with enable_rdma=true,
    // worker2 will not prepare anything Rdma related, so Rdma_info will be empty,
    // so then worker1 will fallback to non-Rdma logic, and all are fine.
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 10 * 1024 * 1024;
    const int64_t num_objects = 20;
    for (int i = 0; i < num_objects; i++) {
        std::string objectKey = NewObjectKey();
        std::string data(size, 'a');

        DS_ASSERT_OK(
            client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        auto start_time = std::chrono::high_resolution_clock::now();
        DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers1));
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Time take " << duration.count() << " ms";
        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    }
    LOG(INFO) << "Test case RdmaRemoteGetSmall success";
}
#endif
}  // namespace st
}  // namespace datasystem