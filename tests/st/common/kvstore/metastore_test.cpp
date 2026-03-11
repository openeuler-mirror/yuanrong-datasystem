/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Test metastore service through datasystem gRPC server
 */

#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/datasystem.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/net_util.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "etcd/api/mvccpb/kv.pb.h"
#include <grpcpp/grpcpp.h>

using namespace datasystem;

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(cluster_name);
DS_DECLARE_bool(start_metastore_service);
DS_DECLARE_string(metastore_address);

namespace datasystem {
namespace st {

class MetastoreGrpcTest : public ExternalClusterTest {
protected:
    MetastoreGrpcTest()
    {
        metastorePort_ = GetFreePort();
        metastoreAddress_ = "127.0.0.1:" + std::to_string(metastorePort_);
        metastoreListenAddress_ = "0.0.0.0:" + std::to_string(metastorePort_);
    }

    ~MetastoreGrpcTest() = default;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 0;  // Do not start etcd, use metastore instead
        opts.numMasters = 0;  // In object cache mode, master is integrated in worker
        opts.numWorkers = 1;  // Start 1 worker to run metastore as master
        opts.masterIdx = 0;  // Specify the first worker as master
        opts.waitWorkerReady = true;  // Wait for worker to be ready
        // Start metastore service and skip authentication
        opts.workerSpecifyGflagParams[0] = " -start_metastore_service=true -metastore_address=" + metastoreListenAddress_ + " -skip_authenticate=true";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        // Initialize datasystem client
        InitTestDsClient();
    }

    void InitTestDsClient(int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

        ConnectOptions opts;
        opts.host = workerAddress.Host();
        opts.port = workerAddress.Port();
        opts.connectTimeoutMs = timeoutMs;

        dsClient_ = std::make_shared<DsClient>(opts);
        DS_ASSERT_OK(dsClient_->Init());
    }

    std::shared_ptr<DsClient> dsClient_;
    int metastorePort_;
    std::string metastoreAddress_;
    std::string metastoreListenAddress_;
};

TEST_F(MetastoreGrpcTest, TestKVOperationsThroughGrpc)
{
    LOG(INFO) << "Test KV operations through datasystem gRPC server to metastore";

    std::shared_ptr<KVClient> kvClient = dsClient_->KV();
    ASSERT_TRUE(kvClient != nullptr);

    std::string testKey = "metastore_grpc_test_key";
    std::string testValue = "metastore_grpc_test_value";

    // Test Put operation
    LOG(INFO) << "Testing Put operation";
    DS_ASSERT_OK(kvClient->Set(testKey, testValue));

    // Test Get operation
    LOG(INFO) << "Testing Get operation";
    std::string getValue;
    DS_ASSERT_OK(kvClient->Get(testKey, getValue));
    EXPECT_EQ(getValue, testValue);

    // Test Delete operation
    LOG(INFO) << "Testing Delete operation";
    DS_ASSERT_OK(kvClient->Del(testKey));

    // Verify key is deleted
    Status getStatus = kvClient->Get(testKey, getValue);
    EXPECT_FALSE(getStatus.IsOk());
    LOG(INFO) << "KV operations through gRPC to metastore are working";
}

TEST_F(MetastoreGrpcTest, TestObjectOperationsThroughGrpc)
{
    LOG(INFO) << "Test object operations through datasystem gRPC server to metastore";

    std::shared_ptr<ObjectClient> objectClient = dsClient_->Object();
    ASSERT_TRUE(objectClient != nullptr);

    std::string objectKey = "metastore_grpc_test_object";
    std::string objectValue = "Test value for metastore grpc test";
    std::vector<std::string> failedObjectKeys;

    // Test Create and Publish operations
    LOG(INFO) << "Testing Create and Publish operations";
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objectClient->GIncreaseRef({objectKey}, failedObjectKeys));
    DS_ASSERT_OK(objectClient->Create(objectKey, objectValue.size(), CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy((void *)objectValue.data(), objectValue.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());

    // Test Get operation
    LOG(INFO) << "Testing Get operation";
    std::vector<Optional<Buffer>> getBuffers;
    DS_ASSERT_OK(objectClient->GIncreaseRef({objectKey}, failedObjectKeys));
    DS_ASSERT_OK(objectClient->Get({objectKey}, 0, getBuffers));
    ASSERT_EQ(getBuffers.size(), 1);
    DS_ASSERT_OK(getBuffers[0]->RLatch());
    std::string getValue = std::string(reinterpret_cast<const char *>(getBuffers[0]->ImmutableData()));
    ASSERT_EQ(getValue, objectValue);
    DS_ASSERT_OK(getBuffers[0]->UnRLatch());

    // Test Delete operations
    LOG(INFO) << "Testing Delete operations";
    DS_ASSERT_OK(objectClient->GDecreaseRef({objectKey}, failedObjectKeys));
    DS_ASSERT_OK(objectClient->GDecreaseRef({objectKey}, failedObjectKeys));
    getBuffers.clear();

    Status getStatus = objectClient->Get({objectKey}, 0, getBuffers);
    EXPECT_FALSE(getStatus.IsOk());
    LOG(INFO) << "Object operations through gRPC to metastore are working";
}

TEST_F(MetastoreGrpcTest, TestMultipleKeyValuePairs)
{
    LOG(INFO) << "Test multiple key-value operations through datasystem gRPC server to metastore";

    std::shared_ptr<KVClient> kvClient = dsClient_->KV();
    ASSERT_TRUE(kvClient != nullptr);

    std::vector<std::pair<std::string, std::string>> testData = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
        {"key4", "value4"},
        {"key5", "value5"}
    };

    // Test batch Put operations
    LOG(INFO) << "Testing multiple Put operations";
    for (const auto& [key, value] : testData) {
        DS_ASSERT_OK(kvClient->Set(key, value));
    }

    // Test batch Get operations
    LOG(INFO) << "Testing multiple Get operations";
    for (const auto& [key, expectedValue] : testData) {
        std::string actualValue;
        DS_ASSERT_OK(kvClient->Get(key, actualValue));
        EXPECT_EQ(actualValue, expectedValue);
    }

    // Test delete operations
    LOG(INFO) << "Testing Delete operations";
    for (const auto& [key, _] : testData) {
        DS_ASSERT_OK(kvClient->Del(key));
        // Verify key is deleted
        std::string value;
        Status status = kvClient->Get(key, value);
        EXPECT_FALSE(status.IsOk());
    }

    LOG(INFO) << "Multiple key-value operations through gRPC to metastore are working";
}

TEST_F(MetastoreGrpcTest, TestMixedObjectAndKVOperations)
{
    LOG(INFO) << "Test mixed object and KV operations through datasystem gRPC server to metastore";

    std::shared_ptr<KVClient> kvClient = dsClient_->KV();
    std::shared_ptr<ObjectClient> objectClient = dsClient_->Object();
    ASSERT_TRUE(kvClient != nullptr);
    ASSERT_TRUE(objectClient != nullptr);

    std::string kvKey = "mixed_operation_kv_key";
    std::string kvValue = "mixed_operation_kv_value";
    std::string objectKey = "mixed_operation_object_key";
    std::string objectValue = "Test value for mixed operation";
    std::vector<std::string> failedObjectKeys;

    // Mixed operations: first Put KV, then Create Object, then Get both
    LOG(INFO) << "Testing mixed operations";
    DS_ASSERT_OK(kvClient->Set(kvKey, kvValue));

    DS_ASSERT_OK(objectClient->GIncreaseRef({objectKey}, failedObjectKeys));
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objectClient->Create(objectKey, objectValue.size(), CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy((void *)objectValue.data(), objectValue.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());

    // Verify both can be retrieved correctly
    std::string retrievedKvValue;
    DS_ASSERT_OK(kvClient->Get(kvKey, retrievedKvValue));
    EXPECT_EQ(retrievedKvValue, kvValue);

    std::vector<Optional<Buffer>> getBuffers;
    DS_ASSERT_OK(objectClient->GIncreaseRef({objectKey}, failedObjectKeys));
    DS_ASSERT_OK(objectClient->Get({objectKey}, 0, getBuffers));
    ASSERT_EQ(getBuffers.size(), 1);
    DS_ASSERT_OK(getBuffers[0]->RLatch());
    std::string retrievedObjectValue = std::string(reinterpret_cast<const char *>(getBuffers[0]->ImmutableData()));
    EXPECT_EQ(retrievedObjectValue, objectValue);
    DS_ASSERT_OK(getBuffers[0]->UnRLatch());

    // Clean up resources
    DS_ASSERT_OK(kvClient->Del(kvKey));
    DS_ASSERT_OK(objectClient->GDecreaseRef({objectKey}, failedObjectKeys));
    DS_ASSERT_OK(objectClient->GDecreaseRef({objectKey}, failedObjectKeys));

    LOG(INFO) << "Mixed object and KV operations through gRPC to metastore are working";
}


TEST_F(MetastoreGrpcTest, TestClusterOperationsThroughGrpc)
{
    LOG(INFO) << "Test cluster operations through datasystem gRPC server to metastore";

    std::shared_ptr<ObjectClient> objectClient = dsClient_->Object();
    ASSERT_TRUE(objectClient != nullptr);

    std::string objectKey1 = "cluster_test_object_1";
    std::string objectKey2 = "cluster_test_object_2";
    std::string objectValue1 = "First test object";
    std::string objectValue2 = "Second test object";
    std::vector<std::string> failedObjectKeys;

    // Test multiple object operations
    LOG(INFO) << "Testing multiple object operations";
    DS_ASSERT_OK(objectClient->GIncreaseRef({objectKey1, objectKey2}, failedObjectKeys));

    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(objectClient->Create(objectKey1, objectValue1.size(), CreateParam{}, buffer1));
    ASSERT_NE(buffer1, nullptr);
    DS_ASSERT_OK(buffer1->WLatch());
    DS_ASSERT_OK(buffer1->MemoryCopy((void *)objectValue1.data(), objectValue1.size()));
    DS_ASSERT_OK(buffer1->Publish());
    DS_ASSERT_OK(buffer1->UnWLatch());

    std::shared_ptr<Buffer> buffer2;
    DS_ASSERT_OK(objectClient->Create(objectKey2, objectValue2.size(), CreateParam{}, buffer2));
    ASSERT_NE(buffer2, nullptr);
    DS_ASSERT_OK(buffer2->WLatch());
    DS_ASSERT_OK(buffer2->MemoryCopy((void *)objectValue2.data(), objectValue2.size()));
    DS_ASSERT_OK(buffer2->Publish());
    DS_ASSERT_OK(buffer2->UnWLatch());

    // Test getting objects
    std::vector<Optional<Buffer>> getBuffers;
    DS_ASSERT_OK(objectClient->GIncreaseRef({objectKey1, objectKey2}, failedObjectKeys));
    DS_ASSERT_OK(objectClient->Get({objectKey1, objectKey2}, 0, getBuffers));
    ASSERT_EQ(getBuffers.size(), 2);

    DS_ASSERT_OK(getBuffers[0]->RLatch());
    std::string getValue1 = std::string(reinterpret_cast<const char *>(getBuffers[0]->ImmutableData()));
    EXPECT_EQ(getValue1, objectValue1);
    DS_ASSERT_OK(getBuffers[0]->UnRLatch());

    DS_ASSERT_OK(getBuffers[1]->RLatch());
    std::string getValue2 = std::string(reinterpret_cast<const char *>(getBuffers[1]->ImmutableData()));
    EXPECT_EQ(getValue2, objectValue2);
    DS_ASSERT_OK(getBuffers[1]->UnRLatch());

    // Clean up resources
    DS_ASSERT_OK(objectClient->GDecreaseRef({objectKey1, objectKey2}, failedObjectKeys));
    DS_ASSERT_OK(objectClient->GDecreaseRef({objectKey1, objectKey2}, failedObjectKeys));

    LOG(INFO) << "Cluster operations through gRPC to metastore are working";
}

TEST_F(MetastoreGrpcTest, TestRangeQuery)
{
    LOG(INFO) << "Test range query through datasystem gRPC server to metastore";

    std::shared_ptr<KVClient> kvClient = dsClient_->KV();
    ASSERT_TRUE(kvClient != nullptr);

    // Put some keys
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};
    std::vector<std::string> values = {"value1", "value2", "value3", "value4", "value5"};

    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(kvClient->Set(keys[i], values[i]));
        LOG(INFO) << "Put key: " << keys[i] << " with value: " << values[i];
    }

    // Test range query for individual keys
    LOG(INFO) << "Testing individual key queries";
    for (size_t i = 0; i < keys.size(); ++i) {
        std::string getValue;
        DS_ASSERT_OK(kvClient->Get(keys[i], getValue));
        EXPECT_EQ(getValue, values[i]);
        LOG(INFO) << "Successfully retrieved key: " << keys[i] << " with value: " << getValue;
    }

    // Test querying multiple keys at once
    LOG(INFO) << "Testing batch key query";
    std::vector<std::string> batchResult;
    DS_ASSERT_OK(kvClient->Get(keys, batchResult));
    EXPECT_EQ(batchResult.size(), keys.size());
    for (size_t i = 0; i < batchResult.size(); ++i) {
        EXPECT_EQ(batchResult[i], values[i]);
        LOG(INFO) << "Batch query retrieved key: " << keys[i] << " with value: " << batchResult[i];
    }

    LOG(INFO) << "Range query operations through gRPC to metastore are working";
}

TEST_F(MetastoreGrpcTest, TestConcurrentOperations)
{
    LOG(INFO) << "Test concurrent operations through datasystem gRPC server to metastore";

    const int kThreadCount = 5;
    const int kOperationsPerThread = 20;
    std::vector<std::thread> threads;
    std::atomic<int> successfulOperations{0};
    std::atomic<int> failedOperations{0};

    // Run concurrent put/get operations
    for (int i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([this, i, kOperationsPerThread, &successfulOperations, &failedOperations]() {
            std::shared_ptr<KVClient> kvClient = dsClient_->KV();
            if (!kvClient) {
                LOG(ERROR) << "Failed to get KV client in thread " << i;
                failedOperations += kOperationsPerThread;
                return;
            }

            for (int j = 0; j < kOperationsPerThread; ++j) {
                std::string key = "concurrent_key_" + std::to_string(i) + "_" + std::to_string(j);
                std::string value = "concurrent_value_" + std::to_string(i) + "_" + std::to_string(j);

                // Put operation
                Status putStatus = kvClient->Set(key, value);
                if (putStatus.IsOk()) {
                    // Get operation
                    std::string getValue;
                    Status getStatus = kvClient->Get(key, getValue);
                    if (getStatus.IsOk() && getValue == value) {
                        successfulOperations++;
                    } else {
                        failedOperations++;
                        LOG(WARNING) << "Get failed for key " << key << ": " << getStatus.ToString();
                    }
                } else {
                    failedOperations++;
                    LOG(WARNING) << "Put failed for key " << key << ": " << putStatus.ToString();
                }
            }

            LOG(INFO) << "Thread " << i << " completed";
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

    LOG(INFO) << "Concurrent operations complete: " << successfulOperations << " succeeded, " << failedOperations << " failed";
    EXPECT_EQ(failedOperations, 0) << "Expected 0 failed operations, but got " << failedOperations;
    EXPECT_EQ(successfulOperations, kThreadCount * kOperationsPerThread)
        << "Expected " << (kThreadCount * kOperationsPerThread) << " successful operations, but got " << successfulOperations;
}

}  // namespace st
}  // namespace datasystem
