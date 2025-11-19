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
 * Description:
 */
#include <string>
#include <cstdint>
#include <memory>

#include <gtest/gtest.h>
#include <tbb/concurrent_hash_map.h>

#include "common.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/context/context.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/common/log/log.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
constexpr int64_t NON_SHM_SIZE = 499 * 1024;
constexpr int64_t SHM_SIZE = 500 * 1024;
class ObjectClientTenantTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const uint32_t numWorkers = 2;
        opts.numEtcd = 1;
        opts.numWorkers = numWorkers;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -authorization_enable=true ";
        opts.systemAccessKey = accessKey_;
        opts.systemSecretKey = secretKey_;
    }

protected:
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(ObjectClientTenantTest, TestAuthFailed)
{
    ConnectOptions connectOptions1;
    InitConnectOpt(0, connectOptions1);
    connectOptions1.accessKey = "";
    connectOptions1.secretKey = "";
    auto client1 = std::make_shared<ObjectClient>(connectOptions1);
    DS_EXPECT_NOT_OK(client1->Init());

    ConnectOptions connectOptions2;
    InitConnectOpt(0, connectOptions2);
    connectOptions2.accessKey = accessKey_;
    connectOptions2.secretKey = "fake-sk";
    auto client2 = std::make_shared<ObjectClient>(connectOptions2);
    DS_EXPECT_NOT_OK(client2->Init());
}

TEST_F(ObjectClientTenantTest, TestDiffAuthType)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", "1*return(token1,tenant1)"));
    InitTestClient(0, client1, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    InitTestClient(0, client2,
                   [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "akskTenant"); });

    const size_t size = 1024 * 1024;
    std::string objectKey = "id";
    std::string data1(size, 'A');
    std::string data2(size, 'B');

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failedObjectKeys));

    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));
    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers2));

    AssertBufferEqual(*buffers1[0], data1);
    AssertBufferEqual(*buffers2[0], data2);

    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey }, failedObjectKeys));

    DS_ASSERT_NOT_OK(client1->Get({ objectKey }, 0, buffers1));
    DS_ASSERT_NOT_OK(client2->Get({ objectKey }, 0, buffers2));
}

TEST_F(ObjectClientTenantTest, TestCreateObject)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.auth", "1*return(token1,tenant1)->1*return(token2,tenant2)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "worker.auth", "1*return(token1,tenant1)->1*return(token2,tenant2)"));
    InitTestClient(0, client1, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    InitTestClient(0, client2, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant2"); });

    std::shared_ptr<ObjectClient> client3;
    std::shared_ptr<ObjectClient> client4;
    InitTestClient(1, client3, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    InitTestClient(1, client4, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant2"); });

    const size_t size = 1024 * 1024;
    std::string objectKey = "id";
    std::string data1(size, 'A');
    std::string data2(size, 'B');
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));
    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers2));

    AssertBufferEqual(*buffers1[0], data1);
    AssertBufferEqual(*buffers2[0], data2);

    std::vector<Optional<Buffer>> buffers3;
    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(client3->Get({ objectKey }, 0, buffers3));
    DS_ASSERT_OK(client4->Get({ objectKey }, 0, buffers4));

    AssertBufferEqual(*buffers3[0], data1);
    AssertBufferEqual(*buffers4[0], data2);
}

TEST_F(ObjectClientTenantTest, TestCacheInvalid)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.auth", "1*return(token1,tenant1)->1*return(token2,tenant2)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.auth", "1*return(token2,tenant2)"));
    InitTestClient(0, client1, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    InitTestClient(0, client2, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    InitTestClient(1, client3, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant2"); });

    const size_t size = 1024 * 1024;
    std::string objectKey = "id";
    std::string data1(size, 'A');
    std::string data2(size, 'B');
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), param));
    DS_ASSERT_OK(client3->Put(objectKey, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), param));

    std::vector<Optional<Buffer>> buffers1;
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    DS_ASSERT_OK(client3->Get({ objectKey }, 0, buffers2));

    AssertBufferEqual(*buffers1[0], data1);
    AssertBufferEqual(*buffers2[0], data2);

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), param));
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers3));
    AssertBufferEqual(*buffers3[0], data2);
}

TEST_F(ObjectClientTenantTest, TestUsingDiffAllocator)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.auth", "1*return(token1,tenant1)->1*return(token2,tenant2)"));
    InitTestClient(0, client1, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    InitTestClient(0, client2, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant2"); });

    const size_t size = 40 * 1024 * 1024;  // 40MB
    std::string objectKey1 = "id1";
    std::string objectKey2 = "id2";
    std::string data1(size, 'A');
    std::string data2(size, 'B');
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.AllocateMemory.afterOOM", "return(K_RUNTIME_ERROR)"));

    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1 }, failedObjectKeys));
    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1 }, failedObjectKeys));

    DS_ASSERT_OK(
        client2->Put(objectKey1, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), CreateParam{}));
    DS_ASSERT_NOT_OK(
        client2->Put(objectKey2, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), CreateParam{}));

    DS_ASSERT_OK(
        client1->Put(objectKey2, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));
}

TEST_F(ObjectClientTenantTest, PutAgainInOtherClient)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1,
                   [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "akskTenant"); });
    std::string objectKey = "put-key";
    std::string data = GenRandomString(1024);
    std::shared_ptr<Buffer> dataBuffer;
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    CreateParam param{};

    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.length(), param));
    client1 = nullptr;

    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client2,
                   [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "akskTenant"); });
    std::string data2 = GenRandomString(1024);
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data2.data()), data2.length(), param));
}

TEST_F(ObjectClientTenantTest, TestHealthCheck)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "akskTenant"); });
    DS_ASSERT_OK(client->HealthCheck());
}

class ObjectClientGetMetaTest : public ObjectClientTenantTest {
protected:
    void SetUp() override
    {
        ObjectClientTenantTest::SetUp();
        db_ = InitTestEtcdInstance();
        GetWorkerUuids(db_.get(), uuidMap_);
    }

    std::unique_ptr<EtcdStore> db_;
    std::unordered_map<HostPort, std::string> uuidMap_;
};

TEST_F(ObjectClientGetMetaTest, GetLocations)
{
    std::string tenantId = "user";
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0, [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId); });
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1, [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId); });
    int testCount = 10;
    uint64_t minObjSize = 10;
    int64_t timeoutMs = 5'000;
    std::vector<std::string> objs;
    std::vector<std::string> failedObjectKeys;
    auto factor = 2;
    for (auto i = 0; i < testCount; i++) {
        objs.emplace_back("obj" + std::to_string(i));
        DS_ASSERT_OK(client0->GIncreaseRef({ objs.back() }, failedObjectKeys));
        std::string data(minObjSize + i, 'A');
        DS_ASSERT_OK(
            client0->Put(objs.back(), reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        if (i % factor) {
            std::vector<datasystem::Optional<datasystem::Buffer>> buffers;
            DS_ASSERT_OK(client1->Get({ objs.back() }, timeoutMs, buffers));
        }
    }

    std::vector<ObjMetaInfo> objMetas;
    DS_ASSERT_OK(client0->GetObjMetaInfo(tenantId, objs, objMetas));
    ASSERT_EQ(objMetas.size(), objs.size());
    for (auto i = 0; i < testCount; i++) {
        ASSERT_EQ(objMetas[i].objSize, minObjSize + i);
        if (i % factor) {
            std::vector<std::string> result{ GetWorkerUuid(1, uuidMap_), GetWorkerUuid(0, uuidMap_) };
            ASSERT_EQ(objMetas[i].locations, result);
        } else {
            ASSERT_EQ(objMetas[i].locations, std::vector<std::string>{ GetWorkerUuid(0, uuidMap_) });
        }
    }
}

TEST_F(ObjectClientGetMetaTest, GetFromDifferentTenant)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "root"); });
    // clients authenticated in different ways
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "user1"); });
    std::shared_ptr<ObjectClient> client2;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", "return(token1,user2)"));
    InitTestClient(0, client2, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "user2"); });

    const size_t size1 = 40 * 1024;
    const size_t size2 = 10;
    std::string objectKey1 = "id1";
    std::string objectKey2 = "id2";
    std::string data1(size1, 'A');
    std::string data2(size2, 'B');
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1 }, failedObjectKeys));
    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey1 }, failedObjectKeys));
    DS_ASSERT_OK(
        client2->Put(objectKey2, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), CreateParam{}));

    // get object of user1
    std::vector<ObjMetaInfo> objMetas;
    DS_ASSERT_OK(client0->GetObjMetaInfo("user1", { objectKey1 }, objMetas));
    ASSERT_EQ(objMetas.size(), 1);
    ASSERT_EQ(objMetas[0].objSize, size1);
    ASSERT_EQ(objMetas[0].locations, std::vector<std::string>{ GetWorkerUuid(0, uuidMap_) });
    // get object of user2
    objMetas.clear();
    DS_ASSERT_OK(client0->GetObjMetaInfo("user2", { objectKey2 }, objMetas));
    ASSERT_EQ(objMetas.size(), 1);
    ASSERT_EQ(objMetas[0].objSize, size2);
    ASSERT_EQ(objMetas[0].locations, std::vector<std::string>{ GetWorkerUuid(0, uuidMap_) });
    // get object with wrong tenant
    objMetas.clear();
    DS_ASSERT_OK(client0->GetObjMetaInfo("user1", { objectKey2 }, objMetas));
    ASSERT_EQ(objMetas.size(), 1);
    ASSERT_EQ(objMetas[0].objSize, 0);  // not found, size is 0.
}

TEST_F(ObjectClientGetMetaTest, GetFailed)
{
    std::string tenantId = "user";
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0, [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId); });
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1, [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId); });

    std::string objectKey0;
    std::string objectKey1;
    DS_ASSERT_OK(client0->GenerateKey("", objectKey0));
    DS_ASSERT_OK(client1->GenerateKey("", objectKey1));

    const size_t size0 = 40 * 1024;
    const size_t size1 = 10;
    std::string data0(size0, 'A');
    std::string data1(size1, 'B');
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client0->GIncreaseRef({ objectKey0, objectKey1 }, failedObjectKeys));
    DS_ASSERT_OK(
        client0->Put(objectKey0, reinterpret_cast<const uint8_t *>(data0.data()), data0.size(), CreateParam{}));
    DS_ASSERT_OK(
        client1->Put(objectKey1, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerRemoteMasterOCApi.GetObjectLocations",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    std::vector<ObjMetaInfo> objMetas;
    // success after retry
    DS_ASSERT_OK(client1->GetObjMetaInfo(tenantId, { objectKey0, objectKey1 }, objMetas));
    ASSERT_EQ(objMetas.at(0).objSize, size0);
    ASSERT_EQ(objMetas.at(1).objSize, size1);
    // the only one master failed.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerRemoteMasterOCApi.GetObjectLocations",
                                           "1*return(K_RUNTIME_ERROR)"));
    objMetas.clear();
    auto status = (client1->GetObjMetaInfo(tenantId, { objectKey0 }, objMetas));
    ASSERT_EQ(status.GetCode(), K_RUNTIME_ERROR) << status;
    ASSERT_EQ(objMetas.size(), 0);
    // one master failed, another success
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    objMetas.clear();
    DS_ASSERT_OK(client1->GetObjMetaInfo(tenantId, { objectKey1, objectKey0 }, objMetas));
    ASSERT_EQ(objMetas.at(0).objSize, size1);
    ASSERT_EQ(objMetas.at(1).objSize, 0);
}

class ObjectClientGetMetaNoTenantAuthTest : public ObjectClientGetMetaTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 1;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -authorization_enable=false ";
        opts.systemAccessKey = "";
        opts.systemSecretKey = "";
    }
};

TEST_F(ObjectClientGetMetaNoTenantAuthTest, GetLocations)
{
    std::shared_ptr<ObjectClient> client;
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    connectOptions.accessKey = "";
    connectOptions.secretKey = "";
    client = std::make_shared<ObjectClient>(connectOptions);
    DS_ASSERT_OK(client->Init());

    std::vector<ObjMetaInfo> objMetas;
    DS_ASSERT_OK(client->GetObjMetaInfo("tenant", { "anyId" }, objMetas));
    ASSERT_EQ(objMetas.size(), 1);
}

class ObjectClientTenantSpillTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        rootDir_ = opts.rootDir;
        opts.numEtcd = 1;
        opts.numWorkers = 1;
        opts.workerGflagParams =
            FormatString(" -spill_directory=%s/spill -shared_memory_size_mb=50 -authorization_enable=true ", rootDir_);
        opts.systemAccessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.systemSecretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        LOG(INFO) << "rootDir=" << opts.rootDir;
    }

protected:
    std::string rootDir_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(ObjectClientTenantSpillTest, DISABLED_TestSpill)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.auth", "1*return(token1,tenant1x)->1*return(token3,tenant3x)"));
    InitTestClient(0, client1, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1x"); });
    InitTestClient(0, client1, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1x"); });
    InitTestClient(0, client2,
                   [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "akskTenant"); });
    InitTestClient(0, client3, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant3x"); });
    InitTestClient(0, client3, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant3x"); });

    const size_t size1 = 20 * 1024 * 1024;
    std::string objectKey = "id1";
    std::string data1(size1, 'A');
    std::string data2(size1, 'B');
    const size_t size2 = 40 * 1024 * 1024;
    std::string data3(size2, 'C');

    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));
    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), CreateParam{}));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.AllocateMemory.afterOOM", "return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Evict", "call(0)"));
    DS_ASSERT_NOT_OK(
        client3->Put(objectKey, reinterpret_cast<const uint8_t *>(data3.data()), data3.size(), CreateParam{}));

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    ASSERT_TRUE(FileExist(rootDir_ + "/spill/datasystem_spill_data/tenant1x"));
    ASSERT_TRUE(FileExist(rootDir_ + "/spill/datasystem_spill_data/akskTenant"));

    std::vector<Optional<Buffer>> buffers1;
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers2));

    AssertBufferEqual(*buffers1[0], data1);
    AssertBufferEqual(*buffers2[0], data2);
}

class TenantResourceAutoReleaseTest : public ObjectClientTenantTest {
protected:
    void CreateBufferSuccess(int64_t size, CreateParam &param, const std::string &data);

    std::shared_ptr<ObjectClient> client1_;
    std::shared_ptr<ObjectClient> client2_;
};

void TenantResourceAutoReleaseTest::CreateBufferSuccess(int64_t size, CreateParam &param, const std::string &data)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;

    DS_ASSERT_OK(client1_->Create(objectKey, size, param, buffer));
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1_->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.data(), size));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2_->Get(objectKeys, 0, buffers));
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(client1_->Get(objectKeys, 0, buffers));
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(client1_->GDecreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_NOT_OK(client1_->Get(objectKeys, 0, buffers));
    DS_ASSERT_NOT_OK(client2_->Get(objectKeys, 0, buffers));
}

TEST_F(TenantResourceAutoReleaseTest, DISABLED_TestResourceRelease)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", "return(token1,tenant1)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.auth", "return(token1,tenant1)"));
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(400)");
    InitTestClient(0, client1_, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    InitTestClient(1, client2_, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "PreReleaseTenantResourceInfo.IsExpired", "call(4000)"));

    CreateParam param{};

    std::string data1 = GenRandomString(SHM_SIZE);
    int waitTimeS = 15;
    CreateBufferSuccess(SHM_SIZE, param, data1);
    sleep(waitTimeS);
    std::string data2 = GenRandomString(SHM_SIZE);
    CreateBufferSuccess(SHM_SIZE, param, data2);
    client1_.reset();
    client2_.reset();
}

TEST_F(TenantResourceAutoReleaseTest, TestResourceNotRelease)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", "return(token1,tenant1)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.auth", "return(token1,tenant1)"));
    InitTestClient(0, client1_, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });
    InitTestClient(1, client2_, [this](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, "tenant1"); });

    CreateParam param{};

    int waitTimeS = 1;
    int loopTimes = 5;
    for (int i = 0; i < loopTimes; i++) {
        std::string data = GenRandomString(SHM_SIZE);
        CreateBufferSuccess(SHM_SIZE, param, data);
        sleep(waitTimeS);
    }
    client1_.reset();
    client2_.reset();
}

class ObjectClientWithTenantIdsTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const uint32_t numWorkers = 2;
        opts.numEtcd = 1;
        opts.numWorkers = numWorkers;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -authorization_enable=true ";
        opts.systemAccessKey = accessKey_;
        opts.systemSecretKey = secretKey_;
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }

    void ObjectClientTest(std::shared_ptr<ObjectClient> &client)
    {
        std::string objectKey = "testobj";
        uint64_t size = 500 * 1024;
        std::shared_ptr<Buffer> buffer;
        std::string data = GenRandomString(size);
        DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
        ASSERT_NE(buffer, nullptr);
        ASSERT_EQ(size, buffer.get()->GetSize());
        std::vector<std::string> objectKeys{ objectKey };
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
        buffer->WLatch();
        buffer->MemoryCopy((void *)data.data(), size);
        buffer->Publish();
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
        std::string data1 = "qqq";
        std::string objectKey1 = "testobj";
        DS_ASSERT_OK(
            client->Put(objectKey1, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));
        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers1));
        AssertBufferEqual(*buffers1[0], data1);
    }

protected:
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(ObjectClientWithTenantIdsTest, TestClientWithTenantIds)
{
    std::shared_ptr<ObjectClient> client1;
    std::string tenantId1 = "akskTenant";
    std::string tenantId2 = "tenantId1";
    InitTestClient(0, client1,
                   [this, &tenantId1](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1); });
    std::string objectKey = "id1";
    const size_t size1 = 20 * 1024 * 1024;
    std::string data1(size1, 'A');
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data1.data()), data1.size(), CreateParam{}));
    Context::SetTenantId(tenantId2);
    std::string data2(size1, 'B');
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data2.data()), data2.size(), CreateParam{}));
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    AssertBufferEqual(*buffers1[0], data2);
    Context::SetTenantId(tenantId1);
    buffers1.clear();
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    AssertBufferEqual(*buffers1[0], data1);
}

TEST_F(ObjectClientWithTenantIdsTest, TestCreatePublishWithDiffTenantIds)
{
    std::shared_ptr<ObjectClient> client1;
    std::string tenantId1 = "akskTenant";
    std::string tenantId2 = "tenantId1";
    InitTestClient(0, client1,
                   [this, tenantId1](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1); });
    std::string objectKey = "id1";
    const size_t size1 = 20 * 1024 * 1024;
    std::string data1(size1, 'A');
    std::shared_ptr<Buffer> buffer;
    client1->Create(objectKey, data1.size(), CreateParam{}, buffer);
    Context::SetTenantId(tenantId2);
    DS_ASSERT_OK(buffer->MemoryCopy(data1.data(), data1.size()));
    auto status = buffer->Publish();
    DS_ASSERT_NOT_OK(status);
    ASSERT_TRUE(status.ToString().find("worker shmunit auth check failed") != std::string::npos);
    Context::SetTenantId(tenantId1);
    DS_ASSERT_OK(buffer->MemoryCopy(data1.data(), data1.size()));
    DS_ASSERT_OK(buffer->Publish());
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    AssertBufferEqual(*buffers1[0], data1);
}

TEST_F(ObjectClientWithTenantIdsTest, TestDifferentTenant)
{
    std::shared_ptr<ObjectClient> client1;
    std::string tenantId1 = "akskTenant";
    std::string tenantId2 = "tenantId1";
    InitTestClient(0, client1,
                   [this, tenantId1](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1); });

    std::thread thread1([&client1, this] {
        Context::SetTenantId("tenant2");
        ObjectClientTest(client1);
    });

    std::thread thread2([&client1, this] {
        Context::SetTenantId("tenantId1");
        ObjectClientTest(client1);
    });
    thread1.join();
    thread2.join();
}

TEST_F(ObjectClientWithTenantIdsTest, TestDifferentTenantRef)
{
    FLAGS_v = 2;  // client vlog is 2
    std::shared_ptr<ObjectClient> client1;
    std::string tenantId1 = "akskTenant";
    std::string tenantId2 = "tenantId1";
    InitTestClient(0, client1,
                   [this, tenantId1](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1); });

    Context::SetTenantId("A");
    std::string objectKey = randomData_.GetRandomString(10);
    std::string value = randomData_.GetRandomString(20);
    std::vector<std::string> objectKeys = { objectKey };
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failObjects));
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), CreateParam{}));

    uint64_t timeout = 2;
    std::vector<Optional<Buffer>> buffer;
    Context::SetTenantId("B");
    DS_ASSERT_NOT_OK(client1->Get(objectKeys, timeout, buffer));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failObjects));
    ASSERT_EQ(failObjects.size(), 0);

    Context::SetTenantId("A");
    DS_ASSERT_OK(client1->Get(objectKeys, timeout, buffer));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failObjects));
    ASSERT_EQ(failObjects.size(), 0);
    DS_ASSERT_NOT_OK(client1->Get(objectKeys, timeout, buffer));
}

class ObjectClientWithTenantIdsDfxTest : public ObjectClientTenantTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        ObjectClientTenantTest::SetClusterSetupOptions(opts);
        opts.disableRocksDB = false;
    }
};

TEST_F(ObjectClientWithTenantIdsDfxTest, LEVEL1_TestWorkerRestartTenantRef)
{
    FLAGS_v = 2;  // client vlog is 2
    std::shared_ptr<ObjectClient> client1, client2;
    std::string tenantId1 = "akskTenant";
    std::string tenantId2 = "tenantId1";
    InitTestClient(0, client1,
                   [this, tenantId1](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1); });
    InitTestClient(1, client2,
                   [this, tenantId1](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1); });
    Context::SetTenantId("A");
    std::string objectKey = randomData_.GetRandomString(10);
    std::string value = randomData_.GetRandomString(20);
    std::vector<std::string> objectKeys = { objectKey };
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failObjects));
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), CreateParam{}));
    uint64_t timeout = 2;
    std::vector<Optional<Buffer>> buffer;
    DS_ASSERT_OK(client2->Get(objectKeys, timeout, buffer));
    // Then we crash the worker and try to operate the shm pointer.
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    sleep(1);  // The heartbeat interval is 0.5s, and the maximum number of worker disconnections is 1s.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    sleep(2);                                             // sleep 2s to wait client reconnect.
    ASSERT_EQ(client1->QueryGlobalRefNum(objectKey), 1);  // ref num is 1
    DS_ASSERT_OK(client2->Get(objectKeys, timeout, buffer));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failObjects));
    DS_ASSERT_NOT_OK(client1->Get(objectKeys, timeout, buffer));
}
}  // namespace st
}  // namespace datasystem