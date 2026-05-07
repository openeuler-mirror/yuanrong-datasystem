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
 * Description:
 */
#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
namespace {
constexpr int64_t NON_SHM_SIZE = 499 * 1024;
constexpr int64_t SHM_SIZE = 500 * 1024;
}  // namespace

class OCClientDistMasterTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
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
        db_ = InitTestEtcdInstance();
        StartClustersAndWaitReady();
        GetWorkerUuids(db_.get(), uuidMap_);
        // Wait for hash ring ready
        uint64_t waitSecs = 2;
        sleep(waitSecs);
    }

    void TearDown() override
    {
        objClient0_.reset();
        objClient1_.reset();
        db_.reset();
        ExternalClusterTest::TearDown();
    }

    void StartClustersAndWaitReady()
    {
        DS_ASSERT_OK(cluster_->StartWorkers());
        DS_ASSERT_OK(cluster_->WaitUntilClusterReadyOrTimeout(30));
        int maxWaitTimeSec = 30;
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0, maxWaitTimeSec));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1, maxWaitTimeSec));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2, maxWaitTimeSec));
    }

    int CalcGRefNumOnWorker(std::string objectKey, std::shared_ptr<ObjectClient> client)
    {
        return client->QueryGlobalRefNum(objectKey);
    }
    void EndToEndSuccess(int64_t size, bool isSeal);
    void EndToEndRemoteGet(int64_t size, bool isSeal);
    void InitObjectClients()
    {
        InitTestClient(0, objClient0_);
        InitTestClient(1, objClient1_);
    }

protected:
    std::unique_ptr<EtcdStore> db_;
    std::unordered_map<HostPort, std::string> uuidMap_;
    std::shared_ptr<ObjectClient> objClient0_;
    std::shared_ptr<ObjectClient> objClient1_;
};

/*
 End to End tests
*/

// Test create->WLatch->Write->Publish->UnWLatch->Get->RLatch->MutableData->UnRLatch->Delete
void OCClientDistMasterTest::EndToEndSuccess(int64_t size, bool isSeal)
{
    std::string objectKey = ObjectKeyWithOwner(0, uuidMap_);
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(1, client);
    std::string data = GenRandomString(size);
    DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer.get()->GetSize());

    buffer->WLatch();
    buffer->MemoryCopy((void *)data.data(), size);
    if (isSeal) {
        buffer->Seal();
    } else {
        buffer->Publish();
    }
    buffer->UnWLatch();

    std::vector<Optional<Buffer>> buffers;
    LOG(INFO) << "Object key is : " << objectKey;
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();
}

void OCClientDistMasterTest::EndToEndRemoteGet(int64_t size, bool isSeal)
{
    std::string objectKey = ObjectKeyWithOwner(2, uuidMap_);
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    std::string data = GenRandomString(size);

    DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer.get()->GetSize());

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
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    AssertBufferEqual(*buffers[0], data);
    buffers[0]->UnRLatch();

    DS_ASSERT_OK(client1->Get(objectKeys, 0, buffer1));
    ASSERT_EQ(buffer1.size(), objectKeys.size());
    ASSERT_EQ(buffer1[0]->GetSize(), size);
    buffer1[0]->RLatch();
    AssertBufferEqual(*buffer1[0], data);
    buffer1[0]->UnRLatch();
}

TEST_F(OCClientDistMasterTest, EndToEndSuccessWithShmSealSuccess)
{
    EndToEndSuccess(SHM_SIZE, true);
}

TEST_F(OCClientDistMasterTest, EndToEndSuccessWithShmPubSuccess)
{
    EndToEndSuccess(SHM_SIZE, false);
}

TEST_F(OCClientDistMasterTest, EndToEndSuccessWithNonShmSealSuccess)
{
    EndToEndSuccess(NON_SHM_SIZE, true);
}

TEST_F(OCClientDistMasterTest, EndToEndSuccessWithNonShmPubSuccess)
{
    EndToEndSuccess(NON_SHM_SIZE, false);
}

TEST_F(OCClientDistMasterTest, EndToEndRemoteGetWithShmSealSuccess)
{
    EndToEndRemoteGet(SHM_SIZE, true);
}

TEST_F(OCClientDistMasterTest, EndToEndRemoteGetWithShmPubSuccess)
{
    EndToEndRemoteGet(SHM_SIZE, false);
}

TEST_F(OCClientDistMasterTest, EndToEndRemoteGetWithNonShmSealSuccess)
{
    EndToEndRemoteGet(NON_SHM_SIZE, true);
}

TEST_F(OCClientDistMasterTest, LEVEL1_EndToEndRemoteGetWithNonShmPubSuccess)
{
    EndToEndRemoteGet(NON_SHM_SIZE, false);
}

/*
 Testing Create and Remote Get
*/

TEST_F(OCClientDistMasterTest, GetRemoteTest)
{
    // Store two objects locally on Node 0
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::string obj1Id = ObjectKeyWithOwner(0, uuidMap_);
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(cliLocal, obj1Id, data);

    // Remote GET immutable object from a client in Node 1
    LOG(INFO) << "cliRemote starts to get immutable object";
    std::shared_ptr<ObjectClient> cliRemote;
    InitTestClient(1, cliRemote);
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliRemote->Get({ obj1Id }, 0, dataList));

    // Check if the Object Data is correct
    ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
    AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });

    // Remote GET mutable object from a client in Node 1
    LOG(INFO) << "cliRemote starts to get mutable object";
    std::string notSealObjKey = ObjectKeyWithOwner(0, uuidMap_);
    CreateAndPublishObject(cliLocal, notSealObjKey, data);
    std::vector<Optional<Buffer>> dataListNotSeal;
    DS_ASSERT_OK(cliRemote->Get({ notSealObjKey }, 0, dataListNotSeal));
}

TEST_F(OCClientDistMasterTest, GetRemoteTestIDNotExists)
{
    // Do remote GET from a client in Node 1
    std::shared_ptr<ObjectClient> cliRemote;
    InitTestClient(1, cliRemote);

    // If ObjectKey doesn't exist (without timeout)
    std::string notExistObjKey = ObjectKeyWithOwner(2, uuidMap_);
    std::vector<Optional<Buffer>> dataListNotExist(1);
    DS_ASSERT_NOT_OK(cliRemote->Get({ notExistObjKey }, 0, dataListNotExist));

    // get not exist id data with timeout
    Timer timer;
    DS_ASSERT_NOT_OK(cliRemote->Get({ notExistObjKey }, 1000, dataListNotExist));
    auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cast: " << timeCost;
    ASSERT_TRUE(timeCost >= 900 && timeCost < 1000);
}

TEST_F(OCClientDistMasterTest, GetRemoteSameIdTest)
{
    // Store two objects locally on Node 0
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::string obj1Id = ObjectKeyWithOwner(0, uuidMap_);
    std::string obj2Id = ObjectKeyWithOwner(0, uuidMap_);
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliLocal, obj2Id, data2);

    // Do remote GET from a client in Node 1
    std::shared_ptr<ObjectClient> cliRemote;
    InitTestClient(1, cliRemote);
    std::vector<std::string> getObjList = { obj1Id, obj1Id, obj2Id };
    LOG(INFO) << "cliRemote starts to get object";
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliRemote->Get(getObjList, 0, dataList));

    // Check if the Object Data is correct
    AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
    ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
    AssertBufferEqual(*dataList[1], std::string{ 65, 66, 67, 68, 69, 70 });
    ASSERT_NE(dataList[2]->ImmutableData(), nullptr);
    AssertBufferEqual(*dataList[2], std::string{ 90, 91, 92, 93, 94 });
    LOG(INFO) << "cliRemote ends to get object";
}

TEST_F(OCClientDistMasterTest, EXCLUSIVE_GetRemoteTimeoutTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cli1Remote;
    InitTestClient(1, cli1Remote);
    std::shared_ptr<ObjectClient> cli2Remote;
    InitTestClient(2, cli2Remote);

    // get not exist data, but create by local other client during timeout
    // get not exist data, but create by local other client after timeout
    ThreadPool threadPool(2);
    std::string objectKeyDelayCreate = ObjectKeyWithOwner(0, uuidMap_);
    std::vector<uint8_t> dataDelayCreate = { 65, 66, 67, 68, 69, 70 };
    auto fut1 = threadPool.Submit([objectKeyDelayCreate, cli1Remote]() {
        LOG(INFO) << "cli2Local starts to get object";
        std::vector<Optional<Buffer>> dataList;
        Timer timer;
        DS_ASSERT_NOT_OK(cli1Remote->Get({ objectKeyDelayCreate }, 500, dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cast: " << timeCost;
        ASSERT_TRUE(timeCost > 450 && timeCost < 550);
        LOG(INFO) << "cli1Remote ends to get object";
    });

    auto fut2 = threadPool.Submit([objectKeyDelayCreate, cli2Remote, this]() {
        LOG(INFO) << "cli3Local starts to get object";
        std::vector<Optional<Buffer>> dataList;
        Timer timer;
        DS_EXPECT_OK(cli2Remote->Get({ objectKeyDelayCreate }, 3000, dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cast: " << timeCost;
        ASSERT_TRUE(timeCost > 1000 && timeCost < 3000);
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        LOG(INFO) << "cli2Remote ends to get object";
    });

    auto fut3 = threadPool.Submit([objectKeyDelayCreate, &dataDelayCreate, cliLocal, this]() {
        LOG(INFO) << "cliLocal starts to create object";
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        CreateAndSealObject(cliLocal, objectKeyDelayCreate, dataDelayCreate);
        LOG(INFO) << "cliLocal ends to create object";
    });

    fut1.get();
    fut2.get();
    fut3.get();
}

TEST_F(OCClientDistMasterTest, GetRemoteMultipleClientsToSingleLocalWorkerTest)
{
    std::shared_ptr<ObjectClient> cli1Local;
    InitTestClient(0, cli1Local);
    std::shared_ptr<ObjectClient> cli2Remote;
    InitTestClient(1, cli2Remote);
    std::shared_ptr<ObjectClient> cli3Remote;
    InitTestClient(2, cli3Remote);

    std::string obj1Id = ObjectKeyWithOwner(0, uuidMap_);
    std::string obj2Id = ObjectKeyWithOwner(0, uuidMap_);
    std::string objNotCreate = ObjectKeyWithOwner(0, uuidMap_);
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    CreateAndSealObject(cli1Local, obj1Id, data1);
    CreateAndSealObject(cli1Local, obj2Id, data2);

    ThreadPool threadPool(2);
    std::vector<std::string> getObjListExist = { obj1Id, obj2Id };
    std::vector<std::string> getObjListPartialExist = { obj1Id, obj2Id, objNotCreate };
    auto fut1 = threadPool.Submit([getObjListExist, cli2Remote, data1, data2, this]() {
        LOG(INFO) << "cli2Remote starts to get object";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cli2Remote->Get(getObjListExist, 1000, dataList));

        // Check the size and contents
        ASSERT_EQ(dataList[0]->GetSize(), static_cast<int>(data1.size()));
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_EQ(dataList[1]->GetSize(), static_cast<int>(data2.size()));
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        LOG(INFO) << "cli2Remote ends to get object";
    });

    auto fut2 = threadPool.Submit([getObjListPartialExist, cli3Remote, data1, data2, this]() {
        LOG(INFO) << "cli3Remote starts to get object";
        std::vector<Optional<Buffer>> dataList;
        Timer timer;
        DS_ASSERT_OK(cli3Remote->Get(getObjListPartialExist, 1000, dataList));
        ASSERT_TRUE(ExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        ASSERT_FALSE(dataList[2]);

        // Check timeout
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cost: " << timeCost;
        ASSERT_TRUE(timeCost > 900 && timeCost < 1100);

        // Check the size and contents
        ASSERT_EQ(dataList[0]->GetSize(), static_cast<int>(data1.size()));
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_EQ(dataList[1]->GetSize(), static_cast<int>(data2.size()));
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        ASSERT_FALSE(dataList[2]);
        LOG(INFO) << "cli3Remote ends to get object";
    });

    fut1.get();
    fut2.get();
}

TEST_F(OCClientDistMasterTest, GetRemoteSingleClientToSingleWorkerConcurrentlyTest)
{
    std::shared_ptr<ObjectClient> cliRemote;
    InitTestClient(1, cliRemote);

    std::string obj1Id = ObjectKeyWithOwner(1, uuidMap_);
    std::string obj2Id = ObjectKeyWithOwner(1, uuidMap_);
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    CreateAndSealObject(cliRemote, obj1Id, data1);
    CreateAndSealObject(cliRemote, obj2Id, data2);

    int threadNum = 20;
    std::vector<std::thread> clientThreads(threadNum);
    std::vector<std::string> getObjList = { obj1Id, obj2Id };
    for (int i = 0; i < threadNum; ++i) {
        std::shared_ptr<ObjectClient> cliLocal;
        InitTestClient(0, cliLocal);
        clientThreads[i] = std::thread([i, getObjList, cliLocal, this]() {
            LOG(INFO) << "client: " << i << " starts to get object";
            std::vector<Optional<Buffer>> dataList;
            DS_ASSERT_OK(cliLocal->Get(getObjList, 2000, dataList));
            ASSERT_TRUE(dataList[0]);
            AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
            ASSERT_TRUE(dataList[1]);
            AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
            LOG(INFO) << "client: " << i << " ends to get object";
        });
    }

    for (auto &t : clientThreads) {
        t.join();
    }
    LOG(INFO) << "end client test.";
}

TEST_F(OCClientDistMasterTest, GetRemoteMultipleClientsToSingleWorkerTest)
{
    std::shared_ptr<ObjectClient> cli1Local1;
    InitTestClient(0, cli1Local1);
    std::shared_ptr<ObjectClient> cli2Local1;
    InitTestClient(0, cli2Local1);
    std::shared_ptr<ObjectClient> cliLocal2;
    InitTestClient(1, cliLocal2);
    std::shared_ptr<ObjectClient> cliRemote;
    InitTestClient(2, cliRemote);

    std::string obj1Id = ObjectKeyWithOwner(2, uuidMap_);
    std::string obj2Id = ObjectKeyWithOwner(2, uuidMap_);
    std::string obj3Id = ObjectKeyWithOwner(2, uuidMap_);
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    CreateAndSealObject(cliRemote, obj1Id, data1);
    CreateAndSealObject(cliRemote, obj2Id, data2);

    ThreadPool threadPool(3);
    std::vector<std::string> getObjList = { obj1Id, obj2Id };
    // get data from local1 client1 to remote without timeout
    auto fut1 = threadPool.Submit([getObjList, cli1Local1, this]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cli1Local1->Get(getObjList, 0, dataList));
        ASSERT_TRUE(dataList[0]);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_TRUE(dataList[1]);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
    });

    // get data from local1 client2 to remote
    auto fut2 = threadPool.Submit([getObjList, cli2Local1, this]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cli2Local1->Get(getObjList, 1000, dataList));
        ASSERT_TRUE(dataList[0]);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_TRUE(dataList[1]);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
    });

    // get data from local2 to remote
    std::vector<std::string> getObjList2 = { obj1Id, obj2Id, obj3Id };
    auto fut3 = threadPool.Submit([getObjList2, cliLocal2, this]() {
        LOG(INFO) << "cliLocal2 starts to get object";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal2->Get(getObjList2, 0, dataList));
        ASSERT_TRUE(dataList[0]);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_TRUE(dataList[1]);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        ASSERT_FALSE(dataList[2]);
        LOG(INFO) << "cliLocal2 ends to get object";
    });

    fut1.get();
    fut2.get();
    fut3.get();
}

TEST_F(OCClientDistMasterTest, GetRemoteSingleClientToMultipleWorkersTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cliRemote1;
    InitTestClient(1, cliRemote1);
    std::shared_ptr<ObjectClient> cliRemote2;
    InitTestClient(2, cliRemote2);

    std::string obj1Id = ObjectKeyWithOwner(0, uuidMap_);
    std::string obj2Id = ObjectKeyWithOwner(1, uuidMap_);
    std::string obj3Id = ObjectKeyWithOwner(2, uuidMap_);

    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    std::vector<uint8_t> data3 = { 80, 81, 82, 83 };

    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliRemote1, obj2Id, data2);
    CreateAndSealObject(cliRemote2, obj3Id, data3);

    std::vector<std::string> getObjList = { obj1Id, obj2Id, obj3Id };

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliLocal->Get(getObjList, 0, dataList));

    // Check the data
    ASSERT_TRUE(dataList[0]);
    AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
    ASSERT_TRUE(dataList[1]);
    AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
    ASSERT_TRUE(dataList[2]);
    AssertBufferEqual(*dataList[2], std::string{ 80, 81, 82, 83 });
}

TEST_F(OCClientDistMasterTest, DISABLED_GetRemoteSingleClientToMultipleWorkersDelayCreateTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cliRemote1;
    InitTestClient(1, cliRemote1);
    std::shared_ptr<ObjectClient> cliRemote2;
    InitTestClient(2, cliRemote2);

    std::string obj1Id = ObjectKeyWithOwner(0, uuidMap_);
    std::string obj2Id = ObjectKeyWithOwner(1, uuidMap_);
    std::string obj3Id = ObjectKeyWithOwner(2, uuidMap_);
    std::string obj4Id = ObjectKeyWithOwner(2, uuidMap_);
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    std::vector<uint8_t> data3 = { 80, 81, 82, 83 };
    std::vector<uint8_t> data4 = { 70, 71, 72 };
    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliRemote1, obj2Id, data2);
    CreateAndSealObject(cliRemote2, obj3Id, data3);

    ThreadPool threadPool(2);
    std::vector<std::string> getObjList = { obj1Id, obj2Id, obj3Id, obj4Id };
    // get data from local to multiple remote
    auto fut1 = threadPool.Submit([getObjList, cliLocal, this]() {
        LOG(INFO) << "cliLocal starts to get object at first time.";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal->Get(getObjList, 0, dataList));
        ASSERT_TRUE(dataList[0]);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_TRUE(dataList[1]);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        ASSERT_TRUE(dataList[2]);
        AssertBufferEqual(*dataList[2], std::string{ 80, 81, 82, 83 });
        ASSERT_FALSE(dataList[3]);
        LOG(INFO) << "cliLocal ends to get object at first time.";

        LOG(INFO) << "cliLocal starts to get object at second time.";
        Timer timer;
        dataList.clear();
        DS_ASSERT_OK(cliLocal->Get(getObjList, 1500, dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        ASSERT_TRUE(timeCost < 1500);
        ASSERT_TRUE(dataList[3]);
        AssertBufferEqual(*dataList[3], std::string{ 70, 71, 72 });
        LOG(INFO) << "cliLocal ends to get object at second time.";
    });

    // create data in remote node after 1000 milliseconds
    auto fut2 = threadPool.Submit([getObjList, obj4Id, &data4, cliRemote2, this]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        CreateAndSealObject(cliRemote2, obj4Id, data4);
    });

    fut1.get();
    fut2.get();
}

TEST_F(OCClientDistMasterTest, EXCLUSIVE_GetRemoteTimeOutDelayCreateTest)
{
    std::shared_ptr<ObjectClient> cliLocal, cliLocal2, cliLocal3, cliLocal4, cliRemote1, cliRemote2;
    InitTestClient(0, cliLocal);
    InitTestClient(0, cliLocal2);
    InitTestClient(0, cliLocal3);
    InitTestClient(0, cliLocal4);

    InitTestClient(1, cliRemote1);
    InitTestClient(2, cliRemote2);

    std::string obj1Id = ObjectKeyWithOwner(1, uuidMap_);
    std::string obj2Id = ObjectKeyWithOwner(1, uuidMap_);
    std::string obj3Id = ObjectKeyWithOwner(1, uuidMap_);
    std::string obj4Id = ObjectKeyWithOwner(2, uuidMap_);
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    std::vector<uint8_t> data3 = { 80, 81, 82, 83 };
    std::vector<uint8_t> data4 = { 70, 71, 72 };
    CreateAndSealObject(cliRemote1, obj1Id, data1);
    CreateAndSealObject(cliRemote1, obj2Id, data2);
    CreateAndSealObject(cliRemote1, obj3Id, data3);

    ThreadPool threadPool(5);
    std::vector<std::string> getObjList = { obj1Id, obj2Id, obj3Id };
    // get data from local to multiple remote
    auto fut1 = threadPool.Submit([getObjList, cliLocal, this]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal->Get(getObjList, 0, dataList));
        ASSERT_TRUE(dataList[0]);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_TRUE(dataList[1]);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        ASSERT_TRUE(dataList[2]);
        AssertBufferEqual(*dataList[2], std::string{ 80, 81, 82, 83 });
    });

    std::vector<std::string> getObjList2 = { obj3Id, obj1Id, obj2Id };
    auto fut2 = threadPool.Submit([getObjList2, cliLocal2, this]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal2->Get(getObjList2, 0, dataList));
        ASSERT_TRUE(dataList[0]);
        AssertBufferEqual(*dataList[0], std::string{ 80, 81, 82, 83 });
        ASSERT_TRUE(dataList[1]);
        AssertBufferEqual(*dataList[1], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_TRUE(dataList[2]);
        AssertBufferEqual(*dataList[2], std::string{ 90, 91, 92, 93, 94 });
    });

    std::vector<std::string> getObjList3 = { obj3Id, obj4Id, obj1Id, obj2Id };
    auto fut3 = threadPool.Submit([getObjList3, cliLocal3, this]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal3->Get(getObjList3, 500, dataList));
        ASSERT_TRUE(dataList[2]);
        AssertBufferEqual(*dataList[2], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_TRUE(dataList[3]);
        AssertBufferEqual(*dataList[3], std::string{ 90, 91, 92, 93, 94 });
        ASSERT_TRUE(dataList[0]);
        AssertBufferEqual(*dataList[0], std::string{ 80, 81, 82, 83 });
        ASSERT_FALSE(dataList[1]);

        LOG(INFO) << "After other thread create object, get continue.";
        dataList.clear();
        DS_ASSERT_OK(cliLocal3->Get(getObjList3, 2000, dataList));
        ASSERT_TRUE(dataList[1]);
        AssertBufferEqual(*dataList[1], std::string{ 70, 71, 72 });
    });

    std::vector<std::string> getObjList4 = { obj4Id, obj2Id, obj3Id };
    auto fut4 = threadPool.Submit([getObjList4, cliLocal4, this]() {
        LOG(INFO) << "cliLocal starts to get object at first time.";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal4->Get(getObjList4, 500, dataList));
        ASSERT_FALSE(dataList[0]);
        ASSERT_TRUE(dataList[1]);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        ASSERT_TRUE(dataList[2]);
        AssertBufferEqual(*dataList[2], std::string{ 80, 81, 82, 83 });

        LOG(INFO) << "After other thread create object, get continue.";
        dataList.clear();
        DS_ASSERT_OK(cliLocal4->Get(getObjList4, 2000, dataList));
        ASSERT_TRUE(dataList[0]);
        AssertBufferEqual(*dataList[0], std::string{ 70, 71, 72 });
    });

    auto fut5 = threadPool.Submit([obj4Id, cliRemote2, &data4, this]() {
        LOG(INFO) << "cliRemote2 starts to create object";
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        CreateAndSealObject(cliRemote2, obj4Id, data4);
        LOG(INFO) << "cliRemote2 ends to create object";
    });

    fut1.get();
    fut2.get();
    fut3.get();
    fut4.get();
    fut5.get();
}

/*
 Testing Global Client Reference Counting
*/

TEST_F(OCClientDistMasterTest, DISABLED_GRemoteRefBasicTest)
{
    // Create an object on Node 0
    std::string objectKey = ObjectKeyWithOwner(0, uuidMap_);

    // Increment and Decrement references for the object by client on Node 1
    std::shared_ptr<ObjectClient> client;
    InitTestClient(1, client);

    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey };

    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));

    LOG(INFO) << CalcGRefNumOnWorker(objectKey, client);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey, client), 1);

    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));

    auto rc = client->GDecreaseRef(objectKeys, failObjects);
    ASSERT_EQ(failObjects.size(), static_cast<size_t>(0));
    ASSERT_EQ(objectKeys.size(), static_cast<size_t>(1));
    DS_ASSERT_OK(rc);
}

TEST_F(OCClientDistMasterTest, GRemoteRefMultiTest)
{
    // Create multiple objects on Node 0 and 2
    std::string objectKey1a = ObjectKeyWithOwner(0, uuidMap_);
    std::string objectKey1b = ObjectKeyWithOwner(0, uuidMap_);
    std::string objectKey2a = ObjectKeyWithOwner(2, uuidMap_);
    std::string objectKey2b = ObjectKeyWithOwner(2, uuidMap_);

    // Increment references for the object by client on Node 1
    std::shared_ptr<ObjectClient> client;
    InitTestClient(1, client);

    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey1a, objectKey1b, objectKey2a, objectKey2b };

    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));

    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1a, client), 1);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1b, client), 1);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2a, client), 1);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2b, client), 1);

    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));

    auto rc = client->GDecreaseRef(objectKeys, failObjects);
    ASSERT_EQ(failObjects.size(), static_cast<size_t>(0));
    DS_ASSERT_OK(rc);
}

/*
 Testing Nested Object References
*/

TEST_F(OCClientDistMasterTest, LEVEL1_AsyncDeleteNestedInOtherMaster)
{
    std::vector<std::string> failObjects;
    std::shared_ptr<ObjectClient> client1, client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string objectKey1 = GenRandomString();
    std::string objectKey2 = GenRandomString();
    std::string objectKey3 = GenRandomString();

    std::string value1 = GenRandomString();
    CreateParam param{};
    uint64_t timeout = 4;
    // GIncr objectKey1/objectKey2/objectKey3, put
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey1 }, failObjects));
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey2, objectKey3 }, failObjects));
    DS_ASSERT_OK(client2->Put(objectKey2, reinterpret_cast<const uint8_t *>(const_cast<char *>(value1.data())),
                              value1.size(), param));
    DS_ASSERT_OK(client2->Put(objectKey3, reinterpret_cast<const uint8_t *>(const_cast<char *>(value1.data())),
                              value1.size(), param));
    std::unordered_set<std::string> objectKeys23 = { objectKey2, objectKey3 };
    DS_ASSERT_OK(client1->Put(objectKey1, reinterpret_cast<const uint8_t *>(const_cast<char *>(value1.data())),
                              value1.size(), param, objectKeys23));
    std::vector<Optional<Buffer>> buffer;
    DS_ASSERT_OK(client1->Get({ objectKey1 }, timeout, buffer));
    DS_ASSERT_OK(client1->Get({ objectKey2 }, timeout, buffer));
    DS_ASSERT_OK(client1->Get({ objectKey3 }, timeout, buffer));
    // GDec objectKey2/objectKey3
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey2, objectKey3 }, failObjects));
    ASSERT_EQ((int)failObjects.size(), 0);
    DS_ASSERT_OK(client1->Get({ objectKey1 }, timeout, buffer));
    DS_ASSERT_OK(client1->Get({ objectKey2 }, timeout, buffer));
    DS_ASSERT_OK(client1->Get({ objectKey3 }, timeout, buffer));
    DS_ASSERT_OK(client2->Get({ objectKey1 }, timeout, buffer));
    DS_ASSERT_OK(client2->Get({ objectKey2 }, timeout, buffer));
    DS_ASSERT_OK(client2->Get({ objectKey3 }, timeout, buffer));
    // GDec objectKey1, get objectKey1/objectKey2/objectKey3
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey1 }, failObjects));
    ASSERT_EQ((int)failObjects.size(), 0);
    sleep(3);
    DS_ASSERT_NOT_OK(client1->Get({ objectKey1 }, timeout, buffer));
    DS_ASSERT_NOT_OK(client1->Get({ objectKey2 }, timeout, buffer));
    DS_ASSERT_NOT_OK(client1->Get({ objectKey3 }, timeout, buffer));
    DS_ASSERT_NOT_OK(client2->Get({ objectKey1 }, timeout, buffer));
    DS_ASSERT_NOT_OK(client2->Get({ objectKey2 }, timeout, buffer));
    DS_ASSERT_NOT_OK(client2->Get({ objectKey3 }, timeout, buffer));
}

TEST_F(OCClientDistMasterTest, NestedNormalTest)
{
    // Create ObjA on Node0
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    std::string objectKey0 = ObjectKeyWithOwner(0, uuidMap_);
    std::vector<uint8_t> data0 = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(client0, objectKey0, data0);

    // Create ObjB on Node1 that depend on ObjA
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::string objectKey1 = ObjectKeyWithOwner(1, uuidMap_);
    std::vector<uint8_t> data1 = { 55, 56, 57, 58, 59, 60 };
    CreateAndSealObject(client1, objectKey1, data1, { objectKey0 });

    // Create ObjC on Node2 that depend on ObjA
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(2, client2);
    std::string objectKey2 = ObjectKeyWithOwner(2, uuidMap_);
    std::vector<uint8_t> data2 = { 55, 56, 57, 58, 59, 60 };
    CreateAndSealObject(client2, objectKey2, data2, { objectKey0 });

    // Increase Reference Count for all objects
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey0, objectKey1, objectKey2 };
    DS_ASSERT_OK(client0->GIncreaseRef(objectKeys, failObjects));

    // Check if objects are available
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client0->Get(objectKeys, 0, dataList));
    ASSERT_EQ(dataList.size(), size_t(3));

    DS_ASSERT_OK(client0->GDecreaseRef({ objectKey0 }, failObjects));
    dataList.clear();
    DS_ASSERT_OK(client0->Get({ objectKey0 }, 0, dataList));
    ASSERT_EQ(dataList.size(), size_t(1));

    DS_ASSERT_OK(client0->GDecreaseRef({ objectKey1, objectKey2 }, failObjects));
    dataList.clear();
    sleep(3);
    DS_ASSERT_NOT_OK(client0->Get({ objectKey0 }, 0, dataList));
}

TEST_F(OCClientDistMasterTest, LEVEL1_GRefAsyncDecrease)
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
            }));
        } else {
            futures.emplace_back(threadPool.Submit([&client2, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
            }));
        }
    }
    for (auto &future : futures) {
        future.get();
    }

    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1, client1), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2, client1), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey3, client1), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey1, client2), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey2, client2), 2);
    ASSERT_EQ(CalcGRefNumOnWorker(objectKey3, client2), 2);

    futures.clear();
    for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
            futures.emplace_back(threadPool.Submit([&client1, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
            }));
        } else {
            futures.emplace_back(threadPool.Submit([&client2, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
            }));
        }
    }
    for (auto &future : futures) {
        future.get();
    }
    std::vector<std::string> failObjects;
    ASSERT_EQ(failObjects.size(), size_t(0));
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_EQ(failObjects.size(), static_cast<size_t>(0));
    failObjects.clear();
    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_EQ(failObjects.size(), static_cast<size_t>(0));
}

TEST_F(OCClientDistMasterTest, GRefAsyncIncrease)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);

    std::string objectKey = ObjectKeyWithOwner(1, uuidMap_);
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    std::vector<std::string> objectKeys = { objectKey };
    CreateAndSealObject(client1, objectKey, data);

    std::vector<Optional<Buffer>> dataList;

    DS_ASSERT_OK(client2->Get(objectKeys, 0, dataList));

    ThreadPool threadPool(10);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
            futures.emplace_back(threadPool.Submit([&client1, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
            }));
        } else {
            futures.emplace_back(threadPool.Submit([&client2, &objectKeys]() {
                std::vector<std::string> failObjects;
                DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failObjects));
            }));
        }
    }
    for (auto &future : futures) {
        future.get();
    }
    std::vector<std::string> failObjects;
    for (int i = 0; i < 5; i++) {
        DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
        DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    }
    ASSERT_EQ(failObjects.size(), size_t(0));
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failObjects));
    ASSERT_EQ(failObjects.size(), static_cast<size_t>(0));
    DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failObjects));
    ASSERT_EQ(failObjects.size(), static_cast<size_t>(0));
}

TEST_F(OCClientDistMasterTest, TestWorkerTimeoutAndPushMeta)
{
    LOG(INFO) << "Test worker recovery timeout and master clean its metadata then worker push metadata to master";
    InitObjectClients();
    size_t num = 2;
    std::vector<uint8_t> data;
    data.resize(500 * 1024ul);
    std::vector<std::string> objKeys;
    std::vector<std::shared_ptr<Buffer>> buffers(num);
    // Metadata stored in master 2
    for (size_t i = 0; i < num; ++i) {
        std::string objKey = ObjectKeyWithOwner(2, uuidMap_);
        objKeys.emplace_back(objKey);
        CreateParam param;
        DS_ASSERT_OK(objClient0_->Create(objKey, data.size(), param, buffers[i]));
        DS_ASSERT_OK(buffers[i]->MemoryCopy(data.data(), data.size()));
        DS_ASSERT_OK(buffers[i]->Publish());
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(objClient0_->GIncreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Let heartbeat timeout and master will clear its metadata, then when worker reconnect,
    // master will ask worker to send the metadata to master.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(2000)"));
    sleep(10);

    for (size_t i = 0; i < num; ++i) {
        DS_ASSERT_OK(buffers[i]->Seal());
    }

    std::vector<Optional<Buffer>> getBuffers;
    DS_ASSERT_OK(objClient1_->Get(objKeys, 60'000, getBuffers));

    DS_ASSERT_OK(objClient0_->GDecreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(OCClientDistMasterTest, GDecreaseWithRedirect)
{
    std::string objectKey1 = ObjectKeyWithOwner(0, uuidMap_);
    std::string objectKey2 = ObjectKeyWithOwner(0, uuidMap_);
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::vector<std::string> failObjects;
    std::vector<std::string> objectKeys = { objectKey1, objectKey2 };
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    HostPort workerAddr1;
    HostPort workerAddr2;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddr1));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(2, workerAddr2));  // worker index is 2
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.Redirect.ConstructRsp",
                                  FormatString("1*return(%s,%s)", workerAddr1.ToString(), workerAddr2.ToString())));
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failObjects));
}

class OCClientDistMasterDfxTest : public OCClientDistMasterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.workerGflagParams = " -node_timeout_s=3";
        opts.waitWorkerReady = false;
        opts.enableDistributedMaster = "true";
    }

    const int timeoutMs_ = 5000; // 5s timeout
};

TEST_F(OCClientDistMasterDfxTest, LEVEL1_MasterTimeout)
{
    InitTestClient(0, objClient0_, timeoutMs_);
    InitTestClient(1, objClient1_, timeoutMs_);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(20000)"));
    sleep(5); // 5 > node_timeout_s

    {
        // fail if no success objects
        std::string objectKey;
        DS_ASSERT_OK(objClient1_->GenerateObjectKey("afinityobj1", objectKey));
        std::vector<std::string> failObjects;
        auto status = (objClient1_->GIncreaseRef({ objectKey }, failObjects));
        DS_EXPECT_NOT_OK(status);
        EXPECT_TRUE(!failObjects.empty()) << VectorToString(failObjects);
    }
    {
        // success if partly fail, and return the fail ids
        std::string objectKey1;
        DS_ASSERT_OK(objClient1_->GenerateObjectKey("obj1", objectKey1));
        std::string objectKey0;
        DS_ASSERT_OK(objClient0_->GenerateObjectKey("obj0", objectKey0));

        std::vector<std::string> failObjects;
        DS_EXPECT_OK(objClient1_->GIncreaseRef({ objectKey0, objectKey1 }, failObjects));
        EXPECT_EQ(failObjects.size(), 1) << VectorToString(failObjects);
        EXPECT_EQ(failObjects[0], objectKey1);
    }
}

class OCClientCentralizedMaster : public OCClientDistMasterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        OCClientDistMasterTest::SetClusterSetupOptions(opts);
        opts.masterIdx = 2;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = " -v=1";
    }
};

TEST_F(OCClientCentralizedMaster, PutGet)
{
    InitObjectClients();
    std::string key = "key1";
    std::string value = "hello";
    EXPECT_EQ(objClient0_->Put(key, reinterpret_cast<const uint8_t *>(value.data()), value.size(), CreateParam(), {}),
              Status::OK());
    std::string outValue;
    std::vector<Optional<Buffer>> buffers;
    EXPECT_EQ(objClient1_->Get({ key }, 0, buffers), Status::OK());
    ASSERT_EQ(buffers.size(), (size_t)1);
}

TEST_F(OCClientCentralizedMaster, LEVEL1_AfterRestartPutGet)
{
    DS_ASSERT_OK(inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)"));
    DS_ASSERT_OK(inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)"));
    DS_ASSERT_OK(inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)"));
    InitObjectClients();
    cluster_->ShutdownNode(WORKER, 1);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    cluster_->StartNode(WORKER, 1, "");
    cluster_->WaitNodeReady(WORKER, 1);

    std::string key = "key1";
    std::string value = "hello";
    EXPECT_EQ(objClient0_->Put(key, reinterpret_cast<const uint8_t *>(value.data()), value.size(), CreateParam(), {}),
              Status::OK());

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::vector<Optional<Buffer>> buffers;
    EXPECT_EQ(objClient1_->Get({ key }, 0, buffers), Status::OK());
    ASSERT_EQ(buffers.size(), (size_t)1);
}

class OCClientDistMasterHeartbeatTest : public OCClientDistMasterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numOBS = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = " -node_timeout_s=1800 -node_dead_timeout_s=864000 -client_reconnect_wait_s=5";
        opts.waitWorkerReady = false;
        opts.enableDistributedMaster = "true";
    }
};

TEST_F(OCClientDistMasterHeartbeatTest, DISABLED_TestMasterRecoverRefAfterWorkerRestart)
{
    InitTestClient(0, objClient0_);
    InitTestClient(1, objClient1_);
    std::string objId = "obj1";
    std::string val = "val";
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(objClient0_->GIncreaseRef({ objId }, failedIds));
    ASSERT_EQ(failedIds.size(), 0ul);
    DS_ASSERT_OK(objClient0_->Put(objId, reinterpret_cast<const uint8_t *>(val.data()), val.size(), CreateParam()));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(objClient1_->Get({ objId }, 0, buffers));
    ASSERT_EQ(buffers.size(), 1ul);

    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 0 }));
    DS_ASSERT_OK(objClient1_->Get({ objId }, 0, buffers));
}

}  // namespace st
}  // namespace datasystem
