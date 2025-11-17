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
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/object_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"
#include "datasystem/common/metrics/res_metric_collector.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
namespace {
constexpr int64_t SHM_SIZE = 500 * 1024;
}  // namespace
class OCClientGetTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.numOcThreadNum = getThreadNum_;
        opts.workerGflagParams = FormatString(" -shared_memory_size_mb=500 -v=2 -log_monitor=true");
    }

protected:
    const int getThreadNum_ = 8;
};

TEST_F(OCClientGetTest, GetLocalTest)
{
    FLAGS_v = 1;
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cli2Local;
    InitTestClient(0, cli2Local);
    std::shared_ptr<ObjectClient> cli3Local;
    InitTestClient(0, cli3Local);

    std::string objKey = NewObjectKey();
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(cliLocal, objKey, data);

    // get exist id data
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliLocal->Get({ objKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    ASSERT_EQ(dataList[0]->GetSize(), static_cast<int>(data.size()));
    ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
    AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });

    // get not exist id data without timeout
    std::string notExistObjKey = NewObjectKey();
    std::vector<Optional<Buffer>> dataListNotExist(1);
    DS_ASSERT_NOT_OK(cliLocal->Get({ notExistObjKey }, 0, dataListNotExist));
    // get not exist id data with timeout
    Timer timer;
    DS_ASSERT_NOT_OK(cliLocal->Get({ notExistObjKey }, 1000, dataListNotExist));
    auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cast: " << timeCost;
    ASSERT_TRUE(timeCost >= 900 && timeCost < 1000);

    // get not seal data in same client
    std::string notSealObjKey = NewObjectKey();
    CreateObject(cliLocal, notSealObjKey, data);
    std::vector<Optional<Buffer>> dataListNotSeal;
    DS_ASSERT_NOT_OK(cliLocal->Get({ notSealObjKey }, 0, dataListNotSeal));

    // get not seal data in different client
    std::vector<Optional<Buffer>> dataListNotSealDiffCli(1);
    DS_ASSERT_NOT_OK(cli2Local->Get({ notSealObjKey }, 0, dataListNotSealDiffCli));
}

TEST_F(OCClientGetTest, EXCLUSIVE_GetLocalTimeoutTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cli2Local;
    InitTestClient(0, cli2Local);
    std::shared_ptr<ObjectClient> cli3Local;
    InitTestClient(0, cli3Local);

    // get not exist data, but create by local other client during timeout
    // get not exist data, but create by local other client after timeout
    ThreadPool threadPool(2);
    std::string objectKeyDelayCreate = NewObjectKey();
    std::vector<uint8_t> dataDelayCreate = { 65, 66, 67, 68, 69, 70 };

    WaitPost wp;
    std::shared_future<void> fut1 = threadPool.Submit([objectKeyDelayCreate, cli2Local, &wp]() {
        wp.Wait();
        LOG(INFO) << "cli2Local starts to get object";
        std::vector<Optional<Buffer>> dataList;
        Timer timer;
        DS_ASSERT_NOT_OK(cli2Local->Get({ objectKeyDelayCreate }, 500, dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cast: " << timeCost;
        ASSERT_TRUE(timeCost > 400 && timeCost < 600);
        LOG(INFO) << "cli2Local ends to get object";
    });

    std::string bufStr;
    auto fut2 = threadPool.Submit([objectKeyDelayCreate, cli3Local, &bufStr, &wp]() {
        wp.Set();
        LOG(INFO) << "cli3Local starts to get object";
        std::vector<Optional<Buffer>> dataList;
        Timer timer;
        RETURN_IF_NOT_OK(cli3Local->Get({ objectKeyDelayCreate }, 2000, dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cast: " << timeCost;
        CHECK_FAIL_RETURN_STATUS(timeCost > 450 && timeCost < 1800, K_RUNTIME_ERROR, "time out incorrect");
        CHECK_FAIL_RETURN_STATUS(dataList[0]->ImmutableData() != nullptr, K_RUNTIME_ERROR, "invalid buffer");
        auto &buffer = *dataList[0];
        ArrToStr(buffer.MutableData(), buffer.GetSize(), bufStr);
        LOG(INFO) << "cli3Local ends to get object";
        return Status::OK();
    });

    auto fut3 = threadPool.Submit([objectKeyDelayCreate, &dataDelayCreate, cliLocal, this, &fut1]() {
        fut1.get();
        LOG(INFO) << "cliLocal starts to create object";
        CreateAndSealObject(cliLocal, objectKeyDelayCreate, dataDelayCreate);
        LOG(INFO) << "cliLocal ends to create object";
    });

    fut3.get();
    ASSERT_EQ(fut2.get(), Status::OK());
    auto expectedStr = std::string{ 65, 66, 67, 68, 69, 70 };
    ASSERT_EQ(bufStr, expectedStr);
}

TEST_F(OCClientGetTest, GetLocalMultipleClientsToSingleWorkerTest)
{
    std::shared_ptr<ObjectClient> cli1Local;
    InitTestClient(0, cli1Local);
    std::shared_ptr<ObjectClient> cli2Local;
    InitTestClient(0, cli2Local);
    std::shared_ptr<ObjectClient> cli3Local;
    InitTestClient(0, cli3Local);

    std::string obj1Id = NewObjectKey();
    std::string obj2Id = NewObjectKey();
    std::string objNotCreate = NewObjectKey();
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    CreateAndSealObject(cli1Local, obj1Id, data1);
    CreateAndSealObject(cli1Local, obj2Id, data2);

    ThreadPool threadPool(2);
    std::vector<std::string> getObjListExist = { obj1Id, obj2Id };
    std::vector<std::string> getObjListPartialExist = { obj1Id, obj2Id, objNotCreate };
    auto fut1 = threadPool.Submit([getObjListExist, cli2Local]() {
        LOG(INFO) << "cli2Local starts to get object";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cli2Local->Get(getObjListExist, 1000, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        LOG(INFO) << "cli2Local ends to get object";
    });

    auto fut2 = threadPool.Submit([getObjListPartialExist, cli3Local]() {
        LOG(INFO) << "cli3Local starts to get object";
        std::vector<Optional<Buffer>> dataList;
        Timer timer;
        DS_ASSERT_OK(cli3Local->Get(getObjListPartialExist, 1000, dataList));
        ASSERT_TRUE(ExistsNone(dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cast: " << timeCost;
        ASSERT_TRUE(timeCost >= 900 && timeCost < 1000);
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        bool valid = false;
        if (dataList[2]) {
            valid = true;
        }
        ASSERT_EQ(valid, false);
        LOG(INFO) << "cli3Local ends to get object";
    });

    fut1.get();
    fut2.get();
}

TEST_F(OCClientGetTest, GetRemoteObjectMeetsRPCError)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    std::string objKey = NewObjectKey();
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(client1, objKey, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.remote_get", "1*return()"));

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client0->Get({ objKey }, 2000, dataList));
    AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
}

TEST_F(OCClientGetTest, GetRemoteObjectShmError)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    std::string objKey = NewObjectKey();
    int64_t size = (int64_t)300 * 1024 * 1024;
    std::string data = GenPartRandomString(size);
    CreateAndSealObject(client1, objKey, data);
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client0->Get({ objKey }, 2000, dataList));
    std::vector<std::string> objectKeys{ objKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
}

TEST_F(OCClientGetTest, GetRemoteSingleClientToSingleWorkerConcurringlyTest)
{
    std::shared_ptr<ObjectClient> cliRemote;
    InitTestClient(1, cliRemote);

    std::string obj1Id = NewObjectKey();
    std::string obj2Id = NewObjectKey();
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
        clientThreads[i] = std::thread([i, getObjList, cliLocal]() {
            LOG(INFO) << "client: " << i << " starts to get object";
            std::vector<Optional<Buffer>> dataList;
            DS_ASSERT_OK(cliLocal->Get(getObjList, 5000, dataList));
            ASSERT_TRUE(NotExistsNone(dataList));
            ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
            AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
            ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
            AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
            LOG(INFO) << "client: " << i << " ends to get object";
        });
    }

    for (auto &t : clientThreads) {
        t.join();
    }
    LOG(INFO) << "end client test.";
}

TEST_F(OCClientGetTest, GetRemoteMultipleClientsToSingleWorkerTest)
{
    std::shared_ptr<ObjectClient> cli1Local1;
    InitTestClient(0, cli1Local1);
    std::shared_ptr<ObjectClient> cli2Local1;
    InitTestClient(0, cli2Local1);
    std::shared_ptr<ObjectClient> cliLocal2;
    InitTestClient(1, cliLocal2);
    std::shared_ptr<ObjectClient> cliRemote;
    InitTestClient(2, cliRemote);

    std::string obj1Id = NewObjectKey();
    std::string obj2Id = NewObjectKey();
    std::string obj3Id = NewObjectKey();
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    CreateAndSealObject(cliRemote, obj1Id, data1);
    CreateAndSealObject(cliRemote, obj2Id, data2);

    ThreadPool threadPool(3);
    std::vector<std::string> getObjList = { obj1Id, obj2Id };
    // get data from local1 client1 to remote without timeout
    auto fut1 = threadPool.Submit([getObjList, cli1Local1]() {
        LOG(INFO) << "cli1Local1 starts to get object";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cli1Local1->Get(getObjList, 0, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        LOG(INFO) << "cli1Local1 ends to get object";
    });

    // get data from local1 client2 to remote
    auto fut2 = threadPool.Submit([getObjList, cli2Local1]() {
        LOG(INFO) << "cli2Local1 starts to get object";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cli2Local1->Get(getObjList, 1000, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        LOG(INFO) << "cli2Local1 ends to get object";
    });

    // get data from local2 to remote
    std::vector<std::string> getObjList2 = { obj1Id, obj2Id, obj3Id };
    auto fut3 = threadPool.Submit([getObjList2, cliLocal2]() {
        LOG(INFO) << "cliLocal2 starts to get object";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal2->Get(getObjList2, 0, dataList));
        ASSERT_TRUE(ExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], std::string{ 90, 91, 92, 93, 94 });
        bool valid = false;
        if (dataList[2]) {
            valid = true;
        }
        ASSERT_EQ(valid, false);
        LOG(INFO) << "cliLocal2 ends to get object";
    });

    fut1.get();
    fut2.get();
    fut3.get();
}

TEST_F(OCClientGetTest, GetRemoteSameIdTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cliRemote;
    InitTestClient(1, cliRemote);
    std::string obj1Id = NewObjectKey();
    std::string obj2Id = NewObjectKey();
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 90, 91, 92, 93, 94 };
    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliLocal, obj2Id, data2);

    std::vector<std::string> getObjList = { obj1Id, obj1Id, obj2Id };

    LOG(INFO) << "cliLocal starts to get object";
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliRemote->Get(getObjList, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
    AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
    ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
    AssertBufferEqual(*dataList[1], std::string{ 65, 66, 67, 68, 69, 70 });
    ASSERT_NE(dataList[2]->ImmutableData(), nullptr);
    AssertBufferEqual(*dataList[2], std::string{ 90, 91, 92, 93, 94 });
    LOG(INFO) << "cliLocal ends to get object";
}

TEST_F(OCClientGetTest, GetRemoteSingleClientToMultipleWorkersDelayCreateTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cliRemote1;
    InitTestClient(1, cliRemote1);
    std::shared_ptr<ObjectClient> cliRemote2;
    InitTestClient(2, cliRemote2);

    std::string obj1Id = NewObjectKey();
    std::string obj2Id = NewObjectKey();
    std::string obj3Id = NewObjectKey();
    std::string obj4Id = NewObjectKey();
    std::string data1 = GenRandomString(SHM_SIZE);
    std::string data2 = GenRandomString(SHM_SIZE);
    std::string data3 = GenRandomString(SHM_SIZE);
    std::string data4 = GenRandomString(SHM_SIZE);
    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliRemote1, obj2Id, data2);
    CreateAndSealObject(cliRemote2, obj3Id, data3);

    ThreadPool threadPool(2);
    std::vector<std::string> getObjList = { obj1Id, obj2Id, obj3Id, obj4Id };
    // get data from local to multiple remote
    auto fut1 = threadPool.Submit([getObjList, cliLocal, data1, data2, data3, data4]() {
        LOG(INFO) << "cliLocal starts to get object at first time.";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal->Get(getObjList, 0, dataList));
        ASSERT_TRUE(ExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], data1);
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], data2);
        ASSERT_NE(dataList[2]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[2], data3);
        bool valid = false;
        if (dataList[3]) {
            valid = true;
        }
        ASSERT_EQ(valid, false);
        LOG(INFO) << "cliLocal ends to get object at first time.";

        LOG(INFO) << "cliLocal starts to get object at second time.";
        Timer timer;
        dataList.clear();
        DS_ASSERT_OK(cliLocal->Get(getObjList, 1500, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cost: " << timeCost;
        ASSERT_TRUE(timeCost < 1500);
        ASSERT_NE(dataList[3]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[3], data4);
        LOG(INFO) << "cliLocal ends to get object at second time.";
    });

    // create data in remote node after 1000 milliseconds
    auto fut2 = threadPool.Submit([getObjList, obj4Id, &data4, cliRemote2, this]() {
        LOG(INFO) << "cliRemote2 starts to create object";
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        CreateAndSealObject(cliRemote2, obj4Id, data4);
        LOG(INFO) << "cliRemote2 ends to create object";
    });

    fut1.get();
    fut2.get();
}

TEST_F(OCClientGetTest, DISABLED_GetRemoteTimeOutDelayCreateTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cliLocal2;
    InitTestClient(0, cliLocal2);
    std::shared_ptr<ObjectClient> cliLocal3;
    InitTestClient(0, cliLocal3);
    std::shared_ptr<ObjectClient> cliLocal4;
    InitTestClient(0, cliLocal4);

    std::shared_ptr<ObjectClient> cliRemote1;
    InitTestClient(1, cliRemote1);

    std::string obj1Id = "key1";
    std::string obj2Id = "key2";
    std::string obj3Id = "key3";
    std::string obj4Id = "key4";
    std::string data1 = GenRandomString(SHM_SIZE);
    std::string data2 = GenRandomString(SHM_SIZE);
    std::string data3 = GenRandomString(SHM_SIZE);
    std::string data4 = GenRandomString(SHM_SIZE);
    CreateAndSealObject(cliRemote1, obj1Id, data1);
    CreateAndSealObject(cliRemote1, obj2Id, data2);
    CreateAndSealObject(cliRemote1, obj3Id, data3);

    ThreadPool threadPool(5);
    std::vector<std::string> getObjList = { obj1Id, obj2Id, obj3Id };
    // get data from local to multiple remote
    auto fut1 = threadPool.Submit([getObjList, cliLocal, data1, data2, data3]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal->Get(getObjList, 0, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], data1);
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], data2);
        ASSERT_NE(dataList[2]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[2], data3);
    });

    std::vector<std::string> getObjList2 = { obj3Id, obj1Id, obj2Id };
    auto fut2 = threadPool.Submit([getObjList2, cliLocal2, data1, data2, data3]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal2->Get(getObjList2, 0, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], data3);
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], data1);
        ASSERT_NE(dataList[2]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[2], data2);
    });

    std::vector<std::string> getObjList3 = { obj3Id, obj4Id, obj1Id, obj2Id };
    auto fut3 = threadPool.Submit([getObjList3, cliLocal3, data1, data2, data3, data4]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal3->Get(getObjList3, 500, dataList));
        ASSERT_TRUE(ExistsNone(dataList));
        ASSERT_NE(dataList[2]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[2], data1);
        ASSERT_NE(dataList[3]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[3], data2);
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], data3);
        bool valid = false;
        if (dataList[1]) {
            valid = true;
        }
        ASSERT_EQ(valid, false);
        LOG(INFO) << "After other thread create object, get continue.";
        dataList.clear();
        DS_ASSERT_OK(cliLocal3->Get(getObjList3, 1000, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], data4);
    });

    std::vector<std::string> getObjList4 = { obj4Id, obj2Id, obj3Id };
    auto fut4 = threadPool.Submit([getObjList4, cliLocal4, data2, data3, data4]() {
        LOG(INFO) << "cliLocal starts to get object at first time.";
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal4->Get(getObjList4, 500, dataList));
        ASSERT_TRUE(ExistsNone(dataList));
        bool valid = false;
        if (dataList[0]) {
            valid = true;
        }
        ASSERT_EQ(valid, false);
        ASSERT_NE(dataList[1]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[1], data2);
        ASSERT_NE(dataList[2]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[2], data3);

        LOG(INFO) << "After other thread create object, get continue.";
        dataList.clear();
        DS_ASSERT_OK(cliLocal4->Get(getObjList4, 1000, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*dataList[0], data4);
    });

    auto fut5 = threadPool.Submit([obj4Id, cliRemote1, &data4, this]() {
        LOG(INFO) << "cliRemote1 starts to create object";
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        CreateAndSealObject(cliRemote1, obj4Id, data4);
        LOG(INFO) << "cliRemote1 ends to create object";
    });

    fut1.get();
    fut2.get();
    fut3.get();
    fut4.get();
    fut5.get();
}

TEST_F(OCClientGetTest, TestShmLatchTimeout)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client);
    InitTestClient(0, client1);
    std::string data = GenRandomString(SHM_SIZE);

    DS_ASSERT_OK(client->Create(objectKey, SHM_SIZE, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(SHM_SIZE, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), SHM_SIZE));
    DS_ASSERT_OK(buffer->Publish());
    std::thread thread([&client1, &objectKeys]() {
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client1->Get(objectKeys, 0, buffers));
        ASSERT_EQ(buffers[0]->WLatch(4).GetCode(), K_RUNTIME_ERROR);
        ASSERT_EQ(buffers[0]->RLatch(4).GetCode(), K_RUNTIME_ERROR);
    });
    thread.join();
    DS_ASSERT_OK(buffer->UnWLatch());
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), SHM_SIZE);
    DS_ASSERT_OK(buffers[0]->RLatch());
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(buffers[0]->UnRLatch());
}

TEST_F(OCClientGetTest, TestLatchFailed)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client);
    InitTestClient(0, client1);
    int dataSize = 50;
    std::string data = GenRandomString(dataSize);
    DS_ASSERT_OK(client->Create(objectKey, dataSize, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(dataSize, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), dataSize));
    DS_ASSERT_OK(buffer->Publish());
    ASSERT_EQ(buffer->WLatch(4).GetCode(), K_RUNTIME_ERROR);
    ASSERT_EQ(buffer->RLatch(4).GetCode(), K_RUNTIME_ERROR);
    DS_ASSERT_OK(buffer->UnWLatch());
    DS_ASSERT_OK(buffer->RLatch(4));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), dataSize);
    DS_ASSERT_OK(buffers[0]->RLatch());
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(buffers[0]->UnRLatch());
}

TEST_F(OCClientGetTest, TestLatchTimeout)
{
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client);
    InitTestClient(0, client1);
    int dataSize = 50;
    std::string data = GenRandomString(dataSize);

    DS_ASSERT_OK(client->Create(objectKey, dataSize, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(dataSize, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), dataSize));
    DS_ASSERT_OK(buffer->Publish());
    std::thread thread([&buffer]() {
        ASSERT_EQ(buffer->WLatch(4).GetCode(), K_RUNTIME_ERROR);
        ASSERT_EQ(buffer->RLatch(4).GetCode(), K_RUNTIME_ERROR);
    });
    thread.join();
    DS_ASSERT_OK(buffer->UnWLatch());
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[0]->GetSize(), dataSize);
    DS_ASSERT_OK(buffers[0]->RLatch());
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(buffers[0]->UnRLatch());
}

TEST_F(OCClientGetTest, MultiLatchTest)
{
    std::vector<std::shared_ptr<ObjectClient>> clients;
    for (int i = 0; i < 10; i++) {
        std::shared_ptr<ObjectClient> client;
        InitTestClient(0, client);
        clients.emplace_back(client);
    }
    std::string objectKey = NewObjectKey();
    int dataSize = SHM_SIZE;
    std::shared_ptr<Buffer> buffer;
    std::string data = GenRandomString(dataSize);

    DS_ASSERT_OK(clients[0]->Create(objectKey, dataSize, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), dataSize));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());
    size_t inClusterClientNum = clients.size();
    ThreadPool createThreadPool(inClusterClientNum);
    std::vector<std::future<Status>> createFutures;
    createFutures.reserve(inClusterClientNum);
    std::vector<std::string> objectKeys{ objectKey };
    for (size_t index = 0; index < inClusterClientNum; index++) {
        if (index % 2 == 0) {
            createFutures.emplace_back(createThreadPool.Submit([&clients, &objectKeys, index]() {
                std::vector<Optional<Buffer>> buffers;
                RETURN_IF_NOT_OK(clients[index]->Get(objectKeys, 10, buffers));
                RETURN_IF_NOT_OK(buffers[0]->WLatch(120));
                buffers[0]->MutableData();
                return buffers[0]->UnWLatch();
            }));
        } else {
            createFutures.emplace_back(createThreadPool.Submit([&clients, &objectKeys, index]() {
                std::vector<Optional<Buffer>> buffers;
                RETURN_IF_NOT_OK(clients[index]->Get(objectKeys, 0, buffers));
                RETURN_IF_NOT_OK(buffers[0]->RLatch(30));
                buffers[0]->ImmutableData();
                return buffers[0]->UnRLatch();
            }));
        }
    }
    for (auto &fut : createFutures) {
        DS_ASSERT_OK(fut.get());
    }
}

TEST_F(OCClientGetTest, GetInvalidTimeoutTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::string objKey = NewObjectKey();

    std::vector<Optional<Buffer>> dataList;
    Status rc = cliLocal->Get({ objKey }, -1, dataList);
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_INVALID);
}

TEST_F(OCClientGetTest, SubTimeoutTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal, 5'000);
    std::string objKey = NewObjectKey();

    std::vector<Optional<Buffer>> dataList;
    Timer timer;
    DS_ASSERT_NOT_OK(cliLocal->Get({ objKey }, 8'000, dataList));
    auto elasped = timer.ElapsedMilliSecond();
    ASSERT_GE(elasped, 7'000);
    ASSERT_LE(elasped, 8'000);
}

TEST_F(OCClientGetTest, ThreadBusySubTimeoutTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objKey = NewObjectKey();

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.Get.asyncGetStart",
                                           FormatString("%d*sleep(2000)", getThreadNum_)));

    ThreadPool threadPool(getThreadNum_);

    for (int i = 0; i < getThreadNum_; i++) {
        threadPool.Execute([&client, &objKey] {
            std::vector<Optional<Buffer>> dataList;
            Timer timer;
            ASSERT_EQ(client->Get({ objKey }, 1000, dataList).GetCode(), K_NOT_FOUND);
            LOG(INFO) << "Cost time:" << timer.ElapsedMilliSecond();
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::vector<Optional<Buffer>> dataList;
    Timer timer;
    ASSERT_EQ(client->Get({ objKey }, 1000, dataList).GetCode(), K_NOT_FOUND);
    ASSERT_LT(timer.ElapsedMilliSecond(), 1500);
}

TEST_F(OCClientGetTest, OOMSubTimeoutTest)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    int timeoutMs = 2000;
    InitTestClient(1, client2, timeoutMs);

    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    CreateAndSealObject(client1, objectKey, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.Allocator.AllocateMemory",
                                           "100*return(K_OUT_OF_MEMORY)"));

    std::vector<Optional<Buffer>> dataList;
    Timer timer;
    ASSERT_EQ(client2->Get({ objectKey }, 2000, dataList).GetCode(), K_OUT_OF_MEMORY);
    ASSERT_LT(timer.ElapsedMilliSecond(), 2500);
}

TEST_F(OCClientGetTest, ClientRetryOutOfMemory)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.Create.AllocateMemory",
                                           "1*return(K_OUT_OF_MEMORY)"));
    CreateAndSealObject(client1, objectKey, data);

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey }, 2000, dataList));
    AssertBufferEqual(dataList[0].value(), data);
}

TEST_F(OCClientGetTest, ClientGetRetry)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    CreateAndSealObject(client1, objectKey, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "WorkerOCServiceImpl.Get.Retry", "1*call()"));

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey }, 2000, dataList));
    AssertBufferEqual(dataList[0].value(), data);
}

TEST_F(OCClientGetTest, ClientGetTimeout)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    CreateAndSealObject(client1, objectKey, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "WorkerOCServiceImpl.Get.Timeout", "1*call(0)"));

    std::vector<Optional<Buffer>> dataList;
    const int K_2000 = 2000;
    DS_ASSERT_NOT_OK(client2->Get({ objectKey }, K_2000, dataList));
}

TEST_F(OCClientGetTest, GetFailedInWorker)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey1 = "obj1";
    std::string objectKey2 = "obj2";
    std::string objectKey3 = "obj3";
    std::string data = GenRandomString(SHM_SIZE);

    std::vector<Optional<Buffer>> dataList;
    ASSERT_EQ(client1->Get({ objectKey1 }, 0, dataList).GetCode(), K_NOT_FOUND);

    CreateAndSealObject(client1, objectKey1, data);

    DS_ASSERT_OK(client1->Get({ objectKey1, objectKey2 }, 0, dataList));
    ASSERT_TRUE(dataList.size() == 2 && dataList[0] && !dataList[1]);
    AssertBufferEqual(dataList[0].value(), data);

    CreateAndSealObject(client1, objectKey2, data);
    CreateAndSealObject(client1, objectKey3, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.LoadObjectData.AddPayload",
                                           "2*return(K_RUNTIME_ERROR)"));

    ASSERT_EQ(client2->Get({ objectKey1 }, 0, dataList).GetCode(), K_RUNTIME_ERROR);
    DS_ASSERT_OK(client2->Get({ objectKey2, objectKey3 }, 0, dataList));
    ASSERT_TRUE(dataList.size() == 2 && (dataList[0] || dataList[1]));
}

TEST_F(OCClientGetTest, ConcurrentGetFailedInWorker)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);
    CreateAndSealObject(client1, objectKey, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.GetObjectFromRemote.AfterAttach",
                                           "1*sleep(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.LoadObjectData.AddPayload",
                                           "3*return(K_RUNTIME_ERROR)"));
    ThreadPool pool(2);
    pool.Execute([&client2, &objectKey] {
        std::vector<Optional<Buffer>> dataList;
        DS_EXPECT_NOT_OK(client2->Get({ objectKey }, 0, dataList));
    });

    pool.Execute([&client2, &objectKey] {
        std::vector<Optional<Buffer>> dataList;
        DS_EXPECT_NOT_OK(client2->Get({ objectKey }, 0, dataList));
    });
}

TEST_F(OCClientGetTest, GetFailedInWorkerReturnNoWait)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = "obj1";
    std::string data = GenRandomString(SHM_SIZE);
    CreateAndSealObject(client1, objectKey, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.LoadObjectData.AddPayload",
                                           "2*return(K_RUNTIME_ERROR)"));

    Timer time;
    std::vector<Optional<Buffer>> dataList;
    DS_EXPECT_NOT_OK(client2->Get({ objectKey }, 10000, dataList));
    const int maxCost = 3000;
    ASSERT_LT(time.ElapsedMilliSecond(), maxCost);
}

TEST_F(OCClientGetTest, GetLocalWorkerFailed)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey1 = "obj1";
    std::string objectKey2 = "obj2";
    std::string objectKey3 = "obj3";
    std::string data = GenRandomString(SHM_SIZE);
    CreateAndSealObject(client1, objectKey1, data);
    CreateAndSealObject(client1, objectKey2, data);
    CreateAndSealObject(client2, objectKey3, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreProcessGetObject.begin",
                                           "1*return(K_RUNTIME_ERROR)"));
    std::vector<Optional<Buffer>> dataList;
    Timer time;
    DS_ASSERT_OK(client1->Get({ objectKey1, objectKey2, objectKey3 }, 10000, dataList));
    ASSERT_LT(time.ElapsedMilliSecond(), 1000);
    ASSERT_TRUE(dataList.size() == 3);
    int count = 0;
    for (auto &buffer : dataList) {
        if (buffer) {
            count++;
            AssertBufferEqual(buffer.value(), data);
        }
    }
    ASSERT_EQ(count, 2);
}

TEST_F(OCClientGetTest, TestNotFoundScenario)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string data = GenRandomString(SHM_SIZE);
    std::vector<std::string> objKeys;
    for (size_t i = 0; i < 1000; ++i) {
        std::string objKey = "obj" + std::to_string(i);
        objKeys.emplace_back(objKey);
        if (i == 0) {
            CreateAndSealObject(client1, objKey, data);
        } else if (i == 1) {
            CreateAndSealObject(client2, objKey, data);
        }
    }
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(objKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objKeys.size());
    ASSERT_TRUE(buffers[0] && buffers[1]);

    objKeys.erase(objKeys.begin(), objKeys.begin() + 2);
    buffers.clear();
    ASSERT_EQ(client1->Get(objKeys, 1000, buffers).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(OCClientGetTest, TestWorkerDownAndGet)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestClient(2, client3);

    std::string data = GenRandomString(SHM_SIZE);
    std::vector<std::string> objKeys;
    for (size_t i = 0; i < 10; ++i) {
        std::string objKey = "obj" + std::to_string(i);
        objKeys.emplace_back(objKey);
        if (i < 5) {
            CreateAndSealObject(client1, objKey, data);
        } else if (i >= 5 && i < 8) {
            CreateAndSealObject(client2, objKey, data);
        } else {
            CreateAndSealObject(client3, objKey, data);
        }
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef(objKeys, failedObjectKeys));

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 2, "worker.LoadObjectData.AddPayload",
                                           "1000*return(K_RUNTIME_ERROR)"));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(objKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objKeys.size());
    for (size_t i = 0; i < 10; ++i) {
        if (i < 8) {
            ASSERT_TRUE(buffers[i]);
        } else {
            ASSERT_FALSE(buffers[i]);
        }
    }

    objKeys.erase(objKeys.begin(), objKeys.begin() + 8);
    buffers.clear();
    ASSERT_EQ(client1->Get(objKeys, 0, buffers).GetCode(), StatusCode::K_RUNTIME_ERROR);
    ASSERT_EQ(buffers.size(), objKeys.size());

    for (size_t i = 0; i < 10; ++i) {
        objKeys.emplace_back("not_exist_id" + std::to_string(i));
    }
    buffers.clear();
    ASSERT_EQ(client1->Get(objKeys, 6000, buffers).GetCode(), StatusCode::K_RUNTIME_ERROR);
    ASSERT_EQ(buffers.size(), objKeys.size());
}

TEST_F(OCClientGetTest, LEVEL1_GetRuntimeErrorAfterWorkerStart)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cli2Local;
    InitTestClient(0, cli2Local);
    int dataNum = 20;
    std::vector<std::string> objKeys;
    for (int i = 0; i < dataNum; i++) {
        std::string objKey = "key" + std::to_string(i);
        objKeys.emplace_back(objKey);
        std::vector<std::string> failIds;
        if (i < 10) {
            DS_ASSERT_OK(cliLocal->GIncreaseRef({ objKey }, failIds));
            std::string value = "vl";
            CreateAndSealObject(cliLocal, objKey, value);
        } else {
            DS_ASSERT_OK(cli2Local->GIncreaseRef({ objKey }, failIds));
            std::string value = "vl";
            CreateAndSealObject(cli2Local, objKey, value);
        }
    }

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    std::vector<Optional<Buffer>> buffers;
    Status rc = cliLocal->Get(objKeys, 1000, buffers);
    ASSERT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    LOG(INFO) << rc.ToString();
    for (size_t i = 0; i < objKeys.size(); i++) {
        std::string objKey = objKeys[i];
        std::vector<std::string> failIds;
        if (i < 10) {
            DS_ASSERT_OK(cliLocal->GDecreaseRef({ objKey }, failIds));
        } else {
            DS_ASSERT_OK(cli2Local->GDecreaseRef({ objKey }, failIds));
        }
    }
    rc = cliLocal->Get(objKeys, 1000, buffers);
    ASSERT_EQ(rc.GetCode(), K_NOT_FOUND);
}

class OCClientMultiThreadTest : public OCClientGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=10240 -v=2 -log_monitor=true";
    }

    void MultiClientMultiObjectsGet(int iterNum, std::vector<std::shared_ptr<ObjectClient>> &inClusterClients,
                                    std::vector<std::shared_ptr<ObjectClient>> &objectClients)
    {
        int objectNum = 200;
        int dataSize = 64;  // 64byte
        int64_t getTimeout = 10000;
        std::vector<uint8_t> data(dataSize, 1);
        size_t inClusterClientNum = inClusterClients.size();
        size_t getClientNum = objectClients.size();
        for (int k = 0; k < iterNum; k++) {
            LOG(INFO) << "At " << k << " time test";
            LOG(INFO) << "create object begin";
            ThreadPool createThreadPool(inClusterClientNum);
            std::vector<std::future<Status>> createFutures;
            createFutures.reserve(inClusterClientNum);
            for (size_t index = 0; index < inClusterClientNum; index++) {
                createFutures.emplace_back(
                    createThreadPool.Submit([this, &data, objectNum, &inClusterClients, index, inClusterClientNum]() {
                        auto client = inClusterClients[index];
                        for (int i = 0; i < objectNum; i++) {
                            if (objectNum % inClusterClientNum != index) {
                                continue;
                            }
                            auto object_key = "id_" + std::to_string(i);
                            CreateAndSealObject(client, object_key, data);
                        }
                        return Status::OK();
                    }));
            }
            for (auto &fut : createFutures) {
                ASSERT_EQ(fut.get(), Status::OK());
            }
            LOG(INFO) << "-------------------create success------------------";
            std::vector<std::string> getObjList;
            getObjList.reserve(objectNum);
            for (int i = 0; i < objectNum; i++) {
                getObjList.emplace_back("id_" + std::to_string(i));
            }
            LOG(INFO) << "get object begin";
            ThreadPool threadPool(getClientNum);
            std::vector<std::future<Status>> futures;
            for (size_t i = 0; i < getClientNum; i++) {
                auto &client = objectClients[i];
                futures.emplace_back(threadPool.Submit([&getObjList, &client, &getTimeout]() {
                    std::vector<Optional<Buffer>> dataList;
                    RETURN_IF_NOT_OK(client->Get(getObjList, getTimeout, dataList));
                    if (ExistsNone(dataList)) {
                        return Status(K_UNKNOWN_ERROR, "not get all objects");
                    }
                    return Status::OK();
                }));
            }
            for (auto &fut : futures) {
                ASSERT_EQ(fut.get(), Status::OK());
            }
            LOG(INFO) << "-------------------get success-------------------";
        }
    }
};

TEST_F(OCClientMultiThreadTest, MultiClientMultiObjectsGetTest)
{
    int inClusterClientNum = cluster_->GetWorkerNum();
    std::vector<std::shared_ptr<ObjectClient>> inClusterClients(inClusterClientNum);
    for (int i = 0; i < inClusterClientNum; i++) {
        InitTestClient(i, inClusterClients[i]);
    }

    int getClientNum = 30;
    std::vector<std::shared_ptr<ObjectClient>> objectClients(getClientNum);
    for (int i = 0; i < getClientNum; i++) {
        InitTestClient(0, objectClients[i]);
    }
    MultiClientMultiObjectsGet(1, inClusterClients, objectClients);
}

class OCClientRemoteGetTest : public OCClientGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=12";
    }
};

TEST_F(OCClientRemoteGetTest, LEVEL2_RemoteGetOomTest)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    int timeoutMs = 10'000;
    InitTestClient(1, client1, timeoutMs);

    std::string obj0 = NewObjectKey();
    std::string obj1 = NewObjectKey();
    std::vector<uint8_t> data0(1024 * 1024 * 8, '0');
    std::vector<uint8_t> data1(1024 * 1024 * 8, '1');
    CreateAndSealObject(client0, obj0, data0);
    CreateAndSealObject(client1, obj1, data1);

    std::vector<Optional<Buffer>> dataList;
    Timer timer;
    DS_ASSERT_OK(client1->Get({ obj0, obj1 }, 10000, dataList));
    auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cast: " << timeCost;
    ASSERT_TRUE(timeCost < 10000);
    ASSERT_TRUE((dataList[0] || dataList[1]) && !(dataList[0] && dataList[1]));
    std::vector<Optional<Buffer>> dataList1;
    if (dataList[0]) {
        ASSERT_EQ(client1->Get({ obj1 }, 0, dataList1).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    } else {
        ASSERT_EQ(client1->Get({ obj0 }, 0, dataList1).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    }
}

TEST_F(OCClientRemoteGetTest, LocalGetOomTest)
{
    std::shared_ptr<ObjectClient> client0;
    int timeoutMs = 10'000;
    InitTestClient(0, client0, timeoutMs);
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreProcessGetObject.begin",
                                           "100*return(K_OUT_OF_MEMORY)"));
    std::string obj0 = NewObjectKey();
    std::vector<uint8_t> data0(1024 * 1024 * 8, '0');
    CreateAndSealObject(client0, obj0, data0);
    std::vector<Optional<Buffer>> dataList;
    ASSERT_EQ(client0->Get({ obj0 }, 10000, dataList).GetCode(), K_OUT_OF_MEMORY);
}

TEST_F(OCClientRemoteGetTest, EXCLUSIVE_LEVEL1_MultiRemoteGetOomTest)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    int timeoutMs = 3'000;
    InitTestClient(0, client0, timeoutMs);
    InitTestClient(1, client1, timeoutMs);
    InitTestClient(1, client2, timeoutMs);
    InitTestClient(1, client3, timeoutMs);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.AllocateMemory.sleepTime", "1000*call(10)"));

    std::string obj0 = NewObjectKey();
    std::string obj1 = NewObjectKey();
    std::vector<uint8_t> data0(1024 * 1024 * 8, '0');
    std::vector<uint8_t> data1(1024 * 1024 * 8, '1');
    CreateAndSealObject(client0, obj0, data0);
    CreateAndSealObject(client1, obj1, data1);
    uint32_t max_wait_millisecond = 11000;  // 31s

    auto testFunc = [&](std::shared_ptr<ObjectClient> client) {
        std::vector<Optional<Buffer>> dataList;
        Timer timer;
        DS_ASSERT_OK(client->Get({ obj0, obj1 }, 8000, dataList));
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "time cast: " << timeCost;
        ASSERT_TRUE(timeCost < max_wait_millisecond);
        ASSERT_TRUE((dataList[0] || dataList[1]) && !(dataList[0] && dataList[1]));
        std::vector<Optional<Buffer>> dataList1;
        if (dataList[0]) {
            DS_ASSERT_NOT_OK(client->Get({ obj1 }, 0, dataList1));
        } else {
            DS_ASSERT_NOT_OK(client->Get({ obj0 }, 0, dataList1));
        }
    };

    ThreadPool threadPool(3);

    auto fut1 = threadPool.Submit([&]() { testFunc(client1); });
    auto fut2 = threadPool.Submit([&]() { testFunc(client2); });
    auto fut3 = threadPool.Submit([&]() { testFunc(client3); });

    fut1.get();
    fut2.get();
    fut3.get();
}

TEST_F(OCClientRemoteGetTest, TestGetReturnOOMBeforeAddTimer)
{
    std::vector<uint8_t> data(1024 * 1024 * 8, '0');
    std::vector<std::string> objKeys = { "object1", "object2" };
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef(objKeys, failedObjectKeys));
    CreateAndSealObject(client1, objKeys[0], data);
    CreateAndSealObject(client2, objKeys[1], data);

    std::vector<Optional<Buffer>> buffers;
    (void)client1->Get(objKeys, 1000, buffers);
    buffers.clear();

    DS_ASSERT_OK(client1->GDecreaseRef({ objKeys[0] }, failedObjectKeys));

    DS_ASSERT_OK(client1->Get({ objKeys[1] }, 1000, buffers));
}

class OCClientRemoteGetTest2 : public OCClientGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.enableSpill = true;
        opts.workerGflagParams =
            "-client_reconnect_wait_s=1 -shared_memory_size_mb=100 -log_monitor=true";
        opts.numEtcd = 1;
    }

    uint32_t GetResMonitorLogNum(std::string &fileName)
    {
        uint32_t totalNum = 0;
        std::stringstream ssTimeCostFile;
        ssTimeCostFile << FLAGS_log_dir.c_str() << "/\\.\\./worker0/log/" << fileName;
        std::string pattern = ssTimeCostFile.str();
        std::vector<std::string> files;
        if (Glob(pattern, files).IsError()) {
            return 0;
        }
        for (const auto &file : files) {
            FILE *fp = fopen(file.c_str(), "r");
            EXPECT_TRUE(fp != nullptr);
            int c;
            do {
                c = fgetc(fp);
                if (c == '\n') {
                    totalNum++;
                }
            } while (c != EOF);
            auto rc = fclose(fp);
            EXPECT_EQ(rc, 0);
        }
        return totalNum;
    };

    Status GetLastResMonitorLogInfo(int index, const std::string &fileName, std::vector<std::string> &infos)
    {
        std::string fullName = FormatString("%s/../worker%d/log/%s", FLAGS_log_dir.c_str(), index, fileName);
        std::ifstream ifs(fullName);
        if (!ifs.is_open()) {
            return Status(K_RUNTIME_ERROR, FormatString("read file %s failed", fullName));
        }
        std::string line;
        std::string lastLine;
        while (std::getline(ifs, line)) {
            if (!line.empty()) {
                lastLine = line;
            }
        }
        infos = Split(lastLine, " | ");
        const int ignoreCount = 7;
        CHECK_FAIL_RETURN_STATUS(
            infos.size() == static_cast<size_t>(ResMetricName::RES_METRICS_END) + ignoreCount,
            K_RUNTIME_ERROR, FormatString("invalid log format: %s", lastLine));
        infos.erase(infos.begin(), infos.begin() + ignoreCount);
        return Status::OK();
    };
};

TEST_F(OCClientRemoteGetTest2, LEVEL1_ObjectFreeTest)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.CollectMetrics", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.CollectMetrics", "call(1000)"));

    InitTestClient(0, client0);
    InitTestClient(1, client1);
    InitTestClient(1, client2);
    std::string obj0 = "key_1";
    std::string obj1 = "key_2";
    std::vector<uint8_t> data0(1024 * 1024 * 8, '0');
    std::vector<uint8_t> data1(1024 * 1024 * 8, '1');

    CreateAndSealObject(client0, obj0, data0);
    CreateAndSealObject(client0, obj1, data1);

    ThreadPool threadPool(1);
    auto fut = threadPool.Submit([&]() {
        std::vector<Optional<Buffer>> dataList;
        client2->Get({ "not_exist" }, 10000, dataList);
    });
    {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(client1->Get({ obj0 }, 0, dataList));
    }
    {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(client1->Get({ obj1 }, 0, dataList));
    }
    fut.get();
    std::string resFile = "resource.log";
    DS_ASSERT_OK(ResMetricCollector::Instance().Init());
    ASSERT_NE(GetResMonitorLogNum(resFile), (uint32_t)0);
    auto autoWriteTime = std::chrono::milliseconds(2 * 1000);
    std::this_thread::sleep_for(autoWriteTime);
    client0.reset();
    client1.reset();
    client2.reset();
    // Ensure all message is write to disk.
    cluster_->ShutdownNode(WORKER, 0);
    // Start and middle time, this test will cost 13+30=43s.
    uint32_t expectNum = 4;
    ASSERT_GE(GetResMonitorLogNum(resFile), expectNum);
}

TEST_F(OCClientRemoteGetTest2, LEVEL1_TestResLogForClientAndObject)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.CollectMetrics", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.CollectMetrics", "call(1000)"));
    Timer timer;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    InitTestClient(1, client2);
    std::string obj0 = "key_1";
    std::string obj1 = "key_2";
    size_t dataSize0 = 1024 * 1024 * 4;
    size_t dataSize1 = 1024 * 1024 * 8;
    size_t metadataSize = 64;
    size_t shmQueueSize = 73744;
    size_t two = 2;
    std::vector<uint8_t> data0(dataSize0, '0');
    std::vector<uint8_t> data1(dataSize1, '1');

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client0->GIncreaseRef({ obj0, obj1 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    CreateAndSealObject(client0, obj0, data0);
    CreateAndSealObject(client0, obj1, data1);

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client1->Get({ obj0 }, 0, dataList));
    DS_ASSERT_OK(client1->Get({ obj1 }, 0, dataList));
    // In case of buffer still hold ObjectClientImpl.
    dataList.clear();

    while (timer.ElapsedMilliSecond() < 10000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    const int waitTimeMs = 2000;
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeMs));
    std::string resFile = "resource.log";
    // Check active client count, object count and object size.
    std::vector<std::string> resInfo1;
    std::vector<std::string> resInfo2;
    DS_ASSERT_OK(GetLastResMonitorLogInfo(0, resFile, resInfo1));
    DS_ASSERT_OK(GetLastResMonitorLogInfo(1, resFile, resInfo2));
    ASSERT_EQ(resInfo1[static_cast<size_t>(ResMetricName::ACTIVE_CLIENT_COUNT)], "1");
    ASSERT_EQ(resInfo1[static_cast<size_t>(ResMetricName::OBJECT_COUNT)], "2");
    ASSERT_EQ(resInfo1[static_cast<size_t>(ResMetricName::OBJECT_SIZE)],
              std::to_string(dataSize0 + dataSize1 + metadataSize * two + shmQueueSize));

    ASSERT_EQ(resInfo2[static_cast<size_t>(ResMetricName::ACTIVE_CLIENT_COUNT)], "2");
    ASSERT_EQ(resInfo2[static_cast<size_t>(ResMetricName::OBJECT_COUNT)], "2");
    ASSERT_EQ(resInfo2[static_cast<size_t>(ResMetricName::OBJECT_SIZE)],
              std::to_string(dataSize0 + dataSize1 + metadataSize * two + shmQueueSize));

    DS_ASSERT_OK(client0->GDecreaseRef({ obj0, obj1 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeMs));
    // Check after all object delete.
    DS_ASSERT_OK(GetLastResMonitorLogInfo(0, resFile, resInfo1));
    DS_ASSERT_OK(GetLastResMonitorLogInfo(1, resFile, resInfo2));
    ASSERT_EQ(resInfo1[static_cast<size_t>(ResMetricName::OBJECT_COUNT)], "0");
    ASSERT_EQ(resInfo1[static_cast<size_t>(ResMetricName::OBJECT_SIZE)], std::to_string(shmQueueSize));

    ASSERT_EQ(resInfo2[static_cast<size_t>(ResMetricName::OBJECT_COUNT)], "0");
    ASSERT_EQ(resInfo2[static_cast<size_t>(ResMetricName::OBJECT_SIZE)], std::to_string(shmQueueSize));

    client0.reset();
    client1.reset();
    client2.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeMs));
    // Check after all client disconnect.
    DS_ASSERT_OK(GetLastResMonitorLogInfo(0, resFile, resInfo1));
    DS_ASSERT_OK(GetLastResMonitorLogInfo(1, resFile, resInfo2));
    ASSERT_EQ(resInfo1[static_cast<size_t>(ResMetricName::ACTIVE_CLIENT_COUNT)], "0");
    ASSERT_EQ(resInfo2[static_cast<size_t>(ResMetricName::ACTIVE_CLIENT_COUNT)], "0");
}

TEST_F(OCClientRemoteGetTest2, LEVEL1_GetLocalObjectOOM)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    auto timeoutMs = 2'000;
    InitTestClient(0, client0, timeoutMs);
    InitTestClient(1, client1, timeoutMs);
    std::string obj0 = "key_1";
    std::string obj1 = "key_2";
    std::vector<uint8_t> data0(1024 * 1024 * 60, '0');
    std::vector<uint8_t> data1(1024 * 1024 * 60, '1');
    CreateAndSealObject(client0, obj0, data0);
    CreateAndSealObject(client1, obj1, data1);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.AllocateMemory.sleepTime", "100*call(100)"));

    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client0->Get({ obj0, obj1 }, 0, dataList));
    ASSERT_EQ(dataList.size(), 2ul);
    ASSERT_TRUE((bool)dataList[0] ^ (bool)dataList[1]);
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "sleep(2000)");
    if (dataList[0]) {
        ASSERT_EQ(client0->Get({ obj1 }, 0, dataList).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    } else {
        ASSERT_EQ(client0->Get({ obj0 }, 0, dataList).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    }
}

class OCClientRemoteGetTest3 : public OCClientGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 4;
        opts.workerGflagParams = "-shared_memory_size_mb=12";
        opts.numEtcd = 1;
    }
};

TEST_F(OCClientRemoteGetTest3, DISABLED_LEVEL1_TestGetOOMScenario)
{
    std::vector<uint8_t> data(1024 * 1024 * 8, '0');
    std::vector<std::string> objKeys = { "ji", "ni", "tai", "mei" };
    std::vector<std::shared_ptr<ObjectClient>> clients;
    for (size_t i = 0; i < 4; ++i) {
        std::shared_ptr<ObjectClient> client;
        int timeoutMs = 30'000;
        InitTestClient(i, client, timeoutMs);
        if (i == 0) {
            std::vector<std::string> failedObjectKeys;
            DS_ASSERT_OK(client->GIncreaseRef(objKeys, failedObjectKeys));
        }
        CreateAndSealObject(client, objKeys[i], data);
        clients.emplace_back(std::move(client));
    }

    // 1. Get local object success and return ok.
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(clients[0]->Get({ objKeys[0] }, 1000, buffers));
    ASSERT_TRUE(buffers[0]);

    // 2. Get remote objects and return OOM.
    std::vector<Optional<Buffer>> buffers1;
    ASSERT_EQ(clients[0]->Get({ objKeys[1], objKeys[2], objKeys[3] }, 1000, buffers1).GetCode(),
              StatusCode::K_OUT_OF_MEMORY);
    ASSERT_TRUE(!buffers1[0] && !buffers1[1] && !buffers1[2]);

    // 3. GDecreaseRef and get, return OK.
    buffers.clear();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(clients[0]->GDecreaseRef({ objKeys[0] }, failedObjectKeys));
    DS_ASSERT_OK(clients[0]->Get({ objKeys[1], objKeys[2], objKeys[3] }, 1000, buffers));
    ASSERT_EQ(buffers.size(), 3ul);
    int succCount = 0;
    for (const auto &buffer : buffers) {
        if (buffer) {
            succCount++;
        }
    }
    ASSERT_EQ(succCount, 1);
}

class MultiObjectClientTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const char *host_ip = "127.0.0.1";
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.workerConfigs.emplace_back(host_ip, GetFreePort());
        opts.workerConfigs.emplace_back(host_ip, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -shared_memory_size_mb=1024";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

TEST_F(MultiObjectClientTest, TestMultiRemoteGet)
{
    int numClient = 10;
    std::vector<std::shared_ptr<ObjectClient>> clientVec(numClient);
    int index = 0;
    for (auto &client : clientVec) {
        if (index == 0) {
            InitTestClient(index, client);
            index++;
        } else {
            InitTestClient(index, client);
        }
    }

    uint64_t dataSize = 200 * 1024 * 1024;
    std::string objectKey = NewObjectKey();
    std::string data = GenPartRandomString(dataSize);
    std::shared_ptr<Buffer> buffer;
    CreateParam param;
    DS_ASSERT_OK(clientVec[0]->Create(objectKey, dataSize, param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
    DS_ASSERT_OK(buffer->Seal());

    ThreadPool threadPool(numClient);
    std::vector<std::future<void>> futureVec;
    for (size_t i = 1; i < clientVec.size(); ++i) {
        auto fut = threadPool.Submit([&objectKey, &clientVec, &data, i]() {
            std::vector<Optional<Buffer>> buffers;
            usleep(50000 * (i - 1));
            DS_ASSERT_OK(clientVec[i]->Get({ objectKey }, 0, buffers));
            ASSERT_TRUE(NotExistsNone(buffers));
            ASSERT_EQ(std::string((const char *)buffers[0]->ImmutableData(), buffers[0]->GetSize()), data);
        });
        futureVec.push_back(std::move(fut));
    }
    for (auto &fut : futureVec) {
        fut.get();
    }
}

class OCClientGetTest2 : public OCClientGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        OCClientGetTest::SetClusterSetupOptions(opts);
        opts.enableDistributedMaster = "false";
    }
};

TEST_F(OCClientGetTest2, LEVEL1_TestRemoteGetMeetsRpcError)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1, 5000);
    InitTestClient(1, client2, 5000);

    std::string data = GenRandomString(SHM_SIZE);
    std::vector<std::string> objKeys;
    for (size_t i = 0; i < 100; ++i) {
        std::string objKey = "obj" + std::to_string(i);
        objKeys.emplace_back(objKey);
        if (i == 0) {
            CreateAndSealObject(client1, objKey, data);
        } else {
            CreateAndSealObject(client2, objKey, data);
        }
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.remote_get_failed", "20000*return(K_RPC_UNAVAILABLE)"));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(objKeys, 0, buffers));
    ASSERT_EQ(buffers.size(), objKeys.size());
    ASSERT_TRUE(buffers[0]);
    for (size_t i = 1; i < buffers.size(); ++i) {
        ASSERT_FALSE(buffers[i]);
    }

    objKeys.erase(objKeys.begin());
    buffers.clear();
    ASSERT_EQ(client1->Get(objKeys, 1000, buffers).GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    ASSERT_EQ(buffers.size(), objKeys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_FALSE(buffers[i]);
    }

    objKeys.emplace_back("obj_not_exist1");
    objKeys.emplace_back("obj_not_exist2");
    buffers.clear();
    ASSERT_EQ(client1->Get(objKeys, 1000, buffers).GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    ASSERT_EQ(buffers.size(), objKeys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_FALSE(buffers[i]);
    }
}

class OCClientRemoteGetTest4 : public OCClientGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 0;
        opts.workerGflagParams = "-shared_memory_size_mb=12";
        opts.numEtcd = 1;
    }
};

TEST_F(OCClientRemoteGetTest4, DISABLED_TestObjectPutAndGetConcurrency)
{
    LOG(INFO) << "Test Key Set and Get concurrency casuse create copy meta bug";
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);

    std::string objKey = "2-6-shame";
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client0->Put(objKey, (uint8_t *)val.c_str(), val.size(), param));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objKey }, failedObjectKeys));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "create_copy_meta", "1*sleep(1000)"));

    std::thread t1([&client1, &objKey, &val]() {
        std::vector<Optional<Buffer>> buffers;
        DS_EXPECT_OK(client1->Get({ objKey }, 0, buffers));
        ASSERT_EQ(buffers.size(), size_t(1));
        ASSERT_TRUE(buffers[0]);
        std::string getVal((char *)buffers[0]->ImmutableData(), buffers[0]->GetSize());
        EXPECT_EQ(getVal, val);
    });

    std::string newVal = RandomData().GetRandomString(1024ul * 1024ul);
    std::thread t2([&client0, &newVal, &objKey, &param]() {
        usleep(1'000);
        DS_EXPECT_OK(client0->Put(objKey, (uint8_t *)newVal.c_str(), newVal.size(), param));
    });

    t1.join();
    t2.join();

    std::vector<Optional<Buffer>> buffers;
    DS_EXPECT_OK(client1->Get({ objKey }, 0, buffers));
    ASSERT_EQ(buffers.size(), size_t(1));
    ASSERT_TRUE(buffers[0]);
    std::string getVal((char *)buffers[0]->ImmutableData(), buffers[0]->GetSize());
    DS_ASSERT_OK(client1->GDecreaseRef({ objKey }, failedObjectKeys));
    EXPECT_EQ(getVal, newVal);
}

class OCClientRemoteGetTest5 : public OCClientGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 0;
        opts.workerGflagParams = "-shared_memory_size_mb=12";
        opts.numEtcd = 1;
    }
};

TEST_F(OCClientRemoteGetTest5, TestGetSameObjectConcurrency)
{
    LOG(INFO) << "Test get same object key concurrency";
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    InitTestClient(2, client2);

    std::string objKey = "2-6-shame";
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client0->Put(objKey, (uint8_t *)val.c_str(), val.size(), param));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.after_query_meta", "1*sleep(5000)"));

    std::thread t1([&client1, &objKey, &val]() {
        std::vector<Optional<Buffer>> buffers;
        DS_EXPECT_OK(client1->Get({ objKey }, 0, buffers));
        ASSERT_EQ(buffers.size(), size_t(1));
        ASSERT_TRUE(buffers[0]);
        std::string getVal((char *)buffers[0]->ImmutableData(), buffers[0]->GetSize());
        EXPECT_EQ(getVal, val);
    });

    std::thread t2([&client2, &objKey, &val]() {
        std::vector<Optional<Buffer>> buffers;
        DS_EXPECT_OK(client2->Get({ objKey }, 0, buffers));
        ASSERT_EQ(buffers.size(), size_t(1));
        ASSERT_TRUE(buffers[0]);
        std::string getVal((char *)buffers[0]->ImmutableData(), buffers[0]->GetSize());
        EXPECT_EQ(getVal, val);
    });

    t1.join();
    t2.join();
}

TEST_F(OCClientRemoteGetTest5, TestRemoteGetAndRemoveLocationFailedThenPut)
{
    LOG(INFO) << "Test remote get failed and put";
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client0, 5000);
    InitTestClient(1, client1, 5000);
    InitTestClient(2, client2, 5000);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.remote_get_failed", "5*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.remove_location", "5*return(K_RPC_UNAVAILABLE)"));

    std::string objKey = "Ugly_iPhone15";
    std::string val = RandomData().GetRandomString(1024ul * 1024ul);

    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client0->Put(objKey, (uint8_t *)val.c_str(), val.size(), param));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(client1->Get({objKey}, 0, buffers));

    DS_ASSERT_OK(client0->Put(objKey, (uint8_t *)val.c_str(), val.size(), param));
}

}  // namespace st
}  // namespace datasystem
