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
 * Description: Device object put to host test.
 */

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <unistd.h>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/hetero_cache/hetero_client.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "device/dev_test_helper.h"

namespace datasystem {
using namespace acl;
namespace st {

class HeteroGetMetaInfoTest : public DevTestHelper {
    void SetUp() override
    {
        const char *ascend_root = std::getenv("ASCEND_HOME_PATH");
        if (ascend_root == nullptr) {
            DS_ASSERT_OK(datasystem::inject::Set("NO_USE_FFTS", "call()"));
            DS_ASSERT_OK(datasystem::inject::Set("client.GetOrCreateHcclComm.setIsSameNode", "call(0)"));
            BINEXPECT_CALL(AclDeviceManager::Instance, ()).WillRepeatedly([]() {
                return AclDeviceManagerMock::Instance();
            });
        }
        std::random_device rd;
        std::mt19937 gen(rd());
        gen_ = gen;
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

protected:
    std::mt19937 gen_;
    int32_t deviceId_ = 3;
};

TEST_F(HeteroGetMetaInfoTest, SubWithGetMetaSize)
{
    size_t numOfObjs = 5;
    std::vector<std::string> inObjectKeys, inObjectKeysBig, failedKeys, noExistKey = { "jfsdioa", "jfdoiqwe" };
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    int64_t retCode = -1;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
    }
    int randomLow = 1, randomHigh = 4;
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> distrib(randomLow, randomHigh);
    auto gen = [&mt, &distrib]() -> int { return distrib(mt); };
    auto initFunc = [this, &client](int deviceId) {
        deviceId_ = deviceId;
        InitAcl(deviceId_);
        InitTestHeteroClient(deviceId_ % DEFAULT_WORKER_NUM, client);
    };
    auto descFunc = [this, &client, &retCode]() {
        AclDeviceManager::Instance()->aclrtResetDevice(deviceId_);
        AclDeviceManager::Instance()->aclFinalize();
        client.reset();
        exit(retCode);
    };
    auto pubPid = fork();
    if (pubPid == 0) {
        initFunc(0);
        RandomFill(numOfObjs, gen, 0, devSetBlobList, 'b');
        DS_ASSERT_OK(IsSameContent(devSetBlobList, devSetBlobList, 'b'));
        Raii raii([&descFunc]() { descFunc(); });
        DS_ASSERT_OK(client->DevPublish(inObjectKeys, devSetBlobList, futures));
        for (auto &fut : futures) {
            DS_ASSERT_OK(fut.Get());
        }
        retCode = 0;
        LOG(INFO) << "Pub ok xxxxxxxxxxxx";
    }
    auto subPid = fork();
    if (subPid == 0) {
        initFunc(1);
        RandomFill(numOfObjs, gen, 0, devSetBlobList, 'b');
        Raii raii([&descFunc]() { descFunc(); });
        sleep(SHORT_WAIT_TIME);
        std::vector<MetaInfo> metaInfos;
        inObjectKeys.insert(inObjectKeys.end(), noExistKey.begin(), noExistKey.end());
        DS_ASSERT_OK(client->GetMetaInfo(inObjectKeys, true, metaInfos, failedKeys));
        DS_ASSERT_TRUE(failedKeys.size(), noExistKey.size());
        metaInfos.clear();
        failedKeys.clear();
        inObjectKeys.pop_back();
        inObjectKeys.pop_back();
        DS_ASSERT_OK(client->GetMetaInfo(inObjectKeys, true, metaInfos, failedKeys));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        std::vector<std::vector<uint64_t>> blobSizes;
        std::transform(metaInfos.begin(), metaInfos.end(), std::back_inserter(blobSizes),
                       [](const MetaInfo &metaInfo) { return metaInfo.blobSizeList; });
        DS_ASSERT_TRUE(!blobSizes.empty(), true);
        AllocAsBlobSize(blobSizes, deviceId_, devGetBlobList);
        DS_ASSERT_OK(client->DevSubscribe(inObjectKeys, devGetBlobList, futures));
        for (auto &fut : futures) {
            DS_ASSERT_OK(fut.Get());
        }
        retCode = 0;
        DS_ASSERT_OK(IsSameContent(devGetBlobList, devSetBlobList, 'b'));
        LOG(INFO) << "Sub ok xxxxxxxxxxxx";
    }
    DS_ASSERT_TRUE(WaitForChildFork(subPid), 0);
    DS_ASSERT_TRUE(WaitForChildFork(pubPid), 0);
};

TEST_F(HeteroGetMetaInfoTest, DevMGetWithGetMetaSize)
{
    size_t numOfObjs = 10;
    std::vector<std::string> inObjectKeys, inObjectKeysBig, failedKeys, noExistKey = { "jfsdioa", "jfdoiqwe" };
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    int64_t retCode = -1;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
    }
    int randomLow = 10, randomHigh = 100;
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> distrib(randomLow, randomHigh);
    auto gen = [&mt, &distrib]() -> int { return distrib(mt); };
    auto initFunc = [this, &client](int deviceId) {
        deviceId_ = deviceId;
        InitAcl(deviceId_);
        InitTestHeteroClient(deviceId_ % DEFAULT_WORKER_NUM, client);
    };
    auto descFunc = [this, &client, &retCode]() {
        AclDeviceManager::Instance()->aclrtResetDevice(deviceId_);
        AclDeviceManager::Instance()->aclFinalize();
        client.reset();
        exit(retCode);
    };
    auto pubPid = fork();
    if (pubPid == 0) {
        initFunc(0);
        RandomFill(numOfObjs, gen, 0, devSetBlobList, 'b');
        DS_ASSERT_OK(IsSameContent(devSetBlobList, devSetBlobList, 'b'));
        Raii raii([&descFunc]() { descFunc(); });
        DS_ASSERT_OK(client->DevMSet(inObjectKeys, devSetBlobList, failedKeys));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        retCode = 0;
        LOG(INFO) << "Pub ok xxxxxxxxxxxx";
        sleep(LONG_WAIT_TIME);
    }
    auto subPid = fork();
    if (subPid == 0) {
        initFunc(1);
        RandomFill(numOfObjs, gen, 0, devSetBlobList, 'b');
        Raii raii([&descFunc]() { descFunc(); });
        sleep(SHORT_WAIT_TIME);
        std::vector<MetaInfo> metaInfos;
        DS_ASSERT_OK(client->GetMetaInfo(inObjectKeys, true, metaInfos, failedKeys));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        std::vector<std::vector<uint64_t>> blobSizes;
        std::transform(metaInfos.begin(), metaInfos.end(), std::back_inserter(blobSizes),
                       [](const MetaInfo &metaInfo) { return metaInfo.blobSizeList; });
        DS_ASSERT_TRUE(!blobSizes.empty(), true);
        AllocAsBlobSize(blobSizes, deviceId_, devGetBlobList);
        DS_ASSERT_OK(client->DevMGet(inObjectKeys, devGetBlobList, failedKeys));
        retCode = 0;
        DS_ASSERT_OK(IsSameContent(devGetBlobList, devSetBlobList, 'b'));
        LOG(INFO) << "Sub ok xxxxxxxxxxxx";
    }
    DS_ASSERT_TRUE(WaitForChildFork(subPid), 0);
    DS_ASSERT_TRUE(WaitForChildFork(pubPid), 0);
};

TEST_F(HeteroGetMetaInfoTest, MGetWithGetMetaSize)
{
    size_t numOfObjs = 10;
    std::vector<std::string> inObjectKeys, inObjectKeysBig, failedKeys, noExistKey = { "jfsdioa", "jfdoiqwe" };
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    int64_t retCode = -1;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
    }
    int randomLow = 10, randomHigh = 100;
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> distrib(randomLow, randomHigh);
    auto gen = [&mt, &distrib]() -> int { return distrib(mt); };
    auto initFunc = [this, &client](int deviceId) {
        deviceId_ = deviceId;
        InitAcl(deviceId_);
        InitTestHeteroClient(deviceId_ % DEFAULT_WORKER_NUM, client);
    };
    auto descFunc = [this, &client, &retCode]() {
        AclDeviceManager::Instance()->aclrtResetDevice(deviceId_);
        AclDeviceManager::Instance()->aclFinalize();
        client.reset();
        exit(retCode);
    };
    auto setPid = fork();
    if (setPid == 0) {
        initFunc(0);
        RandomFill(numOfObjs, gen, 0, devSetBlobList, 'b');
        Raii raii([&descFunc]() { descFunc(); });
        DS_ASSERT_OK(client->MSetD2H(inObjectKeys, devSetBlobList));
        retCode = 0;
    }
    auto getPid = fork();
    if (getPid == 0) {
        sleep(LONG_WAIT_TIME);
        initFunc(1);
        RandomFill(numOfObjs, gen, 0, devSetBlobList, 'b');
        Raii raii([&descFunc]() { descFunc(); });
        std::vector<MetaInfo> metaInfos;
        DS_ASSERT_OK(client->GetMetaInfo(inObjectKeys, false, metaInfos, failedKeys));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        std::vector<std::vector<uint64_t>> blobSizes;
        std::transform(metaInfos.begin(), metaInfos.end(), std::back_inserter(blobSizes),
                       [](const MetaInfo &metaInfo) { return metaInfo.blobSizeList; });
        DS_ASSERT_TRUE(!blobSizes.empty(), true);
        AllocAsBlobSize(blobSizes, deviceId_, devGetBlobList);
        DS_ASSERT_OK(client->MGetH2D(inObjectKeys, devGetBlobList, failedKeys, 0));
        retCode = 0;
        DS_ASSERT_OK(IsSameContent(devGetBlobList, devSetBlobList, 'b'));
    }
    DS_ASSERT_TRUE(WaitForChildFork(setPid), 0);
    DS_ASSERT_TRUE(WaitForChildFork(getPid), 0);
};

TEST_F(HeteroGetMetaInfoTest, GetMetaInfoPerf)
{
    size_t blkSz = 10, numOfObjs = 200, blksPerObj = 10;
    std::vector<std::string> inObjectKeys, inObjectKeysP2p, failedKeys;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
        inObjectKeysP2p.emplace_back(FormatString("p2pkey_%s", j));
    }
    auto initFunc = [this, &devGetBlobList, &devSetBlobList, &client, &blkSz, &numOfObjs, &blksPerObj](int deviceId) {
        deviceId_ = deviceId;
        InitAcl(deviceId_);
        InitTestHeteroClient(deviceId_ % DEFAULT_WORKER_NUM, client);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);
    };
    initFunc(0);
    DS_ASSERT_OK(client->DevMSet(inObjectKeysP2p, devSetBlobList, failedKeys));
    DS_ASSERT_OK(client->MSetD2H(inObjectKeys, devSetBlobList));
    Timer timer;
    //  single get ;
    for (auto &key : inObjectKeys) {
        std::vector<MetaInfo> metaInfos;
        DS_ASSERT_OK(client->GetMetaInfo({ key }, false, metaInfos, failedKeys));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
    }
    auto durationSingle = timer.ElapsedMilliSecond();
    timer.Reset();
    for (auto &key : inObjectKeysP2p) {
        std::vector<MetaInfo> metaInfos;
        DS_ASSERT_OK(client->GetMetaInfo({ key }, true, metaInfos, failedKeys));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
    }
    auto durationSingleP2p = timer.ElapsedMilliSecond();
    //  batch get ;
    std::vector<MetaInfo> metaInfos;
    timer.Reset();
    DS_ASSERT_OK(client->GetMetaInfo(inObjectKeys, false, metaInfos, failedKeys));
    DS_ASSERT_TRUE(failedKeys.empty(), true);
    auto durationBatch = timer.ElapsedMilliSecond();
    metaInfos.clear();
    timer.Reset();
    DS_ASSERT_OK(client->GetMetaInfo(inObjectKeysP2p, true, metaInfos, failedKeys));
    auto durationBatchP2p = timer.ElapsedMilliSecond();
    LOG(INFO) << "---------------- PERF RESULT: ---------------- ";
    LOG(INFO) << FormatString("Get %s keys seperately, cost: %s ms", numOfObjs, durationSingle);
    LOG(INFO) << FormatString("Get %s p2p keys seperately, cost: %s ms", numOfObjs, durationSingleP2p);
    LOG(INFO) << FormatString("Get %s keys batch, cost: %s ms", numOfObjs, durationBatch);
    LOG(INFO) << FormatString("Get %s p2p keys batch, cost: %s ms", numOfObjs, durationBatchP2p);
}

TEST_F(HeteroGetMetaInfoTest, GetMetaInfoMultiThreadPerf)
{
    int numOfThreads = 8;
    size_t blkSz = 10, numOfObjs = 200, blksPerObj = 10;
    std::vector<std::string> inObjectKeys, inObjectKeysP2p, failedKeys;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
        inObjectKeysP2p.emplace_back(FormatString("p2pkey_%s", j));
    }
    auto initFunc = [this, &devGetBlobList, &devSetBlobList, &client, &blkSz, &numOfObjs, &blksPerObj](int deviceId) {
        deviceId_ = deviceId;
        InitAcl(deviceId_);
        InitTestHeteroClient(deviceId_ % DEFAULT_WORKER_NUM, client);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);
    };
    initFunc(0);
    DS_ASSERT_OK(client->DevMSet(inObjectKeysP2p, devSetBlobList, failedKeys));
    DS_ASSERT_OK(client->MSetD2H(inObjectKeys, devSetBlobList));
    ThreadPool threadPool(numOfThreads, numOfThreads);
    //  single get ;
    Timer timer;
    std::vector<std::future<void>> results;
    for (auto &key : inObjectKeys) {
        results.emplace_back(threadPool.Submit([&key, &client, &failedKeys]() -> void {
            std::vector<MetaInfo> metaInfos;
            DS_ASSERT_OK(client->GetMetaInfo({ key }, false, metaInfos, failedKeys));
            DS_ASSERT_TRUE(failedKeys.empty(), true);
        }));
    }
    for (auto &res : results) {
        res.get();
    }
    auto durationSingle = timer.ElapsedMilliSecond();
    results.clear();

    timer.Reset();
    for (auto &key : inObjectKeysP2p) {
        results.emplace_back(threadPool.Submit([&key, &client, &failedKeys]() -> void {
            std::vector<MetaInfo> metaInfos;
            DS_ASSERT_OK(client->GetMetaInfo({ key }, true, metaInfos, failedKeys));
            DS_ASSERT_TRUE(failedKeys.empty(), true);
        }));
    }
    for (auto &res : results) {
        res.get();
    }
    auto durationSingleP2p = timer.ElapsedMilliSecond();
    //  batch get ;
    std::vector<MetaInfo> metaInfos;
    std::vector<std::vector<std::string>> inObjectKeysSlices(numOfThreads), inObjectKeysSlicesP2p(numOfThreads);
    for (size_t i = 0; i < numOfObjs; i++) {
        inObjectKeysSlices[i % numOfThreads].emplace_back(inObjectKeys[i]);
        inObjectKeysSlicesP2p[i % numOfThreads].emplace_back(inObjectKeysP2p[i]);
    }
    results.clear();
    timer.Reset();
    for (int i = 0; i < numOfThreads; i++) {
        results.emplace_back(threadPool.Submit([&inObjectKeysSlices, i, &client, &failedKeys]() -> void {
            std::vector<MetaInfo> metaInfos;
            DS_ASSERT_OK(client->GetMetaInfo(inObjectKeysSlices[i], false, metaInfos, failedKeys));
            DS_ASSERT_TRUE(failedKeys.empty(), true);
        }));
    }
    for (auto &res : results) {
        res.get();
    }
    auto durationBatch = timer.ElapsedMilliSecond();
    metaInfos.clear();
    results.clear();
    timer.Reset();
    for (int i = 0; i < numOfThreads; i++) {
        results.emplace_back(threadPool.Submit([&inObjectKeysSlicesP2p, i, &client, &failedKeys]() -> void {
            std::vector<MetaInfo> metaInfos;
            DS_ASSERT_OK(client->GetMetaInfo(inObjectKeysSlicesP2p[i], true, metaInfos, failedKeys));
            DS_ASSERT_TRUE(failedKeys.empty(), true);
        }));
    }
    for (auto &res : results) {
        res.get();
    }
    auto durationBatchP2p = timer.ElapsedMilliSecond();
    LOG(INFO) << "---------------- PERF RESULT: ---------------- ";
    LOG(INFO) << FormatString("Get %s keys seperately, threads: %s, cost: %s ms", numOfObjs, numOfThreads,
                              durationSingle);
    LOG(INFO) << FormatString("Get %s p2p keys seperately, threads: %s, cost: %s ms", numOfObjs, numOfThreads,
                              durationSingleP2p);
    LOG(INFO) << FormatString("Get %s keys batch, threads: %s, cost: %s ms", numOfObjs, numOfThreads, durationBatch);
    LOG(INFO) << FormatString("Get %s p2p keys batch, threads: %s, cost: %s ms", numOfObjs, numOfThreads,
                              durationBatchP2p);
};

TEST_F(HeteroGetMetaInfoTest, Exceed10000Key)
{
    size_t blkSz = 10, numOfObjs = 10001, blksPerObj = 1;
    std::vector<std::string> inObjectKeys, failedKeys;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::shared_ptr<HeteroClient> client;
    InitAcl(deviceId_);
    InitTestHeteroClient(deviceId_ % DEFAULT_WORKER_NUM, client);
    PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
    }
    std::vector<Future> futures;
    auto hasStr = [](Status status, std::string str) {
        if (status.GetMsg().find(str) != std::string::npos) {
            return Status::OK();
        }
        return status;
    };
    auto check = [hasStr](Status status) { return hasStr(status, "The objectKeys size exceed 10000"); };

    DS_ASSERT_OK(check(client->MSetD2H(inObjectKeys, devSetBlobList)));
    DS_ASSERT_OK(check(client->MGetH2D(inObjectKeys, devSetBlobList, failedKeys, 0)));
    DS_ASSERT_OK(check(client->DevMSet(inObjectKeys, devSetBlobList, failedKeys)));
    DS_ASSERT_OK(check(client->DevMGet(inObjectKeys, devSetBlobList, failedKeys)));
    DS_ASSERT_OK(check(client->DevPublish(inObjectKeys, devSetBlobList, futures)));
    DS_ASSERT_OK(check(client->DevSubscribe(inObjectKeys, devSetBlobList, futures)));
    DS_ASSERT_OK(check(client->DevDelete(inObjectKeys, failedKeys)));
    DS_ASSERT_OK(check(client->DevLocalDelete(inObjectKeys, failedKeys)));
    inObjectKeys.pop_back();
    devSetBlobList.pop_back();
    devSetBlobList[0].deviceIdx = -1;
    DS_ASSERT_OK(
        hasStr(client->DevPublish(inObjectKeys, devSetBlobList, futures), "Got Error/ABNORMAL device, deviceId: -1"));
};
}  // namespace st
}  // namespace datasystem