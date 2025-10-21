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
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
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

class EdgeConditionDevObjectHeteroTest : public DevTestHelper {
    void SetUp() override
    {
        DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(8000)"));
        const char *ascend_root = std::getenv("ASCEND_HOME_PATH");
        if (ascend_root == nullptr) {
            BINEXPECT_CALL(AclDeviceManager::Instance, ()).WillRepeatedly([]() {
                return AclDeviceManagerMock::Instance();
            });
        }
        ExternalClusterTest::SetUp();
    };

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

public:
    void MixTest(
        std::vector<std::vector<std::string>> &tests,
        std::map<std::string, std::vector<std::vector<DeviceBlobList>>> &mp,
        std::function<void(std::vector<std::string> &objectKeysMix, std::vector<DeviceBlobList> &swapOutBlobListMix,
                           std::vector<DeviceBlobList> &swapInBlobListMix)>
            testFunc)
    {
        for (auto &test : tests) {
            LOG(INFO) << "test:" << test[0] << "  " << test[1];
            std::vector<DeviceBlobList> swapIn, swapOut;
            bool shuf = test.back() == "shuf";
            if (shuf) {
                test.pop_back();
            }
            for (auto size : test) {
                auto swapOutkey = FormatString("size%s_0", size);
                auto swapInkey = FormatString("size%s_1", size);
                auto swapInList = mp[swapInkey].back();
                auto swapOutList = mp[swapOutkey].back();
                mp[swapInkey].pop_back();
                mp[swapOutkey].pop_back();
                swapIn.insert(swapIn.end(), swapInList.begin(), swapInList.end());
                swapOut.insert(swapOut.end(), swapOutList.begin(), swapOutList.end());
            }
            if (shuf) {
                std::mt19937 rng(std::random_device{}());
                auto state = rng();
                rng.seed(state);
                std::shuffle(swapIn.begin(), swapIn.end(), rng);
                rng.seed(state);
                std::shuffle(swapOut.begin(), swapOut.end(), rng);
            }
            std::vector<std::string> keys;
            for (auto j = 0ul; j < swapIn.size(); j++) {
                keys.emplace_back(GetStringUuid());
            }
            testFunc(keys, swapOut, swapIn);
        }
    }
};

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_TestInvalidPubSub)
{
    auto blkSz = 1 * 1024 * 1024;
    auto numOfObjs = 2u;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;
    std::uniform_int_distribution<> distrib(0, numOfObjs - 1);
    std::vector<std::string> invalidKeys{ "75561", "##&%$#" };
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    std::vector<Future> ftlist;
    DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(2000)"));
    auto pubPid = fork();
    if (pubPid == 0) {
        deviceIdx_ = 0;
        InitAcl(deviceIdx_);
        std::shared_ptr<DsClient> client;
        InitTestDsClient(0, client);
        std::shared_ptr<HeteroClient> heteroClient = client->Hetero();
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceIdx_);
        auto invalidateRes = heteroClient->DevPublish(invalidKeys, swapOutBlobList, ftlist);
        DS_ASSERT_NOT_OK(invalidateRes);
        DS_ASSERT_TRUE(ftlist.empty(), true);
        exit(0);
    }
    auto subPid = fork();
    if (subPid == 0) {
        deviceIdx_ = 1;
        InitAcl(deviceIdx_);
        std::shared_ptr<DsClient> client;
        InitTestDsClient(1, client);
        std::shared_ptr<HeteroClient> heteroClient = client->Hetero();
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceIdx_);
        DS_ASSERT_NOT_OK(heteroClient->DevSubscribe(invalidKeys, swapInBlobList, ftlist));
        DS_ASSERT_TRUE(ftlist.empty(), true);
        exit(0);
    }
    DS_ASSERT_TRUE(WaitForChildFork(subPid), 0);
    DS_ASSERT_TRUE(WaitForChildFork(pubPid), 0);
}

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_TestPubRetry)
{
    auto blkSz = 1 * 1024 * 1024;
    auto numOfObjs = 2u;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;
    std::uniform_int_distribution<> distrib(0, numOfObjs - 1);
    std::vector<std::string> validKeys{ "75561", "2233" };
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    std::vector<Future> ftlist, ftlist2;
    DS_ASSERT_OK(datasystem::inject::Set("PublishDeviceObject.PutP2PMeta.Timeout", "1*call()"));
    auto pubPid = fork();
    if (pubPid == 0) {
        deviceIdx_ = 0;
        InitAcl(deviceIdx_);
        std::shared_ptr<DsClient> client;
        InitTestDsClient(0, client);
        std::shared_ptr<HeteroClient> heteroClient = client->Hetero();
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceIdx_);
        DS_ASSERT_NOT_OK(heteroClient->DevPublish(validKeys, swapOutBlobList, ftlist));
        DS_ASSERT_TRUE(ftlist.empty(), true);
        DS_ASSERT_OK(heteroClient->DevPublish(validKeys, swapOutBlobList, ftlist));
        DS_ASSERT_TRUE(ftlist.size(), validKeys.size());
        for (auto &f : ftlist) {
            DS_ASSERT_OK(f.Get());
        }
        exit(0);
    }
    auto subPid = fork();
    if (subPid == 0) {
        deviceIdx_ = 1;
        InitAcl(deviceIdx_);
        std::shared_ptr<DsClient> client;
        InitTestDsClient(1, client);
        std::shared_ptr<HeteroClient> heteroClient = client->Hetero();
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceIdx_);
        DS_ASSERT_OK(heteroClient->DevSubscribe(validKeys, swapInBlobList, ftlist));
        DS_ASSERT_TRUE(ftlist.size(), validKeys.size());
        for (auto &f : ftlist) {
            DS_ASSERT_OK(f.Get());
        }
        for (auto &devBlobList : swapInBlobList) {
            for (auto &blob : devBlobList.blobs) {
                CheckDevPtrContent(blob.pointer, blkSz, std::string(blkSz, 'a'));
            }
        }
        exit(0);
    }
    DS_ASSERT_TRUE(WaitForChildFork(subPid), 0);
    DS_ASSERT_TRUE(WaitForChildFork(pubPid), 0);
}

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_MSetMixSizeData)
{
    std::shared_ptr<HeteroClient> client;
    InitTestHeteroClient(0, client);
    InitAcl(deviceIdx_);
    size_t blkSz = 12784, numOfObjs = 8, blksPerObj = 8;
    std::vector<size_t> sz = { blkSz + 1, blkSz - 1, blkSz };
    std::vector<std::string> failedKeys, szName = { ">500k", "<500k", "=500k" };
    std::vector<std::vector<std::string>> tests = { { ">500k", ">500k" }, { "=500k", "=500k" },
                                                    { "<500k", "<500k" }, { ">500k", "<500k" },
                                                    { "<500k", ">500k" }, { "<500k", ">500k", "shuf" } };
    std::vector<std::vector<DeviceBlobList>> swapInBlobsLists, swapOutBlobsLists;
    std::vector<std::vector<std::string>> testKeyLists;
    std::map<std::string, std::vector<std::vector<DeviceBlobList>>> mp;
    for (size_t k = 0; k < tests.size() * tests[0].size(); k++) {
        for (size_t i = 0; i < sz.size(); i++) {
            std::vector<DeviceBlobList> swapInList, swapOutList;
            PrePareDevData(numOfObjs, blksPerObj, sz[i], swapOutList, swapInList, deviceIdx_);
            auto swapOutkey = FormatString("size%s_0", szName[i]);
            auto swapInkey = FormatString("size%s_1", szName[i]);
            if (mp.find(swapInkey) == mp.end()) {
                mp.insert({ swapInkey, {} });
                mp.insert({ swapOutkey, {} });
            }
            mp[swapInkey].emplace_back(swapInList);
            mp[swapOutkey].emplace_back(swapOutList);
        }
    }

    auto testFunc = [&client, &failedKeys, &blksPerObj](std::vector<std::string> &objectKeysMix,
                                                        std::vector<DeviceBlobList> &swapOutBlobListMix,
                                                        std::vector<DeviceBlobList> &swapInBlobListMix) {
        DS_ASSERT_OK(client->MSetD2H(objectKeysMix, swapOutBlobListMix));
        DS_ASSERT_OK(client->MGetH2D(objectKeysMix, swapInBlobListMix, failedKeys, RPC_TIMEOUT));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        for (size_t j = 0; j < objectKeysMix.size(); j++) {
            for (size_t k = 0; k < blksPerObj; k++) {
                CheckDevPtrContent(swapInBlobListMix[j].blobs[k].pointer, swapInBlobListMix[j].blobs[k].size,
                                   std::string(swapInBlobListMix[j].blobs[k].size, 'a'));
            }
        }
    };
    MixTest(tests, mp, testFunc);
}

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_ErrorMsg)
{
    auto blkSz = 1 * 1024 * 1024;
    auto numOfObjs = 2u;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;
    std::shared_ptr<HeteroClient> client;
    std::vector<std::string> validKeys{ "75561", "66237" }, failedList;
    std::vector<DeviceBlobList> swapOutBlobList, swapInBlobList;
    std::vector<Future> ftlist;
    deviceIdx_ = 0;
    InitAcl(deviceIdx_);
    InitTestHeteroClient(0, client);
    PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceIdx_);
    auto changeDevIdxToNegtive = [](std::vector<DeviceBlobList> &blobList) {
        for (auto &item : blobList) {
            item.deviceIdx = -1;
        }
    };
    changeDevIdxToNegtive(swapOutBlobList);
    DS_ASSERT_OK(StausHasStr(client->DevPublish(validKeys, swapOutBlobList, ftlist), "Got Error/ABNORMAL"));
    DS_ASSERT_OK(StausHasStr(client->DevSubscribe(validKeys, swapOutBlobList, ftlist), "Got Error/ABNORMAL"));
    inject::Set("PublishDeviceObject.PutP2PMeta.Timeout", "call()");
    inject::Set("ObjectClientImpl.Get", "call()");
    inject::Set("ObjectClientImpl.DevLocalDelete", "call()");
    DS_ASSERT_NOT_OK(client->DevMSet(validKeys, swapInBlobList, failedList));
    failedList.clear();
    DS_ASSERT_NOT_OK(client->DevMGet(validKeys, swapInBlobList, failedList));
    failedList.clear();
    DS_ASSERT_NOT_OK(client->DevLocalDelete(validKeys, failedList));
}

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_MGetH2DNotExist)
{
    InitAcl(deviceIdx_);

    size_t blkSz = 128 * 1024;
    size_t numOfObjs = 1;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;
    std::shared_ptr<HeteroClient> localClient;
    InitTestHeteroClient(0, localClient);
    std::vector<std::string> inObjectIds;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceIdx_);
    for (auto i = 0ul; i < numOfObjs; i++) {
        inObjectIds.emplace_back(GetStringUuid());
    }
    std::vector<std::string> failedList;
    DS_ASSERT_NOT_OK(localClient->MGetH2D(inObjectIds, swapInBlobList, failedList, 0));
}

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_DiffDevMsetMGet)
{
    size_t blkSz = 10;
    size_t numOfObjs10 = 10;
    size_t numOfObjs15 = 15;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys10, inObjectKeys15, failedKeys;
    std::vector<DeviceBlobList> devGetBlobList10, devSetBlobList10, devGetBlobList15, devSetBlobList15;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs10; j++) {
        inObjectKeys10.emplace_back(FormatString("key_%s", j));
    }

    for (auto j = 0ul; j < numOfObjs15; j++) {
        inObjectKeys15.emplace_back(FormatString("key_%s", j));
    }
    auto setChild = fork();
    if (setChild == 0) {
        deviceIdx_ = 0;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(0, client);
        PrePareDevData(numOfObjs10, blksPerObj, blkSz, devGetBlobList10, devSetBlobList10, deviceIdx_);
        DS_ASSERT_OK(client->DevMSet({ inObjectKeys10.begin(), inObjectKeys10.end() - 1 },
                                     { devSetBlobList10.begin(), devSetBlobList10.end() - 1 }, failedKeys));
        auto timeSec = 2;
        std::this_thread::sleep_for(std::chrono::seconds(timeSec));
        DS_ASSERT_OK(client->DevMSet({ inObjectKeys10.end() - 1, inObjectKeys10.end() },
                                     { devSetBlobList10.end() - 1, devSetBlobList10.end() }, failedKeys));
        std::this_thread::sleep_for(std::chrono::seconds(timeSec));
        client.reset();
        exit(0);
    }
    auto getChild = fork();
    if (getChild == 0) {
        deviceIdx_ = 1;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(1, client);
        PrePareDevData(numOfObjs15, blksPerObj, blkSz, devGetBlobList15, devSetBlobList15, deviceIdx_);
        auto waitMs = 5000, ms1000 = 1000;
        auto start = std::chrono::high_resolution_clock::now();
        DS_ASSERT_OK(client->DevMGet(inObjectKeys15, devSetBlobList15, failedKeys, waitMs));
        auto end = std::chrono::high_resolution_clock::now() - start;
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end).count();
        auto condition = elapsed >= waitMs && elapsed < waitMs + ms1000;
        DS_ASSERT_TRUE(condition, true);
        auto expectFailNum = 5u;
        DS_ASSERT_TRUE(failedKeys.size(), expectFailNum);
        client.reset();
        exit(0);
    }
    DS_ASSERT_TRUE(WaitForChildFork(setChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(getChild), 0);
}

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_MGetBeforeMset)
{
    size_t blkSz = 128 * 1024;
    size_t numOfObjs = 1;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;
    std::shared_ptr<HeteroClient> client;
    std::vector<std::string> inObjectIds;
    std::vector<DeviceBlobList> swapOutBlobList, swapInBlobList;
    int64_t retCode = -1;
    for (auto i = 0ul; i < numOfObjs; i++) {
        inObjectIds.emplace_back(GetStringUuid());
    }
    std::vector<std::string> failedList;
    auto setpid = fork();
    if (setpid == 0) {
        Raii raii([this, &client, &retCode]() {
            AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_);
            AclDeviceManager::Instance()->aclFinalize();
            client.reset();
            exit(retCode);
        });
        deviceIdx_ = 0;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(0, client);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceIdx_);
        auto ms10000 = 10000;
        DS_ASSERT_OK(client->MGetH2D(inObjectIds, swapInBlobList, failedList, ms10000));
        retCode = 0;
    }

    auto pid = fork();
    if (pid == 0) {
        Raii raii([this, &client, &retCode]() {
            AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_);
            AclDeviceManager::Instance()->aclFinalize();
            client.reset();
            exit(retCode);
        });
        std::this_thread::sleep_for(std::chrono::seconds(MINI_WAIT_TIME));
        deviceIdx_ = 1;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(1, client);
        std::vector<std::string> inObjectKeys;
        std::vector<DeviceBlobList> swapOutBlobList;
        std::vector<DeviceBlobList> swapInBlobList;
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceIdx_);
        DS_ASSERT_OK(client->MSetD2H(inObjectIds, swapOutBlobList));
        retCode = 0;
    }
    DS_ASSERT_TRUE(WaitForChildFork(pid), 0);
    DS_ASSERT_TRUE(WaitForChildFork(setpid), 0);
}

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_Publish2W)
{
    size_t blkSz = 10;
    size_t numOfObjs2W = 20000;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys2W, failedKeys;
    std::vector<DeviceBlobList> devGetBlobList2W, devSetBlobList2W;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    int64_t retCode = -1;
    for (auto j = 0ul; j < numOfObjs2W; j++) {
        inObjectKeys2W.emplace_back(FormatString("key_%s", j));
    }
    auto getChild = fork();
    if (getChild == 0) {
        deviceIdx_ = 0;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(1, client);
        Raii raii([this, &client, &retCode]() {
            AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_);
            AclDeviceManager::Instance()->aclFinalize();
            client.reset();
            exit(retCode);
        });
        PrePareDevData(numOfObjs2W, blksPerObj, blkSz, devGetBlobList2W, devSetBlobList2W, deviceIdx_);
        DS_ASSERT_OK(client->DevSubscribe(inObjectKeys2W, devSetBlobList2W, futures));
        DS_ASSERT_TRUE(futures.size(), numOfObjs2W);
        for (auto &f : futures) {
            DS_ASSERT_OK(f.Get());
        }
        DS_ASSERT_OK(IsSameContent(devGetBlobList2W, devSetBlobList2W, 'a'));
        retCode = 0;
    }
    auto setChild = fork();
    if (setChild == 0) {
        deviceIdx_ = 1;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(0, client);
        Raii raii([this, &client, &retCode]() {
            AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_);
            AclDeviceManager::Instance()->aclFinalize();
            client.reset();
            exit(retCode);
        });
        PrePareDevData(numOfObjs2W, blksPerObj, blkSz, devGetBlobList2W, devSetBlobList2W, deviceIdx_);
        DS_ASSERT_OK(client->DevPublish(inObjectKeys2W, devSetBlobList2W, futures));
        DS_ASSERT_TRUE(futures.size(), numOfObjs2W);
        for (auto &f : futures) {
            DS_ASSERT_OK(f.Get());
        }
        retCode = 0;
    }
    DS_ASSERT_TRUE(WaitForChildFork(setChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(getChild), 0);
};

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_PubSubDiffSize)
{
    size_t blkSz = 10;
    size_t numOfObjs = 20;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys, failedKeys;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    int64_t retCode = -1;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
    }
    auto getChild = fork();
    if (getChild == 0) {
        deviceIdx_ = 0;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(1, client);
        Raii raii([this, &client, &retCode]() {
            AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_);
            AclDeviceManager::Instance()->aclFinalize();
            client.reset();
            exit(retCode);
        });
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceIdx_);
        devSetBlobList[0].blobs.pop_back();
        DS_ASSERT_OK(client->DevSubscribe(inObjectKeys, devSetBlobList, futures));
        auto tag = false;
        for (auto &f : futures) {
            auto res = f.Get();
            if (res.IsError()) {
                DS_ASSERT_TRUE(
                    (res.GetMsg().find("The number of data blobs corresponding to the same key is inconsistent "
                                       "between sender and receiver")
                     != std::string::npos),
                    true);
                tag = true;
            }
        }
        DS_ASSERT_TRUE(tag, true);
        retCode = 0;
    }
    auto setChild = fork();
    if (setChild == 0) {
        deviceIdx_ = 1;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(0, client);
        Raii raii([this, &client, &retCode]() {
            AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_);
            AclDeviceManager::Instance()->aclFinalize();
            client.reset();
            exit(retCode);
        });
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceIdx_);
        DS_ASSERT_OK(client->DevPublish(inObjectKeys, devSetBlobList, futures));
        DS_ASSERT_TRUE(futures.size(), numOfObjs);
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        retCode = 0;
    }
    DS_ASSERT_TRUE(WaitForChildFork(setChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(getChild), 0);
};

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_SubTimeout)
{
    size_t blkSz = 10;
    size_t numOfObjs = 20;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys, failedKeys;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    int64_t retCode = -1;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
    }
    auto getChild = fork();
    if (getChild == 0) {
        deviceIdx_ = 0;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(1, client);
        Raii raii([this, &client, &retCode]() {
            AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_);
            AclDeviceManager::Instance()->aclFinalize();
            client.reset();
            exit(retCode);
        });
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceIdx_);
        inject::Set("GETP2PMeta.subTimeoutMs", "call(1)");
        DS_ASSERT_OK(client->DevSubscribe(inObjectKeys, devSetBlobList, futures));
        for (auto &f : futures) {
            auto res = f.Get();
            DS_ASSERT_TRUE((res.GetMsg().find("can't find objects") != std::string::npos), true);
        }
        retCode = 0;
    }
    DS_ASSERT_TRUE(WaitForChildFork(getChild), 0);
};

TEST_F(EdgeConditionDevObjectHeteroTest, DISABLED_RecvDelayedExitEvent)
{
    size_t blkSz = 10, numOfObjs = 10, blksPerObj = 10;
    std::vector<std::string> inObjectKeys, failedKeys;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::vector<Future> futures;
    std::shared_ptr<HeteroClient> client;
    int64_t retCode = -1;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(FormatString("key_%s", j));
    }
    auto initFunc = [this, &devGetBlobList, &devSetBlobList, &client, &blkSz, &numOfObjs, &blksPerObj](int deviceId) {
        deviceIdx_ = deviceId;
        InitAcl(deviceIdx_);
        InitTestHeteroClient(deviceIdx_ % DEFAULT_WORKER_NUM, client);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceIdx_);
    };
    auto descFunc = [this, &client, &retCode]() {
        AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_);
        AclDeviceManager::Instance()->aclFinalize();
        client.reset();
        exit(retCode);
    };
    auto getChild = fork();
    if (getChild == 0) {
        initFunc(0);
        Raii raii([&descFunc]() { descFunc(); });
        DS_ASSERT_OK(client->DevSubscribe(inObjectKeys, devSetBlobList, futures));
        DS_ASSERT_TRUE(futures.size(), numOfObjs);
        for (auto &f : futures) {
            DS_ASSERT_OK(f.Get());
        }
        DS_ASSERT_OK(IsSameContent(devGetBlobList, devSetBlobList, 'a'));
        client.reset();
        InitTestHeteroClient(deviceIdx_ % DEFAULT_WORKER_NUM, client);
        futures.clear();
        DS_ASSERT_OK(client->DevSubscribe(inObjectKeys, devSetBlobList, futures));
        DS_ASSERT_TRUE(futures.size(), numOfObjs);
        for (auto &f : futures) {
            DS_ASSERT_OK(f.Get());
        }
        retCode = 0;
    }
    auto setChild = fork();
    if (setChild == 0) {
        initFunc(1);
        Raii raii([&descFunc]() { descFunc(); });
        DS_ASSERT_OK(client->DevPublish(inObjectKeys, devSetBlobList, futures));
        DS_ASSERT_TRUE(futures.size(), numOfObjs);
        for (auto &f : futures) {
            DS_ASSERT_OK(f.Get());
        }
        futures.clear();
        inject::Set("SubscribeEventTypePb.LIFECYCLE_EXIT_NOTIFICATION", "1*sleep(3)");
        DS_ASSERT_OK(client->DevDelete(inObjectKeys, failedKeys));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        DS_ASSERT_OK(client->DevPublish(inObjectKeys, devSetBlobList, futures));
        DS_ASSERT_TRUE(futures.size(), numOfObjs);
        for (auto &f : futures) {
            DS_ASSERT_OK(f.Get());
        }
        retCode = 0;
    }
    DS_ASSERT_TRUE(WaitForChildFork(setChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(getChild), 0);
};
}  // namespace st
}  // namespace datasystem
