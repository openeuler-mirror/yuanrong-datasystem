/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
#include "datasystem/common/log/log.h"
#include "datasystem/hetero_cache/hetero_client.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "device/dev_test_helper.h"

using datasystem::memory::Allocator;
using datasystem::memory::DevMemFuncRegister;

namespace datasystem {
using namespace acl;
namespace st {

class DevObjectHeteroTest : public DevTestHelper {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.workerGflagParams =
            " -v=1 -authorization_enable=true -shared_memory_size_mb=4096 -enable_fallocate=false -arena_per_tenant=2 "
            "-client_dead_timeout_s=15";
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        FLAGS_v = 0;
    }

    void SetUp() override
    {
        const char *ascend_root = std::getenv("ASCEND_HOME_PATH");
        if (ascend_root == nullptr) {
            BINEXPECT_CALL(AclDeviceManager::Instance, ()).WillRepeatedly([]() {
                return AclDeviceManagerMock::Instance();
            });
        }
        deviceId_ = GetDeviceIdFromEnv("DS_TEST_DEVICE_ID", deviceId_);
        LOG(INFO) << "Set deviceId to " << deviceId_;
        std::random_device rd;
        std::mt19937 gen(rd());
        gen_ = gen;
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

public:
    std::string GetTimeCostUsState(std::vector<double> &costVec);

    void SwapInOutPerformanceTest();
    void Sub(const std::vector<std::string> &inObjectKeys, const std::vector<std::string> &strVec, size_t batch,
             std::shared_ptr<HeteroClient> client = nullptr);
    void Pub(const std::vector<std::string> &inObjectKeys, const std::vector<std::string> &strVec, size_t batch,
             std::shared_ptr<HeteroClient> client = nullptr);

    void DeleteDeviceData(std::vector<std::string> &keys, std::shared_ptr<HeteroClient> &client, bool isDelete = false);

    void DevMSetProcess(const std::vector<std::string> &keys, int devId, size_t blkSz, size_t blksPerObj,
                        int &devMsetChild);
    void DevMGetProcess(const std::vector<std::string> &keys, int devId, size_t blkSz, size_t blksPerObj,
                        int &devMgetChild, int32_t timout);

    std::mt19937 gen_;
    int32_t deviceId_ = 3;
};

std::string DevObjectHeteroTest::GetTimeCostUsState(std::vector<double> &costVec)
{
    if (costVec.empty()) {
        return "error";
    }
    std::sort(costVec.begin(), costVec.end());
    double sum = std::accumulate(costVec.begin(), costVec.end(), 0.0);
    float avg = sum / costVec.size();
    std::ostringstream oss;
    // avg, min, max, p50, p90, p99
    const int p50 = 50;
    const int p90 = 90;
    const int p99 = 99;
    const int pmax = 100;
    auto count = costVec.size();
    oss << avg << "," << costVec[0] << "," << costVec[count - 1] << "," << costVec[count * p50 / pmax] << ","
        << costVec[count * p90 / pmax] << "," << costVec[count * p99 / pmax];
    return oss.str();
}

void DevObjectHeteroTest::SwapInOutPerformanceTest()
{
    std::vector<size_t> numObjChoices = { 1, 5, 20 };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };
    std::vector<size_t> blksPerObjList = { 61 };
    std::shared_ptr<HeteroClient> client1;
    std::shared_ptr<HeteroClient> client2;
    InitTestHeteroClient(0, client1);
    InitTestHeteroClient(1, client2);

    auto func = [&](bool warnup) {
        for (size_t choiceIdx = 0; choiceIdx < numObjChoices.size(); choiceIdx++) {
            PerfManager::Instance()->ResetPerfLog();
            for (auto blksPerObj : blksPerObjList) {
                auto blkSz = blkSzChoices[choiceIdx];
                auto numOfObjs = numObjChoices[choiceIdx];

                // Construct Object - Buffer Mappings.
                std::vector<std::string> inObjectKeys;
                std::vector<DeviceBlobList> swapOutBlobList;
                std::vector<DeviceBlobList> swapInBlobList;

                PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);

                for (auto i = 0ul; i < numOfObjs; i++) {
                    inObjectKeys.emplace_back(GetStringUuid());
                }

                std::vector<double> swapOutCost;
                std::vector<double> swapInCost;

                Timer timer;
                // numObjChoices.size() * 4 = 2GB * loopsNum = 4GB, need 4GB > shared_memory_size_mb to check delete
                // success.
                int loopsNum = warnup ? 5 : 200;
                double resultTime = 0;
                auto getAndcheckFunc = [&](std::shared_ptr<HeteroClient> &client) {
                    std::vector<std::string> failedList;
                    DS_ASSERT_OK(client->MGetH2D(inObjectKeys, swapInBlobList, failedList, DEFAULT_GET_TIMEOUT));
                    resultTime = timer.ElapsedMicroSecond();
                    std::string value(swapOutBlobList[0].blobs[0].size, 'a');
                    for (size_t j = 0; j < numOfObjs; j++) {
                        for (size_t k = 0; k < blksPerObj; k++) {
                            CheckDevPtrContent(swapInBlobList[j].blobs[k].pointer, swapInBlobList[j].blobs[k].size,
                                               value);
                        }
                    }
                };

                for (int i = 0; i < loopsNum; i++) {
                    timer.Reset();
                    DS_ASSERT_OK(client1->MSetD2H(inObjectKeys, swapOutBlobList));
                    swapOutCost.push_back(timer.ElapsedMicroSecond());
                    timer.Reset();
                    getAndcheckFunc(client1);
                    swapInCost.push_back(resultTime);
                    // if test remote get need to : timer.Reset();
                    getAndcheckFunc(client2);
                    std::vector<std::string> failedIds;
                    DS_ASSERT_OK(client1->Delete(inObjectKeys, failedIds));
                    ASSERT_TRUE(failedIds.empty());
                }

                if (!warnup) {
                    LOG(ERROR) << numOfObjs << "," << blkSz << "," << blksPerObj << ",MSetD2H,"
                               << GetTimeCostUsState(swapOutCost);
                    LOG(ERROR) << numOfObjs << "," << blkSz << "," << blksPerObj << ",MGetH2D,"
                               << GetTimeCostUsState(swapInCost);
                    PerfManager::Instance()->PrintPerfLog();
                }
            }
        }
    };

    func(true);
    func(false);
}

void DevObjectHeteroTest::Sub(const std::vector<std::string> &inObjectKeys, const std::vector<std::string> &strVec,
                              size_t batch, std::shared_ptr<HeteroClient> client)
{
    int32_t deviceId = 5;
    std::shared_ptr<HeteroClient> localClient;
    if (client == nullptr) {
        InitAcl(deviceId);
        InitTestHeteroClient(0, localClient);
    } else {
        localClient = client;
    }
    std::vector<std::vector<DataInfo>> v;
    for (size_t i = 0; i < batch; i++) {
        std::vector<DataInfo> info;
        void *devPtr = nullptr;
        acl::AclDeviceManager::Instance()->MallocDeviceMemory(strVec[i].size(), devPtr);
        info.emplace_back(DataInfo{ devPtr, DataType::DATA_TYPE_INT8, strVec[i].size(), strVec[i].size(), deviceId });
        v.emplace_back(info);
    }
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<Future> futureVecDeque;

    ConvertToDeviceBlobList(v, swapOutBlobList);
    DS_ASSERT_OK(localClient->DevSubscribe(inObjectKeys, swapOutBlobList, futureVecDeque));
    for (size_t i = 0; i < futureVecDeque.size(); i++) {
        DS_ASSERT_OK(futureVecDeque[i].Get());
    }
    for (size_t i = 0; i < batch; i++) {
        CheckDevPtrContent(swapOutBlobList[i].blobs[0].pointer, swapOutBlobList[i].blobs[0].size, strVec[i]);
    }
}

void DevObjectHeteroTest::Pub(const std::vector<std::string> &inObjectKeys, const std::vector<std::string> &strVec,
                              size_t batch, std::shared_ptr<HeteroClient> client)
{
    int32_t deviceId = 4;
    std::shared_ptr<HeteroClient> localClient;
    if (client == nullptr) {
        InitAcl(deviceId);
        InitTestHeteroClient(1, localClient);
    } else {
        localClient = client;
    }
    std::vector<std::vector<DataInfo>> v;
    for (size_t i = 0; i < batch; i++) {
        void *devPtr = nullptr;
        std::vector<DataInfo> info;
        acl::AclDeviceManager::Instance()->MallocDeviceMemory(strVec[i].size(), devPtr);
        acl::AclDeviceManager::Instance()->MemCopyH2D(devPtr, strVec[i].size(), strVec[i].data(), strVec[i].size());
        info.emplace_back(DataInfo{ devPtr, DataType::DATA_TYPE_INT8, strVec[i].size(), strVec[i].size(), deviceId });
        v.emplace_back(info);
    }
    std::vector<DeviceBlobList> swapInBlobList;
    std::vector<Future> futureVecEnque;
    ConvertToDeviceBlobList(v, swapInBlobList);
    DS_ASSERT_OK(localClient->DevPublish(inObjectKeys, swapInBlobList, futureVecEnque));
    for (size_t i = 0; i < futureVecEnque.size(); i++) {
        DS_ASSERT_OK(futureVecEnque[i].Get());
    }
}

void DevObjectHeteroTest::DevMSetProcess(const std::vector<std::string> &keys, int devId, size_t blkSz,
                                         size_t blksPerObj, int &devMsetChild)
{
    devMsetChild = fork();
    if (devMsetChild == 0) {
        std::vector<DeviceBlobList> setBlobList;
        std::vector<DeviceBlobList> getBlobList;
        std::vector<std::string> failedList;
        this->deviceId_ = devId;
        InitAcl(deviceId_);
        PrePareDevData(keys.size(), blksPerObj, blkSz, setBlobList, getBlobList, deviceId_);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(keys, setBlobList, failedList));
        DS_ASSERT_TRUE(failedList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(LONG_WAIT_TIME));
        DS_ASSERT_OK(client->DevLocalDelete(keys, failedList));
        client->ShutDown();
        exit(0);
    }
}

void DevObjectHeteroTest::DevMGetProcess(const std::vector<std::string> &keys, int devId, size_t blkSz,
                                         size_t blksPerObj, int &devMgetChild, int32_t timout)
{
    devMgetChild = fork();
    if (devMgetChild == 0) {
        std::vector<DeviceBlobList> setBlobList;
        std::vector<DeviceBlobList> getBlobList;
        std::vector<std::string> failedList;
        this->deviceId_ = devId;
        InitAcl(deviceId_);
        PrePareDevData(keys.size(), blksPerObj, blkSz, setBlobList, getBlobList, deviceId_);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(1, client);
        auto expectContent = std::string(blkSz, 'b');
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        DS_ASSERT_OK(client->DevMGet(keys, getBlobList, failedList, timout));
        client->ShutDown();
        exit(0);
    }
}

void DevObjectHeteroTest::DeleteDeviceData(std::vector<std::string> &keys, std::shared_ptr<HeteroClient> &client,
                                           bool isDelete)
{
    std::vector<std::string> failedIdList;
    if (isDelete) {
        DS_ASSERT_OK(client->DevDelete(keys, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
    }
    DS_ASSERT_OK(client->ShutDown());
}

TEST_F(DevObjectHeteroTest, DISABLED_TestDevMSetAndDevMGetInSameRank)
{
    size_t deviceId = 0;
    size_t blkSz = 100, numOfObjs = 10, blksPerObj = 5;
    std::vector<std::string> objectIds, failedIdList;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        objectIds.emplace_back(GetStringUuid());
    }
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &devGetBlobList, &devSetBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);
    };
    initDev(deviceId);
    InitTestHeteroClient(0, client);
    DS_ASSERT_OK(client->DevMSet(objectIds, devSetBlobList, failedIdList));
    ASSERT_EQ(failedIdList.size(), 0);
    auto getRes = client->DevMGet(objectIds, devGetBlobList, failedIdList, RPC_TIMEOUT);
    ASSERT_EQ(failedIdList.size(), 0);
    std::string value(devSetBlobList[0].blobs[0].size, 'b');
    for (size_t j = 0; j < numOfObjs; j++) {
        for (size_t k = 0; k < blksPerObj; k++) {
            CheckDevPtrContent(devGetBlobList[j].blobs[k].pointer, devGetBlobList[j].blobs[k].size, value);
        }
    }
    AclDeviceManager::Instance()->aclrtResetDevice(deviceId_);
    AclDeviceManager::Instance()->aclFinalize();
    client.reset();
}

TEST_F(DevObjectHeteroTest, DISABLED_TestDevMGetPartDataFromTwoKey)
{
    size_t blkSz = 100, numOfObjs = 2, blksPerObj = 1;
    std::vector<std::string> objectIds, failedIdList;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        objectIds.emplace_back(GetStringUuid());
    }
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &devGetBlobList, &devSetBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);
    };
    auto child1 = ForkForTest([&]() {
        int deviceId = 7;
        initDev(deviceId);
        InitTestHeteroClient(0, client);
        std::vector<std::string> subObjectIds(objectIds.begin(), objectIds.begin() + int(numOfObjs / 2));
        const std::vector<DeviceBlobList> subDevSetBlobList(devSetBlobList.begin(),
                                                            devSetBlobList.begin() + int(numOfObjs / 2));
        DS_ASSERT_OK(client->DevMSet(subObjectIds, subDevSetBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        client.reset();
        AclDeviceManager::Instance()->aclFinalize();
    });
    auto child2 = ForkForTest([&]() {
        int deviceId = 6;
        initDev(deviceId);
        InitTestHeteroClient(0, client);
        std::vector<std::string> subObjectIds(objectIds.begin() + int(numOfObjs / 2), objectIds.end());
        const std::vector<DeviceBlobList> subDevSetBlobList(devSetBlobList.begin() + int(numOfObjs / 2),
                                                            devSetBlobList.end());
        DS_ASSERT_OK(client->DevMSet(subObjectIds, subDevSetBlobList, failedIdList));
        ASSERT_EQ(failedIdList.size(), 0);
        DS_ASSERT_OK(client->DevMGet(objectIds, devGetBlobList, failedIdList, RPC_TIMEOUT));
        ASSERT_EQ(failedIdList.size(), 0);
        std::string value(devSetBlobList[0].blobs[0].size, 'b');
        for (size_t j = 0; j < numOfObjs; j++) {
            for (size_t k = 0; k < blksPerObj; k++) {
                CheckDevPtrContent(devGetBlobList[j].blobs[k].pointer, devGetBlobList[j].blobs[k].size, value);
            }
        }
        client.reset();
        AclDeviceManager::Instance()->aclFinalize();
    });
    DS_ASSERT_TRUE(WaitForChildFork(child1), 0);
    DS_ASSERT_TRUE(WaitForChildFork(child2), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestDevMGetPartData)
{
    size_t blkSz = 10, numOfObjs = 1, blksPerObj = 1;
    std::vector<std::string> objectIds, failedIdList;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        objectIds.emplace_back(GetStringUuid());
    }
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &devGetBlobList, &devSetBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);
    };
    auto child1 = ForkForTest([&]() {
        int deviceId = 7;
        initDev(deviceId);
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(objectIds, devSetBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(LONG_WAIT_TIME));
        AclDeviceManager::Instance()->aclFinalize();
    });
    auto child2 = ForkForTest([&]() {
        int deviceId = 6;
        initDev(deviceId);
        InitTestHeteroClient(0, client);
        int32_t srcOffset = blkSz / 2;
        for (auto &devBlob : devGetBlobList) {
            devBlob.srcOffset = srcOffset;
            for (auto &blob : devBlob.blobs) {
                blob.size = blob.size - srcOffset;
            }
        }
        DS_ASSERT_OK(client->DevMGet(objectIds, devGetBlobList, failedIdList, RPC_TIMEOUT));
        ASSERT_EQ(failedIdList.size(), 0);
        std::string value(devGetBlobList[0].blobs[0].size, 'b');
        for (size_t j = 0; j < numOfObjs; j++) {
            for (size_t k = 0; k < blksPerObj; k++) {
                CheckDevPtrContent(devGetBlobList[j].blobs[k].pointer, devGetBlobList[j].blobs[k].size, value);
            }
        }
        AclDeviceManager::Instance()->aclFinalize();
        client.reset();
    });
    DS_ASSERT_TRUE(WaitForChildFork(child1), 0);
    DS_ASSERT_TRUE(WaitForChildFork(child2), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestDevMGetWithDiffSizeBlobs)
{
    size_t blkSz = 10, numOfObjs = 1, blksPerObj = 2;
    std::vector<std::string> objectIds, failedIdList;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        objectIds.emplace_back(GetStringUuid());
    }
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &devGetBlobList, &devSetBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);

        std::vector<std::vector<DataInfo>> swapOutDataInfoList;
        std::vector<std::vector<DataInfo>> swapInDatInfoList;

        // Swap Out HBM.
        DevPtrQueue swapOutDataInfoQueue1;
        swapOutDataInfoQueue1.Fill(numOfObjs, blksPerObj / 2, DataType::DATA_TYPE_INT8, blkSz / 2, deviceId_, '1');
        DevPtrQueue swapOutDataInfoQueue2;
        swapOutDataInfoQueue2.Fill(numOfObjs, blksPerObj / 2, DataType::DATA_TYPE_INT8, blkSz, deviceId_, '2');

        // Swap In HBM.
        DevPtrQueue resultDataInfoQueue1;
        resultDataInfoQueue1.Fill(numOfObjs, blksPerObj / 2, DataType::DATA_TYPE_INT8, blkSz / 2, deviceId_);
        DevPtrQueue resultDataInfoQueue2;
        resultDataInfoQueue2.Fill(numOfObjs, blksPerObj / 2, DataType::DATA_TYPE_INT8, blkSz, deviceId_);

        for (auto i = 0uL; i < numOfObjs; i++) {
            std::vector<DataInfo> tmpInfos;
            swapOutDataInfoQueue1.Get(tmpInfos);
            swapOutDataInfoList.emplace_back(std::move(tmpInfos));
            tmpInfos.clear();
            swapOutDataInfoQueue2.Get(tmpInfos);
            std::move(tmpInfos.begin(), tmpInfos.end(), std::back_inserter(swapOutDataInfoList[0]));

            tmpInfos.clear();
            resultDataInfoQueue1.Get(tmpInfos);
            swapInDatInfoList.emplace_back(std::move(tmpInfos));
            tmpInfos.clear();
            resultDataInfoQueue2.Get(tmpInfos);
            std::move(tmpInfos.begin(), tmpInfos.end(), std::back_inserter(swapInDatInfoList[0]));
        }

        ConvertToDeviceBlobList(swapOutDataInfoList, devSetBlobList);
        ConvertToDeviceBlobList(swapInDatInfoList, devGetBlobList);
    };
    auto child1 = ForkForTest([&]() {
        int deviceId = 7;
        initDev(deviceId);
        InitTestHeteroClient(0, client);

        DS_ASSERT_OK(client->DevMSet(objectIds, devSetBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        AclDeviceManager::Instance()->aclFinalize();
    });
    auto child2 = ForkForTest([&]() {
        int deviceId = 6;
        initDev(deviceId);
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMGet(objectIds, devGetBlobList, failedIdList, RPC_TIMEOUT));
        ASSERT_EQ(failedIdList.size(), 0);

        std::string value1(devGetBlobList[0].blobs[0].size, '1');
        CheckDevPtrContent(devGetBlobList[0].blobs[0].pointer, devGetBlobList[0].blobs[0].size, value1);
        std::string value2(devGetBlobList[0].blobs[1].size, '2');
        CheckDevPtrContent(devGetBlobList[0].blobs[1].pointer, devGetBlobList[0].blobs[1].size, value2);
        AclDeviceManager::Instance()->aclFinalize();
        client.reset();
    });
    DS_ASSERT_TRUE(WaitForChildFork(child1), 0);
    DS_ASSERT_TRUE(WaitForChildFork(child2), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestDevMSetAndDevMGetPartDataInSameRank)
{
    size_t blkSz = 100, numOfObjs = 10, blksPerObj = 10;
    std::vector<std::string> objectIds, failedIdList;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        objectIds.emplace_back(GetStringUuid());
    }
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &devGetBlobList, &devSetBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);
    };
    int deviceId = 5;
    initDev(deviceId);
    InitTestHeteroClient(0, client);
    DS_ASSERT_OK(client->DevMSet(objectIds, devSetBlobList, failedIdList));
    DS_ASSERT_TRUE(failedIdList.empty(), true);

    int32_t srcOffset = blkSz / 2;
    for (auto &devBlob : devGetBlobList) {
        devBlob.srcOffset = srcOffset;
        for (auto &blob : devBlob.blobs) {
            blob.size = blob.size - srcOffset;
        }
    }
    DS_ASSERT_OK(client->DevMGet(objectIds, devGetBlobList, failedIdList, RPC_TIMEOUT));
    ASSERT_EQ(failedIdList.size(), 0);
    std::string value(devGetBlobList[0].blobs[0].size, 'b');
    for (size_t j = 0; j < numOfObjs; j++) {
        for (size_t k = 0; k < blksPerObj; k++) {
            CheckDevPtrContent(devGetBlobList[j].blobs[k].pointer, devGetBlobList[j].blobs[k].size, value);
        }
    }
    AclDeviceManager::Instance()->aclFinalize();
    client.reset();
}

TEST_F(DevObjectHeteroTest, DISABLED_HostSwapDataTest)
{
    std::shared_ptr<HeteroClient> client0;
    InitTestHeteroClient(0, client0);

    InitAcl(deviceId_);

    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };

    for (size_t vecIdx = 0; vecIdx < numObjChoices.size(); vecIdx++) {
        auto blkSz = blkSzChoices[vecIdx];
        auto numOfObjs = numObjChoices[vecIdx];
        size_t blksPerObj = 31;

        std::shared_ptr<HeteroClient> client1;
        std::shared_ptr<HeteroClient> client2;
        InitTestHeteroClient(0, client1);
        InitTestHeteroClient(1, client2);

        std::vector<DeviceBlobList> setBlobListUseless;
        std::vector<DeviceBlobList> getBlobList;
        PrePareDevData(numOfObjs, blksPerObj, blkSz, setBlobListUseless, getBlobList, deviceId_);

        std::vector<std::string> inObjectIds;
        for (auto i = 0ul; i < numOfObjs; i++) {
            inObjectIds.emplace_back(GetStringUuid());
        }

        int loopsNum = 3;
        for (int i = 0; i < loopsNum; i++) {
            std::vector<DeviceBlobList> setBlobList;
            std::vector<std::vector<std::string>> verifyList;
            PrePareRandomData(numOfObjs, blksPerObj, blkSz, deviceId_, setBlobList, verifyList);
            DS_ASSERT_OK(client1->MSetD2H(inObjectIds, setBlobList));
            std::vector<std::string> failedList;
            DS_ASSERT_OK(client2->MGetH2D(inObjectIds, getBlobList, failedList, DEFAULT_GET_TIMEOUT));
            ASSERT_TRUE(failedList.empty());
            for (size_t j = 0; j < numOfObjs; j++) {
                for (size_t k = 0; k < blksPerObj; k++) {
                    CheckDevPtrContent(getBlobList[j].blobs[k].pointer, getBlobList[j].blobs[k].size, verifyList[j][k]);
                }
            }
            DS_ASSERT_OK(client1->Delete(inObjectIds, failedList));
            ASSERT_TRUE(failedList.empty());
        }
    }
}

TEST_F(DevObjectHeteroTest, DISABLED_TestAsyncMSetD2H)
{
    size_t deviceId = 0;
    size_t blkSz = 1 * 1024 * 1024;
    size_t numOfObjs = 1;
    size_t totalSz = 10 * 1024 * 1024;
    size_t blksPerObj = totalSz / blkSz / numOfObjs;
    std::uniform_int_distribution<> distrib(0, numOfObjs - 1);

    InitAcl(deviceId);

    std::shared_ptr<DsClient> client;
    InitTestDsClient(0, client);
    std::shared_ptr<HeteroClient> heteroClient = client->Hetero();

    std::vector<std::string> inObjectKeys;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;

    PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId);

    for (auto i = 0ul; i < numOfObjs; i++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    Timer timer;
    std::shared_future<AsyncResult> future = heteroClient->AsyncMSetD2H(inObjectKeys, swapOutBlobList);
    auto costMs = timer.ElapsedMilliSecond();
    DS_ASSERT_OK(future.get().status);
    ASSERT_LE(costMs, 1);
    int randomNum = distrib(gen_);

    std::vector<std::string> failedList;
    int getTimeoutMs = 100;
    DS_ASSERT_OK(heteroClient->MGetH2D(inObjectKeys, swapInBlobList, failedList, getTimeoutMs));
    ASSERT_TRUE(failedList.empty());
    std::string value(swapOutBlobList[0].blobs[0].size, 'a');
    for (size_t j = 0; j < numOfObjs; j++) {
        if ((size_t)randomNum == j) {
            continue;
        }
        for (size_t k = 0; k < blksPerObj; k++) {
            CheckDevPtrContent(swapInBlobList[j].blobs[k].pointer, swapInBlobList[j].blobs[k].size, value);
        }
    }

    std::vector<std::string> failedIds;
    DS_ASSERT_OK(heteroClient->Delete(inObjectKeys, failedIds));
    ASSERT_TRUE(failedIds.empty());
}

TEST_F(DevObjectHeteroTest, DISABLED_TestSwapInAndOutWithAcl)
{
    InitAcl(deviceId_);
    SwapInOutPerformanceTest();
}

TEST_F(DevObjectHeteroTest, DISABLED_AllocateDeviceMemTest)
{
    auto allocateFunc = [](void **ptr, size_t maxSize) -> Status {
        AclDeviceManager::Instance()->aclrtMallocHost(&(*ptr), maxSize);
        return Status::OK();
    };

    auto destroyFunc = [](void *ptr, size_t maxSize) -> Status {
        (void)maxSize;
        AclDeviceManager::Instance()->aclrtFreeHost(ptr);
        return Status::OK();
    };

    auto *allocator = Allocator::Instance();
    struct DevMemFuncRegister regFunc;
    regFunc.devDeviceCreateFunc = allocateFunc;
    regFunc.devDeviceDestroyFunc = destroyFunc;
    regFunc.devHostCreateFunc = allocateFunc;
    regFunc.devHostDestroyFunc = destroyFunc;

    uint64_t maxSize1 = 65 * 1024ul * 1024ul;
    uint64_t maxSize2 = 64 * 1024ul * 1024ul;

    DS_ASSERT_OK(AclDeviceManager::Instance()->aclInit(nullptr));
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(deviceId_));

    allocator->InitWithoutShm(maxSize1, maxSize2, regFunc);

    ShmUnit shmUnit;
    auto clearFunc = [&shmUnit]() {
        shmUnit.pointer = nullptr;
        shmUnit.fd = -1;
        shmUnit.offset = 0;
        shmUnit.mmapSize = 0;
        shmUnit.size = 0;
    };

    int loopNums = 5;
    for (int i = 0; i < loopNums; i++) {
        std::string tenantId1 = "tenant1";
        auto rc = shmUnit.AllocateMemory(tenantId1, maxSize2 - 1, false, datasystem::memory::CacheType::DEV_HOST);
        LOG(INFO) << "allocate info " << rc.ToString();
        auto freeRc = shmUnit.FreeMemory();
        LOG(INFO) << "Free result is : " << freeRc.ToString();
        clearFunc();
    }
}

TEST_F(DevObjectHeteroTest, DISABLED_TestPartOfMGet)
{
    InitAcl(deviceId_);
    auto blkSz = 1 * 1024;
    auto numOfObjs = 16u;
    auto totalSz = 16 * 512 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;
    std::uniform_int_distribution<> distrib(0, numOfObjs - 1);

    std::shared_ptr<DsClient> client;
    InitTestDsClient(0, client);
    std::shared_ptr<HeteroClient> heteroClient = client->Hetero();

    std::vector<std::string> inObjectKeys;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;

    PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);

    for (auto i = 0ul; i < numOfObjs; i++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }

    DS_ASSERT_OK(heteroClient->MSetD2H(inObjectKeys, swapOutBlobList));
    std::vector<std::string> failedIds;
    int randomNum = distrib(gen_);
    DS_ASSERT_OK(heteroClient->Delete({ inObjectKeys[randomNum] }, failedIds));

    std::vector<std::string> failedList;
    int getTimeoutMs = 100;
    DS_ASSERT_OK(heteroClient->MGetH2D(inObjectKeys, swapInBlobList, failedList, getTimeoutMs));
    ASSERT_EQ(failedList.size(), 1);
    ASSERT_EQ(failedList[0], inObjectKeys[randomNum]);
    std::string value(swapOutBlobList[0].blobs[0].size, 'a');
    for (size_t j = 0; j < numOfObjs; j++) {
        if ((size_t)randomNum == j) {
            continue;
        }
        for (size_t k = 0; k < blksPerObj; k++) {
            CheckDevPtrContent(swapInBlobList[j].blobs[k].pointer, swapInBlobList[j].blobs[k].size, value);
        }
    }
    swapInBlobList[0].blobs.pop_back();
    DS_ASSERT_OK(StausHasStr(heteroClient->MGetH2D(inObjectKeys, swapInBlobList, failedList, getTimeoutMs),
                             "Blobs count mismatch in devBlobList between sender and receiver"));
    DS_ASSERT_OK(heteroClient->Delete(inObjectKeys, failedIds));
    ASSERT_TRUE(failedIds.empty());
}

TEST_F(DevObjectHeteroTest, DISABLED_MultiCreateFailedInWorker)
{
    InitAcl(deviceId_);

    size_t blkSz = 1 * 1024 * 1024;
    size_t numOfObjs = 128;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;

    std::shared_ptr<HeteroClient> client;
    InitTestHeteroClient(0, client);

    std::vector<std::string> inObjectKeys;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);

    for (auto i = 0ul; i < numOfObjs; i++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "WorkerOCServiceImpl.MultiCreate.Allocate",
                                           "10*call(8)"));
    DS_ASSERT_NOT_OK(client->MSetD2H(inObjectKeys, swapOutBlobList));
    std::vector<std::string> failedList;
    DS_ASSERT_NOT_OK(client->MGetH2D(inObjectKeys, swapInBlobList, failedList, 1));

    failedList.clear();
    DS_ASSERT_OK(client->MSetD2H(inObjectKeys, swapOutBlobList));
    DS_ASSERT_OK(client->MGetH2D(inObjectKeys, swapInBlobList, failedList, DEFAULT_GET_TIMEOUT));

    std::string value(swapOutBlobList[0].blobs[0].size, 'a');
    for (size_t j = 0; j < numOfObjs; j++) {
        for (size_t k = 0; k < blksPerObj; k++) {
            CheckDevPtrContent(swapInBlobList[j].blobs[k].pointer, swapInBlobList[j].blobs[k].size, value);
        }
    }
    failedList.clear();
    client->Delete(inObjectKeys, failedList);
}

TEST_F(DevObjectHeteroTest, DISABLED_MultiCreateFailedInClient)
{
    InitAcl(deviceId_);

    size_t blkSz = 128 * 1024;
    size_t numOfObjs = 1024;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;

    std::shared_ptr<HeteroClient> localClient;
    InitTestHeteroClient(0, localClient);

    std::vector<std::string> inObjectKeys;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);

    for (auto i = 0ul; i < numOfObjs; i++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }

    std::vector<std::string> failedList;
    // Check local Put failed
    DS_ASSERT_OK(inject::Set("ObjectClientImpl.MultiCreate.mmapFailed", "10*call(8)"));
    DS_ASSERT_NOT_OK(localClient->MSetD2H(inObjectKeys, swapOutBlobList));
    DS_ASSERT_NOT_OK(localClient->MGetH2D(inObjectKeys, swapInBlobList, failedList, 1));
}

std::string GetQPS(std::vector<double> &costVec, double transSize)
{
    double sum = std::accumulate(costVec.begin(), costVec.end(), 0.0);
    float avg = sum / costVec.size();

    return std::to_string((transSize) / (avg / 1e6));
}

TEST_F(DevObjectHeteroTest, DISABLED_MGetWithoutPrefetch)
{
    InitAcl(deviceId_);
    std::shared_ptr<HeteroClient> client;
    InitTestHeteroClient(0, client);

    std::vector<DeviceBlobList> testList;

    void *ptr = nullptr;
    size_t size = 1024;
    AclDeviceManager::Instance()->aclrtMalloc(&ptr, size, ACL_MEM_MALLOC_HUGE_FIRST);
    std::vector<Blob> testBs{ { .pointer = ptr, .size = size } };
    testList.emplace_back(DeviceBlobList{ .blobs = testBs, .deviceIdx = deviceId_ });

    std::string id = GetStringUuid();
    std::vector<std::string> failedKeys;
    uint64_t timeout = 1000;
    DS_ASSERT_NOT_OK(client->DevMGet({ id }, testList, failedKeys, timeout));
}

TEST_F(DevObjectHeteroTest, DISABLED_MSetD2HEmptyKey)
{
    InitAcl(deviceId_);
    size_t blkSz = 128 * 1024;
    size_t numOfObjs = 1024;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;
    std::shared_ptr<HeteroClient> localClient;
    InitTestHeteroClient(0, localClient);
    std::vector<std::string> inObjectKeys;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);
    DS_ASSERT_NOT_OK(localClient->MSetD2H(inObjectKeys, swapOutBlobList));
    auto res = localClient->MSetD2H(inObjectKeys, {});
    DS_ASSERT_NOT_OK(res);
    LOG(ERROR) << res.ToString();
    auto pos = res.GetMsg().find("The keys are empty") != std::string::npos;
    DS_ASSERT_TRUE(pos, true);
}

class DisableDevObjectHeteroTest : public DevTestHelper {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = DEFAULT_WORKER_NUM;
        // -enable_huge_tlb=true -enable_fallocate=false
        opts.workerGflagParams =
            " -v=0 -shared_memory_size_mb=2048 enable_fallocate=false -arena_per_tenant=2 "
            "-enable_fallocate=false ";
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        FLAGS_v = 0;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

public:
    int deviceId_ = 4;
};

TEST_F(DisableDevObjectHeteroTest, DISABLED_TestAclInit)
{
#ifdef BUILD_HETERO
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclInit(nullptr));
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(deviceId_));
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(deviceId_));
#else
    DS_ASSERT_NOT_OK(AclDeviceManager::Instance()->aclInit(nullptr));
#endif
}

TEST_F(DevObjectHeteroTest, DISABLED_TestAsyncDevDelete)
{
    size_t blkSz = 10;
    size_t numOfObjs = 10;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys;
    std::vector<std::string> failedIdList;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    std::shared_ptr<HeteroClient> client;
    auto subTimeOutMs = 5000;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &swapOutBlobList, &swapInBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);
    };
    auto devMsetChild = fork();
    if (devMsetChild == 0) {
        initDev(0);
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(inObjectKeys, swapInBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        client.reset();
        exit(0);
    }
    auto prefetchChild = fork();
    if (prefetchChild == 0) {
        initDev(1);
        InitTestHeteroClient(1, client);
        DS_ASSERT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, subTimeOutMs));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        DS_ASSERT_OK(IsSameContent(swapOutBlobList, swapInBlobList, 'b'));
        std::shared_future<AsyncResult> future = client->AsyncDevDelete(inObjectKeys);
        AsyncResult rc = future.get();
        DS_ASSERT_OK(rc.status);
        DS_ASSERT_TRUE(rc.failedList.empty(), true);

        DS_ASSERT_NOT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, subTimeOutMs));
        client.reset();
        exit(0);
    }
    DS_ASSERT_TRUE(WaitForChildFork(prefetchChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestPubSub)
{
    std::random_device randomDevice;
    std::mt19937 gen(randomDevice());
    int strLenLow = 1;
    int strLenHig = 72 * 1024;
    std::uniform_int_distribution<> distribute(strLenLow, strLenHig);
    size_t batch = 100;
    std::vector<std::string> inObjectKeys;
    for (auto i = 0ul; i < batch; i++) {
        inObjectKeys.emplace_back("object_---------------" + std::to_string((i)));
    }
    const int num = 1;
    DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(5000)"));
    RandomData random;
    for (int i = 0; i < num; i++) {
        std::vector<std::string> strVec;
        for (size_t batchIndex = 0; batchIndex < batch; batchIndex++) {
            strVec.emplace_back(random.GetRandomString(distribute(gen)));
        }
        auto enqueChild = fork();
        if (enqueChild == 0) {
            Pub(inObjectKeys, strVec, batch);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            exit(0);
        }
        auto dequeChild = fork();
        if (dequeChild == 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            Sub(inObjectKeys, strVec, batch);
            exit(0);
        }
        DS_ASSERT_TRUE(WaitForChildFork(enqueChild), 0);
        DS_ASSERT_TRUE(WaitForChildFork(dequeChild), 0);
    }
}

TEST_F(DevObjectHeteroTest, DISABLED_PubSubLongTest)
{
    size_t batchNum = 1;  // Number of test rounds to execute

    size_t operatorCount = 500;  // Number of Pub/Sub calls per round, waiting for transmission to complete
    size_t keysCountPerOp = 1;   // Number of Keys processed per Pub/Sub operation
    size_t blobNum = 1;          // Number of Blobs held by each Key
    // [operatorCount, keysCountPerOp, blockNum]
    DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(50)"));
    DeviceBlobListHelper helper = DeviceBlobListHelper(operatorCount, keysCountPerOp, blobNum);

    auto enqueChild = ForkForTest([&]() {
        int32_t deviceId = 4;
        deviceId = GetDeviceIdFromEnv("DS_TEST_DEVICE_ID_PUB", deviceId);
        InitAcl(deviceId);
        std::shared_ptr<HeteroClient> localClient;
        InitTestHeteroClient(1, localClient);
        for (size_t batchIdx = 0; batchIdx < batchNum; batchIdx++) {
            // [operatorCount, keysCountPerOps]
            std::vector<std::vector<DeviceBlobList>> blob3D = helper.MallocDeviceBlobList(deviceId, true);
            for (size_t operIdx = 0; operIdx < operatorCount; operIdx++) {
                std::vector<Future> futureVecEnque;
                DS_ASSERT_OK(localClient->DevPublish(helper.keyVecs[operIdx], blob3D[operIdx], futureVecEnque));
                for (size_t i = 0; i < futureVecEnque.size(); i++) {
                    DS_ASSERT_OK(futureVecEnque[i].Get());
                }
            }
            helper.FreeDeviceBlobList(blob3D);
        }
    });

    auto dequeChild = ForkForTest([&]() {
        int32_t deviceId = 5;
        deviceId = GetDeviceIdFromEnv("DS_TEST_DEVICE_ID_SUB", deviceId);
        InitAcl(deviceId);
        std::shared_ptr<HeteroClient> localClient;
        InitTestHeteroClient(0, localClient);

        for (size_t batchIdx = 0; batchIdx < batchNum; batchIdx++) {
            // [operatorCount, keysCountPerOps]
            std::vector<std::vector<DeviceBlobList>> blob3D = helper.MallocDeviceBlobList(deviceId, false);
            for (size_t operIdx = 0; operIdx < operatorCount; operIdx++) {
                std::vector<Future> futureVecEnque;
                DS_ASSERT_OK(localClient->DevSubscribe(helper.keyVecs[operIdx], blob3D[operIdx], futureVecEnque));
                for (size_t i = 0; i < futureVecEnque.size(); i++) {
                    DS_ASSERT_OK(futureVecEnque[i].Get());
                }
                for (size_t keyIdx = 0; keyIdx < keysCountPerOp; keyIdx++) {
                    for (size_t blobIdx = 0; blobIdx < blobNum; blobIdx++) {
                        auto blob = blob3D[operIdx][keyIdx].blobs[blobIdx];
                        CheckDevPtrContent(blob.pointer, blob.size, helper.dataVecs[operIdx][keyIdx][blobIdx]);
                    }
                }
            }
            helper.FreeDeviceBlobList(blob3D);
        }
    });
    DS_ASSERT_TRUE(WaitForChildFork(enqueChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(dequeChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestDeviceClientDestructorTime)
{
    std::random_device randomDevice;
    std::mt19937 gen(randomDevice());
    int strLenLow = 1, strLenHig = 20000;
    std::uniform_int_distribution<> distribute(strLenLow, strLenHig);
    size_t batch = 1;
    std::vector<std::string> inObjectKeys;
    for (auto i = 0ul; i < batch; i++) {
        inObjectKeys.emplace_back("object" + std::to_string((i)));
    }

    // Set the event subscription time to 80s.
    DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(5000)"));
    DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.slowlyTimeout", "2*off->call(80000)"));

    std::vector<std::string> strVec;
    for (size_t i = 0; i < batch; i++) {
        auto str = std::string(distribute(gen), 'a');
        strVec.emplace_back(std::move(str));
    }
    auto enqueChild = fork();
    if (enqueChild == 0) {
        std::shared_ptr<HeteroClient> localClient;
        Pub(inObjectKeys, strVec, batch, localClient);
        Timer timer;
        localClient.reset();
        auto clientDestructorTime = timer.ElapsedSecond();
        int maxTime = 3;
        ASSERT_LE(clientDestructorTime, maxTime);
        exit(0);
    }
    auto dequeChild = fork();
    if (dequeChild == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::shared_ptr<HeteroClient> localClient;
        Sub(inObjectKeys, strVec, batch, localClient);
        Timer timer;
        localClient.reset();
        auto clientDestructorTime = timer.ElapsedSecond();
        int maxTime = 3;
        ASSERT_LE(clientDestructorTime, maxTime);
        exit(0);
    }
    DS_ASSERT_TRUE(WaitForChildFork(enqueChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(dequeChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestReconnectWithPubsub)
{
    std::random_device randomDevice;
    std::mt19937 gen(randomDevice());
    int strLenLow = 1, strLenHig = 2000;
    std::uniform_int_distribution<> distribute(strLenLow, strLenHig);
    size_t batch = 1;
    std::vector<std::string> inObjectIds;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    for (auto i = 0ul; i < batch; i++) {
        inObjectIds.emplace_back("object_---------------" + std::to_string((i)));
    }

    const int num = 1;
    for (int i = 0; i < num; i++) {
        std::vector<std::string> strVec;
        for (size_t j = 0; j < batch; j++) {
            auto str = std::string(distribute(gen), 'a');
            strVec.emplace_back(std::move(str));
        }
        auto enqueChild = fork();
        if (enqueChild == 0) {
            int32_t deviceId = 4;
            InitAcl(deviceId);
            std::shared_ptr<HeteroClient> keepClient;
            InitTestHeteroClient(1, keepClient);
            Pub(inObjectIds, strVec, batch, keepClient);

            std::vector<std::string> failedIdList;
            DS_ASSERT_OK(keepClient->DevDelete(inObjectIds, failedIdList));
            DS_ASSERT_TRUE(failedIdList.empty(), true);

            Pub(inObjectIds, strVec, batch, keepClient);
            exit(0);
        }
        auto dequeChild1 = fork();
        if (dequeChild1 == 0) {
            Sub(inObjectIds, strVec, batch);
            exit(0);
        }
        // Wait for the client in the dequeChild1 process to exit.
        DS_ASSERT_TRUE(WaitForChildFork(dequeChild1), 0);

        auto dequeChild2 = fork();
        if (dequeChild2 == 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            Sub(inObjectIds, strVec, batch);
            exit(0);
        }
        DS_ASSERT_TRUE(WaitForChildFork(dequeChild2), 0);
        DS_ASSERT_TRUE(WaitForChildFork(enqueChild), 0);
    }
}

TEST_F(DevObjectHeteroTest, DISABLED_TestReplicateDevMSet)
{
    size_t blkSz = 10;
    size_t numOfObjs = 1;
    auto blksPerObj = 10;
    std::vector<std::string> objectIds;
    objectIds.emplace_back(GetStringUuid());
    std::vector<std::string> failedIdList;
    std::vector<DeviceBlobList> devGetBlobList;
    std::vector<DeviceBlobList> devSetBlobList;
    std::shared_ptr<HeteroClient> client;
    DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(1000)"));
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &devGetBlobList, &devSetBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);
    };
    auto devMsetChild = fork();
    if (devMsetChild == 0) {
        initDev(0);
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(objectIds, devSetBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(LONG_WAIT_TIME));
        DeleteDeviceData(objectIds, client);
        exit(0);
    }
    auto prefetchChild = fork();
    if (prefetchChild == 0) {
        initDev(1);
        InitTestHeteroClient(1, client);
        auto expectContent = std::string(blkSz, 'b');
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        DS_ASSERT_OK(client->DevMGet(objectIds, devGetBlobList, failedIdList, RPC_TIMEOUT));
        for (auto &devBlobList : devGetBlobList) {
            for (auto &blob : devBlobList.blobs) {
                CheckDevPtrContent(blob.pointer, blkSz, expectContent);
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(LONG_WAIT_TIME));
        DeleteDeviceData(objectIds, client, true);
        std::this_thread::sleep_for(std::chrono::seconds(MINI_WAIT_TIME));
        exit(0);
    }
    DS_ASSERT_TRUE(WaitForChildFork(prefetchChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
    initDev(1);
    InitTestHeteroClient(0, client);
    DS_ASSERT_OK(client->DevMSet(objectIds, devSetBlobList, failedIdList));
    DS_ASSERT_TRUE(failedIdList.empty(), true);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestPrefetchGetLocalDelete)
{
    size_t blkSz = 10;
    size_t numOfObjs = 10;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys;
    std::vector<std::string> failedIdList;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    auto deletObjs = [&inObjectKeys, &failedIdList](std::shared_ptr<HeteroClient> &client) {
        DS_ASSERT_OK(client->DevLocalDelete(inObjectKeys, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        client->ShutDown();
    };
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &swapOutBlobList, &swapInBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);
    };
    auto devMsetChild = fork();
    if (devMsetChild == 0) {
        initDev(0);
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(inObjectKeys, swapInBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(LONG_WAIT_TIME));
        deletObjs(client);
        exit(0);
    }
    auto prefetchChild = fork();
    if (prefetchChild == 0) {
        initDev(1);
        InitTestHeteroClient(1, client);
        auto expectContent = std::string(blkSz, 'b');
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(2000)"));
        DS_ASSERT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, RPC_TIMEOUT));
        for (auto &devBlobList : swapOutBlobList) {
            for (auto &blob : devBlobList.blobs) {
                CheckDevPtrContent(blob.pointer, blkSz, expectContent);
            }
        }
        deletObjs(client);
        exit(0);
    }
    DS_ASSERT_TRUE(WaitForChildFork(prefetchChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_DevMGetWithoutTimeout)
{
    size_t numOfObjs = 10;
    std::vector<std::string> keys;
    for (auto j = 0ul; j < numOfObjs; j++) {
        keys.emplace_back(GetStringUuid());
    }
    int devMsetChild;
    int devMgetChild;

    DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(2000)"));
    size_t blkSz = 10;
    size_t blksPerObj = 10;
    DevMSetProcess(keys, 0, blkSz, blksPerObj, devMsetChild);
    DevMGetProcess(keys, 1, blkSz, blksPerObj, devMgetChild, 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMgetChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_DevMGetWithAckTimeout)
{
    size_t numOfObjs = 2;
    std::vector<std::string> keys;
    for (auto j = 0ul; j < numOfObjs; j++) {
        keys.emplace_back(GetStringUuid());
    }
    int devMsetChild = fork();
    if (devMsetChild == 0) {
        std::vector<DeviceBlobList> setBlobList;
        std::vector<DeviceBlobList> getBlobList;
        InitAcl(0);
        PrePareDevData(keys.size(), 1, 1, setBlobList, getBlobList, 0);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(0, client, 2000);  // Init hetero client with 2000 timeout
        std::vector<std::string> failedList;
        // Timing 1: location is INIT
        DS_ASSERT_OK(client->DevMSet(keys, setBlobList, failedList));
        DS_ASSERT_TRUE(failedList.empty(), true);
        sleep(1);  // wait 1s for dev mget
        // Timing 3: location from IN_USE to PRE_REMOVE
        LOG_IF_ERROR(client->DevLocalDelete(keys, failedList), "dev local delete failed");
        exit(0);
    }
    int devMgetChild = fork();
    if (devMgetChild == 0) {
        std::vector<DeviceBlobList> setBlobList;
        std::vector<DeviceBlobList> getBlobList;
        std::vector<std::string> failedList;
        InitAcl(1);
        PrePareDevData(keys.size(), 1, 1, setBlobList, getBlobList, 1);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(1, client, 2000);  // Init hetero client with 2000 timeout
        DS_ASSERT_OK(datasystem::inject::Set("P2PSubscribe.AsyncGet.timeout", "call(3000)"));
        DS_ASSERT_OK(datasystem::inject::Set("P2PSubscribe.GetEvent.failed", "call()"));
        // Timing 2: location from INIT to IN_USE
        DS_ASSERT_NOT_OK(client->DevMGet(keys, getBlobList, failedList, 5000));  // subTimeoutMs is 5000
        DS_ASSERT_OK(client->DevDelete(keys, failedList));
        DS_ASSERT_TRUE(failedList.empty(), true);
        client->ShutDown();
        exit(0);
    }
    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMgetChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_DevDelWhenPreRemove)
{
    size_t numOfObjs = 2;
    std::vector<std::string> keys;
    for (auto j = 0ul; j < numOfObjs; j++) {
        keys.emplace_back(GetStringUuid());
    }

    int devMsetChild = fork();
    if (devMsetChild == 0) {
        std::vector<DeviceBlobList> setBlobList;
        std::vector<DeviceBlobList> getBlobList;
        InitAcl(0);
        PrePareDevData(keys.size(), 1, 1, setBlobList, getBlobList, 0);
        DS_ASSERT_OK(datasystem::inject::Set("ClientWorkerApi.FillDevObjMeta.setState", "call()"));
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(0, client);
        std::vector<std::string> failedList;
        DS_ASSERT_OK(client->DevMSet(keys, setBlobList, failedList));
        DS_ASSERT_TRUE(failedList.empty(), true);
        sleep(2);  // wait 2s for dev delete
        client.reset();
        exit(0);
    }

    int devDelChild = fork();
    if (devDelChild == 0) {
        sleep(1);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(1, client);
        DeleteDeviceData(keys, client, true);
        exit(0);
    }

    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devDelChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestGetAfterLocalDelete)
{
    size_t blkSz = 10;
    size_t numOfObjs = 1;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys;
    std::vector<std::string> failedIdList;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    // inject will cause a coredump, comment it out for now.
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    auto deletObjs = [&inObjectKeys, &failedIdList](std::shared_ptr<HeteroClient> &client) {
        DS_ASSERT_OK(client->DevLocalDelete(inObjectKeys, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
    };
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &swapOutBlobList, &swapInBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);
    };
    auto devMsetChild = ForkForTest([&] {
        initDev(0);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(inObjectKeys, swapInBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(LONG_WAIT_TIME));
        deletObjs(client);
    });
    auto prefetchChild = ForkForTest([&] {
        initDev(1);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(1, client);
        auto expectContent = std::string(blkSz, 'b');
        DS_ASSERT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, RPC_TIMEOUT));
        for (auto &devBlobList : swapOutBlobList) {
            for (auto &blob : devBlobList.blobs) {
                CheckDevPtrContent(blob.pointer, blkSz, expectContent);
            }
        }
        deletObjs(client);
        DS_ASSERT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, RPC_TIMEOUT));
    });
    DS_ASSERT_TRUE(WaitForChildFork(prefetchChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestGetAfterDevDelete)
{
    size_t blkSz = 10;
    size_t numOfObjs = 1;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys;
    std::vector<std::string> failedIdList;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    auto deletObjs = [&inObjectKeys, &failedIdList](std::shared_ptr<HeteroClient> &client) {
        DS_ASSERT_OK(client->DevDelete(inObjectKeys, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
    };
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &swapOutBlobList, &swapInBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);
    };
    auto setPid = ForkForTest([&] {
        initDev(0);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(inObjectKeys, swapInBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(LONG_WAIT_TIME));
    });
    auto getPid = ForkForTest([&] {
        initDev(1);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(1, client);
        auto expectContent = std::string(blkSz, 'b');
        auto subTimeout = 10000;
        DS_ASSERT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, subTimeout));
        // inject SubscribeEventTypePb.LIFECYCLE_EXIT_NOTIFICATION", "sleep(2)"  will coredump
        deletObjs(client);
        subTimeout = 0;
        auto ret = client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, subTimeout);
        DS_ASSERT_NOT_OK(ret);
        auto hasPos = ret.GetMsg().find("can't find objects") != std::string::npos;
        DS_ASSERT_TRUE(hasPos, true);
    });
    DS_ASSERT_TRUE(WaitForChildFork(getPid), 0);
    DS_ASSERT_TRUE(WaitForChildFork(setPid), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_DevEmptyKeyTest)
{
    int32_t devId = 1;
    this->deviceId_ = devId;
    InitAcl(deviceId_);

    size_t blkSz = 10;
    size_t numOfObjs = 10;
    auto blksPerObj = 10;
    std::vector<DeviceBlobList> devGetBlobList;
    std::vector<DeviceBlobList> devSetBlobList;
    PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceId_);

    std::vector<std::string> inObjectKeys;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    std::vector<std::string> failedIdList;
    std::vector<DeviceBlobList> devBlobList;
    InitTestHeteroClient(0, client);
    DS_ASSERT_NOT_OK(client->DevMSet({}, devSetBlobList, failedIdList));
    DS_ASSERT_NOT_OK(client->DevMSet(inObjectKeys, {}, failedIdList));
    DS_ASSERT_NOT_OK(client->DevMSet({}, {}, failedIdList));
    DS_ASSERT_NOT_OK(client->DevMGet({}, devBlobList, failedIdList));
    std::vector<Future> futureVecDeque;
    DS_ASSERT_NOT_OK(client->DevPublish(inObjectKeys, {}, futureVecDeque));
    DS_ASSERT_NOT_OK(client->DevPublish({}, devSetBlobList, futureVecDeque));
    DS_ASSERT_NOT_OK(client->DevSubscribe(inObjectKeys, {}, futureVecDeque));
    DS_ASSERT_NOT_OK(client->DevSubscribe({}, devGetBlobList, futureVecDeque));
    DS_ASSERT_NOT_OK(client->DevPublish({}, {}, futureVecDeque));
    DS_ASSERT_NOT_OK(client->DevSubscribe({}, {}, futureVecDeque));
    DS_ASSERT_OK(client->DevMSet(inObjectKeys, devSetBlobList, failedIdList));

    std::vector<DeviceBlobList> invalidDevGetBlobList;
    std::vector<DeviceBlobList> invalidDevSetBlobList;
    int32_t invalidDevice = 99;
    PrePareDevData(numOfObjs, blksPerObj, blkSz, invalidDevGetBlobList, invalidDevSetBlobList, invalidDevice);
    DS_ASSERT_NOT_OK(client->DevMSet(inObjectKeys, invalidDevSetBlobList, failedIdList));
    DS_ASSERT_NOT_OK(client->DevMGet(inObjectKeys, invalidDevGetBlobList, failedIdList));

    DS_ASSERT_TRUE(failedIdList.empty(), true);

    DS_ASSERT_OK(client->DevLocalDelete(inObjectKeys, failedIdList));
    DS_ASSERT_TRUE(failedIdList.empty(), true);
    client->ShutDown();
}

TEST_F(DevObjectHeteroTest, DISABLED_TestLocalDeleteAll)
{
    size_t blkSz = 10;
    size_t numOfObjs = 10;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys;
    std::vector<std::string> failedIdList;
    std::vector<DeviceBlobList> swapOutBlobList;
    std::vector<DeviceBlobList> swapInBlobList;
    auto subTimeOutMs = 5000;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    auto deletObjs = [&inObjectKeys, &failedIdList](std::shared_ptr<HeteroClient> &client) {
        DS_ASSERT_OK(client->DevLocalDelete(inObjectKeys, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
    };
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &swapOutBlobList, &swapInBlobList](int id) {
        this->deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);
    };
    auto devMsetChild = ForkForTest([&] {
        initDev(0);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(inObjectKeys, swapInBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        deletObjs(client);
        DS_ASSERT_NOT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, subTimeOutMs));
    });
    auto prefetchChild = ForkForTest([&] {
        initDev(1);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(1, client);
        DS_ASSERT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, subTimeOutMs));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        DS_ASSERT_OK(IsSameContent(swapOutBlobList, swapInBlobList, 'b'));
        deletObjs(client);
        std::this_thread::sleep_for(std::chrono::seconds(SHORT_WAIT_TIME));
        DS_ASSERT_NOT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, subTimeOutMs));
    });
    DS_ASSERT_TRUE(WaitForChildFork(prefetchChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_TestPartDevMSet)
{
    size_t blkSz = 10;
    size_t numOfObjs = 10;
    auto blksPerObj = 10;
    std::vector<std::string> inObjectKeys, failedIdList;
    std::vector<DeviceBlobList> swapOutBlobList, swapInBlobList;
    auto subLongTimeOutMs = 20000;
    for (auto j = 0ul; j < numOfObjs; j++) {
        inObjectKeys.emplace_back(GetStringUuid());
    }
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &swapOutBlobList, &swapInBlobList](int id) {
        deviceId_ = id;
        InitAcl(deviceId_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);
    };
    auto devMsetChild = ForkForTest([&] {
        initDev(0);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(0, client);
        auto twoSecond = 2;
        auto key3Offset = 2;
        std::vector<std::string> objectKey1{ inObjectKeys.begin(), inObjectKeys.begin() + 1 };
        std::vector<std::string> objectKey2{ inObjectKeys.begin() + 1, inObjectKeys.begin() + key3Offset };
        std::vector<std::string> objectKey3{ inObjectKeys.begin() + key3Offset, inObjectKeys.end() };
        std::vector<DeviceBlobList> swapIn1{ swapInBlobList.begin(), swapInBlobList.begin() + 1 };
        std::vector<DeviceBlobList> swapIn2{ swapInBlobList.begin() + 1, swapInBlobList.begin() + key3Offset };
        std::vector<DeviceBlobList> swapIn3{ swapInBlobList.begin() + key3Offset, swapInBlobList.end() };
        std::this_thread::sleep_for(std::chrono::seconds(twoSecond));
        DS_ASSERT_OK(client->DevMSet(objectKey1, swapIn1, failedIdList));
        std::this_thread::sleep_for(std::chrono::seconds(twoSecond));
        DS_ASSERT_OK(client->DevMSet(objectKey2, swapIn2, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(twoSecond));
        DS_ASSERT_OK(client->DevMSet(objectKey3, swapIn3, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        std::this_thread::sleep_for(std::chrono::seconds(twoSecond));
    });
    auto prefetchChild = ForkForTest([&] {
        initDev(1);
        std::shared_ptr<HeteroClient> client;
        InitTestHeteroClient(1, client);
        DS_ASSERT_OK(client->DevMGet(inObjectKeys, swapOutBlobList, failedIdList, subLongTimeOutMs));
        DS_ASSERT_TRUE(failedIdList.empty(), true);
        DS_ASSERT_OK(IsSameContent(swapOutBlobList, swapInBlobList, 'b'));
    });
    DS_ASSERT_TRUE(WaitForChildFork(prefetchChild), 0);
    DS_ASSERT_TRUE(WaitForChildFork(devMsetChild), 0);
}

TEST_F(DevObjectHeteroTest, DISABLED_AccessLog)
{
    auto blkSz = 1 * 1024 * 1024;
    auto numOfObjs = 2u;
    auto totalSz = 512 * 1024 * 1024u;
    auto blksPerObj = totalSz / blkSz / numOfObjs;
    std::vector<std::vector<std::string>> inValidList{ { "##&%$#xx", "##&%$#" }, {} };
    std::string invalidKey = "##&%$#xx";
    std::vector<DeviceBlobList> swapOutBlobList, swapInBlobList;
    std::shared_ptr<HeteroClient> client;
    std::vector<std::string> failedKeys;
    std::vector<Future> futureVec;
    deviceId_ = 0;
    InitAcl(deviceId_);
    InitTestHeteroClient(0, client);
    PrePareDevData(numOfObjs, blksPerObj, blkSz, swapOutBlobList, swapInBlobList, deviceId_);
    for (auto &inValidKeys : inValidList) {
        DS_ASSERT_NOT_OK(client->Delete(inValidKeys, failedKeys));
        DS_ASSERT_NOT_OK(client->DevLocalDelete(inValidKeys, failedKeys));
        DS_ASSERT_NOT_OK(client->DevDelete(inValidKeys, failedKeys));
        DS_ASSERT_NOT_OK(client->DevMGet(inValidKeys, swapInBlobList, failedKeys));
        DS_ASSERT_NOT_OK(client->DevMSet(inValidKeys, swapInBlobList, failedKeys));
        DS_ASSERT_NOT_OK(client->MSetD2H(inValidKeys, swapInBlobList));
        DS_ASSERT_NOT_OK(client->MGetH2D(inValidKeys, swapInBlobList, failedKeys, 0));
        DS_ASSERT_NOT_OK(client->DevPublish(inValidKeys, swapInBlobList, futureVec));
        DS_ASSERT_NOT_OK(client->DevSubscribe(inValidKeys, swapInBlobList, futureVec));
    }
    DS_ASSERT_OK(client->GenerateKey("abcd", invalidKey));
    DS_ASSERT_OK(client->ShutDown());
}

TEST_F(DevObjectHeteroTest, LEVEL1_WorkerLostHealthCheckTest)
{
    std::shared_ptr<HeteroClient> client;
    InitTestHeteroClient(0, client);
    ServerState state;
    DS_ASSERT_OK(client->HealthCheck(state));
    ASSERT_EQ(state, ServerState::NORMAL);

    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    sleep(MINI_WAIT_TIME);

    Timer timer;
    DS_ASSERT_NOT_OK(client->HealthCheck(state));
    auto timeCost = timer.ElapsedSecond();
    double checkTime = 10;
    // HealthCheck timeout is 3s, Considering that CI sometime executes slowly.
    ASSERT_LE(timeCost, checkTime);

    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    // ensure client send next heartbeat
    sleep(MINI_WAIT_TIME);
    state = ServerState::NORMAL;
    DS_ASSERT_OK(client->HealthCheck(state));
    ASSERT_EQ(state, ServerState::REBOOT);
    // ServerState will reset after get.
    DS_ASSERT_OK(client->HealthCheck(state));
    ASSERT_EQ(state, ServerState::NORMAL);
}
}  // namespace st
}  // namespace datasystem
