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
 * Description: Pub sub benchmark.
 */
#include "device/dev_test_helper.h"

namespace datasystem {
using namespace acl;
namespace st {
class HeteroClientPubSubBenchTest : public DevTestHelper {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.workerGflagParams =
            " -v=0 -authorization_enable=true -shared_memory_size_mb=4096 -enable_fallocate=false -arena_per_tenant=2 ";
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        FLAGS_v = 0;
    }

    void SetUp() override
    {
        const char *ascend_root = std::getenv("ASCEND_HOME_PATH");
        if (ascend_root == nullptr) {
            BINEXPECT_CALL(AclDeviceManager::Instance, ()).WillRepeatedly(Return(&managerMock_));
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
    static uint32_t GetDeviceIdFromEnv(const char *envName, uint32_t defaultDeviceId)
    {
        const char *deviceIdStr = std::getenv(envName);
        if (deviceIdStr != nullptr) {
            return std::stoull(deviceIdStr);
        }
        return defaultDeviceId;
    }

    void PubSubPerformanceTest(size_t objectKeyCount, size_t blobNum, size_t blobSize, bool verify, size_t blobsBatch)
    {
        const size_t totalTransfBytes = 30 * 1024 * 1024 * 1024ul;
        size_t batchNum = 1;  // Number of test rounds to execute
        size_t operatorCount =
            std::min<size_t>(std::max<size_t>(totalTransfBytes / objectKeyCount / blobNum / blobSize, 1), 4000);
        LOG(INFO) << "operatorCount:" << operatorCount;

        DeviceBlobListHelper helper = DeviceBlobListHelper(blobsBatch, objectKeyCount, blobNum, blobSize);

        auto enqueChild = ForkForTest([&]() {
            int32_t deviceId = 4;
            deviceId = GetDeviceIdFromEnv("DS_TEST_DEVICE_ID_PUB", deviceId);
            LOG(INFO) << "set device id " << deviceId;
            InitAcl(deviceId);
            std::shared_ptr<HeteroClient> localClient;
            InitTestHeteroClient(0, localClient);
            for (size_t batchIdx = 0; batchIdx < batchNum; batchIdx++) {
                std::vector<std::vector<DeviceBlobList>> blob3D = helper.MallocDeviceBlobList(deviceId, true);
                LOG(INFO) << "start pub...";
                std::vector<double> costVec;
                std::vector<Future> futureAll;
                size_t reqCount = 0;
                Timer timerAll;
                for (size_t operIdx = 0; operIdx < operatorCount; operIdx++) {
                    std::vector<std::string> objectKeys;
                    objectKeys.reserve(objectKeyCount);
                    for (size_t i = 0; i < objectKeyCount; i++) {
                        objectKeys.emplace_back(FormatString("PerfKey-%zu-%zu", operIdx, i));
                    }
                    Timer timer;
                    std::vector<Future> futureVecEnque;
                    size_t blobIndex = operIdx % blobsBatch;
                    DS_ASSERT_OK(localClient->DevPublish(objectKeys, blob3D[blobIndex], futureVecEnque));
                    if (operIdx == 0) {
                        for (size_t i = 0; i < futureVecEnque.size(); i++) {
                            DS_ASSERT_OK(futureVecEnque[i].Get());
                        }
                        timerAll.Reset();
                    } else {
                        for (auto &fut : futureVecEnque) {
                            futureAll.emplace_back(std::move(fut));
                        }
                        reqCount++;
                    }
                    if (reqCount == blobsBatch) {
                        for (auto &fut : futureAll) {
                            DS_ASSERT_OK(fut.Get());
                        }
                        futureAll.clear();
                        reqCount = 0;
                    }
                    auto elapsed = timer.ElapsedMilliSecond();
                    if (operIdx > 0) {
                        costVec.emplace_back(elapsed);
                    }
                }
                for (auto &fut : futureAll) {
                    DS_ASSERT_OK(fut.Get());
                }
                auto elapsedAll = timerAll.ElapsedMilliSecond();
                LOG(ERROR) << objectKeyCount << "," << blobNum << "," << blobSize << ",DevPublish,"
                           << (elapsedAll / operatorCount) << "," << GetTimeCostUsState(costVec);
                helper.FreeDeviceBlobList(blob3D);
            }
        });

        auto dequeChild = ForkForTest([&]() {
            int32_t deviceId = 5;
            deviceId = GetDeviceIdFromEnv("DS_TEST_DEVICE_ID_SUB", deviceId);
            LOG(INFO) << "set device id " << deviceId;
            InitAcl(deviceId);
            std::shared_ptr<HeteroClient> localClient;
            InitTestHeteroClient(0, localClient);
            for (size_t batchIdx = 0; batchIdx < batchNum; batchIdx++) {
                std::vector<std::vector<DeviceBlobList>> blob3D = helper.MallocDeviceBlobList(deviceId, false);
                std::vector<double> costVec;
                LOG(INFO) << "start sub...";
                std::vector<Future> futureAll;
                size_t reqCount = 0;
                Timer timerAll;
                for (size_t operIdx = 0; operIdx < operatorCount; operIdx++) {
                    std::vector<std::string> objectKeys;
                    objectKeys.reserve(objectKeyCount);
                    for (size_t i = 0; i < objectKeyCount; i++) {
                        objectKeys.emplace_back(FormatString("PerfKey-%zu-%zu", operIdx, i));
                    }
                    Timer timer;
                    std::vector<Future> futureVecEnque;
                    size_t blobIndex = operIdx % blobsBatch;
                    DS_ASSERT_OK(localClient->DevSubscribe(objectKeys, blob3D[blobIndex], futureVecEnque));
                    if (operIdx == 0) {
                        for (size_t i = 0; i < futureVecEnque.size(); i++) {
                            DS_ASSERT_OK(futureVecEnque[i].Get());
                        }
                        timerAll.Reset();
                    } else {
                        for (auto &fut : futureVecEnque) {
                            futureAll.emplace_back(std::move(fut));
                        }
                        reqCount++;
                    }
                    if (reqCount == blobsBatch) {
                        for (auto &fut : futureAll) {
                            DS_ASSERT_OK(fut.Get());
                        }
                        if (verify) {
                            size_t verfiyBlobCount = 0;
                            for (size_t idx = 0; idx < blobsBatch; idx++) {
                                for (size_t keyIdx = 0; keyIdx < objectKeyCount; keyIdx++) {
                                    for (size_t blobIdx = 0; blobIdx < blobNum; blobIdx++) {
                                        auto blob = blob3D[idx][keyIdx].blobs[blobIdx];
                                        CheckDevPtrContent(blob.pointer, blob.size,
                                                           helper.dataVecs[idx][keyIdx][blobIdx]);
                                        verfiyBlobCount++;
                                    }
                                }
                            }
                            LOG(INFO) << "verfiy finish for operIdx:" << operIdx
                                      << ", verfiy blob count:" << verfiyBlobCount;
                        }
                        futureAll.clear();
                        reqCount = 0;
                    }
                    auto elapsed = timer.ElapsedMilliSecond();
                    if (operIdx > 0) {
                        costVec.emplace_back(elapsed);
                    }
                }
                for (auto &fut : futureAll) {
                    DS_ASSERT_OK(fut.Get());
                }
                auto elapsedAll = timerAll.ElapsedMilliSecond();
                LOG(ERROR) << objectKeyCount << "," << blobNum << "," << blobSize << ",DevSubscribe,"
                           << (elapsedAll / operatorCount) << "," << GetTimeCostUsState(costVec);
                helper.FreeDeviceBlobList(blob3D);
            }
        });
        DS_ASSERT_TRUE(WaitForChildFork(enqueChild), 0);
        DS_ASSERT_TRUE(WaitForChildFork(dequeChild), 0);
    }

    std::mt19937 gen_;
    int32_t deviceId_ = 3;
};

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase01)
{
    PubSubPerformanceTest(1, 20, 16 * 1024, false, 20);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase02)
{
    PubSubPerformanceTest(1, 20, 128 * 1024, false, 20);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase03)
{
    PubSubPerformanceTest(20, 20, 16 * 1024, false, 2);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase04)
{
    PubSubPerformanceTest(20, 20, 128 * 1024, false, 2);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase05)
{
    PubSubPerformanceTest(1, 200, 16 * 1024, false, 20);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase06)
{
    PubSubPerformanceTest(1, 200, 128 * 1024, false, 20);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase07)
{
    PubSubPerformanceTest(20, 200, 16 * 1024, false, 2);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase08)
{
    PubSubPerformanceTest(20, 200, 128 * 1024, false, 2);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase09)
{
    PubSubPerformanceTest(1, 6400, 16 * 1024, false, 20);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase10)
{
    PubSubPerformanceTest(1, 800, 128 * 1024, false, 20);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase11)
{
    PubSubPerformanceTest(20, 6400, 16 * 1024, false, 2);
}

TEST_F(HeteroClientPubSubBenchTest, DISABLED_PubSubCase12)
{
    PubSubPerformanceTest(20, 800, 128 * 1024, false, 2);
}
}  // namespace st
}  // namespace datasystem
