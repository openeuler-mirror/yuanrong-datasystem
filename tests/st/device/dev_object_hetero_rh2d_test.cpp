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
 * Description: device rh2d test.
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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/inject/inject_point.h"
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
#include "datasystem/hetero_client.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/utils/status.h"
#include "device/dev_test_helper.h"

using datasystem::memory::DevMemFuncRegister;

namespace datasystem {
using namespace acl;
namespace st {

class DevObjectHeteroRH2DTest : public DevTestHelper {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.workerGflagParams =
            " -v=1 -authorization_enable=true -shared_memory_size_mb=4096 -enable_fallocate=false -arena_per_tenant=1 "
            "-client_dead_timeout_s=15";
        opts.workerSpecifyGflagParams[0] += " -remote_h2d_device_ids=7 ";
        opts.workerSpecifyGflagParams[1] += " -remote_h2d_device_ids=5 ";
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

    void InitTestDsClientForRemoteH2D(uint32_t workerIndex, std::shared_ptr<DsClient> &client);

    void RunMGetH2DTest(const std::shared_ptr<DsClient> &client1, const std::shared_ptr<DsClient> &client2,
                        const std::vector<size_t> &numObjChoices, const std::vector<size_t> &blkSzChoices,
                        size_t deviceId, const size_t blksPerObj = 31, int loopsNum = 5);

public:
    std::mt19937 gen_;
    int32_t deviceId_ = 0;
};

class DevObjectHeteroRH2DMismatchTest : public DevObjectHeteroRH2DTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.workerGflagParams =
            " -v=1 -authorization_enable=true -shared_memory_size_mb=4096 -enable_fallocate=false -arena_per_tenant=1 "
            "-client_dead_timeout_s=15";
        opts.workerSpecifyGflagParams[0] += " -remote_h2d_device_ids=7 ";
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        FLAGS_v = 0;
    }
};

class DevObjectHeteroRH2DDistributedTest : public DevObjectHeteroRH2DTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        DevObjectHeteroRH2DTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_worker_worker_batch_get=true ";
        opts.enableDistributedMaster = "true";
    }
};

class DevObjectHeteroRH2DNoNpuTest : public DevObjectHeteroRH2DTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.workerGflagParams =
            " -v=1 -authorization_enable=true -shared_memory_size_mb=4096 -enable_fallocate=false -arena_per_tenant=1 "
            "-client_dead_timeout_s=15";
            opts.enableDistributedMaster = "false";
            opts.numEtcd = 1;
            FLAGS_v = 0;
    }
};

void DevObjectHeteroRH2DTest::InitTestDsClientForRemoteH2D(uint32_t workerIndex, std::shared_ptr<DsClient> &client)
{
    ConnectOptions connectOptions;
    const int32_t timeoutMs = 60000;
    InitConnectOpt(workerIndex, connectOptions, timeoutMs);
    connectOptions.enableRemoteH2D = true;
    client = std::make_shared<DsClient>(connectOptions);
    DS_ASSERT_OK(client->Init());
}

void DevObjectHeteroRH2DTest::RunMGetH2DTest(const std::shared_ptr<DsClient> &client1,
                                             const std::shared_ptr<DsClient> &client2,
                                             const std::vector<size_t> &numObjChoices,
                                             const std::vector<size_t> &blkSzChoices, size_t deviceId,
                                             const size_t blksPerObj, int loopsNum)
{
    for (size_t vecIdx = 0; vecIdx < numObjChoices.size(); vecIdx++) {
        auto blkSz = blkSzChoices[vecIdx];
        auto numOfObjs = numObjChoices[vecIdx];

        std::vector<DeviceBlobList> setBlobListUseless;
        std::vector<DeviceBlobList> getBlobList;
        PrePareDevData(numOfObjs, blksPerObj, blkSz, setBlobListUseless, getBlobList, deviceId);

        std::vector<std::string> inObjectIds;
        for (auto i = 0ul; i < numOfObjs; i++) {
            inObjectIds.emplace_back(GetStringUuid());
        }

        auto verifyFunc = [&](std::vector<std::vector<std::string>> &verifyList) {
            for (size_t j = 0; j < numOfObjs; j++) {
                for (size_t k = 0; k < blksPerObj; k++) {
                    LOG(INFO) << "Check object " << j << ", blob " << k;
                    CheckDevPtrContent(getBlobList[j].blobs[k].pointer, getBlobList[j].blobs[k].size, verifyList[j][k]);
                }
            }
        };

        for (int i = 0; i < loopsNum; i++) {
            std::vector<DeviceBlobList> setBlobList;
            std::vector<std::vector<std::string>> verifyList;
            PrePareRandomData(numOfObjs, blksPerObj, blkSz, deviceId, setBlobList, verifyList);
            DS_ASSERT_OK(client1->Hetero()->MSetD2H(inObjectIds, setBlobList));
            std::vector<std::string> failedList;
            DS_ASSERT_OK(client2->Hetero()->MGetH2D(inObjectIds, getBlobList, failedList, DEFAULT_GET_TIMEOUT));
            ASSERT_TRUE(failedList.empty());
            verifyFunc(verifyList);
            DS_ASSERT_OK(client1->Hetero()->Delete(inObjectIds, failedList));
            ASSERT_TRUE(failedList.empty());
        }
    }
}

TEST_F(DevObjectHeteroRH2DTest, DISABLED_RemoteH2DTest1)
{
    // Test that Remote H2D works for clients and workers on the same node.
    InitAcl(deviceId_);

    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };

    std::shared_ptr<DsClient> client1;
    std::shared_ptr<DsClient> client2;
    InitTestDsClientForRemoteH2D(0, client1);
    InitTestDsClientForRemoteH2D(1, client2);

    RunMGetH2DTest(client1, client2, numObjChoices, blkSzChoices, deviceId_);
}

TEST_F(DevObjectHeteroRH2DTest, DISABLED_RemoteH2DTestShmDisabled)
{
    // Test that Remote H2D works for get client and its corresponding worker not on the same node.
    // That is, shm is disabled when client and worker are not on the same node.
    InitAcl(deviceId_);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.AddEntryToGetResponse.shmDisabled", "call()"));

    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };

    std::shared_ptr<DsClient> client1;
    std::shared_ptr<DsClient> client2;
    InitTestDsClientForRemoteH2D(0, client1);
    InitTestDsClientForRemoteH2D(1, client2);

    RunMGetH2DTest(client1, client2, numObjChoices, blkSzChoices, deviceId_);
}

TEST_F(DevObjectHeteroRH2DTest, DISABLED_RemoteH2DTestCompatilibity1)
{
    // Test compatibility when non-RH2D requests are sent when RH2D is enabled on workers.
    InitAcl(deviceId_);

    std::shared_ptr<DsClient> dsClient1;
    std::shared_ptr<DsClient> dsClient2;
    InitTestDsClientForRemoteH2D(0, dsClient1);
    InitTestDsClientForRemoteH2D(1, dsClient2);

    std::shared_ptr<KVClient> client1 = dsClient1->KV();
    std::shared_ptr<KVClient> client2 = dsClient2->KV();

    std::string setKey = "testKey";
    std::string setValue = "testValue";
    DS_ASSERT_OK(client1->Set(setKey, setValue));

    std::string getValue;
    DS_ASSERT_OK(client2->Get(setKey, getValue));
    ASSERT_EQ(getValue, setValue);

    DS_ASSERT_OK(client2->Del(setKey));
    DS_ASSERT_NOT_OK(client1->Get(setKey, getValue));
}

TEST_F(DevObjectHeteroRH2DTest, DISABLED_RemoteH2DTestCompatilibity2)
{
    // Test compatibility when workers are RH2D enabled, while the client does not.
    InitAcl(deviceId_);

    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };

    std::shared_ptr<DsClient> client1;
    std::shared_ptr<DsClient> client2;
    InitTestDsClient(0, client1);
    InitTestDsClient(1, client2);

    RunMGetH2DTest(client1, client2, numObjChoices, blkSzChoices, deviceId_);
}

TEST_F(DevObjectHeteroRH2DTest, DISABLED_RemoteH2DTestMultiProcess1)
{
    // Test that multiple client processes get the same keys is supported.
    const int objPerProcess = 40;
    const int clientNum = 4;
    const size_t blkSz = 28800;
    const size_t blksPerObj = 1;
    std::vector<int> pids;
    std::vector<std::vector<std::string>> inObjectIds(clientNum);
    std::vector<std::vector<std::string>> verifyList;
    std::vector<std::string> inObjectIdsConcat;
    for (uint32_t i = 0; i < clientNum * objPerProcess; i++) {
        verifyList.emplace_back();
        for (uint32_t j = 0; j < blksPerObj; j++) {
            verifyList[i].emplace_back(RandomData().GetRandomString(blkSz));
        }
    }
    for (int i = 0; i < clientNum; i++) {
        for (int j = 0ul; j < objPerProcess; j++) {
            inObjectIds[i].emplace_back(GetStringUuid());
        }
        inObjectIdsConcat.insert(inObjectIdsConcat.end(), inObjectIds[i].begin(), inObjectIds[i].end());
        pids.emplace_back(ForkForTest([&, devId = i]() {
            InitAcl(devId);
            std::shared_ptr<DsClient> client;
            InitTestDsClientForRemoteH2D(0, client);
            std::vector<DeviceBlobList> setBlobList;
            for (auto i = devId * objPerProcess; i < devId * objPerProcess + objPerProcess; i++) {
                DeviceBlobList blobList;
                blobList.deviceIdx = devId;
                for (uint32_t j = 0; j < blksPerObj; j++) {
                    void *devPtr = nullptr;
                    AclDeviceManager::Instance()->MallocDeviceMemory(blkSz, devPtr);
                    AclDeviceManager::Instance()->MemCopyH2D(devPtr, blkSz, verifyList[i][j].data(), blkSz);
                    blobList.blobs.emplace_back(Blob{ devPtr, blkSz });
                }
                setBlobList.emplace_back(blobList);
            }
            DS_ASSERT_OK(client->Hetero()->MSetD2H(inObjectIds[devId], setBlobList));
        }));
    }
    for (auto pid : pids) {
        DS_ASSERT_TRUE(WaitForChildFork(pid), 0);
    }
    pids.clear();

    for (int i = 0; i < clientNum; i++) {
        pids.emplace_back(ForkForTest([&, devId = i + clientNum]() {
            InitAcl(devId);
            std::shared_ptr<DsClient> client;
            InitTestDsClientForRemoteH2D(1, client);
            std::vector<DeviceBlobList> setBlobListUseless;
            std::vector<DeviceBlobList> getBlobList;
            PrePareDevData(inObjectIdsConcat.size(), blksPerObj, blkSz, setBlobListUseless, getBlobList, devId);

            std::vector<std::string> failedList;
            DS_ASSERT_OK(client->Hetero()->MGetH2D(inObjectIdsConcat, getBlobList, failedList, DEFAULT_GET_TIMEOUT));
            ASSERT_TRUE(failedList.empty());
            for (size_t j = 0; j < inObjectIdsConcat.size(); j++) {
                for (size_t k = 0; k < blksPerObj; k++) {
                    LOG(INFO) << "Check object " << j << ", blob " << k;
                    CheckDevPtrContent(getBlobList[j].blobs[k].pointer, getBlobList[j].blobs[k].size, verifyList[j][k]);
                }
            }
            LOG(INFO) << "MGet done for devId = " << devId;
        }));
    }
    for (auto pid : pids) {
        DS_ASSERT_TRUE(WaitForChildFork(pid), 0);
    }
}

TEST_F(DevObjectHeteroRH2DMismatchTest, DISABLED_RemoteH2DTestDirection1)
{
    // Test that MGetH2D still works with mismatching RH2D configurations.
    // This testcases tests that the set worker enables RH2D,
    // while the get worker disables RH2D.
    // And also the client enables RH2D.
    InitAcl(deviceId_);

    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };

    std::shared_ptr<DsClient> client1;
    std::shared_ptr<DsClient> client2;
    InitTestDsClientForRemoteH2D(0, client1);
    InitTestDsClientForRemoteH2D(1, client2);

    RunMGetH2DTest(client1, client2, numObjChoices, blkSzChoices, deviceId_);
}

TEST_F(DevObjectHeteroRH2DMismatchTest, DISABLED_RemoteH2DTestDirection2)
{
    // Test that MGetH2D still works with mismatching RH2D configurations.
    // This testcases tests that the get worker enables RH2D,
    // while the set worker disables RH2D.
    // And also the client enables RH2D.
    InitAcl(deviceId_);

    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };

    std::shared_ptr<DsClient> client1;
    std::shared_ptr<DsClient> client2;
    InitTestDsClientForRemoteH2D(0, client1);
    InitTestDsClientForRemoteH2D(1, client2);

    RunMGetH2DTest(client2, client1, numObjChoices, blkSzChoices, deviceId_);
}

TEST_F(DevObjectHeteroRH2DDistributedTest, DISABLED_RemoteH2DTest2)
{
    // Test that Remote H2D works when some of the data is local, some of the data is remote.
    // Note that the local data is fetched at Query Meta.
    InitAcl(deviceId_);

    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };

    std::shared_ptr<DsClient> client1;
    std::shared_ptr<DsClient> client2;
    InitTestDsClientForRemoteH2D(0, client1);
    InitTestDsClientForRemoteH2D(1, client2);

    const size_t blksPerObj = 6;
    RunMGetH2DTest(client1, client2, numObjChoices, blkSzChoices, deviceId_, blksPerObj);
}

TEST_F(DevObjectHeteroRH2DNoNpuTest, DISABLED_RemoteH2DTestNoNpu)
{
    // Test that Remote H2D is turned off when the workers have no npu's specified.
    // Clients will also have RH2D enabled.
    InitAcl(deviceId_);

    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> blkSzChoices = { 73 * 1024, 73 * 1024, 73 * 1024, 73 * 1024 };

    std::shared_ptr<DsClient> client1;
    std::shared_ptr<DsClient> client2;
    InitTestDsClientForRemoteH2D(0, client1);
    InitTestDsClientForRemoteH2D(1, client2);

    RunMGetH2DTest(client1, client2, numObjChoices, blkSzChoices, deviceId_);
}

TEST_F(DevObjectHeteroRH2DTest, DISABLED_RemoteH2DTestMultiThread1)
{
    // Test that Remote H2D supports multiple threads.
    InitAcl(deviceId_);

    const int objPerThread = 40;
    const int threadNum = 4;
    const size_t blkSz = 28800;
    const size_t blksPerObj = 1;
    std::vector<std::thread> threads;
    std::vector<std::vector<std::string>> inObjectIds(threadNum);
    std::vector<std::vector<std::string>> verifyList;
    std::vector<std::string> inObjectIdsConcat;
    for (uint32_t i = 0; i < threadNum * objPerThread; i++) {
        verifyList.emplace_back();
        for (uint32_t j = 0; j < blksPerObj; j++) {
            verifyList[i].emplace_back(RandomData().GetRandomString(blkSz));
        }
    }
    std::shared_ptr<DsClient> setClient;
    InitTestDsClientForRemoteH2D(0, setClient);
    
    for (int i = 0; i < threadNum; i++) {
        for (int j = 0ul; j < objPerThread; j++) {
            inObjectIds[i].emplace_back(GetStringUuid());
        }
        inObjectIdsConcat.insert(inObjectIdsConcat.end(), inObjectIds[i].begin(), inObjectIds[i].end());
        threads.emplace_back([&, threadId = i]() {
            DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(deviceId_));
            std::vector<DeviceBlobList> setBlobList;
            for (auto i = threadId * objPerThread; i < threadId * objPerThread + objPerThread; i++) {
                DeviceBlobList blobList;
                blobList.deviceIdx = deviceId_;
                for (uint32_t j = 0; j < blksPerObj; j++) {
                    void *devPtr = nullptr;
                    AclDeviceManager::Instance()->MallocDeviceMemory(blkSz, devPtr);
                    AclDeviceManager::Instance()->MemCopyH2D(devPtr, blkSz, verifyList[i][j].data(), blkSz);
                    blobList.blobs.emplace_back(Blob{ devPtr, blkSz });
                }
                setBlobList.emplace_back(blobList);
            }
            DS_ASSERT_OK(setClient->Hetero()->MSetD2H(inObjectIds[threadId], setBlobList));
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }
    threads.clear();

    std::shared_ptr<DsClient> getClient;
    InitTestDsClientForRemoteH2D(1, getClient);
    for (int i = 0; i < threadNum; i++) {
        threads.emplace_back([&, threadId = i]() {
            DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(deviceId_));
            std::vector<DeviceBlobList> setBlobListUseless;
            std::vector<DeviceBlobList> getBlobList;
            PrePareDevData(inObjectIdsConcat.size(), blksPerObj, blkSz, setBlobListUseless, getBlobList, deviceId_);

            std::vector<std::string> failedList;
            DS_ASSERT_OK(getClient->Hetero()->MGetH2D(inObjectIdsConcat, getBlobList, failedList, DEFAULT_GET_TIMEOUT));
            ASSERT_TRUE(failedList.empty());
            for (size_t j = 0; j < inObjectIdsConcat.size(); j++) {
                for (size_t k = 0; k < blksPerObj; k++) {
                    LOG(INFO) << "Thread " << threadId << ": Check object " << j << ", blob " << k;
                    CheckDevPtrContent(getBlobList[j].blobs[k].pointer, getBlobList[j].blobs[k].size, verifyList[j][k]);
                }
            }
            LOG(INFO) << "MGet done for thread " << threadId;
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
}

}  // namespace st
}  // namespace datasystem
