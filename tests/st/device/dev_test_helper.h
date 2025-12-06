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
 * Description: Datasystem unit test helper class, each testcases files need include this head file.
 */
#ifndef DATASYSTEM_TEST_ST_DEVICE_HELPER_H
#define DATASYSTEM_TEST_ST_DEVICE_HELPER_H

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/object/buffer.h"
#include "datasystem/client/hetero_cache/device_buffer.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/object_client.h"
#include "datasystem/object/object_enum.h"
#include "mock/ascend_device_manager_mock.cpp"

namespace datasystem {
using namespace acl;
namespace st {
constexpr int DEFAULT_WORKER_NUM = 2;
constexpr int DEFAULT_GET_TIMEOUT = 60000;
constexpr int SHORT_GET_TIMEOUT = 1000;
constexpr int DEFAULT_THREAD_NUM = 16;
constexpr int ABNORMAL_EXIT_CODE = -2;
constexpr int MINI_WAIT_TIME = 2;
constexpr int SHORT_WAIT_TIME = 5;
constexpr int LONG_WAIT_TIME = 10;
constexpr int PRINT_STR_LIMIT = 100;

class DevPtrQueue {
public:
    // per object dataType [3], count [262144], list len : 64 DATA_TYPE_FP16
    void Fill(uint32_t objectCount, uint32_t infoCount, DataType dataType, uint32_t count, int32_t deviceIdx = -1,
              char fillChar = 'a')
    {
        auto size = GetBytesFromDataType(dataType) * count;
        auto dataStr = std::string(size, fillChar);
        for (uint32_t i = 0; i < objectCount; i++) {
            std::vector<DataInfo> dataInfoList;
            for (uint32_t j = 0; j < infoCount; j++) {
                void *devPtr = nullptr;
                DS_ASSERT_OK(AclDeviceManager::Instance()->MallocDeviceMemory(size, devPtr));
                DS_ASSERT_OK(AclDeviceManager::Instance()->MemCopyH2D(devPtr, size, dataStr.data(), size));
                DataInfo info{ devPtr, dataType, count, size, deviceIdx };
                dataInfoList.push_back(std::move(info));
            }
            dataInfoQueue_.Push(std::move(dataInfoList));
        }
    }

    void FillWithSplit(uint32_t objectCount, uint32_t infoCount, DataType dataType, uint32_t count,
                       int32_t deviceIdx = -1, char fillChar = 'a')
    {
        auto size = GetBytesFromDataType(dataType) * count;
        size_t objectSize = size * infoCount;
        auto dataStr = std::string(objectSize, fillChar);
        for (uint32_t i = 0; i < objectCount; i++) {
            std::vector<DataInfo> dataInfoList;
            void *objectPtr = nullptr;
            DS_ASSERT_OK(AclDeviceManager::Instance()->MallocDeviceMemory(objectSize, objectPtr));
            DS_ASSERT_OK(AclDeviceManager::Instance()->MemCopyH2D(objectPtr, objectSize, dataStr.data(), objectSize));
            for (uint32_t j = 0; j < infoCount; j++) {
                void *devPtr = static_cast<char *>(objectPtr) + j * size;
                DataInfo info{ devPtr, dataType, count, size, deviceIdx };
                dataInfoList.push_back(std::move(info));
            }
            dataInfoQueue_.Push(std::move(dataInfoList));
        }
    }

    Status Get(std::vector<DataInfo> &infoList)
    {
        return dataInfoQueue_.Pop(infoList);
    }

    void Put(std::vector<DataInfo> &dataInfoList)
    {
        dataInfoQueue_.Push(std::move(dataInfoList));
    }

private:
    BlockingQueue<std::vector<DataInfo>> dataInfoQueue_;
};

class DevTestHelper : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.workerGflagParams = " -v=1 -shared_memory_size_mb=5120";
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        FLAGS_v = 1;
    }

    void SetUp() override
    {
        const char *ascend_root = std::getenv("ASCEND_HOME_PATH");
        if (ascend_root == nullptr) {
            BINEXPECT_CALL(AclDeviceManager::Instance, ()).WillRepeatedly([]() {
                return AclDeviceManagerMock::Instance();
            });
            // In special cases, wait () is trapped. This problem does not occur in mock ut and st.
            BINEXPECT_CALL(&AclDeviceManager::Shutdown, ());
        }

        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclInit(nullptr));
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(deviceIdx_));
    }

    static void SupportNPU(ConnectOptions &connectOptions)
    {
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    }

    void TearDown() override
    {
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(deviceIdx_));
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclFinalize());
        ExternalClusterTest::TearDown();
    }

    static void CheckDevPtrContent(const void *devPtr, size_t size, std::string content)
    {
        void *hostPtr = malloc(size);
        DS_ASSERT_OK(AclDeviceManager::Instance()->MemCopyD2H(hostPtr, size, devPtr, size));
        auto result = std::string(static_cast<char *>(hostPtr), size);
        auto isEqual = content == result;
        if (!isEqual) {
            LOG(ERROR) << "devPtr content is not equal than content. "
                       << "devPtr content: " << result.substr(0, PRINT_STR_LIMIT)
                       << ", expect content: " << content.substr(0, PRINT_STR_LIMIT);
        }
        EXPECT_TRUE(isEqual);
        free(hostPtr);
    }

    void SetContentToDevPtr(void *devPtr, size_t size, std::string content)
    {
        DS_ASSERT_OK(AclDeviceManager::Instance()->MemCopyH2D(devPtr, size, content.data(), size));
    }

    int WaitForChildFork(pid_t pid)
    {
        if (pid == 0) {
            return 0;
        }
        int statLoc;
        if (waitpid(pid, &statLoc, 0) < 0) {
            LOG(ERROR) << FormatString("waitpid: %d error!", pid);
            return -1;
        }
        if (WIFEXITED(statLoc)) {
            const int exitStatus = WEXITSTATUS(statLoc);
            if (exitStatus != 0) {
                LOG(ERROR) << FormatString("Non-zero exit status %d from test!", exitStatus);
            }
            return exitStatus;
        } else {
            LOG(ERROR) << FormatString("Non-normal exit %d from child!, status: %d", pid, statLoc);
            return ABNORMAL_EXIT_CODE;
        }
    }

    void InitAcl(int32_t deviceId)
    {
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclInit(nullptr));
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(deviceId));
        deviceIdx_ = deviceId;
    }

    void GetAllFuture(std::vector<Future> &futVec)
    {
        for (auto &fut : futVec) {
            DS_EXPECT_OK(fut.Get());
        }
    }

    static Status ConvertToDeviceBlobList(const std::vector<std::vector<DataInfo>> &dataInfoList,
                                          std::vector<DeviceBlobList> &devBlobList)
    {
        if (dataInfoList.empty()) {
            return Status::OK();
        }
        devBlobList.resize(dataInfoList.size());
        for (size_t i = 0; i < dataInfoList.size(); i++) {
            devBlobList[i].deviceIdx = dataInfoList[0][0].deviceIdx;
            for (const auto &info : dataInfoList[i]) {
                Blob blob{ info.devPtr, info.size };
                devBlobList[i].blobs.emplace_back(std::move(blob));
            }
        }
        return Status::OK();
    }

    static void PrePareDevData(size_t numOfObjs, size_t blksPerObj, size_t blkSz,
                               std::vector<DeviceBlobList> &swapOutBlobList,
                               std::vector<DeviceBlobList> &swapInBlobList, int32_t deviceId)
    {
        std::vector<std::vector<DataInfo>> swapOutDataInfoList;
        std::vector<std::vector<DataInfo>> swapInDatInfoList;

        // Swap Out HBM.
        DevPtrQueue swapOutDataInfoQueue;
        swapOutDataInfoQueue.Fill(numOfObjs, blksPerObj, DataType::DATA_TYPE_INT8, blkSz, deviceId);

        // Swap In HBM.
        DevPtrQueue resultDataInfoQueue;
        resultDataInfoQueue.Fill(numOfObjs, blksPerObj, DataType::DATA_TYPE_INT8, blkSz, deviceId, 'b');

        for (auto i = 0uL; i < numOfObjs; i++) {
            std::vector<DataInfo> tmpInfos;
            swapOutDataInfoQueue.Get(tmpInfos);
            swapOutDataInfoList.emplace_back(std::move(tmpInfos));

            resultDataInfoQueue.Get(tmpInfos);
            swapInDatInfoList.emplace_back(std::move(tmpInfos));
        }

        ConvertToDeviceBlobList(swapOutDataInfoList, swapOutBlobList);
        ConvertToDeviceBlobList(swapInDatInfoList, swapInBlobList);
    }

    static void PrePareRandomData(size_t numOfObjs, size_t blksPerObj, size_t blkSz, int32_t deviceIdx,
                                  std::vector<DeviceBlobList> &swapBlobList,
                                  std::vector<std::vector<std::string>> &verifyList)
    {
        swapBlobList.clear();
        verifyList.clear();
        for (uint32_t i = 0; i < numOfObjs; i++) {
            std::vector<std::string> blobInfos;
            DeviceBlobList blobList;
            blobList.deviceIdx = deviceIdx;
            for (uint32_t j = 0; j < blksPerObj; j++) {
                std::string dataStr = RandomData().GetRandomString(blkSz);
                void *devPtr = nullptr;
                AclDeviceManager::Instance()->MallocDeviceMemory(blkSz, devPtr);
                AclDeviceManager::Instance()->MemCopyH2D(devPtr, blkSz, dataStr.data(), blkSz);
                Blob blob{ devPtr, blkSz };
                blobList.blobs.emplace_back(std::move(blob));
                blobInfos.emplace_back(std::move(dataStr));
            }
            swapBlobList.emplace_back(blobList);
            verifyList.emplace_back(blobInfos);
        }
    }

    static uint32_t GetDeviceIdFromEnv(const char *envName, uint32_t defaultDeviceId)
    {
        const char *deviceIdStr = std::getenv(envName);
        if (deviceIdStr != nullptr) {
            return std::stoull(deviceIdStr);
        }
        return defaultDeviceId;
    }

    static pid_t ForkForTest(std::function<void()> func)
    {
        pid_t child = fork();
        if (child == 0) {
            // avoid zmq problem when fork.
            std::thread thread(func);
            thread.join();
            exit(0);
        }
        return child;
    }

    static Status IsSameContent(std::vector<DeviceBlobList> &testBlobList, std::vector<DeviceBlobList> &reference,
                                char fillChar)
    {
        auto contentEqual = [](const void *devPtr, size_t size, std::string content) -> Status {
            void *hostPtr = malloc(size);
            RETURN_IF_NOT_OK(AclDeviceManager::Instance()->MemCopyD2H(hostPtr, size, devPtr, size));
            auto result = std::string(reinterpret_cast<char *>(hostPtr), size);
            auto isEqual = content == result;
            if (!isEqual) {
                LOG(ERROR) << "devPtr content is not equal than content. "
                           << "devPtr content: " << result.substr(0, PRINT_STR_LIMIT)
                           << ", expect content: " << content.substr(0, PRINT_STR_LIMIT);
            }
            free(hostPtr);
            if (isEqual) {
                return Status::OK();
            }
            return Status(K_INVALID, "not equal");
        };
        if (testBlobList.size() != reference.size()) {
            return Status(K_INVALID, "vec size is not equal");
        }
        for (size_t j = 0; j < testBlobList.size(); j++) {
            if (testBlobList[j].blobs.size() != reference[j].blobs.size()) {
                return Status(K_INVALID, FormatString("vec:%d , blob_size is not equal,%d->%d", j,
                                                      testBlobList[j].blobs.size(), reference[j].blobs.size()));
                ;
            }
        }
        for (size_t j = 0; j < testBlobList.size(); j++) {
            for (size_t k = 0; k < testBlobList[j].blobs.size(); k++) {
                if (testBlobList[j].blobs[k].size != reference[j].blobs[k].size) {
                    return Status(K_INVALID, FormatString("vec:%d , data size is not equal,%d->%d->%d->%d", j, k,
                                                          testBlobList[j].blobs[k].size, reference[j].blobs[k].size));
                    ;
                }
                RETURN_IF_NOT_OK(contentEqual(testBlobList[j].blobs[k].pointer, testBlobList[j].blobs[k].size,
                                              std::string(testBlobList[j].blobs[k].size, fillChar)));
            };
        }
        return Status::OK();
    }

    static Status StausHasStr(Status status, const std::string &probe)
    {
        auto str = status.GetMsg();
        if (str.find(probe) == std::string::npos) {
            return Status(K_INVALID, FormatString("%s don't contain %s", str, probe));
        }
        return Status::OK();
    }

    static void RandomFill(const uint32_t objectCount, const std::function<int()> gen, const int deviceId,
                           std::vector<DeviceBlobList> &devBlobList, const char fillChar = 'a')
    {
        auto rn = gen();
        for (auto i = 0u; i < objectCount; i++) {
            DeviceBlobList deviceBlobs{ {}, deviceId };
            auto sizeLimit = 1000;
            for (auto j = 0; j < rn; j++) {
                auto size = static_cast<std::uint64_t>(gen() % sizeLimit + 1);
                auto dataStr = std::string(size, fillChar);
                void *devPtr = nullptr;
                AclDeviceManager::Instance()->MallocDeviceMemory(size, devPtr);
                AclDeviceManager::Instance()->MemCopyH2D(devPtr, size, dataStr.data(), size);
                Blob blob{ devPtr, size };
                deviceBlobs.blobs.push_back(blob);
            }
            devBlobList.emplace_back(deviceBlobs);
        }
    }

    static void AllocAsBlobSize(const std::vector<std::vector<uint64_t>> blobSizes, const int deviceId,
                                std::vector<DeviceBlobList> &devBlobList)
    {
        for (auto sizes : blobSizes) {
            DeviceBlobList deviceBlobs{ {}, deviceId };
            for (auto size : sizes) {
                void *devPtr = nullptr;
                AclDeviceManager::Instance()->MallocDeviceMemory(size, devPtr);
                Blob blob{ devPtr, size };
                deviceBlobs.blobs.push_back(blob);
            }
            devBlobList.emplace_back(deviceBlobs);
        }
    }

    std::string GetTimeCostUsState(std::vector<double> &costVec)
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
    size_t deviceIdx_ = 0;

protected:
    AclDeviceManagerMock managerMock_;
};
class DeviceBlobListHelper {
public:
    DeviceBlobListHelper(size_t operatorCount, size_t keysCountPerOp, size_t blobNum, size_t blobSize = 0)
    {
        this->operatorCount = operatorCount;
        this->keysCountPerOp = keysCountPerOp;
        this->blobNum = blobNum;
        auto randomData = RandomData();
        size_t minDataSize = 10 * 1024;
        size_t maxDataSize = 5 * 1024 * 1024;
        size_t randomDataNum = 30;

        for (size_t i = 0; i < operatorCount; i++) {
            std::vector<std::string> keyEachOps;
            std::vector<std::vector<std::string>> dataEachOps;
            for (size_t j = 0; j < keysCountPerOp; j++) {
                std::vector<std::string> dataEachKey;
                keyEachOps.emplace_back(FormatString("Object_%s_%s", i, j));
                for (size_t k = 0; k < blobNum; k++) {
                    auto length = blobSize == 0 ? randomData.GetRandomUint32(minDataSize, maxDataSize) : blobSize;
                    dataEachKey.emplace_back(randomData.GetPartRandomString(length, randomDataNum));
                }
                dataEachOps.emplace_back(std::move(dataEachKey));
            }
            keyVecs.emplace_back(std::move(keyEachOps));
            dataVecs.emplace_back(std::move(dataEachOps));
        }
    }

    std::vector<std::vector<DeviceBlobList>> MallocDeviceBlobList(int32_t deviceIdx, bool needCopyDataToDevice)
    {
        std::vector<std::vector<DeviceBlobList>> blobListVec;

        for (size_t i = 0; i < operatorCount; i++) {
            LOG(INFO) << "alloc index:" << i << ", copy:" << needCopyDataToDevice << ", deviceId:" << deviceIdx;
            std::vector<DeviceBlobList> blobListPerOps;
            for (size_t j = 0; j < keysCountPerOp; j++) {
                std::vector<Blob> tmpBlobList;
                for (size_t k = 0; k < blobNum; k++) {
                    void *devPtr = nullptr;
                    auto data = dataVecs[i][j][k];
                    acl::AclDeviceManager::Instance()->MallocDeviceMemory(data.size(), devPtr);
                    if (needCopyDataToDevice) {
                        acl::AclDeviceManager::Instance()->MemCopyH2D(devPtr, data.size(), data.data(), data.size());
                    }
                    Blob blob{ .pointer = devPtr, .size = data.size() };
                    tmpBlobList.emplace_back(std::move(blob));
                }
                blobListPerOps.emplace_back(DeviceBlobList{ std::move(tmpBlobList), deviceIdx });
            }
            blobListVec.emplace_back(std::move(blobListPerOps));
        }

        return blobListVec;
    }

    void FreeDeviceBlobList(std::vector<std::vector<DeviceBlobList>> &blobListVec)
    {
        for (auto &blob2D : blobListVec) {
            for (auto &blob1D : blob2D) {
                for (auto &blob : blob1D.blobs) {
                    acl::AclDeviceManager::Instance()->MallocDeviceMemory(blob.size, blob.pointer);
                }
            }
        }
    }

    size_t operatorCount;
    size_t keysCountPerOp;
    size_t blobNum;
    std::vector<std::vector<std::vector<std::string>>> dataVecs;
    std::vector<std::vector<std::string>> keyVecs;
};
}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TEST_ST_COMMON_H
