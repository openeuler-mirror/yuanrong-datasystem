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

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <libgen.h>
#include <tbb/concurrent_hash_map.h>

#include "common.h"
#include "datasystem/client/object_cache/device/p2p_subscribe.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/device/device_pointer_wrapper.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/utils/status.h"
#include "mock/ascend_device_manager_mock.cpp"

namespace datasystem {
namespace st {
using namespace acl;
class AscendDeviceManagerTest : public CommonTest {
    void SetUp() override
    {
        const char *ascend_root = std::getenv("ASCEND_HOME_PATH");
        if (ascend_root == nullptr) {
            BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly(Return(&managerMock_));
        } else {
            LOG(ERROR) << "7777 not AclDeviceManager::Instance MOCK";
        }
    }
    void TearDown() override
    {
    }

public:
    void DeviceMallocTest()
    {
        LOG(ERROR) << "7777 DeviceMallocTest()";
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclInit(nullptr));
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(0));
        size_t size = 100;
        std::string testStr = RandomData().GetRandomString(size);
        void *devPtr = nullptr;
        DS_ASSERT_OK(AclDeviceManager::Instance()->MallocDeviceMemory(size, devPtr));
        DS_ASSERT_OK(AclDeviceManager::Instance()->MemCopyH2D(devPtr, size, testStr.data(), size));
        void *hostPtr = malloc(size);
        DS_ASSERT_OK(AclDeviceManager::Instance()->MemCopyD2H(hostPtr, size, devPtr, size));
        auto result = std::string(reinterpret_cast<char *>(hostPtr), size);
        ASSERT_EQ(testStr, result);
        AclDeviceManager::Instance()->FreeDeviceMemory(devPtr);
        free(hostPtr);
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(0));
        AclDeviceManager::Instance()->aclFinalize();
    }

    void InitAcl(uint32_t deviceIdx)
    {
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclInit(nullptr));
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(deviceIdx));
    }

private:
    AclDeviceManagerMock managerMock_;
};

TEST_F(AscendDeviceManagerTest, DeviceMalloc)
{
    DeviceMallocTest();
}

TEST_F(AscendDeviceManagerTest, CheckDevices)
{
    InitAcl(0);
    uint32_t count;
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtGetDeviceCount(&count));
    LOG(INFO) << "device count is " << count;
    std::vector<uint32_t> devices = { 1, 5, 7, 0, 50 };
    DS_ASSERT_NOT_OK(AclDeviceManager::Instance()->VerifyDeviceId(devices));
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(0));
    AclDeviceManager::Instance()->aclFinalize();
}

TEST_F(AscendDeviceManagerTest, GetDeviceIdx)
{
    InitAcl(0);
    int32_t deviceIdx = -1;
    AclDeviceManager::Instance()->GetDeviceIdx(deviceIdx);
    ASSERT_EQ(deviceIdx, 0);
    int errorBigNum = 100;
    DS_ASSERT_NOT_OK(AclDeviceManager::Instance()->aclrtSetDevice(errorBigNum));
    uint32_t count;
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtGetDeviceCount(&count));
    LOG(INFO) << "device count is " << count;
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(0));
    AclDeviceManager::Instance()->aclFinalize();
}

TEST_F(AscendDeviceManagerTest, MallocSizeTooLarge)
{
    InitAcl(0);
    size_t size = 12ul * 1024 * 1024 * 1024;
    void *devPtr = nullptr;
    DS_ASSERT_OK(AclDeviceManager::Instance()->MallocDeviceMemory(size, devPtr));
    DS_ASSERT_OK(AclDeviceManager::Instance()->FreeDeviceMemory(devPtr));

    size = 12ul * 1024 * 1024 * 1024 + 1;
    devPtr = nullptr;
    DS_ASSERT_NOT_OK(AclDeviceManager::Instance()->MallocDeviceMemory(size, devPtr));
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(0));
    AclDeviceManager::Instance()->aclFinalize();
}

TEST_F(AscendDeviceManagerTest, TestVerifyingSha256Func)
{
    std::string filePath = "xx.so";
    DS_ASSERT_NOT_OK(AclDeviceManager::Instance()->VerifyingSha256(filePath));
    Dl_info dlInfo;
    if (dladdr(reinterpret_cast<void *>(AclDeviceManager::Instance), &dlInfo) != 0) {
        std::string curSoPath = dlInfo.dli_fname;
        std::string aclPluginPath = std::string(dirname(curSoPath.data())) + "/libacl_plugin.so";
        DS_ASSERT_OK(AclDeviceManager::Instance()->VerifyingSha256(aclPluginPath));
    }
}

TEST_F(AscendDeviceManagerTest, CallInMultiThread)
{
    auto threadNum = 10;
    auto taskNum = 100;
    auto pool = std::make_unique<ThreadPool>(threadNum);
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclInit(nullptr));
    for (int i = 0; i < taskNum; i++) {
        pool->Execute([]() {
            DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtSetDevice(0));
            int32_t deviceIdx = -1;
            AclDeviceManager::Instance()->GetDeviceIdx(deviceIdx);
            ASSERT_EQ(deviceIdx, 0);
            DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(0));
        });
    }
}

TEST_F(AscendDeviceManagerTest, TestDSAclrtQueryEventStatus)
{
    InitAcl(0);
    auto size = 1 * 1024 * 1024 * 1024ul;
    auto randomNum = 10;
    auto threadNum = 1;
    auto sleepTime = 3;
    auto pool = std::make_unique<ThreadPool>(threadNum);
    aclrtStream stream;
    DS_ASSERT_OK(AclDeviceManager::Instance()->RtCreateStream(&stream));
    void *devPtr = nullptr;
    DS_ASSERT_OK(AclDeviceManager::Instance()->MallocDeviceMemory(size, devPtr));
    std::string testStr = RandomData().GetPartRandomString(size, randomNum);

    aclrtEvent event;
    DS_ASSERT_OK(AclDeviceManager::Instance()->DSAclrtCreateEvent(&event));
    pool->Execute([&event]() {
        auto itervalMs = 1;
        auto listenEventTimes = 200;
        for (int i = 0; i < listenEventTimes; i++) {
            auto status = AclDeviceManager::Instance()->DSAclrtQueryEventStatus(event);
            LOG(INFO) << i << " / " << listenEventTimes << status;
            std::this_thread::sleep_for(std::chrono::milliseconds(itervalMs));
        }
        LOG(INFO) << "loop end";
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
    Timer timer;
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtMemcpyAsync(devPtr, size, testStr.data(), size,
                                                                ACL_MEMCPY_HOST_TO_DEVICE, stream));
    LOG(INFO) << "call async memcopy " << timer.ElapsedMilliSecondAndReset();
    DS_ASSERT_OK(AclDeviceManager::Instance()->DSAclrtRecordEvent(event, stream));
    LOG(INFO) << "call record event " << timer.ElapsedMilliSecondAndReset();
    DS_ASSERT_OK(AclDeviceManager::Instance()->DSAclrtSynchronizeEvent(event));
    LOG(INFO) << "call sync event " << timer.ElapsedMilliSecondAndReset();
    DS_ASSERT_OK(AclDeviceManager::Instance()->DSAclrtDestroyEvent(event));
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(0));
}

class FutureTest {
public:
    FutureTest(uint64_t dataSize) : dataSize_(dataSize)
    {
    }

    ~FutureTest()
    {
        Reset();
    }

    void Init(int eventCount)
    {
        auto randomNum = 10;
        pool_ = std::make_unique<ThreadPool>(1);
        DS_ASSERT_OK(AclDeviceManager::Instance()->RtCreateStream(&stream_));
        DS_ASSERT_OK(AclDeviceManager::Instance()->MallocDeviceMemory(dataSize_, devPtr_));
        testStr_ = RandomData().GetPartRandomString(dataSize_, randomNum);
        promise_ = std::make_shared<PromiseWithEvent>(GetStringUuid());
        futList_.clear();
        promise_->CreateEventAndFutureList(eventCount, futList_);
    }

    void DoMemcpyAsync()
    {
        auto waitGetMs = 10;
        auto event = promise_->GetEvent();
        DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtMemcpyAsync(devPtr_, dataSize_, testStr_.data(), dataSize_,
                                                                    ACL_MEMCPY_HOST_TO_DEVICE, stream_));
        LOG(INFO) << "{TEST} wait for thread start.";
        DS_ASSERT_OK(AclDeviceManager::Instance()->DSAclrtRecordEvent(event->GetRef(), stream_));
        std::this_thread::sleep_for(std::chrono::milliseconds(waitGetMs));
        promise_->SetPromiseValue(Status::OK());
        LOG(ERROR) << "Mem cpy ok ";
    }

    void Reset()
    {
        pool_.reset();
        DS_ASSERT_OK(AclDeviceManager::Instance()->RtSynchronizeStream(stream_));
        DS_ASSERT_OK(AclDeviceManager::Instance()->FreeDeviceMemory(devPtr_));
        promise_.reset();
        futList_.clear();
    }

    void AssertFuture(int32_t timeout, bool checkOk, bool defaultParam = false)
    {
        pool_->Execute([this, timeout, defaultParam, checkOk]() {
            Timer timer;
            for (auto &fut : futList_) {
                Status rc;
                if (defaultParam) {
                    rc = fut.Get();
                } else {
                    rc = fut.Get(timeout);
                }
                LOG(INFO) << rc;
                if (checkOk) {
                    DS_ASSERT_OK(rc);
                } else {
                    DS_ASSERT_NOT_OK(rc);
                }
            }
            LOG(INFO) << "wait memcpy async : " << timer.ElapsedMilliSecondAndReset();
        });
    }

    void RetryUntilFutureReady(int32_t timeout, int32_t maxWaitTime)
    {
        pool_->Execute([this, timeout, maxWaitTime]() {
            Timer timer;
            auto lastRc = Status::OK();
            for (int i = 0; i < maxWaitTime; i++) {
                for (auto &fut : futList_) {
                    lastRc = fut.Get(timeout);
                }
                if (lastRc.IsOk()) {
                    break;
                }
                DS_ASSERT_TRUE(lastRc.GetCode(), K_FUTURE_TIMEOUT);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            LOG(INFO) << "wait memcpy async : " << timer.ElapsedMilliSecondAndReset();
        });
    }

private:
    uint64_t dataSize_;
    std::shared_ptr<PromiseWithEvent> promise_;
    std::unique_ptr<ThreadPool> pool_;
    void *devPtr_ = nullptr;
    std::string testStr_;
    aclrtStream stream_;
    std::vector<Future> futList_;
};

TEST_F(AscendDeviceManagerTest, TestFuture)
{
    InitAcl(0);
    auto eventCount = 2;
    auto size = 1 * 1024 * 1024 * 1024ul;
    auto longTimeout = 60000;
    auto timeoutOutOfRange = 60 * 30 * 1000 + 1;

    std::unique_ptr test = std::make_unique<FutureTest>(size);
    // future.Get(60)
    test->Init(eventCount);
    test->AssertFuture(longTimeout, true);
    test->DoMemcpyAsync();
    test.reset();
    LOG(INFO) << "Test Get(60) ok";

    // future.Get(1)
    test = std::make_unique<FutureTest>(size);
    test->Init(eventCount);
    test->AssertFuture(1, false);
    test->DoMemcpyAsync();
    test.reset();
    LOG(INFO) << "Test Get(1) ok";

    // future.Get(0)
    test = std::make_unique<FutureTest>(size);
    test->Init(eventCount);
    test->AssertFuture(0, false);
    test->DoMemcpyAsync();
    test.reset();
    LOG(INFO) << "Test Get(0) ok";

    // return K_INVALID if timeout is out of range.
    test = std::make_unique<FutureTest>(size);
    test->Init(eventCount);
    test->AssertFuture(timeoutOutOfRange, false);
    test->DoMemcpyAsync();
    test.reset();
    LOG(INFO) << "Test Get(timeoutOutOfRange) ok";

    // future.Get()
    test = std::make_unique<FutureTest>(size);
    test->Init(eventCount);
    test->AssertFuture(1, true, true);
    test->DoMemcpyAsync();
    test.reset();
    LOG(INFO) << "Test Get() ok";

    // retry future.Get(0) until return ok.
    test = std::make_unique<FutureTest>(size);
    test->Init(eventCount);
    test->RetryUntilFutureReady(0, longTimeout);
    test->DoMemcpyAsync();
    test.reset();
    LOG(INFO) << "Test retry Get(0) ok";
    DS_ASSERT_OK(AclDeviceManager::Instance()->aclrtResetDevice(0));
}
}  // namespace st
}  // namespace datasystem