/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "datasystem/common/device/ascend/acl_resource_manager.h"
#include "mock/ascend_device_manager_mock.cpp"

namespace datasystem {
namespace st {
using namespace acl;

namespace {
constexpr auto PARALLEL_H2D_TEST_BARRIER_TIMEOUT = std::chrono::milliseconds(500);

class TestAclResourceManager : public AclResourceManager {
public:
    void Configure(uint64_t hostSize, uint64_t deviceSize, MemcopyPolicy d2hPolicy, MemcopyPolicy h2dPolicy)
    {
        hostMemSize = hostSize;
        deviceMemSize = deviceSize;
        policyD2H = d2hPolicy;
        policyH2D = h2dPolicy;
    }

    Status ValidateConfig() const
    {
        return ValidateMemConfig();
    }
};

DeviceBatchCopyHelper BuildH2DHelper(const std::vector<std::string> &payloads,
                                     std::vector<std::vector<char>> &deviceDestinations)
{
    DeviceBatchCopyHelper helper;
    deviceDestinations.clear();
    deviceDestinations.reserve(payloads.size());
    helper.bufferMetas.reserve(payloads.size());
    helper.srcList.reserve(payloads.size());
    helper.dstList.reserve(payloads.size());
    helper.dataSizeList.reserve(payloads.size());
    helper.srcBuffers.reserve(payloads.size());
    helper.dstBuffers.reserve(payloads.size());
    size_t firstBlobOffset = 0;
    for (const auto &payload : payloads) {
        deviceDestinations.emplace_back(payload.size(), '\0');
        helper.bufferMetas.emplace_back(
            BufferMetaInfo{ .blobCount = 1, .firstBlobOffset = firstBlobOffset, .size = payload.size() });
        helper.srcList.emplace_back(const_cast<char *>(payload.data()));
        helper.dstList.emplace_back(deviceDestinations.back().data());
        helper.dataSizeList.emplace_back(payload.size());
        helper.srcBuffers.emplace_back(BufferView{ .ptr = const_cast<char *>(payload.data()), .size = payload.size() });
        helper.dstBuffers.emplace_back(BufferView{ .ptr = deviceDestinations.back().data(), .size = payload.size() });
        helper.batchSize++;
        firstBlobOffset++;
    }
    return helper;
}

struct D2HTestBuffers {
    std::vector<std::vector<char>> deviceSources;
    std::vector<std::vector<char>> hostDestinations;
};

DeviceBatchCopyHelper BuildD2HHelper(const std::vector<std::string> &payloads, D2HTestBuffers &buffers)
{
    DeviceBatchCopyHelper helper;
    buffers.deviceSources.clear();
    buffers.hostDestinations.clear();
    buffers.deviceSources.reserve(payloads.size());
    buffers.hostDestinations.reserve(payloads.size());
    helper.bufferMetas.reserve(payloads.size());
    helper.srcList.reserve(payloads.size());
    helper.dstList.reserve(payloads.size());
    helper.dataSizeList.reserve(payloads.size());
    helper.srcBuffers.reserve(payloads.size());
    helper.dstBuffers.reserve(payloads.size());
    for (size_t index = 0; index < payloads.size(); ++index) {
        buffers.deviceSources.emplace_back(payloads[index].begin(), payloads[index].end());
        buffers.hostDestinations.emplace_back(payloads[index].size(), '\0');
        auto &deviceSource = buffers.deviceSources.back();
        auto &hostDestination = buffers.hostDestinations.back();
        helper.bufferMetas.emplace_back(
            BufferMetaInfo{ .blobCount = 1, .firstBlobOffset = index, .size = payloads[index].size() });
        helper.srcList.emplace_back(deviceSource.data());
        helper.dstList.emplace_back(hostDestination.data());
        helper.dataSizeList.emplace_back(payloads[index].size());
        helper.srcBuffers.emplace_back(BufferView{ .ptr = deviceSource.data(), .size = deviceSource.size() });
        helper.dstBuffers.emplace_back(BufferView{ .ptr = hostDestination.data(), .size = hostDestination.size() });
        helper.batchSize++;
    }
    return helper;
}

class ScopedEnv {
public:
    ScopedEnv(std::string name, std::string value) : name_(std::move(name))
    {
        auto oldValue = std::getenv(name_.c_str());
        if (oldValue != nullptr) {
            hadOldValue_ = true;
            oldValue_ = oldValue;
        }
        (void)setenv(name_.c_str(), value.c_str(), 1);
    }

    ~ScopedEnv()
    {
        if (hadOldValue_) {
            (void)setenv(name_.c_str(), oldValue_.c_str(), 1);
        } else {
            (void)unsetenv(name_.c_str());
        }
    }

    ScopedEnv(const ScopedEnv &) = delete;
    ScopedEnv &operator=(const ScopedEnv &) = delete;

private:
    std::string name_;
    std::string oldValue_;
    bool hadOldValue_ = false;
};

class InstrumentedAclDeviceManager : public AclDeviceManagerMock {
public:
    void SetBarrierWidth(size_t barrierWidth)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        barrierWidth_ = barrierWidth;
    }

    void SetFailSource(void *failSource)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        failSource_ = failSource;
    }

    void SetDeviceFailure(bool failSetDevice)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        failSetDevice_ = failSetDevice;
        setDeviceFailureCode_ = K_RUNTIME_ERROR;
        successfulSetDeviceCallsBeforeFailure_ = 0;
    }

    void SetDeviceOutOfMemoryAfter(size_t successfulCalls)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        failSetDevice_ = true;
        setDeviceFailureCode_ = K_OUT_OF_MEMORY;
        successfulSetDeviceCallsBeforeFailure_ = successfulCalls;
    }

    Status SetDevice(int32_t deviceId) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        setDeviceIds_.emplace_back(deviceId);
        if (failSetDevice_ && setDeviceIds_.size() > successfulSetDeviceCallsBeforeFailure_) {
            return Status(setDeviceFailureCode_, "Injected set device failure");
        }
        return Status::OK();
    }

    Status MemcpyBatch(void **dsts, size_t *destMax, void **srcs, size_t *sizes, size_t numBatches, MemcpyKind kind,
                       uint32_t deviceIdx, size_t *failIndex) override
    {
        bool shouldFail = false;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            activeBatchNum_++;
            maxActiveBatchNum_ = std::max(maxActiveBatchNum_, activeBatchNum_);
            callCount_++;
            batchSizes_.emplace_back(numBatches);
            copyKinds_.emplace_back(kind);
            callerThreadIds_.emplace(std::this_thread::get_id());
            shouldFail = failSource_ != nullptr && srcs[0] == failSource_;
            if (barrierWidth_ > 1 && callCount_ < barrierWidth_) {
                (void)barrierCv_.wait_for(lock, PARALLEL_H2D_TEST_BARRIER_TIMEOUT,
                                          [this] { return callCount_ >= barrierWidth_; });
            } else {
                barrierCv_.notify_all();
            }
        }

        auto status = shouldFail ? Status(K_RUNTIME_ERROR, "Injected parallel H2D batch failure")
                                 : AclDeviceManagerMock::MemcpyBatch(dsts, destMax, srcs, sizes, numBatches, kind,
                                                                     deviceIdx, failIndex);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            activeBatchNum_--;
        }
        barrierCv_.notify_all();
        return status;
    }

    size_t GetCallCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return callCount_;
    }

    size_t GetMaxActiveBatchNum() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return maxActiveBatchNum_;
    }

    std::vector<size_t> GetBatchSizes() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return batchSizes_;
    }

    size_t GetActiveBatchNum() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return activeBatchNum_;
    }

    bool WasCalledFrom(std::thread::id threadId) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return callerThreadIds_.count(threadId) > 0;
    }

    bool AllTasksSetDevice(int32_t deviceId, size_t expectedCount) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return setDeviceIds_.size() == expectedCount
               && std::all_of(setDeviceIds_.begin(), setDeviceIds_.end(),
                              [deviceId](int32_t actualDeviceId) { return actualDeviceId == deviceId; });
    }

    bool AllTasksUseKind(MemcpyKind kind) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return !copyKinds_.empty() && std::all_of(copyKinds_.begin(), copyKinds_.end(), [kind](MemcpyKind actualKind) {
            return actualKind == kind;
        });
    }

    size_t GetKindCallCount(MemcpyKind kind) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::count(copyKinds_.begin(), copyKinds_.end(), kind);
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable barrierCv_;
    size_t barrierWidth_ = 0;
    size_t callCount_ = 0;
    size_t activeBatchNum_ = 0;
    size_t maxActiveBatchNum_ = 0;
    void *failSource_ = nullptr;
    bool failSetDevice_ = false;
    StatusCode setDeviceFailureCode_ = K_RUNTIME_ERROR;
    size_t successfulSetDeviceCallsBeforeFailure_ = 0;
    std::set<std::thread::id> callerThreadIds_;
    std::vector<int32_t> setDeviceIds_;
    std::vector<size_t> batchSizes_;
    std::vector<MemcpyKind> copyKinds_;
};

ParallelH2DConfig BuildParallelConfig(size_t workerNum, size_t maxPendingTaskNum, size_t maxInflightBatchNum)
{
    ParallelH2DConfig config;
    config.workerNum = workerNum;
    config.aggregateNum = 1;
    config.maxPendingTaskNum = maxPendingTaskNum;
    config.minBytes = 0;
    config.maxInflightBatchNum = maxInflightBatchNum;
    return config;
}

void AssertPayloadsCopied(const std::vector<std::string> &payloads, const std::vector<std::vector<char>> &destBuffers)
{
    ASSERT_EQ(payloads.size(), destBuffers.size());
    for (size_t index = 0; index < payloads.size(); index++) {
        ASSERT_EQ(std::string(destBuffers[index].begin(), destBuffers[index].end()), payloads[index]);
    }
}
}  // namespace

class AclResourceManagerFallbackTest : public CommonTest {
protected:
    void SetUp() override
    {
        BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly([]() {
            return AclDeviceManagerMock::Instance();
        });
    }
};

class AclParallelMemcpyIntegrationTest : public CommonTest {};

TEST_F(AclResourceManagerFallbackTest, MixedDirectAndHugeFftsKeepsHostPool)
{
    constexpr uint64_t hostSize = 1024;
    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(hostSize, 1024, MemcopyPolicy::FFTS, MemcopyPolicy::DIRECT);

    resourceMgr.SetPolicyByHugeTlb(true);

    ASSERT_EQ(resourceMgr.GetD2HPolicy(), MemcopyPolicy::HUGE_FFTS);
    ASSERT_EQ(resourceMgr.GetH2DPolicy(), MemcopyPolicy::DIRECT);
    ASSERT_EQ(resourceMgr.GetHostMemSize(), hostSize);
}

TEST_F(AclResourceManagerFallbackTest, HugeFftsForBothDirectionsSkipsHostPool)
{
    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(1024, 1024, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);

    resourceMgr.SetPolicyByHugeTlb(true);

    ASSERT_EQ(resourceMgr.GetD2HPolicy(), MemcopyPolicy::HUGE_FFTS);
    ASSERT_EQ(resourceMgr.GetH2DPolicy(), MemcopyPolicy::HUGE_FFTS);
    ASSERT_EQ(resourceMgr.GetHostMemSize(), 0);
    DS_ASSERT_OK(resourceMgr.ValidateConfig());
}

TEST_F(AclResourceManagerFallbackTest, RejectZeroHostPoolWhenFftsUsesHostStaging)
{
    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(0, 1024, MemcopyPolicy::FFTS, MemcopyPolicy::DIRECT);

    DS_ASSERT_NOT_OK(resourceMgr.ValidateConfig());
}

TEST_F(AclResourceManagerFallbackTest, FallbackToDirectWhenHostPinPoolTooSmall)
{
    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(60, 1024, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);
    AclMemCopyPool copyPool(&resourceMgr);

    std::vector<std::string> payloads = { std::string(40, 'a'), std::string(30, 'b') };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(0, helper, MemcopyPolicy::FFTS));
    ASSERT_EQ(std::string(destBuffers[0].begin(), destBuffers[0].end()), payloads[0]);
    ASSERT_EQ(std::string(destBuffers[1].begin(), destBuffers[1].end()), payloads[1]);
}

TEST_F(AclResourceManagerFallbackTest, FallbackToDirectWhenDevicePinPoolTooSmall)
{
    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(1024, 100, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);
    AclMemCopyPool copyPool(&resourceMgr);

    std::vector<std::string> payloads = { std::string(60, 'c') };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(0, helper, MemcopyPolicy::FFTS));
    ASSERT_EQ(std::string(destBuffers[0].begin(), destBuffers[0].end()), payloads[0]);
}

TEST_F(AclResourceManagerFallbackTest, FallbackToDirectWhenFftsH2DMeetsOOMAfterGuardCheck)
{
    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(1024, 120, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);
    AclMemCopyPool copyPool(&resourceMgr);

    std::vector<std::string> payloads = { std::string(60, 'd') };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(0, helper, MemcopyPolicy::FFTS));
    ASSERT_EQ(std::string(destBuffers[0].begin(), destBuffers[0].end()), payloads[0]);
}

TEST_F(AclResourceManagerFallbackTest, RejectParallelDirectAsMemcpyPolicy)
{
    const std::string envName = "DS_TEST_MEMCPY_POLICY";
    ScopedEnv env(envName, "parallel_direct");
    MemcopyPolicy policy = MemcopyPolicy::DIRECT;

    DS_ASSERT_NOT_OK(GetPolicyFromEnv(envName.c_str(), policy));
    ASSERT_EQ(policy, MemcopyPolicy::DIRECT);
}

TEST_F(AclResourceManagerFallbackTest, ParallelH2DConfigLoadsDefaultsOverridesAndRejectsInvalidValues)
{
    ParallelH2DConfig defaultConfig;
    ASSERT_EQ(defaultConfig.workerNum, 1);
    ASSERT_EQ(defaultConfig.aggregateNum, 512);
    ASSERT_EQ(defaultConfig.minBytes, 24ULL * 1024ULL * 1024ULL);

    ScopedEnv workerNum("DS_H2D_PARALLEL_WORKER_NUM", "2");
    ScopedEnv aggregateNum("DS_H2D_PARALLEL_AGGREGATE_NUM", "3");
    ScopedEnv pendingTaskNum("DS_H2D_PARALLEL_MAX_PENDING_TASK_NUM", "4");
    ScopedEnv minBytes("DS_H2D_PARALLEL_MIN_BYTES", "0");
    ScopedEnv inflightBatchNum("DS_H2D_PARALLEL_MAX_INFLIGHT_BATCH_NUM", "3");
    ParallelH2DConfig overriddenConfig;
    DS_ASSERT_OK(overriddenConfig.LoadFromEnv());
    ASSERT_EQ(overriddenConfig.workerNum, 2);
    ASSERT_EQ(overriddenConfig.aggregateNum, 3);
    ASSERT_EQ(overriddenConfig.maxPendingTaskNum, 4);
    ASSERT_EQ(overriddenConfig.minBytes, 0);
    ASSERT_EQ(overriddenConfig.maxInflightBatchNum, 3);

    ScopedEnv invalidAggregateNum("DS_H2D_PARALLEL_AGGREGATE_NUM", "4097");
    ParallelH2DConfig invalidConfig;
    DS_ASSERT_NOT_OK(invalidConfig.LoadFromEnv());
}

TEST_F(AclResourceManagerFallbackTest, ParallelD2HConfigLoadsDefaultsOverridesAndRejectsInvalidValues)
{
    ParallelD2HConfig defaultConfig;
    ASSERT_EQ(defaultConfig.workerNum, 1);
    ASSERT_EQ(defaultConfig.aggregateNum, 512);
    ASSERT_EQ(defaultConfig.minBytes, 24ULL * 1024ULL * 1024ULL);

    ScopedEnv workerNum("DS_D2H_PARALLEL_WORKER_NUM", "2");
    ScopedEnv aggregateNum("DS_D2H_PARALLEL_AGGREGATE_NUM", "3");
    ScopedEnv pendingTaskNum("DS_D2H_PARALLEL_MAX_PENDING_TASK_NUM", "4");
    ScopedEnv minBytes("DS_D2H_PARALLEL_MIN_BYTES", "0");
    ScopedEnv inflightBatchNum("DS_D2H_PARALLEL_MAX_INFLIGHT_BATCH_NUM", "3");
    ParallelD2HConfig overriddenConfig;
    DS_ASSERT_OK(overriddenConfig.LoadFromEnv());
    ASSERT_EQ(overriddenConfig.workerNum, 2);
    ASSERT_EQ(overriddenConfig.aggregateNum, 3);
    ASSERT_EQ(overriddenConfig.maxPendingTaskNum, 4);
    ASSERT_EQ(overriddenConfig.minBytes, 0);
    ASSERT_EQ(overriddenConfig.maxInflightBatchNum, 3);

    ScopedEnv invalidAggregateNum("DS_D2H_PARALLEL_AGGREGATE_NUM", "4097");
    ParallelD2HConfig invalidConfig;
    DS_ASSERT_NOT_OK(invalidConfig.LoadFromEnv());
}

TEST_F(AclParallelMemcpyIntegrationTest, MemCopyPoolRoutesD2HParallelDirectToExecutor)
{
    constexpr uint32_t deviceId = 3;
    ScopedEnv workerNum("DS_D2H_PARALLEL_WORKER_NUM", "2");
    ScopedEnv aggregateNum("DS_D2H_PARALLEL_AGGREGATE_NUM", "1");
    ScopedEnv pendingTaskNum("DS_D2H_PARALLEL_MAX_PENDING_TASK_NUM", "2");
    ScopedEnv minBytes("DS_D2H_PARALLEL_MIN_BYTES", "0");
    ScopedEnv inflightBatchNum("DS_D2H_PARALLEL_MAX_INFLIGHT_BATCH_NUM", "2");
    InstrumentedAclDeviceManager deviceManager;
    deviceManager.SetBarrierWidth(2);
    BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly(Return(&deviceManager));

    TestAclResourceManager resourceMgr;
    AclMemCopyPool copyPool(&resourceMgr);
    std::vector<std::string> payloads = { "route-a", "route-b" };
    D2HTestBuffers buffers;
    auto helper = BuildD2HHelper(payloads, buffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchD2H(deviceId, helper, MemcopyPolicy::DIRECT));
    AssertPayloadsCopied(payloads, buffers.hostDestinations);
    ASSERT_EQ(deviceManager.GetMaxActiveBatchNum(), 2);
    ASSERT_TRUE(deviceManager.AllTasksSetDevice(deviceId, payloads.size()));
    ASSERT_TRUE(deviceManager.AllTasksUseKind(MemcpyKind::DEVICE_TO_HOST));
}

TEST_F(AclParallelMemcpyIntegrationTest, MemCopyPoolRoutesParallelDirectToExecutor)
{
    constexpr uint32_t deviceId = 3;
    ScopedEnv workerNum("DS_H2D_PARALLEL_WORKER_NUM", "2");
    ScopedEnv aggregateNum("DS_H2D_PARALLEL_AGGREGATE_NUM", "1");
    ScopedEnv pendingTaskNum("DS_H2D_PARALLEL_MAX_PENDING_TASK_NUM", "2");
    ScopedEnv minBytes("DS_H2D_PARALLEL_MIN_BYTES", "0");
    ScopedEnv inflightBatchNum("DS_H2D_PARALLEL_MAX_INFLIGHT_BATCH_NUM", "2");
    InstrumentedAclDeviceManager deviceManager;
    deviceManager.SetBarrierWidth(2);
    BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly(Return(&deviceManager));

    TestAclResourceManager resourceMgr;
    AclMemCopyPool copyPool(&resourceMgr);
    std::vector<std::string> payloads = { "route-a", "route-b" };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(deviceId, helper, MemcopyPolicy::DIRECT));
    AssertPayloadsCopied(payloads, destBuffers);
    ASSERT_EQ(deviceManager.GetMaxActiveBatchNum(), 2);
    ASSERT_TRUE(deviceManager.AllTasksSetDevice(deviceId, payloads.size()));
}

TEST_F(AclParallelMemcpyIntegrationTest, MemCopyPoolKeepsParallelDirectExecutorsSeparateByDirection)
{
    constexpr uint32_t deviceId = 3;
    ScopedEnv h2dWorkerNum("DS_H2D_PARALLEL_WORKER_NUM", "2");
    ScopedEnv h2dAggregateNum("DS_H2D_PARALLEL_AGGREGATE_NUM", "1");
    ScopedEnv h2dPendingTaskNum("DS_H2D_PARALLEL_MAX_PENDING_TASK_NUM", "2");
    ScopedEnv h2dMinBytes("DS_H2D_PARALLEL_MIN_BYTES", "0");
    ScopedEnv h2dInflightBatchNum("DS_H2D_PARALLEL_MAX_INFLIGHT_BATCH_NUM", "2");
    ScopedEnv d2hWorkerNum("DS_D2H_PARALLEL_WORKER_NUM", "2");
    ScopedEnv d2hAggregateNum("DS_D2H_PARALLEL_AGGREGATE_NUM", "1");
    ScopedEnv d2hPendingTaskNum("DS_D2H_PARALLEL_MAX_PENDING_TASK_NUM", "2");
    ScopedEnv d2hMinBytes("DS_D2H_PARALLEL_MIN_BYTES", "0");
    ScopedEnv d2hInflightBatchNum("DS_D2H_PARALLEL_MAX_INFLIGHT_BATCH_NUM", "2");
    InstrumentedAclDeviceManager deviceManager;
    BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly(Return(&deviceManager));

    TestAclResourceManager resourceMgr;
    AclMemCopyPool copyPool(&resourceMgr);
    std::vector<std::string> payloads = { "route-a", "route-b" };
    std::vector<std::vector<char>> h2dDestBuffers;
    auto h2dHelper = BuildH2DHelper(payloads, h2dDestBuffers);
    D2HTestBuffers d2hBuffers;
    auto d2hHelper = BuildD2HHelper(payloads, d2hBuffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(deviceId, h2dHelper, MemcopyPolicy::DIRECT));
    DS_ASSERT_OK(copyPool.MemcpyBatchD2H(deviceId, d2hHelper, MemcopyPolicy::DIRECT));

    AssertPayloadsCopied(payloads, d2hBuffers.hostDestinations);
    ASSERT_EQ(deviceManager.GetKindCallCount(MemcpyKind::HOST_TO_DEVICE), payloads.size());
    ASSERT_EQ(deviceManager.GetKindCallCount(MemcpyKind::DEVICE_TO_HOST), payloads.size());
}

TEST_F(AclParallelMemcpyIntegrationTest, SmallParallelDirectRequestFallsBackToDirect)
{
    constexpr uint32_t deviceId = 2;
    ScopedEnv workerNum("DS_H2D_PARALLEL_WORKER_NUM", "2");
    ScopedEnv aggregateNum("DS_H2D_PARALLEL_AGGREGATE_NUM", "2");
    ScopedEnv pendingTaskNum("DS_H2D_PARALLEL_MAX_PENDING_TASK_NUM", "2");
    ScopedEnv minBytes("DS_H2D_PARALLEL_MIN_BYTES", "0");
    ScopedEnv inflightBatchNum("DS_H2D_PARALLEL_MAX_INFLIGHT_BATCH_NUM", "2");
    InstrumentedAclDeviceManager deviceManager;
    BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly(Return(&deviceManager));

    TestAclResourceManager resourceMgr;
    AclMemCopyPool copyPool(&resourceMgr);
    std::vector<std::string> payloads = { "direct-a", "direct-b" };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);
    auto callerThreadId = std::this_thread::get_id();

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(deviceId, helper, MemcopyPolicy::DIRECT));
    AssertPayloadsCopied(payloads, destBuffers);
    ASSERT_EQ(deviceManager.GetCallCount(), 1);
    ASSERT_TRUE(deviceManager.WasCalledFrom(callerThreadId));
    ASSERT_TRUE(deviceManager.AllTasksSetDevice(deviceId, 0));
}

TEST_F(AclResourceManagerFallbackTest, ParallelFftsH2DConfigLoadsDefaultsOverridesAndRejectsInvalidValues)
{
    ParallelFftsH2DConfig defaultConfig;
    ASSERT_EQ(defaultConfig.workerNum, 1);
    ASSERT_EQ(defaultConfig.minBytes, 24ULL * 1024ULL * 1024ULL);

    ScopedEnv workerNum("DS_H2D_FFTS_PARALLEL_WORKER_NUM", "3");
    ScopedEnv minBytes("DS_H2D_FFTS_PARALLEL_MIN_BYTES", "0");
    ParallelFftsH2DConfig overriddenConfig;
    DS_ASSERT_OK(overriddenConfig.LoadFromEnv());
    ASSERT_EQ(overriddenConfig.workerNum, 3);
    ASSERT_EQ(overriddenConfig.minBytes, 0);

    ScopedEnv invalidWorkerNum("DS_H2D_FFTS_PARALLEL_WORKER_NUM", "17");
    ParallelFftsH2DConfig invalidConfig;
    DS_ASSERT_NOT_OK(invalidConfig.LoadFromEnv());
}

TEST_F(AclResourceManagerFallbackTest, ParallelFftsD2HConfigLoadsDefaultsOverridesAndRejectsInvalidValues)
{
    ParallelFftsD2HConfig defaultConfig;
    ASSERT_EQ(defaultConfig.workerNum, 1);
    ASSERT_EQ(defaultConfig.minBytes, 24ULL * 1024ULL * 1024ULL);

    ScopedEnv workerNum("DS_D2H_FFTS_PARALLEL_WORKER_NUM", "3");
    ScopedEnv minBytes("DS_D2H_FFTS_PARALLEL_MIN_BYTES", "0");
    ParallelFftsD2HConfig overriddenConfig;
    DS_ASSERT_OK(overriddenConfig.LoadFromEnv());
    ASSERT_EQ(overriddenConfig.workerNum, 3);
    ASSERT_EQ(overriddenConfig.minBytes, 0);

    ScopedEnv invalidWorkerNum("DS_D2H_FFTS_PARALLEL_WORKER_NUM", "17");
    ParallelFftsD2HConfig invalidConfig;
    DS_ASSERT_NOT_OK(invalidConfig.LoadFromEnv());
}

TEST_F(AclResourceManagerFallbackTest, ParallelFftsPartitionKeepsObjectsWholeAndRebasesBlobOffsets)
{
    std::vector<char> sourceA(20);
    std::vector<char> sourceB(50);
    std::vector<char> sourceC(30);
    std::vector<std::vector<char>> destinations(6, std::vector<char>(10));
    DeviceBatchCopyHelper helper;
    helper.srcBuffers = { { sourceA.data(), sourceA.size() },
                          { sourceB.data(), sourceB.size() },
                          { sourceC.data(), sourceC.size() } };
    for (auto &destination : destinations) {
        helper.dstBuffers.emplace_back(BufferView{ .ptr = destination.data(), .size = destination.size() });
    }
    helper.bufferMetas = { { 2, 0, sourceA.size() }, { 1, 2, sourceB.size() }, { 3, 3, sourceC.size() } };

    std::vector<DeviceBatchCopyHelper> shards;
    uint64_t deviceStagingBytes = 0;
    DS_ASSERT_OK(BuildParallelFftsShards(helper, 2, MemcpyKind::HOST_TO_DEVICE, shards, deviceStagingBytes));

    ASSERT_EQ(shards.size(), 2);
    ASSERT_EQ(shards[0].bufferMetas.size(), 2);
    ASSERT_EQ(shards[0].bufferMetas[0].firstBlobOffset, 0);
    ASSERT_EQ(shards[0].bufferMetas[1].firstBlobOffset, 2);
    ASSERT_EQ(shards[0].dstBuffers.size(), 3);
    ASSERT_EQ(shards[1].bufferMetas.size(), 1);
    ASSERT_EQ(shards[1].bufferMetas[0].firstBlobOffset, 0);
    ASSERT_EQ(shards[1].dstBuffers.size(), 3);
    ASSERT_EQ(deviceStagingBytes, 2 * (sourceB.size() + sourceC.size()));
}

TEST_F(AclResourceManagerFallbackTest, ParallelFftsPartitionRejectsInvalidBlobRange)
{
    std::vector<char> source(8);
    DeviceBatchCopyHelper helper;
    helper.srcBuffers = { { source.data(), source.size() } };
    helper.dstBuffers = { { source.data(), source.size() } };
    helper.bufferMetas = { { 2, 0, source.size() } };
    std::vector<DeviceBatchCopyHelper> shards;
    uint64_t deviceStagingBytes = 0;
    DS_ASSERT_NOT_OK(BuildParallelFftsShards(helper, 2, MemcpyKind::HOST_TO_DEVICE, shards, deviceStagingBytes));
}

TEST_F(AclResourceManagerFallbackTest, ParallelFftsD2HPartitionKeepsObjectsWholeAndRebasesBlobOffsets)
{
    std::vector<std::vector<char>> deviceSources(6, std::vector<char>(10));
    std::vector<char> hostA(20);
    std::vector<char> hostB(10);
    std::vector<char> hostC(30);
    DeviceBatchCopyHelper helper;
    for (auto &source : deviceSources) {
        helper.srcBuffers.emplace_back(BufferView{ .ptr = source.data(), .size = source.size() });
    }
    helper.dstBuffers = { { hostA.data(), hostA.size() },
                          { hostB.data(), hostB.size() },
                          { hostC.data(), hostC.size() } };
    helper.bufferMetas = { { 2, 0, hostA.size() }, { 1, 2, hostB.size() }, { 3, 3, hostC.size() } };

    std::vector<DeviceBatchCopyHelper> shards;
    uint64_t deviceStagingBytes = 0;
    DS_ASSERT_OK(BuildParallelFftsShards(helper, 2, MemcpyKind::DEVICE_TO_HOST, shards, deviceStagingBytes));

    ASSERT_EQ(shards.size(), 2);
    ASSERT_EQ(shards[0].dstBuffers.size(), 2);
    ASSERT_EQ(shards[0].srcBuffers.size(), 3);
    ASSERT_EQ(shards[0].bufferMetas[0].firstBlobOffset, 0);
    ASSERT_EQ(shards[0].bufferMetas[1].firstBlobOffset, 2);
    ASSERT_EQ(shards[1].dstBuffers.size(), 1);
    ASSERT_EQ(shards[1].srcBuffers.size(), 3);
    ASSERT_EQ(shards[1].bufferMetas[0].firstBlobOffset, 0);
    ASSERT_EQ(deviceStagingBytes, 2 * (hostA.size() + hostC.size()));
}

TEST_F(AclResourceManagerFallbackTest, ParallelFftsH2DBindsDeviceBeforeShardWork)
{
    constexpr uint32_t deviceId = 3;
    InstrumentedAclDeviceManager deviceManager;
    deviceManager.SetDeviceFailure(true);

    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(1024, 1024, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);
    ParallelFftsH2DConfig config;
    config.workerNum = 2;
    config.minBytes = 0;
    size_t copyCallCount = 0;
    auto copyFunction = [&copyCallCount](uint32_t, DeviceBatchCopyHelper &, bool, ThreadPool *) {
        ++copyCallCount;
        return Status::OK();
    };
    AclParallelFftsExecutor executor(&resourceMgr, &deviceManager, std::move(copyFunction), config);
    std::vector<std::string> payloads = { "parallel-ffts-a", "parallel-ffts-b" };
    std::vector<std::vector<char>> deviceDestinations;
    auto helper = BuildH2DHelper(payloads, deviceDestinations);

    auto status = executor.Memcpy(deviceId, helper);

    DS_ASSERT_NOT_OK(status);
    ASSERT_TRUE(deviceManager.AllTasksSetDevice(deviceId, config.workerNum));
    ASSERT_EQ(deviceManager.GetCallCount(), 0);
    ASSERT_EQ(copyCallCount, 0);
}

TEST_F(AclResourceManagerFallbackTest, ParallelFftsD2HBindsDeviceBeforeShardWork)
{
    constexpr uint32_t deviceId = 4;
    InstrumentedAclDeviceManager deviceManager;
    deviceManager.SetDeviceFailure(true);

    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(1024, 1024, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);
    ParallelFftsD2HConfig config;
    config.workerNum = 2;
    config.minBytes = 0;
    size_t copyCallCount = 0;
    auto copyFunction = [&copyCallCount](uint32_t, DeviceBatchCopyHelper &, bool, ThreadPool *) {
        ++copyCallCount;
        return Status::OK();
    };
    AclParallelFftsExecutor executor(&resourceMgr, &deviceManager, std::move(copyFunction), config);
    std::vector<std::string> payloads = { "parallel-ffts-a", "parallel-ffts-b" };
    D2HTestBuffers buffers;
    auto helper = BuildD2HHelper(payloads, buffers);

    auto status = executor.Memcpy(deviceId, helper);

    DS_ASSERT_NOT_OK(status);
    ASSERT_TRUE(deviceManager.AllTasksSetDevice(deviceId, config.workerNum));
    ASSERT_EQ(deviceManager.GetCallCount(), 0);
    ASSERT_EQ(copyCallCount, 0);
}

TEST_F(AclParallelMemcpyIntegrationTest, ParallelFftsD2HFallsBackToDirectAfterOutOfMemory)
{
    constexpr uint32_t deviceId = 4;
    ScopedEnv workerNum("DS_D2H_FFTS_PARALLEL_WORKER_NUM", "2");
    ScopedEnv minBytes("DS_D2H_FFTS_PARALLEL_MIN_BYTES", "0");
    InstrumentedAclDeviceManager deviceManager;
    // The caller binds the device once; fail the subsequent bindings in FFTS shard workers.
    deviceManager.SetDeviceOutOfMemoryAfter(1);
    BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly(Return(&deviceManager));

    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(1024, 1024, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);
    AclMemCopyPool copyPool(&resourceMgr);
    std::vector<std::string> payloads = { "parallel-ffts-a", "parallel-ffts-b" };
    D2HTestBuffers buffers;
    auto helper = BuildD2HHelper(payloads, buffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchD2H(deviceId, helper, MemcopyPolicy::FFTS));

    AssertPayloadsCopied(payloads, buffers.hostDestinations);
    ASSERT_EQ(deviceManager.GetCallCount(), 1);
    ASSERT_TRUE(deviceManager.AllTasksUseKind(MemcpyKind::DEVICE_TO_HOST));
}

TEST_F(AclParallelMemcpyIntegrationTest, ParallelDirectFallsBackWhenTasksCannotCoverWorkers)
{
    constexpr uint32_t deviceId = 2;
    ScopedEnv workerNum("DS_H2D_PARALLEL_WORKER_NUM", "4");
    ScopedEnv aggregateNum("DS_H2D_PARALLEL_AGGREGATE_NUM", "2");
    ScopedEnv pendingTaskNum("DS_H2D_PARALLEL_MAX_PENDING_TASK_NUM", "8");
    ScopedEnv minBytes("DS_H2D_PARALLEL_MIN_BYTES", "0");
    ScopedEnv inflightBatchNum("DS_H2D_PARALLEL_MAX_INFLIGHT_BATCH_NUM", "5");
    InstrumentedAclDeviceManager deviceManager;
    BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly(Return(&deviceManager));

    TestAclResourceManager resourceMgr;
    AclMemCopyPool copyPool(&resourceMgr);
    std::vector<std::string> payloads = { "direct-a", "direct-b", "direct-c", "direct-d", "direct-e", "direct-f" };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);
    auto callerThreadId = std::this_thread::get_id();

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(deviceId, helper, MemcopyPolicy::DIRECT));
    AssertPayloadsCopied(payloads, destBuffers);
    ASSERT_EQ(deviceManager.GetCallCount(), 1);
    ASSERT_TRUE(deviceManager.WasCalledFrom(callerThreadId));
    ASSERT_TRUE(deviceManager.AllTasksSetDevice(deviceId, 0));
}

TEST_F(AclResourceManagerFallbackTest, ParallelExecutorRunsBatchesConcurrently)
{
    constexpr uint32_t deviceId = 3;
    InstrumentedAclDeviceManager deviceManager;
    deviceManager.SetBarrierWidth(2);
    auto config = BuildParallelConfig(2, 4, 2);
    AclParallelDirectExecutor executor(deviceId, &deviceManager, config);
    DS_ASSERT_OK(executor.Init());

    std::vector<std::string> payloads = { "parallel-a", "parallel-b", "parallel-c", "parallel-d" };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);
    DS_ASSERT_OK(executor.MemcpyBatch(helper));

    AssertPayloadsCopied(payloads, destBuffers);
    ASSERT_EQ(deviceManager.GetCallCount(), payloads.size());
    ASSERT_GE(deviceManager.GetMaxActiveBatchNum(), 2);
    ASSERT_TRUE(deviceManager.AllTasksSetDevice(deviceId, config.workerNum));
    executor.Shutdown();
    executor.Shutdown();
    DS_ASSERT_NOT_OK(executor.Init());
    DS_ASSERT_NOT_OK(executor.MemcpyBatch(helper));
}

TEST_F(AclResourceManagerFallbackTest, ParallelExecutorBalancesTailByBytes)
{
    constexpr uint32_t deviceId = 3;
    InstrumentedAclDeviceManager deviceManager;
    deviceManager.SetBarrierWidth(2);
    auto config = BuildParallelConfig(2, 4, 2);
    config.aggregateNum = 4;
    AclParallelDirectExecutor executor(deviceId, &deviceManager, config);
    DS_ASSERT_OK(executor.Init());

    std::vector<std::string> payloads = { "payload-a", "payload-b", "payload-c", "payload-d", "payload-e" };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);
    DS_ASSERT_OK(executor.MemcpyBatch(helper));

    AssertPayloadsCopied(payloads, destBuffers);
    auto batchSizes = deviceManager.GetBatchSizes();
    std::sort(batchSizes.begin(), batchSizes.end());
    ASSERT_EQ(batchSizes, (std::vector<size_t>{ 2, 3 }));
    ASSERT_TRUE(deviceManager.AllTasksSetDevice(deviceId, config.workerNum));
}

TEST_F(AclResourceManagerFallbackTest, ParallelExecutorUsesBoundedCallerRunsWhenQueueIsFull)
{
    constexpr uint32_t deviceId = 2;
    InstrumentedAclDeviceManager deviceManager;
    deviceManager.SetBarrierWidth(2);
    auto config = BuildParallelConfig(1, 1, 2);
    AclParallelDirectExecutor executor(deviceId, &deviceManager, config);
    DS_ASSERT_OK(executor.Init());

    std::vector<std::string> payloads = { "inline-a", "inline-b", "inline-c" };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);
    auto callerThreadId = std::this_thread::get_id();
    DS_ASSERT_OK(executor.MemcpyBatch(helper));

    AssertPayloadsCopied(payloads, destBuffers);
    ASSERT_TRUE(deviceManager.WasCalledFrom(callerThreadId));
    ASSERT_EQ(deviceManager.GetMaxActiveBatchNum(), 2);
    ASSERT_EQ(deviceManager.GetCallCount(), payloads.size());
}

TEST_F(AclResourceManagerFallbackTest, ParallelExecutorDrainsSubmittedTasksAfterFailure)
{
    constexpr uint32_t deviceId = 1;
    InstrumentedAclDeviceManager deviceManager;
    deviceManager.SetBarrierWidth(2);
    auto config = BuildParallelConfig(2, 2, 2);
    AclParallelDirectExecutor executor(deviceId, &deviceManager, config);
    DS_ASSERT_OK(executor.Init());

    std::vector<std::string> payloads = { "failed-slice", "successful-slice" };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildH2DHelper(payloads, destBuffers);
    deviceManager.SetFailSource(helper.srcList[0]);

    auto status = executor.MemcpyBatch(helper);
    DS_ASSERT_NOT_OK(status);
    ASSERT_EQ(deviceManager.GetCallCount(), payloads.size());
    ASSERT_EQ(deviceManager.GetActiveBatchNum(), 0);
    ASSERT_EQ(std::string(destBuffers[1].begin(), destBuffers[1].end()), payloads[1]);
}
}  // namespace st
}  // namespace datasystem
