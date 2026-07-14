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

// End-to-end coverage for the process-level local send-Jetty pool. The environment tests under tests/ut/client
// validate the resource state machine; these tests validate Get/Set traffic through real workers and clients.

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/string_intern/string_pool.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/kv_client.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
namespace {
#ifdef USE_URMA
constexpr char kHostIp[] = "127.0.0.1";
constexpr uint32_t kWorkerCount = 2;
constexpr uint32_t kSourceWorker = 0;
constexpr uint32_t kDestinationWorker = 1;
constexpr size_t kValueSize = 512 * 1024;
constexpr char kSendLaneReleaseInject[] = "UrmaManager.ApplySendLaneAction.Release";
constexpr char kModifyJettyInject[] = "urma.ModifyJettyToError";

class UrmaSendJettyPoolStTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = kWorkerCount;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        for (uint32_t i = 0; i < kWorkerCount; ++i) {
            opts.workerConfigs.emplace_back(kHostIp, GetFreePort());
        }
        opts.vLogLevel = 2;
        opts.workerGflagParams = std::string(
            " -shared_memory_size_mb=5120 -payload_nocopy_threshold=1000000 -enable_worker_worker_batch_get=true"
            " -batch_get_threshold_mb=20 -arena_per_tenant=1"
            " -urma_send_jetty_lane_pool_size=")
            + std::to_string(SendLanePoolSize()) + " -urma_send_jetty_lane_refill_extra_size="
            + std::to_string(SendLaneRefillExtraSize()) + " -ipc_through_shared_memory=false";
        opts.workerGflagParams += " -enable_urma=true";
    }

    void SetUp() override
    {
        ImmutableStringPool::Instance().Init();
        intern::StringPool::InitAll();
        ExternalClusterTest::SetUp();
    }

protected:
    virtual uint32_t SendLanePoolSize() const
    {
        return 1;
    }

    virtual uint32_t SendLaneRefillExtraSize() const
    {
        return 1;
    }

    void WaitForWorkerInjectExecuteCount(uint32_t workerIdx, const std::string &name, uint64_t expectedCount,
                                         uint64_t timeoutMs = 5000)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        uint64_t executeCount = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIdx, name, executeCount));
            if (executeCount >= expectedCount) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIdx, name, executeCount));
        ASSERT_GE(executeCount, expectedCount) << name;
    }

    void GetEventually(KVClient &reader, const std::string &key, const std::string &expectedValue)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        Status lastStatus(K_RUNTIME_ERROR, "remote Get was not attempted");
        std::string got;
        while (std::chrono::steady_clock::now() < deadline) {
            got.clear();
            lastStatus = reader.Get(key, got);
            if (lastStatus.IsOk()) {
                ASSERT_EQ(got, expectedValue);
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "remote Get did not recover after send-Jetty retirement: " << lastStatus.ToString();
    }

};

class UrmaSendJettyPoolConcurrentStTest : public UrmaSendJettyPoolStTest {
protected:
    uint32_t SendLanePoolSize() const override
    {
        return 8;
    }

    uint32_t SendLaneRefillExtraSize() const override
    {
        return 8;
    }
};

TEST_F(UrmaSendJettyPoolStTest, SmallPoolReusesLaneForSequentialSetAndRemoteGet)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader);

    constexpr uint32_t kRequestCount = 16;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kRequestCount);
    values.reserve(kRequestCount);
    for (uint32_t i = 0; i < kRequestCount; ++i) {
        keys.emplace_back("urma-send-lane-sequential-" + std::to_string(i));
        values.emplace_back(kValueSize, static_cast<char>('a' + (i % 26)));
        DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "64*call()"));
    for (uint32_t i = 0; i < kRequestCount; ++i) {
        std::string got;
        DS_ASSERT_OK(reader->Get(keys[i], got));
        ASSERT_EQ(got, values[i]);
    }
    WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, kRequestCount);
}

TEST_F(UrmaSendJettyPoolConcurrentStTest, ConcurrentRemoteGetsSucceedAtConfiguredPoolCapacity)
{
    constexpr uint32_t kConcurrentRequests = 8;
    std::shared_ptr<KVClient> writer;
    InitTestKVClient(kSourceWorker, writer);

    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kConcurrentRequests);
    values.reserve(kConcurrentRequests);
    for (uint32_t i = 0; i < kConcurrentRequests; ++i) {
        keys.emplace_back("urma-send-lane-concurrent-" + std::to_string(i));
        values.emplace_back(kValueSize, static_cast<char>('a' + i));
        DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    }

    std::vector<std::shared_ptr<KVClient>> readers(kConcurrentRequests);
    for (auto &reader : readers) {
        InitTestKVClient(kDestinationWorker, reader);
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "64*call()"));

    ThreadPool threadPool(kConcurrentRequests);
    std::promise<void> start;
    const auto startFuture = start.get_future().share();
    std::vector<std::future<bool>> futures;
    futures.reserve(kConcurrentRequests);
    for (uint32_t i = 0; i < kConcurrentRequests; ++i) {
        futures.emplace_back(threadPool.Submit([startFuture, reader = readers[i], key = keys[i], value = values[i]] {
            startFuture.wait();
            std::string got;
            const auto status = reader->Get(key, got);
            return status.IsOk() && got == value;
        }));
    }
    start.set_value();
    for (auto &future : futures) {
        EXPECT_TRUE(future.get());
    }
    WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, kConcurrentRequests);
}

TEST_F(UrmaSendJettyPoolStTest, CqeRetirementRefillsSmallPoolAndSubsequentRemoteGetSucceeds)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader);

    const std::string keyBefore = "urma-send-lane-before-cqe";
    const std::string keyAfter = "urma-send-lane-after-cqe";
    const std::string valueBefore(kValueSize, 'a');
    const std::string valueAfter(kValueSize, 'b');
    DS_ASSERT_OK(writer->Set(keyBefore, valueBefore));
    DS_ASSERT_OK(writer->Set(keyAfter, valueAfter));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kModifyJettyInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, "UrmaManager.CheckCompletionRecordStatus",
                                           "1*call(0, 9)"));
    std::string got;
    DS_ASSERT_OK(reader->Get(keyBefore, got));
    ASSERT_EQ(got, valueBefore);
    WaitForWorkerInjectExecuteCount(kSourceWorker, kModifyJettyInject, 1);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "16*call()"));
    GetEventually(*reader, keyAfter, valueAfter);
    WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, 1);
}

#else
TEST(UrmaSendJettyPoolStTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif

}  // namespace
}  // namespace st
}  // namespace datasystem
