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
#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
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
constexpr char kSendLaneRetireInject[] = "UrmaManager.ApplySendLaneAction.Retire";
constexpr char kModifyJettyInject[] = "urma.ModifyJettyToError";
constexpr char kPauseWriteAfterPostInject[] = "UrmaManager.UrmaWriteAfterPost";
constexpr char kWriteErrorInject[] = "UrmaManager.UrmaWriteError";
constexpr char kPauseGatherWriteAfterAcquireInject[] = "UrmaManager.GatherWriteAfterAcquire";
constexpr char kGatherWriteInject[] = "UrmaManager.GatherWriteError";
constexpr char kPoolExhaustedInject[] = "UrmaManager.AcquireSendLaneFromConnection.PoolExhausted";
constexpr char kBatchGetAfterAcquireInject[] = "WorkerWorkerOCServiceImpl.BatchGetAfterAcquireSendLane";
constexpr char kCqeStatusInject[] = "UrmaManager.CheckCompletionRecordStatus";
constexpr char kInFlightTimeoutInject[] = "UrmaManager.UrmaWaitInFlightTimeout";
constexpr char kAsyncDeleteCompleteInject[] = "urma.SendJettyAsyncDeleteComplete";
constexpr char kRegistryUnregisterInject[] = "urma.SendJettyRegistryUnregister";
constexpr char kRefillAddedInject[] = "urma.SendJettyPoolRefillAdded";

enum class PoolExhaustionFallbackOutcome {
    SUCCESS,
    LIMITER_REJECTED,
};

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
            " -batch_get_threshold_mb=20 -arena_per_tenant=1 -oc_worker_worker_parallel_nums=64"
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

    bool ObserveWorkerInjectExecuteCount(uint32_t workerIdx, const std::string &name, uint64_t expectedCount,
                                         uint64_t &executeCount, uint64_t timeoutMs = 5000)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline) {
            auto rc = cluster_->GetInjectActionExecuteCount(WORKER, workerIdx, name, executeCount);
            if (rc.IsError()) {
                return false;
            }
            if (executeCount >= expectedCount) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return cluster_->GetInjectActionExecuteCount(WORKER, workerIdx, name, executeCount).IsOk()
               && executeCount >= expectedCount;
    }

    void AssertBatchGetSharesOneLane(const std::shared_ptr<KVClient> &reader, const std::vector<std::string> &keys,
                                     const std::vector<std::string> &values, const char *pauseInject)
    {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneRetireInject, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, pauseInject, "1*pause()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject, "call()"));

        std::vector<std::string> got;
        std::promise<Status> getPromise;
        auto getFuture = getPromise.get_future();
        std::thread getThread([reader, keys, &got, promise = std::move(getPromise)]() mutable {
            promise.set_value(reader->Get(keys, got));
        });

        bool paused = false;
        Status pauseStatus = Status::OK();
        Status exhaustedCountStatus = Status::OK();
        uint64_t pauseCount = 0;
        uint64_t exhaustedCount = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            pauseStatus = cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, pauseInject, pauseCount);
            exhaustedCountStatus =
                cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kPoolExhaustedInject, exhaustedCount);
            if (pauseStatus.IsError() || exhaustedCountStatus.IsError()) {
                break;
            }
            paused = pauseCount > 0;
            if (paused || getFuture.wait_for(std::chrono::milliseconds(50)) == std::future_status::ready) {
                break;
            }
        }

        if (paused) {
            const auto overlapDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            while (std::chrono::steady_clock::now() < overlapDeadline) {
                exhaustedCountStatus = cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker,
                                                                               kPoolExhaustedInject, exhaustedCount);
                if (exhaustedCountStatus.IsError() || exhaustedCount > 0) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }

        const auto clearPauseStatus = cluster_->ClearInjectAction(WORKER, kSourceWorker, pauseInject);
        const auto clearExhaustedStatus = cluster_->ClearInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject);
        getThread.join();
        const auto getStatus = getFuture.get();

        ASSERT_TRUE(pauseStatus.IsOk()) << pauseStatus.ToString();
        ASSERT_TRUE(exhaustedCountStatus.IsOk()) << exhaustedCountStatus.ToString();
        ASSERT_TRUE(clearPauseStatus.IsOk()) << clearPauseStatus.ToString();
        ASSERT_TRUE(clearExhaustedStatus.IsOk()) << clearExhaustedStatus.ToString();
        ASSERT_TRUE(paused) << "the expected Batch Get lane overlap was not observed: " << getStatus.ToString();
        ASSERT_EQ(exhaustedCount, 0U) << "Batch Get acquired more than its RPC-scoped lane";
        DS_ASSERT_OK(getStatus);
        WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, 1);

        uint64_t retireCount = 0;
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kSendLaneRetireInject, retireCount));
        ASSERT_EQ(retireCount, 0U) << "Batch Get unexpectedly retired its shared lane";
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneRetireInject));
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject));

        // The next RPC must be able to acquire the lane returned by the completed Batch Get.
        std::vector<std::string> reused;
        DS_ASSERT_OK(reader->Get({ keys.front() }, reused));
        ASSERT_EQ(reused.size(), 1U);
        ASSERT_EQ(reused.front(), values.front());

        ASSERT_EQ(got.size(), values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            ASSERT_EQ(got[i], values[i]) << "object index: " << i;
        }
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

    void AssertPoolExhaustedBatchFallsBack(const std::shared_ptr<KVClient> &reader,
                                           const std::vector<std::string> &keys,
                                           const std::vector<std::string> &values, const char *unexpectedUrmaInject,
                                           PoolExhaustionFallbackOutcome expectedOutcome =
                                               PoolExhaustionFallbackOutcome::SUCCESS)
    {
        const std::string heldKey = keys.front() + "-held-lane";
        const std::string heldValue(kValueSize, 'h');
        std::shared_ptr<KVClient> writer;
        std::shared_ptr<KVClient> heldReader;
        InitTestKVClient(kSourceWorker, writer);
        InitTestKVClient(kDestinationWorker, heldReader, 10000, false, 10000);
        DS_ASSERT_OK(writer->Set(heldKey, heldValue));

        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kPauseWriteAfterPostInject, "1*pause()"));
        std::promise<Status> heldPromise;
        auto heldFuture = heldPromise.get_future();
        std::thread heldThread([heldReader, heldKey, heldValue, promise = std::move(heldPromise)]() mutable {
            std::string got;
            auto rc = heldReader->Get(heldKey, got);
            if (rc.IsOk() && got != heldValue) {
                rc = Status(K_RUNTIME_ERROR, "held-lane Get returned unexpected data");
            }
            promise.set_value(rc);
        });
        Status pauseCountStatus = Status::OK();
        uint64_t pauseCount = 0;
        const auto pauseDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < pauseDeadline) {
            pauseCountStatus = cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker,
                                                                      kPauseWriteAfterPostInject, pauseCount);
            if (pauseCountStatus.IsError() || pauseCount > 0) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        Status setExhaustedStatus(K_RUNTIME_ERROR, "held lane was not observed");
        Status setUnexpectedStatus(K_RUNTIME_ERROR, "held lane was not observed");
        Status getStatus(K_RUNTIME_ERROR, "fallback Batch Get was not attempted");
        std::vector<std::string> got;
        uint64_t exhaustedCount = 0;
        uint64_t unexpectedUrmaCount = 0;
        Status exhaustedCountStatus = Status::OK();
        Status unexpectedCountStatus = Status::OK();
        if (pauseCount > 0) {
            setExhaustedStatus =
                cluster_->SetInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject, "call()");
            setUnexpectedStatus = cluster_->SetInjectAction(WORKER, kSourceWorker, unexpectedUrmaInject, "call()");
            if (setExhaustedStatus.IsOk() && setUnexpectedStatus.IsOk()) {
                getStatus = reader->Get(keys, got);
                exhaustedCountStatus = cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker,
                                                                              kPoolExhaustedInject, exhaustedCount);
                unexpectedCountStatus = cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker,
                                                                               unexpectedUrmaInject,
                                                                               unexpectedUrmaCount);
            }
        }

        const auto clearPause = cluster_->ClearInjectAction(WORKER, kSourceWorker, kPauseWriteAfterPostInject);
        const auto clearExhausted = setExhaustedStatus.IsOk()
                                        ? cluster_->ClearInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject)
                                        : Status::OK();
        const auto clearUnexpected = setUnexpectedStatus.IsOk()
                                         ? cluster_->ClearInjectAction(WORKER, kSourceWorker, unexpectedUrmaInject)
                                         : Status::OK();
        heldThread.join();
        const auto heldStatus = heldFuture.get();

        ASSERT_TRUE(pauseCountStatus.IsOk()) << pauseCountStatus.ToString();
        ASSERT_GT(pauseCount, 0U) << "the request holding the only send lane was not observed";
        ASSERT_TRUE(setExhaustedStatus.IsOk()) << setExhaustedStatus.ToString();
        ASSERT_TRUE(setUnexpectedStatus.IsOk()) << setUnexpectedStatus.ToString();
        ASSERT_TRUE(exhaustedCountStatus.IsOk()) << exhaustedCountStatus.ToString();
        ASSERT_TRUE(unexpectedCountStatus.IsOk()) << unexpectedCountStatus.ToString();
        ASSERT_TRUE(clearPause.IsOk()) << clearPause.ToString();
        ASSERT_TRUE(clearExhausted.IsOk()) << clearExhausted.ToString();
        ASSERT_TRUE(clearUnexpected.IsOk()) << clearUnexpected.ToString();
        ASSERT_TRUE(heldStatus.IsOk()) << heldStatus.ToString();
        ASSERT_EQ(unexpectedUrmaCount, 0U) << "TCP-only Batch Get attempted an object or aggregate URMA write";
        if (expectedOutcome == PoolExhaustionFallbackOutcome::LIMITER_REJECTED) {
            // The worker preserves the pool-exhaustion K_TRY_AGAIN when the limiter rejects fallback. Higher request
            // layers may retry it until their deadline, and the client RPC timeout can replace the inner error. The
            // stable end-to-end contract is failure; the ordinary fallback test above is the below-limit success
            // control for the same pool-exhaustion trigger.
            ASSERT_TRUE(getStatus.IsError()) << getStatus.ToString();
            ASSERT_GT(exhaustedCount, 0U);
        } else {
            ASSERT_TRUE(getStatus.IsOk()) << getStatus.ToString();
            ASSERT_EQ(exhaustedCount, 1U) << "one Batch Get RPC must attempt to acquire only once";
            ASSERT_EQ(got, values);
        }
    }

    void AssertConcurrentFaultStormRecovers(const std::string &faultInject, const std::string &faultAction,
                                             const std::string &keyPrefix)
    {
        constexpr uint32_t kConcurrentRequests = 8;
        constexpr uint32_t kFaultCount = 4;
        constexpr uint32_t kCqeRequestCount = kFaultCount * kFaultCount;
        constexpr uint32_t kRecoveryRequestOffset = kCqeRequestCount;
        constexpr uint32_t kPreparedRequestCount = kRecoveryRequestOffset + kConcurrentRequests;
        // A single object larger than the aggregate threshold forces one ordinary WR/event per Batch Get RPC. Every
        // request also owns a distinct key so destination-side remote-pull locking or caching cannot collapse
        // concurrent client Gets into one worker-to-worker Batch Get.
        constexpr size_t kObjectSize = 128 * 1024;
        std::shared_ptr<KVClient> writer;
        InitTestKVClient(kSourceWorker, writer);
        std::vector<std::vector<std::string>> requestKeys(kPreparedRequestCount);
        std::vector<std::vector<std::string>> expectedValues(kPreparedRequestCount);
        for (uint32_t i = 0; i < kPreparedRequestCount; ++i) {
            requestKeys[i].emplace_back(keyPrefix + "-request-" + std::to_string(i));
            expectedValues[i].emplace_back(kObjectSize, static_cast<char>('a' + (i % 26)));
            DS_ASSERT_OK(writer->Set(requestKeys[i].front(), expectedValues[i].front()));
        }

        std::vector<std::shared_ptr<KVClient>> readers(kConcurrentRequests);
        for (auto &reader : readers) {
            // Fault injection deliberately holds worker RPCs. Leave enough headroom that queued healthy requests do not
            // turn into unrelated real request-deadline timeouts.
            InitTestKVClient(kDestinationWorker, reader, 60000, false, 60000);
        }
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kModifyJettyInject, "8*call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kRefillAddedInject, "8*call()"));
        std::vector<bool> requestResults;
        auto runWave = [&](uint32_t requestOffset, uint32_t requestCount, bool observeHeldLanes = false,
                           uint32_t expectedHeldLanes = 0) {
            ThreadPool threadPool(requestCount);
            std::promise<void> start;
            const auto startFuture = start.get_future().share();
            std::vector<std::future<bool>> futures;
            futures.reserve(requestCount);
            for (uint32_t i = 0; i < requestCount; ++i) {
                const auto requestIndex = requestOffset + i;
                futures.emplace_back(threadPool.Submit(
                    [startFuture, reader = readers[i], &requestKeys, &expectedValues, requestIndex] {
                        startFuture.wait();
                        std::vector<std::string> got;
                        const auto rc = reader->Get(requestKeys[requestIndex], got);
                        return rc.IsOk() && got == expectedValues[requestIndex];
                    }));
            }
            start.set_value();
            bool heldLanesObserved = true;
            if (observeHeldLanes) {
                uint64_t acquiredCount = 0;
                const auto expectedCount = expectedHeldLanes == 0 ? requestCount : expectedHeldLanes;
                heldLanesObserved = ObserveWorkerInjectExecuteCount(
                    kSourceWorker, kBatchGetAfterAcquireInject, expectedCount, acquiredCount, 10000);
            }
            for (auto &future : futures) {
                requestResults.emplace_back(future.get());
            }
            return heldLanesObserved;
        };

        Status clearFault = Status::OK();
        bool timeoutWaveHeldFaultLanes = true;
        bool timeoutRetirementsObserved = true;
        uint64_t timeoutRetireCount = kFaultCount;
        Status clearTimeoutRetire = Status::OK();
        Status clearCompletionPause = Status::OK();
        if (faultInject == kCqeStatusInject) {
            // The CQE hook executes once per poll batch and rewrites only record[0]. Use four isolated rounds so one
            // batched poll cannot consume all four actions or map multiple status=9 records to the same shared lane.
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kAsyncDeleteCompleteInject, "8*call()"));
            for (uint32_t round = 0; round < kFaultCount; ++round) {
                DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, faultInject, "1*call(0, 9)"));
                (void)runWave(round * kFaultCount, kFaultCount);
                clearFault = cluster_->ClearInjectAction(WORKER, kSourceWorker, faultInject);
                if (clearFault.IsError()) {
                    break;
                }
                uint64_t count = 0;
                if (!ObserveWorkerInjectExecuteCount(kSourceWorker, kModifyJettyInject, round + 1, count, 10000)
                    || !ObserveWorkerInjectExecuteCount(kSourceWorker, kRefillAddedInject, round + 1, count, 10000)
                    || !ObserveWorkerInjectExecuteCount(kSourceWorker, kAsyncDeleteCompleteInject, round + 1, count,
                                                        10000)) {
                    break;
                }
            }
            DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kAsyncDeleteCompleteInject));
        } else {
            // A successful CQE can otherwise release a fast event between GetEvent and the injected timeout. Pause the
            // existing completion-status hook for the first poll batch so the first four Wait calls must retire four
            // leases that are still in flight. This uses existing test hooks and does not alter production semantics.
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kCqeStatusInject, "1*sleep(5000)"));
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneRetireInject, "8*call()"));
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kBatchGetAfterAcquireInject, "8*call()"));
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, faultInject, faultAction));
            timeoutWaveHeldFaultLanes = runWave(0, kConcurrentRequests, true, kFaultCount);
            timeoutRetirementsObserved = ObserveWorkerInjectExecuteCount(
                kSourceWorker, kSendLaneRetireInject, kFaultCount, timeoutRetireCount, 10000);
            clearFault = cluster_->ClearInjectAction(WORKER, kSourceWorker, faultInject);
            DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kBatchGetAfterAcquireInject));
            clearCompletionPause = cluster_->ClearInjectAction(WORKER, kSourceWorker, kCqeStatusInject);
            clearTimeoutRetire = cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneRetireInject);
        }

        uint64_t modifyCount = 0;
        const bool observedRetirements =
            ObserveWorkerInjectExecuteCount(kSourceWorker, kModifyJettyInject, kFaultCount, modifyCount, 30000);
        uint64_t refillAddedCount = 0;
        const bool replacementsAdded = ObserveWorkerInjectExecuteCount(
            kSourceWorker, kRefillAddedInject, kFaultCount, refillAddedCount, 10000);

        // Use fresh keys for a recovery wave after refill converges. Full simultaneous occupancy is covered by the
        // dedicated capacity test; here we verify that all recovery requests use URMA, release their leases, and do not
        // observe a pool that remains drained after the injected retirements.
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kBatchGetAfterAcquireInject, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "call()"));
        std::vector<Status> recoveryStatuses(kConcurrentRequests);
        std::vector<std::thread> recoveryThreads;
        recoveryThreads.reserve(kConcurrentRequests);
        for (uint32_t i = 0; i < kConcurrentRequests; ++i) {
            recoveryThreads.emplace_back([&, i] {
                std::vector<std::string> got;
                auto rc = readers[i]->Get(requestKeys[kRecoveryRequestOffset + i], got);
                if (rc.IsOk() && got != expectedValues[kRecoveryRequestOffset + i]) {
                    rc = Status(K_RUNTIME_ERROR, "recovery Batch Get returned unexpected data");
                }
                recoveryStatuses[i] = rc;
            });
        }
        for (auto &thread : recoveryThreads) {
            thread.join();
        }
        uint64_t exhaustedCount = 0;
        const auto exhaustedCountStatus = cluster_->GetInjectActionExecuteCount(
            WORKER, kSourceWorker, kPoolExhaustedInject, exhaustedCount);
        const auto clearExhausted =
            cluster_->ClearInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject);
        uint64_t recoveryAcquireCount = 0;
        uint64_t recoveryReleaseCount = 0;
        uint64_t finalModifyCount = 0;
        uint64_t finalRefillAddedCount = 0;
        const auto acquireCountStatus = cluster_->GetInjectActionExecuteCount(
            WORKER, kSourceWorker, kBatchGetAfterAcquireInject, recoveryAcquireCount);
        const auto releaseCountStatus = cluster_->GetInjectActionExecuteCount(
            WORKER, kSourceWorker, kSendLaneReleaseInject, recoveryReleaseCount);
        const auto finalModifyCountStatus = cluster_->GetInjectActionExecuteCount(
            WORKER, kSourceWorker, kModifyJettyInject, finalModifyCount);
        const auto finalRefillCountStatus = cluster_->GetInjectActionExecuteCount(
            WORKER, kSourceWorker, kRefillAddedInject, finalRefillAddedCount);
        const auto clearAcquire = cluster_->ClearInjectAction(WORKER, kSourceWorker, kBatchGetAfterAcquireInject);
        const auto clearRelease = cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject);
        const auto clearModify = cluster_->ClearInjectAction(WORKER, kSourceWorker, kModifyJettyInject);
        const auto clearRefill = cluster_->ClearInjectAction(WORKER, kSourceWorker, kRefillAddedInject);

        ASSERT_TRUE(clearFault.IsOk()) << clearFault.ToString();
        ASSERT_TRUE(timeoutWaveHeldFaultLanes) << "timeout wave did not hold four distinct fault lanes before Wait";
        ASSERT_TRUE(timeoutRetirementsObserved)
            << "only " << timeoutRetireCount << " in-flight timeout events requested lane retirement";
        ASSERT_EQ(timeoutRetireCount, kFaultCount);
        ASSERT_TRUE(clearCompletionPause.IsOk()) << clearCompletionPause.ToString();
        ASSERT_TRUE(clearTimeoutRetire.IsOk()) << clearTimeoutRetire.ToString();
        for (bool result : requestResults) {
            ASSERT_TRUE(result) << "a concurrent request was corrupted by another lane's injected fault";
        }
        ASSERT_TRUE(observedRetirements) << "only " << modifyCount << " failed lanes reached ModifyToError";
        ASSERT_EQ(modifyCount, kFaultCount);
        ASSERT_TRUE(replacementsAdded) << "only " << refillAddedCount << " replacement lanes were added";
        ASSERT_TRUE(exhaustedCountStatus.IsOk()) << exhaustedCountStatus.ToString();
        ASSERT_TRUE(clearExhausted.IsOk()) << clearExhausted.ToString();
        ASSERT_TRUE(acquireCountStatus.IsOk()) << acquireCountStatus.ToString();
        ASSERT_TRUE(releaseCountStatus.IsOk()) << releaseCountStatus.ToString();
        ASSERT_TRUE(finalModifyCountStatus.IsOk()) << finalModifyCountStatus.ToString();
        ASSERT_TRUE(finalRefillCountStatus.IsOk()) << finalRefillCountStatus.ToString();
        ASSERT_TRUE(clearAcquire.IsOk()) << clearAcquire.ToString();
        ASSERT_TRUE(clearRelease.IsOk()) << clearRelease.ToString();
        ASSERT_TRUE(clearModify.IsOk()) << clearModify.ToString();
        ASSERT_TRUE(clearRefill.IsOk()) << clearRefill.ToString();
        ASSERT_EQ(exhaustedCount, 0U) << "the pool remained drained after fault injection stopped";
        ASSERT_GE(recoveryAcquireCount, kConcurrentRequests)
            << "not every recovery request acquired an URMA send lane";
        ASSERT_EQ(recoveryReleaseCount, recoveryAcquireCount)
            << "recovery Batch Get attempts did not release exactly the lanes they acquired";
        ASSERT_EQ(finalModifyCount, kFaultCount) << "fault storm retired more than the four injected one-event lanes";
        ASSERT_EQ(finalRefillAddedCount, kFaultCount) << "refill created more replacements than retired lanes";
        for (const auto &status : recoveryStatuses) {
            ASSERT_TRUE(status.IsOk()) << status.ToString();
        }
    }

};

class UrmaSendJettyPoolFallbackDisabledStTest : public UrmaSendJettyPoolStTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaSendJettyPoolStTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_transport_fallback=false";
    }
};

class UrmaSendJettyPoolConcurrentStTest : public UrmaSendJettyPoolStTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaSendJettyPoolStTest::SetClusterSetupOptions(opts);
        // The tests below must have eight worker RPC handlers inside BatchGet simultaneously; both ST defaults are four.
        opts.numRpcThreads = 16;
        opts.numOcThreadNum = 16;
    }

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

class UrmaSendJettyPoolMaxParallelStTest : public UrmaSendJettyPoolStTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaSendJettyPoolStTest::SetClusterSetupOptions(opts);
        // Keep 8 KiB objects on the ordinary-write path so every 64-object request exercises the configured TBB cap.
        opts.workerGflagParams += " -oc_worker_worker_parallel_min=32 -oc_worker_aggregate_single_max=4096";
    }

protected:
    uint32_t SendLanePoolSize() const override
    {
        return 64;
    }

    uint32_t SendLaneRefillExtraSize() const override
    {
        return 64;
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

TEST_F(UrmaSendJettyPoolStTest, BatchGetSharesOneLaneAcrossOverlappingObjects)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader, 10000, false, 10000);

    // The large object disables aggregate handling for the entire request. With more than the parallel threshold,
    // the server submits individual objects from multiple TBB workers. The whole RPC must still use one send lane.
    constexpr uint32_t kSmallObjectCount = 1024;
    constexpr size_t kSmallValueSize = 8 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kSmallObjectCount + 1);
    values.reserve(kSmallObjectCount + 1);
    keys.emplace_back("urma-send-lane-batch-exhaust-big");
    values.emplace_back(kValueSize, 'a');
    DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    for (uint32_t i = 0; i < kSmallObjectCount; ++i) {
        keys.emplace_back("urma-send-lane-batch-exhaust-" + std::to_string(i));
        values.emplace_back(kSmallValueSize, static_cast<char>('b' + (i % 26)));
        DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    }

    // Pause immediately after the first WR is posted. This keeps the RPC-scoped lease in use while other parallel
    // sub-requests submit on the same Jetty.
    AssertBatchGetSharesOneLane(reader, keys, values, kPauseWriteAfterPostInject);
}

TEST_F(UrmaSendJettyPoolStTest, BatchGetWriteErrorReleasesSharedLane)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader, 10000, false, 10000);

    // Each object exceeds oc_worker_aggregate_single_max, so this exercises ordinary per-object UrmaWrite while
    // still using one worker-to-worker Batch Get RPC.
    constexpr uint32_t kObjectCount = 128;
    constexpr size_t kValueSize = 128 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kObjectCount);
    values.reserve(kObjectCount);
    for (uint32_t i = 0; i < kObjectCount; ++i) {
        keys.emplace_back("urma-send-lane-write-error-" + std::to_string(i));
        values.emplace_back(kValueSize, static_cast<char>('a' + (i % 26)));
        DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneRetireInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kWriteErrorInject, "1*return()"));

    std::vector<std::string> got;
    const auto getStatus = reader->Get(keys, got);
    uint64_t writeErrorCount = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kWriteErrorInject, writeErrorCount));
    const auto clearWriteStatus = cluster_->ClearInjectAction(WORKER, kSourceWorker, kWriteErrorInject);
    ASSERT_TRUE(clearWriteStatus.IsOk()) << clearWriteStatus.ToString();
    ASSERT_EQ(writeErrorCount, 1U) << "Batch Get did not execute the ordinary UrmaWrite error path";
    ASSERT_TRUE(getStatus.IsOk()) << getStatus.ToString();
    WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, 1);

    uint64_t retireCount = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kSendLaneRetireInject, retireCount));
    ASSERT_EQ(retireCount, 0U) << "ordinary Batch Get object failure unexpectedly retired the shared lane";
    ASSERT_EQ(got.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(got[i], values[i]) << "object index: " << i;
    }
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneRetireInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject));
}

TEST_F(UrmaSendJettyPoolStTest, BatchGetOrdinaryWritePoolExhaustionFallsBackToTcpOnly)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader, 10000, false, 10000);

    constexpr uint32_t kObjectCount = 128;
    // Stay above the 64 KiB aggregate threshold while keeping the whole 128-object TCP fallback below the
    // process-wide 10 MiB pending-payload limit. Otherwise the limiter rejects the tail objects and KVClient retries
    // them in a second Batch Get RPC, invalidating this test's one-RPC/one-acquire assertion.
    constexpr size_t kOrdinaryValueSize = 72 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kObjectCount);
    values.reserve(kObjectCount);
    for (uint32_t i = 0; i < kObjectCount; ++i) {
        keys.emplace_back("urma-send-lane-exhaust-ordinary-" + std::to_string(i));
        values.emplace_back(kOrdinaryValueSize, static_cast<char>('a' + (i % 26)));
        DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    }
    AssertPoolExhaustedBatchFallsBack(reader, keys, values, kWriteErrorInject);
}

TEST_F(UrmaSendJettyPoolStTest, BatchGetPoolExhaustionTcpFallbackRejectedByLimiter)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader, 3000, false, 1000);

    const std::vector<std::string> keys{ "urma-send-lane-exhaust-limiter-reject" };
    const std::vector<std::string> values{
        std::string(UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes, 'a') };
    DS_ASSERT_OK(writer->Set(keys.front(), values.front()));

    // Holding the only lane makes Batch Get select RPC-wide TCP fallback. The payload is exactly the limiter's
    // exclusive upper bound, so TrackUrmaFallbackTcp must reject it without attempting an object URMA write.
    AssertPoolExhaustedBatchFallsBack(reader, keys, values, kWriteErrorInject,
                                      PoolExhaustionFallbackOutcome::LIMITER_REJECTED);
}

TEST_F(UrmaSendJettyPoolStTest, BatchGetGatherWriteSharesOneLaneAcrossOverlappingGroups)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader, 10000, false, 10000);

    // Every object is below oc_worker_aggregate_single_max, while the total size creates multiple aggregate groups.
    constexpr uint32_t kObjectCount = 512;
    constexpr size_t kValueSize = 8 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kObjectCount);
    values.reserve(kObjectCount);
    for (uint32_t i = 0; i < kObjectCount; ++i) {
        keys.emplace_back("urma-send-lane-gather-" + std::to_string(i));
        values.emplace_back(kValueSize, static_cast<char>('a' + (i % 26)));
        DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    }

    // Pause after the first GatherWrite has acquired its lane. Another aggregate group then enters GatherWrite while
    // the lane is held, which would acquire a second pool lane before this fix.
    AssertBatchGetSharesOneLane(reader, keys, values, kPauseGatherWriteAfterAcquireInject);
}

TEST_F(UrmaSendJettyPoolStTest, BatchGetGatherWriteErrorReleasesSharedLane)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader);

    // More than the parallel threshold and below the per-object aggregate limit, so this exercises GatherWrite even
    // though the total request fits in one aggregate group.
    constexpr uint32_t kObjectCount = 128;
    constexpr size_t kValueSize = 8 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kObjectCount);
    values.reserve(kObjectCount);
    for (uint32_t i = 0; i < kObjectCount; ++i) {
        keys.emplace_back("urma-send-lane-gather-error-" + std::to_string(i));
        values.emplace_back(kValueSize, static_cast<char>('a' + (i % 26)));
        DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneRetireInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kGatherWriteInject, "1*return()"));

    std::vector<std::string> got;
    const auto getStatus = reader->Get(keys, got);
    uint64_t gatherErrorCount = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kGatherWriteInject, gatherErrorCount));
    const auto clearGatherStatus = cluster_->ClearInjectAction(WORKER, kSourceWorker, kGatherWriteInject);
    ASSERT_TRUE(clearGatherStatus.IsOk()) << clearGatherStatus.ToString();
    ASSERT_EQ(gatherErrorCount, 1U) << "Batch Get did not execute the GatherWrite error path";
    ASSERT_TRUE(getStatus.IsOk()) << getStatus.ToString();
    WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, 1);

    uint64_t retireCount = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kSendLaneRetireInject, retireCount));
    ASSERT_EQ(retireCount, 0U) << "GatherWrite object failure unexpectedly retired the shared lane";
    ASSERT_EQ(got.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(got[i], values[i]) << "object index: " << i;
    }
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneRetireInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject));
}

TEST_F(UrmaSendJettyPoolStTest, BatchGetAggregatePoolExhaustionFallsBackToTcpOnly)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader, 10000, false, 10000);

    constexpr uint32_t kObjectCount = 128;
    constexpr size_t kAggregateValueSize = 8 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kObjectCount);
    values.reserve(kObjectCount);
    for (uint32_t i = 0; i < kObjectCount; ++i) {
        keys.emplace_back("urma-send-lane-exhaust-aggregate-" + std::to_string(i));
        values.emplace_back(kAggregateValueSize, static_cast<char>('a' + (i % 26)));
        DS_ASSERT_OK(writer->Set(keys.back(), values.back()));
    }
    AssertPoolExhaustedBatchFallsBack(reader, keys, values, kPauseGatherWriteAfterAcquireInject);
}

TEST_F(UrmaSendJettyPoolFallbackDisabledStTest,
       BatchGetPoolExhaustionRetriesBackpressureWithoutTcpFallbackOrWrPost)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    std::shared_ptr<KVClient> heldReader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader, 3000, false, 1000);
    InitTestKVClient(kDestinationWorker, heldReader, 10000, false, 10000);

    const std::string heldKey = "urma-send-lane-disabled-held";
    const std::string value(kValueSize, 'a');
    DS_ASSERT_OK(writer->Set(heldKey, value));
    constexpr uint32_t kObjectCount = 128;
    std::vector<std::string> keys;
    keys.reserve(kObjectCount);
    for (uint32_t i = 0; i < kObjectCount; ++i) {
        keys.emplace_back("urma-send-lane-disabled-" + std::to_string(i));
        DS_ASSERT_OK(writer->Set(keys.back(), value));
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kPauseWriteAfterPostInject, "1*pause()"));
    std::promise<Status> heldPromise;
    auto heldFuture = heldPromise.get_future();
    std::thread heldThread([heldReader, heldKey, promise = std::move(heldPromise)]() mutable {
        std::string got;
        promise.set_value(heldReader->Get(heldKey, got));
    });
    Status pauseCountStatus = Status::OK();
    uint64_t pauseCount = 0;
    const auto pauseDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < pauseDeadline) {
        pauseCountStatus = cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kPauseWriteAfterPostInject,
                                                                  pauseCount);
        if (pauseCountStatus.IsError() || pauseCount > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    Status setExhaustedStatus(K_RUNTIME_ERROR, "held lane was not observed");
    Status setWriteStatus(K_RUNTIME_ERROR, "held lane was not observed");
    Status getStatus(K_RUNTIME_ERROR, "fallback-disabled Batch Get was not attempted");
    std::vector<std::string> got;
    uint64_t exhaustedCount = 0;
    uint64_t writeCount = 0;
    Status exhaustedCountStatus = Status::OK();
    Status writeCountStatus = Status::OK();
    if (pauseCount > 0) {
        setExhaustedStatus = cluster_->SetInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject, "call()");
        setWriteStatus = cluster_->SetInjectAction(WORKER, kSourceWorker, kWriteErrorInject, "call()");
        if (setExhaustedStatus.IsOk() && setWriteStatus.IsOk()) {
            getStatus = reader->Get(keys, got, 500);
            exhaustedCountStatus = cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kPoolExhaustedInject,
                                                                          exhaustedCount);
            writeCountStatus = cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kWriteErrorInject,
                                                                      writeCount);
        }
    }

    const auto clearPause = cluster_->ClearInjectAction(WORKER, kSourceWorker, kPauseWriteAfterPostInject);
    const auto clearExhausted = setExhaustedStatus.IsOk()
                                    ? cluster_->ClearInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject)
                                    : Status::OK();
    const auto clearWrite = setWriteStatus.IsOk()
                                ? cluster_->ClearInjectAction(WORKER, kSourceWorker, kWriteErrorInject)
                                : Status::OK();
    heldThread.join();
    const auto heldStatus = heldFuture.get();

    ASSERT_TRUE(pauseCountStatus.IsOk()) << pauseCountStatus.ToString();
    ASSERT_GT(pauseCount, 0U) << "the request holding the only send lane was not observed";
    ASSERT_TRUE(setExhaustedStatus.IsOk()) << setExhaustedStatus.ToString();
    ASSERT_TRUE(setWriteStatus.IsOk()) << setWriteStatus.ToString();
    ASSERT_TRUE(exhaustedCountStatus.IsOk()) << exhaustedCountStatus.ToString();
    ASSERT_TRUE(writeCountStatus.IsOk()) << writeCountStatus.ToString();
    ASSERT_TRUE(clearPause.IsOk()) << clearPause.ToString();
    ASSERT_TRUE(clearExhausted.IsOk()) << clearExhausted.ToString();
    ASSERT_TRUE(clearWrite.IsOk()) << clearWrite.ToString();
    ASSERT_TRUE(heldStatus.IsOk()) << heldStatus.ToString();
    // Worker-to-worker RPC returns K_TRY_AGAIN, but KVClient retries that code until this short request deadline.
    // The stable end-to-end contract here is repeated pool backpressure, no object WR post, and no TCP success.
    ASSERT_TRUE(getStatus.IsError());
    ASSERT_GT(exhaustedCount, 0U);
    ASSERT_EQ(writeCount, 0U) << "fallback-disabled Batch Get must fail before posting any object WR";
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

TEST_F(UrmaSendJettyPoolConcurrentStTest, LEVEL1_ConcurrentBatchGetsAtCapacityOverflowFallbackAndReuseAllLanes)
{
    constexpr uint32_t kPoolCapacity = 8;
    constexpr uint32_t kWaveCount = 3;
    constexpr uint32_t kTotalBatchCount = kPoolCapacity * kWaveCount;
    constexpr uint32_t kObjectsPerBatch = 64;
    constexpr size_t kObjectSize = 8 * 1024;
    std::shared_ptr<KVClient> writer;
    InitTestKVClient(kSourceWorker, writer);

    std::vector<std::vector<std::string>> keys(kTotalBatchCount);
    std::vector<std::vector<std::string>> values(kTotalBatchCount);
    for (uint32_t batchIndex = 0; batchIndex < kTotalBatchCount; ++batchIndex) {
        keys[batchIndex].reserve(kObjectsPerBatch);
        values[batchIndex].reserve(kObjectsPerBatch);
        for (uint32_t objectIndex = 0; objectIndex < kObjectsPerBatch; ++objectIndex) {
            keys[batchIndex].emplace_back("urma-send-lane-capacity-batch-" + std::to_string(batchIndex) + "-"
                                          + std::to_string(objectIndex));
            values[batchIndex].emplace_back(kObjectSize,
                                            static_cast<char>('a' + ((batchIndex + objectIndex) % 26)));
        }
        std::vector<StringView> valueViews;
        valueViews.reserve(kObjectsPerBatch);
        for (const auto &value : values[batchIndex]) {
            valueViews.emplace_back(value);
        }
        std::vector<std::string> failedKeys;
        DS_ASSERT_OK(writer->MSet(keys[batchIndex], valueViews, failedKeys));
        ASSERT_TRUE(failedKeys.empty());
    }

    std::vector<std::shared_ptr<KVClient>> readers(kPoolCapacity * 2);
    for (auto &reader : readers) {
        InitTestKVClient(kDestinationWorker, reader, 20000, false, 20000);
    }
    auto runBatch = [&keys, &values](const std::shared_ptr<KVClient> &reader, uint32_t batchIndex) {
        std::vector<std::string> got;
        auto rc = reader->Get(keys[batchIndex], got);
        if (rc.IsOk() && got != values[batchIndex]) {
            return Status(K_RUNTIME_ERROR, "Concurrent Batch Get returned unexpected data");
        }
        return rc;
    };

    // Eight overflow RPCs carry only 4 MiB total (8 * 64 * 8 KiB), below the 10 MiB process fallback limit.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kBatchGetAfterAcquireInject, "8*sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "32*call()"));
    std::vector<Status> heldStatuses(kPoolCapacity);
    std::vector<std::thread> heldThreads;
    heldThreads.reserve(kPoolCapacity);
    for (uint32_t i = 0; i < kPoolCapacity; ++i) {
        heldThreads.emplace_back([&, i] { heldStatuses[i] = runBatch(readers[i], i); });
    }

    uint64_t acquiredCount = 0;
    const bool filledPool = ObserveWorkerInjectExecuteCount(kSourceWorker, kBatchGetAfterAcquireInject, kPoolCapacity,
                                                             acquiredCount, 10000);
    Status setExhaustedStatus(K_RUNTIME_ERROR, "pool capacity was not reached");
    Status setUnexpectedWriteStatus(K_RUNTIME_ERROR, "pool capacity was not reached");
    std::vector<Status> overflowStatuses(kPoolCapacity);
    std::vector<std::thread> overflowThreads;
    if (filledPool) {
        setExhaustedStatus = cluster_->SetInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject, "8*call()");
        setUnexpectedWriteStatus = cluster_->SetInjectAction(WORKER, kSourceWorker, kWriteErrorInject, "call()");
        if (setExhaustedStatus.IsOk() && setUnexpectedWriteStatus.IsOk()) {
            overflowThreads.reserve(kPoolCapacity);
            for (uint32_t i = 0; i < kPoolCapacity; ++i) {
                overflowThreads.emplace_back(
                    [&, i] { overflowStatuses[i] = runBatch(readers[kPoolCapacity + i], kPoolCapacity + i); });
            }
            for (auto &thread : overflowThreads) {
                thread.join();
            }
        }
    }

    uint64_t exhaustedCount = 0;
    uint64_t unexpectedWriteCount = 0;
    const auto exhaustedCountStatus = setExhaustedStatus.IsOk()
                                            ? cluster_->GetInjectActionExecuteCount(
                                                  WORKER, kSourceWorker, kPoolExhaustedInject, exhaustedCount)
                                            : Status::OK();
    const auto unexpectedWriteCountStatus = setUnexpectedWriteStatus.IsOk()
                                                ? cluster_->GetInjectActionExecuteCount(
                                                      WORKER, kSourceWorker, kWriteErrorInject, unexpectedWriteCount)
                                                : Status::OK();
    const auto clearUnexpectedWrite = setUnexpectedWriteStatus.IsOk()
                                          ? cluster_->ClearInjectAction(WORKER, kSourceWorker, kWriteErrorInject)
                                          : Status::OK();
    const auto clearExhausted = setExhaustedStatus.IsOk()
                                    ? cluster_->ClearInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject)
                                    : Status::OK();
    const auto clearCapacityPause =
        cluster_->ClearInjectAction(WORKER, kSourceWorker, kBatchGetAfterAcquireInject);
    for (auto &thread : heldThreads) {
        thread.join();
    }

    ASSERT_TRUE(filledPool) << "only " << acquiredCount << " Batch Gets acquired the configured eight lanes";
    ASSERT_TRUE(setExhaustedStatus.IsOk()) << setExhaustedStatus.ToString();
    ASSERT_TRUE(setUnexpectedWriteStatus.IsOk()) << setUnexpectedWriteStatus.ToString();
    ASSERT_TRUE(exhaustedCountStatus.IsOk()) << exhaustedCountStatus.ToString();
    ASSERT_TRUE(unexpectedWriteCountStatus.IsOk()) << unexpectedWriteCountStatus.ToString();
    ASSERT_TRUE(clearUnexpectedWrite.IsOk()) << clearUnexpectedWrite.ToString();
    ASSERT_TRUE(clearExhausted.IsOk()) << clearExhausted.ToString();
    ASSERT_TRUE(clearCapacityPause.IsOk()) << clearCapacityPause.ToString();
    ASSERT_EQ(exhaustedCount, kPoolCapacity) << "each overflow RPC must acquire once and then stay TCP-only";
    ASSERT_EQ(unexpectedWriteCount, 0U) << "overflow Batch Gets posted a WR after selecting TCP-only fallback";
    for (const auto &status : overflowStatuses) {
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }
    for (const auto &status : heldStatuses) {
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }
    WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, kPoolCapacity, 10000);

    // Fill the pool a second time. Reaching all eight post-acquire sleeps proves every lane was returned and reusable.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kBatchGetAfterAcquireInject, "8*sleep(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject, "call()"));
    std::vector<Status> reuseStatuses(kPoolCapacity);
    std::vector<std::thread> reuseThreads;
    reuseThreads.reserve(kPoolCapacity);
    for (uint32_t i = 0; i < kPoolCapacity; ++i) {
        reuseThreads.emplace_back([&, i] { reuseStatuses[i] = runBatch(readers[i], kPoolCapacity * 2 + i); });
    }
    acquiredCount = 0;
    const bool reusedFullPool = ObserveWorkerInjectExecuteCount(kSourceWorker, kBatchGetAfterAcquireInject,
                                                                kPoolCapacity, acquiredCount, 10000);
    uint64_t reuseExhaustedCount = 0;
    const auto reuseExhaustedStatus = cluster_->GetInjectActionExecuteCount(
        WORKER, kSourceWorker, kPoolExhaustedInject, reuseExhaustedCount);
    const auto clearReuseExhausted =
        cluster_->ClearInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject);
    const auto clearReusePause = cluster_->ClearInjectAction(WORKER, kSourceWorker, kBatchGetAfterAcquireInject);
    for (auto &thread : reuseThreads) {
        thread.join();
    }

    ASSERT_TRUE(reusedFullPool) << "only " << acquiredCount << " lanes were reusable after releasing the full pool";
    ASSERT_TRUE(reuseExhaustedStatus.IsOk()) << reuseExhaustedStatus.ToString();
    ASSERT_TRUE(clearReuseExhausted.IsOk()) << clearReuseExhausted.ToString();
    ASSERT_TRUE(clearReusePause.IsOk()) << clearReusePause.ToString();
    ASSERT_EQ(reuseExhaustedCount, 0U);
    for (const auto &status : reuseStatuses) {
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }
    WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, kPoolCapacity * 2, 10000);
    uint64_t finalReleaseCount = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
        WORKER, kSourceWorker, kSendLaneReleaseInject, finalReleaseCount));
    ASSERT_EQ(finalReleaseCount, kPoolCapacity * 2) << "a Batch Get lane was leaked or released more than once";
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject));
}

TEST_F(UrmaSendJettyPoolConcurrentStTest, LEVEL1_ConcurrentBatchGetsRecoverFromCqeStatus9Storm)
{
    AssertConcurrentFaultStormRecovers(kCqeStatusInject, "4*call(0, 9)", "urma-send-lane-cqe-storm");
}

TEST_F(UrmaSendJettyPoolConcurrentStTest, LEVEL1_ConcurrentBatchGetsRecoverFromInFlightTimeoutStorm)
{
    AssertConcurrentFaultStormRecovers(kInFlightTimeoutInject, "4*call(-1)", "urma-send-lane-timeout-storm");
}

TEST_F(UrmaSendJettyPoolMaxParallelStTest, LEVEL1_Concurrent64BatchGetsWith64ObjectsKeepLanePoolUsable)
{
    constexpr uint32_t kConcurrentRequests = 64;
    constexpr uint32_t kObjectsPerRequest = 64;
    constexpr size_t kParallelValueSize = 8 * 1024;
    std::shared_ptr<KVClient> writer;
    InitTestKVClient(kSourceWorker, writer);

    std::vector<std::vector<std::string>> keys(kConcurrentRequests);
    std::vector<std::vector<std::string>> values(kConcurrentRequests);
    for (uint32_t requestIndex = 0; requestIndex < kConcurrentRequests; ++requestIndex) {
        keys[requestIndex].reserve(kObjectsPerRequest);
        values[requestIndex].reserve(kObjectsPerRequest);
        for (uint32_t objectIndex = 0; objectIndex < kObjectsPerRequest; ++objectIndex) {
            keys[requestIndex].emplace_back("urma-send-lane-64x64-" + std::to_string(requestIndex) + "-"
                                            + std::to_string(objectIndex));
            values[requestIndex].emplace_back(kParallelValueSize,
                                              static_cast<char>('a' + ((requestIndex + objectIndex) % 26)));
        }
        std::vector<StringView> valueViews;
        valueViews.reserve(kObjectsPerRequest);
        for (const auto &value : values[requestIndex]) {
            valueViews.emplace_back(value);
        }
        std::vector<std::string> failedKeys;
        DS_ASSERT_OK(writer->MSet(keys[requestIndex], valueViews, failedKeys));
        ASSERT_TRUE(failedKeys.empty());
    }

    std::vector<std::shared_ptr<KVClient>> readers(kConcurrentRequests);
    for (auto &reader : readers) {
        InitTestKVClient(kDestinationWorker, reader, 30000, false, 30000);
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject, "128*call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject, "call()"));

    ThreadPool threadPool(kConcurrentRequests);
    std::promise<void> start;
    const auto startFuture = start.get_future().share();
    std::vector<std::future<bool>> futures;
    futures.reserve(kConcurrentRequests);
    for (uint32_t i = 0; i < kConcurrentRequests; ++i) {
        futures.emplace_back(threadPool.Submit(
            [startFuture, reader = readers[i], requestKeys = keys[i], expectedValues = values[i]] {
                startFuture.wait();
                std::vector<std::string> got;
                const auto status = reader->Get(requestKeys, got);
                return status.IsOk() && got == expectedValues;
            }));
    }
    start.set_value();
    for (auto &future : futures) {
        EXPECT_TRUE(future.get());
    }
    WaitForWorkerInjectExecuteCount(kSourceWorker, kSendLaneReleaseInject, kConcurrentRequests, 30000);
    uint64_t releaseCount = 0;
    uint64_t exhaustedCount = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kSendLaneReleaseInject, releaseCount));
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, kSourceWorker, kPoolExhaustedInject, exhaustedCount));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kSendLaneReleaseInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, kSourceWorker, kPoolExhaustedInject));
    ASSERT_EQ(releaseCount, kConcurrentRequests) << "each Batch Get RPC must release exactly one shared lane";
    ASSERT_EQ(exhaustedCount, 0U)
        << "64 concurrently started Batch Get RPCs exceeded the configured 64-lane pool";

    // The shared start gate, successful requests, zero pool-exhaustion observations, 64 releases, and final reuse prove
    // bounded concurrent operation. This deliberately does not claim all 64 RPCs held lanes simultaneously: adding a
    // pause to force that schedule would couple 4,096 object tasks to one global unblock and make this manual ST
    // brittle.
    std::vector<std::string> got;
    DS_ASSERT_OK(readers.front()->Get(keys.front(), got));
    ASSERT_EQ(got, values.front());
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

TEST_F(UrmaSendJettyPoolStTest, CqeRetirementFlushDeleteConvergesRegistryAndRemainsUsable)
{
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(kSourceWorker, writer);
    InitTestKVClient(kDestinationWorker, reader, 10000, false, 10000);

    const std::string faultKey = "urma-send-lane-lifecycle-fault";
    const std::string recoveryKey = "urma-send-lane-lifecycle-recovery";
    const std::string faultValue(kValueSize, 'a');
    const std::string recoveryValue(kValueSize, 'b');
    DS_ASSERT_OK(writer->Set(faultKey, faultValue));
    DS_ASSERT_OK(writer->Set(recoveryKey, recoveryValue));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kModifyJettyInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kAsyncDeleteCompleteInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kRegistryUnregisterInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, kSourceWorker, kCqeStatusInject, "1*call(0, 9)"));

    std::string got;
    const auto faultStatus = reader->Get(faultKey, got);
    uint64_t modifyCount = 0;
    uint64_t deleteCount = 0;
    uint64_t unregisterCount = 0;
    const bool movedToPending =
        ObserveWorkerInjectExecuteCount(kSourceWorker, kModifyJettyInject, 1, modifyCount, 10000);
    const bool deletedFromPending = ObserveWorkerInjectExecuteCount(
        kSourceWorker, kAsyncDeleteCompleteInject, 1, deleteCount, 10000);
    const bool unregistered = ObserveWorkerInjectExecuteCount(
        kSourceWorker, kRegistryUnregisterInject, 1, unregisterCount, 10000);

    const auto clearCqe = cluster_->ClearInjectAction(WORKER, kSourceWorker, kCqeStatusInject);
    const auto clearModify = cluster_->ClearInjectAction(WORKER, kSourceWorker, kModifyJettyInject);
    const auto clearDelete = cluster_->ClearInjectAction(WORKER, kSourceWorker, kAsyncDeleteCompleteInject);
    const auto clearUnregister = cluster_->ClearInjectAction(WORKER, kSourceWorker, kRegistryUnregisterInject);

    ASSERT_TRUE(faultStatus.IsOk()) << faultStatus.ToString();
    ASSERT_EQ(got, faultValue);
    ASSERT_TRUE(movedToPending) << "retired Jetty did not reach pending delete";
    ASSERT_TRUE(deletedFromPending) << "FLUSH_ERR_DONE did not remove the retired Jetty from pending delete";
    ASSERT_TRUE(unregistered) << "destroyed Jetty remained in the registry";
    ASSERT_EQ(modifyCount, 1U);
    ASSERT_EQ(deleteCount, 1U);
    ASSERT_EQ(unregisterCount, 1U);
    ASSERT_TRUE(clearCqe.IsOk()) << clearCqe.ToString();
    ASSERT_TRUE(clearModify.IsOk()) << clearModify.ToString();
    ASSERT_TRUE(clearDelete.IsOk()) << clearDelete.ToString();
    ASSERT_TRUE(clearUnregister.IsOk()) << clearUnregister.ToString();
    GetEventually(*reader, recoveryKey, recoveryValue);
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
