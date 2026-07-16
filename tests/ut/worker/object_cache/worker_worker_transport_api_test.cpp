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

/**
 * Description: Test WorkerWorkerTransportApi single-flight reconnect waits.
 */

#include "datasystem/worker/object_cache/worker_worker_transport_api.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>
#include <vector>

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include "datasystem/common/util/request_context.h"
#include "datasystem/utils/status.h"
#include "ut/common.h"

namespace datasystem {
namespace object_cache {
namespace {

constexpr int K_WAIT_STARTED_POLLS = 100;
constexpr int K_WAIT_POLL_US = 1'000;
constexpr int64_t K_SHORT_TIMEOUT_MS = 30;
constexpr int64_t K_MEDIUM_TIMEOUT_MS = 300;
constexpr int64_t K_LONG_TIMEOUT_MS = 1'000;
constexpr int64_t K_FIRST_EXCHANGE_SLEEP_US = 200'000;
constexpr size_t K_SINGLE_EXCHANGE_CALLS = 1;
constexpr size_t K_TWO_EXCHANGE_CALLS = 2;

struct ExchangeStep {
    Status status;
    int64_t sleepUs;
    std::atomic<bool> *release = nullptr;
};

class TestWorkerWorkerTransportApi : public WorkerWorkerTransportApi {
public:
    explicit TestWorkerWorkerTransportApi(std::vector<ExchangeStep> steps) : steps_(std::move(steps))
    {
    }

    ~TestWorkerWorkerTransportApi() override = default;

    Status Init() override
    {
        return Status::OK();
    }

    Status ExchangeUrmaConnectInfo(UrmaHandshakeRspPb &) override
    {
        size_t callIndex = callCount_.fetch_add(1);
        if (callIndex == 0) {
            firstExchangeStarted_.store(true);
        }
        if (callIndex >= steps_.size()) {
            return Status(K_RUNTIME_ERROR, "unexpected reconnect exchange");
        }
        const auto &step = steps_[callIndex];
        if (step.release != nullptr) {
            while (!step.release->load(std::memory_order_acquire)) {
                bthread_usleep(K_WAIT_POLL_US);
            }
        } else if (step.sleepUs > 0) {
            bthread_usleep(step.sleepUs);
        }
        return step.status;
    }

    bool FirstExchangeStarted() const
    {
        return firstExchangeStarted_.load();
    }

    size_t CallCount() const
    {
        return callCount_.load();
    }

    bool WaiterEntered() const
    {
        return waiterEntered_.load(std::memory_order_acquire);
    }

protected:
    void OnExchangeWaitForTest() override
    {
        waiterEntered_.store(true, std::memory_order_release);
    }

private:
    std::vector<ExchangeStep> steps_;
    std::atomic<size_t> callCount_{ 0 };
    std::atomic<bool> firstExchangeStarted_{ false };
    std::atomic<bool> waiterEntered_{ false };
};

struct ExecArgs {
    WorkerWorkerTransportApi *api;
    int64_t timeoutMs;
    Status status;
};

void *RunExecOnce(void *raw)
{
    auto *args = reinterpret_cast<ExecArgs *>(raw);
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(args->timeoutMs);
    UrmaHandshakeRspPb rsp;
    args->status = args->api->ExecOnceParrallelExchange(rsp);
    return nullptr;
}

bool WaitForFirstExchange(const TestWorkerWorkerTransportApi &api)
{
    for (int i = 0; i < K_WAIT_STARTED_POLLS; ++i) {
        if (api.FirstExchangeStarted()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(K_WAIT_POLL_US));
    }
    return false;
}

bool WaitForWaiter(const TestWorkerWorkerTransportApi &api)
{
    for (int i = 0; i < K_WAIT_STARTED_POLLS; ++i) {
        if (api.WaiterEntered()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(K_WAIT_POLL_US));
    }
    return false;
}

int StartExecBthread(bthread_t &tid, ExecArgs &args)
{
    return bthread_start_background(&tid, nullptr, RunExecOnce, &args);
}

int JoinExecBthread(bthread_t tid)
{
    return bthread_join(tid, nullptr);
}

}  // namespace

class WorkerWorkerTransportApiTest : public ut::CommonTest {
public:
    void SetUp() override
    {
        InitRequestContext();
    }
};

TEST_F(WorkerWorkerTransportApiTest, ExecOnceWaiterReturnsDeadlineExceeded)
{
    TestWorkerWorkerTransportApi api({ { Status::OK(), K_FIRST_EXCHANGE_SLEEP_US } });
    ExecArgs first{ &api, K_LONG_TIMEOUT_MS, Status::OK() };
    ExecArgs second{ &api, K_SHORT_TIMEOUT_MS, Status::OK() };
    bthread_t firstTid;
    bthread_t secondTid;

    ASSERT_EQ(StartExecBthread(firstTid, first), 0);
    ASSERT_TRUE(WaitForFirstExchange(api));
    ASSERT_EQ(StartExecBthread(secondTid, second), 0);
    ASSERT_EQ(JoinExecBthread(secondTid), 0);
    ASSERT_EQ(JoinExecBthread(firstTid), 0);

    EXPECT_TRUE(first.status.IsOk()) << first.status.ToString();
    EXPECT_EQ(second.status.GetCode(), K_RPC_DEADLINE_EXCEEDED) << second.status.ToString();
    EXPECT_EQ(api.CallCount(), K_SINGLE_EXCHANGE_CALLS);
}

TEST_F(WorkerWorkerTransportApiTest, ExecOnceWaiterReturnsOkAfterPeerSuccess)
{
    TestWorkerWorkerTransportApi api({ { Status::OK(), K_FIRST_EXCHANGE_SLEEP_US } });
    ExecArgs first{ &api, K_MEDIUM_TIMEOUT_MS, Status::OK() };
    ExecArgs second{ &api, K_MEDIUM_TIMEOUT_MS, Status::OK() };
    bthread_t firstTid;
    bthread_t secondTid;

    ASSERT_EQ(StartExecBthread(firstTid, first), 0);
    ASSERT_TRUE(WaitForFirstExchange(api));
    ASSERT_EQ(StartExecBthread(secondTid, second), 0);
    ASSERT_EQ(JoinExecBthread(secondTid), 0);
    ASSERT_EQ(JoinExecBthread(firstTid), 0);

    EXPECT_TRUE(first.status.IsOk()) << first.status.ToString();
    EXPECT_TRUE(second.status.IsOk()) << second.status.ToString();
    EXPECT_EQ(api.CallCount(), K_SINGLE_EXCHANGE_CALLS);
}

TEST_F(WorkerWorkerTransportApiTest, ExecOnceFailureWakesOneRetryingPeer)
{
    std::atomic<bool> releaseFirstExchange{ false };
    TestWorkerWorkerTransportApi api({ { Status(K_URMA_CONNECT_FAILED, "first failed"), 0, &releaseFirstExchange },
                                       { Status::OK(), 0 } });
    ExecArgs first{ &api, K_LONG_TIMEOUT_MS, Status::OK() };
    ExecArgs second{ &api, K_LONG_TIMEOUT_MS, Status::OK() };
    bthread_t firstTid;
    bthread_t secondTid;

    ASSERT_EQ(StartExecBthread(firstTid, first), 0);
    ASSERT_TRUE(WaitForFirstExchange(api));
    int startSecondRc = StartExecBthread(secondTid, second);
    if (startSecondRc != 0) {
        releaseFirstExchange.store(true, std::memory_order_release);
        ASSERT_EQ(JoinExecBthread(firstTid), 0);
        ASSERT_EQ(startSecondRc, 0);
    }
    if (!WaitForWaiter(api)) {
        releaseFirstExchange.store(true, std::memory_order_release);
        ASSERT_EQ(JoinExecBthread(secondTid), 0);
        ASSERT_EQ(JoinExecBthread(firstTid), 0);
        FAIL() << "second reconnect bthread did not enter single-flight wait";
    }
    EXPECT_EQ(api.CallCount(), K_SINGLE_EXCHANGE_CALLS);

    releaseFirstExchange.store(true, std::memory_order_release);
    ASSERT_EQ(JoinExecBthread(secondTid), 0);
    ASSERT_EQ(JoinExecBthread(firstTid), 0);

    EXPECT_EQ(first.status.GetCode(), K_URMA_CONNECT_FAILED) << first.status.ToString();
    EXPECT_TRUE(second.status.IsOk()) << second.status.ToString();
    EXPECT_EQ(api.CallCount(), K_TWO_EXCHANGE_CALLS);
}

TEST_F(WorkerWorkerTransportApiTest, ExecOnceSuccessResetsStopFlagForNextExchange)
{
    TestWorkerWorkerTransportApi api({ { Status::OK(), 0 }, { Status::OK(), 0 } });
    ExecArgs first{ &api, K_MEDIUM_TIMEOUT_MS, Status::OK() };
    ExecArgs second{ &api, K_MEDIUM_TIMEOUT_MS, Status::OK() };
    bthread_t firstTid;
    bthread_t secondTid;

    ASSERT_EQ(StartExecBthread(firstTid, first), 0);
    ASSERT_EQ(JoinExecBthread(firstTid), 0);
    ASSERT_EQ(StartExecBthread(secondTid, second), 0);
    ASSERT_EQ(JoinExecBthread(secondTid), 0);

    EXPECT_TRUE(first.status.IsOk()) << first.status.ToString();
    EXPECT_TRUE(second.status.IsOk()) << second.status.ToString();
    EXPECT_EQ(api.CallCount(), K_TWO_EXCHANGE_CALLS);
}

}  // namespace object_cache
}  // namespace datasystem
