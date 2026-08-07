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

#include "datasystem/common/rdma/rdma_util.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <bthread/bthread.h>
#include <gtest/gtest.h>

namespace datasystem {
namespace {
constexpr auto TEST_WAIT_TIMEOUT = std::chrono::seconds(2);
constexpr auto TEST_STATE_TIMEOUT = std::chrono::seconds(1);
constexpr auto PROBE_SCHEDULE_TIMEOUT = std::chrono::milliseconds(200);
constexpr auto EVENT_TIMEOUT = std::chrono::milliseconds(20);
constexpr auto STATE_POLL_INTERVAL = std::chrono::milliseconds(1);
constexpr int MAX_TEST_BTHREAD_WORKERS = 64;

template <typename Predicate>
bool WaitUntil(Predicate predicate, std::chrono::steady_clock::duration timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(STATE_POLL_INTERVAL);
    }
    return predicate();
}

void *NoopBthread(void *)
{
    return nullptr;
}

struct EventScheduleState {
    std::shared_ptr<Event> event{ std::make_shared<Event>(1) };
    std::atomic<int> entered{ 0 };
    std::atomic<bool> notifyIssued{ false };
    std::atomic<bool> probeRan{ false };
    std::atomic<bool> probeRanBeforeNotify{ false };
};

void *WaitEvent(void *arg)
{
    auto *state = static_cast<EventScheduleState *>(arg);
    state->entered.fetch_add(1, std::memory_order_release);
    (void)state->event->WaitFor(TEST_WAIT_TIMEOUT);
    return nullptr;
}

void *ProbeScheduler(void *arg)
{
    auto *state = static_cast<EventScheduleState *>(arg);
    state->probeRanBeforeNotify.store(!state->notifyIssued.load(std::memory_order_acquire),
                                      std::memory_order_release);
    state->probeRan.store(true, std::memory_order_release);
    return nullptr;
}

TEST(EventBthreadWaitTest, WaitingBthreadsReleaseSchedulerWorkers)
{
    bthread_t bootstrap;
    ASSERT_EQ(bthread_start_background(&bootstrap, nullptr, NoopBthread, nullptr), 0);
    ASSERT_EQ(bthread_join(bootstrap, nullptr), 0);

    const int workerCount = bthread_getconcurrency_by_tag(BTHREAD_TAG_DEFAULT);
    ASSERT_GT(workerCount, 0);
    if (workerCount > MAX_TEST_BTHREAD_WORKERS) {
        GTEST_SKIP() << "Skip scheduler saturation test for unusually high bthread worker count: " << workerCount;
    }

    EventScheduleState state;
    std::vector<bthread_t> waiters(static_cast<size_t>(workerCount));
    int startedWaiters = 0;
    int waiterStartRc = 0;
    for (; startedWaiters < workerCount; ++startedWaiters) {
        waiterStartRc =
            bthread_start_background(&waiters[static_cast<size_t>(startedWaiters)], nullptr, WaitEvent, &state);
        if (waiterStartRc != 0) {
            break;
        }
    }

    const bool allWaitersEntered =
        WaitUntil([&state, startedWaiters] { return state.entered.load(std::memory_order_acquire) == startedWaiters; },
                  TEST_STATE_TIMEOUT);

    bthread_t probe{};
    int probeStartRc = EINVAL;
    if (waiterStartRc == 0 && startedWaiters == workerCount && allWaitersEntered) {
        probeStartRc = bthread_start_background(&probe, nullptr, ProbeScheduler, &state);
    }
    const bool probeRanBeforeRelease =
        probeStartRc == 0
        && WaitUntil([&state] { return state.probeRan.load(std::memory_order_acquire); }, PROBE_SCHEDULE_TIMEOUT);

    state.notifyIssued.store(true, std::memory_order_release);
    state.event->NotifyAll();
    for (int i = 0; i < startedWaiters; ++i) {
        EXPECT_EQ(bthread_join(waiters[static_cast<size_t>(i)], nullptr), 0);
    }
    if (probeStartRc == 0) {
        EXPECT_EQ(bthread_join(probe, nullptr), 0);
    }

    ASSERT_EQ(waiterStartRc, 0);
    ASSERT_EQ(startedWaiters, workerCount);
    ASSERT_TRUE(allWaitersEntered);
    ASSERT_EQ(probeStartRc, 0);
    EXPECT_TRUE(probeRanBeforeRelease);
    EXPECT_TRUE(state.probeRanBeforeNotify.load(std::memory_order_acquire));
}

TEST(EventBthreadWaitTest, TimeoutPreservesEventStatus)
{
    struct WaitArgs {
        std::shared_ptr<Event> event{ std::make_shared<Event>(2) };
        Status status = Status::OK();
        std::chrono::steady_clock::duration elapsed{};
    } args;

    bthread_t waiter;
    auto waitEvent = [](void *arg) -> void * {
        auto *waitArgs = static_cast<WaitArgs *>(arg);
        const auto start = std::chrono::steady_clock::now();
        waitArgs->status = waitArgs->event->WaitFor(EVENT_TIMEOUT);
        waitArgs->elapsed = std::chrono::steady_clock::now() - start;
        return nullptr;
    };
    ASSERT_EQ(bthread_start_background(&waiter, nullptr, waitEvent, &args), 0);
    ASSERT_EQ(bthread_join(waiter, nullptr), 0);

    EXPECT_EQ(args.status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_GE(args.elapsed, EVENT_TIMEOUT);
    EXPECT_LT(args.elapsed, TEST_STATE_TIMEOUT);
}
}  // namespace
}  // namespace datasystem
