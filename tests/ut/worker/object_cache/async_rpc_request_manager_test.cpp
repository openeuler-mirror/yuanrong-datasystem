/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: AsyncRpcRequestManager queue notification and shutdown tests.
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/worker/object_cache/async_rpc_request_manager.h"

#include <gtest/gtest.h>

namespace datasystem {
namespace ut {
namespace {
constexpr uint32_t WATCHDOG_MS = 2000;

struct FakeAsyncRpcRequest : AsyncRpcRequest {
    FakeAsyncRpcRequest() : AsyncRpcRequest(1000) {}
    Status AsyncWrite() override { return Status::OK(); }
    bool HasTimeout() override { return timedOut.load(); }
    bool HasReply(uint32_t = 0) override
    {
        ++replyChecks;
        return ready.load();
    }
    void HandleTimeout() override { ++timeoutCalls; }
    void HandleReturn() override
    {
        ++returnCalls;
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        cv.wait(lock, [this]() { return !block || released; });
    }
    void Release()
    {
        std::lock_guard<std::mutex> lock(mutex);
        released = true;
        cv.notify_all();
    }
    std::atomic<bool> ready{ false }, timedOut{ false }, entered{ false };
    std::atomic<uint32_t> replyChecks{ 0 }, returnCalls{ 0 }, timeoutCalls{ 0 };
    std::mutex mutex;
    std::condition_variable cv;
    bool block = false;
    bool released = false;
};

bool WaitForPredicate(const std::function<bool()> &predicate)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WATCHDOG_MS);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

class AsyncRpcRequestManagerTest : public ::testing::Test {
protected:
    void TearDown() override
    {
        (void)inject::Clear("AsyncRpcRequestManager.ProcessedRequest");
    }
};
}  // namespace

TEST_F(AsyncRpcRequestManagerTest, ManyProducersHaveNoLostOrDuplicateCallbacks)
{
    constexpr uint32_t PRODUCERS = 4;
    constexpr uint32_t REQUESTS_PER_PRODUCER = 64;
    std::vector<std::shared_ptr<FakeAsyncRpcRequest>> requests;
    requests.reserve(PRODUCERS * REQUESTS_PER_PRODUCER);
    for (uint32_t i = 0; i < PRODUCERS * REQUESTS_PER_PRODUCER; ++i) {
        auto request = std::make_shared<FakeAsyncRpcRequest>();
        request->ready = true;
        requests.push_back(request);
    }

    {
        AsyncRpcRequestManager manager(2);
        std::vector<std::thread> producers;
        for (uint32_t producer = 0; producer < PRODUCERS; ++producer) {
            producers.emplace_back([&manager, &requests, producer]() {
                const uint32_t begin = producer * REQUESTS_PER_PRODUCER;
                for (uint32_t i = 0; i < REQUESTS_PER_PRODUCER; ++i) {
                    manager.AddRequest(requests[begin + i]);
                }
            });
        }
        for (auto &producer : producers) {
            producer.join();
        }
        ASSERT_TRUE(WaitForPredicate([&requests]() {
            return std::all_of(requests.begin(), requests.end(),
                               [](const auto &request) { return request->returnCalls.load() == 1; });
        })) << "not all requests completed";
    }

    for (const auto &request : requests) {
        EXPECT_EQ(request->returnCalls.load(), 1u);
        EXPECT_EQ(request->timeoutCalls.load(), 0u);
    }
}

TEST_F(AsyncRpcRequestManagerTest, PendingReplyTimeoutPriorityAndNullEntry)
{
    auto reply = std::make_shared<FakeAsyncRpcRequest>();
    auto timeout = std::make_shared<FakeAsyncRpcRequest>();
    timeout->ready = true;
    timeout->timedOut = true;
    {
        AsyncRpcRequestManager manager(1);
        manager.AddRequest(nullptr);
        manager.AddRequest(timeout);
        manager.AddRequest(reply);
        ASSERT_TRUE(WaitForPredicate([&]() { return reply->replyChecks.load() > 0; }));
        EXPECT_EQ(reply->returnCalls.load(), 0u);
        reply->ready = true;
        ASSERT_TRUE(WaitForPredicate([&]() { return reply->returnCalls.load() == 1; }));
    }
    EXPECT_EQ(reply->returnCalls.load(), 1u);
    EXPECT_EQ(timeout->timeoutCalls.load(), 1u);
    EXPECT_EQ(timeout->returnCalls.load(), 0u);
}

TEST_F(AsyncRpcRequestManagerTest, IdleAndPendingConsumersAbortOnDestruction)
{
    for (uint32_t consumers : { 1u, 4u }) {
        for (bool pending : { false, true }) {
            auto request = std::make_shared<FakeAsyncRpcRequest>();
            auto begin = std::chrono::steady_clock::now();
            {
                AsyncRpcRequestManager manager(consumers);
                if (pending) {
                    manager.AddRequest(request);
                    ASSERT_TRUE(WaitForPredicate([&]() { return request->replyChecks.load() > 0; }));
                }
            }
            EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(2));
            EXPECT_EQ(request->returnCalls.load(), 0u);
        }
    }
}

TEST_F(AsyncRpcRequestManagerTest, DestructorWaitsForRunningCallbackToReturn)
{
    auto request = std::make_shared<FakeAsyncRpcRequest>();
    request->ready = true;
    request->block = true;
    auto manager = std::make_unique<AsyncRpcRequestManager>(1);
    manager->AddRequest(request);
    Raii releaseCallback([&]() { request->Release(); });
    ASSERT_TRUE(WaitForPredicate([&request]() { return request->entered.load(); }))
        << "callback did not start";

    std::atomic<bool> destructorStarted{ false };
    std::atomic<bool> destructorDone{ false };
    std::thread destructor([&manager, &destructorStarted, &destructorDone]() {
        destructorStarted.store(true, std::memory_order_release);
        manager.reset();
        destructorDone.store(true, std::memory_order_release);
    });
    Raii joinDestructor([&]() {
        request->Release();
        if (destructor.joinable()) {
            destructor.join();
        }
    });
    ASSERT_TRUE(WaitForPredicate([&]() { return destructorStarted.load(); })) << "destructor thread did not start";
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(destructorDone.load(std::memory_order_acquire))
        << "destructor returned while the callback was still running";

    request->Release();
    destructor.join();
    EXPECT_TRUE(destructorDone.load(std::memory_order_acquire));
    EXPECT_EQ(request->returnCalls.load(), 1u);
}

TEST_F(AsyncRpcRequestManagerTest, EnqueueBetweenEmptyCheckAndClearIsNotLost)
{
    constexpr char INJECT_NAME[] = "AsyncRpcRequestManager.ProcessedRequest";
    ASSERT_TRUE(inject::Set(INJECT_NAME, "1*pause()").IsOk());

    auto first = std::make_shared<FakeAsyncRpcRequest>();
    first->ready = true;
    auto second = std::make_shared<FakeAsyncRpcRequest>();
    second->ready = true;
    AsyncRpcRequestManager manager(1);
    Raii clearInject([&INJECT_NAME]() { (void)inject::Clear(INJECT_NAME); });
    manager.AddRequest(first);
    ASSERT_TRUE(WaitForPredicate([&]() { return inject::GetExecuteCount(INJECT_NAME) >= 1; }))
        << "first request did not reach the processed point";
    EXPECT_EQ(first->returnCalls.load(), 1u);

    // On the old implementation this runs after Empty() returned true and before WaitPost::Clear().
    manager.AddRequest(second);
    (void)inject::Clear(INJECT_NAME);

    ASSERT_TRUE(WaitForPredicate([&second]() { return second->returnCalls.load() == 1; }))
        << "request enqueued in the wakeup window was not processed promptly";
    EXPECT_EQ(first->returnCalls.load(), 1u);
    EXPECT_EQ(second->returnCalls.load(), 1u);
}
}  // namespace ut
}  // namespace datasystem
