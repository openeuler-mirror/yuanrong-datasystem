/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: UcpManager connection-stability table concurrency tests (no UCP device required).
 */
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rdma/ucp_worker_pool.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"

#define private public
#include "datasystem/common/rdma/ucp_manager.h"
#undef private

#include <gtest/gtest.h>

#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {
constexpr char AFTER_READ_INJECT[] = "UcpManager.CheckUcpConnectionStable.AfterRead";
constexpr char ADDR_A[] = "10.10.0.1:9000";
constexpr char ADDR_B[] = "10.10.0.2:9000";

bool WaitForAtomic(const std::atomic<bool> &flag, uint32_t timeoutMs = 5000)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (flag.load(std::memory_order_acquire)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return flag.load(std::memory_order_acquire);
}

bool WaitForExecuteCount(uint64_t expected, uint32_t timeoutMs = 5000)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (inject::GetExecuteCount(AFTER_READ_INJECT) >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return inject::GetExecuteCount(AFTER_READ_INJECT) >= expected;
}

class BlockingUcpWorkerPool : public UcpWorkerPool {
public:
    bool blocking = false;

    Status RemoveByIp(const std::string &ipAddr) override
    {
        (void)ipAddr;
        removeCalls_.fetch_add(1, std::memory_order_acq_rel);
        std::unique_lock<std::mutex> lock(mutex_);
        if (blocking && !entered_) {
            entered_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this]() { return released_; });
        }
        return Status::OK();
    }

    bool WaitEntered()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(5), [this]() { return entered_; });
    }

    void Release()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }

    uint32_t RemoveCalls() const
    {
        return removeCalls_.load(std::memory_order_acquire);
    }

private:
    std::atomic<uint32_t> removeCalls_{ 0 };
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false;
    bool released_ = false;
};

class UcpManagerConnectionTest : public ::testing::Test {
protected:
    void TearDown() override
    {
        (void)inject::Clear(AFTER_READ_INJECT);
    }

    template <typename Pool>
    std::unique_ptr<UcpManager> MakeManager(Pool *&pool)
    {
        auto manager = std::make_unique<UcpManager>();
        auto ownedPool = std::make_unique<Pool>();
        pool = ownedPool.get();
        manager->workerPool_ = std::move(ownedPool);
        return manager;
    }

    bool TableContains(UcpManager &manager, const std::string &address, const std::string &instanceId)
    {
        std::lock_guard<std::mutex> lock(manager.instanceTableMutex_);
        auto it = manager.instanceTable_.find(address);
        return it != manager.instanceTable_.end() && it->second == instanceId;
    }

    size_t TableSize(UcpManager &manager)
    {
        std::lock_guard<std::mutex> lock(manager.instanceTableMutex_);
        return manager.instanceTable_.size();
    }
};

TEST_F(UcpManagerConnectionTest, RegistrationEmptyIdsAndExplicitRemoval)
{
    BlockingUcpWorkerPool *pool = nullptr;
    auto manager = MakeManager(pool);
    DS_ASSERT_OK(manager->CheckUcpConnectionStable(ADDR_A, "instance-1"));
    DS_ASSERT_OK(manager->CheckUcpConnectionStable(ADDR_A, "instance-1"));
    DS_ASSERT_OK(manager->CheckUcpConnectionStable(ADDR_A, ""));
    DS_ASSERT_OK(manager->CheckUcpConnectionStable(ADDR_B, ""));
    DS_ASSERT_OK(manager->CheckUcpConnectionStable(ADDR_B, "instance-2"));
    EXPECT_EQ(pool->RemoveCalls(), 0u);
    EXPECT_EQ(TableSize(*manager), 2u);
    EXPECT_TRUE(TableContains(*manager, ADDR_A, "instance-1"));
    EXPECT_TRUE(TableContains(*manager, ADDR_B, ""));
    HostPort remoteAddress;
    ASSERT_TRUE(remoteAddress.ParseString(ADDR_A).IsOk());
    DS_ASSERT_OK(manager->RemoveEndpoint(remoteAddress));
    EXPECT_EQ(pool->RemoveCalls(), 1u);
    EXPECT_EQ(TableSize(*manager), 1u);
    EXPECT_TRUE(TableContains(*manager, ADDR_B, ""));
}

TEST_F(UcpManagerConnectionTest, ConcurrentChecksPerformExactlyOneCleanupAndKeepNewInstance)
{
    BlockingUcpWorkerPool *pool = nullptr;
    auto manager = MakeManager(pool);
    DS_ASSERT_OK(manager->CheckUcpConnectionStable(ADDR_B, "old-instance"));
    ASSERT_EQ(pool->RemoveCalls(), 0u);

    ASSERT_TRUE(inject::Set(AFTER_READ_INJECT, "2*pause()").IsOk());
    Status firstStatus;
    Status secondStatus;
    std::thread second;
    std::thread first([&]() { firstStatus = manager->CheckUcpConnectionStable(ADDR_B, "new-instance"); });
    Raii cleanup([&]() {
        (void)inject::Clear(AFTER_READ_INJECT);
        if (first.joinable()) {
            first.join();
        }
        if (second.joinable()) {
            second.join();
        }
    });
    ASSERT_TRUE(WaitForExecuteCount(1)) << "first check did not reach the table snapshot";
    second = std::thread([&]() { secondStatus = manager->CheckUcpConnectionStable(ADDR_B, "new-instance"); });
    ASSERT_TRUE(WaitForExecuteCount(2)) << "second check did not reach the table snapshot";
    EXPECT_EQ(pool->RemoveCalls(), 0u);

    (void)inject::Clear(AFTER_READ_INJECT);
    first.join();
    second.join();

    DS_EXPECT_OK(firstStatus);
    DS_EXPECT_OK(secondStatus);
    EXPECT_EQ(pool->RemoveCalls(), 1u);
    EXPECT_TRUE(TableContains(*manager, ADDR_B, "new-instance"));
    EXPECT_EQ(TableSize(*manager), 1u);
}

TEST_F(UcpManagerConnectionTest, CleanupDoesNotHoldInstanceTableLock)
{
    BlockingUcpWorkerPool *pool = nullptr;
    auto manager = MakeManager(pool);
    DS_ASSERT_OK(manager->CheckUcpConnectionStable(ADDR_A, "old-instance"));

    pool->blocking = true;
    Status queryStatus;
    std::atomic<bool> queryDone{ false };
    Status cleanupStatus;
    std::thread cleaner([&]() { cleanupStatus = manager->CheckUcpConnectionStable(ADDR_A, "new-instance"); });
    std::thread query;
    Raii cleanup([&]() {
        pool->Release();
        if (cleaner.joinable()) {
            cleaner.join();
        }
        if (query.joinable()) {
            query.join();
        }
    });
    ASSERT_TRUE(pool->WaitEntered()) << "endpoint cleanup did not start";

    query = std::thread([&]() {
        queryStatus = manager->CheckUcpConnectionStable(ADDR_B, "instance-b");
        queryDone.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(WaitForAtomic(queryDone)) << "instance table lock was held while endpoints were cleaned";
    EXPECT_EQ(pool->RemoveCalls(), 1u);

    pool->Release();
    cleaner.join();
    query.join();
    DS_EXPECT_OK(cleanupStatus);
    DS_EXPECT_OK(queryStatus);
    EXPECT_TRUE(TableContains(*manager, ADDR_A, "new-instance"));
    EXPECT_TRUE(TableContains(*manager, ADDR_B, "instance-b"));
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
