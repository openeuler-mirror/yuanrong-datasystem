/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: MemMgrBase device/host allocation lock and retry tests.
 */
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <sys/mman.h>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/common/device/device_resource_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/wait_post.h"

#include <gtest/gtest.h>

#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {
constexpr char ALLOCATE_INJECT[] = "worker.Allocator.AllocateMemory";
constexpr uint64_t MB = 1024 * 1024;

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

class TestMemMgr : public MemMgrBase {
public:
    explicit TestMemMgr(AllocateType type) : MemMgrBase(Allocator::Instance())
    {
        type_ = type;
        waitPost_ = std::make_unique<WaitPost>();
    }
};

class MemMgrBaseTest : public ::testing::Test {
protected:
    void TearDown() override
    {
        (void)inject::Clear(ALLOCATE_INJECT);
        Allocator::Instance()->ResetForTest();
    }

    Status InitPool(AllocateType type, uint64_t size)
    {
        AllocatorFuncRegister reg;
        reg.createFunc = [](void **pointer, size_t length) -> Status {
            *pointer = mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (*pointer == MAP_FAILED) {
                *pointer = nullptr;
                return Status(K_OUT_OF_MEMORY, "test mmap failed");
            }
            return Status::OK();
        };
        reg.destroyFunc = [](void *pointer, size_t length) -> Status {
            return munmap(pointer, length) == 0 ? Status::OK() : Status(K_RUNTIME_ERROR, "test munmap failed");
        };
        return Allocator::Instance()->InitWithFlexibleRegister(type, size, reg);
    }

};

TEST_F(MemMgrBaseTest, AllocationAndRetryRules)
{
    struct Case {
        const char *action;
        bool skip;
        StatusCode code;
        uint64_t attempts;
    };
    const Case cases[] = {
        { "call()", false, K_OK, 1 },
        { "1*return(K_RUNTIME_ERROR)", false, K_RUNTIME_ERROR, 1 },
        { "return(K_OUT_OF_MEMORY)", true, K_OUT_OF_MEMORY, 1 },
        { "return(K_OUT_OF_MEMORY)", false, K_OUT_OF_MEMORY, 11 },
    };
    DS_ASSERT_OK(InitPool(AllocateType::DEV_HOST, MB));
    TestMemMgr manager(AllocateType::DEV_HOST);
    for (const auto &item : cases) {
        SCOPED_TRACE(item.action);
        ASSERT_TRUE(inject::Set(ALLOCATE_INJECT, item.action).IsOk());
        std::vector<BufferMetaInfo> meta{ { 1, 0, 4096 } };
        std::vector<ShmUnit> pool(1);
        auto begin = std::chrono::steady_clock::now();
        auto rc = manager.Allocate(meta, pool, item.skip);
        if (item.attempts == 11) {
            EXPECT_GE(std::chrono::steady_clock::now() - begin, std::chrono::milliseconds(800));
        }
        EXPECT_EQ(rc.GetCode(), item.code);
        EXPECT_EQ(inject::GetExecuteCount(ALLOCATE_INJECT), item.attempts);
        DS_ASSERT_OK(manager.Free(pool));
        (void)inject::Clear(ALLOCATE_INJECT);
    }
}

TEST_F(MemMgrBaseTest, FreeFromAnotherBufferUnblocksOomWaiter)
{
    DS_ASSERT_OK(InitPool(AllocateType::DEV_HOST, MB));
    TestMemMgr manager(AllocateType::DEV_HOST);
    std::vector<BufferMetaInfo> metaB{ { 1, 0, 768 * 1024 } };
    std::vector<ShmUnit> poolB(1);
    DS_ASSERT_OK(manager.Allocate(metaB, poolB));

    ASSERT_TRUE(inject::Set(ALLOCATE_INJECT, "1*pause()").IsOk());
    std::vector<BufferMetaInfo> metaA{ { 1, 0, 512 * 1024 } };
    std::vector<ShmUnit> poolA(1);
    Status freeStatus;
    std::atomic<bool> freeStarted{ false };
    std::atomic<bool> freeDone{ false };
    Status allocateStatus;
    std::atomic<bool> allocateDone{ false };
    std::thread allocator([&]() {
        allocateStatus = manager.Allocate(metaA, poolA);
        allocateDone.store(true, std::memory_order_release);
    });

    std::thread releaser;
    Raii cleanup([&]() {
        (void)inject::Clear(ALLOCATE_INJECT);
        if (allocator.joinable()) {
            allocator.join();
        }
        if (releaser.joinable()) {
            releaser.join();
        }
    });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline
           && inject::GetExecuteCount(ALLOCATE_INJECT) == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(inject::GetExecuteCount(ALLOCATE_INJECT), 1u) << "allocator did not reach the pause";

    releaser = std::thread([&]() {
        freeStarted.store(true, std::memory_order_release);
        freeStatus = manager.Free(poolB);
        freeDone.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(WaitForAtomic(freeStarted)) << "releasing thread did not start";

    (void)inject::Clear(ALLOCATE_INJECT);
    ASSERT_TRUE(WaitForAtomic(allocateDone)) << "allocator did not finish after memory was released";
    releaser.join();
    allocator.join();

    DS_EXPECT_OK(freeStatus);
    DS_EXPECT_OK(allocateStatus);
    EXPECT_TRUE(freeDone.load(std::memory_order_acquire));
    EXPECT_NE(poolA[0].pointer, nullptr);
    DS_ASSERT_OK(manager.Free(poolA));
}

TEST_F(MemMgrBaseTest, PartialSuccessCanBeFreedWithoutDoubleFree)
{
    DS_ASSERT_OK(InitPool(AllocateType::DEV_HOST, MB));
    TestMemMgr manager(AllocateType::DEV_HOST);
    constexpr uint64_t chunk = 400 * 1024;
    std::vector<BufferMetaInfo> meta{ { 1, 0, chunk }, { 1, chunk, chunk }, { 1, 2 * chunk, chunk } };
    std::vector<ShmUnit> pool(3);

    auto rc = manager.Allocate(meta, pool);
    EXPECT_EQ(rc.GetCode(), StatusCode::K_OUT_OF_MEMORY);
    EXPECT_NE(pool[0].pointer, nullptr);
    EXPECT_NE(pool[1].pointer, nullptr);
    EXPECT_EQ(pool[2].pointer, nullptr);

    DS_ASSERT_OK(manager.Free(pool));
    for (const auto &unit : pool) {
        EXPECT_EQ(unit.pointer, nullptr);
    }
}

TEST_F(MemMgrBaseTest, HostAndDeviceBatchSizes)
{
    for (auto type : { AllocateType::DEV_HOST, AllocateType::DEV_DEVICE }) {
        DS_ASSERT_OK(InitPool(type, MB));
        TestMemMgr manager(type);
        std::vector<BufferMetaInfo> meta{ { 1, 0, 1024 }, { 2, 1024, 4096 } };
        std::vector<ShmUnit> pool(2);
        DS_ASSERT_OK(manager.Allocate(meta, pool));
        EXPECT_EQ(pool[0].size, type == AllocateType::DEV_DEVICE ? 4096u : 1024u);
        EXPECT_EQ(pool[1].size, 4096u);
        DS_ASSERT_OK(manager.Free(pool));
    }
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
