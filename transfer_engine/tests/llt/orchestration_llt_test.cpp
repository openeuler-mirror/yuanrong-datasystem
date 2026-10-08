/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/transfer_engine/status_helper.h"
#include "datasystem/transfer_engine/transfer_engine.h"
#include "internal/connection/connection_manager.h"
#include "internal/control_plane/transfer_control_service.h"
#include "internal/memory/registered_memory_table.h"

namespace datasystem {
namespace {

constexpr auto K_TEST_WAIT = std::chrono::seconds(2);
constexpr auto K_TEST_POLL_INTERVAL = std::chrono::milliseconds(5);

class ReadGate final {
public:
    ReadGate() = default;
    ~ReadGate() = default;

    void Block()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        entered_ = true;
        cv_.notify_all();
        cv_.wait_for(lock, K_TEST_WAIT, [this]() { return released_; });
    }

    bool WaitForEntry()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, K_TEST_WAIT, [this]() { return entered_; });
    }

    void Release()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_ = false;
    bool released_ = false;
};

class FakeBackend final : public IDataPlaneBackend {
public:
    ~FakeBackend() override = default;

    bool RequiresAclRuntime() const override
    {
        return false;
    }
    std::string BackendKind() const override
    {
        return "ascend";
    }
    std::string RoutePolicy() const override
    {
        return "roce";
    }
    uint64_t MemoryGeneration() const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (bumpGenerationPerCall_) {
            memoryGeneration_ += 1;
        }
        return memoryGeneration_;
    }
    bool SupportsReceiverDrivenRead() const override
    {
        return receiverDriven_;
    }
    uint64_t ReadLeaseTtlMs() const override
    {
        return readLeaseTtlMs_;
    }

    Result InitializeLocal(const std::string &localHost, uint16_t localPort, int32_t localDeviceId) override
    {
        (void)localHost;
        (void)localPort;
        (void)localDeviceId;
        return Result::OK();
    }

    void FinalizeLocal() override
    {
        if (finalizeInFlight_.fetch_add(1) != 0) {
            finalizeConcurrent_.fetch_add(1);
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            finalizeCallCount_ += 1;
            finalizeSeq_ = ++sequence_;
        }
        if (blockFinalize_) {
            finalizeEntered_ = true;
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            while (!releaseFinalize_.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(K_TEST_POLL_INTERVAL);
            }
        }
        finalizeInFlight_.fetch_sub(1);
    }

    Result RegisterLocalMemory(uint64_t addr, uint64_t length) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        registerCallCount_ += 1;
        if (failRegistrationOnCall_ > 0 && registerCallCount_ == failRegistrationOnCall_) {
            return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "injected registration failure");
        }
        registeredRanges_.emplace_back(addr, length);
        ++memoryGeneration_;
        connectionReady_ = false;
        return Result::OK();
    }

    Result UnregisterLocalMemory(uint64_t addr, uint64_t length) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        unregisterAttemptCount_ += 1;
        if (failUnregistrationOnCall_ > 0 && unregisterAttemptCount_ == failUnregistrationOnCall_) {
            return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "injected unregistration failure");
        }
        for (auto iter = registeredRanges_.begin(); iter != registeredRanges_.end(); ++iter) {
            if (iter->first == addr && iter->second == length) {
                registeredRanges_.erase(iter);
                ++memoryGeneration_;
                connectionReady_ = false;
                break;
            }
        }
        return Result::OK();
    }

    Result PrepareReadDestinations(const std::vector<TransferMemoryRegion> &regions) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const auto &region : regions) {
                bool found = false;
                for (const auto &registered : registeredRanges_) {
                    if (region.addr >= registered.first && region.length <= registered.second
                        && region.addr - registered.first <= registered.second - region.length) {
                        found = true;
                        break;
                    }
                }
                TE_CHECK_OR_RETURN(found, ErrorCode::kNotFound, "destination is not registered");
            }
        }
        if (prepareGate_ != nullptr) {
            prepareGate_->Block();
        }
        return Result::OK();
    }

    Result CreateRootInfo(std::string *rootInfoBytes) override
    {
        TE_CHECK_PTR_OR_RETURN(rootInfoBytes);
        *rootInfoBytes = "fake_root_info";
        return Result::OK();
    }

    Result InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes) override
    {
        (void)spec;
        (void)rootInfoBytes;
        std::lock_guard<std::mutex> lock(mutex_);
        initRecvCallCount_ += 1;
        connectionReady_ = true;
        if (changeGenerationDuringConnect_) {
            ++memoryGeneration_;
        }
        return Result::OK();
    }

    Result InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes) override
    {
        (void)spec;
        (void)rootInfoBytes;
        return Result::OK();
    }

    Result PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length) override
    {
        (void)spec;
        (void)localAddr;
        (void)length;
        return Result::OK();
    }

    Result PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length) override
    {
        (void)spec;
        (void)remoteAddr;
        (void)length;
        return Result::OK();
    }

    Result WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs) override
    {
        (void)spec;
        (void)timeoutMs;
        return Result::OK();
    }

    Result TransferSyncRead(const ConnectionSpec &spec, const std::vector<TransferReadOp> &ops,
                            uint64_t timeoutMs) override
    {
        (void)spec;
        (void)ops;
        (void)timeoutMs;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            transferReadCallCount_ += 1;
            TE_CHECK_OR_RETURN(connectionReady_, ErrorCode::kNotReady, "connection invalidated by registration");
            if (notReadyReadsBeforeOk_ > 0) {
                notReadyReadsBeforeOk_ -= 1;
                return TE_MAKE_STATUS(ErrorCode::kNotReady, "injected not-ready read");
            }
        }
        if (readGate_ != nullptr) {
            readGate_->Block();
        }
        if (blockReads_) {
            readEntered_ = true;
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            while (!releaseReads_.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(K_TEST_POLL_INTERVAL);
            }
        }
        std::lock_guard<std::mutex> lock(mutex_);
        lastReadCompletionSeq_ = ++sequence_;
        return Result::OK();
    }

    void AbortConnection(const ConnectionSpec &spec) override
    {
        (void)spec;
        std::lock_guard<std::mutex> lock(mutex_);
        connectionReady_ = false;
        ++abortCount_;
    }

    // knobs
    bool receiverDriven_ = true;
    uint64_t readLeaseTtlMs_ = K_DEFAULT_READ_LEASE_TTL_MS;
    ReadGate *prepareGate_ = nullptr;
    ReadGate *readGate_ = nullptr;
    bool bumpGenerationPerCall_ = false;
    bool changeGenerationDuringConnect_ = false;
    std::atomic<int32_t> abortCount_{ 0 };
    int32_t failRegistrationOnCall_ = -1;
    int32_t failUnregistrationOnCall_ = -1;
    int32_t notReadyReadsBeforeOk_ = 0;
    bool blockReads_ = false;
    std::atomic<bool> releaseReads_{ false };
    std::atomic<bool> readEntered_{ false };
    bool blockFinalize_ = false;
    std::atomic<bool> releaseFinalize_{ false };
    std::atomic<bool> finalizeEntered_{ false };
    std::atomic<int32_t> finalizeInFlight_{ 0 };
    std::atomic<int32_t> finalizeConcurrent_{ 0 };

    // observability
    uint64_t LastReadCompletionSeq()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastReadCompletionSeq_;
    }
    uint64_t FinalizeSeq()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return finalizeSeq_;
    }
    int32_t RegisterCallCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return registerCallCount_;
    }
    int32_t UnregisterAttemptCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return unregisterAttemptCount_;
    }
    int32_t InitRecvCallCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return initRecvCallCount_;
    }
    int32_t TransferReadCallCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return transferReadCallCount_;
    }
    int32_t FinalizeCallCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return finalizeCallCount_;
    }
    bool HasRegisteredRange(uint64_t addr, uint64_t length)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto &range : registeredRanges_) {
            if (range.first == addr && range.second == length) {
                return true;
            }
        }
        return false;
    }

private:
    mutable std::mutex mutex_;
    mutable uint64_t memoryGeneration_ = 1;
    bool connectionReady_ = false;
    int32_t registerCallCount_ = 0;
    int32_t unregisterAttemptCount_ = 0;
    int32_t initRecvCallCount_ = 0;
    int32_t transferReadCallCount_ = 0;
    int32_t finalizeCallCount_ = 0;
    uint64_t sequence_ = 0;
    uint64_t lastReadCompletionSeq_ = 0;
    uint64_t finalizeSeq_ = 0;
    std::vector<std::pair<uint64_t, uint64_t>> registeredRanges_;
};

std::shared_ptr<FakeBackend> MakeEngine(std::unique_ptr<TransferEngine> *engine, const std::string &endpoint)
{
    auto backend = std::make_shared<FakeBackend>();
    *engine = std::make_unique<TransferEngine>(backend);
    Result rc = (*engine)->Initialize(endpoint, "ascend", "npu:0");
    EXPECT_TRUE(rc.IsOk()) << "initialize failed: " << rc.ToString();
    return backend;
}

// 中文说明：两个逻辑区域共享同一 backing 时，backing 只注册一次；
// 注销第一个逻辑区域后 backing 保留，注销第二个后 backend 恰好注销一次。
TEST(TransferEngineOrchestrationLltTest, SharedBackingRefCountLifecycle)
{
    constexpr uint16_t kPort = 55311;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);

    std::vector<MemoryRegistration> registrations = { MemoryRegistration{ 0x100000, 0x100, 0x100000, 0x200 },
                                                      MemoryRegistration{ 0x100100, 0x100, 0x100000, 0x200 } };
    ASSERT_TRUE(engine->BatchRegisterMemoryEx(registrations).IsOk());
    EXPECT_EQ(backend->RegisterCallCount(), 1);
    EXPECT_TRUE(backend->HasRegisteredRange(0x100000, 0x200));

    ASSERT_TRUE(engine->BatchUnregisterMemory({ 0x100000 }).IsOk());
    EXPECT_EQ(backend->UnregisterAttemptCount(), 0);
    EXPECT_TRUE(backend->HasRegisteredRange(0x100000, 0x200));

    ASSERT_TRUE(engine->BatchUnregisterMemory({ 0x100100 }).IsOk());
    EXPECT_EQ(backend->UnregisterAttemptCount(), 1);
    EXPECT_FALSE(backend->HasRegisteredRange(0x100000, 0x200));

    ASSERT_TRUE(engine->Finalize().IsOk());
}

// 中文说明：第 k 个 backing 注册失败时，前 k-1 个已注册的 backing 被回滚，引擎仍可正常使用。
TEST(TransferEngineOrchestrationLltTest, RegistrationFailureRollsBackSucceededBackings)
{
    constexpr uint16_t kPort = 55312;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);

    backend->failRegistrationOnCall_ = 2;
    std::vector<MemoryRegistration> registrations = { MemoryRegistration{ 0x100000, 0x100, 0x100000, 0x100 },
                                                      MemoryRegistration{ 0x100200, 0x100, 0x100200, 0x100 } };
    Result rc = engine->BatchRegisterMemoryEx(registrations);
    ASSERT_TRUE(rc.IsError());
    EXPECT_EQ(rc.GetCode(), ErrorCode::kRuntimeError);
    EXPECT_EQ(backend->RegisterCallCount(), 2);
    EXPECT_EQ(backend->UnregisterAttemptCount(), 1);
    EXPECT_FALSE(backend->HasRegisteredRange(0x100000, 0x100));

    backend->failRegistrationOnCall_ = -1;
    ASSERT_TRUE(engine->BatchRegisterMemoryEx({ MemoryRegistration{ 0x100000, 0x100, 0x100000, 0x100 } }).IsOk());
    EXPECT_TRUE(backend->HasRegisteredRange(0x100000, 0x100));

    ASSERT_TRUE(engine->Finalize().IsOk());
}

// 中文说明：注册失败且回滚也失败时，backend 进入 degraded，延迟清理直到显式 Finalize，
// 后续所有 API 返回 kNotReady。
TEST(TransferEngineOrchestrationLltTest, RegistrationRollbackFailureDegradesBackend)
{
    constexpr uint16_t kPort = 55313;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);

    backend->failRegistrationOnCall_ = 2;
    backend->failUnregistrationOnCall_ = 1;
    std::vector<MemoryRegistration> registrations = { MemoryRegistration{ 0x100000, 0x100, 0x100000, 0x100 },
                                                      MemoryRegistration{ 0x100200, 0x100, 0x100200, 0x100 } };
    Result rc = engine->BatchRegisterMemoryEx(registrations);
    ASSERT_TRUE(rc.IsError());
    EXPECT_EQ(rc.GetCode(), ErrorCode::kRuntimeError);
    EXPECT_EQ(backend->FinalizeCallCount(), 0);

    Result degradedRc = engine->BatchRegisterMemoryEx({ MemoryRegistration{ 0x300000, 0x100, 0x300000, 0x100 } });
    EXPECT_EQ(degradedRc.GetCode(), ErrorCode::kNotReady);

    Result readRc =
        engine->BatchTransferSyncRead("127.0.0.1:" + std::to_string(kPort), { 0x400000 }, { 0x100010 }, { 0x10 });
    EXPECT_EQ(readRc.GetCode(), ErrorCode::kNotReady);

    ASSERT_TRUE(engine->Finalize().IsOk());
}

// 中文说明：注销失败且回滚（重注册）也失败时，backend 进入 degraded。
TEST(TransferEngineOrchestrationLltTest, UnregistrationRollbackFailureDegradesBackend)
{
    constexpr uint16_t kPort = 55314;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);

    std::vector<MemoryRegistration> registrations = { MemoryRegistration{ 0x100000, 0x100, 0x100000, 0x100 },
                                                      MemoryRegistration{ 0x100200, 0x100, 0x100200, 0x100 } };
    ASSERT_TRUE(engine->BatchRegisterMemoryEx(registrations).IsOk());
    EXPECT_EQ(backend->RegisterCallCount(), 2);

    // 第二个 backing 注销失败，回滚需要重新注册第一个 backing（第 3 次注册调用）也失败。
    backend->failUnregistrationOnCall_ = 2;
    backend->failRegistrationOnCall_ = 3;
    Result rc = engine->BatchUnregisterMemory({ 0x100000, 0x100200 });
    ASSERT_TRUE(rc.IsError());
    EXPECT_EQ(rc.GetCode(), ErrorCode::kRuntimeError);
    EXPECT_EQ(backend->FinalizeCallCount(), 0);

    Result degradedRc = engine->BatchRegisterMemoryEx({ MemoryRegistration{ 0x300000, 0x100, 0x300000, 0x100 } });
    EXPECT_EQ(degradedRc.GetCode(), ErrorCode::kNotReady);

    ASSERT_TRUE(engine->Finalize().IsOk());
}

// 中文说明：receiver-driven 读路径上，backend 第一次 TransferSyncRead 返回 kNotReady 会触发
// 路由失效与重建连接，第二次成功后调用方拿到 OK。
TEST(TransferEngineOrchestrationLltTest, ReceiverDrivenReadRetriesNotReadyThenSucceeds)
{
    constexpr uint16_t kPort = 55315;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);

    ASSERT_TRUE(engine->RegisterMemory(0x100000, 0x1000).IsOk());
    ASSERT_TRUE(engine->RegisterMemory(0x400000, 0x1000).IsOk());

    backend->notReadyReadsBeforeOk_ = 1;
    const std::string target = "127.0.0.1:" + std::to_string(kPort);
    Result rc = engine->BatchTransferSyncRead(target, { 0x400000 }, { 0x100010 }, { 0x10 });
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(backend->TransferReadCallCount(), 2);
    EXPECT_EQ(backend->InitRecvCallCount(), 2);

    ASSERT_TRUE(engine->Finalize().IsOk());
}

// 中文说明：owner 内存代持续变化时，读重试两次后耗尽并返回 kNotReady，
// 每次重试都重建了连接（InitRecv 被调用两次）。
TEST(TransferEngineOrchestrationLltTest, GenerationMismatchExhaustsRetries)
{
    constexpr uint16_t kPort = 55316;
    constexpr uint16_t kRequesterPort = 55326;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);
    std::unique_ptr<TransferEngine> requester;
    auto requesterBackend = MakeEngine(&requester, "127.0.0.1:" + std::to_string(kRequesterPort));
    ASSERT_NE(requesterBackend, nullptr);

    ASSERT_TRUE(engine->RegisterMemory(0x100000, 0x1000).IsOk());
    ASSERT_TRUE(requester->RegisterMemory(0x400000, 0x1000).IsOk());

    backend->bumpGenerationPerCall_ = true;
    const std::string target = "127.0.0.1:" + std::to_string(kPort);
    Result rc = requester->BatchTransferSyncRead(target, { 0x400000 }, { 0x100010 }, { 0x10 });
    ASSERT_TRUE(rc.IsError());
    EXPECT_EQ(rc.GetCode(), ErrorCode::kNotReady);
    EXPECT_EQ(requesterBackend->InitRecvCallCount(), 2);

    ASSERT_TRUE(requester->Finalize().IsOk());
    ASSERT_TRUE(engine->Finalize().IsOk());
}

// 中文说明：不支持 receiver-driven 读的后端走 legacy 路径，PostRecv/PostSend/WaitRecv
// override 后读成功。
TEST(TransferEngineOrchestrationLltTest, LegacyReadPathUsesSenderDrivenOperations)
{
    constexpr uint16_t kPort = 55317;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);
    backend->receiverDriven_ = false;

    ASSERT_TRUE(engine->RegisterMemory(0x100000, 0x1000).IsOk());
    ASSERT_TRUE(engine->RegisterMemory(0x400000, 0x1000).IsOk());

    const std::string target = "127.0.0.1:" + std::to_string(kPort);
    Result rc = engine->BatchTransferSyncRead(target, { 0x400000, 0x400100 }, { 0x100010, 0x100020 }, { 0x10, 0x20 });
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();

    ASSERT_TRUE(engine->Finalize().IsOk());
}

// 中文说明：in-flight 读存在时 Finalize 等待读完成后才继续，backend 的 FinalizeLocal
// 发生在最后一次读完成之后。
TEST(TransferEngineOrchestrationLltTest, FinalizeWaitsForInFlightRead)
{
    constexpr uint16_t kPort = 55318;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);

    ASSERT_TRUE(engine->RegisterMemory(0x100000, 0x1000).IsOk());
    ASSERT_TRUE(engine->RegisterMemory(0x400000, 0x1000).IsOk());

    backend->blockReads_ = true;
    const std::string target = "127.0.0.1:" + std::to_string(kPort);
    std::thread readThread(
        [&engine, &target]() { (void)engine->BatchTransferSyncRead(target, { 0x400000 }, { 0x100010 }, { 0x10 }); });

    for (int32_t i = 0; i < 1000 && !backend->readEntered_.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (!backend->readEntered_.load()) {
        backend->releaseReads_ = true;
        readThread.join();
        FAIL() << "read did not enter the backend";
    }

    std::thread finalizeThread([&engine]() { (void)engine->Finalize(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    backend->releaseReads_ = true;
    readThread.join();
    finalizeThread.join();

    EXPECT_GT(backend->FinalizeSeq(), backend->LastReadCompletionSeq());
    EXPECT_TRUE(engine->Finalize().IsOk());
}

TEST(TransferEngineOrchestrationLltTest, ConcurrentFinalizeSerializesBackendTeardown)
{
    constexpr uint16_t kPort = 55328;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);
    ASSERT_TRUE(engine->RegisterMemory(0x100000, 0x1000).IsOk());

    backend->blockFinalize_ = true;
    Result firstRc;
    std::thread firstFinalizer([&engine, &firstRc]() { firstRc = engine->Finalize(); });
    for (int32_t i = 0; i < 1000 && !backend->finalizeEntered_.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (!backend->finalizeEntered_.load()) {
        backend->releaseFinalize_ = true;
        firstFinalizer.join();
        FAIL() << "backend finalize did not enter";
    }

    Result secondRc;
    std::thread secondFinalizer([&engine, &secondRc]() { secondRc = engine->Finalize(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(backend->FinalizeCallCount(), 1);
    EXPECT_EQ(backend->finalizeConcurrent_.load(), 0);
    backend->releaseFinalize_ = true;
    firstFinalizer.join();
    secondFinalizer.join();

    EXPECT_TRUE(firstRc.IsOk()) << firstRc.ToString();
    EXPECT_TRUE(secondRc.IsOk()) << secondRc.ToString();
    EXPECT_EQ(backend->FinalizeCallCount(), 1);
    EXPECT_EQ(backend->finalizeConcurrent_.load(), 0);
}

TEST(TransferEngineOrchestrationLltTest, ReadDestinationsRemainPinnedThroughPreparation)
{
    constexpr uint16_t kPort = 55319;
    constexpr uintptr_t kSource = 0x100000;
    constexpr uintptr_t kDestination = 0x400000;
    constexpr uintptr_t kUnrelated = 0x500000;
    constexpr size_t kLength = 0x1000;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);
    ASSERT_TRUE(
        engine->BatchRegisterMemory({ kSource, kDestination, kUnrelated }, { kLength, kLength, kLength }).IsOk());
    ReadGate gate;
    backend->prepareGate_ = &gate;
    Result readResult;
    std::thread reader([&readResult, &engine, kPort, kDestination, kSource, kLength]() {
        readResult = engine->TransferSyncRead("127.0.0.1:" + std::to_string(kPort), kDestination, kSource, kLength);
    });
    EXPECT_TRUE(gate.WaitForEntry());
    EXPECT_EQ(engine->UnregisterMemory(kDestination).GetCode(), ErrorCode::kNotReady);
    EXPECT_TRUE(engine->UnregisterMemory(kUnrelated).IsOk());
    gate.Release();
    reader.join();
    EXPECT_TRUE(readResult.IsOk()) << readResult.ToString();
    EXPECT_TRUE(engine->UnregisterMemory(kDestination).IsOk());
    EXPECT_TRUE(engine->Finalize().IsOk());
}

TEST(TransferEngineOrchestrationLltTest, InvalidDestinationBatchDoesNotLeavePins)
{
    constexpr uint16_t kPort = 55320;
    constexpr uintptr_t kDestination = 0x400000;
    constexpr uintptr_t kMissingDestination = 0x500000;
    constexpr uintptr_t kSource = 0x100000;
    constexpr size_t kLength = 0x100;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);
    ASSERT_TRUE(engine->RegisterMemory(kDestination, kLength).IsOk());
    auto result =
        engine->BatchTransferSyncRead("127.0.0.1:" + std::to_string(kPort), { kDestination, kMissingDestination },
                                      { kSource, kSource }, { kLength, kLength });
    EXPECT_EQ(result.GetCode(), ErrorCode::kNotFound);
    EXPECT_TRUE(engine->UnregisterMemory(kDestination).IsOk());
    EXPECT_TRUE(engine->Finalize().IsOk());
}

TEST(TransferEngineOrchestrationLltTest, SharedBackingStaysPinnedAcrossBatchAndReadFailure)
{
    constexpr uint16_t kPort = 55325;
    constexpr uintptr_t kFirstLogical = 0x400000;
    constexpr uintptr_t kSecondLogical = 0x400100;
    constexpr uintptr_t kReadDestination = 0x400080;
    constexpr uintptr_t kRemoteAddress = 0x100000;
    constexpr size_t kLogicalLength = 0x40;
    constexpr size_t kBackingLength = 0x200;
    constexpr size_t kReadLength = 0x10;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);
    ASSERT_TRUE(engine
                    ->BatchRegisterMemoryEx(
                        { MemoryRegistration{ kFirstLogical, kLogicalLength, kFirstLogical, kBackingLength },
                          MemoryRegistration{ kSecondLogical, kLogicalLength, kFirstLogical, kBackingLength } })
                    .IsOk());
    ReadGate gate;
    backend->prepareGate_ = &gate;
    Result readResult;
    std::thread reader([&readResult, &engine, kPort, kReadDestination, kRemoteAddress, kReadLength]() {
        readResult =
            engine->BatchTransferSyncRead("127.0.0.1:" + std::to_string(kPort), { kReadDestination, kReadDestination },
                                          { kRemoteAddress, kRemoteAddress }, { kReadLength, kReadLength });
    });
    EXPECT_TRUE(gate.WaitForEntry());
    EXPECT_EQ(engine->UnregisterMemory(kFirstLogical).GetCode(), ErrorCode::kNotReady);
    EXPECT_EQ(engine->UnregisterMemory(kSecondLogical).GetCode(), ErrorCode::kNotReady);
    gate.Release();
    reader.join();
    EXPECT_EQ(readResult.GetCode(), ErrorCode::kNotAuthorized);
    EXPECT_TRUE(engine->BatchUnregisterMemory({ kFirstLogical, kSecondLogical }).IsOk());
    EXPECT_EQ(backend->UnregisterAttemptCount(), 1);
    EXPECT_TRUE(engine->Finalize().IsOk());
}

TEST(TransferEngineOrchestrationLltTest, DegradedOwnerRetainsActiveRemoteReadUntilFinalize)
{
    constexpr uint16_t kOwnerPort = 55321, kRequesterPort = 55322;
    constexpr uintptr_t kSource = 0x100000, kDestination = 0x400000;
    constexpr uintptr_t kNewBacking = 0x500000, kFailedBacking = 0x600000;
    constexpr size_t kLength = 0x100;
    constexpr int32_t kFailRegistrationCall = 3;
    std::unique_ptr<TransferEngine> owner;
    std::unique_ptr<TransferEngine> requester;
    auto ownerBackend = MakeEngine(&owner, "127.0.0.1:" + std::to_string(kOwnerPort));
    auto requesterBackend = MakeEngine(&requester, "127.0.0.1:" + std::to_string(kRequesterPort));
    ASSERT_NE(ownerBackend, nullptr);
    ASSERT_NE(requesterBackend, nullptr);
    ASSERT_TRUE(owner->RegisterMemory(kSource, kLength).IsOk());
    ASSERT_TRUE(requester->RegisterMemory(kDestination, kLength).IsOk());
    ReadGate gate;
    requesterBackend->readGate_ = &gate;
    Result readResult;
    std::thread reader([&readResult, &requester, kOwnerPort, kDestination, kSource, kLength]() {
        readResult =
            requester->TransferSyncRead("127.0.0.1:" + std::to_string(kOwnerPort), kDestination, kSource, kLength);
    });
    EXPECT_TRUE(gate.WaitForEntry());
    ownerBackend->failRegistrationOnCall_ = kFailRegistrationCall;
    ownerBackend->failUnregistrationOnCall_ = 1;
    EXPECT_EQ(owner->BatchRegisterMemory({ kNewBacking, kFailedBacking }, { kLength, kLength }).GetCode(),
              ErrorCode::kRuntimeError);
    EXPECT_EQ(ownerBackend->FinalizeCallCount(), 0);
    EXPECT_TRUE(ownerBackend->HasRegisteredRange(kSource, kLength));
    EXPECT_EQ(owner->RegisterMemory(kFailedBacking, kLength).GetCode(), ErrorCode::kNotReady);
    SocketControlClient client;
    BatchReadTriggerRequest req;
    req.requesterHost = "127.0.0.1";
    req.requesterPort = kRequesterPort;
    req.requesterDeviceId = 0;
    req.items.push_back(BatchReadItem{ 1, kSource, kLength });
    BatchReadTriggerResponse rsp;
    EXPECT_TRUE(client.BatchReadTrigger("127.0.0.1", kOwnerPort, req, &rsp).IsOk());
    EXPECT_EQ(rsp.code, static_cast<int32_t>(ErrorCode::kNotReady));
    gate.Release();
    reader.join();
    EXPECT_TRUE(readResult.IsOk()) << readResult.ToString();
    EXPECT_EQ(ownerBackend->FinalizeCallCount(), 0);
    EXPECT_TRUE(owner->Finalize().IsOk());
    EXPECT_EQ(ownerBackend->FinalizeCallCount(), 1);
    EXPECT_TRUE(owner->Initialize("127.0.0.1:" + std::to_string(kOwnerPort), "ascend", "npu:0").IsOk());
    EXPECT_TRUE(owner->RegisterMemory(kSource, kLength).IsOk());
    EXPECT_TRUE(owner->Finalize().IsOk());
    EXPECT_TRUE(requester->Finalize().IsOk());
}

TEST(TransferEngineOrchestrationLltTest, LocalRegistrationChangeRebuildsBeforeTransfer)
{
    constexpr uint16_t kOwnerPort = 55323;
    constexpr uint16_t kRequesterPort = 55324;
    constexpr uintptr_t kSource = 0x100000;
    constexpr uintptr_t kDestination = 0x400000;
    constexpr uintptr_t kNewBacking = 0x500000;
    constexpr size_t kLength = 0x100;
    constexpr int32_t kExpectedReads = 2;
    std::unique_ptr<TransferEngine> owner;
    std::unique_ptr<TransferEngine> requester;
    auto ownerBackend = MakeEngine(&owner, "127.0.0.1:" + std::to_string(kOwnerPort));
    auto requesterBackend = MakeEngine(&requester, "127.0.0.1:" + std::to_string(kRequesterPort));
    ASSERT_NE(ownerBackend, nullptr);
    ASSERT_NE(requesterBackend, nullptr);
    ASSERT_TRUE(owner->RegisterMemory(kSource, kLength).IsOk());
    ASSERT_TRUE(requester->RegisterMemory(kDestination, kLength).IsOk());
    const auto read = [&requester, kOwnerPort, kDestination, kSource, kLength]() {
        return requester->TransferSyncRead("127.0.0.1:" + std::to_string(kOwnerPort), kDestination, kSource, kLength);
    };
    ASSERT_TRUE(read().IsOk());
    ASSERT_TRUE(requester->RegisterMemory(kNewBacking, kLength).IsOk());
    EXPECT_TRUE(read().IsOk());
    EXPECT_EQ(requesterBackend->TransferReadCallCount(), kExpectedReads);
    EXPECT_TRUE(requester->Finalize().IsOk());
    EXPECT_TRUE(owner->Finalize().IsOk());
}

TEST(TransferEngineOrchestrationLltTest, ConnectionGenerationFailureCleansRequesterRoute)
{
    constexpr uint16_t kPort = 55327;
    constexpr uintptr_t kBuffer = 0x100000;
    constexpr size_t kLength = 0x100;
    std::unique_ptr<TransferEngine> engine;
    auto backend = MakeEngine(&engine, "127.0.0.1:" + std::to_string(kPort));
    ASSERT_NE(backend, nullptr);
    ASSERT_TRUE(engine->RegisterMemory(kBuffer, kLength).IsOk());
    backend->changeGenerationDuringConnect_ = true;
    auto rc = engine->TransferSyncRead("127.0.0.1:" + std::to_string(kPort), kBuffer, kBuffer, kLength);
    EXPECT_EQ(rc.GetCode(), ErrorCode::kNotReady);
    EXPECT_EQ(backend->abortCount_.load(), 1);
    EXPECT_EQ(backend->TransferReadCallCount(), 0);
    backend->changeGenerationDuringConnect_ = false;
    EXPECT_TRUE(engine->TransferSyncRead("127.0.0.1:" + std::to_string(kPort), kBuffer, kBuffer, kLength).IsOk());
    EXPECT_TRUE(engine->UnregisterMemory(kBuffer).IsOk());
    EXPECT_TRUE(engine->Finalize().IsOk());
}

// 中文说明：控制面 RPC 入口对 requester 身份做统一校验：端口必须在 1..65535，
// host 非空，device_id 非负。非法身份在进入 backend 前即被拒绝。
TEST(TransferControlServiceLltTest, RejectsMalformedRequesterIdentity)
{
    auto backend = std::make_shared<FakeBackend>();
    auto connMgr = std::make_shared<ConnectionManager>();
    auto registeredMemory = std::make_shared<RegisteredMemoryTable>();
    auto service = CreateTransferControlService("127.0.0.1", 61234, 0, connMgr, registeredMemory, backend);

    QueryConnReadyRequest queryReq;
    QueryConnReadyResponse queryRsp;
    queryReq.requesterHost = "127.0.0.1";
    queryReq.requesterDeviceId = 0;
    queryReq.requesterPort = 65536;
    EXPECT_EQ(service->QueryConnReady(queryReq, &queryRsp).GetCode(), ErrorCode::kInvalid);
    queryReq.requesterPort = 0;
    EXPECT_EQ(service->QueryConnReady(queryReq, &queryRsp).GetCode(), ErrorCode::kInvalid);
    queryReq.requesterPort = 12345;
    queryReq.requesterHost.clear();
    EXPECT_EQ(service->QueryConnReady(queryReq, &queryRsp).GetCode(), ErrorCode::kInvalid);
    queryReq.requesterHost = "127.0.0.1";
    queryReq.requesterDeviceId = -1;
    EXPECT_EQ(service->QueryConnReady(queryReq, &queryRsp).GetCode(), ErrorCode::kInvalid);
    queryReq.requesterDeviceId = 0;
    EXPECT_TRUE(service->QueryConnReady(queryReq, &queryRsp).IsOk());

    ReadTriggerRequest readReq;
    ReadTriggerResponse readRsp;
    readReq.requesterHost = "";
    readReq.requesterPort = 12345;
    readReq.requesterDeviceId = 0;
    readReq.length = 0x10;
    readReq.remoteAddr = 0x100010;
    EXPECT_EQ(service->ReadTrigger(readReq, &readRsp).GetCode(), ErrorCode::kInvalid);
    readReq.requesterHost = "127.0.0.1";
    readReq.requesterPort = 65537;
    EXPECT_EQ(service->ReadTrigger(readReq, &readRsp).GetCode(), ErrorCode::kInvalid);
    readReq.requesterPort = 12345;
    ASSERT_TRUE(service->ReadTrigger(readReq, &readRsp).IsOk());
    EXPECT_EQ(readRsp.code, static_cast<int32_t>(ErrorCode::kNotAuthorized));

    BatchReadTriggerRequest batchReq;
    BatchReadTriggerResponse batchRsp;
    batchReq.requesterHost = "127.0.0.1";
    batchReq.requesterDeviceId = 0;
    batchReq.requesterPort = 65536;
    batchReq.items.push_back(BatchReadItem{ 1, 0x100010, 0x10 });
    EXPECT_EQ(service->BatchReadTrigger(batchReq, &batchRsp).GetCode(), ErrorCode::kInvalid);

    ReleaseReadLeaseRequest releaseReq;
    ReleaseReadLeaseResponse releaseRsp;
    releaseReq.requesterHost = "127.0.0.1";
    releaseReq.requesterDeviceId = 0;
    releaseReq.requesterPort = 65536;
    releaseReq.readLeaseId = 1;
    EXPECT_EQ(service->ReleaseReadLease(releaseReq, &releaseRsp).GetCode(), ErrorCode::kInvalid);
}

TEST(TransferControlServiceLltTest, GrantsBackendValidatedReadLeaseTtl)
{
    auto backend = std::make_shared<FakeBackend>();
    backend->readLeaseTtlMs_ = 50;
    auto registeredMemory = std::make_shared<RegisteredMemoryTable>();
    ASSERT_TRUE(registeredMemory->AddRegion(RegisteredRegion{ 0x100000, 0x100, 0, 0x100000, 0x100 }));
    auto service = CreateTransferControlService("127.0.0.1", 61235, 0, std::make_shared<ConnectionManager>(),
                                                registeredMemory, backend);

    BatchReadTriggerRequest req;
    req.requesterHost = "127.0.0.1";
    req.requesterPort = 12345;
    req.requesterDeviceId = 0;
    req.items.push_back(BatchReadItem{ 1, 0x100010, 0x10 });
    BatchReadTriggerResponse rsp;
    ASSERT_TRUE(service->BatchReadTrigger(req, &rsp).IsOk());
    ASSERT_EQ(rsp.code, 0);
    ASSERT_NE(rsp.readLeaseId, 0);
    EXPECT_TRUE(registeredMemory->WaitForNoActiveReadLeases(500));
}

}  // namespace
}  // namespace datasystem
