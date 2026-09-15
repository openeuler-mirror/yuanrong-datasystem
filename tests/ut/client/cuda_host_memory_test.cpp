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

/** Description: Test process-wide CUDA callback registration. */
#include "datasystem/common/device/nvidia/cuda_host_memory.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#ifdef __linux__
#include <linux/memfd.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include <gtest/gtest.h>

#include "datasystem/client/mmap_manager/host_memory_pin_manager.h"
#include "datasystem/client/mmap_manager/shm_mmap_table.h"
#include "datasystem/client/mmap_manager/shm_mmap_table_entry.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_int32(v);

namespace datasystem {
namespace {
#ifdef __linux__
constexpr size_t PIN_SLICE_SIZE = 64UL * 1024UL * 1024UL;
constexpr size_t EXPECTED_PIN_FRAGMENT_COUNT = 2;
constexpr auto ASYNC_STATE_TIMEOUT = std::chrono::seconds(2);
constexpr auto ASYNC_STATE_POLL_INTERVAL = std::chrono::milliseconds(1);
constexpr char PIN_LOCK_WAIT_INJECT_POINT[] = "ShmMmapTableEntry.PinHostMemoryOperationLockWait";
constexpr char UNPIN_OPERATION_LOCKED_INJECT_POINT[] = "ShmMmapTableEntry.UnpinHostMemoryOperationLocked";
#endif
std::atomic<int> firstHostRegisterCalls{ 0 };
std::atomic<int> firstHostUnregisterCalls{ 0 };
std::atomic<int> firstMemcpyCalls{ 0 };
std::atomic<int> secondHostRegisterCalls{ 0 };
std::atomic<int> secondHostUnregisterCalls{ 0 };
std::atomic<int> secondGetErrorStringCalls{ 0 };
std::atomic<int> secondMemcpyCalls{ 0 };
std::atomic<bool> slowCallbacks{ false };
std::atomic<bool> markClientExitingOnRegister{ false };
std::atomic<client::ShmMmapTableEntry *> retireEntryOnRegister{ nullptr };
const std::shared_ptr<std::atomic<bool>> clientExiting = std::make_shared<std::atomic<bool>>(false);

void DelaySlowCallback()
{
    if (slowCallbacks.load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(CUDA_SLOW_OPERATION_THRESHOLD_US + 1));
    }
}

int FirstHostRegister(void *, size_t, unsigned int)
{
    DelaySlowCallback();
    ++firstHostRegisterCalls;
    if (markClientExitingOnRegister.load(std::memory_order_acquire)) {
        clientExiting->store(true, std::memory_order_release);
    }
    auto *entry = retireEntryOnRegister.exchange(nullptr, std::memory_order_acq_rel);
    if (entry != nullptr) {
        entry->MarkRetired();
    }
    return kCudaSuccess;
}

int FirstHostUnregister(void *)
{
    DelaySlowCallback();
    ++firstHostUnregisterCalls;
    return kCudaSuccess;
}

int FirstMemcpyAsync(void *, const void *, size_t, DsCudaMemcpyKind, void *)
{
    DelaySlowCallback();
    ++firstMemcpyCalls;
    return 17;
}

const char *ThrowingGetErrorString(int)
{
    throw std::runtime_error("injected getErrorString failure");
}

int SecondHostRegister(void *, size_t, unsigned int)
{
    ++secondHostRegisterCalls;
    return kCudaSuccess;
}

int SecondHostUnregister(void *)
{
    ++secondHostUnregisterCalls;
    return kCudaSuccess;
}

const char *SecondGetErrorString(int)
{
    ++secondGetErrorStringCalls;
    return "second CUDA error";
}

int SecondMemcpyAsync(void *, const void *, size_t, DsCudaMemcpyKind, void *)
{
    ++secondMemcpyCalls;
    return 18;
}

void VerifySlowCallbacksLogWithoutVlog()
{
    const auto oldV = FLAGS_v;
    const auto oldStderr = FLAGS_alsologtostderr;
    Raii restore([oldV, oldStderr] {
        slowCallbacks.store(false);
        FLAGS_v = oldV;
        FLAGS_alsologtostderr = oldStderr;
    });
    FLAGS_v = 0;
    FLAGS_alsologtostderr = true;
    slowCallbacks.store(true);
    char source = 0;
    char destination = 0;
    testing::internal::CaptureStderr();
    const bool registered = RegisterCudaHostMemory(&source, sizeof(source));
    const bool unregistered = UnregisterCudaHostMemory(&source);
    const auto status = DsCudaMemcpyAsync(&destination, &source, sizeof(source),
                                         DsCudaMemcpyKind::DEVICE_TO_HOST, nullptr);
    const auto output = testing::internal::GetCapturedStderr();
    EXPECT_TRUE(registered);
    EXPECT_TRUE(unregistered);
    EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR);
    EXPECT_NE(output.find("[CUDA_HOST_SLOW] operation=register"), std::string::npos) << output;
    EXPECT_NE(output.find("[CUDA_HOST_SLOW] operation=unregister"), std::string::npos) << output;
    EXPECT_NE(output.find("[CUDA_MEMCPY_SLOW] direction=D2H"), std::string::npos) << output;
    EXPECT_NE(output.find("callback_us="), std::string::npos) << output;
    EXPECT_NE(output.find("end_timestamp_us="), std::string::npos) << output;
    EXPECT_NE(output.find("suppressed_count="), std::string::npos) << output;
    EXPECT_NE(output.find("suppressed_max_us="), std::string::npos) << output;
}

#ifdef __linux__
template <typename Predicate>
bool WaitForAsyncState(Predicate &&predicate)
{
    const auto deadline = std::chrono::steady_clock::now() + ASYNC_STATE_TIMEOUT;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(ASYNC_STATE_POLL_INTERVAL);
    }
    return predicate();
}

void VerifyClientExitStopsRemainingPinFragments()
{
    const size_t mmapSize = PIN_SLICE_SIZE + static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const int fd = static_cast<int>(syscall(SYS_memfd_create, "mmap_entry_exit_ut", MFD_ALLOW_SEALING));
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, static_cast<off_t>(mmapSize)), 0);
    firstHostRegisterCalls.store(0, std::memory_order_release);
    firstHostUnregisterCalls.store(0, std::memory_order_release);
    clientExiting->store(false, std::memory_order_release);
    markClientExitingOnRegister.store(true, std::memory_order_release);

    {
        client::ShmMmapTableEntry entry(fd, mmapSize);
        ASSERT_TRUE(entry.Init(false, "").IsOk());
        std::vector<size_t> segmentSizes;
        ASSERT_TRUE(entry.GetMemcpySegmentSizes(entry.Pointer(), mmapSize, segmentSizes).IsOk());
        ASSERT_EQ(segmentSizes.size(), EXPECTED_PIN_FRAGMENT_COUNT);
        entry.SetClientExitingFlag(clientExiting);
        entry.PinHostMemory();

        EXPECT_TRUE(entry.IsCudaHostMemoryRegistrationDone());
        EXPECT_EQ(firstHostRegisterCalls.load(std::memory_order_acquire), 1);
    }
    EXPECT_EQ(firstHostUnregisterCalls.load(std::memory_order_acquire), 1);
    markClientExitingOnRegister.store(false, std::memory_order_release);
}

void VerifyEntryRetirementStopsRemainingPinFragments()
{
    const size_t mmapSize = PIN_SLICE_SIZE + static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const int fd = static_cast<int>(syscall(SYS_memfd_create, "mmap_entry_retired_ut", MFD_ALLOW_SEALING));
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, static_cast<off_t>(mmapSize)), 0);
    firstHostRegisterCalls.store(0, std::memory_order_release);
    firstHostUnregisterCalls.store(0, std::memory_order_release);
    clientExiting->store(false, std::memory_order_release);
    markClientExitingOnRegister.store(false, std::memory_order_release);

    {
        client::ShmMmapTableEntry entry(fd, mmapSize);
        ASSERT_TRUE(entry.Init(false, "").IsOk());
        retireEntryOnRegister.store(&entry, std::memory_order_release);
        entry.PinHostMemory();

        EXPECT_TRUE(entry.IsCudaHostMemoryRegistrationDone());
        EXPECT_EQ(firstHostRegisterCalls.load(std::memory_order_acquire), 1);
        EXPECT_EQ(retireEntryOnRegister.exchange(nullptr, std::memory_order_acq_rel), nullptr);
    }
    EXPECT_EQ(firstHostUnregisterCalls.load(std::memory_order_acquire), 1);
}

void VerifyImmutableRegistrySnapshotPublication()
{
    const size_t mmapSize = PIN_SLICE_SIZE + static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const int firstFd = static_cast<int>(syscall(SYS_memfd_create, "registry_snapshot_first_ut", MFD_ALLOW_SEALING));
    ASSERT_GE(firstFd, 0);
    ASSERT_EQ(ftruncate(firstFd, static_cast<off_t>(mmapSize)), 0);
    const int secondFd = static_cast<int>(syscall(SYS_memfd_create, "registry_snapshot_second_ut", MFD_ALLOW_SEALING));
    ASSERT_GE(secondFd, 0);
    ASSERT_EQ(ftruncate(secondFd, static_cast<off_t>(mmapSize)), 0);

    client::HostMemoryPinManager pinManager;
    auto firstEntry = pinManager.CreateEntry(firstFd, mmapSize, "first_registry_entry");
    ASSERT_TRUE(firstEntry->Init(false, "").IsOk());
    pinManager.Submit(firstEntry);
    auto secondEntry = pinManager.CreateEntry(secondFd, mmapSize, "second_registry_entry");
    ASSERT_TRUE(secondEntry->Init(false, "").IsOk());
    pinManager.Submit(secondEntry);

    std::vector<size_t> firstSegmentSizes;
    ASSERT_TRUE(pinManager.GetMemcpySegmentSizes(firstEntry->Pointer(), mmapSize, firstSegmentSizes).IsOk());
    EXPECT_EQ(firstSegmentSizes.size(), EXPECTED_PIN_FRAGMENT_COUNT);
    EXPECT_EQ(firstSegmentSizes.front(), PIN_SLICE_SIZE);
    std::vector<size_t> secondSegmentSizes;
    ASSERT_TRUE(pinManager.GetMemcpySegmentSizes(secondEntry->Pointer(), mmapSize, secondSegmentSizes).IsOk());
    EXPECT_EQ(secondSegmentSizes.size(), EXPECTED_PIN_FRAGMENT_COUNT);
    EXPECT_EQ(secondSegmentSizes.front(), PIN_SLICE_SIZE);
}

void VerifyWorkerRetirementCancelsPinWaitingForUnpin()
{
    const size_t mmapSize = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const int firstFd = static_cast<int>(syscall(SYS_memfd_create, "retired_unpin_owner_ut", MFD_ALLOW_SEALING));
    ASSERT_GE(firstFd, 0);
    ASSERT_EQ(ftruncate(firstFd, static_cast<off_t>(mmapSize)), 0);
    const int secondFd = static_cast<int>(syscall(SYS_memfd_create, "retired_waiting_pin_ut", MFD_ALLOW_SEALING));
    ASSERT_GE(secondFd, 0);
    ASSERT_EQ(ftruncate(secondFd, static_cast<off_t>(mmapSize)), 0);
    firstHostRegisterCalls.store(0, std::memory_order_release);
    firstHostUnregisterCalls.store(0, std::memory_order_release);

    auto pinManager = std::make_shared<client::HostMemoryPinManager>();
    client::ShmMmapTable mmapTable(false, pinManager);
    constexpr int64_t firstWorkerFd = 1001;
    constexpr int64_t secondWorkerFd = 1002;
    ASSERT_TRUE(mmapTable.MmapAndStoreFd(firstFd, firstWorkerFd, mmapSize, "", "retired_chain_ut").IsOk());
    auto firstEntry = mmapTable.GetMmapEntryByFd(firstWorkerFd);
    ASSERT_NE(firstEntry, nullptr);
    ASSERT_TRUE(WaitForAsyncState([&firstEntry] { return firstEntry->IsCudaHostMemoryRegistrationDone(); }));
    ASSERT_EQ(firstHostRegisterCalls.load(std::memory_order_acquire), 1);
    firstEntry.reset();

    const auto unpinInjectRc = inject::Set(UNPIN_OPERATION_LOCKED_INJECT_POINT, "1*pause()");
    Raii clearInjectPoints([] {
        (void)inject::Clear(PIN_LOCK_WAIT_INJECT_POINT);
        (void)inject::Clear(UNPIN_OPERATION_LOCKED_INJECT_POINT);
    });
    ASSERT_TRUE(unpinInjectRc.IsOk());
    ASSERT_TRUE(inject::Set(PIN_LOCK_WAIT_INJECT_POINT, "1*pause()").IsOk());
    mmapTable.ClearExpiredFds({ firstWorkerFd });
    ASSERT_TRUE(WaitForAsyncState(
        [] { return inject::GetExecuteCount(UNPIN_OPERATION_LOCKED_INJECT_POINT) == 1; }));

    ASSERT_TRUE(mmapTable.MmapAndStoreFd(secondFd, secondWorkerFd, mmapSize, "", "retired_chain_ut").IsOk());
    auto secondEntry = mmapTable.GetMmapEntryByFd(secondWorkerFd);
    ASSERT_NE(secondEntry, nullptr);
    ASSERT_TRUE(WaitForAsyncState([] { return inject::GetExecuteCount(PIN_LOCK_WAIT_INJECT_POINT) == 1; }));
    mmapTable.ClearExpiredFds({ secondWorkerFd });
    EXPECT_EQ(mmapTable.GetMmapEntryByFd(secondWorkerFd), nullptr);

    ASSERT_TRUE(inject::Clear(PIN_LOCK_WAIT_INJECT_POINT).IsOk());
    ASSERT_TRUE(WaitForAsyncState([&secondEntry] { return secondEntry->IsCudaHostMemoryRegistrationDone(); }));
    EXPECT_EQ(firstHostRegisterCalls.load(std::memory_order_acquire), 1);
    EXPECT_EQ(firstHostUnregisterCalls.load(std::memory_order_acquire), 0);
    ASSERT_TRUE(inject::Clear(UNPIN_OPERATION_LOCKED_INJECT_POINT).IsOk());
    ASSERT_TRUE(WaitForAsyncState([] { return firstHostUnregisterCalls.load(std::memory_order_acquire) == 1; }));
}

#endif
}  // namespace

TEST(CudaHostMemoryTest, RegisteredCallbacksAreFrozenAndStopSignalsControlPinFragments)
{
    char source = 0;
    char destination = 0;
    RegisterCudaFuncs({});

    CudaFuncs incompleteFuncs;
    incompleteFuncs.hostRegister = SecondHostRegister;
    incompleteFuncs.hostUnregister = SecondHostUnregister;
    RegisterCudaFuncs(incompleteFuncs);

    CudaFuncs firstFuncs;
    firstFuncs.hostRegister = FirstHostRegister;
    firstFuncs.hostUnregister = FirstHostUnregister;
    firstFuncs.getErrorString = ThrowingGetErrorString;
    firstFuncs.memcpyAsync = FirstMemcpyAsync;
    RegisterCudaFuncs(firstFuncs);

    CudaFuncs secondFuncs;
    secondFuncs.hostRegister = SecondHostRegister;
    secondFuncs.hostUnregister = SecondHostUnregister;
    secondFuncs.getErrorString = SecondGetErrorString;
    secondFuncs.memcpyAsync = SecondMemcpyAsync;
    RegisterCudaFuncs(secondFuncs);

    EXPECT_TRUE(RegisterCudaHostMemory(&source, sizeof(source)));
    EXPECT_TRUE(UnregisterCudaHostMemory(&source));
    const auto status =
        DsCudaMemcpyAsync(&destination, &source, sizeof(source), DsCudaMemcpyKind::HOST_TO_DEVICE, nullptr);

    EXPECT_EQ(firstHostRegisterCalls.load(), 1);
    EXPECT_EQ(firstHostUnregisterCalls.load(), 1);
    EXPECT_EQ(firstMemcpyCalls.load(), 1);
    EXPECT_EQ(secondHostRegisterCalls.load(), 0);
    EXPECT_EQ(secondHostUnregisterCalls.load(), 0);
    EXPECT_EQ(secondGetErrorStringCalls.load(), 0);
    EXPECT_EQ(secondMemcpyCalls.load(), 0);
    EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR);
    EXPECT_NE(status.ToString().find("error: 17"), std::string::npos);

#ifdef __linux__
    VerifyClientExitStopsRemainingPinFragments();
    VerifyEntryRetirementStopsRemainingPinFragments();
    VerifyWorkerRetirementCancelsPinWaitingForUnpin();
#endif
    VerifySlowCallbacksLogWithoutVlog();
}

TEST(CudaHostMemoryTest, ImmutableRegistrySnapshotPreservesPublishedMappings)
{
#ifdef __linux__
    VerifyImmutableRegistrySnapshotPublication();
#else
    GTEST_SKIP() << "Linux memfd + ShmMmapTableEntry path only";
#endif
}

TEST(CudaHostMemoryTest, SlowLogLimiterIntervalAndIndependentCategories)
{
    CudaSlowLogState limiter;
    CudaSlowLogState otherCategory;
    const std::chrono::steady_clock::time_point start{};
    const auto interval = std::chrono::microseconds(CUDA_SLOW_LOG_INTERVAL_US);
    uint64_t suppressed = 0;
    int64_t maxUs = 0;
    EXPECT_TRUE(TryAcquireCudaSlowLog(limiter, 900000, suppressed, maxUs, start));
    EXPECT_EQ(suppressed, 0);
    EXPECT_EQ(maxUs, 0);
    EXPECT_FALSE(TryAcquireCudaSlowLog(limiter, 500000, suppressed, maxUs, start));
    EXPECT_FALSE(TryAcquireCudaSlowLog(limiter, 200000, suppressed, maxUs,
                                      start + interval - std::chrono::microseconds(1)));
    EXPECT_TRUE(TryAcquireCudaSlowLog(otherCategory, 900000, suppressed, maxUs, start));
    EXPECT_EQ(suppressed, 0);
    EXPECT_EQ(maxUs, 0);
    EXPECT_TRUE(TryAcquireCudaSlowLog(limiter, 900000, suppressed, maxUs, start + interval));
    EXPECT_EQ(suppressed, 2);
    EXPECT_EQ(maxUs, 500000);
    EXPECT_TRUE(TryAcquireCudaSlowLog(limiter, 900000, suppressed, maxUs, start + interval + interval));
    EXPECT_EQ(suppressed, 0);
    EXPECT_EQ(maxUs, 0);
}

TEST(CudaHostMemoryTest, SlowLogLimiterConcurrentCallsHaveOneWinner)
{
    constexpr size_t threadCount = 16;
    CudaSlowLogState limiter;
    const std::chrono::steady_clock::time_point start{};
    std::atomic<size_t> ready{ 0 };
    std::atomic<size_t> winners{ 0 };
    std::atomic<uint64_t> reported{ 0 };
    std::atomic<bool> run{ false };
    std::vector<std::thread> threads;
    for (size_t i = 0; i < threadCount; ++i) {
        threads.emplace_back([&limiter, start, &ready, &winners, &reported, &run] {
            ready.fetch_add(1);
            while (!run.load()) {
                std::this_thread::yield();
            }
            uint64_t suppressed = 0;
            int64_t maxUs = 0;
            if (TryAcquireCudaSlowLog(limiter, 200000, suppressed, maxUs, start)) {
                winners.fetch_add(1);
                reported.fetch_add(suppressed);
            }
        });
    }
    while (ready.load() != threadCount) {
        std::this_thread::yield();
    }
    run.store(true);
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(winners.load(), 1);
    uint64_t suppressed = 0;
    int64_t maxUs = 0;
    EXPECT_TRUE(TryAcquireCudaSlowLog(limiter, 900000, suppressed, maxUs,
                                     start + std::chrono::microseconds(CUDA_SLOW_LOG_INTERVAL_US)));
    EXPECT_EQ(reported.load() + suppressed, threadCount - 1);
}
}  // namespace datasystem
