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
 * Description: Client mmap table management.
 */
#include "datasystem/client/mmap_manager/shm_mmap_table_entry.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <shared_mutex>
#include <sstream>
#include <thread>
#include <utility>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <linux/magic.h>

#include "datasystem/common/device/nvidia/cuda_host_memory.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace client {
namespace {
constexpr auto HOST_MEMORY_FRAGMENT_INTERVAL = std::chrono::milliseconds(5);
constexpr auto HOST_MEMORY_OPERATION_LOCK_RETRY_INTERVAL = std::chrono::milliseconds(1);
constexpr size_t HOST_MEMORY_FRAGMENT_SIZE = 64UL * 1024UL * 1024UL;
constexpr size_t HOST_MEMORY_PIN_MAX_RETRY_COUNT = 3;
constexpr size_t UNMAP_FRAGMENT_SIZE = 512UL * 1024UL * 1024UL;
constexpr size_t UNMAP_TOP_DURATION_COUNT = 3;
constexpr auto UNMAP_FRAGMENT_INTERVAL = std::chrono::milliseconds(1);
}  // namespace

Status ShmMmapTableEntry::Init(bool enableHugeTlb, const std::string &tenantId)
{
    (void)tenantId;
    std::stringstream err;
    if (size_ <= 0) {
        err << "The mmap size [" << size_ << "] is invalid for fd [" << fd_ << "]";
        LOG(ERROR) << err.str();
        RETURN_STATUS(StatusCode::K_INVALID, err.str());
    }
    INJECT_POINT("IMmapTableEntry.mmap");
    // mmap fd
    uint32_t mFlag = MAP_SHARED;
    if (enableHugeTlb) {
        RETURN_IF_NOT_OK(InitHugeTlbUnmapAlignment());
        mFlag |= MAP_HUGETLB;
    }
    const auto mmapBegin = std::chrono::steady_clock::now();
    pointer_ = reinterpret_cast<uint8_t *>(mmap(nullptr, size_, PROT_READ | PROT_WRITE, mFlag, fd_, 0));
    const int mmapErrno = errno;
    const auto mmapElapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - mmapBegin);
    if (pointer_ == MAP_FAILED) {
        RETURN_STATUS_LOG_ERROR(
            StatusCode::K_RUNTIME_ERROR,
            FormatString("Mmap [client id = %s, fd = %d] failed. Error no: [%s]", clientId_, fd_, StrErr(mmapErrno)));
    }
    // Exclude the shared memory from core dump.
    int ret = madvise(pointer_, size_, MADV_DONTDUMP);
    if (ret != 0) {
        // Ignore and write log.
        LOG(WARNING) << "madvise DONTDUMP memory failed: " << StrErr(errno);
    }
    // Closing this fd has an effect on performance.
    RETRY_ON_EINTR(close(fd_));
    LOG(INFO) << FormatString("mmap success, client id: %s, fd: %d, size: %zu", clientId_, fd_, size_)
              << ", elapsedUs: " << mmapElapsedUs.count();
    BuildPinRange();
    return Status::OK();
}

Status ShmMmapTableEntry::InitHugeTlbUnmapAlignment()
{
    struct statfs fsInfo {};
    CHECK_FAIL_RETURN_STATUS(fstatfs(fd_, &fsInfo) == 0, K_RUNTIME_ERROR,
                             FormatString("Query HugeTLB page size failed, fd: %d, errno: %s", fd_, StrErr(errno)));
    CHECK_FAIL_RETURN_STATUS(fsInfo.f_type == HUGETLBFS_MAGIC && fsInfo.f_bsize > 0, K_RUNTIME_ERROR,
                             FormatString("Invalid HugeTLB filesystem, fd: %d", fd_));
    unmapAlignment_ = static_cast<size_t>(fsInfo.f_bsize);
    return Status::OK();
}

void ShmMmapTableEntry::BuildPinRange()
{
    pinRange_ = PinRange{ pointer_, size_, HOST_MEMORY_FRAGMENT_SIZE };
}

size_t ShmMmapTableEntry::GetPinFragmentCount() const
{
    if (pinRange_.sliceSize == 0) {
        return 0;
    }
    return pinRange_.totalSize / pinRange_.sliceSize
           + (pinRange_.totalSize % pinRange_.sliceSize == 0 ? 0 : 1);
}

ShmMmapTableEntry::PinFragment ShmMmapTableEntry::GetPinFragment(size_t fragmentIndex) const
{
    const size_t offset = fragmentIndex * pinRange_.sliceSize;
    return PinFragment{ pinRange_.startAddr + offset, std::min(pinRange_.sliceSize, pinRange_.totalSize - offset) };
}

void ShmMmapTableEntry::PinHostMemory()
{
    if (TrySkipPinBeforeOperation()) {
        return;
    }
    std::unique_lock<std::timed_mutex> lock(*hostMemoryOperationMutex_, std::defer_lock);
    if (!LockHostMemoryOperationForPin(lock)) {
        return;
    }
    const bool registrationEnabled = IsCudaHostMemoryRegistrationEnabled();
    const auto begin = std::chrono::steady_clock::now();
    const size_t fragmentCount = GetPinFragmentCount();
    LOG(INFO) << "[CudaHostMemory] Worker shared memory pin started, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << fragmentCount
              << ", fragmentIntervalMs: " << HOST_MEMORY_FRAGMENT_INTERVAL.count()
              << ", registrationEnabled: " << registrationEnabled;
    try {
        INJECT_POINT_NO_RETURN("ShmMmapTableEntry.PinHostMemory");
    } catch (const std::exception &e) {
        LOG(WARNING) << "CUDA host memory pin injection failed: " << e.what();
    } catch (...) {
        LOG(WARNING) << "CUDA host memory pin injection failed with an unknown exception";
    }
    if (!registrationEnabled) {
        CompletePinWithoutRegistration(fragmentCount, begin);
        return;
    }
    pinAttempted_.store(true, std::memory_order_release);
    const auto pinResult = PinHostMemoryFragments();
    pinnedFragmentCount_.store(pinResult.successCount, std::memory_order_release);
    pinCompleted_.store(true, std::memory_order_release);
    const size_t failedCount = pinResult.attemptedFragmentCount - pinResult.successCount;
    const auto elapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
    LOG(INFO) << "[CudaHostMemory] Worker shared memory pin finished, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << fragmentCount
              << ", attemptedCount: " << pinResult.attemptedFragmentCount
              << ", successCount: " << pinResult.successCount << ", failedCount: " << failedCount
              << ", retryCount: " << pinResult.retryCount
              << ", stoppedByClientExit: " << pinResult.stoppedByClientExit
              << ", stoppedByEntryRetirement: " << pinResult.stoppedByEntryRetirement
              << ", registrationEnabled: true, completed: true, elapsedUs: " << elapsedUs.count();
}

void ShmMmapTableEntry::CompletePinWithoutRegistration(
    size_t fragmentCount, const std::chrono::steady_clock::time_point &begin)
{
    (void)RegisterCudaHostMemory(pointer_, size_);
    pinCompleted_.store(true, std::memory_order_release);
    const auto elapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
    LOG(INFO) << "[CudaHostMemory] Worker shared memory pin finished, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << fragmentCount << ", attemptedCount: 0, successCount: 0, failedCount: 0"
              << ", registrationEnabled: false, completed: true, elapsedUs: " << elapsedUs.count();
}

ShmMmapTableEntry::PinResult ShmMmapTableEntry::PinHostMemoryFragments()
{
    PinResult result;
    size_t remainingRetryCount = HOST_MEMORY_PIN_MAX_RETRY_COUNT;
    const size_t fragmentCount = GetPinFragmentCount();
    for (size_t i = 0; i < fragmentCount; ++i) {
        if (ShouldStopPinning(result)) {
            break;
        }
        if (i > 0) {
            std::this_thread::sleep_for(HOST_MEMORY_FRAGMENT_INTERVAL);
            if (ShouldStopPinning(result)) {
                break;
            }
        }
        ++result.attemptedFragmentCount;
        while (!PinHostMemoryFragment(i)) {
            if (ShouldStopPinning(result)) {
                return result;
            }
            if (remainingRetryCount == 0) {
                const auto fragment = GetPinFragment(i);
                LOG(ERROR) << "[CudaHostMemory] Worker shared memory pin stopped after retries were exhausted, "
                           << "clientId: " << clientId_ << ", fragmentIndex: " << i
                           << ", pointer: " << static_cast<void *>(fragment.pointer)
                           << ", size: " << fragment.size << ", successCount: " << result.successCount;
                return result;
            }
            --remainingRetryCount;
            ++result.retryCount;
            LOG(WARNING) << "[CudaHostMemory] Retry Worker shared memory fragment pin, clientId: " << clientId_
                         << ", fragmentIndex: " << i << ", retryCount: " << result.retryCount
                         << ", remainingRetryCount: " << remainingRetryCount;
        }
        ++result.successCount;
    }
    return result;
}

bool ShmMmapTableEntry::IsClientExiting() const
{
    return clientExiting_ != nullptr && clientExiting_->load(std::memory_order_acquire);
}

bool ShmMmapTableEntry::IsRetired() const
{
    return retired_.load(std::memory_order_acquire);
}

bool ShmMmapTableEntry::TrySkipPinBeforeOperation()
{
    const bool clientExiting = IsClientExiting();
    const bool retired = IsRetired();
    if (!clientExiting && !retired) {
        return false;
    }
    pinCompleted_.store(true, std::memory_order_release);
    LOG(INFO) << "[CudaHostMemory] Worker shared memory pin skipped before operation, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", clientExiting: " << clientExiting << ", entryRetired: " << retired;
    return true;
}

bool ShmMmapTableEntry::LockHostMemoryOperationForPin(std::unique_lock<std::timed_mutex> &lock)
{
    while (!lock.try_lock_for(HOST_MEMORY_OPERATION_LOCK_RETRY_INTERVAL)) {
        try {
            INJECT_POINT_NO_RETURN("ShmMmapTableEntry.PinHostMemoryOperationLockWait");
        } catch (const std::exception &e) {
            LOG(WARNING) << "CUDA host memory pin lock-wait injection failed: " << e.what();
        } catch (...) {
            LOG(WARNING) << "CUDA host memory pin lock-wait injection failed with an unknown exception";
        }
        if (TrySkipPinBeforeOperation()) {
            return false;
        }
    }
    return !TrySkipPinBeforeOperation();
}

bool ShmMmapTableEntry::ShouldStopPinning(PinResult &result) const
{
    result.stoppedByClientExit = IsClientExiting();
    result.stoppedByEntryRetirement = IsRetired();
    return result.stoppedByClientExit || result.stoppedByEntryRetirement;
}

bool ShmMmapTableEntry::PinHostMemoryFragment(size_t fragmentIndex)
{
    const auto fragment = GetPinFragment(fragmentIndex);
    try {
        return RegisterCudaHostMemory(fragment.pointer, fragment.size);
    } catch (const std::exception &e) {
        LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment pin failed unexpectedly, clientId: "
                   << clientId_ << ", fragmentIndex: " << fragmentIndex
                   << ", pointer: " << static_cast<void *>(fragment.pointer)
                   << ", size: " << fragment.size << ", error: " << e.what();
    } catch (...) {
        LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment pin failed with an unknown exception, clientId: "
                   << clientId_ << ", fragmentIndex: " << fragmentIndex
                   << ", pointer: " << static_cast<void *>(fragment.pointer) << ", size: " << fragment.size;
    }
    return false;
}

void ShmMmapTableEntry::SkipHostMemoryPin()
{
    pinCompleted_.store(true, std::memory_order_release);
}

void ShmMmapTableEntry::MarkRetired() noexcept
{
    retired_.store(true, std::memory_order_release);
}

void ShmMmapTableEntry::SetHostMemoryOperationMutex(const std::shared_ptr<std::timed_mutex> &mutex)
{
    if (mutex != nullptr) {
        hostMemoryOperationMutex_ = mutex;
    }
}

void ShmMmapTableEntry::SetClientExitingFlag(const std::shared_ptr<std::atomic<bool>> &clientExiting)
{
    clientExiting_ = clientExiting;
}

bool ShmMmapTableEntry::IsCudaHostMemoryRegistrationDone() const
{
    return pinCompleted_.load(std::memory_order_acquire);
}

bool ShmMmapTableEntry::Contains(const void *pointer) const
{
    const auto address = reinterpret_cast<uintptr_t>(pointer);
    const auto begin = reinterpret_cast<uintptr_t>(pointer_);
    return address >= begin && address - begin < size_;
}

Status ShmMmapTableEntry::GetMemcpySegmentSizes(const void *pointer, size_t size,
                                                std::vector<size_t> &segmentSizes) const
{
    const auto address = reinterpret_cast<uintptr_t>(pointer);
    const auto begin = reinterpret_cast<uintptr_t>(pinRange_.startAddr);
    CHECK_FAIL_RETURN_STATUS(pinRange_.sliceSize > 0, K_RUNTIME_ERROR,
                             "CUDA host-memory pin range is not initialized");
    CHECK_FAIL_RETURN_STATUS(address >= begin && address - begin < pinRange_.totalSize, K_INVALID,
                             "Host pointer is not in this Worker shared memory mapping");
    const size_t offset = static_cast<size_t>(address - begin);
    CHECK_FAIL_RETURN_STATUS(size <= pinRange_.totalSize - offset, K_INVALID,
                             "CUDA memcpy range exceeds the Worker shared memory mapping");
    size_t remaining = size;
    size_t offsetInFragment = offset % pinRange_.sliceSize;
    do {
        const size_t bytes = std::min(remaining, pinRange_.sliceSize - offsetInFragment);
        segmentSizes.emplace_back(bytes);
        remaining -= bytes;
        offsetInFragment = 0;
    } while (remaining > 0);
    return Status::OK();
}

ShmMmapTableEntry::~ShmMmapTableEntry()
{
    try {
        INJECT_POINT_NO_RETURN("ShmMmapTableEntry.Unmap");
    } catch (const std::exception &e) {
        LOG(WARNING) << "Worker shared memory unmap injection failed: " << e.what();
    } catch (...) {
        LOG(WARNING) << "Worker shared memory unmap injection failed with an unknown exception";
    }
    if (pointer_ == nullptr || pointer_ == MAP_FAILED) {
        LOG(ERROR) << FormatString("Mmap pointer is invalid, client id: %s, fd: %d, it may be nullptr", clientId_,
                                   fd_);
        return;
    }
    if (pinAttempted_.load(std::memory_order_acquire)) {
        UnpinHostMemory();
    }
    UnmapMemory();
}

void ShmMmapTableEntry::UnmapMemory()
{
    bool skipSleep = false;
    INJECT_POINT_NO_RETURN("ShmMmapTableEntry.UnmapMemory.skipSleep", [&skipSleep] { skipSleep = true; });
    size_t unmapFragmentSize = std::min(UNMAP_FRAGMENT_SIZE, size_);
    unmapFragmentSize += (unmapAlignment_ - unmapFragmentSize % unmapAlignment_) % unmapAlignment_;
    std::array<std::pair<int64_t, size_t>, UNMAP_TOP_DURATION_COUNT> topDurations{};
    bool unmapSucceeded = true;
    std::chrono::microseconds unmapElapsedUs{ 0 };
    const auto loopBegin = std::chrono::steady_clock::now();
    bool cleanupRemaining = false;
    for (size_t offset = 0; offset < size_;) {
        const size_t fragmentSize = cleanupRemaining ? size_ - offset : std::min(unmapFragmentSize, size_ - offset);
        const auto begin = std::chrono::steady_clock::now();
        int ret = munmap(pointer_ + offset, fragmentSize);
        const int unmapErrno = errno;
        const auto elapsedUs =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
        unmapElapsedUs += elapsedUs;
        std::pair<int64_t, size_t> duration{ elapsedUs.count(), offset / unmapFragmentSize + 1 };
        for (auto &topDuration : topDurations) {
            if (duration > topDuration) {
                std::swap(duration, topDuration);
            }
        }
        if (ret != 0) {
            unmapSucceeded = false;
            LOG(ERROR) << FormatString(
                "munmap failed, client id: %s, fd: %d, offset: %zu, size: %zu, returned: [%d], errno = [%s]",
                clientId_, fd_, offset, fragmentSize, ret, StrErr(unmapErrno));
            if (cleanupRemaining) {
                break;
            }
            // Only the failed fragment and its suffix are still owned; the released prefix may have been reused.
            cleanupRemaining = true;
            continue;
        }
        unmapSucceeded = true;
        offset += fragmentSize;
        if (offset < size_ && !skipSleep) {
            std::this_thread::sleep_for(UNMAP_FRAGMENT_INTERVAL);
        }
    }
    const auto totalElapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - loopBegin);
    std::ostringstream topDurationLog;
    for (size_t i = 0; i < topDurations.size() && topDurations[i].second != 0; ++i) {
        if (i > 0) {
            topDurationLog << "; ";
        }
        topDurationLog << "fragment: " << topDurations[i].second << ", elapsedUs: " << topDurations[i].first;
    }
    LOG(INFO) << "munmap for client id: " << clientId_ << ", fd: " << fd_
              << ", totalElapsedUs: " << totalElapsedUs.count()
              << ", unmapElapsedUs: " << unmapElapsedUs.count()
              << ", top 3 durations us: [ " << topDurationLog.str() << " ]";
    if (unmapSucceeded) {
        LOG(INFO) << FormatString("munmap success, client id: %s, fd: %d, size: %zu", clientId_, fd_, size_);
    }
}

void ShmMmapTableEntry::UnpinHostMemory()
{
    std::lock_guard<std::timed_mutex> lock(*hostMemoryOperationMutex_);
    try {
        INJECT_POINT_NO_RETURN("ShmMmapTableEntry.UnpinHostMemoryOperationLocked");
    } catch (const std::exception &e) {
        LOG(WARNING) << "CUDA host memory unpin operation-lock injection failed: " << e.what();
    } catch (...) {
        LOG(WARNING) << "CUDA host memory unpin operation-lock injection failed with an unknown exception";
    }
    const auto begin = std::chrono::steady_clock::now();
    const size_t totalFragmentCount = GetPinFragmentCount();
    const size_t pinnedFragmentCount = pinnedFragmentCount_.load(std::memory_order_acquire);
    const bool initialClientExiting = clientExiting_ != nullptr && clientExiting_->load(std::memory_order_acquire);
    const bool initialSkipFragmentInterval = initialClientExiting;
    LOG(INFO) << "[CudaHostMemory] Worker shared memory unpin started, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << totalFragmentCount << ", pinnedFragmentCount: " << pinnedFragmentCount
              << ", fragmentIntervalMs: "
              << (initialSkipFragmentInterval ? 0 : HOST_MEMORY_FRAGMENT_INTERVAL.count())
              << ", clientExiting: " << initialClientExiting;
    size_t failedCount = 0;
    for (size_t i = 0; i < pinnedFragmentCount; ++i) {
        const bool clientExiting = clientExiting_ != nullptr && clientExiting_->load(std::memory_order_acquire);
        if (i > 0 && !clientExiting) {
            std::this_thread::sleep_for(HOST_MEMORY_FRAGMENT_INTERVAL);
        }
        if (!UnpinHostMemoryFragment(i)) {
            ++failedCount;
            LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment unpin failed, clientId: " << clientId_
                       << ", fragmentIndex: " << i;
        }
    }
    const auto elapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
    const bool finalClientExiting = clientExiting_ != nullptr && clientExiting_->load(std::memory_order_acquire);
    LOG(INFO) << "[CudaHostMemory] Worker shared memory unpin finished, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << totalFragmentCount << ", pinnedFragmentCount: " << pinnedFragmentCount
              << ", attemptedCount: " << pinnedFragmentCount
              << ", successCount: " << pinnedFragmentCount - failedCount << ", failedCount: " << failedCount
              << ", clientExiting: " << finalClientExiting
              << ", completed: true, elapsedUs: " << elapsedUs.count();
}

bool ShmMmapTableEntry::UnpinHostMemoryFragment(size_t fragmentIndex)
{
    const auto fragment = GetPinFragment(fragmentIndex);
    try {
        return UnregisterCudaHostMemory(fragment.pointer);
    } catch (const std::exception &e) {
        LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment unpin failed unexpectedly, clientId: "
                   << clientId_ << ", fragmentIndex: " << fragmentIndex
                   << ", pointer: " << static_cast<void *>(fragment.pointer)
                   << ", size: " << fragment.size << ", error: " << e.what();
    } catch (...) {
        LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment unpin failed with an unknown exception, "
                   << "clientId: " << clientId_ << ", fragmentIndex: " << fragmentIndex
                   << ", pointer: " << static_cast<void *>(fragment.pointer) << ", size: " << fragment.size;
    }
    return false;
}
}  // namespace client
}  // namespace datasystem
