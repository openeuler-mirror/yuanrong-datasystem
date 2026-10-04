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
 * Description: Client mmap table entry management.
 */
#ifndef DATASYSTEM_CLIENT_MMAP_SHM_MMAP_TABLE_ENTRY_H
#define DATASYSTEM_CLIENT_MMAP_SHM_MMAP_TABLE_ENTRY_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/client/mmap_manager/immap_table_entry.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class ShmMmapTableEntry : public IMmapTableEntry {
public:
    ShmMmapTableEntry(int fd, size_t mmapSize, std::string clientId = "")
        : IMmapTableEntry(fd, mmapSize), clientId_(std::move(clientId))
    {
    }
    ~ShmMmapTableEntry();

    /**
     * @brief Mmap the client fd.
     * @param[in] enableHugeTlb huge_tlb switch
     * @return Status of the call.
     */
    Status Init(bool enableHugeTlb, const std::string &tenantId) override;

    /**
     * @brief Get the fd pointer.
     * @return The fd pointer.
     */
    const uint8_t *Pointer()
    {
        return pointer_;
    }

    void PinHostMemory();

    void SkipHostMemoryPin();

    void MarkRetired() noexcept override;

    void SetHostMemoryOperationMutex(const std::shared_ptr<std::timed_mutex> &mutex);

    void SetClientExitingFlag(const std::shared_ptr<std::atomic<bool>> &clientExiting);

    bool IsCudaHostMemoryRegistrationDone() const override;

    bool Contains(const void *pointer) const;

    Status GetMemcpySegmentSizes(const void *pointer, size_t size, std::vector<size_t> &segmentSizes) const;

private:
    struct PinRange {
        uint8_t *startAddr{ nullptr };
        size_t totalSize{ 0 };
        size_t sliceSize{ 0 };
    };

    struct PinFragment {
        uint8_t *pointer;
        size_t size;
    };

    struct PinResult {
        size_t successCount{ 0 };
        size_t attemptedFragmentCount{ 0 };
        size_t retryCount{ 0 };
        bool stoppedByClientExit{ false };
        bool stoppedByEntryRetirement{ false };
    };

    void BuildPinRange();
    size_t GetPinFragmentCount() const;
    PinFragment GetPinFragment(size_t fragmentIndex) const;
    bool IsClientExiting() const;
    bool IsRetired() const;
    bool TrySkipPinBeforeOperation();
    bool LockHostMemoryOperationForPin(std::unique_lock<std::timed_mutex> &lock);
    bool ShouldStopPinning(PinResult &result) const;
    void CompletePinWithoutRegistration(size_t fragmentCount,
                                        const std::chrono::steady_clock::time_point &begin);
    bool PinHostMemoryFragment(size_t fragmentIndex);
    PinResult PinHostMemoryFragments();
    bool UnpinHostMemoryFragment(size_t fragmentIndex);
    void UnpinHostMemory();
    void UnmapMemory();
    Status InitHugeTlbUnmapAlignment();

    const std::string clientId_;
    size_t unmapAlignment_{ 1 };
    PinRange pinRange_;
    std::shared_ptr<std::timed_mutex> hostMemoryOperationMutex_{ std::make_shared<std::timed_mutex>() };
    std::shared_ptr<std::atomic<bool>> clientExiting_;
    std::atomic<bool> retired_{ false };
    std::atomic<bool> pinCompleted_{ false };
    std::atomic<bool> pinAttempted_{ false };
    std::atomic<size_t> pinnedFragmentCount_{ 0 };
};
}  // namespace client
}  // namespace datasystem
#endif
