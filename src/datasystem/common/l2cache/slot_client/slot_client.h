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
 * Description: Distributed-disk slot storage client.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_CLIENT_H
#define DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_CLIENT_H

#include <cstdint>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "datasystem/common/l2cache/storage_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class Slot;

/**
 * @brief Route object requests into one of the local slots.
 */
class SlotClient : public StorageClient {
public:
    /**
     * @brief Construct a slot client on top of the given shared filesystem root.
     * @param[in] sfsPath The root path of the shared filesystem.
     */
    explicit SlotClient(const std::string &sfsPath);
    ~SlotClient();

    /**
     * @brief Initialize slot root path and partition parameters.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Save one object into its target slot.
     * @param[in] objectKey The object key used to select the target slot.
     * @param[in] version The object version to be persisted.
     * @param[in] timeoutMs The request timeout in millisecond.
     * @param[in] body The object content stream.
     * @param[in] asyncElapse The time this object being in the async queue.
     * @return Status of the call.
     */
    Status Save(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                const std::shared_ptr<std::iostream> &body, uint64_t asyncElapse = 0,
                WriteMode writeMode = WriteMode::NONE_L2_CACHE) override;

    /**
     * @brief Read one exact object version from its slot.
     * @param[in] objectKey The object key used to select the target slot.
     * @param[in] version The exact object version to read.
     * @param[in] timeoutMs The request timeout in millisecond.
     * @param[out] content The returned object content.
     * @return Status of the call.
     */
    Status Get(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
               std::shared_ptr<std::stringstream> &content) override;

    /**
     * @brief Read the latest visible object version from its slot.
     * @param[in] objectKey The object key used to select the target slot.
     * @param[in] timeoutMs The request timeout in millisecond.
     * @param[in] minVersion The lower version bound that returned version must exceed.
     * @param[out] content The returned object content.
     * @return Status of the call.
     */
    Status GetWithoutVersion(const std::string &objectKey, int64_t timeoutMs, uint64_t minVersion,
                             std::shared_ptr<std::stringstream> &content) override;

    /**
     * @brief Append a delete tombstone into the target slot.
     * @param[in] objectKey The object key used to select the target slot.
     * @param[in] maxVerToDelete The max version covered by this delete request.
     * @param[in] deleteAllVersion Whether all versions of the object should be deleted.
     * @param[in] asyncElapse The time this object being in the async queue.
     * @return Status of the call.
     */
    Status Delete(const std::string &objectKey, uint64_t maxVerToDelete, bool deleteAllVersion,
                  uint64_t asyncElapse = 0) override;

    /**
     * @brief Repair one slot after an interrupted write.
     * @param[in] slotId The slot identifier to be repaired.
     * @return Status of the call.
     */
    Status RepairSlot(uint32_t slotId);

    /**
     * @brief Compact one slot into a new active index/data set.
     * @param[in] slotId The slot identifier to compact.
     * @return Status of the call.
     */
    Status CompactSlot(uint32_t slotId);

    /**
     * @brief Merge one remote worker slot into the local slot with the same slot id.
     * @param[in] sourceWorkerAddress The source worker address.
     * @param[in] slotId The slot identifier on both source and target workers.
     * @return Status of the call.
     */
    Status MergeSlot(const std::string &sourceWorkerAddress, uint32_t slotId) override;

    /**
     * @brief Preload one remote worker slot into the local slot with the same slot id.
     * @param[in] sourceWorkerAddress The source worker address.
     * @param[in] slotId The slot identifier on both source and target workers.
     * @param[in] callback The optional callback that consumes visible object metadata and content.
     * @return Status of the call. Only the durable transfer result affects the returned status.
     */
    Status PreloadSlot(const std::string &sourceWorkerAddress, uint32_t slotId,
                       const SlotPreloadCallback &callback = SlotPreloadCallback{}) override;

    /**
     * @brief Return request success metrics. Phase 1 keeps this empty.
     * @return The request success metric string.
     */
    std::string GetRequestSuccessRate() const override;

private:
    void StartBackgroundCompactThread();
    void StopBackgroundCompactThread();
    void BackgroundCompactLoop();
    bool ShouldStopBackgroundCompactThread();
    void WakeBackgroundCompactThread();
    int64_t ComputeNextCompactDelayMs() const;
    std::vector<uint32_t> CollectCompactionCandidates() const;

    uint32_t GetSlotId(const std::string &objectKey) const;
    Slot &GetSlot(uint32_t slotId);
    std::string GetSlotPath(uint32_t slotId) const;
    std::string GetSlotPathForWorker(const std::string &workerAddress, uint32_t slotId) const;

    const std::string sfsPath_;
    std::string rootPath_;
    uint32_t slotNum_{ 128 };
    uint64_t maxDataFileBytes_{ 1024ul * 1024ul * 1024ul };
    mutable std::shared_mutex mu_;
    // Protected by mu_. One Slot is created lazily for each slot id.
    std::unordered_map<uint32_t, std::unique_ptr<Slot>> slots_;
    mutable std::mutex compactMu_;
    std::condition_variable compactCv_;
    bool stopCompactThread_{ false };
    uint64_t compactWakeupSeq_{ 0 };
    std::thread compactThread_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_CLIENT_H
