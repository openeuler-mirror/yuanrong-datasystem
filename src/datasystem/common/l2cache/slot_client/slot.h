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
 * Description: Single-slot storage manager.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_H
#define DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "datasystem/common/l2cache/slot_client/slot_manifest.h"
#include "datasystem/common/l2cache/slot_client/slot_snapshot.h"
#include "datasystem/common/l2cache/slot_client/slot_takeover_planner.h"
#include "datasystem/common/l2cache/slot_client/slot_transfer.h"
#include "datasystem/common/l2cache/slot_client/slot_writer.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Runtime manifest and snapshot cached for the active slot view.
 */
struct SlotRuntimeState {
    SlotManifestData manifest;
    SlotSnapshot snapshot;
    int64_t manifestModifiedTimeUs{ 0 };
    bool initialized{ false };

    /**
     * @brief Reset the cached runtime state to an uninitialized state.
     */
    void Reset()
    {
        manifest = SlotManifest::Bootstrap();
        snapshot = SlotSnapshot{};
        manifestModifiedTimeUs = 0;
        initialized = false;
    }
};

/**
 * @brief Persist and query objects inside one local slot directory.
 */
class Slot {
public:
    ~Slot() = default;

    /**
     * @brief Construct a slot for one local slot directory.
     * @param[in] slotId The slot identifier.
     * @param[in] slotPath The absolute path of this slot directory.
     * @param[in] maxDataFileBytes The max size of one data file in bytes.
     */
    Slot(uint32_t slotId, std::string slotPath, uint64_t maxDataFileBytes);

    /**
     * @brief Append object data and a PUT record into this slot.
     * @param[in] key The object key.
     * @param[in] version The object version.
     * @param[in] body The object content stream.
     * @param[in] asyncElapse The time this object being in the async queue.
     * @return Status of the call.
     */
    Status Save(const std::string &key, uint64_t version, const std::shared_ptr<std::iostream> &body,
                uint64_t asyncElapse = 0, WriteMode writeMode = WriteMode::NONE_L2_CACHE, uint32_t ttlSecond = 0);

    /**
     * @brief Read one exact version from this slot.
     * @param[in] key The object key.
     * @param[in] version The exact object version to read.
     * @param[out] content The returned object content.
     * @return Status of the call.
     */
    Status Get(const std::string &key, uint64_t version, std::shared_ptr<std::stringstream> &content);

    /**
     * @brief Read the latest visible version from this slot.
     * @param[in] key The object key.
     * @param[in] minVersion The lower version bound that returned version must exceed.
     * @param[out] content The returned object content.
     * @return Status of the call.
     */
    Status GetWithoutVersion(const std::string &key, uint64_t minVersion, std::shared_ptr<std::stringstream> &content);

    /**
     * @brief Append a DELETE tombstone into this slot.
     * @param[in] key The object key.
     * @param[in] maxVerToDelete The max version covered by this delete request.
     * @param[in] deleteAllVersion Whether all versions of the object should be deleted.
     * @return Status of the call.
     */
    Status Delete(const std::string &key, uint64_t maxVerToDelete, bool deleteAllVersion);

    /**
     * @brief Enumerate the current visible objects of this local slot and feed them to preload callback.
     * @param[in] callback The callback consuming visible object metadata and content.
     * @return Status of the call. Callback errors stop enumeration but do not fail the call.
     */
    Status PreloadLocal(const SlotPreloadCallback &callback);

    /**
     * @brief Replay the active index and rebuild a consistent snapshot.
     * @param[out] snapshot The rebuilt in-memory snapshot.
     * @return Status of the call.
     */
    Status ReplayIndex(SlotSnapshot &snapshot);

    /**
     * @brief Create manifest/index/data files if the slot is not initialized yet.
     * @return Status of the call.
     */
    Status BootstrapManifestIfNeed();

    /**
     * @brief Repair torn index tail bytes left by interrupted writes.
     * @return Status of the call.
     */
    Status Repair();

    /**
     * @brief Compact the slot into a new active index/data set.
     * @return Status of the call.
     */
    Status Compact();

    /**
     * @brief Flush and drain the slot before upper-layer graceful handoff.
     * This is no longer a durable state transition.
     * @param[in] sealReason Unused compatibility hint.
     * @return Status of the call.
     */
    Status Seal(const std::string &sealReason);

    /**
     * @brief Take over a source slot into this target slot.
     * Caller must ensure control-plane ownership fencing has already completed.
     * @param[in] sourceSlotPath The absolute source home slot directory path.
     * @param[in] loadToMemory Whether the caller will later preload objects into memory.
     * @return Status of the call.
     */
    Status Takeover(const std::string &sourceSlotPath, bool loadToMemory);

    /**
     * @brief Take over a source slot into this target slot with optional preload callback.
     * @param[in] sourceSlotPath The absolute source home slot directory path.
     * @param[in] request The takeover request metadata.
     * @return Status of the call.
     */
    Status Takeover(const std::string &sourceSlotPath, const SlotTakeoverRequest &request);

    /**
     * @brief Return the absolute path of this slot directory.
     * @return The slot path.
     */
    const std::string &GetSlotPath() const
    {
        return slotPath_;
    }

private:
    Status EnsureRuntimeReadyLocked();
    Status BuildRuntimeStateLocked();
    bool IsRuntimeStaleLocked() const;
    Status FlushRuntimeLocked(bool force);
    void ResetRuntimeLocked();
    Status AllocateNextDataFileIdLocked(uint32_t &fileId) const;
    Status RotateWritableDataFileLocked(uint64_t payloadSize, uint32_t &fileId);
    Status AllocateExclusiveDataFileLocked(uint32_t &fileId);
    Status GetPayloadSize(const std::shared_ptr<std::iostream> &body, uint64_t &payloadSize) const;
    Status WriteStreamToFd(const std::shared_ptr<std::iostream> &body, int fd, uint64_t startOffset,
                           uint64_t &writtenBytes) const;
    Status AppendPayloadToActiveFileLocked(const std::shared_ptr<std::iostream> &body, uint64_t payloadSize,
                                           uint64_t &offset);
    Status WriteExclusivePayloadLocked(const std::shared_ptr<std::iostream> &body, uint32_t fileId,
                                       uint64_t payloadSize) const;
    Status BuildBootstrapManifestFromDisk(SlotManifestData &manifest);
    Status RecoverManifestIfNeeded(SlotManifestData &manifest);
    Status RecoverCompactCommitting(SlotManifestData &manifest);
    Status RecoverTransfer(SlotManifestData &manifest, const SlotTakeoverRequest *request = nullptr,
                           std::vector<SlotPutRecord> *preloadPuts = nullptr);
    Status ContinueGc(SlotManifestData &manifest);
    Status DeleteFileIfExists(const std::string &relativePath, bool &deletedAnything);
    Status CleanupArtifactFiles(const std::vector<std::string> &relativePaths);
    Status CleanupStaleTakeoverArtifacts();
    bool PendingArtifactsReady(const SlotManifestData &manifest) const;
    Status LoadManifest(SlotManifestData &manifest);
    Status EnsureActiveFiles(const SlotManifestData &manifest);
    Status PersistManifest(const SlotManifestData &manifest);
    Status ReadRecordData(const SlotSnapshotValue &value, std::shared_ptr<std::stringstream> &content) const;
    Status EnsureWritable(const SlotManifestData &manifest) const;
    Status SyncActiveFiles(const SlotManifestData &manifest) const;
    Status CollectPreloadPuts(const SlotTakeoverPlan &plan, const SlotTakeoverRequest &request,
                              std::vector<SlotPutRecord> &visiblePuts) const;
    Status RunPreloadCallback(const SlotTakeoverPlan &plan, const SlotTakeoverRequest &request) const;
    Status RunPreloadCallback(const std::vector<SlotPutRecord> &visiblePuts, const SlotPreloadCallback &callback) const;
    Status ResetTransferTargetManifest(SlotManifestData &manifest);
    Status CleanupTransferArtifacts(const SlotManifestData &manifest, const SlotTakeoverPlan *plan = nullptr);
    Status LoadRecoveryTransferState(const std::string &sourceRecoveryPath, SlotManifestData &sourceManifest,
                                     SlotSnapshot &sourceSnapshot);
    Status RebuildPreparedTransferPlan(SlotManifestData &manifest, SlotTakeoverPlan &plan);
    bool AreTransferTargetDataReady(const SlotTakeoverPlan &plan) const;
    Status CheckCompactTransferPreemptionLocked() const;
    Status ReadIndexDeltaRecords(const std::string &indexPath, size_t startOffset, size_t endOffset,
                                 std::vector<SlotRecord> &records, size_t &deltaBytes) const;
    Status PublishImportBatch(const std::string &indexPath, const std::string &txnId,
                              const std::string &importIndexFile, bool &published);
    Status PersistSourceTransferManifest(const std::string &sourceRecoveryPath, const SlotManifestData &targetManifest,
                                         const std::string &sourceHomePath, SlotOperationPhase phase) const;
    Status FinalizeSourceAfterTakeover(const std::string &sourceRecoveryPath, const SlotManifestData &targetManifest);

    const uint32_t slotId_;
    const std::string slotPath_;
    const uint64_t maxDataFileBytes_;
    SlotRuntimeState runtime_;
    SlotWriter writer_;
    std::atomic<bool> transferIntentActive_{ false };
    // Protects runtime state, writer state, manifest creation, file rotation and recovery for this slot.
    std::mutex mu_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_H
