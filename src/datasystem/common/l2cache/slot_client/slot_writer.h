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
 * Description: Slot active writer and group commit helper.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_WRITER_H
#define DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_WRITER_H

#include <cstdint>
#include <string>

#include "datasystem/common/l2cache/slot_client/slot_manifest.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Keep active slot index/data files open and flush them in batch.
 */
class SlotWriter {
public:
    SlotWriter() = default;
    ~SlotWriter();

    /**
     * @brief Open the current active slot files and reset buffered write tracking.
     * @param[in] slotPath The slot directory path.
     * @param[in] manifest The manifest that defines the active file set.
     * @return Status of the call.
     */
    Status Init(const std::string &slotPath, const SlotManifestData &manifest);

    /**
     * @brief Close all active file descriptors and clear buffered state.
     */
    void Close();

    /**
     * @brief Append one payload to the active data file.
     * @param[in] payload The object payload bytes.
     * @param[out] offset The written offset inside the active data file.
     * @return Status of the call.
     */
    Status AppendData(const std::string &payload, uint64_t &offset);

    /**
     * @brief Append one payload buffer to the active data file.
     * @param[in] buffer The object payload bytes.
     * @param[in] len The payload length.
     * @param[out] offset The written offset inside the active data file.
     * @return Status of the call.
     */
    Status AppendData(const char *buffer, size_t len, uint64_t &offset);

    /**
     * @brief Append one pre-encoded index payload to the active index file.
     * @param[in] payload The encoded index bytes to append.
     * @return Status of the call.
     */
    Status AppendIndexPayload(const std::string &payload);

    /**
     * @brief Mark one logical write operation as pending for group commit.
     * @param[in] bufferedBytes The bytes newly buffered by this operation.
     */
    void RecordOperation(uint64_t bufferedBytes);

    /**
     * @brief Flush active file descriptors if there are pending buffered writes.
     * @return Status of the call.
     */
    Status Flush();

    /**
     * @brief Check whether current buffered state should trigger a group commit.
     * @param[in] syncIntervalMs The max buffered interval in millisecond.
     * @param[in] batchBytes The max buffered bytes threshold.
     * @return True if the caller should flush now.
     */
    bool ShouldFlush(uint64_t syncIntervalMs, uint64_t batchBytes) const;

    /**
     * @brief Check whether there are active buffered writes waiting to flush.
     * @return True if there are unflushed operations.
     */
    bool HasPendingWrites() const;

    /**
     * @brief Check whether any active writer file descriptor has been opened.
     * @return True if the writer has been initialized.
     */
    bool IsInitialized() const;

    /**
     * @brief Return the current active data file id.
     * @return The current active data file id.
     */
    uint32_t GetActiveDataFileId() const;

    /**
     * @brief Return the current active data file size in bytes.
     * @return The current active data file size.
     */
    uint64_t GetActiveDataSize() const;

    /**
     * @brief Return the current active index file size in bytes.
     * @return The current active index file size.
     */
    uint64_t GetActiveIndexSize() const;

    /**
     * @brief Return the number of logical operations buffered since the last flush.
     * @return The buffered logical op count.
     */
    uint64_t GetPendingOps() const;

private:
    uint64_t NowMonotonicMs() const;

    int activeDataFd_{ -1 };
    int activeIndexFd_{ -1 };
    uint32_t activeDataFileId_{ 0 };
    uint64_t activeDataSize_{ 0 };
    uint64_t activeIndexSize_{ 0 };
    uint64_t bufferedBytes_{ 0 };
    uint64_t pendingOps_{ 0 };
    uint64_t lastFlushMs_{ 0 };
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_WRITER_H
