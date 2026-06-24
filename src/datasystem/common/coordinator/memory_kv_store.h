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
 * Description: In-memory KV store with CAS, range query, and watch support.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_MEMORY_KV_STORE_H
#define DATASYSTEM_COMMON_COORDINATOR_MEMORY_KV_STORE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>

#include <shared_mutex>
#include <string>
#include <vector>

#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/coordinator/watch_event.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class MemoryKvStore {
public:
    MemoryKvStore() = default;
    ~MemoryKvStore() = default;

    /**
     * @brief Put a key-value pair with optional TTL metadata.
     * @param[in] key The key.
     * @param[in] value The value.
     * @param[in] ttlMs TTL in milliseconds. 0 means no expiration.
     * @param[in] expectedVersion Expected version for CAS. 0 means no check.
     * @param[out] version The new version after put.
     * @param[out] revision The new global revision after put.
     * @param[out] ttlGeneration TTL generation after this mutation.
     * @return Status of the operation. K_INVALID if version mismatch.
     */
    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision, uint64_t &ttlGeneration);

    /**
     * @brief Range query over [key, rangeEnd).
     * @param[in] key Start key (inclusive).
     * @param[in] rangeEnd End key (exclusive). Empty for single key lookup.
     * @param[out] kvs Output key-value entries.
     * @param[out] revision Snapshot revision corresponding to the returned entries.
     */
    void Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs, int64_t &revision);

    /**
     * @brief Delete keys in range [key, rangeEnd).
     * @param[in] key Start key (inclusive).
     * @param[in] rangeEnd End key (exclusive). Empty for single key delete.
     * @param[out] deleted Number of deleted keys.
     * @param[out] revision The new global revision.
     * @param[out] deletedEntries The deleted entries for watch notification.
     */
    void Delete(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                std::vector<KeyValueEntry> &deletedEntries);

    /**
     * @brief Delete a key only when its current modification revision and TTL generation match.
     */
    bool DeleteIfTtlExpired(const std::string &key, int64_t revision, uint64_t ttlGeneration);

    /**
     * @brief Renew TTL for a key.
     */
    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs, int64_t &revision,
                     uint64_t &ttlGeneration);

    /**
     * @brief Get current global revision.
     */
    int64_t CurrentRevision() const;

    /**
     * @brief Set mutation callback invoked after each Put/Delete. Only configured during coordinator startup.
     * @param[in] callback Function receiving the watch event.
     */
    void SetMutationCallback(std::function<void(std::shared_ptr<WatchEvent>)> callback);

private:
    struct ValueEntry {
        std::string value;
        int64_t version = 0;
        int64_t createRevision = 0;
        int64_t modRevision = 0;
        int64_t ttlMs = 0;
        uint64_t ttlGeneration = 0;
    };

    mutable std::shared_mutex mutex_;
    std::map<std::string, ValueEntry> data_;
    std::function<void(std::shared_ptr<WatchEvent>)> mutationCallback_;
    std::atomic<int64_t> revision_{ 1 };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_MEMORY_KV_STORE_H
