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
 * Description: Coordinator store facade composing memory store, watch, and TTL.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_STORE_H
#define DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_STORE_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class CoordinatorStore {
public:
    using CommittedMutationObserver = std::function<void(WatchEvent::Type type, const std::string &key)>;

    CoordinatorStore() = default;
    CoordinatorStore(std::shared_ptr<MemoryKvStore> memKvStore, std::shared_ptr<WatchRegistry> watchRegistry,
                     std::shared_ptr<WatchDispatcher> watchDispatcher, std::shared_ptr<TtlManager> ttlManager);
    ~CoordinatorStore();

    /**
     * @brief Put a key-value pair.
     * @param[in] key The key.
     * @param[in] value The value.
     * @param[in] ttlMs TTL in milliseconds. 0 means no expiration.
     * @param[in] expectedVersion Expected version for CAS. COORDINATOR_NO_VERSION_CHECK means no check.
     * COORDINATOR_KEY_NOT_EXISTS_VERSION means the key must not exist.
     * @param[out] version The new version after put.
     * @param[out] revision The new global revision.
     * @return Status of the operation.
     */
    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision);

    /**
     * @brief Range query over [key, rangeEnd).
     * @param[in] key Start key (inclusive).
     * @param[in] rangeEnd End key (exclusive). Empty for single key.
     * @param[out] kvs Output key-value entries.
     * @param[out] revision Snapshot revision corresponding to the returned entries.
     * @return Status of the operation.
     */
    Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                 int64_t &revision);

    /**
     * @brief Delete keys in range [key, rangeEnd).
     * @param[in] key Start key (inclusive).
     * @param[in] rangeEnd End key (exclusive). Empty for single key.
     * @param[out] deleted Number of deleted keys.
     * @param[out] revision The new global revision.
     * @return Status of the operation.
     */
    Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision);

    /**
     * @brief Watch a key or range for changes.
     * @param[in] key Start key.
     * @param[in] rangeEnd End key (exclusive). Empty for single key.
     * @param[in] watcherAddr Address for notifications.
     * @param[in] registrationId Stable token for an ambiguous registration retry.
     * @param[out] watchId Assigned watch ID.
     * @param[out] initialKvs Current snapshot of the watched range.
     * @return Status of the operation.
     */
    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      const std::string &registrationId, int64_t &watchId,
                      std::vector<KeyValueEntry> &initialKvs);

    /**
     * @brief Cancel watches for a watcher address.
     * @param[in] watcherAddr Address that owns the watches.
     * @param[in] watchIds Watch IDs to cancel.
     * @return Status of the operation.
     */
    Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds);

    /**
     * @brief Renew TTL for a key.
     * @param[in] key The key.
     * @param[out] ttlMs Original TTL.
     * @param[out] remainingTtlMs Remaining TTL.
     * @return Status of the operation.
     */
    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs);

    /**
     * @brief Install an observer invoked after committed Store mutations.
     * @param[in] observer Observer receiving only mutation type and key; empty detaches it.
     */
    void SetCommittedMutationObserver(CommittedMutationObserver observer);

    /**
     * @brief Stop and join TTL expiry before observer detachment.
     */
    void StopTtl();

    /**
     * @brief Idempotently stop TTL and watch dispatch.
     */
    void Shutdown();

private:
    /**
     * @brief Bind callbacks between memory store, watch dispatcher, and TTL manager.
     */
    void BindCallbacks();

    /**
     * @brief Check whether all components are initialized.
     * @return Status::OK if initialized.
     */
    Status CheckInitialized() const;

    std::shared_ptr<MemoryKvStore> memKvStore_;
    std::shared_ptr<WatchRegistry> watchRegistry_;
    std::shared_ptr<WatchDispatcher> watchDispatcher_;
    std::shared_ptr<TtlManager> ttlManager_;
    // Bound and detached only while mutation producers are stopped.
    CommittedMutationObserver committedMutationObserver_;
    bool shutdown_{ false };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_STORE_H
