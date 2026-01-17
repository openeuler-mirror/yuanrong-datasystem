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
 * Description: The client memory ref table implementation.
 */

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_MEMORY_REF_TABLE_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_MEMORY_REF_TABLE_H

#include <shared_mutex>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
using TbbMemoryRefTable = tbb::concurrent_hash_map<ShmKey, int>;

class ClientMemoryRefTable {
public:
    ClientMemoryRefTable() = default;
    ~ClientMemoryRefTable() = default;

    /**
     * @brief Create a reference entry for the given shared unid id.
     * @param[in] shmId The shared unit id.
     * @return K_OK on success; an error code otherwise.
     */
    Status CreateRef(const ShmKey &shmId);

    /**
     * @brief Create a reference entry for the given shared unid id with accessor.
     * @param[in] shmId The shared unit id.
     * @param[out] accessor The accessor to the hash map entry.
     * @return K_OK on success; an error code otherwise.
     */
    Status CreateRef(const ShmKey &shmId, TbbMemoryRefTable::accessor &accessor);

    /**
     * @brief Delete the reference entry for the given shared unid id.
     * @param[in] shmId The shared unit id.
     */
    void DeleteRef(const ShmKey &shmId);

    /**
     * @brief Delete the reference entry using the provided accessor.
     * @param[in] accessor The accessor to the hash map entry.
     */
    void DeleteRef(TbbMemoryRefTable::accessor &accessor);

    /**
     * @brief Find the reference entry for the given shared unid id.
     * @param[in] shmId The shared unit id.
     * @param[out] accessor The accessor to the hash map entry.
     * @return K_OK on success; an error code otherwise.
     */
    Status Find(const ShmKey &shmId, TbbMemoryRefTable::accessor &accessor);

    /**
     * @brief Increase the reference count for the given shared unid id.
     * @param[in] shmId The shared unit id.
     */
    void IncreaseRef(const ShmKey &shmId);

    /**
     * @brief Decrease the reference count using the provided accessor.
     * @param[in] accessor The accessor to the hash map entry.
     * @param[out] needDecreaseWorkerRef Flag indicating if worker reference should be decreased.
     * @return K_OK on success; an error code otherwise.
     */
    Status DecreaseRef(TbbMemoryRefTable::accessor &accessor, bool &needDecreaseWorkerRef);

    /**
     * @brief Set the flag indicating support for multi-SHM reference counting.
     * @param[in] value The boolean value to set.
     */
    void SetSupportMultiShmRefCount(bool value);

    /**
     * @brief Clear all entries in the reference table.
     */
    void Clear();

private:
    mutable std::shared_mutex mutex_;
    TbbMemoryRefTable table_;
    std::atomic<bool> workerSupportMultiShmRefCount_{ true };
};
}  // namespace object_cache
}  // namespace datasystem

#endif
