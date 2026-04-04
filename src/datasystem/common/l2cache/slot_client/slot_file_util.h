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
 * Description: Slot path helpers.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_FILE_UTIL_H
#define DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_FILE_UTIL_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {

constexpr const char *DEFAULT_CLUSTER_NAME = "default";
constexpr const char *DEFAULT_WORKER_NAME = "worker_local";

/**
 * @brief Format the slot directory name.
 * @param[in] slotId Slot identifier.
 * @return Directory name like slot_007.
 */
std::string FormatSlotDir(uint32_t slotId);

/**
 * @brief Format the data file name.
 * @param[in] fileId Monotonic data file identifier.
 * @return File name like data_00000001.bin.
 */
std::string FormatDataFileName(uint32_t fileId);

/**
 * @brief Format the compact index file name.
 * @param[in] compactEpochMs The compact build epoch in millisecond.
 * @return File name like index_compact_123.log.
 */
std::string FormatCompactIndexFileName(uint64_t compactEpochMs);

/**
 * @brief Format the takeover plan file name for one importing transaction.
 * @param[in] txnId The unique takeover transaction identifier.
 * @return File name like takeover_<txn>.plan.
 */
std::string FormatTakeoverPlanFileName(const std::string &txnId);

/**
 * @brief Format the import log file name for one importing transaction.
 * @param[in] txnId The unique takeover transaction identifier.
 * @return File name like index_import_<txn>.log.
 */
std::string FormatImportIndexFileName(const std::string &txnId);

/**
 * @brief Parse the numeric id from a data file name.
 * @param[in] filename Data file name.
 * @param[out] fileId Parsed numeric identifier.
 * @return Status of the call.
 */
Status ParseDataFileId(const std::string &filename, uint32_t &fileId);

/**
 * @brief Build the root path used by the local slot store.
 * @param[in] sfsPath Shared filesystem root.
 * @param[in] clusterName Cluster name from flags.
 * @return Absolute slot store root path.
 */
std::string BuildSlotStoreRoot(const std::string &sfsPath, const std::string &clusterName);

/**
 * @brief Build the root path used by one worker-scoped slot store namespace.
 * @param[in] sfsPath Shared filesystem root.
 * @param[in] clusterName Cluster name from flags.
 * @param[in] workerNamespace Worker namespace component used under slot_store.
 * @return Absolute slot store root path.
 */
std::string BuildSlotStoreRootForWorker(const std::string &sfsPath, const std::string &clusterName,
                                        const std::string &workerNamespace);

/**
 * @brief Convert a worker identifier into a filesystem-safe namespace component.
 * @param[in] workerIdentifier Worker identifier such as host:port.
 * @return Sanitized namespace component.
 */
std::string SanitizeSlotWorkerNamespace(const std::string &workerIdentifier);

/**
 * @brief Set the process-wide worker namespace used by slot store root building.
 * @param[in] workerNamespace Filesystem-safe worker namespace.
 */
void SetSlotWorkerNamespace(const std::string &workerNamespace);

/**
 * @brief Get the process-wide worker namespace used by slot store root building.
 * @return Current worker namespace or the default local namespace when unset.
 */
std::string GetSlotWorkerNamespace();
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_FILE_UTIL_H
