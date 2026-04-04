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

#include "datasystem/common/l2cache/slot_client/slot_file_util.h"

#include <mutex>
#include <sstream>

#include "datasystem/common/util/file_util.h"

namespace datasystem {
namespace {
std::mutex g_slotWorkerNamespaceMutex;
std::string g_slotWorkerNamespace;
}  // namespace

std::string FormatSlotDir(uint32_t slotId)
{
    std::ostringstream ss;
    ss.fill('0');
    constexpr int width = 4;
    ss.width(width);
    ss << slotId;
    return "slot_" + ss.str();
}

std::string FormatDataFileName(uint32_t fileId)
{
    std::ostringstream ss;
    ss.fill('0');
    constexpr int width = 8;
    ss.width(width);
    ss << fileId;
    return "data_" + ss.str() + ".bin";
}

std::string FormatCompactIndexFileName(uint64_t compactEpochMs)
{
    return "index_compact_" + std::to_string(compactEpochMs) + ".log";
}

std::string FormatTakeoverPlanFileName(const std::string &txnId)
{
    return "takeover_" + txnId + ".plan";
}

std::string FormatImportIndexFileName(const std::string &txnId)
{
    return "index_import_" + txnId + ".log";
}

Status ParseDataFileId(const std::string &filename, uint32_t &fileId)
{
    constexpr size_t prefixLen = 5;  // data_
    constexpr size_t suffixLen = 4;  // .bin
    if (filename.size() <= prefixLen + suffixLen || filename.rfind("data_", 0) != 0
        || filename.substr(filename.size() - suffixLen) != ".bin") {
        RETURN_STATUS(StatusCode::K_INVALID, "Invalid data filename: " + filename);
    }
    auto idStr = filename.substr(prefixLen, filename.size() - prefixLen - suffixLen);
    try {
        fileId = static_cast<uint32_t>(std::stoul(idStr));
    } catch (const std::exception &e) {
        RETURN_STATUS(StatusCode::K_INVALID, std::string("Invalid data filename: ") + filename + ", " + e.what());
    }
    return Status::OK();
}

std::string BuildSlotStoreRoot(const std::string &sfsPath, const std::string &clusterName)
{
    return BuildSlotStoreRootForWorker(sfsPath, clusterName, GetSlotWorkerNamespace());
}

std::string BuildSlotStoreRootForWorker(const std::string &sfsPath, const std::string &clusterName,
                                        const std::string &workerNamespace)
{
    const std::string effectiveCluster = clusterName.empty() ? DEFAULT_CLUSTER_NAME : clusterName;
    const std::string effectiveWorker = workerNamespace.empty() ? DEFAULT_WORKER_NAME : workerNamespace;
    auto root = JoinPath(sfsPath, "datasystem");
    root = JoinPath(root, effectiveCluster);
    root = JoinPath(root, "slot_store");
    root = JoinPath(root, effectiveWorker);
    return root;
}

std::string SanitizeSlotWorkerNamespace(const std::string &workerIdentifier)
{
    if (workerIdentifier.empty()) {
        return DEFAULT_WORKER_NAME;
    }
    std::string sanitized = workerIdentifier;
    for (auto &ch : sanitized) {
        const bool isAlphaNum = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
        if (!isAlphaNum && ch != '-' && ch != '_' && ch != '.') {
            ch = '_';
        }
    }
    return sanitized.empty() ? DEFAULT_WORKER_NAME : sanitized;
}

void SetSlotWorkerNamespace(const std::string &workerNamespace)
{
    std::lock_guard<std::mutex> lock(g_slotWorkerNamespaceMutex);
    g_slotWorkerNamespace = workerNamespace;
}

std::string GetSlotWorkerNamespace()
{
    std::lock_guard<std::mutex> lock(g_slotWorkerNamespaceMutex);
    return g_slotWorkerNamespace.empty() ? DEFAULT_WORKER_NAME : g_slotWorkerNamespace;
}
}  // namespace datasystem
