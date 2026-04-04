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

#include "datasystem/common/l2cache/slot_client/slot_client.h"

#include <functional>
#include <shared_mutex>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"

DS_DEFINE_uint32(distributed_disk_slot_num, 128, "The number of slot partitions used by distributed disk.");
DS_DEFINE_uint32(distributed_disk_max_data_file_size_mb, 1024, "The max size in MB of a single slot data file.");
DS_DEFINE_validator(distributed_disk_slot_num, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});
DS_DEFINE_validator(distributed_disk_max_data_file_size_mb, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});
DS_DECLARE_string(cluster_name);

namespace datasystem {

SlotClient::SlotClient(const std::string &sfsPath) : sfsPath_(sfsPath)
{
}

SlotClient::~SlotClient() = default;

Status SlotClient::Init()
{
    rootPath_ = BuildSlotStoreRoot(sfsPath_, FLAGS_cluster_name);
    slotNum_ = FLAGS_distributed_disk_slot_num;
    maxDataFileBytes_ = static_cast<uint64_t>(FLAGS_distributed_disk_max_data_file_size_mb) * 1024ul * 1024ul;
    RETURN_IF_NOT_OK(CreateDir(rootPath_, true));
    return Status::OK();
}

Status SlotClient::Save(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                        const std::shared_ptr<std::iostream> &body, uint64_t asyncElapse, WriteMode writeMode)
{
    (void)timeoutMs;
    return GetSlot(GetSlotId(objectKey)).Save(objectKey, version, body, asyncElapse, writeMode);
}

Status SlotClient::Get(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                       std::shared_ptr<std::stringstream> &content)
{
    (void)timeoutMs;
    return GetSlot(GetSlotId(objectKey)).Get(objectKey, version, content);
}

Status SlotClient::GetWithoutVersion(const std::string &objectKey, int64_t timeoutMs, uint64_t minVersion,
                                     std::shared_ptr<std::stringstream> &content)
{
    (void)timeoutMs;
    return GetSlot(GetSlotId(objectKey)).GetWithoutVersion(objectKey, minVersion, content);
}

Status SlotClient::Delete(const std::string &objectKey, uint64_t maxVerToDelete, bool deleteAllVersion,
                          uint64_t asyncElapse)
{
    (void)asyncElapse;
    return GetSlot(GetSlotId(objectKey)).Delete(objectKey, maxVerToDelete, deleteAllVersion);
}

Status SlotClient::RepairSlot(uint32_t slotId)
{
    return GetSlot(slotId).Repair();
}

Status SlotClient::CompactSlot(uint32_t slotId)
{
    return GetSlot(slotId).Compact();
}

Status SlotClient::MergeSlot(const std::string &sourceWorkerAddress, uint32_t slotId)
{
    return GetSlot(slotId).Takeover(GetSlotPathForWorker(sourceWorkerAddress, slotId), false);
}

Status SlotClient::PreloadSlot(const std::string &sourceWorkerAddress, uint32_t slotId,
                               const SlotPreloadCallback &callback)
{
    if (SanitizeSlotWorkerNamespace(sourceWorkerAddress) == GetSlotWorkerNamespace()) {
        return GetSlot(slotId).PreloadLocal(callback);
    }
    SlotTakeoverRequest request;
    request.mode = SlotTakeoverMode::PRELOAD;
    request.callback = callback;
    return GetSlot(slotId).Takeover(GetSlotPathForWorker(sourceWorkerAddress, slotId), request);
}

std::string SlotClient::GetRequestSuccessRate() const
{
    return "";
}

uint32_t SlotClient::GetSlotId(const std::string &objectKey) const
{
    return static_cast<uint32_t>(std::hash<std::string>{}(objectKey) % slotNum_);
}

std::string SlotClient::GetSlotPath(uint32_t slotId) const
{
    return JoinPath(rootPath_, FormatSlotDir(slotId));
}

std::string SlotClient::GetSlotPathForWorker(const std::string &workerAddress, uint32_t slotId) const
{
    auto workerRoot =
        BuildSlotStoreRootForWorker(sfsPath_, FLAGS_cluster_name, SanitizeSlotWorkerNamespace(workerAddress));
    return JoinPath(workerRoot, FormatSlotDir(slotId));
}

Slot &SlotClient::GetSlot(uint32_t slotId)
{
    {
        std::shared_lock<std::shared_mutex> readLock(mu_);
        auto it = slots_.find(slotId);
        if (it != slots_.end()) {
            return *it->second;
        }
    }

    std::unique_lock<std::shared_mutex> writeLock(mu_);
    auto it = slots_.find(slotId);
    if (it != slots_.end()) {
        return *it->second;
    }
    auto slot = std::make_unique<Slot>(slotId, GetSlotPath(slotId), maxDataFileBytes_);
    auto &slotRef = *slot;
    slots_.emplace(slotId, std::move(slot));
    return slotRef;
}
}  // namespace datasystem
