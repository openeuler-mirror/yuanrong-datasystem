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

#include <algorithm>
#include <chrono>
#include <functional>
#include <set>
#include <shared_mutex>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_uint32(distributed_disk_slot_num, 128, "The number of slot partitions used by distributed disk.");
DS_DEFINE_uint32(distributed_disk_max_data_file_size_mb, 1024,
                 "The target max size in MB of a rolling slot data file. Large objects may use a dedicated file.");
DS_DEFINE_validator(distributed_disk_slot_num, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});
DS_DEFINE_validator(distributed_disk_max_data_file_size_mb, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});
DS_DECLARE_string(cluster_name);

constexpr uint32_t DEFAULT_DISTRIBUTED_DISK_COMPACT_INTERVAL_S = 3600;
#ifdef WITH_TESTS
constexpr uint32_t MIN_DISTRIBUTED_DISK_COMPACT_INTERVAL_S = 1;
#else
constexpr uint32_t MIN_DISTRIBUTED_DISK_COMPACT_INTERVAL_S = 60;
#endif

DS_DEFINE_uint32(distributed_disk_compact_interval_s, DEFAULT_DISTRIBUTED_DISK_COMPACT_INTERVAL_S,
                 "Fixed interval in seconds between distributed disk background compact runs.");
DS_DEFINE_validator(distributed_disk_compact_interval_s, [](const char *flagName, uint32_t value) {
    if (value < MIN_DISTRIBUTED_DISK_COMPACT_INTERVAL_S) {
        LOG(ERROR) << FormatString("The value of %s flag is %u, which must be greater than or equal to %u.",
                                   flagName, value, MIN_DISTRIBUTED_DISK_COMPACT_INTERVAL_S);
        return false;
    }
    return true;
});

namespace datasystem {
namespace {
bool ParseSlotIdFromPath(const std::string &path, uint32_t &slotId)
{
    auto pos = path.find_last_of('/');
    auto name = pos == std::string::npos ? path : path.substr(pos + 1);
    constexpr char prefix[] = "slot_";
    if (name.rfind(prefix, 0) != 0) {
        return false;
    }
    const auto idStr = name.substr(sizeof(prefix) - 1);
    if (idStr.empty() || !std::all_of(idStr.begin(), idStr.end(), [](char ch) { return ch >= '0' && ch <= '9'; })) {
        return false;
    }
    try {
        slotId = static_cast<uint32_t>(std::stoul(idStr));
    } catch (const std::exception &) {
        return false;
    }
    return true;
}
}  // namespace

SlotClient::SlotClient(const std::string &sfsPath) : sfsPath_(sfsPath)
{
}

SlotClient::~SlotClient()
{
    StopBackgroundCompactThread();
}

Status SlotClient::Init()
{
    CHECK_FAIL_RETURN_STATUS(Validator::ValidatePathString("distributed_disk_path", sfsPath_), StatusCode::K_INVALID,
                             "distributed_disk_path is invalid when l2_cache_type=distributed_disk");
    CHECK_FAIL_RETURN_STATUS(!sfsPath_.empty(), StatusCode::K_INVALID,
                             "distributed_disk_path must not be empty when l2_cache_type=distributed_disk");
    rootPath_ = BuildSlotStoreRoot(sfsPath_, FLAGS_cluster_name);
    slotNum_ = FLAGS_distributed_disk_slot_num;
    maxDataFileBytes_ = static_cast<uint64_t>(FLAGS_distributed_disk_max_data_file_size_mb) * 1024ul * 1024ul;
    VLOG(1) << "Initializing slot client, sfsPath=" << sfsPath_ << ", rootPath=" << rootPath_
            << ", slotNum=" << slotNum_ << ", maxDataFileBytes=" << maxDataFileBytes_;
    RETURN_IF_NOT_OK(CreateDir(rootPath_, true));
    StartBackgroundCompactThread();
    VLOG(1) << "Initialized slot client successfully, rootPath=" << rootPath_;
    return Status::OK();
}

Status SlotClient::Save(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                        const std::shared_ptr<std::iostream> &body, uint64_t asyncElapse, WriteMode writeMode)
{
    (void)timeoutMs;
    auto rc = GetSlot(GetSlotId(objectKey)).Save(objectKey, version, body, asyncElapse, writeMode);
    if (rc.IsOk()) {
        WakeBackgroundCompactThread();
    }
    return rc;
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
    auto rc = GetSlot(GetSlotId(objectKey)).Delete(objectKey, maxVerToDelete, deleteAllVersion);
    if (rc.IsOk()) {
        WakeBackgroundCompactThread();
    }
    return rc;
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

void SlotClient::StartBackgroundCompactThread()
{
    std::lock_guard<std::mutex> lock(compactMu_);
    if (compactThread_.joinable()) {
        return;
    }
    stopCompactThread_ = false;
    compactThread_ = std::thread(&SlotClient::BackgroundCompactLoop, this);
}

void SlotClient::StopBackgroundCompactThread()
{
    {
        std::lock_guard<std::mutex> lock(compactMu_);
        stopCompactThread_ = true;
    }
    compactCv_.notify_all();
    if (compactThread_.joinable()) {
        compactThread_.join();
    }
}

void SlotClient::WakeBackgroundCompactThread()
{
    {
        std::lock_guard<std::mutex> lock(compactMu_);
        ++compactWakeupSeq_;
    }
    compactCv_.notify_all();
}

bool SlotClient::ShouldStopBackgroundCompactThread()
{
    std::lock_guard<std::mutex> lock(compactMu_);
    return stopCompactThread_;
}

void SlotClient::BackgroundCompactLoop()
{
    auto nextCompactDeadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(ComputeNextCompactDelayMs());
    while (!ShouldStopBackgroundCompactThread()) {
        auto waitMs = std::max<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                            nextCompactDeadline - std::chrono::steady_clock::now())
                                            .count(),
                                        0);
        INJECT_POINT_NO_RETURN("slotstore.SlotClient.BackgroundCompact.WaitMs",
                               [&waitMs](int64_t overrideWaitMs) { waitMs = std::max<int64_t>(overrideWaitMs, 0); });
        {
            std::unique_lock<std::mutex> lock(compactMu_);
            const auto observedWakeupSeq = compactWakeupSeq_;
            if (compactCv_.wait_for(lock, std::chrono::milliseconds(waitMs), [this, observedWakeupSeq]() {
                    return stopCompactThread_ || compactWakeupSeq_ != observedWakeupSeq;
                })) {
                if (!stopCompactThread_) {
                    continue;
                }
                return;
            }
        }

        auto slotIds = CollectCompactionCandidates();
        for (const auto slotId : slotIds) {
            auto rc = CompactSlot(slotId);
            if (rc.IsOk()) {
                LOG(INFO) << FormatString("action=background_compact_success slot_id=%u root=%s", slotId, rootPath_);
                continue;
            }
            if (rc.GetCode() == StatusCode::K_TRY_AGAIN || rc.GetCode() == StatusCode::K_NOT_FOUND) {
                LOG(INFO) << FormatString("action=background_compact_skip slot_id=%u status=%s", slotId, rc.ToString());
                continue;
            }
            LOG(WARNING) << FormatString("action=background_compact_failed slot_id=%u status=%s", slotId,
                                         rc.ToString());
        }
        nextCompactDeadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(ComputeNextCompactDelayMs());
    }
}

int64_t SlotClient::ComputeNextCompactDelayMs() const
{
    constexpr int64_t s2ms = 1000;
    return static_cast<int64_t>(FLAGS_distributed_disk_compact_interval_s) * s2ms;
}

std::vector<uint32_t> SlotClient::CollectCompactionCandidates() const
{
    std::set<uint32_t> slotIds;
    {
        std::shared_lock<std::shared_mutex> readLock(mu_);
        for (const auto &entry : slots_) {
            slotIds.insert(entry.first);
        }
    }

    std::vector<std::string> slotPaths;
    auto rc = Glob(JoinPath(rootPath_, "slot_*"), slotPaths);
    if (rc.IsError() && rc.GetCode() != StatusCode::K_NOT_FOUND) {
        LOG(WARNING) << FormatString("action=background_compact_glob_failed root=%s status=%s", rootPath_,
                                     rc.ToString());
        return std::vector<uint32_t>(slotIds.begin(), slotIds.end());
    }
    for (const auto &slotPath : slotPaths) {
        uint32_t slotId = 0;
        if (ParseSlotIdFromPath(slotPath, slotId)) {
            slotIds.insert(slotId);
        }
    }
    VLOG(1) << "Collected slot compact candidates, rootPath=" << rootPath_
            << ", candidateCount=" << slotIds.size();
    return std::vector<uint32_t>(slotIds.begin(), slotIds.end());
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
    VLOG(1) << "Creating slot instance, slotId=" << slotId << ", slotPath=" << GetSlotPath(slotId)
            << ", maxDataFileBytes=" << maxDataFileBytes_;
    auto &slotRef = *slot;
    slots_.emplace(slotId, std::move(slot));
    return slotRef;
}
}  // namespace datasystem
