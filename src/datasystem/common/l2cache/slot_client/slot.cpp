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

#include "datasystem/common/l2cache/slot_client/slot.h"

#include <algorithm>
#include <chrono>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/l2cache/slot_client/slot_compactor.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot_internal_config.h"
#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/l2cache/slot_client/slot_takeover_planner.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

constexpr uint32_t DEFAULT_SLOT_SYNC_INTERVAL_MS = 1000;
constexpr uint64_t DEFAULT_SLOT_SYNC_BATCH_BYTES = 32UL * 1024UL * 1024UL;

DS_DEFINE_uint32(distributed_disk_sync_interval_ms, DEFAULT_SLOT_SYNC_INTERVAL_MS,
                 "Max buffered write interval in millisecond before one slot group commit.");
DS_DEFINE_uint64(distributed_disk_sync_batch_bytes, DEFAULT_SLOT_SYNC_BATCH_BYTES,
                 "Max buffered write bytes before one slot group commit.");
DS_DEFINE_validator(distributed_disk_sync_interval_ms, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value <= 60000;
});
DS_DEFINE_validator(distributed_disk_sync_batch_bytes, [](const char *flagName, uint64_t value) {
    (void)flagName;
    return value > 0;
});

namespace datasystem {
namespace {
constexpr size_t SLOT_IO_CHUNK_BYTES = 4UL * 1024UL * 1024UL;

std::string BaseName(const std::string &path)
{
    auto pos = path.find_last_of('/');
    return pos == std::string::npos ? path : path.substr(pos + 1);
}

bool IsCompactIndexFile(const std::string &filename)
{
    constexpr char prefix[] = "index_compact_";
    constexpr char suffix[] = ".log";
    return filename.rfind(prefix, 0) == 0 && filename.size() > sizeof(prefix) + sizeof(suffix) - 2
           && filename.substr(filename.size() - (sizeof(suffix) - 1)) == suffix;
}

bool IsImportIndexFile(const std::string &filename)
{
    constexpr char prefix[] = "index_import_";
    constexpr char suffix[] = ".log";
    return filename.rfind(prefix, 0) == 0 && filename.size() > sizeof(prefix) + sizeof(suffix) - 2
           && filename.substr(filename.size() - (sizeof(suffix) - 1)) == suffix;
}

bool ShouldUseAsBootstrapIndex(const std::string &candidateName, size_t candidateValidBytes,
                               const std::string &bestName, size_t bestValidBytes)
{
    if (bestName.empty()) {
        return true;
    }
    if (candidateValidBytes != bestValidBytes) {
        return candidateValidBytes > bestValidBytes;
    }
    auto candidateIsCompact = IsCompactIndexFile(candidateName);
    auto bestIsCompact = IsCompactIndexFile(bestName);
    if (candidateIsCompact != bestIsCompact) {
        return candidateIsCompact;
    }
    return candidateName > bestName;
}

std::string GenerateTxnId(uint32_t slotId)
{
    const auto nowNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch());
    return std::to_string(slotId) + "_" + std::to_string(nowNs.count());
}

std::string BuildRecoverySlotPath(const std::string &sourceSlotPath, const std::string &txnId)
{
    return sourceSlotPath + ".recovery." + txnId;
}

bool IsNormalManifest(const SlotManifestData &manifest)
{
    return manifest.state == SlotState::NORMAL && manifest.opType == SlotOperationType::NONE
           && manifest.opPhase == SlotOperationPhase::NONE && manifest.role == SlotOperationRole::NONE;
}

bool IsTransferManifest(const SlotManifestData &manifest)
{
    return manifest.state == SlotState::IN_OPERATION && manifest.opType == SlotOperationType::TRANSFER;
}

std::string ManifestDebugString(const SlotManifestData &manifest)
{
    return FormatString("state=%s, opType=%s, opPhase=%s, role=%s, txnId=%s, activeIndex=%s, activeDataCount=%zu, "
                        "pendingIndex=%s, pendingDataCount=%zu, obsoleteIndex=%s, obsoleteDataCount=%zu, gcPending=%d",
                        ToString(manifest.state), ToString(manifest.opType), ToString(manifest.opPhase),
                        ToString(manifest.role), manifest.txnId, manifest.activeIndex, manifest.activeData.size(),
                        manifest.pendingIndex, manifest.pendingData.size(), manifest.obsoleteIndex,
                        manifest.obsoleteData.size(), manifest.gcPending);
}

std::string EncodeTransferFileMap(const SlotTakeoverPlan &plan)
{
    std::ostringstream ss;
    for (size_t i = 0; i < plan.dataMappings.size(); ++i) {
        if (i > 0) {
            ss << ",";
        }
        ss << plan.dataMappings[i].sourceFileId << ":" << plan.dataMappings[i].targetFileId;
    }
    return ss.str();
}

Status DecodeTransferFileMap(const std::string &encoded, std::unordered_map<uint32_t, uint32_t> &mapping)
{
    mapping.clear();
    if (encoded.empty()) {
        return Status::OK();
    }
    for (const auto &item : Split(encoded, ",")) {
        auto fields = Split(item, ":");
        CHECK_FAIL_RETURN_STATUS(fields.size() == 2, StatusCode::K_INVALID, "Invalid transfer file map entry: " + item);
        try {
            auto sourceFileId = static_cast<uint32_t>(std::stoul(fields[0]));
            auto targetFileId = static_cast<uint32_t>(std::stoul(fields[1]));
            mapping[sourceFileId] = targetFileId;
        } catch (const std::exception &e) {
            RETURN_STATUS(StatusCode::K_INVALID, std::string("Invalid transfer file map entry: ") + e.what());
        }
    }
    return Status::OK();
}

Status RenameDataFileIfNeeded(const std::string &sourcePath, const std::string &targetPath)
{
    if (FileExist(targetPath)) {
        if (FileExist(sourcePath)) {
            RETURN_IF_NOT_OK(DeleteFile(sourcePath));
        }
        return Status::OK();
    }
    if (!FileExist(sourcePath)) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Source data file not found during takeover: " + sourcePath);
    }
    RETURN_IF_NOT_OK(RenameFile(sourcePath, targetPath));
    return Status::OK();
}

bool HasClosedImportBatch(const std::vector<SlotRecord> &records, const std::string &txnId)
{
    bool importBatchOpen = false;
    for (const auto &record : records) {
        if (record.type == SlotRecordType::IMPORT_BEGIN && record.import.txnId == txnId) {
            importBatchOpen = true;
            continue;
        }
        if (record.type == SlotRecordType::IMPORT_END && record.import.txnId == txnId && importBatchOpen) {
            return true;
        }
    }
    return false;
}

uint32_t GetCurrentSlotSyncIntervalMs()
{
    return FLAGS_distributed_disk_sync_interval_ms;
}

uint64_t GetCurrentSlotSyncBatchBytes()
{
    return FLAGS_distributed_disk_sync_batch_bytes;
}

uint64_t GetCurrentSlotCompactCutoverBytes()
{
    return DISTRIBUTED_DISK_COMPACT_CUTOVER_BYTES;
}

uint32_t GetCurrentSlotCompactCutoverRecords()
{
    return DISTRIBUTED_DISK_COMPACT_CUTOVER_RECORDS;
}
}  // namespace

Slot::Slot(uint32_t slotId, std::string slotPath, uint64_t maxDataFileBytes)
    : slotId_(slotId), slotPath_(std::move(slotPath)), maxDataFileBytes_(maxDataFileBytes)
{
    runtime_.Reset();
}

Status Slot::EnsureRuntimeReadyLocked()
{
    if (runtime_.initialized && !IsRuntimeStaleLocked()) {
        return Status::OK();
    }
    ResetRuntimeLocked();
    RETURN_IF_NOT_OK(BuildRuntimeStateLocked());
    return Status::OK();
}

Status Slot::BuildRuntimeStateLocked()
{
    VLOG(1) << "Building slot runtime state, slotId=" << slotId_ << ", slotPath=" << slotPath_;
    RETURN_IF_NOT_OK(BootstrapManifestIfNeed());
    SlotManifestData manifest;
    RETURN_IF_NOT_OK(LoadManifest(manifest));
    RETURN_IF_NOT_OK(RecoverManifestIfNeeded(manifest));
    SlotSnapshot snapshot;
    RETURN_IF_NOT_OK(SlotSnapshot::Replay(slotPath_, manifest, snapshot));
    runtime_.manifest = manifest;
    runtime_.snapshot = snapshot;
    auto manifestPath = JoinPath(slotPath_, "manifest");
    RETURN_IF_NOT_OK(GetFileModifiedTime(manifestPath, runtime_.manifestModifiedTimeUs));
    runtime_.initialized = true;
    RETURN_IF_NOT_OK(writer_.Init(slotPath_, manifest));
    VLOG(1) << "Built slot runtime state successfully, slotId=" << slotId_ << ", slotPath=" << slotPath_
            << ", " << ManifestDebugString(manifest);
    return Status::OK();
}

bool Slot::IsRuntimeStaleLocked() const
{
    if (!runtime_.initialized) {
        return true;
    }
    int64_t currentManifestMtimeUs = 0;
    auto rc = GetFileModifiedTime(JoinPath(slotPath_, "manifest"), currentManifestMtimeUs);
    if (rc.IsError()) {
        return true;
    }
    return currentManifestMtimeUs != runtime_.manifestModifiedTimeUs;
}

Status Slot::FlushRuntimeLocked(bool force)
{
    if (!writer_.IsInitialized()) {
        return Status::OK();
    }
    if (!force && !writer_.ShouldFlush(GetCurrentSlotSyncIntervalMs(), GetCurrentSlotSyncBatchBytes())) {
        return Status::OK();
    }
    auto pendingOps = writer_.GetPendingOps();
    RETURN_IF_NOT_OK(writer_.Flush());
    INJECT_POINT("slotstore.SlotWriter.Flush.After", []() { return Status::OK(); });
    if (pendingOps > 0) {
        VLOG(1) << "slot flush finished, slotId=" << slotId_ << ", pendingOps=" << pendingOps;
    }
    return Status::OK();
}

void Slot::ResetRuntimeLocked()
{
    writer_.Close();
    runtime_.Reset();
}

Status Slot::AllocateNextDataFileIdLocked(uint32_t &fileId) const
{
    CHECK_FAIL_RETURN_STATUS(runtime_.initialized, StatusCode::K_RUNTIME_ERROR, "Slot runtime is not initialized");
    uint32_t maxFileId = 0;
    for (const auto &dataFile : runtime_.manifest.activeData) {
        uint32_t candidate = 0;
        RETURN_IF_NOT_OK(ParseDataFileId(dataFile, candidate));
        maxFileId = std::max(maxFileId, candidate);
    }
    fileId = maxFileId + 1;
    return Status::OK();
}

Status Slot::RotateWritableDataFileLocked(uint64_t payloadSize, uint32_t &fileId)
{
    CHECK_FAIL_RETURN_STATUS(runtime_.initialized, StatusCode::K_RUNTIME_ERROR, "Slot runtime is not initialized");
    if (runtime_.manifest.activeData.empty()) {
        runtime_.manifest.activeData.emplace_back(FormatDataFileName(1));
        RETURN_IF_NOT_OK(PersistManifest(runtime_.manifest));
        RETURN_IF_NOT_OK(writer_.Init(slotPath_, runtime_.manifest));
        VLOG(1) << "Bootstrapped first active data file for slot, slotId=" << slotId_
                << ", dataFile=" << runtime_.manifest.activeData.back();
    }
    fileId = writer_.GetActiveDataFileId();
    if (writer_.GetActiveDataSize() + payloadSize <= maxDataFileBytes_) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
    RETURN_IF_NOT_OK(AllocateNextDataFileIdLocked(fileId));
    runtime_.manifest.activeData.emplace_back(FormatDataFileName(fileId));
    RETURN_IF_NOT_OK(PersistManifest(runtime_.manifest));
    RETURN_IF_NOT_OK(writer_.Init(slotPath_, runtime_.manifest));
    VLOG(1) << "Rotated writable slot data file, slotId=" << slotId_ << ", payloadSize=" << payloadSize
            << ", newDataFile=" << runtime_.manifest.activeData.back();
    return Status::OK();
}

Status Slot::AllocateExclusiveDataFileLocked(uint32_t &fileId)
{
    CHECK_FAIL_RETURN_STATUS(runtime_.initialized, StatusCode::K_RUNTIME_ERROR, "Slot runtime is not initialized");
    CHECK_FAIL_RETURN_STATUS(!runtime_.manifest.activeData.empty(), StatusCode::K_RUNTIME_ERROR,
                             "Slot active data file list is empty");
    RETURN_IF_NOT_OK(AllocateNextDataFileIdLocked(fileId));
    runtime_.manifest.activeData.insert(runtime_.manifest.activeData.end() - 1, FormatDataFileName(fileId));
    RETURN_IF_NOT_OK(PersistManifest(runtime_.manifest));
    VLOG(1) << "Allocated exclusive slot data file, slotId=" << slotId_ << ", dataFile=" << FormatDataFileName(fileId);
    return Status::OK();
}

Status Slot::GetPayloadSize(const std::shared_ptr<std::iostream> &body, uint64_t &payloadSize) const
{
    CHECK_FAIL_RETURN_STATUS(body != nullptr, StatusCode::K_INVALID, "body is nullptr");
    body->clear();
    auto currentPos = body->tellg();
    CHECK_FAIL_RETURN_STATUS(currentPos != static_cast<std::streampos>(-1), StatusCode::K_INVALID,
                             "body stream is not seekable");
    (void)body->seekg(0, std::ios::end);
    auto endPos = body->tellg();
    CHECK_FAIL_RETURN_STATUS(endPos != static_cast<std::streampos>(-1), StatusCode::K_INVALID,
                             "body stream failed to determine payload size");
    payloadSize = static_cast<uint64_t>(endPos);
    body->clear();
    (void)body->seekg(0, std::ios::beg);
    return Status::OK();
}

Status Slot::WriteStreamToFd(const std::shared_ptr<std::iostream> &body, int fd, uint64_t startOffset,
                             uint64_t &writtenBytes) const
{
    CHECK_FAIL_RETURN_STATUS(body != nullptr, StatusCode::K_INVALID, "body is nullptr");
    CHECK_FAIL_RETURN_STATUS(fd >= 0, StatusCode::K_INVALID, "fd is invalid");
    body->clear();
    (void)body->seekg(0, std::ios::beg);
    std::vector<char> buffer(SLOT_IO_CHUNK_BYTES);
    uint64_t offset = startOffset;
    while (true) {
        body->read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        auto bytesRead = body->gcount();
        if (bytesRead > 0) {
            RETURN_IF_NOT_OK(WriteFile(fd, buffer.data(), static_cast<size_t>(bytesRead), static_cast<off_t>(offset)));
            offset += static_cast<uint64_t>(bytesRead);
        }
        if (body->eof()) {
            break;
        }
        CHECK_FAIL_RETURN_STATUS(!body->fail(), StatusCode::K_RUNTIME_ERROR, "Failed to read payload stream");
    }
    writtenBytes = offset - startOffset;
    body->clear();
    (void)body->seekg(0, std::ios::beg);
    return Status::OK();
}

Status Slot::AppendPayloadToActiveFileLocked(const std::shared_ptr<std::iostream> &body, uint64_t payloadSize,
                                             uint64_t &offset)
{
    body->clear();
    (void)body->seekg(0, std::ios::beg);
    std::vector<char> buffer(SLOT_IO_CHUNK_BYTES);
    uint64_t writtenBytes = 0;
    bool firstChunk = true;
    while (true) {
        body->read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        auto bytesRead = body->gcount();
        if (bytesRead > 0) {
            uint64_t chunkOffset = 0;
            RETURN_IF_NOT_OK(writer_.AppendData(buffer.data(), static_cast<size_t>(bytesRead), chunkOffset));
            if (firstChunk) {
                offset = chunkOffset;
                firstChunk = false;
            }
            writtenBytes += static_cast<uint64_t>(bytesRead);
        }
        if (body->eof()) {
            break;
        }
        CHECK_FAIL_RETURN_STATUS(!body->fail(), StatusCode::K_RUNTIME_ERROR, "Failed to read payload stream");
    }
    body->clear();
    (void)body->seekg(0, std::ios::beg);
    CHECK_FAIL_RETURN_STATUS(writtenBytes == payloadSize, StatusCode::K_RUNTIME_ERROR,
                             "Active data file payload size mismatch");
    return Status::OK();
}

Status Slot::WriteExclusivePayloadLocked(const std::shared_ptr<std::iostream> &body, uint32_t fileId,
                                         uint64_t payloadSize) const
{
    auto dataPath = JoinPath(slotPath_, FormatDataFileName(fileId));
    RETURN_IF_NOT_OK(EnsureFile(dataPath));
    int dataFd = -1;
    RETURN_IF_NOT_OK(OpenFile(dataPath, O_RDWR, &dataFd));
    Raii closeDataFd([&dataFd]() {
        if (dataFd >= 0) {
            close(dataFd);
            dataFd = -1;
        }
    });
    uint64_t writtenBytes = 0;
    RETURN_IF_NOT_OK(WriteStreamToFd(body, dataFd, 0, writtenBytes));
    CHECK_FAIL_RETURN_STATUS(writtenBytes == payloadSize, StatusCode::K_RUNTIME_ERROR,
                             "Exclusive data file payload size mismatch");
    RETURN_IF_NOT_OK(FsyncFd(dataFd));
    return Status::OK();
}

Status Slot::Save(const std::string &key, uint64_t version, const std::shared_ptr<std::iostream> &body,
                  uint64_t asyncElapse, WriteMode writeMode, uint32_t ttlSecond)
{
    (void)asyncElapse;
    VLOG(1) << "Slot save begin, slotId=" << slotId_ << ", key=" << key << ", version=" << version
            << ", writeMode=" << static_cast<uint32_t>(writeMode);
    std::lock_guard<std::mutex> lock(mu_);
    RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
    RETURN_IF_NOT_OK(EnsureWritable(runtime_.manifest));
    uint64_t payloadSize = 0;
    RETURN_IF_NOT_OK(GetPayloadSize(body, payloadSize));
    uint32_t fileId = 0;
    bool useExclusiveFile = payloadSize >= maxDataFileBytes_;
    if (useExclusiveFile) {
        RETURN_IF_NOT_OK(AllocateExclusiveDataFileLocked(fileId));
    } else {
        RETURN_IF_NOT_OK(RotateWritableDataFileLocked(payloadSize, fileId));
    }
    uint64_t offset = 0;
    if (useExclusiveFile) {
        RETURN_IF_NOT_OK(WriteExclusivePayloadLocked(body, fileId, payloadSize));
    } else {
        RETURN_IF_NOT_OK(AppendPayloadToActiveFileLocked(body, payloadSize, offset));
    }
    SlotPutRecord record;
    record.key = key;
    record.fileId = fileId;
    record.offset = offset;
    record.size = payloadSize;
    record.version = version;
    record.writeMode = writeMode;
    record.ttlSecond = ttlSecond;
    std::string encoded;
    RETURN_IF_NOT_OK(SlotIndexCodec::EncodePut(record, encoded));
    RETURN_IF_NOT_OK(writer_.AppendIndexPayload(encoded));
    writer_.RecordOperation(payloadSize + encoded.size());
    runtime_.snapshot.ApplyPut(record);
    RETURN_IF_NOT_OK(FlushRuntimeLocked(false));
    VLOG(1) << "Slot save end, slotId=" << slotId_ << ", key=" << key << ", version=" << version
            << ", payloadSize=" << payloadSize << ", fileId=" << fileId << ", offset=" << offset
            << ", exclusiveFile=" << useExclusiveFile;
    return Status::OK();
}

Status Slot::Get(const std::string &key, uint64_t version, std::shared_ptr<std::stringstream> &content)
{
    VLOG(1) << "Slot get begin, slotId=" << slotId_ << ", key=" << key << ", version=" << version;
    SlotSnapshotValue value;
    {
        std::lock_guard<std::mutex> lock(mu_);
        RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
        RETURN_IF_NOT_OK(runtime_.snapshot.FindExact(key, version, value));
    }
    RETURN_IF_NOT_OK(ReadRecordData(value, content));
    VLOG(1) << "Slot get end, slotId=" << slotId_ << ", key=" << key << ", version=" << version
            << ", fileId=" << value.fileId << ", offset=" << value.offset << ", size=" << value.size;
    return Status::OK();
}

Status Slot::GetWithoutVersion(const std::string &key, uint64_t minVersion, std::shared_ptr<std::stringstream> &content)
{
    VLOG(1) << "Slot get-latest begin, slotId=" << slotId_ << ", key=" << key << ", minVersion=" << minVersion;
    SlotSnapshotValue value;
    {
        std::lock_guard<std::mutex> lock(mu_);
        RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
        RETURN_IF_NOT_OK(runtime_.snapshot.FindLatest(key, minVersion, value));
    }
    RETURN_IF_NOT_OK(ReadRecordData(value, content));
    VLOG(1) << "Slot get-latest end, slotId=" << slotId_ << ", key=" << key
            << ", resolvedVersion=" << value.version << ", fileId=" << value.fileId
            << ", offset=" << value.offset << ", size=" << value.size;
    return Status::OK();
}

Status Slot::Delete(const std::string &key, uint64_t maxVerToDelete, bool deleteAllVersion)
{
    VLOG(1) << "Slot delete begin, slotId=" << slotId_ << ", key=" << key
            << ", maxVerToDelete=" << maxVerToDelete << ", deleteAllVersion=" << deleteAllVersion;
    std::lock_guard<std::mutex> lock(mu_);
    RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
    RETURN_IF_NOT_OK(EnsureWritable(runtime_.manifest));
    SlotDeleteRecord record;
    record.key = key;
    record.version = deleteAllVersion ? UINT64_MAX : maxVerToDelete;
    std::string encoded;
    RETURN_IF_NOT_OK(SlotIndexCodec::EncodeDelete(record, encoded));
    RETURN_IF_NOT_OK(writer_.AppendIndexPayload(encoded));
    writer_.RecordOperation(encoded.size());
    runtime_.snapshot.ApplyDelete(record);
    RETURN_IF_NOT_OK(FlushRuntimeLocked(false));
    VLOG(1) << "Slot delete end, slotId=" << slotId_ << ", key=" << key << ", tombstoneVersion=" << record.version;
    return Status::OK();
}

Status Slot::PreloadLocal(const SlotPreloadCallback &callback)
{
    if (!callback) {
        return Status::OK();
    }

    std::vector<SlotPutRecord> visiblePuts;
    {
        std::lock_guard<std::mutex> lock(mu_);
        RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
        RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
        RETURN_IF_NOT_OK(runtime_.snapshot.CollectVisiblePuts(visiblePuts));
    }
    VLOG(1) << "Slot preload-local begin, slotId=" << slotId_ << ", visiblePutCount=" << visiblePuts.size();
    return RunPreloadCallback(visiblePuts, callback);
}

Status Slot::ReplayIndex(SlotSnapshot &snapshot)
{
    std::lock_guard<std::mutex> lock(mu_);
    VLOG(1) << "Slot replay-index begin, slotId=" << slotId_ << ", slotPath=" << slotPath_;
    INJECT_POINT("slotstore.Slot.ReplayIndex.Enter", []() { return Status::OK(); });
    RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
    RETURN_IF_NOT_OK(BootstrapManifestIfNeed());
    SlotManifestData manifest;
    RETURN_IF_NOT_OK(LoadManifest(manifest));
    RETURN_IF_NOT_OK(RecoverManifestIfNeeded(manifest));
    RETURN_IF_NOT_OK(SlotSnapshot::Replay(slotPath_, manifest, snapshot));
    VLOG(1) << "Slot replay-index end, slotId=" << slotId_ << ", slotPath=" << slotPath_
            << ", " << ManifestDebugString(manifest);
    return Status::OK();
}

Status Slot::BootstrapManifestIfNeed()
{
    VLOG(1) << "Bootstrap slot manifest if needed, slotId=" << slotId_ << ", slotPath=" << slotPath_;
    RETURN_IF_NOT_OK(CreateDir(slotPath_, true));
    SlotManifestData manifest;
    auto rc = SlotManifest::Load(slotPath_, manifest);
    if (rc.GetCode() == StatusCode::K_NOT_FOUND) {
        RETURN_IF_NOT_OK(BuildBootstrapManifestFromDisk(manifest));
        RETURN_IF_NOT_OK(PersistManifest(manifest));
        VLOG(1) << "Created bootstrap slot manifest, slotId=" << slotId_ << ", " << ManifestDebugString(manifest);
    } else {
        RETURN_IF_NOT_OK(rc);
        VLOG(1) << "Slot manifest already exists, slotId=" << slotId_ << ", " << ManifestDebugString(manifest);
    }
    RETURN_IF_NOT_OK(EnsureActiveFiles(manifest));
    return Status::OK();
}

Status Slot::BuildBootstrapManifestFromDisk(SlotManifestData &manifest)
{
    manifest = SlotManifest::Bootstrap();
    bool rebuiltFromDisk = false;

    std::vector<std::string> indexPaths;
    RETURN_IF_NOT_OK(Glob(JoinPath(slotPath_, "index*.log"), indexPaths));
    std::string bestIndexName;
    size_t bestValidBytes = 0;
    for (const auto &indexPath : indexPaths) {
        auto filename = BaseName(indexPath);
        if (filename.empty()) {
            continue;
        }
        if (IsImportIndexFile(filename)) {
            continue;
        }
        std::vector<SlotRecord> records;
        size_t validBytes = 0;
        auto rc = SlotIndexCodec::ReadAllRecords(indexPath, records, validBytes);
        if (rc.IsError()) {
            LOG(WARNING) << "Skip invalid slot index while rebuilding manifest, path=" << indexPath
                         << ", err=" << rc.ToString();
            continue;
        }
        if (ShouldUseAsBootstrapIndex(filename, validBytes, bestIndexName, bestValidBytes)) {
            bestIndexName = filename;
            bestValidBytes = validBytes;
        }
    }
    if (!bestIndexName.empty()) {
        manifest.activeIndex = bestIndexName;
        rebuiltFromDisk = true;
    }

    std::vector<std::string> dataPaths;
    RETURN_IF_NOT_OK(Glob(JoinPath(slotPath_, "data_*.bin"), dataPaths));
    std::vector<std::pair<uint32_t, std::string>> parsedDataFiles;
    for (const auto &dataPath : dataPaths) {
        auto filename = BaseName(dataPath);
        uint32_t fileId = 0;
        if (ParseDataFileId(filename, fileId).IsOk()) {
            parsedDataFiles.emplace_back(fileId, filename);
        }
    }
    if (!parsedDataFiles.empty()) {
        std::sort(parsedDataFiles.begin(), parsedDataFiles.end(),
                  [](const auto &lhs, const auto &rhs) { return lhs.first < rhs.first; });
        manifest.activeData.clear();
        for (const auto &entry : parsedDataFiles) {
            manifest.activeData.emplace_back(entry.second);
        }
        rebuiltFromDisk = true;
    }

    if (rebuiltFromDisk) {
        LOG(INFO) << "Rebuilt slot manifest from disk layout, slotId=" << slotId_
                  << ", activeIndex=" << manifest.activeIndex << ", activeDataCount=" << manifest.activeData.size();
    }
    return Status::OK();
}

Status Slot::Repair()
{
    VLOG(1) << "Slot repair begin, slotId=" << slotId_ << ", slotPath=" << slotPath_;
    std::lock_guard<std::mutex> lock(mu_);
    RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
    writer_.Close();
    runtime_.Reset();
    RETURN_IF_NOT_OK(BootstrapManifestIfNeed());
    SlotManifestData manifest;
    RETURN_IF_NOT_OK(LoadManifest(manifest));
    RETURN_IF_NOT_OK(RecoverManifestIfNeeded(manifest));
    size_t validBytes = 0;
    std::vector<SlotRecord> records;
    RETURN_IF_NOT_OK(SlotIndexCodec::ReadAllRecords(JoinPath(slotPath_, manifest.activeIndex), records, validBytes));
    SlotSnapshot snapshot;
    RETURN_IF_NOT_OK(SlotSnapshot::Replay(slotPath_, manifest, snapshot));
    runtime_.manifest = manifest;
    runtime_.snapshot = snapshot;
    RETURN_IF_NOT_OK(GetFileModifiedTime(JoinPath(slotPath_, "manifest"), runtime_.manifestModifiedTimeUs));
    runtime_.initialized = true;
    RETURN_IF_NOT_OK(writer_.Init(slotPath_, manifest));
    VLOG(1) << "Slot repair end, slotId=" << slotId_ << ", slotPath=" << slotPath_
            << ", recordCount=" << records.size() << ", validBytes=" << validBytes
            << ", " << ManifestDebugString(manifest);
    return Status::OK();
}

Status Slot::Compact()
{
    VLOG(1) << "Slot compact begin, slotId=" << slotId_ << ", slotPath=" << slotPath_;
    RETURN_IF_NOT_OK(Repair());
    SlotManifestData baseManifest;
    SlotSnapshot snapshot;
    size_t appliedFrontier = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
        RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
        RETURN_IF_NOT_OK(CheckCompactTransferPreemptionLocked());
        RETURN_IF_NOT_OK(EnsureWritable(runtime_.manifest));
        baseManifest = runtime_.manifest;
        snapshot = runtime_.snapshot;
        appliedFrontier = static_cast<size_t>(writer_.GetActiveIndexSize());
    }

    const auto compactEpochMs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    SlotCompactor compactor(slotPath_, maxDataFileBytes_);
    SlotCompactBuildResult buildResult;
    RETURN_IF_NOT_OK(compactor.BuildArtifacts(baseManifest, snapshot, compactEpochMs, buildResult));
    bool keepCompactArtifacts = false;
    Raii cleanupCompactArtifacts([&compactor, &buildResult, &keepCompactArtifacts]() {
        if (!keepCompactArtifacts) {
            (void)compactor.CleanupArtifacts(buildResult);
        }
    });
    INJECT_POINT("slotstore.Slot.Compact.BeforeCommit",
                 [this](std::string objectKey, std::string version, std::string payload) {
                     auto body = std::make_shared<std::stringstream>();
                     *body << payload;
                     return Save(objectKey, std::stoull(version), body);
                 });
    INJECT_POINT("slotstore.Slot.Compact.BeforeCommitDelete",
                 [this](std::string objectKey, std::string version, std::string deleteAllVersion) {
                     return Delete(objectKey, std::stoull(version), deleteAllVersion == "true");
                 });
    INJECT_POINT("slotstore.Slot.Compact.BeforeCommitTransfer",
                 [this](std::string sourceSlotPath, std::string preload) {
                     SlotTakeoverRequest request;
                     request.mode = preload == "true" ? SlotTakeoverMode::PRELOAD : SlotTakeoverMode::MERGE;
                     return Takeover(sourceSlotPath, request);
                 });

    for (;;) {
        SlotManifestData currentManifest;
        size_t catchupFrontier = 0;
        {
            std::lock_guard<std::mutex> lock(mu_);
            RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
            RETURN_IF_NOT_OK(CheckCompactTransferPreemptionLocked());
            RETURN_IF_NOT_OK(EnsureWritable(runtime_.manifest));
            currentManifest = runtime_.manifest;
            if (currentManifest.activeIndex != baseManifest.activeIndex) {
                RETURN_STATUS(StatusCode::K_TRY_AGAIN, "Slot active index changed during compact");
            }
            RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
            catchupFrontier = static_cast<size_t>(writer_.GetActiveIndexSize());
        }

        std::vector<SlotRecord> deltaRecords;
        size_t deltaBytes = 0;
        RETURN_IF_NOT_OK(ReadIndexDeltaRecords(JoinPath(slotPath_, baseManifest.activeIndex), appliedFrontier,
                                               catchupFrontier, deltaRecords, deltaBytes));
        if (!deltaRecords.empty()) {
            auto rc = compactor.ApplyDeltaRecords(deltaRecords, buildResult);
            RETURN_IF_NOT_OK(rc);
            appliedFrontier = catchupFrontier;
            VLOG(1) << "Slot compact applied catchup delta, slotId=" << slotId_
                    << ", deltaRecordCount=" << deltaRecords.size() << ", deltaBytes=" << deltaBytes
                    << ", frontier=" << appliedFrontier;
        }
        if (deltaBytes <= GetCurrentSlotCompactCutoverBytes()
            && deltaRecords.size() <= GetCurrentSlotCompactCutoverRecords()) {
            break;
        }
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
        RETURN_IF_NOT_OK(CheckCompactTransferPreemptionLocked());
        RETURN_IF_NOT_OK(EnsureWritable(runtime_.manifest));
        auto manifest = runtime_.manifest;
        if (manifest.activeIndex != baseManifest.activeIndex) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "Slot active index changed before compact cutover");
        }
        RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
        const auto finalFrontier = static_cast<size_t>(writer_.GetActiveIndexSize());
        std::vector<SlotRecord> finalDeltaRecords;
        size_t ignoredFinalDeltaBytes = 0;
        RETURN_IF_NOT_OK(ReadIndexDeltaRecords(JoinPath(slotPath_, baseManifest.activeIndex), appliedFrontier,
                                               finalFrontier, finalDeltaRecords, ignoredFinalDeltaBytes));
        if (!finalDeltaRecords.empty()) {
            auto rc = compactor.ApplyDeltaRecords(finalDeltaRecords, buildResult);
            RETURN_IF_NOT_OK(rc);
            VLOG(1) << "Slot compact applied final delta, slotId=" << slotId_
                    << ", deltaRecordCount=" << finalDeltaRecords.size();
        }

        SlotManifestData switching = manifest;
        switching.state = SlotState::IN_OPERATION;
        switching.opType = SlotOperationType::COMPACT;
        switching.opPhase = SlotOperationPhase::COMPACT_COMMITTING;
        switching.role = SlotOperationRole::LOCAL;
        switching.txnId = std::to_string(compactEpochMs);
        switching.pendingIndex = buildResult.indexFile;
        switching.pendingData = buildResult.dataFiles;
        switching.lastCompactEpochMs = compactEpochMs;
        keepCompactArtifacts = true;
        RETURN_IF_NOT_OK(PersistManifest(switching));

        SlotManifestData active = switching;
        active.state = SlotState::NORMAL;
        active.opType = SlotOperationType::NONE;
        active.opPhase = SlotOperationPhase::NONE;
        active.role = SlotOperationRole::NONE;
        active.txnId.clear();
        active.obsoleteIndex = manifest.activeIndex;
        active.obsoleteData = manifest.activeData;
        active.activeIndex = buildResult.indexFile;
        active.activeData = buildResult.dataFiles;
        active.gcPending = true;
        active.pendingIndex.clear();
        active.pendingData.clear();
        RETURN_IF_NOT_OK(PersistManifest(active));
        RETURN_IF_NOT_OK(ContinueGc(active));
        ResetRuntimeLocked();
        RETURN_IF_NOT_OK(BuildRuntimeStateLocked());
    }
    VLOG(1) << "Slot compact end, slotId=" << slotId_ << ", slotPath=" << slotPath_;
    return Status::OK();
}

Status Slot::Seal(const std::string &sealReason)
{
    (void)sealReason;
    std::lock_guard<std::mutex> lock(mu_);
    RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
    RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
    RETURN_IF_NOT_OK(SyncActiveFiles(runtime_.manifest));
    return Status::OK();
}

Status Slot::Takeover(const std::string &sourceSlotPath, bool loadToMemory)
{
    SlotTakeoverRequest request;
    request.mode = loadToMemory ? SlotTakeoverMode::PRELOAD : SlotTakeoverMode::MERGE;
    return Takeover(sourceSlotPath, request);
}

Status Slot::Takeover(const std::string &sourceSlotPath, const SlotTakeoverRequest &request)
{
    CHECK_FAIL_RETURN_STATUS(!sourceSlotPath.empty(), StatusCode::K_INVALID, "sourceSlotPath must not be empty");
    CHECK_FAIL_RETURN_STATUS(sourceSlotPath != slotPath_, StatusCode::K_INVALID,
                             "sourceSlotPath must differ from target slot path");
    VLOG(1) << "Slot takeover begin, slotId=" << slotId_ << ", targetSlot=" << slotPath_
            << ", sourceSlot=" << sourceSlotPath << ", loadToMemory=" << request.IsPreload();
    bool expected = false;
    CHECK_FAIL_RETURN_STATUS(transferIntentActive_.compare_exchange_strong(expected, true), StatusCode::K_TRY_AGAIN,
                             "Another transfer already owns this target slot");
    Raii clearTransferIntent([this]() { transferIntentActive_.store(false); });
    RETURN_IF_NOT_OK(Repair());
    {
        Slot sourceManager(0, sourceSlotPath, maxDataFileBytes_);
        RETURN_IF_NOT_OK(sourceManager.Repair());
    }

    std::lock_guard<std::mutex> lock(mu_);
    RETURN_IF_NOT_OK(EnsureRuntimeReadyLocked());
    RETURN_IF_NOT_OK(FlushRuntimeLocked(true));
    SlotManifestData targetManifest = runtime_.manifest;
    RETURN_IF_NOT_OK(EnsureWritable(targetManifest));
    RETURN_IF_NOT_OK(CleanupStaleTakeoverArtifacts());

    const std::string txnId = GenerateTxnId(slotId_);
    const std::string recoverySlotPath = BuildRecoverySlotPath(sourceSlotPath, txnId);

    targetManifest.state = SlotState::IN_OPERATION;
    targetManifest.opType = SlotOperationType::TRANSFER;
    targetManifest.opPhase = SlotOperationPhase::TRANSFER_FENCING;
    targetManifest.role = SlotOperationRole::TARGET;
    targetManifest.txnId = txnId;
    targetManifest.ownerEpoch += 1;
    targetManifest.peerSlotPath = sourceSlotPath;
    targetManifest.recoverySlotPath = recoverySlotPath;
    targetManifest.transferPlanPath.clear();
    targetManifest.transferFileMap.clear();
    RETURN_IF_NOT_OK(PersistManifest(targetManifest));
    VLOG(1) << "Slot takeover fenced target manifest, slotId=" << slotId_ << ", txnId=" << txnId
            << ", recoverySlotPath=" << recoverySlotPath;
    INJECT_POINT("slotstore.Slot.Takeover.AfterManifestFencing", []() { return Status::OK(); });

    if (!FileExist(recoverySlotPath)) {
        CHECK_FAIL_RETURN_STATUS(FileExist(sourceSlotPath), StatusCode::K_NOT_FOUND,
                                 "Source slot path not found for takeover: " + sourceSlotPath);
        RETURN_IF_NOT_OK(RenameFile(sourceSlotPath, recoverySlotPath));
    }
    RETURN_IF_NOT_OK(PersistSourceTransferManifest(recoverySlotPath, targetManifest, sourceSlotPath,
                                                   SlotOperationPhase::TRANSFER_FENCING));

    SlotManifestData sourceManifest;
    RETURN_IF_NOT_OK(SlotManifest::Load(recoverySlotPath, sourceManifest));
    SlotSnapshot sourceSnapshot;
    RETURN_IF_NOT_OK(SlotSnapshot::Replay(recoverySlotPath, sourceManifest, sourceSnapshot));

    SlotTakeoverPlan plan;
    RETURN_IF_NOT_OK(SlotTakeoverPlanner::BuildPlan(sourceSlotPath, recoverySlotPath, sourceManifest, sourceSnapshot,
                                                    slotPath_, targetManifest, request, txnId, plan));
    RETURN_IF_NOT_OK(SlotTakeoverPlanner::DumpPlan(slotPath_, plan));
    VLOG(1) << "Slot takeover durable plan ready, slotId=" << slotId_ << ", txnId=" << txnId
            << ", dataMappingCount=" << plan.dataMappings.size();
    INJECT_POINT("slotstore.Slot.Takeover.AfterPlanDurable", []() { return Status::OK(); });

    targetManifest.transferPlanPath = FormatTakeoverPlanFileName(plan.txnId);
    targetManifest.transferFileMap = EncodeTransferFileMap(plan);
    targetManifest.opPhase = SlotOperationPhase::TRANSFER_PREPARED;
    RETURN_IF_NOT_OK(PersistManifest(targetManifest));
    RETURN_IF_NOT_OK(PersistSourceTransferManifest(recoverySlotPath, targetManifest, sourceSlotPath,
                                                   SlotOperationPhase::TRANSFER_PREPARED));
    INJECT_POINT("slotstore.Slot.Takeover.AfterManifestImporting", []() { return Status::OK(); });

    RETURN_IF_NOT_OK(RecoverTransfer(targetManifest, &request));
    ResetRuntimeLocked();
    RETURN_IF_NOT_OK(BuildRuntimeStateLocked());
    VLOG(1) << "Slot takeover end, slotId=" << slotId_ << ", targetSlot=" << slotPath_ << ", txnId=" << txnId;
    return Status::OK();
}

Status Slot::RecoverManifestIfNeeded(SlotManifestData &manifest)
{
    VLOG(1) << "Checking slot manifest recovery, slotId=" << slotId_ << ", slotPath=" << slotPath_
            << ", " << ManifestDebugString(manifest);
    if (manifest.state == SlotState::IN_OPERATION && manifest.opType == SlotOperationType::COMPACT
        && manifest.opPhase == SlotOperationPhase::COMPACT_COMMITTING) {
        RETURN_IF_NOT_OK(RecoverCompactCommitting(manifest));
    } else if (IsTransferManifest(manifest)) {
        RETURN_IF_NOT_OK(RecoverTransfer(manifest, nullptr));
    }
    if (manifest.gcPending) {
        RETURN_IF_NOT_OK(ContinueGc(manifest));
    }
    VLOG(1) << "Finished slot manifest recovery check, slotId=" << slotId_ << ", "
            << ManifestDebugString(manifest);
    return Status::OK();
}

Status Slot::RecoverCompactCommitting(SlotManifestData &manifest)
{
    VLOG(1) << "Recovering compact-committing slot manifest, slotId=" << slotId_ << ", "
            << ManifestDebugString(manifest);
    if (PendingArtifactsReady(manifest)) {
        manifest.obsoleteIndex = manifest.activeIndex;
        manifest.obsoleteData = manifest.activeData;
        manifest.activeIndex = manifest.pendingIndex;
        manifest.activeData = manifest.pendingData;
        manifest.gcPending = true;
    } else {
        std::vector<std::string> stalePaths = manifest.pendingData;
        if (!manifest.pendingIndex.empty()) {
            stalePaths.emplace_back(manifest.pendingIndex);
        }
        RETURN_IF_NOT_OK(CleanupArtifactFiles(stalePaths));
    }
    manifest.state = SlotState::NORMAL;
    manifest.opType = SlotOperationType::NONE;
    manifest.opPhase = SlotOperationPhase::NONE;
    manifest.role = SlotOperationRole::NONE;
    manifest.txnId.clear();
    manifest.pendingIndex.clear();
    manifest.pendingData.clear();
    RETURN_IF_NOT_OK(PersistManifest(manifest));
    VLOG(1) << "Recovered compact-committing slot manifest, slotId=" << slotId_ << ", "
            << ManifestDebugString(manifest);
    return Status::OK();
}

Status Slot::RecoverTransfer(SlotManifestData &manifest, const SlotTakeoverRequest *request)
{
    CHECK_FAIL_RETURN_STATUS(IsTransferManifest(manifest), StatusCode::K_INVALID,
                             "RecoverTransfer requires transfer manifest");
    CHECK_FAIL_RETURN_STATUS(!manifest.recoverySlotPath.empty(), StatusCode::K_INVALID,
                             "TRANSFER manifest missing recovery slot path");
    CHECK_FAIL_RETURN_STATUS(!manifest.txnId.empty(), StatusCode::K_INVALID, "TRANSFER manifest missing txn_id");
    VLOG(1) << "Recovering slot transfer, slotId=" << slotId_ << ", targetSlot=" << slotPath_
            << ", " << ManifestDebugString(manifest) << ", hasRequest=" << (request != nullptr);

    while (IsTransferManifest(manifest)) {
        VLOG(1) << "Slot transfer phase begin, slotId=" << slotId_ << ", txnId=" << manifest.txnId
                << ", phase=" << ToString(manifest.opPhase);
        switch (manifest.opPhase) {
            case SlotOperationPhase::TRANSFER_FENCING: {
                if (!FileExist(manifest.recoverySlotPath)) {
                    CHECK_FAIL_RETURN_STATUS(!manifest.peerSlotPath.empty(), StatusCode::K_INVALID,
                                             "TRANSFER_FENCING missing source home slot path");
                    if (!FileExist(manifest.peerSlotPath)) {
                        LOG(WARNING) << "Abort transfer fencing because neither source home nor recovery slot exists, "
                                     << "targetSlot=" << slotPath_ << ", txnId=" << manifest.txnId;
                        RETURN_IF_NOT_OK(CleanupTransferArtifacts(manifest));
                        RETURN_IF_NOT_OK(ResetTransferTargetManifest(manifest));
                        return Status::OK();
                    }
                    RETURN_IF_NOT_OK(RenameFile(manifest.peerSlotPath, manifest.recoverySlotPath));
                }
                RETURN_IF_NOT_OK(PersistSourceTransferManifest(
                    manifest.recoverySlotPath, manifest, manifest.peerSlotPath, SlotOperationPhase::TRANSFER_FENCING));

                SlotManifestData sourceManifest;
                SlotSnapshot sourceSnapshot;
                RETURN_IF_NOT_OK(LoadRecoveryTransferState(manifest.recoverySlotPath, sourceManifest, sourceSnapshot));
                SlotTakeoverPlan plan;
                SlotTakeoverRequest rebuildRequest;
                rebuildRequest.mode = SlotTakeoverMode::MERGE;
                RETURN_IF_NOT_OK(SlotTakeoverPlanner::BuildPlan(manifest.peerSlotPath, manifest.recoverySlotPath,
                                                                sourceManifest, sourceSnapshot, slotPath_, manifest,
                                                                rebuildRequest, manifest.txnId, plan));
                RETURN_IF_NOT_OK(SlotTakeoverPlanner::DumpPlan(slotPath_, plan));
                manifest.transferPlanPath = FormatTakeoverPlanFileName(plan.txnId);
                manifest.transferFileMap = EncodeTransferFileMap(plan);
                manifest.opPhase = SlotOperationPhase::TRANSFER_PREPARED;
                RETURN_IF_NOT_OK(PersistManifest(manifest));
                RETURN_IF_NOT_OK(PersistSourceTransferManifest(
                    manifest.recoverySlotPath, manifest, manifest.peerSlotPath, SlotOperationPhase::TRANSFER_PREPARED));
                VLOG(1) << "Slot transfer moved to PREPARED, slotId=" << slotId_ << ", txnId=" << manifest.txnId
                        << ", dataMappingCount=" << plan.dataMappings.size();
                break;
            }
            case SlotOperationPhase::TRANSFER_PREPARED: {
                SlotTakeoverPlan plan;
                auto needRebuildPlan =
                    manifest.transferPlanPath.empty() || !FileExist(JoinPath(slotPath_, manifest.transferPlanPath));
                if (!needRebuildPlan) {
                    auto rc = SlotTakeoverPlanner::LoadPlan(JoinPath(slotPath_, manifest.transferPlanPath), plan);
                    if (rc.IsError()) {
                        needRebuildPlan = true;
                    }
                }
                const auto importIndexPath = JoinPath(slotPath_, FormatImportIndexFileName(manifest.txnId));
                if (!FileExist(importIndexPath)) {
                    needRebuildPlan = true;
                }
                if (needRebuildPlan) {
                    if (!FileExist(manifest.recoverySlotPath) && !FileExist(importIndexPath)) {
                        LOG(ERROR) << "Abort prepared transfer because plan/import artifacts are missing and source "
                                   << "recovery is unavailable, targetSlot=" << slotPath_
                                   << ", txnId=" << manifest.txnId;
                        RETURN_IF_NOT_OK(CleanupTransferArtifacts(manifest));
                        RETURN_IF_NOT_OK(ResetTransferTargetManifest(manifest));
                        return Status::OK();
                    }
                    RETURN_IF_NOT_OK(RebuildPreparedTransferPlan(manifest, plan));
                }
                if (!FileExist(plan.sourceRecoverySlotPath) && !AreTransferTargetDataReady(plan)) {
                    LOG(ERROR) << "Abort prepared transfer because source recovery slot is missing before all data "
                               << "files are durable on target, targetSlot=" << slotPath_
                               << ", txnId=" << manifest.txnId;
                    RETURN_IF_NOT_OK(CleanupTransferArtifacts(manifest, &plan));
                    RETURN_IF_NOT_OK(ResetTransferTargetManifest(manifest));
                    return Status::OK();
                }
                for (size_t i = 0; i < plan.dataMappings.size(); ++i) {
                    const auto &mapping = plan.dataMappings[i];
                    RETURN_IF_NOT_OK(
                        RenameDataFileIfNeeded(JoinPath(plan.sourceRecoverySlotPath, mapping.sourceDataFile),
                                               JoinPath(slotPath_, mapping.targetDataFile)));
                    if (i == 0) {
                        INJECT_POINT("slotstore.Slot.Takeover.AfterFirstMove", []() { return Status::OK(); });
                    }
                }
                if (FileExist(plan.sourceRecoverySlotPath)) {
                    RETURN_IF_NOT_OK(FsyncDir(plan.sourceRecoverySlotPath));
                }
                RETURN_IF_NOT_OK(FsyncDir(slotPath_));
                if (request != nullptr && request->IsPreload() && request->callback) {
                    RETURN_IF_NOT_OK(RunPreloadCallback(plan, *request));
                }

                bool published = false;
                RETURN_IF_NOT_OK(PublishImportBatch(JoinPath(slotPath_, manifest.activeIndex), plan.txnId,
                                                    plan.importIndexFile, published));
                INJECT_POINT("slotstore.Slot.Takeover.AfterImportPublished", []() { return Status::OK(); });
                for (const auto &mapping : plan.dataMappings) {
                    if (std::find(manifest.activeData.begin(), manifest.activeData.end(), mapping.targetDataFile)
                        == manifest.activeData.end()) {
                        manifest.activeData.emplace_back(mapping.targetDataFile);
                    }
                }
                manifest.opPhase = SlotOperationPhase::TRANSFER_INDEX_PUBLISHED;
                RETURN_IF_NOT_OK(PersistManifest(manifest));
                if (FileExist(plan.sourceRecoverySlotPath)) {
                    RETURN_IF_NOT_OK(PersistSourceTransferManifest(plan.sourceRecoverySlotPath, manifest,
                                                                   plan.sourceHomeSlotPath,
                                                                   SlotOperationPhase::TRANSFER_INDEX_PUBLISHED));
                }
                VLOG(1) << "Slot transfer published import batch, slotId=" << slotId_ << ", txnId=" << manifest.txnId
                        << ", dataMappingCount=" << plan.dataMappings.size() << ", publishedActiveDataCount="
                        << manifest.activeData.size();
                break;
            }
            case SlotOperationPhase::TRANSFER_INDEX_PUBLISHED: {
                RETURN_IF_NOT_OK(FinalizeSourceAfterTakeover(manifest.recoverySlotPath, manifest));
                manifest.opPhase = SlotOperationPhase::TRANSFER_SOURCE_RETIRED;
                RETURN_IF_NOT_OK(PersistManifest(manifest));
                VLOG(1) << "Slot transfer source retired, slotId=" << slotId_ << ", txnId=" << manifest.txnId;
                break;
            }
            case SlotOperationPhase::TRANSFER_SOURCE_RETIRED: {
                const auto completedTxnId = manifest.txnId;
                const auto transferPlanFile = manifest.transferPlanPath;
                manifest.state = SlotState::NORMAL;
                manifest.opType = SlotOperationType::NONE;
                manifest.opPhase = SlotOperationPhase::NONE;
                manifest.role = SlotOperationRole::NONE;
                manifest.txnId.clear();
                manifest.peerSlotPath.clear();
                manifest.recoverySlotPath.clear();
                manifest.transferFileMap.clear();
                manifest.transferPlanPath.clear();
                RETURN_IF_NOT_OK(PersistManifest(manifest));

                bool deletedImportIndex = false;
                (void)DeleteFileIfExists(FormatImportIndexFileName(completedTxnId), deletedImportIndex);
                if (!transferPlanFile.empty()) {
                    bool deletedPlan = false;
                    (void)DeleteFileIfExists(BaseName(transferPlanFile), deletedPlan);
                }
                VLOG(1) << "Slot transfer completed, slotId=" << slotId_ << ", completedTxnId=" << completedTxnId;
                return Status::OK();
            }
            default:
                RETURN_STATUS(StatusCode::K_INVALID, "Unexpected TRANSFER op_phase");
        }
    }
    return Status::OK();
}

Status Slot::ContinueGc(SlotManifestData &manifest)
{
    VLOG(1) << "Slot GC begin, slotId=" << slotId_ << ", obsoleteDataCount=" << manifest.obsoleteData.size()
            << ", obsoleteIndex=" << manifest.obsoleteIndex << ", gcPending=" << manifest.gcPending;
    bool deletedAnything = false;
    for (const auto &dataFile : manifest.obsoleteData) {
        RETURN_IF_NOT_OK(DeleteFileIfExists(dataFile, deletedAnything));
    }
    RETURN_IF_NOT_OK(DeleteFileIfExists(manifest.obsoleteIndex, deletedAnything));
    if (deletedAnything || manifest.gcPending) {
        manifest.gcPending = false;
        manifest.obsoleteIndex.clear();
        manifest.obsoleteData.clear();
        RETURN_IF_NOT_OK(PersistManifest(manifest));
    }
    VLOG(1) << "Slot GC end, slotId=" << slotId_ << ", deletedAnything=" << deletedAnything
            << ", gcPending=" << manifest.gcPending;
    return Status::OK();
}

Status Slot::DeleteFileIfExists(const std::string &relativePath, bool &deletedAnything)
{
    if (relativePath.empty()) {
        return Status::OK();
    }
    auto absPath = JoinPath(slotPath_, relativePath);
    if (!FileExist(absPath)) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(DeleteFile(absPath));
    deletedAnything = true;
    return Status::OK();
}

Status Slot::CleanupArtifactFiles(const std::vector<std::string> &relativePaths)
{
    bool deletedAnything = false;
    for (const auto &relativePath : relativePaths) {
        RETURN_IF_NOT_OK(DeleteFileIfExists(relativePath, deletedAnything));
    }
    return Status::OK();
}

Status Slot::CleanupStaleTakeoverArtifacts()
{
    std::vector<std::string> stalePlanPaths;
    RETURN_IF_NOT_OK(Glob(JoinPath(slotPath_, "takeover_*.plan"), stalePlanPaths));
    std::vector<std::string> staleImportPaths;
    RETURN_IF_NOT_OK(Glob(JoinPath(slotPath_, "index_import_*.log"), staleImportPaths));
    std::vector<std::string> staleRelativePaths;
    staleRelativePaths.reserve(stalePlanPaths.size() + staleImportPaths.size());
    for (const auto &path : stalePlanPaths) {
        staleRelativePaths.emplace_back(BaseName(path));
    }
    for (const auto &path : staleImportPaths) {
        staleRelativePaths.emplace_back(BaseName(path));
    }
    if (!staleRelativePaths.empty()) {
        VLOG(1) << "Cleaning stale slot takeover artifacts, slotId=" << slotId_
                << ", artifactCount=" << staleRelativePaths.size();
    }
    RETURN_IF_NOT_OK(CleanupArtifactFiles(staleRelativePaths));
    return Status::OK();
}

bool Slot::PendingArtifactsReady(const SlotManifestData &manifest) const
{
    if (manifest.pendingIndex.empty() || manifest.pendingData.empty()) {
        return false;
    }
    auto indexPath = JoinPath(slotPath_, manifest.pendingIndex);
    if (!FileExist(indexPath) || FileSize(indexPath, false) < static_cast<off_t>(SlotIndexCodec::HEADER_SIZE)) {
        return false;
    }
    for (const auto &dataFile : manifest.pendingData) {
        if (!FileExist(JoinPath(slotPath_, dataFile))) {
            return false;
        }
    }
    return true;
}

Status Slot::LoadManifest(SlotManifestData &manifest)
{
    RETURN_IF_NOT_OK(SlotManifest::Load(slotPath_, manifest));
    return Status::OK();
}

Status Slot::EnsureActiveFiles(const SlotManifestData &manifest)
{
    if (!manifest.activeIndex.empty()) {
        RETURN_IF_NOT_OK(SlotIndexCodec::EnsureIndexFile(JoinPath(slotPath_, manifest.activeIndex)));
    }
    for (const auto &dataFile : manifest.activeData) {
        RETURN_IF_NOT_OK(::datasystem::EnsureFile(JoinPath(slotPath_, dataFile)));
    }
    return Status::OK();
}

Status Slot::PersistManifest(const SlotManifestData &manifest)
{
    SlotManifestData persisted = manifest;
    if (persisted.homeSlotPath.empty()) {
        persisted.homeSlotPath = slotPath_;
    }
    RETURN_IF_NOT_OK(SlotManifest::StoreAtomic(slotPath_, persisted));
    RETURN_IF_NOT_OK(EnsureActiveFiles(persisted));
    if (runtime_.initialized) {
        runtime_.manifest = persisted;
        RETURN_IF_NOT_OK(GetFileModifiedTime(JoinPath(slotPath_, "manifest"), runtime_.manifestModifiedTimeUs));
    }
    VLOG(1) << "Slot persisted manifest, slotId=" << slotId_ << ", slotPath=" << slotPath_
            << ", " << ManifestDebugString(persisted);
    return Status::OK();
}

Status Slot::ReadRecordData(const SlotSnapshotValue &value, std::shared_ptr<std::stringstream> &content) const
{
    CHECK_FAIL_RETURN_STATUS(content != nullptr, StatusCode::K_INVALID, "content is nullptr");
    int fd = -1;
    auto dataPath = JoinPath(slotPath_, FormatDataFileName(value.fileId));
    RETURN_IF_NOT_OK(OpenFile(dataPath, O_RDONLY, &fd));
    Raii closeFd([fd]() {
        if (fd >= 0) {
            close(fd);
        }
    });
    content->str("");
    content->clear();
    std::vector<char> buffer(std::min<uint64_t>(value.size, SLOT_IO_CHUNK_BYTES));
    uint64_t remaining = value.size;
    uint64_t offset = value.offset;
    while (remaining > 0) {
        auto bytesToRead = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        RETURN_IF_NOT_OK(ReadFile(fd, buffer.data(), bytesToRead, static_cast<off_t>(offset)));
        content->write(buffer.data(), static_cast<std::streamsize>(bytesToRead));
        offset += bytesToRead;
        remaining -= bytesToRead;
    }
    return Status::OK();
}

Status Slot::EnsureWritable(const SlotManifestData &manifest) const
{
    CHECK_FAIL_RETURN_STATUS(IsNormalManifest(manifest), StatusCode::K_TRY_AGAIN,
                             "Slot is not writable in current state");
    return Status::OK();
}

Status Slot::SyncActiveFiles(const SlotManifestData &manifest) const
{
    if (!manifest.activeIndex.empty()) {
        int fd = -1;
        RETURN_IF_NOT_OK(OpenFile(JoinPath(slotPath_, manifest.activeIndex), O_RDWR, &fd));
        Raii closeIndexFd([fd]() {
            if (fd >= 0) {
                close(fd);
            }
        });
        RETURN_IF_NOT_OK(FsyncFd(fd));
    }
    for (const auto &dataFile : manifest.activeData) {
        int fd = -1;
        RETURN_IF_NOT_OK(OpenFile(JoinPath(slotPath_, dataFile), O_RDWR, &fd));
        Raii closeDataFd([fd]() {
            if (fd >= 0) {
                close(fd);
            }
        });
        RETURN_IF_NOT_OK(FsyncFd(fd));
    }
    RETURN_IF_NOT_OK(FsyncDir(slotPath_));
    return Status::OK();
}

Status Slot::RunPreloadCallback(const SlotTakeoverPlan &plan, const SlotTakeoverRequest &request) const
{
    if (!request.IsPreload() || !request.callback) {
        return Status::OK();
    }
    std::vector<SlotRecord> importRecords;
    size_t validBytes = 0;
    RETURN_IF_NOT_OK(
        SlotIndexCodec::ReadAllRecords(JoinPath(slotPath_, plan.importIndexFile), importRecords, validBytes));
    std::vector<SlotPutRecord> visiblePuts;
    visiblePuts.reserve(importRecords.size());
    for (const auto &record : importRecords) {
        if (record.type != SlotRecordType::PUT) {
            continue;
        }
        visiblePuts.emplace_back(record.put);
    }
    return RunPreloadCallback(visiblePuts, request.callback);
}

Status Slot::RunPreloadCallback(const std::vector<SlotPutRecord> &visiblePuts,
                                const SlotPreloadCallback &callback) const
{
    if (!callback) {
        return Status::OK();
    }
    for (const auto &put : visiblePuts) {
        SlotSnapshotValue value{ put.key, put.fileId, put.offset, put.size, put.version, put.writeMode, false };
        auto content = std::make_shared<std::stringstream>();
        RETURN_IF_NOT_OK(ReadRecordData(value, content));

        SlotPreloadMeta meta;
        meta.objectKey = put.key;
        meta.version = put.version;
        meta.writeMode = put.writeMode;
        meta.size = put.size;
        meta.ttlSecond = put.ttlSecond;

        auto rc = callback(meta, content);
        if (rc.IsError()) {
            LOG(WARNING) << "Preload callback stopped early, slotPath=" << slotPath_ << ", objectKey=" << meta.objectKey
                         << ", version=" << meta.version << ", code=" << Status::StatusCodeName(rc.GetCode())
                         << ", msg=" << rc.GetMsg();
            if (rc.GetCode() == StatusCode::K_OUT_OF_MEMORY) {
                LOG(WARNING) << "Preload callback stopped by out-of-memory, disk takeover continues.";
            }
            break;
        }
    }
    return Status::OK();
}

Status Slot::CheckCompactTransferPreemptionLocked() const
{
    CHECK_FAIL_RETURN_STATUS(!transferIntentActive_.load(), StatusCode::K_TRY_AGAIN,
                             "Compact preempted by transfer intent");
    return Status::OK();
}

Status Slot::ReadIndexDeltaRecords(const std::string &indexPath, size_t startOffset, size_t endOffset,
                                   std::vector<SlotRecord> &records, size_t &deltaBytes) const
{
    records.clear();
    deltaBytes = 0;
    CHECK_FAIL_RETURN_STATUS(endOffset >= startOffset, StatusCode::K_INVALID, "Invalid compact delta frontier range");
    if (endOffset == startOffset) {
        return Status::OK();
    }

    std::vector<SlotRecordFrame> frames;
    size_t validBytes = 0;
    RETURN_IF_NOT_OK(SlotIndexCodec::ReadAllRecordFrames(indexPath, frames, validBytes));
    CHECK_FAIL_RETURN_STATUS(endOffset <= validBytes, StatusCode::K_RUNTIME_ERROR,
                             "Compact frontier exceeds validated active index bytes");
    for (const auto &frame : frames) {
        if (frame.endOffset <= startOffset) {
            continue;
        }
        if (frame.startOffset >= endOffset) {
            break;
        }
        CHECK_FAIL_RETURN_STATUS(frame.startOffset >= startOffset && frame.endOffset <= endOffset,
                                 StatusCode::K_RUNTIME_ERROR, "Compact delta frontier split one index record");
        records.emplace_back(frame.record);
    }
    deltaBytes = endOffset - startOffset;
    return Status::OK();
}

Status Slot::ResetTransferTargetManifest(SlotManifestData &manifest)
{
    CHECK_FAIL_RETURN_STATUS(manifest.role == SlotOperationRole::TARGET, StatusCode::K_INVALID,
                             "ResetTransferTargetManifest requires target transfer manifest");
    manifest.state = SlotState::NORMAL;
    manifest.opType = SlotOperationType::NONE;
    manifest.opPhase = SlotOperationPhase::NONE;
    manifest.role = SlotOperationRole::NONE;
    manifest.txnId.clear();
    manifest.peerSlotPath.clear();
    manifest.recoverySlotPath.clear();
    manifest.transferPlanPath.clear();
    manifest.transferFileMap.clear();
    return PersistManifest(manifest);
}

Status Slot::CleanupTransferArtifacts(const SlotManifestData &manifest, const SlotTakeoverPlan *plan)
{
    std::vector<std::string> relativePaths;
    if (!manifest.txnId.empty()) {
        relativePaths.emplace_back(FormatImportIndexFileName(manifest.txnId));
    }
    if (!manifest.transferPlanPath.empty()) {
        relativePaths.emplace_back(BaseName(manifest.transferPlanPath));
    }

    std::unordered_map<uint32_t, uint32_t> fileIdMap;
    if (plan == nullptr) {
        RETURN_IF_NOT_OK(DecodeTransferFileMap(manifest.transferFileMap, fileIdMap));
        for (const auto &entry : fileIdMap) {
            const auto targetDataFile = FormatDataFileName(entry.second);
            if (std::find(manifest.activeData.begin(), manifest.activeData.end(), targetDataFile)
                == manifest.activeData.end()) {
                relativePaths.emplace_back(targetDataFile);
            }
        }
    } else {
        for (const auto &mapping : plan->dataMappings) {
            if (std::find(manifest.activeData.begin(), manifest.activeData.end(), mapping.targetDataFile)
                == manifest.activeData.end()) {
                relativePaths.emplace_back(mapping.targetDataFile);
            }
        }
    }
    return CleanupArtifactFiles(relativePaths);
}

Status Slot::LoadRecoveryTransferState(const std::string &sourceRecoveryPath, SlotManifestData &sourceManifest,
                                       SlotSnapshot &sourceSnapshot)
{
    RETURN_IF_NOT_OK(SlotManifest::Load(sourceRecoveryPath, sourceManifest));
    RETURN_IF_NOT_OK(SlotSnapshot::Replay(sourceRecoveryPath, sourceManifest, sourceSnapshot));
    return Status::OK();
}

Status Slot::RebuildPreparedTransferPlan(SlotManifestData &manifest, SlotTakeoverPlan &plan)
{
    plan = SlotTakeoverPlan{};
    if (FileExist(manifest.recoverySlotPath)) {
        SlotManifestData sourceManifest;
        SlotSnapshot sourceSnapshot;
        RETURN_IF_NOT_OK(LoadRecoveryTransferState(manifest.recoverySlotPath, sourceManifest, sourceSnapshot));
        SlotTakeoverRequest rebuildRequest;
        rebuildRequest.mode = SlotTakeoverMode::MERGE;
        RETURN_IF_NOT_OK(SlotTakeoverPlanner::BuildPlan(manifest.peerSlotPath, manifest.recoverySlotPath,
                                                        sourceManifest, sourceSnapshot, slotPath_, manifest,
                                                        rebuildRequest, manifest.txnId, plan));

        std::unordered_map<uint32_t, uint32_t> fileIdMap;
        RETURN_IF_NOT_OK(DecodeTransferFileMap(manifest.transferFileMap, fileIdMap));
        if (!fileIdMap.empty()) {
            for (auto &mapping : plan.dataMappings) {
                auto it = fileIdMap.find(mapping.sourceFileId);
                CHECK_FAIL_RETURN_STATUS(it != fileIdMap.end(), StatusCode::K_INVALID,
                                         "Missing source file mapping while rebuilding transfer plan");
                mapping.targetFileId = it->second;
                mapping.targetDataFile = FormatDataFileName(mapping.targetFileId);
            }

            auto importIndexPath = JoinPath(slotPath_, plan.importIndexFile);
            RETURN_IF_NOT_OK(SlotIndexCodec::EnsureIndexFile(importIndexPath));
            RETURN_IF_NOT_OK(SlotIndexCodec::TruncateTail(importIndexPath, SlotIndexCodec::HEADER_SIZE));

            std::vector<SlotPutRecord> visiblePuts;
            RETURN_IF_NOT_OK(sourceSnapshot.CollectVisiblePuts(visiblePuts));
            std::string importPayload;
            importPayload.reserve(visiblePuts.size() * 64);
            for (const auto &record : visiblePuts) {
                auto it = fileIdMap.find(record.fileId);
                CHECK_FAIL_RETURN_STATUS(it != fileIdMap.end(), StatusCode::K_INVALID,
                                         "Missing target file mapping for rebuilt visible put");
                auto imported = record;
                imported.fileId = it->second;
                std::string encoded;
                RETURN_IF_NOT_OK(SlotIndexCodec::EncodePut(imported, encoded));
                importPayload.append(encoded);
            }
            RETURN_IF_NOT_OK(SlotIndexCodec::AppendEncodedRecords(importIndexPath, importPayload, true));
        }

        RETURN_IF_NOT_OK(SlotTakeoverPlanner::DumpPlan(slotPath_, plan));
        manifest.transferPlanPath = FormatTakeoverPlanFileName(plan.txnId);
        manifest.transferFileMap = EncodeTransferFileMap(plan);
        RETURN_IF_NOT_OK(PersistManifest(manifest));
        return Status::OK();
    }

    auto importIndexFile = FormatImportIndexFileName(manifest.txnId);
    CHECK_FAIL_RETURN_STATUS(FileExist(JoinPath(slotPath_, importIndexFile)), StatusCode::K_NOT_FOUND,
                             "Cannot rebuild transfer plan without source recovery or import index");
    std::unordered_map<uint32_t, uint32_t> fileIdMap;
    RETURN_IF_NOT_OK(DecodeTransferFileMap(manifest.transferFileMap, fileIdMap));

    plan.txnId = manifest.txnId;
    plan.sourceHomeSlotPath = manifest.peerSlotPath;
    plan.sourceRecoverySlotPath = manifest.recoverySlotPath;
    plan.targetSlotPath = slotPath_;
    plan.importIndexFile = importIndexFile;
    for (const auto &entry : fileIdMap) {
        SlotTakeoverDataFileMapping mapping;
        mapping.sourceFileId = entry.first;
        mapping.sourceDataFile = FormatDataFileName(entry.first);
        mapping.targetFileId = entry.second;
        mapping.targetDataFile = FormatDataFileName(entry.second);
        plan.dataMappings.emplace_back(std::move(mapping));
    }
    std::sort(plan.dataMappings.begin(), plan.dataMappings.end(),
              [](const SlotTakeoverDataFileMapping &lhs, const SlotTakeoverDataFileMapping &rhs) {
                  return lhs.sourceFileId < rhs.sourceFileId;
              });
    RETURN_IF_NOT_OK(SlotTakeoverPlanner::DumpPlan(slotPath_, plan));
    manifest.transferPlanPath = FormatTakeoverPlanFileName(plan.txnId);
    RETURN_IF_NOT_OK(PersistManifest(manifest));
    return Status::OK();
}

bool Slot::AreTransferTargetDataReady(const SlotTakeoverPlan &plan) const
{
    return std::all_of(plan.dataMappings.begin(), plan.dataMappings.end(),
                       [this](const auto &mapping) { return FileExist(JoinPath(slotPath_, mapping.targetDataFile)); });
}

Status Slot::PublishImportBatch(const std::string &indexPath, const std::string &txnId,
                                const std::string &importIndexFile, bool &published)
{
    published = false;
    VLOG(1) << "Publishing slot import batch, slotId=" << slotId_ << ", txnId=" << txnId
            << ", activeIndexPath=" << indexPath << ", importIndexFile=" << importIndexFile;
    std::vector<SlotRecord> currentRecords;
    size_t validBytes = 0;
    RETURN_IF_NOT_OK(SlotIndexCodec::ReadAllRecords(indexPath, currentRecords, validBytes));
    if (HasClosedImportBatch(currentRecords, txnId)) {
        published = true;
        return Status::OK();
    }

    std::string payload;
    RETURN_IF_NOT_OK(SlotIndexCodec::EncodeImportBegin(SlotImportRecord{ txnId }, payload));
    INJECT_POINT("slotstore.Slot.Takeover.AfterImportBegin", []() { return Status::OK(); });
    std::string importContent;
    RETURN_IF_NOT_OK(ReadWholeFile(JoinPath(slotPath_, importIndexFile), importContent));
    CHECK_FAIL_RETURN_STATUS(importContent.size() >= SlotIndexCodec::HEADER_SIZE, StatusCode::K_RUNTIME_ERROR,
                             "Import index content is shorter than header");
    payload.append(importContent.data() + SlotIndexCodec::HEADER_SIZE,
                   importContent.size() - SlotIndexCodec::HEADER_SIZE);
    INJECT_POINT("slotstore.Slot.Takeover.BeforeImportEnd", []() { return Status::OK(); });
    std::string importEndPayload;
    RETURN_IF_NOT_OK(SlotIndexCodec::EncodeImportEnd(SlotImportRecord{ txnId }, importEndPayload));
    payload.append(importEndPayload);
    RETURN_IF_NOT_OK(SlotIndexCodec::AppendEncodedRecords(indexPath, payload, true));
    RETURN_IF_NOT_OK(FsyncDir(slotPath_));
    published = true;
    VLOG(1) << "Published slot import batch successfully, slotId=" << slotId_ << ", txnId=" << txnId
            << ", payloadBytes=" << payload.size();
    return Status::OK();
}

Status Slot::PersistSourceTransferManifest(const std::string &sourceRecoveryPath,
                                           const SlotManifestData &targetManifest, const std::string &sourceHomePath,
                                           SlotOperationPhase phase) const
{
    CHECK_FAIL_RETURN_STATUS(!sourceRecoveryPath.empty(), StatusCode::K_INVALID,
                             "sourceRecoveryPath must not be empty");
    SlotManifestData sourceManifest;
    auto rc = SlotManifest::Load(sourceRecoveryPath, sourceManifest);
    if (rc.GetCode() == StatusCode::K_NOT_FOUND) {
        sourceManifest = SlotManifest::Bootstrap();
        sourceManifest.homeSlotPath = sourceHomePath;
        sourceManifest.activeIndex = "index_active.log";
        sourceManifest.activeData = { FormatDataFileName(1) };
    } else {
        RETURN_IF_NOT_OK(rc);
    }
    sourceManifest.state = SlotState::IN_OPERATION;
    sourceManifest.opType = SlotOperationType::TRANSFER;
    sourceManifest.opPhase = phase;
    sourceManifest.role = SlotOperationRole::SOURCE;
    sourceManifest.txnId = targetManifest.txnId;
    sourceManifest.ownerEpoch = targetManifest.ownerEpoch;
    sourceManifest.homeSlotPath = sourceHomePath;
    sourceManifest.recoverySlotPath = sourceRecoveryPath;
    sourceManifest.peerSlotPath = slotPath_;
    sourceManifest.transferPlanPath = targetManifest.transferPlanPath;
    sourceManifest.transferFileMap = targetManifest.transferFileMap;
    RETURN_IF_NOT_OK(SlotManifest::StoreAtomic(sourceRecoveryPath, sourceManifest));
    RETURN_IF_NOT_OK(FsyncDir(sourceRecoveryPath));
    return Status::OK();
}

Status Slot::FinalizeSourceAfterTakeover(const std::string &sourceRecoveryPath, const SlotManifestData &targetManifest)
{
    if (!FileExist(sourceRecoveryPath)) {
        return Status::OK();
    }
    VLOG(1) << "Finalizing slot source after takeover, slotId=" << slotId_
            << ", sourceRecoveryPath=" << sourceRecoveryPath << ", txnId=" << targetManifest.txnId;
    RETURN_IF_NOT_OK(PersistSourceTransferManifest(sourceRecoveryPath, targetManifest, targetManifest.peerSlotPath,
                                                   SlotOperationPhase::TRANSFER_SOURCE_RETIRED));
    INJECT_POINT("slotstore.Slot.Takeover.BeforeSourceFinalize", []() { return Status::OK(); });
    if (FileExist(sourceRecoveryPath)) {
        RETURN_IF_NOT_OK(RemoveAll(sourceRecoveryPath));
    }
    return Status::OK();
}
}  // namespace datasystem
