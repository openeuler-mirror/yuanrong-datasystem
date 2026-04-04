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
 * Description: Slot compact artifact builder.
 */

#include "datasystem/common/l2cache/slot_client/slot_compactor.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

#include <fcntl.h>
#include <unistd.h>

#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {
std::string BaseName(const std::string &path)
{
    auto pos = path.find_last_of('/');
    return pos == std::string::npos ? path : path.substr(pos + 1);
}
}  // namespace

SlotCompactor::SlotCompactor(std::string slotPath, uint64_t maxDataFileBytes)
    : slotPath_(std::move(slotPath)), maxDataFileBytes_(maxDataFileBytes)
{
}

Status SlotCompactor::BuildArtifacts(const SlotManifestData &manifest, const SlotSnapshot &snapshot,
                                     uint64_t compactEpochMs, SlotCompactBuildResult &result)
{
    result = SlotCompactBuildResult{};
    SlotCompactBuildResult buildResult;
    bool keepArtifacts = false;
    Raii cleanupOnFailure([this, &buildResult, &keepArtifacts]() {
        if (!keepArtifacts) {
            (void)CleanupArtifacts(buildResult);
        }
    });

    RETURN_IF_NOT_OK(AllocateCompactIndexFile(manifest, compactEpochMs, buildResult.indexFile));
    auto indexPath = JoinPath(slotPath_, buildResult.indexFile);
    RETURN_IF_NOT_OK(SlotIndexCodec::EnsureIndexFile(indexPath));
    int indexFd = -1;
    RETURN_IF_NOT_OK(OpenFile(indexPath, O_RDWR, &indexFd));
    Raii closeIndexFd([&indexFd]() {
        if (indexFd >= 0) {
            close(indexFd);
            indexFd = -1;
        }
    });
    uint64_t indexOffset = static_cast<uint64_t>(FdFileSize(indexFd));

    std::vector<SlotPutRecord> visiblePuts;
    RETURN_IF_NOT_OK(snapshot.CollectVisiblePuts(visiblePuts));
    std::sort(visiblePuts.begin(), visiblePuts.end(), [](const SlotPutRecord &lhs, const SlotPutRecord &rhs) {
        if (lhs.fileId != rhs.fileId) {
            return lhs.fileId < rhs.fileId;
        }
        if (lhs.offset != rhs.offset) {
            return lhs.offset < rhs.offset;
        }
        if (lhs.key != rhs.key) {
            return lhs.key < rhs.key;
        }
        return lhs.version < rhs.version;
    });

    uint32_t nextFileId = 0;
    RETURN_IF_NOT_OK(GetNextDataFileId(manifest, nextFileId));
    auto openTargetDataFile = [this, &buildResult](uint32_t fileId, int &fd, uint64_t &fileSize) -> Status {
        if (fd >= 0) {
            RETURN_IF_NOT_OK(FsyncFd(fd));
            close(fd);
            fd = -1;
        }
        auto filename = FormatDataFileName(fileId);
        buildResult.dataFiles.emplace_back(filename);
        auto dataPath = JoinPath(slotPath_, filename);
        RETURN_IF_NOT_OK(EnsureFile(dataPath));
        RETURN_IF_NOT_OK(OpenFile(dataPath, O_RDWR, &fd));
        fileSize = static_cast<uint64_t>(FdFileSize(fd));
        return Status::OK();
    };

    int currentTargetDataFd = -1;
    Raii closeTargetDataFd([&currentTargetDataFd]() {
        if (currentTargetDataFd >= 0) {
            close(currentTargetDataFd);
            currentTargetDataFd = -1;
        }
    });
    uint64_t currentTargetFileSize = 0;
    RETURN_IF_NOT_OK(openTargetDataFile(nextFileId, currentTargetDataFd, currentTargetFileSize));
    if (visiblePuts.empty()) {
        RETURN_IF_NOT_OK(FsyncFd(indexFd));
        RETURN_IF_NOT_OK(FsyncFd(currentTargetDataFd));
        RETURN_IF_NOT_OK(FsyncDir(slotPath_));
        keepArtifacts = true;
        result = buildResult;
        return Status::OK();
    }

    std::unordered_map<uint32_t, int> sourceDataFds;
    Raii closeSourceFds([&sourceDataFds]() {
        for (auto &entry : sourceDataFds) {
            if (entry.second >= 0) {
                close(entry.second);
            }
        }
    });
    std::string indexBuffer;
    indexBuffer.reserve(1024 * 1024);
    for (const auto &record : visiblePuts) {
        auto fdIt = sourceDataFds.find(record.fileId);
        if (fdIt == sourceDataFds.end()) {
            int sourceFd = -1;
            RETURN_IF_NOT_OK(OpenFile(JoinPath(slotPath_, FormatDataFileName(record.fileId)), O_RDONLY, &sourceFd));
            fdIt = sourceDataFds.emplace(record.fileId, sourceFd).first;
        }
        std::string payload(record.size, '\0');
        RETURN_IF_NOT_OK(ReadFile(fdIt->second, payload.data(), record.size, static_cast<off_t>(record.offset)));
        if (currentTargetFileSize + payload.size() > maxDataFileBytes_ && currentTargetFileSize > 0) {
            ++nextFileId;
            RETURN_IF_NOT_OK(openTargetDataFile(nextFileId, currentTargetDataFd, currentTargetFileSize));
        }
        auto offset = currentTargetFileSize;
        RETURN_IF_NOT_OK(WriteFile(currentTargetDataFd, payload.data(), payload.size(), static_cast<off_t>(offset)));
        SlotPutRecord compacted = record;
        compacted.fileId = nextFileId;
        compacted.offset = offset;
        std::string encoded;
        RETURN_IF_NOT_OK(SlotIndexCodec::EncodePut(compacted, encoded));
        indexBuffer.append(encoded);
        if (indexBuffer.size() >= 1024 * 1024) {
            RETURN_IF_NOT_OK(SlotIndexCodec::AppendEncodedRecords(indexFd, indexOffset, indexBuffer));
            indexBuffer.clear();
        }
        currentTargetFileSize = offset + payload.size();
    }
    RETURN_IF_NOT_OK(SlotIndexCodec::AppendEncodedRecords(indexFd, indexOffset, indexBuffer));
    RETURN_IF_NOT_OK(FsyncFd(indexFd));
    RETURN_IF_NOT_OK(FsyncFd(currentTargetDataFd));
    RETURN_IF_NOT_OK(FsyncDir(slotPath_));
    keepArtifacts = true;
    result = buildResult;
    return Status::OK();
}

Status SlotCompactor::ApplyDeltaRecords(const std::vector<SlotRecord> &records, SlotCompactBuildResult &result)
{
    CHECK_FAIL_RETURN_STATUS(!result.indexFile.empty(), StatusCode::K_INVALID,
                             "Compact build result missing pending index file");
    if (records.empty()) {
        return Status::OK();
    }

    auto indexPath = JoinPath(slotPath_, result.indexFile);
    RETURN_IF_NOT_OK(SlotIndexCodec::EnsureIndexFile(indexPath));
    int indexFd = -1;
    RETURN_IF_NOT_OK(OpenFile(indexPath, O_RDWR, &indexFd));
    Raii closeIndexFd([&indexFd]() {
        if (indexFd >= 0) {
            close(indexFd);
            indexFd = -1;
        }
    });
    uint64_t indexOffset = static_cast<uint64_t>(FdFileSize(indexFd));

    uint32_t nextFileId = 0;
    if (!result.dataFiles.empty()) {
        RETURN_IF_NOT_OK(ParseDataFileId(result.dataFiles.back(), nextFileId));
    }

    auto openTargetDataFile = [this, &result](uint32_t fileId, int &fd, uint64_t &fileSize) -> Status {
        if (fd >= 0) {
            RETURN_IF_NOT_OK(FsyncFd(fd));
            close(fd);
            fd = -1;
        }
        auto filename = FormatDataFileName(fileId);
        if (result.dataFiles.empty() || result.dataFiles.back() != filename) {
            result.dataFiles.emplace_back(filename);
        }
        auto dataPath = JoinPath(slotPath_, filename);
        RETURN_IF_NOT_OK(EnsureFile(dataPath));
        RETURN_IF_NOT_OK(OpenFile(dataPath, O_RDWR, &fd));
        fileSize = static_cast<uint64_t>(FdFileSize(fd));
        return Status::OK();
    };

    int currentTargetDataFd = -1;
    Raii closeTargetDataFd([&currentTargetDataFd]() {
        if (currentTargetDataFd >= 0) {
            close(currentTargetDataFd);
            currentTargetDataFd = -1;
        }
    });
    uint64_t currentTargetFileSize = 0;
    if (!result.dataFiles.empty()) {
        RETURN_IF_NOT_OK(ParseDataFileId(result.dataFiles.back(), nextFileId));
        RETURN_IF_NOT_OK(openTargetDataFile(nextFileId, currentTargetDataFd, currentTargetFileSize));
    }

    std::unordered_map<uint32_t, int> sourceDataFds;
    Raii closeSourceFds([&sourceDataFds]() {
        for (auto &entry : sourceDataFds) {
            if (entry.second >= 0) {
                close(entry.second);
            }
        }
    });

    std::string indexBuffer;
    indexBuffer.reserve(1024 * 1024);
    for (const auto &record : records) {
        switch (record.type) {
            case SlotRecordType::PUT: {
                auto fdIt = sourceDataFds.find(record.put.fileId);
                if (fdIt == sourceDataFds.end()) {
                    int sourceFd = -1;
                    RETURN_IF_NOT_OK(
                        OpenFile(JoinPath(slotPath_, FormatDataFileName(record.put.fileId)), O_RDONLY, &sourceFd));
                    fdIt = sourceDataFds.emplace(record.put.fileId, sourceFd).first;
                }
                std::string payload(record.put.size, '\0');
                RETURN_IF_NOT_OK(
                    ReadFile(fdIt->second, payload.data(), record.put.size, static_cast<off_t>(record.put.offset)));
                if (currentTargetDataFd < 0) {
                    nextFileId = nextFileId == 0 ? 1 : nextFileId;
                    RETURN_IF_NOT_OK(openTargetDataFile(nextFileId, currentTargetDataFd, currentTargetFileSize));
                }
                if (currentTargetFileSize + payload.size() > maxDataFileBytes_ && currentTargetFileSize > 0) {
                    ++nextFileId;
                    RETURN_IF_NOT_OK(openTargetDataFile(nextFileId, currentTargetDataFd, currentTargetFileSize));
                }
                auto offset = currentTargetFileSize;
                RETURN_IF_NOT_OK(
                    WriteFile(currentTargetDataFd, payload.data(), payload.size(), static_cast<off_t>(offset)));
                SlotPutRecord compacted = record.put;
                compacted.fileId = nextFileId;
                compacted.offset = offset;
                std::string encoded;
                RETURN_IF_NOT_OK(SlotIndexCodec::EncodePut(compacted, encoded));
                indexBuffer.append(encoded);
                currentTargetFileSize = offset + payload.size();
                break;
            }
            case SlotRecordType::DELETE: {
                std::string encoded;
                RETURN_IF_NOT_OK(SlotIndexCodec::EncodeDelete(record.del, encoded));
                indexBuffer.append(encoded);
                break;
            }
            case SlotRecordType::IMPORT_BEGIN:
            case SlotRecordType::IMPORT_END:
                RETURN_STATUS(StatusCode::K_TRY_AGAIN, "Compact delta stream hit concurrent import records");
        }
        if (indexBuffer.size() >= 1024 * 1024) {
            RETURN_IF_NOT_OK(SlotIndexCodec::AppendEncodedRecords(indexFd, indexOffset, indexBuffer));
            indexBuffer.clear();
        }
    }

    RETURN_IF_NOT_OK(SlotIndexCodec::AppendEncodedRecords(indexFd, indexOffset, indexBuffer));
    RETURN_IF_NOT_OK(FsyncFd(indexFd));
    if (currentTargetDataFd >= 0) {
        RETURN_IF_NOT_OK(FsyncFd(currentTargetDataFd));
    }
    RETURN_IF_NOT_OK(FsyncDir(slotPath_));
    return Status::OK();
}

Status SlotCompactor::CleanupArtifacts(const SlotCompactBuildResult &result)
{
    if (!result.indexFile.empty() && FileExist(JoinPath(slotPath_, result.indexFile))) {
        (void)DeleteFile(JoinPath(slotPath_, result.indexFile));
    }
    for (const auto &dataFile : result.dataFiles) {
        if (FileExist(JoinPath(slotPath_, dataFile))) {
            (void)DeleteFile(JoinPath(slotPath_, dataFile));
        }
    }
    return Status::OK();
}

Status SlotCompactor::AllocateCompactIndexFile(const SlotManifestData &manifest, uint64_t compactEpochMs,
                                               std::string &indexFile) const
{
    std::unordered_set<std::string> reservedFiles;
    reservedFiles.insert(manifest.activeIndex);
    if (!manifest.pendingIndex.empty()) {
        reservedFiles.insert(manifest.pendingIndex);
    }
    if (!manifest.obsoleteIndex.empty()) {
        reservedFiles.insert(manifest.obsoleteIndex);
    }
    uint64_t candidateEpochMs = compactEpochMs;
    for (size_t attempt = 0; attempt < 1024; ++attempt, ++candidateEpochMs) {
        auto candidate = FormatCompactIndexFileName(candidateEpochMs);
        if (reservedFiles.count(candidate) > 0 || FileExist(JoinPath(slotPath_, candidate))) {
            continue;
        }
        indexFile = candidate;
        return Status::OK();
    }
    RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "No available compact index file for build");
}

Status SlotCompactor::GetNextDataFileId(const SlotManifestData &manifest, uint32_t &nextFileId) const
{
    std::vector<std::string> dataPaths;
    RETURN_IF_NOT_OK(Glob(JoinPath(slotPath_, "data_*.bin"), dataPaths));
    uint32_t maxFileId = 0;
    auto updateMaxId = [&maxFileId](const std::string &relativeName) {
        uint32_t fileId = 0;
        auto rc = ParseDataFileId(relativeName, fileId);
        if (rc.IsOk()) {
            maxFileId = std::max(maxFileId, fileId);
        }
    };
    for (const auto &dataPath : dataPaths) {
        updateMaxId(BaseName(dataPath));
    }
    for (const auto &dataFile : manifest.activeData) {
        updateMaxId(dataFile);
    }
    for (const auto &dataFile : manifest.pendingData) {
        updateMaxId(dataFile);
    }
    for (const auto &dataFile : manifest.obsoleteData) {
        updateMaxId(dataFile);
    }
    nextFileId = maxFileId == 0 ? 1 : maxFileId + 1;
    return Status::OK();
}
}  // namespace datasystem
