/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Object spilling implementation.
 */
#include "datasystem/worker/object_cache/worker_oc_spill.h"

#include <iterator>
#include <limits>
#include <shared_mutex>
#include <string>
#include <utility>

#include <dirent.h>
#include <fcntl.h>
#include <linux/falloc.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/utils/status.h"

// Default spill directory under the worker workspace
DS_DEFINE_string(spill_directory, "",
                 "The path and file name prefix of the spilling, empty means spill disabled. The path length must less "
                 "than 4095 characters. If spill_type is local_disk, this para must be set.");
DS_DEFINE_validator(spill_directory, &Validator::ValidatePathString);
DS_DEFINE_uint64(spill_size_limit, 0,
                 "Maximum amount of spilled data that can be stored in the spill directory. If spill is enable and "
                 "spill_size_limit is 0, spill_size_limit will be set to 95% of the spill directory.");
DS_DEFINE_uint64(
    spill_file_max_size_mb, 200,
    "The size limit of single spill file, spilling objects which lager than that value with one object per "
    "file. If there are some big objects, you can increase this value to avoid run out of inodes quickly. "
    "The valid range is 200-10240.");
DS_DEFINE_validator(spill_file_max_size_mb, &Validator::ValidateSpillFileMaxSize);
DS_DEFINE_uint32(
    spill_thread_num, 8,
    "It represents the maximum parallelism of writing files, more threads will consume more CPU and I/O resources.");
DS_DEFINE_validator(spill_thread_num, &Validator::ValidateThreadNum);
DS_DEFINE_uint64(
    spill_file_open_limit, 512,
    "The maximum number of open file descriptors about spill. If opened file exceed this value, some files "
    "will be temporarily closed to prevent exceeding the maximum system limit. You need reduce this value if "
    "your system resources are limited. The valid range is greater than or equal to 8.");
DS_DEFINE_validator(spill_file_open_limit, &Validator::ValidateSpillOpenFileLimit);
DS_DEFINE_bool(spill_enable_readahead, true,
               "Disable readahead can mitigate the read amplification problem for offset read, default is true");

namespace datasystem {
namespace object_cache {
constexpr int SPILL_FILE_MODE = 0600;
constexpr size_t MB = 1024 * 1024;
constexpr int MAX_OPEN_RETRY = 10;
constexpr int BACKOFF_TIME_MS = 200;
constexpr int INVALID_FD = -1;
constexpr int PERMISSION = 0700;

// Total spill file disk size
std::atomic<uint64_t> totalSpillFileDiskSize{ 0 };
std::atomic<int64_t> openFdCount{ 0 };

SpillBuffer::SpillBuffer()
{
    data_.reserve(SpillFileManager::LARGE_OBJ_SIZE_THRESHOLD);
}

void SpillBuffer::Append(const std::string &objectKey, const char *input, size_t size)
{
    PerfPoint point(PerfKey::WORKER_SPILL_APPEND_BUFFER);
    size_t offset = data_.size();
    (void)index_.emplace(objectKey, std::pair<size_t, size_t>{ offset, size });
    (void)data_.append(input, size);
    VLOG(1) << "Append buffer " << objectKey << ", offset " << offset << ", size " << size;
}

void SpillBuffer::Remove(const std::string &objectKey)
{
    (void)index_.erase(objectKey);
}

Status SpillBuffer::CopyTo(const std::string &objectKey, const char *output, size_t size, size_t offset)
{
    PerfPoint point(PerfKey::WORKER_SPILL_COPY_FROM_BUFFER);
    const auto &offSize = index_[objectKey];
    size_t offsetInBuffer = offSize.first;
    size_t sz = offSize.second;
    // 1. check read range in object is valid.
    // 2. check object range in Buffer is valid.
    CHECK_FAIL_RETURN_STATUS(
        size + offset <= sz && size > 0 && offsetInBuffer + sz <= data_.size(), StatusCode::K_RUNTIME_ERROR,
        FormatString(
            "Invalid size, objectKey=%s, need size=%ld, real size=%ld, offset=%ld, offsetInBuffer=%ld, bufferSize=%ld",
            objectKey, size, sz, offset, offsetInBuffer, data_.size()));
    LOG(INFO) << FormatString(
        "Copy data from spill buffer, objectKey=%s, need size=%ld, real size=%ld, offset=%ld, offsetInBuffer=%ld, "
        "bufferSize=%ld",
        objectKey, size, sz, offset, offsetInBuffer, data_.size());
    int ret = memcpy_s((void *)output, size, data_.data() + offset + offsetInBuffer, size);
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Copy data failed, memcpy_s return %d, oobjectKey=%s, need size=%ld, real "
                                          "size=%ld, offset=%ld, offsetInBuffer=%ld, "
                                          "bufferSize=%ld",
                                          objectKey, size, sz, offset, offsetInBuffer, data_.size()));
    return Status::OK();
}

Status SpillBuffer::CopyTo(const std::string &objectKey, std::vector<RpcMessage> &messages, size_t size, size_t offset)
{
    PerfPoint point(PerfKey::WORKER_SPILL_COPY_BUFFER_TO_MESSAGE);
    const auto &offSize = index_[objectKey];
    size_t offsetInBuffer = offSize.first;
    size_t sz = offSize.second;
    CHECK_FAIL_RETURN_STATUS(
        size + offset <= sz && size > 0 && offsetInBuffer + sz <= data_.size(), StatusCode::K_RUNTIME_ERROR,
        FormatString(
            "Invalid size, objectKey=%s, need size=%ld, real size=%ld, offset=%ld, offsetInBuffer=%ld, bufferSize=%ld",
            objectKey, size, sz, offset, offsetInBuffer, data_.size()));
    LOG(INFO) << FormatString(
        "Copy data from spill buffer, objectKey=%s, need size=%ld, real size=%ld, offset=%ld, offsetInBuffer=%ld, "
        "bufferSize=%ld",
        objectKey, size, sz, offset, offsetInBuffer, data_.size());
    return CopyAndSplitBuffer(TenantAuthManager::ExtractTenantId(objectKey), data_.data() + offset + offsetInBuffer,
                              size, messages);
}

bool SpillBuffer::Exist(const std::string &objectKey, uint64_t &size)
{
    auto iter = index_.find(objectKey);
    if (iter == index_.end()) {
        return false;
    }
    size = iter->second.second;
    return true;
}

bool SpillBuffer::UniqObject(const std::string &objectKey)
{
    return index_.count(objectKey) > 0 && index_.size() == 1;
}

size_t SpillBuffer::Size()
{
    return data_.size();
}

const char *SpillBuffer::GetData()
{
    return data_.data();
}

const std::unordered_map<std::string, std::pair<size_t, size_t>> &SpillBuffer::GetIndex()
{
    return index_;
}

void SpillBuffer::Reset()
{
    data_.clear();
    index_.clear();
}

void SpillBuffer::CloneTo(SpillBuffer &buffer)
{
    for (const auto &kv : index_) {
        const auto &objectKey = kv.first;
        auto offset = kv.second.first;
        auto size = kv.second.second;
        buffer.Append(objectKey, data_.data() + offset, size);
    }
}

Status ActiveSpillFile::Read(void *buffer, size_t count, off_t offset)
{
    return ReadFile(fd_, buffer, count, offset);
}

Status ActiveSpillFile::ReadToRpcMessage(size_t size, size_t offset, std::vector<RpcMessage> &messages)
{
    const size_t maxInt = std::numeric_limits<int32_t>::max();
    while (size > 0) {
        size_t bufSize = std::min(size, maxInt);
        messages.emplace_back();
        RETURN_IF_NOT_OK(messages.back().Resize(bufSize));
        PerfPoint p(PerfKey::WORKER_SPILL_READ_FILE);
        RETURN_IF_NOT_OK(ReadFile(fd_, messages.back().Data(), bufSize, offset));
        p.Record();
        size -= bufSize;
        offset += bufSize;
    }
    return Status::OK();
}

Status ActiveSpillFile::Write(const void *buffer, size_t count, off_t offset)
{
    INJECT_POINT("worker.Spill.Write");
    return WriteFile(fd_, buffer, count, offset);
}

void ActiveSpillFile::Sync()
{
    INJECT_POINT("worker.Spill.Sync", [] {});
    (void)syncfs(fd_);
}

int ActiveSpillFile::GetFd()
{
    return fd_;
}

ActiveSpillFile::~ActiveSpillFile()
{
    if (fd_ == INVALID_FD) {
        return;
    }
    RETRY_ON_EINTR(close(fd_));
    openFdCount--;
    if (openFdCount < 0) {
        openFdCount = 0;
    }
    VLOG(1) << FormatString("Close fd %d success, openFdCount: %ld", fd_, openFdCount.load());
    fd_ = INVALID_FD;
}

SpillFileManager::~SpillFileManager()
{
    // Close all Fd and delete file
    for (auto &tenantInfo : tenant2FileInfo_) {
        for (auto &fileInfo : tenantInfo.second) {
            if (fileInfo.second.file != nullptr) {
                fileInfo.second.file = nullptr;
            }
            VLOG(1) << "Delete file in deconstruct: " << fileInfo.first << ", id:" << id_;
            auto status = DeleteFile(fileInfo.first);
            if (status.IsError()) {
                LOG(ERROR) << "Delete file " << fileInfo.first << " failed, status: " << status.ToString();
            }
        }
    }
}

void SpillFileManager::Init(const std::string &directory)
{
    spillDir_ = directory;
    tenant2FileInfo_[DEFAULT_TENANT_ID] = {};
}

Status SpillFileManager::SpillSmallObject(const std::string &objectKey, const void *buffer, size_t size)
{
    PerfPoint p(PerfKey::WORKER_SPILL_TO_BUFFER);
    std::unique_lock<std::shared_timed_mutex> lock(fileInfoMutex_);
    buffer_.Append(objectKey, static_cast<const char *>(buffer), size);
    if (buffer_.Size() < SpillFileManager::LARGE_OBJ_SIZE_THRESHOLD) {
        return Status::OK();
    }

    PerfPoint point(PerfKey::WORKER_SPILL_FLUSH_BUFFER);

    SpillBuffer spillBuffer;
    // Copy SpillBuffer will ignore the deleted object data.
    buffer_.CloneTo(spillBuffer);
    const auto &index = spillBuffer.GetIndex();

    ObjectLocation flushLoc;
    std::shared_ptr<ActiveSpillFile> file;
    RETURN_IF_NOT_OK(FindBestWriteFile(DEFAULT_TENANT_ID, spillBuffer.Size(), flushLoc, file));
    // Write to file.
    Timer timer;
    RETURN_IF_NOT_OK(file->Write(spillBuffer.GetData(), flushLoc.size, flushLoc.offset));
    auto writeElapsed = timer.ElapsedMilliSecond();
    // Create location.
    for (const auto &kv : index) {
        const auto &objectKey = kv.first;
        const auto &offset = kv.second.first;
        const auto &size = kv.second.second;
        ObjectLocation loc;
        loc.path = flushLoc.path;
        loc.offset = flushLoc.offset + offset;
        loc.size = size;
        UpdateSpillInfo(DEFAULT_TENANT_ID, objectKey, loc);
    }

    buffer_.Reset();
    lock.unlock();

    // Sync to disk.
    timer.Reset();
    file->Sync();
    auto syncElapsed = timer.ElapsedMilliSecond();
    LOG(INFO) << FormatString(
        "Id: %d, Flush %d objects and %d bytes from buffer to file, fd: %d, write: %fms, sync: %fms.", id_,
        index.size(), spillBuffer.Size(), file->GetFd(), writeElapsed, syncElapsed);
    return Status::OK();
}

Status SpillFileManager::Spill(const std::string &objectKey,
                               const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads, uint64_t size)
{
    PerfPoint point(PerfKey::WORKER_SPILL);
    // Default tenant can write to buffer, otherwise spill to file.
    auto tenantId = TenantAuthManager::ExtractTenantId(objectKey);
    std::replace(tenantId.begin(), tenantId.end(), '/', '_');
    if (size <= sizeThreshold_ && tenantId == DEFAULT_TENANT_ID) {
        CHECK_FAIL_RETURN_STATUS(
            payloads.size() == 1, StatusCode::K_RUNTIME_ERROR,
            FormatString("Spill small object %s[size: %ld] but payload size is %ld", objectKey, size, payloads.size()));
        return SpillSmallObject(objectKey, payloads[0].first, payloads[0].second);
    }

    INJECT_POINT("worker.Spill", [](int waitTimeMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeMs));
        return Status::OK();
    });

    Timer timer;
    std::unique_lock<std::shared_timed_mutex> lock(fileInfoMutex_);
    auto waitLockElapsed = timer.ElapsedMilliSecond();

    PerfPoint p(PerfKey::WORKER_SPILL_WRITE_FILE);
    ObjectLocation loc;
    std::shared_ptr<ActiveSpillFile> file;
    RETURN_IF_NOT_OK(FindBestWriteFile(tenantId, size, loc, file));
    // Add id to FileInfo to prevent files from being deleted when the object list is empty.
    tenant2FileInfo_[tenantId][loc.path].objectKeys.insert(objectKey);
    lock.unlock();

    // Write to file.
    timer.Reset();
    Status rc = WriteFile(file, payloads, loc.offset);
    if (rc.IsError()) {
        std::unique_lock<std::shared_timed_mutex> lock(fileInfoMutex_);
        tenant2FileInfo_[tenantId][loc.path].objectKeys.erase(objectKey);
        return rc;
    }
    totalSpillFileDiskSize += size;
    auto writeElapsed = timer.ElapsedMilliSecond();

    // Sync to disk.
    timer.Reset();
    file->Sync();
    p.Record();
    auto syncElapsed = timer.ElapsedMilliSecond();

    // update after spill to file.
    {
        std::unique_lock<std::shared_timed_mutex> lock(fileInfoMutex_);
        objLocations_[objectKey] = loc;
    }
    LOG(INFO) << FormatString(
        "Id: %d, Spill object [%s], fd: %d, path: %s, offset: %ld, size: %ld, wait: %fms, write: %fms, syncfs: %fms",
        id_, objectKey, file->GetFd(), loc.path, loc.offset, loc.size, waitLockElapsed, writeElapsed, syncElapsed);
    return Status::OK();
}

Status SpillFileManager::Spill(const std::string &objectKey, void *buffer, uint64_t size)
{
    std::vector<std::pair<const uint8_t *, uint64_t>> payloads{ { static_cast<const uint8_t *>(buffer), size } };
    return Spill(objectKey, payloads, size);
}

Status SpillFileManager::GetObjectFileLocation(const std::string &objectKey, ObjectLocation &loc)
{
    auto it = objLocations_.find(objectKey);
    if (it != objLocations_.end()) {
        loc = it->second;
    } else {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_NOT_FOUND, "Object " + objectKey + " is not found in the spilled file");
    }
    return Status::OK();
}

Status SpillFileManager::GetFileInfoPtr(const std::string &objectKey, const std::string &path, FileInfo *&outFileInfo)
{
    auto tenantId = TenantAuthManager::ExtractTenantId(objectKey);
    std::replace(tenantId.begin(), tenantId.end(), '/', '_');
    CHECK_FAIL_RETURN_STATUS(tenant2FileInfo_.count(tenantId) > 0, StatusCode::K_NOT_FOUND,
                             FormatString("Cannot find tenant %s in the tenantInfoMap", tenantId));
    auto &fileInfoMap = tenant2FileInfo_[tenantId];
    CHECK_FAIL_RETURN_STATUS(fileInfoMap.count(path) > 0, StatusCode::K_NOT_FOUND,
                             FormatString("Cannot find the file %s in the fileInfoMap", path));
    outFileInfo = &fileInfoMap[path];
    return Status::OK();
}

Status SpillFileManager::GetFileInfoAndReopenFile(const std::string &objectKey, const std::string &path,
                                                  FileInfo *&outFileInfo, bool &isReopen)
{
    RETURN_IF_NOT_OK(GetFileInfoPtr(objectKey, path, outFileInfo));
    isReopen = false;
    if (outFileInfo->file != nullptr) {
        return Status::OK();
    }
    int fd;
    RETURN_IF_NOT_OK(RetryFileOperation(MAX_OPEN_RETRY, BACKOFF_TIME_MS, [&path, &fd]() {
        return OpenFile(path, O_RDWR | O_CREAT, SPILL_FILE_MODE, &fd);
    }));
    DisableReadAheadIfNeed(fd);
    openFdCount++;
    isReopen = true;
    outFileInfo->file = std::make_shared<ActiveSpillFile>(fd);
    VLOG(1) << FormatString("Reopen file: %s, objectKey: %s, openFdCount: %ld", path, objectKey, openFdCount.load());
    return Status::OK();
}

void SpillFileManager::CloseFileIfExceedLimit(FileInfo *fileInfo)
{
    if (FdCountExceedLimit() && fileInfo->file != nullptr) {
        fileInfo->file = nullptr;
        VLOG(1) << "Close exceed file: " << fileInfo->path;
    }
}

bool SpillFileManager::FdCountExceedLimit()
{
    return static_cast<uint64_t>(openFdCount) >= FLAGS_spill_file_open_limit;
}

Status SpillFileManager::CreateSpillFile(const std::string &directory, SpillFileType spillFileType,
                                         std::string &outFileURL, int *outFd)
{
    std::string fileUrl;
    do {
        fileUrl = directory + "/" + GetStringUuid();
    } while (FileExist(fileUrl));
    RETURN_IF_NOT_OK(RetryFileOperation(MAX_OPEN_RETRY, BACKOFF_TIME_MS, [&fileUrl, &directory, outFd]() {
        RETURN_IF_NOT_OK(CreateDir(directory, true, PERMISSION));
        return OpenFile(fileUrl, O_RDWR | O_CREAT, SPILL_FILE_MODE, outFd);
    }));
    DisableReadAheadIfNeed(*outFd);
    outFileURL = fileUrl;
    openFdCount++;
    // NO Space allocated at this stage. Allocate before the first obj write.
    LOG(INFO) << "[OCCreateSpillFile] FileType(0:large/1:small): " << static_cast<unsigned int>(spillFileType)
              << " path: " << outFileURL << ", openFdCount: " << openFdCount;
    return Status::OK();
}

Status SpillFileManager::FindBestWriteFile(const std::string &tenantId, size_t size, ObjectLocation &outLocation,
                                           std::shared_ptr<ActiveSpillFile> &file)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(size < SIZE_MAX, K_RUNTIME_ERROR,
                                         FormatString("The size is overflow, size:%d > SIZE_MAX:%d", size, SIZE_MAX));
    SpillFileType spillFileType =
        size >= LARGE_OBJ_SIZE_THRESHOLD ? SpillFileType::LARGE_OBJ_FILE : SpillFileType::SMALL_OBJ_FILE;
    auto &fileInfoMap = tenant2FileInfo_[tenantId];
    auto iter = fileInfoMap.begin();
    while (iter != fileInfoMap.end()) {
        if (iter->second.full || iter->second.file == nullptr) {
            CloseFileIfExceedLimit(&iter->second);
            ++iter;
            continue;
        }
        if (iter->second.spillFileType != spillFileType) {
            ++iter;
            continue;
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            std::numeric_limits<size_t>::max() - iter->second.size > size, K_RUNTIME_ERROR,
            FormatString("The size is overflow, size:%d + add:%d > SIZE_MAX:%d", iter->second.size, size, SIZE_MAX));
        if (iter->second.size + size > FLAGS_spill_file_max_size_mb * MB) {
            iter->second.full = true;
            if (iter->second.objectKeys.empty() && spillFileType == SpillFileType::LARGE_OBJ_FILE) {
                iter->second.file = nullptr;
                LOG_IF_ERROR(DeleteFile(iter->second.path), "[Spill] Delete Empty file Fail");
                iter = fileInfoMap.erase(iter);
            } else {
                CloseFileIfExceedLimit(&iter->second);
                ++iter;
            }
            continue;
        }
        outLocation = ObjectLocation{ .path = iter->first, .offset = iter->second.size, .size = size };
        RETURN_RUNTIME_ERROR_IF_NULL(iter->second.file);
        file = iter->second.file;
        iter->second.size += size;
        return Status::OK();
    }
    // No file available, create a new one.
    CHECK_FAIL_RETURN_STATUS(!spillDir_.empty(), K_RUNTIME_ERROR, "Variable directory should not be empty.");
    auto dir = spillDir_ + "/" + tenantId;
    std::string newFilePath;
    int newFd;
    RETURN_IF_NOT_OK(CreateSpillFile(dir, spillFileType, newFilePath, &newFd));
    auto isFull = size >= FLAGS_spill_file_max_size_mb * MB;
    file = std::make_shared<ActiveSpillFile>(newFd);
    fileInfoMap[newFilePath] = FileInfo{
        .file = file, .size = size, .holesSize = 0, .full = isFull, .spillFileType = spillFileType, .path = newFilePath
    };
    outLocation = ObjectLocation{ .path = newFilePath, .offset = 0, .size = size };
    return Status::OK();
}

Status SpillFileManager::WriteFile(const std::shared_ptr<ActiveSpillFile> &file,
                                   const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads, uint64_t offset)
{
    Status rc;
    CHECK_FAIL_RETURN_STATUS(file != nullptr, K_RUNTIME_ERROR, "file is null");
    for (const auto &payload : payloads) {
        rc = file->Write(payload.first, payload.second, offset);
        if (rc.IsOk()) {
            offset += payload.second;
        } else {
            break;
        }
    }
    return rc;
}

void SpillFileManager::UpdateSpillInfo(const std::string &tenantId, const std::string &objectKey,
                                       const ObjectLocation &location)
{
    (void)tenant2FileInfo_[tenantId][location.path].objectKeys.insert(objectKey);
    objLocations_[objectKey] = location;
}

Status SpillFileManager::LoadFromDisk(const std::string &objectKey, void *buffer, size_t size, size_t offset)
{
    PerfPoint point(PerfKey::WORKER_SPILL_GET);
    auto readBufferFunc = [&objectKey, buffer, offset, size](SpillBuffer &spillBuffer) {
        return spillBuffer.CopyTo(objectKey, static_cast<const char *>(buffer), size, offset);
    };
    auto readFileFunc = [buffer, offset, size](std::shared_ptr<ActiveSpillFile> &file, uint64_t objectOffset,
                                               uint64_t objectSize) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(SIZE_MAX - size >= offset, K_RUNTIME_ERROR,
                                             FormatString("size: %zu + offset: %zu > SIZE_MAX", size, offset));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            size + offset <= objectSize && size > 0, K_RUNTIME_ERROR,
            FormatString("Invalid size: %zu, offset: %zu, objectSize: %zu", size, offset, objectSize));
        return file->Read(buffer, size, objectOffset + offset);
    };
    return LoadFromDiskImpl(objectKey, readBufferFunc, readFileFunc);
}

Status SpillFileManager::LoadFromDisk(const std::string &objectKey, std::vector<RpcMessage> &messages, size_t size,
                                      size_t offset)
{
    PerfPoint point(PerfKey::WORKER_SPILL_GET_TO_MESSAGE);
    auto readBufferFunc = [&objectKey, &messages, size, offset](SpillBuffer &spillBuffer) {
        return spillBuffer.CopyTo(objectKey, messages, size, offset);
    };
    auto readFileFunc = [&messages, size, offset](std::shared_ptr<ActiveSpillFile> &file, uint64_t objectOffset,
                                                  uint64_t objectSize) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(SIZE_MAX - size >= offset, K_RUNTIME_ERROR,
                                             FormatString("size: %zu + offset: %zu > SIZE_MAX", size, offset));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            size + offset <= objectSize && size > 0, K_RUNTIME_ERROR,
            FormatString("Invalid size: %zu, offset: %zu, objectSize: %zu", size, offset, objectSize));
        return file->ReadToRpcMessage(size, objectOffset + offset, messages);
    };
    return LoadFromDiskImpl(objectKey, readBufferFunc, readFileFunc);
}

Status SpillFileManager::DeleteFromDisk(const std::string &objectKey, uint64_t &decSize)
{
    PerfPoint point(PerfKey::WORKER_SPILL_DELETE);
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> fileLock(fileInfoMutex_);
    auto waitElapsed = timer.ElapsedMilliSecond();
    workerOperationTimeCost.Append("Delete From Disk Get lock", timer.ElapsedMilliSecond());
    // Delete from SpillBuffer.
    if (buffer_.Exist(objectKey, decSize)) {
        if (buffer_.UniqObject(objectKey)) {
            buffer_.Reset();
        } else {
            buffer_.Remove(objectKey);
        }
        return Status::OK();
    }

    // Delete object location info.
    ObjectLocation loc;
    RETURN_IF_NOT_OK(GetObjectFileLocation(objectKey, loc));
    const auto &filename = loc.path;
    LOG(INFO) << FormatString("Id: %d, Delete object [%s], path: %s, offset: %ld, size: %ld, wait lock: %fms", id_,
                              objectKey, filename, loc.offset, loc.size, waitElapsed);
    decSize = loc.size;

    auto tenantId = TenantAuthManager::ExtractTenantId(objectKey);
    std::replace(tenantId.begin(), tenantId.end(), '/', '_');
    CHECK_FAIL_RETURN_STATUS(tenant2FileInfo_.count(tenantId) > 0, StatusCode::K_NOT_FOUND,
                             FormatString("Cannot find tenant %s in the tenantInfoMap", tenantId));
    auto &fileInfoMap = tenant2FileInfo_[tenantId];
    CHECK_FAIL_RETURN_STATUS(fileInfoMap.count(filename) > 0, StatusCode::K_NOT_FOUND,
                             FormatString("Cannot find the file %s in the fileInfoMap", filename));
    auto &fileInfo = fileInfoMap[filename];
    CHECK_FAIL_RETURN_STATUS(fileInfo.objectKeys.erase(objectKey) == 1, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Failed to erase object %s from fileInfoMap", objectKey));
    (void)objLocations_.erase(objectKey);
    fileInfo.holesSize += decSize;
    DeleteLargeObj(objectKey, loc, fileInfoMap);
    return Status::OK();
}

void SpillFileManager::DeleteLargeObj(const std::string &objectKey, const ObjectLocation &loc,
                                      std::unordered_map<std::string, FileInfo> &fileInfoMap)
{
    if (loc.size >= LARGE_OBJ_SIZE_THRESHOLD) {
        auto &fileInfo = fileInfoMap[loc.path];
        if (fileInfo.objectKeys.empty() && fileInfo.full) {
            LOG(INFO) << FormatString("Delete empty spill file %s immediately", loc.path);
            if (fileInfo.file != nullptr) {
                fileInfo.file = nullptr;
            }
            Status rc = DeleteFile(loc.path);
            LOG_IF_ERROR(rc, "failed to delete file");
            (void)fileInfoMap.erase(loc.path);
            if (totalSpillFileDiskSize >= loc.size) {
                totalSpillFileDiskSize -= loc.size;
            } else {
                LOG(WARNING) << FormatString("Computation overflow: totalSpillFileDiskSize=%llu, loc.size=%llu",
                                             totalSpillFileDiskSize.load(), loc.size);
                totalSpillFileDiskSize = 0;
            }
        } else {
            std::lock_guard<std::shared_timed_mutex> queueLock(fallocateQueueMutex_);
            fallocateQueue_.emplace_back(objectKey, loc);
        }
    }
}

Status SpillFileManager::FallocateInplace(const ObjectLocation &loc)
{
    int tmpFd;
    Status rc = RetryFileOperation(MAX_OPEN_RETRY, BACKOFF_TIME_MS, [&loc, &tmpFd]() {
        return OpenFile(loc.path, O_RDWR | O_CREAT, SPILL_FILE_MODE, &tmpFd);
    });
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[ProcessFallocateQueue] Failed to open file %s with error:%d", loc.path, errno);
        return rc;
    }
    int ret = fallocate(tmpFd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, loc.offset, loc.size);
    if (ret != 0) {
        LOG(ERROR) << FormatString("[ProcessFallocateQueue] Failed to fallocate file with error:%d", errno);
    }
    RETRY_ON_EINTR(close(tmpFd));
    return Status::OK();
}

void SpillFileManager::ProcessFallocateQueue()
{
    std::vector<std::pair<std::string, ObjectLocation>> fallocateQueueCopy;
    {
        std::lock_guard<std::shared_timed_mutex> lock(fallocateQueueMutex_);
        fallocateQueueCopy.swap(fallocateQueue_);
    }
    for (const auto &objectLocationIter : fallocateQueueCopy) {
        const auto &objectKey = objectLocationIter.first;
        const auto &loc = objectLocationIter.second;
        VLOG(1) << "Process Fallocate task"
                << ", objectKey: " << objectKey << ", path: " << loc.path << ", offset: " << loc.offset
                << ", size: " << loc.size;
        ProcessFallocateOneTask(objectKey, loc);
    }
}

void SpillFileManager::ProcessFallocateOneTask(const std::string &objectKey, const ObjectLocation &loc)
{
    if (totalSpillFileDiskSize >= loc.size) {
        totalSpillFileDiskSize -= loc.size;
    } else {
        LOG(WARNING) << FormatString("Computation overflow: totalSpillFileDiskSize=%llu, loc.size=%llu",
                                     totalSpillFileDiskSize.load(), loc.size);
        totalSpillFileDiskSize = 0;
    }
    auto tenantId = TenantAuthManager::ExtractTenantId(objectKey);
    std::replace(tenantId.begin(), tenantId.end(), '/', '_');
    {
        std::lock_guard<std::shared_timed_mutex> lock(fileInfoMutex_);
        auto &fileInfoMap = tenant2FileInfo_[tenantId];
        auto iter = fileInfoMap.find(loc.path);
        if (iter == fileInfoMap.end()) {
            VLOG(1) << "file has been deleted, skip it.";
            return;
        }
        auto &fileInfo = iter->second;
        // only delete large object file here, small object file will be deleted by CompactFile
        if (fileInfo.objectKeys.empty() && fileInfo.full && fileInfo.spillFileType == SpillFileType::LARGE_OBJ_FILE) {
            if (fileInfo.file != nullptr) {
                fileInfo.file = nullptr;
            }
            Status rc = DeleteFile(loc.path);
            if (rc.IsError()) {
                LOG_IF_ERROR(rc, "[ProcessFallocateQueue] failed to delete file");
                return;
            }
            LOG(INFO) << FormatString("Spill file %s has been removed as all objs expired.", loc.path);
            if (fileInfoMap.erase(loc.path) != 1) {
                LOG(ERROR) << "[ProcessFallocateQueue] failed to erase file from fileInfoMap: " << loc.path;
            }
            return;
        }
    }
    Status rc = FallocateInplace(loc);
    LOG_IF_ERROR(rc, FormatString("FallocateInplace failed, objectKey: %s", objectKey));
}

Status SpillFileManager::CompactFiles(float holeSizeRatioThreshold)
{
    ProcessFallocateQueue();
    std::vector<HoleFileInfo> holeFileArray;
    GetHoleFiles(holeSizeRatioThreshold, holeFileArray);
    throttle_.Reset();
    for (auto &iter : holeFileArray) {
        const std::string &tenantId = iter.tenantId;
        const std::string &oldSpillFilename = iter.spillFilename;
        FileInfo &fileinfo = *(iter.fileinfo);
        size_t oldFileSize = fileinfo.size;
        RETURN_IF_NOT_OK(CompactFile(tenantId, oldSpillFilename, fileinfo));
        RETURN_IF_NOT_OK(DeleteFile(oldSpillFilename));
        if (totalSpillFileDiskSize >= oldFileSize) {
            totalSpillFileDiskSize -= oldFileSize;
        } else {
            LOG(WARNING) << FormatString("Computation overflow: totalSpillFileDiskSize=%llu, oldFileSize=%llu",
                                         totalSpillFileDiskSize.load(), oldFileSize);
            totalSpillFileDiskSize = 0;
        }
        LOG(INFO) << FormatString("[Compact] old Spill file %s remove.", oldSpillFilename);
    }
    return Status::OK();
}

void SpillFileManager::GetHoleFiles(float holeSizeRatioThreshold, std::vector<HoleFileInfo> &holeFileArray)
{
    std::shared_lock<std::shared_timed_mutex> l(fileInfoMutex_);
    for (const auto &tenantInfo : tenant2FileInfo_) {
        for (const auto &fileinfo : tenantInfo.second) {
            if (!fileinfo.second.full || fileinfo.second.spillFileType == SpillFileType::LARGE_OBJ_FILE) {
                VLOG(1) << FormatString("[Compact] File %s not full or not large obj file.", fileinfo.first);
                continue;
            }
            if (fileinfo.second.size == 0) {
                LOG(WARNING) << FormatString("[Compact] File %s is empty.", fileinfo.first);
                continue;
            }
            // FindBestWriteFile() can ensure that size is greater than 0 when the small object file status is full
            float holeSizeRatio =
                static_cast<float>(fileinfo.second.holesSize) / static_cast<float>(fileinfo.second.size);
            VLOG(1) << FormatString("[Compact] File %s hole ratio %d, threshold %d.", fileinfo.first, holeSizeRatio,
                                    holeSizeRatioThreshold);
            if (holeSizeRatio > holeSizeRatioThreshold) {
                holeFileArray.emplace_back(HoleFileInfo{ holeSizeRatio, tenantInfo.first, fileinfo.first,
                                                         const_cast<FileInfo *>(&fileinfo.second) });
            }
        }
    }
    std::sort(holeFileArray.begin(), holeFileArray.end(),
              [](HoleFileInfo &a, HoleFileInfo &b) { return a.holeSizeRatio > b.holeSizeRatio; });
}

Status SpillFileManager::CompactFile(const std::string &tenantId, const std::string &oldSpillFilename,
                                     FileInfo &fileinfo)
{
    LOG(INFO) << FormatString("[Compact] start to Compact file %s, holesSize:%d, size:%d.", oldSpillFilename,
                              fileinfo.holesSize, fileinfo.size);
    std::string newFilePath;
    int newFd = -1;
    auto dir = spillDir_ + "/" + tenantId;
    RETURN_IF_NOT_OK(CreateSpillFile(dir, fileinfo.spillFileType, newFilePath, &newFd));
    FileInfo newFileInfo = { .file = std::make_shared<ActiveSpillFile>(newFd),
                             .size = 0,
                             .holesSize = 0,
                             .full = false,
                             .spillFileType = fileinfo.spillFileType,
                             .path = newFilePath };
    // copy objects
    std::unordered_map<std::string, ObjectLocation> newObjectLocationsMap;
    Status rc = CopyObjects(fileinfo, newFilePath, newFileInfo, newObjectLocationsMap);
    if (rc.IsError()) {
        LOG(ERROR) << "[Compact] failed to CopyObjects: " << rc.ToString();
        newFileInfo.file = nullptr;
        Status ret = DeleteFile(newFilePath);
        LOG_IF_ERROR(ret, "[Compact] failed to delete new file");
        return rc;
    }
    totalSpillFileDiskSize += newFileInfo.size;
    // update objects meta info, delete old spill file and the map fileInfo
    uint64_t usedSize = 0;
    std::lock_guard<std::shared_timed_mutex> lock(fileInfoMutex_);
    auto &fileInfoMap = tenant2FileInfo_[tenantId];
    fileInfoMap[newFilePath] = newFileInfo;
    for (const auto &objectKey : fileinfo.objectKeys) {
        auto &loc = newObjectLocationsMap[objectKey];
        UpdateSpillInfo(tenantId, objectKey, loc);
        usedSize += loc.size;
        VLOG(1) << "[Compact] update object meta: " << objectKey;
    }
    FileInfo &newFileinfo = fileInfoMap[newFilePath];
    newFileinfo.holesSize = newFileinfo.size - usedSize;
    LOG(INFO) << FormatString("[Compact] update %d objects meta info, newFile holesize:%d", fileinfo.objectKeys.size(),
                              newFileinfo.holesSize);
    CHECK_FAIL_RETURN_STATUS(fileInfoMap.erase(oldSpillFilename) == 1, K_RUNTIME_ERROR,
                             FormatString("[Compact] Fail to erase file from fileInfoMap %s", oldSpillFilename));
    LOG(INFO) << FormatString("[Compact] old fileInfo remove from map, Compact file success %s -> %s.",
                              oldSpillFilename, newFilePath);
    if (fileinfo.file != nullptr) {
        fileinfo.file = nullptr;
    }
    return Status::OK();
}

Status SpillFileManager::CopyObjects(const FileInfo &fileinfo, const std::string &newFilePath, FileInfo &newFileinfo,
                                     std::unordered_map<std::string, ObjectLocation> &newObjectLocationsMap)
{
    // copy objects from old spill file to new file
    ObjectLocation oldLoc;
    std::unordered_map<std::string, ObjectLocation> oldObjectLocationsMap;
    {
        // copy old spill file obj meta info
        std::shared_lock<std::shared_timed_mutex> lock(fileInfoMutex_);
        for (auto &objectKey : fileinfo.objectKeys) {
            RETURN_IF_NOT_OK(GetObjectFileLocation(objectKey, oldLoc));
            oldObjectLocationsMap[objectKey] = oldLoc;
        }
    }
    int tmpOldFileFd;
    RETURN_IF_NOT_OK(RetryFileOperation(MAX_OPEN_RETRY, BACKOFF_TIME_MS, [&fileinfo, &tmpOldFileFd]() {
        return OpenFile(fileinfo.path, O_RDWR | O_CREAT, SPILL_FILE_MODE, &tmpOldFileFd);
    }));
    Raii raii([&tmpOldFileFd]() { RETRY_ON_EINTR(close(tmpOldFileFd)); });
    // copy objects
    ObjectLocation newLocation;
    auto compactionBuffer = std::make_unique<uint8_t[]>(LARGE_OBJ_SIZE_THRESHOLD);
    for (auto &objectLocation : oldObjectLocationsMap) {
        const auto &objectKey = objectLocation.first;
        oldLoc = objectLocation.second;
        RETURN_IF_NOT_OK(ReadFile(tmpOldFileFd, compactionBuffer.get(), oldLoc.size, oldLoc.offset));
        newLocation.path = newFilePath;
        newLocation.size = oldLoc.size;
        newLocation.offset = newFileinfo.size;
        newFileinfo.size += oldLoc.size;
        newObjectLocationsMap[objectKey] = newLocation;
        RETURN_IF_NOT_OK(newFileinfo.file->Write(compactionBuffer.get(), newLocation.size, newLocation.offset));
        VLOG(1) << "[Compact] copy object: " << objectKey << ", path: " << newLocation.path
                << ", offset: " << newLocation.offset << ", size: " << newLocation.size;
        throttle_.LimitIORate(newLocation.size);
    }
    newFileinfo.file->Sync();
    LOG(INFO) << FormatString("[Compact] copy %d objects from %s to new file %s success.", oldObjectLocationsMap.size(),
                              fileinfo.path, newFileinfo.path);
    return Status::OK();
}

std::string SpillFileManager::GetObjectLocation(const std::string &objectKey)
{
    // There is no lock to protect these code.
    uint64_t size = 0;
    if (buffer_.Exist(objectKey, size)) {
        return SPILL_BUFFER;
    }
    ObjectLocation loc;
    Status rc = GetObjectFileLocation(objectKey, loc);
    if (rc.IsError()) {
        return "";
    }
    return loc.path;
}

void SpillFileManager::DisableReadAheadIfNeed(int fd)
{
    if (!FLAGS_spill_enable_readahead) {
        int ret = posix_fadvise(fd, 0, 0, MADV_RANDOM);
        if (ret != 0) {
            // Ignore and write log.
            LOG(WARNING) << "posix_fadvise MADV_RANDOM failed: " << StrErr(errno);
        }
    }
}

WorkerOcSpill *WorkerOcSpill::Instance()
{
    static WorkerOcSpill instance;
    return &instance;
}

WorkerOcSpill::~WorkerOcSpill()
{
    LOG(INFO) << "WorkerOcSpill exit";
    stopCompaction_ = true;
    waitPost_.Set();
    if (spillCompactionThread_.joinable()) {
        spillCompactionThread_.join();
    }
}

Status WorkerOcSpill::Init()
{
    CHECK_FAIL_RETURN_STATUS(fileMgr_.empty(), K_RUNTIME_ERROR, "The Spill File Manager has already been Initialized.");
    std::string realSpillDirectory = FLAGS_spill_directory + SPILL_PATH_PREFIX;
    if (!FLAGS_spill_directory.empty()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveAll(realSpillDirectory), "[Spill] RemoveAll failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateDir(realSpillDirectory, true, PERMISSION),
                                         "create spill directory failed");
        freeSpaceInRealTime_ = GetFreeSpaceBytes(realSpillDirectory);
        initial95SpillFreeSpace_ = GetFreeSpaceBytes(realSpillDirectory) / 100u * 95u;
        LOG(INFO) << FormatString("[SPill] spill_size_limit automatically set to %llu bytes.",
                                  initial95SpillFreeSpace_);
    }
    for (uint32_t i = 0; i < FLAGS_spill_thread_num; i++) {
        auto tmpMgr = std::make_unique<SpillFileManager>(i);
        tmpMgr->Init(realSpillDirectory);
        fileMgr_.emplace_back(std::move(tmpMgr));
    }
    totalActiveSpilledSize_ = 0;
    stopCompaction_ = false;
    spillCompactionThread_ = Thread(&WorkerOcSpill::Compact, this);
    LOG(INFO) << "WorkerOcSpill init success, spill_directory: " << realSpillDirectory
              << ", spill_size_limit: " << GetSpillLimitSize();
    return Status::OK();
}

void WorkerOcSpill::Compact()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG(INFO) << "[Compact] compact spill files thread start to run.";
    uint64_t compactIntervalMs = 10 * 1000;
    if (!IsEnabled()) {
        LOG(INFO) << "[Compact] spill is disabled, exit.";
        return;
    }
    uint64_t diskSpaceThreshold = GetSpillLimitSize() / 100u * 90u;
    LOG(INFO) << "[Compact] diskSpaceThreshold is: " << diskSpaceThreshold;
    std::string realSpillDirectory = FLAGS_spill_directory + SPILL_PATH_PREFIX;
    while (!stopCompaction_) {
        // Flags_spill_size_limit may be dynamically modified during the while loop.
        auto diskSpaceThreshold = GetSpillLimitSize() / 100u * 90u;
        freeSpaceInRealTime_ = GetFreeSpaceBytes(realSpillDirectory);
        LOG(INFO) << "[Spill] freeSpaceInRealTime: " << freeSpaceInRealTime_;
        waitPost_.WaitForNext(compactIntervalMs);
        for (uint32_t mgrIndex = 0; mgrIndex < FLAGS_spill_thread_num && !stopCompaction_; mgrIndex++) {
            Status rc = fileMgr_[mgrIndex]->CompactFiles(SpillFileManager::HOLE_SIZE_RATIO_THRESHOLD_50);
            LOG_IF_ERROR(rc, "failed to CompactFiles");
        }
        if (totalSpillFileDiskSize >= diskSpaceThreshold) {
            VLOG(1) << "[Compact] start to compact spill files as disk usage is:" << totalSpillFileDiskSize;
            for (uint32_t mgrIndex = 0; mgrIndex < FLAGS_spill_thread_num && !stopCompaction_; mgrIndex++) {
                Status rc = fileMgr_[mgrIndex]->CompactFiles(SpillFileManager::HOLE_SIZE_RATIO_THRESHOLD_30);
                LOG_IF_ERROR(rc, "failed to CompactFiles");
            }
        }
    }
    LOG(INFO) << "[Compact] compact spill files thread end.";
}

Status WorkerOcSpill::Spill(const std::string &objectKey, const void *buffer, size_t size, bool evictable)
{
    std::vector<std::pair<const uint8_t *, uint64_t>> payloads{ { static_cast<const uint8_t *>(buffer), size } };
    return Spill(objectKey, payloads, evictable);
}

Status WorkerOcSpill::Spill(const std::string &objectKey,
                            const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads, bool evictable)
{
    uint64_t size = 0;
    for (const auto &payload : payloads) {
        size += payload.second;
    }

    if (IsSpaceFull(size)) {
        LOG(INFO) << FormatString("[ObjectKey %s] The spill space is full.", objectKey);
        RETURN_STATUS(K_NO_SPACE, "No space when WorkerOcSpill::Spill");
    }
    size_t mgrIndex = GetMgrIndex(objectKey);
    VLOG(1) << FormatString("[ObjectKey %s] SpillFileManager: %d", objectKey, mgrIndex);
    RETURN_IF_NOT_OK(fileMgr_[mgrIndex]->Spill(objectKey, payloads, size));
    totalActiveSpilledSize_ += size;

    if (evictable) {
        spillEvictionList_.Add(objectKey, size >= SpillFileManager::LARGE_OBJ_SIZE_THRESHOLD ? Q2 : Q1);
    }
    return Status::OK();
}

Status WorkerOcSpill::Get(const std::string &objectKey, void *buffer, size_t size, size_t offset)
{
    size_t mgrIndex = GetMgrIndex(objectKey);
    Status status = fileMgr_[mgrIndex]->LoadFromDisk(objectKey, buffer, size, offset);
    if (status.IsOk() && spillEvictionList_.Exist(objectKey)) {
        spillEvictionList_.Add(objectKey, Q1);
    }
    return status;
}

Status WorkerOcSpill::Get(const std::string &objectKey, std::vector<RpcMessage> &message, size_t size, size_t offset)
{
    size_t mgrIndex = GetMgrIndex(objectKey);
    Status status = fileMgr_[mgrIndex]->LoadFromDisk(objectKey, message, size, offset);
    if (status.IsOk() && spillEvictionList_.Exist(objectKey)) {
        spillEvictionList_.Add(objectKey, Q1);
    }
    return status;
}

Status WorkerOcSpill::Delete(const std::string &objectKey)
{
    size_t mgrIndex = GetMgrIndex(objectKey);
    uint64_t decSize = 0;
    RETURN_IF_NOT_OK(fileMgr_[mgrIndex]->DeleteFromDisk(objectKey, decSize));
    CHECK_FAIL_RETURN_STATUS(totalActiveSpilledSize_ >= decSize, K_RUNTIME_ERROR,
                             FormatString("Computation overflow: totalActiveSpilledSize_=%llu, decSize=%llu",
                                          totalActiveSpilledSize_.load(), decSize));
    totalActiveSpilledSize_ -= decSize;
    spillEvictionList_.Erase(objectKey);
    return Status::OK();
}

bool WorkerOcSpill::IsEnabled() const
{
    return !FLAGS_spill_directory.empty();
}

size_t WorkerOcSpill::GetMgrIndex(const std::string &objectKey)
{
    std::hash<std::string> hash;
    return hash(objectKey) % FLAGS_spill_thread_num;
}

std::string WorkerOcSpill::GetObjectLocation(const std::string &objectKey)
{
    size_t mgrIndex = GetMgrIndex(objectKey);
    return fileMgr_[mgrIndex]->GetObjectLocation(objectKey);
}

std::vector<std::string> WorkerOcSpill::ParseGFlagDirectories()
{
    if (FLAGS_spill_directory.empty()) {
        return {};
    }
    return Split(FLAGS_spill_directory, ",");
}

std::vector<std::string> WorkerOcSpill::GetSpilledFileName(const std::string &path)
{
    bool isDir;
    std::vector<std::string> files = {};
    if (!FileExist(path)) {
        return files;
    }
    DIR *dir = opendir(path.c_str());
    if (dir == nullptr) {
        LOG(ERROR) << "GetSpilledFileName open dir failed with " << path;
        return files;
    }
    Raii releaseDir([dir] { closedir(dir); });

    struct dirent *ent;
    std::string filePath;
    while ((ent = readdir(dir)) != nullptr) {
        if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) {
            continue;
        }
        // full path of file
        filePath = path + "/" + ent->d_name;
        if (IsDirectory(filePath, isDir).IsOk() && isDir) {
            auto tmpFiles = GetSpilledFileName(filePath);
            files.insert(files.end(), tmpFiles.begin(), tmpFiles.end());
        }
        files.emplace_back(filePath);
    }
    return files;
}

uint64_t WorkerOcSpill::GetSpilledSize()
{
    return totalActiveSpilledSize_;
}

bool WorkerOcSpill::IsSpaceFull(size_t size)
{
    return IsSpaceExceed(1.0, size);
}

bool WorkerOcSpill::IsSpaceExceedLWM(size_t size)
{
    return IsSpaceExceed(lowWaterFactor_, size);
}

bool WorkerOcSpill::IsSpaceExceedHWM(size_t size)
{
    return IsSpaceExceed(highWaterFactor_, size);
}

bool WorkerOcSpill::IsSpaceExceed(double ratio, size_t size)
{
    auto spillSizeLimit = GetSpillLimitSize();
    return (totalSpillFileDiskSize + size >= spillSizeLimit * ratio);
}

bool WorkerOcSpill::IsActiveSpillSizeExceedLWM(size_t size)
{
    return IsActiveSpillSizeExceed(lowWaterFactor_, size);
}

bool WorkerOcSpill::IsActiveSpillSizeExceedHWM(size_t size)
{
    return IsActiveSpillSizeExceed(lowWaterFactor_, size);
}

bool WorkerOcSpill::IsActiveSpillSizeExceed(double ratio, size_t size)
{
    auto spillSizeLimit = GetSpillLimitSize();
    if (size > std::numeric_limits<size_t>::max() - totalActiveSpilledSize_) {
        return true;
    }
    return (totalActiveSpilledSize_ + size >= spillSizeLimit * ratio);
}

uint64_t WorkerOcSpill::GetRemainActiveSpillSize()
{
    return GetSpillLimitSize() - totalActiveSpilledSize_;
}

std::string WorkerOcSpill::GetSpillUsage()
{
    auto spillSizeLimit = GetSpillLimitSize();
    if (spillSizeLimit == 0) {
        return "0/0/0/0";
    }
    auto workerSpillHardDiskUsage = totalActiveSpilledSize_.load() / static_cast<float>(spillSizeLimit);
    return FormatString("%lu/%lu/%lu/%.3f", totalActiveSpilledSize_.load(), totalSpillFileDiskSize.load(),
                        spillSizeLimit, workerSpillHardDiskUsage);
}

uint64_t WorkerOcSpill::GetSpillLimitSize()
{
    return FLAGS_spill_size_limit == 0 ? initial95SpillFreeSpace_ : FLAGS_spill_size_limit;
}

Throttle::Throttle(uint64_t desiredRatePerSec, uint64_t checkIntervalMs)
    : desiredRatePerSec_(desiredRatePerSec), checkIntervalMs_(checkIntervalMs), totalBytes_(0)
{
    desiredRatePerMs_ = static_cast<float>(desiredRatePerSec_) / TIME_UNIT_CONVERSION;
}

void Throttle::LimitIORate(size_t dataSize)
{
    totalBytes_ += dataSize;
    uint64_t costTimeMs = static_cast<uint64_t>(timer_.ElapsedMilliSecond());
    if (costTimeMs < checkIntervalMs_) {
        return;
    }
    float currentRatePerSec = static_cast<float>(totalBytes_) * TIME_UNIT_CONVERSION / costTimeMs;
    VLOG(1) << FormatString("[Throttle] current rate:%f B/s, desired rate:%f B/s, cost:%d.", currentRatePerSec,
                            desiredRatePerSec_, costTimeMs);
    if (currentRatePerSec > desiredRatePerSec_) {
        if (desiredRatePerMs_ <= std::numeric_limits<float>::epsilon()) {
            LOG(INFO) << FormatString("[Throttle] desired rate(%f) is invalid.", desiredRatePerMs_);
        } else {
            int64_t sleepTime = static_cast<int64_t>(static_cast<float>(totalBytes_) / desiredRatePerMs_ - costTimeMs);
            if (sleepTime > 0) {
                LOG(INFO) << FormatString("[Throttle] sleep %d ms to Limit IO Rate.", sleepTime);
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
            }
        }
    }
    totalBytes_ = 0;
    timer_.Reset();
}

void Throttle::Reset()
{
    totalBytes_ = 0;
    timer_.Reset();
}

Status Throttle::UpdateDesiredRatePerSec(uint64_t desiredRatePerSec)
{
    CHECK_FAIL_RETURN_STATUS(desiredRatePerSec > 0, K_INVALID, "desiredRatePerSec must be greater than 0");
    desiredRatePerSec_ = desiredRatePerSec;
    desiredRatePerMs_ = static_cast<float>(desiredRatePerSec_) / TIME_UNIT_CONVERSION;
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
