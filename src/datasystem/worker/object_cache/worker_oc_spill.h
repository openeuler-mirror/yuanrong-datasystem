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
 * Description: Object spilling declaration.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_OC_SPILL_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_OC_SPILL_H

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/worker/object_cache/eviction_list.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"

namespace datasystem {
namespace object_cache {

const std::string SPILL_BUFFER = "SPILL_BUFFER";
const std::string SPILL_PATH_PREFIX = "/datasystem_spill_data";

struct ObjectLocation {
    // Path to the file stored the object
    std::string path;
    // Offset of the file to the object
    uint64_t offset = 0;
    // Size of the data stored in the file
    uint64_t size = 0;
};

// spill file type, large object or small object file
enum class SpillFileType { LARGE_OBJ_FILE, SMALL_OBJ_FILE };

class ActiveSpillFile {
public:
    ActiveSpillFile(int fd) : fd_(fd)
    {
        LOG(INFO) << "New fd:" << fd;
    }
    ActiveSpillFile(const ActiveSpillFile &) = delete;
    ActiveSpillFile &operator=(const ActiveSpillFile &) = delete;
    ~ActiveSpillFile();

    /**
     * @brief Reads from file into the buffer provided.
     * @param[out] buffer Pointer to the buffer to read in to.
     * @param[in] count The requested number of bytes to read.
     * @param[in] offset The offset in the file to read from.
     * @return Status of the call.
     */
    Status Read(void *buffer, size_t count, off_t offset);

    /**
     * @brief Read data from file to RpcMessage.
     * @param[in] size The size of the data to be loaded from the file.
     * @param[in] offset The offset of the data int the the file.
     * @param[out] messages The rpc messages that load the object data.
     * @return Status of the call.
     */
    Status ReadToRpcMessage(size_t size, size_t offset, std::vector<RpcMessage> &messages);

    /**
     * @brief Writes to file from the buffer provided.
     * @param[in] buffer Pointer to the buffer to write from.
     * @param[in] count The requested number of bytes to write.
     * @param[in] offset The offset in the file to write to.
     * @return Status of the call.
     */
    Status Write(const void *buffer, size_t count, off_t offset);

    /**
     * @brief Sync data to disk.
     *
     */
    void Sync();

    /**
     * @brief Get fd.
     * @return Current fd.
     */
    int GetFd();

private:
    int fd_;
};

struct FileInfo {
    std::shared_ptr<ActiveSpillFile> file;
    size_t size;
    size_t holesSize;
    bool full = false;
    SpillFileType spillFileType;
    std::string path;
    std::unordered_set<std::string> objectKeys{};
};

struct HoleFileInfo {
    float holeSizeRatio;
    std::string tenantId;
    std::string spillFilename;
    FileInfo *fileinfo;
};

class Throttle {
public:
    explicit Throttle(uint64_t desiredRatePerSec = 100 * 1024 * 1024, uint64_t checkIntervalMs = 50);

    ~Throttle() = default;

    /**
     * @brief limit the io rate.
     * @param[in] dataSize the data size that input/read or output/write.
     */
    void LimitIORate(size_t dataSize);

    /**
     * @brief Reset the IO rate calculation startTime and the totalBytes.
     */
    void Reset();

    /**
     * @brief Update the desired IO rate PerSecond.
     * @param[in] desiredRatePerSec The rate we want to limit in bytes/sec, it must be greater than 0.
     * @return Status of the call.
     */
    Status UpdateDesiredRatePerSec(uint64_t desiredRatePerSec);

private:
    const uint32_t TIME_UNIT_CONVERSION = 1000;

    // The rate we want to limit in bytes/sec
    uint64_t desiredRatePerSec_;
    // The rate we want to limit in bytes/ms
    float desiredRatePerMs_;
    // The interval that check rate
    uint64_t checkIntervalMs_;
    // total byte in the interval
    size_t totalBytes_;
    // the timer that record the start time and calculate interval
    Timer timer_;
};

class SpillBuffer {
public:
    SpillBuffer();

    ~SpillBuffer() = default;

    /**
     * @brief Append object to spill buffer.
     * @param[in] objectKey The ID of the object to be appended.
     * @param[in] input The pointer to the buffer stores the object data.
     * @param[in] size The size of the buffer need to be appended.
     */
    void Append(const std::string &objectKey, const char *input, size_t size);

    /**
     * @brief Remote object from spill buffer.
     * @param[in] objectKey The ID of the object to be remove.
     */
    void Remove(const std::string &objectKey);

    /**
     * @brief Copy object from spill buffer to shared memory.
     * @param[in] objectKey The ID of the object to be copied.
     * @param[out] output The destination buffer that save the object data.
     * @param[in] size The size of the data need to be copied.
     * @param[in] offset The offset of the object data need to be copied.
     * @return Status of the call.
     */
    Status CopyTo(const std::string &objectKey, const char *output, size_t size, size_t offset = 0);

    /**
     * @brief Copy object from spill buffer to the rpc messages.
     * @param[in] objectKey The ID of the object to be copied.
     * @param[out] messages The rpc messages that save the object data.
     * @param[in] size The size of the data need to be copied.
     * @param[in] offset The offset of the object data need to be copied.
     * @return Status of the call.
     */
    Status CopyTo(const std::string &objectKey, std::vector<RpcMessage> &messages, size_t size, size_t offset = 0);

    /**
     * @brief Check whether an object are in spill buffer.
     * @param[in] objectKey The ID of the object to be checked.
     * @param[out] size The object size.
     * @return True if object are in spill buffer.
     */
    bool Exist(const std::string &objectKey, uint64_t &size);

    /**
     * @brief Check whether an object are uniq object in spill buffer.
     * @param[in] objectKey The ID of the object to be checked.
     * @return True if object are uniq object in spill buffer.
     */
    bool UniqObject(const std::string &objectKey);

    /**
     * @brief Get the byte-size of spill buffer.
     * @return The byte-size of spill buffer.
     */
    size_t Size();

    /**
     * @brief Get the spill buffer data pointer.
     * @return The pointer to the buffer data.
     */
    const char *GetData();

    /**
     * @brief Get the spill buffer index.
     * @return The spill buffer index.
     */
    const std::unordered_map<std::string, std::pair<size_t, size_t>> &GetIndex();

    /**
     * @brief Reset spill buffer, clear internal data structure.
     */
    void Reset();

    /**
     * @brief Copy SpillBuffer instance.
     * @param[out] buffer Buffer instance copied to.
     */
    void CloneTo(SpillBuffer &buffer);

private:
    std::string data_;
    std::unordered_map<std::string, std::pair<size_t, size_t>> index_;
};

class SpillFileManager {
public:
    // large object size threshold 1 MB
    static const size_t LARGE_OBJ_SIZE_THRESHOLD = 1024 * 1024;

    SpillFileManager(uint32_t id = 0) : id_(id){};

    ~SpillFileManager();

    /**
     * @brief Init the Spill File Manager.
     * @param[in] directory Directory to store the spill file.
     */
    void Init(const std::string &directory);

    /**
     * @brief Spill the object to local disk.
     * @param[in] objectKey The Id of the object to be spilled.
     * @param[in] payloads The pointer to the buffer stores the object data.
     * @param[in] size The size of the buffer to be spilled.
     * @return Status of the call.
     */
    Status Spill(const std::string &objectKey, const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads,
                 uint64_t size);

    /**
     * @brief Spill the object to local disk.
     * @param[in] objectKey The Id of the object to be spilled.
     * @param[in] buffer The pointer to the buffer stores the object data.
     * @param[in] size The size of the buffer to be spilled.
     * @return Status of the call.
     */
    Status Spill(const std::string &objectKey, void *buffer, uint64_t size);

    /**
     * @brief Spill the small object.
     * @param[in] objectKey The Id of the object to be spilled.
     * @param[in] buffer The pointer to the buffer stores the object data.
     * @param[in] size The size of the buffer to be spilled.
     * @return Status of the call.
     */
    Status SpillSmallObject(const std::string &objectKey, const void *buffer, size_t size);

    /**
     * @brief Load the data from the file to the buffer.
     * @param[in] objectKey The Id of the object to be loaded.
     * @param[out] buffer The pointer of the buffer that load the object data.
     * @param[in] size The size of the data to be loaded from the file.
     * @param[in] offset The offset of the data to be loaded.
     * @return Status of the call.
     */
    Status LoadFromDisk(const std::string &objectKey, void *buffer, size_t size, size_t offset = 0);

    /**
     * @brief Load the data from the file to the rpc messages.
     * @param[in] objectKey The Id of the object to be loaded.
     * @param[out] messages The rpc messages that load the object data.
     * @param[in] size The size of the data to be loaded from the file.
     * @param[in] offset The offset of the data to be loaded.
     * @return Status of the call.
     */
    Status LoadFromDisk(const std::string &objectKey, std::vector<RpcMessage> &messages, size_t size,
                        size_t offset = 0);

    /**
     * @brief Remove the spilled data from the file.
     * @param[in] objectKey The ID of the object that data to be deleted.
     * @param[out] decSize The size of the object that to be deleted.
     * @return Status of the call.
     */
    Status DeleteFromDisk(const std::string &objectKey, uint64_t &decSize);

    /**
     * @brief Get object location, for testing.
     * @param[in] objectKey The ID of the object that need to query.
     * @return Return SPILL_BUFFER if object exist in spill buffer or file path if object exist in file.
     */
    std::string GetObjectLocation(const std::string &objectKey);

    /**
     * @brief Check the hole size ratio and compact the hole files that holes size ratio more than threshold.
     * @param[in] holeSizeRatioThreshold The holes size ratio threshold.
     * @return Status of the call.
     */
    Status CompactFiles(float holeSizeRatioThreshold);

    /**
     * file holes size ratio threshold 50%.
     * @brief Lower the threshold, the more disk space will be released by compaction in time,
     * but the disk IO will be increased and occupy the IO of the disk.
     * Increase the threshold, the more disk space is wasted and can't be reclaimed by compaction in time,
     * but it will reduce the amount of disk IO.
     */
    constexpr static const float HOLE_SIZE_RATIO_THRESHOLD_50 = 0.5;
    // file holes size ratio threshold 30%
    constexpr static const float HOLE_SIZE_RATIO_THRESHOLD_30 = 0.3;

private:
    /**
     * @brief Find the place to write data of requested size.
     * @param[in] tenantId Tenant ID corresponding to spilled data.
     * @param[in] size Size requested for the written data.
     * @param[out] outLocation The location of the file to write.
     * @return Status of the call.
     */
    Status FindBestWriteFile(const std::string &tenantId, size_t size, ObjectLocation &outLocation,
                             std::shared_ptr<ActiveSpillFile> &file);

    /**
     * @brief Write file to spill dir.
     * @param[in] file File handler.
     * @param[in] payloads Data need to be write.
     * @param[in] offset Offset of the file.
     * @return Status of the call.
     */
    Status WriteFile(const std::shared_ptr<ActiveSpillFile> &file,
                     const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads, uint64_t offset);

    /**
     * @brief Get the object location in file.
     * @param[in] objectKey The ID of the object requested.
     * @param[out] loc The ObjectLocation Object.
     * @return Status of the call.
     */
    Status GetObjectFileLocation(const std::string &objectKey, ObjectLocation &loc);

    /**
     * @brief Get a FileInfo in FileInfoMap, just return a pointer, no memory copy.
     * @param[in] objectKey The ID of the object need get.
     * @param[in] path The path of spill file.
     * @param[out] fileInfo The returned file info.
     * @return Status of the call.
     */
    Status GetFileInfoPtr(const std::string &objectKey, const std::string &path, FileInfo *&outFileInfo);

    /**
     * @brief Get a FileInfo used for read, if old file descriptor in FileInfo closed, reopen and get a new one.
     * @param[in] objectKey The ID of the object need get.
     * @param[in] path The path of spill file.
     * @param[out] fileInfo The returned file info.
     * @param[out] isReopen Indicates whether opened a new FD.
     * @return Status of the call.
     */
    Status GetFileInfoAndReopenFile(const std::string &objectKey, const std::string &path, FileInfo *&outFileInfo,
                                    bool &isReopen);

    /**
     * @brief Close the spill file if the number of open files is greater than the threshold.
     * @param[in] fileInfo The FileInfo struct need to check and close.
     */
    void CloseFileIfExceedLimit(FileInfo *fileInfo);

    static bool FdCountExceedLimit();

    /**
     * @brief Update the spilled object and file info to the map recorded info for future restore.
     * @param[in] tenantId Tenant ID corresponding to spilled data.
     * @param[in] objectKey The ID of the object that spilled to the file.
     * @param[in] location The location of the file in the file system that contains the spilled object.
     */
    void UpdateSpillInfo(const std::string &tenantId, const std::string &objectKey, const ObjectLocation &location);

    /**
     * @brief Create a Spill File.
     * @param[in] directory The dir the file to be created.
     * @param[in] spillFileType The spill file type.
     * @param[out] outFileURL The URL of the created file.
     * @param[out] outFd The Fd associated with the created file.
     * @return Status of the file creation.
     */
    Status CreateSpillFile(const std::string &directory, SpillFileType spillFileType, std::string &outFileURL,
                           int *outFd);

    /**
     * @brief Get the files that hole size ratio more than threshold.
     * @param[in] holeSizeRatioThreshold The holes size ratio threshold.
     * @param[out] holeFileArray The hole file array {holeSizeRatio, spillFilename, FileInfo*} that will be compacted.
     */
    void GetHoleFiles(float holeSizeRatioThreshold, std::vector<HoleFileInfo> &holeFileArray);

    /**
     * @brief Compact the hole file.
     * @param[in] tenantId Tenant ID corresponding to spilled data.
     * @param[in] filename The hole spill file path.
     * @param[in] fileInfo The hole spill file that will be compacted.
     * @return Status of the call.
     */
    Status CompactFile(const std::string &tenantId, const std::string &filename, FileInfo &fileinfo);

    /**
     * @brief Copy the objects from hole file to new file.
     * @param[in] fileinfo The old hole spill file .
     * @param[in] newFilePath The new spill file path.
     * @param[out] newFileinfo The new hole spill file.
     * @param[out] newObjectLocationsMap The map record the new location of each object.
     * @return Status of the call.
     */
    Status CopyObjects(const FileInfo &fileinfo, const std::string &newFilePath, FileInfo &newFileinfo,
                       std::unordered_map<std::string, ObjectLocation> &newObjectLocationsMap);

    /**
     * @brief Process the fallocate task in queue, it will exec fallocate function or delete file.
     */
    void ProcessFallocateQueue();

    /**
     * @brief Process one fallocate task.
     * @param[in] objectKey The object key.
     * @param[in] loc The location object.
     */
    void ProcessFallocateOneTask(const std::string &objectKey, const ObjectLocation &loc);

    /**
     * @brief Call fallocate on spill file.
     * @param[in] loc The file location of a object.
     * @return Status of the call.
     */
    Status FallocateInplace(const ObjectLocation &loc);

    /**
     * @brief Delete Large object on spill file.
     * @param[in] objectKey objectKey The object key.
     * @param[in] loc The location of a object.
     * @param[out] fileInfoMap The fileInfo map.
     */
    void DeleteLargeObj(const std::string &objectKey, const ObjectLocation &loc,
                        std::unordered_map<std::string, FileInfo> &fileInfoMap);

    /**
     * @brief Load the data from file.
     * @tparam F1 The read spill buffer function type.
     * @tparam F2 The ready file function type.
     * @param[in] objectKey The Id of the object to be loaded.
     * @param[in] readBufferFunc The read spill buffer function
     * @param[in] readFileFunc The ready file function
     * @return Status
     */
    template <typename F1, typename F2>
    Status LoadFromDiskImpl(const std::string &objectKey, F1 &&readBufferFunc, F2 &&readFileFunc)
    {
        std::shared_lock<std::shared_timed_mutex> rLock(fileInfoMutex_);
        uint64_t size = 0;
        if (buffer_.Exist(objectKey, size)) {
            return readBufferFunc(buffer_);
        }

        ObjectLocation loc;
        RETURN_IF_NOT_OK(GetObjectFileLocation(objectKey, loc));
        PerfPoint p(PerfKey::WORKER_SPILL_READ_FILE);
        FileInfo *fileInfo;
        std::shared_ptr<ActiveSpillFile> file;
        bool isReopen = false;
        RETURN_IF_NOT_OK(GetFileInfoPtr(objectKey, loc.path, fileInfo));
        if (fileInfo->file != nullptr) {
            file = fileInfo->file;
            rLock.unlock();
        } else {
            rLock.unlock();
            std::lock_guard<std::shared_timed_mutex> wLock(fileInfoMutex_);
            RETURN_IF_NOT_OK(GetObjectFileLocation(objectKey, loc));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                GetFileInfoAndReopenFile(objectKey, loc.path, fileInfo, isReopen),
                FormatString("Get fileInfo failed, objectKey: %s, path: %s", objectKey, loc.path));
            file = fileInfo->file;
        }
        Timer timer;
        Status rc = readFileFunc(file, loc.offset, loc.size);
        LOG(INFO) << FormatString("Id: %d, Read object [%s], fd: %d, path: %s, offset: %ld, size: %ld, cost: %fms", id_,
                                  objectKey, file->GetFd(), loc.path, loc.offset, loc.size, timer.ElapsedMilliSecond());
        if (isReopen && FdCountExceedLimit()) {
            std::lock_guard<std::shared_timed_mutex> wLock(fileInfoMutex_);
            Status status = GetFileInfoPtr(objectKey, loc.path, fileInfo);
            if (status.IsOk()) {
                CloseFileIfExceedLimit(fileInfo);
            }
        }
        return rc;
    }

    /**
     * @brief Disable readahead to relove the read amplification problem for offset read.
     * @param fd The fd.
     */
    void DisableReadAheadIfNeed(int fd);

    // I/O rate limit Throttle
    Throttle throttle_;

    // fallocate task queue, {objectKey, Location}
    std::vector<std::pair<std::string, ObjectLocation>> fallocateQueue_;
    // mutex for fallocateQueue_;
    std::shared_timed_mutex fallocateQueueMutex_;

    // Directories that to save the spilled file.
    // [<fileURL/prefix>, ...]
    // Each string is under the format <fileURL>/<prefix> followed by a UUID
    // E.g. "/tmp/file/path/spill" which generate a series files as the following:
    // "/tmp/file/path/spill758b73e5-e3a7-4fd6-9044-77651bf79962"
    std::string spillDir_;

    std::shared_timed_mutex fileInfoMutex_;

    // tenant ID -> file info map
    std::unordered_map<std::string, std::unordered_map<std::string, FileInfo>> tenant2FileInfo_;

    // Record the spilled location of each object.
    std::unordered_map<std::string, ObjectLocation> objLocations_;  // {objectKey: spilledLocation}

    SpillBuffer buffer_;
    size_t sizeThreshold_ = 10 * 1024;
    uint32_t id_;
};

class WorkerOcSpill {
public:
    /**
     * @brief Obtain the instance of this singleton class.
     * @return The pointer of WorkerOcSpill.
     */
    static WorkerOcSpill *Instance();

    ~WorkerOcSpill();

    /**
     * @brief Spill the object to the external storage.
     * @param[in] objectKey The Id of the object to be spilled.
     * @param[in] buffer The pointer to the buffer stores the object data.
     * @param[in] size The size of the buffer to be spilled.
     * @param[in] evictable The object is evictable or not.
     * @return Status of the call.
     */
    Status Spill(const std::string &objectKey, const void *buffer, size_t size, bool evictable = false);

    Status Spill(const std::string &objectKey, const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads,
                 bool evictable = false);

    /**
     * @brief Delete the object that has already spilled to the external storage.
     * @param[in] objectKey The Id of the object to be deleted.
     * @return Status of the call.
     */
    Status Delete(const std::string &objectKey);

    /**
     * @brief Get the object spilled data from external storage.
     * @param[in] objectKey The Id of the object to be loaded.
     * @param[out] buffer The pointer to the buffer that the object data will be loaded.
     * @param[in] size The size of the data to be loaded.
     * @param[in] offset The offset of the data to be loaded.
     * @return Status of the call.
     */
    Status Get(const std::string &objectKey, void *buffer, size_t size, size_t offset = 0);

    /**
     * @brief Get the object spilled data from external storage.
     * @param[in] objectKey The Id of the object to be loaded.
     * @param[out] messages The rpc messages that the object data will be loaded.
     * @param[in] size The size of the data to be loaded.
     * @param[in] offset The offset of the data to be loaded.
     * @return Status of the call.
     */
    Status Get(const std::string &objectKey, std::vector<RpcMessage> &messages, size_t size, size_t offset = 0);

    /**
     * @brief Init procedure of this request handler.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Check whether spill is enabled.
     * @return True if spill is enabled.
     */
    bool IsEnabled() const;

    /**
     * @brief Get object location, for testing.
     * @param[in] objectKey The ID of the object that need to query.
     * @return Return SPILL_BUFFER if object exist in spill buffer or file path if object exist in file.
     */
    std::string GetObjectLocation(const std::string &objectKey);

    /**
     * @brief Parse spill directories from gflag string.
     * @return std::vector<std::string>.
     */
    static std::vector<std::string> ParseGFlagDirectories();

    /**
     * @brief Get all spilled file name by path.
     * @param[in] The path of spilled.
     * @return The files of spilled.
     */
    static std::vector<std::string> GetSpilledFileName(const std::string &path);

    /**
     * @brief Get active size of spilled.
     * @return The size of spilled.
     */
    uint64_t GetSpilledSize();

    /**
     * @brief Check whether the spill space is full.
     * @param[in] size The size of object need to spill.
     * @return True if spill space is full.
     */
    bool IsSpaceFull(size_t size);

    /**
     * @brief Check whether the spill space exceed lower watermark.
     * @param[in] size The size of object need to spill.
     * @return True if spill space is exceed lower watermark.
     */
    bool IsSpaceExceedLWM(size_t size = 0);

    /**
     * @brief Check whether the spill space exceed high watermark.
     * @param[in] size The size of object need to spill.
     * @return True if spill space is exceed high watermark.
     */
    bool IsSpaceExceedHWM(size_t size = 0);

    /**
     * @brief Check whether the spill space exceed the specified ratio.
     * @param[in] ratio The specified ratio.
     * @param[in] size The size of object need to spill.
     * @return True if spill space is exceed the specified ratio.
     */
    bool IsSpaceExceed(double ratio, size_t size);

    /**
     * @brief Check whether the spill space exceed lower watermark.
     * @param[in] size The size of object need to spill.
     * @return True if spill space is exceed lower watermark.
     */
    bool IsActiveSpillSizeExceedLWM(size_t size = 0);

    /**
     * @brief Check whether the spill space exceed high watermark.
     * @param[in] size The size of object need to spill.
     * @return True if spill space is exceed high watermark.
     */
    bool IsActiveSpillSizeExceedHWM(size_t size = 0);

    /**
     * @brief Check whether the spill space exceed the specified ratio.
     * @param[in] ratio The specified ratio.
     * @param[in] size The size of object need to spill.
     * @return True if spill space is exceed the specified ratio.
     */
    bool IsActiveSpillSizeExceed(double ratio, size_t size);

    /**
     * @brief Get remain active spill size.
     * @return Remain active spill size.
     */
    uint64_t GetRemainActiveSpillSize();

    /**
     * @brief Get low water factor.
     * @return Low water factor.
     */
    double LowWaterFactor() const
    {
        return lowWaterFactor_;
    }

    /**
     * @brief Get high water factor.
     * @return High water factor.
     */
    double HighWaterFactor() const
    {
        return highWaterFactor_;
    }

    /**
     * @brief Get the total spill space usage.
     * @return Usage: "spaceUsage/physicalSpaceUsage/totalLimit/workerSpillHardDiskUsage"
     */
    std::string GetSpillUsage();

    /**
     * @brief Get the size of the spill limit.
     * @note   FlagS_spill_size_limit=0 means spill_size_limit is 95% of the initial disk space
     * @return  uint64_t - The size of the spill limit.
     */
    uint64_t GetSpillLimitSize();

    /**
     * @brief Wake up compact thread to GC.
     */
    void ForceCompact()
    {
        waitPost_.Set();
    }

    /**
     * @brief Get eviction list.
     * @return Eviction list.
     */
    EvictionList &GetEvictionList()
    {
        return spillEvictionList_;
    }

private:
    friend std::unique_ptr<WorkerOcSpill> std::make_unique<WorkerOcSpill>();

    WorkerOcSpill() = default;

    /**
     * @brief Calculate a index of fileMgr_ according to objectKey.
     * @param[in] objectKey The Id of the object need to be calculated.
     * @return Index of fileMgr_.
     */
    size_t GetMgrIndex(const std::string &objectKey);

    /**
     * @brief Compact the spill file.
     */
    void Compact();

    // The flag whether to stop compaction
    std::atomic<bool> stopCompaction_{ false };
    WaitPost waitPost_;
    // compaction thread
    Thread spillCompactionThread_;

    // Pointer to the SpillFileManager instance.
    std::vector<std::unique_ptr<SpillFileManager>> fileMgr_;
    // The size of all active spilled object data that has not been deleted from file and is still in use
    std::atomic<uint64_t> totalActiveSpilledSize_{ 0 };

    // The  95% of the disk free space when the worker is started.
    uint64_t initial95SpillFreeSpace_ = 0;
    // The free space of disk in real time
    uint64_t freeSpaceInRealTime_ = 0;

    EvictionList spillEvictionList_;

    static constexpr double highWaterFactor_ = 0.8;
    static constexpr double lowWaterFactor_ = 0.6;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_OC_SPILL_H
