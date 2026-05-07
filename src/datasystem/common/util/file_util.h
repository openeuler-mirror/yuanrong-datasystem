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
 * Description: File util.
 */
#ifndef DATASYSTEM_COMMON_UTIL_FILE_UTIL_H
#define DATASYSTEM_COMMON_UTIL_FILE_UTIL_H

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>
#include <sys/types.h>
#include <unistd.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
/**
 * @brief Get file size in bytes.
 * @param[in] filename Filename.
 * @param[in] logError Whether to log error when failed.
 * @return File size in bytes.
 */
off_t FileSize(const std::string &filename, bool logError = true);

/**
 * @brief Get the file size via the file descriptor.
 * @param[in] fd The file descriptor.
 * @return size_t Size in bytes.
 */
size_t FdFileSize(const int fd);

/**
 * @brief Get file size in bytes.
 * It is similar to FileSize call, but this one returns a Status code to capture any IO errors that may have happened
 * when checking the file size.
 * @param[out] size The size of the file returned.
 * @param[in] filename Filename.
 * @return Status of the call.
 */
Status CheckFileSize(size_t *size, const std::string &filename);

/**
 * @brief Get the filesystem's available space of the path.
 * @param[in] path the path to be checked.
 * @return Bytes available of the filesystem of the path.
 */
uint64_t GetFreeSpaceBytes(const std::string &path);

/**
 * @brief Check whether the file or directory exists.
 * @param[in] filename Filename.
 * @param[in] mode Optional mask to specify a permission check (R_OK, W_OK, X_OK). Dft = F_OK.
 * @return True if file or directory exist, otherwise false.
 */
bool FileExist(const std::string &filename, int mode = F_OK);

/**
 * @brief Check if the file exists. Returns a status so if the system call itself failed for other reasons (besides
 * just not existing) then it gives a filesystem error.
 * @param[out] fileExists True if the file exists.
 * @param[in] filename Filename.
 * @param[in] mode Optional mask to specify a permission check (R_OK, W_OK, X_OK). Dft = F_OK.
 * @return Status of the call.
 */
Status CheckFileExists(bool *fileExists, const std::string &filename, int mode = F_OK);

/**
 * @brief Get file last modified time via filename.
 * @param[in] filename Filename.
 * @param[out] timestamp Timestamp of the time when the file was last modified.
 * @return Status of the call.
 */
Status GetFileModifiedTime(const std::string &filename, int64_t &timestamp);

/**
 * @brief Convert a microsecond-level Unix timestamp to a formatted string.
 * @param[in] timestamp_us The timestamp in microseconds.
 * @return A string formatted as `YYYYMMDDHHMMSS` representing the given timestamp.
 */
std::string FormatTimestampToString(int64_t timestamp_us);

/**
 * @brief Get the current local time as a formatted string.
 * @return A string formatted as `YYYYMMDDHHMMSS` representing the local time.
 */
std::string GetCurrentTimestamp();

/**
 * @brief Delete file via filename.
 * @param[in] filename Filename.
 * @return Status of the call.
 */
Status DeleteFile(const std::string &filename);

/**
 * @brief Rename a file from the source path to the target path.
 * @param[in] srcFile The source file path (original name).
 * @param[in] targetFile The target file path (new name).
 * @return Status of the call.
 */
Status RenameFile(const std::string &srcFile, const std::string &targetFile);

/**
 * @brief Join two path fragments with exactly one slash.
 * @param[in] lhs Left-hand path fragment.
 * @param[in] rhs Right-hand path fragment.
 * @return Joined path.
 */
std::string JoinPath(const std::string &lhs, const std::string &rhs);

/**
 * @brief Ensure that the target file exists.
 * @param[in] path Target file path.
 * @return Status of the call.
 */
Status EnsureFile(const std::string &path);

/**
 * @brief Flush file content and metadata to disk.
 * @param[in] fd Target file descriptor.
 * @return Status of the call.
 */
Status FsyncFd(int fd);

/**
 * @brief Flush directory metadata to disk.
 * @param[in] dirPath Target directory path.
 * @return Status of the call.
 */
Status FsyncDir(const std::string &dirPath);

/**
 * @brief Read the whole file into memory.
 * @param[in] path Target file path.
 * @param[out] content Full file content.
 * @return Status of the call.
 */
Status ReadWholeFile(const std::string &path, std::string &content);

/**
 * @brief Atomically replace a text file via temp-file + fsync + rename.
 * @param[in] path Target file path.
 * @param[in] content File content to persist.
 * @return Status of the call.
 */
Status AtomicWriteTextFile(const std::string &path, const std::string &content);

constexpr const char *WORKER_ENV_FILE_NAME = "env";
constexpr const char *WORKER_ENV_POD_IP_KEY = "pod_ip";

/**
 * @brief Get the persisted worker environment file path from log directory.
 * @param[in] logDir Worker/client log directory.
 * @return Empty if logDir is empty, otherwise <logDir>/env.
 */
std::string GetWorkerEnvFilePath(const std::string &logDir);

/**
 * @brief Get string value from environment variable or the persisted worker environment file.
 * @param[in] env Environment variable.
 * @param[in] filePath Persisted worker env file path. Empty means file recovery is disabled.
 * @param[in] key Persisted key to read or update. For host affinity this is the actual env variable name from
 * `FLAGS_host_id_env_name` or `ServiceDiscoveryOptions::hostIdEnvName`, such as `JDOS_HOST_IP`.
 * @param[in] defValue Default value if environment variable and persisted file are unavailable.
 * @return Environment value first, then persisted value, otherwise default value.
 */
std::string GetStringFromEnvOrFile(const char *env, const std::string &filePath, const std::string &key,
                                   const std::string &defValue);

/**
 * @brief Check whether the file is directory.
 * @param[in] path File path.
 * @param[out] isDir Determine is directory.
 * @return Status of the call.
 */
Status IsDirectory(const std::string &path, bool &isDir);

/**
 * @brief Create directory via given directory path.
 * @param[in] dir directory path.
 * @param[in] recursively Create directory recursively.
 * @param[in] mode Permission mode.
 * @return Status of the call.
 */
Status CreateDir(const std::string &dir, bool recursively = false, uint32_t mode = 0755);

/**
 * @brief Get set of filenames via regex pattern.
 * @param[in] pathPattern regex pattern.
 * @param[out] paths Set of filenames matching the regex pattern.
 * @return Status of the call.
 */
Status Glob(const std::string &pathPattern, std::vector<std::string> &paths);

/**
 * @brief Compress file into .gz format.
 * @param[in] src Source filename.
 * @param[in] dest Destination filename.
 * @return Status of the call.
 */
Status CompressFile(const std::string &src, std::string &dest);

/**
 * @brief Validate a file Descriptor.
 * @param[in] pathname Pathname of the file.
 * @param[in] fileDescriptor The fd to be validated.
 * @return Status of the call.
 */
Status ValidateFD(const std::string &pathname, int fileDescriptor);

/**
 * @brief Open file and return a Status.
 * @param[in] pathname Pathname of the file.
 * @param[in] flag Flag for different access modes.
 * @param[out] fileDescriptor The fd to be returned.
 * @return Status of the call.
 */
Status OpenFile(const std::string &pathname, int flags, int *fileDescriptor);

/**
 * @brief Open file and return a Status.
 * @param[in] pathname Pathname of the file.
 * @param[in] flag Flag for different access modes.
 * @param[in] mode File mode bits to be applied when a new file is created.
 * @param[out] fileDescriptor The fd to be returned.
 * @return Status of the call.
 */
Status OpenFile(const std::string &pathname, int flags, mode_t mode, int *fileDescriptor);

/**
 * @brief Set NOFILE limit per process.
 * @param[in] softLimit The number file limit opened that should not be surpassed.
 * @return Status of the call.
 */
Status SetFileLimit(rlim_t softLimit);

/**
 * @brief Get file last modified timestamp.
 * @param[in] pathname Pathname of the file to get timestamp.
 * @param[out] timestamp The variable to save last modified timestamp.
 * @return Status of the call.
 */
Status GetFileLastModified(std::string pathname, uint64_t *timestamp);

/**
 * @brief Move all file from old path to new path.
 * @param[in] oldPath old path
 * @param[in] newPath new path
 * @return Status of the call.
 */
Status MoveAll(const std::string &oldPath, const std::string &newPath);

/**
 * @brief Timestamp to date.
 * @param[in] timestamp Timestamp need to transfer.
 * @param[out] date The date of timestamp.
 * @return Status of the call.
 */
Status StringToDateTime(std::string &timestamp, time_t &date);

/**
 * @brief Date to format string.
 * @param[in] date Date need to transfer.
 * @param[out] formatDate The format string.
 * @return Status of the call.
 */
Status GetFormatDate(time_t &date, std::string &formatDate);

/**
 * @brief Reads from the given file descriptor into the buffer provided. This is simply a wrapper for pread and remaps.
 * Any error will be wrapped into the Status return code.
 * @note In this read call, if the amount of bytes read is not equal to the requested amount of bytes, then the call is
 * considered to be a failure. Thus, it does not return the amount of bytes read.
 * @param[in] fileDescriptor The descriptor for the file to read from.
 * @param[out] buffer Pointer to the buffer to read in to.
 * @param[in] count The requested number of bytes to read.
 * @param[in] offset The offset in the file to read from.
 * @return Status of the call.
 */
Status ReadFile(int fileDescriptor, void *buffer, size_t count, off_t offset);

/**
 * @brief Writes to a given file descriptor from the buffer provided. This is simply a wrapper for pwrite and remaps.
 * Any errors will be wrapped into the Status return code.
 * @note In this write call, if the amount of bytes written is not equal to the requested amount of bytes, then the call
 * is considered to be a failure. Thus, it does not return the amount of bytes written.
 * @param[in] fileDescriptor The descriptor for the file to write to.
 * @param[in] buffer Pointer to the buffer to write from.
 * @param[in] count The requested number of bytes to write.
 * @param[in] offset The offset in the file to write to.
 * @return Status of the call.
 */
Status WriteFile(int fileDescriptor, const void *buffer, size_t count, off_t offset);

/**
 * @brief Writes to a given file descriptor from the buffer provided. This is simply a wrapper for pwrite and remaps.
 * Any errors will be wrapped into the Status return code, but not output any logs.
 * @note In this write call, if the amount of bytes written is not equal to the requested amount of bytes, then the call
 * is considered to be a failure. Thus, it does not return the amount of bytes written.
 * @param[in] fileDescriptor The descriptor for the file to write to.
 * @param[in] buffer Pointer to the buffer to write from.
 * @param[in] count The requested number of bytes to write.
 * @param[in] offset The offset in the file to write to.
 * @return Status of the call.
 */
Status WriteFileNoErrorLog(int fileDescriptor, const void *buffer, size_t count, off_t offset);

/**
 * @brief A templated retry loop designed to work with any of the file util functions that return a Status code.
 * This function will simply retry the identical call each time if the call returns an error status (does not matter
 * what the error was) up to the given number of retries. An error is returned if the maximum number of retries is
 * attempted and the call is still failing.
 * @param[in] maxRetries The maximum number of times to retry the call.
 * @param[in] sleepTime The time in milliseconds for how long to sleep in between each retry
 * @param[in/out] func A function reference to a file operation call such as the ones defined here in file_util.h
 * @param[in/out] args The arguments for the func
 * @return Status of the call.
 */
template <class Function, class... Args>
Status RetryFileOperation(int maxRetries, int sleepTime, Function &&func, Args &&...args)
{
    int retryCount = 0;
    Status rc;
    do {
        rc = func(std::forward<Args>(args)...);
        if (rc.IsOk()) {
            break;
        } else {
            ++retryCount;
            LOG(INFO) << "Attempt: " + std::to_string(retryCount) + ". File operation failed with rc: " + rc.ToString();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
        }
    } while (rc.IsError() && retryCount < maxRetries);

    if (retryCount == maxRetries) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, "Maximum retries exceeded. Failed to perform file operation.");
    }

    if (retryCount > 0) {
        LOG(INFO) << "File operation succeeded after " << retryCount << " retries.";
    }
    return Status::OK();
}

/**
 * @brief Read file content into the buffer provided.
 * @param[in] path The file path.
 * @param[out] buffer The read data.
 * @return Status of the call.
 */
Status ReadFileToString(const std::string &path, std::string &buffer);

/**
 * @brief Helper function to generate a debug string for fd.
 * @param[in] fd The File Descriptor.
 * @param[in] offset The offset given.
 * @param[in] size The sive given.
 * @return String of the debug message.
 */
std::string FdErrorMsg(int fd, off_t offset, size_t size);

/**
 * @brief Join a list of string as a path url.
 * @param[in] segments A vector of string of each part of the url.
 * @return String of the path url.
 */
std::string JoinPath(const std::vector<std::string> &segments);

/**
 * @brief Change File permission.
 * @param[in] path path to file.
 * @param[in] permission new file permission.
 * @return Status of the call.
 */
Status ChangeFileMod(const std::string &path, const mode_t &permission);

/**
 * @brief Delete an empty directory or a file
 * @param[in] path path to file or directory.
 * @return Status of the call.
 */
Status Remove(const std::string &path);

/**
 * @brief Delete directories and files recursively
 * @param[in] path path to file or directory.
 * @return Status of the call.
 */
Status RemoveAll(const std::string &path);

/**
 * @brief Resize file
 * @param[in] path path to file.
 * @return Status of the call.
 */
Status ResizeFile(const std::string &path, size_t newSize);

/**
 * @brief Check whether the directory empty
 * @param[in] path path of directory.
 * @return True if empty, otherwise false.
 */
bool IsEmptyDir(const std::string &path);

/**
 * @brief Move the file to a new path.
 * @param[in] filePath Path of the file to be moved.
 * @param[in] newPath Path to which the file needs to be moved.
 * @return K_OK if file moved successfully, otherwise false.
 */
Status MoveFileToNewPath(const std::string &filePath, const std::string &newPath);

/**
 * @brief Check if path is safe path.
 * @param[in] path File path.
 * @param[in] newPath Path to which the file needs to be moved.
 * @return True if path is safe path.
 */
bool IsSafePath(const std::string &path);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_FILE_UTIL_H
