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
#include "datasystem/common/util/file_util.h"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <ios>
#include <ostream>
#include <iomanip>
#include <string>
#include <type_traits>
#include <unordered_set>

#include <dirent.h>
#include <fcntl.h>
#include <glob.h>
#include <linux/limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <securec.h>
#include <unistd.h>
#include <zlib.h>

#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/utils/status.h"

#define STREAM_RETRY_ON_EINTR(nread, stream, expr)                                                              \
    do {                                                                                                        \
        static_assert(std::is_unsigned<decltype(nread)>::value == true, #nread " must be an unsigned integer"); \
        (nread) = (expr);                                                                                       \
    } while ((nread) == 0 && ferror(stream) == EINTR)

static thread_local bool ifPrintwriteFailLog = true;

namespace datasystem {
off_t FileSize(const std::string &filename)
{
    struct stat st{};
    errno = 0;
    if (stat(filename.c_str(), &st) < 0) {
        LOG(ERROR) << "Get file size failed, file: " << filename << ", errno: " << errno;
        return -1;
    };
    return st.st_size;
}

size_t FdFileSize(const int fileDescriptor)
{
    struct stat st{};
    auto res = fstat(fileDescriptor, &st);
    if (res == 0) {
        return st.st_size;
    }
    return 0;
}

Status CheckFileSize(size_t *size, const std::string &filename)
{
    struct stat st{};
    auto res = stat(filename.c_str(), &st);
    if (res == 0) {
        *size = st.st_size;
        return Status::OK();
    }

    // Otherwise, the stat call had a problem.
    *size = 0;
    std::string err = "Stat call for file " + filename + " failed with errno: " + std::to_string(errno);
    RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, err);
}

bool IsSafePathLength(const std::string &path)
{
    return path.size() < PATH_MAX;
}

uint64_t GetFreeSpaceBytes(const std::string &path)
{
    if (!IsSafePathLength(path)) {
        LOG(ERROR) << "Illegal path length: " << path.size();
        return 0;
    }
    struct statvfs st {};
    char realPath[PATH_MAX + 1] = { 0 };
    if (realpath(path.c_str(), realPath) == nullptr) {
        return 0;
    }
    auto res = statvfs(realPath, &st);
    if (res == 0) {
        if (st.f_bsize == 0) {
            LOG(ERROR) << "Get free space bytes failed, f_bsize is 0";
            return 0;
        }
        if (st.f_bavail <= UINT64_MAX / st.f_bsize) {
            // Ensure the return value not overflow.
            return st.f_bsize * st.f_bavail;
        }
        // The available space is greater than UINT64_MAX.
        // Return the largest size a 64-bit OS can support.
        return UINT64_MAX;
    }
    return 0;
}

std::vector<std::string> SplitPath(const std::string &path)
{
    if (path.empty()) {
        return {};
    }
    std::vector<std::string> segments;
    if (path[0] == '/') {
        segments.emplace_back("/");
    }

    char *s = new (std::nothrow) char[path.length() + 1];
    if (s == nullptr) {
        return {};
    }
    int ret = strcpy_s(s, path.length() + 1ul, path.c_str());
    if (ret != EOK) {
        delete[] s;
        return {};
    }
    char *savePtr = nullptr;
    char *p = strtok_s(s, "/", &savePtr);
    while (p != nullptr) {
        std::string tmp = p;
        segments.emplace_back(tmp);
        p = strtok_s(nullptr, "/", &savePtr);
    }
    delete[] s;

    return segments;
}

std::string JoinPath(const std::vector<std::string> &segments)
{
    if (segments.empty()) {
        return "";
    }
    std::stringstream s;
    for (auto p = segments.begin(); p != segments.end(); ++p) {
        s << *p;
        if (p != segments.end() - 1) {
            s << '/';
        }
    }
    return s.str();
}

bool FileExist(const std::string &filename, int mode)
{
    return access(filename.c_str(), mode) == 0;
}

Status CheckFileExists(bool *fileExists, const std::string &filename, int mode)
{
    auto res = access(filename.c_str(), mode);
    if (res == 0) {
        // Access call was success, and the file exists.
        *fileExists = true;
        return Status::OK();
    }

    // The call successfully determined that the file did not exist. But it's not a failure
    // of the system call itself. The file simply did not exist.
    if (errno == ENOENT) {
        *fileExists = false;
        return Status::OK();
    }

    // Otherwise, the access to the file itself had a problem. This could be permission error, filesystem IO
    // error, or other errors.
    *fileExists = false;
    std::string err = "Access call for file " + filename + " failed with errno: " + std::to_string(errno);
    RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, err);
}

Status GetFileModifiedTime(const std::string &filename, int64_t &timestamp)
{
    struct stat statBuf{};
    if (stat(filename.c_str(), &statBuf) != 0) {
        std::stringstream ss;
        ss << "Get file " << filename << " last modify time failed.";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
    }

    const int thousandsOfMagnitude = 1000;
    const int millionOfMagnitude = 1000'000;
    timestamp = statBuf.st_mtim.tv_sec * millionOfMagnitude + statBuf.st_mtim.tv_nsec / thousandsOfMagnitude;
    return Status::OK();
}

namespace time_format {
constexpr int YEAR_WIDTH = 4;
constexpr int MONTH_WIDTH = 2;
constexpr int DAY_WIDTH = 2;
constexpr int HOUR_WIDTH = 2;
constexpr int MINUTE_WIDTH = 2;
constexpr int SECOND_WIDTH = 2;
constexpr int TM_YEAR_BASE = 1900;
}  // namespace time_format

std::string FormatTimestampToString(int64_t timestampUs)
{
    using namespace std::chrono;
    auto us = microseconds(timestampUs);
    auto tp = time_point<system_clock>(us);

    auto t = system_clock::to_time_t(tp);
    struct tm *time_info = std::localtime(&t);
    if (!time_info) {
        return std::to_string(timestampUs);
    }

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(time_format::YEAR_WIDTH) << (time_info->tm_year + time_format::TM_YEAR_BASE)
        << std::setw(time_format::MONTH_WIDTH) << (time_info->tm_mon + 1) << std::setw(time_format::DAY_WIDTH)
        << time_info->tm_mday << std::setw(time_format::HOUR_WIDTH) << time_info->tm_hour
        << std::setw(time_format::MINUTE_WIDTH) << time_info->tm_min << std::setw(time_format::SECOND_WIDTH)
        << time_info->tm_sec;

    return oss.str();
}

std::string GetCurrentTimestamp()
{
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now = *std::localtime(&time_t_now);

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(time_format::YEAR_WIDTH) << (tm_now.tm_year + time_format::TM_YEAR_BASE)
        << std::setw(time_format::MONTH_WIDTH) << (tm_now.tm_mon + 1) << std::setw(time_format::DAY_WIDTH)
        << tm_now.tm_mday << std::setw(time_format::HOUR_WIDTH) << tm_now.tm_hour
        << std::setw(time_format::MINUTE_WIDTH) << tm_now.tm_min << std::setw(time_format::SECOND_WIDTH)
        << tm_now.tm_sec;

    return oss.str();
}

Status DeleteFile(const std::string &filename)
{
    if (unlink(filename.c_str()) != 0) {
        std::stringstream ss;
        ss << "Delete file " << filename << " failed with error:" << errno;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
    }
    return Status::OK();
}

Status RenameFile(const std::string &srcFile, const std::string &targetFile)
{
    // Remove the target file if it already exists
    (void)std::remove(targetFile.c_str());

    // Rename the source file to the target file
    if (std::rename(srcFile.c_str(), targetFile.c_str()) != 0) {
        std::stringstream ss;
        ss << "Rename file " << srcFile << " failed with error:" << errno;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
    }

    return Status::OK();
}

Status IsDirectory(const std::string &path, bool &isDir)
{
    struct stat statBuf{};
    if (stat(path.c_str(), &statBuf) != 0) {
        std::stringstream ss;
        ss << "error while invoke IsDirectory(" << path << "), errno: " << errno;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
    }
    isDir = S_ISDIR(statBuf.st_mode);
    return Status::OK();
}

Status CreateDir(const std::string &dir, bool recursively, uint32_t mode)
{
    if (!recursively) {
        int ret = mkdir(dir.c_str(), mode);
        if (ret != 0) {
            std::stringstream ss;
            ss << "mkdir path: " << dir << " failed with code: " << ret << ", errno: " << errno
               << ", errmsg: " << StrErr(errno);
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
        }
    } else {
        std::vector<std::string> segments = SplitPath(dir);
        std::string partialPath;
        for (const auto &segment : segments) {
            partialPath = (partialPath.empty() || partialPath == "/") ? partialPath.append(segment)
                                                                      : partialPath.append("/").append(segment);
            // Check whether the partialPath is directory.
            struct stat statBuf{};
            if (stat(partialPath.c_str(), &statBuf) == 0 && S_ISDIR(statBuf.st_mode)) {
                continue;
            }
            RETURN_IF_NOT_OK(CreateDir(partialPath, false, mode));
        }
    }
    return Status::OK();
}

Status Glob(const std::string &pathPattern, std::vector<std::string> &paths)
{
    glob_t result;

    int ret = glob(pathPattern.c_str(), GLOB_TILDE | GLOB_ERR, nullptr, &result);
    switch (ret) {
        case 0:
            break;
        case GLOB_NOMATCH:
            globfree(&result);
            return Status::OK();
        case GLOB_NOSPACE:
            globfree(&result);
            RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, "Out Of Memory Error.");
        default:
            globfree(&result);
            std::stringstream ss;
            ss << "glob failed, pattern:" << pathPattern << ", ret:" << ret << ", errmsg:" << StrErr(errno);
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
    }

    for (size_t i = 0; i < result.gl_pathc; ++i) {
        paths.emplace_back(result.gl_pathv[i]);
    }

    globfree(&result);
    return Status::OK();
}

Status Read(FILE *f, uint8_t *buf, size_t *pSize)
{
    size_t numReads = 0;
    size_t size = *pSize;
    // If fread_unlocked() return value is EINTR, the system call is interrupted.
    // Need to retry to read.
    STREAM_RETRY_ON_EINTR(numReads, f, fread_unlocked(buf, 1, size, f));
    if (numReads < size) {
        if (feof(f)) {
            *pSize = numReads;
        } else {
            std::stringstream ss;
            ss << "IOError occurred! errno: " << errno;
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
        }
    }
    return Status::OK();
}

Status CompressFile(const std::string &src, std::string &dest)
{
    FILE *file = fopen(src.c_str(), "r");
    RETURN_RUNTIME_ERROR_IF_NULL(file);
    gzFile gzf = gzopen(dest.c_str(), "w");
    if (gzf == nullptr) {
        fclose(file);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("The destination gz file: %s pointer is null.", dest));
    }

    size_t size = 32 * 1024ul;  // Just represent 32KB, ugly CI!
    uint8_t buf[size];
    while (true) {
        Status status = Read(file, buf, &size);
        if (!status.IsOk()) {
            (void)gzclose(gzf);
            (void)fclose(file);
            return status;
        }
        if (size == 0) {
            break;
        }
        int n = gzwrite(gzf, buf, size);
        if (n == 0) {
            int err;
            const char *errStr = gzerror(gzf, &err);
            std::stringstream ss("Unable to write to gzip output", std::ios_base::ate);
            if (errStr != nullptr) {
                ss << ": " << errStr;
            }
            (void)gzclose(gzf);
            (void)fclose(file);
            status = DeleteFile(dest);
            RETURN_STATUS(status.IsOk() ? StatusCode::K_RUNTIME_ERROR : status.GetCode(), ss.str() + status.GetMsg());
        } else if (n != static_cast<int>(size)) {
            (void)gzclose(gzf);
            (void)fclose(file);
            status = DeleteFile(dest);
            RETURN_STATUS(status.IsOk() ? StatusCode::K_RUNTIME_ERROR : status.GetCode(),
                          FormatString("The gz file: %s is not fully written. %s.", dest, status.GetMsg()));
        }
    }
    (void)gzclose(gzf);
    (void)fclose(file);
    // Change mode to 0440, we only allow the read permission. And we
    // will never check the return even the chmod operation is failed.
    mode_t permission = 0440;  // Change permission to 0440.
    RETURN_IF_NOT_OK(ChangeFileMod(dest.c_str(), permission));
    return Status::OK();
}

Status ValidateFD(const std::string &pathname, int fileDescriptor)
{
    if (fileDescriptor < 0) {
        StatusCode currentCode = StatusCode::K_IO_ERROR;
        // We want to explicitly print out if it reaches number of file opened limit.
        // In future we might retry instead of giving a FATAL error.
        if (errno == EMFILE) {
            currentCode = StatusCode::K_FILE_LIMIT_REACHED;
        }
        RETURN_STATUS_LOG_ERROR(currentCode, "Could not open " + pathname + ", errno: " + std::to_string(errno));
    }
    return Status::OK();
}

Status OpenFile(const std::string &pathname, int flags, int *fileDescriptor)
{
    *fileDescriptor = open(pathname.c_str(), flags);
    return ValidateFD(pathname, *fileDescriptor);
}

Status OpenFile(const std::string &pathname, int flags, mode_t mode, int *fileDescriptor)
{
    *fileDescriptor = open(pathname.c_str(), flags, mode);
    return ValidateFD(pathname, *fileDescriptor);
}

Status SetFileLimit(rlim_t softLimit)
{
    // Set in one process, child threads within the same process
    // will share the limit.
    struct rlimit rlimSet, rlimGet;
    if (getrlimit(RLIMIT_NOFILE, &rlimGet) != 0) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, "getrlimit failed, errno: " + std::to_string(errno));
    }

    // If the current max hard limit is infinite, then we'll create a setting for hard limit based on system limit.
    rlim_t userMax = rlimGet.rlim_max;
    if (userMax == RLIM_INFINITY) {
        LOG(INFO) << "Current FD limit is unlimited, check system's FD limit.";
        std::ifstream ifs("/proc/sys/fs/file-max");
        if (!ifs.is_open()) {
            RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, "Cannot open file-max.");
        }
        ifs >> userMax;
        if (userMax == 0) {
            RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, "Cannot retrieve limit from file-max.");
        }
    }

    // Now configure the hard limit to set (will be either the user's hard limit or system hard limit).
    rlimSet.rlim_max = userMax;

    // Soft limit cannot exceed the hard limit.
    if (softLimit > rlimSet.rlim_max) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR,
                                "setrlimit exceeds the hard limit. Soft limit: " + std::to_string(softLimit)
                                    + " Hard limit: " + std::to_string(rlimSet.rlim_max));
    }

    // Setting soft limit. This overrides any existing soft limit in the user settings.
    rlimSet.rlim_cur = softLimit;
    if (setrlimit(RLIMIT_NOFILE, &rlimSet) != 0) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, "setrlimit failed, errno: " + std::to_string(errno));
    }
    LOG(INFO) << "Set process file limit to " + std::to_string(softLimit);
    return Status::OK();
}

Status GetFileLastModified(std::string pathname, uint64_t *timestamp)
{
    struct stat sb{};
    if (lstat(pathname.c_str(), &sb) != 0) {
        RETURN_STATUS(StatusCode::K_IO_ERROR, "lstat failed, errno: " + std::to_string(errno));
    }
    if (sb.st_mtime < 0) {
        RETURN_STATUS(StatusCode::K_IO_ERROR, "lstat failed, timestamp is negative: " + std::to_string(sb.st_mtime));
    }
    *timestamp = static_cast<uint64_t>(sb.st_mtime);
    return Status::OK();
}

Status StringToDateTime(std::string &timestamp, time_t &date)
{
    auto chr = timestamp.data();
    date = (time_t)strtol(chr, NULL, 10);  // 10 means using decimal system.
    return Status::OK();
}

Status GetFormatDate(time_t &date, std::string &formatDate)
{
    struct tm tm_time;
    localtime_r(&date, &tm_time);
    std::ostringstream time_pid_stream;
    time_pid_stream.fill('0');
    // formateDate example: 20230101123059
    time_pid_stream << 1900 + tm_time.tm_year << std::setw(2) << 1 + tm_time.tm_mon << std::setw(2) << tm_time.tm_mday
                    << std::setw(2) << tm_time.tm_hour << std::setw(2) << tm_time.tm_min << std::setw(2)
                    << tm_time.tm_sec;
    formatDate = time_pid_stream.str();
    return Status::OK();
}

Status ReadFile(int fileDescriptor, void *buffer, size_t count, off_t offset)
{
    if (offset < 0) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_INVALID, FormatString("offset %lld < 0", offset));
    }
    size_t totalRead = 0;
    ssize_t bytesRead = 0;
    while (totalRead < count) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            SSIZE_MAX - totalRead >= static_cast<size_t>(offset), K_IO_ERROR,
            FormatString("The write offset is overflow, offset:%d > SIZE_MAX:%d", offset + totalRead, SIZE_MAX));
        bytesRead = pread(fileDescriptor, (char *)buffer + totalRead, count - totalRead,
                          offset + static_cast<ssize_t>(totalRead));
        if (bytesRead <= 0) {
            break;
        }
        totalRead += bytesRead;
    }
    if (bytesRead < 0 || totalRead != count) {
        std::stringstream err;
        err << "pread failed ";
        if (bytesRead < 0) {
            err << ". Errno = " << errno;
        } else {
            err << "Expect to read " << count << " bytes but pread returns " << totalRead;
        }
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, err.str());
    }
    return Status::OK();
}

Status WriteFile(int fileDescriptor, const void *buffer, size_t count, off_t offset)
{
    Status rc = WriteFileNoErrorLog(fileDescriptor, buffer, count, offset);
    if (rc.IsError()) {
        if (ifPrintwriteFailLog) {
            ifPrintwriteFailLog = false;
            LOG(ERROR) << "WriteFile failed:" << rc.ToString();
        }
        return rc;
    }

    if (!ifPrintwriteFailLog) {
        ifPrintwriteFailLog = true;
        LOG(INFO) << "WriteFile success again";
    }
    return Status::OK();
}

Status WriteFileNoErrorLog(int fileDescriptor, const void *buffer, size_t count, off_t offset)
{
    if (offset < 0) {
        RETURN_STATUS(StatusCode::K_INVALID, "offset is negative: " + std::to_string(offset));
    }
    // !!!Forbidden using glog to output any logs in this function.
    size_t totalWrite = 0;
    ssize_t bytesWritten = 0;
    while (totalWrite < count) {
        CHECK_FAIL_RETURN_STATUS(
            static_cast<size_t>(offset) <= SSIZE_MAX - totalWrite, K_IO_ERROR,
            FormatString("The write offset is overflow, offset:%llu > SIZE_MAX:%llu", offset + totalWrite, SIZE_MAX));
        bytesWritten = pwrite(fileDescriptor, (char *)buffer + totalWrite, count - totalWrite,
                              offset + static_cast<ssize_t>(totalWrite));
        if (bytesWritten <= 0) {
            break;
        }
        size_t newTotalWrite = totalWrite + static_cast<size_t>(bytesWritten);
        totalWrite = newTotalWrite > totalWrite ? newTotalWrite : SIZE_MAX;
    }
    if (bytesWritten < 0 || totalWrite != count) {
        std::stringstream err;
        err << "pwrite failed ";
        if (bytesWritten < 0) {
            err << ". Errno = " << errno;
        } else {
            err << "Expect to write " << count << " bytes but pwrite returns " << totalWrite;
        }
        RETURN_STATUS(StatusCode::K_IO_ERROR, err.str());
    }
    return Status::OK();
}

Status ReadFileToString(const std::string &path, std::string &buffer)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        std::stringstream ss;
        ss << "Cannot open " << path << ", errno:" << errno << ", msg:" << StrErr(errno);
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, ss.str());
    }
    std::ostringstream ss;
    ss << file.rdbuf();
    buffer = ss.str();
    return Status::OK();
}

std::string FdErrorMsg(int fd, off_t offset, size_t size)
{
    std::stringstream msg;
    msg << "Fd: " << fd << ", Offset: " << offset << ", Size: " << size;
    return msg.str();
}

Status ChangeFileMod(const std::string &path, const mode_t &permission)
{
    if (chmod(path.c_str(), permission) != 0) {
        std::stringstream ss;
        ss << "Change mode on " << path << " fail: " << std::to_string(errno);
        RETURN_STATUS_LOG_ERROR(K_IO_ERROR, ss.str());
    }
    return Status::OK();
}

Status Remove(const std::string &path)
{
    auto ret = remove(path.c_str());
    CHECK_FAIL_RETURN_STATUS(ret == 0, StatusCode::K_IO_ERROR,
                             FormatString("Remove failed. path=%s, errno=%d", path, errno));
    return Status::OK();
}

Status RemoveAll(const std::string &path)
{
    if (!IsSafePathLength(path)) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_INVALID, FormatString("Illegal path length: %zu", path.size()));
    }
    bool isDir;
    int result;
    // path not exist, return ok
    RETURN_OK_IF_TRUE(!FileExist(path));
    char resultPath[PATH_MAX + 1] = { 0 };
    if (realpath(path.c_str(), resultPath) == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Unable to resolve %s. Errno = %d", path, errno));
    }
    const std::string realPath = std::string(resultPath);
    // if input is file (include link)
    if (IsDirectory(realPath, isDir).IsError() || !isDir) {
        result = unlink(realPath.c_str());
        CHECK_FAIL_RETURN_STATUS(result == 0, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("Failed to remove %s, errno:%d", realPath, errno));
        return Status::OK();
    }

    // input is directory
    DIR *dir = opendir(realPath.c_str());
    CHECK_FAIL_RETURN_STATUS(dir != nullptr, K_RUNTIME_ERROR, "Failed to remove: Open dir failed with " + realPath);
    Raii releaseDir([dir] { closedir(dir); });

    struct dirent *ent;
    std::string filePath;
    // remove all files and directories within input path
    while ((ent = readdir(dir)) != nullptr) {
        if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) {
            continue;
        }
        // full path of file
        filePath = realPath + "/" + ent->d_name;
        if (IsDirectory(filePath, isDir).IsOk() && isDir) {
            RETURN_IF_NOT_OK(RemoveAll(filePath));  // call recursively
        } else {
            result = unlink(filePath.c_str());
            CHECK_FAIL_RETURN_STATUS(result == 0, StatusCode::K_RUNTIME_ERROR,
                                     FormatString("Failed to remove file %s, errno:%d", filePath, errno));
        }
    }
    result = rmdir(realPath.c_str());
    CHECK_FAIL_RETURN_STATUS(result == 0, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Failed to remove directory %s, errno:%d", realPath, errno));
    return Status::OK();
}

Status MoveAll(const std::string &oldPath, const std::string &newPath)
{
    if (!IsSafePathLength(oldPath) || !IsSafePathLength(newPath)) {
        RETURN_STATUS_LOG_ERROR(
            StatusCode::K_INVALID,
            FormatString("Illegal old path length: %zu or new path length: %zu", oldPath.size(), newPath.size()));
    }
    bool isDir;
    int result;
    // path not exist, return ok
    RETURN_OK_IF_TRUE(!FileExist(oldPath));
    char resultPath[PATH_MAX + 1] = { 0 };
    if (realpath(oldPath.c_str(), resultPath) == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Unable to resolve %s. Errno = %d", oldPath, errno));
    }
    const std::string realPath = std::string(resultPath);

    char formatNewPath[PATH_MAX + 1] = { 0 };
    if (realpath(newPath.c_str(), formatNewPath) == nullptr) {
        RETURN_STATUS(K_INVALID, FormatString("Invalid new path %s.", newPath));
    }
    const std::string realNewPath = std::string(formatNewPath);

    // if input is file (include link)
    if (IsDirectory(realPath, isDir).IsError() || !isDir) {
        RETURN_IF_NOT_OK_APPEND_MSG(MoveFileToNewPath(oldPath, newPath),
                                    FormatString("move file : %s to %s failed", oldPath, newPath));
        return Status::OK();
    }

    // input is directory
    DIR *dir = opendir(realPath.c_str());
    CHECK_FAIL_RETURN_STATUS(dir != nullptr, K_RUNTIME_ERROR, "Failed to remove: Open dir failed with " + realPath);
    Raii releaseDir([dir] { closedir(dir); });

    struct dirent *ent;
    // remove all files and directories within input path
    while ((ent = readdir(dir)) != nullptr) {
        if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) {
            continue;
        }
        std::string oldFilePath = realPath + "/" + ent->d_name;
        std::string newFilePath = realNewPath;
        if (IsDirectory(oldFilePath, isDir).IsOk() && isDir) {
            RETURN_IF_NOT_OK(MoveAll(oldFilePath, newFilePath));  // call recursively
        } else {
            RETURN_IF_NOT_OK_APPEND_MSG(MoveFileToNewPath(oldFilePath, newFilePath),
                                        FormatString("move file : %s to %s failed", oldFilePath, newFilePath));
        }
    }
    result = rmdir(realPath.c_str());
    CHECK_FAIL_RETURN_STATUS(result == 0, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Failed to remove directory %s, errno:%d", realPath, errno));
    return Status::OK();
}

Status ResizeFile(const std::string &path, size_t newSize)
{
    CHECK_FAIL_RETURN_STATUS(newSize <= SSIZE_MAX, K_INVALID,
                             FormatString("Invalid size %zu exceed SSISE_MAX", newSize));
    auto ret = truncate(path.c_str(), static_cast<ssize_t>(newSize));
    CHECK_FAIL_RETURN_STATUS(ret == 0, StatusCode::K_IO_ERROR,
                             FormatString("Resize file failed. path=%s, newSize=%zu, ret=%d", path, newSize, ret));
    return Status::OK();
}

bool IsEmptyDir(const std::string &path)
{
    DIR *dir = opendir(path.c_str());
    if (dir == nullptr) {
        return false;
    }
    Raii releaseDir([dir] { closedir(dir); });
    struct dirent *ent;
    uint32_t elementNum = 0;
    const uint32_t minElementNum = 2;  // 2 means element '.' and '..'
    while ((ent = readdir(dir)) != nullptr) {
        ++elementNum;
        if (elementNum > minElementNum) {
            break;
        }
    }
    return (elementNum == minElementNum);
}

Status MoveFileToNewPath(const std::string &filePath, const std::string &newPath)
{
    if (!IsSafePathLength(filePath) || !IsSafePathLength(newPath)) {
        RETURN_STATUS_LOG_ERROR(
            StatusCode::K_INVALID,
            FormatString("Illegal old path length: %zu or new path length: %zu", filePath.size(), newPath.size()));
    }
    char formatPath[PATH_MAX + 1] = { 0 };
    if (realpath(filePath.c_str(), formatPath) == nullptr) {
        RETURN_STATUS(K_INVALID, FormatString("Invalid file path %s.", filePath));
    }
    const std::string realPath = std::string(formatPath);

    char formatNewPath[PATH_MAX + 1] = { 0 };
    if (realpath(newPath.c_str(), formatNewPath) == nullptr) {
        RETURN_STATUS(K_INVALID, FormatString("Invalid new path %s.", newPath));
    }
    const std::string realNewPath = std::string(formatNewPath);

    std::string::size_type pos = realPath.find_last_of('/');
    std::string filename = pos == std::string::npos ? realPath : realPath.substr(pos + 1);
    std::string newFilePath = realNewPath + "/" + filename;
    errno = 0;
    int ret = std::rename(realPath.c_str(), newFilePath.c_str());
    CHECK_FAIL_RETURN_STATUS(ret == EOK, K_RUNTIME_ERROR,
                             FormatString("Move %s to new path %s failed, errno: %d", filePath, newPath, errno));
    return Status::OK();
}

bool IsSafePath(const std::string &path)
{
    Uri uri(path);
    uri.NormalizePath();
    static const std::unordered_set<std::string> unsafeDir = { "/bin",  "/sbin", "/usr", "/lib", "/lib64", "/",
                                                               "/boot", "/dev",  "/etc", "/sys", "/proc" };
    if (unsafeDir.find(uri.Path()) != unsafeDir.end()) {
        LOG(ERROR) << "Unsafe path: " << path;
        return false;
    }
    for (const auto &unsafe : unsafeDir) {
        if (unsafe != "/" && uri.IsSubPathOf(unsafe)) {
            LOG(ERROR) << "Unsafe path: " << path << " in system dir: " << unsafe;
            return false;
        }
    }
    return true;
}
}  // namespace datasystem
