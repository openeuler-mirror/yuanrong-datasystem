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
 * Description: The Uri management.
 */

#include "datasystem/common/util/uri.h"

#include <cstdlib>
#include <limits>
#include <sstream>
#include <string>

#include <dirent.h>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/sysmacros.h>

#include "re2/re2.h"
#include "datasystem/common/flags/flags.h"
#ifdef WITH_TESTS
#include "datasystem/common/inject/inject_point.h"
#endif
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/status.h"

namespace {
// "//foo" is treated as one element instead of '/' and "foo" at the beginning. (From POSIX.1-2017 4.13)
bool IsBeginWithDoubleSlash(const std::string &str)
{
    const std::string doubleSlash("//");
    const uint32_t doubleSlashLen = doubleSlash.size();
    if (str.size() <= doubleSlashLen) {
        return false;
    }

    return (str.substr(0, doubleSlashLen) == doubleSlash && str[doubleSlashLen] != '/');
}

std::vector<std::string> Split(const std::string &path)
{
    std::vector<std::string> result;
    std::stringstream input(path);
    std::string temp;
    while (getline(input, temp, '/')) {
        if (!temp.empty()) {
            result.push_back(temp);
        }
    }
    return result;
}
}  // namespace
namespace datasystem {
std::mutex Uri::mutex_;

constexpr int MAX_NAME_LENGTH = 255;

Uri::Uri(const std::string &pathStr) : origPath_(pathStr), formatPath_(pathStr)
{
    NormalizePath();
}

Uri::Uri(const Uri &other)
{
    this->origPath_ = other.origPath_;
    this->formatPath_ = other.formatPath_;
}

Uri::Uri(Uri &&other) noexcept
{
    this->origPath_ = std::move(other.origPath_);
    this->formatPath_ = std::move(other.formatPath_);
}

Uri &Uri::operator=(const Uri &other)
{
    this->origPath_ = other.origPath_;
    this->formatPath_ = other.formatPath_;
    return *this;
}

Uri &Uri::operator=(Uri &&other) noexcept
{
    this->origPath_ = std::move(other.origPath_);
    this->formatPath_ = std::move(other.formatPath_);
    return *this;
}

std::string Uri::Path() const
{
    return formatPath_;
}

std::vector<std::string> Uri::GetSeparatePath() const
{
    if (formatPath_.empty()) {
        return {};
    }

    auto results = Split(formatPath_);

    // end with '/' && is not root directory
    if (formatPath_.find_last_not_of('/') != std::string::npos && formatPath_.back() == '/') {
        results.push_back(".");
    }
    // form as "//foo" should be treated as a group: "//" only
    if (formatPath_ == "//") {
        return { "//" };
    }
    // form as "//foo" should be treated as a group: begin with "//"
    if (IsBeginWithDoubleSlash(formatPath_)) {
        results[0].insert(0, "//");
        return results;
    }
    // form as "/foo" should supply '/' at beginning
    if (formatPath_[0] == '/') {
        results.insert(results.begin(), "/");
    }

    return results;
}

std::string Uri::GetFileName() const
{
    auto pos = formatPath_.find_last_of('/');
    if (pos == std::string::npos) {
        return formatPath_;
    }

    // special handling for "//foo" and "//"
    if (pos == 1 && formatPath_[0] == '/') {
        return formatPath_;
    }

    if (pos == formatPath_.size() - 1) {                               // end with '/'
        if (formatPath_.find_last_not_of('/') == std::string::npos) {  // only have '/'
            return "/";
        } else {
            return ".";
        }
    }

    // must have '/', so won't be out-of-bound
    return formatPath_.substr(pos + 1);
}

bool Uri::IsRoot() const
{
    return formatPath_ == separator_;
}

void Uri::NormalizePath()
{
    // explain continuous '/' as '/'
    ReplaceRecursively(formatPath_, "//", separator_);
    // special handling of "//" at beginning
    if (IsBeginWithDoubleSlash(origPath_)) {
        formatPath_.insert(0, 1, '/');  // supply one '/' character at index 0
    }

    // explain "/./" as current '/'
    ReplaceRecursively(formatPath_, "/./", separator_);

    // explain "/../" as parent path
    if (formatPath_.size() > strlen("/..") && endsWith(formatPath_, "/..")) {
        formatPath_.push_back('/');
    }
    re2::RE2 reg("/[^/]+/\\.\\./");
    while (RE2::GlobalReplace(&formatPath_, reg, "/") > 0) {
    };

    const std::string ends = "/.";
    // If the path ends with "/."
    if (endsWith(formatPath_, ends)) {
        formatPath_.pop_back();
    }
    // Not root dir, erase '/'.
    if (formatPath_ != separator_ && endsWith(formatPath_, separator_)) {
        formatPath_.pop_back();
    }
}

std::string Uri::GetParent() const
{
    if (formatPath_.find_first_not_of('/') == std::string::npos) {
        return "";
    }
    auto pos = formatPath_.find_last_of('/');
    if (pos == std::string::npos) {
        return "";
    }
    if (pos == 0) {
        return "/";
    }

    auto result = formatPath_.substr(0, pos);
    // trim end /
    while (!result.empty() && result.back() == '/') {
        result.pop_back();
    }
    return result;
}

Status Uri::ValidateFile() const
{
    if (endsWith(origPath_, "/") || endsWith(origPath_, ".") || endsWith(origPath_, "..")) {
        RETURN_STATUS(StatusCode::K_INVALID, "path: \"" + origPath_ + "\" is a directory");
    }
    // We've excluded strings ending with "/", "." and "..",
    // so we can safely use ValidateDir for detection.
    return Validate();
}

bool Uri::IsWindowsName(const std::string &name) const
{
    char windowsInvalidChars[] =
        "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F"
        "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F"
        "<>:\"/\\|";

    return !name.empty() && name[0] != ' '
           && (name.find_first_of(windowsInvalidChars, 0, sizeof(windowsInvalidChars)) == std::string::npos)
           && *name.rbegin() != ' ' && (*name.rbegin() != '.' || name.size() == 1 || name == "..");
}

Status Uri::Validate() const
{
    auto sepPaths = GetSeparatePath();
    if (sepPaths.empty()) {
        RETURN_STATUS(StatusCode::K_INVALID, "empty path");
    }
    if (sepPaths[0] != separator_) {
        RETURN_STATUS(StatusCode::K_INVALID, "path: \"" + origPath_ + "\" not start with /");
    }
    for (auto iter = sepPaths.begin() + 1; iter != sepPaths.end(); iter++) {
        // The allowed characters are anything except 0x0-0x1F, '<', '>', ':', '"', '/', '\', and '|'.
        if (!IsWindowsName(*iter)) {
            RETURN_STATUS(StatusCode::K_INVALID, "path: \"" + origPath_ + "\" contains invalid characters");
        } else if ((*iter).size() > MAX_NAME_LENGTH) {
            RETURN_STATUS(StatusCode::K_FILE_NAME_TOO_LONG, "path: \"" + origPath_ + "\" too long");
        }
    }
    return Status::OK();
}

bool Uri::IsSubPathOf(const std::string &parent) const
{
    Uri parentUri(parent);

    // Ensure parent is not a prefix of child (e.g., /home should not be a sub-path of /home2)
    if (formatPath_.size() < parentUri.formatPath_.size()) {
        return false;
    }

    // Compare prefixes
    if (formatPath_.compare(0, parentUri.formatPath_.size(), parentUri.formatPath_) != 0) {
        return false;
    }

    // Ensure the next character is '/' or we've reached the end
    return formatPath_.size() == parentUri.formatPath_.size() || formatPath_[parentUri.formatPath_.size()] == '/'
           || parentUri.formatPath_ == "/";
}

Status Uri::GetHomeDir(std::string &userPath)
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::string homePath = GetStringFromEnv("HOME", "");
    if (!homePath.empty()) {
        return Realpath(homePath, userPath);
    }

    int retryCount = 0;
    const int maxRetries = 3;

    uid_t uid = getuid();
    struct passwd pwdBuf;

    long temp = sysconf(_SC_GETPW_R_SIZE_MAX);
    size_t bufSize = static_cast<size_t>(temp);
    if (temp == -1L) {
        const size_t initialBufferSize = 2048;
        bufSize = initialBufferSize;
    }

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(bufSize);
    struct passwd *result;

    const size_t bufferGrowthFactor = 2;

    int ret = 0;
    while (retryCount < maxRetries) {
        // Try to get home directory using UID
        int ret = getpwuid_r(uid, &pwdBuf, buf.get(), bufSize, &result);
        if (ret == 0 && result != nullptr) {
            break;
        } else {
            retryCount++;
            if (ret == ERANGE) {
                buf.reset();
                bufSize *= bufferGrowthFactor;  // Simple doubling of buffer size
                buf = std::make_unique<char[]>(bufSize);
            }
        }
    }

    if (ret != 0) {
        LOG(ERROR) << "getpwuid_r failed with error: " << errno << " (" << StrErr(errno) << ")";
    }

    if (result != nullptr && result->pw_dir != nullptr) {
        homePath = std::string(result->pw_dir);
    } else {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to get current user's UID.");
    }

    return Realpath(homePath, userPath);
}

Status Uri::Realpath(const std::string &path, std::string &resolvedPath)
{
    if (path.size() >= PATH_MAX) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_INVALID, FormatString("Illegal path length: %zu", path.size()));
    }
    char resultPath[PATH_MAX + 1] = { 0 };
    if (realpath(path.c_str(), resultPath) == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Unable to resolve %s. Errno = %s", path, StrErr(errno)));
    }
    resolvedPath = std::string(resultPath);
    return Status::OK();
}

Status Uri::GetRealfile(const std::string &rawFilePath, std::string &realPath)
{
    size_t pos = rawFilePath.find_last_of('/');
    CHECK_FAIL_RETURN_STATUS(pos != rawFilePath.size() - 1, K_RUNTIME_ERROR,
                             FormatString("The file path %s do not contain a filename", rawFilePath));
    if (pos == std::string::npos) {
        realPath = rawFilePath;
    } else {
        std::string dirName = rawFilePath.substr(0, pos);
        std::string fileName = rawFilePath.substr(pos + 1);
        RETURN_IF_NOT_OK(Realpath(dirName, realPath));
        realPath += "/" + fileName;
    }
    return Status::OK();
}

// When the default value is not empty:
// 1.If the received value here is empty, it means that the user has actively changed it to empty,
// and an error should be reported in this case.
// 2. When the received value is the default value, each process adds its own path based on the default value.
// For example, the log_dir's default value is "~/.datasystem/logs",
// and the master process will turn it into homeDir + "/.datasystem/logs/master.
// 3. When the received value is not the default value, replace '~' with user's homeDir.
Status Uri::NormalizePathWithUserHomeDir(std::string &dir, const std::string &defaultDir,
                                         const std::string &additionalPath)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!dir.empty(), K_INVALID, "Empty input dir!");

    if (dir.at(0) != '~') {
        return Status::OK();
    }

    // Get user's home directory.
    std::string userPath;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Uri::GetHomeDir(userPath), "Unable to get user directory.");
    if (dir == defaultDir) {
        dir = dir.replace(0, 1, userPath).append(additionalPath);
    } else {
        dir = dir.replace(0, 1, userPath);
    }
    return Status::OK();
}

Status Uri::DeleteAppendPath(std::string &dir)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!dir.empty(), K_INVALID, "The input dir cannot be empty!");
    auto index = dir.size() - 1;
    if (dir.at(index) == '/') {
        dir.pop_back();
    }
    auto pos = dir.find_last_of('/');
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pos != std::string::npos, K_INVALID, "Invalid input dir: " + dir);
    dir = dir.substr(0, pos + 1);
    return Status::OK();
}

Status Uri::ModifyFilesInInputDir(const std::string &inputDir, mode_t permission)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!inputDir.empty(), K_RUNTIME_ERROR, "The dir_name is null!");
    // Check if inputDir is a valid dir.
    struct stat s;
    if (lstat(inputDir.c_str(), &s) != 0) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, "lstat failed, errno: " + std::to_string(errno));
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(S_ISDIR(s.st_mode), K_RUNTIME_ERROR,
                                         FormatString("The dir_name %s is not a valid directory!", inputDir));

    struct dirent *filename;
    DIR *dir = opendir(inputDir.c_str());
    RETURN_RUNTIME_ERROR_IF_NULL(dir);
    LOG(INFO) << FormatString("Successfully open dir: %s .", inputDir);

    // Read all the files in the dir.
    while ((filename = readdir(dir)) != NULL) {
        // To get rid of "." and ".."
        if (strcmp(filename->d_name, ".") == 0 || strcmp(filename->d_name, "..") == 0) {
            continue;
        }
#ifdef WITH_TESTS
        INJECT_POINT("util.beforeChmod");
#endif
        std::string fileName = filename->d_name;
        const int rc = chmod((inputDir + "/" + fileName).c_str(), permission);
        // Close dir before return.
        if (rc != 0) {
            std::string errMsg =
                FormatString("Failed to modify permission of %s with errno %d!", inputDir + "/" + fileName, errno);
            if (closedir(dir) != 0) {
                errMsg += " And close file error with: " + std::to_string(errno);
            }
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, errMsg);
        }
    }
    auto closeResult = closedir(dir);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(closeResult == 0, K_RUNTIME_ERROR, "Failed to close the file!");
    return Status::OK();
}

template <typename NumType1, typename NumType2>
bool Uri::StrToInteger(const char *str, std::function<NumType2(const char *, char **, int)> fun, NumType1 type1Max,
                       NumType1 type1Min, NumType2 type2Max, NumType2 type2Min, NumType1 &res)
{
    res = 0;
    if (str == nullptr) {
        LOG(ERROR) << "The input parameter should not be a null pointer";
        return false;
    }
    if (*str == ' ') {
        LOG(ERROR) << "Not converted or not converted complete. str: " << str;
        return false;
    }
    char *end;
    errno = 0;
    NumType2 num = fun(str, &end, 10);
    if (end == str || *end != '\0') {
        LOG(ERROR) << "Not converted or not converted complete. str: " << str;
        return false;
    }

    if (type2Max > type1Max && type2Min < type1Min) {
        if (num > type1Max || num < type1Min) {
            errno = ERANGE;
        }
    }

    if (errno == ERANGE) {
        LOG(ERROR) << "When convert string to integer, range error has occurred. str: " << str;
        return false;
    }
    res = num;
    return true;
}

bool Uri::StrToInt32(const char *str, int32_t &res)
{
    return StrToInteger<int32_t, long>(str, strtol, INT32_MAX, INT32_MIN, LONG_MAX, LONG_MIN, res);
}

bool Uri::StrToInt(const char *str, int &res)
{
    return StrToInteger<int, long>(str, strtol, INT_MAX, INT_MIN, LONG_MAX, LONG_MIN, res);
}

bool Uri::StrToLong(const char *str, long &res)
{
    return StrToInteger<long, long>(str, strtol, LONG_MAX, LONG_MIN, LONG_MAX, LONG_MIN, res);
}

bool Uri::StrToUint64(const char *str, uint64_t &res)
{
    return StrToInteger<uint64_t, uint64_t>(str, strtoull, UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX, res);
}
}  // namespace datasystem
