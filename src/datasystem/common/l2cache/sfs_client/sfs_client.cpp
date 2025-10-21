/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Interface to SFS turbo service. Please do NOT operate on the same file concurrently. Please clear
 * leftovers if upload failed.
 */

#include "datasystem/common/l2cache/sfs_client/sfs_client.h"

#include <dirent.h>
#include <climits>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/status.h"

DS_DEFINE_string(sfs_path, "", "The path to the mounted SFS.");
DS_DEFINE_validator(sfs_path, &Validator::ValidatePathString);

namespace datasystem {
Status SfsClient::IsSfsUsable()
{
    struct stat info;
    CHECK_FAIL_RETURN_STATUS(stat(sfsPath_.c_str(), &info) == 0, StatusCode::K_RUNTIME_ERROR,
                             "SFS path does not exist");
    CHECK_FAIL_RETURN_STATUS((info.st_mode & S_IFDIR) != 0, StatusCode::K_RUNTIME_ERROR, "SFS path is not a directory");
    CHECK_FAIL_RETURN_STATUS(access(sfsPath_.c_str(), R_OK) == 0, StatusCode::K_RUNTIME_ERROR,
                             "SFS path is not readable");
    CHECK_FAIL_RETURN_STATUS(access(sfsPath_.c_str(), W_OK) == 0, StatusCode::K_RUNTIME_ERROR,
                             "SFS path is not writable");
    return Status::OK();
}

Status SfsClient::Init()
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsNotEmpty(FLAGS_sfs_path), StatusCode::K_INVALID,
                             "sfs_path gflag can't be empty.");
    RETURN_IF_NOT_OK(IsSfsUsable());
    RETURN_IF_NOT_OK(NewDirIfNotExists(dsPath_));
    LOG(INFO) << FormatString("Successfully initialized SfsClient.");
    return Status::OK();
}

Status SfsClient::GenerateFullPath(const std::string &objectPath, std::string &outPath)
{
    std::string fullPath = dsPath_;
    if (!objectPath.empty()) {
        objectPath.front() == '/' ? fullPath.append(objectPath) : fullPath.append("/" + objectPath);
    }
    Uri uri(fullPath);
    uri.NormalizePath();
    CHECK_FAIL_RETURN_STATUS(uri.IsSubPathOf(dsPath_), StatusCode::K_RUNTIME_ERROR,
                             FormatString("%s is not in %s", uri.Path(), dsPath_));
    outPath = uri.Path();
    return Status::OK();
}

Status SfsClient::Upload(const std::string &objectPath, int64_t timeoutMs, const std::shared_ptr<std::iostream> &body,
                         uint64_t asyncElapse)
{
    RETURN_IF_NOT_OK(ValidateObjNameWithVersion(objectPath));
    Timer timer(timeoutMs);
    (void)asyncElapse;
    LOG(INFO) << FormatString("Writing object to SFS. Object name with version: %s timeoutMs: %d", objectPath,
                              timeoutMs);

    // parse obj path
    auto pos = objectPath.find_last_of('/');
    CHECK_FAIL_RETURN_STATUS(pos != std::string::npos, K_RUNTIME_ERROR,
                             "Cannot parse object name with version: " + objectPath);
    std::string objName = objectPath.substr(0, pos);
    // check if the directory for this object exists
    std::string objDir;
    RETURN_IF_NOT_OK(GenerateFullPath(objName, objDir));
    RETURN_IF_NOT_OK(NewDirIfNotExists(objDir));

    std::string fullPath;
    RETURN_IF_NOT_OK(GenerateFullPath(objectPath, fullPath));
    if (fullPath.empty()) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Cannot generate the full path for object: %s", objectPath));
    }
    std::string tmpPath = fullPath + "_";
    std::ofstream file;
    file.open(tmpPath, std::ios::out | std::ios::trunc);
    if (file.is_open()) {
        Status re = LoopUpload(body, writeChunkSize_, file, timer);
        file.close();
        if (re.IsOk()) {
            if (std::rename(tmpPath.c_str(), fullPath.c_str()) != 0) {
                RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Rename failed. Upload to SFS failed. errno: %d", errno));
            }
        }
        return re;
    }
    RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Failed to open object file with errno: %s", strerror(errno)));
}

Status SfsClient::LoopUpload(const std::shared_ptr<std::iostream> &body, const size_t chunkSize, std::ofstream &file,
                             Timer &timer)
{
    // upload chunks one by one
    size_t bufSize = 0;
    std::unique_ptr<char[]> buf = GetAChunk(body, chunkSize, bufSize);
    while (buf != nullptr) {
        if (timer.GetRemainingTimeMs() == 0) {
            RETURN_STATUS(K_RUNTIME_ERROR,
                          "Timed out during uploading object to SFS. Please clear leftovers. Upload to SFS failed.");
        }
        CHECK_FAIL_RETURN_STATUS(bufSize <= SSIZE_MAX, K_INVALID,
                                 FormatString("bufSize %zu exceed SSIZE_MAX", bufSize));
        file.write(buf.get(), static_cast<ssize_t>(bufSize));
        buf = GetAChunk(body, chunkSize, bufSize);
    }
    return Status::OK();
}

std::unique_ptr<char[]> SfsClient::GetAChunk(const std::shared_ptr<std::iostream> &body, const size_t chunkSize,
                                             size_t &bufSize)
{
    auto currentPos = body->tellg();
    body->seekg(0, std::ios::end);
    auto endPos = body->tellg();
    body->seekg(currentPos);
    size_t remaining = static_cast<size_t>(endPos - currentPos);
    size_t toRead = std::min(chunkSize, remaining);
    if (toRead <= 0 || toRead > SSIZE_MAX) {
        bufSize = 0;
        return nullptr;
    }
    auto buf = std::make_unique<char[]>(toRead);
    body->read(buf.get(), static_cast<ssize_t>(toRead));
    bufSize = toRead;
    // copy elision
    return buf;
}

Status SfsClient::List(const std::string &objectPrefix, int64_t timeoutMs, bool listIncompleteVersions,
                       std::shared_ptr<GetObjectInfoListResp> &listResp)
{
    Timer timer(timeoutMs);
    // list all objects
    std::vector<std::string> objsToList;
    RETURN_IF_NOT_OK(IfPathExists(dsPath_));
    DIR *dir = opendir(dsPath_.c_str());
    LOG(INFO) << "Listing objects with prefix: " << objectPrefix << " timeoutMs: " << timeoutMs;
    if (dir == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Cannot open the path in SFS for object persistence.");
    }
    Raii releaseDir([dir]() {
        if (dir) {
            closedir(dir);
        }
    });
    struct dirent *ent;
    while ((ent = readdir(dir)) != nullptr) {
        if (timer.GetRemainingTimeMs() == 0) {
            RETURN_STATUS(K_RUNTIME_ERROR, "Timed out during listing objects.");
        }
        if (strcmp(ent->d_name, "..") == 0 || strcmp(ent->d_name, ".") == 0) {
            continue;
        }
        std::string nameStr = ent->d_name;
        if (objectPrefix.back() != '/') {
            auto pos = nameStr.find(objectPrefix);
            if (pos != std::string::npos && pos == 0) {
                objsToList.push_back(nameStr);
            }
        } else {
            std::string objName = objectPrefix.substr(0, objectPrefix.size() - 1);
            if (objName == nameStr) {
                objsToList.push_back(nameStr);
            }
        }
    }
    SfsObjList objList;
    RETURN_IF_NOT_OK(ListAllObjects(objsToList, objList, timer, listIncompleteVersions));
    FillInListObjectData(objList, listResp);
    return Status::OK();
}

Status SfsClient::ListAllObjects(const std::vector<std::string> &objsToList, SfsObjList &objList, Timer &timer,
                                 bool listIncompleteVersions)
{
    for (auto &obj : objsToList) {
        if (timer.GetRemainingTimeMs() == 0) {
            RETURN_STATUS(K_RUNTIME_ERROR, "Timed out during listing objects.");
        }
        RETURN_IF_NOT_OK(ListOneObject(obj, objList, listIncompleteVersions));
    }
    return Status::OK();
}

Status SfsClient::ListOneObject(const std::string &object, SfsObjList &objList, bool listIncompleteVersions)
{
    LOG(INFO) << "Listing object: " << object;
    std::string objectDir = dsPath_ + "/" + object;
    RETURN_IF_NOT_OK(IfPathExists(objectDir));
    DIR *dir = opendir(objectDir.c_str());
    if (dir == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Cannot open the path for given object");
    }
    Raii releaseDir([dir]() {
        if (dir) {
            closedir(dir);
        }
    });
    struct dirent *ent;
    while ((ent = readdir(dir)) != nullptr) {
        if (ent->d_type != DT_REG) {
            continue;
        }
        std::string name = ent->d_name;
        if (!listIncompleteVersions && name.size() > 1 && name.back() == '_') {
            continue;
        }
        std::string versionPath = objectDir + "/" + name;
        struct stat info;
        if (stat(versionPath.c_str(), &info) == 0) {
            auto &obj = objList.objects.emplace_back();
            obj.size = static_cast<size_t>(info.st_size);
            obj.key = object + "/" + name;
            obj.lastModified = info.st_mtim;
        }
    }
    return Status::OK();
}

Status SfsClient::Download(const std::string &objectPath, int64_t timeoutMs,
                           std::shared_ptr<std::stringstream> &content)
{
    RETURN_IF_NOT_OK(ValidateObjNameWithVersion(objectPath));
    Timer timer(timeoutMs);
    std::string fullPath;
    RETURN_IF_NOT_OK(GenerateFullPath(objectPath, fullPath));
    if (fullPath.empty()) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Cannot generate the full path for object: %s", objectPath));
    }
    RETURN_IF_NOT_OK(IfPathExists(fullPath));
    LOG(INFO) << "Downloading object " << objectPath << " timeoutMs: " << timeoutMs;

    CHECK_FAIL_RETURN_STATUS(readChunkSize_ <= SSIZE_MAX, K_INVALID,
                             FormatString("readChunkSize %zu exceed SSIZE_MAX", readChunkSize_));
    std::ifstream ifs(fullPath);
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(readChunkSize_);
    while (!ifs.eof()) {
        if (timer.GetRemainingTimeMs() == 0) {
            ifs.close();
            RETURN_STATUS(K_RUNTIME_ERROR, "Timed out during downloading object from SFS. Read from SFS failed.");
        }
        ifs.read(buf.get(), static_cast<ssize_t>(readChunkSize_));
        auto readSize = ifs.gcount();
        content->write(buf.get(), readSize);
    }
    ifs.close();
    return Status::OK();
}

Status SfsClient::Delete(const std::vector<std::string> &objects, uint64_t asyncElapse)
{
    (void)asyncElapse;
    const std::string delimiter = ", ";
    std::stringstream failed;
    for (const auto &object : objects) {
        Status re = DeleteOne(object);
        if (re.IsError()) {
            failed << object << delimiter;
        }
    }
    const std::string &str = failed.str();
    if (str.empty()) {
        return Status::OK();
    }
    RETURN_STATUS(K_RUNTIME_ERROR, FormatString("The following objects were not successfully removed: %s",
                                                str.substr(0, str.size() - delimiter.size())));
}

Status SfsClient::DeleteOne(const std::string &objectPath)
{
    LOG(INFO) << "Deleting object " << objectPath << " in SFS";
    RETURN_IF_NOT_OK(ValidateObjNameWithVersion(objectPath));
    std::string fullPath;
    RETURN_IF_NOT_OK(GenerateFullPath(objectPath, fullPath));
    if (fullPath.empty()) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Cannot generate the full path for the given object."));
    }

    struct stat buffer;
    std::string tmpPath = fullPath + "_";
    // fullPath and tempPath cannot exist at the same time
    if (stat(fullPath.c_str(), &buffer) == 0) {
        if (std::remove(fullPath.c_str()) != 0) {
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Failed to remove object"));
        }
    } else if (stat(tmpPath.c_str(), &buffer) == 0) {
        if (std::remove(tmpPath.c_str()) != 0) {
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Failed to remove tmp object"));
        }
    }
    // if the directory where the file is is empty, delete it
    auto pos = fullPath.find_last_of("/");
    if (pos != std::string::npos) {
        std::string dirStr = fullPath.substr(0, pos);
        RETURN_IF_NOT_OK(IfPathExists(dirStr));
        DIR *dir = opendir(dirStr.c_str());
        if (dir == nullptr) {
            RETURN_STATUS(K_RUNTIME_ERROR, "Failed to open directory");
        }
        Raii releaseDir([dir]() {
            if (dir) {
                closedir(dir);
            }
        });
        struct dirent *ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, "..") == 0 || strcmp(ent->d_name, ".") == 0) {
                continue;
            }
            // not empty, do not delete this dir
            return Status::OK();
        }
        rmdir(dirStr.c_str());
    }
    return Status::OK();
}

Status SfsClient::ValidateObjNameWithVersion(const std::string &name)
{
    auto pos = name.find_last_of('/');
    std::stringstream err;
    err << "Illegal object name: " << name;
    CHECK_FAIL_RETURN_STATUS(pos != std::string::npos, K_RUNTIME_ERROR, err.str());
    CHECK_FAIL_RETURN_STATUS(pos != 0, K_RUNTIME_ERROR, err.str());
    CHECK_FAIL_RETURN_STATUS(pos != name.size() - 1, K_RUNTIME_ERROR, err.str());
    std::string verStr = name.substr(pos + 1);
    try {
        StrToUnsignedLongLong(verStr);
    } catch (const std::exception &e) {
        RETURN_STATUS(K_RUNTIME_ERROR, err.str());
    }
    return Status::OK();
}

void SfsClient::FillInListObjectData(const SfsObjList &objList, std::shared_ptr<GetObjectInfoListResp> &listResp)
{
    // we don't need to set nextMarker and isTruncated
    listResp->SetBucket("");
    listResp->SetTruncated(false);
    listResp->SetNextMarker("");
    for (auto &obj : objList.objects) {
        struct tm buf;
        time_t tt = (time_t)obj.lastModified.tv_sec;
        static const size_t timeStrLen = 256;
        char lastModifiedStr[timeStrLen] = { 0 };
        (void)strftime(lastModifiedStr, sizeof(lastModifiedStr), "%Y-%m-%dT%H:%M:%SZ", gmtime_r(&tt, &buf));
        listResp->AppendL2CacheObjectInfo(
            "", lastModifiedStr, obj.size, obj.key,
            listResp->ParseVersion(obj.key.back() == '_' ? obj.key.substr(0, obj.key.size() - 1) : obj.key));
    }
}

std::string SfsClient::GetRequestSuccessRate()
{
    return "";
}
}  // namespace datasystem
