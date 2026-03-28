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

#ifndef DATASYSTEM_COMMON_L2CACHE_SFS_CLIENT_SFS_CLIENT_H
#define DATASYSTEM_COMMON_L2CACHE_SFS_CLIENT_SFS_CLIENT_H

#include <sys/stat.h>
#include <sys/types.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/l2cache/get_object_info_list_resp.h"
#include "datasystem/common/l2cache/l2cache_client.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/status.h"

#include <shared_mutex>

namespace datasystem {
class SfsClient : public L2CacheClient {
public:
    /**
     * @brief SfsClient.
     * @param[in] path the path to the mounted SFS. The working directory will be "pathToSfs/datasystem"
     * @param[in] writeChunkSize how many bytes to write to SFS in a chunk
     * @param[in] readChunkSize how many bytes to read from SFS in a chunk
     * @return Status of the call
     */
    SfsClient(const std::string &path)
        : sfsPath_(path), dsPath_(path.back() == '/' ? path + "datasystem" : path + "/datasystem")
    {
    }

    SfsClient() = delete;
    SfsClient(const SfsClient&) = delete;
    SfsClient &operator=(const SfsClient&) = delete;

    /**
     * @brief Initialize SfsClient. Check the availability of SFS.
     * @return Status of the call
     */
    Status Init() override;

    /**
     * @brief Upload an object to SFS.
     * @param[in] objectPath objectKey/versionNum
     * @param[in] timeoutMs timeout
     * @param[in] body payload to upload
     * @param[in] asyncElapse The time this object being in the async queue
     * @return Status of the call
     */
    Status Upload(const std::string &objectPath, int64_t timeoutMs, const std::shared_ptr<std::iostream> &body,
                  uint64_t asyncElapse = 0) override;

    /**
     * @brief List all the objects matching the given prefix.
     * @param[in] objectPrefix prefix of object key/name. If you want to list only one specific object, this parameter
     * should be "objectKey/"
     * @param[in] timeoutMs timeout
     * @param[in] listIncompleteVersions whether to list those incomplete versions. Usually they are partially uploaded.
     * @param[out] listResp holder of the meta info of the objects listed in a batch. For SFS, one batch contains all
     * the objects matching the prefix. Some variables defined in GetObjectInfoListResp, (including marker and
     * isTruncated, working as a bookmark between two batches), is not used here.
     * @return Status of the call
     */
    Status List(const std::string &objectPrefix, int64_t timeoutMs, bool listIncompleteVersions,
                std::shared_ptr<GetObjectInfoListResp> &listResp) override;

    /**
     * @brief Download objects from SFS
     * @param[in] objectPath objectKey/versionNum
     * @param[in] timeoutMs timeout
     * @param[out] content a stringstream to hold the object content
     * @return Status of the call
     */
    Status Download(const std::string &objectPath, int64_t timeoutMs,
                    std::shared_ptr<std::stringstream> &content) override;

    /**
     * @brief Delete object on SFS
     * @param[in] objects objects to delete
     * @param[in] asyncElapse The time this object being in the async queue
     * @return Status of the call
     */
    Status Delete(const std::vector<std::string> &objects, uint64_t asyncElapse = 0) override;

    /**
     * @brief Obtains the request success rate of l2cache.
     * @return Success rate of l2cache request.
     */
    std::string GetRequestSuccessRate() override;

private:
    struct SfsObjList {
        struct SfsObject {
            std::string key;
            timespec lastModified;
            size_t size;
        };

        std::vector<SfsObject> objects;
    };

    Status IsSfsUsable();
    Status DeleteOne(const std::string &objectPath);
    Status GenerateFullPath(const std::string &objectPath, std::string &outPath);
    Status LoopUpload(const std::shared_ptr<std::iostream> &body, const size_t chunkSize, std::ofstream &file,
                      Timer &timer);
    std::unique_ptr<char[]> GetAChunk(const std::shared_ptr<std::iostream> &body, const size_t chunkSize,
                                      size_t &bufSize);
    Status ListAllObjects(const std::vector<std::string> &objsToList, SfsObjList &objList, Timer &timer,
                          bool listIncompleteVersions);
    Status ListOneObject(const std::string &objectDir, SfsObjList &objList, bool listIncompleteVersions);

    inline Status IfPathExists(const std::string &path)
    {
        struct stat info;
        CHECK_FAIL_RETURN_STATUS(
            stat(path.c_str(), &info) == 0, StatusCode::K_NOT_FOUND, "Path does not exist");
        return Status::OK();
    }

    inline Status NewDirIfNotExists(const std::string &dirPath)
    {
        struct stat info;
        if (stat(dirPath.c_str(), &info) != 0) {
            if (mkdir(dirPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH) != 0) {
                RETURN_STATUS(K_RUNTIME_ERROR, "Failed to create the given directory.");
            }
        }
        return Status::OK();
    }

    Status ValidateObjNameWithVersion(const std::string &name);
    void FillInListObjectData(const SfsObjList &objList, std::shared_ptr<GetObjectInfoListResp> &listResp);

    const std::string sfsPath_;
    const std::string dsPath_;
    const size_t writeChunkSize_{ 1024 };
    const size_t readChunkSize_{ 1024 };
};
}
#endif