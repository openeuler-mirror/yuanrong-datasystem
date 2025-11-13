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
 * Description: the response of get object list
 */
#ifndef DATASYSTEM_COMMON_GET_OBJECT_LIST_RESP_H
#define DATASYSTEM_COMMON_GET_OBJECT_LIST_RESP_H
#include <string>
#include <vector>

#include "datasystem/utils/status.h"
#include "datasystem/common/l2cache/obs_client/obs_client.h"
#include "datasystem/common/l2cache/l2cache_object_info.h"

namespace datasystem {
class L2CacheClient;
class GetObjectInfoListResp {
public:
    /**
     * @brief build GetObjectInfoListResp from string
     */
    GetObjectInfoListResp() = default;

    /**
     * @brief destroy object
     */
    virtual ~GetObjectInfoListResp() = default;

    void FillInListObjectData(const ObsClient::ListObjectData &listObjData);

    /**
     * @brief set the isTruncated_ flag
     * @param[in] truncatedFlag true or false
     */
    void SetTruncated(bool truncatedFlag)
    {
        isTruncated_ = truncatedFlag;
    }

    /**
     * @brief a flag that indicates whether l2cache return all of the result that satisfied the search criteria
     * @return false if return all of the result, otherwise true
     */
    bool IsTruncated() const;

    /**
     * @brief set the object list info
     * @param[in] contents object list info
     */
    void SetObjectInfo(std::vector<L2CacheObjectInfo> contents)
    {
        contents_ = std::move(contents);
    }

    /**
     * @brief the object info list
     * @return object info list
     */
    const std::vector<L2CacheObjectInfo> &GetObjectInfo() const;

    /**
     * @brief set the max version
     * @param maxVersion the max version object in the list
     */
    void SetMaxVersion(uint64_t maxVersion)
    {
        maxVersion_ = maxVersion;
    }

    /**
     * @brief get the max version in the object info list
     * @return max version
     */
    uint64_t MaxVersion() const
    {
        return maxVersion_;
    }

    /**
     * @brief marker for continue query more satisfied the search criteria object
     * @return nextMarker
     */
    std::string NextMarker() const
    {
        return nextMarker_;
    }

    void SetBucket(const std::string &bucket)
    {
        bucket_ = bucket;
    }

    void AppendL2CacheObjectInfo(const std::string &contentType, const std::string &lastModified, uint64_t size,
                                 const std::string &key, uint64_t version)
    {
        contents_.emplace_back(L2CacheObjectInfo{ contentType, lastModified, size, key, version });
    }

    void SetNextMarker(const std::string &nextMarker)
    {
        nextMarker_ = nextMarker;
    }

    /**
     * @brief the object key contain version info, we parse it for downstream business use.
     * version return 0 has ambiguity, but this ambiguity has no side effect on downstream business:
     * 1) the version is not number, failed to parse, return 0
     * 2) the version is actually 0
     * @param[in] key the object key
     * @return object version if found, otherwise 0
     */
    uint64_t ParseVersion(const std::string &key);

protected:
    std::vector<L2CacheObjectInfo> contents_;
    std::string bucket_;
    std::string productId_;  // This guy is empty when use OBS.
    bool isTruncated_{ false };
    // if has more satisfied the search criteria, the server will return the continue query marker
    std::string nextMarker_;
    // the max version in the response list
    uint64_t maxVersion_ {0};
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_GET_OBJECT_LIST_RESP_H