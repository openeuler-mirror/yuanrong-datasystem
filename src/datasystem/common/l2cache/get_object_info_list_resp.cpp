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
#include "datasystem/common/l2cache/get_object_info_list_resp.h"

#include <limits.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"

DS_DECLARE_string(obs_bucket);

namespace datasystem {
void GetObjectInfoListResp::FillInListObjectData(const ObsClient::ListObjectData &listObjData)
{
    bucket_.clear();
    nextMarker_.clear();

    bucket_ = FLAGS_obs_bucket;
    nextMarker_ = listObjData.nextMarker;
    isTruncated_ = listObjData.isTruncated;
    for (auto &obj : listObjData.objects) {
        struct tm buf;
        time_t tt = (time_t)obj.lastModified;
        static const size_t timeStrLen = 256;
        char lastModifiedStr[timeStrLen] = {0};
        (void)strftime(lastModifiedStr, sizeof(lastModifiedStr), "%Y-%m-%dT%H:%M:%SZ", gmtime_r(&tt, &buf));
        contents_.emplace_back(
            L2CacheObjectInfo{ obj.type, lastModifiedStr, obj.size, obj.key, ParseVersion(obj.key) });
    }
}
uint64_t GetObjectInfoListResp::ParseVersion(const std::string &key)
{
    uint64_t version = 0;
    auto pos = key.rfind('/');
    if (pos != std::string::npos) {
        std::string versionStr = key.substr(pos + 1);
        try {
            version = StrToUnsignedLongLong(versionStr);
        } catch (std::invalid_argument &e) {
            LOG(ERROR) << "Fail to parse version from key, key id: " << key;
        } catch (const std::out_of_range &e) {
            LOG(ERROR) << "The string is out of range : " << version;
            version = LONG_MAX;
        }
    }

    if (version > maxVersion_) {
        maxVersion_ = version;
    }
    return version;
}

bool GetObjectInfoListResp::IsTruncated() const
{
    return isTruncated_;
}

const std::vector<L2CacheObjectInfo> &GetObjectInfoListResp::GetObjectInfo() const
{
    return contents_;
}
}  // namespace datasystem