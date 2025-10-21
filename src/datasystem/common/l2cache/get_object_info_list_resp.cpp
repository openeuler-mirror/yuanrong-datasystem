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

#include <initializer_list>
#include <limits.h>

#include <nlohmann/json.hpp>

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"

DS_DECLARE_string(obs_bucket);

namespace datasystem {
const std::string KEY_BUCKET = "bucket";
const std::string KEY_PRODUCT_ID = "productId";
const std::string KEY_IS_TRUNCATED = "isTruncated";
const std::string KEY_NEXT_MARKER = "nextMarker";
const std::string KEY_CONTENTS = "contents";
const std::string KEY_CONTENT_SIZE = "size";
const std::string KEY_CONTENT_KEY = "key";
const std::string KEY_CONTENT_CONTENTTYPE = "contentType";
const std::string KEY_CONTENT_LASTMODIFIED = "lastModified";

namespace {
bool ParseFromJson(const nlohmann::json &objJson, std::string &bucket, std::string &productId, bool &isTruncated,
                   std::string &nextMarker)
{
    if (!objJson.is_object()) {
        LOG(ERROR) << "The response is not a json object.";
        return false;
    }

    if (objJson.contains(KEY_BUCKET) && objJson[KEY_BUCKET].is_string()) {
        bucket = objJson[KEY_BUCKET];
    }

    if (objJson.contains(KEY_PRODUCT_ID) && objJson[KEY_PRODUCT_ID].is_string()) {
        productId = objJson[KEY_PRODUCT_ID];
    }

    if (objJson.contains(KEY_IS_TRUNCATED) && objJson[KEY_IS_TRUNCATED].is_boolean()) {
        isTruncated = objJson[KEY_IS_TRUNCATED];
    }

    if (objJson.contains(KEY_NEXT_MARKER) && objJson[KEY_NEXT_MARKER].is_string()) {
        nextMarker = objJson[KEY_NEXT_MARKER];
    }

    if ((isTruncated && nextMarker.empty()) || (!isTruncated && !nextMarker.empty())) {
        LOG(ERROR) << FormatString("Property isTruncated(%d) and nextMarker(%s) not match", isTruncated, nextMarker);
        // Property mismatch, set to false to indicate no remaining objects
        isTruncated = false;
        nextMarker.clear();
    }

    // if no object match the request's query prefix, then server response body has no contents field.
    return !objJson.contains(KEY_CONTENTS) || objJson[KEY_CONTENTS].is_array();
}

bool IsObjectContainsStringKeys(const nlohmann::json &object, const std::initializer_list<std::string> &keys)
{
    for (const auto &key : keys) {
        if (!object.contains(key) || !object[key].is_string()) {
            return false;
        }
    }
    return object.is_object();
}
}  // namespace

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

class GetObjectInfoListRespWrapper : public GetObjectInfoListResp {
public:
    /**
     * @brief construct from GetObjectInfoListResp
     */
    explicit GetObjectInfoListRespWrapper(GetObjectInfoListResp resp) : GetObjectInfoListResp(std::move(resp))
    {
    }

    /**
     * @brief parse list json to GetObjectInfoListResp object
     * @param[in] objJson response body in json format
     * @param[out] resp get object list parse result
     * @return Status of the call
     */
    friend Status ParseGetObjectInfoListRespJson(const nlohmann::json &objJson, GetObjectInfoListResp &resp);
};

Status ParseGetObjectInfoListRespJson(const nlohmann::json &objJson, GetObjectInfoListResp &resp)
{
    GetObjectInfoListRespWrapper respWrapper(resp);
    if (!ParseFromJson(objJson, respWrapper.bucket_, respWrapper.productId_, respWrapper.isTruncated_,
                       respWrapper.nextMarker_)) {
        return Status(K_RUNTIME_ERROR, "Failed to parse obj list json result.");
    }
    if (!objJson.contains(KEY_CONTENTS)) {
        return Status::OK();
    }

    nlohmann::json::array_t contents = objJson[KEY_CONTENTS];
    auto keys = { KEY_CONTENT_SIZE, KEY_CONTENT_KEY, KEY_CONTENT_CONTENTTYPE, KEY_CONTENT_LASTMODIFIED };
    for (auto item = contents.begin(); item < contents.end(); item++) {
        const auto &content = *item;
        if (!IsObjectContainsStringKeys(content, keys)) {
            LOG(ERROR) << "Parse List response failed, Attributes <" << VectorToString(keys)
                       << "> should both exist and be strings, invalid content:" << content << VectorToString(keys);
            continue;
        }
        std::string size = content[KEY_CONTENT_SIZE];
        std::string key = content[KEY_CONTENT_KEY];

        uint64_t integerSize;
        try {
            integerSize = StrToUnsignedLongLong(size);
        } catch (const std::out_of_range &e) {
            LOG(ERROR) << "The string is out of range : " << size;
            integerSize = LONG_MAX;
        } catch (const std::invalid_argument &e) {
            LOG(ERROR) << "Got invalid size : " << size;
            integerSize = 0;
        }
        L2CacheObjectInfo obj{ .contentType = content[KEY_CONTENT_CONTENTTYPE],
                               .lastModified = content[KEY_CONTENT_LASTMODIFIED],
                               .size = integerSize,
                               .key = key,
                               .version = respWrapper.ParseVersion(key) };
        respWrapper.contents_.emplace_back(obj);
    }

    resp = std::move(respWrapper);
    return Status::OK();
}
}  // namespace datasystem