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
 * Description: persistence api for the cloud persistence service
 */
#include "datasystem/common/l2cache/persistence_api.h"

#include <curl/curl.h>

#include "datasystem/common/l2cache/sfs_client/sfs_client.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(l2_cache_type);
#ifndef DISABLE_OBS
DS_DECLARE_string(obs_endpoint);
DS_DECLARE_string(obs_bucket);
#endif
DS_DECLARE_string(sfs_path);

namespace datasystem {
Status PersistenceApi::Init()
{
    if (FLAGS_l2_cache_type == "obs") {
    #ifndef DISABLE_OBS
        client_ = std::make_unique<ObsClient>(FLAGS_obs_endpoint, FLAGS_obs_bucket);
    #else
        LOG(FATAL) << "obs client not implemented for bazel build, set FLAGS_l2_cache_type to other type.";
    #endif

    } else if (FLAGS_l2_cache_type == "sfs") {
        client_ = std::make_unique<SfsClient>(FLAGS_sfs_path);
    } else {
        LOG(INFO) << FormatString("L2 cache is of type: %s, will not init PersistenceApi.", FLAGS_l2_cache_type);
        return Status::OK();
    }
    RETURN_IF_NOT_OK(client_->Init());
    return Status::OK();
}

Status PersistenceApi::Save(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                            const std::shared_ptr<std::iostream> &body, uint64_t asyncElapse)
{
    INJECT_POINT("PersistenceApi.Save.timeout", [&timeoutMs](int timeout) {
        timeoutMs = timeout;
        return Status::OK();
    });
    LOG(INFO) << FormatString("invoke save object to persistence. objectKey:%s, version %llu", objectKey, version);
    INJECT_POINT("persistence.service.save");

    std::string encodeKey;
    RETURN_IF_NOT_OK(PersistenceApi::UrlEncode(objectKey, encodeKey));
    std::string objectPath;
    objectPath.append(encodeKey).append("/").append(std::to_string(version));
    auto rc = client_->Upload(objectPath, timeoutMs, body, asyncElapse);
    if (rc.IsOk()) {
        LOG(INFO) << FormatString(
            "The object %s (EncodeKey %s) with version %llu is saved to persistence successfully.", objectKey,
            encodeKey, version);
    } else {
        LOG(ERROR) << FormatString(
            "The object %s (EncodeKey %s) with version %llu is saved to persistence with "
            "error code %s.",
            objectKey, encodeKey, version, Status::StatusCodeName(rc.GetCode()));
    }
    return rc;
}

Status PersistenceApi::Get(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                           std::shared_ptr<std::stringstream> &content)
{
    INJECT_POINT("persistence.service.get", [&content](std::string mockContent) {
        *content.get() << mockContent;
        return Status::OK();
    });
    Timer timer;
    LOG(INFO) << FormatString("invoke get object from persistence. objectKey: %s, version: %llu", objectKey, version);

    std::string encodeKey;
    RETURN_IF_NOT_OK(PersistenceApi::UrlEncode(objectKey, encodeKey));

    std::string objectPath;
    objectPath.append(encodeKey).append("/").append(std::to_string(version));
    Status res = client_->Download(objectPath, timeoutMs, content);
    if (res.IsOk()) {
        LOG(INFO) << FormatString("The object %s with version %llu is successfully got from persistence", objectKey,
                                  version);
        return Status::OK();
    }

    if (res.GetCode() != StatusCode::K_NOT_FOUND) {
        return res;
    }

    return GetWithoutVersion(objectKey, timeoutMs - timer.ElapsedMilliSecond(), 0, content);
}

// Create a new Get function without version to avoid trying download from persistant api twice. It will download the
// copy with max version number.
Status PersistenceApi::GetWithoutVersion(const std::string &objectKey, int64_t timeoutMs, uint64_t minVersion,
                                         std::shared_ptr<std::stringstream> &content)
{
    Timer timer;
    LOG(INFO) << FormatString("invoke get object from persistence without version parameter. objectKey: %s", objectKey);

    std::string encodeKey;
    RETURN_IF_NOT_OK(PersistenceApi::UrlEncode(objectKey, encodeKey));

    std::string objectPath;
    std::vector<L2CacheObjectInfo> objInfoList;
    uint64_t existMaxVersion = 0;
    RETURN_IF_NOT_OK(ListAllVersion(encodeKey + "/", timeoutMs, objInfoList, existMaxVersion));
    // objInfoList will be empty if no object satisfy the ListAllVersion query condition.
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!objInfoList.empty(), StatusCode::K_NOT_FOUND_IN_L2CACHE,
                                         "The object is not exist in persistence.");

    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        existMaxVersion > minVersion, StatusCode::K_NOT_FOUND_IN_L2CACHE,
        FormatString("The max version %zu in persistence should greater than the deleting version %zu.",
                     existMaxVersion, minVersion));

    LOG(INFO) << FormatString(
        "Try to get the newest version %llu of total %u versions exist in persistence, min version %zu.",
        existMaxVersion, objInfoList.size(), minVersion);

    objectPath.append(encodeKey).append("/").append(std::to_string(existMaxVersion));
    Status res = client_->Download(objectPath, timeoutMs - static_cast<int64_t>(timer.ElapsedMilliSecond()), content);
    if (res.IsOk()) {
        LOG(INFO) << FormatString("The object %s with version %llu is successfully got from persistence", objectKey,
                                  existMaxVersion);
    }
    return res;
}

Status PersistenceApi::Del(const std::string &objectKey, uint64_t maxVerToDelete, bool deleteAllVersion,
                           uint64_t asyncElapse, const uint64_t * const objectVersion, bool listIncompleteVersions)
{
    INJECT_POINT("persistence.service.del");
    LOG(INFO) << FormatString("invoke delete object from persistence. objectKey: %s, max version is %llu", objectKey,
                              maxVerToDelete);
    std::string encodeKey;
    RETURN_IF_NOT_OK(PersistenceApi::UrlEncode(objectKey, encodeKey));

    std::string objectPathWithoutVersion;
    // append '/', use max prefix to list all version
    objectPathWithoutVersion.append(encodeKey).append("/");

    std::vector<L2CacheObjectInfo> objInfoList;
    uint64_t existMaxVersion = 0;
    RETURN_IF_NOT_OK(ListAllVersion(objectPathWithoutVersion, HTTP_DEFAULT_TIMEOUT_MS, objInfoList, existMaxVersion,
                                    listIncompleteVersions));

    uint64_t actuallyMaxVerToDel = maxVerToDelete;
    if (!deleteAllVersion && existMaxVersion <= maxVerToDelete) {
        /**
         * in the clear old version scenarios.
         * we must keep a max version exist in the persistence.
         * if persistence only has one version, no matter what version it is, we must not delete it,
         * because the object has not been delete in datasystem.
         */
        actuallyMaxVerToDel = existMaxVersion > 0 ? existMaxVersion - 1 : 0;
        LOG(INFO) << FormatString(
            "The scenarios is clear old version, cloud storage exist Max Version(%llu) <= maxVerToDelete(%llu),"
            " we need keep the existMaxVersion, so actuallyMaxVerToDel: %llu",
            maxVerToDelete, existMaxVersion, actuallyMaxVerToDel);
    }

    bool verToDeleteIsFound = false;
    std::vector<std::string> objectsShouldBeDeleted;
    for (auto item = objInfoList.begin(); item < objInfoList.end(); item++) {
        if (item->version > actuallyMaxVerToDel) {
            continue;
        }
        if (item->version == maxVerToDelete || (objectVersion != nullptr && item->version == *objectVersion)) {
            verToDeleteIsFound = true;
        }
        objectsShouldBeDeleted.emplace_back(item->key);
    }
    if (!objectsShouldBeDeleted.empty()) {
        RETURN_IF_NOT_OK(client_->Delete(objectsShouldBeDeleted, asyncElapse));
        LOG(INFO) << "Delete [" << VectorToString(objectsShouldBeDeleted) << "] from persistence successfully.";
    }

    if (deleteAllVersion) {
        /**
         * in delete object scenarios, when all other version of object has been delete success, but the
         * 'maxVerToDelete' is not found, we consider this deletion is not complete.
         */
        CHECK_FAIL_RETURN_STATUS(
            verToDeleteIsFound, StatusCode::K_NOT_FOUND,
            FormatString("The scenarios is delete object %s, but the maxVerToDelete "
                         "%llu is not found. The deletion is not complete, and will be retried 1h later",
                         objectKey, maxVerToDelete));
    }

    return Status::OK();
}

Status PersistenceApi::ListAllVersion(const std::string &objectKey, int64_t timeoutMs,
                                      std::vector<L2CacheObjectInfo> &objInfoList, uint64_t &existMaxVersion,
                                      bool listIncompleteVersions)
{
    std::shared_ptr<GetObjectInfoListResp> resp = std::make_shared<GetObjectInfoListResp>();
    Timer timer;

    std::string preNextMarker;
    do {
        Status listRes = client_->List(objectKey, timeoutMs - static_cast<int64_t>(timer.ElapsedMilliSecond()),
                                       listIncompleteVersions, resp);
        RETURN_IF_NOT_OK(listRes);
        auto objs = resp->GetObjectInfo();
        RETURN_OK_IF_TRUE(objs.empty());
        if (resp->MaxVersion() > existMaxVersion) {
            existMaxVersion = resp->MaxVersion();
        }
        std::copy(std::make_move_iterator(objs.begin()), std::make_move_iterator(objs.end()),
                  std::back_inserter(objInfoList));
        if (!preNextMarker.empty() && preNextMarker == resp->NextMarker()) {
            LOG(ERROR) << "the nextMarker " << preNextMarker << " not change!";
            break;
        }
        preNextMarker = resp->NextMarker();
    } while (resp->IsTruncated());

    LOG(INFO) << "List version of " << objectKey << " from persistence, num = " << objInfoList.size();
    return Status::OK();
}

Status PersistenceApi::UrlEncode(const std::string &objectPath, std::string &encodePath)
{
    std::ostringstream uniqSlash;
    CURL *curl = curl_easy_init();
    if (curl == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to init curl, encode the object key failed.");
    }
    Raii raii([&curl] { curl_easy_cleanup(curl); });
    char *urlEncode = curl_easy_escape(curl, objectPath.c_str(), objectPath.size());
    if (urlEncode == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to curl_easy_escape, encode the object key failed.");
    }

    Raii raiiEncode([&urlEncode] { curl_free(urlEncode); });
    std::string path(urlEncode);
    for (size_t i = 0; i < path.size(); i++) {
        if (path.at(i) == '%') {
            uniqSlash << L2CACHE_PERCENT_SIGN_ENCODE;
        } else {
            uniqSlash << path.at(i);
        }
    }
    encodePath = uniqSlash.str();
    return Status::OK();
}

std::string PersistenceApi::GetL2CacheRequestSuccessRate() const
{
    if (client_ == nullptr) {
        return "";
    }
    return client_->GetRequestSuccessRate();
}
}  // namespace datasystem
