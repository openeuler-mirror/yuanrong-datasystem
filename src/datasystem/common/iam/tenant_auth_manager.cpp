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
 * Description: TenantAuthManager is used to cache tokens and manage token reference counting.
 */
#include "datasystem/common/iam/tenant_auth_manager.h"

#include <cstring>
#include <string_view>

#include <nlohmann/json.hpp>

#include "datasystem/common/log/log.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/iam/yuanrong_iam.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"

DS_DEFINE_string(
    iam_kit, "none",
    "The type of iam kit, none is default, value range: [none, yuanrong_iam]");
DS_DEFINE_validator(iam_kit, &Validator::ValidateIAMKit);

namespace datasystem {
TenantAuthManager *TenantAuthManager::Instance()
{
    static TenantAuthManager tenantAuthManager;
    return &tenantAuthManager;
}

std::string IAMKitToString(IAMKit kit)
{
    if (kit == YUANRONG_IAM) {
        return "yuanrong iam";
    }
    return "";
}

Status TenantAuthManager::Init(bool authEnable, std::shared_ptr<AkSkManager> akSkManager)
{
    IAMKit iamKit = YUANRONG_IAM;
    LOG(INFO) << "Init TenantAuthManager, authorization_enable_enable flag is " << authEnable
              << ", iam_kit is: " << IAMKitToString(iamKit);
    authEnable_ = authEnable;
    akSkManager_ = std::move(akSkManager);
    iamAuth_ = std::make_shared<YuanRongIAM>(akSkManager_);
    return iamAuth_->Init(authEnable_);
}

Status TenantAuthManager::TenantTokenAuth(const SensitiveValue &token, std::string &tenantId)
{
    TbbClientTokenTable::accessor accessor;
    if (clientTokenTable_.find(accessor, token)) {
        tenantId = accessor->second;
    } else {
        LOG(INFO) << "Unable to get tenantId from cache, trying to get from iam.";
        uint64_t expireSec = 0;
        RETURN_IF_NOT_OK(iamAuth_->VerifyTenantToken(token, tenantId, expireSec));
        LOG(INFO) << FormatString("Successfully obtained tenantId(%s) from iam, TTL: %llu", tenantId, expireSec);
        (void)clientTokenTable_.insert({ token, tenantId });
        // Add timer and callback func for client token.
        TimerQueue::TimerImpl timer;
        (void)clientTokenTimer_.insert({ token, timer });
        const uint32_t toMs = 1000;
        INJECT_POINT("worker.tokenexpire", [&expireSec]() {
            expireSec = toMs;
            return Status::OK();
        });
        RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
            expireSec * toMs,
            [this, token]() {
                if (!clientTokenTable_.erase(token)) {
                    LOG(INFO) << "Fail to erase token in clientTokenTable";
                }
                if (!clientTokenTimer_.erase(token)) {
                    LOG(INFO) << "Fail to erase token in clientTokenTimer";
                }
            },
            timer));
    }
    return Status::OK();
}

Status TenantAuthManager::TenantAkAuth(const std::string &accessKey, const std::string &reqTenantId,
                                       std::string &tenantId)
{
    TbbClientAkTable::accessor accessor;
    if (clientAkTable_.find(accessor, accessKey)) {
        tenantId = accessor->second.second ? reqTenantId : accessor->second.first;
    } else {
        LOG(INFO) << "Unable to get credential from cache, trying to get from iam.";
        uint64_t expireSec = 0;
        SensitiveValue secretKey;
        bool isSystemRole = false;
        auto tmpTenantId = reqTenantId;
        RETURN_IF_NOT_OK(iamAuth_->VerifyTenantAkSk(accessKey, secretKey, tmpTenantId, expireSec, isSystemRole));
        if (isSystemRole) {
            tenantId = reqTenantId;
        } else {
            tenantId = tmpTenantId;
        }
        LOG(INFO) << FormatString(
            "[%s] Successfully obtained credential from iam, (ak hash: %s, sk hash: %s, TTL: %llu, isSystemRole: %d).",
            tenantId, std::hash<std::string>()(accessKey),
            GetTruncatedStr(std::to_string(std::hash<std::string>()(secretKey.GetData()))), expireSec, isSystemRole);
        (void)clientAkTable_.insert({ accessKey, { tenantId, isSystemRole } });
        // Create and use dataKey to decrypt secertKey when set tenant aksk.
        RETURN_IF_NOT_OK(akSkManager_->SetTenantAkSk(accessKey, secretKey));
        RETURN_OK_IF_TRUE(expireSec == 0);
        // Add timer and callback func for client aksk.
        TimerQueue::TimerImpl timer;
        const uint32_t toMs = 1000;
        RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
            expireSec * toMs,
            [this, accessKey]() {
                if (!clientAkTable_.erase(accessKey)) {
                    LOG(INFO) << "Fail to erase accessKey in clientAkTable, the accessKey is: " << accessKey;
                }
                if (!akSkManager_->RemoveAccessKey(accessKey)) {
                    LOG(INFO) << "Fail to erase accessKey in AkSkManager";
                }
            },
            timer));
    }
    return Status::OK();
}

void TenantAuthManager::NamespaceUriToObjectKey(const std::string &namespaceUri, std::string &objectKey)
{
    size_t pos = namespaceUri.find(K_SEPARATOR);
    if (pos == std::string::npos) {
        objectKey = namespaceUri;
        return;
    }
    objectKey = namespaceUri.substr(pos + 1);
}

void TenantAuthManager::NamespaceUriToObjectKey(const std::string &namespaceUri, std::string_view &objectKey)
{
    std::string_view v{ namespaceUri };
    auto pos = v.find(K_SEPARATOR);
    if (pos != std::string_view::npos) {
        objectKey = v.substr(pos + 1);
    } else {
        objectKey = v;
    }
}

Status TenantAuthManager::ConstructNamespaceUri(const std::string &token, const std::string &objectKey,
                                                std::string &namespaceUri)
{
    namespaceUri = objectKey;
    if (authEnable_) {
        std::string tenantId;
        std::string token1(token);
        RETURN_IF_NOT_OK(TenantTokenAuth(token1, tenantId));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!tenantId.empty(), K_RUNTIME_ERROR, "Get tenant id failed");
        namespaceUri = tenantId + K_SEPARATOR + objectKey;
    }
    return Status::OK();
}

std::string TenantAuthManager::ConstructNamespaceUriWithTenantId(const std::string &tenantId,
                                                                 const std::string &objectKey)
{
    std::string namespaceUri;
    if (!tenantId.empty()) {
        namespaceUri = tenantId + K_SEPARATOR + objectKey;
    } else {
        namespaceUri = objectKey;
    }
    return namespaceUri;
}

Status TenantAuthManager::ConstructNamespaceUri(const std::string &token,
                                                const google::protobuf::RepeatedPtrField<std::string> &objectKeys,
                                                std::vector<std::string> &outNamespaceUris)
{
    outNamespaceUris.resize(objectKeys.size());
    for (int i = 0; i < objectKeys.size(); i++) {
        std::string namespaceUri;
        RETURN_IF_NOT_OK(ConstructNamespaceUri(token, objectKeys[i], namespaceUri));
        outNamespaceUris[i] = std::move(namespaceUri);
    }
    return Status::OK();
}

std::vector<std::string> TenantAuthManager::ConstructNamespaceUriWithTenantId(
    const std::string &tenantId, const google::protobuf::RepeatedPtrField<std::string> &objectKeys)
{
    std::vector<std::string> namespaceUris;
    namespaceUris.reserve(objectKeys.size());
    for (int i = 0; i < objectKeys.size(); i++) {
        std::string namespaceUri = ConstructNamespaceUriWithTenantId(tenantId, objectKeys[i]);
        namespaceUris.emplace_back(std::move(namespaceUri));
    }
    return namespaceUris;
}

std::string TenantAuthManager::ExtractTenantId(const std::string &objectKey)
{
    auto pos = objectKey.find_last_of(K_SEPARATOR);
    if (pos != std::string::npos) {
        auto tenantId = objectKey.substr(0, pos);
        return tenantId;
    }
    return DEFAULT_TENANT_ID;
}

std::string TenantAuthManager::ExtractRealObjectKey(const std::string &namespaceUri)
{
    auto pos = namespaceUri.find_last_of(K_SEPARATOR);
    if (pos != std::string::npos) {
        return namespaceUri.substr(pos + 1);
    }
    return namespaceUri;
}
}  // namespace datasystem
