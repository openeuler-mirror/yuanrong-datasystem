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
 * Description: Some tools for parsing ID.
 */
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/util/id_tool.h"

DS_DECLARE_string(other_cluster_names);

namespace datasystem {
Status TrySplitWorkerIdFromObjecId(const std::string &objKey, std::string &workerUuid)
{
    workerUuid.clear();
    std::string objKeyWithoutTenantId;
    TenantAuthManager::Instance()->NamespaceUriToObjectKey(objKey, objKeyWithoutTenantId);
    auto res = Split(objKeyWithoutTenantId, ";");
    CHECK_FAIL_RETURN_STATUS(res.size() > 1, K_INVALID, "Cannot find workerId in objectKey");
    workerUuid = res[res.size() - 1];
    if (!Validator::IsUuid(workerUuid)) {
        workerUuid.clear();
        RETURN_STATUS(K_INVALID, "Cannot find workerId in objectKey");
    }
    return Status::OK();
}

bool HasWorkerId(const std::string &objKey)
{
    std::string objKeyWithoutTenantId;
    TenantAuthManager::Instance()->NamespaceUriToObjectKey(objKey, objKeyWithoutTenantId);
    auto res = Split(objKeyWithoutTenantId, ";");
    return res.size() > 1 && Validator::IsUuid(res[res.size() - 1]);
}

std::string SplitWorkerIdFromObjecId(const std::string &objKey)
{
    std::string objKeyWithoutTenantId;
    TenantAuthManager::Instance()->NamespaceUriToObjectKey(objKey, objKeyWithoutTenantId);
    auto res = Split(objKeyWithoutTenantId, ";");
    if (res.size() > 1 && Validator::IsUuid(res[res.size() - 1])) {
        return res[res.size() - 1];
    }
    return "";
}
}  // namespace datasystem
