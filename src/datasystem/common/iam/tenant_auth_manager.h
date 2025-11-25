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
#ifndef DATASYSTEM_COMMON_IAM_TENANT_AUTH_MANAGER_H
#define DATASYSTEM_COMMON_IAM_TENANT_AUTH_MANAGER_H

#include <memory>
#include <mutex>
#include <unordered_map>

#include <google/protobuf/repeated_field.h>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"

namespace datasystem {
const std::string K_SEPARATOR = "$";

struct SensitiveValueHashCompare {
    static size_t hash(const SensitiveValue &a)
    {
        if (a.Empty()) {
            return 0;
        }
        return MurmurHash3_32((const uint8_t *)a.GetData(), a.GetSize());
    }
    static bool equal(const SensitiveValue &a, const SensitiveValue &b)
    {
        return a == b;
    }
};

using TbbClientTokenTable = tbb::concurrent_hash_map<SensitiveValue, std::string, SensitiveValueHashCompare>;
using TbbTokenTimerTable = tbb::concurrent_hash_map<SensitiveValue, TimerQueue::TimerImpl, SensitiveValueHashCompare>;
using TbbClientAkTable = tbb::concurrent_hash_map<std::string, std::pair<std::string, bool>>;

class TenantAuthManager {
public:
    static TenantAuthManager *Instance();
    Status Init(bool authEnable, std::shared_ptr<AkSkManager> akSkManager = nullptr);

    Status TenantTokenAuth(const SensitiveValue &token, std::string &tenantId);

    Status TenantAkAuth(const std::string &accessKey, const std::string &reqTenantId, std::string &tenantId);

    /**
     * @brief Convert the namespace URI to the original objectKey.
     * @param[in] namespaceUri A namespace URI of the object.
     * @param[out] objectKey The original object key that convert from namespace URI.
     * @return Status of the call.
     */
    void NamespaceUriToObjectKey(const std::string &namespaceUri, std::string &objectKey);

    /**
     * @brief Convert the namespace URI to the original objectKey.
     * @param[in] namespaceUri A namespace URI of the object.
     * @param[out] objectKey The original object key that convert from namespace URI.
     * @return Status of the call.
     */
    void NamespaceUriToObjectKey(const std::string &namespaceUri, std::string_view &objectKey);

    /**
     * @brief Construct namespace URI by token and object key.
     * @param[in] token The tenant authentication identity.
     * @param[in] objectKey The original object key of a tenant.
     * @param[out] namespaceUri A namespace URI of the object.
     * @return Status of the call.
     */
    Status ConstructNamespaceUri(const std::string &token, const std::string &objectKey, std::string &namespaceUri);

    /**
     * @brief Construct namespace URI by tenant id in threadlocal and object key.
     * @param[in] tenantId The tenant id.
     * @param[in] objectKey The original object key of a tenant.
     * @return The namespace URI of the object.
     */
    static std::string ConstructNamespaceUriWithTenantId(const std::string &tenantId, const std::string &objectKey);

    /**
     * @brief Construct namespace URI by token and object key.
     * @param[in] token Token to be authenticated.
     * @param[in] objectKeys The original object keys of a tenant.
     * @param[out] outNamespaceUris Namespace URIs of the object.
     * @return Return status code.
     */
    Status ConstructNamespaceUri(const std::string &token,
                                 const google::protobuf::RepeatedPtrField<std::string> &objectKeys,
                                 std::vector<std::string> &outNamespaceUris);

    /**
     * @brief Construct namespace URI by tenant id in threadlocal and object key.
     * @param[in] tenantId The tenant id.
     * @param[in] objectKeys The original object keys of a tenant.
     * @return Namespace URIs of the objects.
     */
    static std::vector<std::string> ConstructNamespaceUriWithTenantId(
        const std::string &tenantId, const google::protobuf::RepeatedPtrField<std::string> &objectKeys);

    /**
     * @brief Extract tenant ID from objectKey.
     * @param[in] objectKey The ID of the object that need to be extracted.
     * @return Tenant ID of the object key.
     */
    static std::string ExtractTenantId(const std::string &objectKey);

    /**
     * @brief Extract real object key from namespaceUri.
     * @param[in] namespaceUri The ID of the object that need to be extracted.
     * @return Real object key.
     */
    static std::string ExtractRealObjectKey(const std::string &namespaceUri);

    bool AuthEnabled() const
    {
        return authEnable_;
    }

    TbbTokenTimerTable clientTokenTimer_;

protected:
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };

private:
    TbbClientTokenTable clientTokenTable_;
    TbbClientAkTable clientAkTable_;
    bool authEnable_{ false };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_IAM_TENANT_AUTH_MANAGER_H
