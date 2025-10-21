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
 * Description: This api provides calls to a persistence service through a cloud client
 */
#ifndef DATASYSTEM_COMMON_PERSISTENCE_API_H
#define DATASYSTEM_COMMON_PERSISTENCE_API_H

#include "datasystem/common/l2cache/l2cache_client.h"
#include "datasystem/common/l2cache/l2cache_object_info.h"

namespace datasystem {
class PersistenceApi {
public:
    PersistenceApi() = default;

    ~PersistenceApi() = default;

    /**
     * @brief init the PersistenceApi
     * @return execute result
     */
    Status Init();

    /**
     * @brief save the object to 3rd storage, for example obs
     * @param[in] objectKey the object namespace in datastystem. i.e. <tenantId>/<objectKey>
     * @param[in] version the version of the object
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[in] body the content of the object
     * @param[in] asyncElapse The time this object being in the async queue
     * @return execute result
     */
    Status Save(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                const std::shared_ptr<std::iostream> &body, uint64_t asyncElapse = 0);

    /**
     * @brief get the persistence object with the given version.
     * if the given version is not exist, return the max version exist in persistence.
     *
     * @param[in] objectKey the object key:<TenantId>/<ObjectKey>
     * @param[in] version the object meta version
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[out] content the string stream byte of the object
     * @return Status of call
     */
    Status Get(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
               std::shared_ptr<std::stringstream> &content);

    /**
     * @brief get the persistence object without any given version.
     * @param[in] objectKey the object key:<TenantId>/<ObjectKey>
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[in] minVersion the min version for object.
     * @param[out] content the string stream byte of the object
     * @return Status of call
     */
    Status GetWithoutVersion(const std::string &objectKey, int64_t timeoutMs, uint64_t minVersion,
                             std::shared_ptr<std::stringstream> &content);

    /**
     * @brief delete all the satisfied version of the object
     * satisfied version means: version <= maxVerToDelete.
     *
     * two scenarios:
     * 1) deleteAllVersion=false indicate datasystem don't delete the object, but only clear the old version of the
     * object, in this scenarios, invoker need to know that:
     * if existMaxVersion(the max version exist persistence) <= maxVerToDelete, we must not delete the existMaxVersion,
     * the purpose is that we need keep a version of object in cloud storage, finally we will delete the version
     * satisfied: version <= existMaxVersion - 1
     *
     * 2) deleteAllVersion=true, datasystem delete the object, it means clear all the version of the object,
     * in this scenarios, invoker need to know that:
     * if the maxVerToDelete is not found in persistence(i.e. maybe the maxVerToDelete version is in upload process),
     * we do not consider the deletion is complete, even though all other version success to delete.
     *
     * @param[in] objectKey the object path in persistence
     * @param[in] maxVerToDelete the max version can be delete
     * @param[in] deleteAllVersion true if the datasystem delete the object, otherwise false
     * @param[in] asyncElapse The time this object being in the async queue
     * @param[in] objectVersion The object version.
     * @param[in] listIncompleteVersions whether to list those incomplete versions. Usually they are partially uploaded.
     * @return Status of call
     */
    Status Del(const std::string &objectKey, uint64_t maxVerToDelete, bool deleteAllVersion, uint64_t asyncElapse = 0,
               const uint64_t * const objectVersion = nullptr, bool listIncompleteVersions = false);

    /**
     * @brief Obtains the request success rate of l2cache.
     * @return Success rate of l2cache request.
     */
    std::string GetL2CacheRequestSuccessRate() const;

protected:
    /**
     * @brief list all the version of the object in persistence
     * @param[in] objectKey the object key in datasystem
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[out] objInfoList the object info list
     * @param[out] existMaxVersion the max version of the object exist in persistence
     * @param[in] listIncompleteVersions whether to list those incomplete versions. Usually they are partially uploaded.
     * @return Status of call
     */
    Status ListAllVersion(const std::string &objectKey, int64_t timeoutMs, std::vector<L2CacheObjectInfo> &objInfoList,
                          uint64_t &existMaxVersion, bool listIncompleteVersions = false);

    std::unique_ptr<L2CacheClient> client_;
};
}  // namespace datasystem

#endif