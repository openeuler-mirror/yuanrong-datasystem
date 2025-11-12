/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Module responsible for provide mediator in master's delete process.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_DELETE_OBJECT_MEDIATOR_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_DELETE_OBJECT_MEDIATOR_H

#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <map>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace master {
struct DeleteStruct {
    std::set<std::string> locations;
    uint64_t version;
    uint32_t writeMode;
};

class DeleteObjectMediator {
public:
    DeleteObjectMediator(const std::string &workerAddress, const std::unordered_map<std::string, bool> &map);

    /**
     * @brief Get objectKeys in this mediator.
     * @return The vector of objectKeys in this mediator.
     */
    std::vector<std::string> GetObjKeys();

    /**
     * @brief Set status if input status is error.
     * @param[in] status The status.
     */
    void SetStatusIfError(const Status &status);

    /**
     * @brief Set status.
     * @param[in] status The status.
     */
    void SetStatus(const Status &status);

    /**
     * @brief Return the successful deleted objectKeys.
     * @return The const reference of successDelIds.
     */
    const std::unordered_set<std::string> &GetSuccessDelIds() const;

    /**
     * @brief Add successful deleted objectKey.
     * @param[in] objKey The successful deleted objectKey.
     */
    void AddSuccessDelId(const std::string &objKey);

    /**
     * @brief Return the failed delete objectKeys.
     * @return The const reference of failedObjs.
     */
    const std::unordered_set<std::string> &GetFailedObjs() const;

    /**
     * @brief Add failed delete objectKey.
     * @param[in] objKey The failed delete objectKey.
     */
    void AddFailedDelId(const std::string &objKey);

    /**
     * @brief Add failed delete objectKeys.
     * @param[in] objKeys The failed delete objectKeys.
     */
    template <typename container>
    void AddFailedDelIds(const container &objKeys)
    {
        failedIds_.insert(objKeys.begin(), objKeys.end());
    }

    /**
     * @brief Return the objectKeys without meta.
     * @note Currently we only care about hash type objectKey.
     * @return The const reference of objsWithoutMeta_.
     */
    const std::unordered_set<std::string> &GetHashObjsWithoutMeta() const;

    /**
     * @brief Add an objectKey without meta.
     * @note Currently we only care about hash type objectKey.
     * @param[in] objKey An objectKey without meta.
     */
    void AddHashObjsWithoutMeta(const std::string &objKey);

    /**
     * @brief Return the last status of mediator.
     * @return The const reference of status.
     */
    const Status &GetStatus() const;

    /**
     * @brief Return the objectKeys map in this mediator.
     * @return The const reference of ids map.
     */
    const std::unordered_map<std::string, bool> &GetReqIdsMap() const;

    /**
     * @brief Set the map of objectKeys which need to notify other worker delete.
     * The key of map is objectKeys, value is set of worker addresses which master need to notify.
     * @param[in] map The map of objectKeys which need to notify other worker delete.
     */
    void SetIdsNeedToNotifyWorker(std::unordered_map<std::string, DeleteStruct> &&map);

    /**
     * @brief Get the map of objectKeys which need to notify other worker delete.
     * @return The const reference of idsNeedToNotifyWorker_.
     */
    std::unordered_map<std::string, DeleteStruct> &GetIdsNeedToNotifyWorker();

    /**
     * @brief Set the nested refs ids needed to notify other workers.
     * @param[in] nestedRefsIds The nested refs ids needed to notify other workers.
     */
    void SetToBeNotifiedNestedRefs(std::vector<std::string> &&nestedRefsIds);

    const std::vector<std::string> &GetToBeNotifiedNestedRefs() const;

    /**
     * @brief Check these objectKeys don't need to notify worker to delete.
     * @return True if all value set of idsNeedToNotifyWorker_ is empty, otherwise return false.
     */
    bool CheckNoNeedToNotifyWorker();

    /**
     * @brief Get the worker address that make this request.
     * @return The const reference of sourceWorker_.
     */
    const std::string &GetSourceWorker() const;

    /**
     * @brief Get all objectKeys with no refcount in the range of this request.
     * Don't contain the nested objects deleted successfully but not in reqIdsMap_.
     * @return The vector of objectKeys with no refcount.
     */
    std::vector<std::string> GetNotRefIds();

    /**
     * @brief If the object in the delete request carries a version, store the version here.
     * @param[in] objKey2Version The object2Version massage in delete request.
     */
    void SetObjKey2Version(std::unordered_map<std::string, uint64_t> &&objKey2Version);

    /**
     * @brief Check whether the version of the object in the delete request is expired.
     * @param[in] objKey The object to be checked.
     * @param[in] currVersion The version of this object in this node.
     * @return T/F
     */
    bool CheckIfExpired(const std::string &objKey, uint64_t currVersion);

    /**
     * @brief Get the version of the object in the delete request.
     * @param[in] objKey The object key.
     * @return the version of the object in the delete request.
     */
    int64_t GetObjectVersionInRequest(const std::string &objKey);

    /**
     * @brief If the version of the object in the delete request is out of date, store the object here.
     * @param[in] outdatedObj The outdated object.
     */
    void SetOutdatedObj(const std::string &outdatedObj);

    /**
     * @brief Get outdated objs.
     * @return outdatedObj The outdated object.
     */
    const std::vector<std::string> &GetOutdatedObjs() const;

private:
    std::string sourceWorker_;
    std::unordered_map<std::string, bool> reqIdsMap_;
    std::unordered_set<std::string> failedIds_;
    std::unordered_set<std::string> successDelIds_;
    std::unordered_set<std::string> hashObjsWithoutMeta_;
    std::unordered_map<std::string, uint64_t> objKey2Version_;
    std::vector<std::string> outdatedObjs_;
    /**
     * The key of idsNeedToNotifyWorker_ is objectKeys, value is set of worker addresses which master need to notify.
     * idsNeedToNotifyWorker_ may include some nested children objects which may not in reqIdsMap_.
     */
    std::unordered_map<std::string, DeleteStruct> idsNeedToNotifyWorker_;
    std::vector<std::string> toBeNotifiedNestedRefs_;
    Status lastRc_;
};
}  // namespace master
}  // namespace datasystem
#endif