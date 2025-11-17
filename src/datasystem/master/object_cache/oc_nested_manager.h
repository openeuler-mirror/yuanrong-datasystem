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
 * Description: Module responsible for managing the object's nested relationship on the master.
 */
#ifndef DATASYSTEM_MASTER_NESTED_OBJECT_MANAGER_H
#define DATASYSTEM_MASTER_NESTED_OBJECT_MANAGER_H

#include "datasystem/common/log/log.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

/*
 * Nested manager example :
 *
 *       nested(ObjA, {OBjB, ObjC, ObjD})
 *       nested(ObjB, {OBjC})
 *       nested(ObjC, {OBjD})
 *
 *       ObjA -> ObjB -> ObjC -> ObjD
 *            -> ObjC
 *            -> ObjD
 *
 *  ----------------------------      ---------------------
 * |    RelationShip_table     |     | ParentRefCnt_table |
 *  ----------------------------      ---------------------
 * | parentId |     childId    |     | childId | refCount |
 * |   ObjA   | ObjB,ObjC,ObjD |     |  ObjB   |     1    |
 * |   ObjB   |      ObjC      |     |  ObjC   |     2    |
 * |   ObjC   |      ObjD      |     |  ObjD   |     2    |
 *  ----------------------------      ---------------------
 *
 *  RelationShip_table (dependencyTable_) to cache the relationship of nested
 *
 *  ParentRefCnt_table (nestedRef_) to caches how many times' obj is referenced
 *  ObjB depends on ObjA , the refCount of ObjC is 1
 *  ObjC depends on ObjA and ObjB , the refCount of ObjC is 2
 *  ObjD depends on ObjA and ObjC , the refCount of ObjC is 2
 */

namespace datasystem {
namespace master {
class OCNestedManager {
public:
    OCNestedManager(std::shared_ptr<ObjectMetaStore> objectRockStore, EtcdClusterManager *cm)
        : objectStore_(std::move(objectRockStore)),
          nestedRef_(std::make_unique<object_cache::ObjectRefInfo<std::string>>(false))
    {
        etcdCM_ = cm;
    }

    ~OCNestedManager() = default;

    /**
     * @brief Check if the obj is dependent on other objs.
     * @param[in] childObjectKey The child object key in nested relationship.
     * @return Whether it is no ref.
     */
    bool CheckIsNoneNestedRefById(const std::string &childObjectKey);

    /**
     * @brief Add relationship for parentObjectKey.
     * @param[in] parentObjectKey The parent object key in nested relationship.
     * @param[in] nestedObjectKeys The children object keys in nested relationship.
     * @return Status of the result.
     */
    Status IncreaseNestedRefCnt(const std::string &parentObjectKey, const std::set<ImmutableString> &nestedObjectKeys);

    /**
     * @brief Increase dependent object count from remote worker.
     * @param[in] nestedObjectKey The children object keys in nested relationship.
     * @return Status of the result.
     */
    Status IncreaseNestedRefCnt(const std::string &nestedObjectKey, uint32_t i = 1);

    /**
     * @brief Remove relationship for parentObjectKey.
     * @param[in] parentObjectKey The parent object key in nested relationship.
     * @param[out] zeroRefIds The object keys which is not referenced by parent object.
     * @return Status of the result.
     */
    Status DecreaseNestedRefCnt(const std::string &parentObjectKey, std::vector<std::string> &zeroRefIds);

    /**
     * @brief Decrease dependent object count from remote worker.
     * @param[in] nestedObjectKey The children object keys in nested relationship.
     * @return Status of the result.
     */
    Status DecreaseNestedRefCnt(const std::string &nestedObjectKey);

    /**
     * @brief Recover the nested relationship and reference count from rocksdb.
     * @param[in] refTable The table name of relationship info in database.
     * @param[in] countTable The table name that shows nested object count
     * @return Status of the result.
     */
    Status RecoverRelationshipData(const std::string &refTable, const std::string &countTable);

    /**
     * @brief Check whether nested keys can be set.
     * @param[in] parentObjectKey The parent object key in nested relationship.
     * @param[in] nestedObjectKeys The children object keys need to be check.
     * @return Return true if nested keys can be set.
     */
    bool NestedKeysCanSet(const std::string &parentObjectKey, const std::set<ImmutableString> &nestedObjectKeys);

    /**
     * @brief Check whether nested keys are same.
     * @param[in] parentObjectKey The parent object key in nested relationship.
     * @param[in] nestedObjectKeys The children object keys need to be check.
     * @return Return true if nested keys are same.
     */
    bool IsNestedKeysDiff(const std::string &parentObjectKey, const std::set<ImmutableString> &nestedObjectKeys);

    /**
     * @brief Get the Nested Relationship object
     * @param parentIds[in] parent ids for nested object
     * @param nestedKeys[out] nested keys for object
     */
    void GetNestedRelationship(const std::string &parentIds, std::vector<std::string> &nestedKeys);

    /**
     * @brief Get the All Parent Ids object
     * @param objKeys[out] all parent ids for nested object.
     */
    void GetAllParentIds(std::vector<std::string> &objKeys);

    /**
     * @brief Get the All Nested Keys object
     * @param nestedKeys[out] nested keys
     */
    void GetAllNestedKeys(std::vector<std::string> &nestedKeys);

    /**
     * @brief Get the Nested Key Ref object
     * @param objKey get nested keys ref.
     * @return uint32_t ref
     */
    uint32_t GetNestedKeyRef(const std::string &objKey);

    /**
     * @brief RemoveRelationshipData
     * @param parentObjectKey[in] parent object keys.
     */
    void RemoveRelationshipData(const std::string &parentObjectKey);

    /**
     * @brief RemoveNestIdsRef
     * @param nestedKeys[in] nested object keys
     */
    void RemoveNestIdsRef(const std::vector<std::string> &nestedKeys);

    /**
     * @brief
     * @param[in] objKey object key.
     * @return true if object key is not a parent obj.
     */
    bool CheckIsNotParentId(const std::string &objKey);

private:
    std::unordered_map<ImmutableString, std::set<ImmutableString>> dependencyTable_;
    std::shared_ptr<ObjectMetaStore> objectStore_;
    std::unique_ptr<object_cache::ObjectRefInfo<std::string>> nestedRef_;  // std::string -> ObjectKey
    std::shared_timed_mutex mutex_;
    EtcdClusterManager *etcdCM_ = nullptr;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_NESTED_OBJECT_MANAGER_H
