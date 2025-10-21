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
 * Description: The hashmap object struct of worker.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_OBJ_CACHE_HASHMAP_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_OBJ_CACHE_HASHMAP_H

#include <memory>
#include <string>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/shared_memory/shm_unit.h"

namespace datasystem {
namespace object_cache {

using TbbHashMap = tbb::concurrent_hash_map<std::string, std::shared_ptr<ShmUnit>>;

class ObjCacheHashmap : public ObjectInterface {
public:
    /**
     * @brief Default constructor.
     */
    ObjCacheHashmap();

    /**
     * @brief Set the value of the field.
     * @param[in] field The field.
     * @param[in] value The value.
     * @return Status of this call.
     */
    Status Set(const std::string &field, const std::shared_ptr<ShmUnit> &value);

    /**
     * @brief Get the value of the field.
     * @param[in] field The field.
     * @param[out] value The value.
     * @return Status of this call.
     */
    Status Get(const std::string &field, std::string &value);

    /**
     * @brief Check the field exists or not.
     * @param[in] field The field.
     * @return True if field exist.
     */
    bool Exist(const std::string &field) const;

    /**
     * @brief Erase the field from the map. overrides from ObjectInterface
     * @param field The field.
     * @return Status of this call.
     */
    Status Erase(const std::string &field) override;

    /**
     * @brief Free all values and clear the map.
     * @return Status of this call.
     */
    Status FreeResources() override;

    virtual ~ObjCacheHashmap();

private:
    TbbHashMap map_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_OBJ_CACHE_HASHMAP_H
