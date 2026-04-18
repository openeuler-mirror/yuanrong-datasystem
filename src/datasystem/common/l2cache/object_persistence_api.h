/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Persistence API implementation for object-per-file backends.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBJECT_PERSISTENCE_API_H
#define DATASYSTEM_COMMON_L2CACHE_OBJECT_PERSISTENCE_API_H

#include <memory>
#include <vector>

#include "datasystem/common/l2cache/l2cache_client.h"
#include "datasystem/common/l2cache/l2cache_object_info.h"
#include "datasystem/common/l2cache/persistence_api.h"

namespace datasystem {
class ObjectPersistenceApi : public PersistenceApi {
public:
    ObjectPersistenceApi();
    ~ObjectPersistenceApi() override;

    Status Init() override;
    Status Save(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                const std::shared_ptr<std::iostream> &body, uint64_t asyncElapse = 0,
                WriteMode writeMode = WriteMode::NONE_L2_CACHE, uint32_t ttlSecond = 0) override;
    Status Get(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
               std::shared_ptr<std::stringstream> &content) override;
    Status GetWithoutVersion(const std::string &objectKey, int64_t timeoutMs, uint64_t minVersion,
                             std::shared_ptr<std::stringstream> &content) override;
    Status Del(const std::string &objectKey, uint64_t maxVerToDelete, bool deleteAllVersion, uint64_t asyncElapse = 0,
               const uint64_t *const objectVersion = nullptr, bool listIncompleteVersions = false) override;
    Status PreloadSlot(const std::string &sourceWorkerAddress, uint32_t slotId,
                       const SlotPreloadCallback &callback) override;
    Status MergeSlot(const std::string &sourceWorkerAddress, uint32_t slotId) override;
    Status CleanupLocalSlots() override;
    std::string GetL2CacheRequestSuccessRate() const override;

private:
    Status ListAllVersion(const std::string &objectKey, int64_t timeoutMs, std::vector<L2CacheObjectInfo> &objInfoList,
                          uint64_t &existMaxVersion, bool listIncompleteVersions = false);

    std::unique_ptr<L2CacheClient> client_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_OBJECT_PERSISTENCE_API_H
