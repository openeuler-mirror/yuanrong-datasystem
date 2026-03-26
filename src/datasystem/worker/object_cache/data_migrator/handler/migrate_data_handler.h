/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Migrate data handler.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_HANDLER_H
#define DATASYSTEM_MIGRATE_DATA_HANDLER_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/worker/object_cache/data_migrator/basic/base_data_unit.h"
#include "datasystem/worker/object_cache/limiter/data_limiter.h"
#include "datasystem/worker/object_cache/data_migrator/basic/migrate_progress.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/selection_strategy.h"
#include "datasystem/worker/object_cache/data_migrator/transport/migrate_transport.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"

namespace datasystem {
namespace object_cache {

class MigrateDataHandler {
public:
    MigrateDataHandler(MigrateType type, const std::string &localAddr,
                       const std::vector<ImmutableString> &needMigrateDataIds, std::shared_ptr<ObjectTable> objectTable,
                       std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi, std::shared_ptr<SelectionStrategy> strategy,
                       std::shared_ptr<MigrateProgress> progress = nullptr, bool isRetry = false, uint32_t slotId = 0);

    ~MigrateDataHandler() = default;

    struct MigrateResult {
        std::string address;
        Status status;
        std::unordered_set<ImmutableString> successIds;
        std::unordered_set<ImmutableString> failedIds;
        std::unordered_set<ImmutableString> skipIds;
        std::shared_ptr<SelectionStrategy> strategy;
        int retryCount = 0;
    };

    /**
     * @brief Migrate object data to remote node.
     * @param[in] isSlotMigration Whether this is a slot migration (no batching).
     * @return Migrate result contains ip address, status, success ids, failed ids and skip ids.
     */
    MigrateResult MigrateDataToRemote(bool isSlotMigration = false);

    /**
     * @brief Pretty print the migrate result.
     * @param[in] result Migrate data result.
     * @return migrate result in string.
     */
    static std::string ResultToString(const MigrateResult &result);

    /**
     * @brief Migrate data to remote rpc, if meets RPC error, it would retry.
     * @param[in] api Remote worker api.
     * @param[in] req Migrate data request.
     * @param[in] payloads Need migrate data.
     * @param[out] rsp Migrate data response.
     * @return K_OK if success, the error otherwise.
     */
    Status MigrateDataToRemoteRetry(const std::shared_ptr<WorkerRemoteWorkerOCApi> &api, MigrateDataReqPb &req,
                                    const std::vector<MemView> &payloads, MigrateDataRspPb &rsp);

private:
    /**
     * @brief Spy on remote node remian bytes.
     * @param[in] type The cache type.
     * @return K_OK if success, the error otherwise.
     */
    Status SpyOnRemoteRemainBytes(CacheType type);

    /**
     * @brief Adjust max batch size via size.
     * @param[in] size New size.
     */
    void AdjustMaxBatchSize(uint64_t size);

    /**
     * @brief Indicate the remote node is lack of resources or not.
     * @return True if remote node is lack of resources.
     */
    bool IsRemoteLackResources() const;

    /**
     * @brief Add object data into migrate data list.
     * @param[in] objectKV Object key value.
     * @return K_OK if success, the error otherwise.
     */
    Status AddObjectDataLocked(const ObjectKV &objectKV);

    /**
     * @brief Send data to remote node.
     * @param[in] isSlotMigration Whether this is a slot migration.
     */
    void SendDataToRemote(bool isSlotMigration = false);

    /**
     * @brief Try update rate by response for 5 times.
     * @param[in] rate Rate from response.
     * @return K_OK if success, the error otherwise.
     */
    Status TryUpdateRate(uint64_t rate);

    /**
     * @brief Construct the migrate data result.
     * @param[in] status Migrate data status.
     * @return Migrate data result.
     */
    MigrateResult ConstructResult(Status status) const;

    /**
     * @brief Clear datas and state.
     */
    void Clear();

    /**
     * @brief Indicate current batch is full or not.
     * @return True if current batch is full.
     */
    bool IsFull() const
    {
        constexpr uint64_t maxBatchCount = 300;
        return currBatchSize_ >= maxBatchSize_ || currBatchCount_ >= maxBatchCount;
    }

    /**
     * @brief Split migrate data by cache type.
     * @param[in] memoryDataIds The memory data.
     * @param[in] diskDataIds The disk data.
     */
    void SplitByCacheType(std::vector<std::string> &memoryDataIds, std::vector<std::string> &diskDataIds);

    /**
     * @brief Migrate data for one cache type.
     * @param[in] type The cache type.
     * @param[in] needMigrateDataIds Objects to migrate.
     * @param[in] isSlotMigration Whether this is a slot migration.
     * @return K_OK if success, the error otherwise.
     */
    Status MigrateDataByCacheType(CacheType type, std::vector<std::string> &needMigrateDataIds, bool isSlotMigration);

    /**
     * @brief Prepare remote state before migrating a cache type.
     * @param[in] type The cache type.
     * @param[in] needMigrateDataIds Objects to migrate.
     * @return K_OK if success, the error otherwise.
     */
    Status PrepareRemoteMigration(CacheType type, const std::vector<std::string> &needMigrateDataIds);

    /**
     * @brief Collect one object into the current migrate batch.
     * @param[in] objectKey Object key.
     * @param[in] isSlotMigration Whether this is a slot migration.
     */
    void CollectObjectForMigration(const std::string &objectKey, bool isSlotMigration);

    /**
     * @brief Indicate whether to use fast transport for migration.
     * @return True if fast transport should be used.
     */
    bool ShouldUseFastTransport() const;

    /**
     * @brief Release resources for successfully migrated objects.
     * @param[in] successIds Successfully migrated object ids.
     */
    void ReleaseResources(const std::unordered_set<ImmutableString> &successIds);

    MigrateType type_;
    std::string localAddr_;
    std::unordered_set<ImmutableString> needMigrateDataIds_;
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi_;

    uint64_t maxBatchSize_;
    uint64_t remoteMemoryRemainSize_;
    uint64_t remoteDiskRemainSize_;
    uint64_t currentMemorySize_;
    uint64_t currentDiskSize_;
    uint64_t currBatchSize_;
    uint64_t currBatchCount_;

    DataLimiter limiter_;
    std::shared_ptr<SelectionStrategy> strategy_;
    std::shared_ptr<MigrateProgress> progress_{ nullptr };
    std::shared_ptr<MigrateTransport> transport_;
    bool isRetry_{ false };
    uint32_t slotId_{ 0 };

    std::unordered_set<ImmutableString> successIds_;
    std::unordered_set<ImmutableString> failedIds_;
    std::unordered_set<ImmutableString> skipIds_;
    std::vector<std::unique_ptr<BaseDataUnit>> datas_;

    Status lastRc_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif
