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
 * Description: Migrate data.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_H
#define DATASYSTEM_MIGRATE_DATA_H

#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/data_migrator/handler/migrate_data_handler.h"

namespace datasystem {
namespace object_cache {
class DataMigrator {
public:
    DataMigrator(MigrateType type, EtcdClusterManager *etcdCM, HostPort &localAddress,
                 std::shared_ptr<AkSkManager> manager, std::shared_ptr<ObjectTable> objectTable,
                 const std::string &taskId = "", int maxRetryCount = -1)
        : type_(type),
          etcdCM_(etcdCM),
          localAddress_(localAddress),
          akSkManager_(std::move(manager)),
          objectTable_(std::move(objectTable)),
          taskId_(taskId),
          maxRetryCount_(maxRetryCount)
    {
    }

    ~DataMigrator() = default;

    /**
     * @brief Initialize data migrator.
     */
    void Init();

    /**
     * @brief Migrate objects to remote nodes.
     * @param[in] objectKeys Object keys to migrate.
     * @param[in] objectSizes Object sizes mapping.
     * @return Status of the call.
     */
    Status Migrate(const std::vector<std::string> &objectKeys,
                   const std::unordered_map<std::string, uint64_t> &objectSizes);

    /**
     * @brief Get not migrated object keys.
     * @param[out] failedMigrateObjectKeys The list of object keys that are failed.
     */
    void GetFailedKeys(std::vector<std::string> &failedMigrateObjectKeys)
    {
        failedMigrateObjectKeys.clear();
        failedMigrateObjectKeys.reserve(failedKeys_.size());
        std::copy(failedKeys_.begin(), failedKeys_.end(), std::back_inserter(failedMigrateObjectKeys));
    }

private:
    /**
     * @brief Get migration strategy by type.
     * @return Migration strategy instance.
     */
    std::shared_ptr<SelectionStrategy> GetStrategyByType();

    /**
     * @brief Calculate total size of objects.
     * @param[in] objectKeys Object keys.
     * @param[in] objectSizes Object sizes mapping.
     * @return Total size in bytes.
     */
    uint64_t CalculateTotalSize(const std::unordered_set<ImmutableString> &objectKeys,
                                const std::unordered_map<std::string, uint64_t> &objectSizes);

    /**
     * @brief Migrate data to remote node via addr.
     * @param[in] addr Remote node address.
     * @param[in] objectKeys Need migrate data object keys.
     * @param[in] strategy Migration data strategy instance used to select the target node for data migration.
     * @return Task future.
     */
    std::future<MigrateDataHandler::MigrateResult> MigrateDataByNode(const MetaAddrInfo &addr,
                                                                     const std::vector<std::string> &objectKeys,
                                                                     std::shared_ptr<SelectionStrategy> strategy);

    /**
     * @brief Migrate data to remote node implemetation.
     * @param[in] remoteWorkerStub Remote node rpc stub.
     * @param[in] objectKeys Need migrate data object keys.
     * @param[in] strategy Migration data strategy instance used to select the target node for data migration.
     * @return Task future.
     */
    MigrateDataHandler::MigrateResult MigrateDataByNodeImpl(
        const std::shared_ptr<WorkerRemoteWorkerOCApi> &remoteWorkerStub, const std::vector<std::string> &objectKeys,
        const std::shared_ptr<SelectionStrategy> &strategy);

    /**
     * @brief Checks the connection to the target Worker node and creates a remote Worker API if connected.
     * @param[in,out] remoteWorkerStub Pointer to the remote Worker API.
     * @param[in] workerAddr Address of the target Worker node.
     * @return Status The status of the connection and API creation.
     */
    Status ConnectAndCreateRemoteApi(std::shared_ptr<WorkerRemoteWorkerOCApi> &remoteWorkerStub, HostPort workerAddr);

    /**
     * @brief Handle migration failure result.
     * @return Status of the call.
     */
    Status HandleFailedResult();

    /**
     * @brief Handle migrate data future results.
     * @param[in] objectSizes Object sizes mapping.
     * @param[in,out] futures Migrate data futures.
     * @param[in,out] newFutures New added migrate data futures.
     * @return Status of the call.
     */
    Status HandleMigrateDataResult(const std::unordered_map<std::string, uint64_t> &objectSizes,
                                   std::vector<std::future<MigrateDataHandler::MigrateResult>> &futures,
                                   std::vector<std::future<MigrateDataHandler::MigrateResult>> &newFutures);

    /**
     * @brief Redirect migration data to other node.
     * @param[in] result Failed migration result.
     * @param[in] totalSize Total data size.
     * @return New migration task future.
     */
    std::future<MigrateDataHandler::MigrateResult> RedirectMigrateData(MigrateDataHandler::MigrateResult &result,
                                                                       uint64_t totalSize);

    /**
     * @brief Construct failed migrate result.
     * @param[in] workerAddr Worker address.
     * @param[in] status Status of the migration.
     * @param[in] objectKeys Failed object list.
     * @param[in] strategy Migration data strategy instance used to select the target node for data migration.
     * @return Migrate data result future.
     */
    std::future<MigrateDataHandler::MigrateResult> ConstructFailedFuture(const std::string &workerAddr,
                                                                         const Status &status,
                                                                         const std::vector<std::string> &objectKeys,
                                                                         std::shared_ptr<SelectionStrategy> &strategy);

    MigrateType type_;
    EtcdClusterManager *etcdCM_{ nullptr };
    HostPort &localAddress_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    std::shared_ptr<ObjectTable> objectTable_;
    std::string taskId_;
    int maxRetryCount_ = -1;

    std::unique_ptr<ThreadPool> threadPool_;

    std::shared_ptr<SelectionStrategy> strategy_;
    std::unordered_set<ImmutableString> failedKeys_;
    std::unordered_set<ImmutableString> skippedKeys_;
    std::shared_ptr<MigrateProgress> progress_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif