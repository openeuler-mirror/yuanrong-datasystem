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
 * Description: TCP migrate transport.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_TCP_TRANSPORT_H
#define DATASYSTEM_MIGRATE_DATA_TCP_TRANSPORT_H

#include "datasystem/worker/object_cache/data_migrator/basic/migrate_progress.h"
#include "datasystem/worker/object_cache/data_migrator/transport/migrate_transport.h"

namespace datasystem {
namespace object_cache {
class TcpMigrateTransport : public MigrateTransport {
public:
    TcpMigrateTransport() = default;

    ~TcpMigrateTransport() override = default;

    /**
     * @brief Migrate data to remote node.
     * @param[in] api Remote worker api.
     * @param[in] datas Data units to migrate.
     * @param[in] localAddr Local node address.
     * @param[in] batchSize Batch size for migration.
     * @param[in] progress Migration progress tracker.
     * @param[out] remainBytes Remaining bytes of remote worker.
     * @param[out] successKeys Keys migrated successfully.
     * @param[out] failedKeys Keys failed to migrate.
     * @param[out] limitRate Current migration rate limit.
     * @return Status of the call.
     */
    Status MigrateDataToRemote(const std::shared_ptr<WorkerRemoteWorkerOCApi> &api,
                               const std::vector<std::unique_ptr<BaseDataUnit>> &datas, const std::string &localAddr,
                               const uint64_t &batchSize, std::shared_ptr<MigrateProgress> progress,
                               uint64_t &remainBytes, std::unordered_set<ImmutableString> &successKeys,
                               std::unordered_set<ImmutableString> &failedKeys, uint64_t &limitRate) override;
};

}  // namespace object_cache
}  // namespace datasystem

#endif