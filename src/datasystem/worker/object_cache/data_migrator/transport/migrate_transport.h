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
 * Description: Migrate transport.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_MIGRATE_TRANSPORT_H
#define DATASYSTEM_MIGRATE_DATA_MIGRATE_TRANSPORT_H

#include <cstdint>
#include "datasystem/worker/object_cache/data_migrator/basic/base_data_unit.h"
#include "datasystem/worker/object_cache/data_migrator/basic/migrate_progress.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class MigrateTransport {
public:
    MigrateTransport() = default;

    virtual ~MigrateTransport() = default;

    struct Request {
        MigrateType type;
        std::shared_ptr<WorkerRemoteWorkerOCApi> api;
        const std::vector<std::unique_ptr<BaseDataUnit>> *datas{nullptr};
        std::string localAddr;
        uint64_t batchSize{0};
        std::shared_ptr<MigrateProgress> progress{nullptr};
        bool isSlotMigration{false};
        bool isRetry{false};
        uint32_t slotId{0};
    };

    struct Response {
        uint64_t remainBytes{ UINT64_MAX };  // UINT64_MAX means the field is not set.
        std::unordered_set<ImmutableString> successKeys;
        std::unordered_set<ImmutableString> failedKeys;
        uint64_t limitRate{ 0 };
    };

    /**
     * @brief Migrate data to remote node.
     * @param[in] req The Requestparameters for migration.
     * @param[out] rsp The Response of migration.
     * @return Status of the call.
     */
    virtual Status MigrateDataToRemote(const Request &req, Response &rsp) = 0;

protected:
    int64_t CalcMigrateDataDirectTimeoutMs(uint64_t totalDataBytes)
    {
        constexpr int64_t maxTimeoutMs = 180'000;
        constexpr int64_t minTimeoutMs = 60'000;
        constexpr int64_t addTimeoutMs = 5'000;
        constexpr long double bandwidthBytesPerSecond = 10.0L * 1024.0L * 1024.0L * 1024.0L;

        if (totalDataBytes == 0) {
            return minTimeoutMs;
        }

        int64_t transferMs = static_cast<int64_t>(
            std::ceil(static_cast<long double>(totalDataBytes) * SECS_TO_MS / bandwidthBytesPerSecond));
        int64_t timeoutMs = transferMs + addTimeoutMs;
        return std::clamp(timeoutMs, minTimeoutMs, maxTimeoutMs);
    }

    const int maxRetryCount_ = 3;
};

}  // namespace object_cache
}  // namespace datasystem

#endif
