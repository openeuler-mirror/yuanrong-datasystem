/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Define api of perf client for performance test scenario.
 */
#ifndef DATASYSTEM_PERF_CLIENT_H
#define DATASYSTEM_PERF_CLIENT_H

#include <unordered_map>

#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class PerfClientWorkerApi;
class __attribute((visibility("default"))) PerfClient {
public:
    /**
     * @brief Construct PerfClient.
     * @param[in] connectOptions The connection options.
     */
    explicit PerfClient(const ConnectOptions &connectOptions = {});

    ~PerfClient() = default;

    Status Init();

    /**
     * @brief Get the client perf log list.
     * @param[in] type Select the client or worker to get the perf log. Optional Value: "client", "worker".
     * @param[out] perfLog The performance information. The performance information. Each element of the map
     * represent one perf point for worker/master, and each perf point contains the these metrics data: "count",
     * "min_time", "max_time", "total_time", "avg_time", "avg_time", "max_frequency".
     * @return Status of the call.
     */
    Status GetPerfLog(const std::string &type,
                      std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> &perfLog);

    /**
     * @brief Reset the perf log.
     * @param[in] type Select the client or worker to reset the perf log. Optional Value: "client", "worker".
     * @return Status of the call.
     */
    Status ResetPerfLog(const std::string &type);

private:
    /**
     * @brief Check type is valid.
     * @param[in] type Select the client or worker to get the perf log. Optional Value: "client", "worker".
     * @return Status of the call.
     */
    Status CheckTypeParam(const std::string &type);

    std::shared_ptr<PerfClientWorkerApi> clientWorkerApi_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_PERF_CLIENT_H