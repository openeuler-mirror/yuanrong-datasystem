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
 * Description: Public API for Coordinator Server.
 */
#ifndef DATASYSTEM_COORDINATOR_SERVER_H
#define DATASYSTEM_COORDINATOR_SERVER_H

#include <functional>
#include <memory>
#include <string>

#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class CoordinatorRuntime;

struct CoordinatorOptions {
    // Absolute path to coordinator_config.json file.
    std::string configFilePath;
    // Required candidate provider for parameterized startup.
    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery;
    // Target coordinator member count, enable raft election if greater than 1.
    int expectedMemberCount = 1;
    // Optional lifecycle callbacks. Both must be configured together or both left empty.
    std::function<Status()> onStart;
    std::function<Status()> onStop;
};

class CoordinatorServer {
public:
    ~CoordinatorServer();

    CoordinatorServer(const CoordinatorServer &) = delete;
    CoordinatorServer &operator=(const CoordinatorServer &) = delete;

    /// @brief Get the singleton instance.
    static CoordinatorServer *GetInstance();

    /// @brief Coordinator server startup, blocks until termination signal or Stop() is called.
    /// @return K_OK on normal shutdown; error code otherwise.
    Status InitAndRun();

    /// @brief Coordinator server startup.
    /// @details Reads and parses the JSON config file, blocks until termination signal or Stop() is called.
    /// @param options Startup options containing the config file path and election membership inputs.
    /// @return K_OK on normal exit; error code otherwise.
    Status InitAndRun(const CoordinatorOptions &options);

    /// @brief Update runtime-applicable Coordinator log-sampler rates from a JSON object.
    /// @details Supported keys are request_sample_rate, access_sample_rate, and diagnostic_sample_rate; values must be
    ///          strings. The update is synchronous and is rejected before startup is ready or after shutdown begins.
    /// @param configJson JSON object containing flag name/value pairs.
    /// @return K_OK on success; K_NOT_READY or K_SHUTTING_DOWN for lifecycle rejection; error code otherwise.
    Status UpdateConfig(const std::string &configJson);

    /// @brief Request shutdown of a running InitAndRun().
    /// @return K_OK
    Status Stop();

private:
    CoordinatorServer();

    std::unique_ptr<CoordinatorRuntime> runtime_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_SERVER_H
