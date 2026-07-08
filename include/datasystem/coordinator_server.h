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

#include <atomic>
#include <memory>
#include <mutex>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace coordinator {

class CoordinatorServiceImpl;

}  // namespace coordinator

struct CoordinatorOptions {
    // Absolute path to coordinator_config.json file.
    std::string configFilePath;
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
    /// @param options Startup options containing the config file path.
    /// @return K_OK on normal exit; error code otherwise.
    Status InitAndRun(const CoordinatorOptions &options);

    /// @brief Request shutdown of a running InitAndRun().
    /// @return K_OK
    Status Stop();

private:
    CoordinatorServer() = default;

    /// @brief Initialize environment and delegate to service_->Init().
    Status Init();
    /// @brief Delegate to service_->Start().
    Status Start();
    /// @brief Delegate to service_->Shutdown() and reset resources.
    Status Shutdown();
    /// @brief Blocking event loop until termination signal or Stop().
    void RunEventLoop();

    std::unique_ptr<coordinator::CoordinatorServiceImpl> service_;
    std::atomic<bool> isStarted_{false};
    std::mutex initMutex_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_SERVER_H
