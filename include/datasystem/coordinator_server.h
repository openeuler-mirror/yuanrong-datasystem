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
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace coordinator {

class CoordinatorServiceImpl;

}  // namespace coordinator

struct CoordinatorOptions {
    // Absolute path to coordinator_config.json file.
    std::string configFilePath;
    // Required candidate provider for parameterized startup.
    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery;
    // Target voting member count for the Coordinator Raft group.
    int expectedMemberCount = 0;
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
    /// @param options Startup options containing the config file path.
    /// @return K_OK on normal exit; error code otherwise.
    Status InitAndRun(const CoordinatorOptions &options);

    /// @brief Request shutdown of a running InitAndRun().
    /// @return K_OK
    Status Stop();

private:
    enum class LifecycleCallbackState : uint8_t { NOT_CONFIGURED, READY, START_ATTEMPTED, STOP_INVOKED };

    CoordinatorServer() = default;

    /// @brief Validate and run one configured Coordinator lifecycle.
    Status InitAndRunInternal(const CoordinatorOptions *options);
    /// @brief Initialize environment and delegate to service_->Init().
    Status Init();
    /// @brief Delegate to service_->Start().
    Status Start();
    /// @brief Invoke the configured start callback once.
    /// @note Called by the owning InitAndRun lifecycle thread. callbackState_ is not independently synchronized.
    Status InvokeOnStart();
    /// @brief Invoke the configured stop callback once after any start attempt.
    /// @note Must be serialized with InvokeOnStart. Destruction may call it only after lifecycle execution has stopped.
    Status InvokeOnStop();
    /// @brief Delegate to service_->Shutdown() and reset resources.
    Status Shutdown();
    /// @brief Blocking event loop until termination signal or Stop().
    void RunEventLoop();

    std::unique_ptr<coordinator::CoordinatorServiceImpl> service_;
    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery_;
    std::function<Status()> onStart_;
    std::function<Status()> onStop_;
    int expectedMemberCount_{ 0 };
    LifecycleCallbackState callbackState_{ LifecycleCallbackState::NOT_CONFIGURED };
    std::atomic<bool> isStarted_{ false };
    std::mutex initMutex_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_SERVER_H
