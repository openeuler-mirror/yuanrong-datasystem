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
 * Description: Public API for KVCache Worker.
 *              This header has zero internal dependencies.
 */
#ifndef DATASYSTEM_DATA_WORKER_H
#define DATASYSTEM_DATA_WORKER_H

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "datasystem/utils/embedded_config.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class DynamicFlagConfig;

namespace worker {
class WorkerOCServer;
}

struct DataWorkerOptions {
    // Absolute path to worker_config.json file.
    std::string configFilePath;
};

class DataWorker {
public:
    ~DataWorker();

    DataWorker& operator=(const DataWorker&) = delete;
    DataWorker(const DataWorker&) = delete;

    /// @brief Get the singleton instance.
    static DataWorker *GetInstance();

    /// @brief Process-mode startup (command-line arguments).
    /// @details Internally parses argc/argv, blocks until termination signal or Stop() is called.
    /// @param argc Argument count.
    /// @param argv Argument vector.
    /// @return K_OK on normal exit; error code otherwise.
    Status InitAndRun(int argc, char **argv);

    /// @brief Process-mode startup (config file).
    /// @details Reads and parses the JSON config file, blocks until termination signal or Stop() is called.
    /// @param options Startup options containing the config file path.
    /// @return K_OK on normal exit; error code otherwise.
    Status InitAndRun(const DataWorkerOptions &options);

    /// @brief Embedded-mode initialization.
    /// @details Initializes without blocking. Caller manages lifecycle.
    /// @param config Embedded configuration, depends only on public type EmbeddedConfig.
    /// @return K_OK on success; error code otherwise.
    Status InitEmbeddedWorker(const EmbeddedConfig &config);

    /// @brief Request shutdown of a running InitAndRun() event loop.
    /// @details Thread-safe, idempotent. Sets exit flag and wakes the event loop;
    ///          actual PreShutDown/ShutDown is performed by InitAndRun after the loop exits.
    /// @return K_OK
    Status Stop();

    /// @brief Stop an embedded-mode Worker.
    /// @details Executes PreShutDown -> ShutDown directly. Idempotent.
    ///          Called by extern "C" WorkerDestroy for embedded lifecycle management.
    /// @return K_OK
    Status StopEmbeddedWorker();

    /// @brief Apply runtime JSON config updates to modifiable worker flags.
    /// @param configJson JSON object mapping flag names to string values.
    /// @return Status::OK() on success; error status otherwise.
    Status UpdateConfig(const std::string &configJson);

private:
    /// @brief Initialize WorkerOCServer and start all services.
    Status InitWorker(DynamicFlagConfig &flags, bool isEmbeddedClient);

    /// @brief Shutdown Worker.
    Status ShutDown();

    /// @brief Pre-shutdown (wait for async tasks to complete).
    Status PreShutDown();

    /// @brief Run the blocking event loop, then perform PreShutDown and ShutDown.
    void RunEventLoopAndShutdown(DynamicFlagConfig &flags);

    /// @brief InitWorker + Register + signal handler. Caller must hold initMutex_.
    Status DoInit(DynamicFlagConfig &flags, const char *crashReporterLabel);

    DataWorker() = default;
    std::unique_ptr<worker::WorkerOCServer> worker_{nullptr};
    std::atomic<bool> started_{false};
    std::mutex initMutex_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_DATA_WORKER_H
