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
 * Description: The interface of worker server.
 */
#ifndef DATASYSTEM_WORKER_H
#define DATASYSTEM_WORKER_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/utils/embedded_config.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/worker_oc_server.h"
#include "datasystem/worker/worker_service_impl.h"

namespace datasystem {
namespace worker {

class Worker {
public:
    ~Worker();

    Worker& operator=(const Worker&) = delete;

    Worker(const Worker&) = delete;

    /**
     * @brief Init worker for process mode
     */
    Status Init(Flags &flags, int argc, char **argv);

    /**
     * @brief Get worker instance.
     */
    static Worker *GetInstance();

    /**
     * @brief Init worker for embedded client.
     */
    Status InitEmbeddedWorker(const EmbeddedConfig &config);

    /**
     * @brief Worker shutdown.
     */
    Status ShutDown();

    /**
     * @brief Worker pre-shutdown.
     */
    Status PreShutDown();

    /**
     * @brief GetWorkerService ptr
     */
    WorkerServiceImpl *GetWorkerService();

    /**
     * @brief GetWorkerOCService ptr
     */
    object_cache::WorkerOCServiceImpl *GetWorkerOCService();

    /**
     * @brief Apply runtime JSON config updates to modifiable worker flags.
     * @param[in] configJson JSON object mapping flag names to string values.
     * @return Status::OK() on success; error status otherwise.
     * @note Requires Init/InitEmbeddedWorker to have completed. The caller-owned Flags passed to Init must
     *       outlive this Worker instance; UpdateConfig is only valid between Init and ShutDown.
     */
    Status UpdateConfig(const std::string &configJson);

private:
    Status InitWorker(Flags &flags, const GFlagsMap &defaultGflagMap, const bool isEmbeddedClient);

    Worker() = default;

    std::unique_ptr<WorkerOCServer> worker_{ nullptr };
    /** Non-owning pointer to caller Flags; valid only between InitWorker and ShutDown. */
    Flags *runtimeFlags_{ nullptr };
};
}  // namespace worker

/**
 * @brief The signal handler
 * @param[in] signum The signal number.
 */
void SignalHandler(int signum);
}  // namespace datasystem
#endif