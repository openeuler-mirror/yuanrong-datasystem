// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Description: Non-singleton Coordinator runtime lifecycle owner.
 */
#ifndef DATASYSTEM_COORDINATOR_COORDINATOR_RUNTIME_H
#define DATASYSTEM_COORDINATOR_COORDINATOR_RUNTIME_H

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "datasystem/coordinator_server.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"

namespace datasystem {
class DynamicConfigUpdater;
class DynamicFlagConfig;

namespace coordinator {
class CoordinatorServiceImpl;
}  // namespace coordinator

class CoordinatorRuntime {
public:
    CoordinatorRuntime();
    virtual ~CoordinatorRuntime() noexcept;

    CoordinatorRuntime(const CoordinatorRuntime &) = delete;
    CoordinatorRuntime &operator=(const CoordinatorRuntime &) = delete;
    CoordinatorRuntime(CoordinatorRuntime &&) = delete;
    CoordinatorRuntime &operator=(CoordinatorRuntime &&) = delete;

    /**
     * @brief Run the dscli-compatible Coordinator lifecycle. Empty coordinator_raft_initial_peers keeps single-node
     *        no-election mode; non-empty static peers can enable election. A Runtime instance is one-shot; callers must
     *        not invoke InitAndRun more than once on the same instance.
     */
    Status InitAndRun();

    /**
     * @brief Run one election-enabled Coordinator lifecycle. A non-empty configFilePath is parsed before startup; an
     * empty path skips file parsing and uses the process flags already prepared by the caller. Direct empty-path
     * startup is limited to internal in-process tests; production uses CoordinatorServer. A Runtime instance is
     * one-shot; callers must create a new instance to retry after any return.
     */
    Status InitAndRun(const CoordinatorOptions &options);

    /**
     * @brief Synchronously update Coordinator log-sampler rates while this Runtime is running.
     * @details Only request_sample_rate, access_sample_rate, and diagnostic_sample_rate are runtime-applicable. Calls
     *          are serialized with Stop(). Production supports one Coordinator Runtime per process because the
     *          underlying flag values are process-global.
     */
    Status UpdateConfig(const std::string &configJson);

    /**
     * @brief Request this Runtime's event loop to stop without changing the process termination flag.
     */
    Status Stop();

    bool IsLeader() const;
    Status GetLeader(std::string &leaderAddress) const;

protected:
    virtual coordinator::CoordinatorRaftFlags GetRaftFlags() const;

private:
    enum class LifecycleCallbackState : uint8_t { NOT_CONFIGURED, READY, START_ATTEMPTED, STOP_INVOKED };
    enum class ConfigState : uint8_t { NOT_READY, READY, STOPPING };

    Status InitAndRunInternal(const CoordinatorOptions *options);
    Status InitWatchDispatcherBthreadPool();
    Status InvokeOnStart();
    Status InvokeOnStop();
    Status ShutdownService();
    void EnableConfigUpdates();
    void DisableConfigUpdates();
    void RunEventLoop();

    mutable std::mutex mutex_;
    std::condition_variable stopCv_;
    bool stopRequested_{ false };
    std::unique_ptr<coordinator::CoordinatorServiceImpl> service_;
    std::function<Status()> onStart_;
    std::function<Status()> onStop_;
    LifecycleCallbackState callbackState_{ LifecycleCallbackState::NOT_CONFIGURED };
    mutable std::mutex configMutex_;  // Acquired after mutex_ when both lifecycle domains are needed.
    std::unique_ptr<DynamicFlagConfig> runtimeFlags_;
    std::unique_ptr<DynamicConfigUpdater> configUpdater_;
    ConfigState configState_{ ConfigState::NOT_READY };
    int watchDispatcherBthreadTag_{ 0 };
};

}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_COORDINATOR_RUNTIME_H
