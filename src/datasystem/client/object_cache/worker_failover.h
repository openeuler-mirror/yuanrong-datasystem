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
 * Description: Defines the worker failover split of the object client.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_FAILOVER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_FAILOVER_H

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/log.h"

#include <bthread/mutex.h>

namespace datasystem {
namespace object_cache {

struct ObjectClientImpl::ShmRecoveryState {
    enum class Stage : uint8_t {
        IDLE = 0,
        CLEANUP_REQUIRED,
        REGISTER_REQUIRED,
        REBUILD_REQUIRED,
    };

    // Serialize cleanup, registration and mmap rebuild without blocking a bthread worker.
    bthread::Mutex mutex;
    Stage stage{ Stage::IDLE };
};


class WorkerFailover {
public:
    explicit WorkerFailover(ObjectClientImpl &owner);

    ~WorkerFailover() = default;

    // WorkerNode comes from client_mode_types.h; the remaining failover-domain types
    // (WorkerSwitchState/StandbySwitchAttemptResult/ShmRecoveryState) stay nested in
    // ObjectClientImpl and are re-exposed for the migrated method set.
    using WorkerSwitchState = ObjectClientImpl::WorkerSwitchState;
    using StandbySwitchAttemptResult = ObjectClientImpl::StandbySwitchAttemptResult;
    using ShmRecoveryState = ObjectClientImpl::ShmRecoveryState;

    void ConfigureUrmaDataPlaneFailureCallback(WorkerNode node, const std::shared_ptr<IClientWorkerApi> &workerApi);
    bool SubmitUrmaDataPlaneSwitch(WorkerNode node, std::weak_ptr<client::IClientWorkerCommonApi> weakWorkerApi);
    bool SubmitUnavailableWorkerSwitch(const std::shared_ptr<IClientWorkerApi> &workerApi);
    void DrainAsyncSwitchWorkerPool();
    bool IsCurrentWorkerSwitchTrigger(WorkerNode node,
                                      const std::shared_ptr<client::IClientWorkerCommonApi> &workerApi);
    Status ProcessWorkerLost(client::WorkerRecoveryReason reason);
    Status RegisterWorkerAfterWorkerLost(client::WorkerRecoveryReason reason);
    Status RebuildWorkerShm();
    void CleanupWorkerShmAfterWorkerLost();
    void ProcessWorkerTimeout();
    Status ProcessStandbyWorkerLost(WorkerNode node, client::WorkerRecoveryReason reason);
    WorkerNode GetNextWorkerNode(WorkerNode current);
    void StopStandbyWorkerListen(WorkerNode id);
    void MarkWorkerAvailableLocked();
    void MarkNoSwitchableWorkerLocked();
    Status NoSwitchableWorkerStatus() const;
    bool SwitchWorkerNode(WorkerNode node, client::SwitchTriggerReason reason);
    bool SwitchToStandbyWorkerImpl(const std::shared_ptr<IClientWorkerApi> &currentApi, WorkerNode current,
                                   WorkerNode next, uint64_t switchGeneration, client::SwitchTriggerReason reason);
    StandbySwitchAttemptResult TrySwitchToCandidateList(const std::shared_ptr<IClientWorkerApi> &currentApi,
                                                        WorkerNode current, WorkerNode next,
                                                        uint64_t switchGeneration,
                                                        const std::vector<HostPort> &candidates, bool isSameHost);
    void GetStandbyWorkersForSwitch(const std::shared_ptr<IClientWorkerApi> &currentApi,
                                    std::vector<HostPort> &sameHost, std::vector<HostPort> &others) const;
    bool CommitStandbySwitch(WorkerNode current, WorkerNode next, uint64_t switchGeneration,
                             const std::shared_ptr<IClientWorkerApi> &candidateWorkerApi,
                             const std::shared_ptr<client::ListenWorker> &candidateListenWorker);
    StandbySwitchAttemptResult TrySwitchToStandbyWorker(const std::shared_ptr<IClientWorkerApi> &currentApi,
                                                        WorkerNode current, WorkerNode next, uint64_t switchGeneration,
                                                        const HostPort &standbyWorker);
    StandbySwitchAttemptResult TrySwitchToLocalSameHost(WorkerNode current, uint64_t switchGeneration,
                                                        const HostPort &localAddress);
    void MarkNoSwitchableWorkerIfNeeded(WorkerNode current, uint64_t switchGeneration);
    void RestoreWorkerAvailableIfNeeded(WorkerNode current, uint64_t switchGeneration);
    void ReplacePreferredLocalWorkerLocked(std::unique_ptr<client::MmapManager> &localMmapManager,
                                           std::shared_ptr<client::ListenWorker> &oldLocalListener,
                                           std::unique_ptr<client::MmapManager> &oldMmapManager);
    bool TrySwitchBackToLocalWorker();
    bool GetPreferredLocalWorkerToRecover(WorkerNode &oldNode, HostPort &localAddress, HeartbeatType &heartbeatType);
    Status PreparePreferredLocalWorker(const HostPort &localAddress, HeartbeatType heartbeatType,
                                       std::shared_ptr<ClientWorkerRemoteApi> &localWorkerApi,
                                       std::unique_ptr<client::MmapManager> &localMmapManager,
                                       std::shared_ptr<client::ListenWorker> &localListenWorker);
    bool CommitPreferredLocalWorker(WorkerNode oldNode, const HostPort &localAddress,
                                    const std::shared_ptr<ClientWorkerRemoteApi> &localWorkerApi,
                                    std::unique_ptr<client::MmapManager> localMmapManager,
                                    const std::shared_ptr<client::ListenWorker> &localListenWorker);
    bool RecoverPreferredLocalWorker();
    bool ReadyToExit(WorkerNode node, const std::shared_ptr<IClientWorkerApi> &workerApi,
                     const std::shared_ptr<client::ListenWorker> &listenWorker);
    bool WaitStandbyWorkerReady(const std::shared_ptr<IClientWorkerApi> &clientWorkerApi);

private:
    // Callbacks installed on owner_'s listener/workerApi capture `this`; safety relies on
    // ObjectClientImpl::ShutDown draining async switches first and clearing the URMA
    // data-plane callbacks on workerApi_ before any member destruction.
    ObjectClientImpl &owner_;
};

// Shared by ObjectClientImpl::PickFallbackWorker (init path) and GetStandbyWorkersForSwitch (switch path).
void ShuffleWorkerCandidates(std::vector<HostPort> &candidates);

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_FAILOVER_H
