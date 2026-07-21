/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker business adapter for cluster topology phase callbacks.
 */
#ifndef DATASYSTEM_WORKER_WORKER_TOPOLOGY_PHASE_CALLBACKS_H
#define DATASYSTEM_WORKER_WORKER_TOPOLOGY_PHASE_CALLBACKS_H

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "datasystem/cluster/executor/topology_phase_callbacks.h"

namespace datasystem {

class MetadataManagerHolder;

namespace master {
class OCMetadataManager;
class SCMetadataManager;
}  // namespace master

namespace worker {

using TopologyReadinessCheck =
    std::function<Status(std::chrono::steady_clock::time_point, const cluster::CancellationToken &)>;

/**
 * @brief Inject object-cache actions triggered by Worker topology callbacks.
 */
class IWorkerTopologyObjectCacheActions {
public:
    /**
     * @brief Destroy the injected action interface.
     */
    virtual ~IWorkerTopologyObjectCacheActions() = default;

    /**
     * @brief Drain local object-cache data for one ScaleIn batch.
     * @param[in] context Fenced callback context.
     * @return Drain status.
     */
    virtual Status DrainScaleInData(const cluster::TopologyCallbackContext &context) = 0;

    /**
     * @brief Prepare object-cache ScaleIn cleanup authorization and effect.
     * @param[in] context Fenced callback context.
     * @param[out] prepared Executor-owned cleanup authorization and effect.
     * @return Preparation status.
     */
    virtual Status PrepareScaleInCleanup(const cluster::TopologyCallbackContext &context,
                                         std::unique_ptr<cluster::TopologyPreparedCleanup> &prepared) = 0;

    /**
     * @brief Run object-cache local data cleanup for one Failure callback.
     * @param[in] context Fenced callback context.
     * @return Cleanup status.
     */
    virtual Status CleanupLocalData(const cluster::TopologyCallbackContext &context) = 0;
};

/**
 * @brief Complete Worker dependencies required by topology phase callbacks.
 */
struct WorkerTopologyPhaseCallbackDependencies {
    bool centralizedMetadata;
    bool localMetadataMaster;
    bool streamMetadataEnabled;
    MetadataManagerHolder &metadataManagers;
    TopologyReadinessCheck readinessCheck;
    std::shared_ptr<IWorkerTopologyObjectCacheActions> objectCacheActions;
};

/**
 * @brief Adapt business-neutral topology phases to existing metadata and object-cache workflows.
 */
class WorkerTopologyPhaseCallbacks final : public cluster::ITopologyPhaseCallbacks {
public:
    /**
     * @brief Construct the Worker topology callback adapter.
     * @param[in] dependencies Complete callback dependencies and deployment-mode facts.
     */
    explicit WorkerTopologyPhaseCallbacks(WorkerTopologyPhaseCallbackDependencies dependencies);

    /**
     * @brief Destroy the callback adapter after the topology executor is stopped.
     */
    ~WorkerTopologyPhaseCallbacks() override = default;

    /**
     * @brief Migrate metadata for one ScaleOut task scope.
     * @param[in] context Fenced callback context.
     * @return Callback status.
     */
    Status OnScaleOut(const cluster::TopologyCallbackContext &context) override;

    /**
     * @brief Migrate metadata for one ScaleIn task scope.
     * @param[in] context Fenced callback context.
     * @return Callback status.
     */
    Status OnScaleIn(const cluster::TopologyCallbackContext &context) override;

    /**
     * @brief Drain local data for one ScaleIn task after the source metadata gate is complete.
     * @param[in] context Fenced callback context.
     * @return Callback status.
     */
    Status OnScaleInDataDrain(const cluster::TopologyCallbackContext &context) override;

    /**
     * @brief Prepare ScaleIn cleanup authorization and an idempotent bounded effect after migration succeeds.
     * @param[in] context Fenced callback context.
     * @param[out] prepared Executor-owned cleanup authorization and effect.
     * @return Preparation status.
     */
    Status PrepareScaleInCleanup(const cluster::TopologyCallbackContext &context,
                                 std::unique_ptr<cluster::TopologyPreparedCleanup> &prepared) override;

    /**
     * @brief Attempt bounded Failure recovery and cleanup without retrying ordinary business failures.
     * @param[in] context Fenced callback context.
     * @return First observed business error, or K_OK when all steps succeed.
     */
    Status OnFailure(const cluster::TopologyCallbackContext &context) override;

private:
    /**
     * @brief Process-local coalescing state for one member-wide ScaleIn data drain.
     */
    struct ScaleInDrainState {
        std::string sourceId;
        uint64_t batchEpoch{ 0 };
        bool running{ false };
        bool complete{ false };
    };

    /**
     * @brief Reject cancelled or expired callback contexts before business effects.
     * @param[in] context Fenced callback context.
     * @return Context validation status.
     */
    static Status CheckContext(const cluster::TopologyCallbackContext &context);

    /**
     * @brief Migrate object and optional stream metadata for one ordinary task.
     * @param[in] context Fenced callback context.
     * @return Migration status.
     */
    Status MigrateMetadata(const cluster::TopologyCallbackContext &context);

    /**
     * @brief Run or join the member-wide local data drain for one ScaleIn batch.
     * @param[in] context Fenced callback context.
     * @return Drain status or a bounded wait error.
     */
    Status DrainScaleInData(const cluster::TopologyCallbackContext &context);

    /**
     * @brief Acquire the member-wide drain leadership or observe a completed drain.
     * @param[in] context Fenced callback context.
     * @param[out] leader True when this callback must perform the drain.
     * @return K_OK on leadership/completion; a bounded wait error otherwise.
     */
    Status AcquireScaleInDrain(const cluster::TopologyCallbackContext &context, bool &leader);

    /**
     * @brief Publish one member-wide drain attempt result and wake peer task callbacks.
     * @param[in] context Callback context that owned the attempt.
     * @param[in] status Drain attempt result.
     */
    void CompleteScaleInDrain(const cluster::TopologyCallbackContext &context, const Status &status);

    /**
     * @brief Retain and log the first best-effort Failure step error.
     * @param[in] step Stable diagnostic step name.
     * @param[in] status Step status.
     * @param[in,out] firstError First observed error.
     */
    void RecordFailureStep(const std::string &step, const Status &status, Status &firstError) const;

    /**
     * @brief Run one complete bounded Failure attempt.
     * @param[in] context Fenced callback context.
     * @return First observed error, or K_OK.
     */
    Status RunFailureBestEffort(const cluster::TopologyCallbackContext &context);

    /**
     * @brief Run cleanup stages after metadata recovery has been attempted.
     * @param[in] context Fenced callback context.
     * @param[in] ocMetadata Optional local object metadata manager.
     * @param[in] scMetadata Optional local stream metadata manager.
     * @param[in,out] firstError First observed error.
     */
    void RunFailureCleanup(const cluster::TopologyCallbackContext &context,
                           const std::shared_ptr<master::OCMetadataManager> &ocMetadata,
                           const std::shared_ptr<master::SCMetadataManager> &scMetadata, Status &firstError);

    const bool centralizedMetadata_;
    const bool localMetadataMaster_;
    const bool streamMetadataEnabled_;
    MetadataManagerHolder &metadataManagers_;
    TopologyReadinessCheck readinessCheck_;
    std::shared_ptr<IWorkerTopologyObjectCacheActions> objectCacheActions_;
    // Protects scaleInDrainState_; peer ScaleIn task callbacks wait on scaleInDrainChanged_.
    std::mutex scaleInDrainMutex_;
    std::condition_variable scaleInDrainChanged_;
    ScaleInDrainState scaleInDrainState_;
};

}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_WORKER_TOPOLOGY_PHASE_CALLBACKS_H
