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
#include "datasystem/worker/worker_topology_phase_callbacks.h"

#include <algorithm>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/metadata_manager_holder.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

namespace datasystem::worker {
namespace {
constexpr auto SCALE_IN_DRAIN_WAIT_POLL = std::chrono::milliseconds(100);

/**
 * @brief Calculate one callback's remaining monotonic budget.
 * @param[in] deadline Absolute callback deadline.
 * @return Remaining budget in microseconds; non-positive when exhausted.
 */
int64_t RemainingCallbackBudgetUs(std::chrono::steady_clock::time_point deadline)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(deadline - std::chrono::steady_clock::now()).count();
}


/**
 * @brief Remove cached worker<->worker RPC stubs pointing at a failed member.
 *
 * After node_dead_timeout_s the topology controller confirms a failed member and runs
 * OnFailure on every surviving worker. Each survivor's RpcStubCacheMgr may still hold
 * WORKER_WORKER_OC_SVC / WORKER_WORKER_SC_SVC / WORKER_WORKER_TRANS_SVC stubs to the dead
 * peer; without removal they keep reconnecting and retransmitting TCP SYNs (Issue #766).
 * brpc Channel and ZMQ socket destruction are both driven by RpcStubCacheMgr::Remove.
 *
 * @param[in] action Topology callback facts carrying the failed member identity.
 * @return K_OK on success; K_INVALID if the failed address is absent/unparseable;
 *         the first non K_NOT_FOUND error from any Remove otherwise. K_NOT_FOUND is
 *         benign (no cached stub in that direction) and is reported as K_OK.
 */
Status EraseFailedWorkerWorkerStub(const cluster::TopologyPhaseAction &action)
{
    if (!action.failed.has_value() || action.failed->address.empty()) {
        return Status(K_INVALID, "failure cleanup lacks a failed member address");
    }
    HostPort failedAddr;
    auto rc = failedAddr.ParseString(action.failed->address);
    if (rc.IsError() || failedAddr.Empty()) {
        return Status(K_INVALID, "failed member address is invalid: " + action.failed->address);
    }
    for (auto type : { StubType::WORKER_WORKER_OC_SVC, StubType::WORKER_WORKER_SC_SVC,
                       StubType::WORKER_WORKER_TRANS_SVC }) {
        auto removeRc = RpcStubCacheMgr::Instance().Remove(failedAddr, type);
        // K_NOT_FOUND: no cached stub for this worker<->worker direction; not an error.
        if (removeRc.IsError() && removeRc.GetCode() != StatusCode::K_NOT_FOUND) {
            LOG(ERROR) << "Remove cached worker<->worker stub failed, address=" << action.failed->address
                       << ", type=" << static_cast<int>(type) << ", rc=" << removeRc.ToString();
            if (rc.IsOk()) {  // preserve first real error
                rc = removeRc;
            }
        }
    }
    return rc;
}
}  // namespace

WorkerTopologyPhaseCallbacks::WorkerTopologyPhaseCallbacks(WorkerTopologyPhaseCallbackDependencies dependencies)
    : centralizedMetadata_(dependencies.centralizedMetadata),
      localMetadataMaster_(dependencies.localMetadataMaster),
      streamMetadataEnabled_(dependencies.streamMetadataEnabled),
      metadataManagers_(dependencies.metadataManagers),
      objectCacheServiceProvider_(std::move(dependencies.objectCacheServiceProvider)),
      readinessCheck_(std::move(dependencies.readinessCheck))
{
}

Status WorkerTopologyPhaseCallbacks::OnScaleOut(const cluster::TopologyCallbackContext &context)
{
    RETURN_IF_NOT_OK(CheckContext(context));
    ApiDeadlineGuard deadlineGuard(RemainingCallbackBudgetUs(context.deadline), InUs{});
    return MigrateMetadata(context);
}

Status WorkerTopologyPhaseCallbacks::OnScaleIn(const cluster::TopologyCallbackContext &context)
{
    RETURN_IF_NOT_OK(CheckContext(context));
    ApiDeadlineGuard deadlineGuard(RemainingCallbackBudgetUs(context.deadline), InUs{});
    CHECK_FAIL_RETURN_STATUS(readinessCheck_ != nullptr, K_NOT_READY, "Worker readiness check is not initialized");
    RETURN_IF_NOT_OK(readinessCheck_(context.deadline, context.cancellation));
    auto *objectCacheService = GetObjectCacheService();
    CHECK_FAIL_RETURN_STATUS(objectCacheService != nullptr, K_NOT_READY,
                             "Worker object-cache service is not initialized");
    RETURN_IF_NOT_OK(DrainScaleInData(context, *objectCacheService));
    return MigrateMetadata(context);
}

Status WorkerTopologyPhaseCallbacks::PrepareScaleInCleanup(const cluster::TopologyCallbackContext &context,
                                                           std::unique_ptr<cluster::TopologyPreparedCleanup> &prepared)
{
    RETURN_IF_NOT_OK(CheckContext(context));
    auto *objectCacheService = GetObjectCacheService();
    CHECK_FAIL_RETURN_STATUS(objectCacheService != nullptr, K_NOT_READY,
                             "Worker object-cache service is not initialized");
    std::function<Status()> authorize;
    cluster::TopologyCleanupEffect apply;
    RETURN_IF_NOT_OK(objectCacheService->PrepareTopologyScaleInCleanup(context.action, context.keyFilter,
                                                                       context.businessOperationId, context.deadline,
                                                                       context.cancellation, authorize, apply));
    prepared = std::make_unique<cluster::TopologyPreparedCleanup>(std::move(authorize), std::move(apply));
    return Status::OK();
}

Status WorkerTopologyPhaseCallbacks::OnFailure(const cluster::TopologyCallbackContext &context)
{
    RETURN_IF_NOT_OK(CheckContext(context));
    ApiDeadlineGuard deadlineGuard(RemainingCallbackBudgetUs(context.deadline), InUs{});
    return RunFailureBestEffort(context);
}

Status WorkerTopologyPhaseCallbacks::CheckContext(const cluster::TopologyCallbackContext &context)
{
    CHECK_FAIL_RETURN_STATUS(!context.cancellation.IsCancelled(), K_NOT_READY, "topology callback cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < context.deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology callback deadline exceeded");
    return Status::OK();
}

Status WorkerTopologyPhaseCallbacks::MigrateMetadata(const cluster::TopologyCallbackContext &context)
{
    if (centralizedMetadata_) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(master::OCMigrateMetadataManager::Instance().MigrateTopologyMetadata(
        context.action, context.keyFilter, context.businessOperationId, context.deadline, context.cancellation));
    if (!streamMetadataEnabled_) {
        return Status::OK();
    }
    return master::SCMigrateMetadataManager::Instance().MigrateTopologyMetadata(
        context.action, context.keyFilter, context.businessOperationId, context.deadline, context.cancellation);
}

Status WorkerTopologyPhaseCallbacks::AcquireScaleInDrain(const cluster::TopologyCallbackContext &context,
                                                         bool &leader)
{
    CHECK_FAIL_RETURN_STATUS(context.action.source.has_value(), K_INVALID, "ScaleIn callback lacks source");
    const auto &sourceId = context.action.source->id;
    std::unique_lock<std::mutex> lock(scaleInDrainMutex_);
    while (scaleInDrainState_.running) {
        CHECK_FAIL_RETURN_STATUS(!context.cancellation.IsCancelled(), K_NOT_READY, "topology ScaleIn cancelled");
        CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < context.deadline, K_RPC_DEADLINE_EXCEEDED,
                                 "topology ScaleIn data drain wait exceeded deadline");
        const auto wake = std::min(context.deadline, std::chrono::steady_clock::now() + SCALE_IN_DRAIN_WAIT_POLL);
        scaleInDrainChanged_.wait_until(lock, wake);
    }
    const bool sameBatch = scaleInDrainState_.sourceId == sourceId
                           && scaleInDrainState_.batchEpoch == context.action.batchEpoch;
    if (sameBatch && scaleInDrainState_.complete) {
        leader = false;
        return Status::OK();
    }
    scaleInDrainState_ = { sourceId, context.action.batchEpoch, true, false };
    leader = true;
    return Status::OK();
}

void WorkerTopologyPhaseCallbacks::CompleteScaleInDrain(const cluster::TopologyCallbackContext &context,
                                                        const Status &status)
{
    {
        std::lock_guard<std::mutex> lock(scaleInDrainMutex_);
        scaleInDrainState_.running = false;
        scaleInDrainState_.complete = status.IsOk();
    }
    scaleInDrainChanged_.notify_all();
    LOG(INFO) << "CLUSTER_SCALE_IN_DRAIN source=" << context.action.source->address
              << " batch_epoch=" << context.action.batchEpoch << " task_id=" << context.action.taskId
              << " status=" << status.ToString();
}

Status WorkerTopologyPhaseCallbacks::DrainScaleInData(
    const cluster::TopologyCallbackContext &context, object_cache::WorkerOCServiceImpl &objectCacheService)
{
    bool leader = false;
    RETURN_IF_NOT_OK(AcquireScaleInDrain(context, leader));
    if (!leader) {
        return Status::OK();
    }
    auto status = objectCacheService.DrainTopologyScaleInData(
        context.action, context.businessOperationId, context.deadline, context.cancellation);
    CompleteScaleInDrain(context, status);
    return status;
}

object_cache::WorkerOCServiceImpl *WorkerTopologyPhaseCallbacks::GetObjectCacheService() const
{
    return objectCacheServiceProvider_ == nullptr ? nullptr : objectCacheServiceProvider_();
}

void WorkerTopologyPhaseCallbacks::RecordFailureStep(const std::string &step, const Status &status,
                                                     Status &firstError) const
{
    if (status.IsOk()) {
        return;
    }
    LOG(ERROR) << "CLUSTER_FAILURE action=callback_step step=" << step << " outcome=failed status="
               << status.ToString();
    if (firstError.IsOk()) {
        firstError = status;
    }
}

Status WorkerTopologyPhaseCallbacks::RunFailureBestEffort(const cluster::TopologyCallbackContext &context)
{
    std::shared_ptr<master::OCMetadataManager> ocMetadata;
    std::shared_ptr<master::SCMetadataManager> scMetadata;
    Status firstError;
    const bool localMetadata = !centralizedMetadata_ || localMetadataMaster_;
    if (localMetadata) {
        RecordFailureStep("get-object-metadata", metadataManagers_.GetOcMetadataManager(ocMetadata), firstError);
    }
    if (localMetadata && streamMetadataEnabled_) {
        RecordFailureStep("get-stream-metadata", metadataManagers_.GetScMetadataManager(scMetadata), firstError);
    }
    if (!centralizedMetadata_ && ocMetadata != nullptr) {
        RecordFailureStep(
            "recover-object",
            ocMetadata->RecoverTopologyFailure(context.action, context.keyFilter, context.storageScanPlan,
                                               context.businessOperationId, context.deadline, context.cancellation),
            firstError);
    }
    if (!centralizedMetadata_ && streamMetadataEnabled_ && scMetadata != nullptr) {
        RecordFailureStep(
            "recover-stream",
            scMetadata->RecoverTopologyFailure(context.action, context.keyFilter, context.businessOperationId,
                                               context.deadline, context.cancellation),
            firstError);
    }
    RunFailureCleanup(context, ocMetadata, scMetadata, firstError);
    return firstError;
}

void WorkerTopologyPhaseCallbacks::RunFailureCleanup(const cluster::TopologyCallbackContext &context,
                                                     const std::shared_ptr<master::OCMetadataManager> &ocMetadata,
                                                     const std::shared_ptr<master::SCMetadataManager> &scMetadata,
                                                     Status &firstError)
{
    const bool localMetadata = !centralizedMetadata_ || localMetadataMaster_;
    if (streamMetadataEnabled_ && localMetadata && scMetadata != nullptr) {
        RecordFailureStep("cleanup-stream",
                          scMetadata->CleanupTopologyFailedMember(context.action, context.businessOperationId,
                                                                  context.deadline, context.cancellation),
                          firstError);
    }
    if (localMetadata && ocMetadata != nullptr) {
        RecordFailureStep("cleanup-object",
                          ocMetadata->CleanupTopologyFailedMember(context.action, context.businessOperationId,
                                                                  context.deadline, context.cancellation),
                          firstError);
    }
    auto *objectCacheService = GetObjectCacheService();
    if (objectCacheService != nullptr) {
        RecordFailureStep(
            "cleanup-local-data",
            objectCacheService->SubmitTopologyFailureCleanup(
                context.action, context.keyFilter, context.businessOperationId, context.deadline, context.cancellation),
            firstError);
    } else {
        RecordFailureStep("cleanup-local-data", Status(K_NOT_READY, "object-cache service is not initialized"),
                          firstError);
    }
    if (!centralizedMetadata_ && ocMetadata != nullptr) {
        RecordFailureStep("cleanup-device",
                          ocMetadata->CleanupTopologyDeviceClientMeta(context.action, context.businessOperationId,
                                                                      context.deadline, context.cancellation),
                          firstError);
    }
    // Drop cached worker<->worker RPC stubs to the failed member so that no
    // healthy worker keeps reconnecting (and retransmitting TCP SYNs) to a
    // dead peer after node_dead_timeout_s. Issue #766: survivors leaked the
    // TCP link info and kept retrying the 25 kill-9'd workers.
    // Unconditional: every worker owns its own RpcStubCacheMgr regardless of
    // metadata mode. Remove is idempotent (K_NOT_FOUND is benign).
    RecordFailureStep("cleanup-rpc-stub", EraseFailedWorkerWorkerStub(context.action), firstError);
}

}  // namespace datasystem::worker
