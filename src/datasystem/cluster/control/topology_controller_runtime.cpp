/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Single-cluster topology Controller composition and lifecycle.
 */
#include "datasystem/cluster/control/topology_controller_runtime.h"

#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {

/**
 * @brief Preserve the first component error while later cleanup steps continue.
 * @param[in] candidate Status returned by the current cleanup step.
 * @param[in,out] firstError First error observed across cleanup steps.
 */
void PreserveFirstError(const Status &candidate, Status &firstError)
{
    if (firstError.IsOk() && candidate.IsError()) {
        firstError = candidate;
    }
}

/**
 * @brief Identify a Stop result that still has a component thread using Runtime dependencies.
 * @param[in] status Component Stop result.
 * @return True when the component did not join before its deadline.
 */
bool HasLiveComponent(const Status &status)
{
    return status.GetCode() == K_RPC_DEADLINE_EXCEEDED;
}

}  // namespace

Status TopologyControllerRuntime::Create(Options options, ICoordinationBackend &backend,
                                         const IPlanningAlgorithm &algorithm,
                                         std::unique_ptr<TopologyControllerRuntime> &runtime)
{
    const bool janitorValid = !options.janitor.has_value() || options.janitor->IsValid();
    CHECK_FAIL_RETURN_STATUS(options.eventQueueCapacity > 0 && options.controller.IsValid() && janitorValid, K_INVALID,
                             "invalid topology Controller Runtime component options");
    std::unique_ptr<TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(TopologyKeyHelper::Create(options.clusterName, keys));
    auto candidate = std::unique_ptr<TopologyControllerRuntime>(
        new TopologyControllerRuntime(std::move(options), backend, algorithm, std::move(keys)));
    runtime = std::move(candidate);
    return Status::OK();
}

TopologyControllerRuntime::TopologyControllerRuntime(Options options, ICoordinationBackend &backend,
                                                     const IPlanningAlgorithm &algorithm,
                                                     std::unique_ptr<TopologyKeyHelper> keys)
    : options_(std::move(options)),
      backend_(backend),
      algorithm_(algorithm),
      keys_(std::move(keys)),
      repository_(backend_, *keys_),
      dispatcher_(options_.eventQueueCapacity),
      controller_(backend_, repository_, *keys_, algorithm_, dispatcher_, options_.controller)
{
    if (options_.janitor.has_value()) {
        janitor_ = std::make_unique<TopologyTaskJanitor>(repository_, algorithm_, materializer_, *options_.janitor);
    }
}

TopologyControllerRuntime::~TopologyControllerRuntime()
{
    LOG_IF_ERROR(Stop(std::chrono::steady_clock::time_point::max()),
                 "CLUSTER_LIFECYCLE role=controller_runtime state=destructor_stop_failed");
}

Status TopologyControllerRuntime::ClaimStart()
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    CHECK_FAIL_RETURN_STATUS(!lifecycleOperationInFlight_, K_TRY_AGAIN,
                             "topology Controller Runtime lifecycle operation is in progress");
    CHECK_FAIL_RETURN_STATUS(!startAttempted_, K_INVALID, "topology Controller Runtime Start is one-shot");
    startAttempted_ = true;
    lifecycleOperationInFlight_ = true;
    return Status::OK();
}

void TopologyControllerRuntime::CommitStart(bool started)
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    started_ = started;
    stopping_ = false;
    lifecycleOperationInFlight_ = false;
}

Status TopologyControllerRuntime::Start()
{
    RETURN_IF_NOT_OK(ClaimStart());
    auto status = controller_.Start();
    if (status.IsError()) {
        CommitStart(false);
        return status;
    }
    if (janitor_ != nullptr) {
        status = janitor_->Start();
    }
    if (status.IsError()) {
        // Preserve the live Controller responsibility for the caller's bounded Stop transaction. Marking this Runtime
        // stopped here would make Engine rollback skip the Controller after a Janitor startup failure.
        CommitStart(true);
        LOG(WARNING) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                     << " role=controller_runtime component=janitor state=start_failed action=bounded_stop_required"
                     << " status=" << status.ToString();
        return status;
    }
    CommitStart(true);
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName << " role=controller_runtime state=ready";
    return Status::OK();
}

Status TopologyControllerRuntime::ClaimStop(bool &shouldStop)
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    CHECK_FAIL_RETURN_STATUS(!lifecycleOperationInFlight_, K_TRY_AGAIN,
                             "topology Controller Runtime lifecycle operation is in progress");
    shouldStop = started_;
    if (shouldStop) {
        stopping_ = true;
        lifecycleOperationInFlight_ = true;
    }
    return Status::OK();
}

void TopologyControllerRuntime::CommitStop(bool allStopped)
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    if (allStopped) {
        started_ = false;
        stopping_ = false;
    }
    lifecycleOperationInFlight_ = false;
}

Status TopologyControllerRuntime::StopComponents(std::chrono::steady_clock::time_point deadline, bool &allStopped)
{
    Status firstError;
    allStopped = true;
    if (janitor_ != nullptr) {
        auto janitorStatus = janitor_->Stop(deadline);
        PreserveFirstError(janitorStatus, firstError);
        allStopped = allStopped && !HasLiveComponent(janitorStatus);
    }
    auto controllerStatus = controller_.Stop(deadline);
    PreserveFirstError(controllerStatus, firstError);
    allStopped = allStopped && !HasLiveComponent(controllerStatus);
    return firstError;
}

Status TopologyControllerRuntime::Stop(std::chrono::steady_clock::time_point deadline)
{
    bool shouldStop = false;
    RETURN_IF_NOT_OK(ClaimStop(shouldStop));
    if (!shouldStop) {
        return Status::OK();
    }
    bool allStopped = false;
    auto status = StopComponents(deadline, allStopped);
    CommitStop(allStopped);
    if (allStopped) {
        LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName << " role=controller_runtime state=stopped";
    }
    return status;
}

TopologyControllerDiagnostics TopologyControllerRuntime::GetDiagnostics() const
{
    return controller_.GetDiagnostics();
}

}  // namespace datasystem::cluster
