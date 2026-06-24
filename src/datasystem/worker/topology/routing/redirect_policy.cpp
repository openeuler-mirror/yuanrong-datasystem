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
 * Description: Master metadata response redirect policy.
 */
#include "datasystem/worker/topology/routing/redirect_policy.h"

#include <utility>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_string(master_address);

namespace datasystem {
namespace topology {
namespace {
bool IsSameWorker(const WorkerEndpoint &lhs, const WorkerEndpoint &rhs)
{
    if (!lhs.workerId.empty() && !rhs.workerId.empty()) {
        return lhs.workerId == rhs.workerId;
    }
    return !lhs.address.Empty() && !rhs.address.Empty() && lhs.address == rhs.address;
}
}  // namespace

RedirectPolicy::RedirectPolicy(std::shared_ptr<IRoutingView> routingView, std::shared_ptr<IWorkerDirectory> directory,
                               std::shared_ptr<IWorkerLocator> locator)
    : routingView_(std::move(routingView)), directory_(std::move(directory)), locator_(std::move(locator))
{
}

Status RedirectPolicy::Evaluate(const std::string &objectKey, const RouteOptions &options,
                                RedirectDecision &decision) const
{
    decision = RedirectDecision{};
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "Redirect object key is empty.");
    CHECK_FAIL_RETURN_STATUS(directory_ != nullptr, K_INVALID, "Redirect policy directory is not set.");
    if (options.centralizedMode) {
        return EvaluateCentralized(decision);
    }

    WorkerEndpoint targetEndpoint;
    RETURN_IF_NOT_OK(ResolveRedirectTarget(objectKey, targetEndpoint));
    return FinishDecision(targetEndpoint, decision);
}

Status RedirectPolicy::EvaluateCentralized(RedirectDecision &decision) const
{
    WorkerEndpoint targetEndpoint;
    targetEndpoint.availability = WorkerAvailability::READY;
    RETURN_IF_NOT_OK(targetEndpoint.address.ParseString(FLAGS_master_address));
    return FinishDecision(targetEndpoint, decision);
}

Status RedirectPolicy::ResolveRedirectTarget(const std::string &objectKey, WorkerEndpoint &targetEndpoint) const
{
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr && locator_ != nullptr, K_INVALID,
                             "Redirect policy dependencies are not set.");

    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(routingView_->GetSnapshot(snapshot));
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_NOT_READY, "Routing snapshot is not published.");

    const uint32_t objectHash = MurmurHash3_32(objectKey);
    bool hasHint = false;
    RETURN_IF_NOT_OK(ResolveHintTarget(objectHash, snapshot, targetEndpoint, hasHint));
    if (hasHint) {
        return Status::OK();
    }

    RouteDecision route;
    RouteOptions options;
    RETURN_IF_NOT_OK(locator_->LocateMetaOwner(objectKey, options, nullptr, route));
    targetEndpoint = route.ownerEndpoint;
    return Status::OK();
}

Status RedirectPolicy::ResolveHintTarget(uint32_t objectHash, const std::shared_ptr<const RoutingSnapshot> &snapshot,
                                         WorkerEndpoint &targetEndpoint, bool &hasHint) const
{
    hasHint = false;
    RoutingRedirectHint hint;
    if (!snapshot->FindRedirectHint(objectHash, hint)) {
        return Status::OK();
    }

    targetEndpoint = WorkerEndpoint{ hint.targetWorkerId, hint.targetAddress, WorkerAvailability::NOT_READY };
    if (!hint.targetWorkerId.empty()) {
        WorkerEndpoint resolvedEndpoint;
        if (directory_->ResolveWorker(hint.targetWorkerId, resolvedEndpoint).IsOk()) {
            targetEndpoint = resolvedEndpoint;
        }
    }
    CHECK_FAIL_RETURN_STATUS(!targetEndpoint.address.Empty(), K_INVALID, "Redirect hint target address is empty.");
    hasHint = true;
    return Status::OK();
}

Status RedirectPolicy::FinishDecision(const WorkerEndpoint &targetEndpoint, RedirectDecision &decision) const
{
    WorkerEndpoint localWorker;
    RETURN_IF_NOT_OK(directory_->GetLocalWorker(localWorker));
    CHECK_FAIL_RETURN_STATUS(!targetEndpoint.address.Empty(), K_INVALID, "Redirect target address is empty.");
    decision.targetEndpoint = targetEndpoint;
    if (IsSameWorker(localWorker, decision.targetEndpoint)) {
        decision.action = RedirectAction::SERVE_LOCAL;
        return Status::OK();
    }
    decision.action = RedirectAction::REDIRECT;
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
