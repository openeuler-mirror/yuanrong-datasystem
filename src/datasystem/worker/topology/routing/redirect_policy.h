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
#ifndef DATASYSTEM_WORKER_TOPOLOGY_ROUTING_REDIRECT_POLICY_H
#define DATASYSTEM_WORKER_TOPOLOGY_ROUTING_REDIRECT_POLICY_H

#include <memory>

#include "datasystem/worker/topology/membership/worker_directory.h"
#include "datasystem/worker/topology/routing/routing_view.h"
#include "datasystem/worker/topology/routing/worker_locator.h"
#include "datasystem/worker/topology/runtime/placement_types.h"

namespace datasystem {
namespace topology {

class IRedirectPolicy {
public:
    virtual ~IRedirectPolicy() = default;

    /**
     * @brief Evaluate redirect action for an object key using local R0 facts.
     * @param[in] objectKey Business object key.
     * @param[in] options Route and response policy options.
     * @param[out] decision Redirect decision.
     * @return K_OK if a response action is generated, K_INVALID if context is malformed, K_NOT_READY if routing
     * snapshot or worker directory is not published, K_NOT_FOUND if local worker or routing owner cannot be resolved,
     * K_RPC_UNAVAILABLE if the locator fallback rejects an unavailable target.
     *
     * Redirect policy only reads local immutable snapshots and worker directory facts. It must not scan tasks, wait for
     * topology progress, perform repository/backend IO, or execute migration/recovery/cleanup. Worker default request
     * paths should not call this policy.
     */
    virtual Status Evaluate(const std::string &objectKey, const RouteOptions &options,
                            RedirectDecision &decision) const = 0;
};

class RedirectPolicy final : public IRedirectPolicy {
public:
    RedirectPolicy(std::shared_ptr<IRoutingView> routingView, std::shared_ptr<IWorkerDirectory> directory,
                   std::shared_ptr<IWorkerLocator> locator);
    ~RedirectPolicy() override = default;

    Status Evaluate(const std::string &objectKey, const RouteOptions &options,
                    RedirectDecision &decision) const override;

private:
    /**
     * @brief Build a redirect decision for centralized (single-master) mode.
     * @param[out] decision Redirect decision carrying the centralized master endpoint.
     * @return K_OK on success, otherwise the parse status of the master address or the finish status.
     */
    Status EvaluateCentralized(RedirectDecision &decision) const;

    /**
     * @brief Resolve the worker endpoint that should receive this object's request.
     * @param[in] objectKey Business object key.
     * @param[out] targetEndpoint Resolved redirect target endpoint.
     * @return K_OK if a target is resolved, K_INVALID if dependencies are unset, K_NOT_READY if the snapshot is absent.
     *
     * Prefers a redirect hint for the object hash and falls back to the regular meta-owner lookup when no hint exists.
     */
    Status ResolveRedirectTarget(const std::string &objectKey, WorkerEndpoint &targetEndpoint) const;

    /**
     * @brief Resolve the redirect target from a routing redirect hint, if present.
     * @param[in] objectHash Hash value of the business key.
     * @param[in] snapshot Immutable routing snapshot to search.
     * @param[out] targetEndpoint Resolved hint target endpoint.
     * @param[out] hasHint True when a hint matched the hash, false otherwise.
     * @return K_OK on success, K_INVALID if the matched hint has an empty target address.
     *
     * When hasHint is false the caller must fall back to meta-owner lookup. A hint whose worker id resolves in the
     * directory upgrades the endpoint to the directory's availability; otherwise the raw hint address is NOT_READY.
     */
    Status ResolveHintTarget(uint32_t objectHash, const std::shared_ptr<const RoutingSnapshot> &snapshot,
                             WorkerEndpoint &targetEndpoint, bool &hasHint) const;

    /**
     * @brief Finalize a redirect decision by comparing the target against the local worker.
     * @param[in] targetEndpoint Redirect target endpoint.
     * @param[out] decision Redirect decision with action and target filled in.
     * @return K_OK on success, K_INVALID if the target address is empty or local worker lookup fails.
     *
     * Sets SERVE_LOCAL when the target is the local worker, REDIRECT otherwise.
     */
    Status FinishDecision(const WorkerEndpoint &targetEndpoint, RedirectDecision &decision) const;

    std::shared_ptr<IRoutingView> routingView_;
    std::shared_ptr<IWorkerDirectory> directory_;
    std::shared_ptr<IWorkerLocator> locator_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_TOPOLOGY_ROUTING_REDIRECT_POLICY_H
