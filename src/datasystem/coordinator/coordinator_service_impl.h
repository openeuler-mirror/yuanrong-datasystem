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
  * Description: Coordinator RPC service implementation skeleton.
 */
#ifndef DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H
#define DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H

#include <memory>
#include <mutex>
#include <string>

#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/coordinator/watch_dispatcher_impl.h"
#include "datasystem/protos/coordinator.brpc.pb.h"
#include "datasystem/protos/coordinator.service.rpc.pb.h"

namespace datasystem {
namespace coordinator {
class TopologyRecoveryManager;

class CoordinatorServiceImpl : public CoordinatorService, public ICoordinatorService {
public:
    /**
     * @brief Construct an in-memory Coordinator RPC service.
     * @param[in] localAddress Coordinator listen address.
     */
    explicit CoordinatorServiceImpl(const HostPort &localAddress);

    /**
     * @brief Invoke idempotent Shutdown before releasing owned components.
     */
    ~CoordinatorServiceImpl() override;

    /**
     * @brief Load authentication and construct the in-memory service component tree.
     * @return Operation status.
     */
    Status Init();

    /**
     * @brief Start the configured RPC server and bind its endpoints.
     * @return Operation status.
     */
    Status Start();

    /**
     * @brief Stop RPC and destroy all components in reverse dependency order.
     * @return Operation status.
     */
    Status Shutdown();

    /**
     * @brief Store one key/value request after recovery-gate validation.
     * @param[in] req Key, value, TTL, and CAS expectation.
     * @param[out] rsp Committed version, revision, and CoordinatorId.
     * @return Store, gate, or validation status.
     */
    Status Put(const PutReqPb &req, PutRspPb &rsp) override;

    /**
     * @brief Read one exact key or key range after recovery-gate validation.
     * @param[in] req Physical key and optional range end.
     * @param[out] rsp Matching values, revision, and CoordinatorId.
     * @return Store, gate, or validation status.
     */
    Status Range(const RangeReqPb &req, RangeRspPb &rsp) override;

    /**
     * @brief Delete one exact key or key range after recovery-gate validation.
     * @param[in] req Physical key and optional range end.
     * @param[out] rsp Delete count, revision, and CoordinatorId.
     * @return Store, gate, or validation status.
     */
    Status DeleteRange(const DeleteRangeReqPb &req, DeleteRangeRspPb &rsp) override;

    /**
     * @brief Register one watch and return its initial snapshot.
     * @param[in] req Physical range and watcher callback address.
     * @param[out] rsp Watch identity, initial values, and CoordinatorId.
     * @return Store or validation status.
     */
    Status WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp) override;

    /**
     * @brief Cancel watch IDs owned by one watcher address.
     * @param[in] req Watcher address and watch identities.
     * @param[out] rsp CoordinatorId after cancellation.
     * @return Store or validation status.
     */
    Status CancelWatch(const CancelWatchReqPb &req, CancelWatchRspPb &rsp) override;

    /**
     * @brief Renew one membership lease and wake recovery reconciliation.
     * @param[in] req Exact membership key.
     * @param[out] rsp Lease timing and CoordinatorId.
     * @return Store, gate, or validation status.
     */
    Status KeepAlive(const KeepAliveReqPb &req, KeepAliveRspPb &rsp) override;

    /**
     * @brief Return the current CoordinatorId without reading cluster recovery state.
     * @param[in] req Empty identity query.
     * @param[out] rsp Current CoordinatorId in the response header.
     * @return Operation status.
     */
    Status GetCoordinatorId(const GetCoordinatorIdReqPb &req, GetCoordinatorIdRspPb &rsp) override;

    /**
     * @brief Accept one Worker-initiated topology recovery candidate report.
     * @param[in] req Cluster, CoordinatorId, reporter, and candidate evidence or payload.
     * @param[out] rsp Admission decision, recovery state, and payload request.
     * @return Validation, admission, or recovery status.
     */
    Status ReportTopologyRecoveryCandidate(const ReportTopologyRecoveryCandidateReqPb &req,
                                           ReportTopologyRecoveryCandidateRspPb &rsp) override;

    /**
     * @brief Read raw topology and membership facts without recovery gating or domain projection.
     * @param[in] req Validated logical cluster name.
     * @param[out] rsp Raw key/value groups including each entry's modification revision.
     * @return Store, validation, or response-size status.
     */
    Status GetClusterRawSnapshot(const GetClusterRawSnapshotReqPb &req,
                                 GetClusterRawSnapshotRspPb &rsp) override;

private:
    /**
     * @brief Construct the Store, watch, TTL, and topology recovery component tree.
     */
    void BuildComponentTree();

    /**
     * @brief Configure the selected RPC transport and service endpoint.
     */
    void ConfigureRpcService();

    /**
     * @brief Reconcile a membership callback against the latest committed key before watch cleanup.
     * @param[in] key Physical membership key reported by the Store.
     */
    void HandleCommittedMembershipMutation(const std::string &key);

    /**
     * @brief Reject a topology watch whose owning membership no longer exists.
     * @param[in] req Watch request to validate while membershipWatchMutex_ is held.
     * @return K_OK for a live member or non-topology watch; K_NOT_FOUND for a stale member.
     */
    Status CheckWatcherMembership(const WatchRangeReqPb &req);

    /**
     * @brief Fill leader and CoordinatorId response metadata.
     * @param[out] header Response header to fill.
     */
    void FillResponseHeader(ResponseHeader *header) const;

    HostPort coordinatorAddr_;
    RpcServer::Builder builder_;
    // brpc mode — MUST be declared before rpcServer_: the brpc adapter holds a
    // reference to *this and is registered with the brpc::Server inside rpcServer_.
    // On destruction, rpcServer_ (~RpcServer → StopBrpcServer) must outlive the adapter.
    std::unique_ptr<CoordinatorServiceBrpcAdapter> brpcAdapter_;
    std::unique_ptr<RpcServer> rpcServer_;
    std::shared_ptr<MemoryKvStore> memStore_;
    std::shared_ptr<WatchRegistry> watchRegistry_;
    std::shared_ptr<WatchDispatcherImpl> watchDispatcher_;
    std::shared_ptr<SteadyClockReal> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::shared_ptr<CoordinatorStore> store_;
    std::unique_ptr<TopologyRecoveryManager> topologyRecoveryManager_;
    // Serializes membership-current-value checks, stale-channel cleanup and new watch registration.
    std::mutex membershipWatchMutex_;
    // brpc mode address (set in Init, consumed in Start)
    std::string brpcAddr_;
    int brpcPort_ = 0;
    std::string coordinatorId_;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H
