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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
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
#include "datasystem/coordinator/raft/coordinator_election_manager.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"
#include "datasystem/coordinator/watch_dispatcher_impl.h"
#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/protos/coordinator.brpc.pb.h"
#include "datasystem/protos/coordinator.service.rpc.pb.h"

namespace datasystem {
namespace st {
class CoordinatorServiceElectionTestBase;
}
namespace coordinator {
class TopologyControlHost;
class TopologyRecoveryManager;

class CoordinatorServiceImpl : public CoordinatorService, public ICoordinatorService {
public:
    /**
     * @brief Construct an in-memory Coordinator RPC service. Only `(nullptr, 0)` disables election; otherwise,
     *        coordinatorDiscovery must be non-null and expectedMemberCount must be greater than zero.
     * @param[in] localAddress Coordinator listen address.
     * @param[in] coordinatorDiscovery Election candidate provider.
     * @param[in] expectedMemberCount Fixed election voting-member target.
     * @param[in] raftFlags Immutable Raft identity and timing snapshot for this service generation.
     */
    explicit CoordinatorServiceImpl(const HostPort &localAddress,
                                    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery = nullptr,
                                    size_t expectedMemberCount = 0, CoordinatorRaftFlags raftFlags = {});

    /**
     * @brief Invoke best-effort Shutdown without allowing exceptions to escape destruction.
     */
    ~CoordinatorServiceImpl() noexcept override;

    /**
     * @brief Initialize a newly created service and publish INITIALIZED only after all initialization succeeds.
     * @return Operation status. Initialization failures preserve the original status and leave the service STOPPED.
     */
    Status Init() override;

    /**
     * @brief Initialize a newly created service and optionally publish STARTING for in-process direct-call tests.
     * @param[in] publishStarting Whether to publish STARTING instead of INITIALIZED after successful initialization.
     * @return Operation status. Initialization failures preserve the original status and leave the service STOPPED.
     */
    Status Init(bool publishStarting);

    /**
     * @brief Start RPC services. No-election mode publishes RUNNING; election mode remains STARTING.
     * @return Operation status. Startup failures preserve the original status and leave the service STOPPED.
     */
    Status Start();

    /**
     * @brief Publish the election owner and start its background bootstrap worker after external registration succeeds.
     * @return Operation status. A synchronous failure detaches and cleans up the Manager, leaves the service STARTING,
     *         and cannot be retried.
     */
    Status StartElectionManager();

    /**
     * @brief Report whether the running election-enabled service owns Raft leadership.
     */
    bool IsLeader() const;

    /**
     * @brief Report the current normalized Raft leader address.
     */
    Status GetLeader(std::string &leaderAddress) const;

    /**
     * @brief Best-effort stop RPC and destroy all components in reverse dependency order.
     * @return The first public cleanup owner's saved status for concurrent or repeated callers, K_OK if already STOPPED
     *         before public cleanup starts, or fixed K_RUNTIME_ERROR for an unexpected coordination exception.
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
     * @brief Forward the published election Manager's bootstrap snapshot without applying the business serving gate.
     * @param[in] req Fixed Coordinator Raft group identity.
     * @param[out] rsp Current Manager-owned bootstrap observation.
     * @return Validation, lifecycle, or snapshot status.
     */
    Status GetRaftBootstrapState(const GetRaftBootstrapStateReqPb &req, GetRaftBootstrapStateRspPb &rsp) override;

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
    Status GetClusterRawSnapshot(const GetClusterRawSnapshotReqPb &req, GetClusterRawSnapshotRspPb &rsp) override;

private:
    friend class ::datasystem::st::CoordinatorServiceElectionTestBase;

    enum class ServingState : uint8_t { CREATED, INITIALIZED, STARTING, RUNNING, STOPPING, STOPPED };

    /**
     * @brief Construct and start the Store, recovery, and topology control component tree.
     * @return Component construction or Host startup status.
     */
    Status BuildComponentTree();

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
     * @brief Route one committed Store mutation to Recovery, Host and watch cleanup.
     * @param[in] type Mutation type.
     * @param[in] key Physical Store key.
     */
    void HandleCommittedMutation(WatchEvent::Type type, const std::string &key);

    /**
     * @brief Reserve Controller capacity before one membership Put can commit.
     * @param[in] key Physical Put key.
     * @param[out] clusterName Parsed cluster when this is a membership Put.
     * @param[out] reserved True when reservation completion is required.
     * @return Admission, parse, or lifecycle status.
     */
    Status PrepareTopologyMembershipPut(const std::string &key, std::string &clusterName, bool &reserved);

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

    bool IsElectionConfigured() const noexcept;
    Status ValidateElectionConfiguration() const;
    Status BuildElectionStartupContext(CoordinatorElectionOptions &options) const;
    CoordinatorRaftEventCallbacks BuildRaftEventCallbacks();
    Status CheckServing() const;
    Status InitInternal();
    Status FinishSuccessfulStart();
    Status StartInternal();
    Status ShutdownElectionManager(std::unique_ptr<CoordinatorElectionManager> electionManager);
    Status ShutdownRemainingComponents(Status firstError);
    Status ShutdownInternal(std::unique_lock<std::mutex> &lifecycleLock);

    HostPort coordinatorAddr_;
    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery_;
    size_t expectedMemberCount_{ 0 };
    CoordinatorRaftFlags raftFlags_;
    RpcServer::Builder builder_;
    std::shared_ptr<MemoryKvStore> memStore_;
    std::shared_ptr<WatchRegistry> watchRegistry_;
    std::shared_ptr<WatchDispatcherImpl> watchDispatcher_;
    std::shared_ptr<SteadyClockReal> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::shared_ptr<CoordinatorStore> store_;
    std::unique_ptr<TopologyRecoveryManager> topologyRecoveryManager_;
    std::unique_ptr<TopologyControlHost> topologyControlHost_;
    // Serializes membership-current-value checks, stale-channel cleanup and new watch registration.
    std::mutex membershipWatchMutex_;
    // brpc mode address (set in Init, consumed in Start)
    std::string brpcAddr_;
    int brpcPort_ = 0;
    std::string coordinatorId_;
    // Serializes one-way lifecycle state transitions and leader/bootstrap queries. Election startup reserves one
    // attempt, publishes Manager ownership before starting its background worker, then publishes completion under this
    // mutex. Shutdown publishes STOPPING and transfers Manager ownership under this mutex, then performs every blocking
    // cleanup stage without the lock before reacquiring it only to publish the shared result.
    mutable std::mutex lifecycleMutex_;
    std::condition_variable lifecycleCv_;
    bool electionStartInProgress_{ false };
    bool electionStartAttempted_{ false };
    bool shutdownInProgress_{ false };
    bool shutdownComplete_{ false };
    Status shutdownStatus_;
    std::atomic<ServingState> servingState_{ ServingState::CREATED };
    std::atomic<bool> raftServing_{ false };

#ifdef WITH_TESTS
    // Narrow deterministic seams for lifecycle publication, real brpc handler/server ordering, snapshot-copy, and
    // Manager cleanup ordering.
    std::function<void()> electionManagerPublishedHook_;
    std::function<void()> raftBootstrapHandlerEnteredHook_;
    std::function<void()> raftBootstrapSnapshotCopiedHook_;
    std::function<Status()> electionManagerShutdownHook_;
    std::function<void()> rpcServerShutdownHook_;
#endif

    // Declaration order is the reverse-destruction fallback. Explicit Shutdown remains authoritative:
    // gate closed -> ElectionManager (Membership then Node) -> RpcServer -> business brpc adapter.
    std::unique_ptr<CoordinatorServiceBrpcAdapter> brpcAdapter_;
    std::unique_ptr<RpcServer> rpcServer_;
    std::unique_ptr<CoordinatorElectionManager> electionManager_;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H
