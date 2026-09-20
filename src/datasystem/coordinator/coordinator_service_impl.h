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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include "datasystem/cluster/control/topology_controller.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/coordinator/topology_control_host.h"
#include "datasystem/coordinator/topology_recovery_manager.h"
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
class CoordinatorServiceImpl : public CoordinatorService, public ICoordinatorService {
public:
    /**
     * @brief Construct an in-memory Coordinator RPC service. Only `(nullptr, 0)` disables election; otherwise,
     *        coordinatorDiscovery must be non-null and expectedMemberCount must be greater than zero.
     * @param[in] localAddress Coordinator listen address.
     * @param[in] coordinatorDiscovery Election candidate provider.
     * @param[in] expectedMemberCount Fixed election voting-member target.
     * @param[in] raftFlags Immutable Raft identity and timing snapshot for this service generation.
     * @param[in] watchDispatcherBthreadTag Bthread tag used by watch notification tasks.
     * @param[in] bootstrapMode Static dscli peers or integrated Discovery observation bootstrap.
     */
    explicit CoordinatorServiceImpl(const HostPort &localAddress,
                                    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery = nullptr,
                                    size_t expectedMemberCount = 0, CoordinatorRaftFlags raftFlags = {},
                                    bthread_tag_t watchDispatcherBthreadTag = BTHREAD_TAG_DEFAULT,
                                    RaftBootstrapMode bootstrapMode = RaftBootstrapMode::DISCOVERY_OBSERVATION);

    /**
     * @brief Invoke best-effort Shutdown without allowing exceptions to escape destruction.
     */
    ~CoordinatorServiceImpl() noexcept override;

    /**
     * @brief Initialize a newly created service. Successful initialization keeps request entry in CREATED.
     * @return Operation status. Initialization failures preserve the original status and leave the service STOPPED.
     */
    Status Init() override;

    /**
     * @brief Start RPC services. No-election mode publishes RUNNING; election mode remains CREATED.
     * @return Operation status. Startup failures preserve the original status and leave the service STOPPED.
     */
    Status Start();

    /**
     * @brief Publish the election owner and start its background bootstrap worker after external registration succeeds.
     * @return Operation status. A synchronous failure detaches the Manager, stops the service, and cannot be retried.
     */
    Status StartElectionManager();

    // Raft state-machine callbacks begin and end recovery rounds without changing request-entry lifecycle.
    void OnLeaderStart(int64_t term);
    void OnLeaderStop(const Status &status);

    /**
     * @brief Report one coherent Raft leadership observation for the running service.
     */
    Status GetLeadershipSnapshot(CoordinatorLeadershipSnapshot &snapshot) const;

    /**
     * @brief Report whether the running service owns leadership.
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

    Status ExchangeBootstrapObservation(const RaftBootstrapObservationPb &req,
                                        RaftBootstrapObservationPb &rsp) override;

    /**
     * @brief Accept one Worker-initiated topology recovery candidate report.
     * @param[in] req Cluster, CoordinatorId, reporter, and candidate evidence or payload.
     * @param[out] rsp Admission decision, recovery state, and payload request.
     * @return Validation, admission, or recovery status.
     */
    Status ReportTopologyRecoveryCandidate(const ReportTopologyRecoveryCandidateReqPb &req,
                                           ReportTopologyRecoveryCandidateRspPb &rsp) override;

    /**
     * @brief Recreate exactly one Worker's membership while the elected Leader recovers its memory state.
     */
    Status EnsureLeaderMembership(const EnsureLeaderMembershipReqPb &req, EnsureLeaderMembershipRspPb &rsp) override;

    /**
     * @brief Accept one witness result for a Controller-owned worker probe round.
     */
    Status ReportWorkerLiveness(const ReportWorkerLivenessReqPb &req, ReportWorkerLivenessRspPb &rsp) override;

    /**
     * @brief Read raw topology and membership facts after cluster recovery admission, without domain projection.
     * @param[in] req Validated logical cluster name.
     * @param[out] rsp Raw key/value groups including each entry's modification revision.
     * @return Store, validation, or response-size status.
     */
    Status GetClusterRawSnapshot(const GetClusterRawSnapshotReqPb &req, GetClusterRawSnapshotRspPb &rsp) override;

private:
    friend class ::datasystem::st::CoordinatorServiceElectionTestBase;

    enum class LifecycleState : uint8_t {
        CREATED,  // Request entry is not published yet.
        RUNNING,  // RPC request entry is published.
        STOPPED,  // Request entry is permanently closed.
    };

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
     * @param[in] parsed Parsed membership key from the committed-mutation observer.
     */
    void HandleCommittedMembershipMutation(const std::string &key, const ParsedTopologyCoordinationKey &parsed);

    /**
     * @brief Route one committed Store mutation to Recovery, Host and watch cleanup.
     * @param[in] type Mutation type.
     * @param[in] key Physical Store key.
     */
    void HandleCommittedMutation(WatchEvent::Type type, const std::string &key);

    /**
     * @brief Reserve Controller capacity before one membership Put can commit.
     * @param[in] parsed Parsed Put key.
     * @param[out] reserved True when reservation completion is required.
     * @return Admission or lifecycle status.
     */
    Status PrepareTopologyMembershipPut(const ParsedTopologyCoordinationKey &parsed, bool &reserved);

    /**
     * @brief Reject a topology watch whose owning membership no longer exists.
     * @param[in] req Watch request to validate while membershipWatchMutex_ is held.
     * @param[in] parsed Parsed watch key.
     * @return K_OK for a live member; K_NOT_FOUND for a stale member.
     */
    Status CheckWatcherMembership(const WatchRangeReqPb &req, const ParsedTopologyCoordinationKey &parsed);

    /**
     * @brief Build one routeable response header from a single leadership snapshot.
     * @param[out] header Response header to replace after a successful snapshot.
     * @return Lifecycle or leadership snapshot status. K_OK guarantees a complete routeable header.
     */
    Status PrepareResponseHeader(ResponseHeader *header) const;

    /**
     * @brief Build one per-cluster response header from leadership and recovery state.
     * @param[in] clusterName Cluster whose recovery state controls Leader admission.
     * @param[out] header Response header to replace after a successful snapshot.
     * @return Lifecycle, leadership, or recovery-manager status. K_OK guarantees a complete routeable header.
     */
    Status PrepareResponseHeader(const std::string &clusterName, ResponseHeader *header) const;

    template <typename Request>
    bool AllowContinue(const ResponseHeader &header) const;

    template <typename Request>
    bool AllowRecoveryControl(const ResponseHeader &header) const;

    Status RequireTopologyRecoveryManager() const;
    Status RequireRecoveryLeader(uint64_t term, std::string_view coordinatorId) const;
    bool IsCurrentLeaderRound(uint64_t term, std::string_view coordinatorId) const;

    bool IsElectionConfigured() const noexcept;
    Status ValidateElectionConfiguration() const;
    Status BuildElectionStartupContext(CoordinatorElectionOptions &options) const;
    CoordinatorRaftEventCallbacks BuildRaftEventCallbacks();
    void ConfigureTopologyHostOptions(TopologyControlHost::Options &options) const;
    std::optional<uint64_t> GetCollectiveControlEpoch() const;
    Status RunUnderCollectiveReplacementFence(uint64_t expectedEpoch, const std::function<Status()> &mutation) const;
    std::vector<cluster::ControlBackendProbeResult> ProbeMembersLiveness(
        const std::vector<cluster::MemberIdentity> &targets, std::chrono::steady_clock::time_point deadline) const;
    Status InitInternal();
    Status FinishSuccessfulStart();
    Status StartInternal();
    Status ShutdownElectionManager(std::unique_ptr<CoordinatorElectionManager> electionManager);
    Status ShutdownRemainingComponents(Status firstError);
    Status ShutdownInternal(std::unique_lock<bthread::Mutex> &lifecycleLock);

    HostPort coordinatorAddr_;
    std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery_;
    size_t expectedMemberCount_{ 0 };
    RaftBootstrapMode bootstrapMode_{ RaftBootstrapMode::DISCOVERY_OBSERVATION };
    CoordinatorRaftFlags raftFlags_;
    const bthread_tag_t watchDispatcherBthreadTag_;
    RpcServer::Builder builder_;
    std::shared_ptr<MemoryKvStore> memStore_;
    std::shared_ptr<WatchRegistry> watchRegistry_;
    std::shared_ptr<WatchDispatcherImpl> watchDispatcher_;
    std::shared_ptr<SteadyClockReal> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::shared_ptr<CoordinatorStore> store_;
    std::unique_ptr<TopologyRecoveryManager> topologyRecoveryManager_;
    std::unique_ptr<TopologyControlHost> topologyControlHost_;
    // Serializes membership checks, stale-channel cleanup and watch registration. Registration can yield;
    // waiters must not block the Raft bthread worker pool.
    bthread::Mutex membershipWatchMutex_;
    // brpc mode address (set in Init, consumed in Start)
    std::string brpcAddr_;
    int brpcPort_ = 0;
    std::string coordinatorId_;
    // Serializes initialization, RPC/election startup, Manager ownership and the shutdown transaction. Shutdown waits
    // for election startup publication, then publishes STOPPED and transfers Manager ownership under this mutex. It
    // releases the mutex before taking leaderOperationMutex_ or performing any blocking cleanup and reacquires it only
    // to publish the cleanup result.
    mutable bthread::Mutex lifecycleMutex_;
    bthread::ConditionVariable lifecycleCv_;
    bool initialized_{ false };
    bool rpcStartInProgress_{ false };
    bool rpcStarted_{ false };
    bool electionStartInProgress_{ false };
    bool electionStartAttempted_{ false };
    bool shutdownInProgress_{ false };
    bool shutdownComplete_{ false };
    Status shutdownStatus_;
    std::atomic<LifecycleState> lifecycleState_{ LifecycleState::CREATED };
    // This fence linearizes operations with Raft Leader round transitions; CoordinatorElectionManager remains the
    // Raft source of truth.
    mutable SharedMutex leaderOperationMutex_;
    std::atomic<uint64_t> leaderTerm_{ 0 };

#ifdef WITH_TESTS
    // Narrow deterministic seams for lifecycle publication, handler/server ordering, snapshot/recovery observation,
    // and Manager cleanup ordering.
    std::function<void()> electionManagerPublishedHook_;
    std::function<void()> raftBootstrapHandlerEnteredHook_;
    std::function<void()> raftBootstrapSnapshotCopiedHook_;
    std::function<Status()> electionManagerShutdownHook_;
    std::function<void()> rpcServerShutdownHook_;
    std::function<Status(CoordinatorLeadershipSnapshot &)> leadershipSnapshotProvider_;
    std::function<TopologyRecoveryState(const std::string &)> recoveryStateProvider_;
#endif

    // Declaration order is the reverse-destruction fallback. Explicit Shutdown remains authoritative:
    // lifecycle STOPPED -> ElectionManager (Membership then Node) -> RpcServer -> business brpc adapter.
    std::unique_ptr<CoordinatorServiceBrpcAdapter> brpcAdapter_;
    std::unique_ptr<RpcServer> rpcServer_;
    std::unique_ptr<CoordinatorElectionManager> electionManager_;
};

template <typename Request>
bool CoordinatorServiceImpl::AllowContinue(const ResponseHeader &header) const
{
    return header.state() == ResponseHeader::SERVING;
}

template <typename Request>
bool CoordinatorServiceImpl::AllowRecoveryControl(const ResponseHeader &header) const
{
    return header.state() == ResponseHeader::RECOVERING || header.state() == ResponseHeader::SERVING;
}

template <>
inline bool CoordinatorServiceImpl::AllowContinue<KeepAliveReqPb>(const ResponseHeader &header) const
{
    return AllowRecoveryControl<KeepAliveReqPb>(header);
}

template <>
inline bool CoordinatorServiceImpl::AllowContinue<EnsureLeaderMembershipReqPb>(const ResponseHeader &header) const
{
    return AllowRecoveryControl<EnsureLeaderMembershipReqPb>(header);
}

template <>
inline bool CoordinatorServiceImpl::AllowContinue<ReportTopologyRecoveryCandidateReqPb>(
    const ResponseHeader &header) const
{
    return AllowRecoveryControl<ReportTopologyRecoveryCandidateReqPb>(header);
}

}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H
