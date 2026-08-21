# Coordinator Election Current Implementation Record

> **Status:** Current source-backed implementation and test-path record. This file documents checked-in behavior, not a future design plan.

## Implemented Architecture

- `CoordinatorRuntime` is non-singleton and owns at most one active blocking attempt, one Service per attempt, one committed lifecycle, callbacks, leader observation, and instance-local event-loop/`Stop()` state.
- `CoordinatorServer` is the singleton public façade and owns one `CoordinatorRuntime` for compatibility.
- `CoordinatorOptions` contains a config path, Discovery provider, expected member count, and paired `onStart`/`onStop` callbacks.
- `CoordinatorServer` is the production singleton façade, owns one Runtime, and rejects an empty parameterized `configFilePath` before delegation. Production supports one Coordinator Runtime per process.
- `CoordinatorRuntime` passes a non-empty path directly to `FlagManager::ParseConfigFile`; parse failures return the `K_INVALID` error built from the configured path and parser message.
- `CoordinatorRuntime` owns one long-lived `DynamicFlagConfig` and `DynamicConfigUpdater`; `UpdateConfig()` is admitted
  only after Service `Init()`/`Start()`, `onStart`, and election-manager startup and is serialized with `Stop()` by a
  dedicated config mutex. Its capability filter accepts only `request_sample_rate`, `access_sample_rate`, and
  `diagnostic_sample_rate`, whose runtime state is atomically published by `LogSampler`. Other dynamic gflags are
  rejected until all Coordinator consumers support synchronized live update. Changes are process-local and are not
  distributed or persisted; lifecycle rejections are audited without logging the JSON payload.
- The no-argument dscli-compatible path reads process flags directly: empty `coordinator_raft_initial_peers` means single-node no-election mode; non-empty static peers are converted to `StaticCoordinatorDiscovery` and can start election through the Service election path. An empty path on the parameterized Runtime overload skips parsing and consumes process flags prepared by the caller; that direct Runtime path is internal and used by in-process tests only.
- After parsing succeeds or is skipped, `CoordinatorRuntime::GetRaftFlags()` is called exactly once for that attempt and supplies the local endpoint, exclusive data root, election timeout, and membership timing.
- In-process multi-Runtime fixtures set common flags before launch, keep them unchanged while any Runtime is active, and restore them only after every Runtime stops and joins.
- `CoordinatorServiceImpl` owns the shared business/braft brpc server and a two-stage election startup.
- `RegisterCoordinatorRaftServices` is called by the Service/shared-server owner. Registration is not a Node or Manager API.
- `CoordinatorElectionManager` is the sole owner of bootstrap state/control. Its worker first collects every Discovery-visible bootstrap observation, then selects Bootstrap, Recover, or WaitingToJoin before starting Node then Membership; shutdown requests worker stop and joins bootstrap control before Membership-before-Node destruction.

## Current Startup Path

```text
Service Init
-> Service Start: register business and braft services, listen, state STARTING
-> Runtime onStart: register endpoint
-> Service StartElectionManager: construct/publish Manager, start its bootstrap worker, publish lifecycle RUNNING
-> Manager worker: recover locally or collect all visible Discovery/peer states and decide, then start Node and Membership
-> braft Leader callback: open business gate only on the Leader
-> Runtime event loop
```

Detailed order:

1. `CoordinatorServer::InitAndRun(options)` rejects an empty `configFilePath` and delegates to its owned Runtime.
2. `CoordinatorRuntime::InitAndRun(options)` validates required Discovery/member-count inputs and paired callbacks. A Runtime instance is one-shot by interface contract; callers must not invoke `InitAndRun` concurrently or more than once on the same instance.
3. Runtime calls `FlagManager::ParseConfigFile` when the path is non-empty. It skips parsing when the path is empty. Parse failure returns the parser error with the configured path before a Raft snapshot is requested.
4. After parse success or skip, Runtime captures one Raft snapshot plus the config-log snapshot, parses its local address, constructs one `CoordinatorServiceImpl`, and completes `Service::Init()` plus `Service::Start()`.
5. `Service::Start()` builds the RPC server, registers the business adapter, registers braft services through `RegisterCoordinatorRaftServices`, starts listening, and leaves election-enabled startup in `STARTING`; no-election startup publishes `RUNNING` directly.
6. Runtime publishes Service ownership, invokes `onStart` without its mutex held, and emits the captured config log snapshot before election startup.
7. Runtime calls `Service::StartElectionManager()` after callbacks. If `Service::IsElectionConfigured()` is false, the Service returns `OK` without publishing an election Manager. If election is configured, `StartElectionManager()` reserves one attempt, constructs and publishes the Manager under its lifecycle mutex, starts the Manager bootstrap worker without that mutex, then publishes Service `RUNNING`. The worker owns local metadata probing, complete Discovery-visible peer observation, startup-plan selection, and eventual Node/Membership startup.
8. After Service startup, Runtime enters its condition-variable event loop until that Runtime's `Stop()` or a process termination signal. Election-enabled `RUNNING` does not imply Raft leadership or business readiness.
9. Cleanup invokes `onStop` after an attempted `onStart` and destroys the Service after its shutdown attempt. Retry requires a new Runtime instance.

All election-enabled business RPCs return `K_NOT_READY` until the local Node is the current braft Leader. `GetRaftBootstrapState` remains available independently of that business gate once the Manager is published. `StartElectionManager()` publishes lifecycle `RUNNING` after bootstrap control starts, so one-candidate shortage, peer/digest disagreement, and WaitingToJoin are normal asynchronous non-serving states rather than synchronous `InitAndRun` failures. A synchronous Manager-start failure retains the listening endpoint until Runtime invokes `onStop` and explicit Service shutdown. Concurrent Shutdown waits for an in-progress attempt to publish completion before teardown.

For valid local metadata, the worker directly starts `RecoverPlan` and never queries first-bootstrap Discovery. After braft successfully initializes a non-empty `BootstrapPlan`, the Node publishes lifecycle `STARTED` but does not synthesize `initial_conf` as committed index 0; only real braft configuration callbacks publish committed membership. For local `ABSENT`, one bootstrap round probes every normalized Discovery-visible candidate with the bounded peer RPC, including candidates after the deterministic first `N` and views below bootstrap quorum. The Manager decides from the complete observation set: one converged valid non-empty committed configuration takes priority, becomes `BootstrapPlan` when it includes local so braft can rebuild local election/membership metadata, and becomes `WaitingToJoinPlan` when it excludes local. Conflicting normalized full lists, including an `N` view versus a transitional `N + 1` view, return retryable `K_NOT_READY` and create no Node until they converge, except that local `ABSENT` may rebuild from a below-`N` committed candidate subset that is contained in the current normalized candidates and includes local. A successful remote observation of `UNKNOWN` or `CORRUPT` metadata is non-authoritative and retryable; local `UNKNOWN`/`CORRUPT` remains terminal. Only when no observation reports a real non-empty committed configuration, at least `floor(N / 2) + 1` candidates are bootstrappable, and local is in the deterministic first `min(N, bootstrappable_count)` may first bootstrap proceed. A candidate is bootstrappable when it is observable, agrees on the full-view candidate count/digest plus immutable group/target identity, and reports either fresh `ABSENT` metadata or `VALID` metadata with no committed configuration from an in-progress staggered bootstrap. Unavailable observations are excluded from first-bootstrap selection and do not block when a matching target majority remains; successful invalid, `UNKNOWN`/`CORRUPT`, or digest-mismatched observations keep the Manager retrying with the business gate closed. Counts from quorum through `N` use every bootstrappable candidate; counts above `N` select the first `N` bootstrappable endpoints after the full probe.

`ICoordinatorDiscovery::GetCoordinators` remains a synchronous provider API whose implementations must return within a finite provider-controlled bound. Manager shutdown observes its stop request only between calls and joins after an in-flight call returns; a provider that violates the bounded-return contract can block Manager and Service shutdown.

## Current Shutdown Path

```text
Runtime onStop: provider callback records delayed 10-second unregister
-> Service Shutdown waits for election startup under lifecycle mutex
-> first caller publishes STOPPING and moves Manager ownership under lifecycle mutex
-> Service unlocks; Manager joins bootstrap control, shuts down Membership, and drains Node
-> shared server Stop/Join without Service lifecycle mutex
-> brpc adapter and remaining components without Service lifecycle mutex
-> relock only to publish STOPPED/the saved first cleanup status and notify concurrent Shutdown callers
```

`CoordinatorRuntime::Stop()` closes `UpdateConfig()` admission, sets only that Runtime's `stopRequested_`, and notifies only its `stopCv_`; it does not change `g_exitFlag`. The process signal handler only sets `g_exitFlag`, and each bounded Runtime event loop observes it and closes update admission before cleanup. After the event loop exits, Runtime invokes `onStop` once when `onStart` was attempted, then shuts down its Service and completes the lifecycle future/thread.

The unregister delay belongs to provider callback semantics. The in-process Discovery mock records an expiration deadline and returns immediately. It retains the endpoint for 10 seconds and removes it lazily on a later query; test-only virtual time avoids a real 10-second sleep.

Public `CoordinatorServiceImpl::Shutdown()` serializes cleanup ownership with the Service lifecycle mutex and condition variable. It waits for `electionStartInProgress_` to clear before taking the published Manager, then release-stores `STOPPING` and moves `electionManager_` to owner-local storage before unlocking. Manager bootstrap stop/join, Membership shutdown, Node drain, shared-server Stop/Join, adapter destruction, and remaining component cleanup all run without the Service lifecycle mutex. A Discovery callback or accepted bootstrap RPC can therefore enter the Service, acquire the mutex, observe `STOPPING`, and return `K_SHUTTING_DOWN` while brpc Join drains it. The owner reacquires the mutex only to publish `STOPPED`, save the first cleanup status, and wake waiters. Concurrent or repeated public shutdown callers wait and return that same status without touching the server. `Init()`/RPC `Start()` failure cleanup uses the same lock-free cleanup execution while keeping the original startup status authoritative.

## Bootstrap RPC And Serving Contract

The Manager snapshot publishes these source-backed states:

| Event | Phase | Status code |
| --- | --- | ---: |
| worker starts observing | `OBSERVING` | `K_OK` |
| retryable Discovery, peer, or conflicting-configuration observation | `RETRYING` | `K_NOT_READY` |
| Node and Membership start successfully | `STARTED` | `K_OK` |
| first terminal local inconsistency or startup failure | `TERMINAL` | original stable `StatusCode` |

`TERMINAL` is a one-way Manager state. Generic bootstrap publication ignores later non-terminal phases, Node and Membership startup recheck terminal state after external calls return, and final ownership plus `STARTED` publication is decided with lifecycle mutex before bootstrap mutex. Cleanup of unpublished Membership/Node instances runs after both locks are released; ownership already published before a later error remains available to `Shutdown()` while the business gate is closed.

`CoordinatorServiceImpl::GetRaftBootstrapState` copies the Manager value snapshot under the lifecycle lock, releases that lock, then encodes protobuf fields. The response includes phase and numeric `status_code`; it has no raw Status text or Raft data-directory field. The custom ZMQ transport depends on protobuf method indexes, so `ReportTopologyRecoveryCandidate` remains index 7, `GetClusterRawSnapshot` remains index 8, and the appended `GetRaftBootstrapState` is index 9.

Service `RUNNING` and Runtime startup mean the endpoint is active and the background Manager worker started. They do not mean election completion or business readiness. `CoordinatorServiceImpl::CheckServing()` opens business access only while the local serving gate is open and the election manager still reports the local Node as current braft Leader.

## Replication Boundary

The current `CoordinatorRaftStateMachine::on_apply` rejects management-log apply, and `CoordinatorRaftNode` exposes braft operations only for voting-member `AddPeer` and `RemovePeer`. This Raft group therefore persists election and committed voting-membership state; it does not replicate Coordinator business key/value or topology payloads. Business RPC admission is determined by the local current-Leader gate.

## Current Membership Reconciliation

`CoordinatorMembershipManager` owns one reconciliation thread. Committed configuration B is the only membership authority; Discovery list A supplies candidates only, and fixed target N comes from the owner.

1. Only the current Leader submits membership changes.
2. Every round rebuilds policy from the current term, committed B, and follower health; callback status never advances policy.
3. `B.size() < N` fills one vacancy with Add only.
4. Full B with a confirmed failure submits Add first, observes committed `N + 1`, then removes the exact failed peer.
5. Add/Remove callbacks are diagnostic-only and capture no Manager state; braft serializes configuration changes.
6. Candidate selection prefers never-attempted peers, then the oldest Add-attempt timestamp, with normalized identity as tie-break.
7. A replacement intent records the failed peer, original committed peers, and synchronously accepted candidate submissions. Rollback ownership requires an exact committed difference that belongs to that candidate set.
8. Rejected or throwing Add submissions do not establish rollback ownership.
9. Fresh pre-submission status must preserve leadership, term, configuration index, committed peers, known quorum, and the selected failure/rollback policy; stale decisions return `K_TRY_AGAIN` without submission.
10. Discovery failure, no candidate, unknown health, quorum loss, stale policy, and unexplained over-target membership fail closed without removal.
11. Shutdown interrupts the reconciliation wait and lifecycle is rechecked before Discovery and before Add/Remove submission.

## Current File Responsibilities

| Path | Responsibility |
| --- | --- |
| `include/datasystem/coordinator_server.h` | Public `CoordinatorOptions` and singleton `CoordinatorServer` façade. |
| `src/datasystem/coordinator/coordinator_runtime.h/.cpp` | Non-singleton lifecycle, optional config parsing, per-attempt flag snapshot, callbacks, event loop, Stop, and leader forwarding. |
| `src/datasystem/coordinator/coordinator_server.cpp` | Singleton façade delegation to one Runtime. |
| `src/datasystem/coordinator/coordinator_service_impl.h/.cpp` | Business components, two-stage startup, shared-server ownership, readiness, election construction, and ordered shutdown. |
| `src/datasystem/coordinator/raft/coordinator_raft_service.h/.cpp` | Service-owned braft service registrar. |
| `src/datasystem/coordinator/raft/coordinator_election_manager.h/.cpp` | Sole bootstrap-state/control owner, startup-plan convergence, Node/Membership ownership, and lifecycle order. |
| `src/datasystem/coordinator/raft/coordinator_raft_node.h/.cpp` | braft Node/FSM mechanism, observation, exclusive single-operation membership admission, operations, and drain. |
| `src/datasystem/coordinator/raft/coordinator_membership_manager.h/.cpp` | Dynamic-Discovery policy, health/grace tracking, least-recently-attempted candidate scheduling, term-local replacement proof, fresh submission revalidation, and fail-closed recovery. |
| `tests/st/common/raft/coordinator_runtime_election_test.cpp` | Real in-process multi-Runtime bootstrap, serving-gate, membership, failure, and recovery integration tests. |

## In-Process Runtime Election ST

`tests/st/common/raft/coordinator_runtime_election_test.cpp` uses real in-process Runtime lifecycles with empty config paths and real shared brpc/braft endpoints. Its fixture sets `FLAGS_use_brpc=true` before launch, keeps common flags stable until every Runtime stops and joins, reserves four loopback endpoints, supplies exclusive Raft roots and short per-Runtime timing snapshots, and mocks only `ICoordinatorDiscovery` behavior. Bootstrap and membership are observed through the existing Coordinator bootstrap RPC; business readiness is observed through `GetCoordinatorId`.

| Test | Source-backed contract |
| --- | --- |
| `OneOfThreeCandidateWaitsWithoutSynchronousStartupFailure` | One fresh candidate remains lifecycle-active for three election timeouts, publishes a diagnosable absent/no-configuration bootstrap snapshot, has no Leader, and returns `K_NOT_READY` from business RPC. |
| `TwoOfThreeCandidatesBootstrapAtTargetQuorum` | Two of target three commit the same two-peer initial configuration, elect one Leader, and open only that Leader's business gate, without requiring the third static peer to be observable during first bootstrap. |
| `LaterCandidateFillsBootstrapVacancyWithoutRemoval` | A third fresh Runtime is added to the two-peer cluster; every observed configuration preserves both bootstrap peers and the final configuration reaches target three. |
| `ExtraCandidateWaitsOutsideFirstExpectedPeers` | Four candidates with target three select the first three normalized endpoints independent of launch order; all three selected peers converge on the same committed configuration, while the fourth has valid waiting metadata, no committed peers, no leadership, and a closed gate. |
| `WaitingCandidateReplacesFailedFollowerAddBeforeRemove` | After one selected follower stops, polling must never observe removal before waiting-candidate admission, and the final unique-Leader configuration must contain the waiting peer and exclude the failed peer. The ST does not require sampling the transient `N + 1` configuration; deterministic Add-before-Remove ordering is covered by Membership Manager UTs. |
| `InconsistentBootstrapDigestsNeverCreateConfiguration` | Caller-specific two-peer views produce three distinct digests; all lifecycles remain active and non-serving with no committed configuration through a three-election-timeout isolation window. |
| `RunningClusterSurvivesDiscoveryFailureWithoutMembershipChange` | A follower failure forces a candidate query; injected Discovery failure leaves the original three-peer committed configuration and healthy Leader gate unchanged and does not remove the failed voter. |
| `LeaderFailoverRestartsOldLeaderAsPersistedFollower` | Relaunches generation 2 on the old Leader's exact endpoint/data root, requires recovery as follower with the valid committed configuration, and asserts zero queries on that generation's provider while sharing cluster registration/delayed-unregister state. |
| `PersistedFollowerRestartMaintainsUniqueServingLeader` | Relaunches generation 2 on the follower's exact endpoint/data root, asserts zero queries on that generation's provider while sharing cluster registration/delayed-unregister state, requires the original Leader to keep serving during follower downtime, then permits any unique Leader after restart while checking only its gate is open and all members expose the committed configuration. |
| `QuorumLossClosesServingGateUntilOriginalMemberRestarts` | Records monotonic time immediately after the second member Stop/join, derives an independent deadline from the surviving Leader generation's configured election timeout, and requires `IsLeader() == false`, `GetLeader() == K_NOT_READY` with no endpoint, and business RPC `K_NOT_READY` within at most two election timeouts before recovery. |

Polling is deadline-based with a bounded 20ms interval; there is no wall-clock 10-second unregister wait. Runtime replacement polling intentionally does not require observing the transient `N + 1` configuration because the deterministic ordering contract is exercised directly by Membership Manager UTs. Every case body has one 6-second deadline, while mandatory teardown Stop/join cleanup is not claimed to be inside that body budget; the quorum-loss observation has the stricter relative deadline described above, and each complete case is bounded by the CTest 8-second timeout. Failure diagnostics include registered candidates, Discovery status/query count, per-Runtime lifecycle/Leader state, bootstrap metadata/count/digest/committed peers, and last business/bootstrap RPC status.

Deleting a member's braft data and restarting the same peer in place is excluded from the supported recovery contract because of braft issue #340. In container deployments, replace the failed container with a new instance; while a Leader exists, membership reconciliation removes the failed peer and adds the replacement.

## Build Declaration Record

CMake:

- excludes the new source from generic `DS_TEST_ST_SRCS`;
- builds `coordinator_runtime_election_test` with `_ds_st_main_obj`;
- directly links the Runtime, test harness, port allocator, Raft types, flags/signal/util, braft, and brpc closure;
- registers concrete gtest cases with `TIMEOUT 8`;
- includes `coordinator_raft_service` in `DS_UT_DEPEND_LIBS`; the Node UT calls the registrar and links `test_port_allocator` through `ds_ut` for collision-safe real brpc/braft startup coverage;
- directly links `test_port_allocator`, `common_flags`, `common_rpc_zmq`, `common_signal`, and `coordinator_protos` into the focused `coordinator_server_options_test` executable;
- registers both `coordinator_server_options_test` and `coordinator_election_manager_test` with `TIMEOUT 8`.

Bazel:

- declares `//tests/st/common/raft:coordinator_runtime_election_test` as a dedicated short `ds_cc_test`;
- the focused Coordinator options UT uses `timeout = "short"`, directly depends on `//tests/st/cluster:test_port_allocator`, and retains dynamic-flags/signal, `brpc_factory`, and Coordinator brpc dependencies for real-server lifecycle regressions;
- `coordinator_election_manager_test` uses `timeout = "short"`;
- `coordinator_raft_node_test` directly depends on `coordinator_raft_service` and `test_port_allocator` for collision-safe real brpc/braft startup coverage;
- the Runtime election ST directly depends on `coordinator_runtime`, `coordinator_raft_types`, `common_test`, `test_port_allocator`, DataSystem dynamic-flags/signal/status targets, and butil.

Focused CMake selection:

```bash
make -j30
ctest --output-on-failure --timeout 8 \
  -R 'RaftOperationSubmissionGateTest|CoordinatorRaftTypesTest|CoordinatorRaftStateMachineTest|CoordinatorRaftNodeTest|CoordinatorMembershipManagerTest|CoordinatorElectionManagerTest|CoordinatorElectionServiceTest|CoordinatorServerOptionsTest|CoordinatorRaftMembershipTest|CoordinatorServiceElectionTest|CoordinatorRuntimeElectionTest|BraftReplayTest|BraftClusterTest'
```

## Risk And Ownership Record

| Surface | Current conclusion |
| --- | --- |
| Common process flags | Production starts one Runtime per process through the façade and parses its non-empty path before the per-attempt snapshot. Internal multi-Runtime tests prepare common flags before launch, never mutate them while any Runtime is active, and restore them after all threads join. Concurrent mutation is outside the supported contract; parse failures are sanitized. |
| Runtime isolation | Service, stop state, condition variable, callbacks, lifecycle thread, and one `GetRaftFlags()` snapshot belong to one Runtime instance. `InitAndRun` is one-shot by interface contract; retry requires a new Runtime instance. |
| Callback lifetime | Runtime moves each owned callback exactly once before invocation; test callbacks capture shared Discovery and endpoint values. |
| Discovery shared state | The no-argument mock can share one mutex-protected registration/generation/expiration/barrier/callback state across provider instances while retaining per-provider failure, fixed-snapshot, and query observation. Lifecycle callbacks and Manager queries use the same provider instance for every Runtime. Caller-specific fixed views are explicit digest-mismatch fault injection and do not claim normal global convergence. No external callback runs under the shared mock mutex. `GetCoordinators` is synchronous and must return within a provider-controlled finite bound; contract violations can block shutdown. |
| Membership mutation admission | `CoordinatorRaftNode` uses its operation drain token as an exclusive Add/Remove gate. While the token is held, the Manager skips reconciliation before reading committed status; completion publishes committed configuration before releasing the token, so the next round rebuilds from the new B. A busy Node returns `K_TRY_AGAIN`, shutdown first stops admission and then drains the accepted operation, and Manager callbacks remain diagnostic-only. |
| Service readiness | Election-enabled `Start()` leaves `STARTING`; `StartElectionManager()` publishes Manager/bootstrap observability and lifecycle `RUNNING`, while the atomic Raft gate opens only for the braft Leader. Bootstrap waiters, WaitingToJoin nodes, followers, quorum-lost nodes, and asynchronous bootstrap errors remain non-serving. |
| Server ownership and shutdown | The first public Shutdown caller release-publishes `STOPPING` and transfers Manager ownership under the Service lifecycle mutex, then drains Manager/Membership/Node and stops/joins the shared server without that mutex. Adapter/components remain alive through active RPC drain; the owner relocks only to publish `STOPPED` and the saved first cleanup status. Concurrent callers wait and receive that status. |
| Persistent identity | Each Runtime snapshot binds one endpoint to one exclusive data root for the lifecycle. Valid metadata recovers locally; peer-assisted rebuild is restricted to local `ABSENT`, while local `CORRUPT`/`UNKNOWN` is terminal. Persisted restart STs use a generation-local provider query count of zero to prove local recovery; persisted-follower downtime preserves the original serving Leader, while post-restart role selection is unconstrained beyond one serving Leader and gated followers. The deleted-root follower ST requires a positive count and the original authoritative three-peer configuration. |
| RPC compatibility | Bootstrap diagnostics expose only phase and stable numeric status code. Coordinator ZMQ method indexes remain 7/8 for legacy methods and 9 for the appended bootstrap RPC. |
| Business boundary | The Raft state machine does not apply business logs; business access is determined by the local current-Leader gate, and Coordinator Raft does not replicate business data. |
| Test bound | Manager and Server-options focused UTs have CTest `TIMEOUT 8` and Bazel short timeouts; the Server-options fixture uses allocator-held ports through real brpc teardown. Each Runtime ST body deadline is 6 seconds; quorum-loss stepdown/gate closure is additionally bounded from the second Stop/join by at most `2 * GetRaftFlags().electionTimeoutMs`; each concrete CTest timeout is 8 seconds. |

No context directory or module boundary changed, so module metadata and generated repository indexes are unchanged.
