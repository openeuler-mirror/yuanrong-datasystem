# Coordinator Raft Node Current Implementation Record

> **Status:** Canonical current implementation and checked-in test-coverage record. Source code and the module `design.md` remain the final authority. This document is not an active implementation plan and contains no pending development sequence.

## Implemented Goal

The standalone `CoordinatorRaftNode` owns braft registration, startup, recovery, leader/configuration observation, health observation, asynchronous single-peer membership operations, synchronous destructor drain, and callback exception containment while sharing the Coordinator brpc server. The standalone `CoordinatorMembershipManager` owns dynamic-Discovery vacancy filling and add-before-remove failed-member replacement above the Node. Product integration remains future work.

## Architecture Summary

- `coordinator_raft_peer.*` owns stable numeric IPv4/VIP parsing and normalized public formatting.
- `coordinator_raft_types.*` owns fixed GroupId, startup-plan types, and exact metadata/start-plan validation.
- `CoordinatorRaftNode` solely owns `braft::Node` and `CoordinatorRaftStateMachine`.
- The external integration owner solely owns Node lifetime. Node destruction is the only synchronous Node close entry and calls private `ShutdownInternal()`.
- FSM and Add/Remove callbacks may notify the external owner but must not destroy the Node or retain a strong reference that could become its final owner.
- `RaftOperationSubmissionGate` handles inline completion deferral, first-result-wins, outside-lock user callback dispatch, catch-all exception containment, and one wrapper-owned callback drain token; callback invocation holds neither the Gate mutex nor `lifecycleMutex_`.
- The committed-configuration callback publishes an immutable normalized snapshot; policy consumes that snapshot rather than transient braft configuration.
- `CoordinatorMembershipManager` owns dynamic membership policy, one background thread, one operation at a time, a callback-safe completion mailbox, and a fresh status/policy gate immediately before every Add/Remove submission.

## Current File Responsibilities

| Path | Responsibility |
| --- | --- |
| `src/datasystem/coordinator/raft/coordinator_raft_peer.h/.cpp` | Parse and normalize stable numeric IPv4/VIP peers; hide braft index `0`. |
| `src/datasystem/coordinator/raft/coordinator_raft_types.h/.cpp` | Fixed GroupId, startup-plan types, inclusive election-timeout bounds, and metadata/start-plan validation. |
| `src/datasystem/coordinator/raft/coordinator_raft_node.h/.cpp` | Shared brpc registration, Node/FSM ownership, destructor drain, lifecycle queries, health projection, committed snapshots, and async peer operations. |
| `src/datasystem/coordinator/raft/coordinator_raft_operation.h` | Submission Gate, inline-result deferral, duplicate-result suppression, outside-lock callback dispatch, and catch-all operation callback boundary. |
| `src/datasystem/coordinator/raft/coordinator_raft_state_machine.h/.cpp` | Synchronous FSM event forwarding and catch-all user-exception boundaries. |
| `src/datasystem/coordinator/raft/coordinator_membership_manager.h/.cpp` | Dynamic-Discovery policy, health/grace tracking, serialized operations, term rebuild, bounded replacement intent, and fail-closed recovery. |
| `src/datasystem/coordinator/raft/CMakeLists.txt` | Test-gated CMake libraries; no product package wiring. |
| `src/datasystem/coordinator/raft/BUILD.bazel` | Mirrored direct Bazel dependencies for Node and Manager. |
| `tests/ut/common/coordinator/*` | Focused deterministic codec, FSM, Node/Gate, and Manager tests. |
| `tests/st/common/raft/coordinator_raft_node_test.cpp` | Real braft Node and Manager scenarios. |
| `tests/st/common/raft/braft_cluster_test.cpp` | Raw braft election and unsupported-log replay regression. |

## Peer Codec And Stable Identity

The implementation fixes `kCoordinatorRaftGroupId` to `datasystem-coordinator`. `CoordinatorRaftOptions` does not accept a caller-supplied group.

`ParseCoordinatorRaftPeer`:

1. accepts one stable numeric IPv4 and one decimal port;
2. rejects empty, whitespace-modified, malformed, domain, IPv6, wildcard, zero/out-of-range port, and nonzero-index identities;
3. calls braft parsing only after Coordinator validation;
4. requires a non-empty `PeerId` with index `0`;
5. normalizes accepted output through `CoordinatorRaftPeerAddress`.

Bootstrap validation normalizes every initial peer, rejects duplicates after normalization, and requires the local peer to be present. Diagnostics avoid logging raw rejected identity payloads.

Stable deployment identity remains `IPv4/VIP ↔ PeerId ↔ exclusive data root/PVC`. The same identity and persisted data must not be active in two processes.

## Startup And Shared Service Registration

Registration and startup are separate:

1. owner creates a brpc `RpcServer` generation and registers business services;
2. Node registers braft services through `RpcServer::AddBrpcServices`;
3. owner starts the shared server;
4. owner calls `Start` with verified metadata state.

Validation failures before `AddBrpcServices` leave the Node `CONSTRUCTED`. If `AddBrpcServices` fails after server mutation begins, the Node becomes `STOPPED` and the complete server generation must be discarded. The implementation does not attempt braft service rollback or retry against the same server.

Startup mapping is exact:

| Plan | Metadata | Initial configuration |
| --- | --- | --- |
| `BOOTSTRAP` | `ABSENT` | full normalized bootstrap peers |
| `RECOVER` | `VALID` | empty |
| `WAITING_TO_JOIN` | `ABSENT` | empty |

Corrupt metadata returns `K_DATA_INCONSISTENCY`; unknown metadata returns `K_NOT_READY`. Recovery never falls back to bootstrap.

braft options use local log, raft-meta, and snapshot directories under the supplied data root; CLI is disabled and periodic snapshots are disabled.

## Node Lifetime And Destruction

`CoordinatorRaftNode` has no public synchronous close method. One external owner destroys it only after stopping upper-layer submissions and fully destroying the Manager that borrows it.

Required owner order:

1. revoke business serving and stop new work;
2. call Manager `Shutdown()` when explicit control is needed and destroy `CoordinatorMembershipManager`;
3. destroy `CoordinatorRaftNode`;
4. after the Node destructor returns, stop and destroy the shared brpc server.

The Node destructor calls private `ShutdownInternal()`:

1. lock `lifecycleMutex_`;
2. return immediately if already `STOPPED`;
3. stop accepting Add/Remove completion tokens, publish `STOPPING`, and move Node/FSM ownership to locals;
4. release `lifecycleMutex_`;
5. call `braft::Node::shutdown(nullptr)` and `join()`;
6. wait for all admitted wrapper-owned completion callbacks to return and release their tokens;
7. reset the Node;
8. reset the borrowed FSM;
9. clear committed configuration under its dedicated mutex;
10. publish `STOPPED` under `lifecycleMutex_`.

The shared server remains alive through this sequence. A failed `braft::Node::init` uses braft v1.1.2 Node destruction as canonical partial-init cleanup, then resets the local FSM and snapshot before publishing `STOPPED`.

Destruction is not supported concurrently with `Start`, any query, `AddPeer`, or `RemovePeer`. The external owner must serialize those calls. This is a lifetime contract, not an operation for callbacks to coordinate.

All Node callbacks are notification-only for lifetime. They must not destroy the Node and must not retain a strong owner that can become the final owner. In particular, braft's `onShutdown` callback can execute while the destructor is joining the FSM queue, so it may only signal an already-independent owner path.

## Committed Configuration And Health Projection

`CoordinatorRaftNode` wraps the StateMachine configuration callback to:

1. require a positive committed index and non-empty peer list;
2. validate exact internal PeerId shape and index `0`;
3. normalize peers through the Coordinator codec;
4. reject duplicate normalized identities;
5. sort and publish an immutable snapshot under `committedConfigurationMutex_`;
6. release the mutex before invoking the user callback.

`GetCommittedConfiguration` returns `K_NOT_READY` before the first valid snapshot. `braft::Node::list_peers()` is not used as committed authority.

`GetMembershipStatus` holds Node lifetime through `lifecycleMutex_`, reads `braft::NodeStatus`, copies the committed snapshot under `committedConfigurationMutex_`, converts `stable_followers` to Coordinator DTOs, intersects them with committed B, sorts them, and ignores `unstable_followers` as membership authority.

## FSM Callback Boundaries

The five StateMachine callback entries are:

- `onLeaderStart`;
- `onLeaderStop`;
- `onConfigurationCommitted`;
- `onShutdown`;
- `onError`.

Ordinary callback exceptions are converted to one generic `K_RUNTIME_ERROR` report. A throwing `onError` is swallowed and does not recurse. Standard and non-standard exceptions are caught. Diagnostics use the fixed `Coordinator raft callback failure` marker, append `exception.what()` for standard exceptions, retain only the marker for non-standard exceptions, and omit user callback payloads.

Committed-configuration validation has an independent catch-all report boundary so invalid internal configuration never reaches the user configuration callback and a throwing `onError` cannot escape back into braft.

Callbacks execute without `lifecycleMutex_`, `committedConfigurationMutex_`, or the operation Gate mutex held.

## AddPeer, RemovePeer, And Submission Gate

Each membership API submits exactly one braft operation after checking:

- Node state is `STARTED` and the owned braft Node exists;
- callback is non-empty;
- target peer is valid and normalized;
- `RemovePeer` is not removing the sole committed voter;
- local Node reports Leader state.

Submission remains under `lifecycleMutex_` until the braft call returns. braft normally completes on a completion bthread but may run the closure inline if bthread creation fails.

`RaftOperationSubmissionGate` implements the ordering boundary:

1. if completion arrives before submission finishes, store one callback/result pair;
2. after Node submission releases `lifecycleMutex_`, `MarkSubmissionComplete` dispatches any pending result;
3. if submission was already marked complete, dispatch completion directly;
4. `resultReceived_` accepts only the first completion;
5. both paths release the Gate mutex before invoking user code;
6. the callback invocation helper catches all exceptions, logging the fixed marker plus `exception.what()` for standard exceptions and only the fixed marker for non-standard exceptions.

The self-deleting braft closure owns operation metadata and the Gate. It captures no Node wrapper pointer. The Gate-owned tracked callback retains the drain token through user callback return. `Run` converts braft status, moves the Gate to a local, deletes the closure, then calls `DispatchOrDefer`.

## Recovery And Unsupported Logs

braft owns term, vote, log, and committed configuration under the configured data root. The adapter does not rewrite or repair metadata.

- `RECOVER` supplies empty initial configuration and loads persisted state.
- `WAITING_TO_JOIN` also supplies empty initial configuration but requires absent metadata and cannot self-elect.
- directory creation, permissions, corrupt metadata, and braft init failures leave the Node not ready or terminally stopped.
- `CoordinatorRaftStateMachine::on_apply` has no management-record codec. Unsupported records fail with `ESTATEMACHINE`.
- restart replays an unsupported record and remains fail closed rather than silently advancing to a usable Leader.
- periodic snapshots stay disabled until codec and snapshot support are implemented.

## Membership Manager

The standalone Manager is constructed only for dynamic Discovery. Fixed target count N is supplied by its owner. Committed configuration B is voting authority; Discovery A is candidate input only.

Implemented policy:

1. only a current Leader submits membership changes;
2. `consecutive_error_times > 5` starts suspected-failure observation;
3. the observation must remain continuous for `memberFailureGrace`;
4. a new term or new Leader restarts the complete grace interval;
5. healthy full B performs no Discovery call;
6. `B.size() < N` fills one vacancy with Add only;
7. full B with a confirmed failure adds a candidate, verifies committed `N+1`, then removes the exact failed peer and verifies final `N`;
8. callback result is never membership authority; later committed state is always checked;
9. one Manager permits one operation at a time;
10. no candidate, Discovery failure, unknown health, loss of quorum, stale term, or failed Add cannot authorize Remove;
11. immediately before every Add/Remove submission, fetch fresh status and reject stale leadership, term, configuration index, committed peers, quorum, failed-member state, or rollback policy with `K_TRY_AGAIN`;
12. unexplained over-target B fails closed unless an exact safe target is proven;
13. Manager policy state is not persisted and is rebuilt from committed B.

The Manager's Node reference is non-owned. The Node must outlive complete Manager destruction, whether shutdown was requested explicitly or initiated by the Manager destructor. Manager destruction completes before Node destruction begins.

## Real All-Node Manager Leader Failover

`CoordinatorRaftMembershipTest.ManagersOnAllNodesReplaceFailedLeaderWithDiscoveredCandidate` covers the realistic ownership and policy topology:

1. four Nodes are running: three bootstrap voters and one waiting candidate;
2. all four Nodes run `CoordinatorMembershipManager` with target `N=3`;
3. healthy full membership keeps Discovery call count at zero;
4. the old Leader's Manager is stopped and destroyed, then the old Leader Node is destroyed;
5. a surviving Node becomes the new Leader;
6. the new Leader observes the old Leader above the braft error threshold but performs no Discovery before a complete new grace interval;
7. Discovery supplies the waiting candidate;
8. committed history proves candidate membership at `N+1` before removal;
9. removal targets the destroyed old Leader and final committed membership is `N`.

This supplements single-Leader Manager scenarios by proving term-local grace rebuilding and policy activation when Managers already run on every Node.

## Build Declaration Record

The CMake adapter and Manager remain under `WITH_TESTS`; there is no production consumer or package output. Existing CMake dependencies already match the implementation's direct includes, so no CMake declaration update is required.

The Bazel `coordinator_raft_node` target directly depends on:

- `coordinator_raft_peer`;
- `coordinator_raft_state_machine`;
- `coordinator_raft_types`;
- DataSystem utility/status/logging/filesystem/string targets;
- `rpc_server`;
- braft and brpc.

## Current Checked-In Test Coverage

| Test source | Contract encoded by checked-in tests |
| --- | --- |
| `coordinator_raft_types_test.cpp` | peer normalization/rejection, fixed identity, exact startup metadata mapping, and election-timeout lower/upper boundaries plus adjacent invalid values |
| `coordinator_raft_state_machine_test.cpp` | five FSM event paths, catch-all containment, one-time reporting, standard-exception detail logging, non-standard exception fallback, empty callbacks |
| `coordinator_raft_node_test.cpp` UT | Gate defer/direct/first-result/reentry/catch-all, registration failure, committed-state validation |
| `coordinator_membership_manager_test.cpp` | options, lifecycle, grace, Discovery cadence, fresh submission snapshot/policy gates, quorum, Add/Remove ordering, term rebuild, ambiguity, and Manager shutdown races |
| `coordinator_raft_node_test.cpp` ST | election, wrapper completion-callback drain, sole-voter guard and recovery, failed Start, waiting node, direct membership, follower health, vacancy/replacement, Discovery failure, all-node Manager leader failover |
| `braft_cluster_test.cpp` | raw braft election and unsupported-log first-apply/restart-replay fail closed; multi-stage waits share one absolute case deadline |

Notable current tests:

- `RaftOperationSubmissionGateTest.DefersResultUntilSubmissionCompletes`
- `RaftOperationSubmissionGateTest.DispatchesImmediatelyAfterSubmissionCompletes`
- `RaftOperationSubmissionGateTest.InvokesReentrantCallbacksOutsideGateMutex`
- `RaftOperationSubmissionGateTest.ContainsThrowingCallbacks`
- `CoordinatorRaftNodeTest.DestructionWaitsForSharedDrainCompletion`
- `CoordinatorRaftNodeTest.FailedStartLeavesNodeStoppedAndDestructible`
- `CoordinatorRaftMembershipTest.ManagersOnAllNodesReplaceFailedLeaderWithDiscoveredCandidate`
- `BraftReplayTest.UnsupportedUserLogReplayFailsClosedAfterRestart`

Focused verification commands for a configured test build are:

```bash
make -j30
ctest --output-on-failure --timeout 8 \
  -R 'RaftOperationSubmissionGateTest|CoordinatorRaftTypesTest|CoordinatorRaftStateMachineTest|CoordinatorRaftNodeTest|CoordinatorMembershipManagerTest|CoordinatorRaftMembershipTest|BraftReplayTest|BraftClusterTest'
```

Bazel closure checks should include the Node, StateMachine, Manager, and focused UT/ST targets after the required full CMake build. Validation commands require fresh execution before claiming runtime results; this documentation update does not itself provide new compile or test evidence.

## Risk And Integration Boundaries

| Surface | Current conclusion |
| --- | --- |
| Hot path | Coordinator control path only; no request-path change |
| Ownership | one external Node owner; Manager borrows Node; callbacks cannot become final owner; Node destruction waits for wrapper-owned Add/Remove callback tokens |
| Locking | lifecycle before committed snapshot; no callback or braft drain under Node locks |
| Persistence | no new format; braft owns durable state; unsupported records fail closed |
| Recovery | exact plan mapping; no recover-to-bootstrap fallback |
| Membership safety | no sole-voter Remove; no policy Remove without committed spare, fresh status/policy, and exact safe target |
| Failover | new term rebuilds from committed B and restarts full grace |
| Build | CMake remains test-gated; Bazel direct dependency closure mirrors actual includes |
| Rollback | remove test-gated adapter/Manager integration; no persisted migration needed |

## Product Integration Follow-Up

Future integration must:

1. define Coordinator options and require brpc mode;
2. create one owner that stores Manager before Node before shared server in destruction order;
3. revoke serving and stop upper-layer submissions before Manager destruction;
4. destroy Manager completely before Node destruction;
5. let Node destructor drain before stopping the shared server;
6. add serving gates, Worker reconciliation, deployment configuration, observability, packaging, and product scenarios;
7. preserve callback notification-only lifetime semantics.
