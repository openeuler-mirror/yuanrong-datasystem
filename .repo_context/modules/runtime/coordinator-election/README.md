# Coordinator Election

## Scope

- Canonical source roots: `src/datasystem/coordinator`, `src/datasystem/coordinator/raft`, and the public façade header `include/datasystem/coordinator_server.h`.
- `CoordinatorRuntime` is the non-singleton lifecycle owner. `CoordinatorServer` remains the singleton public façade and delegates to one Runtime.
- Election uses the Coordinator business service's shared brpc server and stable numeric IPv4 `host:port` endpoint.
- `CoordinatorServiceImpl` owns the two-stage service startup and shared-server generation. `CoordinatorElectionManager` is the sole owner of bootstrap state/control plus Node and Membership lifecycles.
- CMake and Bazel both provide production targets and dedicated election ST targets.

## Current Components

| Component | Responsibility |
| --- | --- |
| `CoordinatorServer` | Singleton compatibility façade for public embedded startup, runtime `UpdateConfig()`, and `Stop()`. |
| `CoordinatorRuntime` | Non-singleton config parsing, dynamic-config ownership and lifecycle gating, Raft flag snapshot, callbacks, event loop, instance-local `Stop()`, and leader observation. |
| `CoordinatorServiceImpl` | Business components, shared brpc server, braft service registration, two-stage startup, RPC readiness state, and ordered shutdown. |
| `RegisterCoordinatorRaftServices` | Registers braft services on the shared brpc server generation; called by the Service/shared-server owner. |
| `CoordinatorElectionManager` | Owns bootstrap-state publication, Discovery/peer convergence, startup-plan selection, Node then Membership startup, and Membership-before-Node shutdown. |
| `CoordinatorRaftNode` | Owns braft Node/FSM, leader and committed-membership observation, exclusive single-operation membership admission, peer operations, and synchronous drain. |
| `CoordinatorMembershipManager` | Runs one reconciliation thread after Node startup, rebuilds each decision from Leader/term, committed configuration, and follower health, and rotates discovered candidates by oldest Add-attempt time while the Node admits at most one in-flight Add/Remove. |

## Runtime And Service Lifecycle

```mermaid
sequenceDiagram
    participant C as Caller
    participant F as CoordinatorServer
    participant R as CoordinatorRuntime
    participant G as FlagManager
    participant S as CoordinatorServiceImpl
    participant D as Discovery provider
    participant E as CoordinatorElectionManager
    participant N as CoordinatorRaftNode
    participant M as CoordinatorMembershipManager
    participant P as Shared brpc server

    C->>F: InitAndRun(options)
    F->>F: Reject empty configFilePath
    F->>R: InitAndRun(options)
    R->>G: Parse non-empty configFilePath
    R->>R: Capture one GetRaftFlags and config-log snapshot
    R->>R: ParseLocalAddress
    R->>S: Construct, Init(), and Start()
    S->>P: Register business and braft services, then listen
    S->>S: Keep state STARTING
    R->>D: onStart registers local endpoint
    R->>S: StartElectionManager()
    S->>E: Construct, publish, and Start bootstrap worker
    S->>S: Publish lifecycle RUNNING
    E->>D: Bootstrap Discovery and peer probes, or local-data recovery
    E->>N: Start Node after a plan converges
    E->>M: Start Membership
    R->>R: Event loop
    C->>R: Stop()
    R->>D: onStop requests delayed unregister
    Note over D: Provider callback retains the endpoint for 10 seconds
    R->>S: Shutdown()
    S->>S: Wait for election startup; publish STOPPING and move Manager under lifecycle mutex
    S->>E: Unlock Service; join bootstrap worker, stop Membership, drain Node
    S->>P: Stop and join shared server without lifecycle mutex
    S->>S: Destroy adapter/components without lifecycle mutex
    S->>S: Reacquire lifecycle mutex, publish STOPPED/result, wake shutdown waiters
```

`CoordinatorOptions` carries a config path, Discovery provider, expected member count, and paired lifecycle callbacks. The production `CoordinatorServer` façade rejects an empty parameterized path and owns one Runtime. Runtime passes a non-empty path directly to `FlagManager::ParseConfigFile`; parse failures return the `K_INVALID` error built from the path and parser message. Runtime does not add path-form or file-metadata checks.

The no-argument `CoordinatorServer::InitAndRun()` / `CoordinatorRuntime::InitAndRun()` path is the dscli-compatible entry. When `coordinator_raft_initial_peers` is empty, it constructs the Service with no Discovery and member count `0`, so the Coordinator runs in single-node no-election mode; the Runtime still calls `StartElectionManager()`, and the Service returns `OK` without publishing an election Manager. When `coordinator_raft_initial_peers` is non-empty, Runtime builds `StaticCoordinatorDiscovery` from that exact list; if the resulting member count configures election, startup uses the same Service `StartElectionManager()` path as parameterized Runtime startup. Direct `CoordinatorRuntime` startup with an empty config path remains restricted to internal in-process tests for the parameterized overload. Each Runtime starts Coordinator logging and immediately records the build's Git commit and branch before processing startup options. After parsing succeeds or is skipped, each startup attempt calls `GetRaftFlags()` exactly once and captures the config-log snapshot before local-address parsing and Service construction. In-process multi-Runtime fixtures set shared common flags before launching any Runtime, do not mutate them while any Runtime is active, and restore them only after every Runtime has stopped and joined. Endpoint, exclusive Raft data root, Raft heartbeat/election timing, Discovery retry timing, member failure grace, and internal membership timing are isolated by each Runtime's `GetRaftFlags()` snapshot. Production runs one Coordinator Runtime per process.

`CoordinatorRuntime::UpdateConfig()` owns one long-lived `DynamicFlagConfig` and `DynamicConfigUpdater`. It accepts
string-valued JSON objects only after Service `Init()`/`Start()`, `onStart`, and election-manager startup succeed. The
Coordinator capability list is intentionally limited to `request_sample_rate`, `access_sample_rate`, and
`diagnostic_sample_rate`: the common updater commits them through the atomically published `LogSampler` snapshot. Other
process-dynamic flags, including `node_dead_timeout_s`, are rejected as not runtime-applicable until every Coordinator
consumer has a synchronized live-update path. A dedicated Runtime config mutex protects readiness and updater lifetime
and serializes each accepted update with `Stop()`; startup returns `K_NOT_READY`, while Stop, failed startup cleanup, or
signal shutdown returns `K_SHUTTING_DOWN`. Lifecycle rejections write an operation-log failure without the JSON payload.
Updates are process-local and are not distributed or persisted.

`CoordinatorRuntime::Stop()` updates only that Runtime's state, closes dynamic-update admission, and wakes only its event loop. The process signal handler only sets `g_exitFlag`, which each bounded Runtime event loop observes. Runtime moves each callback out of owned state exactly once and invokes it without the Runtime mutex held. A `CoordinatorRuntime` instance is one-shot by interface contract: callers must not invoke `InitAndRun` concurrently or more than once on the same instance, and must create a new Runtime instance to retry after any return.

## Service Readiness And Registration

`CoordinatorServiceImpl` uses `CREATED -> INITIALIZED -> STARTING -> RUNNING -> STOPPING -> STOPPED`.

- `Start()` registers business services and braft services on one shared brpc server, starts listening, and leaves election-enabled startup in `STARTING`.
- All business RPCs return `K_NOT_READY` during the `STARTING` window.
- `StartElectionManager()` reserves one attempt under the Service lifecycle mutex, constructs and publishes the Manager, starts its bootstrap worker without that mutex, then publishes Service `RUNNING`; Raft and business readiness may still be pending.
- A failed election-start attempt keeps the Service `STARTING` and the endpoint listening until Runtime invokes `onStop` and calls explicit Service shutdown; retries are rejected.
- `Shutdown()` waits for any lock-free election-start attempt to publish its local Manager before teardown. The first caller becomes cleanup owner, release-stores `STOPPING`, and moves Manager ownership while holding the lifecycle mutex.
- The cleanup owner releases the Service lifecycle mutex before Manager shutdown drains Membership then Node. A reentrant `GetLeader()` can therefore acquire the mutex and immediately return `K_SHUTTING_DOWN`.
- Concurrent or repeated public `Shutdown()` callers wait on the lifecycle condition variable while cleanup is active and return the first owner's saved cleanup status; they cannot stop the shared server early.
- After Manager destruction, the owner stops and releases the shared server, brpc adapter, and remaining components without the lifecycle mutex; it reacquires the mutex only to publish `STOPPED`, save the result, and wake waiters.
- `RegisterCoordinatorRaftServices` is a Service/shared-server-owner operation. Node and Manager APIs do not register braft services.

## Configuration Surface

User-facing Coordinator election timing flags are `coordinator_raft_heartbeat_interval_ms`, `coordinator_raft_election_timeout_ms`, `coordinator_discovery_retry_interval_ms`, and `coordinator_member_failure_grace_ms`. `coordinator_raft_heartbeat_interval_ms` defaults to 100 ms and must be in `[10, 10000]`; `coordinator_raft_election_timeout_ms` defaults to 1000 ms and must be an integer multiple of heartbeat with a ratio in `[5, 10]`. Discovery retry defaults to 5000 ms and member failure grace defaults to 10000 ms.

The membership health-check interval and bootstrap retry-warning interval are internal values. Production snapshots inject 3000 ms defaults, while in-process tests may override them by constructing `CoordinatorRaftFlags` directly.

## Bootstrap Convergence And Serving Gate

- `expectedMemberCount = N` is the voting-member target, not an exact fresh-bootstrap candidate count. A fresh cluster may bootstrap once the normalized candidate count reaches `floor(N / 2) + 1`; the initial committed configuration need not already contain `N` members.
- Candidate snapshots are endpoint-normalized, sorted, deduplicated, and SHA-256 digested. From quorum through `N`, every bootstrappable candidate is used. Above `N`, the Manager still probes every visible candidate before deterministically selecting the first `N` bootstrappable candidates; an unselected fresh node waits for an authoritative committed configuration.
- Local metadata probing treats an absent data root, missing persistence paths, and empty regular persistence files as no
  persistence evidence. After the complete layout is checked and no recognized non-empty regular persistence file
  exists, the probe revalidates and removes only the recognized empty persistence files before publishing `ABSENT`, so
  braft observes a fresh layout during bootstrap or peer-assisted election/membership metadata rebuild. If any
  recognized non-empty persistence file exists, no empty file is removed. Any recognized non-empty
  `raft_meta`, `log_meta`, or correctly named log-segment file selects `VALID` and `RecoverPlan`, leaving content
  consistency to braft. Recognized path type conflicts return `K_INVALID`; other filesystem-access failures return
  `K_NOT_READY`; both are terminal and fail-closed.
- Before first bootstrap or rebuild, the Manager probes every normalized Discovery-visible candidate, including candidates after the first `N`, and collects the complete observation set before deciding. Candidate shortage does not skip this probe phase.
- Valid non-empty committed configurations take precedence only after the same normalized committed peer list is confirmed by that committed list's own quorum (`size / 2 + 1`), counting only reports from peers that are themselves members of that committed list. A quorum-confirmed configuration containing an `ABSENT` local endpoint becomes the explicit `BootstrapPlan` used to rebuild local election/membership metadata; a quorum-confirmed configuration excluding local produces `WaitingToJoinPlan`.
- Observed committed configurations without any quorum-confirmed peer list are retryable `K_NOT_READY`; conflicting quorum-confirmed configurations are also retryable `K_NOT_READY`, including the expected `N` versus `N + 1` observation window during Add-before-Remove membership replacement. The narrow exception is target-quorum fresh bootstrap (`committed_size < N`): if a valid peer already reports a committed peer subset of the current normalized candidates and that subset includes the local `ABSENT` endpoint, the local peer may rebuild from that same subset to converge with the first peer that won the bootstrap race.
- A successful remote probe reporting `UNKNOWN` or `CORRUPT` metadata is non-authoritative and retryable: it prevents first bootstrap with `K_NOT_READY` while the same Manager worker remains active.
- Only when no visible candidate reports an authoritative committed configuration may a fresh set bootstrap. A peer is bootstrappable only after it is observable, has fresh `ABSENT` metadata, and reports the same full candidate count/digest plus immutable group/target identity. Peer unavailability excludes that peer from first-bootstrap selection and does not block when a fresh target majority remains; successful invalid observations, remote `UNKNOWN`/`CORRUPT` metadata, or digest disagreement keep the lifecycle active with no committed configuration and the business gate closed.
- After braft successfully initializes a non-empty `BootstrapPlan`, `CoordinatorRaftNode` publishes lifecycle `STARTED` but does not synthesize the normalized `initial_conf` as committed configuration index 0. Only braft configuration callbacks publish committed membership. While no candidate reports a real non-empty committed configuration, an observable peer with `VALID` metadata but an empty committed configuration remains bootstrappable only when its candidate count/digest and immutable group/target identity match the current fresh bootstrap view. This lets staggered peers start the same plan without treating one node's local initialization as a committed membership decision.
- `CoordinatorMembershipManager` treats committed configuration as the only membership authority; Add/Remove callbacks are diagnostic-only and capture no Manager state.
- `CoordinatorRaftNode` admits at most one in-flight Add/Remove through its operation drain token. While that token is held, `CoordinatorMembershipManager` skips reconciliation before reading committed status; completion publishes committed configuration before releasing the token, so the next round rebuilds its decision from the new B. A concurrent submission still returns `K_TRY_AGAIN`, and Node shutdown stops new admission before draining the accepted operation.
- It fills a bootstrap vacancy with Add-only reconciliation. For replacement, it commits the waiting candidate at `N + 1` before removing the confirmed failed follower.
- Eligible candidates rotate by never-attempted first, then oldest Add-attempt timestamp. Replacement rollback requires committed-state proof that the candidate was submitted by the current term's replacement intent.
- Service lifecycle `RUNNING` means the endpoint is active and the Manager's background bootstrap worker has started; it is not election completion or business readiness. Only the current braft Leader has `raftServing_ = true`; followers, waiting candidates, bootstrap waiters, and quorum-lost nodes return `K_NOT_READY` from business RPCs.

The first-bootstrap Discovery safety premise is one globally convergent deployment control domain. Mutually invisible candidate sets that independently reach target majority are outside the supported bootstrap contract.

## Discovery Callback Contract

The Runtime calls `onStart` after the shared endpoint is listening and before election bootstrap queries Discovery. The registration provider can therefore coordinate all intended members before any bootstrap snapshot is read.

`onStop` is invoked before Service shutdown. The provider callback owns unregister policy. The in-process ST models the approved policy by retaining the endpoint for 10 seconds without sleeping, then lazily expiring it from later Discovery queries.

`ICoordinatorDiscovery::GetCoordinators` is a synchronous provider contract and every provider must return within a finite provider-controlled bound. Manager shutdown can observe its stop request only between synchronous calls and joins after the current call returns, so a provider that violates the bounded-return contract can block Manager and Service shutdown.

## Bootstrap Diagnostics And Wire Compatibility

`GetRaftBootstrapState` bypasses the Leader-only business gate and copies the Manager-owned value snapshot. Its public phase is `OBSERVING`, `RETRYING`, `STARTED`, or `TERMINAL`; `status_code` is the stable numeric `StatusCode`. `TERMINAL` is one-way and preserves the first terminal status: later observation, retry, Node startup, Membership startup, or ownership publication cannot replace it with another phase/status. Unpublished Node/Membership instances are cleaned up outside the bootstrap mutex; if ownership was already published before a later terminal callback, the Manager remains shutdown-safe and the business gate stays closed. The response deliberately has no raw Status text or Raft data-directory field.

The Coordinator protobuf service order is a wire contract for the custom ZMQ transport. Legacy `ReportTopologyRecoveryCandidate` and `GetClusterRawSnapshot` remain method indexes 7 and 8; the appended `GetRaftBootstrapState` method is index 9. New methods must remain after all existing methods.

## Replication And Request Boundary

The current braft state machine rejects management-log apply and the Node exposes only voting-membership `AddPeer`/`RemovePeer` operations. Coordinator Raft therefore persists election state and committed voting configuration; it does not replicate Coordinator business key/value or topology data. Business RPC admission in election mode is determined by `CoordinatorServiceImpl`'s local `raftServing_` gate, which is opened and closed by braft leadership lifecycle callbacks.

## Build And Test

| Level | Path | Coverage |
| --- | --- | --- |
| UT | `tests/ut/common/coordinator/coordinator_raft_types_test.cpp` | Stable peer identity and Raft option validation. |
| UT | `tests/ut/common/coordinator/coordinator_raft_state_machine_test.cpp` | FSM event and exception boundaries. |
| UT | `tests/ut/common/coordinator/coordinator_raft_node_test.cpp` | Node lifecycle, absence of synthetic Bootstrap index-0 publication, non-Bootstrap startup, committed state, operations, and drain. |
| UT | `tests/ut/common/coordinator/coordinator_membership_manager_test.cpp` | Committed-state membership policy, candidate fairness, replacement proof and rollback, fresh submission revalidation, timing, and shutdown. |
| UT | `tests/ut/common/coordinator/coordinator_election_manager_test.cpp` | Target-quorum bootstrap, local-`ABSENT` rebuild from authoritative peers, retrying `N`/`N + 1` conflicts, sanitized phase/status transitions, and Node/Membership ownership and lifecycle ordering. |
| UT | `tests/ut/coordinator/coordinator_server_options_test.cpp` | Public empty-path rejection, direct Runtime empty-path process-flags startup, non-empty config parsing, per-instance Raft snapshots, lifecycle retry, readiness, shutdown ordering, ZMQ method-index compatibility, and allocator-backed real brpc lifecycle. |
| ST | `tests/st/common/raft/coordinator_service_election_test.cpp` | Real single-Service shared endpoint, election, recovery, and cleanup. |
| ST | `tests/st/common/raft/coordinator_runtime_election_test.cpp` | Real in-process Runtimes launched with empty config paths, stable fixture-owned common flags, and per-generation endpoint/data/timing snapshots; covers one-candidate waiting, target-quorum bootstrap, membership, Discovery failure, serving-gate isolation, failover, persisted restart, deleted-root rebuild, and bounded quorum loss. |
| ST | `tests/st/common/raft/coordinator_raft_node_test.cpp` | Real Node and Membership scenarios. |
| ST | `tests/st/common/raft/braft_cluster_test.cpp` | Raw braft cluster behavior. |

The in-process Runtime ST validates non-singleton Runtime isolation, callback ordering, failure, and recovery behavior inside one address space.

Focused CMake verification for a configured `WITH_TESTS=on` build is:

```bash
make -j30
ctest --output-on-failure --timeout 8 \
  -R 'RaftOperationSubmissionGateTest|CoordinatorRaftTypesTest|CoordinatorRaftStateMachineTest|CoordinatorRaftNodeTest|CoordinatorMembershipManagerTest|CoordinatorElectionManagerTest|CoordinatorElectionServiceTest|CoordinatorServerOptionsTest|CoordinatorRaftMembershipTest|CoordinatorServiceElectionTest|CoordinatorRuntimeElectionTest|BraftReplayTest|BraftClusterTest'
```

Dedicated targets are `coordinator_service_election_test` and `coordinator_runtime_election_test` in both CMake and Bazel. The focused `coordinator_server_options_test` retains process-safe port leases through real brpc Stop/Join and links `test_port_allocator` in CMake and Bazel. Both `coordinator_server_options_test` and `coordinator_election_manager_test` have CTest `TIMEOUT 8` and Bazel `timeout = "short"` declarations.

Each Runtime election case body has a 6-second deadline; mandatory teardown Stop/join cleanup is not claimed to be inside that body budget. The quorum-loss case additionally starts an independent `steady_clock` deadline immediately after the second member's Stop/join completes and requires old-Leader stepdown plus `GetLeader`/business-gate `K_NOT_READY` within at most twice that Runtime generation's `GetRaftFlags().electionTimeoutMs`. The old-Leader and persisted-follower restart cases relaunch the same endpoint/data root with a generation-specific provider sharing the global registration state and assert zero Discovery queries on that provider through local metadata recovery. During persisted-follower downtime the original Leader remains serving; after restart the test permits any unique Leader and checks the observed Leader can serve, non-Leaders return only readiness-compatible business statuses, and all members expose the committed configuration. The deleted-follower-root case fully stops one follower, removes only that follower's Raft data root, relaunches the same endpoint/root before membership failure grace, and requires the new generation to query Discovery, rebuild the original three-peer committed configuration from authoritative peers, remain a non-serving follower, and publish `STARTED`. Each complete concrete case is bounded by the CTest 8-second timeout.

## Change Boundaries

- Keep `CoordinatorRuntime` non-singleton and `CoordinatorServer` a singleton façade.
- Keep `CoordinatorServer` as the production singleton façade owning one Runtime; its parameterized entry rejects an empty config path before delegation.
- Keep file access, parsing, and flag validation in `FlagManager` and Runtime startup. Runtime parses a non-empty path directly and returns the parser failure status with the configured path and parser text.
- Keep direct empty-path startup of the parameterized Runtime overload internal to in-process tests. The no-argument dscli-compatible path may use empty `coordinator_raft_initial_peers` for single-node no-election mode or non-empty static peers for election startup. Tests prepare shared flags before launch, keep them unchanged while Runtimes are active, and restore them after all Runtime threads join.
- Keep local address/data/timing in one per-Runtime `GetRaftFlags()` snapshot for every startup attempt.
- Keep `CoordinatorRuntime` instances one-shot by interface contract; retry requires constructing a new Runtime instance.
- Keep Manager publication and bootstrap RPC availability ahead of asynchronous Node startup; Service lifecycle `RUNNING` must remain distinct from Leader-only business readiness.
- Keep peer-assisted rebuild limited to local `ABSENT`, keep conflicting authoritative full-list observations retryable,
  and keep local metadata probe errors terminal without automatically deleting the Raft data root.
- Preserve Coordinator ZMQ method indexes 7 and 8 and append bootstrap diagnostics at index 9; expose only phase and stable numeric status code, never raw Status text or data paths.
- Keep Coordinator Raft limited to election and voting membership, keep business payloads outside its state machine, and keep business admission determined by the local `raftServing_` gate driven by braft leadership lifecycle callbacks.
- Keep braft registration in the Service/shared-server owner and out of Node/Manager APIs.
- Preserve lock-held `STOPPING` publication and Manager ownership transfer, lock-free Membership-before-Node drain, concurrent-shutdown waiting, and lock-free shared-server/remaining cleanup only after Manager destruction; reacquire the lifecycle mutex only to publish the shared result.
- Keep registration and unregister policy in the external lifecycle callback provider.
