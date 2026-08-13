# Coordinator Election Product Lifecycle Design

## Status

The approved implementation consists of a non-singleton `CoordinatorRuntime`, a singleton `CoordinatorServer` façade, two-stage `CoordinatorServiceImpl` startup, Service-owned braft registration, and Manager-owned bootstrap/Node/Membership lifecycle. Dedicated single-Service, in-process multi-Runtime, and multi-process STs cover complementary integration boundaries.

## Goals

1. Allow independent Coordinator lifecycle instances in one process through `CoordinatorRuntime`.
2. Preserve the singleton `CoordinatorServer` public façade for compatibility.
3. Register business and braft services on one Service-owned brpc server before election startup.
4. Register the local endpoint through the Runtime callback before bootstrap Discovery is queried.
5. Keep Service lifecycle readiness distinct from Leader-only business readiness while bootstrap, recovery, or waiting-to-join remains active.
6. Keep Stop, event-loop wakeup, service ownership, and leader queries instance-local.
7. Drain Membership, Node, and shared-server resources in strict ownership order.
8. Keep bootstrap diagnostics wire-compatible and sanitized to phase plus stable numeric status code.
9. Keep default tests deterministic and below the repository's 8-second limit.

## Component Boundary

```mermaid
classDiagram
    class CoordinatorServer {
        +GetInstance() CoordinatorServer*
        +InitAndRun(options) Status
        +UpdateConfig(configJson) Status
        +Stop() Status
    }
    class CoordinatorRuntime {
        +InitAndRun(options) Status
        +UpdateConfig(configJson) Status
        +Stop() Status
        +IsLeader() bool
        +GetLeader(address) Status
        #GetRaftFlags() CoordinatorRaftFlags
    }
    class CoordinatorServiceImpl {
        +Init() Status
        +Start() Status
        +StartElectionManager() Status
        +Shutdown() Status
    }
    class CoordinatorElectionManager {
        +Start() Status
        +Shutdown() Status
    }
    class CoordinatorRaftNode
    class CoordinatorMembershipManager
    class SharedBrpcServer
    class DiscoveryProvider

    CoordinatorServer *-- CoordinatorRuntime : façade owns one
    CoordinatorRuntime *-- CoordinatorServiceImpl : each Runtime owns one
    CoordinatorRuntime --> DiscoveryProvider : lifecycle callbacks and bootstrap input
    CoordinatorServiceImpl *-- SharedBrpcServer
    CoordinatorServiceImpl *-- CoordinatorElectionManager
    CoordinatorElectionManager *-- CoordinatorRaftNode
    CoordinatorElectionManager *-- CoordinatorMembershipManager
```

`CoordinatorRuntime` is the reusable internal lifecycle abstraction. `CoordinatorServer` is the production singleton façade and owns one Runtime. `CoordinatorOptions` contains a config file path, shared Discovery provider, expected member count, and paired lifecycle callbacks. The façade requires a non-empty path; file access, parsing, and flag validation belong to `FlagManager` and Runtime startup. The Runtime owns one `DynamicFlagConfig` and one `DynamicConfigUpdater`; its public `UpdateConfig` façade accepts string-valued JSON only after the Service, lifecycle callback, and election manager have started and serializes each update with `Stop()`. Its role-specific capability list contains only the three log-sampler rates because their consumers use atomically published `LogSampler` snapshots. Snapshot-only and unsynchronized flags such as `node_dead_timeout_s` are rejected rather than reporting a partially effective update. Local endpoint, exclusive Raft data root, user-facing Raft heartbeat/election/Discovery/member-failure timing, and internal membership timing come from one `GetRaftFlags()` snapshot per Runtime startup.

## Startup Sequence

```mermaid
sequenceDiagram
    participant C as Caller
    participant F as CoordinatorServer
    participant R as CoordinatorRuntime
    participant G as FlagManager
    participant S as CoordinatorServiceImpl
    participant P as Shared brpc server
    participant D as Discovery provider
    participant E as CoordinatorElectionManager
    participant N as CoordinatorRaftNode
    participant M as CoordinatorMembershipManager

    C->>F: InitAndRun(options)
    F->>F: Reject empty configFilePath
    F->>R: InitAndRun(options)
    R->>G: Parse non-empty configFilePath
    R->>R: Capture one GetRaftFlags and config-log snapshot
    R->>R: ParseLocalAddress
    R->>S: Construct, Init, and Start
    S->>P: Register business services
    S->>P: Register braft services
    S->>P: Start listening
    S->>S: State remains STARTING
    R->>D: onStart register endpoint
    R->>S: StartElectionManager
    S->>E: Construct and publish Manager
    S->>E: Start bootstrap control worker
    S->>S: Publish lifecycle RUNNING
    E->>D: Query candidates and probe every normalized Discovery-visible candidate when local data is fresh
    E->>N: Start after Bootstrap, Recover, or WaitingToJoin plan converges
    E->>M: Construct and Start
    R->>R: Enter event loop
```

The production façade validates only that `configFilePath` is non-empty, then delegates to its owned Runtime. Runtime passes a non-empty path directly to `FlagManager::ParseConfigFile`; parse failures return the `K_INVALID` error built from the configured path and parser message. After parsing succeeds, Runtime captures exactly one `GetRaftFlags()` snapshot and one config-log snapshot, then preserves the established `ParseLocalAddress -> Service construct/Init/Start -> publish Service -> callbacks -> StartElectionManager` order.

Direct empty-path Runtime startup is reserved for internal in-process tests. It skips parsing and consumes common process flags prepared by the fixture. The fixture sets those flags before launching any Runtime, never mutates them while a Runtime is active, and restores them only after every Runtime stops and joins. Each Runtime override supplies its endpoint, exclusive data root, and timing through `GetRaftFlags()`. Production supports one Coordinator Runtime per process. A `CoordinatorRuntime` instance is one-shot by interface contract; callers must create a new Runtime instance to retry after any return.

`CoordinatorServiceImpl::Start()` owns the shared-server generation. It registers the Coordinator business adapter and calls `RegisterCoordinatorRaftServices`, starts listening, and returns while the election-enabled Service remains `STARTING`. Node and Manager APIs do not expose registration.

`CoordinatorServiceImpl::StartElectionManager()` reserves one non-retryable attempt under its lifecycle mutex, constructs and publishes the Manager, releases the mutex while starting its background bootstrap control, then reacquires it to publish Service `RUNNING`. `RUNNING` therefore means the endpoint is active and the Manager worker is owned and started, not that election or business readiness has completed. The Manager snapshot is available through `GetRaftBootstrapState` while Raft startup is still waiting. Business RPCs remain `K_NOT_READY` until braft calls `onLeaderStart`; every follower or waiting node stays gated. On synchronous startup failure, the endpoint remains listening and the Manager is detached for ordered cleanup. Concurrent Shutdown waits for an in-progress attempt to publish completion before teardown.

## Bootstrap And Membership Policy

`expectedMemberCount = N` is a target, not a requirement that the first committed configuration already contain `N` members. For fresh local metadata, `CoordinatorElectionManager` computes `bootstrapQuorum = floor(N / 2) + 1`, normalizes and sorts the full Discovery view, and probes every normalized Discovery-visible candidate before deciding, including candidates after the first `N` and views below quorum. `VALID` observations with non-empty committed configurations are evaluated before probe failures, candidate shortage, or first-bootstrap selection, but a committed configuration becomes authoritative only after that same normalized peer list is reported by its own member quorum, except for the target-quorum fresh-bootstrap race where one peer has already committed the exact current candidate set and that set is smaller than `N`.

| Observation | Plan or action |
| --- | --- |
| valid local Raft metadata | `RecoverPlan` without first-bootstrap Discovery |
| corrupt or unknown local metadata | terminal fail-closed state with the endpoint diagnostic but non-serving |
| successful remote probe reports corrupt or unknown metadata and no authoritative committed configuration exists | return retryable `K_NOT_READY`; prevent first bootstrap and keep the same Manager worker active |
| fewer than `bootstrapQuorum` normalized candidates | probe the full visible set, then keep retrying; no Node/configuration/Leader |
| `bootstrapQuorum..N` bootstrappable candidates, with no authoritative configuration | bootstrap all currently bootstrappable candidates |
| more than `N` bootstrappable candidates, with no authoritative configuration | probe all candidates, then only the first `N` sorted bootstrappable endpoints may bootstrap; unselected endpoints wait for authority |
| one quorum-confirmed valid non-empty committed configuration excludes local `ABSENT` endpoint | `WaitingToJoinPlan` |
| one quorum-confirmed valid non-empty committed configuration includes local `ABSENT` endpoint | `BootstrapPlan` with the full authoritative peer list to rebuild local election/membership metadata |
| observed committed configurations have no member quorum, or quorum-confirmed valid non-empty committed configurations conflict, including `N` versus transitional `N + 1` | retry with `K_NOT_READY` until one normalized full list reaches quorum; create no Node, except that local `ABSENT` may rebuild from a committed candidate subset when the committed set is below `N`, is contained in the current normalized candidates, and includes local |
| fewer than `bootstrapQuorum` bootstrappable candidates because peers are unavailable, with no authoritative configuration | keep retrying with the business gate closed |
| successful peer count/digest/group/target disagreement or otherwise invalid observable metadata, with no authoritative configuration | keep retrying with the business gate closed |

After braft successfully initializes a non-empty `BootstrapPlan`, the Node publishes lifecycle `STARTED`, marks configuration publication in progress, releases its lifecycle mutex, and then publishes the normalized `initial_conf` as committed configuration index 0 through the existing FSM wrapped configuration callback and `HandleConfigurationCommitted`. Shutdown waits for the publication marker to clear before moving Node/FSM ownership, so callback reentry can observe the committed snapshot without deadlocking and startup does not access a member FSM after unlock without lifetime protection. Recover and Waiting-to-Join do not synthesize an empty configuration.

After a smaller target-majority configuration commits, the Leader's Membership Manager fills vacancies with Add-only operations. A waiting replacement is added before any confirmed failed member is removed, so the observable committed sequence reaches `N + 1` before returning to `N`. Discovery failure yields no candidate and therefore cannot trigger `RemovePeer` or close a healthy Leader's serving gate.

Committed configuration B is the only membership authority. Before reading B or generating policy, the Manager skips the reconciliation round while `CoordinatorRaftNode` reports an in-flight membership operation. The Node admits at most one Add/Remove through the same operation token used for shutdown drain; braft publishes committed configuration before completion releases that token, so the next admitted reconciliation rebuilds its decision from the new Leader/term, committed B, and follower health. A concurrent submission returns `K_TRY_AGAIN`, while `discoveryRetryInterval` bounds repeated Manager submissions after failed completion. Add/Remove completion callbacks remain diagnostic-only and capture no Manager state.

| Committed state | Membership action |
| --- | --- |
| `B.size() < N` | Discover and add one eligible candidate; remove no committed member. |
| `B.size() == N` without confirmed failure | Perform local health polling and do not call Discovery. |
| `B.size() == N` with confirmed failure | Discover and submit one replacement candidate. |
| `B.size() > N` with confirmed failure | Remove the exact confirmed failed peer. |
| `B.size() > N` after failed-peer recovery | Remove only the exact committed candidate proven to belong to the current replacement intent. |
| unexplained `B.size() > N` | Fail closed without removal. |

Candidate selection prefers candidates never attempted in the current Discovery set, then the candidate with the oldest Add-attempt timestamp, using normalized identity as the deterministic tie-break. Replacement intent records the failed peer, original committed peers, and candidates whose Add submission synchronously succeeded. A committed over-target difference is rollback-owned only when it is exactly one of those attempted candidates; rejected or throwing submissions establish no ownership.

Immediately before Add/Remove submission, the Manager fetches fresh status and requires unchanged leadership, term, configuration index, committed peers, known quorum, failed-peer policy, and rollback ownership. A stale decision returns `K_TRY_AGAIN` without submission. Lifecycle is rechecked before Discovery and before mutation submission, so shutdown cannot admit new external work after an in-flight status query returns.

The Manager-owned bootstrap RPC snapshot publishes `OBSERVING/K_OK` while collecting facts, `RETRYING/K_NOT_READY` for retryable Discovery/peer/config observations, `STARTED/K_OK` after Node and Membership startup, and `TERMINAL/<stable StatusCode>` on the first terminal failure. `TERMINAL` is sticky: all later non-terminal publications are ignored, external Node/Membership startup stages recheck it after returning, and the final ownership/`STARTED` decision checks it while holding the established lifecycle-then-bootstrap lock order. Unpublished components are cleaned up only after locks are released; already-published ownership remains available to normal Manager shutdown. `CoordinatorServiceImpl` copies this value and emits protobuf phase plus numeric `status_code`; it does not expose raw Status text or a data-directory field. For custom ZMQ wire compatibility, legacy methods remain at indexes 7 and 8 and `GetRaftBootstrapState` is appended at index 9.

This first-bootstrap policy assumes a single globally convergent deployment control domain. It does not claim safety for mutually invisible target-majority candidate sets.

## Shutdown Sequence

```mermaid
sequenceDiagram
    participant C as Caller
    participant R as CoordinatorRuntime
    participant D as Discovery provider
    participant S as CoordinatorServiceImpl
    participant E as CoordinatorElectionManager
    participant M as CoordinatorMembershipManager
    participant N as CoordinatorRaftNode
    participant P as Shared brpc server

    C->>R: Stop
    R->>R: Wake this instance's event loop
    R->>D: onStop schedules delayed unregister
    Note over D: Endpoint remains discoverable for 10 seconds
    R->>S: Shutdown
    S->>S: Lock; wait election startup completion
    S->>S: Become owner, release-store STOPPING, move Manager
    S->>E: Unlock Service; Shutdown
    E->>M: Stop and destroy
    E->>N: Drain and destroy
    S->>P: Stop and join without lifecycle mutex
    S->>S: Destroy adapter/components without lifecycle mutex
    S->>S: Reacquire lifecycle mutex; publish STOPPED/result and notify waiters
    R-->>C: InitAndRun returns
```

The Runtime invokes `onStop` once after an attempted `onStart` and before Service shutdown. Delayed unregister is provider callback semantics: the callback records expiry and returns without sleeping; a later provider query removes expired endpoints.

`ICoordinatorDiscovery::GetCoordinators` is synchronous and providers must return within a finite provider-controlled bound. Manager shutdown observes its stop request only between synchronous calls and joins after an in-flight call returns, so a provider that violates this contract can block Manager and Service shutdown.

Public Service shutdown has one cleanup owner. Under the lifecycle mutex it first waits for any election startup attempt to publish completion, then release-stores `STOPPING` and transfers `electionManager_` to owner-local storage. The owner releases the mutex before Manager shutdown requests and joins bootstrap control, then stops Membership and drains Node, so a Discovery callback reentering `GetLeader()` acquires the Service mutex and returns `K_SHUTTING_DOWN` without waiting for Manager completion. Still without the lifecycle mutex, the owner stops and joins the shared brpc server, destroys the adapter, and cleans up the remaining components. It reacquires the mutex only to publish `STOPPED`, save the first cleanup status, and notify waiters. Concurrent or repeated public shutdown callers wait for completion and return the same saved status; they never run an overlapping cleanup stage. Lock-held `Init()`/`Start()` failure cleanup may still use the internal Manager-capable path because no successful external Membership lifecycle is running there, and it preserves the initiating startup error.

## State And Concurrency Model

| State/resource | Owner | Protection and lifetime |
| --- | --- | --- |
| Runtime stop and lifecycle state | Each `CoordinatorRuntime` | Runtime mutex protects callbacks, Service ownership, and stop state; `Stop()` wakes only that instance. `InitAndRun` is one-shot by interface contract, so callers must not invoke it concurrently or more than once on the same Runtime. |
| Runtime dynamic-config state | Each `CoordinatorRuntime` | Dedicated config mutex protects `NOT_READY`/`READY`/`STOPPING`, the updater, and its referenced flag-config lifetime. The role filter admits only atomically published sampler configuration. Accepted updates finish before `Stop()` closes admission; later updates return `K_SHUTTING_DOWN`. Lifecycle failures are audited without request payloads. |
| Common process flags | Production caller or test fixture | Production starts one Runtime per process through `CoordinatorServer`. In-process tests set shared flags before launch, do not mutate them while any Runtime is active, and restore them only after every Runtime stops and joins. Concurrent flag mutation is outside the supported contract. |
| Per-instance Raft startup values | Each `CoordinatorRuntime` | One `GetRaftFlags()` snapshot per attempt isolates endpoint, data root, and election timing before local-address parsing and Service construction. |
| Lifecycle callbacks | Each Runtime | Moved exactly once from Runtime-owned `std::function`; invoked without the Runtime mutex. |
| Shared Discovery mock state | Discovery provider | One shared registration-state mutex protects the ordered endpoint set, generation ownership, pending expirations, virtual time, callback history, and barrier state. Each no-argument provider keeps its own failure/fixed-snapshot behavior and query count while optionally sharing registration state; caller-specific snapshots are digest-mismatch fault injection, not a normal convergence claim. |
| Service readiness and public shutdown result | `CoordinatorServiceImpl` | Lifecycle mutex protects startup/shutdown coordination, cleanup ownership, and the saved first cleanup status; atomic serving state release-publishes `STOPPING` before Manager drain begins and `STOPPED` after remaining cleanup. |
| Shared brpc server generation | `CoordinatorServiceImpl` | Outlives Manager destruction and its lock-free Membership/Node drain; the cleanup owner stops and joins it without the lifecycle mutex before adapter/component cleanup. |
| Bootstrap snapshot/worker, Node, and Membership | `CoordinatorElectionManager` | Bootstrap mutex protects the diagnostic snapshot and worker stop/wakeup state; lifecycle mutex protects published Node/Membership ownership. `TERMINAL` is one-way, and the final ownership/`STARTED` transition follows lifecycle mutex then bootstrap mutex. Neither lock is held while stopping or destroying components. Shutdown joins bootstrap control, then stops Membership before Node. |
| Runtime thread | Test fixture | Fixture owns Runtime, thread, promise/future, and always stops then joins. |

No Discovery callback is invoked while the Discovery mock mutex is held. Its registration barrier uses a bounded condition-variable wait. Unregister does not sleep; it records `now + 10s`. Test-only virtual time advances expiration without wall-clock waiting.

## Replication And Request Boundary

Coordinator braft currently governs election state and committed voting membership. The Raft state machine rejects management-log apply, and `CoordinatorRaftNode` exposes only `AddPeer` and `RemovePeer`; Coordinator business key/value and topology payloads are not replicated by this Raft group. `CoordinatorServiceImpl::CheckServing()` admits election-mode business RPCs only when the local `raftServing_` gate is open; braft leadership lifecycle callbacks open and close that gate. Consequently, `RUNNING` and bootstrap `STARTED` are lifecycle diagnostics rather than business-readiness signals.

## Identity And Persistence

Each Runtime uses a distinct numeric loopback endpoint and exclusive data root. The endpoint and data/timing values are immutable for that attempt because the Runtime calls virtual `GetRaftFlags()` and captures `CoordinatorRaftFlags` before constructing the Service, including across retries and independent Runtime instances. Production exposes only `coordinator_raft_heartbeat_interval_ms`, `coordinator_raft_election_timeout_ms`, `coordinator_discovery_retry_interval_ms`, and `coordinator_member_failure_grace_ms`; membership health-check and bootstrap retry-warning intervals are internal defaults that tests can override through `CoordinatorRaftFlags`. Fresh roots use target-majority bootstrap or WaitingToJoin from normalized Discovery/peer snapshots; valid existing roots recover locally. The persisted old-Leader and follower restart STs relaunch generation 2 with the same endpoint/data root and a generation-specific provider that shares registration semantics with the cluster provider; recovery reaches the valid committed configuration while that provider's Discovery query count remains zero, excluding queries from other running Managers from the assertion. During persisted-follower downtime the original Leader remains serving; after restart any unique Leader is valid if the observed Leader serves successfully, non-Leaders return only readiness-compatible business statuses, and all members expose the committed configuration. The deleted-follower-root ST instead removes only the fully stopped follower's real Raft root and proves the new generation queries Discovery, observes authoritative peers, rebuilds the original three-member committed configuration, remains non-serving, and reaches `STARTED` before failure-grace replacement.

## Validation Surface

| Test | Boundary |
| --- | --- |
| `CoordinatorElectionManagerTest` | Target-quorum selection, local-`ABSENT` authoritative rebuild, retrying `N`/`N + 1` conflicts, phase/status transitions, and ownership/lifecycle invariants. |
| `CoordinatorServerOptionsTest` | Public empty-path rejection, direct Runtime empty-path process-flags startup, non-empty config parsing, malformed-config failure, independent per-instance Raft snapshots, one-shot Runtime contract, ZMQ indexes 7/8/9, and allocator-backed real brpc lifecycle. |
| `CoordinatorServiceElectionTest` | One real Service and one shared business/braft endpoint. |
| `CoordinatorRuntimeElectionTest` | Real Runtimes use empty config paths with stable fixture-owned common flags and per-generation endpoint/data/timing snapshots; covers target-majority bootstrap, deterministic over-target selection, membership replacement, Discovery failures, observed-Leader serving readiness, persisted restart, deleted-root authoritative rebuild, bounded quorum-loss stepdown, teardown, and port release. |

`CoordinatorRuntimeElectionTest` starts only real `CoordinatorRuntime::InitAndRun(options)` lifecycles with empty config paths and observes Runtime methods plus real Coordinator business/bootstrap brpc RPCs; it does not construct or start `CoordinatorElectionManager` directly. The fixture owns stable common flags, while only `ICoordinatorDiscovery` views/failures and per-Runtime Raft snapshots are mocked. Each case body has one 6-second deadline; mandatory teardown joins are not claimed to be controlled by that deadline. The quorum-loss case records a separate monotonic deadline immediately after the second member's Stop/join and requires `IsLeader() == false`, `GetLeader() == K_NOT_READY` with an empty endpoint, and business RPC `K_NOT_READY` within at most twice the stopped Leader generation's configured election timeout. Each complete concrete case is bounded by the CTest 8-second timeout.

## Build Contract

- CMake excludes `coordinator_runtime_election_test.cpp` from generic `DS_TEST_ST_SRCS`, builds a dedicated executable, and registers discovered gtests with `TIMEOUT 8`.
- CMake links `test_port_allocator` into `coordinator_server_options_test`; the fixture retains process-safe leases through real brpc Stop/Join. Both focused Manager and Server-options UTs are registered with `TIMEOUT 8`.
- Bazel declares `//tests/st/common/raft:coordinator_runtime_election_test` as a dedicated `ds_cc_test` with `timeout = "short"`; the Manager and Server-options UTs also use `timeout = "short"`, and the latter directly depends on `//tests/st/cluster:test_port_allocator`.
- The focused CTest regex includes `CoordinatorRuntimeElectionTest` so the generated concrete gtest name is selected.
- Tests that call `RegisterCoordinatorRaftServices` directly depend on `coordinator_raft_service`.
