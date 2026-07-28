# Coordinator Election

## Scope

- Source root:
  - `src/datasystem/coordinator/raft`
- Build integration:
  - `src/datasystem/coordinator/CMakeLists.txt`
  - `src/datasystem/coordinator/raft/CMakeLists.txt`
  - `src/datasystem/coordinator/raft/BUILD.bazel`
- Representative tests:
  - `tests/ut/common/coordinator/coordinator_raft_types_test.cpp`
  - `tests/ut/common/coordinator/coordinator_raft_state_machine_test.cpp`
  - `tests/ut/common/coordinator/coordinator_raft_node_test.cpp`
  - `tests/ut/common/coordinator/coordinator_membership_manager_test.cpp`
  - `tests/st/common/raft/coordinator_raft_node_test.cpp`
  - `tests/st/common/raft/braft_cluster_test.cpp`

This module owns the test-gated Coordinator braft adapter, committed-membership observation, asynchronous Add/Remove operations, and standalone membership reconciliation policy. It does not yet wire Coordinator Raft into `CoordinatorServiceImpl` or expose production flags and deployment integration.

## Current Components

| Component | Responsibility |
| --- | --- |
| `CoordinatorRaftNode` | Owns braft Node/FSM lifecycle, shared brpc service registration, startup-plan validation, committed configuration observation, and asynchronous membership submissions. |
| `CoordinatorRaftStateMachine` | Adapts braft lifecycle events to Coordinator callbacks and contains callback exceptions at the braft boundary. |
| `CoordinatorMembershipManager` | Runs one reconciliation thread, observes leader and committed configuration state, discovers candidates, and serializes vacancy/replacement Add/Remove policy. |
| `CoordinatorRaftOperation` helpers | Enforce first-result-wins callback ordering and drain submitted callbacks before Node teardown. |
| Peer/type helpers | Normalize stable numeric IPv4 peer identities and validate bootstrap, recovery, waiting-to-join, and election-timeout inputs. |

## Key Contracts

- Stable Raft identity is numeric `IPv4:port` with braft peer index `0`; domain names and IPv6 are unsupported in the current phase.
- `CoordinatorRaftNode` must be destroyed after `CoordinatorMembershipManager` and before the borrowed shared brpc server.
- Node destruction drains braft work and Add/Remove callbacks before releasing the FSM.
- Committed configuration callbacks, rather than transient `list_peers()` results, are the membership authority.
- Standard callback exceptions are contained and logged with the fixed failure marker plus `exception.what()`. Non-standard ordinary FSM callback exceptions are reported through a generic fixed-marker `Status` when `onError` is configured; direct `onError` and operation-callback boundaries log the fixed marker.
- Recovery never falls back to bootstrap when local Raft metadata is expected.
- Product serving eligibility, Worker reconciliation, and production deployment wiring remain follow-up integration work.

## Companion Context

- Design and lifecycle invariants:
  - `.repo_context/modules/runtime/coordinator-election/design.md`
- Implementation and validation workflow:
  - `.repo_context/playbooks/features/runtime/coordinator-election/implementation.md`
- Module metadata:
  - `.repo_context/modules/metadata/runtime.coordinator-election.json`
- Related modules:
  - `.repo_context/modules/runtime/topology/README.md`
  - `.repo_context/modules/runtime/worker-runtime.md`
  - `.repo_context/modules/infra/common-infra.md`
  - `.repo_context/modules/quality/cmake-build/README.md`

## Build And Validation

The current CMake integration is guarded by `WITH_TESTS`; production Coordinator service construction is not yet enabled. Validate changes with a full incremental CMake build before running the registered Coordinator Raft UT/ST coverage. Follow `.repo_context/playbooks/features/runtime/coordinator-election/implementation.md` for the current test and lifecycle verification surface.
