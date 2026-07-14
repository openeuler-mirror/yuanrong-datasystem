# Recovery And Persistence Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - durable state, metadata, recovery, failover, restart, compaction, migration, transfer, preload, and cleanup changes
- Owning module or area:
  - repository-wide recovery and persistence gates
- Primary source paths:
  - `src/datasystem/common/kvstore`
  - `src/datasystem/common/l2cache`
  - `src/datasystem/worker/object_cache`
  - `src/datasystem/cluster`
  - `src/datasystem/master`
  - `tests/st/client/kv_cache`
  - `tests/st/worker/object_cache`
  - `tests/ut/common/l2cache`
- Last verified against source:
  - `2026-05-14`

## When To Use

Use this playbook when a change touches:

- ETCD, Metastore, RocksDB, l2cache, slot storage, or local persisted files;
- object metadata, copy metadata, topology metadata, cluster membership, or recovery incidents;
- worker restart, passive scale-down, voluntary scale-down, scale-up, or failover;
- compaction, cleanup, preload, spill/load, migration, transfer, or ownership handoff;
- storage format, record format, manifest/index semantics, or version compatibility.

## Persistence Decision Gate

For every stateful change, answer:

| Question | Required conclusion |
| --- | --- |
| Can the state be rebuilt after process restart? | source of truth and rebuild path |
| Can the state be rebuilt after node failure? | owner and successor behavior |
| Is the state needed for retry/idempotency? | replay key, dedupe key, or no durable need |
| Does metadata need to match data durability? | ordering and reconciliation plan |
| Is cleanup safe after partial progress? | cleanup marker or idempotent delete |

If the answer is "no persistence needed", record why when the state crosses an async, recovery, or failover boundary.

## Crash Consistency Checklist

- [ ] Write ordering is explicit.
- [ ] Metadata is not committed before data is recoverable unless recovery can reconcile it.
- [ ] Partial writes are detected or ignored safely.
- [ ] Atomic replace or manifest transition is used for multi-file state when needed.
- [ ] fsync, flush, or backend durability assumptions are named.
- [ ] Replaying the operation is idempotent.
- [ ] Cleanup can run more than once.
- [ ] Startup rebuild handles old, missing, partial, and extra records.
- [ ] Format or schema changes have compatibility notes.

## Failover And Recovery Checklist

- [ ] Owner and successor are explicit.
- [ ] In-progress task state has a recovery owner.
- [ ] Retry loops are bounded or have backoff.
- [ ] Node restart does not duplicate committed state.
- [ ] Passive and voluntary scale-down behavior is considered when the module participates in ownership transfer.
- [ ] Preload or migration failure leaves previous valid data intact.
- [ ] Recovery observability exists through logs, metrics, inject points, or tests.

## Source Areas To Inspect

Use the nearest module docs first, then source:

- `.repo_context/modules/runtime/topology/README.md`
- `.repo_context/modules/runtime/etcd-metadata/README.md`
- `.repo_context/modules/infra/l2cache/design.md`
- `.repo_context/modules/infra/slot/design.md`

Useful searches:

```bash
rtk rg -n "Recover|Recovery|Replay|Restart|Scale|Failover|Incident|Preload|Compact|Manifest" src tests
rtk rg -n "RawPut|RawGet|CompareAndSwap|Put\\(|Delete\\(|Rocks|Metastore|Etcd|SlotManifest" src/datasystem
rtk rg -n "INJECT_POINT|WaitUntil|Restart|ScaleDown|Recover|Compact" tests
```

## Test Expectations

Prefer inject-point or cluster tests when failure ordering matters. Recovery tests should cover at least one of:

- restart before/after commit;
- partial write or partially switched manifest;
- owner failure during recovery;
- duplicate retry of a task;
- compaction racing with foreground mutation;
- metadata present with missing data or data present with missing metadata;
- cleanup after failed or repeated recovery.

If adding only a unit test, explain why the state transition does not require cluster-level validation.

## Stop Conditions

Stop and narrow scope when:

- the change alters a persisted format without migration requirements;
- the change needs cross-version compatibility that is not specified;
- recovery correctness depends on a component outside the inspected module;
- tests cannot simulate the failure order needed to prove safety.
