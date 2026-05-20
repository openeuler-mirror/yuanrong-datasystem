# Performance-Sensitive Change Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - latency, throughput, allocation, locking, IO, batching, transfer, and background-work changes
- Owning module or area:
  - repository-wide performance gates
- Primary source paths:
  - `src/datasystem/client`
  - `src/datasystem/worker`
  - `src/datasystem/master`
  - `src/datasystem/common`
  - `dsbench`
  - `tests/perf`
  - `src/datasystem/common/perf`
- Last verified against source:
  - `2026-05-14`

## When To Use

Use this playbook when a change touches:

- SDK `Set/Get/MSet/MGet/Del` or object/stream APIs;
- worker CRUD/get/remote-get/batch-get paths;
- RDMA, P2P, shared memory, mmap, l2cache, spill/load, or preload;
- metadata fanout, route lookup, hash-ring, ETCD/Metastore access;
- thread pools, async queues, timers, metrics, logging, or background compaction;
- benchmark or perf instrumentation.

## Hot Path Classification

Before editing, classify the path:

| Class | Examples | Performance questions |
| --- | --- | --- |
| request critical | client API, worker service handler, remote get, batch get | added latency, allocations, copies, lock hold time, RPC count |
| data transfer | RDMA/UCX/P2P, shared memory, spill/load, l2cache get/put | buffer lifetime, flush/wait semantics, copy count, backpressure |
| metadata critical | ETCD/Metastore, hash ring, master metadata | fanout, retry bounds, CAS/lease cost, O(N) scans |
| background | eviction, compaction, async queue, metric flush | queue bounds, CPU/IO budget, foreground interference |
| test/benchmark | `dsbench`, `tests/perf`, perf client | measurement validity, warmup, cache hit/miss shape |

## Performance Risk Gates

Treat the following as review-blocking until justified:

- request-path string formatting or large string copies;
- per-object heap allocation in a batch path when capacity is knowable;
- broad mutexes around IO, RPC, sleep, callbacks, or logging;
- added synchronous filesystem/ETCD/Metastore access in a foreground path;
- unbounded scans, queues, retries, or background tasks;
- high-frequency `shared_ptr` copies or shared ownership in tight loops;
- changes that increase RDMA flush count or block on visibility without a measurement plan;
- added logs in high-frequency success paths;
- `unordered_map` growth without reserve in known-size batch handling.

## Required Design Notes

Record these before implementation or in the PR body:

| Topic | What to record |
| --- | --- |
| operation shape | single object, batch, stream, transfer, recovery, or metadata |
| complexity | expected O(1), O(batch), O(nodes), or other bound |
| allocation plan | new allocations, reserve strategy, or no new allocation |
| copy plan | data copy count and buffer ownership |
| lock plan | lock type, scope, and whether IO/RPC happens while locked |
| background impact | queue limits, scheduling, and foreground interference |
| measurement | test, perf point, benchmark, or reason measurement is not needed |

## Reuse First

Search existing primitives before adding instrumentation or scheduling logic:

```bash
rtk rg -n "PerfPoint|PerfKey|PerfManager|GetPerfLog|Timer" src/datasystem tests
rtk rg -n "ThreadPool|OrderedThreadPool|TimerQueue|WaitPost|Async|Queue" src/datasystem/common src/datasystem/worker
rtk rg -n "dsbench|throughput|latency|QPS|BatchGet|RemoteGet" dsbench tests/perf tests/benchmark
```

## Verification Options

Choose based on risk:

- local unit/component test for helper-level behavior;
- CTest label or single generated CTest for service behavior;
- system test for worker/client/master interactions;
- `dsbench` or `tests/perf` when latency/throughput is the purpose of the change;
- perf point/log inspection when adding instrumentation or diagnosing a bottleneck.

If a benchmark cannot be run locally, preserve a targeted functional test and record the missing performance evidence.

## Review Checklist

- [ ] No new unbounded work in request paths.
- [ ] Known-size vectors/maps reserve capacity where practical.
- [ ] No blocking IO/RPC while holding broad locks.
- [ ] No high-frequency success logs.
- [ ] Background work has a resource bound or backpressure story.
- [ ] Batch path complexity is explicit.
- [ ] Measurement or rationale is recorded.
