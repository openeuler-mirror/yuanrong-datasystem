#include "test_harness.h"
#include "benchmark/benchmark_runner.h"
#include "common/config.h"
#include <thread>

// --- Key calculation tests ---

TEST(CalcKeysPerRound_Basic) {
    // 4096MB * 0.8 / 8MB = 409 keys
    int keys = CalcKeysPerRound(4096, 8 * 1024 * 1024);
    ASSERT_EQ(keys, 409);
}

TEST(CalcKeysPerRound_SmallMemory) {
    // 100MB * 0.8 / 8MB = 10 keys
    int keys = CalcKeysPerRound(100, 8 * 1024 * 1024);
    ASSERT_EQ(keys, 10);
}

TEST(CalcKeysPerRound_LargeData) {
    // 4096MB * 0.8 / 4GB = 0 keys -> clamp to 1
    int keys = CalcKeysPerRound(4096, 4ULL * 1024 * 1024 * 1024);
    ASSERT_EQ(keys, 1);
}

TEST(CalcKeysPerRound_ZeroMemory) {
    int keys = CalcKeysPerRound(0, 8 * 1024 * 1024);
    ASSERT_EQ(keys, 0);
}

// --- Key name generation tests ---

TEST(MakeBenchKey_Basic) {
    ASSERT_EQ(MakeBenchKey(0, 0, 42), std::string("bench_0_0_42"));
    ASSERT_EQ(MakeBenchKey(0, 3, 0), std::string("bench_0_3_0"));
    ASSERT_EQ(MakeBenchKey(1, 10, 999), std::string("bench_1_10_999"));
}

// --- Thread key range tests ---

TEST(ThreadKeyRange_Basic) {
    // 512 keys, 8 threads
    auto r = ThreadKeyRange(512, 8, 0);
    ASSERT_EQ(r.first, 0);
    ASSERT_EQ(r.second, 64);
}

TEST(ThreadKeyRange_LastThread) {
    // 512 keys, 8 threads, thread 7
    auto r = ThreadKeyRange(512, 8, 7);
    ASSERT_EQ(r.first, 448);
    ASSERT_EQ(r.second, 64);
}

TEST(ThreadKeyRange_Uneven) {
    // 100 keys, 8 threads: first 4 threads get 13, rest get 12
    auto r0 = ThreadKeyRange(100, 8, 0);
    ASSERT_EQ(r0.first, 0);
    ASSERT_EQ(r0.second, 13);

    auto r7 = ThreadKeyRange(100, 8, 7);
    ASSERT_EQ(r7.first, 88);
    ASSERT_EQ(r7.second, 12);
}

TEST(ThreadKeyRange_SingleThread) {
    auto r = ThreadKeyRange(50, 1, 0);
    ASSERT_EQ(r.first, 0);
    ASSERT_EQ(r.second, 50);
}

TEST(ThreadKeyRange_MoreThreadsThanKeys) {
    // 3 keys, 8 threads: thread 0-2 get 1 each, thread 3-7 get 0
    auto r0 = ThreadKeyRange(3, 8, 0);
    ASSERT_EQ(r0.second, 1);
    auto r3 = ThreadKeyRange(3, 8, 3);
    ASSERT_EQ(r3.second, 0);
}

#include "stubs/kv_client_stub.h"

// --- PhaseResult tests ---

TEST(BenchmarkWorker_SetPhase_StringView) {
    StubKVClient client;
    auto result = RunSetPhase(&client, 0, 0, 0, 4, "string_view", "data");
    ASSERT_EQ(result.successCount, 4);
    ASSERT_EQ(client.setCount.load(), 4);
    ASSERT_EQ(result.latenciesMs.size(), 4u);
    for (auto ms : result.latenciesMs) {
        ASSERT_TRUE(ms >= 0);
    }
}

TEST(BenchmarkWorker_GetPhase) {
    StubKVClient client;
    auto result = RunGetPhase(&client, 0, 0, 0, 4);
    ASSERT_EQ(result.successCount, 4);
    ASSERT_EQ(client.getCount.load(), 4);
    ASSERT_EQ(result.latenciesMs.size(), 4u);
}

TEST(BenchmarkWorker_DelPhase) {
    StubKVClient client;
    RunDelPhase(&client, 0, 0, 0, 4);
    ASSERT_EQ(client.delCount.load(), 4);
}

TEST(BenchmarkWorker_DelPhase_Batched) {
    // 2500 keys, timeout=20 -> 20 parallel batches of ~125 keys each
    StubKVClient client;
    RunDelPhase(&client, 0, 0, 0, 2500);
    ASSERT_EQ(client.delCount.load(), 2500);
}

TEST(BenchmarkWorker_DelPhase_LargeBatch) {
    // 50000 keys, timeout=10 -> min(10,1000,50000)=10 batches, 5000 each -> exceeds 1000
    // -> recalculated: 50 batches of 1000 each
    StubKVClient client;
    RunDelPhase(&client, 0, 0, 0, 50000);
    ASSERT_EQ(client.delCount.load(), 50000);
}

TEST(BenchmarkWorker_SetPhase_Failure) {
    StubKVClient client;
    client.failSets = true;
    auto result = RunSetPhase(&client, 0, 0, 0, 4, "string_view", "data");
    ASSERT_EQ(result.successCount, 0);
    ASSERT_EQ(result.failureCount, 4);
    ASSERT_EQ(result.latenciesMs.size(), 0u);
}

TEST(BenchmarkWorker_SetPhase_CreateBuffer) {
    StubKVClient client;
    auto result = RunSetPhase(&client, 0, 0, 0, 4, "create_buffer", "data");
    ASSERT_EQ(result.successCount, 4);
    ASSERT_EQ(client.createCount.load(), 4);
    ASSERT_EQ(client.setCount.load(), 4);
    ASSERT_EQ(result.latenciesMs.size(), 4u);
}

// --- Percentile computation tests ---

TEST(ComputePercentiles_Basic) {
    std::vector<double> lat = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
    auto p = ComputePercentiles(std::move(lat));
    ASSERT_EQ(p.avg, 5.5);
    ASSERT_EQ(p.min, 1.0);
    // nearest-rank: ceil(pct/100 * N) - 1
    // p50: ceil(0.5*10)-1 = 5-1 = 4 → lat[4] = 5.0
    // p90: ceil(0.9*10)-1 = 9-1 = 8 → lat[8] = 9.0
    // p99: ceil(0.99*10)-1 = 10-1 = 9 → lat[9] = 10.0
    // p999/p9999: ceil(0.999*10)/ceil(0.9999*10)-1 = 10-1 = 9 → lat[9] = 10.0
    ASSERT_EQ(p.p50, 5.0);
    ASSERT_EQ(p.p90, 9.0);
    ASSERT_EQ(p.p99, 10.0);
    ASSERT_EQ(p.p999, 10.0);
    ASSERT_EQ(p.p9999, 10.0);
    ASSERT_EQ(p.max, 10.0);
}

TEST(ComputePercentiles_Single) {
    std::vector<double> lat = {3.14};
    auto p = ComputePercentiles(std::move(lat));
    ASSERT_EQ(p.avg, 3.14);
    ASSERT_EQ(p.min, 3.14);
    ASSERT_EQ(p.p50, 3.14);
    ASSERT_EQ(p.p99, 3.14);
    ASSERT_EQ(p.p999, 3.14);
    ASSERT_EQ(p.max, 3.14);
}

TEST(ComputePercentiles_Empty) {
    auto p = ComputePercentiles({});
    ASSERT_EQ(p.avg, 0);
    ASSERT_EQ(p.min, 0);
    ASSERT_EQ(p.p50, 0);
    ASSERT_EQ(p.p99, 0);
    ASSERT_EQ(p.p999, 0);
    ASSERT_EQ(p.max, 0);
}

// --- Barrier tests ---

TEST(Barrier_Wait) {
    Barrier barrier(3);
    std::atomic<int> phase{0};
    auto fn = [&](int id) {
        barrier.Wait();
        if (id == 0) phase.store(1);
        barrier.Wait();
    };
    std::thread t0(fn, 0), t1(fn, 1), t2(fn, 2);
    t0.join(); t1.join(); t2.join();
    ASSERT_EQ(phase.load(), 1);
}

// --- Round loop test with stub ---

TEST(RunBenchmarkRounds_SingleRound) {
    StubKVClient client;
    BenchmarkStats stats;
    BenchmarkParams params;
    params.numThreads = 2;
    params.keysPerRound = 10;
    params.setApi = "string_view";
    params.isGetMode = true;
    params.cleanupMethod = "del";
    params.maxRounds = 1;
    params.maxDurationMs = 0;
    params.dataSize = 8;
    params.data = "testdata";
    RunBenchmarkRounds(&client, &stats, params);
    ASSERT_EQ(stats.roundsCompleted.load(), 1);
    ASSERT_EQ(stats.totalSet.load(), 10);
    ASSERT_EQ(stats.totalGet.load(), 10);
    ASSERT_EQ(stats.totalDel.load(), 10);
}

// --- MSet/MGet phase tests ---

TEST(BenchmarkWorker_MSetPhase) {
    StubKVClient client;
    auto result = RunMSetPhase(&client, 0, 0, 0, 10, 3, "data");
    ASSERT_EQ(result.successCount, 10);
    ASSERT_EQ(client.msetCount.load(), 10);
}

TEST(BenchmarkWorker_MSetPhase_ExactBatch) {
    StubKVClient client;
    // 9 keys, batchSize=3 -> 3 batches of exactly 3
    auto result = RunMSetPhase(&client, 0, 0, 0, 9, 3, "data");
    ASSERT_EQ(result.successCount, 9);
    ASSERT_EQ(client.msetCount.load(), 9);
}

TEST(BenchmarkWorker_MSetPhase_PartialBatch) {
    StubKVClient client;
    // 10 keys, batchSize=3 -> batches of 3, 3, 3, 1
    auto result = RunMSetPhase(&client, 0, 0, 0, 10, 3, "data");
    ASSERT_EQ(result.successCount, 10);
    ASSERT_EQ(client.msetCount.load(), 10);
}

TEST(BenchmarkWorker_MSetPhase_SingleBatch) {
    StubKVClient client;
    // 5 keys, batchSize=10 -> single batch of 5
    auto result = RunMSetPhase(&client, 0, 0, 0, 5, 10, "data");
    ASSERT_EQ(result.successCount, 5);
    ASSERT_EQ(client.msetCount.load(), 5);
}

TEST(BenchmarkWorker_MSetPhase_Failure) {
    StubKVClient client;
    client.failSets = true;
    auto result = RunMSetPhase(&client, 0, 0, 0, 6, 3, "data");
    ASSERT_EQ(result.successCount, 0);
    ASSERT_EQ(result.failureCount, 2);
    ASSERT_EQ(result.latenciesMs.size(), 0u);
}

TEST(BenchmarkWorker_MGetPhase) {
    StubKVClient client;
    auto result = RunMGetPhase(&client, 0, 0, 0, 10, 3);
    ASSERT_EQ(result.successCount, 10);
    ASSERT_EQ(client.mgetCount.load(), 10);
}

TEST(BenchmarkWorker_MGetPhase_ExactBatch) {
    StubKVClient client;
    auto result = RunMGetPhase(&client, 0, 0, 0, 9, 3);
    ASSERT_EQ(result.successCount, 9);
    ASSERT_EQ(client.mgetCount.load(), 9);
}

TEST(BenchmarkWorker_MGetPhase_Failure) {
    StubKVClient client;
    client.failGets = true;
    auto result = RunMGetPhase(&client, 0, 0, 0, 6, 3);
    ASSERT_EQ(result.successCount, 0);
    ASSERT_EQ(result.failureCount, 2);
    ASSERT_EQ(result.latenciesMs.size(), 0u);
}

TEST(RunBenchmarkRounds_MultipleRounds) {
    StubKVClient client;
    BenchmarkStats stats;
    BenchmarkParams params;
    params.numThreads = 2;
    params.keysPerRound = 4;
    params.setApi = "string_view";
    params.isGetMode = false;  // set_local: Set only, no Get
    params.cleanupMethod = "del";
    params.maxRounds = 3;
    params.maxDurationMs = 0;
    params.dataSize = 8;
    params.data = "testdata";
    RunBenchmarkRounds(&client, &stats, params);
    ASSERT_EQ(stats.roundsCompleted.load(), 3);
    ASSERT_EQ(stats.totalSet.load(), 12);  // 3 rounds * 4 keys
    ASSERT_EQ(stats.totalGet.load(), 0);   // not get mode
    ASSERT_EQ(stats.totalDel.load(), 12);
}

// --- GetRoundForGet tests ---

TEST(GetRoundForGet_SameKeys_RoundZero) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::SAME_KEYS, 0), 0);
}

TEST(GetRoundForGet_SameKeys_RoundOne) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::SAME_KEYS, 1), 1);
}

TEST(GetRoundForGet_SameKeys_RoundFive) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::SAME_KEYS, 5), 5);
}

TEST(GetRoundForGet_ReadPrev_RoundZero) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::READ_PREV, 0), -1);
}

TEST(GetRoundForGet_ReadPrev_RoundOne) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::READ_PREV, 1), 0);
}

TEST(GetRoundForGet_ReadPrev_RoundFive) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::READ_PREV, 5), 4);
}

TEST(GetRoundForGet_Independent_RoundZero) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::INDEPENDENT, 0), 0);
}

TEST(GetRoundForGet_Independent_RoundFive) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::INDEPENDENT, 5), 0);
}

TEST(GetRoundForGet_Independent_RoundHundred) {
    ASSERT_EQ(GetRoundForGet(MixedKeyStrategy::INDEPENDENT, 100), 0);
}

