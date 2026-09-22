#include "test_harness.h"
#include "pipeline/pipeline.h"
#include "metrics/metrics.h"
#include <filesystem>
#include <random>

TEST(GetAllOpNames_Base) {
    auto &names = GetAllOpNames(false);
    ASSERT_EQ(names.size(), 10u);
}

TEST(GetAllOpNames_Cache) {
    auto &names = GetAllOpNames(true);
    ASSERT_EQ(names.size(), 14u);
}

TEST(GetAllOpNames_StableReference) {
    auto &a = GetAllOpNames(false);
    auto &b = GetAllOpNames(false);
    ASSERT_EQ(&a, &b);
}

TEST(GetOpFunc_KnownOps) {
    auto fn = GetOpFunc("setStringView");
    ASSERT_TRUE(fn != nullptr);
    fn = GetOpFunc("getBuffer");
    ASSERT_TRUE(fn != nullptr);
    fn = GetOpFunc("exist");
    ASSERT_TRUE(fn != nullptr);
    fn = GetOpFunc("mCreate");
    ASSERT_TRUE(fn != nullptr);
    fn = GetOpFunc("mSet");
    ASSERT_TRUE(fn != nullptr);
    fn = GetOpFunc("mGet");
    ASSERT_TRUE(fn != nullptr);
}

TEST(GetOpFunc_Unknown) {
    auto fn = GetOpFunc("unknownOp");
    ASSERT_TRUE(fn == nullptr);
}

TEST(OpNameConstants) {
    ASSERT_EQ(std::string(kOpSetStringView), "setStringView");
    ASSERT_EQ(std::string(kOpGetBuffer), "getBuffer");
    ASSERT_EQ(std::string(kOpExist), "exist");
    ASSERT_EQ(std::string(kOpCreateBuffer), "createBuffer");
    ASSERT_EQ(std::string(kOpMemoryCopy), "memoryCopy");
    ASSERT_EQ(std::string(kOpSetBuffer), "setBuffer");
    ASSERT_EQ(std::string(kOpMCreate), "mCreate");
    ASSERT_EQ(std::string(kOpMSet), "mSet");
    ASSERT_EQ(std::string(kOpMGet), "mGet");
    ASSERT_EQ(std::string(kOpCacheGetOrCreate), "cacheGetOrCreate");
}

TEST(ExecutePipeline_Empty) {
    MetricsCollector m(0, 1000, "/tmp/kvtest_pipeline_test");
    m.Start();
    std::atomic<uint64_t> vf{0};
    PipelineContext ctx;
    std::vector<std::pair<std::string, OpFunc>> ops;
    bool ok = ExecutePipeline(ops, ctx, m, vf, 0);
    ASSERT_TRUE(ok);
    m.Stop();
    std::filesystem::remove_all("/tmp/kvtest_pipeline_test");
}

TEST(SampleMgetBatchSize_Disabled_AlwaysOne) {
    Config::MgetSizeDistribution d;
    d.enabled = false;
    d.singleProb = 1.0;
    std::mt19937 rng(42);
    for (int i = 0; i < 1000; i++) {
        ASSERT_EQ(SampleMgetBatchSize(d, rng), 1);
    }
}

TEST(SampleMgetBatchSize_Distribution_FrequencyMatches) {
    Config::MgetSizeDistribution d;
    d.enabled = true;
    d.singleProb = 0.95;
    d.minBatch = 2;
    d.maxBatch = 5;
    std::mt19937 rng(12345);
    constexpr int N = 100000;
    int singleCount = 0;
    int batchCounts[6] = {0};  // index 2..5 used
    for (int i = 0; i < N; i++) {
        int b = SampleMgetBatchSize(d, rng);
        ASSERT_TRUE(b == 1 || (b >= 2 && b <= 5));
        if (b == 1) singleCount++;
        else batchCounts[b]++;
    }
    // 95% +/- 1% tolerance (Monte Carlo noise)
    double singleRatio = static_cast<double>(singleCount) / N;
    ASSERT_TRUE(singleRatio > 0.94 && singleRatio < 0.96);
    // Batch branch total should be ~5%
    int batchTotal = N - singleCount;
    double batchRatio = static_cast<double>(batchTotal) / N;
    ASSERT_TRUE(batchRatio > 0.04 && batchRatio < 0.06);
    // Each of [2,5] should be roughly 1/4 of the batch total
    int expectedPerBatch = batchTotal / 4;
    for (int b = 2; b <= 5; b++) {
        // Allow 30% relative tolerance for uniform distribution noise
        ASSERT_TRUE(batchCounts[b] > expectedPerBatch * 0.7);
        ASSERT_TRUE(batchCounts[b] < expectedPerBatch * 1.3);
    }
}

TEST(SampleMgetBatchSize_Edge_AllBatch) {
    // singleProb = 0 -> always batch path
    Config::MgetSizeDistribution d;
    d.enabled = true;
    d.singleProb = 0.0;
    d.minBatch = 3;
    d.maxBatch = 3;
    std::mt19937 rng(7);
    for (int i = 0; i < 1000; i++) {
        ASSERT_EQ(SampleMgetBatchSize(d, rng), 3);
    }
}
