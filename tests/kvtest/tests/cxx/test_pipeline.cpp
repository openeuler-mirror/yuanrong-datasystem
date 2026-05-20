#include "test_harness.h"
#include "pipeline/pipeline.h"
#include "metrics/metrics.h"
#include <filesystem>

TEST(GetAllOpNames_Base) {
    auto &names = GetAllOpNames(false);
    ASSERT_EQ(names.size(), 9u);
}

TEST(GetAllOpNames_Cache) {
    auto &names = GetAllOpNames(true);
    ASSERT_EQ(names.size(), 13u);
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
    bool ok = ExecutePipeline(ops, ctx, m, vf);
    ASSERT_TRUE(ok);
    m.Stop();
    std::filesystem::remove_all("/tmp/kvtest_pipeline_test");
}
