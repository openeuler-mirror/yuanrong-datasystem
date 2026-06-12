#include "test_harness.h"
#include "common/config.h"
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

static std::string WriteTempConfig(const std::string &json) {
    static int idx = 0;
    std::string path = "/tmp/kvtest_config_" + std::to_string(getpid()) + "_" + std::to_string(idx++) + ".json";
    std::ofstream f(path);
    f << json;
    f.close();
    return path;
}

static void CleanupDir(const std::string &dir) {
    std::filesystem::remove_all(dir);
}

// --- ParseSize tests ---

TEST(ParseSize_MB) {
    ASSERT_EQ(ParseSize("8MB"), 8ULL * 1024 * 1024);
}

TEST(ParseSize_KB) {
    ASSERT_EQ(ParseSize("512KB"), 512ULL * 1024);
}

TEST(ParseSize_GB) {
    ASSERT_EQ(ParseSize("2GB"), 2ULL * 1024 * 1024 * 1024);
}

TEST(ParseSize_Bytes) {
    ASSERT_EQ(ParseSize("1024"), 1024ULL);
}

TEST(ParseSize_ExplicitB) {
    ASSERT_EQ(ParseSize("4096B"), 4096ULL);
}

TEST(ParseSize_Lowercase) {
    ASSERT_EQ(ParseSize("8mb"), 8ULL * 1024 * 1024);
}

TEST(ParseSize_Empty) {
    ASSERT_EQ(ParseSize(""), 0ULL);
}

TEST(ParseSize_Invalid) {
    ASSERT_EQ(ParseSize("abc"), 0ULL);
}

TEST(ParseSize_Zero) {
    ASSERT_EQ(ParseSize("0MB"), 0ULL);
}

TEST(ParseSize_Fractional) {
    ASSERT_EQ(ParseSize("1.5MB"), static_cast<uint64_t>(1.5 * 1024 * 1024));
}

// --- LoadConfig tests ---

TEST(LoadConfig_Minimal) {
    auto path = WriteTempConfig(R"({"etcd_address":"127.0.0.1:2379","listen_port":9000})");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.etcdAddress, "127.0.0.1:2379");
    ASSERT_EQ(cfg.listenPort, 9000);
    ASSERT_EQ(cfg.dataSizes.size(), 1u);
    ASSERT_EQ(cfg.dataSizes[0], 8ULL * 1024 * 1024);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_DataSizes) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000,"data_sizes":["8MB","512KB"]})");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.dataSizes.size(), 2u);
    ASSERT_EQ(cfg.dataSizes[0], 8ULL * 1024 * 1024);
    ASSERT_EQ(cfg.dataSizes[1], 512ULL * 1024);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_MissingEtcd) {
    auto path = WriteTempConfig(R"({"listen_port":9000})");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_InvalidPort) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":99999})");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_NegativeThreads) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000,"num_threads":0})");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_OutputDir) {
    auto path = WriteTempConfig(R"({"etcd_address":"192.168.1.10:2379","listen_port":9000})");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_TRUE(cfg.outputDir.find("metrics_192.168.1.10_") == 0);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_MetricsFile_Default) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000})");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.metricsFile, cfg.outputDir + "/latency_timeseries.csv");
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_MetricsFile_PlainName) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000,"metrics_file":"custom.csv"})");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.metricsFile, cfg.outputDir + "/custom.csv");
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_MetricsFile_Absolute) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000,"metrics_file":"/tmp/custom.csv"})");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.metricsFile, "/tmp/custom.csv");
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_NodesAutoPeers) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,"instance_id":0,
        "nodes":[
            {"host":"h1","port":9000,"instance_id":0},
            {"host":"h2","port":9000,"instance_id":1}
        ]
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.peers.size(), 1u);
    ASSERT_EQ(cfg.peers[0], "http://h2:9000");
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_CacheModePeerFilter) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,"instance_id":0,
        "role":"writer","key_pool_size":100,
        "nodes":[
            {"host":"h1","port":9000,"instance_id":0,"role":"writer"},
            {"host":"h2","port":9000,"instance_id":1,"role":"reader"}
        ]
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.peers.size(), 1u);
    ASSERT_EQ(cfg.peers[0], "http://h2:9000");
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_QpsStages) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "target_qps":[100,200,300],"stage_duration_seconds":60
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.targetQpsStages.size(), 3u);
    ASSERT_EQ(cfg.targetQpsStages[0], 100);
    ASSERT_EQ(cfg.targetQpsStages[2], 300);
    ASSERT_EQ(cfg.targetQps, 100);
    ASSERT_EQ(cfg.stageDurationSeconds, 60);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_QpsStages_MissingDuration) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000,"target_qps":[100,200]})");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_MaxKeyPoolSize_Auto) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000,"key_pool_size":100})");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.maxKeyPoolSize, 2000);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_BatchKeysCount_Invalid) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000,"batch_keys_count":0})");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_BadJson) {
    auto path = WriteTempConfig("{invalid");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_NonexistentFile) {
    Config cfg;
    ASSERT_FALSE(LoadConfig("/tmp/kvtest_nonexistent_xyz.json", cfg));
}

// --- TestMode tests ---

TEST(ParseTestMode_Valid) {
    ASSERT_EQ(ParseTestMode("set_local"), TestMode::SET_LOCAL);
    ASSERT_EQ(ParseTestMode("set_remote"), TestMode::SET_REMOTE);
    ASSERT_EQ(ParseTestMode("get_local"), TestMode::GET_LOCAL);
    ASSERT_EQ(ParseTestMode("get_cross_node"), TestMode::GET_CROSS_NODE);
    ASSERT_EQ(ParseTestMode("get_remote_direct"), TestMode::GET_REMOTE_DIRECT);
    ASSERT_EQ(ParseTestMode("get_remote_cross"), TestMode::GET_REMOTE_CROSS);
}

TEST(ParseTestMode_Invalid) {
    ASSERT_EQ(ParseTestMode("unknown"), TestMode::NONE);
    ASSERT_EQ(ParseTestMode(""), TestMode::NONE);
}

TEST(ParseTestMode_NeedsRemoteWorker) {
    ASSERT_FALSE(NeedsRemoteWorker(TestMode::SET_LOCAL));
    ASSERT_FALSE(NeedsRemoteWorker(TestMode::GET_LOCAL));
    ASSERT_TRUE(NeedsRemoteWorker(TestMode::SET_REMOTE));
    ASSERT_TRUE(NeedsRemoteWorker(TestMode::GET_CROSS_NODE));
    ASSERT_TRUE(NeedsRemoteWorker(TestMode::GET_REMOTE_DIRECT));
    ASSERT_TRUE(NeedsRemoteWorker(TestMode::GET_REMOTE_CROSS));
}

TEST(ParseTestMode_IsGetMode) {
    ASSERT_FALSE(IsGetMode(TestMode::SET_LOCAL));
    ASSERT_FALSE(IsGetMode(TestMode::SET_REMOTE));
    ASSERT_TRUE(IsGetMode(TestMode::GET_LOCAL));
    ASSERT_TRUE(IsGetMode(TestMode::GET_CROSS_NODE));
    ASSERT_TRUE(IsGetMode(TestMode::GET_REMOTE_DIRECT));
    ASSERT_TRUE(IsGetMode(TestMode::GET_REMOTE_CROSS));
}

TEST(LoadConfig_TestMode_SetLocal) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_local","worker_memory_mb":4096,"num_threads":4
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.testMode, TestMode::SET_LOCAL);
    ASSERT_EQ(cfg.workerMemoryMb, 4096);
    ASSERT_EQ(cfg.numThreads, 4);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_TestMode_RemoteWorkerRequired) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_remote","worker_memory_mb":4096
    })");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_TestMode_RemoteWorker) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_remote","worker_memory_mb":4096,
        "remote_worker":{"host":"192.168.1.100","port":31501}
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.remoteWorker.host, "192.168.1.100");
    ASSERT_EQ(cfg.remoteWorker.port, 31501);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_SetApi) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_local","worker_memory_mb":4096,
        "set_api":"create_buffer"
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.setApi, std::string("create_buffer"));
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_CleanupMethod_TTL) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_local","worker_memory_mb":4096,
        "cleanup_method":"ttl","set_param":{"ttl_second":10}
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.cleanupMethod, std::string("ttl"));
    ASSERT_EQ(cfg.ttlSeconds, 10u);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_CleanupTTL_NoTTLSecond) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_local","worker_memory_mb":4096,
        "cleanup_method":"ttl"
    })");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_DurationAndRounds) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_local","worker_memory_mb":4096,
        "duration_seconds":30,"total_rounds":5
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.durationSeconds, 30);
    ASSERT_EQ(cfg.totalRounds, 5);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_ConnectOptions) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_local","worker_memory_mb":4096,
        "connect_options":{
            "connect_timeout_ms":5000,
            "request_timeout_ms":50,
            "enable_cross_node_connection":false,
            "fast_transport_mem_size":1073741824
        }
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.connectTimeoutMs, 5000);
    ASSERT_EQ(cfg.requestTimeoutMs, 50);
    ASSERT_EQ(cfg.enableCrossNodeConnection, false);
    ASSERT_EQ(cfg.fastTransportMemSize, 1073741824);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

// --- RunMode tests ---

TEST(ParseRunMode_Valid) {
    ASSERT_EQ(ParseRunMode("pipeline"), RunMode::PIPELINE);
    ASSERT_EQ(ParseRunMode("cache"), RunMode::CACHE);
    ASSERT_EQ(ParseRunMode("benchmark"), RunMode::BENCHMARK);
}

TEST(ParseRunMode_Invalid) {
    ASSERT_EQ(ParseRunMode("unknown").has_value(), false);
    ASSERT_EQ(ParseRunMode("").has_value(), false);
}

TEST(LoadConfig_ModeAutoInfer_Pipeline) {
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000})");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.runMode, RunMode::PIPELINE);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_ModeAutoInfer_Benchmark) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"set_local","worker_memory_mb":4096
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.runMode, RunMode::BENCHMARK);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_ModeAutoInfer_Cache) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,"key_pool_size":100
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.runMode, RunMode::CACHE);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_ModeExplicit_Pipeline) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,"mode":"pipeline"
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.runMode, RunMode::PIPELINE);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_ModeExplicit_Benchmark) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,"mode":"benchmark",
        "test_mode":"set_local","worker_memory_mb":4096
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.runMode, RunMode::BENCHMARK);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}

TEST(LoadConfig_ModeBenchmark_MissingTestMode) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,"mode":"benchmark",
        "worker_memory_mb":4096
    })");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_ModeCache_MissingKeyPoolSize) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,"mode":"cache"
    })");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_ModePipeline_InvalidRole) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,"mode":"pipeline","role":"invalid"
    })");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

// --- MixedMode tests ---

TEST(ParseTestMode_MixedLocal) {
    ASSERT_TRUE(ParseTestMode("mixed_local") == TestMode::MIXED_LOCAL);
}

TEST(ParseTestMode_MixedCrossNode) {
    ASSERT_TRUE(ParseTestMode("mixed_cross_node") == TestMode::MIXED_CROSS_NODE);
}

TEST(IsMixedMode_MixedModes) {
    ASSERT_TRUE(IsMixedMode(TestMode::MIXED_LOCAL));
    ASSERT_TRUE(IsMixedMode(TestMode::MIXED_CROSS_NODE));
    ASSERT_FALSE(IsMixedMode(TestMode::SET_LOCAL));
    ASSERT_FALSE(IsMixedMode(TestMode::NONE));
}

TEST(ParseMixedKeyStrategy_All) {
    ASSERT_TRUE(ParseMixedKeyStrategy("same_keys").value() == MixedKeyStrategy::SAME_KEYS);
    ASSERT_TRUE(ParseMixedKeyStrategy("read_prev").value() == MixedKeyStrategy::READ_PREV);
    ASSERT_TRUE(ParseMixedKeyStrategy("independent").value() == MixedKeyStrategy::INDEPENDENT);
    ASSERT_TRUE(!ParseMixedKeyStrategy("unknown").has_value());
    ASSERT_TRUE(!ParseMixedKeyStrategy("").has_value());
}

TEST(LoadConfig_MixedMode_InvalidStrategy) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"mixed_local","worker_memory_mb":4096,
        "set_ratio":0.5,"mixed_key_strategy":"invalid_strategy"
    })");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_MixedMode_SetRatioOne) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"mixed_local","worker_memory_mb":4096,
        "set_ratio":1.0
    })");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_MixedMode_IndependentPlusTTL) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"mixed_local","worker_memory_mb":4096,
        "set_ratio":0.5,"mixed_key_strategy":"independent",
        "cleanup_method":"ttl","set_param":{"ttl_second":10}
    })");
    Config cfg;
    ASSERT_FALSE(LoadConfig(path, cfg));
    std::remove(path.c_str());
}

TEST(LoadConfig_MixedMode_ValidRatio) {
    auto path = WriteTempConfig(R"({
        "etcd_address":"x:1","listen_port":9000,
        "test_mode":"mixed_local","worker_memory_mb":4096,
        "set_ratio":0.7
    })");
    Config cfg;
    ASSERT_TRUE(LoadConfig(path, cfg));
    ASSERT_EQ(cfg.setRatio, 0.7);
    CleanupDir(cfg.outputDir);
    std::remove(path.c_str());
}
