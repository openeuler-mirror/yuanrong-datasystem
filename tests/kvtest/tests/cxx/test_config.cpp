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
    auto path = WriteTempConfig(R"({"etcd_address":"x:1","listen_port":9000,"num_set_threads":0})");
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
    ASSERT_EQ(cfg.maxKeyPoolSize, 1000);
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
