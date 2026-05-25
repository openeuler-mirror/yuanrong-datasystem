#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <optional>

struct NodeInfo {
    std::string host;
    int port = 9000;
    int instanceId = 0;
    std::string role;  // "writer" or "reader", empty = same as local role
};

enum class TestMode {
    NONE = 0,
    SET_LOCAL,
    SET_REMOTE,
    GET_LOCAL,
    GET_CROSS_NODE,
    GET_REMOTE_DIRECT,
    GET_REMOTE_CROSS,
};

enum class RunMode { PIPELINE, CACHE, BENCHMARK };

TestMode ParseTestMode(const std::string &s);
bool NeedsRemoteWorker(TestMode mode);
bool IsGetMode(TestMode mode);
std::optional<RunMode> ParseRunMode(const std::string &s);

struct Config {
    RunMode runMode = RunMode::PIPELINE;
    int instanceId = 0;
    int listenPort = 9000;
    std::string etcdAddress;
    std::string clusterName = "";
    std::string hostIdEnvName = "JD_HOST_IP";
    int32_t connectTimeoutMs = 1000;
    int32_t requestTimeoutMs = 20;
    uint64_t fastTransportMemSize = 512ULL * 1024 * 1024;  // 512MB default
    std::vector<uint64_t> dataSizes;          // parsed bytes
    uint32_t ttlSeconds = 0;            // from set_param.ttl_second, 0 = no expiry
    int targetQps = 100; // 0 = unlimited
    std::vector<int> targetQpsStages;  // QPS stages (empty = single fixed QPS)
    int stageDurationSeconds = 0;      // seconds per stage, 0 = disabled
    int numThreads = 16;
    int notifyCount = 10;
    int notifyIntervalUs = 0; // delay between peer notifications in microseconds, 0 = parallel
    bool enableJitter = true; // randomize sleep to stagger requests
    bool enableCrossNodeConnection = true; // allow failover to standby workers on other nodes
    int batchKeysCount = 1;                 // batch 操作的 key 数量，1 = 单 key 兼容
    int metricsIntervalMs = 3000;
    std::string metricsFile;  // resolved in LoadConfig: outputDir_/latency_timeseries.csv
    std::string outputDir;    // e.g. metrics_192.168.1.10_20260519_171440
    std::vector<NodeInfo> nodes;
    std::vector<std::string> peers;           // from config or auto-generated from nodes
    // Pipeline fields
    std::string role = "writer";
    std::vector<std::string> pipeline = {"setStringView"};
    std::vector<std::string> notifyPipeline = {"getBuffer"};
    std::string cpuAffinity;  // optional, e.g. "0-7" or "0,2,4,6", empty = auto-detect
    // Cache mode fields
    int keyPoolSize = 0;             // 0 = disabled (normal pipeline mode)
    int inferenceDelayMs = 0;        // simulate inference delay on cache miss
    int warmupBatchSize = 100;       // reserved: keys per warmup batch (currently individual writes)
    int warmupRetryCount = 3;        // retries per warmup key
    int warmupRetryDelayMs = 1000;   // delay between retries
    int warmupTimeoutSeconds = 300;  // Reader wait timeout for all Writer warmups
    int maxKeyPoolSize = 0;          // 0 = auto (keyPoolSize * 20)
    double targetHitRate = 0.0;      // 0 = fixed pool, 0.01-1.0 = auto-adjust pool to target hit rate
    // Benchmark mode fields (test_mode != NONE)
    TestMode testMode = TestMode::NONE;
    int workerMemoryMb = 0;
    int durationSeconds = 0;
    int totalRounds = 0;
    std::string setApi = "string_view";
    std::string cleanupMethod = "del";
    struct {
        std::string host;
        int port = 31501;
    } remoteWorker;
};

// Parse "8MB" -> 8388608, "512KB" -> 524288, "1024" -> 1024
uint64_t ParseSize(const std::string &str);

// Load config from JSON file. Returns true on success.
bool LoadConfig(const std::string &path, Config &cfg);
