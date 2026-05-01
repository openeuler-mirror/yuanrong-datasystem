#pragma once

#include <string>
#include <vector>
#include <cstdint>

struct NodeInfo {
    std::string host;
    int port = 9000;
    int instanceId = 0;
};

struct Config {
    int instanceId = 0;
    int listenPort = 9000;
    std::string etcdAddress;
    std::string clusterName = "";
    std::string hostIdEnvName = "JD_HOST_IP";
    int32_t connectTimeoutMs = 1000;
    int32_t requestTimeoutMs = 20;
    std::vector<uint64_t> dataSizes;          // parsed bytes
    uint32_t ttlSeconds = 5;
    int targetQps = 100; // 0 = unlimited
    int numSetThreads = 16;
    int notifyCount = 10;
    int notifyIntervalUs = 0; // delay between peer notifications in microseconds, 0 = parallel
    bool enableJitter = true; // randomize sleep to stagger requests
    bool enableCrossNodeConnection = true; // allow failover to standby workers on other nodes
    int batchKeysCount = 1;                 // batch 操作的 key 数量，1 = 单 key 兼容
    int metricsIntervalMs = 3000;
    std::string metricsFile = "metrics_{instance_id}.csv";
    std::vector<NodeInfo> nodes;
    std::vector<std::string> peers;           // from config or auto-generated from nodes
    // Pipeline fields
    std::string role = "writer";
    std::vector<std::string> pipeline = {"setStringView"};
    std::vector<std::string> notifyPipeline = {"getBuffer"};
    std::string cpuAffinity;  // optional, e.g. "0-7" or "0,2,4,6", empty = auto-detect
};

// Parse "8MB" -> 8388608, "512KB" -> 524288, "1024" -> 1024
uint64_t ParseSize(const std::string &str);

// Load config from JSON file. Returns true on success.
bool LoadConfig(const std::string &path, Config &cfg);
