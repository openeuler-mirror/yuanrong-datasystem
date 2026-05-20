#include "config.h"
#include "vendor/nlohmann_json.hpp"
#include <fstream>
#include <sstream>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <chrono>
#include "simple_log.h"

using json = nlohmann::json;

uint64_t ParseSize(const std::string &str) {
    if (str.empty()) return 0;

    char *end = nullptr;
    double val = std::strtod(str.c_str(), &end);
    if (end == str.c_str()) return 0;
    if (val < 0 || val == HUGE_VAL || val == -HUGE_VAL) {
        SLOG_WARN("ParseSize: invalid value '" << str << "', treating as 0");
        return 0;
    }

    std::string unit(end);
    uint64_t multiplier = 1;
    if (unit == "GB" || unit == "gb") multiplier = 1024ULL * 1024 * 1024;
    else if (unit == "MB" || unit == "mb") multiplier = 1024ULL * 1024;
    else if (unit == "KB" || unit == "kb") multiplier = 1024ULL;
    else if (unit == "B" || unit == "b" || unit.empty()) multiplier = 1;

    return static_cast<uint64_t>(val * multiplier);
}

bool LoadConfig(const std::string &path, Config &cfg) {
    std::ifstream f(path);
    if (!f.is_open()) {
        SLOG_ERROR("Cannot open config file: " << path);
        return false;
    }

    try {
        json j = json::parse(f);

        if (j.contains("instance_id")) cfg.instanceId = j["instance_id"];
        if (j.contains("listen_port")) cfg.listenPort = j["listen_port"];
        if (j.contains("etcd_address")) cfg.etcdAddress = j["etcd_address"];
        if (j.contains("cluster_name")) cfg.clusterName = j["cluster_name"];
        if (j.contains("host_id_env_name")) cfg.hostIdEnvName = j["host_id_env_name"];
        if (j.contains("connect_timeout_ms")) cfg.connectTimeoutMs = j["connect_timeout_ms"];
        if (j.contains("request_timeout_ms")) cfg.requestTimeoutMs = j["request_timeout_ms"];
        if (j.contains("fast_transport_mem_size")) {
            cfg.fastTransportMemSize = ParseSize(j["fast_transport_mem_size"].get<std::string>());
        }
        if (j.contains("ttl_seconds")) cfg.ttlSeconds = j["ttl_seconds"];
        if (j.contains("target_qps")) {
            if (j["target_qps"].is_array()) {
                for (auto &v : j["target_qps"]) {
                    cfg.targetQpsStages.push_back(v.get<int>());
                }
                if (!cfg.targetQpsStages.empty()) {
                    cfg.targetQps = cfg.targetQpsStages[0];
                }
            } else {
                cfg.targetQps = j["target_qps"].get<int>();
            }
        }
        if (j.contains("stage_duration_seconds")) {
            cfg.stageDurationSeconds = j["stage_duration_seconds"];
        }
        if (j.contains("num_set_threads")) cfg.numSetThreads = j["num_set_threads"];
        if (j.contains("notify_count")) cfg.notifyCount = j["notify_count"];
        if (j.contains("notify_interval_us")) cfg.notifyIntervalUs = j["notify_interval_us"];
        if (j.contains("enable_jitter")) cfg.enableJitter = j["enable_jitter"];
        if (j.contains("enable_cross_node_connection")) cfg.enableCrossNodeConnection = j["enable_cross_node_connection"];
        if (j.contains("batch_keys_count")) cfg.batchKeysCount = j["batch_keys_count"];
        if (j.contains("cpu_affinity")) cfg.cpuAffinity = j["cpu_affinity"].get<std::string>();
        if (j.contains("key_pool_size")) cfg.keyPoolSize = j["key_pool_size"];
        if (j.contains("inference_delay_ms")) cfg.inferenceDelayMs = j["inference_delay_ms"];
        if (j.contains("warmup_batch_size")) cfg.warmupBatchSize = j["warmup_batch_size"];
        if (j.contains("warmup_retry_count")) cfg.warmupRetryCount = j["warmup_retry_count"];
        if (j.contains("warmup_retry_delay_ms")) cfg.warmupRetryDelayMs = j["warmup_retry_delay_ms"];
        if (j.contains("warmup_timeout_seconds")) cfg.warmupTimeoutSeconds = j["warmup_timeout_seconds"];
        if (j.contains("max_key_pool_size")) cfg.maxKeyPoolSize = j["max_key_pool_size"];
        if (j.contains("target_hit_rate")) cfg.targetHitRate = j["target_hit_rate"];
        if (j.contains("metrics_interval_ms")) cfg.metricsIntervalMs = j["metrics_interval_ms"];
        if (j.contains("metrics_file")) cfg.metricsFile = j["metrics_file"];
        if (j.contains("output_dir")) cfg.outputDir = j["output_dir"].get<std::string>();

        if (j.contains("data_sizes")) {
            for (auto &s : j["data_sizes"]) {
                cfg.dataSizes.push_back(ParseSize(s.get<std::string>()));
            }
        }
        if (cfg.dataSizes.empty()) {
            cfg.dataSizes.push_back(8 * 1024 * 1024);  // default 8MB
        }

        if (j.contains("peers")) {
            for (auto &p : j["peers"]) {
                cfg.peers.push_back(p.get<std::string>());
            }
        }

        if (j.contains("nodes")) {
            for (auto &n : j["nodes"]) {
                NodeInfo ni;
                ni.host = n.value("host", "");
                ni.port = n.value("port", 9000);
                ni.instanceId = n.value("instance_id", 0);
                ni.role = n.value("role", "");
                cfg.nodes.push_back(ni);
            }
        }

        // Auto-generate peers from nodes if peers not explicitly set
        if (cfg.peers.empty() && !cfg.nodes.empty()) {
            for (auto &n : cfg.nodes) {
                if (n.instanceId == cfg.instanceId) continue;
                cfg.peers.push_back("http://" + n.host + ":" + std::to_string(n.port));
            }
        }

        if (j.contains("role")) cfg.role = j["role"].get<std::string>();
        // Cache mode: Writer only notifies Readers
        if (cfg.keyPoolSize > 0 && cfg.role == "writer" && !cfg.nodes.empty()) {
            std::vector<std::string> filteredPeers;
            for (auto &n : cfg.nodes) {
                if (n.instanceId == cfg.instanceId) continue;
                if (n.role == "reader") {
                    filteredPeers.push_back("http://" + n.host + ":" + std::to_string(n.port));
                }
            }
            if (!filteredPeers.empty()) {
                cfg.peers = std::move(filteredPeers);
                cfg.notifyCount = static_cast<int>(cfg.peers.size());
            }
        }
        if (j.contains("pipeline")) {
            cfg.pipeline.clear();
            for (auto &s : j["pipeline"]) {
                cfg.pipeline.push_back(s.get<std::string>());
            }
        }
        if (j.contains("notify_pipeline")) {
            cfg.notifyPipeline.clear();
            for (auto &s : j["notify_pipeline"]) {
                cfg.notifyPipeline.push_back(s.get<std::string>());
            }
        }

        // Generate output directory: metrics_{ip}_{timestamp}
        if (cfg.outputDir.empty()) {
            std::string ip = cfg.etcdAddress;
            auto colonPos = ip.find(':');
            if (colonPos != std::string::npos) ip = ip.substr(0, colonPos);
            auto now = std::chrono::system_clock::now();
            auto nowT = std::chrono::system_clock::to_time_t(now);
            std::tm tmBuf;
            localtime_r(&nowT, &tmBuf);
            std::ostringstream ts;
            ts << std::put_time(&tmBuf, "%Y%m%d_%H%M%S");
            cfg.outputDir = "metrics_" + ip + "_" + ts.str();
        }
        std::filesystem::create_directories(cfg.outputDir);

        // If user set metrics_file explicitly, place it inside outputDir;
        // otherwise use the default name.
        if (cfg.metricsFile.empty() || cfg.metricsFile.find("{instance_id}") != std::string::npos) {
            cfg.metricsFile = cfg.outputDir + "/latency_timeseries.csv";
        } else if (cfg.metricsFile.find('/') == std::string::npos) {
            // Simple filename without path → place inside outputDir
            cfg.metricsFile = cfg.outputDir + "/" + cfg.metricsFile;
        }
        // else: absolute or relative path with directory → use as-is

        if (cfg.maxKeyPoolSize == 0 && cfg.keyPoolSize > 0) {
            cfg.maxKeyPoolSize = cfg.keyPoolSize * 10;
        }

    } catch (const std::exception &e) {
        SLOG_ERROR("Parse config failed: " << e.what());
        return false;
    }

    if (cfg.etcdAddress.empty()) {
        SLOG_ERROR("etcd_address is required");
        return false;
    }
    if (cfg.listenPort <= 0 || cfg.listenPort > 65535) {
        SLOG_ERROR("Invalid listen_port: " << cfg.listenPort);
        return false;
    }
    if (cfg.numSetThreads <= 0) {
        SLOG_ERROR("num_set_threads must be > 0, got " << cfg.numSetThreads);
        return false;
    }
    if (cfg.targetQps < 0) {
        SLOG_ERROR("target_qps must be >= 0, got " << cfg.targetQps);
        return false;
    }
    if (!cfg.targetQpsStages.empty() && cfg.stageDurationSeconds <= 0) {
        SLOG_ERROR("stage_duration_seconds required when target_qps is an array");
        return false;
    }
    for (auto qps : cfg.targetQpsStages) {
        if (qps < 0) {
            SLOG_ERROR("target_qps stage values must be >= 0, got " << qps);
            return false;
        }
    }
    for (auto sz : cfg.dataSizes) {
        if (sz == 0) {
            SLOG_ERROR("data_sizes contains a zero-size entry");
            return false;
        }
    }
    if (cfg.batchKeysCount < 1) {
        SLOG_ERROR("batch_keys_count must be >= 1, got " << cfg.batchKeysCount);
        return false;
    }
    if (cfg.connectTimeoutMs <= 0) {
        SLOG_ERROR("connect_timeout_ms must be > 0, got " << cfg.connectTimeoutMs);
        return false;
    }
    if (cfg.requestTimeoutMs < 0) {
        SLOG_ERROR("request_timeout_ms must be >= 0, got " << cfg.requestTimeoutMs);
        return false;
    }
    if (cfg.metricsIntervalMs <= 0) {
        SLOG_ERROR("metrics_interval_ms must be > 0, got " << cfg.metricsIntervalMs);
        return false;
    }
    if (cfg.ttlSeconds == 0) {
        SLOG_WARN("ttl_seconds is 0, data will not expire");
    }
    if (cfg.keyPoolSize > 0 && cfg.ttlSeconds > 0) {
        SLOG_WARN("Cache mode with ttl_seconds=" << cfg.ttlSeconds
                  << ": data may expire before LRU evicts it, affecting hit rate accuracy");
    }

    auto joinStr = [](const std::vector<std::string> &v) -> std::string {
        std::string r;
        for (size_t i = 0; i < v.size(); i++) {
            if (i > 0) r += ",";
            r += v[i];
        }
        return r;
    };

    SLOG_INFO("Config loaded: instance_id=" << cfg.instanceId
              << ", port=" << cfg.listenPort
              << ", etcd=" << cfg.etcdAddress
              << ", role=" << cfg.role
              << ", pipeline=[" << joinStr(cfg.pipeline) << "]"
              << ", notify_pipeline=[" << joinStr(cfg.notifyPipeline) << "]"
              << ", nodes=" << cfg.nodes.size()
              << ", peers=" << cfg.peers.size()
              << ", data_sizes_count=" << cfg.dataSizes.size()
              << ", target_qps=" << cfg.targetQps
              << (cfg.targetQpsStages.empty() ? "" :
                  " [" + std::to_string(cfg.targetQpsStages.size()) + " stages x "
                  + std::to_string(cfg.stageDurationSeconds) + "s]")
              << ", threads=" << cfg.numSetThreads
              << ", batch_keys_count=" << cfg.batchKeysCount
              << ", key_pool_size=" << cfg.keyPoolSize
              << ", output_dir=" << cfg.outputDir);
    return true;
}
