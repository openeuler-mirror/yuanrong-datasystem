#include "config.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <sstream>
#include <cstring>
#include "simple_log.h"

using json = nlohmann::json;

uint64_t ParseSize(const std::string &str) {
    if (str.empty()) return 0;

    char *end = nullptr;
    double val = std::strtod(str.c_str(), &end);
    if (end == str.c_str()) return 0;

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
        if (j.contains("connect_timeout_ms")) cfg.connectTimeoutMs = j["connect_timeout_ms"];
        if (j.contains("request_timeout_ms")) cfg.requestTimeoutMs = j["request_timeout_ms"];
        if (j.contains("ttl_seconds")) cfg.ttlSeconds = j["ttl_seconds"];
        if (j.contains("target_qps")) cfg.targetQps = j["target_qps"];
        if (j.contains("num_set_threads")) cfg.numSetThreads = j["num_set_threads"];
        if (j.contains("notify_count")) cfg.notifyCount = j["notify_count"];
        if (j.contains("metrics_interval_ms")) cfg.metricsIntervalMs = j["metrics_interval_ms"];
        if (j.contains("metrics_file")) cfg.metricsFile = j["metrics_file"];

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

        if (j.contains("role")) cfg.role = j["role"].get<std::string>();
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

        // Replace {instance_id} in metrics file name
        auto pos = cfg.metricsFile.find("{instance_id}");
        if (pos != std::string::npos) {
            cfg.metricsFile.replace(pos, 13, std::to_string(cfg.instanceId));
        }

    } catch (const std::exception &e) {
        SLOG_ERROR("Parse config failed: " << e.what());
        return false;
    }

    if (cfg.etcdAddress.empty()) {
        SLOG_ERROR("etcd_address is required");
        return false;
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
              << ", data_sizes_count=" << cfg.dataSizes.size()
              << ", target_qps=" << cfg.targetQps
              << ", threads=" << cfg.numSetThreads);
    return true;
}
