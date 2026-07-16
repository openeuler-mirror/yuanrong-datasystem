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

TestMode ParseTestMode(const std::string &s) {
    if (s == "set_local") return TestMode::SET_LOCAL;
    if (s == "set_remote") return TestMode::SET_REMOTE;
    if (s == "get_local") return TestMode::GET_LOCAL;
    if (s == "get_cross_node") return TestMode::GET_CROSS_NODE;
    if (s == "get_remote_direct") return TestMode::GET_REMOTE_DIRECT;
    if (s == "get_remote_cross") return TestMode::GET_REMOTE_CROSS;
    if (s == "mixed_local_set_get") return TestMode::MIXED_LOCAL_SET_GET;
    if (s == "mixed_remote_set_get") return TestMode::MIXED_REMOTE_SET_GET;
    if (s == "mixed_local_set_cross_get") return TestMode::MIXED_LOCAL_SET_CROSS_GET;
    if (s == "mixed_remote_set_remote_cross_get") return TestMode::MIXED_REMOTE_SET_REMOTE_CROSS_GET;
    if (s == "mset_local") return TestMode::MSET_LOCAL;
    if (s == "mset_remote") return TestMode::MSET_REMOTE;
    if (s == "mget_local") return TestMode::MGET_LOCAL;
    if (s == "mget_cross_node") return TestMode::MGET_CROSS_NODE;
    if (s == "mget_remote_direct") return TestMode::MGET_REMOTE_DIRECT;
    if (s == "mget_remote_cross") return TestMode::MGET_REMOTE_CROSS;
    return TestMode::NONE;
}

bool NeedsRemoteWorker(TestMode mode) {
    return mode == TestMode::SET_REMOTE
        || mode == TestMode::GET_CROSS_NODE
        || mode == TestMode::GET_REMOTE_DIRECT
        || mode == TestMode::GET_REMOTE_CROSS
        || mode == TestMode::MIXED_REMOTE_SET_GET
        || mode == TestMode::MIXED_LOCAL_SET_CROSS_GET
        || mode == TestMode::MIXED_REMOTE_SET_REMOTE_CROSS_GET
        || mode == TestMode::MSET_REMOTE
        || mode == TestMode::MGET_CROSS_NODE
        || mode == TestMode::MGET_REMOTE_DIRECT
        || mode == TestMode::MGET_REMOTE_CROSS;
}

bool IsGetMode(TestMode mode) {
    return mode == TestMode::GET_LOCAL || mode == TestMode::GET_CROSS_NODE
        || mode == TestMode::GET_REMOTE_DIRECT || mode == TestMode::GET_REMOTE_CROSS
        || mode == TestMode::MGET_LOCAL || mode == TestMode::MGET_CROSS_NODE
        || mode == TestMode::MGET_REMOTE_DIRECT || mode == TestMode::MGET_REMOTE_CROSS;
}

bool IsMixedMode(TestMode mode) {
    return mode == TestMode::MIXED_LOCAL_SET_GET
        || mode == TestMode::MIXED_REMOTE_SET_GET
        || mode == TestMode::MIXED_LOCAL_SET_CROSS_GET
        || mode == TestMode::MIXED_REMOTE_SET_REMOTE_CROSS_GET;
}

bool IsMSetMode(TestMode mode) {
    return mode == TestMode::MSET_LOCAL || mode == TestMode::MSET_REMOTE;
}

bool IsMGetMode(TestMode mode) {
    return mode == TestMode::MGET_LOCAL || mode == TestMode::MGET_CROSS_NODE
        || mode == TestMode::MGET_REMOTE_DIRECT || mode == TestMode::MGET_REMOTE_CROSS;
}

std::optional<MixedKeyStrategy> ParseMixedKeyStrategy(const std::string &s) {
    if (s == "same_keys") return MixedKeyStrategy::SAME_KEYS;
    if (s == "read_prev") return MixedKeyStrategy::READ_PREV;
    if (s == "independent") return MixedKeyStrategy::INDEPENDENT;
    return std::nullopt;
}

std::optional<RunMode> ParseRunMode(const std::string &s) {
    if (s == "pipeline") return RunMode::PIPELINE;
    if (s == "cache") return RunMode::CACHE;
    if (s == "benchmark") return RunMode::BENCHMARK;
    return std::nullopt;
}

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
        if (j.contains("coordinator_address")) cfg.coordinatorAddress = j["coordinator_address"];
        if (j.contains("cluster_name")) cfg.clusterName = j["cluster_name"];
        if (j.contains("host_id_env_name")) cfg.hostIdEnvName = j["host_id_env_name"];
        if (j.contains("connect_timeout_ms")) cfg.connectTimeoutMs = j["connect_timeout_ms"];
        if (j.contains("fast_transport_mem_size")) {
            const auto &ftm = j["fast_transport_mem_size"];
            cfg.fastTransportMemSize = ftm.is_string() ? ParseSize(ftm.get<std::string>()) : ftm.get<uint64_t>();
        }
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
        if (j.contains("num_threads")) cfg.numThreads = j["num_threads"];
        if (j.contains("notify_count")) cfg.notifyCount = j["notify_count"];
        if (j.contains("notify_interval_us")) cfg.notifyIntervalUs = j["notify_interval_us"];
        if (j.contains("enable_jitter")) cfg.enableJitter = j["enable_jitter"];
        if (j.contains("enable_cross_node_connection")) cfg.enableCrossNodeConnection = j["enable_cross_node_connection"];
        if (j.contains("batch_keys_count")) cfg.batchKeysCount = j["batch_keys_count"];
        if (j.contains("mset_batch_size")) cfg.msetBatchSize = j["mset_batch_size"];
        if (j.contains("mget_batch_size")) cfg.mgetBatchSize = j["mget_batch_size"];
        if (j.contains("cpu_affinity")) cfg.cpuAffinity = j["cpu_affinity"].get<std::string>();
        if (j.contains("numa_node")) cfg.numaNode = j["numa_node"].get<int>();
        if (j.contains("key_pool_size")) cfg.keyPoolSize = j["key_pool_size"];
        if (j.contains("inference_delay_ms")) cfg.inferenceDelayMs = j["inference_delay_ms"];
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

        // Benchmark mode fields
        if (j.contains("test_mode")) cfg.testMode = ParseTestMode(j["test_mode"].get<std::string>());
        if (j.contains("worker_memory_mb")) cfg.workerMemoryMb = j["worker_memory_mb"];
        if (j.contains("duration_seconds")) cfg.durationSeconds = j["duration_seconds"];
        if (j.contains("total_rounds")) cfg.totalRounds = j["total_rounds"];
        if (j.contains("set_api")) cfg.setApi = j["set_api"].get<std::string>();
        if (j.contains("cleanup_method")) cfg.cleanupMethod = j["cleanup_method"].get<std::string>();
        if (j.contains("remote_worker") && j["remote_worker"].is_object()) {
            const auto &rw = j["remote_worker"];
            if (rw.contains("host")) cfg.remoteWorker.host = rw["host"].get<std::string>();
            if (rw.contains("port")) cfg.remoteWorker.port = rw["port"];
        }
        if (j.contains("connect_options") && j["connect_options"].is_object()) {
            const auto &co = j["connect_options"];
            if (co.contains("connect_timeout_ms")) cfg.connectTimeoutMs = co["connect_timeout_ms"];
            if (co.contains("request_timeout_ms")) cfg.requestTimeoutMs = co["request_timeout_ms"];
            if (co.contains("enable_cross_node_connection")) cfg.enableCrossNodeConnection = co["enable_cross_node_connection"];
            if (co.contains("enable_local_cache")) cfg.enableLocalCache = co["enable_local_cache"];
            if (co.contains("fast_transport_mem_size")) {
                const auto &ftm = co["fast_transport_mem_size"];
                cfg.fastTransportMemSize = ftm.is_string() ? ParseSize(ftm.get<std::string>()) : ftm.get<uint64_t>();
            }
        }
        if (j.contains("set_param") && j["set_param"].is_object()) {
            const auto &sp = j["set_param"];
            if (sp.contains("ttl_second")) {
                cfg.ttlSeconds = sp["ttl_second"];
            }
        }
        if (j.contains("verify") && j["verify"].is_object()) {
            const auto &v = j["verify"];
            if (v.contains("level")) {
                cfg.verifyLevel = v["level"].get<std::string>();
            }
            if (v.contains("sample_bytes")) {
                const auto &sb = v["sample_bytes"];
                cfg.verifySampleBytes = sb.is_string() ? ParseSize(sb.get<std::string>()) : sb.get<uint64_t>();
            }
            if (v.contains("sample_step")) {
                const auto &ss = v["sample_step"];
                cfg.verifySampleStepBytes = ss.is_string() ? ParseSize(ss.get<std::string>()) : ss.get<uint64_t>();
            }
            if (v.contains("fail_op")) {
                cfg.verifyFailOp = v["fail_op"].get<bool>();
            }
        }
        if (j.contains("set_ratio")) {
            cfg.setRatio = j["set_ratio"].get<double>();
        }
        if (j.contains("mixed_key_strategy")) {
            auto parsed = ParseMixedKeyStrategy(j["mixed_key_strategy"].get<std::string>());
            if (!parsed.has_value()) {
                SLOG_ERROR("Unknown mixed_key_strategy '" << j["mixed_key_strategy"].get<std::string>()
                          << "', must be same_keys/read_prev/independent");
                return false;
            }
            cfg.mixedKeyStrategy = parsed.value();
        }

        // Generate output directory: metrics_{ip}_{timestamp}
        if (cfg.outputDir.empty()) {
            std::string ip = cfg.etcdAddress.empty() ? cfg.coordinatorAddress : cfg.etcdAddress;
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
            cfg.maxKeyPoolSize = cfg.keyPoolSize * 20;
        }

        // Mode: explicit or auto-infer
        bool modeExplicit = false;
        if (j.contains("mode") && j["mode"].is_string()) {
            auto parsed = ParseRunMode(j["mode"].get<std::string>());
            if (!parsed.has_value()) {
                SLOG_ERROR("Unknown mode '" << j["mode"].get<std::string>()
                          << "', must be pipeline/cache/benchmark");
                return false;
            }
            cfg.runMode = parsed.value();
            modeExplicit = true;
        }
        if (!modeExplicit) {
            if (cfg.testMode != TestMode::NONE) {
                cfg.runMode = RunMode::BENCHMARK;
            } else if (cfg.keyPoolSize > 0) {
                cfg.runMode = RunMode::CACHE;
            } else {
                cfg.runMode = RunMode::PIPELINE;
            }
        }

    } catch (const std::exception &e) {
        SLOG_ERROR("Parse config failed: " << e.what());
        return false;
    }

    if (cfg.etcdAddress.empty() && cfg.coordinatorAddress.empty()) {
        SLOG_ERROR("either etcd_address or coordinator_address is required");
        return false;
    }
    if (cfg.listenPort <= 0 || cfg.listenPort > 65535) {
        SLOG_ERROR("Invalid listen_port: " << cfg.listenPort);
        return false;
    }
    if (cfg.numThreads <= 0) {
        SLOG_ERROR("num_threads must be > 0, got " << cfg.numThreads);
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
    if ((IsMSetMode(cfg.testMode) || IsMGetMode(cfg.testMode)) && cfg.msetBatchSize < 1) {
        SLOG_ERROR("mset_batch_size must be >= 1 for MSet/MGet modes, got " << cfg.msetBatchSize);
        return false;
    }
    if (IsMGetMode(cfg.testMode) && cfg.mgetBatchSize < 1) {
        SLOG_ERROR("mget_batch_size must be >= 1 for MGet modes, got " << cfg.mgetBatchSize);
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
    if (cfg.ttlSeconds == 0 && cfg.keyPoolSize == 0) {
        SLOG_INFO("TTL is 0, data will not expire");
    }
    if (cfg.verifyLevel != "off" && cfg.verifyLevel != "size"
        && cfg.verifyLevel != "sample" && cfg.verifyLevel != "full") {
        SLOG_ERROR("verify.level must be one of off/size/sample/full, got '"
                  << cfg.verifyLevel << "'");
        return false;
    }
    if (cfg.verifySampleBytes == 0) {
        SLOG_ERROR("verify.sample_bytes must be > 0");
        return false;
    }
    if (cfg.verifySampleStepBytes == 0) {
        SLOG_ERROR("verify.sample_step must be > 0");
        return false;
    }
    if (cfg.maxKeyPoolSize > 0 && cfg.maxKeyPoolSize < static_cast<uint64_t>(cfg.keyPoolSize)) {
        SLOG_ERROR("max_key_pool_size (" << cfg.maxKeyPoolSize
                  << ") must be >= key_pool_size (" << cfg.keyPoolSize << ")");
        return false;
    }

    // Mode-specific validation
    if (cfg.runMode == RunMode::PIPELINE || cfg.runMode == RunMode::CACHE) {
        if (cfg.role != "writer" && cfg.role != "reader") {
            SLOG_ERROR("role must be 'writer' or 'reader', got '" << cfg.role << "'");
            return false;
        }
        if (cfg.pipeline.empty()) {
            SLOG_ERROR("pipeline must not be empty");
            return false;
        }
    }
    if (cfg.runMode == RunMode::CACHE) {
        if (cfg.keyPoolSize <= 0) {
            SLOG_ERROR("key_pool_size must be > 0 for cache mode");
            return false;
        }
        if (cfg.targetHitRate < 0.0 || cfg.targetHitRate > 1.0) {
            SLOG_ERROR("target_hit_rate must be in [0, 1.0], got " << cfg.targetHitRate);
            return false;
        }
    }
    if (cfg.runMode == RunMode::BENCHMARK) {
        if (cfg.testMode == TestMode::NONE) {
            SLOG_ERROR("test_mode is required for benchmark mode");
            return false;
        }
        if (cfg.workerMemoryMb <= 0) {
            SLOG_ERROR("worker_memory_mb required for benchmark mode");
            return false;
        }
        if (NeedsRemoteWorker(cfg.testMode) && cfg.remoteWorker.host.empty()) {
            SLOG_ERROR("remote_worker required for test_mode " << static_cast<int>(cfg.testMode));
            return false;
        }
        if (cfg.cleanupMethod == "ttl" && cfg.ttlSeconds == 0) {
            SLOG_ERROR("set_param.ttl_second must be > 0 when cleanup_method=ttl");
            return false;
        }
        if (cfg.setApi != "string_view" && cfg.setApi != "create_buffer" && cfg.setApi != "create_buffer_raw") {
            SLOG_ERROR("set_api must be 'string_view', 'create_buffer', or 'create_buffer_raw'");
            return false;
        }
        if (cfg.cleanupMethod != "del" && cfg.cleanupMethod != "ttl") {
            SLOG_ERROR("cleanup_method must be 'del' or 'ttl'");
            return false;
        }
        if (IsMixedMode(cfg.testMode)) {
            if (cfg.numThreads < 2) {
                SLOG_ERROR("Mixed mode requires num_threads >= 2, got " << cfg.numThreads);
                return false;
            }
            if (cfg.setRatio <= 0.0 || cfg.setRatio >= 1.0) {
                SLOG_ERROR("set_ratio must be in (0.0, 1.0) for mixed mode, got " << cfg.setRatio);
                return false;
            }
            if (cfg.mixedKeyStrategy == MixedKeyStrategy::INDEPENDENT && cfg.cleanupMethod == "ttl") {
                SLOG_ERROR("independent strategy is incompatible with cleanup_method=ttl "
                          "(pre-populated keys would expire before benchmark reads them)");
                return false;
            }
        }
    }

    auto joinStr = [](const std::vector<std::string> &v) -> std::string {
        std::string r;
        for (size_t i = 0; i < v.size(); i++) {
            if (i > 0) r += ",";
            r += v[i];
        }
        return r;
    };

    std::ostringstream log;
    const char *runModeNames[] = {"pipeline", "cache", "benchmark"};
    log << "Config loaded: mode=" << runModeNames[static_cast<int>(cfg.runMode)]
        << ", instance_id=" << cfg.instanceId
        << ", port=" << cfg.listenPort
        << ", etcd=" << cfg.etcdAddress
        << ", coordinator=" << cfg.coordinatorAddress;
    if (cfg.testMode != TestMode::NONE) {
        const char *modeNames[] = {
            "none", "set_local", "set_remote", "get_local",
            "get_cross_node", "get_remote_direct", "get_remote_cross",
            "mixed_local_set_get", "mixed_remote_set_get",
            "mixed_local_set_cross_get", "mixed_remote_set_remote_cross_get",
            "mset_local", "mset_remote", "mget_local",
            "mget_cross_node", "mget_remote_direct", "mget_remote_cross",
        };
        static_assert(sizeof(modeNames) / sizeof(modeNames[0]) ==
                          static_cast<int>(TestMode::MGET_REMOTE_CROSS) + 1,
                      "modeNames must cover all TestMode values");
        log << ", test_mode=" << modeNames[static_cast<int>(cfg.testMode)]
            << ", worker_memory_mb=" << cfg.workerMemoryMb
            << ", num_threads=" << cfg.numThreads
            << ", total_rounds=" << cfg.totalRounds
            << ", duration_seconds=" << cfg.durationSeconds
            << ", set_api=" << cfg.setApi
            << ", cleanup_method=" << cfg.cleanupMethod;
        if (NeedsRemoteWorker(cfg.testMode)) {
            log << ", remote_worker=" << cfg.remoteWorker.host << ":" << cfg.remoteWorker.port;
        }
    } else {
        log << ", role=" << cfg.role
            << ", pipeline=[" << joinStr(cfg.pipeline) << "]"
            << ", notify_pipeline=[" << joinStr(cfg.notifyPipeline) << "]"
            << ", nodes=" << cfg.nodes.size()
            << ", peers=" << cfg.peers.size()
            << ", target_qps=" << cfg.targetQps
            << (cfg.targetQpsStages.empty() ? "" :
                " [" + std::to_string(cfg.targetQpsStages.size()) + " stages x "
                + std::to_string(cfg.stageDurationSeconds) + "s]")
            << ", threads=" << cfg.numThreads
            << ", batch_keys_count=" << cfg.batchKeysCount
            << ", key_pool_size=" << cfg.keyPoolSize;
    }
    log << ", data_sizes_count=" << cfg.dataSizes.size()
        << ", output_dir=" << cfg.outputDir;
    log << ", verify=" << cfg.verifyLevel
        << (cfg.verifyFailOp ? "+fail" : "")
        << ", sample_bytes=" << cfg.verifySampleBytes
        << ", sample_step=" << cfg.verifySampleStepBytes;
    SLOG_INFO(log.str());
    return true;
}
