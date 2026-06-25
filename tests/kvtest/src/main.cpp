#include "common/config.h"
#include "common/simple_log.h"
#include "common/cpu_affinity.h"
#include "metrics/metrics.h"
#include "pipeline/pipeline.h"
#include "pipeline/kv_worker.h"
#include "pipeline/cache_reader.h"
#include "rpc/http_server.h"
#include "pipeline/stop.h"
#include "benchmark/benchmark_runner.h"
#include "benchmark/kv_client_adapter.h"
#include "benchmark/subprocess.h"

#include <datasystem/kv_client.h>
#include <datasystem/utils/connection.h>
#include <datasystem/utils/service_discovery.h>

#include <iostream>
#include <string>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <fstream>
#include <memory>
#include <thread>
#include <algorithm>
#include <cmath>

using namespace datasystem;

static std::atomic<bool> gRunning{true};

static void SignalHandler(int sig) {
    std::cerr << "Received signal " << sig << ", shutting down..." << std::endl;
    gRunning = false;
}

static int RunBenchmarkMode(Config &cfg) {
    SLOG_INFO("Benchmark mode: test_mode=" << static_cast<int>(cfg.testMode));
    std::signal(SIGTERM, SignalHandler);
    std::signal(SIGINT, SignalHandler);
    signal(SIGPIPE, SIG_IGN);

    // Calculate params before forking (no KVClient/SD needed)
    uint64_t dataSize = cfg.dataSizes[0];
    int keysPerRound = CalcKeysPerRound(cfg.workerMemoryMb, dataSize);
    int numThreads = cfg.numThreads;
    bool isGetMode = IsGetMode(cfg.testMode);
    bool isMixedMode = IsMixedMode(cfg.testMode);
    bool isMSetMode = IsMSetMode(cfg.testMode);
    bool isMGetMode = IsMGetMode(cfg.testMode);
    int numSetThreads = cfg.numThreads;
    int numGetThreads = 0;
    if (isMixedMode) {
        if (cfg.numThreads < 2) {
            SLOG_ERROR("Mixed mode requires num_threads >= 2, got " << cfg.numThreads);
            return 1;
        }
        numSetThreads = std::max(1, static_cast<int>(std::round(cfg.setRatio * cfg.numThreads)));
        numSetThreads = std::min(numSetThreads, cfg.numThreads - 1);
        numGetThreads = cfg.numThreads - numSetThreads;
        SLOG_INFO("Mixed mode: set_ratio=" << cfg.setRatio
                  << " -> " << numSetThreads << " set threads, "
                  << numGetThreads << " get threads"
                  << " (strategy=" << static_cast<int>(cfg.mixedKeyStrategy) << ")");
    }

    SLOG_INFO("Benchmark config: keys_per_round=" << keysPerRound
              << ", threads=" << numThreads
              << ", data_size=" << dataSize
              << ", set_api=" << cfg.setApi
              << ", cleanup=" << cfg.cleanupMethod
              << ", is_get_mode=" << isGetMode
              << ", is_mixed_mode=" << isMixedMode
              << ", is_mset_mode=" << isMSetMode
              << ", is_mget_mode=" << isMGetMode);

    // --- Spawn child processes ---
    // children[0] = setChild, children[1] = getChild (optional), children[2] = delChild (optional)
    // Reserve to avoid reallocation (ChildProcess has pipe fds that must stay valid)
    std::vector<ChildProcess> children;
    children.reserve(3);
    size_t setChildIdx = 0;
    size_t getChildIdx = SIZE_MAX;  // invalid if not needed
    size_t delChildIdx = SIZE_MAX;

    // setChild: always needed
    children.push_back(SpawnChild(cfg, ROLE_SET));
    if (children.back().pid <= 0) {
        SLOG_ERROR("Failed to spawn setChild");
        return 1;
    }
    setChildIdx = children.size() - 1;

    // getChild: only if setClient != getClient (cross-node modes)
    if ((isGetMode || isMixedMode || isMGetMode) && NeedsSeparateGetChild(cfg.testMode)) {
        children.push_back(SpawnChild(cfg, ROLE_GET));
        if (children.back().pid <= 0) {
            SLOG_ERROR("Failed to spawn getChild");
            KillAllChildren(children);
            return 1;
        }
        getChildIdx = children.size() - 1;
    }

    // delChild: only if cleanup method is "del"
    if (cfg.cleanupMethod == "del") {
        children.push_back(SpawnChild(cfg, ROLE_DEL));
        if (children.back().pid <= 0) {
            SLOG_ERROR("Failed to spawn delChild");
            KillAllChildren(children);
            return 1;
        }
        delChildIdx = children.size() - 1;
    }

    // Wait for all children to initialize
    for (auto &cp : children) {
        if (!WaitForInit(cp)) {
            SLOG_ERROR("Child init failed, killing all children");
            KillAllChildren(children);
            return 1;
        }
    }
    SLOG_INFO("All " << children.size() << " child processes initialized");

    // --- Round loop ---
    BenchmarkStats stats;
    BenchmarkMetrics benchMetrics(cfg.outputDir);
    int64_t maxDurationMs = static_cast<int64_t>(cfg.durationSeconds) * 1000;
    auto benchStart = std::chrono::steady_clock::now();

    // Pre-populate get key space for independent strategy
    if (isMixedMode && cfg.mixedKeyStrategy == MixedKeyStrategy::INDEPENDENT) {
        SLOG_INFO("Pre-populating get keys (round 0)...");
        ResultMsg preRes{};
        if (!SendCommand(children[setChildIdx], CMD_RUN_SET, 0) ||
            !RecvResult(children[setChildIdx], preRes)) {
            SLOG_ERROR("Pre-population failed");
            KillAllChildren(children);
            return 1;
        }
        SLOG_INFO("Pre-populated " << preRes.successCount << " get keys");
    }

    int startRound = 0;
    if (isMixedMode && cfg.mixedKeyStrategy == MixedKeyStrategy::INDEPENDENT) {
        startRound = 1;  // round 0 is pre-populated, skip deletion
    }

    for (int round = startRound; cfg.totalRounds == 0 || round < startRound + cfg.totalRounds; round++) {
        if (!gRunning) break;

        if (maxDurationMs > 0) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - benchStart).count();
            if (elapsed >= maxDurationMs) break;
        }

        auto roundStart = std::chrono::steady_clock::now();
        SLOG_INFO("Round " << round << " starting");

        ResultMsg setRes{};
        ResultMsg getRes{};
        ResultMsg delRes{};

        if (isMixedMode) {
            // --- Mixed mode: concurrent set+get via dual child processes ---
            int getRound = GetRoundForGet(cfg.mixedKeyStrategy, round);
            bool skipGet = (getRound < 0);

            // Send Set command
            if (!SendCommand(children[setChildIdx], CMD_RUN_SET, round, numSetThreads)) {
                SLOG_ERROR("Set command failed (pipe error) in round " << round);
                break;
            }

            // Send Get command (skip for read_prev round 0)
            if (!skipGet) {
                if (!SendCommand(children[getChildIdx], CMD_RUN_GET, getRound, numGetThreads)) {
                    SLOG_ERROR("Get command failed (pipe error) in round " << round);
                    break;
                }
            }

            // Collect results
            if (!RecvResult(children[setChildIdx], setRes)) {
                SLOG_ERROR("Set result failed (pipe error) in round " << round);
                break;
            }
            if (!skipGet) {
                if (!RecvResult(children[getChildIdx], getRes)) {
                    SLOG_ERROR("Get result failed (pipe error) in round " << round);
                    break;
                }
            }

            stats.totalSet += setRes.successCount;
            stats.totalGet += getRes.successCount;
        } else if (isMGetMode) {
            // --- MGet mode: sequential MSet -> MGet (matching GET_* pattern) ---
            // MSet phase (write data first)
            if (!SendCommand(children[setChildIdx], CMD_RUN_MSET, round, numSetThreads) ||
                !RecvResult(children[setChildIdx], setRes)) {
                SLOG_ERROR("MSet phase failed (pipe error) in round " << round);
                break;
            }
            stats.totalSet += setRes.successCount;

            // MGet phase (read after write completes)
            if (!SendCommand(children[getChildIdx], CMD_RUN_MGET, round, numGetThreads) ||
                !RecvResult(children[getChildIdx], getRes)) {
                SLOG_ERROR("MGet phase failed (pipe error) in round " << round);
                break;
            }
            stats.totalGet += getRes.successCount;
        } else if (isMSetMode) {
            // --- MSet mode: MSet only (write-only, no get phase) ---
            if (!SendCommand(children[setChildIdx], CMD_RUN_MSET, round) ||
                !RecvResult(children[setChildIdx], setRes)) {
                SLOG_ERROR("MSet phase failed (pipe error) in round " << round);
                break;
            }
            stats.totalSet += setRes.successCount;
        } else {
            // --- Sequential mode: set -> get -> del ---
            // Set phase
            if (!SendCommand(children[setChildIdx], CMD_RUN_SET, round) ||
                !RecvResult(children[setChildIdx], setRes)) {
                SLOG_ERROR("Set phase failed (pipe error) in round " << round);
                break;
            }
            stats.totalSet += setRes.successCount;

            // Get phase
            if (isGetMode) {
                size_t getIdx = (getChildIdx != SIZE_MAX) ? getChildIdx : setChildIdx;
                if (!SendCommand(children[getIdx], CMD_RUN_GET, round) ||
                    !RecvResult(children[getIdx], getRes)) {
                    SLOG_ERROR("Get phase failed (pipe error) in round " << round);
                    break;
                }
                stats.totalGet += getRes.successCount;
            }
        }

        // Del phase
        if (delChildIdx != SIZE_MAX) {
            // For read_prev: delay deletion by one round so get threads can read
            // the previous round's keys before they're deleted. Round N deletes
            // round N-1 (which was read by round N's get threads).
            int delRound = round;
            if (isMixedMode && cfg.mixedKeyStrategy == MixedKeyStrategy::READ_PREV) {
                delRound = round - 1;  // delete previous round's keys
            }
            if (delRound >= startRound) {
                if (!SendCommand(children[delChildIdx], CMD_RUN_DEL, delRound) ||
                    !RecvResult(children[delChildIdx], delRes)) {
                    SLOG_ERROR("Del phase failed (pipe error) in round " << round);
                    break;
                }
                stats.totalDel += delRes.successCount;
            }
        }

        auto roundEnd = std::chrono::steady_clock::now();
        double roundTotalMs = std::chrono::duration<double, std::milli>(roundEnd - roundStart).count();

        // Record set metrics
        {
            double qps = roundTotalMs > 0 ? setRes.successCount * 1000.0 / roundTotalMs : 0;
            benchMetrics.RecordPhase(round, "set", setRes.successCount,
                setRes.avgMs, setRes.p50Ms, setRes.p90Ms, setRes.p99Ms,
                setRes.maxMs, setRes.totalLatMs, qps);
        }

        // Record get metrics (mixed mode always has get; sequential only if isGetMode)
        if (isMixedMode || isGetMode) {
            double qps = roundTotalMs > 0 ? getRes.successCount * 1000.0 / roundTotalMs : 0;
            benchMetrics.RecordPhase(round, "get", getRes.successCount,
                getRes.avgMs, getRes.p50Ms, getRes.p90Ms, getRes.p99Ms,
                getRes.maxMs, getRes.totalLatMs, qps);
        }

        // Record del metrics
        if (cfg.cleanupMethod == "del") {
            double qps = roundTotalMs > 0 ? delRes.successCount * 1000.0 / roundTotalMs : 0;
            benchMetrics.RecordPhase(round, "del", delRes.successCount,
                delRes.avgMs, delRes.p50Ms, delRes.p90Ms, delRes.p99Ms,
                delRes.maxMs, delRes.totalLatMs, qps);
        }

        stats.roundsCompleted++;

        if (cfg.cleanupMethod == "ttl" && cfg.ttlSeconds > 0) {
            SLOG_INFO("Waiting for TTL " << cfg.ttlSeconds << "s...");
            std::this_thread::sleep_for(std::chrono::seconds(cfg.ttlSeconds));
        }

        SLOG_INFO("Round " << round << " complete: set=" << setRes.successCount
                  << " get=" << getRes.successCount << " del=" << delRes.successCount
                  << " roundMs=" << roundTotalMs);
    }

    // --- Final cleanup for delayed-deletion strategies ---
    if (gRunning && delChildIdx != SIZE_MAX) {
        // read_prev: delete the last round's keys (skipped by delayed deletion)
        if (isMixedMode && cfg.mixedKeyStrategy == MixedKeyStrategy::READ_PREV
            && stats.roundsCompleted > 0) {
            int lastRound = startRound + stats.roundsCompleted - 1;
            ResultMsg finalDelRes{};
            SLOG_INFO("read_prev final cleanup: deleting round " << lastRound);
            if (SendCommand(children[delChildIdx], CMD_RUN_DEL, lastRound)) {
                if (!RecvResult(children[delChildIdx], finalDelRes)) {
                    SLOG_ERROR("read_prev final cleanup del phase failed (pipe error)");
                } else {
                    stats.totalDel += finalDelRes.successCount;
                }
            }
        }
        // independent: delete pre-populated round 0 keys
        if (isMixedMode && cfg.mixedKeyStrategy == MixedKeyStrategy::INDEPENDENT) {
            ResultMsg finalDelRes{};
            SLOG_INFO("independent final cleanup: deleting pre-populated round 0");
            if (SendCommand(children[delChildIdx], CMD_RUN_DEL, 0)) {
                if (!RecvResult(children[delChildIdx], finalDelRes)) {
                    SLOG_ERROR("independent final cleanup del phase failed (pipe error)");
                } else {
                    stats.totalDel += finalDelRes.successCount;
                }
            }
        }
    }

    // --- Shutdown children ---
    SLOG_INFO("Shutting down " << children.size() << " child processes");
    for (auto &cp : children) {
        ShutdownChild(cp);
    }

    benchMetrics.Flush();

    SLOG_INFO("Benchmark finished: rounds=" << stats.roundsCompleted.load()
              << ", set=" << stats.totalSet.load()
              << ", get=" << stats.totalGet.load()
              << ", del=" << stats.totalDel.load());
    return 0;
}

static int RunServerMode(const Config &cfg) {
    std::cerr << "kvtest v" BUILD_VERSION << std::endl;
    std::cerr << "Output directory: " << cfg.outputDir << std::endl;

    // Apply CPU/NUMA affinity before creating any threads
    ApplyAffinityFromConfig(cfg.cpuAffinity, cfg.numaNode);

    std::cerr << "Initializing ServiceDiscovery..." << std::endl;

    ServiceDiscoveryOptions sdOpts;
    sdOpts.etcdAddress = cfg.etcdAddress;
    sdOpts.clusterName = cfg.clusterName;
    sdOpts.hostIdEnvName = cfg.hostIdEnvName;

    auto sd = std::make_shared<ServiceDiscovery>(sdOpts);
    Status rc = sd->Init();
    if (!rc.IsOk()) {
        std::cerr << "ServiceDiscovery init failed: " << rc.GetMsg() << std::endl;
        return 1;
    }
    std::cerr << "ServiceDiscovery initialized: etcd=" << cfg.etcdAddress << std::endl;

    ConnectOptions connOpts;
    connOpts.serviceDiscovery = sd;
    connOpts.connectTimeoutMs = cfg.connectTimeoutMs;
    connOpts.requestTimeoutMs = cfg.requestTimeoutMs;
    connOpts.enableCrossNodeConnection = cfg.enableCrossNodeConnection;
    connOpts.fastTransportMemSize = cfg.fastTransportMemSize;

    auto client = std::make_shared<KVClient>(connOpts);
    rc = client->Init();
    if (!rc.IsOk()) {
        std::cerr << "KVClient init failed: " << rc.GetMsg() << std::endl;
        return 1;
    }
    std::cerr << "KVClient initialized" << std::endl;

    bool cacheMode = cfg.keyPoolSize > 0;
    MetricsCollector metrics(cfg.instanceId, cfg.metricsIntervalMs, cfg.outputDir, cacheMode, cfg.metricsFile);
    if (!cfg.targetQpsStages.empty()) {
        metrics.SetQpsStages(cfg.targetQpsStages, cfg.stageDurationSeconds);
    }
    metrics.Start();

    std::signal(SIGTERM, SignalHandler);
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGPIPE, SIG_IGN);

    HttpServer httpServer(cfg, client, metrics, gRunning);

    // Cache mode: create CacheReader before httpServer starts accepting connections
    std::unique_ptr<CacheReader> cacheReader;
    if (cfg.keyPoolSize > 0 && cfg.role == "reader") {
        cacheReader = std::make_unique<CacheReader>(cfg, client, metrics);
        httpServer.SetCacheReader(cacheReader.get());
    }

    httpServer.Start();

    if (cacheReader) {
        cacheReader->Start();
    }

    std::unique_ptr<KVWorker> worker;
    if (cfg.role == "writer") {
        worker = std::make_unique<KVWorker>(cfg, client, metrics);
        worker->Start();
    } else {
        std::cerr << "Reader mode: waiting for notifications..." << std::endl;
    }

    std::vector<uint64_t> prevCounts;
    auto prevTime = std::chrono::steady_clock::now();

    while (gRunning) {
        std::this_thread::sleep_for(std::chrono::seconds(3));

        auto now = std::chrono::steady_clock::now();
        double elapsedSec = std::chrono::duration<double>(now - prevTime).count();
        prevTime = now;

        auto snap = metrics.SnapshotCounts();
        if (prevCounts.size() != snap.size()) {
            prevCounts.resize(snap.size(), 0);
        }

        std::string rates;
        for (size_t i = 0; i < snap.size(); i++) {
            uint64_t delta = snap[i].count - prevCounts[i];
            double rate = elapsedSec > 0 ? delta / elapsedSec : 0;
            if (!rates.empty()) rates += ", ";
            rates += snap[i].name + "=" + std::to_string(static_cast<int>(rate)) + "/s";
            prevCounts[i] = snap[i].count;
        }

        // Queue depths
        size_t notifyOutQ = 0, notifyInQ = 0;
        if (worker) notifyOutQ = worker->NotifyQueueSize();
        notifyInQ = httpServer.NotifyQueueSize();

        if (notifyOutQ > 1000) {
            SLOG_WARN("notify out queue backlog: " << notifyOutQ);
        }
        if (notifyInQ > 1000) {
            SLOG_WARN("notify in queue backlog: " << notifyInQ);
        }

        SLOG_INFO("[" << rates << "] "
                  << "[out_q=" << notifyOutQ << ", in_q=" << notifyInQ << "]"
                  << (cacheMode ? (" [pool=" + std::to_string(worker ? worker->CurrentPoolSize() : 0) +
                      ", hit_rate=" + std::to_string(metrics.CacheHitRate()) + "]") : "")
                  << (!cfg.targetQpsStages.empty() && worker ?
                      (" [qps=" + std::to_string(worker->CurrentTargetQps()) + "]") : ""));

        if (worker && cfg.targetHitRate > 0.0) {
            worker->AdjustPoolSize();
        }
        if (worker && !cfg.targetQpsStages.empty()) {
            worker->AdvanceStage();
        }
    }

    std::cerr << "Shutting down..." << std::endl;

    // Wait for in-flight set/get/exist operations to complete before stopping
    constexpr int kShutdownDelaySeconds = 5;
    std::cerr << "Waiting " << kShutdownDelaySeconds
              << "s for in-flight operations to complete..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(kShutdownDelaySeconds));
    std::cerr << "Shutdown delay complete" << std::endl;

    if (cacheReader) cacheReader->Stop();
    if (worker) worker->Stop();
    httpServer.Stop();
    metrics.Stop();

    std::cerr << "Shutdown complete" << std::endl;
    return 0;
}

static int StopMode(const Config &cfg) {
    if (cfg.peers.empty()) {
        std::cerr << "No peers in config" << std::endl;
        return 1;
    }
    int ok = StopAllPeers(cfg.peers);
    return (ok == static_cast<int>(cfg.peers.size())) ? 0 : 1;
}

int main(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--version" || arg == "-v") {
            std::cout << "kvtest " << BUILD_VERSION << " (commit: " << BUILD_COMMIT << ")" << std::endl;
            return 0;
        }
    }

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " [--stop|--version] <config.json>\n";
        return 1;
    }

    bool stopMode = false;
    std::string configPath;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--stop") {
            stopMode = true;
        } else if (arg != "--version" && arg != "-v") {
            configPath = arg;
        }
    }

    Config cfg;
    if (!LoadConfig(configPath, cfg)) {
        return 1;
    }

    if (stopMode) {
        return StopMode(cfg);
    }

    // Redirect std::cout/std::cerr into outputDir/run.log via rdbuf
    // dup2/freopen don't work reliably with libstdc++ buffered streams.
    // Using a static ofstream ensures the object outlives main's scope.
    static std::ofstream logStream(cfg.outputDir + "/run.log", std::ios::app);
    if (logStream.is_open()) {
        std::cout.rdbuf(logStream.rdbuf());
        std::cerr.rdbuf(logStream.rdbuf());
    }

    if (cfg.runMode == RunMode::BENCHMARK) {
        // Log to terminal BEFORE redirect takes effect for SLOG (SLOG uses std::cout)
        // This printf goes to the original fd 1, not the redirected rdbuf
        fprintf(stderr, "[INFO] Entering benchmark mode, detailed logs: %s/run.log\n",
                cfg.outputDir.c_str());
        return RunBenchmarkMode(cfg);
    }

    return RunServerMode(cfg);
}
