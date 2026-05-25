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

using namespace datasystem;

static std::atomic<bool> gRunning{true};

static void SignalHandler(int sig) {
    std::cerr << "Received signal " << sig << ", shutting down..." << std::endl;
    gRunning = false;
}

static int RunBenchmarkMode(Config &cfg) {
    SLOG_INFO("Benchmark mode: test_mode=" << static_cast<int>(cfg.testMode));

    ServiceDiscoveryOptions sdOpts;
    sdOpts.etcdAddress = cfg.etcdAddress;
    sdOpts.clusterName = cfg.clusterName;
    sdOpts.hostIdEnvName = cfg.hostIdEnvName;
    auto sd = std::make_shared<ServiceDiscovery>(sdOpts);
    Status rc = sd->Init();
    if (!rc.IsOk()) {
        SLOG_ERROR("ServiceDiscovery init failed: " << rc.GetMsg());
        return 1;
    }

    ConnectOptions connOpts;
    connOpts.serviceDiscovery = sd;
    connOpts.connectTimeoutMs = cfg.connectTimeoutMs;
    connOpts.requestTimeoutMs = cfg.requestTimeoutMs;
    connOpts.enableCrossNodeConnection = cfg.enableCrossNodeConnection;
    connOpts.fastTransportMemSize = cfg.fastTransportMemSize;

    auto localClient = std::make_shared<KVClient>(connOpts);
    rc = localClient->Init();
    if (!rc.IsOk()) {
        SLOG_ERROR("Local client init failed: " << rc.GetMsg());
        return 1;
    }
    SLOG_INFO("Local client initialized");

    std::shared_ptr<KVClient> remoteClient;
    if (NeedsRemoteWorker(cfg.testMode)) {
        ConnectOptions remoteOpts;
        remoteOpts.host = cfg.remoteWorker.host;
        remoteOpts.port = cfg.remoteWorker.port;
        remoteOpts.connectTimeoutMs = cfg.connectTimeoutMs;
        remoteOpts.requestTimeoutMs = cfg.requestTimeoutMs;
        remoteOpts.enableCrossNodeConnection = cfg.enableCrossNodeConnection;
        remoteOpts.fastTransportMemSize = cfg.fastTransportMemSize;
        remoteClient = std::make_shared<KVClient>(remoteOpts);
        rc = remoteClient->Init();
        if (!rc.IsOk()) {
            SLOG_ERROR("Remote client init failed: " << rc.GetMsg());
            return 1;
        }
        SLOG_INFO("Remote client initialized: " << cfg.remoteWorker.host << ":" << cfg.remoteWorker.port);
    }

    // Determine which client does Set vs Get based on test_mode
    std::shared_ptr<KVClient> setClient, getClient;
    switch (cfg.testMode) {
        case TestMode::SET_LOCAL:
        case TestMode::GET_LOCAL:
            setClient = localClient; getClient = localClient; break;
        case TestMode::SET_REMOTE:
            setClient = remoteClient; getClient = remoteClient; break;
        case TestMode::GET_CROSS_NODE:
            setClient = remoteClient; getClient = localClient; break;
        case TestMode::GET_REMOTE_DIRECT:
            setClient = remoteClient; getClient = remoteClient; break;
        case TestMode::GET_REMOTE_CROSS:
            setClient = localClient; getClient = remoteClient; break;
        default:
            SLOG_ERROR("Unknown test_mode"); return 1;
    }

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
    if (cfg.ttlSeconds > 0) {
        param.ttlSecond = cfg.ttlSeconds;
    }

    KVClientAdapter setAdapter(setClient, param);
    KVClientAdapter getAdapter(getClient, param);

    // Del client: separate KVClient with 5s timeout to avoid cleanup failures
    // affecting the next round's Set/Get latency measurements.
    // For remote modes, delClient must connect to the same worker as setClient.
    std::shared_ptr<KVClient> delClient;
    KVClientAdapter delAdapter(setClient, param);  // fallback: reuse setClient
    try {
        ConnectOptions delConnOpts;
        if (setClient == remoteClient) {
            delConnOpts.host = cfg.remoteWorker.host;
            delConnOpts.port = cfg.remoteWorker.port;
        } else {
            delConnOpts.serviceDiscovery = sd;
        }
        delConnOpts.connectTimeoutMs = cfg.connectTimeoutMs;
        delConnOpts.requestTimeoutMs = 5000;
        delConnOpts.enableCrossNodeConnection = cfg.enableCrossNodeConnection;
        delConnOpts.fastTransportMemSize = cfg.fastTransportMemSize;
        delClient = std::make_shared<KVClient>(delConnOpts);
        rc = delClient->Init();
        if (!rc.IsOk()) {
            SLOG_WARN("Del client init failed (fallback to set client): " << rc.GetMsg());
            delClient.reset();
        } else {
            delAdapter = KVClientAdapter(delClient, param);
        }
    } catch (const std::exception &e) {
        SLOG_WARN("Del client exception (fallback to set client): " << e.what());
        delClient.reset();
    }

    uint64_t dataSize = cfg.dataSizes[0];
    int keysPerRound = CalcKeysPerRound(cfg.workerMemoryMb, dataSize);
    int numThreads = cfg.numThreads;
    bool isGetMode = IsGetMode(cfg.testMode);

    SLOG_INFO("Benchmark config: keys_per_round=" << keysPerRound
              << ", threads=" << numThreads
              << ", data_size=" << dataSize
              << ", set_api=" << cfg.setApi
              << ", cleanup=" << cfg.cleanupMethod
              << ", is_get_mode=" << isGetMode);

    std::string data(dataSize, 'A');

    BenchmarkStats stats;
    BenchmarkMetrics benchMetrics(cfg.outputDir);
    int64_t maxDurationMs = static_cast<int64_t>(cfg.durationSeconds) * 1000;

    auto benchStart = std::chrono::steady_clock::now();

    for (int round = 0; cfg.totalRounds == 0 || round < cfg.totalRounds; round++) {
        if (maxDurationMs > 0) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - benchStart).count();
            if (elapsed >= maxDurationMs) break;
        }

        SLOG_INFO("Round " << round << " starting");

        // Per-thread latency storage
        std::vector<std::vector<double>> setLatencies(numThreads);
        std::vector<std::vector<double>> getLatencies(numThreads);
        std::vector<int> setOks(numThreads, 0);
        std::vector<int> getOks(numThreads, 0);
        std::vector<int> delOks(numThreads, 0);
        std::vector<std::vector<double>> delLatencies(numThreads);

        Barrier barrier(numThreads);
        std::vector<std::thread> threads;

        auto roundStart = std::chrono::steady_clock::now();

        for (int t = 0; t < numThreads; t++) {
            threads.emplace_back([&, t]() {
                auto range = ThreadKeyRange(keysPerRound, numThreads, t);
                int startKey = range.first;
                int numKeys = range.second;

                if (numKeys == 0) {
                    barrier.Wait();  // Set phase barrier
                    if (isGetMode) barrier.Wait();  // Get phase barrier
                    if (cfg.cleanupMethod == "del") barrier.Wait();  // Del phase barrier
                    return;
                }

                // Set phase (all threads share setAdapter)
                auto setResult = RunSetPhase(&setAdapter, round, startKey, numKeys, cfg.setApi, data);
                setOks[t] = setResult.successCount;
                setLatencies[t] = std::move(setResult.latenciesMs);

                barrier.Wait();

                // Get phase (all threads share getAdapter)
                if (isGetMode) {
                    auto getResult = RunGetPhase(&getAdapter, round, startKey, numKeys);
                    getOks[t] = getResult.successCount;
                    getLatencies[t] = std::move(getResult.latenciesMs);
                    barrier.Wait();
                }

                // Cleanup phase (use delAdapter with 5s timeout, 1K keys/batch)
                if (cfg.cleanupMethod == "del") {
                    auto delResult = RunDelPhase(&delAdapter, round, startKey, numKeys);
                    delOks[t] = delResult.successCount;
                    delLatencies[t] = std::move(delResult.latenciesMs);
                    barrier.Wait();
                }
            });
        }

        for (auto &t : threads) t.join();
        auto roundEnd = std::chrono::steady_clock::now();
        double roundTotalMs = std::chrono::duration<double, std::milli>(roundEnd - roundStart).count();

        // Merge set latencies and record
        {
            std::vector<double> allLat;
            int totalOk = 0;
            for (int t = 0; t < numThreads; t++) {
                totalOk += setOks[t];
                allLat.insert(allLat.end(), setLatencies[t].begin(), setLatencies[t].end());
            }
            stats.totalSet += totalOk;
            auto pct = ComputePercentiles(std::move(allLat));
            double qps = roundTotalMs > 0 ? totalOk * 1000.0 / roundTotalMs : 0;
            double totalSetMs = 0;
            for (int t = 0; t < numThreads; t++) {
                for (auto v : setLatencies[t]) totalSetMs += v;
            }
            benchMetrics.RecordPhase(round, "set", totalOk, pct.avg, pct.p50, pct.p90, pct.p99, pct.max, totalSetMs, qps);
        }

        // Merge get latencies and record
        if (isGetMode) {
            std::vector<double> allLat;
            int totalOk = 0;
            for (int t = 0; t < numThreads; t++) {
                totalOk += getOks[t];
                allLat.insert(allLat.end(), getLatencies[t].begin(), getLatencies[t].end());
            }
            stats.totalGet += totalOk;
            auto pct = ComputePercentiles(std::move(allLat));
            double qps = roundTotalMs > 0 ? totalOk * 1000.0 / roundTotalMs : 0;
            double totalGetMs = 0;
            for (int t = 0; t < numThreads; t++) {
                for (auto v : getLatencies[t]) totalGetMs += v;
            }
            benchMetrics.RecordPhase(round, "get", totalOk, pct.avg, pct.p50, pct.p90, pct.p99, pct.max, totalGetMs, qps);
        }

        // Del phase stats
        if (cfg.cleanupMethod == "del") {
            std::vector<double> allDelLat;
            int totalDelOk = 0;
            for (int t = 0; t < numThreads; t++) {
                totalDelOk += delOks[t];
                allDelLat.insert(allDelLat.end(), delLatencies[t].begin(), delLatencies[t].end());
            }
            stats.totalDel += totalDelOk;
            auto delPct = ComputePercentiles(std::move(allDelLat));
            double qps = roundTotalMs > 0 ? totalDelOk * 1000.0 / roundTotalMs : 0;
            double totalDelMs = 0;
            for (int t = 0; t < numThreads; t++) {
                for (auto v : delLatencies[t]) totalDelMs += v;
            }
            benchMetrics.RecordPhase(round, "del", totalDelOk, delPct.avg, delPct.p50, delPct.p90, delPct.p99, delPct.max, totalDelMs, qps);
        }

        stats.roundsCompleted++;

        if (cfg.cleanupMethod == "ttl" && cfg.ttlSeconds > 0) {
            SLOG_INFO("Waiting for TTL " << cfg.ttlSeconds << "s...");
            std::this_thread::sleep_for(std::chrono::seconds(cfg.ttlSeconds));
        }

        SLOG_INFO("Round " << round << " complete");
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

    // Apply CPU affinity before creating any threads
    std::vector<int> cpus;
    if (!cfg.cpuAffinity.empty()) {
        cpus = ParseCpuList(cfg.cpuAffinity);
    }
    if (cpus.empty()) {
        cpus = GetAvailableCpus();
    }
    if (!cpus.empty() && !ApplyProcessAffinity(cpus)) {
        std::cerr << "WARNING: failed to set CPU affinity" << std::endl;
    }
    std::string cpuList;
    for (size_t i = 0; i < cpus.size() && i < 32; i++) {
        if (i > 0) cpuList += ",";
        cpuList += std::to_string(cpus[i]);
    }
    if (cpus.size() > 32) cpuList += ",...";
    std::cerr << "CPU affinity: " << cpus.size() << " CPUs [" << cpuList << "]" << std::endl;

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
