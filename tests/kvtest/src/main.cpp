#include "common/config.h"
#include "common/simple_log.h"
#include "common/cpu_affinity.h"
#include "metrics/metrics.h"
#include "pipeline/pipeline.h"
#include "pipeline/kv_worker.h"
#include "pipeline/cache_reader.h"
#include "rpc/http_server.h"
#include "pipeline/stop.h"

#include <datasystem/kv_client.h>
#include <datasystem/utils/connection.h>
#include <datasystem/utils/service_discovery.h>

#include <iostream>
#include <string>
#include <atomic>
#include <csignal>
#include <fstream>
#include <memory>
#include <thread>

using namespace datasystem;

static std::atomic<bool> gRunning{true};

static void SignalHandler(int sig) {
    std::cerr << "Received signal " << sig << ", shutting down..." << std::endl;
    gRunning = false;
}

static int RunMode(const Config &cfg) {
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
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " [--stop] <config.json>\n";
        return 1;
    }

    bool stopMode = false;
    std::string configPath;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--stop") {
            stopMode = true;
        } else {
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

    return RunMode(cfg);
}
