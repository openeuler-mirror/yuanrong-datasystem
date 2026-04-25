#include "config.h"
#include "metrics.h"
#include "stop.h"
#include "kv_worker.h"
#include "http_server.h"
#include "pipeline.h"
#include "simple_log.h"

#include <datasystem/kv_client.h>
#include <datasystem/utils/connection.h>
#include <datasystem/utils/service_discovery.h>

#include <iostream>
#include <string>
#include <atomic>
#include <csignal>
#include <memory>
#include <thread>

using namespace datasystem;

static std::atomic<bool> gRunning{true};

static void SignalHandler(int sig) {
    std::cerr << "Received signal " << sig << ", shutting down..." << std::endl;
    gRunning = false;
}

static int RunMode(const Config &cfg) {
    std::cerr << "kvclient_standalone_test v" BUILD_VERSION << std::endl;
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

    auto client = std::make_shared<KVClient>(connOpts);
    rc = client->Init();
    if (!rc.IsOk()) {
        std::cerr << "KVClient init failed: " << rc.GetMsg() << std::endl;
        return 1;
    }
    std::cerr << "KVClient initialized" << std::endl;

    MetricsCollector metrics(cfg.instanceId, cfg.metricsIntervalMs, cfg.metricsFile);
    metrics.Start();

    std::signal(SIGTERM, SignalHandler);
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGPIPE, SIG_IGN);

    HttpServer httpServer(cfg, client, metrics, gRunning);
    httpServer.Start();

    std::unique_ptr<KVWorker> worker;
    if (cfg.role == "writer") {
        worker = std::make_unique<KVWorker>(cfg, client, metrics);
        worker->Start();
    } else {
        std::cerr << "Reader mode: waiting for notifications..." << std::endl;
    }

    std::vector<uint64_t> prevCounts;

    while (gRunning) {
        std::this_thread::sleep_for(std::chrono::seconds(3));

        auto snap = metrics.SnapshotCounts();
        if (prevCounts.size() != snap.size()) {
            prevCounts.resize(snap.size(), 0);
        }

        // Build rate string: delta / 3s
        std::string rates;
        for (size_t i = 0; i < snap.size(); i++) {
            uint64_t delta = snap[i].count - prevCounts[i];
            double rate = delta / 3.0;
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
                  << "[out_q=" << notifyOutQ << ", in_q=" << notifyInQ << "]");
    }

    std::cerr << "Shutting down..." << std::endl;
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
    return RunMode(cfg);
}
