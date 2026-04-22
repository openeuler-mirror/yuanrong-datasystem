#include "config.h"
#include "metrics.h"
#include "stop.h"
#include "kv_worker.h"
#include "http_server.h"

#include <datasystem/kv_client.h>
#include <datasystem/utils/connection.h>
#include <datasystem/utils/service_discovery.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <string>
#include <atomic>
#include <csignal>
#include <memory>
#include <thread>

using namespace datasystem;

static std::atomic<bool> gRunning{true};

static void SignalHandler(int) {
    gRunning = false;
}

static int RunMode(const Config &cfg) {
    ServiceDiscoveryOptions sdOpts;
    sdOpts.etcdAddress = cfg.etcdAddress;
    sdOpts.clusterName = cfg.clusterName;

    auto sd = std::make_shared<ServiceDiscovery>(sdOpts);
    Status rc = sd->Init();
    if (!rc.IsOk()) {
        spdlog::error("ServiceDiscovery init failed: {}", rc.GetMsg());
        return 1;
    }
    spdlog::info("ServiceDiscovery initialized: etcd={}", cfg.etcdAddress);

    ConnectOptions connOpts;
    connOpts.serviceDiscovery = sd;
    connOpts.connectTimeoutMs = cfg.connectTimeoutMs;
    connOpts.requestTimeoutMs = cfg.requestTimeoutMs;

    auto client = std::make_shared<KVClient>(connOpts);
    rc = client->Init();
    if (!rc.IsOk()) {
        spdlog::error("KVClient init failed: {}", rc.GetMsg());
        return 1;
    }
    spdlog::info("KVClient initialized");

    MetricsCollector metrics(cfg.instanceId, cfg.metricsIntervalMs, cfg.metricsFile);
    metrics.Start();

    std::signal(SIGTERM, SignalHandler);
    std::signal(SIGINT, SignalHandler);

    HttpServer httpServer(cfg, client, metrics, gRunning);
    httpServer.Start();

    KVWorker worker(cfg, client, metrics);
    worker.Start();

    while (gRunning) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    spdlog::info("Shutting down...");
    worker.Stop();
    httpServer.Stop();
    metrics.Stop();

    spdlog::info("Shutdown complete");
    return 0;
}

static int StopMode(const Config &cfg) {
    if (cfg.peers.empty()) {
        spdlog::error("No peers in config");
        return 1;
    }
    int ok = StopAllPeers(cfg.peers);
    return (ok == static_cast<int>(cfg.peers.size())) ? 0 : 1;
}

int main(int argc, char *argv[]) {
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");

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
