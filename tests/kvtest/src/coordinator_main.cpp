#include "common/jf_service_discovery.h"
#include "common/jemalloc_prof.h"
#include "common/config_update_http_server.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <string>
#include <thread>

#if __has_include("build_info.h")
#include "build_info.h"
#endif
#ifndef BUILD_VERSION
#define BUILD_VERSION "unknown"
#endif
#ifndef BUILD_COMMIT
#define BUILD_COMMIT "unknown"
#endif

#include "datasystem/coordinator_server.h"
#include "datasystem/utils/status.h"

// Forward-declared (not included) because common/signal/signal.h is an
// internal header not shipped in the SDK; the symbol is exported by
// libdatasystem_coordinator.so (no -fvisibility=hidden on that target).
namespace datasystem {
bool IsTermSignalReceived();
}  // namespace datasystem

using namespace datasystem;

static std::atomic<bool> g_shutdownRequested(false);

static void SignalHandler(int)
{
    g_shutdownRequested.store(true);
}

struct Args {
    std::string configPath;
    std::string coordinatorAddr;
    std::string jfAddr;
    std::string serviceName = "kvcache_coordinator";
    bool hooks = false;
    int ttl = 30;
    int expectedMemberCount = 1;
    // 0 = disabled（默认不开启 update-config HTTP server）；显式指定端口才开启。
    int updateConfigPort = 0;
    int updateConfigNotReadyHoldMs = 0;
    int updateConfigShutdownHoldMs = 0;
    bool showVersion = false;
};

static bool ParseArgs(int argc, char **argv, Args &args)
{
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--version" || arg == "-v") {
            printf("coordinator_test %s (commit: %s)\n", BUILD_VERSION, BUILD_COMMIT);
            args.showVersion = true;
            return true;
        }
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                fprintf(stderr, "Missing value for %s\n", arg.c_str());
                exit(1);
            }
            return argv[++i];
        };
        if (arg == "--config")
            args.configPath = next();
        else if (arg == "--coordinator")
            args.coordinatorAddr = next();
        else if (arg == "--jf")
            args.jfAddr = next();
        else if (arg == "--service")
            args.serviceName = next();
        else if (arg == "--hooks")
            args.hooks = true;
        else if (arg == "--ttl")
            args.ttl = std::stoi(next());
        else if (arg == "--expected-member-count")
            args.expectedMemberCount = std::stoi(next());
        else if (arg == "--update-config-port")
            args.updateConfigPort = std::stoi(next());
        else if (arg == "--update-config-not-ready-hold-ms")
            args.updateConfigNotReadyHoldMs = std::stoi(next());
        else if (arg == "--update-config-shutdown-hold-ms")
            args.updateConfigShutdownHoldMs = std::stoi(next());
        else {
            fprintf(stderr, "Unknown arg: %s\n", arg.c_str());
            return false;
        }
    }
    if (args.configPath.empty()) {
        fprintf(stderr, "--config required\n");
        return false;
    }
    if (args.coordinatorAddr.empty()) {
        fprintf(stderr, "--coordinator required\n");
        return false;
    }
    if (args.jfAddr.empty()) {
        fprintf(stderr, "--jf required (standalone mode requires JF)\n");
        return false;
    }
    return true;
}

static int ExtractPort(const std::string &addr)
{
    auto pos = addr.rfind(':');
    if (pos == std::string::npos)
        return 0;
    return std::stoi(addr.substr(pos + 1));
}

int main(int argc, char **argv)
{
    Args args;
    if (!ParseArgs(argc, argv, args))
        return 1;

    printf("jemalloc_prof_supported=%s\n", JemallocProfSupported() ? "true" : "false");
    if (args.showVersion)
        return 0;

    auto jfClient = std::make_shared<kvtest::JfClient>(args.jfAddr, args.ttl);

    CoordinatorOptions options;
    options.configFilePath = args.configPath;
    options.coordinatorDiscovery = std::make_shared<kvtest::UserCoordinatorDiscovery>(jfClient, args.serviceName);
    options.expectedMemberCount = args.expectedMemberCount;

    int coordPort = ExtractPort(args.coordinatorAddr);

    if (args.hooks) {
        options.onStart = [jfClient, args, coordPort]() -> Status {
            return jfClient->RegisterService(args.serviceName, coordPort);
        };
        options.onStop = [jfClient, args, coordPort]() -> Status {
            return jfClient->UnregisterService(args.serviceName, coordPort);
        };
    }

    signal(SIGTERM, SignalHandler);
    signal(SIGINT, SignalHandler);
    signal(SIGPIPE, SIG_IGN);

    std::promise<Status> resultPromise;
    auto resultFuture = resultPromise.get_future().share();

    // update-config HTTP server 先于 runtime 启动，使启动早期（runtime 尚未 Ready）与
    // 停止后期（runtime 已 STOPPING）都能经 HTTP 命中 UpdateConfig 的状态码；配合
    // --update-config-not-ready-hold-ms / --update-config-shutdown-hold-ms 两个测试钩子
    // 稳定暴露 K_NOT_READY / K_SHUTTING_DOWN 窗口（默认 0，行为与原来一致）。
    kvtest::ConfigUpdateHttpServer updateServer(args.updateConfigPort, [](const std::string &configJson) {
        auto status = CoordinatorServer::GetInstance()->UpdateConfig(configJson);
        return kvtest::BuildUpdateConfigResponse(static_cast<uint32_t>(status.GetCode()), status.ToString());
    });
    updateServer.Start();

    if (args.updateConfigNotReadyHoldMs > 0) {
        printf("coordinator_test: holding UpdateConfig NOT_READY window for %d ms "
               "(update-config HTTP server ready, runtime not started)\n",
               args.updateConfigNotReadyHoldMs);
        std::this_thread::sleep_for(std::chrono::milliseconds(args.updateConfigNotReadyHoldMs));
    }

    std::thread runtimeThread([&]() {
        try {
            resultPromise.set_value(CoordinatorServer::GetInstance()->InitAndRun(options));
        } catch (const std::exception &e) {
            resultPromise.set_value(Status(K_RUNTIME_ERROR, std::string("Coordinator thread threw: ") + e.what()));
        } catch (...) {
            resultPromise.set_value(Status(K_RUNTIME_ERROR, "Coordinator thread threw unknown exception"));
        }
    });

    while (!g_shutdownRequested.load() && !IsTermSignalReceived()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    CoordinatorServer::GetInstance()->Stop();

    if (args.updateConfigShutdownHoldMs > 0) {
        printf("coordinator_test: holding UpdateConfig SHUTTING_DOWN window for %d ms "
               "(runtime stopping, update-config HTTP server still alive)\n",
               args.updateConfigShutdownHoldMs);
        std::this_thread::sleep_for(std::chrono::milliseconds(args.updateConfigShutdownHoldMs));
    }

    runtimeThread.join();

    Status status = resultFuture.get();
    if (status.IsError()) {
        fprintf(stderr, "Coordinator InitAndRun failed: %s\n", status.ToString().c_str());
        return 1;
    }
    printf("Coordinator exited normally\n");
    return 0;
}
