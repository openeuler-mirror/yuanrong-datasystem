#include "common/jf_service_discovery.h"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "datasystem/coordinator_server.h"
#include "datasystem/utils/status.h"

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
};

static bool ParseArgs(int argc, char **argv, Args &args)
{
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
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

    std::thread runtimeThread([&]() {
        try {
            resultPromise.set_value(CoordinatorServer::GetInstance()->InitAndRun(options));
        } catch (const std::exception &e) {
            resultPromise.set_value(Status(K_RUNTIME_ERROR, std::string("Coordinator thread threw: ") + e.what()));
        } catch (...) {
            resultPromise.set_value(Status(K_RUNTIME_ERROR, "Coordinator thread threw unknown exception"));
        }
    });

    while (!g_shutdownRequested.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    CoordinatorServer::GetInstance()->Stop();
    runtimeThread.join();

    Status status = resultFuture.get();
    if (status.IsError()) {
        fprintf(stderr, "Coordinator InitAndRun failed: %s\n", status.ToString().c_str());
        return 1;
    }
    printf("Coordinator exited normally\n");
    return 0;
}
