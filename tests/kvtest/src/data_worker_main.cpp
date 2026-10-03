#include "common/jf_service_discovery.h"
#include "common/jemalloc_prof.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
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

#include "datasystem/data_worker.h"
#include "datasystem/utils/status.h"

// Forward declarations for internal symbols exported by
// libdatasystem_worker.so. These are declared in internal headers
// (src/datasystem/common/flags/flags.h) not shipped in the SDK;
// forward-declaring avoids the internal header dependency so the
// file compiles in CMake mode (SDK-only headers).
namespace datasystem {
void SetVersionString(const std::string &version);
void ParseCommandLineFlags(int argc, char **argv);
}  // namespace datasystem

#ifndef DATASYSTEM_VERSION
#define DATASYSTEM_VERSION "unknown"
#endif

using namespace datasystem;

struct Args {
    std::string configPath;
    std::string jfAddr;
    std::string serviceName = "kvcache_coordinator";
    bool showVersion = false;
};

static bool ParseArgs(int argc, char **argv, Args &args)
{
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--version" || arg == "-v") {
            printf("worker_test %s (commit: %s)\n", BUILD_VERSION, BUILD_COMMIT);
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
        else if (arg == "--jf")
            args.jfAddr = next();
        else if (arg == "--service")
            args.serviceName = next();
        else {
            fprintf(stderr, "Unknown arg: %s\n", arg.c_str());
            return false;
        }
    }
    if (args.configPath.empty()) {
        fprintf(stderr, "--config required\n");
        return false;
    }
    if (args.jfAddr.empty()) {
        fprintf(stderr, "--jf required (standalone mode requires JF)\n");
        return false;
    }
    return true;
}

// Poll discovery (not InitAndRun, which is unre-enterable after a failure:
// ArenaManager becomes terminally destroyed) until non-empty, then start the
// worker once. Only K_NOT_FOUND ("JF reachable, list empty") is retried;
// a wrong --service name also lands here and exits after the budget.
// The gap between this precheck and the discovery inside InitAndRun stays
// fail-fast. Jitter avoids lockstep polling across thousands of workers.
static Status WaitDiscoveryThenRun(const DataWorkerOptions &options,
                                   const std::shared_ptr<kvtest::UserCoordinatorDiscovery> &discovery)
{
    constexpr int MAX_WAIT_SECONDS = 120;
    constexpr int MAX_DELAY_SECONDS = 30;
    auto startedAt = std::chrono::steady_clock::now();
    std::mt19937 rng{ std::random_device{}() };
    std::uniform_real_distribution<double> jitter(0.8, 1.2);
    int delaySeconds = 5;
    while (true) {
        std::vector<std::string> candidates;
        Status rc = discovery->GetCoordinators(candidates);
        if (rc.IsOk()) {
            return DataWorker::GetInstance()->InitAndRun(options);
        }
        if (rc.GetCode() != StatusCode::K_NOT_FOUND) {
            fprintf(stderr, "Worker coordinator discovery failed, not retried: %s\n", rc.ToString().c_str());
            return rc;
        }
        auto waitedSec = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startedAt);
        if (waitedSec.count() >= MAX_WAIT_SECONDS) {
            fprintf(stderr, "Worker startup aborted: coordinator discovery stayed empty for %ds "
                            "(registry reachable, service not registered, or wrong --service)\n",
                    MAX_WAIT_SECONDS);
            return rc;
        }
        double sleepSec = delaySeconds * jitter(rng);
        fprintf(stderr, "Worker coordinator discovery empty, waited %llds/%ds, next retry in %.1fs (%s)\n",
                static_cast<long long>(waitedSec.count()), MAX_WAIT_SECONDS, sleepSec, rc.ToString().c_str());
        std::this_thread::sleep_for(std::chrono::duration<double>(sleepSec));
        delaySeconds = std::min(delaySeconds * 2, MAX_DELAY_SECONDS);
    }
}

int main(int argc, char **argv)
{
    Args args;
    if (!ParseArgs(argc, argv, args))
        return 1;

    printf("jemalloc_prof_supported=%s\n", JemallocProfSupported() ? "true" : "false");
    if (args.showVersion)
        return 0;

    SetVersionString(DATASYSTEM_VERSION);
    char *fake_argv[] = { argv[0], nullptr };
    int fake_argc = 1;
    ParseCommandLineFlags(fake_argc, fake_argv);

    auto jfClient = std::make_shared<kvtest::JfClient>(args.jfAddr);
    auto discovery = std::make_shared<kvtest::UserCoordinatorDiscovery>(jfClient, args.serviceName);

    DataWorkerOptions options;
    options.configFilePath = args.configPath;
    options.coordinatorDiscovery = discovery;

    auto status = WaitDiscoveryThenRun(options, discovery);
    if (status.IsError()) {
        fprintf(stderr, "Worker startup failed: %s\n", status.ToString().c_str());
        return 1;
    }
    printf("Worker exited normally\n");
    return 0;
}
