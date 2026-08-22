#include "common/jf_service_discovery.h"

#include <cstdio>
#include <cstdlib>
#include <string>

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
};

static bool ParseArgs(int argc, char **argv, Args &args)
{
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--version" || arg == "-v") {
            printf("worker_test %s (commit: %s)\n", BUILD_VERSION, BUILD_COMMIT);
            return false;
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

int main(int argc, char **argv)
{
    Args args;
    if (!ParseArgs(argc, argv, args))
        return 1;

    SetVersionString(DATASYSTEM_VERSION);
    char *fake_argv[] = { argv[0], nullptr };
    int fake_argc = 1;
    ParseCommandLineFlags(fake_argc, fake_argv);

    auto jfClient = std::make_shared<kvtest::JfClient>(args.jfAddr);

    DataWorkerOptions options;
    options.configFilePath = args.configPath;
    options.coordinatorDiscovery = std::make_shared<kvtest::UserCoordinatorDiscovery>(jfClient, args.serviceName);

    auto status = DataWorker::GetInstance()->InitAndRun(options);
    if (status.IsError()) {
        fprintf(stderr, "Worker InitAndRun failed: %s\n", status.ToString().c_str());
        return 1;
    }
    printf("Worker exited normally\n");
    return 0;
}
