#pragma once
#include "common/config.h"
#include "common/cpu_affinity.h"
#include "common/simple_log.h"
#include "benchmark/benchmark_runner.h"
#include "benchmark/kv_client_adapter.h"

#include <datasystem/kv_client.h>
#include <datasystem/utils/connection.h>
#include <datasystem/utils/service_discovery.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <csignal>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// --- Pipe protocol types ---

enum ChildCmd : int32_t { CMD_EXIT = 0, CMD_RUN_SET = 1, CMD_RUN_GET = 2,
                          CMD_RUN_DEL = 3, CMD_RUN_MSET = 4, CMD_RUN_MGET = 5 };
enum ChildRole : int32_t { ROLE_SET = 0, ROLE_GET = 1, ROLE_DEL = 2 };

constexpr char BENCHMARK_CHILD_MODE[] = "--benchmark-child";
constexpr int BENCHMARK_CHILD_ARG_COUNT = 7;
constexpr int BENCHMARK_CHILD_EXEC_FAILURE_EXIT_CODE = 127;

struct CmdMsg {
    int32_t cmd = 0;
    int32_t round = 0;
    int32_t numThreads = 0;
};

struct ResultMsg {
    int32_t successCount = 0;
    int32_t failureCount = 0;
    double avgMs = 0;
    double minMs = 0;
    double p50Ms = 0;
    double p90Ms = 0;
    double p99Ms = 0;
    double p999Ms = 0;
    double p9999Ms = 0;
    double maxMs = 0;
    double totalLatMs = 0;
};

// INIT_OK handshake: child sends this after KVClient::Init succeeds.
struct InitMsg {
    int32_t ok = 0;   // 1 = success, 0 = failure
    char errorMsg[256] = {};
};


// --- Pipe I/O helpers ---

inline bool WriteExact(int fd, const void *buf, size_t len) {
    const char *p = static_cast<const char *>(buf);
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, p + written, len - written);
        if (n <= 0) return false;
        written += static_cast<size_t>(n);
    }
    return true;
}

inline bool ReadExact(int fd, void *buf, size_t len) {
    char *p = static_cast<char *>(buf);
    size_t got = 0;
    while (got < len) {
        ssize_t n = read(fd, p + got, len - got);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

// --- Child process handle ---

struct ChildProcess {
    pid_t pid = -1;
    int toChildFd = -1;    // parent writes, child reads
    int fromChildFd = -1;  // parent reads, child writes
    ChildRole role{};
    bool initOk = false;
};

/**
 * @brief Get the stable benchmark child role name.
 * @param[in] role Child role.
 * @return Role name.
 */
inline const char *GetChildRoleName(ChildRole role) {
    switch (role) {
        case ROLE_SET:
            return "set";
        case ROLE_GET:
            return "get";
        case ROLE_DEL:
            return "del";
    }
    return "unknown";
}

/**
 * @brief Redirect kvtest output to the role-specific child log.
 * @param[in] outputDir Benchmark output directory.
 * @param[in] role Child role.
 */
inline void RedirectChildLogs(const std::string &outputDir, ChildRole role) {
    static std::ofstream childLog(
        outputDir + "/child_" + GetChildRoleName(role) + ".log", std::ios::app);
    if (childLog.is_open()) {
        std::cout.rdbuf(childLog.rdbuf());
        std::cerr.rdbuf(childLog.rdbuf());
    }
}

/**
 * @brief Configure whether a file descriptor closes across exec.
 * @param[in] fd File descriptor.
 * @param[in] enabled Whether close-on-exec is enabled.
 * @return True on success.
 */
inline bool SetCloseOnExec(int fd, bool enabled) {
    int flags = fcntl(fd, F_GETFD);
    if (flags < 0) {
        return false;
    }
    int updated = enabled ? (flags | FD_CLOEXEC) : (flags & ~FD_CLOEXEC);
    return fcntl(fd, F_SETFD, updated) == 0;
}

// --- Determine connection type from role + testMode ---

inline bool RoleUsesServiceDiscovery(ChildRole role, TestMode testMode) {
    switch (role) {
        case ROLE_SET:
            return testMode == TestMode::SET_LOCAL
                || testMode == TestMode::GET_LOCAL
                || testMode == TestMode::GET_REMOTE_CROSS
                || testMode == TestMode::MIXED_LOCAL_SET_GET
                || testMode == TestMode::MIXED_LOCAL_SET_CROSS_GET
                || testMode == TestMode::MSET_LOCAL
                || testMode == TestMode::MGET_LOCAL
                || testMode == TestMode::MGET_REMOTE_CROSS;
        case ROLE_GET:
            return testMode == TestMode::GET_LOCAL
                || testMode == TestMode::GET_CROSS_NODE
                || testMode == TestMode::MIXED_LOCAL_SET_GET
                || testMode == TestMode::MIXED_REMOTE_SET_REMOTE_CROSS_GET
                || testMode == TestMode::MGET_LOCAL
                || testMode == TestMode::MGET_CROSS_NODE;
        case ROLE_DEL:
            return RoleUsesServiceDiscovery(ROLE_SET, testMode);
    }
    return false;
}

// Whether a separate getChild is needed (vs reusing setChild)
inline bool NeedsSeparateGetChild(TestMode testMode) {
    return testMode == TestMode::GET_CROSS_NODE
        || testMode == TestMode::GET_REMOTE_CROSS
        || IsMixedMode(testMode)
        || IsMGetMode(testMode);
}

// --- Create KVClient for a role ---

inline std::shared_ptr<datasystem::KVClient> CreateClientForRole(
    ChildRole role, const Config &cfg) {
    using namespace datasystem;

    bool useSD = RoleUsesServiceDiscovery(role, cfg.testMode);
    ConnectOptions opts;
    opts.connectTimeoutMs = cfg.connectTimeoutMs;
    opts.enableCrossNodeConnection = cfg.enableCrossNodeConnection;
    opts.enableLocalCache = cfg.enableLocalCache;
    opts.dataPlacementPolicy = cfg.dataPlacementPolicy;
    opts.fastTransportMemSize = cfg.fastTransportMemSize;

    if (role == ROLE_DEL) {
        opts.requestTimeoutMs = 5000;  // 5s timeout for cleanup
    } else {
        opts.requestTimeoutMs = cfg.requestTimeoutMs;
    }

    if (useSD) {
        std::shared_ptr<IServiceDiscovery> sd;
        if (!cfg.coordinatorAddress.empty()) {
            CoordinatorServiceDiscoveryOptions cdOpts;
            cdOpts.serviceAddress = cfg.coordinatorAddress;
            cdOpts.clusterName = cfg.clusterName;
            cdOpts.hostIdEnvName = cfg.hostIdEnvName;
            sd = std::make_shared<CoordinatorServiceDiscovery>(cdOpts);
        } else {
            ServiceDiscoveryOptions sdOpts;
            sdOpts.etcdAddress = cfg.etcdAddress;
            sdOpts.clusterName = cfg.clusterName;
            sdOpts.hostIdEnvName = cfg.hostIdEnvName;
            sd = std::make_shared<ServiceDiscovery>(sdOpts);
        }
        Status rc = sd->Init();
        if (!rc.IsOk()) {
            SLOG_ERROR("Child ServiceDiscovery init failed: " << rc.GetMsg());
            return nullptr;
        }
        opts.serviceDiscovery = sd;
    } else {
        opts.host = cfg.remoteWorker.host;
        opts.port = cfg.remoteWorker.port;
    }

    auto client = std::make_shared<KVClient>(opts);
    Status rc = client->Init();
    if (!rc.IsOk()) {
        SLOG_ERROR("Child KVClient init failed: " << rc.GetMsg());
        return nullptr;
    }
    return client;
}

// --- Run a phase with multiple threads inside a child process ---

inline PhaseResult RunPhaseMultiThread(
    KVClientAdapter *adapter, ChildCmd phase, int round,
    int numThreads, int keysPerRound, const std::string &setApi,
    const std::string &data, int instanceId,
    int msetBatchSize = 8, int mgetBatchSize = 8) {
    std::vector<PhaseResult> threadResults(numThreads);
    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&, t]() {
            auto range = ThreadKeyRange(keysPerRound, numThreads, t);
            int startKey = range.first;
            int numKeys = range.second;
            if (numKeys == 0) return;

            switch (phase) {
                case CMD_RUN_SET:
                    threadResults[t] = RunSetPhase(adapter, instanceId, round, startKey, numKeys, setApi, data);
                    break;
                case CMD_RUN_GET:
                    threadResults[t] = RunGetPhase(adapter, instanceId, round, startKey, numKeys);
                    break;
                case CMD_RUN_DEL:
                    threadResults[t] = RunDelPhase(adapter, instanceId, round, startKey, numKeys);
                    break;
                case CMD_RUN_MSET:
                    threadResults[t] = RunMSetPhase(adapter, instanceId, round, startKey, numKeys,
                                                    msetBatchSize, data);
                    break;
                case CMD_RUN_MGET:
                    threadResults[t] = RunMGetPhase(adapter, instanceId, round, startKey, numKeys,
                                                    mgetBatchSize);
                    break;
                default:
                    break;
            }
        });
    }

    for (auto &t : threads) t.join();

    // Merge per-thread results
    PhaseResult merged;
    for (auto &r : threadResults) {
        merged.successCount += r.successCount;
        merged.failureCount += r.failureCount;
        merged.latenciesMs.insert(merged.latenciesMs.end(),
                                  r.latenciesMs.begin(), r.latenciesMs.end());
    }
    return merged;
}

inline ResultMsg PhaseResultToMsg(const PhaseResult &result) {
    ResultMsg msg{};
    msg.successCount = result.successCount;
    msg.failureCount = result.failureCount;
    if (result.latenciesMs.empty()) return msg;

    auto pct = ComputePercentiles(result.latenciesMs);
    msg.avgMs = pct.avg;
    msg.minMs = pct.min;
    msg.p50Ms = pct.p50;
    msg.p90Ms = pct.p90;
    msg.p99Ms = pct.p99;
    msg.p999Ms = pct.p999;
    msg.p9999Ms = pct.p9999;
    msg.maxMs = pct.max;

    double total = 0;
    for (auto v : result.latenciesMs) total += v;
    msg.totalLatMs = total;
    return msg;
}

// --- Child process main entry point ---

inline void ChildProcessMain(int readFd, int writeFd, const Config &cfg, ChildRole role) {
    // Ignore SIGINT/SIGPIPE — parent controls shutdown via CMD_EXIT
    signal(SIGINT, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    const char *roleName = GetChildRoleName(role);

    SLOG_INFO("Child process started, role=" << roleName << ", pid=" << getpid());

    // 1.5 Apply CPU/NUMA affinity (same logic as RunServerMode)
    ApplyAffinityFromConfig(cfg.cpuAffinity, cfg.numaNode, cfg.randomNumaNode);

    // Disable SDK-internal thread pools. Benchmark children already use
    // RunPhaseMultiThread; nested SDK pools (ParallelFor, parallel memcpy)
    // cause SIGSEGV when multiple threads call batch APIs concurrently.
    if (setenv("CLIENT_MEMORY_COPY_THREAD_NUM", "0", 1) != 0) {
        SLOG_INFO("Child WARNING: failed to set CLIENT_MEMORY_COPY_THREAD_NUM");
    }
    if (setenv("CLIENT_MEMORY_COPY_THREAD_NUM_PER_KEY", "0", 1) != 0) {
        SLOG_INFO("Child WARNING: failed to set CLIENT_MEMORY_COPY_THREAD_NUM_PER_KEY");
    }
    if (setenv("CLIENT_MEMCOPY_PARALLEL_THRESHOLD", "2147483647", 1) != 0) {
        SLOG_INFO("Child WARNING: failed to set CLIENT_MEMCOPY_PARALLEL_THRESHOLD");
    }
    if (setenv("CLIENT_PARALLEL_THREAD_MIN_NUM", "0", 1) != 0) {
        SLOG_INFO("Child WARNING: failed to set CLIENT_PARALLEL_THREAD_MIN_NUM");
    }

    // 2. Create KVClient for this role
    auto client = CreateClientForRole(role, cfg);

    // 3. Send INIT_OK/INIT_FAILED
    InitMsg init{};
    if (!client) {
        init.ok = 0;
        snprintf(init.errorMsg, sizeof(init.errorMsg), "KVClient init failed for role %s", roleName);
        WriteExact(writeFd, &init, sizeof(init));
        _exit(1);
    }
    init.ok = 1;
    if (!WriteExact(writeFd, &init, sizeof(init))) _exit(1);

    SLOG_INFO("Child " << roleName << " KVClient initialized OK, waiting 3s for init to settle...");
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 4. Prepare adapter and data
    datasystem::SetParam param;
    param.writeMode = datasystem::WriteMode::NONE_L2_CACHE_EVICT;
    if (cfg.ttlSeconds > 0) param.ttlSecond = cfg.ttlSeconds;
    KVClientAdapter adapter(client, param);

    uint64_t dataSize = cfg.dataSizes[0];
    std::string data(dataSize, 'A');
    int keysPerRound = CalcKeysPerRound(cfg.workerMemoryMb, dataSize);

    // 5. Command loop
    while (true) {
        CmdMsg cmd{};
        if (!ReadExact(readFd, &cmd, sizeof(cmd))) break;
        if (cmd.cmd == CMD_EXIT) break;

        ChildCmd phase = static_cast<ChildCmd>(cmd.cmd);
        PhaseResult result;
        if (phase == CMD_RUN_MSET) {
            // MSet is a batch API; the SDK does not support concurrent MSet
            // calls from multiple threads. Run on a single thread, processing
            // all keys in batches of cfg.msetBatchSize.
            result = RunMSetPhase(&adapter, cfg.instanceId, cmd.round, 0,
                                  keysPerRound, cfg.msetBatchSize, data);
        } else if (phase == CMD_RUN_MGET) {
            // MGet is a batch API; the SDK does not support concurrent MGet
            // calls from multiple threads. Run on a single thread, processing
            // all keys in batches of cfg.mgetBatchSize.
            result = RunMGetPhase(&adapter, cfg.instanceId, cmd.round, 0,
                                  keysPerRound, cfg.mgetBatchSize);
        } else {
            int nThreads = cmd.numThreads > 0 ? cmd.numThreads : cfg.numThreads;
            result = RunPhaseMultiThread(
                &adapter, phase, cmd.round, nThreads, keysPerRound,
                cfg.setApi, data, cfg.instanceId,
                cfg.msetBatchSize, cfg.mgetBatchSize);
        }

        ResultMsg msg = PhaseResultToMsg(result);
        if (!WriteExact(writeFd, &msg, sizeof(msg))) break;

        SLOG_INFO("Child " << roleName << " round=" << cmd.round
                  << " phase=" << cmd.cmd << " ok=" << result.successCount);
    }

    SLOG_INFO("Child " << roleName << " waiting 3s for in-flight operations to complete...");
    std::this_thread::sleep_for(std::chrono::seconds(3));
    SLOG_INFO("Child " << roleName << " exiting");
    exit(0);
}

/**
 * @brief Check whether argv selects the internal benchmark child entrypoint.
 * @param[in] argc Argument count.
 * @param[in] argv Argument values.
 * @return True for a valid benchmark child invocation shape.
 */
inline bool IsBenchmarkChildInvocation(int argc, char *argv[]) {
    return argc == BENCHMARK_CHILD_ARG_COUNT && std::string(argv[1]) == BENCHMARK_CHILD_MODE;
}

/**
 * @brief Parse one integer argument passed to a benchmark child.
 * @param[in] text Argument text.
 * @param[out] value Parsed integer.
 * @return True on success.
 */
inline bool ParseChildIntArg(const char *text, int &value) {
    errno = 0;
    char *end = nullptr;
    long parsed = std::strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed < INT_MIN || parsed > INT_MAX) {
        return false;
    }
    value = static_cast<int>(parsed);
    return true;
}

/**
 * @brief Report benchmark child initialization failure to the parent.
 * @param[in] writeFd Child-to-parent pipe descriptor.
 * @param[in] message Failure detail.
 */
inline void SendChildInitFailure(int writeFd, const std::string &message) {
    InitMsg init{};
    init.ok = 0;
    snprintf(init.errorMsg, sizeof(init.errorMsg), "%s", message.c_str());
    (void)WriteExact(writeFd, &init, sizeof(init));
}

/**
 * @brief Run the re-executed benchmark child entrypoint.
 * @param[in] argc Argument count.
 * @param[in] argv Argument values.
 * @return Process exit code.
 */
inline int RunBenchmarkChild(int argc, char *argv[]) {
    int roleValue = -1;
    int readFd = -1;
    int writeFd = -1;
    if (!IsBenchmarkChildInvocation(argc, argv)
        || !ParseChildIntArg(argv[2], roleValue)
        || !ParseChildIntArg(argv[3], readFd)
        || !ParseChildIntArg(argv[4], writeFd)
        || roleValue < ROLE_SET || roleValue > ROLE_DEL || readFd < 0 || writeFd < 0) {
        return 1;
    }

    auto role = static_cast<ChildRole>(roleValue);
    std::string outputDir = argv[6];
    RedirectChildLogs(outputDir, role);

    Config cfg;
    if (!LoadConfig(argv[5], cfg, outputDir)) {
        SendChildInitFailure(writeFd, "Failed to load benchmark child config");
        return 1;
    }
    ChildProcessMain(readFd, writeFd, cfg, role);
    return 1;
}

// --- Parent-side helpers ---

inline ChildProcess SpawnChild(const Config &cfg, ChildRole role, const std::string &configPath) {
    ChildProcess cp;
    int toChild[2] = {-1, -1};    // [0]=child reads, [1]=parent writes
    int fromChild[2] = {-1, -1};  // [0]=parent reads, [1]=child writes

    if (pipe(toChild) != 0 || pipe(fromChild) != 0) {
        SLOG_ERROR("pipe() failed: " << strerror(errno));
        if (toChild[0] >= 0) close(toChild[0]);
        if (toChild[1] >= 0) close(toChild[1]);
        if (fromChild[0] >= 0) close(fromChild[0]);
        if (fromChild[1] >= 0) close(fromChild[1]);
        return cp;
    }
    if (!SetCloseOnExec(toChild[0], true) || !SetCloseOnExec(toChild[1], true)
        || !SetCloseOnExec(fromChild[0], true) || !SetCloseOnExec(fromChild[1], true)) {
        SLOG_ERROR("Failed to set close-on-exec for child pipes: " << strerror(errno));
        close(toChild[0]); close(toChild[1]);
        close(fromChild[0]); close(fromChild[1]);
        return cp;
    }

    pid_t pid = fork();
    if (pid < 0) {
        SLOG_ERROR("fork() failed: " << strerror(errno));
        close(toChild[0]); close(toChild[1]);
        close(fromChild[0]); close(fromChild[1]);
        return cp;
    }

    if (pid == 0) {
        // Child
        close(toChild[1]);    // close parent write end
        close(fromChild[0]);  // close parent read end
        if (!SetCloseOnExec(toChild[0], false) || !SetCloseOnExec(fromChild[1], false)) {
            SendChildInitFailure(fromChild[1], "Failed to preserve benchmark child pipes across exec");
            _exit(BENCHMARK_CHILD_EXEC_FAILURE_EXIT_CODE);
        }
        std::string roleArg = std::to_string(static_cast<int>(role));
        std::string readFdArg = std::to_string(toChild[0]);
        std::string writeFdArg = std::to_string(fromChild[1]);
        execl("/proc/self/exe", "kvtest", BENCHMARK_CHILD_MODE, roleArg.c_str(), readFdArg.c_str(),
              writeFdArg.c_str(), configPath.c_str(), cfg.outputDir.c_str(), static_cast<char *>(nullptr));
        int execErrno = errno;
        SendChildInitFailure(fromChild[1], "Failed to exec benchmark child: " + std::string(strerror(execErrno)));
        _exit(BENCHMARK_CHILD_EXEC_FAILURE_EXIT_CODE);
    }

    // Parent
    close(toChild[0]);    // close child read end
    close(fromChild[1]);  // close child write end

    cp.pid = pid;
    cp.toChildFd = toChild[1];
    cp.fromChildFd = fromChild[0];
    cp.role = role;
    return cp;
}

inline bool WaitForInit(ChildProcess &cp) {
    InitMsg init{};
    if (!ReadExact(cp.fromChildFd, &init, sizeof(init))) {
        SLOG_ERROR("Child (pid=" << cp.pid << ") init failed: pipe closed");
        return false;
    }
    cp.initOk = (init.ok == 1);
    if (!cp.initOk) {
        SLOG_ERROR("Child (pid=" << cp.pid << ") init failed: " << init.errorMsg);
    }
    return cp.initOk;
}

inline bool SendCommand(const ChildProcess &cp, ChildCmd cmd, int32_t round,
                        int32_t numThreads = 0) {
    CmdMsg msg{cmd, round, numThreads};
    return WriteExact(cp.toChildFd, &msg, sizeof(msg));
}

inline bool RecvResult(const ChildProcess &cp, ResultMsg &result) {
    return ReadExact(cp.fromChildFd, &result, sizeof(result));
}


inline void ShutdownChild(ChildProcess &cp) {
    if (cp.pid <= 0) return;
    CmdMsg exitCmd{CMD_EXIT, 0};
    WriteExact(cp.toChildFd, &exitCmd, sizeof(exitCmd));
    close(cp.toChildFd);
    close(cp.fromChildFd);

    // Wait with timeout
    int status;
    for (int i = 0; i < 50; i++) {  // 5 seconds
        pid_t ret = waitpid(cp.pid, &status, WNOHANG);
        if (ret == cp.pid) {
            cp.pid = -1;
            return;
        }
        if (ret < 0) break;
        usleep(100000);  // 100ms
    }

    // Force kill
    if (cp.pid > 0) {
        kill(cp.pid, SIGKILL);
        waitpid(cp.pid, &status, 0);
        cp.pid = -1;
    }
}

inline void KillAllChildren(std::vector<ChildProcess> &children) {
    for (auto &cp : children) {
        if (cp.pid > 0) {
            kill(cp.pid, SIGKILL);
        }
    }
    for (auto &cp : children) {
        if (cp.pid > 0) {
            int status;
            waitpid(cp.pid, &status, 0);
        }
        if (cp.toChildFd >= 0) close(cp.toChildFd);
        if (cp.fromChildFd >= 0) close(cp.fromChildFd);
        cp.pid = -1;
    }
}
