#pragma once
#include "common/config.h"
#include "common/client_config.h"
#include "common/cpu_affinity.h"
#include "common/simple_log.h"
#include "benchmark/benchmark_runner.h"
#include "benchmark/kv_client_adapter.h"
#include "benchmark/remote_worker_resolver.h"

#include <datasystem/kv_client.h>
#include <datasystem/utils/connection.h>
#include <datasystem/utils/service_discovery.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <algorithm>
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
#include <system_error>
#include <thread>
#include <vector>

// --- Pipe protocol types ---

enum ChildCmd : int32_t { CMD_EXIT = 0, CMD_RUN_SET = 1, CMD_RUN_GET = 2,
                          CMD_RUN_DEL = 3, CMD_RUN_MSET = 4, CMD_RUN_MGET = 5,
                          CMD_PREPARE_SET = 6, CMD_PREPARE_GET = 7, CMD_PREPARE_DEL = 8 };
enum ChildRole : int32_t { ROLE_SET = 0, ROLE_GET = 1, ROLE_DEL = 2 };

constexpr char BENCHMARK_CHILD_MODE[] = "--benchmark-child";
constexpr int BENCHMARK_CHILD_ARG_COUNT = 7;
constexpr int BENCHMARK_CHILD_EXEC_FAILURE_EXIT_CODE = 127;
constexpr size_t BENCHMARK_INIT_FIELD_SIZE = 256;

struct CmdMsg {
    int32_t cmd = 0;
    int32_t round = 0;
    int32_t numThreads = 0;
    int32_t startKey = 0;
    int32_t numKeys = 0;
    int32_t maxPasses = 1;
    int32_t oneOpPerThread = 0;
};

struct ArmMsg {
    int64_t startAtNs = 0;
    int64_t stopAtNs = 0;
};

struct ReadyMsg {
    int32_t ready = 0;
};

struct StreamingResultHeader {
    int64_t successCount = 0;
    int64_t failureCount = 0;
    int64_t notFoundCount = 0;
    int64_t timeoutCount = 0;
    double totalLatencyMs = 0;
    double minLatencyMs = 0;
    double maxLatencyMs = 0;
    int64_t firstStartNs = 0;
    int64_t lastEndNs = 0;
    uint64_t centroidCount = 0;
};

struct CentroidMsg {
    double mean = 0;
    double weight = 0;
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
    char errorMsg[BENCHMARK_INIT_FIELD_SIZE] = {};
    char selectedWorker[BENCHMARK_INIT_FIELD_SIZE] = {};
};


// --- Pipe I/O helpers ---

inline bool WriteExact(int fd, const void *buf, size_t len) {
    const char *p = static_cast<const char *>(buf);
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, p + written, len - written);
        if (n < 0 && errno == EINTR) continue;
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
        if (n < 0 && errno == EINTR) continue;
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
    std::string selectedWorker;
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

inline bool RoleUsesServiceDiscovery(ChildRole role, const Config &cfg) {
    switch (role) {
        case ROLE_SET:
            return cfg.testMode == TestMode::SET_LOCAL
                || cfg.testMode == TestMode::GET_LOCAL
                || cfg.ShouldUseServiceDiscoveryForRemoteDirect()
                || cfg.testMode == TestMode::GET_REMOTE_CROSS
                || cfg.testMode == TestMode::MIXED_LOCAL_SET_GET
                || cfg.testMode == TestMode::MIXED_LOCAL_SET_CROSS_GET
                || cfg.testMode == TestMode::MSET_LOCAL
                || cfg.testMode == TestMode::MGET_LOCAL
                || cfg.testMode == TestMode::MGET_REMOTE_CROSS;
        case ROLE_GET:
            return cfg.testMode == TestMode::GET_LOCAL
                || cfg.testMode == TestMode::GET_CROSS_NODE
                || cfg.testMode == TestMode::MIXED_LOCAL_SET_GET
                || cfg.testMode == TestMode::MIXED_REMOTE_SET_REMOTE_CROSS_GET
                || cfg.testMode == TestMode::MGET_LOCAL
                || cfg.testMode == TestMode::MGET_CROSS_NODE;
        case ROLE_DEL:
            return RoleUsesServiceDiscovery(ROLE_SET, cfg);
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

/**
 * @brief Create and initialize the configured ServiceDiscovery implementation.
 * @param[in] cfg Benchmark configuration.
 * @return Initialized ServiceDiscovery, or null on failure.
 */
inline std::shared_ptr<datasystem::IServiceDiscovery> CreateBenchmarkServiceDiscovery(const Config &cfg)
{
    using namespace datasystem;
    std::shared_ptr<IServiceDiscovery> serviceDiscovery;
    if (!cfg.coordinatorAddress.empty()) {
        CoordinatorServiceDiscoveryOptions opts;
        opts.serviceAddress = cfg.coordinatorAddress;
        opts.clusterName = cfg.clusterName;
        opts.hostIdEnvName = cfg.hostIdEnvName;
        serviceDiscovery = std::make_shared<CoordinatorServiceDiscovery>(opts);
    } else {
        ServiceDiscoveryOptions opts;
        opts.etcdAddress = cfg.etcdAddress;
        opts.clusterName = cfg.clusterName;
        opts.hostIdEnvName = cfg.hostIdEnvName;
        serviceDiscovery = std::make_shared<ServiceDiscovery>(opts);
    }
    Status rc = serviceDiscovery->Init();
    if (!rc.IsOk()) {
        SLOG_ERROR("Child ServiceDiscovery init failed: " << rc.GetMsg());
        return nullptr;
    }
    return serviceDiscovery;
}

/**
 * @brief Discover one non-local Worker and keep the selection deterministic across benchmark Clients.
 * @param[in] cfg Benchmark configuration.
 * @param[out] endpoint Selected remote Worker endpoint.
 * @return True when a remote Worker is selected.
 */
inline bool DiscoverRemoteWorker(const Config &cfg, RemoteWorkerEndpoint &endpoint)
{
    auto serviceDiscovery = CreateBenchmarkServiceDiscovery(cfg);
    if (serviceDiscovery == nullptr) {
        return false;
    }
    if (!serviceDiscovery->HasHostAffinity()) {
        SLOG_ERROR("set_remote ServiceDiscovery requires a valid SDK host ID from host_id_env_name");
        return false;
    }
    std::vector<std::string> sameHostWorkers;
    std::vector<std::string> remoteWorkers;
    auto rc = serviceDiscovery->GetAllWorkers(sameHostWorkers, remoteWorkers);
    if (!rc.IsOk()) {
        SLOG_ERROR("Failed to discover Workers for set_remote: " << rc.GetMsg());
        return false;
    }
    if (!SelectRemoteWorkerEndpoint(std::move(remoteWorkers), endpoint)) {
        SLOG_ERROR("No valid remote Worker is available for set_remote");
        return false;
    }
    SLOG_INFO("set_remote discovered and pinned remote Worker " << endpoint.ToString());
    return true;
}

inline std::shared_ptr<datasystem::KVClient> CreateClientForRole(
    ChildRole role, const Config &cfg, std::string *selectedWorker = nullptr) {
    using namespace datasystem;

    if (selectedWorker != nullptr) {
        selectedWorker->clear();
    }
    bool useSD = RoleUsesServiceDiscovery(role, cfg);
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

    if (cfg.ShouldDiscoverRemoteWorkerForSet() && (role == ROLE_SET || role == ROLE_DEL)) {
        RemoteWorkerEndpoint endpoint;
        if (!DiscoverRemoteWorker(cfg, endpoint)) {
            return nullptr;
        }
        opts.host = endpoint.host;
        opts.port = endpoint.port;
        if (selectedWorker != nullptr) {
            *selectedWorker = endpoint.ToString();
        }
    } else if (useSD) {
        opts.serviceDiscovery = CreateBenchmarkServiceDiscovery(cfg);
        if (opts.serviceDiscovery == nullptr) {
            return nullptr;
        }
    } else {
        opts.host = cfg.remoteWorker.host;
        opts.port = cfg.remoteWorker.port;
    }

    auto client = std::make_shared<KVClient>(opts);
    Status rc = InitKvtestClient(cfg, *client);
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

/** @brief Return the current steady-clock timestamp in nanoseconds. */
inline int64_t SteadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

/** @brief Describes one benchmark operation and its measured interval. */
struct TimedBenchmarkOpResult {
    BenchmarkOpResult op;
    int64_t startNs = 0;
    int64_t endNs = 0;
    bool measured = false;
};

/** @brief Invoke the configured single-key Set API and measure only the Set call. */
inline TimedBenchmarkOpResult RunStreamingSet(KVClientAdapter *adapter, const Config &cfg, const std::string &key,
                                             const std::string &data)
{
    if (cfg.setApi == "string_view") {
        const int64_t startNs = SteadyNowNs();
        auto op = adapter->SetWithStatus(key, data);
        const int64_t endNs = SteadyNowNs();
        return { op, startNs, endNs, true };
    }
    BenchmarkOpTiming timing;
    auto op = cfg.setApi == "create_buffer"
                  ? adapter->CreateAndSetWithStatus(key, data.size(), data, &timing)
                  : adapter->CreateAndSetRawWithStatus(key, data.size(), data, &timing);
    return { op, timing.startNs, timing.endNs, timing.startNs != 0 };
}

/** @brief Delete one thread's key partition in bounded batches. */
inline void RunStreamingDelete(KVClientAdapter *adapter, const std::vector<std::string> &keys, int startKey,
                               int numKeys, StreamingPhaseResult &result)
{
    constexpr int DELETE_BATCH_SIZE = 1000;
    constexpr int DELETE_MAX_ATTEMPTS = 3;
    constexpr int DELETE_RETRY_WAIT_MS = 100;
    for (int offset = 0; offset < numKeys; offset += DELETE_BATCH_SIZE) {
        const int count = std::min(DELETE_BATCH_SIZE, numKeys - offset);
        std::vector<std::string> batchKeys;
        batchKeys.reserve(count);
        for (int i = 0; i < count; ++i) {
            batchKeys.push_back(keys[startKey + offset + i]);
        }
        const int64_t startNs = SteadyNowNs();
        BenchmarkOpResult op{ false, false, false };
        for (int attempt = 0; attempt < DELETE_MAX_ATTEMPTS && !op.success; ++attempt) {
            std::vector<std::string> failedKeys;
            op = adapter->DelWithStatus(batchKeys, &failedKeys);
            if (!failedKeys.empty()) {
                batchKeys = std::move(failedKeys);
            }
            if (!op.success && attempt + 1 < DELETE_MAX_ATTEMPTS) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DELETE_RETRY_WAIT_MS));
            }
        }
        const int64_t endNs = SteadyNowNs();
        result.RecordBatch(op, count,
                           static_cast<double>(endNs - startNs) / BENCHMARK_NANOSECONDS_PER_MILLISECOND,
                           startNs, endNs);
    }
}

/** @brief Execute one thread's scheduled benchmark partition. */
template <bool CaptureSetSuccess>
inline void RunStreamingThread(KVClientAdapter *adapter, const Config &cfg, const CmdMsg &cmd, int threadId,
                               const ArmMsg &arm, const std::vector<std::string> &keys,
                               StreamingPhaseResult &result, const std::string &data,
                               std::vector<uint8_t> *successfulSetMask)
{
    auto range = ThreadKeyRange(cmd.numKeys, cmd.numThreads, threadId);
    if (range.second == 0) {
        return;
    }
    const int startKey = range.first;
    if (cmd.cmd == CMD_PREPARE_DEL) {
        RunStreamingDelete(adapter, keys, startKey, range.second, result);
        return;
    }

    const int passLimit = cmd.oneOpPerThread != 0 ? 1 : cmd.maxPasses;
    for (int pass = 0; passLimit == 0 || pass < passLimit; ++pass) {
        const int keyCount = cmd.oneOpPerThread != 0 ? 1 : range.second;
        for (int i = 0; i < keyCount; ++i) {
            const int64_t dispatchNs = SteadyNowNs();
            if (arm.stopAtNs > 0 && dispatchNs >= arm.stopAtNs) {
                return;
            }
            const std::string &key = keys[startKey + i];
            TimedBenchmarkOpResult timed{};
            if (cmd.cmd == CMD_PREPARE_SET) {
                timed = RunStreamingSet(adapter, cfg, key, data);
            } else {
                timed.startNs = dispatchNs;
                timed.op = adapter->GetWithStatus(key);
                timed.endNs = SteadyNowNs();
                timed.measured = true;
            }
            if (timed.measured) {
                result.Record(timed.op,
                              static_cast<double>(timed.endNs - timed.startNs)
                                  / BENCHMARK_NANOSECONDS_PER_MILLISECOND,
                              dispatchNs, timed.endNs);
            } else {
                result.RecordUnmeasuredFailure(timed.op);
            }
            if constexpr (CaptureSetSuccess) {
                if (timed.op.success) {
                    (*successfulSetMask)[startKey + i] = 1;
                }
            }
        }
    }
}

/** @brief Serialize one bounded-memory phase result to the parent. */
inline bool WriteStreamingResult(int fd, StreamingPhaseResult &result)
{
    result.latencyDigest.compress();
    const auto &centroids = result.latencyDigest.processed();
    StreamingResultHeader header;
    header.successCount = result.successCount;
    header.failureCount = result.failureCount;
    header.notFoundCount = result.notFoundCount;
    header.timeoutCount = result.timeoutCount;
    header.totalLatencyMs = result.totalLatencyMs;
    header.minLatencyMs = result.successCount == 0 ? 0 : result.minLatencyMs;
    header.maxLatencyMs = result.maxLatencyMs;
    header.firstStartNs = result.firstStartNs;
    header.lastEndNs = result.lastEndNs;
    header.centroidCount = centroids.size();
    if (!WriteExact(fd, &header, sizeof(header))) {
        return false;
    }
    for (const auto &centroid : centroids) {
        CentroidMsg msg{ centroid.mean(), centroid.weight() };
        if (!WriteExact(fd, &msg, sizeof(msg))) {
            return false;
        }
    }
    return true;
}

struct ScheduledPhaseGate {
    std::mutex mutex;
    std::condition_variable condition;
    int readyThreads = 0;
    bool armed = false;
    ArmMsg arm;
};

/** @brief Holds the successful preload keys for later Get phases. */
struct ScheduledDataset {
    bool initialized = false;
    std::vector<std::string> keys;
};

/** @brief Build this Client's key partition before entering the measurement window. */
inline std::vector<std::string> BuildScheduledKeys(const Config &cfg, const CmdMsg &cmd)
{
    std::vector<std::string> keys;
    keys.reserve(cmd.numKeys);
    for (int i = 0; i < cmd.numKeys; ++i) {
        keys.push_back(MakeBenchKey(cfg.instanceId, cmd.round, cmd.startKey + i));
    }
    return keys;
}

/** @brief Retain the keys whose Set operations succeeded. */
inline void RetainSuccessfulSetKeys(std::vector<std::string> &keys, const std::vector<uint8_t> &successMask,
                                    ScheduledDataset &dataset)
{
    size_t retained = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (successMask[i] != 0) {
            if (retained != i) {
                keys[retained] = std::move(keys[i]);
            }
            ++retained;
        }
    }
    keys.resize(retained);
    dataset.keys = std::move(keys);
    dataset.initialized = true;
}

/** @brief Start workers and hold them at the local ready gate. */
inline bool StartScheduledThreads(KVClientAdapter *adapter, const Config &cfg, const CmdMsg &cmd,
                                  const std::vector<std::string> &keys, const std::string &data,
                                  ScheduledPhaseGate &gate, std::vector<StreamingPhaseResult> &results,
                                  std::vector<std::thread> &threads, std::vector<uint8_t> *successfulSetMask)
{
    try {
        for (int threadId = 0; threadId < cmd.numThreads; ++threadId) {
            threads.emplace_back([&, threadId]() {
                std::unique_lock<std::mutex> lock(gate.mutex);
                ++gate.readyThreads;
                gate.condition.notify_all();
                gate.condition.wait(lock, [&] { return gate.armed; });
                lock.unlock();
                std::this_thread::sleep_until(
                    std::chrono::steady_clock::time_point(std::chrono::nanoseconds(gate.arm.startAtNs)));
                if (successfulSetMask == nullptr) {
                    RunStreamingThread<false>(adapter, cfg, cmd, threadId, gate.arm, keys, results[threadId], data,
                                              successfulSetMask);
                } else {
                    RunStreamingThread<true>(adapter, cfg, cmd, threadId, gate.arm, keys, results[threadId], data,
                                             successfulSetMask);
                }
            });
        }
        return true;
    } catch (const std::system_error &error) {
        SLOG_ERROR("Failed to start benchmark thread: " << error.what());
        {
            std::lock_guard<std::mutex> lock(gate.mutex);
            gate.arm.startAtNs = SteadyNowNs();
            gate.arm.stopAtNs = gate.arm.startAtNs;
            gate.armed = true;
        }
        gate.condition.notify_all();
        for (auto &thread : threads) {
            thread.join();
        }
        return false;
    }
}

/** @brief Complete the child ready/arm handshake and release local workers. */
inline bool ArmScheduledThreads(int readFd, int writeFd, const CmdMsg &cmd, ScheduledPhaseGate &gate)
{
    {
        std::unique_lock<std::mutex> lock(gate.mutex);
        gate.condition.wait(lock, [&] { return gate.readyThreads == cmd.numThreads; });
    }
    ReadyMsg ready{ 1 };
    const bool protocolOk = WriteExact(writeFd, &ready, sizeof(ready))
                            && ReadExact(readFd, &gate.arm, sizeof(gate.arm));
    if (!protocolOk) {
        gate.arm.startAtNs = SteadyNowNs();
        gate.arm.stopAtNs = gate.arm.startAtNs;
    }
    {
        std::lock_guard<std::mutex> lock(gate.mutex);
        gate.armed = true;
    }
    gate.condition.notify_all();
    return protocolOk;
}

/** @brief Run one prepared phase after every local thread and Client is ready. */
inline bool RunScheduledPhase(KVClientAdapter *adapter, const Config &cfg, const CmdMsg &cmd, int readFd, int writeFd,
                              const std::string &data, ScheduledDataset &dataset)
{
    CmdMsg effectiveCmd = cmd;
    std::vector<std::string> generatedKeys;
    const std::vector<std::string> *keys = &dataset.keys;
    if (cmd.cmd == CMD_PREPARE_SET || !dataset.initialized) {
        generatedKeys = BuildScheduledKeys(cfg, cmd);
        keys = &generatedKeys;
    } else {
        effectiveCmd.numKeys = static_cast<int32_t>(dataset.keys.size());
    }
    const bool shouldRetainSetKeys = cmd.cmd == CMD_PREPARE_SET && IsGetMode(cfg.testMode);
    std::vector<uint8_t> successfulSetMask;
    if (shouldRetainSetKeys) {
        successfulSetMask.resize(keys->size());
    }
    ScheduledPhaseGate gate;
    std::vector<StreamingPhaseResult> threadResults(effectiveCmd.numThreads);
    std::vector<std::thread> threads;
    threads.reserve(effectiveCmd.numThreads);
    auto *successMask = successfulSetMask.empty() ? nullptr : &successfulSetMask;
    if (!StartScheduledThreads(adapter, cfg, effectiveCmd, *keys, data, gate, threadResults, threads, successMask)) {
        return false;
    }
    const bool protocolOk = ArmScheduledThreads(readFd, writeFd, effectiveCmd, gate);
    for (auto &thread : threads) {
        thread.join();
    }
    if (!protocolOk) {
        return false;
    }
    StreamingPhaseResult merged;
    for (auto &result : threadResults) {
        merged.Merge(result);
    }
    if (shouldRetainSetKeys) {
        RetainSuccessfulSetKeys(generatedKeys, successfulSetMask, dataset);
    }
    return WriteStreamingResult(writeFd, merged);
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
    SetKvtestClientInitialized(false);
    // The child keeps SIGINT so a terminal stop also interrupts an unbounded phase.
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
    std::string selectedWorker;
    auto client = CreateClientForRole(role, cfg, &selectedWorker);

    // 3. Send INIT_OK/INIT_FAILED
    InitMsg init{};
    if (!client) {
        init.ok = 0;
        snprintf(init.errorMsg, sizeof(init.errorMsg), "KVClient init failed for role %s", roleName);
        WriteExact(writeFd, &init, sizeof(init));
        _exit(1);
    }
    SLOG_INFO("Child " << roleName << " KVClient initialized OK, waiting 3s for init to settle...");
    SetKvtestClientInitialized(true);
    init.ok = 1;
    snprintf(init.selectedWorker, sizeof(init.selectedWorker), "%s", selectedWorker.c_str());
    if (!WriteExact(writeFd, &init, sizeof(init))) _exit(1);
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 4. Prepare adapter and data
    datasystem::SetParam param;
    param.writeMode = datasystem::WriteMode::NONE_L2_CACHE_EVICT;
    if (cfg.ttlSeconds > 0) param.ttlSecond = cfg.ttlSeconds;
    KVClientAdapter adapter(client, param);

    uint64_t dataSize = cfg.dataSizes[0];
    std::string data;
    if (role != ROLE_DEL) {
        data.assign(dataSize, 'A');
    }
    int keysPerRound = CalcKeysPerRound(cfg.workerMemoryMb, dataSize);
    ScheduledDataset scheduledDataset;
    std::unique_ptr<KVClientAdapter> cleanupAdapter;

    // 5. Command loop
    while (true) {
        CmdMsg cmd{};
        if (!ReadExact(readFd, &cmd, sizeof(cmd))) break;
        if (cmd.cmd == CMD_EXIT) break;

        if (cmd.cmd == CMD_PREPARE_SET || cmd.cmd == CMD_PREPARE_GET || cmd.cmd == CMD_PREPARE_DEL) {
            KVClientAdapter *phaseAdapter = &adapter;
            if (cmd.cmd == CMD_PREPARE_DEL && role != ROLE_DEL) {
                if (cleanupAdapter == nullptr) {
                    auto cleanupClient = CreateClientForRole(ROLE_DEL, cfg);
                    if (cleanupClient != nullptr) {
                        cleanupAdapter = std::make_unique<KVClientAdapter>(std::move(cleanupClient), param);
                    }
                }
                if (cleanupAdapter != nullptr) {
                    phaseAdapter = cleanupAdapter.get();
                }
            }
            if (!RunScheduledPhase(phaseAdapter, cfg, cmd, readFd, writeFd, data, scheduledDataset)) {
                break;
            }
            continue;
        }

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
    } else {
        cp.selectedWorker = init.selectedWorker;
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
