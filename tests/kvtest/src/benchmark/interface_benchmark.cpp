/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Description: Implements synchronized single-interface benchmark orchestration. */
#include "benchmark/interface_benchmark.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <poll.h>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "benchmark/benchmark_runner.h"
#include "benchmark/subprocess.h"
#include "common/simple_log.h"

namespace {
constexpr int64_t PHASE_START_LEAD_NS = 200'000'000;
constexpr uint64_t MAX_RESULT_CENTROIDS = 1'000'000;
constexpr int CLEANUP_FAILURE_EXIT_CODE = 3;
constexpr int MEASUREMENT_FAILURE_EXIT_CODE = 2;

struct GroupResult {
    StreamingPhaseResult total;
    std::vector<StreamingPhaseResult> clients;
};

struct CleanupResult {
    bool executionOk = false;
    int64_t operationFailureCount = 0;
};

/** @brief Calculate active Set concurrency across partially successful Clients. */
int64_t CalcGroupSetConcurrency(const GroupResult &group, int threadsPerClient)
{
    int64_t concurrency = 0;
    for (const auto &client : group.clients) {
        concurrency += std::min<int64_t>(threadsPerClient, client.successCount);
    }
    return concurrency;
}

class InterfaceCsvWriter {
public:
    InterfaceCsvWriter(const std::string &outputDir, int threadsPerClient, bool measureSetBufferOnly)
        : phases_(outputDir + "/benchmark_phases.csv", std::ios::trunc),
          clients_(outputDir + "/benchmark_clients.csv", std::ios::trunc),
          threadsPerClient_(threadsPerClient),
          measureSetBufferOnly_(measureSetBufferOnly)
    {
        phases_ << "scope,round,operation,success,failures,elapsed_ms,qps,avg_ms,p50_ms,p99_ms,max_ms,"
                   "throughput_mib_s,valid\n";
        clients_ << "round,operation,client_id,success,failures,elapsed_ms,qps,avg_ms,p99_ms,max_ms,"
                    "start_offset_us,valid\n";
    }

    bool IsOpen() const
    {
        return phases_.is_open() && clients_.is_open();
    }

    void WriteGroup(const std::string &scope, int round, const std::string &operation, GroupResult &group,
                    uint64_t dataSize, double elapsedOverrideMs = -1)
    {
        WritePhase(scope, round, operation, group.total, dataSize, elapsedOverrideMs);
        const int64_t firstStartNs = group.total.firstStartNs;
        for (size_t i = 0; i < group.clients.size(); ++i) {
            WriteClient(round, operation, static_cast<int>(i), group.clients[i], firstStartNs);
        }
        phases_.flush();
        clients_.flush();
    }

    void WriteSummary(const std::string &operation, StreamingPhaseResult &result, uint64_t dataSize,
                      double activeElapsedMs)
    {
        WritePhase("total", -1, operation, result, dataSize, activeElapsedMs);
        phases_.flush();
    }

private:
    void WritePhase(const std::string &scope, int round, const std::string &operation,
                    StreamingPhaseResult &result, uint64_t dataSize, double elapsedOverrideMs)
    {
        const double elapsedMs = elapsedOverrideMs >= 0 ? elapsedOverrideMs : result.ElapsedMs();
        const double throughput = CalcBenchmarkThroughputMiBps(result.successCount, dataSize, elapsedMs);
        auto pct = result.GetPercentiles();
        phases_ << std::fixed << std::setprecision(3) << scope << ',' << round << ',' << operation << ','
                << result.successCount << ',' << result.failureCount << ',' << elapsedMs << ','
                << CalcBenchmarkQps(result.successCount, elapsedMs) << ',' << pct.avg << ',' << pct.p50 << ','
                << pct.p99 << ',' << pct.max << ',' << throughput << ','
                << (result.failureCount == 0 && result.successCount > 0 ? "true" : "false") << '\n';
    }

    void WriteClient(int round, const std::string &operation, int clientId, StreamingPhaseResult &result,
                     int64_t groupStartNs)
    {
        const double elapsedMs = operation == "set" && measureSetBufferOnly_
                                     ? CalcSetOnlyElapsedMs(result, threadsPerClient_)
                                     : result.ElapsedMs();
        const double offsetUs = result.firstStartNs == 0
                                    ? 0
                                    : static_cast<double>(result.firstStartNs - groupStartNs)
                                          / BENCHMARK_NANOSECONDS_PER_MICROSECOND;
        auto pct = result.GetPercentiles();
        clients_ << std::fixed << std::setprecision(3) << round << ',' << operation << ',' << clientId << ','
                 << result.successCount << ',' << result.failureCount << ',' << elapsedMs << ','
                 << CalcBenchmarkQps(result.successCount, elapsedMs) << ',' << pct.avg << ',' << pct.p99 << ','
                 << pct.max << ',' << offsetUs << ','
                 << (result.failureCount == 0 && result.successCount > 0 ? "true" : "false") << '\n';
    }

    std::ofstream phases_;
    std::ofstream clients_;
    int threadsPerClient_;
    bool measureSetBufferOnly_;
};

bool ReadExactUntilStopped(int fd, void *buffer, size_t length, const std::atomic<bool> &running)
{
    char *current = static_cast<char *>(buffer);
    size_t received = 0;
    while (received < length && running) {
        pollfd descriptor{ fd, POLLIN, 0 };
        const int rc = poll(&descriptor, 1, 100);
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        if (rc <= 0) {
            if (rc < 0) {
                return false;
            }
            continue;
        }
        const ssize_t count = read(fd, current + received, length - received);
        if (count <= 0) {
            return false;
        }
        received += static_cast<size_t>(count);
    }
    return received == length;
}

bool ReadStreamingResult(const ChildProcess &child, StreamingPhaseResult &result, const std::atomic<bool> &running)
{
    StreamingResultHeader header;
    if (!ReadExactUntilStopped(child.fromChildFd, &header, sizeof(header), running)
        || header.centroidCount > MAX_RESULT_CENTROIDS) {
        return false;
    }
    result.successCount = header.successCount;
    result.failureCount = header.failureCount;
    result.notFoundCount = header.notFoundCount;
    result.timeoutCount = header.timeoutCount;
    result.totalLatencyMs = header.totalLatencyMs;
    result.minLatencyMs = header.minLatencyMs;
    result.maxLatencyMs = header.maxLatencyMs;
    result.firstStartNs = header.firstStartNs;
    result.lastEndNs = header.lastEndNs;
    for (uint64_t i = 0; i < header.centroidCount; ++i) {
        CentroidMsg centroid;
        if (!ReadExactUntilStopped(child.fromChildFd, &centroid, sizeof(centroid), running)) {
            return false;
        }
        result.latencyDigest.add(centroid.mean, centroid.weight);
    }
    return true;
}

bool SpawnGroup(const Config &cfg, ChildRole role, int count, const std::string &configPath,
                std::vector<ChildProcess> &children, std::vector<size_t> &indices)
{
    indices.reserve(count);
    for (int i = 0; i < count; ++i) {
        children.push_back(SpawnChild(cfg, role, configPath));
        if (children.back().pid <= 0) {
            return false;
        }
        indices.push_back(children.size() - 1);
    }
    return true;
}

bool SendPrepare(const std::vector<ChildProcess> &children, const std::vector<size_t> &indices, ChildCmd phase,
                 int round, int numThreads, int totalKeys, int maxPasses, bool oneOpPerThread)
{
    for (size_t clientId = 0; clientId < indices.size(); ++clientId) {
        auto range = ThreadKeyRange(totalKeys, static_cast<int>(indices.size()), static_cast<int>(clientId));
        CmdMsg cmd;
        cmd.cmd = phase;
        cmd.round = round;
        cmd.numThreads = numThreads;
        cmd.startKey = range.first;
        cmd.numKeys = range.second;
        cmd.maxPasses = maxPasses;
        cmd.oneOpPerThread = oneOpPerThread ? 1 : 0;
        if (!WriteExact(children[indices[clientId]].toChildFd, &cmd, sizeof(cmd))) {
            return false;
        }
    }
    return true;
}

bool WaitUntilReady(const std::vector<ChildProcess> &children, const std::vector<size_t> &indices)
{
    for (auto index : indices) {
        ReadyMsg ready;
        if (!ReadExact(children[index].fromChildFd, &ready, sizeof(ready)) || ready.ready != 1) {
            return false;
        }
    }
    return true;
}

bool ArmGroup(const std::vector<ChildProcess> &children, const std::vector<size_t> &indices, int64_t durationMs)
{
    ArmMsg arm;
    arm.startAtNs = SteadyNowNs() + PHASE_START_LEAD_NS;
    arm.stopAtNs = durationMs > 0 ? arm.startAtNs + durationMs * BENCHMARK_NANOSECONDS_PER_MILLISECOND : 0;
    for (auto index : indices) {
        if (!WriteExact(children[index].toChildFd, &arm, sizeof(arm))) {
            return false;
        }
    }
    return true;
}

bool ReceiveGroup(const std::vector<ChildProcess> &children, const std::vector<size_t> &indices, GroupResult &group,
                  const std::atomic<bool> &running)
{
    group.clients.resize(indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
        if (!ReadStreamingResult(children[indices[i]], group.clients[i], running)) {
            return false;
        }
        group.total.Merge(group.clients[i]);
    }
    return true;
}

bool ExecuteGroup(const std::vector<ChildProcess> &children, const std::vector<size_t> &indices, ChildCmd phase,
                  int round, int numThreads, int totalKeys, int maxPasses, int64_t durationMs, bool oneOpPerThread,
                  GroupResult &group, const std::atomic<bool> &running)
{
    return SendPrepare(children, indices, phase, round, numThreads, totalKeys, maxPasses, oneOpPerThread)
           && WaitUntilReady(children, indices) && ArmGroup(children, indices, durationMs)
           && ReceiveGroup(children, indices, group, running);
}

bool WaitForAllChildren(std::vector<ChildProcess> &children, const Config &cfg)
{
    std::string selectedWorker;
    for (auto &child : children) {
        if (!WaitForInit(child)) {
            return false;
        }
        if (!cfg.ShouldDiscoverRemoteWorkerForSet()) {
            continue;
        }
        if (child.selectedWorker.empty()) {
            SLOG_ERROR("set_remote child did not report its discovered Worker");
            return false;
        }
        if (selectedWorker.empty()) {
            selectedWorker = child.selectedWorker;
        } else if (selectedWorker != child.selectedWorker) {
            SLOG_ERROR("set_remote Clients selected different Workers: " << selectedWorker << " and "
                                                                          << child.selectedWorker);
            return false;
        }
    }
    if (!selectedWorker.empty()) {
        SLOG_INFO("All set_remote Clients pinned Worker " << selectedWorker);
    }
    return true;
}

void ShutdownAllChildren(std::vector<ChildProcess> &children)
{
    constexpr int SHUTDOWN_POLL_COUNT = 50;
    constexpr int SHUTDOWN_POLL_INTERVAL_US = 100'000;
    for (auto &child : children) {
        if (child.pid <= 0) {
            continue;
        }
        CmdMsg exitCmd;
        exitCmd.cmd = CMD_EXIT;
        (void)WriteExact(child.toChildFd, &exitCmd, sizeof(exitCmd));
        close(child.toChildFd);
        close(child.fromChildFd);
        child.toChildFd = -1;
        child.fromChildFd = -1;
    }
    for (auto &child : children) {
        if (child.pid <= 0) {
            continue;
        }
        int status = 0;
        bool exited = false;
        for (int attempt = 0; attempt < SHUTDOWN_POLL_COUNT; ++attempt) {
            if (waitpid(child.pid, &status, WNOHANG) == child.pid) {
                exited = true;
                break;
            }
            usleep(SHUTDOWN_POLL_INTERVAL_US);
        }
        if (!exited) {
            kill(child.pid, SIGKILL);
            (void)waitpid(child.pid, &status, 0);
        }
        child.pid = -1;
    }
}

CleanupResult CleanupDataset(const Config &cfg, const std::vector<ChildProcess> &children,
                             const std::vector<size_t> &cleanupIndices, int round, int keysPerDataset,
                             const std::atomic<bool> &running)
{
    if (cleanupIndices.empty()) {
        return { true, 0 };
    }
    GroupResult cleanup;
    if (!ExecuteGroup(children, cleanupIndices, CMD_PREPARE_DEL, round, cfg.numThreads, keysPerDataset, 1, 0, false,
                      cleanup, running)) {
        return {};
    }
    return { true, cleanup.total.failureCount };
}

bool TtlCoversGetWindow(const Config &cfg, const StreamingPhaseResult &setup)
{
    if (cfg.ttlSeconds == 0) {
        return true;
    }
    if (cfg.durationSeconds <= 0 || setup.firstStartNs == 0) {
        return false;
    }
    const int requestTimeoutMs = cfg.requestTimeoutMs > 0 ? cfg.requestTimeoutMs : cfg.connectTimeoutMs;
    const int64_t expiresAtNs = setup.firstStartNs
                                + static_cast<int64_t>(cfg.ttlSeconds) * BENCHMARK_NANOSECONDS_PER_SECOND;
    const int64_t requiredEndNs = SteadyNowNs() + PHASE_START_LEAD_NS
                                  + static_cast<int64_t>(cfg.durationSeconds) * BENCHMARK_NANOSECONDS_PER_SECOND
                                  + static_cast<int64_t>(requestTimeoutMs)
                                        * BENCHMARK_NANOSECONDS_PER_MILLISECOND;
    return expiresAtNs >= requiredEndNs;
}

bool TtlCanCoverConfiguredGet(const Config &cfg)
{
    if (cfg.ttlSeconds == 0) {
        return true;
    }
    if (cfg.durationSeconds <= 0) {
        return false;
    }
    const int requestTimeoutMs = cfg.requestTimeoutMs > 0 ? cfg.requestTimeoutMs : cfg.connectTimeoutMs;
    const int64_t ttlNs = static_cast<int64_t>(cfg.ttlSeconds) * BENCHMARK_NANOSECONDS_PER_SECOND;
    const int64_t requiredNs = static_cast<int64_t>(cfg.durationSeconds) * BENCHMARK_NANOSECONDS_PER_SECOND
                               + static_cast<int64_t>(requestTimeoutMs)
                                     * BENCHMARK_NANOSECONDS_PER_MILLISECOND
                               + PHASE_START_LEAD_NS;
    return ttlNs >= requiredNs;
}

int RunGetBenchmark(const Config &cfg, std::vector<ChildProcess> &children, const std::vector<size_t> &measuredIndices,
                    const std::vector<size_t> &cleanupIndices, int keysPerDataset, InterfaceCsvWriter &csv,
                    std::atomic<bool> &running)
{
    GroupResult setup;
    if (!ExecuteGroup(children, measuredIndices, CMD_PREPARE_SET, 0, cfg.numThreads, keysPerDataset, 1, 0, false,
                      setup, running)) {
        return 1;
    }
    const int64_t setConcurrency = CalcGroupSetConcurrency(setup, cfg.numThreads);
    if (cfg.setApi == "string_view") {
        csv.WriteGroup("setup", 0, "set", setup, cfg.dataSizes[0]);
    } else {
        csv.WriteGroup("setup", 0, "set", setup, cfg.dataSizes[0],
                       CalcSetOnlyElapsedMs(setup.total, setConcurrency));
    }
    if (setup.total.successCount == 0) {
        SLOG_ERROR("Get setup produced no keys: failures=" << setup.total.failureCount);
        (void)CleanupDataset(cfg, children, cleanupIndices, 0, keysPerDataset, running);
        return MEASUREMENT_FAILURE_EXIT_CODE;
    }
    if (setup.total.failureCount != 0) {
        int64_t effectiveConcurrency = 0;
        for (const auto &client : setup.clients) {
            effectiveConcurrency += std::min<int64_t>(cfg.numThreads, client.successCount);
        }
        SLOG_WARN("Get setup partially succeeded: success=" << setup.total.successCount
                                                              << ", failures=" << setup.total.failureCount
                                                              << ", effective_concurrency="
                                                              << effectiveConcurrency);
    }

    GroupResult warmup;
    if (!ExecuteGroup(children, measuredIndices, CMD_PREPARE_GET, 0, cfg.numThreads, keysPerDataset, 1, 0, true,
                      warmup, running)) {
        SLOG_ERROR("Get warmup execution failed");
        (void)CleanupDataset(cfg, children, cleanupIndices, 0, keysPerDataset, running);
        return MEASUREMENT_FAILURE_EXIT_CODE;
    }
    if (warmup.total.successCount == 0) {
        SLOG_ERROR("Get warmup produced no successful reads: failures=" << warmup.total.failureCount);
        (void)CleanupDataset(cfg, children, cleanupIndices, 0, keysPerDataset, running);
        return MEASUREMENT_FAILURE_EXIT_CODE;
    }
    if (warmup.total.failureCount != 0) {
        SLOG_WARN("Get warmup had failures: success=" << warmup.total.successCount
                                                        << ", failures=" << warmup.total.failureCount
                                                        << "; continuing benchmark");
    }
    if (!TtlCoversGetWindow(cfg, setup.total)) {
        SLOG_ERROR("TTL cannot cover the complete Get measurement window");
        (void)CleanupDataset(cfg, children, cleanupIndices, 0, keysPerDataset, running);
        return 1;
    }

    GroupResult measured;
    const int64_t durationMs = static_cast<int64_t>(cfg.durationSeconds) * BENCHMARK_MILLISECONDS_PER_SECOND;
    if (!ExecuteGroup(children, measuredIndices, CMD_PREPARE_GET, 0, cfg.numThreads, keysPerDataset, cfg.totalRounds,
                      durationMs, false, measured, running)) {
        return 1;
    }
    csv.WriteGroup("total", -1, "get", measured, cfg.dataSizes[0]);
    const auto cleanup = CleanupDataset(cfg, children, cleanupIndices, 0, keysPerDataset, running);
    SLOG_INFO("Get benchmark finished: success=" << measured.total.successCount
                                                   << ", failures=" << measured.total.failureCount
                                                   << ", not_found=" << measured.total.notFoundCount
                                                   << ", timeouts=" << measured.total.timeoutCount
                                                   << ", elapsed_ms=" << measured.total.ElapsedMs());
    if (!cleanup.executionOk || cleanup.operationFailureCount != 0) {
        return CLEANUP_FAILURE_EXIT_CODE;
    }
    return measured.total.failureCount == 0 ? 0 : MEASUREMENT_FAILURE_EXIT_CODE;
}

bool DurationReached(const Config &cfg, const std::chrono::steady_clock::time_point &start)
{
    if (cfg.durationSeconds <= 0) {
        return false;
    }
    return std::chrono::steady_clock::now() - start >= std::chrono::seconds(cfg.durationSeconds);
}

double GetSetElapsedMs(const Config &cfg, const GroupResult &group)
{
    if (cfg.setApi == "string_view") {
        return group.total.ElapsedMs();
    }
    return CalcSetOnlyElapsedMs(group.total, CalcGroupSetConcurrency(group, cfg.numThreads));
}

int RunContinuousSetBenchmark(const Config &cfg, const std::vector<ChildProcess> &children,
                              const std::vector<size_t> &measuredIndices,
                              const std::vector<size_t> &cleanupIndices, int keysPerDataset, InterfaceCsvWriter &csv,
                              const std::atomic<bool> &running)
{
    GroupResult setup;
    if (!ExecuteGroup(children, measuredIndices, CMD_PREPARE_SET, 0, cfg.numThreads, keysPerDataset, 1, 0, false,
                      setup, running)) {
        return 1;
    }
    csv.WriteGroup("setup", 0, "set", setup, cfg.dataSizes[0], GetSetElapsedMs(cfg, setup));
    if (!HasRunnableContinuousSetSetup(setup.total.successCount)) {
        SLOG_ERROR("Continuous Set setup produced no keys: failures=" << setup.total.failureCount);
        (void)CleanupDataset(cfg, children, cleanupIndices, 0, keysPerDataset, running);
        return MEASUREMENT_FAILURE_EXIT_CODE;
    }
    if (setup.total.failureCount != 0 || setup.total.successCount != keysPerDataset) {
        SLOG_WARN("Continuous Set setup partially succeeded: expected=" << keysPerDataset
                                                                          << ", success="
                                                                          << setup.total.successCount
                                                                          << ", failures="
                                                                          << setup.total.failureCount
                                                                          << "; continuing benchmark");
    }
    GroupResult measured;
    const int64_t durationMs = static_cast<int64_t>(cfg.durationSeconds) * BENCHMARK_MILLISECONDS_PER_SECOND;
    if (!ExecuteGroup(children, measuredIndices, CMD_PREPARE_SET, 0, cfg.numThreads, keysPerDataset, cfg.totalRounds,
                      durationMs, false, measured, running)) {
        return 1;
    }
    csv.WriteGroup("total", -1, "set", measured, cfg.dataSizes[0], GetSetElapsedMs(cfg, measured));
    const auto cleanup = CleanupDataset(cfg, children, cleanupIndices, 0, keysPerDataset, running);
    SLOG_INFO("Continuous Set benchmark finished: resident_keys=" << keysPerDataset
                                                                    << ", success=" << measured.total.successCount
                                                                    << ", failures=" << measured.total.failureCount
                                                                    << ", set_only_elapsed_ms="
                                                                    << GetSetElapsedMs(cfg, measured));
    if (!cleanup.executionOk || cleanup.operationFailureCount != 0) {
        return CLEANUP_FAILURE_EXIT_CODE;
    }
    return setup.total.failureCount == 0 && measured.total.failureCount == 0 ? 0 : MEASUREMENT_FAILURE_EXIT_CODE;
}

int RunSetBenchmark(const Config &cfg, std::vector<ChildProcess> &children, const std::vector<size_t> &measuredIndices,
                    const std::vector<size_t> &cleanupIndices, int keysPerDataset, InterfaceCsvWriter &csv,
                    std::atomic<bool> &running)
{
    if (cfg.cleanupMethod == "none") {
        return RunContinuousSetBenchmark(cfg, children, measuredIndices, cleanupIndices, keysPerDataset, csv, running);
    }
    StreamingPhaseResult summary;
    double activeElapsedMs = 0;
    bool cleanupFailed = false;
    const auto benchmarkStart = std::chrono::steady_clock::now();
    int completedRounds = 0;
    for (int round = 0; running && (cfg.totalRounds == 0 || round < cfg.totalRounds); ++round) {
        if (DurationReached(cfg, benchmarkStart)) {
            break;
        }
        GroupResult measured;
        if (!ExecuteGroup(children, measuredIndices, CMD_PREPARE_SET, round, cfg.numThreads, keysPerDataset, 1, 0,
                          false, measured, running)) {
            return 1;
        }
        const double setElapsedMs = GetSetElapsedMs(cfg, measured);
        csv.WriteGroup("round", round, "set", measured, cfg.dataSizes[0], setElapsedMs);
        activeElapsedMs += setElapsedMs;
        summary.Merge(measured.total);
        ++completedRounds;
        const auto cleanup = CleanupDataset(cfg, children, cleanupIndices, round, keysPerDataset, running);
        if (!cleanup.executionOk) {
            cleanupFailed = true;
            break;
        }
        if (cleanup.operationFailureCount != 0) {
            cleanupFailed = true;
            SLOG_WARN("Set cleanup had failures in round " << round
                                                             << ": failures=" << cleanup.operationFailureCount
                                                             << "; continuing benchmark");
        }
        if (cfg.cleanupMethod == "ttl") {
            std::this_thread::sleep_for(std::chrono::seconds(cfg.ttlSeconds));
        } else if (cfg.roundCleanupWaitMs > 0 && !DurationReached(cfg, benchmarkStart)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(cfg.roundCleanupWaitMs));
        }
    }
    csv.WriteSummary("set", summary, cfg.dataSizes[0], activeElapsedMs);
    const char *elapsedName = cfg.setApi == "string_view" ? "active_elapsed_ms" : "set_only_elapsed_ms";
    SLOG_INFO("Set benchmark finished: rounds=" << completedRounds << ", success=" << summary.successCount
                                                 << ", failures=" << summary.failureCount << ", " << elapsedName
                                                 << '=' << activeElapsedMs);
    if (cleanupFailed) {
        return CLEANUP_FAILURE_EXIT_CODE;
    }
    return summary.failureCount == 0 ? 0 : MEASUREMENT_FAILURE_EXIT_CODE;
}
}  // namespace

bool IsInterfaceBenchmarkMode(TestMode mode)
{
    return mode == TestMode::SET_LOCAL || mode == TestMode::SET_REMOTE || mode == TestMode::GET_LOCAL
           || mode == TestMode::GET_REMOTE_DIRECT;
}

int RunInterfaceBenchmark(const Config &cfg, const std::string &configPath, std::atomic<bool> &running)
{
    const int keysPerDataset = CalcKeysPerRound(cfg.workerMemoryMb, cfg.dataSizes[0]);
    const int totalConcurrency = cfg.numClients * cfg.numThreads;
    if (keysPerDataset < totalConcurrency) {
        SLOG_ERROR("keys_per_dataset=" << keysPerDataset << " is smaller than total_concurrency="
                                        << totalConcurrency);
        return 1;
    }
    if (IsGetMode(cfg.testMode) && !TtlCanCoverConfiguredGet(cfg)) {
        SLOG_ERROR("TTL must cover duration_seconds, request_timeout_ms, and synchronized start lead time");
        return 1;
    }
    std::vector<ChildProcess> children;
    std::vector<size_t> measuredIndices;
    std::vector<size_t> cleanupIndices;
    children.reserve(static_cast<size_t>(cfg.numClients) * 2);
    if (!SpawnGroup(cfg, ROLE_SET, cfg.numClients, configPath, children, measuredIndices)
        || (cfg.cleanupMethod == "del" && !IsGetMode(cfg.testMode)
            && !SpawnGroup(cfg, ROLE_DEL, cfg.numClients, configPath, children, cleanupIndices))
        || !WaitForAllChildren(children, cfg)) {
        KillAllChildren(children);
        return 1;
    }
    if (cfg.cleanupMethod == "del" && IsGetMode(cfg.testMode)) {
        cleanupIndices = measuredIndices;
    } else if (cfg.cleanupMethod == "none") {
        cleanupIndices = measuredIndices;
    }

    InterfaceCsvWriter csv(cfg.outputDir, cfg.numThreads, cfg.setApi != "string_view");
    if (!csv.IsOpen()) {
        SLOG_ERROR("Failed to open benchmark CSV outputs in " << cfg.outputDir);
        KillAllChildren(children);
        return 1;
    }
    SLOG_INFO("Interface benchmark: clients=" << cfg.numClients << ", threads_per_client=" << cfg.numThreads
                                               << ", total_concurrency=" << totalConcurrency
                                               << ", keys_per_dataset=" << keysPerDataset
                                               << ", resident_data_gib=" << std::fixed << std::setprecision(3)
                                               << static_cast<long double>(keysPerDataset) * cfg.dataSizes[0]
                                                      / (1024.0L * 1024.0L * 1024.0L)
                                               << "; target_qps is not used in benchmark mode");
    int rc = IsGetMode(cfg.testMode)
                 ? RunGetBenchmark(cfg, children, measuredIndices, cleanupIndices, keysPerDataset, csv, running)
                 : RunSetBenchmark(cfg, children, measuredIndices, cleanupIndices, keysPerDataset, csv, running);
    ShutdownAllChildren(children);
    return rc;
}
