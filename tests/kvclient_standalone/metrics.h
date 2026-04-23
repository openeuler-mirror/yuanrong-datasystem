#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>

struct OpMetrics {
    std::string opName;
    std::atomic<uint64_t> totalCount{0};
    std::atomic<uint64_t> successCount{0};
    std::atomic<uint64_t> failCount{0};

    std::mutex windowMutex;
    std::vector<double> windowLatencies;

    std::mutex globalMutex;
    std::vector<double> globalLatencies;
};

class MetricsCollector {
public:
    MetricsCollector(int instanceId, int intervalMs, const std::string &metricsFile);

    void Record(const std::string &op, double latencyMs, bool success);
    void RecordVerifyFail();

    std::atomic<uint64_t> &VerifyFailCounter() { return verifyFailCount_; }

    void Start();
    void Stop();

    std::string GetStatsJson();

private:
    OpMetrics& GetOrCreateOp(const std::string &op);
    void FlushWindow();
    void WriteSummary();

    int instanceId_;
    int intervalMs_;
    std::string metricsFile_;
    std::chrono::steady_clock::time_point startTime_;

    std::mutex opsMutex_;
    std::vector<std::unique_ptr<OpMetrics>> ops_;

    std::atomic<uint64_t> verifyFailCount_{0};
    std::atomic<bool> running_{false};
    std::thread flushThread_;
};
