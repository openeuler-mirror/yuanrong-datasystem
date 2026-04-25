#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <unordered_map>

struct OpMetrics {
    std::string opName;
    std::atomic<uint64_t> totalCount{0};
    std::atomic<uint64_t> successCount{0};
    std::atomic<uint64_t> failCount{0};
    std::atomic<uint64_t> totalBytes{0};

    std::mutex windowMutex;
    std::vector<double> windowLatencies;
    uint64_t windowBytes = 0;

    std::mutex globalMutex;
    std::vector<double> globalRing;  // fixed-size ring buffer
    size_t globalHead = 0;
    size_t globalCount = 0;
};

class MetricsCollector {
public:
    MetricsCollector(int instanceId, int intervalMs, const std::string &metricsFile);

    void Record(const std::string &op, double latencyMs, bool success, uint64_t bytes = 0);
    void RecordVerifyFail();

    std::atomic<uint64_t> &VerifyFailCounter() { return verifyFailCount_; }

    void Start();
    void Stop();

    std::string GetStatsJson();
    void WriteSummary();

    struct OpSnapshot { std::string name; uint64_t count; };
    std::vector<OpSnapshot> SnapshotCounts();

private:
    OpMetrics& GetOrCreateOp(const std::string &op);
    void FlushWindow();

    int instanceId_;
    int intervalMs_;
    std::string metricsFile_;
    std::chrono::steady_clock::time_point startTime_;

    std::mutex opsMutex_;
    std::vector<std::unique_ptr<OpMetrics>> ops_;
    std::unordered_map<std::string, OpMetrics*> opsMap_;

    std::atomic<uint64_t> verifyFailCount_{0};
    std::atomic<bool> running_{false};
    std::thread flushThread_;
    bool started_ = false;
};
