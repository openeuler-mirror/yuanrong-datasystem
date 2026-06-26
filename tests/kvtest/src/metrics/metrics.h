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
    MetricsCollector(int instanceId, int intervalMs, const std::string &outputDir,
                     bool cacheMode = false, const std::string &metricsFile = "");

    void SetQpsStages(const std::vector<int> &stages, int durationSec) {
        qpsStages_ = stages;
        stageDurationSec_ = durationSec;
    }

    void Record(const std::string &op, double latencyMs, bool success, uint64_t bytes = 0);
    void RecordVerifyFail();

    std::atomic<uint64_t> &VerifyFailCounter() { return verifyFailCount_; }

    void RecordCacheHit()  { cacheHitCount_++; }
    void RecordCacheMiss() { cacheMissCount_++; }
    double CacheHitRate() const;

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
    std::string outputDir_;
    std::string metricsFile_;   // = outputDir_ + "/latency_timeseries.csv"
    std::chrono::steady_clock::time_point startTime_;

    std::mutex opsMutex_;
    std::vector<std::unique_ptr<OpMetrics>> ops_;
    std::unordered_map<std::string, OpMetrics*> opsMap_;

    std::atomic<uint64_t> verifyFailCount_{0};
    std::atomic<uint64_t> cacheHitCount_{0};
    std::atomic<uint64_t> cacheMissCount_{0};
    bool cacheModeEnabled_ = false;
    std::vector<int> qpsStages_;
    int stageDurationSec_ = 0;
    std::atomic<bool> running_{false};
    std::thread flushThread_;
    std::atomic<bool> started_{false};
};

// Per-phase benchmark metrics (simple CSV output)
struct BenchmarkPhaseRecord {
    int round;
    std::string phase;  // "set", "get", "del"
    int ops;
    int failures;
    double avgMs;
    double minMs;
    double p50Ms;
    double p90Ms;
    double p99Ms;
    double p999Ms;
    double p9999Ms;
    double maxMs;
    double totalMs;
    double phaseElapsedMs;
    double qps;
    double throughputMBs;
};

class BenchmarkMetrics {
public:
    explicit BenchmarkMetrics(const std::string &outputDir);
    void RecordPhase(int round, const std::string &phase, int ops,
                     int failures, double avgMs, double minMs,
                     double p50Ms, double p90Ms, double p99Ms,
                     double p999Ms, double p9999Ms, double maxMs,
                     double totalMs, double phaseElapsedMs,
                     double qps, double throughputMBs);
    void Flush();
private:
    std::string csvPath_;
    std::vector<BenchmarkPhaseRecord> records_;
    bool headerWritten_ = false;
};
