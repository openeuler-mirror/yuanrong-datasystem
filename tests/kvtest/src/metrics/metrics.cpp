#include "metrics.h"
#include "pipeline/pipeline.h"
#include "common/simple_log.h"
#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <thread>
#include "vendor/nlohmann_json.hpp"

using json = nlohmann::json;

MetricsCollector::MetricsCollector(int instanceId, int intervalMs,
                                   const std::string &outputDir, bool cacheMode,
                                   const std::string &metricsFile)
    : instanceId_(instanceId), intervalMs_(intervalMs),
      outputDir_(outputDir),
      metricsFile_(metricsFile.empty() ? outputDir + "/latency_timeseries.csv" : metricsFile),
      cacheModeEnabled_(cacheMode) {}

OpMetrics& MetricsCollector::GetOrCreateOp(const std::string &op) {
    // Fast path: after Start(), all ops are pre-created, no lock needed
    if (started_) {
        auto it = opsMap_.find(op);
        if (it != opsMap_.end()) return *it->second;
    }
    std::lock_guard<std::mutex> lock(opsMutex_);
    auto it = opsMap_.find(op);
    if (it != opsMap_.end()) return *it->second;
    auto m = std::make_unique<OpMetrics>();
    m->opName = op;
    auto &ref = *m;
    opsMap_[op] = &ref;
    ops_.push_back(std::move(m));
    return ref;
}

void MetricsCollector::Record(const std::string &op, double latencyMs, bool success, uint64_t bytes) {
    auto &m = GetOrCreateOp(op);
    m.totalCount++;
    if (success) m.successCount++;
    else m.failCount++;
    m.totalBytes += bytes;

    {
        std::lock_guard<std::mutex> lock(m.windowMutex);
        m.windowLatencies.push_back(latencyMs);
        m.windowBytes += bytes;
    }
    {
        std::lock_guard<std::mutex> lock(m.globalMutex);
        if (m.globalRing.empty()) return;  // not yet initialized
        size_t cap = m.globalRing.size();
        m.globalRing[m.globalHead] = latencyMs;
        m.globalHead = (m.globalHead + 1) % cap;
        if (m.globalCount < cap) m.globalCount++;
    }
}

void MetricsCollector::RecordVerifyFail() {
    verifyFailCount_++;
}

void MetricsCollector::Start() {
    // Pre-create all pipeline op types and pre-allocate ring buffers
    // Reserve to prevent reallocation (which would invalidate opsMap_ pointers)
    ops_.reserve(GetAllOpNames(cacheModeEnabled_).size());
    for (auto *name : GetAllOpNames(cacheModeEnabled_)) {
        auto &m = GetOrCreateOp(name);
        m.globalRing.resize(100000);
    }

    startTime_ = std::chrono::steady_clock::now();
    running_ = true;

    // Truncate and write CSV header (fresh file for each run)
    {
        std::ofstream f(metricsFile_, std::ios::trunc);
        if (f.is_open()) {
            f << "timestamp,op,count,avg_ms,p90_ms,p99_ms,p99.9_ms,p99.99_ms,min_ms,max_ms,qps,throughput_MB_s\n";
        }
    }

    flushThread_ = std::thread([this]() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs_));
            if (running_) FlushWindow();
        }
    });

    started_ = true;
}

void MetricsCollector::Stop() {
    running_ = false;
    if (flushThread_.joinable()) flushThread_.join();
    FlushWindow();
    WriteSummary();
}

static double Percentile(std::vector<double> &sorted, double p) {
    if (sorted.empty()) return 0.0;
    size_t idx = static_cast<size_t>(std::ceil(p / 100.0 * sorted.size())) - 1;
    idx = std::min(idx, sorted.size() - 1);
    return sorted[idx];
}

void MetricsCollector::FlushWindow() {
    auto now = std::chrono::system_clock::now();
    auto nowT = std::chrono::system_clock::to_time_t(now);
    std::tm tmBuf;
    std::stringstream ts;
    ts << std::put_time(localtime_r(&nowT, &tmBuf), "%Y-%m-%d %H:%M:%S");

    std::ofstream f(metricsFile_, std::ios::app);
    if (!f.is_open()) {
        SLOG_ERROR("Cannot open metrics file: " << metricsFile_);
        return;
    }

    double intervalSec = intervalMs_ / 1000.0;

    for (auto &m : ops_) {
        std::vector<double> latencies;
        uint64_t bytes = 0;
        {
            std::lock_guard<std::mutex> lock(m->windowMutex);
            latencies.swap(m->windowLatencies);
            bytes = m->windowBytes;
            m->windowBytes = 0;
        }
        if (latencies.empty()) continue;

        std::sort(latencies.begin(), latencies.end());
        double sum = 0;
        for (auto l : latencies) sum += l;
        double avg = sum / latencies.size();
        double p90 = Percentile(latencies, 90.0);
        double p99 = Percentile(latencies, 99.0);
        double p999 = Percentile(latencies, 99.9);
        double p9999 = Percentile(latencies, 99.99);

        double minV = latencies.front();
        double maxV = latencies.back();
        double qps = latencies.size() / intervalSec;
        double throughputMB = bytes / (1024.0 * 1024.0) / intervalSec;

        f << ts.str() << "," << m->opName << "," << latencies.size()
          << std::fixed << std::setprecision(3)
          << "," << avg << "," << p90 << "," << p99 << "," << p999 << "," << p9999
          << "," << minV << "," << maxV << "," << qps
          << "," << throughputMB << "\n";
    }

    // Cache mode: append hit_rate summary line
    if (cacheModeEnabled_) {
        uint64_t hits = cacheHitCount_.load();
        uint64_t misses = cacheMissCount_.load();
        uint64_t total = hits + misses;
        double hitRate = total > 0 ? static_cast<double>(hits) / total : 0.0;
        f << ts.str() << ",cache_summary," << total
          << std::fixed << std::setprecision(4)
          << ",0,0,0,0,0,0,0,0,0," << hitRate << "\n";
    }

    f.flush();
}

void MetricsCollector::WriteSummary() {
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - startTime_).count();

    std::string filename = outputDir_ + "/run_summary.txt";
    std::ofstream f(filename);
    if (!f.is_open()) {
        SLOG_ERROR("Cannot open summary file: " << filename);
        return;
    }

    f << "=== KVTest Summary ===\n";
    f << "Instance ID: " << instanceId_ << "\n";
    f << "Uptime: " << uptime << " seconds\n\n";

    for (auto &m : ops_) {
        std::vector<double> latencies;
        {
            std::lock_guard<std::mutex> lock(m->globalMutex);
            if (m->globalCount < m->globalRing.size()) {
                latencies.assign(m->globalRing.begin(),
                                m->globalRing.begin() + m->globalCount);
            } else {
                latencies.assign(m->globalRing.begin() + m->globalHead,
                                m->globalRing.end());
                latencies.insert(latencies.end(),
                                m->globalRing.begin(),
                                m->globalRing.begin() + m->globalHead);
            }
        }

        f << "--- " << m->opName << " ---\n";
        f << "Total: " << m->totalCount << ", Success: " << m->successCount
          << ", Fail: " << m->failCount << "\n";

        if (!latencies.empty()) {
            std::sort(latencies.begin(), latencies.end());
            double sum = 0;
            for (auto l : latencies) sum += l;
            double avg = sum / latencies.size();
            double p90 = Percentile(latencies, 90.0);
            double p99 = Percentile(latencies, 99.0);
            double p999 = Percentile(latencies, 99.9);
            double p9999 = Percentile(latencies, 99.99);
            double minV = latencies.front();
            double maxV = latencies.back();
            double qps = uptime > 0 ? (double)m->totalCount.load() / uptime : 0;
            uint64_t bytes = m->totalBytes.load();
            double throughputMB = uptime > 0 ? bytes / (1024.0 * 1024.0) / uptime : 0;

            f << std::fixed << std::setprecision(3);
            f << "Avg: " << avg << "ms, P90: " << p90 << "ms, P99: " << p99
              << "ms, P99.9: " << p999 << "ms, P99.99: " << p9999 << "ms, "
              << "Min: " << minV << "ms, Max: " << maxV << "ms\n";
            f << "QPS: " << qps << "\n";
            f << std::setprecision(2) << "Throughput: " << throughputMB << " MB/s\n";
        }
        f << "\n";
    }

    if (verifyFailCount_ > 0) {
        f << "Verify Fail: " << verifyFailCount_ << "\n";
    }

    if (cacheModeEnabled_) {
        f << "\n=== Cache Statistics ===\n";
        f << "Hits: " << cacheHitCount_.load() << "\n";
        f << "Misses: " << cacheMissCount_.load() << "\n";
        f << std::fixed << std::setprecision(4);
        f << "Hit Rate: " << CacheHitRate() << "\n";
    }

    // Per-stage summary from CSV when QPS stages are configured
    if (!qpsStages_.empty() && stageDurationSec_ > 0) {
        f << "\n=== Per-Stage Statistics ===\n";
        // Parse CSV: group rows by qps column
        // CSV format: timestamp,op,count,avg_ms,...,qps,...
        struct StageRow {
            std::string op;
            int count;
            double avg;
            double p90;
            double p99;
            double minV;
            double maxV;
            double qps;
        };
        // stage_idx -> op -> aggregated
        struct StageAgg {
            int totalRows = 0;
            uint64_t totalCount = 0;
            double weightedAvgSum = 0;  // count * avg
            double minLat = 1e9;
            double maxLat = 0;
            double p99Max = 0;
            double actualDurationSec = 0;  // totalRows * intervalMs / 1000
        };
        std::map<int, std::map<std::string, StageAgg>> stageAggs;

        std::ifstream csv(metricsFile_);
        if (csv.is_open()) {
            std::string line;
            int lineIdx = 0;
            int rowIdx = 0;
            while (std::getline(csv, line)) {
                lineIdx++;
                if (line.find("timestamp,op,") == 0) continue;
                if (line.find("cache_summary") != std::string::npos) continue;
                // Parse CSV fields
                std::vector<std::string> fields;
                std::stringstream ss(line);
                std::string field;
                while (std::getline(ss, field, ',')) fields.push_back(field);
                if (fields.size() < 11) continue;
                // fields: 0=ts, 1=op, 2=count, 3=avg, 4=p90, 5=p99, 6=p99.9, 7=p99.99, 8=min, 9=max, 10=qps
                try {
                    std::string op = fields[1];
                    int count = std::stoi(fields[2]);
                    double avg = std::stod(fields[3]);
                    double p99 = std::stod(fields[5]);
                    double minV = std::stod(fields[8]);
                    double maxV = std::stod(fields[9]);
                    double qps = std::stod(fields[10]);

                    // Determine stage index by matching qps to stages
                    int stageIdx = -1;
                    for (int i = 0; i < (int)qpsStages_.size(); i++) {
                        if (std::abs(qps - qpsStages_[i]) < 0.5) {
                            stageIdx = i;
                            break;
                        }
                    }
                    if (stageIdx < 0) continue;

                    auto &agg = stageAggs[stageIdx][op];
                    agg.totalRows++;
                    agg.totalCount += count;
                    agg.weightedAvgSum += count * avg;
                    agg.minLat = std::min(agg.minLat, minV);
                    agg.maxLat = std::max(agg.maxLat, maxV);
                    agg.p99Max = std::max(agg.p99Max, p99);
                    agg.actualDurationSec = agg.totalRows * intervalMs_ / 1000.0;
                } catch (...) { continue; }
            }
        }

        for (int si = 0; si < (int)qpsStages_.size(); si++) {
            f << "\n--- Stage " << si << ": " << qpsStages_[si] << " QPS"
              << " (" << stageDurationSec_ << "s) ---\n";
            auto it = stageAggs.find(si);
            if (it == stageAggs.end() || it->second.empty()) {
                f << "  (no data)\n";
                continue;
            }
            for (auto &[op, agg] : it->second) {
                double avg = agg.totalCount > 0
                    ? agg.weightedAvgSum / agg.totalCount : 0;
                double actualQps = agg.actualDurationSec > 0
                    ? (double)agg.totalCount / agg.actualDurationSec : 0;
                f << "  " << op << ": total=" << agg.totalCount
                  << std::fixed << std::setprecision(3)
                  << ", avg=" << avg << "ms"
                  << ", p99_max=" << agg.p99Max << "ms"
                  << ", min=" << agg.minLat << "ms"
                  << ", max=" << agg.maxLat << "ms"
                  << std::setprecision(1)
                  << ", actual_qps=" << actualQps << "\n";
            }
        }
    }

    SLOG_INFO("Summary written to " << filename);
}

std::string MetricsCollector::GetStatsJson() {
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - startTime_).count();

    json j;
    j["instance_id"] = instanceId_;
    j["uptime_seconds"] = uptime;

    for (auto &m : ops_) {
        j[m->opName + "_count"] = m->totalCount.load();
        j[m->opName + "_success"] = m->successCount.load();
        j[m->opName + "_fail"] = m->failCount.load();
    }
    j["verify_fail"] = verifyFailCount_.load();

    if (cacheModeEnabled_) {
        uint64_t hits = cacheHitCount_.load();
        uint64_t misses = cacheMissCount_.load();
        j["cache_hit_count"] = hits;
        j["cache_miss_count"] = misses;
        j["cache_hit_rate"] = CacheHitRate();
    }

    return j.dump(2);
}

std::vector<MetricsCollector::OpSnapshot> MetricsCollector::SnapshotCounts() {
    std::vector<OpSnapshot> result;
    for (auto &m : ops_) {
        result.push_back({m->opName, m->totalCount.load()});
    }
    return result;
}

double MetricsCollector::CacheHitRate() const {
    uint64_t hits = cacheHitCount_.load();
    uint64_t misses = cacheMissCount_.load();
    uint64_t total = hits + misses;
    return total > 0 ? static_cast<double>(hits) / total : 0.0;
}
