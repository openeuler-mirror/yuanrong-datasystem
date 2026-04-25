#include "metrics.h"
#include "pipeline.h"
#include "simple_log.h"
#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <thread>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

MetricsCollector::MetricsCollector(int instanceId, int intervalMs, const std::string &metricsFile)
    : instanceId_(instanceId), intervalMs_(intervalMs), metricsFile_(metricsFile) {}

OpMetrics& MetricsCollector::GetOrCreateOp(const std::string &op) {
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
        if (m.globalRing.empty()) {
            m.globalRing.resize(100000);  // 100K entries ~800KB
        }
        m.globalRing[m.globalHead] = latencyMs;
        m.globalHead = (m.globalHead + 1) % m.globalRing.size();
        if (m.globalCount < m.globalRing.size()) m.globalCount++;
    }
}

void MetricsCollector::RecordVerifyFail() {
    verifyFailCount_++;
}

void MetricsCollector::Start() {
    // Pre-create all pipeline op types to avoid race in GetOrCreateOp
    for (auto *name : GetAllOpNames()) {
        GetOrCreateOp(name);
    }

    startTime_ = std::chrono::steady_clock::now();
    running_ = true;

    std::ofstream f(metricsFile_, std::ios::app);
    if (f.is_open()) {
        f << "timestamp,op,count,avg_ms,p90_ms,p99_ms,min_ms,max_ms,qps,throughput_MB_s\n";
    }

    flushThread_ = std::thread([this]() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs_));
            if (running_) FlushWindow();
        }
    });
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
    std::stringstream ts;
    ts << std::put_time(std::localtime(&nowT), "%Y-%m-%d %H:%M:%S");

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
        double minV = latencies.front();
        double maxV = latencies.back();
        double qps = latencies.size() / intervalSec;
        double throughputMB = bytes / (1024.0 * 1024.0) / intervalSec;

        f << ts.str() << "," << m->opName << "," << latencies.size()
          << std::fixed << std::setprecision(3)
          << "," << avg << "," << p90 << "," << p99
          << "," << minV << "," << maxV << "," << qps
          << "," << throughputMB << "\n";
    }
    f.flush();
}

void MetricsCollector::WriteSummary() {
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - startTime_).count();

    std::string filename = "summary_" + std::to_string(instanceId_) + ".txt";
    std::ofstream f(filename);
    if (!f.is_open()) {
        SLOG_ERROR("Cannot open summary file: " << filename);
        return;
    }

    f << "=== KVClient Standalone Test Summary ===\n";
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
            double minV = latencies.front();
            double maxV = latencies.back();
            double qps = uptime > 0 ? (double)latencies.size() / uptime : 0;
            uint64_t bytes = m->totalBytes.load();
            double throughputMB = uptime > 0 ? bytes / (1024.0 * 1024.0) / uptime : 0;

            f << std::fixed << std::setprecision(3);
            f << "Avg: " << avg << "ms, P90: " << p90 << "ms, P99: " << p99
              << "ms, Min: " << minV << "ms, Max: " << maxV << "ms\n";
            f << "QPS: " << qps << "\n";
            f << std::setprecision(2) << "Throughput: " << throughputMB << " MB/s\n";
        }
        f << "\n";
    }

    if (verifyFailCount_ > 0) {
        f << "Verify Fail: " << verifyFailCount_ << "\n";
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
    return j.dump(2);
}

std::vector<MetricsCollector::OpSnapshot> MetricsCollector::SnapshotCounts() {
    std::vector<OpSnapshot> result;
    for (auto &m : ops_) {
        result.push_back({m->opName, m->totalCount.load()});
    }
    return result;
}
