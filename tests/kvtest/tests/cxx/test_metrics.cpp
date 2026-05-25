#include "test_harness.h"
#include "metrics/metrics.h"
#include "pipeline/pipeline.h"
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <unistd.h>

static std::string MakeTempDir() {
    static int idx = 0;
    std::string dir = "/tmp/kvtest_metrics_" + std::to_string(getpid()) + "_" + std::to_string(idx++);
    std::filesystem::create_directories(dir);
    return dir;
}

static std::vector<std::string> ReadFileLines(const std::string &path) {
    std::vector<std::string> lines;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) lines.push_back(line);
    return lines;
}

TEST(RecordAndCount) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir);
    m.Start();
    m.Record("setStringView", 1.0, true, 1024);
    auto snap = m.SnapshotCounts();
    ASSERT_TRUE(!snap.empty());
    bool found = false;
    for (auto &s : snap) {
        if (s.name == "setStringView") { found = true; ASSERT_EQ(s.count, 1ULL); }
    }
    ASSERT_TRUE(found);
    m.Stop();
    std::filesystem::remove_all(dir);
}

TEST(RecordMultipleOps) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir);
    m.Start();
    m.Record("setStringView", 1.0, true);
    m.Record("getBuffer", 2.0, true);
    auto snap = m.SnapshotCounts();
    ASSERT_TRUE(snap.size() >= 2);
    m.Stop();
    std::filesystem::remove_all(dir);
}

TEST(RecordSuccessFail) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir);
    m.Start();
    m.Record("op", 1.0, true);
    m.Record("op", 2.0, false);
    auto json = m.GetStatsJson();
    ASSERT_TRUE(json.find("\"op_count\": 2") != std::string::npos);
    ASSERT_TRUE(json.find("\"op_success\": 1") != std::string::npos);
    ASSERT_TRUE(json.find("\"op_fail\": 1") != std::string::npos);
    m.Stop();
    std::filesystem::remove_all(dir);
}

TEST(CacheHitRate) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir, true);
    m.Start();
    m.RecordCacheHit();
    m.RecordCacheHit();
    m.RecordCacheHit();
    m.RecordCacheMiss();
    ASSERT_NEAR(m.CacheHitRate(), 0.75, 0.001);
    m.Stop();
    std::filesystem::remove_all(dir);
}

TEST(CacheHitRate_NoData) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir, true);
    m.Start();
    ASSERT_NEAR(m.CacheHitRate(), 0.0, 0.001);
    m.Stop();
    std::filesystem::remove_all(dir);
}

TEST(VerifyFailCounter) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir);
    m.Start();
    for (int i = 0; i < 5; i++) m.RecordVerifyFail();
    ASSERT_EQ(m.VerifyFailCounter(), 5ULL);
    m.Stop();
    std::filesystem::remove_all(dir);
}

TEST(SnapshotCounts_Accurate) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir);
    m.Start();
    for (int i = 0; i < 100; i++) m.Record("setStringView", 1.0, true);
    auto snap = m.SnapshotCounts();
    uint64_t total = 0;
    for (auto &s : snap) {
        if (s.name == "setStringView") total = s.count;
    }
    ASSERT_EQ(total, 100ULL);
    m.Stop();
    std::filesystem::remove_all(dir);
}

TEST(FlushWindow_CsvFormat) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 100, dir);
    m.Start();
    m.Record("setStringView", 1.0, true, 1024);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    m.Stop();
    auto lines = ReadFileLines(dir + "/latency_timeseries.csv");
    ASSERT_TRUE(lines.size() >= 2);
    // First line is header
    ASSERT_TRUE(lines[0].find("timestamp,op,") == 0);
    // Data line has 11 commas (12 fields)
    // Find the setStringView data line (skip cache_summary)
    bool foundData = false;
    for (size_t i = 1; i < lines.size(); i++) {
        if (lines[i].find("setStringView") != std::string::npos) {
            int commas = 0;
            for (char c : lines[i]) if (c == ',') commas++;
            ASSERT_EQ(commas, 11);
            foundData = true;
            break;
        }
    }
    ASSERT_TRUE(foundData);
    std::filesystem::remove_all(dir);
}

TEST(FlushWindow_Percentile) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 100, dir);
    m.Start();
    for (int i = 1; i <= 100; i++) {
        m.Record("setStringView", static_cast<double>(i), true);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    m.Stop();
    auto lines = ReadFileLines(dir + "/latency_timeseries.csv");
    ASSERT_TRUE(lines.size() >= 2);
    // Parse P99 from second data line
    std::stringstream ss(lines[1]);
    std::string field;
    std::vector<std::string> fields;
    while (std::getline(ss, field, ',')) fields.push_back(field);
    // fields[5] = p99_ms
    double p99 = std::stod(fields[5]);
    ASSERT_TRUE(p99 >= 98.0 && p99 <= 100.0);
    std::filesystem::remove_all(dir);
}

TEST(WriteSummary_ContainsOps) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 100, dir);
    m.Start();
    m.Record("setStringView", 1.0, true);
    m.Stop();
    auto lines = ReadFileLines(dir + "/run_summary.txt");
    bool found = false;
    for (auto &l : lines) {
        if (l.find("--- setStringView ---") != std::string::npos) found = true;
    }
    ASSERT_TRUE(found);
    std::filesystem::remove_all(dir);
}

TEST(WriteSummary_Percentile) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 100, dir);
    m.Start();
    for (int i = 1; i <= 100; i++) {
        m.Record("setStringView", static_cast<double>(i), true);
    }
    m.Stop();
    auto lines = ReadFileLines(dir + "/run_summary.txt");
    bool hasAvg = false, hasP99 = false;
    for (auto &l : lines) {
        if (l.find("Avg:") != std::string::npos) hasAvg = true;
        if (l.find("P99:") != std::string::npos) hasP99 = true;
    }
    ASSERT_TRUE(hasAvg);
    ASSERT_TRUE(hasP99);
    std::filesystem::remove_all(dir);
}

TEST(WriteSummary_CacheStats) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 100, dir, true);
    m.Start();
    m.RecordCacheHit();
    m.RecordCacheMiss();
    m.Stop();
    auto lines = ReadFileLines(dir + "/run_summary.txt");
    bool found = false;
    for (auto &l : lines) {
        if (l.find("Cache Statistics") != std::string::npos) found = true;
    }
    ASSERT_TRUE(found);
    std::filesystem::remove_all(dir);
}

TEST(GetStatsJson_Valid) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir);
    m.Start();
    m.Record("setStringView", 1.0, true);
    auto json = m.GetStatsJson();
    ASSERT_TRUE(json.find("instance_id") != std::string::npos);
    ASSERT_TRUE(json.find("uptime_seconds") != std::string::npos);
    ASSERT_TRUE(json.find("setStringView_count") != std::string::npos);
    m.Stop();
    std::filesystem::remove_all(dir);
}

TEST(RingBuffer_Overwrite) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 1000, dir);
    m.Start();
    // Record more than the 100k ring buffer size
    for (int i = 0; i < 200000; i++) {
        m.Record("setStringView", static_cast<double>(i), true);
    }
    m.Stop();
    // Summary should exist and show 200k total
    auto lines = ReadFileLines(dir + "/run_summary.txt");
    bool found200k = false;
    for (auto &l : lines) {
        if (l.find("Total: 200000") != std::string::npos) found200k = true;
    }
    ASSERT_TRUE(found200k);
    std::filesystem::remove_all(dir);
}

TEST(FlushWindow_QpsStages) {
    auto dir = MakeTempDir();
    MetricsCollector m(0, 100, dir);
    m.SetQpsStages({100, 200}, 60);
    m.Start();
    m.Record("setStringView", 1.0, true);
    m.Stop();
    auto lines = ReadFileLines(dir + "/run_summary.txt");
    bool found = false;
    for (auto &l : lines) {
        if (l.find("Per-Stage Statistics") != std::string::npos) found = true;
    }
    ASSERT_TRUE(found);
    std::filesystem::remove_all(dir);
}

TEST(BenchmarkMetrics_RecordAndFlush) {
    std::string dir = "/tmp/kvtest_bench_metrics_" + std::to_string(getpid());
    std::filesystem::create_directories(dir);
    {
        BenchmarkMetrics bm(dir);
        bm.RecordPhase(0, "set", 100, 3.2, 2.8, 4.1, 5.8, 12.1, 320.0, 31250.0);
        bm.RecordPhase(0, "get", 100, 1.1, 0.9, 1.5, 2.3, 5.2, 110.0, 90909.0);
        bm.RecordPhase(1, "set", 100, 3.0, 2.6, 3.8, 5.5, 11.0, 300.0, 33333.0);
        bm.Flush();
        bm.RecordPhase(1, "get", 100, 1.0, 0.8, 1.4, 2.1, 4.8, 100.0, 100000.0);
        bm.Flush();
    }
    std::string csvPath = dir + "/benchmark_phases.csv";
    std::ifstream f(csvPath);
    ASSERT_TRUE(f.is_open());
    std::string line;
    // Header
    std::getline(f, line);
    ASSERT_TRUE(line.find("round,phase,ops") == 0);
    // First record
    std::getline(f, line);
    ASSERT_TRUE(line.find("0,set,100,") == 0);
    // Second record
    std::getline(f, line);
    ASSERT_TRUE(line.find("0,get,100,") == 0);
    // Third record (round 1 set, flushed with round 1 get)
    std::getline(f, line);
    ASSERT_TRUE(line.find("1,set,100,") == 0);
    // Fourth record
    std::getline(f, line);
    ASSERT_TRUE(line.find("1,get,100,") == 0);
    // No more lines
    ASSERT_TRUE(!std::getline(f, line));
    std::filesystem::remove_all(dir);
}
