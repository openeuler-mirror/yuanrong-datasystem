// Generates comprehensive metrics output for visual inspection.
// Build: g++ -std=c++17 -I src -I tests/cxx/stubs -o gen_metrics_output gen_metrics_output.cpp src/metrics/metrics.cpp src/pipeline/pipeline.cpp -lpthread
// Run:   ./gen_metrics_output <output_dir>

#include "metrics/metrics.h"
#include <cstdio>
#include <filesystem>
#include <thread>

static std::string sub(const std::string &base, const std::string &name) {
    auto p = base + "/" + name;
    std::filesystem::create_directories(p);
    return p;
}

int main(int argc, char **argv) {
    std::string dir = argc > 1 ? argv[1] : "/tmp/kvtest_inspect";
    std::filesystem::create_directories(dir);
    printf("Output directory: %s\n\n", dir.c_str());

    // --- Scenario 1: Basic single-op ---
    {
        auto d = sub(dir, "01_basic_single_op");
        MetricsCollector m(0, 100, d);
        m.Start();
        for (int i = 0; i < 200; i++) {
            m.Record("setStringView", static_cast<double>(i % 50 + 1), true, 1024 * (i % 10 + 1));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        m.Stop();
        printf("[1] Basic setStringView: %s\n", d.c_str());
    }

    // --- Scenario 2: Multiple ops with failures ---
    {
        auto d = sub(dir, "02_multi_op_with_failures");
        MetricsCollector m(1, 100, d);
        m.Start();
        for (int i = 0; i < 100; i++) {
            m.Record("getBuffer", static_cast<double>(i % 30 + 1), i % 10 != 0);
        }
        for (int i = 0; i < 50; i++) {
            m.Record("exist", static_cast<double>(i % 5 + 1), true);
        }
        for (int i = 0; i < 80; i++) {
            m.Record("mCreate", static_cast<double>(i % 20 + 1), i % 5 != 0, 4096);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        m.Stop();
        printf("[2] Multiple ops with failures: %s\n", d.c_str());
    }

    // --- Scenario 3: Cache mode ---
    {
        auto d = sub(dir, "03_cache_mode");
        MetricsCollector m(2, 100, d, true);
        m.Start();
        for (int i = 0; i < 75; i++) m.RecordCacheHit();
        for (int i = 0; i < 25; i++) m.RecordCacheMiss();
        for (int i = 0; i < 50; i++) {
            m.Record("cacheGetOrCreate", static_cast<double>(i % 10 + 1), true);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        m.Stop();
        printf("[3] Cache mode (75%% hit rate): %s\n", d.c_str());
    }

    // --- Scenario 4: QPS stages ---
    {
        auto d = sub(dir, "04_qps_stages");
        MetricsCollector m(3, 100, d);
        m.SetQpsStages({100, 200, 500}, 60);
        m.Start();
        for (int i = 0; i < 500; i++) {
            m.Record("setStringView", static_cast<double>(i % 100 + 1), true, 8192);
        }
        for (int i = 0; i < 200; i++) {
            m.Record("mSet", static_cast<double>(i % 50 + 1), true, 4096);
        }
        m.RecordVerifyFail();
        m.RecordVerifyFail();
        m.RecordVerifyFail();
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        m.Stop();
        printf("[4] QPS stages with verify fails: %s\n", d.c_str());
    }

    // --- Scenario 5: Large dataset (ring buffer stress) ---
    {
        auto d = sub(dir, "05_ring_buffer_stress");
        MetricsCollector m(4, 200, d);
        m.Start();
        for (int i = 0; i < 50000; i++) {
            m.Record("setStringView", static_cast<double>(i % 200 + 1), true, 1024);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        m.Stop();
        printf("[5] 50k records ring buffer stress: %s\n", d.c_str());
    }

    // --- Scenario 6: JSON stats example ---
    {
        auto d = sub(dir, "06_json_stats");
        MetricsCollector m(99, 1000, d);
        m.Start();
        for (int i = 0; i < 10; i++) m.Record("setStringView", static_cast<double>(i + 1), true);
        auto json = m.GetStatsJson();
        m.Stop();
        // Write JSON to file
        FILE *f = fopen((d + "/stats.json").c_str(), "w");
        if (f) { fprintf(f, "%s\n", json.c_str()); fclose(f); }
        printf("[6] JSON stats: %s\n", d.c_str());
    }

    // List all output files
    printf("\n=== Generated files ===\n");
    for (auto &entry : std::filesystem::recursive_directory_iterator(dir)) {
        if (entry.is_regular_file()) {
            auto rel = std::filesystem::relative(entry.path(), dir);
            printf("  %s (%lld bytes)\n", rel.c_str(),
                   static_cast<long long>(entry.file_size()));
        }
    }
    printf("\nDone. Inspect files in: %s\n", dir.c_str());
    return 0;
}
