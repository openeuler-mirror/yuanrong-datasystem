#include "test_harness.h"
#include "pipeline/pipeline.h"
#include "metrics/metrics.h"
#include <filesystem>
#include <thread>
#include <unistd.h>

extern "C" {
void FakeDeviceCount(int count);
void FakeFailCopyAfter(int count);
void FakeFailEvent();
int FakeCopyCount();
int FakeStreamWaits();
size_t FakeAllocations();
size_t FakePending();
}

using namespace datasystem;

static void Run(const char *library, const std::string &mode) {
    Config cfg;
    cfg.instanceId = 7;
    cfg.dataSizes = {1024};
    cfg.batchKeysCount = 2;
    cfg.numThreads = 1;
    cfg.numTotalThreads = 3;
    cfg.verifyLevel = mode == "verify_off" ? "off" : (mode == "verify_sample" ? "sample" : "full");
    cfg.cuda.runtimeLibrary = library;
    cfg.cuda.transferEnabled = mode != "cpu" && mode != "pin_only";
    cfg.cuda.pin = mode != "pin_off" && mode != "warmup_pin_off";
    if (mode == "load_fail") cfg.cuda.runtimeLibrary = std::string(library) + "/missing-libcudart.so";
    if (mode == "cpu" || mode == "required_no_gpu") FakeDeviceCount(0);
    Status rc = kvtest::InitCudaWorkload(cfg);
    if (mode == "load_fail") {
        ASSERT_FALSE(rc.IsOk());
        const auto message = rc.GetMsg();
        ASSERT_TRUE(message.find("Cannot load CUDA runtime library") != std::string::npos);
        ASSERT_TRUE(message.find("dlopen(" + cfg.cuda.runtimeLibrary + "): ") != std::string::npos);
        ASSERT_TRUE(message.find("LD_LIBRARY_PATH=") != std::string::npos);
        ASSERT_TRUE(message.find("ldconfig") != std::string::npos);
        ASSERT_EQ(KVClient::registrations, 0);
        ASSERT_EQ(FakeAllocations(), 0u);
        return;
    }
    if (mode == "required_no_gpu") { ASSERT_FALSE(rc.IsOk()); return; }
    ASSERT_TRUE(rc.IsOk());
    if (mode == "cpu") { ASSERT_EQ(KVClient::registrations, 0); return; }
    ASSERT_EQ(KVClient::registrations, cfg.cuda.pin ? 1 : 0);
    if (mode == "pin_only") {
        int callbackRc = -1;
        std::thread thread([&callbackRc] { callbackRc = KVClient::callbacks.hostRegister(nullptr, 1, 0); });
        thread.join();
        ASSERT_EQ(callbackRc, 0);
        ASSERT_EQ(FakeAllocations(), 0u);
        return;
    }
    {
        kvtest::CudaLaneGuard first(true), second(true), third(true), exhausted(true);
        ASSERT_TRUE(first.Get() && second.Get() && third.Get());
        ASSERT_TRUE(first.Get()->source != second.Get()->source);
        ASSERT_TRUE(first.Get()->destination != second.Get()->destination);
        ASSERT_TRUE(first.Get()->event != second.Get()->event);
        ASSERT_TRUE(exhausted.Get() == nullptr);
    }
    auto client = std::make_shared<KVClient>();
    if (mode == "warmup" || mode == "warmup_fail" || mode == "warmup_pin_off") {
        const int before = FakeCopyCount();
        if (mode == "warmup_fail") FakeFailCopyAfter(1);
        const auto warmup = WarmupCudaPipeline(cfg, client);
        ASSERT_EQ(warmup.IsOk(), mode != "warmup_fail");
        ASSERT_EQ(FakePending(), 0u);
        ASSERT_EQ(KVClient::deletedKeys, 1);
        ASSERT_EQ(FakeCopyCount() - before, mode == "warmup_fail" ? 1 : 3);
        {
            kvtest::CudaLaneGuard first(true), second(true), third(true);
            ASSERT_TRUE(first.Get() && second.Get() && third.Get());
        }
        kvtest::CloseCudaWorkload();
        ASSERT_EQ(FakeAllocations(), 0u);
        return;
    }
    const auto verify = BuildVerifyConfig(cfg.verifyLevel, 16, 128, true);
    if (mode == "concurrent") {
        std::atomic<int> failures{0};
        std::vector<std::thread> threads;
        for (int i = 0; i < cfg.numTotalThreads; ++i) threads.emplace_back([&client, &failures, &cfg, &verify]() {
            kvtest::CudaLaneGuard lane(true);
            if (!lane.Get()) { ++failures; return; }
            std::vector<char> a(1024), b(1024);
            std::vector<void *> hosts{a.data(), b.data()};
            kvtest::CudaTiming timing;
            for (int iteration = 0; iteration < 10; ++iteration) {
                bool matches = true;
                if (!kvtest::CopyCudaBuffers(*client, *lane.Get(), hosts, 1024, false, timing).IsOk()
                    || !kvtest::CopyCudaBuffers(*client, *lane.Get(), hosts, 1024, true, timing).IsOk()
                    || !kvtest::VerifyCudaBuffers(*lane.Get(), 2, 1024, cfg.instanceId, verify, matches).IsOk()
                    || !matches) ++failures;
            }
        });
        for (auto &thread : threads) thread.join();
        ASSERT_EQ(failures.load(), 0);
        ASSERT_EQ(FakePending(), 0u);
    } else if (mode == "partial_fail" || mode == "fragment_fail" || mode == "event_fail"
               || mode == "capacity" || mode == "verify_corrupt") {
        kvtest::CudaLaneGuard lane(true);
        std::vector<char> a(1024), b(1024);
        std::vector<void *> hosts{a.data(), b.data()};
        if (mode == "partial_fail" || mode == "fragment_fail") FakeFailCopyAfter(2);
        if (mode == "fragment_fail") KVClient::splitCopies = true;
        if (mode == "event_fail") FakeFailEvent();
        kvtest::CudaTiming timing;
        rc = kvtest::CopyCudaBuffers(*client, *lane.Get(), hosts, mode == "capacity" ? 1025 : 1024, false, timing);
        ASSERT_EQ(FakePending(), 0u);
        if (mode == "verify_corrupt") {
            ASSERT_TRUE(rc.IsOk());
            a[10] ^= 1;
            ASSERT_TRUE(kvtest::CopyCudaBuffers(*client, *lane.Get(), hosts, 1024, true, timing).IsOk());
            bool matches = true;
            ASSERT_TRUE(kvtest::VerifyCudaBuffers(*lane.Get(), 2, 1024, cfg.instanceId, verify, matches).IsOk());
            ASSERT_FALSE(matches);
        } else ASSERT_FALSE(rc.IsOk());
        if (mode == "event_fail") ASSERT_TRUE(FakeStreamWaits() > 0);
        if (mode == "partial_fail" || mode == "fragment_fail") ASSERT_EQ(a[0], PatternByteAt(0, cfg.instanceId));
    } else {
        const auto dir = "/tmp/kvtest_cuda_" + std::to_string(getpid());
        std::filesystem::create_directories(dir);
        MetricsCollector metrics(7, 10, dir);
        metrics.EnableCudaMetrics();
        metrics.Start();
        PipelineContext ctx;
        ctx.client = client; ctx.size = 1024; ctx.senderId = cfg.instanceId; ctx.verifyCfg = verify;
        ctx.key = "gpu_single"; ctx.batchKeys = {"gpu_a", "gpu_b"};
        std::atomic<uint64_t> failures{0};
        const int copiesBefore = FakeCopyCount();
        bool allOk = true;
        for (const auto &names : {std::vector<std::string>{"createBuffer", "d2h", "setBuffer", "getBuffer", "h2d"},
                                  std::vector<std::string>{"mCreate", "mD2h", "mSet", "mGet", "mH2d"}}) {
            std::vector<std::pair<std::string, OpFunc>> ops;
            for (const auto &name : names) ops.emplace_back(name, GetOpFunc(name));
            allOk = ExecutePipeline(ops, ctx, metrics, failures, cfg.instanceId) && allOk;
        }
        metrics.Stop();
        ASSERT_TRUE(allOk);
        ASSERT_EQ(failures.load(), 0u);
        ASSERT_EQ(FakePending(), 0u);
        ASSERT_EQ(KVClient::dsCopies.load(), cfg.cuda.pin ? 6 : 0);
        ASSERT_EQ(FakeCopyCount() - copiesBefore, mode == "verify_off" ? 6 : 9);
        bool counted = false;
        for (const auto &count : metrics.SnapshotCounts()) if (count.name == "mH2d_wait" && count.count == 1) counted = true;
        ASSERT_TRUE(counted);
        std::filesystem::remove_all(dir);
    }
    kvtest::CloseCudaWorkload();
    ASSERT_EQ(FakeAllocations(), 0u);
    ASSERT_TRUE(FakeCopyCount() > 0);
}

int main(int argc, char **argv) {
    if (argc != 3) return 2;
    try { Run(argv[1], argv[2]); }
    catch (const std::exception &e) { std::fprintf(stderr, "%s\n", e.what()); return 1; }
    return 0;
}
