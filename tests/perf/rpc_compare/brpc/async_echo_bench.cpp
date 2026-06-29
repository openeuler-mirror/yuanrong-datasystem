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

/**
 * E2E async RPC benchmark using the PRODUCTION BrpcAsyncContext.
 *
 * Starts a local HelloService, then spawns client threads that replicate
 * the exact AsyncWrite/AsyncRead pattern:
 *   AllocateTag → CallMethod(..., MakeDone) → TakeCall → cv.wait
 *
 * Build twice (old single-mutex vs new S32 sharded) to measure real E2E impact.
 * Metrics: QPS, avg, P50, P99, P999, Pmax (per-operation latency)
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <brpc/socket_map.h>
#include <gflags/gflags.h>

#include "datasystem/common/rpc/brpc/hello.pb.h"
#include "datasystem/common/rpc/brpc_async_context.h"

using Clock = std::chrono::steady_clock;
using datasystem::rpc::brpc::HelloRequest;
using datasystem::rpc::brpc::HelloResponse;
using datasystem::rpc::brpc::HelloService;

DEFINE_int32(server_port, 18600, "TCP port for the benchmark server");
DEFINE_int32(warmup_sec, 5, "Warmup duration in seconds");
DEFINE_int32(measure_sec, 30, "Measurement duration in seconds per thread count");
DEFINE_int32(timeout_ms, 3000, "RPC timeout in milliseconds");
DEFINE_string(threads, "1,8,32,64,128", "Comma-separated thread counts");

// ============================================================================
// Echo service
// ============================================================================
class EchoServiceImpl : public HelloService {
public:
    void SayHello(google::protobuf::RpcController *,
                  const HelloRequest *request,
                  HelloResponse *response,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_message(request->name());
    }
};

// ============================================================================
// Benchmark result
// ============================================================================
struct BenchResult {
    int threads;
    double durationSec;
    uint64_t totalOps;
    double qps;
    double avgUs;
    double p50Us;
    double p99Us;
    double p999Us;
    double maxUs;
    double minUs;
    uint64_t errors;
};

static BenchResult RunAsyncBench(datasystem::BrpcAsyncContext &ctx, int numThreads,
                                  int warmupSec, int measureSec, int serverPort, int timeoutMs)
{
    const google::protobuf::MethodDescriptor *method = HelloService::descriptor()->method(0);

    std::vector<std::thread> workers;
    std::mutex histMutex;
    std::vector<double> allLatencyUs;
    std::atomic<uint64_t> completedOps{0};
    std::atomic<uint64_t> errorCount{0};
    std::atomic<bool> startFlag{false};
    std::atomic<bool> stopFlag{false};

    auto workerFunc = [&](int /*tid*/) {
        brpc::ChannelOptions opts;
        opts.timeout_ms = timeoutMs;
        opts.max_retry = 0;
        opts.connection_type = "single";

        brpc::Channel channel;
        char addr[64];
        std::snprintf(addr, sizeof(addr), "127.0.0.1:%d", serverPort);
        if (channel.Init(addr, &opts) != 0) { errorCount.fetch_add(1); return; }

        // Wait for TCP connection
        {
            butil::EndPoint ep;
            butil::str2endpoint("127.0.0.1", serverPort, &ep);
            brpc::SocketMapKey key(ep);
            brpc::SocketId sid;
            for (int i = 0; i < 60; ++i) {
                if (brpc::SocketMapFind(key, &sid) == 0) {
                    brpc::SocketUniquePtr ptr;
                    if (brpc::Socket::Address(sid, &ptr) == 0 && ptr->IsAvailable()) break;
                }
                usleep(50000);
            }
        }

        std::vector<double> localSamples;
        localSamples.reserve(100000);

        while (!startFlag.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        while (!stopFlag.load(std::memory_order_acquire)) {
            auto t0 = Clock::now();

            // === AsyncWrite: AllocateTag + CallMethod ===
            int64_t tagId = ctx.AllocateTag();
            auto call = ctx.GetCall(tagId);
            if (!call) { errorCount.fetch_add(1); continue; }

            auto req = std::make_unique<HelloRequest>();
            req->set_name("bench");
            auto rsp = std::make_unique<HelloResponse>();
            call->cntl.set_timeout_ms(timeoutMs);

            // Production order: allocate → move into call → MakeDone → CallMethod
            // (matches brpc_stub_generator.cpp AsyncWrite generated code)
            call->request = std::move(req);
            call->response = std::move(rsp);
            auto *done = ctx.MakeDone(call);
            channel.CallMethod(method, &call->cntl, call->request.get(), call->response.get(), done);

            // === AsyncRead: TakeCall + wait ===
            auto taken = ctx.TakeCall(tagId);
            if (!taken) { errorCount.fetch_add(1); continue; }

            {
                std::unique_lock<std::mutex> lock(taken->mtx);
                taken->cv.wait(lock, [&taken] { return taken->completed; });
            }

            if (taken->failed) {
                errorCount.fetch_add(1);
            }

            // Release protos (unique_ptr handles cleanup)
            taken->response.reset();
            taken->request.reset();

            auto t1 = Clock::now();
            double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
            localSamples.push_back(us);
            completedOps.fetch_add(1);

            if (localSamples.size() >= 5000) {
                std::lock_guard<std::mutex> lk(histMutex);
                allLatencyUs.insert(allLatencyUs.end(), localSamples.begin(), localSamples.end());
                localSamples.clear();
            }
        }
        std::lock_guard<std::mutex> lk(histMutex);
        allLatencyUs.insert(allLatencyUs.end(), localSamples.begin(), localSamples.end());
    };

    // --- Warmup ---
    {
        std::atomic<bool> warmStart{false};
        std::vector<std::thread> warmThreads;
        for (int i = 0; i < std::min(numThreads, 4); ++i) {
            warmThreads.emplace_back([&]() {
                brpc::Channel ch;
                brpc::ChannelOptions o; o.timeout_ms = timeoutMs; o.max_retry = 0; o.connection_type = "single";
                char a[64]; std::snprintf(a, sizeof(a), "127.0.0.1:%d", serverPort);
                if (ch.Init(a, &o) != 0) return;
                while (!warmStart.load(std::memory_order_acquire)) std::this_thread::yield();
                HelloRequest req; req.set_name("warmup");
                HelloResponse rsp;
                brpc::Controller cntl;
                for (int j = 0; j < 500; ++j) { cntl.Reset(); ch.CallMethod(method, &cntl, &req, &rsp, nullptr); }
            });
        }
        warmStart.store(true);
        std::this_thread::sleep_for(std::chrono::seconds(warmupSec));
        for (auto &t : warmThreads) t.join();
    }

    // --- Measurement ---
    for (int i = 0; i < numThreads; ++i) workers.emplace_back(workerFunc, i);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto wallStart = Clock::now();
    startFlag.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::seconds(measureSec));
    stopFlag.store(true, std::memory_order_release);
    auto wallEnd = Clock::now();

    for (auto &t : workers) t.join();

    // --- Compute metrics ---
    BenchResult r;
    r.threads = numThreads;
    r.durationSec = std::chrono::duration<double>(wallEnd - wallStart).count();
    r.totalOps = completedOps.load();
    r.qps = r.durationSec > 0 ? r.totalOps / r.durationSec : 0;
    r.errors = errorCount.load();

    if (!allLatencyUs.empty()) {
        std::sort(allLatencyUs.begin(), allLatencyUs.end());
        size_t n = allLatencyUs.size();
        double sum = 0;
        for (double v : allLatencyUs) sum += v;
        r.avgUs = sum / n;
        r.minUs = allLatencyUs[0];
        r.maxUs = allLatencyUs[n - 1];
        r.p50Us = allLatencyUs[static_cast<size_t>(0.50 * (n - 1))];
        r.p99Us = allLatencyUs[static_cast<size_t>(0.99 * (n - 1))];
        r.p999Us = allLatencyUs[static_cast<size_t>(0.999 * (n - 1))];
    }
    return r;
}

// ============================================================================
int main(int argc, char *argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::vector<int> threadCounts;
    {
        std::istringstream iss(FLAGS_threads);
        std::string tok;
        while (std::getline(iss, tok, ',')) {
            threadCounts.push_back(std::stoi(tok));
        }
    }

    std::cout << "=== E2E Async Echo (Real BrpcAsyncContext) ===\n";
    std::cout << "Warmup: " << FLAGS_warmup_sec << "s  Measure: " << FLAGS_measure_sec
              << "s  Port: " << FLAGS_server_port << "  Threads: " << FLAGS_threads << "\n\n";

    // Start server
    EchoServiceImpl echoSvc;
    brpc::Server server;
    server.AddService(&echoSvc, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc::ServerOptions srvOpts;
    srvOpts.idle_timeout_sec = -1;
    srvOpts.num_threads = 0;
    butil::EndPoint ep(butil::IP_ANY, FLAGS_server_port);
    if (server.Start(ep, &srvOpts) != 0) {
        std::cerr << "ERROR: failed to start server\n";
        return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::vector<BenchResult> results;

    for (int n : threadCounts) {
        std::cout << "  threads=" << n << " ... " << std::flush;
        datasystem::BrpcAsyncContext ctx;
        auto r = RunAsyncBench(ctx, n, FLAGS_warmup_sec, FLAGS_measure_sec,
                                FLAGS_server_port, FLAGS_timeout_ms);
        results.push_back(r);
        std::cout << std::fixed << std::setprecision(0)
                  << "QPS=" << r.qps
                  << std::setprecision(1)
                  << " avg=" << r.avgUs << "us"
                  << " P50=" << r.p50Us << "us"
                  << " P99=" << r.p99Us << "us"
                  << " P999=" << r.p999Us << "us"
                  << " max=" << r.maxUs << "us"
                  << " err=" << r.errors << "\n";
    }

    // CSV output
    std::cout << "\n[RESULT_CSV]\n";
    std::cout << "threads,qps,avg_us,p50_us,p99_us,p999_us,max_us,min_us,errors,duration_sec\n";
    for (const auto &r : results) {
        std::cout << std::fixed
                  << r.threads << ","
                  << std::setprecision(0) << r.qps << ","
                  << std::setprecision(3) << r.avgUs << "," << r.p50Us << "," << r.p99Us
                  << "," << r.p999Us << "," << r.maxUs << "," << r.minUs << ","
                  << r.errors << "," << std::setprecision(1) << r.durationSec << "\n";
    }

    server.Stop(0);
    server.Join();
    std::cout << "\n[INFO] Done.\n";
    return 0;
}
