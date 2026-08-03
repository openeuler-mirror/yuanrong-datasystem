/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Implementation of the centralized brpc channel/controller factory.
 * See brpc_factory.h for the design rationale.
 */
#include "datasystem/common/rpc/brpc_factory.h"

#include <gflags/gflags.h>
#include <mutex>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace {

// Enable brpc wire-level delivery of timeout_ms so the server-side
// BuildScTimeoutDurationInitSnippet (brpc_service_generator.cpp) can read
// cntl->timeout_ms() and initialize reqTimeoutDuration from the client
// budget instead of falling back to the 60s DEFAULT_TIMEOUT.
// brpc's baidu_std_protocol_deliver_timeout_ms defaults to false;
// this sets it true exactly once, before any brpc channel is initialized.
// Called from BrpcChannelFactory::Create, which is the common entry point
// for all brpc-outgoing paths (worker, client SDK, etc.).
void EnsureBrpcDeliverTimeoutMs()
{
    static std::once_flag once;
    std::call_once(once, []() {
        const std::string prev = gflags::SetCommandLineOption(
            "baidu_std_protocol_deliver_timeout_ms", "true");
        LOG(INFO) << "baidu_std_protocol_deliver_timeout_ms=" << prev
                  << " -> true (enabled for deadline propagation)";
    });
}

// Raise brpc's global max_body_size above its 64MB default so large object
// payloads (e.g. a 300MB cross-node Get response) are accepted by
// input_messenger on the receiving socket instead of being rejected with
// "too big data" -> connection close -> Host is down. Fixed at 2GB: brpc
// rejects any single RPC body at or above this limit, so objects >= 2GB
// surface a brpc-level error rather than succeeding silently.
// brpc::FLAGS_max_body_size is read by input_messenger at socket-init time, so
// it must be set before the first brpc socket is created. BrpcChannelFactory
// ::Create is the common entry point for all brpc-outgoing paths (worker,
// client SDK), and runs before the first channel/socket is initialized.
// Worker server sockets set it again in RpcServer::StartBrpcServer.
// Idempotent via std::call_once.
void EnsureBrpcMaxBodySize()
{
    static std::once_flag once;
    std::call_once(once, []() {
        constexpr uint64_t kBrpcMaxBodySize = 2ULL * 1024 * 1024 * 1024;  // 2GB
        const std::string value = std::to_string(kBrpcMaxBodySize);
        const std::string prev = gflags::SetCommandLineOption("max_body_size", value.c_str());
        LOG(INFO) << "max_body_size=" << prev << " -> " << value
                  << " (raised for large object payloads)";
    });
}

// Bound the brpc circuit-breaker isolation duration. brpc default
// circuit_breaker_max_isolation_duration_ms=30000 (30s) keeps a transiently-
// faulty socket isolated long after the fault clears (e.g. UB/URMA link-down
// recovers ~10s but the socket stays isolated up to 30s, escalating via the
// doubling min->max). 3000 (3s) caps recovery so business resumes within 3s
// of the fault clearing. Set exactly once before any brpc channel is created.
// Only effective when CB is enabled (FLAGS_brpc_enable_circuit_breaker=true).
void EnsureBrpcCircuitBreakerIsolationCap()
{
    static std::once_flag once;
    std::call_once(once, []() {
        constexpr int64_t kCbMaxIsolationMs = 3000;  // 3s; bounds CB isolation for fast UB recovery
        const std::string value = std::to_string(kCbMaxIsolationMs);
        const std::string prev = gflags::SetCommandLineOption(
            "circuit_breaker_max_isolation_duration_ms", value.c_str());
        LOG(INFO) << "circuit_breaker_max_isolation_duration_ms=" << prev
                  << " -> " << value << " (bound CB isolation to 3s; brpc default 30000/30s)";
    });
}

}  // namespace

// Force brpc's global one-shot init (GlobalInitializeOrDie, guarded by
// pthread_once) on the calling thread. See header for full rationale.
// Trigger: a throwaway brpc::Channel + Init against an unreachable
// endpoint. brpc connects lazily on first RPC, so Init only sets up the
// channel/socket-map entry without opening a TCP socket; the channel
// destructor immediately releases it. The actual value of Init's return
// is irrelevant — we only care that the pthread_once body ran.
void BrpcChannelFactory::EnsureGlobalInitialized()
{
    static std::once_flag once;
    std::call_once(once, []() {
        brpc::ChannelOptions opts;
        brpc::Channel ch;
        // 127.0.0.1:1 — loopback, no DNS; port 1 is unreachable on a normal
        // host, but Init does not connect, so the return code is irrelevant.
        // Discard via (void) to make the intent explicit.
        (void)ch.Init("127.0.0.1:1", &opts);
        LOG(INFO) << "brpc global initialization pre-warmed on main thread";
    });
}

std::unique_ptr<brpc::Channel> BrpcChannelFactory::Create(const BrpcChannelConfig &cfg)
{
    // Pre-warm brpc global init on the calling thread before touching any
    // channel. Idempotent via std::call_once. For callers that already
    // invoked EnsureGlobalInitialized() explicitly during startup (worker,
    // coordinator) this is a no-op; for ad-hoc callers (client SDK, tests,
    // dsbench) this guarantees the once-init runs on a known thread before
    // the first Channel::Init instead of racing inside a multi-threaded
    // context. See header for the TSAN rationale.
    EnsureGlobalInitialized();
    EnsureBrpcDeliverTimeoutMs();
    EnsureBrpcMaxBodySize();
    EnsureBrpcCircuitBreakerIsolationCap();
    auto ch = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions opts;
    opts.timeout_ms = cfg.timeout_ms;
    opts.connect_timeout_ms = cfg.connect_timeout_ms;
    // P2-1: always POOLED. SINGLE bottlenecks worker<->master high-QPS paths to
    // one TCP connection (one IO thread) and wastes the rest of the cores.
    opts.connection_type = brpc::CONNECTION_TYPE_POOLED;
    // P2-3: circuit breaker auto-isolates peers with high error rates so a
    // half-dead worker (TCP alive but handler hung) cannot drag down QPS.
    // Gated by FLAGS_brpc_enable_circuit_breaker (default false). When true,
    // the per-channel cfg.enable_circuit_breaker takes effect (mesh paths
    // have it off by default). When false, cb is globally disabled regardless
    // of per-channel config.
    opts.enable_circuit_breaker = cfg.enable_circuit_breaker && FLAGS_brpc_enable_circuit_breaker;
    // P2-4: brpc-level retry. Default 3 covers transient EHOSTDOWN/ECONNREFUSED
    // without the old app-level RetryOnRPCError sleep-loop (retry storm fix).
    opts.max_retry = cfg.max_retry;
    // NOTE: brpc::ChannelOptions has no max_body_size field; the body limit is
    // the global brpc::FLAGS_max_body_size gflag, set in RpcServer::StartBrpcServer
    // before the first socket is created, which also covers client-side receive
    // sockets. No per-channel setting needed here.
    if (cfg.backup_request_ms > 0) {
        opts.backup_request_ms = cfg.backup_request_ms;
    }
    if (ch->Init(cfg.endpoint.c_str(), &opts) != 0) {
        LOG(ERROR) << "Failed to create brpc channel to " << cfg.endpoint;
        return nullptr;
    }
    LOG(INFO) << "BrpcChannel created: " << cfg.endpoint << " timeout=" << cfg.timeout_ms
              << "ms connect_timeout=" << cfg.connect_timeout_ms << "ms retry=" << cfg.max_retry
              << " cb=" << (opts.enable_circuit_breaker ? "on" : "off");
    return ch;
}

std::unique_ptr<brpc::Controller> BrpcControllerFactory::Create(const BrpcControllerConfig &cfg)
{
    auto cntl = std::make_unique<brpc::Controller>();
    if (cfg.timeout_ms > 0) {
        cntl->set_timeout_ms(cfg.timeout_ms);
    }
    if (cfg.max_retry > 0) {
        cntl->set_max_retry(cfg.max_retry);
    }
    if (cfg.backup_request_ms > 0) {
        cntl->set_backup_request_ms(cfg.backup_request_ms);
    }
    return cntl;
}

}  // namespace datasystem
