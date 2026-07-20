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

}  // namespace

std::unique_ptr<brpc::Channel> BrpcChannelFactory::Create(const BrpcChannelConfig &cfg)
{
    EnsureBrpcDeliverTimeoutMs();
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
