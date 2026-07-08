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

#include "datasystem/common/log/log.h"

namespace datasystem {

std::unique_ptr<brpc::Channel> BrpcChannelFactory::Create(const BrpcChannelConfig &cfg)
{
    auto ch = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions opts;
    opts.timeout_ms = cfg.timeout_ms;
    opts.connect_timeout_ms = cfg.connect_timeout_ms;
    // P2-1: always POOLED. SINGLE bottlenecks worker<->master high-QPS paths to
    // one TCP connection (one IO thread) and wastes the rest of the cores.
    opts.connection_type = brpc::CONNECTION_TYPE_POOLED;
    // P2-3: circuit breaker auto-isolates peers with high error rates so a
    // half-dead worker (TCP alive but handler hung) cannot drag down QPS.
    // Default on (cfg.enable_circuit_breaker defaults true). Caller can disable
    // per-channel via cfg (e.g. worker<->worker mesh paths where isolating a
    // peer under momentary back-pressure would amplify the failure).
    opts.enable_circuit_breaker = cfg.enable_circuit_breaker;
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
              << "ms retry=" << cfg.max_retry << " cb=" << (opts.enable_circuit_breaker ? "on" : "off");
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
