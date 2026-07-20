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
 * Description: Centralized factory for creating brpc::Channel / brpc::Controller.
 *
 * Design principle (brpc-centralized-usage-design-2026-07-05):
 *   "use brpc centrally, do NOT abstract brpc away". This is a thin tool layer,
 *   not a virtual interface. Callers receive brpc native types
 *   (brpc::Channel* / brpc::Controller*) and call brpc directly.
 *
 * All brpc ChannelOptions/Controller configuration is centralized here so that:
 *   - connection_type POOLED, circuit_breaker, max_retry defaults are applied
 *     consistently (no call site can forget them);
 *   - future brpc tuning changes one place, not N.
 *
 * Intentionally NOT provided (rejected abstraction-layer approach):
 *   - IRpcChannel / IRpcController / IRpcStub virtual interfaces
 *   - BrpcChannelImpl / BrpcControllerImpl adapters
 *   - RpcCallOptions chain API (use the BrpcChannelConfig struct instead)
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_FACTORY_H
#define DATASYSTEM_COMMON_RPC_BRPC_FACTORY_H

#include <memory>
#include <string>

#include <brpc/channel.h>
#include <brpc/controller.h>

namespace datasystem {

/**
 * @brief Value object holding all configuration for a brpc::Channel.
 *
 * Fields default to the centralized brpc policy:
 *   - POOLED connection (set unconditionally in the factory, not here)
 *   - circuit_breaker gated by FLAGS_brpc_enable_circuit_breaker (default false).
 *     When the flag is true, cfg.enable_circuit_breaker=true enables the breaker
 *     per-channel; when false the breaker is globally off.
 *   - max_retry = 3 by default
 */
struct BrpcChannelConfig {
    /// "host:port" (or brpc load-balancer naming) to connect to.
    std::string endpoint;
    /// Per-RPC timeout (ms). brpc default 500.
    int32_t timeout_ms = 500;
    /// TCP connect timeout (ms). brpc default 1000.
    int32_t connect_timeout_ms = 1000;
    /// brpc-level retry count. 0 disables retry. Default 3.
    int max_retry = 3;
    /// Backup-request delay (ms). 0 = disabled. Only enable for idempotent
    /// READ RPCs; never set for Publish/Set (non-idempotent writes).
    int32_t backup_request_ms = 0;
    /// Enable brpc circuit breaker (auto isolation on high error rate).
    /// Default true. Set false per-channel for internal mesh paths where
    /// isolating a peer under momentary back-pressure would amplify failure.
    bool enable_circuit_breaker = true;
};

/**
 * @brief Value object holding per-call overrides for a brpc::Controller.
 *
 * A value of 0 means "do not override; use the channel default".
 */
struct BrpcControllerConfig {
    int32_t timeout_ms = 0;
    int max_retry = 0;
    int32_t backup_request_ms = 0;
};

/**
 * @brief Centralized factory for brpc::Channel.
 *
 * Always sets connection_type = POOLED so no call site can regress to SINGLE.
 * Returns nullptr on Init failure (caller must check).
 */
class BrpcChannelFactory {
public:
    BrpcChannelFactory() = default;
    ~BrpcChannelFactory() = default;
    static std::unique_ptr<brpc::Channel> Create(const BrpcChannelConfig &cfg);
};

/**
 * @brief Centralized factory for brpc::Controller.
 *
 * Only applies non-zero overrides; zero fields inherit the channel default.
 */
class BrpcControllerFactory {
public:
    BrpcControllerFactory() = default;
    ~BrpcControllerFactory() = default;
    static std::unique_ptr<brpc::Controller> Create(const BrpcControllerConfig &cfg = {});
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_BRPC_FACTORY_H
