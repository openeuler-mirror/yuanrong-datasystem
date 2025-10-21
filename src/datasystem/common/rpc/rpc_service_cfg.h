/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Service config.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_SERVICE_CFG_H
#define DATASYSTEM_COMMON_RPC_ZMQ_SERVICE_CFG_H

#include <string>
#include <vector>

namespace datasystem {
/**
 * @brief A config file for Service setup.
 */
struct RpcServiceCfg {
    int32_t numRegularSockets_;
    int32_t numStreamSockets_;
    int32_t hwm_;
    bool udsEnabled_;
    std::string udsDirectory_;
    std::vector<std::pair<std::string, mode_t>> sockList_;
    std::string tcpDirect_;
    RpcServiceCfg();
    ~RpcServiceCfg() = default;
    RpcServiceCfg(const RpcServiceCfg &other) = default;
    RpcServiceCfg &operator=(const RpcServiceCfg &other) = default;
    RpcServiceCfg(RpcServiceCfg &&other) = default;
    RpcServiceCfg &operator=(RpcServiceCfg &&other) = default;
};
};      // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_SERVICE_CFG_H
