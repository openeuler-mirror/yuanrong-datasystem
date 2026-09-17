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

/** Description: Selects a deterministic remote Worker for kvtest benchmark clients. */
#pragma once

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

/** @brief Holds a parsed remote Worker endpoint. */
struct RemoteWorkerEndpoint {
    std::string host;
    int port = 0;

    /**
     * @brief Return the normalized Worker address.
     * @return Worker address in host:port form.
     */
    std::string ToString() const
    {
        return host.find(':') == std::string::npos ? host + ":" + std::to_string(port)
                                                   : "[" + host + "]:" + std::to_string(port);
    }
};

/**
 * @brief Parse one ServiceDiscovery Worker address.
 * @param[in] address Worker address in host:port form.
 * @param[out] endpoint Parsed Worker endpoint.
 * @return True when the address is valid.
 */
inline bool ParseRemoteWorkerEndpoint(const std::string &address, RemoteWorkerEndpoint &endpoint)
{
    const auto separator = address.rfind(':');
    if (separator == std::string::npos || separator == 0 || separator + 1 >= address.size()) {
        return false;
    }
    std::string host = address.substr(0, separator);
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    int port = 0;
    const char *portBegin = address.data() + separator + 1;
    const char *portEnd = address.data() + address.size();
    const auto parsed = std::from_chars(portBegin, portEnd, port);
    constexpr int maxPort = std::numeric_limits<uint16_t>::max();
    if (host.empty() || parsed.ec != std::errc() || parsed.ptr != portEnd || port <= 0 || port > maxPort) {
        return false;
    }
    endpoint = { std::move(host), port };
    return true;
}

/**
 * @brief Select the stable first endpoint from discovered remote Workers.
 * @param[in] remoteWorkers Discovered non-local Worker addresses.
 * @param[out] endpoint Selected Worker endpoint.
 * @return True when a valid endpoint is selected.
 */
inline bool SelectRemoteWorkerEndpoint(std::vector<std::string> remoteWorkers, RemoteWorkerEndpoint &endpoint)
{
    if (remoteWorkers.empty()) {
        return false;
    }
    std::sort(remoteWorkers.begin(), remoteWorkers.end());
    for (const auto &worker : remoteWorkers) {
        if (ParseRemoteWorkerEndpoint(worker, endpoint)) {
            return true;
        }
    }
    return false;
}
