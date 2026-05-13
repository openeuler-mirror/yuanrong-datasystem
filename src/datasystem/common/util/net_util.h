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
 * Description: Network utility.
 */
#ifndef DATASYSTEM_COMMON_UTIL_NET_UTIL_H
#define DATASYSTEM_COMMON_UTIL_NET_UTIL_H

#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <unistd.h>

#include "datasystem/utils/status.h"

namespace datasystem {
static constexpr int TO_MILLISECOND = 1000;

/**
 * @brief Split string.
 * @param[in] input String to be split.
 * @param[in] pattern Pattern for split.
 * @return Split string vector
 */
std::vector<std::string> Split(const std::string &input, const std::string &pattern);

/**
 * @brief Split string to get host string and port string.
 * @param[in] str String to be split.
 * @param[out] host Host string.
 * @param[out] port Port string.
 * @param[out] isIPv6 T/F of the input host port string was an IPv6 format
 * @return Status of the call.
 */
Status ParseToHostPortString(const std::string &str, std::string &host, std::string &port, bool &isIPv6);

/**
 * @brief Check if a port is available for binding.
 * @param[in] host The host address (e.g., "0.0.0.0", "127.0.0.1")
 * @param[in] port The port number to check
 * @return Status::OK() if port is available, K_RUNTIME_ERROR with specific message if port is in use
 */
Status IsPortAvailable(const std::string &host, int port);

class HostPort {
public:
    explicit HostPort(std::string host = "", int port = -1);

    HostPort(const HostPort &other) = default;

    HostPort(HostPort &&other) noexcept
    {
        this->host_.swap(other.host_);
        this->port_ = other.port_;
        this->isIPv6_ = other.isIPv6_;
    }

    ~HostPort() = default;

    HostPort &operator=(const HostPort &other) = default;

    HostPort &operator=(HostPort &&other) noexcept
    {
        this->host_.swap(other.host_);
        this->port_ = other.port_;
        this->isIPv6_ = other.isIPv6_;
        return *this;
    }

    bool operator==(const HostPort &other) const
    {
        // not required to compare the isIPv6_ flag. If host and port are equal then the 2 are equal.
        return this->host_ == other.host_ && this->port_ == other.port_;
    }

    bool operator!=(const HostPort &other) const
    {
        return !(*this == other);
    }

    bool operator<(const HostPort &other) const
    {
        if (this->Host() == other.Host()) {
            return this->Port() < other.Port();
        }
        return this->Host() < other.Host();
    }

    std::string ToString() const
    {
        if (host_.empty() && port_ == -1) {
            return "";
        }
        // v4 address format: "ip_string:host"
        // v6 address format: "[ip_string]:host"
        if (!isIPv6_) {
            return host_ + ":" + std::to_string(port_);
        } else {
            return "[" + host_ + "]:" + std::to_string(port_);
        }
    }

    friend std::ostream &operator<<(std::ostream &os, const HostPort &h)
    {
        os << h.ToString();
        return os;
    }

    int Port() const
    {
        return port_;
    }

    const std::string &Host() const
    {
        return host_;
    }

    Status ParseString(const std::string &str);

    bool Empty() const
    {
        return host_.empty();
    }

    size_t hash() const
    {
        return std::hash<std::string>{}(host_ + std::to_string(port_));
    }

    void Clear()
    {
        host_.clear();
        port_ = -1;
    }

    bool IsIPv6() const
    {
        return isIPv6_;
    }

private:
    std::string host_;
    int port_;
    bool isIPv6_{ false };
};
}  // namespace datasystem

namespace std {
/**
 * @brief This template specialization on the std::hash function provides the ability to use it as a key in unordered
 * std containers (unordered_set, unordered_map).  HostPort class must also override the == operator for hash collision
 * resolution.
 */
template <>
struct hash<datasystem::HostPort> {
    size_t operator()(const datasystem::HostPort &hostPort) const
    {
        size_t h1 = std::hash<std::string>{}(hostPort.Host());
        size_t h2 = std::hash<int>{}(static_cast<int>(hostPort.Port()));
        return h1 ^ (h2 << 1);
    }
};
}  // namespace std
#endif  // DATASYSTEM_COMMON_UTIL_NET_UTIL_H
