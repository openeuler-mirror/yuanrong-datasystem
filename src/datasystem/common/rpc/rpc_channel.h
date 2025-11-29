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
 * Description: A channel encapsulates the zmq transport method.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_CHANNEL_H
#define DATASYSTEM_COMMON_RPC_RPC_CHANNEL_H

#include <map>
#include <mutex>
#include <string>
#include <utility>

#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
class RpcChannel {
public:
    /**
     * @brief This form of constructor takes a ZMQ transport directly.
     * @note A ZMQ transport begins with tcpip:// or ipc:// or inproc://.
     * @param[in] zmqEndPoint Zmq endpoint string.
     * @param[in] cred RPC credential
     */
    RpcChannel(std::string zmqEndPoint, const RpcCredential &cred);

    /**
     * @brief This form of constructor takes a target HostPort and return a tcp/ip ZMQ end point.
     * @param[in] destAddr Zmq endpoint info in host-port structure.
     * @param[in] cred RPC credential
     */
    RpcChannel(const HostPort &destAddr, const RpcCredential &cred);

    virtual ~RpcChannel();

    /**
     * @brief Helper return a string of unix socket path.
     * @param[in] socketFileDir The socket file directory.
     * @param[in] localAddress Local host address.
     * @return std::string of unix socket.
     */
    static std::string UnixSocketPath(const std::string &socketFileDir, const HostPort &localAddress);

    /**
     * @brief Convert HostPort into tcp/ip endpoint.
     * @param[in] localAddress Local host address.
     * @return std::string of tcp/ip endpoint.
     */
    static std::string TcpipEndPoint(const HostPort &localAddress);

    /**
     * @brief Checks if the string endpoint is a an IPv6 endpoint
     * @param[in] endPoint The end point string
     * @return T/F if its IPv6 format
     */
    static bool IsTcpipEndPointIPv6(const std::string &endPoint)
    {
        // IPv6 endpoints start with '[' character in the string
        const std::string TCPIPV6_PREFIX = "tcp://[";
        return (endPoint.compare(0, TCPIPV6_PREFIX.size(), TCPIPV6_PREFIX) == 0);
    }

    /**
     * @brief Parses a tcpip address/port string into its 2 parts. Supports both ipv4 and ipv6 formats.
     * @param[in] endPoint The end point string in the form of either ipv4:port or [ipv6]:port
     * @param[out] addr The host portion of the endpoint string
     * @param[out] port The port portion of the endpoint string
     * @return status of the call
     */
    static Status ParseTcpipEndpoint(const std::string &endPoint, std::string &addr, std::string &port);

    /**
     * @brief Get Zmq End Point.
     * @return const std::string& Zmq endpoint string
     */
    const std::string &GetZmqEndPoint() const;

    /**
     * @brief Get the HostPort object.
     * @return const HostPort& The HostPort.
     */
    const HostPort &GetHostPort() const;

    RpcCredential GetCredential() const
    {
        return cred_;
    }

    /**
     * @brief Enable uds for matching service name
     * @param[in] svcName. Full service names (with namespace)
     * @param[in] sockName. Socket filename to connect to
     */
    void SetServiceUdsEnabled(const std::string &svcName, const std::string &sockName);

    /**
     * @brief Get uds sock name
     * @param[in] svcName. Full service names (with namespace)
     * @return uds sock name. Empty string if not found.
     */
    std::string GetServiceSockName(const std::string &svcName);

    /**
     * @brief Enable tcp/ip direct access
     * @param svcName
     */
    void SetServiceTcpDirect(const std::string &svcName);

    /**
     * @brief Check tcp/ip direct access status
     * @param svcName
     * @return
     */
    bool GetServiceTcpDirect(const std::string &svcName);

    /**
     * @brief Set connect pool size. Default 1
     * @param svcName
     * @param sz
     */
    void SetServiceConnectPoolSize(const std::string &svcName, size_t sz = 1);

    /**
     * @brief Get connect pool size.
     * @param svcName
     * @return
     */
    size_t GetServiceConnectPoolSize(const std::string &svcName);

    bool IsIPv6() const
    {
        return isIPv6_;
    }

private:
    std::string endPoint_;
    RpcCredential cred_;
    std::mutex udsMux_;
    std::map<std::string, std::string> udsCfg_;
    std::map<std::string, bool> tcpDirect_;
    std::map<std::string, size_t> connectPoolSize_;
    const HostPort destAddr_;
    bool isIPv6_{ false };
};

}  // namespace datasystem

/**
 * To allow we can create an unordered_map using RpcChannel as a key.
 */
namespace std {
template <>
struct hash<datasystem::RpcChannel> {
public:
    size_t operator()(const datasystem::RpcChannel &channel) const
    {
        return std::hash<std::string>()(channel.GetZmqEndPoint());
    }
};
template <>
struct equal_to<datasystem::RpcChannel> {
    bool operator()(const datasystem::RpcChannel &lhs, const datasystem::RpcChannel &rhs) const
    {
        return lhs.GetZmqEndPoint() == rhs.GetZmqEndPoint();
    }
};
template <>
struct less<datasystem::RpcChannel> {
    bool operator()(const datasystem::RpcChannel &lhs, const datasystem::RpcChannel &rhs) const
    {
        return lhs.GetZmqEndPoint() < rhs.GetZmqEndPoint();
    }
};
}  // namespace std
#endif  // DATASYSTEM_COMMON_RPC_RPC_CHANNEL_H
