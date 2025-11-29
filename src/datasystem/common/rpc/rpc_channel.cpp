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
 * Description: Rpc channel description.
 * This file does not have any connection.
 */
#include <sstream>
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
RpcChannel::RpcChannel(std::string zmqEndPoint, const RpcCredential &cred)
    : endPoint_(std::move(zmqEndPoint)), cred_(cred)
{
    isIPv6_ = IsTcpipEndPointIPv6(zmqEndPoint);
}

RpcChannel::RpcChannel(const HostPort &destAddr, const RpcCredential &cred)
    : endPoint_(TcpipEndPoint(destAddr)), cred_(cred), destAddr_(destAddr), isIPv6_(destAddr.IsIPv6())
{
}

RpcChannel::~RpcChannel() = default;

std::string RpcChannel::UnixSocketPath(const std::string &socketFileDir, const HostPort &localAddress)
{
    std::string unixSocket =
        "ipc://" + socketFileDir + "/" + localAddress.Host() + "_" + std::to_string(localAddress.Port());
    return unixSocket;
}

void RpcChannel::SetServiceUdsEnabled(const std::string &svcName, const std::string &sockName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    udsCfg_[svcName] = sockName;
}

std::string RpcChannel::GetServiceSockName(const std::string &svcName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    auto it = udsCfg_.find(svcName);
    return it == udsCfg_.end() ? "" : it->second;
}

void RpcChannel::SetServiceTcpDirect(const std::string &svcName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    tcpDirect_[svcName] = true;
}

bool RpcChannel::GetServiceTcpDirect(const std::string &svcName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    auto it = tcpDirect_.find(svcName);
    return it == tcpDirect_.end() ? false : it->second;
}

void RpcChannel::SetServiceConnectPoolSize(const std::string &svcName, size_t sz)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    connectPoolSize_[svcName] = std::max<size_t>(sz, 1);
}

size_t RpcChannel::GetServiceConnectPoolSize(const std::string &svcName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    auto it = connectPoolSize_.find(svcName);
    return it == connectPoolSize_.end() ? 1 : it->second;
}

std::string RpcChannel::TcpipEndPoint(const HostPort &localAddress)
{
    return std::string("tcp://") + localAddress.ToString();
}

const std::string &RpcChannel::GetZmqEndPoint() const
{
    return endPoint_;
}

const HostPort &RpcChannel::GetHostPort() const
{
    return destAddr_;
}

Status RpcChannel::ParseTcpipEndpoint(const std::string &endPoint, std::string &addr, std::string &port)
{
    auto pos = endPoint.find_last_of(':');
    CHECK_FAIL_RETURN_STATUS(pos != std::string::npos, K_INVALID, FormatString("Invalid address %s", endPoint));
    port = endPoint.substr(pos + 1);

    // If addr is ipv6, it might be surrounded by [] paranthesis. Remove these.
    // If the first char is not '[' then assume a v4 address
    if (endPoint[0] != '[') {
        addr = endPoint.substr(0, pos);
    } else {
        const int lastCharPosTruncate = 2;
        // strip the first and last characters of the host part of the string (remove the '[' and ']')
        CHECK_FAIL_RETURN_STATUS(
            endPoint[pos - 1] == ']', K_INVALID, FormatString("Malformed ipv6 address %s", endPoint));
        addr = endPoint.substr(1, pos - lastCharPosTruncate);
    }

    if (port == "*") {
        port = "0";
    }
    return Status::OK();
}
}  // namespace datasystem
