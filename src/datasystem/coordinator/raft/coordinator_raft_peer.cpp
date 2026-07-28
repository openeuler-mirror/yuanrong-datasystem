// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Description: Coordinator raft peer identity parsing and formatting.
 */
#include "datasystem/coordinator/raft/coordinator_raft_peer.h"

#include <charconv>
#include <string_view>

#include <butil/endpoint.h>

namespace datasystem::coordinator {
namespace {
constexpr int kMaxNetworkPort = 65'535;
constexpr size_t kIpv4OctetCount = 4;
constexpr size_t kMaxIpv4OctetLength = 3;
constexpr size_t kMaxPortLength = 5;
constexpr int kMaxIpv4OctetValue = 255;

bool IsQuietDecimalIpv4(std::string_view host)
{
    size_t octetBegin = 0;
    for (size_t octetIndex = 0; octetIndex < kIpv4OctetCount; ++octetIndex) {
        const auto separator = host.find('.', octetBegin);
        const bool isLastOctet = octetIndex + 1 == kIpv4OctetCount;
        if ((!isLastOctet && separator == std::string_view::npos)
            || (isLastOctet && separator != std::string_view::npos)) {
            return false;
        }

        const auto octetEnd = isLastOctet ? host.size() : separator;
        const auto octet = host.substr(octetBegin, octetEnd - octetBegin);
        if (octet.empty() || octet.size() > kMaxIpv4OctetLength
            || (octet.size() == kMaxIpv4OctetLength && octet.front() == '0')) {
            return false;
        }

        int octetValue = 0;
        const auto parseResult = std::from_chars(octet.data(), octet.data() + octet.size(), octetValue);
        if (parseResult.ec != std::errc() || parseResult.ptr != octet.data() + octet.size()
            || octetValue > kMaxIpv4OctetValue) {
            return false;
        }
        octetBegin = octetEnd + 1;
    }
    return true;
}

bool ParseQuietDecimalPort(std::string_view portText, int &port)
{
    if (portText.empty() || portText.size() > kMaxPortLength) {
        return false;
    }
    for (const char digit : portText) {
        if (digit < '0' || digit > '9') {
            return false;
        }
    }

    // Preserve the existing validator's handling of five-digit, zero-padded ports.
    if (portText.size() == kMaxPortLength && portText.front() == '0') {
        for (const char digit : portText) {
            if (digit > '5') {
                return false;
            }
        }
    }

    const auto parseResult = std::from_chars(portText.data(), portText.data() + portText.size(), port);
    return parseResult.ec == std::errc() && parseResult.ptr == portText.data() + portText.size()
           && port <= kMaxNetworkPort;
}
}  // namespace

Status ParseCoordinatorRaftPeer(const std::string &address, braft::PeerId &peer)
{
    peer.reset();
    if (address.empty()) {
        return Status(K_INVALID, "Coordinator Raft peer address must not be empty");
    }

    const auto separator = address.find(':');
    if (separator == std::string::npos || separator != address.rfind(':')) {
        return Status(
            K_INVALID,
            "Coordinator Raft peer codec currently requires numeric IPv4:port without an explicit braft index");
    }
    if (address.find_first_of(" \t\n\r\f\v") != std::string::npos) {
        return Status(K_INVALID, "Coordinator Raft peer address must not contain whitespace or trailing data");
    }

    const std::string_view host(address.data(), separator);
    const std::string_view portText(address.data() + separator + 1, address.size() - separator - 1);
    int port = 0;
    if (!ParseQuietDecimalPort(portText, port) || (host != "localhost" && !IsQuietDecimalIpv4(host))) {
        return Status(K_INVALID, "Coordinator Raft peer codec currently requires numeric IPv4 with a decimal port");
    }
    if (host == "localhost") {
        return Status(K_INVALID, "Coordinator Raft peer codec currently requires a numeric IPv4 address");
    }
    if (port < 1 || port > kMaxNetworkPort) {
        return Status(K_INVALID, "Coordinator Raft peer address port must be in range 1..65535");
    }

    butil::EndPoint endpoint;
    const std::string hostText(host);
    if (butil::str2ip(hostText.c_str(), &endpoint.ip) != 0) {
        return Status(K_INVALID, "Coordinator Raft peer codec currently requires a valid numeric IPv4 address");
    }
    if (endpoint.ip == butil::IP_ANY) {
        return Status(K_INVALID, "Coordinator Raft peer address must not use wildcard IPv4 0.0.0.0");
    }
    endpoint.port = port;
    const std::string normalizedAddress = butil::endpoint2str(endpoint).c_str();

    if (peer.parse(normalizedAddress) != 0 || peer.is_empty() || peer.idx != 0) {
        peer.reset();
        return Status(K_INVALID, "Coordinator Raft peer address could not be normalized with braft index 0");
    }
    return Status::OK();
}

std::string CoordinatorRaftPeerAddress(const braft::PeerId &peer)
{
    if (peer.is_empty() || peer.idx != 0 || peer.addr.ip == butil::IP_ANY || peer.addr.port < 1
        || peer.addr.port > kMaxNetworkPort) {
        return {};
    }
    return butil::endpoint2str(peer.addr).c_str();
}

}  // namespace datasystem::coordinator
