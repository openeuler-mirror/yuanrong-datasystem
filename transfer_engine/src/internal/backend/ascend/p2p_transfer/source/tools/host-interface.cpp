/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#include "tools/host-interface.h"
#include "tools/npu-error.h"
#include "tools/env.h"
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

static constexpr int DECIMAL = 10;
static constexpr int IPV4_LIMIT = 255;
static constexpr int ADDR_FAMILY_AUTO = AF_UNSPEC;

struct InterfaceGroups {
    const std::vector<InterfaceInfo> &external;
    const std::vector<InterfaceInfo> &container;
    const std::vector<InterfaceInfo> &lo;
};

struct EnvLookupContext {
    const InterfaceGroups &interfaces;
    const std::string &envName;
    int preferredFamily;
};

std::string FamilyToString(int family)
{
    if (family == AF_INET) {
        return "IPv4";
    }
    if (family == AF_INET6) {
        return "IPv6";
    }
    return "Unknown";
}

Status ParseAddressFamilyPreference(const char *envValue, int &family)
{
    family = ADDR_FAMILY_AUTO;
    if (envValue == nullptr || envValue[0] == '\0') {
        return Status::Success();
    }

    std::string value(envValue);
    if (value == "auto") {
        family = ADDR_FAMILY_AUTO;
        return Status::Success();
    }
    if (value == "ipv4" || value == "IPv4") {
        family = AF_INET;
        return Status::Success();
    }
    if (value == "ipv6" || value == "IPv6") {
        family = AF_INET6;
        return Status::Success();
    }
    return Status::Error(ErrorCode::INVALID_ENV, std::string("Invalid ") + ADDR_FAMILY_ENV + " value.");
}

bool AddressFamilyAccepted(int family, int preferredFamily)
{
    return preferredFamily == ADDR_FAMILY_AUTO || family == preferredFamily;
}

int DetectIpFamily(const std::string &ipString)
{
    struct in_addr addr4 {};
    if (inet_pton(AF_INET, ipString.c_str(), &addr4) == 1) {
        return AF_INET;
    }

    struct in6_addr addr6 {};
    if (inet_pton(AF_INET6, ipString.c_str(), &addr6) == 1) {
        return AF_INET6;
    }

    return AF_UNSPEC;
}

std::vector<std::string> split(const std::string &s, char delimiter)
{
    std::vector<std::string> segments;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
        segments.push_back(item);
    }
    return segments;
}

int convertToInt(const std::string &str)
{
    int result = 0;

    // Convert string to integer, checking for non-digit characters
    for (char ch : str) {
        if (!std::isdigit(ch)) {
            return -1;  // Invalid character found
        }

        result = result * DECIMAL + (ch - '0');

        // Early return if value exceeds 255
        if (result > IPV4_LIMIT) {
            return result;
        }
    }

    return result;
}

// Checks whether a IP address is a valid IPv4 address
bool IsValidIPv4(const std::string &ipString)
{
    // IPv4 cannot be empty or end with a dot
    if (ipString.empty() || ipString.back() == '.') {
        return false;
    }

    // Split the string by dots
    std::vector<std::string> segments = split(ipString, '.');

    // IPv4 must have exactly 4 segments
    const size_t kIPv4PartCount = 4;
    if (segments.size() != kIPv4PartCount) {
        return false;
    }

    // Validate each segment
    for (const std::string &segment : segments) {
        // Segment cannot be empty or have leading zeros (except for "0" itself)
        if (segment.empty() || (segment.size() > 1 && segment[0] == '0')) {
            return false;
        }

        // Convert segment to integer and validate range [0, 255]
        int value = convertToInt(segment);
        if (value < 0 || value > IPV4_LIMIT) {
            return false;
        }
    }

    return true;
}

Status GetHostInterfaces(std::vector<InterfaceInfo> &external_interfaces,
                         std::vector<InterfaceInfo> &container_interfaces, std::vector<InterfaceInfo> &lo_interfaces)
{
    struct ifaddrs *ifaddr, *ifa;
    if (getifaddrs(&ifaddr) == -1) {
        return Status::Error(ErrorCode::INTERNAL_ERROR, "Failed to obtain local interface addresses.");
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        // Skip interfaces which are not UP or don't have an address
        if (ifa->ifa_addr == NULL || !(ifa->ifa_flags & IFF_RUNNING)) {
            continue;
        }

        int family = ifa->ifa_addr->sa_family;

        if (family == AF_INET || family == AF_INET6) {
            char host[NI_MAXHOST];
            int s = getnameinfo(ifa->ifa_addr,
                                (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6), host,
                                NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                std::cerr << "getHostInterfaces getnameinfo() failed: " << gai_strerror(s) << std::endl;
                continue;
            }

            InterfaceInfo info;
            info.name = ifa->ifa_name;
            info.family = family;
            info.address = host;

            // Refined categorization logic
            if (info.name == "lo") {
                lo_interfaces.push_back(info);
            } else if (info.name.rfind("docker", 0) == 0 || info.name.rfind("kube", 0) == 0
                       || info.name.rfind("flannel", 0) == 0 || info.name.rfind("cni", 0) == 0
                       || info.name.rfind("nodelocaldns", 0) == 0) {
                container_interfaces.push_back(info);
            } else {
                external_interfaces.push_back(info);
            }
        }
    }

    freeifaddrs(ifaddr);
    return Status::Success();
}

bool containsIp(const std::vector<InterfaceInfo> &interfaces, const std::string &ipToCheck)
{
    for (const auto &iface : interfaces) {
        if (iface.address == ipToCheck) {
            return true;
        }
    }
    return false;
}

bool containsIp(const std::vector<InterfaceInfo> &interfaces, const std::string &ipToCheck, int family)
{
    for (const auto &iface : interfaces) {
        if (iface.address == ipToCheck && iface.family == family) {
            return true;
        }
    }
    return false;
}

bool findIfNameIp(const std::vector<InterfaceInfo> &interfaces, const std::string &ifName, int preferredFamily,
                  std::string &ip, int &family)
{
    for (const auto &iface : interfaces) {
        if (iface.name == ifName && AddressFamilyAccepted(iface.family, preferredFamily)) {
            ip = iface.address;
            family = iface.family;
            return true;
        }
    }
    return false;
}

bool ContainsIpInAnyGroup(const InterfaceGroups &interfaces, const std::string &ipToCheck, int family)
{
    return containsIp(interfaces.external, ipToCheck, family) || containsIp(interfaces.container, ipToCheck, family)
           || containsIp(interfaces.lo, ipToCheck, family);
}

// Helper function to handle environment variable IP lookup
Status findAndValidateIp(const char *env_var, std::string &ip, int &family, const EnvLookupContext &context)
{
    if (env_var != nullptr) {
        std::string ip_str(env_var);
        int detectedFamily = DetectIpFamily(ip_str);
        if (detectedFamily == AF_UNSPEC) {
            return Status::Error(ErrorCode::INVALID_ENV, "Invalid format for " + context.envName + " IP address.");
        }
        if (!AddressFamilyAccepted(detectedFamily, context.preferredFamily)) {
            return Status::Error(ErrorCode::INVALID_ENV,
                                 context.envName + " IP address does not match address family.");
        }
        if (!ContainsIpInAnyGroup(context.interfaces, ip_str, detectedFamily) && ip_str != "0.0.0.0" &&
            ip_str != "::") {
            std::cerr << "[Warning] IP specified in " + context.envName + " was not found on any active interface.";
        }
        ip = ip_str;
        family = detectedFamily;
        return Status::Success();
    }
    return Status::Error(ErrorCode::NOT_FOUND, "IP not found or environment variable not set.");
}

// Helper function to handle environment variable interface name lookup
Status findAndValidateInterface(const char *env_var, std::string &ip, int &family, const EnvLookupContext &context)
{
    if (env_var != nullptr) {
        if (findIfNameIp(context.interfaces.external, env_var, context.preferredFamily, ip, family)
            || findIfNameIp(context.interfaces.container, env_var, context.preferredFamily, ip, family)
            || findIfNameIp(context.interfaces.lo, env_var, context.preferredFamily, ip, family)) {
            return Status::Success();
        } else {
            return Status::Error(ErrorCode::INVALID_ENV,
                                 "IP for " + context.envName + " environment variable not found.");
        }
    }
    return Status::Error(ErrorCode::NOT_FOUND, "Interface name not found or environment variable not set.");
}

bool PickFirstAddress(const std::vector<InterfaceInfo> &interfaces, int preferredFamily, std::string &ip, int &family)
{
    if (preferredFamily == AF_INET || preferredFamily == AF_INET6) {
        for (const auto &iface : interfaces) {
            if (iface.family == preferredFamily) {
                ip = iface.address;
                family = iface.family;
                return true;
            }
        }
        return false;
    }

    for (const auto &iface : interfaces) {
        if (iface.family == AF_INET) {
            ip = iface.address;
            family = iface.family;
            return true;
        }
    }
    for (const auto &iface : interfaces) {
        if (iface.family == AF_INET6) {
            ip = iface.address;
            family = iface.family;
            return true;
        }
    }
    return false;
}

Status GetHostIp(std::string &ip, int &family)
{
    std::vector<InterfaceInfo> external_interfaces;
    std::vector<InterfaceInfo> container_interfaces;
    std::vector<InterfaceInfo> lo_interfaces;
    CHECK_STATUS(GetHostInterfaces(external_interfaces, container_interfaces, lo_interfaces));
    InterfaceGroups interfaces{ external_interfaces, container_interfaces, lo_interfaces };

    int preferredFamily = ADDR_FAMILY_AUTO;
    CHECK_STATUS(ParseAddressFamilyPreference(std::getenv(ADDR_FAMILY_ENV), preferredFamily));

    // 1. P2P_IF_IP
    EnvLookupContext p2pIpContext{ interfaces, IF_IP_ENV, preferredFamily };
    Status p2pIpStatus = findAndValidateIp(std::getenv(IF_IP_ENV), ip, family, p2pIpContext);
    if (p2pIpStatus.IsSuccess()) {
        return p2pIpStatus;
    }

    // 2. P2P_SOCKET_IFNAME
    EnvLookupContext p2pIfNameContext{ interfaces, IF_NAME_ENV, preferredFamily };
    Status p2pIfNameStatus = findAndValidateInterface(std::getenv(IF_NAME_ENV), ip, family, p2pIfNameContext);
    if (p2pIfNameStatus.IsSuccess()) {
        return p2pIfNameStatus;
    }

    // 3. HCCL_IF_IP
    EnvLookupContext hcclIpContext{ interfaces, IF_IP_ENV_HCCL, preferredFamily };
    Status hcclIpStatus = findAndValidateIp(std::getenv(IF_IP_ENV_HCCL), ip, family, hcclIpContext);
    if (hcclIpStatus.IsSuccess()) {
        return hcclIpStatus;
    }

    // 4. HCCL_SOCKET_IFNAME
    EnvLookupContext hcclIfNameContext{ interfaces, IF_NAME_ENV_HCCL, preferredFamily };
    Status hcclIfNameStatus = findAndValidateInterface(std::getenv(IF_NAME_ENV_HCCL), ip, family, hcclIfNameContext);
    if (hcclIfNameStatus.IsSuccess()) {
        return hcclIfNameStatus;
    }

    // 5. external network card (in order appears)
    if (PickFirstAddress(external_interfaces, preferredFamily, ip, family)) {
        return Status::Success();
    }

    // 6. docker network card
    if (PickFirstAddress(container_interfaces, preferredFamily, ip, family)) {
        return Status::Success();
    }

    // 7. lo network card
    if (PickFirstAddress(lo_interfaces, preferredFamily, ip, family)) {
        return Status::Success();
    }

    return Status::Error(ErrorCode::NOT_FOUND, "No valid host interface IP found.");
}

Status GetHostIp(std::string &ip)
{
    int family = AF_UNSPEC;
    return GetHostIp(ip, family);
}
