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
#include <iostream>
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

        if (family == AF_INET) {
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
            info.family = (family == AF_INET) ? "IPv4" : "IPv6";
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

bool findIfNameIp(const std::vector<InterfaceInfo> &interfaces, const std::string &ifName, std::string &ip)
{
    for (const auto &iface : interfaces) {
        if (iface.name == ifName) {
            ip = iface.address;
            return true;
        }
    }
    return false;
}

// Helper function to handle environment variable IP lookup
Status findAndValidateIp(const char *env_var, std::string &ip, const std::vector<InterfaceInfo> &external_interfaces,
                         const std::vector<InterfaceInfo> &container_interfaces,
                         const std::vector<InterfaceInfo> &lo_interfaces, const std::string &env_name)
{
    if (env_var != nullptr) {
        std::string ip_str(env_var);
        if (!IsValidIPv4(ip_str)) {
            return Status::Error(ErrorCode::INVALID_ENV, "Invalid format for " + env_name + " IP address.");
        }
        if (!containsIp(external_interfaces, ip_str) && !containsIp(container_interfaces, ip_str)
            && !containsIp(lo_interfaces, ip_str) && ip_str != "0.0.0.0") {
            std::cerr << "[Warning] IP specified in " + env_name + " was not found on any active interface.";
        }
        ip = ip_str;
        return Status::Success();
    }
    return Status::Error(ErrorCode::NOT_FOUND, "IP not found or environment variable not set.");
}

// Helper function to handle environment variable interface name lookup
Status findAndValidateInterface(const char *env_var, std::string &ip,
                                const std::vector<InterfaceInfo> &external_interfaces,
                                const std::vector<InterfaceInfo> &container_interfaces,
                                const std::vector<InterfaceInfo> &lo_interfaces, const std::string &env_name)
{
    if (env_var != nullptr) {
        if (findIfNameIp(external_interfaces, env_var, ip) || findIfNameIp(container_interfaces, env_var, ip)
            || findIfNameIp(lo_interfaces, env_var, ip)) {
            return Status::Success();
        } else {
            return Status::Error(ErrorCode::INVALID_ENV, "IP for " + env_name + " environment variable not found.");
        }
    }
    return Status::Error(ErrorCode::NOT_FOUND, "Interface name not found or environment variable not set.");
}

Status GetHostIp(std::string &ip)
{
    std::vector<InterfaceInfo> external_interfaces;
    std::vector<InterfaceInfo> container_interfaces;
    std::vector<InterfaceInfo> lo_interfaces;
    CHECK_STATUS(GetHostInterfaces(external_interfaces, container_interfaces, lo_interfaces));

    // 1. P2P_IF_IP
    Status p2pIpStatus = findAndValidateIp(std::getenv(IF_IP_ENV), ip, external_interfaces, container_interfaces,
                                           lo_interfaces, IF_IP_ENV);
    if (p2pIpStatus.IsSuccess()) {
        return p2pIpStatus;
    }

    // 2. P2P_SOCKET_IFNAME
    Status p2pIfNameStatus = findAndValidateInterface(std::getenv(IF_NAME_ENV), ip, external_interfaces,
                                                      container_interfaces, lo_interfaces, IF_NAME_ENV);
    if (p2pIfNameStatus.IsSuccess()) {
        return p2pIfNameStatus;
    }

    // 3. HCCL_IF_IP
    Status hcclIpStatus = findAndValidateIp(std::getenv(IF_IP_ENV_HCCL), ip, external_interfaces, container_interfaces,
                                            lo_interfaces, IF_IP_ENV_HCCL);
    if (hcclIpStatus.IsSuccess()) {
        return hcclIpStatus;
    }

    // 4. HCCL_SOCKET_IFNAME
    Status hcclIfNameStatus = findAndValidateInterface(std::getenv(IF_NAME_ENV_HCCL), ip, external_interfaces,
                                                       container_interfaces, lo_interfaces, IF_NAME_ENV_HCCL);
    if (hcclIfNameStatus.IsSuccess()) {
        return hcclIfNameStatus;
    }

    // 5. external network card (in order appears)
    if (!external_interfaces.empty()) {
        ip = external_interfaces[0].address;
        return Status::Success();
    }

    // 6. docker network card
    if (!container_interfaces.empty()) {
        ip = container_interfaces[0].address;
        return Status::Success();
    }

    // 7. lo network card
    if (!lo_interfaces.empty()) {
        ip = lo_interfaces[0].address;
        return Status::Success();
    }

    return Status::Error(ErrorCode::NOT_FOUND, "No valid host interface IP found.");
}