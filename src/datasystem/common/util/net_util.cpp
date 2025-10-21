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
 * Description: Network util.
 */
#include "datasystem/common/util/net_util.h"

#include <sstream>

#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <securec.h>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"

namespace datasystem {
constexpr int PORT_MIN = 1024;
constexpr int PORT_MAX = 65535;
int GetDeviceIp(const std::string &devName, std::string &devIp)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return -1;
    }

    // Type of address to retrieve - IPv4 IP address.
    struct ifreq ifr;
    ifr.ifr_addr.sa_family = AF_INET;
    // Copy the interface name in the ifreq structure.
    int ret = strcpy_s(ifr.ifr_name, sizeof(ifr.ifr_name), devName.c_str());
    if (ret != EOK) {
        RETRY_ON_EINTR(close(fd));
        return -1;
    }

    if (ioctl(fd, SIOCGIFADDR, &ifr) < 0) {
        RETRY_ON_EINTR(close(fd));
        return -1;
    }

    const int ipLength = 32;
    char buf[ipLength] = { 0 };
    if (inet_ntop(AF_INET, &((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr, buf, ipLength) == NULL) {
        RETRY_ON_EINTR(close(fd));
        return -1;
    }

    devIp = std::string(buf);
    RETRY_ON_EINTR(close(fd));

    return 0;
}

std::vector<std::string> Split(const std::string &input, const std::string &pattern)
{
    std::string str = input;
    std::string::size_type pos;
    std::vector<std::string> result;
    str += pattern;
    size_t len = str.size();

    for (size_t i = 0; i < len; i++) {
        pos = str.find(pattern, i);
        if (pos < len) {
            std::string ss = str.substr(i, pos - i);
            result.push_back(ss);
            i = pos + pattern.size() - 1;
        }
    }
    return result;
}

Status ParseToNodeIdString(const std::string &str, std::string &nodeId)
{
    const auto &parsed = Split(str, ":");
    if (parsed.size() == SPLIT_NODE_LIST_NUM) {
        nodeId = parsed[0];
        return Status::OK();
    }
    RETURN_STATUS(StatusCode::K_INVALID, "Parse node failed");
}

Status ParseToHostPortString(const std::string &str, std::string &host, std::string &port, int expectedSz)
{
    int idxHost = 0;
    int idxPort = 1;
    std::vector<std::string> parsed = Split(str, ":");
    if (parsed.size() == SPLIT_NODE_LIST_NUM) {
        ++idxHost;
        ++idxPort;
    } else if (parsed.size() != SPLIT_LIST_NUM) {
        RETURN_STATUS(StatusCode::K_INVALID, "Size of vector of parsed string must be 2 or 3");
    }
    CHECK_FAIL_RETURN_STATUS(expectedSz == SPLIT_NUM_AUTO || static_cast<int>(parsed.size()) == expectedSz,
                             StatusCode::K_INVALID, "Size of vector of parsed string is not expected");
    CHECK_FAIL_RETURN_STATUS(Validator::IsIpv4OrUrl(parsed[idxHost]), K_INVALID, "Invalid IPv4 address parsed");
    CHECK_FAIL_RETURN_STATUS(Validator::IsInPortRange(parsed[idxPort]), K_INVALID, "Invalid port number");
    host = parsed[idxHost];
    port = parsed[idxPort];

    return Status::OK();
}

Status HostPort::ParseString(const std::string &str, const HostPort &defaultHostPort)
{
    if (ParseString(str).IsError()) {
        *this = defaultHostPort;
    }
    return Status::OK();
}

Status HostPort::ParseString(const std::string &str)
{
    std::string port;
    RETURN_IF_NOT_OK(ParseToHostPortString(str, host_, port));
    CHECK_FAIL_RETURN_STATUS(StringToInt(port, port_), StatusCode::K_INVALID, "Parse failed with port " + port);

    if (port_ > PORT_MAX) {
        std::stringstream error;
        error << "port [" << port_ << "] > 65535";
        RETURN_STATUS(StatusCode::K_INVALID, error.str());
    }
    return Status::OK();
}

bool HostPort::IsValidateAddress(const std::string &address)
{
    if (!address.empty()) {
        std::vector<std::string> split_str = Split(address, ":");
        if (split_str.size() != SPLIT_LIST_NUM) {
            return false;
        }
        int port = 0;
        if (!StringToInt(split_str[1], port)) {
            return false;
        }
        // The Ip address is invalid OR The port is invalid.
        if ((INADDR_NONE == inet_addr(split_str[0].c_str()))
            || (port < PORT_MIN || port > PORT_MAX)) {
            return false;
        }
    }
    return true;
}
}  // namespace datasystem
