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

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"

namespace datasystem {
Status ParseToHostPortString(const std::string &str, std::string &host, std::string &port, bool &isIPv6)
{
    std::string addrStr;
    std::string portNumStr;

    CHECK_FAIL_RETURN_STATUS(Validator::ValidateHostPortString("HostPort", str), K_INVALID,
                             "Invalid IP address and port parsed");
    
    auto pos = str.find_last_of(':');
    portNumStr = str.substr(pos + 1);

    // If the host string part begins with '[' then this can be a valid IPv6 host.
    // If it does not start with this, assume IPv4 format.
    if (str[0] != '[') {
        addrStr = str.substr(0, pos);
    } else {
        const int lastCharPosTruncate = 2;
        isIPv6 = true;
        // strip the first and last characters (validator already confirmed the format was good, no need to check again)
        addrStr = str.substr(1, pos - lastCharPosTruncate);
    }

    host = addrStr;
    port = portNumStr;

    return Status::OK();
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

HostPort::HostPort(std::string host, int port)
    : host_(std::move(host)), port_(port)
{
    // IPv6 addresses have the form like: xxxx:xxxx:xxxx:xxxx:xxxx:xxxx
    // Where x's are hex digits. It also supports compaction-like wild cards, so the actual regex to match it
    // is complicated.
    // Quick method: if there's a : in the host name part, assume its v6.
    // The flag setting is needed so that the correct format can be produced in the call to ToString later.
    if (!host_.empty() && host_.find(':') != std::string::npos) {
        isIPv6_ = true;
    }
}

Status HostPort::ParseString(const std::string &str)
{
    std::string port;
    // This parse call already validates the format. Not need to sanity check here.
    RETURN_IF_NOT_OK(ParseToHostPortString(str, host_, port, isIPv6_));
    StringToInt(port, port_);
    return Status::OK();
}
}  // namespace datasystem
