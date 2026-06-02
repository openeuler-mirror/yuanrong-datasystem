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

/**
 * Description: Minimal RPC failure diagnostic helpers.
 */
#ifndef DATASYSTEM_COMMON_UTIL_RPC_DIAGNOSTIC_H
#define DATASYSTEM_COMMON_UTIL_RPC_DIAGNOSTIC_H

#include <string>
#include <string_view>

#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
struct RpcDiagnosticInfo {
    std::string_view method;
    std::string_view src;
    std::string_view dst;
};

std::string FormatRpcDiag(const RpcDiagnosticInfo &info, const Status &status);

Status WithRpcDiag(const Status &status, const RpcDiagnosticInfo &info);

inline Status WithRpcDiag(const Status &status, std::string_view method, std::string_view src, std::string_view dst)
{
    if (status.IsOk()) {
        return status;
    }
    return WithRpcDiag(status, RpcDiagnosticInfo{ method, src, dst });
}

inline Status WithRpcDiag(const Status &status, std::string_view method, const HostPort &dst)
{
    if (status.IsOk()) {
        return status;
    }
    auto dstAddr = dst.ToString();
    return WithRpcDiag(status, RpcDiagnosticInfo{ method, "", dstAddr });
}

inline Status WithRpcDiag(const Status &status, std::string_view method, const HostPort &src, const HostPort &dst)
{
    if (status.IsOk()) {
        return status;
    }
    auto srcAddr = src.ToString();
    auto dstAddr = dst.ToString();
    return WithRpcDiag(status, method, srcAddr, dstAddr);
}

inline Status WithRpcDiag(const Status &status, std::string_view method, const HostPort &src, std::string_view dst)
{
    if (status.IsOk()) {
        return status;
    }
    auto srcAddr = src.ToString();
    return WithRpcDiag(status, method, srcAddr, dst);
}

inline Status WithRpcDiag(const Status &status, std::string_view method, std::string_view src, const HostPort &dst)
{
    if (status.IsOk()) {
        return status;
    }
    auto dstAddr = dst.ToString();
    return WithRpcDiag(status, method, src, dstAddr);
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_RPC_DIAGNOSTIC_H
