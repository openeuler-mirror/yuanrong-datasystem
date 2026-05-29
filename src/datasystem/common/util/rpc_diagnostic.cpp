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
#include "datasystem/common/util/rpc_diagnostic.h"

namespace datasystem {
namespace {
constexpr std::string_view STATUS_CODE_PREFIX = "StatusCode: ";
constexpr std::string_view LEGACY_CODE_PREFIX = "code: [";
constexpr std::string_view LEGACY_MSG_MARK = "], msg: [";
constexpr std::string_view DIAG_SRC_PREFIX = "[";
constexpr std::string_view DIAG_SRC_METHOD_SEP = "]-";
constexpr std::string_view DIAG_METHOD_DST_SEP = "->[";
constexpr std::string_view DIAG_DST_SUFFIX = "]";
constexpr size_t SERVICE_ENDPOINT_EXTRA_SIZE =
    DIAG_SRC_PREFIX.size() + DIAG_SRC_METHOD_SEP.size() + DIAG_METHOD_DST_SEP.size() + DIAG_DST_SUFFIX.size();
constexpr size_t CLIENT_ENDPOINT_EXTRA_SIZE = DIAG_METHOD_DST_SEP.size() + DIAG_DST_SUFFIX.size();

std::string StripRepeatedStatusCode(std::string reason)
{
    if (reason.rfind(STATUS_CODE_PREFIX, 0) == 0) {
        auto commaPos = reason.find(',');
        if (commaPos != std::string::npos) {
            auto firstMsgPos = commaPos + 1;
            while (firstMsgPos < reason.size() && reason[firstMsgPos] == ' ') {
                ++firstMsgPos;
            }
            return reason.substr(firstMsgPos);
        }
    }
    if (reason.rfind(LEGACY_CODE_PREFIX, 0) == 0) {
        auto msgPos = reason.find(LEGACY_MSG_MARK);
        if (msgPos != std::string::npos) {
            auto firstMsgPos = msgPos + LEGACY_MSG_MARK.size();
            auto lastBracketPos = reason.rfind(']');
            if (lastBracketPos != std::string::npos && lastBracketPos > firstMsgPos) {
                return reason.substr(firstMsgPos, lastBracketPos - firstMsgPos);
            }
            return reason.substr(firstMsgPos);
        }
    }
    return reason;
}

bool HasRpcDiag(const std::string &reason, const RpcDiagnosticInfo &info)
{
    if (info.method.empty()) {
        return false;
    }
    std::string method(info.method);
    if (!info.src.empty()) {
        std::string servicePattern = "]-" + method + "->[";
        return reason.find(servicePattern) != std::string::npos;
    }
    auto servicePatternPos = reason.find(DIAG_SRC_METHOD_SEP);
    if (servicePatternPos != std::string::npos
        && reason.find(DIAG_METHOD_DST_SEP, servicePatternPos + DIAG_SRC_METHOD_SEP.size()) != std::string::npos) {
        return true;
    }
    std::string clientPattern = method + "->[";
    return reason.find(clientPattern) != std::string::npos;
}

std::string BuildEndpoint(const RpcDiagnosticInfo &info)
{
    std::string diag;
    if (!info.src.empty()) {
        diag.reserve(info.src.size() + info.method.size() + info.dst.size() + SERVICE_ENDPOINT_EXTRA_SIZE);
        diag.append(DIAG_SRC_PREFIX);
        diag.append(info.src);
        diag.append(DIAG_SRC_METHOD_SEP);
        diag.append(info.method);
        diag.append(DIAG_METHOD_DST_SEP);
        diag.append(info.dst);
        diag.append(DIAG_DST_SUFFIX);
        return diag;
    }
    diag.reserve(info.method.size() + info.dst.size() + CLIENT_ENDPOINT_EXTRA_SIZE);
    diag.append(info.method);
    if (!info.dst.empty()) {
        diag.append(DIAG_METHOD_DST_SEP);
        diag.append(info.dst);
        diag.append(DIAG_DST_SUFFIX);
    }
    return diag;
}
}  // namespace

std::string FormatRpcDiag(const RpcDiagnosticInfo &info, const Status &status)
{
    auto reason = StripRepeatedStatusCode(status.GetMsg());
    if (HasRpcDiag(reason, info)) {
        return reason;
    }
    auto diag = BuildEndpoint(info);
    if (diag.empty()) {
        return reason;
    }
    if (!reason.empty()) {
        diag.push_back(' ');
        diag.append(reason);
    }
    return diag;
}

Status WithRpcDiag(const Status &status, const RpcDiagnosticInfo &info)
{
    if (status.IsOk()) {
        return status;
    }
    return Status(status.GetCode(), FormatRpcDiag(info, status));
}
}  // namespace datasystem
