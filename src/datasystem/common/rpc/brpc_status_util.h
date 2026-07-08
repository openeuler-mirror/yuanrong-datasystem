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
 * Description: Utility for extracting embedded Status codes from brpc response
 * protobuf messages. When a brpc server returns an error, it populates an
 * ErrorInfoPb field (named "error", "last_rc", or "err") in the response. This
 * utility reads that field so the client stub can return the server's actual
 * error code instead of K_RPC_CANCELLED.
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_STATUS_UTIL_H
#define DATASYSTEM_COMMON_RPC_BRPC_STATUS_UTIL_H

#include <climits>
#include <string>
#include <vector>

#include <google/protobuf/message.h>
#include "datasystem/utils/status.h"

namespace datasystem {

/// Timeout (seconds) for StreamClose to deliver on_closed/on_failed callback.
/// Shared by brpc client streaming adapters to bound destructor wait time.
inline constexpr int kStreamCloseTimeoutSec = 5;

/// Timeout (seconds) for a single Stream Read wait. Bounds the readCond_ wait so
/// a peer crash / network partition cannot block the calling bthread forever and
/// exhaust the bthread pool under nested RPC load. Must be > kStreamCloseTimeoutSec.
inline constexpr int kStreamReadTimeoutSec = 30;

/**
 * @brief Try to extract an embedded Status from a brpc response protobuf.
 *
 * Examines the response message for an ErrorInfoPb sub-message (by convention
 * named "error", "last_rc", or "err"). If found and its error_code is non-zero,
 * returns that Status. Otherwise returns Status::OK(), meaning the caller should
 * fall back to its own error handling.
 *
 * @param response The protobuf response message received from a brpc RPC.
 * @return The extracted Status (if an error was found), or Status::OK().
 */
inline Status TryExtractStatusFromResponse(const google::protobuf::Message &response)
{
    const auto *desc = response.GetDescriptor();
    const auto *reflection = response.GetReflection();
    // Candidate field names for ErrorInfoPb across proto schema evolutions.
    // If ErrorInfoPb field names change in master_object.proto / common.proto,
    // update this list (and the "error_code" / "error_msg" sub-field lookups
    // below) to stay in sync.
    static const std::vector<const char *> candidateFields = {"error", "last_rc", "err"};
    for (const char *fieldName : candidateFields) {
        const auto *field = desc->FindFieldByName(fieldName);
        if (field == nullptr ||
            field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        if (field->is_repeated()) {
            if (reflection->FieldSize(response, field) == 0) {
                continue;
            }
        } else {
            if (!reflection->HasField(response, field)) {
                continue;
            }
        }
        const auto &subMsg = field->is_repeated()
            ? reflection->GetRepeatedMessage(response, field, 0)
            : reflection->GetMessage(response, field);
        const auto *subDesc = subMsg.GetDescriptor();
        const auto *codeField = subDesc->FindFieldByName("error_code");
        if (codeField == nullptr || codeField->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_INT32) {
            continue;
        }
        int32_t code = subMsg.GetReflection()->GetInt32(subMsg, codeField);
        if (code == 0) {
            // 0 = success (K_OK), not an error we should return
            continue;
        }
        const auto *msgField = subDesc->FindFieldByName("error_msg");
        std::string msgStr = msgField ? subMsg.GetReflection()->GetString(subMsg, msgField) : "";
        return Status(static_cast<StatusCode>(code), __LINE__, __FILE__, msgStr);
    }
    return Status::OK();
}

/**
 * @brief Parse the DS_ERR sentinel body (digits only) into a positive int32 code.
 *
 * @param codeStr The substring between \\x01DS_ERR: and \\x02.
 * @param[out] codeOut The parsed code on success.
 * @return True if codeStr is non-empty, all digits, and within (0, INT_MAX].
 */
inline bool TryParseDsErrCode(const std::string &codeStr, long &codeOut)
{
    if (codeStr.empty()) {
        return false;
    }
    for (char c : codeStr) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    try {
        long codeLong = std::stol(codeStr);
        if (codeLong > 0 && codeLong <= INT_MAX) {
            codeOut = codeLong;
            return true;
        }
    } catch (const std::exception &) {
        // out_of_range — fall through to K_RPC_CANCELLED
    }
    return false;
}

/**
 * @brief Extract Status from brpc Controller error text (\\x01DS_ERR:code\\x02 sentinel).
 *
 * When the brpc server calls cntl->SetFailed("...\\x01DS_ERR:5\\x02"), and the
 * response protobuf has no ErrorInfoPb field, this function recovers the server
 * error code from the Controller's error text.
 *
 * @param errorText The error text from brpc::Controller::ErrorText().
 * @return The extracted Status, or K_RPC_CANCELLED if parsing fails.
 */
inline Status TryExtractStatusFromControllerError(const std::string &errorText)
{
    // Search for the \x01DS_ERR:<code>\x02 sentinel set by SendStatus.
    // \x01/\x02 (SOH/STX) delimiters prevent false matches on business error
    // text that may contain the literal substring "DS_ERR:".
    // Use rfind to match the LAST (outermost) DS_ERR sentinel. In a call
    // chain A→B→C, if C returns DS_ERR:5 and B wraps it as DS_ERR:7,
    // the caller should see 7 (the error from the immediate callee).
    size_t dsPos = errorText.rfind("\x01" "DS_ERR:");
    if (dsPos != std::string::npos) {
        size_t codeStart = dsPos + 8;
        size_t codeEnd = errorText.find("\x02", codeStart);
        if (codeEnd != std::string::npos) {
            std::string codeStr = errorText.substr(codeStart, codeEnd - codeStart);
            long code = 0;
            if (TryParseDsErrCode(codeStr, code)) {
                // Strip sentinel + control bytes to produce clean message.
                std::string cleanMsg = "RPC failed: code=" + codeStr;
                return Status(static_cast<StatusCode>(code), __LINE__, __FILE__, cleanMsg);
            }
            LOG(WARNING) << "Corrupt DS_ERR sentinel in Controller error text: '"
                         << codeStr << "', falling back to K_RPC_CANCELLED";
        }
    }
    return Status(StatusCode::K_RPC_CANCELLED, __LINE__, __FILE__, "RPC cancelled");
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_BRPC_STATUS_UTIL_H
