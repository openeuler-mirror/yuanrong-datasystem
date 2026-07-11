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
#include <errno.h>
#include <string>
#include <vector>

#include <butil/errno.h>  // berror(): renders both system errno and brpc-registered errno to text
#include <google/protobuf/message.h>
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/// Timeout (seconds) for StreamClose to deliver on_closed/on_failed callback.
/// Shared by brpc client streaming adapters to bound destructor wait time.
inline constexpr int kStreamCloseTimeoutSec = 5;

/// Timeout (seconds) for a single Stream Read wait. Bounds the readCond_ wait so
/// a peer crash / network partition cannot block the calling bthread forever and
/// exhaust the bthread pool under nested RPC load. Must be > kStreamCloseTimeoutSec.
inline constexpr int kStreamReadTimeoutSec = 30;

/// brpc::Errno namespace values, mirrored from <brpc/errno.pb.h> (brpc 1.15.0,
/// pinned by cmake/external_libs/brpc.cmake). brpc::Controller::ErrorCode()
/// returns either a system errno (ECONNREFUSED, ETIMEDOUT, ...) or one of
/// these brpc-internal codes. Each constant is annotated with its brpc enum
/// name so a future brpc upgrade can be cross-checked against the source.
/// If brpc changes these values, the mismatch surfaces as a wrong/default
/// mapping (not a crash); bump the pin and re-verify against errno.pb.h.
inline constexpr int kBrpcErpcTimedOut = 1008;   // brpc::ERPCTIMEDOUT
inline constexpr int kBrpcFailedSocket = 1009;   // brpc::EFAILEDSOCKET
inline constexpr int kBrpcEof = 1014;            // brpc::EEOF
inline constexpr int kBrpcSsl = 1016;            // brpc::ESSL
inline constexpr int kBrpcReject = 1018;         // brpc::EREJECT
inline constexpr int kBrpcInternal = 2001;       // brpc::EINTERNAL
inline constexpr int kBrpcLogoff = 2003;         // brpc::ELOGOFF
inline constexpr int kBrpcClose = 2005;          // brpc::ECLOSE
inline constexpr int kBrpcDetailLogLevel = 2;

/**
 * @brief Map a brpc Controller ErrorCode() to a datasystem Status.
 *
 * brpc::Controller::ErrorCode() returns an int that is either a system errno
 * (ECONNREFUSED, ETIMEDOUT, ...) or a brpc-internal Errno (ERPCTIMEDOUT,
 * EFAILEDSOCKET, ...). Without this mapping every non-sentinel controller
 * failure surfaces as K_RPC_CANCELLED, masking connection/timeout errors as
 * "rpc cancelled" and blocking diagnosis of cross-node RPC failures.
 *
 * Mapping (kBrpc* constants mirror the brpc enum names shown in comments):
 *   - timeouts (kBrpcErpcTimedOut / ERPCTIMEDOUT, ETIMEDOUT)
 *                                              → K_RPC_DEADLINE_EXCEEDED
 *   - peer-unreachable (kBrpcFailedSocket / EFAILEDSOCKET,
 *     kBrpcLogoff / ELOGOFF, ECONNREFUSED, ECONNRESET, ECONNABORTED,
 *     EHOSTUNREACH, ENETUNREACH, ENOTCONN, EPIPE, EHOSTDOWN, ESHUTDOWN,
 *     kBrpcEof / EEOF, kBrpcClose / ECLOSE, kBrpcSsl / ESSL,
 *     kBrpcReject / EREJECT, kBrpcInternal / EINTERNAL)
 *                                              → K_RPC_UNAVAILABLE
 *   - ECANCELED (brpc/OS cancellation)         → K_RPC_CANCELLED (genuine)
 *   - everything else                          → K_RPC_CANCELLED (fallback,
 *     preserves prior behavior; message carries the raw code + text so the
 *     failure is no longer opaque)
 *
 * @param errorCode brpc::Controller::ErrorCode().
 * @param errorText brpc::Controller::ErrorText() (for the diagnostic message).
 * @return Status with a code that reflects the failure class.
 */
inline Status MapBrpcErrorCodeToStatus(int errorCode, const std::string &errorText)
{
    auto makeStatus = [&](StatusCode code, const char *hint) {
        // berror() renders brpc-registered errno (EFAILEDSOCKET, ELOGOFF, ...)
        // and system errno (ECONNREFUSED, ETIMEDOUT, ...) to a human-readable
        // name, so the StatusCode category plus this text lets an operator
        // tell exactly which transport failure occurred without memorizing
        // numeric codes. No __LINE__/__FILE__: they would all point at this
        // helper, which carries no diagnostic value.
        const char *errName = berror(errorCode);
        return Status(code,
                      std::string(hint) + " [brpc errno=" + std::to_string(errorCode)
                          + (errName != nullptr && errName[0] != '\0'
                                 ? std::string(" ") + errName
                                 : std::string())
                          + "] " + errorText);
    };
    switch (errorCode) {
        // Timeouts → deadline exceeded
        case kBrpcErpcTimedOut:
        case ETIMEDOUT:
            return makeStatus(StatusCode::K_RPC_DEADLINE_EXCEEDED, "RPC timed out");
        // Peer not reachable → unavailable
        case kBrpcFailedSocket:
        case kBrpcEof:
        case kBrpcSsl:
        case kBrpcReject:
        case kBrpcInternal:
        case kBrpcLogoff:
        case kBrpcClose:
        case ECONNREFUSED:
        case ECONNRESET:
        case ECONNABORTED:
        case EHOSTUNREACH:
        case ENETUNREACH:
        case ENOTCONN:
        case EPIPE:
        case EHOSTDOWN:
        case ESHUTDOWN:
            return makeStatus(StatusCode::K_RPC_UNAVAILABLE, "RPC peer unavailable");
        // Genuine cancellation (brpc/OS) — keep K_RPC_CANCELLED
        case ECANCELED:
            return makeStatus(StatusCode::K_RPC_CANCELLED, "RPC cancelled");
        default:
            break;
    }
    return makeStatus(StatusCode::K_RPC_CANCELLED, "RPC failed");
}

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
 * If no DS_ERR sentinel is present, the failure is a brpc-internal/transport
 * error (connection refused, timeout, peer down, ...) rather than a server
 * application error. In that case the server never ran application code, so
 * there is no embedded code to recover — fall back to MapBrpcErrorCodeToStatus()
 * which maps the brpc ErrorCode() to K_RPC_UNAVAILABLE / K_RPC_DEADLINE_EXCEEDED
 * / K_RPC_CANCELLED so the failure class is visible instead of being masked as
 * a generic "rpc cancelled".
 *
 * @param errorText The error text from brpc::Controller::ErrorText().
 * @param errorCode brpc::Controller::ErrorCode() (0 = caller did not capture it;
 *                  treated as unknown → K_RPC_CANCELLED fallback, preserving
 *                  prior behavior for call sites not yet updated).
 * @return The extracted Status, or a mapped transport Status if no sentinel.
 */
inline Status TryExtractStatusFromControllerError(const std::string &errorText, int errorCode = 0)
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
                // Keep the server error code and the full brpc ErrorText so
                // on-call retains the original application error context (the
                // sentinel itself is stripped). No __LINE__/__FILE__: they
                // would point at this helper, not the failing call site.
                std::string cleanMsg = "RPC failed: code=" + codeStr + "; " + errorText;
                VLOG(kBrpcDetailLogLevel)
                    << "[BRPC_STATUS] Extract DS_ERR from controller error, code=" << codeStr
                    << ", errorTextLen=" << errorText.size();
                return Status(static_cast<StatusCode>(code), cleanMsg);
            }
            LOG(WARNING) << "Corrupt DS_ERR sentinel in Controller error text: '"
                         << codeStr << "', falling back to brpc errno mapping";
        }
    }
    // No (parseable) DS_ERR sentinel → transport/brpc-internal failure.
    // Map the brpc ErrorCode so connection/timeout errors are not masked as
    // K_RPC_CANCELLED. When errorCode == 0 (call site did not capture it),
    // MapBrpcErrorCodeToStatus falls through to K_RPC_CANCELLED, preserving
    // prior behavior for legacy callers.
    VLOG(1) << "[BRPC_STATUS] No DS_ERR in controller error, fallback to brpc errno mapping"
            << ", errorCode=" << errorCode << ", errorTextLen=" << errorText.size()
            << ", errorText=" << FormatStringForLog(errorText);
    return MapBrpcErrorCodeToStatus(errorCode, errorText);
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_BRPC_STATUS_UTIL_H
