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
 * Description: Status class.
 */
#include "datasystem/common/util/status_helper.h"

#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>

#include "datasystem/common/log/trace.h"

namespace datasystem {
Status::Status() noexcept : code_(StatusCode::K_OK)
{
}

Status &Status::operator=(const Status &other)
{
    if (this == &other) {
        return *this;
    }
    code_ = other.code_;
    errMsg_ = other.errMsg_;
    return *this;
}

Status::Status(Status &&other) noexcept
{
    code_ = other.code_;
    other.code_ = StatusCode::K_OK;
    errMsg_ = std::move(other.errMsg_);
}

Status &Status::operator=(Status &&other) noexcept
{
    if (this == &other) {
        return *this;
    }
    code_ = other.code_;
    other.code_ = StatusCode::K_OK;
    errMsg_ = std::move(other.errMsg_);
    return *this;
}

Status::Status(StatusCode code, std::string msg) : code_(code), errMsg_(std::move(msg))
{
    auto traceId = Trace::Instance().GetTraceID();
    if (code_ != K_OK && !traceId.empty() && errMsg_.find(traceId) == std::string::npos) {
        if (!errMsg_.empty() && errMsg_.back() == '.') {
            errMsg_.pop_back();
        }
        errMsg_ += ", traceId: " + traceId;
    }
}

Status::Status(StatusCode code, int lineOfCode, const std::string &fileName, const std::string &extra)
{
    this->code_ = code;
    std::ostringstream ss;
    ss << "Thread ID " << std::this_thread::get_id() << " " << StatusCodeName(code) << ". ";
    if (!extra.empty()) {
        ss << extra;
    }
    ss << std::endl;
    ss << "Line of code : " << lineOfCode << std::endl;
    if (!fileName.empty()) {
        size_t position = fileName.find_last_of('/') + 1;
        ss << "File         : " << fileName.substr(position, fileName.length() - position) << std::endl;
    }
    auto traceId = Trace::Instance().GetTraceID();
    if (code_ != K_OK && !traceId.empty() && errMsg_.find(traceId) == std::string::npos) {
        ss << "traceId      : " << traceId;
    }
    errMsg_ = ss.str();
}

std::ostream &operator<<(std::ostream &os, const Status &s)
{
    os << s.ToString();
    return os;
}

std::string Status::ToString() const
{
    return "code: [" + StatusCodeName(code_) + "], msg: [" + errMsg_ + "]";
}

StatusCode Status::GetCode() const
{
    return code_;
}

std::string Status::GetMsg() const
{
    return errMsg_;
}

void Status::AppendMsg(const std::string &appendMsg)
{
    errMsg_ +=  (!errMsg_.empty() && errMsg_.back() != '.' ? ". " : " ") + appendMsg;
}

// clang-format off.
#define STATUS_CODE_DEF(code, msg) \
    case StatusCode::code:         \
        returnMsg = (msg);         \
        break;
std::string Status::StatusCodeName(StatusCode code)
{
    // If using singleton, this method may be called after singleton destroy and coredump with "Segmentation fault".
    std::string returnMsg = "UNKNOWN_TYPE";
    switch (code) {
#include "datasystem/common/util/status_code.def"

        default:
            break;
    };

    return returnMsg;
}
#undef STATUS_CODE_DEF

#define STATUS_CODE_DEF(code, msg)     \
    else if (name == #code)            \
    {                                  \
        statusCode = StatusCode::code; \
    }
StatusCode GetStatusCodeByName(const std::string &name)
{
    StatusCode statusCode = StatusCode::K_INVALID;
    if (name.empty()) {
        statusCode = StatusCode::K_OK;
    }
#include "datasystem/common/util/status_code.def"

    return statusCode;
}
#undef STATUS_CODE_DEF
// clang-format on
}  // namespace datasystem
