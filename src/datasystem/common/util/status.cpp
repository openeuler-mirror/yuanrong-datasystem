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
bool HasCurrentTraceId(const std::string &msg, const std::string &traceId)
{
    if (msg.empty() || traceId.empty()) {
        return false;
    }
    return msg.find(traceId) != std::string::npos;
}

Status::Status() noexcept : state_(nullptr)
{
}

Status::Status(const Status &other) noexcept
{
    Assign(other);
}

Status &Status::operator=(const Status &other) noexcept
{
    Assign(other);
    return *this;
}

Status::Status(Status &&other) noexcept
{
    std::swap(state_, other.state_);
}

Status &Status::operator=(Status &&other) noexcept
{
    std::swap(state_, other.state_);
    return *this;
}

Status::Status(StatusCode code, std::string msg) noexcept
{
    if (code == StatusCode::K_OK) {
        return;
    }
    auto traceId = Trace::Instance().GetTraceID();
    if (code != K_OK && !traceId.empty() && !HasCurrentTraceId(msg, traceId)) {
        if (!msg.empty() && msg.back() == '.') {
            msg.pop_back();
        }
        msg += ", traceId: " + traceId;
    }
    state_ = std::make_unique<State>();
    state_->code = code;
    state_->errMsg = std::move(msg);
}

Status::Status(StatusCode code, int lineOfCode, const std::string &fileName, const std::string &extra)
{
     if (code == StatusCode::K_OK) {
        return;
    }
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
    if (code != K_OK && !traceId.empty() && !HasCurrentTraceId(ss.str(), traceId)) {
        ss << "traceId      : " << traceId;
    }
    state_ = std::make_unique<State>();
    state_->code = code;
    state_->errMsg = ss.str();
}

std::ostream &operator<<(std::ostream &os, const Status &s)
{
    os << s.ToString();
    return os;
}

std::string Status::ToString() const
{
    return "code: [" + StatusCodeName(GetCode()) + "], msg: [" + GetMsg() + "]";
}

StatusCode Status::GetCode() const
{
    return state_ == nullptr ? K_OK : state_->code;
}

std::string Status::GetMsg() const
{
    return state_ == nullptr ? "" : state_->errMsg;
}

void Status::AppendMsg(const std::string &appendMsg)
{
    if (IsOk()) {
        return;
    }
    auto &errMsg = state_->errMsg;
    errMsg += (!errMsg.empty() && errMsg.back() != '.' ? ". " : " ") + appendMsg;
}

void Status::Assign(const Status &other) noexcept
{
    if (other.IsOk()) {
        state_ = nullptr;
        return;
    }
    if (state_ == nullptr) {
        state_ = std::make_unique<State>();
    }
    *state_ = *other.state_;
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
    else if (name == #code) {          \
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
