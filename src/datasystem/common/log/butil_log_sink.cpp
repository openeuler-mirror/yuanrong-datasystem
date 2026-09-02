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
 * Description: Adapter from butil logging to the DataSystem logging provider.
 */
#include "datasystem/common/log/butil_log_sink.h"

#include <cstddef>
#include <memory>
#include <mutex>

#include "datasystem/common/log/butil_log_sink_lease.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/no_destructor.h"

namespace datasystem {
namespace {

struct ButilLogSinkState {
    std::mutex mutex;
    size_t leaseCount{ 0 };
    std::unique_ptr<ButilLogSink> sink;
    logging::LogSink *previousSink{ nullptr };
};

ButilLogSinkState &GetButilLogSinkState()
{
    static NoDestructor<ButilLogSinkState> state;
    return *state;
}

LogSeverity ToDatasystemSeverity(int severity)
{
    if (severity >= logging::BLOG_FATAL) {
        return LogSeverity::FATAL;
    }
    if (severity >= logging::BLOG_ERROR) {
        return LogSeverity::ERROR;
    }
    if (severity >= logging::BLOG_WARNING) {
        return LogSeverity::WARNING;
    }
    return LogSeverity::INFO;
}

}  // namespace

ButilLogSink::ButilLogSink() = default;

ButilLogSink::~ButilLogSink() = default;

ButilLogSinkLease::ButilLogSinkLease()
{
    auto &state = GetButilLogSinkState();
    std::lock_guard<std::mutex> lock(state.mutex);
    if (state.leaseCount++ == 0) {
        state.sink = std::make_unique<ButilLogSink>();
        state.previousSink = logging::SetLogSink(state.sink.get());
    }
}

ButilLogSinkLease::~ButilLogSinkLease()
{
    auto &state = GetButilLogSinkState();
    std::lock_guard<std::mutex> lock(state.mutex);
    if (--state.leaseCount == 0) {
        logging::SetLogSink(state.previousSink);
        state.sink.reset();
        state.previousSink = nullptr;
    }
}

bool ButilLogSink::OnLogMessage(int severity, const char *file, int line,
                                const butil::StringPiece &logContent)
{
    const auto logSeverity = ToDatasystemSeverity(severity);
    if (IsLogSeverityEnabled(logSeverity)) {
        LogMessage(logSeverity, file, line, true, true)
            .Stream()
            .write(logContent.data(), static_cast<std::streamsize>(logContent.size()));
    }
    return true;
}

}  // namespace datasystem
