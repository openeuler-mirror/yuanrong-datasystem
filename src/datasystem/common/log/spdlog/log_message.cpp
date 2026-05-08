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

/**
 * Description: Log message.
 */
#include "datasystem/common/log/spdlog/log_message.h"
#include "datasystem/common/log/spdlog/log_message_impl.h"
#include "datasystem/common/log/spdlog/log_rate_limiter.h"
#include "datasystem/common/log/spdlog/log_severity.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {
bool ShouldCreateLogMessage(LogSeverity logSeverity)
{
    auto level = ToSpdlogLevel(logSeverity);
    if (level >= ds_spdlog::level::err) {
        return true;
    }

    auto &trace = Trace::Instance();
    bool admitted = false;
    if (trace.GetRequestSampleDecision(admitted)) {
        return admitted;
    }

    auto &limiter = LogRateLimiter::Instance();
    if (!limiter.IsEnabled()) {
        return true;
    }

    uint64_t traceHash = trace.IsRequestLogTrace() ? trace.GetCachedHash() : uint64_t(0);
    return limiter.ShouldLog(level, traceHash);
}

LogMessage::LogMessage(LogSeverity logSeverity, const char *file, int line)
{
    impl_ = std::make_shared<LogMessageImpl>(logSeverity, file, line);
}

std::ostream &LogMessage::Stream()
{
    return impl_->Stream();
}
}  // namespace datasystem
