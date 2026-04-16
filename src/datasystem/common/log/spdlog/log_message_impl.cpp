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

#include "datasystem/common/log/spdlog/log_message_impl.h"

#include <csignal>
#include <cstdlib>
#include <iostream>

#include <unistd.h>
#include <sys/syscall.h>

#include "datasystem/common/log/log_time.h"
#include "datasystem/common/log/spdlog/log_severity.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/trace.h"

DS_DEFINE_int32(v, 0, "Show all VLOG(m) messages for m <= this.");
DS_DECLARE_string(cluster_name);

namespace datasystem {
// thread_local for store log info
const size_t MAX_LOG_SIZE = 30000;
static thread_local char g_ThreadLogData[MAX_LOG_SIZE];

LogStreamBuf::LogStreamBuf(char *buf, int len)
{
    constexpr int TOTAL_RESERVED_BYTES = 2;
    setp(buf, buf + len - TOTAL_RESERVED_BYTES);
}

std::streambuf::int_type LogStreamBuf::overflow(int_type ch)
{
    return ch;
}

size_t LogStreamBuf::pcount() const
{
    return pptr() - pbase();
}

char *LogStreamBuf::pbase() const
{
    return std::streambuf::pbase();
}

static void AppendLogMessageImplPrefix(const std::string &podName, std::ostream &logStream)
{
    PerfPoint point(PerfKey::APPEND_LOG_MESSAGE_PREFIX);
    static const pid_t pid = getpid();
    static thread_local pid_t tid = syscall(__NR_gettid);

    logStream << podName << " | " << pid << ":" << tid << " | " << Trace::Instance().GetTraceID() << " | "
              << FLAGS_cluster_name << " |  ";
}

static DsLogger GetMessageLogger()
{
    PerfPoint point(PerfKey::GET_MESSAGE_LOGGER);
    if (Provider::IsAlive()) {
        auto lp = Provider::Instance().GetLoggerProvider();
        if (lp) {
            // thread_local cache: store shared_ptr to prevent provider address reuse after free.
            // When tests swap LoggerProvider in SetUp/TearDown, the old provider stays alive
            // (held by cachedProvider) until the next call updates the cache.
            thread_local DsLogger cachedLogger;
            thread_local std::shared_ptr<LoggerProvider> cachedProvider;
            if (cachedProvider == lp && cachedLogger) {
                return cachedLogger;
            }
            auto logger = lp->GetDsLogger();
            if (logger) {
                cachedLogger = logger;
                cachedProvider = lp;
                return logger;
            }
        }
    }
    return nullptr;
}

std::string LogMessageImpl::podName_ = Provider::GetPodName();

LogMessageImpl::LogMessageImpl(LogSeverity logSeverity, const char *file, int line)
    : level_(ToSpdlogLevel(logSeverity)),
      sourceLoc_{ file, line, "" },
      streamBuf_(g_ThreadLogData, MAX_LOG_SIZE),
      logStream_(&streamBuf_)
{
    Init();
}

LogMessageImpl::~LogMessageImpl()
{
    Flush();
}

std::ostream &LogMessageImpl::Stream()
{
    return logStream_;
}

void LogMessageImpl::Init()
{
    PerfPoint point(PerfKey::LOG_MESSAGE_INIT);
    logger_ = GetMessageLogger();
    if (logger_) {
        // Log rate sampling: check before formatting to avoid wasted work on dropped logs
        bool wasSampled = false;
        uint64_t traceHash = Trace::Instance().GetCachedHash();
        if (!LogRateLimiter::Instance().ShouldLog(level_, traceHash, &wasSampled)) {
            skip_ = true;
            return;
        }
        sampled_ = wasSampled;
        AppendLogMessageImplPrefix(podName_, logStream_);
    }
}

void LogMessageImpl::ToSpdlog()
{
    logger_->log(sourceLoc_, level_, ds_spdlog::string_view_t{g_ThreadLogData, msgSize_});

    if (level_ == SPDLOG_LEVEL_CRITICAL) {
        logger_->flush();
        (void)raise(SIGABRT);
    }
}

void LogMessageImpl::ToStderr()
{
    LogTime logTime;
    const char *LogSeverityName = GetLogSeverityName(level_ - 2);  // info(2) → INFO(0)
    const char *baseFilename = sourceLoc_.filename;
    const char *slash = ::strrchr(baseFilename, '/');
    if (slash != nullptr) {
        baseFilename = slash + 1;
    }

    ConstructLogPrefix(std::cerr, logTime.getTm(), logTime.getUsec(), baseFilename, sourceLoc_.line, podName_.c_str(),
                       LogSeverityName[0], FLAGS_cluster_name);

    std::cerr.write(g_ThreadLogData, static_cast<std::streamsize>(msgSize_));
    std::cerr << '\n';
}

void LogMessageImpl::Flush()
{
    if (skip_) {
        return;  // Dropped by log rate sampling, skip spdlog formatting and disk I/O
    }
    PerfPoint point(PerfKey::LOG_MESSAGE_FLUSH);
    msgSize_ = streamBuf_.pcount();
    if (logger_) {
        // Append sampling rate annotation only for logs kept through uniform-interval sampling fallback
        if (sampled_) {
            auto sampleRate = LogRateLimiter::Instance().GetSamplingRate();
            logStream_ << " [sampled 1/" << sampleRate << "]";
            msgSize_ = streamBuf_.pcount();
        }
        ToSpdlog();
    } else {
        ToStderr();
    }
}

}  // namespace datasystem