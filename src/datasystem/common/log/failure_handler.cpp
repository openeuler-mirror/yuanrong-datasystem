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
 * Description: Log failure handler.
 */
#include "datasystem/common/log/failure_handler.h"

#include <cstdlib>
#include <iostream>
#include <string>

#include <limits.h>

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/spdlog/provider.h"

DS_DECLARE_string(log_dir);

namespace datasystem {

void FailureWriter(const char *data)
{
    // Null `data` is a hint to flush any buffered data before the program may be terminated.
    if (!data) {
        Provider::Instance().FlushLogs();
        return;
    }

    char resolvedPath[PATH_MAX + 1] = { 0 };
    std::string logDir = FLAGS_log_dir;
    if (realpath(logDir.c_str(), resolvedPath) == nullptr) {
        return;
    }

    std::string backtracePath = std::string(resolvedPath) + "/container.log";
    (void)Logging::WriteLogToFile(__LINE__, __FILE__, backtracePath, 'E', std::string(data));
}

void InstallFailureSignalHandler(const char *arg0)
{
    absl::InitializeSymbolizer(arg0);

    absl::FailureSignalHandlerOptions options;
    options.call_previous_handler = true;
    options.writerfn = FailureWriter;
    options.alarm_on_failure_secs = 0;

    absl::InstallFailureSignalHandler(options);
}

}  // namespace datasystem
