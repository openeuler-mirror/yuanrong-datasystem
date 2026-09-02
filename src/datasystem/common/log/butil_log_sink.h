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
#ifndef DATASYSTEM_COMMON_LOG_BUTIL_LOG_SINK_H
#define DATASYSTEM_COMMON_LOG_BUTIL_LOG_SINK_H

#include <butil/logging.h>

namespace datasystem {

class ButilLogSink final : public logging::LogSink {
public:
    ButilLogSink();
    ~ButilLogSink() override;

    ButilLogSink(const ButilLogSink &) = delete;
    ButilLogSink &operator=(const ButilLogSink &) = delete;
    ButilLogSink(ButilLogSink &&) = delete;
    ButilLogSink &operator=(ButilLogSink &&) = delete;

    bool OnLogMessage(int severity, const char *file, int line,
                      const butil::StringPiece &logContent) override;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_BUTIL_LOG_SINK_H
