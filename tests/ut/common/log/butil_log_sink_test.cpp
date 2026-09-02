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

#include <memory>

#include <gtest/gtest.h>

#include "datasystem/common/log/butil_log_sink.h"
#include "datasystem/common/log/butil_log_sink_lease.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace {

class RecordingButilSink final : public logging::LogSink {
public:
    bool OnLogMessage(int, const char *, int, const butil::StringPiece &) override
    {
        return true;
    }
};

TEST(ButilLogSinkTest, NonLifoLeasesRestorePreviousSinkOnlyAfterLastRelease)
{
    RecordingButilSink previous;
    RecordingButilSink probe;
    auto *original = logging::SetLogSink(&previous);

    auto first = std::make_unique<ButilLogSinkLease>();
    auto second = std::make_unique<ButilLogSinkLease>();
    first.reset();

    auto *active = logging::SetLogSink(&probe);
    EXPECT_NE(active, &previous);
    logging::SetLogSink(active);

    second.reset();
    EXPECT_EQ(logging::SetLogSink(&probe), &previous);
    logging::SetLogSink(original);
}

TEST(ButilLogSinkTest, SuppressesButilFallbackWhenSeverityIsFiltered)
{
    ButilLogSink sink;
    const auto previousMinLogLevel = FLAGS_minloglevel;
    FLAGS_minloglevel = static_cast<int32_t>(LogSeverity::FATAL);
    EXPECT_TRUE(sink.OnLogMessage(logging::BLOG_WARNING, __FILE__, __LINE__, butil::StringPiece("warning")));
    FLAGS_minloglevel = previousMinLogLevel;
}

}  // namespace
}  // namespace datasystem
