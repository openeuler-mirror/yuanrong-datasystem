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

#include "datasystem/common/rpc/trace_attachment.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "gtest/gtest.h"

namespace datasystem {
namespace {
constexpr char TRACE_MAGIC[] = "TRCID:V1";
constexpr size_t TRACE_MAGIC_SIZE = sizeof(TRACE_MAGIC) - 1;

std::string ExtractTraceId(butil::IOBuf attachment)
{
    char magic[TRACE_MAGIC_SIZE];
    if (attachment.copy_to(magic, sizeof(magic)) != sizeof(magic)
        || std::memcmp(magic, TRACE_MAGIC, sizeof(magic)) != 0) {
        return {};
    }
    attachment.pop_front(sizeof(magic));

    uint32_t traceLength = 0;
    if (attachment.copy_to(&traceLength, sizeof(traceLength)) != sizeof(traceLength)) {
        return {};
    }
    attachment.pop_front(sizeof(traceLength));
    if (traceLength == 0 || traceLength > static_cast<uint32_t>(Trace::TRACEID_MAX_SIZE)
        || attachment.size() < traceLength) {
        return {};
    }

    std::string traceId(traceLength, '\0');
    if (attachment.copy_to(traceId.data(), traceLength) != traceLength) {
        return {};
    }
    return traceId;
}
}  // namespace

TEST(TraceAttachmentTest, PreservesExistingTraceId)
{
    const std::string expectedTraceId = "existing-brpc-trace";
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(expectedTraceId);
    butil::IOBuf attachment;

    AttachTraceIDToAttachment(attachment);

    EXPECT_EQ(ExtractTraceId(attachment), expectedTraceId);
    EXPECT_EQ(Trace::Instance().GetTraceID(), expectedTraceId);
}

TEST(TraceAttachmentTest, GeneratesTraceIdWhenCallerContextIsEmpty)
{
    Trace::Instance().Invalidate();
    butil::IOBuf attachment;

    AttachTraceIDToAttachment(attachment);

    EXPECT_FALSE(ExtractTraceId(attachment).empty());
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());
}
}  // namespace datasystem
