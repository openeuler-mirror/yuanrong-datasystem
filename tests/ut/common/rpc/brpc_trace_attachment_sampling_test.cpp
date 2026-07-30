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
 * Description: Test request-log sampling state propagation through the brpc
 * request attachment. Mirrors ZmqTraceSamplingMetaTest for the brpc transport:
 * AttachTraceIDToAttachment() encodes traceID + 1-byte LogSampleState;
 * ExtractTraceIDAndSampleState() decodes it; ApplyLogSampleState() restores
 * requestLogTrace + sampleDecision on the receiving side so the generated
 * brpc CallMethod handler participates in LogSampler instead of being
 * bypassed (the regression this guards against).
 */

#include "datasystem/common/rpc/trace_attachment.h"

#include <cstdint>
#include <cstring>
#include <string>

#include <gtest/gtest.h>

#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
namespace ut {

namespace {
constexpr char kTraceMagic[] = { 'T', 'R', 'C', 'I', 'D', ':', 'V', '1' };
constexpr size_t kTraceMagicSize = sizeof(kTraceMagic);

// Build an attachment frame for a given traceID + LogSampleState, mirroring
// the wire format produced by AttachTraceIDToAttachment(). Used to exercise
// the decode path independently of the encode-side sampler state.
butil::IOBuf BuildAttachment(const std::string &traceID, LogSampleState state)
{
    butil::IOBuf buf;
    buf.append(kTraceMagic, kTraceMagicSize);
    const uint32_t len = static_cast<uint32_t>(traceID.size());
    buf.append(&len, sizeof(len));
    buf.append(traceID.data(), traceID.size());
    const uint8_t stateByte = static_cast<uint8_t>(state);
    buf.append(&stateByte, sizeof(stateByte));
    return buf;
}

// Build a legacy frame that carries traceID but no trailing state byte (the
// pre-fix wire format). Used to verify the defensive UNDECIDED default.
butil::IOBuf BuildLegacyAttachmentNoState(const std::string &traceID)
{
    butil::IOBuf buf;
    buf.append(kTraceMagic, kTraceMagicSize);
    const uint32_t len = static_cast<uint32_t>(traceID.size());
    buf.append(&len, sizeof(len));
    buf.append(traceID.data(), traceID.size());
    return buf;
}
}  // namespace

class BrpcTraceAttachmentSamplingTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        Trace::Instance().Invalidate();
        LogSampler::Instance().ResetForTest();
        GetRequestContext()->reqTimeoutDuration.Init();
    }

    void TearDown() override
    {
        Trace::Instance().Invalidate();
        LogSampler::Instance().ResetForTest();
    }

    // Enable the sampler with an explicit non-1.0 request rate so the ADMIT /
    // REJECT decision paths in GetOrCreateLogSampleState() are exercised.
    void EnableSampler()
    {
        LogSampleUserConfig cfg;
        cfg.requestSampleRate = 0.5;
        cfg.requestSampleRateExplicit = true;
        LogSampler::Instance().UpdateConfigFromFlags(cfg);
    }
};

// Round-trip: encode via AttachTraceIDToAttachment(), decode via
// ExtractTraceIDAndSampleState(), restore via ApplyLogSampleState(). Verifies
// the wire format carries the sampling decision end-to-end and that the
// receiving-side Trace ends up in the same state the sender had.
TEST_F(BrpcTraceAttachmentSamplingTest, RoundTripCarriesAdmittedDecision)
{
    EnableSampler();
    TraceGuard reqGuard = Trace::Instance().SetRequestTraceUUID();
    Trace::Instance().SetRequestSampleDecision(true, true);  // ADMIT
    const std::string encodedTraceId = Trace::Instance().GetTraceID();
    ASSERT_FALSE(encodedTraceId.empty());
    butil::IOBuf attachment;
    AttachTraceIDToAttachment(attachment);
    ASSERT_FALSE(attachment.empty());

    Trace::Instance().Invalidate();  // simulate a fresh receiving thread
    butil::IOBuf buf = attachment;
    std::string traceID;
    LogSampleState state = LOG_SAMPLE_UNDECIDED;
    ExtractTraceIDAndSampleState(buf, traceID, state);
    EXPECT_EQ(traceID, encodedTraceId);
    EXPECT_EQ(state, LOG_SAMPLE_ADMIT);
    TraceGuard guard = Trace::Instance().SetTraceNewID(traceID);
    ApplyLogSampleState(state);
    EXPECT_TRUE(Trace::Instance().IsRequestLogTrace());
    bool admitted = false;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_TRUE(admitted);
}

TEST_F(BrpcTraceAttachmentSamplingTest, RoundTripCarriesRejectedDecision)
{
    EnableSampler();
    TraceGuard reqGuard = Trace::Instance().SetRequestTraceUUID();
    Trace::Instance().SetRequestSampleDecision(true, false);  // REJECT
    butil::IOBuf attachment;
    AttachTraceIDToAttachment(attachment);
    ASSERT_FALSE(attachment.empty());

    // Capture the encoded traceID before clearing the receiver trace.
    const std::string encodedTraceId = [&] {
        butil::IOBuf tmp = attachment;
        std::string t;
        LogSampleState s = LOG_SAMPLE_UNDECIDED;
        ExtractTraceIDAndSampleState(tmp, t, s);
        return t;
    }();
    ASSERT_FALSE(encodedTraceId.empty());

    Trace::Instance().Invalidate();
    butil::IOBuf buf = attachment;
    std::string traceID;
    LogSampleState state = LOG_SAMPLE_UNDECIDED;
    ExtractTraceIDAndSampleState(buf, traceID, state);
    EXPECT_EQ(traceID, encodedTraceId);
    EXPECT_EQ(state, LOG_SAMPLE_REJECT);
    TraceGuard guard = Trace::Instance().SetTraceNewID(traceID);
    ApplyLogSampleState(state);
    EXPECT_TRUE(Trace::Instance().IsRequestLogTrace());
    bool admitted = true;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_FALSE(admitted);
}

TEST_F(BrpcTraceAttachmentSamplingTest, RoundTripCarriesUndecidedWhenSamplerDisabled)
{
    // Sampler disabled (default after ResetForTest): a request trace encodes
    // UNDECIDED so the receiver makes its own local decision.
    TraceGuard reqGuard = Trace::Instance().SetRequestTraceUUID();
    butil::IOBuf attachment;
    AttachTraceIDToAttachment(attachment);
    ASSERT_FALSE(attachment.empty());

    butil::IOBuf buf = attachment;
    std::string traceID;
    LogSampleState state = LOG_SAMPLE_ADMIT;  // poison
    ExtractTraceIDAndSampleState(buf, traceID, state);
    EXPECT_FALSE(traceID.empty());
    EXPECT_EQ(state, LOG_SAMPLE_UNDECIDED);
    TraceGuard guard = Trace::Instance().SetTraceNewID(traceID);
    ApplyLogSampleState(state);
    EXPECT_TRUE(Trace::Instance().IsRequestLogTrace());
    bool admitted = false;
    // Sampler disabled -> UNDECIDED does not synthesize a local decision.
    EXPECT_FALSE(Trace::Instance().GetRequestSampleDecision(admitted));
}

TEST_F(BrpcTraceAttachmentSamplingTest, RoundTripCarriesNoneForBackgroundTrace)
{
    // A background (non-request) trace encodes NONE; the receiver must NOT be
    // marked as a request trace, so its logs bypass request sampling.
    TraceGuard bgGuard = Trace::Instance().SetTraceNewID("bg-trace");
    Trace::Instance().SetRequestLogTrace(false);
    butil::IOBuf attachment;
    AttachTraceIDToAttachment(attachment);
    ASSERT_FALSE(attachment.empty());

    butil::IOBuf buf = attachment;
    std::string traceID;
    LogSampleState state = LOG_SAMPLE_ADMIT;  // poison
    ExtractTraceIDAndSampleState(buf, traceID, state);
    EXPECT_EQ(traceID, "bg-trace");
    EXPECT_EQ(state, LOG_SAMPLE_NONE);
    TraceGuard guard = Trace::Instance().SetTraceNewID(traceID);
    ApplyLogSampleState(state);
    EXPECT_FALSE(Trace::Instance().IsRequestLogTrace());
    bool admitted = false;
    EXPECT_FALSE(Trace::Instance().GetRequestSampleDecision(admitted));
}

// Defensive default: a frame that ends right after the traceID (no state byte,
// e.g. a peer that predates this change) must decode to UNDECIDED so the
// receiver still participates in local sampling rather than being bypassed.
TEST_F(BrpcTraceAttachmentSamplingTest, AbsentStateByteDefaultsToUndecided)
{
    const std::string traceID = "legacy-trace";
    butil::IOBuf buf = BuildLegacyAttachmentNoState(traceID);
    std::string outTraceID;
    LogSampleState state = LOG_SAMPLE_ADMIT;  // poison
    ExtractTraceIDAndSampleState(buf, outTraceID, state);
    EXPECT_EQ(outTraceID, traceID);
    EXPECT_EQ(state, LOG_SAMPLE_UNDECIDED);
}

// Out-of-range state byte (e.g. corrupted/trailing payload byte read as state
// on a single-version-mismatch) must not produce a bogus enum; fall back to
// UNDECIDED.
TEST_F(BrpcTraceAttachmentSamplingTest, OutOfRangeStateByteDefaultsToUndecided)
{
    butil::IOBuf buf;
    buf.append(kTraceMagic, kTraceMagicSize);
    const uint32_t len = 4;
    buf.append(&len, sizeof(len));
    buf.append("abcd", 4);
    const uint8_t badStateByte = 7;  // > LOG_SAMPLE_UNDECIDED (3)
    buf.append(&badStateByte, sizeof(badStateByte));
    std::string outTraceID;
    LogSampleState state = LOG_SAMPLE_NONE;  // poison (non-default)
    ExtractTraceIDAndSampleState(buf, outTraceID, state);
    EXPECT_EQ(outTraceID, "abcd");
    EXPECT_EQ(state, LOG_SAMPLE_UNDECIDED);
}

// No magic prefix (raw/external brpc client): decode leaves traceID empty and
// state UNDECIDED; the caller mints a fresh trace.
TEST_F(BrpcTraceAttachmentSamplingTest, NoMagicLeavesTraceIdEmpty)
{
    butil::IOBuf buf;
    buf.append("plain payload bytes", 20);
    std::string outTraceID = "poison";
    LogSampleState state = LOG_SAMPLE_ADMIT;  // poison
    ExtractTraceIDAndSampleState(buf, outTraceID, state);
    EXPECT_TRUE(outTraceID.empty());
    EXPECT_EQ(state, LOG_SAMPLE_UNDECIDED);
    // Payload bytes are untouched (no magic to strip).
    EXPECT_EQ(buf.size(), 20u);
}

// Empty attachment (no payload at all): safe no-op.
TEST_F(BrpcTraceAttachmentSamplingTest, EmptyAttachmentIsSafe)
{
    butil::IOBuf buf;
    std::string outTraceID = "poison";
    LogSampleState state = LOG_SAMPLE_ADMIT;  // poison
    ExtractTraceIDAndSampleState(buf, outTraceID, state);
    EXPECT_TRUE(outTraceID.empty());
    EXPECT_EQ(state, LOG_SAMPLE_UNDECIDED);
}

// Direct encode/decode parity for each enum value via the manual frame
// builder, independent of the sampler's GetOrCreateLogSampleState() logic
// (which is already covered by ZmqTraceSamplingMetaTest).
TEST_F(BrpcTraceAttachmentSamplingTest, ManualFrameDecodesAllStates)
{
    const std::string traceID = "rpc-trace-x";
    for (uint8_t v = 0; v <= static_cast<uint8_t>(LOG_SAMPLE_UNDECIDED); ++v) {
        butil::IOBuf buf = BuildAttachment(traceID, static_cast<LogSampleState>(v));
        std::string outTraceID;
        LogSampleState state = LOG_SAMPLE_ADMIT;
        ExtractTraceIDAndSampleState(buf, outTraceID, state);
        EXPECT_EQ(outTraceID, traceID);
        EXPECT_EQ(state, static_cast<LogSampleState>(v));
    }
}

}  // namespace ut
}  // namespace datasystem
