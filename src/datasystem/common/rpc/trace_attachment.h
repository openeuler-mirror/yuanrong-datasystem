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
 * Description: Attach per-request traceID and log sampling state to the brpc
 * request attachment.
 *
 * Wire format:
 *   [8-byte magic "TRCID:V1"][4-byte uint32_t len][traceID bytes][1-byte LogSampleState]
 * The trailing 1-byte LogSampleState carries the request sampling decision
 * (NONE/ADMIT/REJECT/UNDECIDED) so the receiving side restores
 * requestLogTrace + sampleDecision via ApplyLogSampleState(), mirroring the
 * MetaPb.log_sample_state field. The server adapter strips this
 * prefix in CallMethod before dispatching. The magic version is intentionally
 * left at V1; the state byte is read defensively on the server (absent or
 * out-of-range byte falls back to LOG_SAMPLE_UNDECIDED), so this change
 * assumes a single-version deployment (no rolling upgrade with old binaries
 * that emit the format without the trailing state byte).
 */

#ifndef DATASYSTEM_COMMON_RPC_TRACE_ATTACHMENT_H
#define DATASYSTEM_COMMON_RPC_TRACE_ATTACHMENT_H

#include <cstdint>
#include <cstring>

#include <butil/iobuf.h>

#include "datasystem/common/log/log_sample_state.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {

inline void AttachTraceIDToAttachment(butil::IOBuf &buf)
{
    // Capture the request sampling state BEFORE any trace manipulation so the
    // SDK request scope's requestLogTrace/decision is read intact. SetTraceUUID
    // below mints a fresh UUID only when the trace is empty; when an SDK
    // SetRequestTraceUUID scope is active the traceID is already set and
    // SetTraceUUID returns an INVALID guard without touching requestLogTrace.
    const LogSampleState sampleState = GetOrCreateLogSampleState();
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    const char *traceID = Trace::Instance().GetTraceIDPtr();
    const size_t traceIDSize = std::strlen(traceID);
    if (traceIDSize == 0) {
        return;
    }
    const char magic[8] = { 'T', 'R', 'C', 'I', 'D', ':', 'V', '1' };
    buf.append(magic, sizeof(magic));
    uint32_t len = static_cast<uint32_t>(traceIDSize);
    buf.append(&len, sizeof(len));
    buf.append(traceID, traceIDSize);
    // Append the 1-byte sampling state so the worker side can restore
    // requestLogTrace/decision and participate in LogSampler.
    const uint8_t stateByte = static_cast<uint8_t>(sampleState);
    buf.append(&stateByte, sizeof(stateByte));
}

/**
 * Strip the [TRCID:V1][len][traceID][1B state] prefix produced by
 * AttachTraceIDToAttachment from the front of an incoming request attachment.
 *
 * On match: traceID is filled and state is set to the propagated
 * LogSampleState. If the trailing state byte is absent (e.g. a legacy frame
 * ending right after traceID) or out of range, state keeps the UNDECIDED
 * default so the receiver makes its own local sampling decision instead of
 * bypassing sampling entirely.
 *
 * On no match (external/raw brpc client with no trace prefix): traceID stays
 * empty and state stays UNDECIDED; the caller mints a fresh traceID.
 *
 * The remaining attachment after the prefix is the original payload (if any).
 */
inline void ExtractTraceIDAndSampleState(butil::IOBuf &buf, std::string &traceID, LogSampleState &state)
{
    traceID.clear();
    state = LOG_SAMPLE_UNDECIDED;
    const char magic[8] = { 'T', 'R', 'C', 'I', 'D', ':', 'V', '1' };
    char magicBuf[sizeof(magic)];
    constexpr size_t kMinTraceAttachment = sizeof(magic) + sizeof(uint32_t);
    if (buf.size() < kMinTraceAttachment
        || buf.copy_to(magicBuf, sizeof(magicBuf)) != sizeof(magicBuf)
        || std::memcmp(magicBuf, magic, sizeof(magic)) != 0) {
        return;
    }
    buf.pop_front(sizeof(magic));
    uint32_t len = 0;
    buf.copy_to(&len, sizeof(len));
    buf.pop_front(sizeof(len));
    if (len == 0 || len > static_cast<uint32_t>(Trace::TRACEID_MAX_SIZE) || buf.size() < len) {
        return;
    }
    traceID.resize(len);
    buf.copy_to(&traceID[0], len);
    buf.pop_front(len);
    // Read the 1-byte LogSampleState that follows the traceID. Absent or
    // out-of-range byte keeps the UNDECIDED default.
    if (buf.size() >= 1) {
        uint8_t stateByte = 0;
        buf.copy_to(&stateByte, sizeof(stateByte));
        buf.pop_front(sizeof(stateByte));
        if (stateByte <= static_cast<uint8_t>(LOG_SAMPLE_UNDECIDED)) {
            state = static_cast<LogSampleState>(stateByte);
        }
    }
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_TRACE_ATTACHMENT_H
