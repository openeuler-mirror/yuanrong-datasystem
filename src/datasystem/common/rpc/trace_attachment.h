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
 * Description: Attach per-request traceID to brpc request attachment.
 *
 * Wire format: [8-byte magic "TRCID:V1"][4-byte uint32_t len][traceID bytes]
 * The server adapter strips this prefix in CallMethod before dispatching.
 */

#ifndef DATASYSTEM_COMMON_RPC_TRACE_ATTACHMENT_H
#define DATASYSTEM_COMMON_RPC_TRACE_ATTACHMENT_H

#include <cstdint>
#include <cstring>

#include <butil/iobuf.h>

#include "datasystem/common/log/trace.h"

namespace datasystem {

inline void AttachTraceIDToAttachment(butil::IOBuf &buf)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    const char *traceID = Trace::Instance().GetTraceIDPtr();
    const size_t traceIDSize = std::strlen(traceID);
    if (traceIDSize == 0) {
        return;
    }
    const char magic[8] = {'T', 'R', 'C', 'I', 'D', ':', 'V', '1'};
    buf.append(magic, sizeof(magic));
    uint32_t len = static_cast<uint32_t>(traceIDSize);
    buf.append(&len, sizeof(len));
    buf.append(traceID, traceIDSize);
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_TRACE_ATTACHMENT_H
