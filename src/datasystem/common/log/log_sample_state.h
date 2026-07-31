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
 * Description: Transport-neutral helpers for request log sampling state.
 *
 * GetOrCreateLogSampleState() serializes the current thread/bthread Trace
 * request-sampling decision into the LogSampleState enum carried by RPC
 * metadata (zmq MetaPb.log_sample_state, or the brpc trace attachment state
 * byte). ApplyLogSampleState() restores that decision into the receiving
 * side's Trace. Both transports (zmq MetaPb, brpc attachment) share these
 * helpers so the sampling contract has exactly one definition.
 *
 * The LogSampleState enum itself lives in the transport-neutral
 * datasystem/protos/log_sample.proto (imported by meta_zmq.proto for the
 * MetaPb field), so this log module header depends on log_sample_cc_proto and
 * does NOT pull the zmq-specific meta_zmq proto.
 */

#ifndef DATASYSTEM_COMMON_LOG_LOG_SAMPLE_STATE_H
#define DATASYSTEM_COMMON_LOG_LOG_SAMPLE_STATE_H

#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/protos/log_sample.pb.h"

namespace datasystem {

inline LogSampleState GetOrCreateLogSampleState()
{
    if (!Trace::Instance().IsRequestLogTrace()) {
        return LOG_SAMPLE_NONE;
    }

    if (LogSampler::Instance().IsSamplerEnabledFast()) {
        bool sampledIn = LogSampler::Instance().IsCurrentRequestSampledIn();
        bool admitted = false;
        bool hasDecision = Trace::Instance().GetRequestSampleDecision(admitted);
        if (hasDecision) {
            return admitted ? LOG_SAMPLE_ADMIT : LOG_SAMPLE_REJECT;
        }

        if (sampledIn) {
            return LOG_SAMPLE_UNDECIDED;
        } else {
            Trace::Instance().SetRequestSampleDecision(true, false);
            return LOG_SAMPLE_REJECT;
        }
    }

    return LOG_SAMPLE_UNDECIDED;
}

inline void ApplyLogSampleState(LogSampleState state)
{
    switch (state) {
        case LOG_SAMPLE_UNDECIDED: {
            Trace::Instance().SetRequestLogTrace(true);
            Trace::Instance().SetRequestSampleDecision(false, false);
            if (LogSampler::Instance().IsSamplerEnabledFast()) {
                (void)LogSampler::Instance().IsCurrentRequestSampledIn();
            }
            break;
        }
        case LOG_SAMPLE_ADMIT:
            Trace::Instance().SetRequestLogTrace(true);
            Trace::Instance().SetRequestSampleDecision(true, true);
            break;
        case LOG_SAMPLE_REJECT:
            Trace::Instance().SetRequestLogTrace(true);
            Trace::Instance().SetRequestSampleDecision(true, false);
            break;
        case LOG_SAMPLE_NONE:
        default:
            Trace::Instance().SetRequestLogTrace(false);
            Trace::Instance().SetRequestSampleDecision(false, false);
            break;
    }
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_LOG_SAMPLE_STATE_H
