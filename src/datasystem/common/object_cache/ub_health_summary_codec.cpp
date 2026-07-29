/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "datasystem/common/object_cache/ub_health_summary_codec.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
void EncodeUbHealthSummary(const UbHealthSummary &summary, UbHealthSummaryPb &pb)
{
    pb.set_worker_address(summary.worker.ToString());
    pb.set_incarnation(summary.incarnation);
    pb.set_writable(summary.writable);
    pb.set_state(static_cast<int32_t>(summary.state));
    pb.set_reason(static_cast<int32_t>(summary.reason));
    pb.set_last_status_code(static_cast<int32_t>(summary.lastStatusCode));
    pb.set_epoch(summary.epoch);
    pb.set_backoff_level(summary.backoffLevel);
    pb.set_backoff_deadline_ms(summary.backoffDeadlineMs);
}

Status DecodeUbHealthSummary(const UbHealthSummaryPb &pb, UbHealthSummary &summary)
{
    RETURN_IF_NOT_OK(summary.worker.ParseString(pb.worker_address()));
    CHECK_FAIL_RETURN_STATUS(!pb.incarnation().empty(), K_INVALID, "UB health incarnation is empty");
    CHECK_FAIL_RETURN_STATUS(pb.state() >= static_cast<int32_t>(UbAdmissionState::AVAILABLE)
                                 && pb.state() <= static_cast<int32_t>(UbAdmissionState::PROBING),
                             K_INVALID, "Invalid UB admission state");
    CHECK_FAIL_RETURN_STATUS(pb.reason() >= static_cast<int32_t>(UbFailureClass::SUCCESS)
                                 && pb.reason() <= static_cast<int32_t>(UbFailureClass::NON_UB_FAILURE),
                             K_INVALID, "Invalid UB failure class");
    summary.incarnation = pb.incarnation();
    summary.writable = pb.writable();
    summary.state = static_cast<UbAdmissionState>(pb.state());
    summary.reason = static_cast<UbFailureClass>(pb.reason());
    summary.lastStatusCode = static_cast<StatusCode>(pb.last_status_code());
    summary.epoch = pb.epoch();
    summary.backoffLevel = pb.backoff_level();
    summary.backoffDeadlineMs = pb.backoff_deadline_ms();
    return Status::OK();
}

Status ApplyHeartbeatUbHealthSummary(const HeartbeatRspPb &rsp, const HostPort &expectedWorker,
                                     const std::string &expectedIncarnation, UbHealthSummaryCache &cache,
                                     const UbHealthSummaryApplyHook &hook)
{
    if (!rsp.has_ub_health_summary()) {
        return Status::OK();
    }
    UbHealthSummary summary;
    RETURN_IF_NOT_OK(DecodeUbHealthSummary(rsp.ub_health_summary(), summary));
    CHECK_FAIL_RETURN_STATUS(summary.worker == expectedWorker, K_INVALID,
                             "UB health summary Worker does not match heartbeat source");
    if (cache.Apply(summary, expectedIncarnation) && hook) {
        hook(summary);
    }
    return Status::OK();
}
}  // namespace datasystem
