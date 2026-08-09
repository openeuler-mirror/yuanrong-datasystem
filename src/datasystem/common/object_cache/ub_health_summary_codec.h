/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_UB_HEALTH_SUMMARY_CODEC_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_UB_HEALTH_SUMMARY_CODEC_H

#include <functional>
#include <string>

#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/protos/share_memory.pb.h"

namespace datasystem {
using UbHealthSummaryApplyHook = std::function<void(const UbHealthSummary &)>;

void EncodeUbHealthSummary(const UbHealthSummary &summary, UbHealthSummaryPb &pb);
Status DecodeUbHealthSummary(const UbHealthSummaryPb &pb, UbHealthSummary &summary);
Status ApplyHeartbeatUbHealthSummary(const HeartbeatRspPb &rsp, const HostPort &expectedWorker,
                                     UbHealthSummaryCache &cache,
                                     const UbHealthSummaryApplyHook &hook = {});
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_UB_HEALTH_SUMMARY_CODEC_H
