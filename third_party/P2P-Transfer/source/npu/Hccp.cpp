/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/Hccp.h"
#include "external/tsd.h"

// HCCP process started if >= 2
constexpr uint32_t TSD_OPEN_DEFAULT_RANK_SIZE = 2;

Hccp::~Hccp()
{
    stop();
}

Status Hccp::start()
{
    if (status == HccpStatus::HCCP_STARTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "HCCP is already running");
    }

    ACL_CHECK_STATUS(TsdOpen(devId, TSD_OPEN_DEFAULT_RANK_SIZE));
    status = HccpStatus::HCCP_STARTED;
    return Status::Success();
}

Status Hccp::stop()
{
    if (status != HccpStatus::HCCP_STARTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "HCCP is not running");
    }

    ACL_CHECK_STATUS(TsdClose(devId));
    status = HccpStatus::HCCP_UNINITIALIZED;
    return Status::Success();
}