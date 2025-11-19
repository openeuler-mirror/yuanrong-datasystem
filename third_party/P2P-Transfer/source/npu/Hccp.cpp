/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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