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

/** Description: Implements fixed-location replica polling for object reads. */

#include "datasystem/client/transport/object_read/replica_reader.h"

#include <cstddef>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

ReplicaReader::ReplicaReader(std::shared_ptr<DataPlaneExecutor> executor, std::shared_ptr<DeadlineRetry> retry)
    : executor_(std::move(executor)), retry_(std::move(retry))
{
}

bool ReplicaReader::IsRetryableLocationError(const Status &status) const
{
    if (retry_->IsRetryableRpcError(status)) {
        return true;
    }
    switch (status.GetCode()) {
        case K_URMA_NEED_CONNECT:
        case K_URMA_CONNECT_FAILED:
        case K_WORKER_PULL_OBJECT_NOT_FOUND:
        case K_NOT_FOUND:
        case K_OUT_OF_MEMORY:
            return true;
        default:
            return false;
    }
}

Status ReplicaReader::Read(const master::ObjectLocationInfoPb &location, ObjectReadItemResult &result)
{
    RETURN_RUNTIME_ERROR_IF_NULL(executor_);
    RETURN_RUNTIME_ERROR_IF_NULL(retry_);
    CHECK_FAIL_RETURN_STATUS(location.object_locations_size() > 0, K_NOT_FOUND, "Object was not found");
    int64_t backoffMs = 1;
    size_t round = 0;
    Status lastError(K_NOT_FOUND, "Cannot get objects from worker");
    while (true) {
        ++round;
        for (const auto &address : location.object_locations()) {
            RETURN_IF_NOT_OK(retry_->CheckDeadline());
            HostPort workerAddr;
            RETURN_IF_NOT_OK(workerAddr.ParseString(address));
            VLOG(1) << "[TransportGet][Data] Read replica, key: " << location.object_key()
                    << ", worker: " << workerAddr.ToString() << ", round: " << round;
            DataGetResult data;
            DataGetRequest request{ location.object_key(), location.object_size() };
            Status rc = executor_->Execute(workerAddr, [&request, &data](IDataTransporter &transporter) {
                return transporter.Get(request, data);
            });
            if (rc.IsOk()) {
                result.objectKey = location.object_key();
                result.data = std::move(data);
                VLOG(1) << "[TransportGet][Data] Read succeeded, key: " << location.object_key()
                        << ", worker: " << workerAddr.ToString() << ", round: " << round;
                return Status::OK();
            }
            const bool retryable = IsRetryableLocationError(rc);
            VLOG(1) << "[TransportGet][Data] Replica read failed, key: " << location.object_key()
                    << ", worker: " << workerAddr.ToString() << ", round: " << round
                    << ", retryable: " << retryable << ", status: " << rc.ToString();
            if (!retryable) {
                return rc;
            }
            lastError = rc;
        }
        Status backoffRc = retry_->Backoff(backoffMs);
        if (backoffRc.IsError()) {
            backoffRc.AppendMsg(lastError.GetMsg());
            return backoffRc;
        }
    }
}
}  // namespace client
}  // namespace datasystem
