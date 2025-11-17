/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

#include "datasystem/worker/stream_cache/producer.h"

#include <climits>

#include "datasystem/common/constants.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
Producer::Producer(std::string producerId, std::string streamName, std::shared_ptr<Cursor> cursor)
    : id_(std::move(producerId)), streamName_(std::move(streamName)), cursor_(std::move(cursor))
{
}

std::string Producer::GetId() const
{
    return id_;
}

Status Producer::CleanupProducer()
{
    if (cursor_) {
        RETURN_IF_NOT_OK(cursor_->Init());
        RETURN_IF_NOT_OK(cursor_->SetLastPage(ShmView(), DEFAULT_TIMEOUT_MS));
    }
    return Status::OK();
}

void Producer::SetForceClose()
{
    if (cursor_) {
        cursor_->SetForceClose();
    }
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
