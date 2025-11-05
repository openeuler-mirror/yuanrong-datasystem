/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: This file is used to read data in the server.
 */
#include "datasystem/common/util/status_helper.h"

#include "datasystem/common/log/log.h"
#include "datasystem/kv/read_only_buffer.h"

namespace datasystem {
int64_t ReadOnlyBuffer::GetSize() const
{
    if (buffer_ == nullptr) {
        LOG(ERROR) << "buffer is not initialized";
        return 0;
    }
    return buffer_->GetSize();
}

const void *ReadOnlyBuffer::ImmutableData()
{
    if (buffer_ == nullptr) {
        LOG(ERROR) << "buffer is not initialized";
        return nullptr;
    }
    return buffer_->ImmutableData();
}

Status ReadOnlyBuffer::RLatch(uint64_t timeout)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(buffer_ != nullptr, K_RUNTIME_ERROR, "buffer is not initialized");
    return buffer_->RLatch(timeout);
}

Status ReadOnlyBuffer::UnRLatch()
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(buffer_ != nullptr, K_RUNTIME_ERROR, "buffer is not initialized");
    return buffer_->UnRLatch();
}
}  // namespace datasystem
