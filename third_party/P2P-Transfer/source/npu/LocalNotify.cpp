
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
#include "npu/LocalNotify.h"

LocalNotify::~LocalNotify()
{
    if (state != NotifyState::NOTIFY_UNINITIALIZED) {
        NPU_ERROR(rtNotifyDestroy(notify));
    }
}

void *LocalNotify::get()
{
    return notify;
}

Status LocalNotify::create(uint32_t deviceId)
{
    ACL_CHECK_STATUS(rtNotifyCreate(deviceId, &notify));
    state = NotifyState::NOTIFY_INITIALIZED;
    return Status::Success();
}

Status LocalNotify::wait(aclrtStream stream)
{
    if (state == NotifyState::NOTIFY_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Wait called on uninitialized notify");
    }

    ACL_CHECK_STATUS(rtNotifyWait(notify, stream));
    return Status::Success();
}

Status LocalNotify::record(aclrtStream stream)
{
    if (state == NotifyState::NOTIFY_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Record called on uninitialized notify");
    }

    ACL_CHECK_STATUS(rtNotifyRecord(notify, stream));
    return Status::Success();
}