/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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