/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/P2PNotify.h"
#include "runtime/event.h"
#include "runtime/kernel.h"

P2PNotify::~P2PNotify()
{
    if (type != NotifyType::P2P_NOTIFY_UNINITIALIZED) {
        NPU_ERROR(rtNotifyDestroy(notify));
    }
}

std::array<char, NOTIFY_NAME_LENGTH> P2PNotify::getName()
{
    return notifyName;
}

void *P2PNotify::get()
{
    return notify;
}

Status P2PNotify::create(uint32_t deviceId)
{
    ACL_CHECK_STATUS(rtNotifyCreate(deviceId, &notify));
    ACL_CHECK_STATUS(rtIpcSetNotifyName(notify, notifyName.data(), NOTIFY_NAME_LENGTH));
    type = NotifyType::P2P_LOCAL_NOTIFY;
    return Status::Success();
}

Status P2PNotify::allowAccess(int32_t pid)
{
    if (type == NotifyType::P2P_REMOTE_NOTIFY) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "allowAccess cannot be called on remote P2P notify");
    }

    ACL_CHECK_STATUS(rtSetIpcNotifyPid(notifyName.data(), &pid, 1));
    return Status::Success();
}

Status P2PNotify::open(std::array<char, NOTIFY_NAME_LENGTH> &openName)
{
    if (type != NotifyType::P2P_NOTIFY_UNINITIALIZED) {
        return Status::Error(ErrorCode::REPEAT_INITIALIZE, "Notify already initialized");
    }

    ACL_CHECK_STATUS(rtIpcOpenNotify(&notify, openName.data()));

    type = NotifyType::P2P_REMOTE_NOTIFY;
    return Status::Success();
}

Status P2PNotify::wait(aclrtStream stream)
{
    if (type == NotifyType::P2P_NOTIFY_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Wait called on uninitialized notify");
    }

    if (type == NotifyType::P2P_REMOTE_NOTIFY) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "Wait cannot be called on remote P2P notify");
    }
    ACL_CHECK_STATUS(rtNotifyWait(notify, stream));
    return Status::Success();
}

Status P2PNotify::record(aclrtStream stream)
{
    if (type == NotifyType::P2P_NOTIFY_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Record called on uninitialized notify");
    }

    if (type == NotifyType::P2P_LOCAL_NOTIFY) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "Record cannot be called on local P2P notify");
    }
    ACL_CHECK_STATUS(rtNotifyRecord(notify, stream));
    return Status::Success();
}

Status P2PNotify::getAddr(uint64_t *notifyAddr)
{
    if (!isAddressSet) {
        ACL_CHECK_STATUS(rtGetNotifyAddress(notify, &this->notifyAddr));
        isAddressSet = true;
    }
    *notifyAddr = this->notifyAddr;
    return Status::Success();
}

Status P2PNotify::getId(uint32_t *notifyID)
{
    if (!isIdSet) {
        ACL_CHECK_STATUS(rtGetNotifyID(notify, &this->notifyID));
        isIdSet = true;
    }
    *notifyID = this->notifyID;
    return Status::Success();
}