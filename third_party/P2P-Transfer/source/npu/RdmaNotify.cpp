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
#include "npu/RdmaNotify.h"

RdmaNotify::~RdmaNotify()
{
    if (type != RdmaNotifyType::UNINITIALIZED_RDMA_NOTIFY && type != RdmaNotifyType::REMOTE_RDMA_NOTIFY) {
        NPU_ERROR(rtNotifyDestroy(notify));
    }
}

void *RdmaNotify::get()
{
    return notify;
}

Status RdmaNotify::create(uint32_t deviceId)
{
    if (type != RdmaNotifyType::UNINITIALIZED_RDMA_NOTIFY) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Notify already initialized");
    }

    ACL_CHECK_STATUS(rtNotifyCreate(deviceId, &notify));
    type = RdmaNotifyType::LOCAL_RDMA_NOTIFY;
    return Status::Success();
}

Status RdmaNotify::getAddrOffset(uint64_t *notifyAddrOffset)
{
    if (type != RdmaNotifyType::LOCAL_RDMA_NOTIFY) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "getAddrOffset only supported on local notify");
    }
    ACL_CHECK_STATUS(rtNotifyGetAddrOffset(notify, notifyAddrOffset));
    return Status::Success();
}

Status RdmaNotify::wait(aclrtStream stream)
{
    if (type != RdmaNotifyType::LOCAL_RDMA_NOTIFY) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Wait only supported on local notify");
    }

    ACL_CHECK_STATUS(rtNotifyWait(notify, stream));
    return Status::Success();
}

Status RdmaNotify::open(uint64_t notifyRemoteAddr)
{
    if (type != RdmaNotifyType::UNINITIALIZED_RDMA_NOTIFY) {
        return Status::Error(ErrorCode::REPEAT_INITIALIZE, "Notify already initialized");
    }

    notifyAddr = notifyRemoteAddr;

    type = RdmaNotifyType::REMOTE_RDMA_NOTIFY;
    return Status::Success();
}

Status RdmaNotify::record(aclrtStream stream, RdmaQp* qp)
{
    if (type != RdmaNotifyType::REMOTE_RDMA_NOTIFY) {
        return Status::Error(ErrorCode::NOT_INITIALIZED,
                             "Record only supported on remote notify");
    }

    void* notifySrcValAddr;
    uint32_t notifySize;
    CHECK_STATUS(qp->getSrcValInfo(&notifySrcValAddr, &notifySize));

    CHECK_STATUS(qp->rdmaWrite(reinterpret_cast<uint64_t>(notifySrcValAddr),
                               notifyAddr,
                               notifySize,
                               stream));

    return Status::Success();
}

Status RdmaNotify::getRecordInfo(RdmaQp* qp,
                                 uint64_t* srcAddr,
                                 uint64_t* dstAddr,
                                 uint32_t* length)
{
    void* notifySrcValAddr;
    CHECK_STATUS(qp->getSrcValInfo(&notifySrcValAddr, length));

    *srcAddr = reinterpret_cast<uint64_t>(notifySrcValAddr);
    *dstAddr = notifyAddr;
    return Status::Success();
}

Status RdmaNotify::getId(uint32_t *notifyID)
{
    if (!isIdSet) {
        ACL_CHECK_STATUS(rtGetNotifyID(notify, &this->notifyID));
        isIdSet = true;
    }
    *notifyID = this->notifyID;
    return Status::Success();
}