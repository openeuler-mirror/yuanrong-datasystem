/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#include "npu/NotifyValueMem.h"

NotifyValueMem::~NotifyValueMem()
{
    if (initialized) {
        aclrtFree(srcValAddr);
    }
}

Status NotifyValueMem::alloc()
{
    if (initialized) {
        return Status::Error(ErrorCode::REPEAT_INITIALIZE, "Memory already initialized");
    }
    size_t notifyValueSize = HUGE_PAGE_MEMORY_MIN_SIZE;
    const int HCCLType = 3;
    ACL_CHECK_STATUS(rtMalloc(&srcValAddr, notifyValueSize, RT_MEMORY_P2P_HBM, HCCLType));
    uint64_t notifyValue = 1;
    srcValSize = 4;  // 910A: 8 bytes, 910B: 4 bytes (see hrtGetNotifySize). Later make dynamic
    ACL_CHECK_STATUS(aclrtMemcpy(srcValAddr, notifyValueSize, &notifyValue, srcValSize, ACL_MEMCPY_HOST_TO_DEVICE));

    initialized = true;

    return Status::Success();
}

Status NotifyValueMem::get(void **notifySrcValAddr, uint32_t *notifyValSize)
{
    if (!initialized) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "memory not yet initialized");
    }
    *notifySrcValAddr = srcValAddr;
    *notifyValSize = srcValSize;
    return Status::Success();
}
