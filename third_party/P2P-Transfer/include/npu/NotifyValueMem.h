/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_NOTIFY_VAL_MEM_H
#define P2P_NOTIFY_VAL_MEM_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"
#include "runtime/mem.h"

constexpr uint64_t HUGE_PAGE_MEMORY_MIN_SIZE = 2 * 1024 * 1024;

class NotifyValueMem {
public:
    NotifyValueMem() {}
    ~NotifyValueMem();

    NotifyValueMem(const NotifyValueMem &) = delete;
    NotifyValueMem &operator=(const NotifyValueMem &) = delete;

    static Status get(void **notifySrcValAddr, uint32_t *notifyValSize)
    {
        if (srcValAddr == nullptr) {
            size_t notifyValueSize = HUGE_PAGE_MEMORY_MIN_SIZE;
            const int kMallocType = 3;
            ACL_CHECK_STATUS(rtMalloc(&srcValAddr, notifyValueSize, RT_MEMORY_P2P_HBM, kMallocType));
            uint64_t notifyValue = 1;
            srcValSize = 4;  // 910A: 8 bytes, 910B: 4 bytes (see hrtGetNotifySize).
            ACL_CHECK_STATUS(
                aclrtMemcpy(srcValAddr, notifyValueSize, &notifyValue, srcValSize, ACL_MEMCPY_HOST_TO_DEVICE));
        }

        *notifySrcValAddr = srcValAddr;
        *notifyValSize = srcValSize;
        return Status::Success();
    }

private:
    static void* srcValAddr;
    static uint32_t srcValSize;
};

#endif  // P2P_NOTIFY_VAL_MEM_H