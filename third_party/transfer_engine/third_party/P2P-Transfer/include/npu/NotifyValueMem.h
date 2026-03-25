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
    NotifyValueMem()
    {
    }
    ~NotifyValueMem();

    NotifyValueMem(const NotifyValueMem &) = delete;
    NotifyValueMem &operator=(const NotifyValueMem &) = delete;

    Status alloc();
    Status get(void **notifySrcValAddr, uint32_t *notifyValSize);

private:
    bool initialized = false;
    void *srcValAddr = nullptr;
    uint32_t srcValSize;
};

#endif  // P2P_NOTIFY_VAL_MEM_H