/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_LOCAL_NOTIFY_H
#define P2P_LOCAL_NOTIFY_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "tools/Status.h"
#include "tools/npu-error.h"
#include "acl/acl.h"
#include "experiment/msprof/toolchain/prof_api.h"
#include "runtime/event.h"

enum NotifyState { NOTIFY_INITIALIZED, NOTIFY_UNINITIALIZED };

class LocalNotify {
public:
    LocalNotify() : state(NotifyState::NOTIFY_UNINITIALIZED) {}
    ~LocalNotify();

    LocalNotify(const LocalNotify &) = delete;
    LocalNotify &operator=(const LocalNotify &) = delete;

    void *get();

    // Create notify on specific deviceId (device where wait is to be called)
    Status create(uint32_t deviceId);

    // Block stream until notify is recorded
    Status wait(aclrtStream stream);

    // Record notify on a specific stream
    Status record(aclrtStream stream);

private:
    NotifyState state;
    rtNotify_t notify = nullptr;
};

#endif  // P2P_LOCAL_NOTIFY_H