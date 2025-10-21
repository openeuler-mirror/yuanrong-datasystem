/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_NOTIFY_H
#define P2P_NOTIFY_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "tools/Status.h"
#include "tools/npu-error.h"
#include "acl/acl.h"
#include "experiment/msprof/toolchain/prof_api.h"
#include "runtime/event.h"

constexpr uint32_t NOTIFY_NAME_LENGTH = 65;  // Needs to be 65, otherwise setNotifyName does not work

enum NotifyType { P2P_LOCAL_NOTIFY, P2P_REMOTE_NOTIFY, P2P_NOTIFY_UNINITIALIZED };

class P2PNotify {
public:
    P2PNotify() : type(NotifyType::P2P_NOTIFY_UNINITIALIZED), isAddressSet(false), isIdSet(false) {}
    ~P2PNotify();

    P2PNotify(const P2PNotify &) = delete;
    P2PNotify &operator=(const P2PNotify &) = delete;

    void *get();

    // Get notify name, which can be used by a remote NPU to record a notify on another NPU
    std::array<char, NOTIFY_NAME_LENGTH> getName();

    // Create notify on specific deviceId (device where wait is to be called)
    Status create(uint32_t deviceId);

    // Allow remote process to record notify
    Status allowAccess(int32_t pid);

    // Open notify on remote NPU
    Status open(std::array<char, NOTIFY_NAME_LENGTH> &openName);

    // Block stream until notify is recorded
    Status wait(aclrtStream stream);

    // Record notify on a specific stream
    Status record(aclrtStream stream);

    Status getAddr(uint64_t *notifyAddr);
    Status getId(uint32_t *notifyID);

private:
    NotifyType type;

    rtNotify_t notify = nullptr;

    bool isAddressSet;
    uint64_t notifyAddr;
    bool isIdSet;
    uint32_t notifyID;

    std::array<char, NOTIFY_NAME_LENGTH> notifyName;
};

#endif  // P2P_NOTIFY_H