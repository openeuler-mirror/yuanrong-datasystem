/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_MEM_H
#define P2P_MEM_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "tools/Status.h"
#include "tools/npu-error.h"
#include "runtime/mem.h"
#include "acl/acl.h"

constexpr uint32_t MEM_NAME_LENGTH = 65;  // Needs to be 65, otherwise setNotifyName does not work

enum MemType {
    LOCAL_MEM,   // Physically located on local NPU
    REMOTE_MEM,  // Remote NPU mapped memory
    MEM_UNINITIALIZED
};

class P2PMem {
public:
    P2PMem() : type(MemType::MEM_UNINITIALIZED), initialized(false) {}
    ~P2PMem();

    P2PMem(const P2PMem &) = delete;
    P2PMem &operator=(const P2PMem &) = delete;

    // Get memory pointer
    void *get();

    // Get memory name, which can by a remote NPU to be mapped
    std::array<char, MEM_NAME_LENGTH> getName();

    // Allocate local memory
    Status alloc(size_t size, aclrtMemMallocPolicy policy);

    // Allow remote process to access local memory
    Status allowAccess(int32_t pid);

    // Open remote memory
    Status open(std::array<char, MEM_NAME_LENGTH> &openName);

private:
    MemType type;
    bool initialized;

    void *mem = nullptr;
    std::array<char, MEM_NAME_LENGTH> memName;
};

#endif  // P2P_MEM_H