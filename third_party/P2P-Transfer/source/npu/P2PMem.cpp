/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/P2PMem.h"

P2PMem::~P2PMem()
{
    if (type == MemType::LOCAL_MEM) {
        aclrtFree(mem);
        rtIpcDestroyMemoryName(memName.data());
    } else if (type == MemType::REMOTE_MEM) {
        rtIpcCloseMemory(mem);
    }
}

void *P2PMem::get()
{
    return mem;
}

std::array<char, MEM_NAME_LENGTH> P2PMem::getName()
{
    return memName;
}

Status P2PMem::alloc(size_t size, aclrtMemMallocPolicy policy)
{
    ACL_CHECK_STATUS(aclrtMalloc((void **)&mem, size, policy));
    ACL_CHECK_STATUS(rtIpcSetMemoryName(mem, size, memName.data(), MEM_NAME_LENGTH));
    type = MemType::LOCAL_MEM;
    return Status::Success();
}

Status P2PMem::allowAccess(int32_t pid)
{
    if (type == MemType::REMOTE_MEM) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "allowAccess cannot be called on remote P2P memory");
    }

    ACL_CHECK_STATUS(rtSetIpcMemPid(memName.data(), &pid, 1));
    return Status::Success();
}

Status P2PMem::open(std::array<char, MEM_NAME_LENGTH> &openName)
{
    if (type != MemType::MEM_UNINITIALIZED) {
        return Status::Error(ErrorCode::REPEAT_INITIALIZE, "Memory already initialized");
    }

    ACL_CHECK_STATUS(rtIpcOpenMemory(&mem, openName.data()));
    type = MemType::REMOTE_MEM;
    return Status::Success();
}