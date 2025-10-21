/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

#include "function_stub.h"
#include "jmp_code.h"
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>

namespace {
constexpr auto LONG_JMP_BYTES = { 0x68, 0, 0, 0, 0, 0xC7, 0x44, 0x24, 0x04, 0, 0, 0, 0, 0xC3 };
constexpr auto SHORT_JMP_BYTES = { 0xE9, 0, 0, 0, 0 };

}  // namespace

namespace testing {
FunctionStub::FunctionStub(void *oldFunc, void *newFunc) : jmpCode_(oldFunc, newFunc)
{
    oldFunc_ = oldFunc;
    pageSize_ = GetPageSize();
    ReplaceFunc();
}

// Get the size of system page.
int64_t FunctionStub::GetPageSize() const
{
    int64_t pageSize = sysconf(_SC_PAGE_SIZE);
    if (pageSize <= 0) {
        pageSize = 4096;
    }
    return pageSize;
}

FunctionStub::~FunctionStub()
{
    RestoreFunc();
}

// Obtain the page address of the page where the function is located.
void *FunctionStub::PageOf(const void *p) const
{
    return (void *)((intptr_t)p & ~(pageSize_ - 1));
}

bool FunctionStub::RestoreFunc()
{
    // Request write access.
    if (-1 == mprotect(PageOf(oldFunc_), static_cast<size_t>(pageSize_ * 2), PROT_READ | PROT_WRITE | PROT_EXEC)) {
        return false;
    }

    std::copy(funcHeadBinary_.begin(), funcHeadBinary_.end(), reinterpret_cast<uint8_t *>(oldFunc_));
    // flush cache to avoid machine using the old function in aarch64.
    jmpCode_.flushCache();
    // restore access.
    mprotect(PageOf(oldFunc_), static_cast<size_t>(pageSize_ * 2), PROT_READ | PROT_EXEC);
    return true;
}

bool FunctionStub::ReplaceFunc()
{
    if (-1 == mprotect(PageOf(oldFunc_), pageSize_ * 2, PROT_READ | PROT_WRITE | PROT_EXEC)) {
        return false;
    }
    auto oldFunc = static_cast<uint8_t *>(oldFunc_);
    // backup the old function.
    funcHeadBinary_ = std::vector<uint8_t>(oldFunc, oldFunc + jmpCode_.getCodeSize());
    memcpy(oldFunc_, jmpCode_.getCodeData(), jmpCode_.getCodeSize());
    jmpCode_.flushCache();

    mprotect(PageOf(oldFunc_), static_cast<size_t>(pageSize_ * 2), PROT_READ | PROT_EXEC);
    return true;
}

void FunctionStub::ShortJmp(uint8_t *const function, intptr_t distance)
{
    funcHeadBinary_ = std::vector<uint8_t>(function, function + SHORT_JMP_BYTES.size());

    auto bytes = reinterpret_cast<const uint8_t *>(&distance);
    std::copy(SHORT_JMP_BYTES.begin(), SHORT_JMP_BYTES.end(), function);
    std::copy(bytes, bytes + 4, function + 1);
}

void FunctionStub::LongJmp(uint8_t *const function, const void *dest)
{
    funcHeadBinary_ = std::vector<uint8_t>(function, function + LONG_JMP_BYTES.size());
    /* push low 32bit
    mov [rsp + 4] high 32bit
    ret */
    auto bytes = reinterpret_cast<const uint8_t *>(&dest);
    std::copy(LONG_JMP_BYTES.begin(), LONG_JMP_BYTES.end(), function);
    std::copy(bytes, bytes + 4, function + 1);
    std::copy(bytes + 4, bytes + 8, function + 9);
}
}  // namespace testing