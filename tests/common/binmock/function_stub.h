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

#ifndef __FUNCTION_STUB__
#define __FUNCTION_STUB__

#include <cstdint>
#include <vector>

#include "jmp_code.h"

namespace testing {
class FunctionStub {
public:
    FunctionStub(void *oldFunc, void *newFunc);
    ~FunctionStub();

private:
    int64_t GetPageSize() const;
    void *PageOf(const void *ptr) const;
    bool ReplaceFunc();
    bool RestoreFunc();
    void ShortJmp(uint8_t *const function, intptr_t distance);
    void LongJmp(uint8_t *const function, const void *dest);
    void ArmJmp(uint8_t *const oldFunc, void *newFunc);

    FunctionStub(const FunctionStub &) = delete;
    FunctionStub(FunctionStub &&) = delete;
    FunctionStub &operator=(const FunctionStub &) = delete;
    FunctionStub &operator=(FunctionStub &&) = delete;

private:
    void *oldFunc_{ nullptr };
    std::vector<uint8_t> funcHeadBinary_;
    int64_t pageSize_;
    JmpCode jmpCode_;
};

template <typename T>
void *AddrOf(T function)
{
    union {
        void *addr;
        T func;
    };
    func = function;
    return addr;
}

#define PATCH(FUNC_OBJ, FUNC_TO) FunctionStub __patch_(AddrOf(FUNC_OBJ), AddrOf(FUNC_TO));
}  // namespace testing
#endif