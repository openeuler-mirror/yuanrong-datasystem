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

#ifndef __MOCKCPP_JMP_CODE_ARCH_H__
#define __MOCKCPP_JMP_CODE_ARCH_H__

template <typename T>
inline T changeByteOrder(const T v)
{
    enum { S = sizeof(T) };
    T rst = v;
    char *p = (char *)&rst;
    char tmp = 0;
    for (unsigned int i = 0; i < S / 2; ++i) {
        tmp = p[i];
        p[i] = p[S - i - 1];
        p[S - i - 1] = tmp;
    }

    return rst;
}

#if defined(BUILD_FOR_X64)
#include "jmp_code_x64.h"
#elif defined(BUILD_FOR_X86)
#include "jmp_code_x86.h"
#elif defined(BUILD_FOR_AARCH32)
#include "jmp_code_aarch32.h"
#elif defined(BUILD_FOR_AARCH64)
#include "jmp_code_aarch64.h"
#endif
#endif