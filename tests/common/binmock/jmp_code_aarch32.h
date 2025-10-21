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

#ifndef __MOCKCPP_JMP_CODE_AARCH32_H__
#define __MOCKCPP_JMP_CODE_AARCH32_H__

#include <cstdlib>

const unsigned char jmpCodeTemplate[] = { 0xEA, 0x00, 0x00, 0x00 };

#define SET_JMP_CODE(base, from, to)            \
    do {                                        \
        int offset = (int)to - (int)from - 8;   \
        offset = (offset >> 2) & 0x00FFFFFF;    \
        int code = *(int *)(base) | offset;     \
        *(int *)(base) = changeByteOrder(code); \
    } while (0)

#define FLUSH_CACHE(from, length)                      \
    do {                                               \
        ::system("echo 3 > /proc/sys/vm/drop_caches"); \
    } while (0)

#endif