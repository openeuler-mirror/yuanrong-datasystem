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

#ifndef __MOCKCPP_JMP_CODE_AARCH64_H__
#define __MOCKCPP_JMP_CODE_AARCH64_H__

#include <asm/unistd.h>
#include <cstdint>
#include <cerrno>
#include <fcntl.h>
#include <cstdio>
#include <cstdlib>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

struct l2cache_addr_range {
    uintptr_t start;
    uintptr_t end;
};

#define ADDR_ALIGN_UP(addr) ((((addr) + ((4096) - 1)) & (~((4096) - 1))) & 0xffffffffffffffff)
#define ADDR_ALIGN_DOWN(addr) (((addr) & (~((4096) - 1))) & 0xffffffffffffffff)
#define OUTER_CACHE_INV_RANGE _IOWR('S', 0x00, struct l2cache_addr_range)
#define OUTER_CACHE_CLEAN_RANGE _IOWR('S', 0x01, struct l2cache_addr_range)
#define OUTER_CACHE_FLUSH_RANGE _IOWR('S', 0x02, struct l2cache_addr_range)
#define L1_INV_I_CACHE _IOWR('S', 0x03, struct l2cache_addr_range)
#define D_TO_I_CACHEFLUSH_RANGE _IOWR('S', 0x04, struct l2cache_addr_range)

const unsigned char jmpCodeTemplate[] = { 0x00, 0x00, 0x00, 0x00 };

#define SET_JMP_CODE(base, from, to)                                           \
    do {                                                                       \
        using instruct_t = signed int;                                         \
        instruct_t offset = (instruct_t)((long)to) - (instruct_t)((long)from); \
        offset = ((offset >> 2) & 0x03FFFFFF) | 0x14000000;                    \
        *(instruct_t *)(base) = offset;                                        \
    } while (0)

#define FLUSH_CACHE(from, length)                                              \
    do {                                                                       \
        struct l2cache_addr_range usr_data;                                    \
        usr_data.start = ADDR_ALIGN_DOWN((unsigned long long)from);            \
        usr_data.end = ADDR_ALIGN_UP((unsigned long long)from) + length;       \
        __builtin___clear_cache((void *)usr_data.start, (void *)usr_data.end); \
    } while (0)
#endif