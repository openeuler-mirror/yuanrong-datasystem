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

#ifndef __MOCKCPP_JMP_CODE_H__
#define __MOCKCPP_JMP_CODE_H__

#include <stddef.h>

#if defined(__aarch64__)

#define BUILD_FOR_AARCH64

#elif (defined(__LP64__) || defined(__64BIT__) || defined(_LP64) || ((defined(__WORDSIZE)) && (__WORDSIZE == 64)) \
       || defined(WIN64))

#define BUILD_FOR_X64

#else

#define BUILD_FOR_X86

#endif

struct JmpCodeImpl;

struct JmpCode {
    JmpCode(const void *from, const void *to);
    ~JmpCode();

    void *getCodeData() const;
    size_t getCodeSize() const;
    void flushCache() const;

private:
    JmpCodeImpl *This;
};

#endif