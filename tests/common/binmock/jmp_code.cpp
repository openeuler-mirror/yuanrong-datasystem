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

#include "jmp_code.h"

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "jmp_code_aarch.h"

#define JMP_CODE_SIZE sizeof(jmpCodeTemplate)

#ifdef BUILD_FOR_AARCH64
#include <sys/mman.h>
#include <unistd.h>

// On aarch64 the inline jump patch is a single B instruction whose 26-bit
// immediate can only reach +/-128MB. When the stub lives far away from the
// patched function (common with ASLR + many shared libs), the offset is
// silently truncated and execution jumps to a garbage address. To stay within
// 4 bytes at the patch site, allocate a 16-byte trampoline within +/-128MB of
// the patched function: `LDR x16,#8; BR x16; .quad target` performs an absolute
// indirect jump to the real stub. The 4-byte patch site then only needs a near
// B into the trampoline.
struct JmpCodeImpl {
    static constexpr size_t TRAMP_SIZE = 16;  // LDR x16,#8(4) + BR x16(4) + addr(8)
    static constexpr long B_LIMIT = 128L * 1024 * 1024;

    static void *MakeTrampoline(const void *from, const void *to)
    {
        uintptr_t base = reinterpret_cast<uintptr_t>(from) & ~0xfffUL;
        // Scan page-aligned hints within +/-128MB of `from`. Linux/aarch64
        // honours the hint when the region is free, so the first usable slot is
        // expected to be close to `from`.
        for (long off = 0; off < B_LIMIT; off += 0x10000) {
            for (int sign = -1; sign <= 1; sign += 2) {
                void *hint = reinterpret_cast<void *>(base + sign * off);
                void *p = mmap(hint, TRAMP_SIZE, PROT_READ | PROT_WRITE | PROT_EXEC,
                               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                if (p == MAP_FAILED) {
                    continue;
                }
                if (llabs(reinterpret_cast<long>(p) - reinterpret_cast<long>(from)) <= B_LIMIT) {
                    uint32_t insn[2] = { 0x58000050u, 0xd61f0200u };  // LDR x16,#8 ; BR x16
                    std::memcpy(p, insn, sizeof(insn));
                    uint64_t addr = reinterpret_cast<uint64_t>(to);
                    std::memcpy(static_cast<char *>(p) + 8, &addr, sizeof(addr));
                    __builtin___clear_cache(static_cast<char *>(p), static_cast<char *>(p) + TRAMP_SIZE);
                    return p;
                }
                munmap(p, TRAMP_SIZE);
            }
        }
        return nullptr;  // No slot within +/-128MB; caller aborts to avoid a truncated B.
    }

    JmpCodeImpl(const void *from, const void *to)
    {
        m_from = from;
        m_trampoline = nullptr;
        const void *target = to;
        if (llabs(reinterpret_cast<long>(to) - reinterpret_cast<long>(from)) > B_LIMIT) {
            m_trampoline = MakeTrampoline(from, to);
            if (m_trampoline == nullptr) {
                // No usable trampoline slot within +/-128MB. Emitting a 4-byte
                // B here would silently truncate the offset and branch to a
                // garbage address (the original bug). Fail deterministically
                // instead of producing a known-broken patch.
                fprintf(stderr,
                        "binmock: cannot patch %p -> %p (distance %ld > %ld B-limit): "
                        "no free trampoline slot within range. Aborting to avoid a "
                        "truncated jump.\n",
                        from, to, reinterpret_cast<long>(to) - reinterpret_cast<long>(from), B_LIMIT);
                abort();
            }
            target = m_trampoline;
        }
        std::memcpy(m_code, jmpCodeTemplate, JMP_CODE_SIZE);
        SET_JMP_CODE(m_code, from, target);
    }

    ~JmpCodeImpl()
    {
        if (m_trampoline != nullptr) {
            munmap(m_trampoline, TRAMP_SIZE);
        }
    }

    void *getCodeData() const
    {
        return (void *)m_code;
    }

    size_t getCodeSize() const
    {
        return JMP_CODE_SIZE;
    }

    void flushCache() const
    {
        FLUSH_CACHE(m_from, JMP_CODE_SIZE);
        if (m_trampoline != nullptr) {
            __builtin___clear_cache(static_cast<char *>(m_trampoline),
                                    static_cast<char *>(m_trampoline) + TRAMP_SIZE);
        }
    }

    unsigned char m_code[JMP_CODE_SIZE];
    const void *m_from;
    void *m_trampoline;
};
#else
struct JmpCodeImpl {
    JmpCodeImpl(const void *from, const void *to)
    {
        m_from = from;
        std::memcpy(m_code, jmpCodeTemplate, JMP_CODE_SIZE);
        SET_JMP_CODE(m_code, from, to);
    }

    void *getCodeData() const
    {
        return (void *)m_code;
    }

    size_t getCodeSize() const
    {
        return JMP_CODE_SIZE;
    }

    void flushCache() const
    {
        FLUSH_CACHE(m_from, JMP_CODE_SIZE);
    }

    unsigned char m_code[JMP_CODE_SIZE];
    const void *m_from;
};
#endif

JmpCode::JmpCode(const void *from, const void *to) : This(new JmpCodeImpl(from, to))
{
}

JmpCode::~JmpCode()
{
    delete This;
}

void *JmpCode::getCodeData() const
{
    return This->getCodeData();
}

size_t JmpCode::getCodeSize() const
{
    return This->getCodeSize();
}

void JmpCode::flushCache() const
{
    This->flushCache();
}