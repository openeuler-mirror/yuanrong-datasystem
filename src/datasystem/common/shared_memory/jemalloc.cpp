/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

/**
 * Description: Define Jemalloc allocator class, Jemalloc is used to manage
 *              memory allocate and free.
 */
#include "datasystem/common/shared_memory/jemalloc.h"

#define JEMALLOC_NO_DEMANGLE
#include "jemalloc/jemalloc.h"
#undef JEMALLOC_NO_DEMANGLE

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/flags/flags.h"

DS_DEFINE_uint32(memory_alignment, 64, "Alignment for jemalloc");
DS_DEFINE_validator(memory_alignment, &Validator::ValidateMemoryAlignment);

namespace datasystem {
namespace memory {
::AllocHook *Jemalloc::alloc_ = nullptr;
::DestroyHook *Jemalloc::destroy_ = nullptr;
::CommitHook *Jemalloc::commit_ = nullptr;

void Jemalloc::Init(::AllocHook *alloc, ::DestroyHook *destroy, ::CommitHook *commit)
{
    alloc_ = alloc;
    destroy_ = destroy;
    commit_ = commit;
}

Status Jemalloc::CreateArena(ssize_t decayMs, unsigned &arenaInd, void *&handler)
{
    CHECK_FAIL_RETURN_STATUS(handler == nullptr, StatusCode::K_RUNTIME_ERROR, "handler is not null");
    // 1. Get arena index.
    size_t sizeOfArena = sizeof(arenaInd);
    if (auto ret = datasystem_mallctl("arenas.create", &arenaInd, &sizeOfArena, nullptr, 0)) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to create arena, errno: " + std::to_string(ret));
    }

    DLOG(INFO) << "arena index = " << arenaInd << ", decay ms:" << decayMs;
    // 2. Get arena original extent hooks via arena index.
    std::stringstream hooksKey;
    hooksKey << "arena." << arenaInd << ".extent_hooks";
    extent_hooks_t *origHooks = nullptr;
    size_t sizeOfHooks = sizeof(extent_hooks_t *);
    if (auto ret = datasystem_mallctl(hooksKey.str().c_str(), &origHooks, &sizeOfHooks, nullptr, 0)) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Unable to get the hooks, errno: " + std::to_string(ret));
    }

    // 3. Set the custom hook.
    auto hooks = std::make_unique<extent_hooks_t>();
    *hooks = *origHooks;
    hooks->alloc = &AllocHook;
    hooks->destroy = &DestroyHook;
    hooks->purge_lazy = &PurgeLazyHook;
    hooks->purge_forced = &PurgeForcedHook;
    hooks->commit = &CommitHook;
    hooks->decommit = &DecommitHook;
    auto *rawPointer = hooks.get();

    if (auto ret = datasystem_mallctl(hooksKey.str().c_str(), nullptr, nullptr, &rawPointer, sizeof(rawPointer))) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to set arena custom hook, errno: " + std::to_string(ret));
    }
    // 4. Set dirty decay and muzzy decay time. Using short dirty decay time, let the small extent become muzzy state
    // ASAP to trigger the merge operation
    std::stringstream dirtyDecayKey;
    const ssize_t shortDecayMs = 10'000;  // 10s
    auto dirtyDecayMs = std::min<ssize_t>(decayMs * 0.2, shortDecayMs);
    dirtyDecayKey << "arena." << arenaInd << ".dirty_decay_ms";
    if (auto ret =
            datasystem_mallctl(dirtyDecayKey.str().c_str(), nullptr, nullptr, &dirtyDecayMs, sizeof(dirtyDecayMs))) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      "Failed to set arena dirty decay time, errno: " + std::to_string(ret));
    }

    std::stringstream muzzyDecayKey;
    muzzyDecayKey << "arena." << arenaInd << ".muzzy_decay_ms";
    auto muzzyDecayMs = decayMs - dirtyDecayMs;
    if (auto ret =
            datasystem_mallctl(muzzyDecayKey.str().c_str(), nullptr, nullptr, &muzzyDecayMs, sizeof(muzzyDecayMs))) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      "Failed to set arena muzzy decay time, errno: " + std::to_string(ret));
    }
    handler = hooks.release();
    return Status::OK();
}

Status Jemalloc::DestroyArena(unsigned arenaInd)
{
    std::stringstream destroyKey;
    destroyKey << "arena." << arenaInd << ".destroy";
    if (auto ret = datasystem_mallctl(destroyKey.str().c_str(), nullptr, nullptr, nullptr, 0)) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR, "Failed to destroy arena, errno: " + std::to_string(ret));
    }
    return Status::OK();
}

void Jemalloc::DestroyHandler(void *handler)
{
    auto *hooks = (extent_hooks_t *)handler;
    delete hooks;
}

Status Jemalloc::Allocate(unsigned arenaInd, uint64_t &bytes, void *&pointer)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(bytes > 0, StatusCode::K_INVALID,
                                         "The value of bytes should be greater than 0");
    pointer = datasystem_mallocx(bytes, GetAllocxFlags(arenaInd));
    if (pointer == nullptr) {
        RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, "Allocate memory failed in arena: " + std::to_string(arenaInd));
    }
    bytes = GetAllocatedSize(pointer, arenaInd);
    return Status::OK();
}

unsigned int Jemalloc::GetAllocxFlags(unsigned int arenaInd)
{
    auto alignBits = FLAGS_memory_alignment;
    return MALLOCX_ARENA(arenaInd) | MALLOCX_TCACHE_NONE | MALLOCX_ALIGN(alignBits);
}

void Jemalloc::Free(unsigned arenaInd, void *pointer)
{
    PerfPoint point(PerfKey::JEMALLOC_FREE);
    if (pointer != nullptr) {
        datasystem_dallocx(pointer, GetAllocxFlags(arenaInd));
    }
}

size_t Jemalloc::GetAllocatedSize(void *pointer, unsigned arenaInd)
{
    return datasystem_sallocx(pointer, GetAllocxFlags(arenaInd));
}

void *Jemalloc::AllocHook(extent_hooks_t *extentHooks, void *newAddr, size_t size, size_t alignment, bool *zero,
                          bool *commit, unsigned arenaInd)
{
    (void)extentHooks;
    (void)newAddr;
    void *addr = alloc_(size, alignment, arenaInd, zero, commit);
    VLOG(3) << "Alloc arena: " << arenaInd << ", size: " << size << ", alignment: " << alignment << ", zero:" << *zero
            << ", commit:" << *commit;
    return addr;
}

void Jemalloc::DestroyHook(extent_hooks_t *extentHooks, void *addr, size_t size, bool committed, unsigned arenaInd)
{
    (void)extentHooks;
    VLOG(3) << "Destroy arena: " << arenaInd << ", size: " << size;
    destroy_(addr, size, committed, arenaInd);
}

bool Jemalloc::CommitHook(extent_hooks_t *extentHooks, void *addr, size_t size, size_t offset, size_t length,
                          unsigned arenaInd)
{
    (void)extentHooks;
    VLOG(3) << "Commit arena: " << arenaInd << ", size: " << size << ", offset: " << offset << ", length: " << length;
    return commit_(true, addr, size, offset, length, arenaInd);
}

bool Jemalloc::DecommitHook(extent_hooks_t *extentHooks, void *addr, size_t size, size_t offset, size_t length,
                            unsigned arenaInd)
{
    (void)extentHooks;
    VLOG(3) << "Decommit arena: " << arenaInd << ", size: " << size << ", offset: " << offset << ", length: " << length;
    return commit_(false, addr, size, offset, length, arenaInd);
}

bool Jemalloc::PurgeLazyHook(extent_hooks_t *extentHooks, void *addr, size_t size, size_t offset, size_t length,
                             unsigned arenaInd)
{
    (void)extentHooks;
    (void)addr;
    VLOG(3) << "Purge lazy arena: " << arenaInd << ", size: " << size << ", offset: " << offset
            << ", length: " << length;

    // Jemalloc default purge lazy implementation would invoke *madvise(xx, MADV_FREE)* to tell the kernel
    // can free these pages, but the freeing could be delayed until memory pressure occurs. However in shared
    // memory scenario, we don't need this operation, so let's return false directly and make jemalloc think purge lazy
    // success, and change state to muzzy.
    return false;
}

bool Jemalloc::PurgeForcedHook(extent_hooks_t *extentHooks, void *addr, size_t size, size_t offset, size_t length,
                               unsigned arenaInd)
{
    (void)extentHooks;
    (void)addr;
    VLOG(3) << "Purge forced arena: " << arenaInd << ", size: " << size << ", offset: " << offset
            << ", length: " << length;
    // Jemalloc default purge force implementation would invoke *madvise(xx, MADV_DONTNEED)* to tell the kernel
    // that pages don't expect accessed in near future. However in shared memory scenario, we don't need
    // this operation, so let's return false directly and make jemalloc think purge force success, and change state to
    // retained.
    return false;
}
}  // namespace memory
}  // namespace datasystem
