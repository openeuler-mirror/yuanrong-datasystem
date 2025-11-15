/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: A RAII wrapper for POSIX semaphore operations
 */
#ifndef DATASYSTEM_COMMON_PARALLEL_NATIVE_SEM_H
#define DATASYSTEM_COMMON_PARALLEL_NATIVE_SEM_H

#include <semaphore.h>
#include <malloc.h>

namespace datasystem {
namespace Parallel {
class NativeSem {
public:
    NativeSem(const NativeSem &) = delete;
    NativeSem(NativeSem &&) = delete;
    NativeSem &operator = (const NativeSem &) = delete;
    NativeSem &operator = (NativeSem &&) = delete;

    NativeSem()
    {
        sem = (sem_t *)malloc(sizeof(sem_t));
    }

    ~NativeSem()
    {
        if (sem != nullptr) {
            free(sem);
            sem = nullptr;
        }
    }

    inline void SemInit(int32_t initCnt)
    {
        (void)sem_init(sem, 0, initCnt);
    }

    inline void SemDestroy()
    {
        (void)sem_destroy(sem);
    }

    inline void SemPend()
    {
        (void)sem_wait(sem);
    }

    inline void SemPost()
    {
        (void)sem_post(sem);
    }

private:
    sem_t *sem;
};
}
}

#endif