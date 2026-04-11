/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: pipeline h2d interface for client and worker
 */

#ifndef OS_XPRT_PIPLN_COMMON_API
#define OS_XPRT_PIPLN_COMMON_API

#ifdef BUILD_PIPLN_H2D
#include <ub/umdk/urma/urma_api.h>
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/os_transport_pipeline/chunk_manager.h"
static inline bool SupportPipelineRH2D()
{
    return (FLAGS_enable_urma && FLAGS_enable_pipeline_h2d);
}
#define RETURN_IF_NOT_SUPPORT_PIPLN_H2D() \
    if (!SupportPipelineRH2D())           \
        return Status(StatusCode::K_NOT_SUPPORTED, "enable_pipeline_h2d is false");
#define DEFINE_HOOK_WITH_RETTYPE(retType, hook) retType hook;
#define DEFINE_HOOK(hook) hook

using H2DChunkManager = OsXprtPipln::ChunkManager;

#else

static inline bool SupportPipelineRH2D()
{
    return false;
}
#define RETURN_IF_NOT_SUPPORT_PIPLN_H2D() return Status(StatusCode::K_NOT_SUPPORTED, "not build with OS_PIPLN_H2D");

#define DEFINE_HOOK_WITH_RETTYPE(retType, hook)                                                                        \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wunused-parameter\"") static inline retType hook \
    {                                                                                                                  \
        return (retType)0;                                                                                             \
    }                                                                                                                  \
    _Pragma("GCC diagnostic pop")

#define DEFINE_HOOK(hook)                                                                                      \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wunused-parameter\"") static inline hook \
    {                                                                                                          \
        return Status::OK();                                                                                   \
    }                                                                                                          \
    _Pragma("GCC diagnostic pop")

struct H2DChunkManager {
    H2DChunkManager(bool dummy)
    {
        (void)dummy;
    }
    static inline int KeyNum()
    {
        return 0;
    }
};

#endif

#endif