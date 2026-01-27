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

// UCP dlopen wrapper - provides lazy-loaded function wrappers for UCP/UCX

#ifndef DATASYSTEM_COMMON_RDMA_UCP_DLOPEN_UTIL_H
#define DATASYSTEM_COMMON_RDMA_UCP_DLOPEN_UTIL_H

#include <ucp/api/ucp.h>

// Init API - must call before using any UCP functions via dlopen
namespace datasystem {
namespace ucp_dlopen {

bool Init();
bool IsAvailable();
void Cleanup();

}  // namespace ucp_dlopen
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_UCP_DLOPEN_UTIL_H
