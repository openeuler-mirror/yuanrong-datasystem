/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/common/rdma/npu/hixl_plugin_api.h"

#include <stddef.h>

_Static_assert(sizeof(DsHixlResult) == sizeof(int32_t), "DsHixlResult ABI changed");
_Static_assert(sizeof(DsHixlStringView) == 16, "DsHixlStringView ABI changed");
_Static_assert(sizeof(DsHixlOption) == 32, "DsHixlOption ABI changed");
_Static_assert(sizeof(DsHixlTransferDesc) == 24, "DsHixlTransferDesc ABI changed");
_Static_assert(sizeof(DsHixlRegisterMemoryRequest) == 24, "DsHixlRegisterMemoryRequest ABI changed");
_Static_assert(sizeof(DsHixlTransferRequest) == 40, "DsHixlTransferRequest ABI changed");
_Static_assert(offsetof(DsHixlApi, create_engine) == 8, "DsHixlApi prefix ABI changed");
_Static_assert(sizeof(DsHixlApi) == 80, "DsHixlApi v1 ABI changed");

void HixlPluginApiCCompileTest(void)
{
}
