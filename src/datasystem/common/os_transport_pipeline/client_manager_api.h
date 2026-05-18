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
 * Description: pipeline api for client manager
 */

#ifndef OS_XPRT_PIPLN_CLNT_MGR_API
#define OS_XPRT_PIPLN_CLNT_MGR_API

#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_common_api.h"

namespace OsXprtPipln {
DEFINE_HOOK(Status HoleOnePiplnRH2DQueue(uint32_t &queueId));

DEFINE_HOOK(Status ReleaseAvailableQueue(uint32_t queueId));
}  // namespace OsXprtPipln

#endif