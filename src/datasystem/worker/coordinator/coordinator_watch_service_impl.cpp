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

#include "datasystem/worker/coordinator/coordinator_watch_service_impl.h"

namespace datasystem {
namespace coordinator {
Status CoordinatorWatchServiceImpl::HandleEvent(const EventReqPb &req, EventRspPb &rsp)
{
    (void)req;
    (void)rsp;
    return Status(StatusCode::K_NOT_READY, "coordinator watch handler is not bound");
}
}  // namespace coordinator
}  // namespace datasystem
