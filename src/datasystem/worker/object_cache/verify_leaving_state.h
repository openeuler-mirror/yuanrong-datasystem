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
 * Description: VerifyLeavingState - worker-side LEAVING check for write RPCs only.
 * Only Create/Publish/MultiCreate/MultiPublish are intercepted.
 * Read RPCs (Get) are NOT intercepted - data is still valid.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_LEAVING_STATE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_LEAVING_STATE_H

#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"

namespace datasystem {
namespace worker {

inline Status VerifyLeavingState(ClusterManager *cm)
{
    if (cm == nullptr) {
        return Status::OK();
    }
    if (!cm->CheckLocalNodeIsExiting()) {
        return Status::OK();
    }
    return Status(K_SCALE_DOWN, "Worker is exiting, stop writing");
}

}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_LEAVING_STATE_H
