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
 * Description: VerifyOwner - worker-side ownership check for 7 metadata RPCs.
 * If key not in local range, returns K_NOT_OWNER + WithExtra(realOwnerAddr).
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_OWNER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_OWNER_H

#include <string>

#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"

namespace datasystem {
namespace worker {

inline Status VerifyOwner(ClusterManager *cm, const std::string &objKey)
{
    if (cm == nullptr) {
        return Status::OK();
    }
    if (cm->IsCentralized()) {
        return Status::OK();
    }

    auto ranges = cm->GetHashRangeNonBlock();
    if (cm->IsInRange(ranges, objKey)) {
        return Status::OK();
    }

    HostPort realOwner;
    Status s = cm->LocateMetaOwner(objKey, true, realOwner);
    if (s.IsError()) {
        return Status(K_NOT_OWNER, "Key not owned by this worker");
    }
    return Status(K_NOT_OWNER, "Key not owned by this worker").WithExtra(realOwner.ToString());
}

inline Status VerifyOwner(ClusterManager *cm,
                          const google::protobuf::RepeatedPtrField<std::string> &objectKeys)
{
    if (cm == nullptr || objectKeys.empty()) {
        return Status::OK();
    }
    if (cm->IsCentralized()) {
        return Status::OK();
    }

    // Batch semantics: if any key belongs to this worker, allow the entire batch.
    // The worker-side processor (e.g., GroupKeysByMetaOwner) will dispatch
    // keys to their correct owners internally. Rejecting the whole batch when
    // only some keys are out-of-range would cause unnecessary K_NOT_OWNER storms.
    // Only reject when ALL keys are out-of-range.
    auto ranges = cm->GetHashRangeNonBlock();
    for (const auto &key : objectKeys) {
        if (cm->IsInRange(ranges, key)) {
            return Status::OK();
        }
    }

    HostPort realOwner;
    Status s = cm->LocateMetaOwner(objectKeys[0], true, realOwner);
    if (s.IsError()) {
        return Status(K_NOT_OWNER, "Key not owned by this worker");
    }
    return Status(K_NOT_OWNER, "Key not owned by this worker").WithExtra(realOwner.ToString());
}

}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_VERIFY_OWNER_H
