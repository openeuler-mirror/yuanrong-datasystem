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
 * Description: Selection strategy for migrate data.
 */

#include "datasystem/worker/object_cache/data_migrator/strategy/spill_node_selector.h"

#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace object_cache {

Status SpillNodeSelector::SelectNode(const std::string &originAddr, const std::string &preferNode, size_t needSize,
                                     std::string &outNode)
{
    (void)needSize;

    if (!preferNode.empty()) {
        outNode = preferNode;
        return Status::OK();
    }
    Status status = etcdCM_->GetStandbyWorkerByAddr(originAddr, outNode);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] Failed to get [%s]'s next addr: %s", originAddr, status.ToString());
        outNode = originAddr;
        return status;
    }
    if (outNode == localAddress_.ToString()) {
        std::string errMsg = FormatString("[Migrate Data] Skip, [%s]'s next addr is ourselves", originAddr);
        LOG(INFO) << errMsg;
        RETURN_STATUS(K_DUPLICATED, errMsg);
    }

    INJECT_POINT_NO_RETURN("SpillNodeSelector.SelectNode.force",
                           [&outNode](std::string node) { outNode = std::move(node); });
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
