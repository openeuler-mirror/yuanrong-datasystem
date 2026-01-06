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
 * Description: Selection strategy for migrate data.
 */
#include "datasystem/worker/object_cache/data_migrator/strategy/scale_down_node_selector.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace object_cache {
void ScaleDownNodeSelector::UpdateForRedirect(const std::string &currentWorker)
{
    if (visitedAddresses_.find(currentWorker) != visitedAddresses_.end()) {
        IncrementStage(currentStage_);
        visitedAddresses_.clear();
        visitedAddresses_.insert(currentWorker);
    } else {
        visitedAddresses_.insert(currentWorker);
    }

    if (visitedAddressesForDisk_.find(currentWorker) != visitedAddressesForDisk_.end()) {
        IncrementStage(currentDiskStage_);
        visitedAddressesForDisk_.clear();
        visitedAddressesForDisk_.insert(currentWorker);
    } else {
        visitedAddressesForDisk_.insert(currentWorker);
    }
}

bool ScaleDownNodeSelector::CheckCondition(const MigrateDataRspPb &rsp, const CacheType &type)
{
    bool isDataMigrationStarted = rsp.scale_down_state() == MigrateDataRspPb::DATA_MIGRATION_STARTED;
    bool needScaleDown = rsp.scale_down_state() == MigrateDataRspPb::NEED_SCALE_DOWN;
    double availableSpaceRatio = type == CacheType::MEMORY ? rsp.available_ratio() : rsp.disk_available_ratio();

    Stage &stage = type == CacheType::MEMORY ? currentStage_ : currentDiskStage_;

    INJECT_POINT_NO_RETURN("ScaleDownNodeSelector.CheckCondition",
                           [&needScaleDown, &isDataMigrationStarted, &availableSpaceRatio, &stage](
                               int64_t newNeedScaleDown, int64_t newIsDataMigrationStarted,
                               int64_t newAvailableSpaceRatio, int64_t newStage) {
                                    needScaleDown = (newNeedScaleDown != 0);
                                    isDataMigrationStarted = (newIsDataMigrationStarted != 0);
                                    availableSpaceRatio = static_cast<double>(newAvailableSpaceRatio);
                                    stage = static_cast<Stage>(newStage);
                                });
    INJECT_POINT_NO_RETURN("ScaleDownNodeSelector.CheckCondition1",
                           [this](int64_t newStage) { currentStage_ = static_cast<Stage>(newStage); });
    bool res = EvaluateStageCondition(stage, needScaleDown, isDataMigrationStarted, availableSpaceRatio);
    if (res) {
        if (type == CacheType::MEMORY) {
            visitedAddresses_.clear();
        } else {
            visitedAddressesForDisk_.clear();
        }
    }
    return res;
}

bool ScaleDownNodeSelector::EvaluateStageCondition(Stage stage, bool needScaleDown, bool isDataMigrationStarted,
                                                   double availableSpaceRatio)
{
    const double firstStageThreshold = 50.0;
    const double secondStageThreshold = 20.0;
    switch (stage) {
        case Stage::FIRST:
            return !needScaleDown && !isDataMigrationStarted && availableSpaceRatio > firstStageThreshold;
        case Stage::SECOND:
            return !needScaleDown && !isDataMigrationStarted && availableSpaceRatio > secondStageThreshold;
        case Stage::THIRD:
            return !needScaleDown && !isDataMigrationStarted;
        case Stage::FINAL:
            return !isDataMigrationStarted;
        default:
            LOG(WARNING) << FormatString("Unknown migration stage: %d", static_cast<int>(stage));
            return false;
    }
}

void ScaleDownNodeSelector::IncrementStage(Stage &stage)
{
    if (stage < Stage::FINAL) {
        stage = static_cast<Stage>(static_cast<int>(stage) + 1);
    }
}

Status ScaleDownNodeSelector::SelectNode(const std::string &originAddr, const std::string &preferNode, size_t needSize,
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
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem