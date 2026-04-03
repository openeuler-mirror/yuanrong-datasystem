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
 * Description: Manager for recovering object metadata from worker to master.
 */
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

DS_DEFINE_bool(enable_metadata_recovery, false, "enable metadata recovery when worker failed");

namespace datasystem {
namespace object_cache {
MetaDataRecoveryManager::MetaDataRecoveryManager(
    const HostPort &localAddress, const std::shared_ptr<ObjectTable> &objectTable, EtcdClusterManager *etcdCM,
    const std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> &workerMasterApiManager)
    : localAddress_(localAddress),
      objectTable_(objectTable),
      etcdCM_(etcdCM),
      workerMasterApiManager_(workerMasterApiManager)
{
}

Status MetaDataRecoveryManager::RecoverMetadata(const std::vector<std::string> &objectKeys,
                                                std::vector<std::string> &failedIds, std::string stanbyMasterAddr)
{
    RETURN_OK_IF_TRUE(objectKeys.empty());
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, K_RUNTIME_ERROR, "etcdCM is null");
    CHECK_FAIL_RETURN_STATUS(objectTable_ != nullptr, K_RUNTIME_ERROR, "objectTable is null");
    CHECK_FAIL_RETURN_STATUS(workerMasterApiManager_ != nullptr, K_RUNTIME_ERROR, "workerMasterApiManager is null");
    LOG(INFO) << "recovery meta begin";
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> groupedByMaster;
    if (!stanbyMasterAddr.empty()) {
        MetaAddrInfo info;
        HostPort masterAddr;
        masterAddr.ParseString(stanbyMasterAddr);
        info.SetDbName(etcdCM_->GetWorkerIdByWorkerAddr(stanbyMasterAddr));
        info.SetAddress(masterAddr);
        groupedByMaster[info] = { objectKeys.begin(), objectKeys.end() };
    } else {
        groupedByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    }
    using GroupItem = decltype(groupedByMaster)::value_type;
    std::vector<const GroupItem *> groupedByMasterKeys;
    groupedByMasterKeys.reserve(groupedByMaster.size());
    for (const auto &item : groupedByMaster) {
        groupedByMasterKeys.emplace_back(&item);
    }

    std::vector<DispatchResult> results(groupedByMasterKeys.size());
    RETURN_IF_NOT_OK(Parallel::ParallelFor<size_t>(
        0, groupedByMasterKeys.size(),
        [this, &groupedByMasterKeys, &results](size_t start, size_t end) {
            for (size_t idx = start; idx < end; ++idx) {
                const auto *group = groupedByMasterKeys[idx];
                results[idx] = SendRecoverRequest(group->first, group->second);
            }
        },
        1));

    Status lastRrr = Status::OK();
    for (auto &result : results) {
        for (const auto &objectKey : result.failedIds) {
            failedIds.emplace_back(objectKey);
        }
        if (result.status.IsError()) {
            lastRrr = result.status;
        }
    }
    return lastRrr;
}

bool MetaDataRecoveryManager::FillRecoveredMeta(const std::string &objectKey, ObjectMetaPb &metadata) const
{
    std::shared_ptr<SafeObjType> currSafeObj;
    if (objectTable_->Get(objectKey, currSafeObj).IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] FillRecoveredMeta get object failed.", objectKey);
        return false;
    }
    if (currSafeObj->RLock().IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] FillRecoveredMeta lock object failed.", objectKey);
        return false;
    }
    Raii unlock([&currSafeObj]() { currSafeObj->RUnlock(); });
    if ((*currSafeObj)->IsBinary() && (*currSafeObj)->IsInvalid()) {
        return false;
    }
    metadata.set_object_key(objectKey);
    metadata.set_data_size((*currSafeObj)->GetDataSize());
    metadata.set_life_state(static_cast<uint64_t>((*currSafeObj)->GetLifeState()));
    metadata.set_version((*currSafeObj)->GetCreateTime());
    metadata.set_is_recovered(true);
    ConfigPb *configPb = metadata.mutable_config();
    configPb->set_write_mode(static_cast<uint32_t>((*currSafeObj)->modeInfo.GetWriteMode()));
    configPb->set_data_format(static_cast<uint32_t>((*currSafeObj)->stateInfo.GetDataFormat()));
    if ((*currSafeObj)->stateInfo.IsPrimaryCopy()) {
        metadata.set_primary_address(localAddress_.ToString());
    }
    return true;
}

bool MetaDataRecoveryManager::InitRecoverApi(
    const MetaAddrInfo &metaAddrInfo, const std::vector<std::string> &objectKeys, HostPort &addr,
    std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi, DispatchResult &result) const
{
    addr = metaAddrInfo.GetAddressAndSaveDbName();
    workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(addr);
    if (workerMasterApi != nullptr) {
        return true;
    }
    result.failedIds.insert(result.failedIds.end(), objectKeys.begin(), objectKeys.end());
    result.status = Status(K_RUNTIME_ERROR, "get worker master api failed");
    LOG(ERROR) << "failed to get worker master api, master addr: " << addr.ToString();
    return false;
}

void MetaDataRecoveryManager::HandleFillMetaFailure(const std::string &objectKey, DispatchResult &result) const
{
    result.failedIds.emplace_back(objectKey);
    if (result.status.IsOk()) {
        result.status = Status(K_RUNTIME_ERROR, "fill recovered meta failed");
    }
    LOG(ERROR) << "failed to fill meta, object id: " << objectKey;
}

void MetaDataRecoveryManager::SendRecoverBatch(const MetaAddrInfo &metaAddrInfo, const HostPort &addr,
                                               const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                               master::PushMetaToMasterReqPb &req,
                                               std::vector<std::string> &batchObjectKeys, DispatchResult &result) const
{
    if (req.metas_size() == 0) {
        return;
    }
    Status rc = etcdCM_->CheckConnection(addr);
    if (rc.IsError()) {
        result.failedIds.insert(result.failedIds.end(), batchObjectKeys.begin(), batchObjectKeys.end());
        result.status = rc;
        LOG(ERROR) << FormatString("CheckConnection failed, master: %s, status: %s", metaAddrInfo.ToString(),
                                   rc.ToString());
    }

    master::PushMetaToMasterRspPb rsp;
    rc = workerMasterApi->PushMetadataToMaster(req, rsp);
    if (rc.IsError()) {
        result.failedIds.insert(result.failedIds.end(), batchObjectKeys.begin(), batchObjectKeys.end());
        result.status = rc;
        LOG(ERROR) << FormatString("Recover metadata failed, master: %s, status: %s", metaAddrInfo.ToString(),
                                   rc.ToString());
    }
    req.clear_metas();
    batchObjectKeys.clear();
}

MetaDataRecoveryManager::DispatchResult MetaDataRecoveryManager::SendRecoverRequest(
    const MetaAddrInfo &metaAddrInfo, const std::vector<std::string> &objectKeys) const
{
    DispatchResult result;
    HostPort addr;
    std::shared_ptr<worker::WorkerMasterOCApi> workerMasterApi;
    if (!InitRecoverApi(metaAddrInfo, objectKeys, addr, workerMasterApi, result)) {
        return result;
    }
    LOG(INFO) << "send recover request to master addr: " << metaAddrInfo.GetAddress().ToString();
    constexpr size_t maxBatchSize = 500;
    master::PushMetaToMasterReqPb req;
    req.set_address(localAddress_.ToString());
    std::vector<std::string> batchObjectKeys;
    batchObjectKeys.reserve(maxBatchSize);
    for (const auto &objectKey : objectKeys) {
        ObjectMetaPb *metaPb = req.add_metas();
        if (!FillRecoveredMeta(objectKey, *metaPb)) {
            req.mutable_metas()->RemoveLast();
            HandleFillMetaFailure(objectKey, result);
            continue;
        }
        batchObjectKeys.emplace_back(objectKey);
        if (batchObjectKeys.size() >= maxBatchSize) {
            SendRecoverBatch(metaAddrInfo, addr, workerMasterApi, req, batchObjectKeys, result);
        }
    }
    SendRecoverBatch(metaAddrInfo, addr, workerMasterApi, req, batchObjectKeys, result);
    return result;
}
}  // namespace object_cache
}  // namespace datasystem
