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
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_table.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/worker/object_cache/device/device_obj_cache.h"

DS_DEFINE_bool(enable_metadata_recovery, false, "enable metadata recovery when worker failed");

namespace datasystem {
namespace object_cache {
namespace {
bool IsRestartRecoverableWriteMode(uint32_t writeMode)
{
    auto mode = static_cast<WriteMode>(writeMode);
    return mode == WriteMode::WRITE_THROUGH_L2_CACHE || mode == WriteMode::WRITE_BACK_L2_CACHE
           || mode == WriteMode::WRITE_BACK_L2_CACHE_EVICT;
}
}  // namespace

MetaDataRecoveryManager::MetaDataRecoveryManager(
    const HostPort &localAddress, const std::shared_ptr<ObjectTable> &objectTable, EtcdClusterManager *etcdCM,
    const std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> &workerMasterApiManager,
    uint64_t metadataSize, const std::shared_ptr<WorkerOcEvictionManager> &evictionManager,
    const std::shared_ptr<ThreadPool> &memCpyThreadPool)
    : localAddress_(localAddress),
      objectTable_(objectTable),
      etcdCM_(etcdCM),
      workerMasterApiManager_(workerMasterApiManager),
      metadataSize_(metadataSize),
      evictionManager_(evictionManager),
      memCpyThreadPool_(memCpyThreadPool)
{
}

MetaDataRecoveryManager::RecoverySummary MetaDataRecoveryManager::RecoverMetadataWithSummary(
    const std::vector<std::string> &objectKeys, std::string stanbyAddr)
{
    RecoverySummary summary;
    summary.requestedCount = objectKeys.size();
    if (objectKeys.empty()) {
        return summary;
    }
    if (etcdCM_ == nullptr) {
        summary.status = Status(K_RUNTIME_ERROR, "etcdCM is null");
        summary.failedIds = objectKeys;
        return summary;
    }
    if (objectTable_ == nullptr) {
        summary.status = Status(K_RUNTIME_ERROR, "objectTable is null");
        summary.failedIds = objectKeys;
        return summary;
    }
    if (workerMasterApiManager_ == nullptr) {
        summary.status = Status(K_RUNTIME_ERROR, "workerMasterApiManager is null");
        summary.failedIds = objectKeys;
        return summary;
    }

    auto groupedByMaster = BuildGroupedByMaster(objectKeys, stanbyAddr);
    std::vector<const GroupItem *> groupedByMasterKeys;
    groupedByMasterKeys.reserve(groupedByMaster.size());
    for (const auto &item : groupedByMaster) {
        groupedByMasterKeys.emplace_back(&item);
    }
    summary.groupedMasterCount = groupedByMasterKeys.size();

    std::vector<DispatchResult> results(groupedByMasterKeys.size());
    Status parallelRc = Parallel::ParallelFor<size_t>(
        0, groupedByMasterKeys.size(),
        [this, &groupedByMasterKeys, &results](size_t start, size_t end) {
            for (size_t idx = start; idx < end; ++idx) {
                const auto *group = groupedByMasterKeys[idx];
                results[idx] = SendRecoverRequest(group->first, group->second);
            }
        },
        1);
    if (parallelRc.IsError()) {
        summary.status = parallelRc;
        summary.failedIds = objectKeys;
        return summary;
    }

    MergeDispatchResults(results, summary);
    LogRecoverySummary(summary, "Recover metadata");
    return summary;
}

MetaDataRecoveryManager::RecoverySummary MetaDataRecoveryManager::RecoverMetadataWithSummary(
    const std::vector<ObjectMetaPb> &metas)
{
    RecoverySummary summary;
    summary.requestedCount = metas.size();
    auto appendFailedMetas = [&summary, &metas]() {
        for (const auto &meta : metas) {
            summary.failedIds.emplace_back(meta.object_key());
        }
    };
    if (metas.empty()) {
        return summary;
    }
    if (etcdCM_ == nullptr || workerMasterApiManager_ == nullptr) {
        summary.status = etcdCM_ == nullptr ? Status(K_RUNTIME_ERROR, "etcdCM is null")
                                            : Status(K_RUNTIME_ERROR, "workerMasterApiManager is null");
        appendFailedMetas();
        return summary;
    }

    std::vector<std::string> objectKeys;
    std::unordered_map<std::string, std::vector<const ObjectMetaPb *>> metasByObjectKey;
    BuildMetaIndex(metas, objectKeys, metasByObjectKey, summary);
    if (objectKeys.empty()) {
        return summary;
    }

    auto groupedByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    std::vector<const GroupItem *> groupedByMasterKeys;
    groupedByMasterKeys.reserve(groupedByMaster.size());
    for (const auto &item : groupedByMaster) {
        groupedByMasterKeys.emplace_back(&item);
    }
    summary.groupedMasterCount = groupedByMasterKeys.size();

    std::vector<DispatchResult> results(groupedByMasterKeys.size());
    Status parallelRc = Parallel::ParallelFor<size_t>(
        0, groupedByMasterKeys.size(),
        [this, &groupedByMasterKeys, &metasByObjectKey, &results](size_t start, size_t end) {
            for (size_t idx = start; idx < end; ++idx) {
                const auto *group = groupedByMasterKeys[idx];
                auto groupedMetas = BuildGroupedMetas(group->second, metasByObjectKey);
                results[idx] = SendRecoverRequest(group->first, groupedMetas);
            }
        },
        1);
    if (parallelRc.IsError()) {
        summary.status = parallelRc;
        appendFailedMetas();
        return summary;
    }

    MergeDispatchResults(results, summary);
    LogRecoverySummary(summary, "Recover metadata from metas");
    return summary;
}

MetaDataRecoveryManager::GroupedByMaster MetaDataRecoveryManager::BuildGroupedByMaster(
    const std::vector<std::string> &objectKeys, const std::string &stanbyAddr) const
{
    if (stanbyAddr.empty()) {
        return etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    }
    MetaAddrInfo info;
    HostPort masterAddr;
    masterAddr.ParseString(stanbyAddr);
    info.SetDbName(etcdCM_->GetWorkerIdByWorkerAddr(stanbyAddr));
    info.SetAddress(masterAddr);
    return { { info, { objectKeys.begin(), objectKeys.end() } } };
}

void MetaDataRecoveryManager::MergeDispatchResults(const std::vector<DispatchResult> &results,
                                                   RecoverySummary &summary) const
{
    for (const auto &result : results) {
        summary.recoveredCount += result.recoveredCount;
        summary.failedIds.insert(summary.failedIds.end(), result.failedIds.begin(), result.failedIds.end());
        if (result.status.IsError()) {
            summary.status = result.status;
        }
    }
}

void MetaDataRecoveryManager::BuildMetaIndex(
    const std::vector<ObjectMetaPb> &metas, std::vector<std::string> &objectKeys,
    std::unordered_map<std::string, std::vector<const ObjectMetaPb *>> &metasByObjectKey,
    RecoverySummary &summary) const
{
    objectKeys.reserve(metas.size());
    for (const auto &meta : metas) {
        if (meta.object_key().empty()) {
            summary.failedIds.emplace_back("");
            if (summary.status.IsOk()) {
                summary.status = Status(K_INVALID, "recovery metadata contains empty object key");
            }
            continue;
        }
        objectKeys.emplace_back(meta.object_key());
        metasByObjectKey[meta.object_key()].emplace_back(&meta);
    }
}

std::vector<ObjectMetaPb> MetaDataRecoveryManager::BuildGroupedMetas(
    const std::vector<std::string> &objectKeys,
    const std::unordered_map<std::string, std::vector<const ObjectMetaPb *>> &metasByObjectKey) const
{
    std::vector<ObjectMetaPb> groupedMetas;
    for (const auto &objectKey : objectKeys) {
        auto iter = metasByObjectKey.find(objectKey);
        if (iter == metasByObjectKey.end()) {
            continue;
        }
        for (const auto *meta : iter->second) {
            groupedMetas.emplace_back(*meta);
        }
    }
    return groupedMetas;
}

void MetaDataRecoveryManager::LogRecoverySummary(const RecoverySummary &summary, const std::string &prefix) const
{
    if (summary.status.IsError()) {
        LOG(WARNING) << FormatString(
            "%s finished with partial failures. requested: %zu, recovered: %zu, "
            "failed: %zu, grouped masters: %zu, status: %s",
            prefix, summary.requestedCount, summary.recoveredCount, summary.failedIds.size(),
            summary.groupedMasterCount, summary.status.ToString());
        return;
    }
    LOG(INFO) << FormatString("%s finished. requested: %zu, recovered: %zu, grouped masters: %zu", prefix,
                              summary.requestedCount, summary.recoveredCount, summary.groupedMasterCount);
}

Status MetaDataRecoveryManager::RecoverLocalEntries(const std::vector<ObjectMetaPb> &recoverMetas,
                                                    std::vector<std::string> &recoveredObjectKeys) const
{
    static const std::unordered_map<std::string, std::shared_ptr<std::stringstream>> emptyContents;
    return RecoverLocalEntries(recoverMetas, emptyContents, recoveredObjectKeys);
}

Status MetaDataRecoveryManager::RecoverLocalEntries(
    const std::vector<ObjectMetaPb> &recoverMetas,
    const std::unordered_map<std::string, std::shared_ptr<std::stringstream>> &recoveredContents,
    std::vector<std::string> &recoveredObjectKeys) const
{
    RETURN_OK_IF_TRUE(recoverMetas.empty());
    CHECK_FAIL_RETURN_STATUS(objectTable_ != nullptr, K_RUNTIME_ERROR, "objectTable is null");

    for (const auto &meta : recoverMetas) {
        if (meta.object_key().empty()) {
            LOG(WARNING) << "Skip restart recovery metadata with empty object key.";
            continue;
        }
        if (static_cast<DataFormat>(meta.config().data_format()) != DataFormat::BINARY) {
            LOG(INFO) << FormatString("[ObjectKey %s] Skip non-binary restart recovery metadata.", meta.object_key());
            continue;
        }
        if (!IsRestartRecoverableWriteMode(meta.config().write_mode())) {
            LOG(INFO) << FormatString("[ObjectKey %s] Skip non-L2 restart recovery metadata.", meta.object_key());
            continue;
        }
        if (static_cast<ObjectLifeState>(meta.life_state()) == ObjectLifeState::OBJECT_INVALID) {
            LOG(INFO) << FormatString("[ObjectKey %s] Skip invalid restart recovery metadata.", meta.object_key());
            continue;
        }

        std::shared_ptr<SafeObjType> entry;
        bool isInsert = false;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectTable_->ReserveGetAndLock(meta.object_key(), entry, isInsert),
                                         FormatString("[ObjectKey %s] ReserveGetAndLock failed.", meta.object_key()));
        Raii unlock([&entry]() { entry->WUnlock(); });

        SetObjectEntryAccordingToMeta(meta, metadataSize_, *entry);
        (*entry)->SetAddress(localAddress_.ToString());
        (*entry)->stateInfo.SetPrimaryCopy(true);

        auto foundContent = recoveredContents.find(meta.object_key());
        if (foundContent != recoveredContents.end() && foundContent->second != nullptr) {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(evictionManager_ != nullptr, K_RUNTIME_ERROR,
                                                 "evictionManager is null");
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(memCpyThreadPool_ != nullptr, K_RUNTIME_ERROR,
                                                 "memCpyThreadPool is null");
            const auto content = foundContent->second->str();
            std::vector<RpcMessage> payloads;
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                CopyAndSplitBuffer(TenantAuthManager::ExtractTenantId(meta.object_key()), content.data(),
                                   content.size(), payloads),
                FormatString("[ObjectKey %s] CopyAndSplitBuffer failed.", meta.object_key()));
            ObjectKV objectKV(meta.object_key(), *entry);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                SaveBinaryObjectToMemory(objectKV, payloads, evictionManager_, memCpyThreadPool_),
                FormatString("[ObjectKey %s] SaveBinaryObjectToMemory failed.", meta.object_key()));
            (*entry)->stateInfo.SetCacheInvalid(false);
            (*entry)->stateInfo.SetIncompleted(false);
        }
        recoveredObjectKeys.emplace_back(meta.object_key());

        LOG(INFO) << FormatString(
            "[ObjectKey %s] Recover restart metadata success, primary copy: %d, primary address: %s, local worker: %s",
            meta.object_key(), true, localAddress_.ToString(), localAddress_.ToString());
    }
    return Status::OK();
}

Status MetaDataRecoveryManager::RecoverMetadata(const std::vector<ObjectMetaPb> &metas, std::vector<std::string> &failedIds,
                                                std::string stanbyMasterAddr)
{
    RETURN_OK_IF_TRUE(metas.empty());
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, K_RUNTIME_ERROR, "etcdCM is null");
    CHECK_FAIL_RETURN_STATUS(workerMasterApiManager_ != nullptr, K_RUNTIME_ERROR, "workerMasterApiManager is null");
    LOG(INFO) << "recovery meta from slot preload begin";

    std::unordered_map<std::string, ObjectMetaPb> latestMetaByKey;
    latestMetaByKey.reserve(metas.size());
    std::vector<std::string> objectKeys;
    objectKeys.reserve(metas.size());
    for (const auto &meta : metas) {
        if (meta.object_key().empty()) {
            failedIds.emplace_back("");
            continue;
        }
        auto it = latestMetaByKey.find(meta.object_key());
        if (it == latestMetaByKey.end()) {
            latestMetaByKey.emplace(meta.object_key(), meta);
            objectKeys.emplace_back(meta.object_key());
            continue;
        }
        if (meta.version() >= it->second.version()) {
            it->second = meta;
        }
    }
    RETURN_OK_IF_TRUE(latestMetaByKey.empty());

    std::unordered_map<MetaAddrInfo, std::vector<std::string>> groupedKeysByMaster;
    if (!stanbyMasterAddr.empty()) {
        MetaAddrInfo info;
        HostPort masterAddr;
        masterAddr.ParseString(stanbyMasterAddr);
        info.SetDbName(etcdCM_->GetWorkerIdByWorkerAddr(stanbyMasterAddr));
        info.SetAddress(masterAddr);
        groupedKeysByMaster[info] = { objectKeys.begin(), objectKeys.end() };
    } else {
        groupedKeysByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    }

    std::unordered_map<MetaAddrInfo, std::vector<ObjectMetaPb>> groupedMetasByMaster;
    groupedMetasByMaster.reserve(groupedKeysByMaster.size());
    for (const auto &item : groupedKeysByMaster) {
        auto &groupedMetas = groupedMetasByMaster[item.first];
        groupedMetas.reserve(item.second.size());
        for (const auto &objectKey : item.second) {
            auto found = latestMetaByKey.find(objectKey);
            if (found != latestMetaByKey.end()) {
                groupedMetas.emplace_back(found->second);
            }
        }
    }

    using GroupItem = decltype(groupedMetasByMaster)::value_type;
    std::vector<const GroupItem *> groupedMetas;
    groupedMetas.reserve(groupedMetasByMaster.size());
    for (const auto &item : groupedMetasByMaster) {
        groupedMetas.emplace_back(&item);
    }

    std::vector<DispatchResult> results(groupedMetas.size());
    RETURN_IF_NOT_OK(Parallel::ParallelFor<size_t>(
        0, groupedMetas.size(),
        [this, &groupedMetas, &results](size_t start, size_t end) {
            for (size_t idx = start; idx < end; ++idx) {
                const auto *group = groupedMetas[idx];
                results[idx] = SendRecoverRequest(group->first, group->second);
            }
        },
        1));

    Status lastRrr = Status::OK();
    for (auto &result : results) {
        failedIds.insert(failedIds.end(), result.failedIds.begin(), result.failedIds.end());
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
    metadata.set_life_state(static_cast<uint32_t>((*currSafeObj)->GetLifeState()));
    metadata.set_version((*currSafeObj)->GetCreateTime());
    metadata.set_ttl_second((*currSafeObj)->GetTtlSecond());
    metadata.set_is_recovered(true);

    ConfigPb *configPb = metadata.mutable_config();
    configPb->set_write_mode(static_cast<uint32_t>((*currSafeObj)->modeInfo.GetWriteMode()));
    configPb->set_data_format(static_cast<uint32_t>((*currSafeObj)->stateInfo.GetDataFormat()));
    configPb->set_consistency_type(static_cast<uint32_t>((*currSafeObj)->modeInfo.GetConsistencyType()));
    configPb->set_cache_type(static_cast<uint32_t>((*currSafeObj)->modeInfo.GetCacheType()));
    configPb->set_is_replica(!(*currSafeObj)->stateInfo.IsPrimaryCopy());
    if ((*currSafeObj)->stateInfo.IsPrimaryCopy()) {
        metadata.set_primary_address(localAddress_.ToString());
    } else if (!(*currSafeObj)->GetAddress().empty()) {
        metadata.set_primary_address((*currSafeObj)->GetAddress());
    }
    if ((*currSafeObj)->stateInfo.GetDataFormat() == DataFormat::HETERO) {
        auto *devObj = SafeObjType::GetDerived<DeviceObjCache>(*currSafeObj);
        if (devObj == nullptr) {
            LOG(WARNING) << FormatString("[ObjectKey %s] FillRecoveredMeta found HETERO object without DeviceObjCache.",
                                         objectKey);
            return false;
        }
        auto *deviceInfo = metadata.mutable_device_info();
        deviceInfo->set_device_id(devObj->GetDeviceIdx());
        deviceInfo->set_offset(devObj->GetOffset());
    }
    return true;
}

bool MetaDataRecoveryManager::InitRecoverApi(const MetaAddrInfo &metaAddrInfo,
                                             const std::vector<std::string> &objectKeys, HostPort &addr,
                                             std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                             DispatchResult &result) const
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

MetaDataRecoveryManager::DispatchResult MetaDataRecoveryManager::SendRecoverRequest(
    const MetaAddrInfo &metaAddrInfo, const std::vector<ObjectMetaPb> &metas) const
{
    DispatchResult result;
    result.requestedCount = metas.size();
    auto addr = metaAddrInfo.GetAddressAndSaveDbName();
    std::shared_ptr<worker::WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(addr);
    if (workerMasterApi == nullptr) {
        for (const auto &meta : metas) {
            result.failedIds.emplace_back(meta.object_key());
        }
        result.status = Status(K_RUNTIME_ERROR, "get worker master api failed");
        LOG(ERROR) << "Failed to get worker master api, master addr: " << addr.ToString();
        return result;
    }

    constexpr size_t maxBatchSize = 500;
    master::PushMetaToMasterReqPb req;
    req.set_address(localAddress_.ToString());
    std::vector<std::string> batchObjectKeys;
    batchObjectKeys.reserve(maxBatchSize);

    for (const auto &meta : metas) {
        if (meta.object_key().empty()) {
            result.failedIds.emplace_back("");
            if (result.status.IsOk()) {
                result.status = Status(K_INVALID, "recovery metadata contains empty object key");
            }
            continue;
        }
        ObjectMetaPb *metaPb = req.add_metas();
        *metaPb = meta;
        metaPb->set_is_recovered(true);
        batchObjectKeys.emplace_back(meta.object_key());
        if (batchObjectKeys.size() >= maxBatchSize) {
            SendRecoverBatch(metaAddrInfo, addr, workerMasterApi, req, batchObjectKeys, result);
        }
    }
    SendRecoverBatch(metaAddrInfo, addr, workerMasterApi, req, batchObjectKeys, result);
    return result;
}
}  // namespace object_cache
}  // namespace datasystem
