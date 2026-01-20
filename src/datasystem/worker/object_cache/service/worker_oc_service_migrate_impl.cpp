/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the worker service processing publish process.
 */

#include "datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <google/protobuf/repeated_field.h>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/master_object.service.rpc.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DEFINE_validator(data_migrate_rate_limit_mb, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});

using worker::WorkerMasterOCApi;

constexpr double MIGRATE_SCALE_DOWN_HIGH_WATER_FACTOR = 0.95;

namespace datasystem {
namespace object_cache {

WorkerOcServiceMigrateImpl::WorkerOcServiceMigrateImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                                                       std::shared_ptr<ThreadPool> memcpyThreadPool,
                                                       std::shared_ptr<AkSkManager> akSkManager,
                                                       const std::string &localAddr)
    : WorkerOcServiceCrudCommonApi(initParam),
      etcdCM_(etcdCM),
      memcpyThreadPool_(std::move(memcpyThreadPool)),
      akSkManager_(std::move(akSkManager)),
      localAddr_(localAddr),
      rateLimiter_(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul)
{
}

Status WorkerOcServiceMigrateImpl::CheckResource(const MigrateDataReqPb &req, MigrateDataRspPb &rsp)
{
    bool oom = false;
    switch (req.type()) {
        case MigrateType::SCALE_DOWN:
            oom = !IsMemoryAvailable(0, req.type()) && !IsSpillAvaialble() && !IsDiskAvailable();
            break;
        case MigrateType::SPILL:
            oom = !IsMemoryAvailable(0, req.type());
            break;
        default:
            RETURN_STATUS(StatusCode::K_INVALID, "Invalid migrate type");
    }
    RETURN_OK_IF_TRUE(!oom);

    std::unordered_set<std::string> failedIds;
    std::transform(req.objects().begin(), req.objects().end(), std::inserter(failedIds, failedIds.end()),
                   [](const auto &info) { return info.object_key(); });
    FillMigrateDataResponse(req, {}, failedIds, true, rsp);
    LOG(INFO) << "[Migrate Data] OOM";
    RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, "OOM");
}

Status WorkerOcServiceMigrateImpl::MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                               std::vector<RpcMessage> payloads)
{
    LOG(INFO) << FormatString("[Migrate Data] Type: %d, Count: %d, Objects: %s", static_cast<int>(req.type()),
                              req.objects_size(), VectorToString(GetObjects(req)));
    INJECT_POINT("worker.migrate_service.return");
    RETURN_IF_NOT_OK(CheckResource(req, rsp));
    // 1. Lock objects.
    LockedEntryMap lockedEntries;
    std::unordered_set<std::string> successIds;
    std::unordered_set<std::string> failedIds;
    BatchLockForMigrateData(req.objects(), lockedEntries, successIds, failedIds);
    Raii raii([this, &lockedEntries]() { BatchUnlock(lockedEntries); });

    if (etcdCM_ != nullptr && etcdCM_->IsDataMigrationStarted()) {
        std::unordered_set<std::string> failedIds;
        std::transform(req.objects().begin(), req.objects().end(), std::inserter(failedIds, failedIds.end()),
                       [](const auto &info) { return info.object_key(); });
        FillMigrateDataResponse(req, {}, failedIds, false, rsp);
        RETURN_STATUS(StatusCode::K_NOT_READY, "Data migration already in progress");
    }

    // 2. Get object metadata from master.
    std::unordered_set<std::string> needQueryIds;
    std::transform(lockedEntries.begin(), lockedEntries.end(), std::inserter(needQueryIds, needQueryIds.end()),
                   [](const auto &entry) { return entry.first; });
    QueryMetaMap metas;
    Status rc = QueryMasterMetadata(needQueryIds, metas, failedIds);
    if (rc.IsError()) {
        FillMigrateDataResponse(req, successIds, failedIds, false, rsp);
        return rc;
    }

    // 3. Fill data and metadata to object entry.
    ObjectInfoMap needSendMasterIds;
    Status status = FillObjectsLocked(req, lockedEntries, metas, payloads, successIds, failedIds, needSendMasterIds);
    bool oom = IsNoSpace(status);

    // 4. Send replace primary copy to master.
    if (!needSendMasterIds.empty()) {
        status = ReplacePrimaryImpl(req.worker_addr(), needSendMasterIds, req.type(), successIds, failedIds);
    }

    // 5. Fill response.
    FillMigrateDataResponse(req, successIds, failedIds, oom, rsp);
    LOG(INFO) << "[Migrate Data] Migrate finish, success size: " << successIds.size()
              << ", failed size: " << failedIds.size() << ", last status: " << status.ToString();
    return successIds.empty() ? status : Status::OK();
}

Status WorkerOcServiceMigrateImpl::MigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp)
{
    LOG(INFO) << FormatString("[Migrate Data] Count: %d, Objects: %s", req.objects_size(),
                              VectorToString(GetObjects(req)));
    RETURN_OK_IF_TRUE(req.objects().empty());
    auto fillResponseAndReturn = [this, &req, &rsp](StatusCode code, const std::string &message) {
        std::unordered_set<std::string> failedIds;
        std::transform(req.objects().begin(), req.objects().end(), std::inserter(failedIds, failedIds.end()),
                       [](const auto &info) { return info.object_key(); });
        FillMigrateDataDirectResponse(failedIds, code == StatusCode::K_OUT_OF_MEMORY, rsp);
        LOG(INFO) << "[Migrate Data] " << message;
        return Status(code, message);
    };
    if (!IsMemoryAvailable(0, MigrateType::SPILL)) {
        return fillResponseAndReturn(StatusCode::K_OUT_OF_MEMORY, "OOM");
    }
    if (etcdCM_ != nullptr && etcdCM_->CheckLocalNodeIsExiting()) {
        return fillResponseAndReturn(StatusCode::K_SCALE_DOWN, "Worker is exiting");
    }
    if (!IsUrmaEnabled()) {
        return fillResponseAndReturn(StatusCode::K_RUNTIME_ERROR, "URMA is not enabled");
    }
    // 1. Lock objects.
    LockedEntryMap lockedEntries;
    std::unordered_set<std::string> successIds;
    std::unordered_set<std::string> failedIds;
    BatchLockForMigrateData(req.objects(), lockedEntries, successIds, failedIds);
    Raii raii([this, &lockedEntries]() { BatchUnlock(lockedEntries); });

    // 2. Get object metadata from master.
    std::unordered_set<std::string> needQueryIds;
    std::transform(lockedEntries.begin(), lockedEntries.end(), std::inserter(needQueryIds, needQueryIds.end()),
                   [](const auto &entry) { return entry.first; });
    QueryMetaMap metas;
    Status rc = QueryMasterMetadata(needQueryIds, metas, failedIds);
    if (rc.IsError()) {
        FillMigrateDataDirectResponse(failedIds, false, rsp);
        return rc;
    }

    // 3. Fill metadata to object entry.
    ObjectInfoMap needReadDataIds;
    FillMetaToObjectEntries(lockedEntries, metas, successIds, failedIds, needReadDataIds);

    // 4. Fill data to object entry.
    ObjectInfoMap needSendMasterIds;
    Status status = FillDataToObjectEntries(req, needReadDataIds, needSendMasterIds, failedIds);
    bool oom = IsNoSpace(status);

    // 5. Send replace primary copy to master.
    if (!needSendMasterIds.empty()) {
        status = ReplacePrimaryImpl(req.worker_addr(), needSendMasterIds, MigrateType::SPILL, successIds, failedIds);
    }
    if (!failedIds.empty()) {
        RollbackObjects(failedIds, needReadDataIds);
    }

    // 6. Fill response.
    FillMigrateDataDirectResponse(failedIds, oom, rsp);
    LOG(INFO) << "[Migrate Data] Migrate direct finish, success size: " << successIds.size()
              << ", failed size: " << failedIds.size() << ", last status: " << status.ToString();
    return successIds.empty() ? status : Status::OK();
}

void WorkerOcServiceMigrateImpl::BatchLock(const std::map<std::string, uint64_t> &toLockIds,
                                           LockedEntryMap &lockedEntries, std::unordered_set<std::string> &successIds,
                                           std::unordered_set<std::string> &failedIds)
{
    for (const auto &[objectKey, version] : toLockIds) {
        std::shared_ptr<SafeObjType> entry;
        bool isInsert = false;

        Status s = objectTable_->ReserveGetAndLock(objectKey, entry, isInsert, false, false);
        if (!s.IsOk()) {
            LOG(ERROR) << FormatString("[Migrate Data] %s get failed, would not be process this time.", objectKey);
            (void)failedIds.emplace(objectKey);
            continue;
        }
        if (isInsert) {
            SetEmptyObjectEntry(objectKey, *entry);
            (void)lockedEntries.emplace(objectKey, std::make_pair(std::move(entry), version));
        } else {
            s = TryLockWithRetry(objectKey, entry, true);
            if (!s.IsOk()) {
                LOG(ERROR) << FormatString("[Migrate Data] %s try lock failed, would not be process this time.",
                                           objectKey);
                (void)failedIds.emplace(objectKey);
                continue;
            }
            if (entry->Get() == nullptr) {
                SetEmptyObjectEntry(objectKey, *entry);
            }
            if (!IsNewerVersion(entry, version)) {
                (void)lockedEntries.emplace(objectKey, std::make_pair(std::move(entry), version));
            } else {
                std::stringstream ss;
                ss << FormatString(
                    "[Migrate Data] %s version [%ld >= %ld] is newer, cache invalid: %s, primary copy: %s, not need to "
                    "migrate data.",
                    objectKey, (*entry)->GetCreateTime(), version,
                    (*entry)->stateInfo.IsCacheInvalid() ? "true" : "false", (*entry)->GetAddress());
                if (entry->Get()->IsWriteBackMode() && IsEqualVersion(entry, version)) {
                    ss << " Would add to l2 queue.";
                    std::future<Status> future;
                    LOG_IF_ERROR(asyncSendManager_->Add(objectKey, entry, future),
                                 FormatString("[Migrate Data] [%s] add to async queue failed", objectKey));
                }
                LOG(INFO) << ss.str();
                // The object's version is newer than the request node, it means that the object has been updated before
                // RPC arrived, so we can just treat it as an success op.
                entry->WUnlock();
                (void)successIds.emplace(objectKey);
            }
        }
    }
}

Status WorkerOcServiceMigrateImpl::QueryMasterMetadata(const std::unordered_set<std::string> &objectKeys,
                                                       QueryMetaMap &queryMetas,
                                                       std::unordered_set<std::string> &failedIds)
{
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    Status lastRc;
    std::unordered_map<std::string, std::unordered_set<std::string>> redirectIds;
    std::unordered_set<std::string> tmpFailedIds;
    for (auto &item : objKeysGrpByMaster) {
        HostPort masterAddr = item.first.GetAddressAndSaveDbName();
        const auto &ids = item.second;
        auto workerMasterApi = GetWorkerMasterApi(masterAddr);
        if (workerMasterApi == nullptr) {
            std::stringstream ss;
            ss << "[Migrate Data] hash master get failed, Replace primary copy failed: " << masterAddr.ToString();
            LOG(ERROR) << ss.str();
            lastRc = Status(StatusCode::K_RUNTIME_ERROR, ss.str());
            tmpFailedIds.insert(ids.begin(), ids.end());
            continue;
        }
        master::PureQueryMetaReqPb req;
        req.set_redirect(true);
        for (const auto &id : ids) {
            req.add_object_keys(id);
        }
        master::PureQueryMetaRspPb rsp;
        Status rc = PureQueryMetaRetry(workerMasterApi, req, rsp);
        if (rc.IsError()) {
            LOG(ERROR) << "[Migrate Data] Pure query meta failed: " << rc.ToString();
            tmpFailedIds.insert(ids.begin(), ids.end());
            lastRc = rc;
            continue;
        }
        for (const auto &meta : rsp.query_metas()) {
            (void)queryMetas.emplace(meta.meta().object_key(), meta);
        }

        for (const auto &info : rsp.info()) {
            auto &objectList = redirectIds[info.redirect_meta_address()];
            objectList.insert(info.change_meta_ids().begin(), info.change_meta_ids().end());
        }
    }

    Status rc = PureQueryMetaToRedirectMaster(redirectIds, queryMetas, tmpFailedIds);
    lastRc = rc.IsError() ? rc : lastRc;

    size_t failedSize = tmpFailedIds.size();
    failedIds.insert(tmpFailedIds.begin(), tmpFailedIds.end());
    return failedSize == objectKeys.size() ? lastRc : Status::OK();
}

Status WorkerOcServiceMigrateImpl::FillObjectsLocked(const MigrateDataReqPb &req, LockedEntryMap &lockedEntries,
                                                     const QueryMetaMap &metas, std::vector<RpcMessage> &payloads,
                                                     std::unordered_set<std::string> &successIds,
                                                     std::unordered_set<std::string> &failedIds,
                                                     ObjectInfoMap &needSendMasterIds)
{
    Status lastRc;
    const auto &infoList = req.objects();
    auto iter = infoList.begin();
    for (auto it = infoList.begin(); it != infoList.end(); ++it) {
        const auto &objectKey = it->object_key();
        auto lockedIt = lockedEntries.find(objectKey);
        if (lockedIt == lockedEntries.end()) {
            LOG(INFO) << FormatString("[Migrate Data] %s lock failed, would not be process this time.", objectKey);
            continue;
        }
        const auto &metaIt = metas.find(objectKey);
        if (metaIt == metas.end()) {
            LOG(INFO) << FormatString("[Migrate Data] %s has been deleted, not need to be process.", objectKey);
            if (failedIds.find(objectKey) == failedIds.end()) {
                (void)successIds.emplace(objectKey);
            }
            continue;
        }
        Status status =
            FillOneObjectLocked(lockedIt->second.first, *it, metaIt->second, payloads, req.type(), needSendMasterIds);
        if (IsNoSpace(status)) {
            std::transform(it, infoList.end(), std::inserter(failedIds, failedIds.end()),
                           [](const MigrateDataReqPb::ObjectInfoPb &info) { return info.object_key(); });
            lastRc = status;
            iter = it;
            break;
        }
        if (status.IsError()) {
            (void)failedIds.emplace(objectKey);
            lastRc = status;
        }
    }

    if (IsNoSpace(lastRc)) {
        for (auto it = iter; it != infoList.end(); ++it) {
            const auto &objectKey = it->object_key();
            auto lockedIt = lockedEntries.find(objectKey);
            if (lockedIt == lockedEntries.end()) {
                continue;
            }
            VLOG(1) << "[Migrate Data] " << objectKey << " Set cache invalid";
            lockedIt->second.first->Get()->stateInfo.SetCacheInvalid(true);
        }
    }
    return lastRc;
}

void WorkerOcServiceMigrateImpl::FillMetaToObjectEntries(LockedEntryMap &lockedEntries, const QueryMetaMap &metas,
                                                         std::unordered_set<std::string> &successIds,
                                                         std::unordered_set<std::string> &failedIds,
                                                         ObjectInfoMap &needReadDataIds)
{
    for (auto &[objectKey, it] : lockedEntries) {
        const auto &metaIt = metas.find(objectKey);
        if (metaIt == metas.end()) {
            LOG(INFO) << FormatString("[Migrate Data] %s has been deleted, not need to be process.", objectKey);
            if (failedIds.find(objectKey) == failedIds.end()) {
                (void)successIds.emplace(objectKey);
            }
            continue;
        }
        const auto &meta = metaIt->second;
        const auto &version = it.second;
        if (meta.meta().version() != version) {
            VLOG(1) << FormatString("[ObjectKey %s] Version %ld != %ld", objectKey, version, meta.meta().version());
            // Version mismatch means the object has been update, we will no need to update.
            (void)successIds.emplace(objectKey);
            continue;
        }
        // Fill metadata to object entry
        auto &entry = it.first;
        if ((*entry)->IsSpilled()) {
            LOG_IF_ERROR(WorkerOcSpill::Instance()->Delete(objectKey),
                         FormatString("[Migrate Data] Delete elder object %s failed", objectKey));
        }
        bool isNewCreate = IsNewCreatedObject(entry);
        SetObjectEntryAccordingToMeta(meta.meta(), GetMetadataSize(), *entry);
        (*entry)->stateInfo.SetPrimaryCopy(true);
        needReadDataIds.emplace(objectKey, std::make_pair(entry, isNewCreate));
    }
}

Status WorkerOcServiceMigrateImpl::AggregateAllocateHelper(const MigrateDataDirectReqPb &req,
                                                           const ObjectInfoMap &needReadDataIds,
                                                           std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                                           std::vector<uint32_t> &shmIndexMapping)
{
    // Calculate total size of small objects (< 1MB) that need to be aggregated and
    // verify memory availability for them before attempting aggregate allocation.
    constexpr uint64_t smallObjectThreshold = 1UL * 1024UL * 1024UL;
    uint64_t sumSmallObjects = 0;
    for (int i = 0; i < req.objects_size(); ++i) {
        const auto &object = req.objects(i);
        if (needReadDataIds.find(object.object_key()) == needReadDataIds.end()) {
            continue;
        }
        if (object.data_size() < smallObjectThreshold) {
            sumSmallObjects =
                (sumSmallObjects > UINT64_MAX - object.data_size()) ? UINT64_MAX : sumSmallObjects + object.data_size();
        }
    }
    static constexpr double factor = 1.2;
    uint64_t checkSize = static_cast<uint64_t>(static_cast<double>(sumSmallObjects) * factor);
    if (!IsMemoryAvailable(checkSize, MigrateType::SPILL)) {
        return Status(StatusCode::K_OUT_OF_MEMORY, "OOM");
    }

    // Perform aggregate allocation.
    const size_t metaSz = GetMetadataSize();
    std::function<void(std::function<void(uint64_t, uint64_t, uint32_t)>, bool &)> traversalHelper =
        [&req, &needReadDataIds, &metaSz](const std::function<void(uint64_t, uint64_t, uint32_t)> &collector,
                                          bool &needAggregate) {
            needAggregate = req.objects_size() > 1;
            for (int i = 0; i < req.objects_size(); i++) {
                const auto &object = req.objects(i);
                if (needReadDataIds.find(object.object_key()) == needReadDataIds.end()) {
                    continue;
                }
                collector(object.data_size(), object.data_size() + metaSz, i);
            }
        };
    const auto &firstObjectKey = req.objects().begin()->object_key();
    return AggregateAllocate(firstObjectKey, traversalHelper, evictionManager_, shmOwners, shmIndexMapping, false);
}

Status WorkerOcServiceMigrateImpl::FillDataToObjectEntries(const MigrateDataDirectReqPb &req,
                                                           const ObjectInfoMap &needReadDataIds,
                                                           ObjectInfoMap &needSendMasterIds,
                                                           std::unordered_set<std::string> &failedIds)
{
    // 1. Aggregate allocated memory for small objects.
    std::vector<uint32_t> shmIndexMapping(req.objects_size(), std::numeric_limits<uint32_t>::max());
    std::vector<std::shared_ptr<ShmOwner>> shmOwners;
    Status rc = AggregateAllocateHelper(req, needReadDataIds, shmOwners, shmIndexMapping);
    if (rc.IsError()) {
        LOG(ERROR) << "[Migrate Data] Aggregate allocate memory failed: " << rc.ToString();
        shmOwners.clear();
    }

    // 2. Start URMA read tasks.
    std::vector<ReadTask> tasks;
    Status startRc = StartRemoteReadTasks(req, needReadDataIds, shmIndexMapping, shmOwners, tasks, failedIds);
    if (tasks.empty()) {
        return startRc;
    }

    // 3. Wait tasks and finalize object entries.
    Status waitRc = WaitRemoteReadTasks(tasks, needSendMasterIds, failedIds);
    return waitRc.IsError() ? waitRc : startRc;
}

std::shared_ptr<ShmOwner> WorkerOcServiceMigrateImpl::GetShmOwnerByIndex(
    int idx, const std::vector<uint32_t> &shmIndexMapping,
    const std::vector<std::shared_ptr<ShmOwner>> &shmOwners) const
{
    if (idx < 0 || static_cast<size_t>(idx) >= shmIndexMapping.size()) {
        return nullptr;
    }
    const auto ownerIdx = shmIndexMapping[idx];
    return ownerIdx < shmOwners.size() ? shmOwners[ownerIdx] : nullptr;
}

Status WorkerOcServiceMigrateImpl::ProcessRemoteReadForObject(const MigrateDataDirectReqPb::ObjectInfoPb &object,
                                                              ObjectInfoMap::const_iterator needReadIt,
                                                              std::shared_ptr<ShmUnit> shmUnit, size_t metaSize,
                                                              std::vector<ReadTask> &tasks,
                                                              std::unordered_set<std::string> &failedIds)
{
    const auto &objectKey = object.object_key();
    const auto dataSize = object.data_size();
    const uint64_t localObjectAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
    uint64_t localSegAddress;
    uint64_t localSegSize;
    GetSegmentInfoFromShmUnit(shmUnit, localObjectAddress, localSegAddress, localSegSize);

    std::vector<uint64_t> eventKeys;
    Status rc =
        UrmaRead(object.urma_info(), localSegAddress, localSegSize, localObjectAddress, dataSize, metaSize, eventKeys);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] %s urma read failed: %s", objectKey, rc.ToString());
        failedIds.insert(objectKey);
        return rc;
    }

    tasks.push_back(ReadTask{ objectKey, std::move(eventKeys), std::move(shmUnit), needReadIt->second.first,
                              needReadIt->second.second });
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::StartRemoteReadTasks(const MigrateDataDirectReqPb &req,
                                                        const ObjectInfoMap &needReadDataIds,
                                                        const std::vector<uint32_t> &shmIndexMapping,
                                                        const std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                                        std::vector<ReadTask> &tasks,
                                                        std::unordered_set<std::string> &failedIds)
{
    tasks.reserve(static_cast<size_t>(req.objects_size()));

    const auto metaSize = GetMetadataSize();
    Status lastRc;

    auto markRemainingAsFailed = [&req, &needReadDataIds, &failedIds](int fromIdx) {
        for (int i = fromIdx; i < req.objects_size(); ++i) {
            const auto &objectKey = req.objects(i).object_key();
            if (needReadDataIds.find(objectKey) != needReadDataIds.end()) {
                failedIds.insert(objectKey);
            }
        }
    };

    for (int i = 0; i < req.objects_size(); ++i) {
        const auto &object = req.objects(i);
        const auto &objectKey = object.object_key();
        const auto needReadIt = needReadDataIds.find(objectKey);
        if (needReadIt == needReadDataIds.end()) {
            continue;
        }

        const auto dataSize = object.data_size();
        auto shmUnit = std::make_shared<ShmUnit>();
        const auto shmOwner = GetShmOwnerByIndex(i, shmIndexMapping, shmOwners);
        if (shmOwner) {
            lastRc = DistributeMemoryForObject(objectKey, dataSize, metaSize, true, shmOwner, *shmUnit);
        } else {
            lastRc = Status(StatusCode::K_OUT_OF_MEMORY, "OOM");
            if (IsMemoryAvailable(dataSize, MigrateType::SPILL)) {
                lastRc = AllocateMemoryForObject(objectKey, dataSize, metaSize, true, evictionManager_, *shmUnit);
            }
        }
        if (lastRc.IsError()) {
            LOG(ERROR) << FormatString("[Migrate Data] %s allocate memory failed: %s", objectKey, lastRc.ToString());
            markRemainingAsFailed(i);
            break;
        }
        evictionManager_->Add(objectKey);

        lastRc = ProcessRemoteReadForObject(object, needReadIt, shmUnit, metaSize, tasks, failedIds);
    }

    return lastRc;
}

Status WorkerOcServiceMigrateImpl::WaitRemoteReadTasks(std::vector<ReadTask> &tasks, ObjectInfoMap &needSendMasterIds,
                                                       std::unordered_set<std::string> &failedIds)
{
    const auto metaSize = GetMetadataSize();
    Status lastRc;
    for (auto &task : tasks) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        Status waitRc = WaitFastTransportEvent(task.eventKeys, remainingTime, errorHandler);
        if (waitRc.IsError()) {
            failedIds.insert(task.objectKey);
            lastRc = waitRc;
            continue;
        }

        needSendMasterIds.emplace(task.objectKey, std::make_pair(task.entry, task.isNewCreate));

        task.shmUnit->id = ShmKey::Intern(GetStringUuid());
        if (metaSize > 0) {
            (void)memset_s(task.shmUnit->GetPointer(), metaSize, 0, metaSize);
        }
        (*task.entry)->SetShmUnit(task.shmUnit);
        if (task.entry->Get()->IsWriteBackMode()) {
            std::future<Status> future;
            LOG_IF_ERROR(asyncSendManager_->Add(task.objectKey, task.entry, future),
                         FormatString("[Migrate Data] [%s] add to async queue failed", task.objectKey));
        }
    }
    return lastRc;
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryImpl(const std::string &originAddr,
                                                      const ObjectInfoMap &needSendMasterIds, const MigrateType &type,
                                                      std::unordered_set<std::string> &successIds,
                                                      std::unordered_set<std::string> &failedIds)
{
    std::unordered_set<std::string> objectKeys;
    for (const auto &item : needSendMasterIds) {
        const auto &objectKey = item.first;
        (void)objectKeys.emplace(objectKey);
    }

    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    Status lastRc;
    RedirectMap needRedirectIds;
    for (auto &item : objKeysGrpByMaster) {
        master::ReplacePrimaryReqPb req;
        req.set_redirect(true);
        req.set_origin_primary_addr(originAddr);
        req.set_new_primary_addr(localAddr_);
        req.set_remove_location(type == MigrateType::SPILL);
        HostPort masterAddr = item.first.GetAddressAndSaveDbName();
        const auto &ids = item.second;
        for (const auto &id : ids) {
            auto info = req.add_object_infos();
            info->set_object_key(id);
            auto iter = needSendMasterIds.find(id);
            if (iter != needSendMasterIds.end()) {
                info->set_version((*iter->second.first)->GetCreateTime());
            }
        }
        VLOG(1) << FormatString("[Migrate Data] Replace %ld objects primary location from %s to %s, master address: %s",
                                ids.size(), originAddr, localAddr_, masterAddr.ToString());
        auto workerMasterApi = GetWorkerMasterApi(masterAddr);
        if (workerMasterApi == nullptr) {
            std::stringstream ss;
            ss << "[Migrate Data] hash master get failed, Replace primary copy failed: " << masterAddr.ToString();
            LOG(ERROR) << ss.str();
            lastRc = Status(StatusCode::K_RUNTIME_ERROR, ss.str());
            failedIds.insert(ids.begin(), ids.end());
            continue;
        }
        master::ReplacePrimaryRspPb rsp;
        Status rc = ReplacePrimaryRetry(workerMasterApi, req, rsp);
        if (rc.IsError()) {
            lastRc = rc;
        }
        ProcessReplacePrimaryRsp(rsp, needSendMasterIds, successIds, failedIds, needRedirectIds);
    }

    Status rc = ReplacePrimaryToRedirectMaster(originAddr, needRedirectIds, needSendMasterIds, successIds, failedIds);
    if (rc.IsError()) {
        lastRc = rc;
    }
    return lastRc;
}

uint64_t WorkerOcServiceMigrateImpl::CalcRemainBytes(const MigrateType &type)
{
    switch (type) {
        case MigrateType::SPILL:
            return memory::Allocator::Instance()->GetMemoryAvailToHighWater();
        case MigrateType::SCALE_DOWN:
        default: {
            constexpr double remainThreshold = 0.8;
            uint64_t remainBytes = memory::Allocator::Instance()->GetTotalRealMemoryFree();
            if (WorkerOcSpill::Instance()->IsEnabled()) {
                remainBytes += WorkerOcSpill::Instance()->GetRemainActiveSpillSize();
            }
            return static_cast<uint64_t>(remainBytes * remainThreshold);
        }
    }
}

void WorkerOcServiceMigrateImpl::FillMigrateDataResponse(const MigrateDataReqPb &req,
                                                         const std::unordered_set<std::string> &successIds,
                                                         const std::unordered_set<std::string> &failedIds, bool oom,
                                                         MigrateDataRspPb &rsp)
{
    for (const auto &id : successIds) {
        rsp.add_success_ids(id);
    }
    for (const auto &id : failedIds) {
        rsp.add_fail_ids(id);
    }
    if (oom) {
        rsp.set_remain_bytes(0);
        rsp.set_disk_remain_bytes(0);
        rsp.set_limit_rate(0);
    } else {
        rateLimiter_.SlidingWindowUpdateRate(req.bytes_send());
        rsp.set_remain_bytes(CalcRemainBytes(req.type()));
        rsp.set_available_ratio(memory::Allocator::Instance()->GetMemoryAvailableRatio());
        uint64_t diskRemainBytes = memory::Allocator::Instance()->GetTotalRealMemoryFree(memory::CacheType::DISK);
        constexpr double remainThreshold = 0.8;
        rsp.set_disk_remain_bytes(static_cast<uint64_t>(diskRemainBytes * remainThreshold));
        rsp.set_disk_available_ratio(memory::Allocator::Instance()->GetMemoryAvailableRatio(memory::CacheType::DISK));
        uint64_t limitRate = CalculateNewRate(req.worker_addr());
        rsp.set_limit_rate(limitRate);
    }
    if (etcdCM_ != nullptr) {
        if (etcdCM_->IsPreLeaving(localAddr_)) {
            rsp.set_scale_down_state(MigrateDataRspPb::NEED_SCALE_DOWN);
        } else if (etcdCM_->IsDataMigrationStarted()) {
            rsp.set_scale_down_state(MigrateDataRspPb::DATA_MIGRATION_STARTED);
        } else {
            rsp.set_scale_down_state(MigrateDataRspPb::NONE);
        }
    }
}

void WorkerOcServiceMigrateImpl::FillMigrateDataDirectResponse(const std::unordered_set<std::string> &failedIds,
                                                               bool oom, MigrateDataDirectRspPb &rsp)
{
    for (const auto &id : failedIds) {
        rsp.add_failed_object_keys(id);
    }
    if (oom) {
        rsp.set_remain_bytes(0);
    } else {
        rsp.set_remain_bytes(CalcRemainBytes(MigrateType::SPILL));
    }
}

uint64_t WorkerOcServiceMigrateImpl::CalculateNewRate(const std::string &workerAddr)
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);

    uint64_t lastRate = rateMap_.count(workerAddr) ? rateMap_[workerAddr] : rateLimiter_.GetMaxBandwidth() / 2;
    auto availableBandwidth = rateLimiter_.GetAvailableBandwidth();
    uint64_t newRate;
    if (availableBandwidth < lastRate) {
        newRate = availableBandwidth;
    } else {
        newRate = (lastRate + availableBandwidth) / 2;  // 2 means new rate is half of available bandwidth and last rate
    }

    auto toMs = std::chrono::milliseconds(1);
    uint64_t timeStamp = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch() / toMs);
    rateTimeStampMap_[workerAddr] = timeStamp;
    rateMap_[workerAddr] = newRate;
    TimerQueue::TimerImpl timer;
    const uint32_t expireMs = 60000;
    auto weakPtr = weak_from_this();
    TimerQueue::GetInstance()->AddTimer(
        expireMs,
        [workerAddr, toMs, weakPtr]() {
            auto sharedPtr = weakPtr.lock();
            if (sharedPtr == nullptr) {
                return;
            }
            std::lock_guard<std::shared_timed_mutex> l(sharedPtr->mutex_);
            auto it = sharedPtr->rateTimeStampMap_.find(workerAddr);
            if (it == sharedPtr->rateTimeStampMap_.end()) {
                return;
            }
            uint64_t currTime = std::chrono::steady_clock::now().time_since_epoch() / toMs;
            if (currTime - it->second >= expireMs) {
                sharedPtr->rateMap_.erase(workerAddr);
                sharedPtr->rateTimeStampMap_.erase(workerAddr);
            }
        },
        timer);
    return newRate;
}

Status WorkerOcServiceMigrateImpl::PureQueryMetaOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                                     master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp)
{
    return api->PureQueryMeta(req, rsp);
}

Status WorkerOcServiceMigrateImpl::PureQueryMetaRetry(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                                      master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp)
{
    const int maxRetryCount = 3;
    int count = 0;
    Status status;
    do {
        count++;
        status = PureQueryMetaOnce(api, req, rsp);
        if (IsRpcError(status)) {
            continue;
        }
        if (MetaMovingDone(rsp)) {
            break;
        }
        rsp.Clear();
    } while (count <= maxRetryCount);
    return status;
}

Status WorkerOcServiceMigrateImpl::PureQueryMetaToRedirectMaster(
    const std::unordered_map<std::string, std::unordered_set<std::string>> &redirectIds, QueryMetaMap &queryMetas,
    std::unordered_set<std::string> &failedIds)
{
    Status lastRc;
    for (const auto &info : redirectIds) {
        const auto &addr = info.first;
        const auto &objects = info.second;

        // 1. Fill redirect request.
        master::PureQueryMetaReqPb req;
        master::PureQueryMetaRspPb rsp;
        req.set_redirect(false);
        for (const auto &objectKey : objects) {
            req.add_object_keys(objectKey);
        }

        // 2. Get redirect master api.
        HostPort masterAddr;
        Status rc = GetPrimaryReplicaAddr(addr, masterAddr);
        if (!rc.IsOk()) {
            lastRc = rc;
            LOG(WARNING) << "[Migrate Data] Get redirect master address failed: " << rc.ToString();
            failedIds.insert(objects.begin(), objects.end());
            continue;
        }
        auto masterApi = GetWorkerMasterApi(masterAddr);
        if (masterApi == nullptr) {
            LOG(ERROR) << "[Migrate Data] Failed to get redirect WorkerMasterApi, masterAddr: " << addr;
            failedIds.insert(objects.begin(), objects.end());
            continue;
        }

        // 3. Send request to redirect master.
        rc = PureQueryMetaRetry(masterApi, req, rsp);
        if (rc.IsError()) {
            lastRc = rc;
            LOG(WARNING) << "remove meta failed: " << rc.ToString();
        }

        for (const auto &meta : rsp.query_metas()) {
            (void)queryMetas.emplace(meta.meta().object_key(), meta);
        }
    }
    return lastRc;
}

Status WorkerOcServiceMigrateImpl::FillOneObjectLocked(std::shared_ptr<SafeObjType> &entry,
                                                       const MigrateDataReqPb::ObjectInfoPb &info,
                                                       const master::QueryMetaInfoPb &meta,
                                                       std::vector<RpcMessage> &payloads, const MigrateType &type,
                                                       ObjectInfoMap &needSendMasterIds)
{
    const auto &objectKey = info.object_key();
    if (meta.meta().version() != info.version()) {
        VLOG(1) << FormatString("[ObjectKey %s] Version %ld != %ld", objectKey, info.version(), meta.meta().version());
        // Version mismatch means the object has been update, we will no need to update.
        return Status::OK();
    }
    if ((*entry)->IsSpilled()) {
        LOG_IF_ERROR(WorkerOcSpill::Instance()->Delete(objectKey),
                     FormatString("[Migrate Data] Delete elder object %s failed", objectKey));
    }
    bool isNewCreate = IsNewCreatedObject(entry);
    SetObjectEntryAccordingToMeta(meta.meta(), GetMetadataSize(), *entry);
    RETURN_IF_NOT_OK(SaveDataWithObjectLocked(entry, info, payloads, type));
    if ((*entry)->IsMemoryCache() && (*entry)->GetShmUnit() == nullptr) {
        (*entry)->stateInfo.SetSpillState(true);
    }
    (*entry)->stateInfo.SetPrimaryCopy(true);
    needSendMasterIds.emplace(objectKey, std::make_pair(entry, isNewCreate));
    if ((*entry).Get()->IsWriteBackMode()) {
        std::future<Status> future;
        LOG_IF_ERROR(asyncSendManager_->Add(objectKey, entry, future),
                     FormatString("[Migrate Data] [%s] add to async queue failed", objectKey));
    }
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::SaveDataWithObjectLocked(std::shared_ptr<SafeObjType> &entry,
                                                            const MigrateDataReqPb::ObjectInfoPb &info,
                                                            std::vector<RpcMessage> &payloads, const MigrateType &type)
{
    const auto &objectKey = info.object_key();
    const auto &indexs = info.part_index();
    if (indexs.empty()) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("[Migrate Data] [%s]'s part index [%s] is large than payloads size [%ld]",
                                             objectKey, VectorToString(info.part_index()), payloads.size()));
    }

    std::vector<std::pair<const uint8_t *, uint64_t>> pairs;
    for (int i = 0; i < indexs.size(); ++i) {
        auto index = indexs[i];
        if (index >= payloads.size()) {
            RETURN_STATUS_LOG_ERROR(
                StatusCode::K_RUNTIME_ERROR,
                FormatString("[Migrate Data] [%s]'s part index [%s] is large than payloads size [%ld]", objectKey,
                             VectorToString(info.part_index()), payloads.size()));
        }
        pairs.emplace_back(std::make_pair<const uint8_t *, uint64_t>(
            static_cast<const uint8_t *>(payloads[index].Data()), payloads[index].Size()));
    }

    Status rc = Status(StatusCode::K_OUT_OF_MEMORY, "OOM");
    if (IsResourceAvailable(type, (*entry)->modeInfo.GetCacheType(), info.data_size())) {
        rc = AllocateAndAssignData(objectKey, entry, pairs, info.data_size());
        VLOG(1) << FormatString("[ObjectKey %s] Save data to memory, result: %s", objectKey, rc.ToString());
    }
    if (type == MigrateType::SCALE_DOWN && (*entry)->IsMemoryCache() && rc.IsError()
        && IsSpillAvaialble(info.data_size())) {
        rc = WorkerOcSpill::Instance()->Spill(objectKey, pairs, info.data_size());
        VLOG(1) << FormatString("[ObjectKey %s] Save data to spill dir, result: %s", objectKey, rc.ToString());
        LOG_IF(ERROR, rc.IsError()) << FormatString("[Migrate Data] Spill object [%s] failed: %s", objectKey,
                                                    rc.ToString());
    }
    return rc;
}

Status WorkerOcServiceMigrateImpl::AllocateAndAssignData(
    const std::string &objectKey, std::shared_ptr<SafeObjType> &entry,
    const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads, uint64_t size)
{
    auto shmUnit = std::make_shared<ShmUnit>();
    auto metaSize = GetMetadataSize();
    auto needSize = size + metaSize;
    auto tenantId = TenantAuthManager::ExtractTenantId(objectKey);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        shmUnit->AllocateMemory(tenantId, needSize, false, ServiceType::OBJECT,
                                static_cast<memory::CacheType>((*entry)->modeInfo.GetCacheType())),
        FormatString("[Migrate Data] %s allocate memory failed, size: %ld", objectKey, needSize));
    shmUnit->id = ShmKey::Intern(GetStringUuid());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        shmUnit->MemoryCopy(payloads, memcpyThreadPool_, metaSize),
        FormatString("[Migrate Data] Memory copy failed, offset: %ld, size: %ld", metaSize, needSize));
    if (metaSize > 0) {
        (void)memset_s(shmUnit->GetPointer(), metaSize, 0, metaSize);
    }
    (*entry)->SetShmUnit(shmUnit);
    evictionManager_->Add(objectKey);
    return Status::OK();
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryOnce(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                                      master::ReplacePrimaryReqPb &req,
                                                      master::ReplacePrimaryRspPb &rsp)
{
    return api->ReplacePrimary(req, rsp);
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryRetry(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                                       master::ReplacePrimaryReqPb &req,
                                                       master::ReplacePrimaryRspPb &rsp)
{
    constexpr int maxRetryCount = 3;
    int count = 0;
    Status status;
    do {
        ++count;
        status = ReplacePrimaryOnce(api, req, rsp);
        if (!status.IsOk()) {
            continue;
        }
        if (MetaMovingDone(rsp)) {
            break;
        }
    } while (count <= maxRetryCount);

    if (IsRpcTimeout(status)) {
        rsp.Clear();
        for (const auto &info : req.object_infos()) {
            rsp.add_failed_ids(info.object_key());
        }
    }
    return status;
}

Status WorkerOcServiceMigrateImpl::ReplacePrimaryToRedirectMaster(const std::string &originAddr,
                                                                  const RedirectMap &needRedirectIds,
                                                                  const ObjectInfoMap &needSendMasterIds,
                                                                  std::unordered_set<std::string> &successIds,
                                                                  std::unordered_set<std::string> &failedIds)
{
    Status lastRc;
    for (const auto &item : needRedirectIds) {
        const auto &address = item.first;
        const auto &infos = item.second;

        // 1. Fill redirect request.
        VLOG(1) << FormatString("[Migrate Data] Redirect Replace %ld objects primary, meta address: %s", infos.size(),
                                address);
        master::ReplacePrimaryReqPb req;
        master::ReplacePrimaryRspPb rsp;
        req.set_origin_primary_addr(originAddr);
        req.set_new_primary_addr(localAddr_);
        req.set_redirect(false);
        for (const auto &info : infos) {
            auto newInfo = req.add_object_infos();
            newInfo->CopyFrom(info);
        }

        // 2. Get redirect master api.
        HostPort masterAddr;
        Status rc = GetPrimaryReplicaAddr(address, masterAddr);
        if (!rc.IsOk()) {
            lastRc = rc;
            LOG(WARNING) << "[Migrate Data] Get redirect master address failed: " << rc.ToString();
            std::transform(infos.begin(), infos.end(), std::inserter(failedIds, failedIds.end()),
                           [](const auto &info) { return info.object_key(); });
            continue;
        }

        auto masterApi = GetWorkerMasterApi(masterAddr);
        if (masterApi == nullptr) {
            std::stringstream ss;
            ss << "[Migrate Data] Failed to get redirect WorkerMasterApi, masterAddr: " << address;
            LOG(ERROR) << ss.str();
            lastRc = Status(StatusCode::K_RUNTIME_ERROR, ss.str());
            std::transform(infos.begin(), infos.end(), std::inserter(failedIds, failedIds.end()),
                           [](const auto &info) { return info.object_key(); });
            continue;
        }

        // 3. Send request to redirect master.
        rc = ReplacePrimaryRetry(masterApi, req, rsp);
        if (rc.IsError()) {
            lastRc = rc;
            LOG(WARNING) << "remove meta failed: " << rc.ToString();
        }
        RedirectMap needRedirectIds1;
        ProcessReplacePrimaryRsp(rsp, needSendMasterIds, successIds, failedIds, needRedirectIds1);
        if (!needRedirectIds1.empty()) {
            LOG(WARNING) << "[Migrate Data] The redirect ids should not happen: " << needRedirectIds1.size();
        }
    }

    return lastRc;
}

void WorkerOcServiceMigrateImpl::ProcessReplacePrimaryRsp(master::ReplacePrimaryRspPb &rsp,
                                                          const ObjectInfoMap &needSendMasterIds,
                                                          std::unordered_set<std::string> &successIds,
                                                          std::unordered_set<std::string> &failedIds,
                                                          RedirectMap &needRedirectIds)
{
    // 1. We treat the expire objects as success objects, we just need to clear their resources.
    successIds.insert(rsp.expired_ids().begin(), rsp.expired_ids().end());
    successIds.insert(rsp.success_ids().begin(), rsp.success_ids().end());
    failedIds.insert(rsp.failed_ids().begin(), rsp.failed_ids().end());

    // 2. Fill redirect infos.
    for (const auto &redirectInfo : rsp.info()) {
        const auto &addr = redirectInfo.redirect_meta_address();
        for (const auto &objectKey : redirectInfo.change_meta_ids()) {
            auto it = needSendMasterIds.find(objectKey);
            if (it == needSendMasterIds.end()) {
                LOG(WARNING) << FormatString("[Migrate Data] %s not found in needSendMasterIds, it should not happen!",
                                             objectKey);
                (void)failedIds.emplace(objectKey);
                continue;
            }
            auto info = needRedirectIds[addr].Add();
            info->set_object_key(objectKey);
            info->set_version((*it->second.first)->GetCreateTime());
        }
    }

    // 3. Rollback failed or expired objects.
    RollbackObjects(rsp.expired_ids(), needSendMasterIds);
}

template <typename Container>
void WorkerOcServiceMigrateImpl::RollbackObjects(const Container &objectKeys, const ObjectInfoMap &objectInfos)
{
    for (const auto &objectKey : objectKeys) {
        VLOG(1) << "Rollback object: " << objectKey;
        auto it = objectInfos.find(objectKey);
        if (it == objectInfos.end()) {
            LOG(WARNING) << FormatString("[Migrate Data] %s is not found in send list, it should not happen in general",
                                         objectKey);
            continue;
        }
        bool needDel = it->second.second;
        auto &entry = it->second.first;
        if ((*entry)->IsSpilled() && (*entry)->GetShmUnit() == nullptr) {
            LOG_IF_ERROR(WorkerOcSpill::Instance()->Delete(objectKey),
                         FormatString("[Migrate Data] Rollback %s from disk failed", objectKey));
        } else {
            evictionManager_->Erase(objectKey);
        }
        if (needDel) {
            (void)objectTable_->Erase(objectKey, *entry);
        } else {
            (*entry)->stateInfo.SetSpillState(false);
            (*entry)->stateInfo.SetCacheInvalid(true);
            (*entry)->SetShmUnit(nullptr);
        }
    }
}

bool WorkerOcServiceMigrateImpl::IsEqualVersion(const std::shared_ptr<SafeObjType> &entry, uint64_t version)
{
    auto *entryImpl = entry->Get();
    return !entryImpl->stateInfo.IsCacheInvalid() && entryImpl->GetCreateTime() >= version;
}

bool WorkerOcServiceMigrateImpl::IsNewerVersion(const std::shared_ptr<SafeObjType> &entry, uint64_t version)
{
    auto *entryImpl = entry->Get();
    return (entryImpl->stateInfo.IsCacheInvalid() && entryImpl->GetCreateTime() > version)
           || (!entryImpl->stateInfo.IsCacheInvalid() && entryImpl->GetCreateTime() >= version);
}

bool WorkerOcServiceMigrateImpl::IsNewCreatedObject(std::shared_ptr<SafeObjType> &entry) const
{
    return (*entry)->GetCreateTime() == 0 && (*entry)->GetDataSize() == 0;
}

bool WorkerOcServiceMigrateImpl::IsMemoryAvailable(uint64_t size, MigrateType type) const
{
    INJECT_POINT("worker.migrate_service.memory_available", []() { return false; });
    INJECT_POINT("worker.migrate_service.memory_available1", []() {
        static int count = 0;
        constexpr int maxCount = 200;
        return count++ < maxCount;
    });
    uint64_t usedMemory = memory::Allocator::Instance()->GetTotalRealMemoryUsage();
    if (usedMemory > UINT64_MAX - size) {
        // overflow check
        return false;
    }
    uint64_t memOccupied = usedMemory + size;
    uint64_t freeMemory = memory::Allocator::Instance()->GetTotalRealMemoryFree();
    if (usedMemory > UINT64_MAX - freeMemory) {
        // overflow check
        return false;
    }
    switch (type) {
        case MigrateType::SPILL:
            return memory::Allocator::Instance()->GetMemoryAvailToHighWater() > size;
        case MigrateType::SCALE_DOWN:
        default:
            return memOccupied <= (usedMemory + freeMemory) * MIGRATE_SCALE_DOWN_HIGH_WATER_FACTOR;
    }
}

bool WorkerOcServiceMigrateImpl::IsSpillAvaialble(uint64_t size) const
{
    INJECT_POINT("worker.migrate_service.spill_available", []() { return false; });
    INJECT_POINT("worker.migrate_service.spill_available1", []() {
        static int count = 0;
        constexpr int maxCount = 200;
        return count++ < maxCount;
    });
    auto handler = WorkerOcSpill::Instance();
    return handler->IsEnabled() && !handler->IsActiveSpillSizeExceedHWM(size);
}

bool WorkerOcServiceMigrateImpl::IsDiskAvailable(uint64_t size) const
{
    if (!memory::Allocator::Instance()->IsDiskAvailable()) {
        constexpr int freq = 10;
        LOG_EVERY_T(INFO, freq) << "[Migrate Data] Disk now is not available";
        return false;
    }
    auto realMemoryUsage =
        memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::OBJECT, memory::CacheType::DISK);
    uint64_t used = size < UINT64_MAX - realMemoryUsage ? realMemoryUsage + size : UINT64_MAX;
    uint64_t total = memory::Allocator::Instance()->GetMaxMemoryLimit(memory::CacheType::DISK);
    const double factor = 0.95;
    return used <= total * factor;
}

bool WorkerOcServiceMigrateImpl::IsResourceAvailable(const MigrateType &type, CacheType cacheType, uint64_t size) const
{
    return cacheType == CacheType::MEMORY ? IsMemoryAvailable(size, type) : IsDiskAvailable(size);
}

bool WorkerOcServiceMigrateImpl::IsNoSpace(const Status &status) const
{
    return status.GetCode() == StatusCode::K_NO_SPACE || status.GetCode() == StatusCode::K_OUT_OF_MEMORY;
}
}  // namespace object_cache
}  // namespace datasystem
