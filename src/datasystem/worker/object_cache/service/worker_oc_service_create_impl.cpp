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
 * Description: Defines the worker service processing create buffer process.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_create_impl.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"

DS_DECLARE_uint64(oc_shm_transfer_threshold_kb);

namespace datasystem {
namespace object_cache {

WorkerOcServiceCreateImpl::WorkerOcServiceCreateImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                                                     std::shared_ptr<AkSkManager> akSkManager)
    : WorkerOcServiceCrudCommonApi(initParam), etcdCM_(etcdCM), akSkManager_(std::move(akSkManager))
{
}

Status WorkerOcServiceCreateImpl::Create(const CreateReqPb &req, CreateRspPb &resp)
{
    INJECT_POINT("worker.Create.begin");
    workerOperationTimeCost.Clear();
    Timer timer;
    PerfPoint point(PerfKey::WORKER_CREATE_OBJECT);
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_CREATE);
    LOG(INFO) << FormatString(
        "[Create] Receive create meta request, clientId: %s, objectKey: %s, cacheType: %u, size: %zu", req.client_id(),
        req.object_key(), req.cache_type(), req.data_size());
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    INJECT_POINT_NO_RETURN("WorkerOcServiceCreateImpl.Create.timeoutMs",
                           [&remainingTimeMs]() { remainingTimeMs = -1; });
    if (remainingTimeMs <= 0) {
        return Status(
            StatusCode::K_RPC_DEADLINE_EXCEEDED,
            FormatString("The create request process time has exceeded the request timeout time (remaining: %lld ms)",
                         remainingTimeMs));
    }
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, StatusCode::K_NOT_READY, "ETCD cluster manager is not provided.");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    Status rc = CreateImpl(tenantId, ClientKey::Intern(req.client_id()), req.object_key(), req.data_size(), resp,
                           static_cast<CacheType>(req.cache_type()));
    RequestParam reqParam;
    reqParam.objectKey = req.object_key().substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    posixPoint.Record(rc.GetCode(), std::to_string(req.data_size()), reqParam, rc.GetMsg());
    point.Record();
    workerOperationTimeCost.Append("Total Create", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("[Client %s] [ObjectKey %s] The operations of worker Create %s", req.client_id(),
                              req.object_key(), workerOperationTimeCost.GetInfo());
    return rc;
}

Status WorkerOcServiceCreateImpl::CreateImpl(const std::string &tenantId, const ClientKey &clientId,
                                             const std::string &rawObjectKey, size_t dataSize, CreateRspPb &resp,
                                             CacheType cacheType)
{
    auto objectKey = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, rawObjectKey);
    // Check whether the object is sealed.
    std::shared_ptr<SafeObjType> entry;
    CHECK_FAIL_RETURN_STATUS(
        !(objectTable_->Get(objectKey, entry).IsOk() && entry->Get() != nullptr && (*entry)->IsSealed()),
        K_OC_ALREADY_SEALED, "Cannot create sealed object.");

    // Given size, construct shmUnit, generate shm uuid and add client's reference on shmUnit.
    auto shmUnit = std::make_shared<ShmUnit>();
    auto metadataSize = GetMetadataSize();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AllocateMemoryForObject(objectKey, dataSize, metadataSize, true, evictionManager_, *shmUnit, cacheType),
        "worker allocate memory failed");

    std::string shmUnitId;
    IndexUuidGenerator(shmIdCounter.fetch_add(1), shmUnitId);
    shmUnit->id = ShmKey::Intern(std::move(shmUnitId));
    memoryRefTable_->AddShmUnit(clientId, shmUnit);

    // Construct CreateRespPb.
    resp.set_store_fd(shmUnit->GetFd());
    resp.set_mmap_size(shmUnit->GetMmapSize());
    resp.set_offset(shmUnit->GetOffset());
    resp.set_shm_id(shmUnit->GetId());
    resp.set_metadata_size(metadataSize);
    INJECT_POINT("worker.Create.AllocateMemory");
    return Status::OK();
}

Status WorkerOcServiceCreateImpl::AggregateAllocateHelper(const MultiCreateReqPb &req,
                                                          std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                                          std::vector<uint32_t> &shmIndexMapping)
{
    const size_t metaSz = GetMetadataSize();
    std::function<void(std::function<void(uint64_t, uint64_t, uint32_t)>, bool &)> traversalHelper =
        [&req, &metaSz](const std::function<void(uint64_t, uint64_t, uint32_t)> &collector, bool &needAggregate) {
            needAggregate = req.object_key_size() > 1;
            for (int i = 0; i < req.object_key_size(); i++) {
                collector(req.data_size(i), req.data_size(i) + metaSz, i);
            }
        };
    const auto &firstObjectKey = *req.object_key().begin();
    return AggregateAllocate(firstObjectKey, traversalHelper, evictionManager_, shmOwners, shmIndexMapping);
}

Status WorkerOcServiceCreateImpl::MultiCreateImpl(const MultiCreateReqPb &req, const std::string &tenantId,
                                                  MultiCreateRspPb &resp)
{
    PerfPoint point(PerfKey::WORKER_MULTI_CREATE_AGGREGATE_ALLOC);
    int objectSize = req.object_key_size();
    std::vector<uint32_t> shmIndexMapping(req.object_key_size(), std::numeric_limits<uint32_t>::max());
    std::vector<std::shared_ptr<ShmOwner>> shmOwners;
    AggregateAllocateHelper(req, shmOwners, shmIndexMapping);
    std::vector<CreateRspPb> subRsp(objectSize);
    std::vector<Status> results(objectSize);

    point.RecordAndReset(PerfKey::WORKER_MULTI_CREATE_GET_SHM_UNITS);
    auto createMeta = [&] (int start, int end) {
        std::vector<std::shared_ptr<ShmUnit>> shmUnits(end - start + 1);
        for (int i = start, j = 0; i < end; i++, j++) {
            if (!req.skip_check_existence() && resp.exists(i)) {
                continue;
            }
            const auto &objectKey = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_key(i));

            PerfPoint point(PerfKey::WORKER_MULTI_CREATE_ALLOC_FOR_OBJECT);
            std::shared_ptr<ShmOwner> shmOwner = nullptr;
            if (shmIndexMapping.size() > static_cast<size_t>(i) && shmOwners.size() > shmIndexMapping[i]) {
                shmOwner = shmOwners[shmIndexMapping[i]];
            }
            // Given size, construct shmUnit, generate shm uuid and add client's reference on shmUnit.
            auto shmUnit = std::make_shared<ShmUnit>();
            auto metadataSize = GetMetadataSize();
            auto dataSize = req.data_size(i);
            if (shmOwner) {
                results[i] = DistributeMemoryForObject(objectKey, dataSize, metadataSize, true, shmOwner, *shmUnit);
            } else {
                results[i] =
                    AllocateMemoryForObject(objectKey, dataSize, metadataSize, true, evictionManager_, *shmUnit);
            }
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(results[i], "worker allocate memory failed");

            point.RecordAndReset(PerfKey::WORKER_MULTI_CREATE_GENERATE_SHM_UUID);
            std::string shmUnitId;
            IndexUuidGenerator(shmIdCounter.fetch_add(1), shmUnitId);
            shmUnit->id = ShmKey::Intern(shmUnitId);
            shmUnits[j] = shmUnit;

            point.RecordAndReset(PerfKey::WORKER_MULTI_CREATE_FILL_SUB_RSP);
            // Construct CreateRespPb.
            CreateRspPb subResp;
            subRsp[i].set_store_fd(shmUnit->GetFd());
            subRsp[i].set_mmap_size(shmUnit->GetMmapSize());
            subRsp[i].set_offset(shmUnit->GetOffset());
            subRsp[i].set_shm_id(shmUnit->GetId());
            subRsp[i].set_metadata_size(metadataSize);
        }
        PerfPoint point(PerfKey::WORKER_MULTI_CREATE_ADD_SHM_UNITS);
        memoryRefTable_->AddShmUnits(ClientKey::Intern(req.client_id()), shmUnits);
        return Status::OK();
    };

    static const int parallelThreshold = 128;
    static const int parallism = 4;
    if (objectSize > parallelThreshold) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Parallel::ParallelFor<int>(0, objectSize, createMeta, 0, parallism),
                                         "ParallelFor failed");
    } else {
        createMeta(0, objectSize);
    }

    point.RecordAndReset(PerfKey::WORKER_MULTI_CREATE_FILL_ALL_RSP);
    resp.mutable_results()->Reserve(objectSize);
    for (int i = 0; i < objectSize; i++) {
        RETURN_IF_NOT_OK(results[i]);
        resp.mutable_results()->Add(std::move(subRsp[i]));
    }
    return Status::OK();
}

void WorkerOcServiceCreateImpl::CheckExistence(const MultiCreateReqPb &req, const std::string &tenantId,
                                               MultiCreateRspPb &resp)
{
    for (int i = 0; i < req.object_key().size(); i++) {
        const auto &objectKey = req.object_key(i);
        // Check whether the object is in local.
        {
            auto key = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, objectKey);
            std::shared_ptr<SafeObjType> entry;
            if (objectTable_->Get(key, entry).IsOk() && entry->RLock(false).IsOk()) {
                Raii unlock([&entry]() { entry->RUnlock(); });
                if ((*entry)->IsBinary() && !(*entry)->IsInvalid()) {
                    resp.add_exists(true);
                    continue;
                };
            }
        }
        resp.add_exists(false);
    }
}

Status WorkerOcServiceCreateImpl::MultiCreate(const MultiCreateReqPb &req, MultiCreateRspPb &resp)
{
    PerfPoint pointAll(PerfKey::WORKER_MULTI_CREATE_TOTAL);
    PerfPoint point(PerfKey::WORKER_MULTI_CREATE_INPUT_CHECK);
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, StatusCode::K_NOT_READY, "ETCD cluster manager is not provided.");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_key_size()), StatusCode::K_INVALID,
                                         "invalid object size");
    CHECK_FAIL_RETURN_STATUS(req.object_key_size() == req.data_size_size(), K_INVALID,
                             FormatString("object key count %zu not match with data size count %zu",
                                          req.object_key_size(), req.data_size_size()));
    if (!req.skip_check_existence()) {
        CheckExistence(req, tenantId, resp);
    }
    point.RecordAndReset(PerfKey::WORKER_MULTI_CREATE_IMPL);
    Status rc = MultiCreateImpl(req, tenantId, resp);
    if (rc.IsError()) {
        // Rollback all memory if failed.
        const auto clientId = ClientKey::Intern(req.client_id());
        for (auto &subResp : resp.results()) {
            memoryRefTable_->RemoveShmUnit(clientId, ShmKey::Intern(subResp.shm_id()));
        }
        resp.Clear();
    }
    return rc;
}
}  // namespace object_cache
}  // namespace datasystem
