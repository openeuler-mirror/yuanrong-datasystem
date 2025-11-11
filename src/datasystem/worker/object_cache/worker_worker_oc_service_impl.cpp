/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Defines the worker worker service processing main class.
 */
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"

#include <thread>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/rdma/urma_manager_wrapper.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/common/perf/perf_manager.h"

DS_DECLARE_int32(oc_worker_worker_direct_port);

namespace datasystem {
namespace object_cache {
WorkerWorkerOCServiceImpl::WorkerWorkerOCServiceImpl(
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc, std::shared_ptr<AkSkManager> akSkManager,
    EtcdStore *etcdStore, EtcdClusterManager *etcdCm)
    : ocClientWorkerSvc_(std::move(clientSvc)),
      akSkManager_(std::move(akSkManager)),
      etcdStore_(etcdStore),
      etcdCm_(etcdCm)
{
}

WorkerWorkerOCServiceImpl::~WorkerWorkerOCServiceImpl()
{
    LOG(INFO) << "WorkerWorkerOCServiceImpl exit";
}

Status WorkerWorkerOCServiceImpl::Init()
{
    CHECK_FAIL_RETURN_STATUS(ocClientWorkerSvc_ != nullptr, StatusCode::K_NOT_READY,
                             "ClientWorkerService must be initialized before WorkerWorkerService construction");
    return WorkerWorkerOCService::Init();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemote(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetObjectRemoteRspPb, GetObjectRemoteReqPb>> serverApi)
{
    PerfPoint point(PerfKey::WORKER_SERVER_GET_REMOTE);
    GetObjectRemoteReqPb req;
    GetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    PerfPoint pointRead(PerfKey::WORKER_SERVER_GET_REMOTE_READ);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "GetObjectRemote read error");
    pointRead.Record();
    PerfPoint pointImpl(PerfKey::WORKER_SERVER_GET_REMOTE_IMPL);
    INJECT_POINT("worker.GetObjectRemote.afterRead");
    // K_OC_REMOTE_GET_NOT_ENOUGH error happens only when URMA is used for RDMA and size of the object
    // is different from the request
    RETURN_IF_NOT_OK_EXCEPT(GetObjectRemote(req, rsp, payload), StatusCode::K_OC_REMOTE_GET_NOT_ENOUGH);
    pointImpl.Record();
    PerfPoint pointWrite(PerfKey::WORKER_SERVER_GET_REMOTE_WRITE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "GetObjectRemote write error");
    pointWrite.Record();
    PerfPoint pointSendPayload(PerfKey::WORKER_SERVER_GET_REMOTE_SENDPAYLOAD);

    if (rsp.data_in_payload()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload({}, FLAGS_oc_worker_worker_direct_port > 0),
                                         "GetObjectRemote send payload error");
    } else {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload(payload, FLAGS_oc_worker_worker_direct_port > 0),
                                         "GetObjectRemote send payload error");
    }

    pointSendPayload.Record();
    LOG(INFO) << FormatString("pull success");
    point.Record();
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                  std::vector<RpcMessage> &payload)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::vector<uint64_t> dummyKeys;
    RETURN_IF_NOT_OK(GetObjectRemoteHandler(req, rsp, payload, true, dummyKeys));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteBatchWrite(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                            std::vector<RpcMessage> &payload,
                                                            std::vector<uint64_t> &keys)
{
    RETURN_IF_NOT_OK(GetObjectRemoteHandler(req, rsp, payload, false, keys));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteHandler(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                         std::vector<RpcMessage> &payload, bool blocking,
                                                         std::vector<uint64_t> &keys)
{
    const std::string &objectKey = req.object_key();
    const std::string &requestId = req.request_id();
    LOG(INFO) << FormatString("Processing pull object[%s] request[%s] offset[%ld] size[%ld]", objectKey, requestId,
                              req.read_offset(), req.read_size());
    INJECT_POINT("worker.worker_worker_remote_get_sleep");
    INJECT_POINT("worker.worker_worker_remote_get_failure");
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "objectKey is empty.");
    Status status = GetObjectRemoteImpl(req, rsp, payload, blocking, keys);
    INJECT_POINT("worker.batch_get_failure_for_keys", [&objectKey]() {
        if (objectKey == "key2") {
            return Status(K_RUNTIME_ERROR, "Injected K_RUNTIME_ERROR");
        } else if (objectKey == "key3") {
            return Status(K_WORKER_PULL_OBJECT_NOT_FOUND, "Injected K_WORKER_PULL_OBJECT_NOT_FOUND");
        } else if (objectKey == "key0") {
            return Status(K_OUT_OF_MEMORY, "Injected K_OUT_OF_MEMORY");
        }
        return Status::OK();
    });
    if (status.GetCode() == K_INVALID || status.GetCode() == K_NOT_FOUND) {
        status = Status(K_WORKER_PULL_OBJECT_NOT_FOUND, status.GetMsg());
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        status, FormatString("[ObjectKey %s] Get object remote failed, requestId: %s, workerAddr: %s", objectKey,
                             requestId, localAddress_.ToString()));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetSafeObjectEntry(const std::string &objectKey, bool tryLock, uint64_t version,
                                                     std::shared_ptr<SafeObjType> &safeEntry)
{
    if (ocClientWorkerSvc_->IsInRemoteGetProgress(objectKey)) {
        LOG(INFO) << FormatString("[ObjectKey %s] Data is in getting, now can not be get", objectKey);
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object not found");
    } else if (ocClientWorkerSvc_->IsInRollbackProgress(objectKey)) {
        LOG(INFO) << FormatString("[ObjectKey %s] Data is in rolling back, now can not be get", objectKey);
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object not found");
    }
    bool insert = false;
    RETURN_IF_NOT_OK(ocClientWorkerSvc_->objectTable_->ReserveGetAndLock(objectKey, safeEntry, insert, false, false));
    if (insert) {
        Raii innerUnlock([&safeEntry]() { safeEntry->WUnlock(); });
        StatusCode code;
        if (ocClientWorkerSvc_->IsInRemoteGetProgress(objectKey)) {
            LOG(INFO) << FormatString("[ObjectKey %s] Data is in getting, now can not be get", objectKey);
            code = StatusCode::K_NOT_FOUND;
        } else if (ocClientWorkerSvc_->IsInRollbackProgress(objectKey)) {
            LOG(INFO) << FormatString("[ObjectKey %s] Data is in rolling back, now can not be get", objectKey);
            code = StatusCode::K_NOT_FOUND;
        } else if (!tryLock) {
            // get data from L2 cache if worker is primary copy
            return ocClientWorkerSvc_->GetDataFromL2CacheForPrimaryCopy(objectKey, version, safeEntry);
        } else {
            // tryLock it is the local master call, we need to avoid deadlock.
            code = StatusCode::K_UNKNOWN_ERROR;
        }
        RETURN_STATUS(code, "Object not found");
    }
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteImpl(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                      std::vector<RpcMessage> &outPayload, bool blocking,
                                                      std::vector<uint64_t> &keys)
{
    (void)keys;
    (void)blocking;
    const std::string &objectKey = req.object_key();
    const bool tryLock = req.try_lock();
    const uint64_t version = req.version();
    const uint64_t offset = req.read_offset();
    const uint64_t size = req.read_size();
    const uint64_t expectedDataSize = req.data_size();
    std::shared_ptr<SafeObjType> safeEntry;
    // If entry insert failed, it would not be locked.
    RETURN_IF_NOT_OK(GetSafeObjectEntry(objectKey, tryLock, version, safeEntry));

    // not need WLock
    if (tryLock) {
        int maxRetryCount = 5;
        Status s;
        do {
            s = safeEntry->TryRLock();
            if (s.IsOk()) {
                break;
            }
            --maxRetryCount;
        } while (maxRetryCount > 0 && s.GetCode() == StatusCode::K_TRY_AGAIN);
        RETURN_IF_NOT_OK(s);
    } else {
        RETURN_IF_NOT_OK(safeEntry->RLock());
    }
    Raii raii([safeEntry]() { safeEntry->RUnlock(); });
    auto &entry = *safeEntry;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!entry->stateInfo.IsCacheInvalid() && !entry->IsInvalid(), K_INVALID,
                                         FormatString("[ObjectKey %s] is invalid", objectKey));
    LOG_IF(WARNING, entry->GetCreateTime() != version) << FormatString(
        "[ObjectKey %s] Version: %ld, require version: %ld", objectKey, entry->GetCreateTime(), version);
    if (IsUrmaEnabled() && req.has_urma_info() && entry->GetDataSize() != expectedDataSize) {
        // Return error with changed size, so the request can be retried.
        rsp.mutable_error()->set_error_code(StatusCode::K_OC_REMOTE_GET_NOT_ENOUGH);
        rsp.set_data_size(static_cast<int64_t>(entry->GetDataSize()));
        INJECT_POINT("WorkerWorkerOCServiceImpl.GetObjectRemoteImpl.changeDataSize", [&rsp](int64_t size) {
            rsp.set_data_size(size);
            return Status::OK();
        });
        rsp.set_create_time(static_cast<int64_t>(entry->GetCreateTime()));
        rsp.set_life_state(static_cast<uint32_t>(entry->GetLifeState()));
        RETURN_STATUS_LOG_ERROR(K_OC_REMOTE_GET_NOT_ENOUGH,
                                FormatString("[ObjectKey %s] data size mismatch, actual = %zu, expected = %zu",
                                             objectKey, entry->GetDataSize(), expectedDataSize));
    }
    PerfPoint point(PerfKey::WORKER_LOAD_OBJECT_DATA);
    ReadObjectKV objKv(ReadKey(objectKey, offset, size), entry);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objKv.CheckReadOffset(), "Read offset verify failed");
    if (entry->IsSpilled() && entry->GetShmUnit() == nullptr) {  // At this step, the local memory may already exist.
        PerfPoint p(PerfKey::WORKER_REMOTE_GET_PAYLOAD_FROM_DISK);
        RETURN_IF_NOT_OK(
            WorkerOcSpill::Instance()->Get(objectKey, outPayload, objKv.GetReadSize(), objKv.GetReadOffset()));
        p.Record();
    } else {
        ShmGuard shmGuard(entry->GetShmUnit(), entry->GetDataSize(), entry->GetMetadataSize());
        if (WorkerOcServiceCrudCommonApi::ShmEnable()) {
            RETURN_IF_NOT_OK(shmGuard.TryRLatch());
        }
        INJECT_POINT("worker.LoadObjectData.AddPayload");

        PerfPoint p(PerfKey::WORKER_REMOTE_GET_PAYLOAD);
        // Support send payload exceed 2GB
        if (IsUrmaEnabled() && req.has_urma_info()) {
            // later add a check on data size and read size.
            auto shmUnit = entry->GetShmUnit();
            const uint64_t localObjectAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
            uint64_t localSegAddress;
            uint64_t localSegSize;
            GetSegmentInfoFromShmUnit(shmUnit, localObjectAddress, localSegAddress, localSegSize);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                UrmaWritePayload(req.urma_info(), localSegAddress, localSegSize, localObjectAddress, offset, size,
                                 entry->GetMetadataSize(), blocking, keys),
                "");
            rsp.set_data_in_payload(true);
        }
        // We need to extend the ShmGuard lifecycle if we perform parallel urma_write.
        if (!IsUrmaEnabled() || !blocking) {
            RETURN_IF_NOT_OK(shmGuard.TransferTo(outPayload, objKv.GetReadOffset(), objKv.GetReadSize()));
        }
    }

    rsp.mutable_error()->set_error_code(StatusCode::K_OK);  // No size change
    rsp.set_data_size(static_cast<int64_t>(entry->GetDataSize()));
    rsp.set_create_time(static_cast<int64_t>(entry->GetCreateTime()));
    rsp.set_life_state(static_cast<uint32_t>(entry->GetLifeState()));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::CheckEtcdState(const CheckEtcdStateReqPb &req, CheckEtcdStateRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    bool isEtcdAvailable = etcdStore_->Writable().IsOk();
    rsp.set_available(isEtcdAvailable);
    LOG_IF(INFO, isEtcdAvailable) << "Etcd is available";
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetClusterState(const GetClusterStateReqPb &req, GetClusterStateRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    bool isEtcdAvailable = etcdStore_->Writable().IsOk();
    rsp.set_etcd_available(isEtcdAvailable);
    RETURN_RUNTIME_ERROR_IF_NULL(etcdCm_);
    *rsp.mutable_hash_ring() = etcdCm_->GetHashRing()->GetHashRingPb();
    LOG_IF(INFO, isEtcdAvailable) << "Etcd is available";
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                              std::vector<::datasystem::RpcMessage> payloads)
{
    return ocClientWorkerSvc_->MigrateData(req, rsp, std::move(payloads));
}

Status WorkerWorkerOCServiceImpl::BatchGetObjectRemote(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<BatchGetObjectRemoteRspPb, BatchGetObjectRemoteReqPb>>
        serverApi)
{
    PerfPoint point(PerfKey::WORKER_SERVER_GET_REMOTE);
    BatchGetObjectRemoteReqPb req;
    BatchGetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "GetObjectRemote read error");
    PerfPoint pointImpl(PerfKey::WORKER_SERVER_GET_REMOTE_IMPL);
    std::map<uint64_t, std::pair<std::vector<uint64_t>, std::vector<RpcMessage>>> keys;
    std::vector<GetObjectRemoteRspPb> getObjRemoteSubRsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    for (int i = 0; i < req.requests_size(); i++) {
        const auto &subReq = req.requests(i);
        auto &subRsp = *(rsp.add_responses());
        std::vector<RpcMessage> subPayload;
        std::vector<uint64_t> subKeys;
        auto status = GetObjectRemoteBatchWrite(subReq, subRsp, subPayload, subKeys);
        if (status.IsOk()) {
            // If keys are empty, we 1) get payload from spill or 2) urma is not enabled.
            // In both cases we send payload as part of the response.
            // Otherwise we extend the lifecycle of payload only until urma_write is done.
            if (subKeys.empty()) {
                payload.insert(payload.end(), std::make_move_iterator(subPayload.begin()),
                               std::make_move_iterator(subPayload.end()));
            } else {
                keys.emplace(i, std::make_pair(std::move(subKeys), std::move(subPayload)));
            }
        } else {
            subRsp.mutable_error()->set_error_code(status.GetCode());
            subRsp.mutable_error()->set_error_msg(status.GetMsg());
        }
    }
    pointImpl.Record();
    // Wait for urma events if the events are created and not already waited.
    for (auto &pair : keys) {
        int index = pair.first;
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [index, &rsp](Status &status) {
            rsp.mutable_responses()->at(index).mutable_error()->set_error_code(status.GetCode());
            rsp.mutable_responses()->at(index).mutable_error()->set_error_msg(status.GetMsg());
            return status;
        };
        (void)WaitUrmaEvent(pair.second.first, remainingTime, errorHandler);
        // Early release of ShmGuard.
        pair.second.second.clear();
    }
    PerfPoint pointWrite(PerfKey::WORKER_SERVER_GET_REMOTE_WRITE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "GetObjectRemote write error");
    pointWrite.Record();
    PerfPoint pointSendPayload(PerfKey::WORKER_SERVER_GET_REMOTE_SENDPAYLOAD);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload(payload, FLAGS_oc_worker_worker_direct_port > 0),
                                     "GetObjectRemote send payload error");
    pointSendPayload.Record();
    LOG(INFO) << FormatString("pull success");
    point.Record();
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
