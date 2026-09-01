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

/** Description: Implements Worker-side metadata-affine QueryAndGet processing. */
#include "datasystem/worker/object_cache/service/worker_query_and_get_impl.h"

#include <chrono>
#include <limits>
#include <optional>
#include <unordered_map>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/shared_memory/delayed_release_shm_manager.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/worker/client_manager/client_manager.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr uint64_t QUERY_AND_GET_MAX_TCP_PAYLOAD_SIZE = 512 * 1024UL;
constexpr double MICROSECONDS_PER_MILLISECOND = 1000.0;

uint64_t GetSteadyTimeUs()
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

void FillLocation(const master::ObjectLocationInfoPb &source, QueryAndGetLocationInfoPb &target)
{
    target.set_object_key(source.object_key());
    target.set_object_size(source.object_size());
    target.set_topology_version(source.topology_version());
    *target.mutable_object_locations() = source.object_locations();
}
}  // namespace

WorkerQueryAndGetImpl::WorkerQueryAndGetImpl(std::shared_ptr<WorkerOcServiceGetImpl> getProc,
                                             std::shared_ptr<SharedMemoryRefTable> memoryRefTable,
                                             std::shared_ptr<AkSkManager> akSkManager, HostPort localAddress,
                                             std::shared_ptr<PeerUbAdmission> ubAdmission)
    : getProc_(std::move(getProc)),
      memoryRefTable_(std::move(memoryRefTable)),
      akSkManager_(std::move(akSkManager)),
      localAddress_(std::move(localAddress)),
      ubAdmission_(std::move(ubAdmission))
{
}

Status WorkerQueryAndGetImpl::QueryAndGet(
    std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> &serverApi)
{
    RequestState state;
    state.startUs = GetSteadyTimeUs();
    state.lastCheckpointUs = state.startUs;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_QUERY_AND_GET);
    Status rc = ReadAndAuthenticate(serverApi, state.request);
    state.stats.preprocessUs = RecordPhase(state);
    if (rc.IsOk()) {
        access.ObjectKeysRef(state.request.object_keys()).TransportType(GetTransportName(state.request));
        rc = ProcessAndDeliver(serverApi, state);
    }
    const uint64_t totalUs = state.lastCheckpointUs - state.startUs;
    access.DataSize(state.stats.dataSize).Result(rc).Record();
    LogCompletion(state, rc, totalUs);
    return rc;
}

Status WorkerQueryAndGetImpl::ReadAndAuthenticate(
    const std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> &serverApi,
    QueryAndGetReqPb &request) const
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(request), "Read QueryAndGet request failed");
    RETURN_RUNTIME_ERROR_IF_NULL(akSkManager_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(request), "AK/SK failed.");
    return Status::OK();
}

Status WorkerQueryAndGetImpl::ProcessAndDeliver(
    const std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> &serverApi,
    RequestState &state)
{
    Raii rollback([this, &state] {
        if (!state.delivered) {
            RollbackShmRefs(state);
        }
    });
    RETURN_IF_NOT_OK(BuildResponse(state));
    CollectStats(state);
    RETURN_IF_NOT_OK(DeliverResponse(serverApi, state));
    state.delivered = true;
    return Status::OK();
}

Status WorkerQueryAndGetImpl::DeliverResponse(
    const std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> &serverApi,
    RequestState &state) const
{
    Status deliveryRc = serverApi->Write(state.response);
    if (deliveryRc.IsOk()) {
        deliveryRc = serverApi->SendPayload(state.payloads);
    }
    if (deliveryRc.IsOk()) {
        serverApi->SetRequestComplete();
    }
    state.stats.deliveryUs = RecordPhase(state);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(deliveryRc, "Deliver QueryAndGet response failed");
    return Status::OK();
}

Status WorkerQueryAndGetImpl::BuildResponse(RequestState &state)
{
    Status rc = PrepareLocalResponse(state);
    state.stats.localReadUs = RecordPhase(state);
    RETURN_IF_NOT_OK(rc);
    rc = FillMissLocations(state);
    state.stats.metadataUs = RecordPhase(state);
    return rc;
}

Status WorkerQueryAndGetImpl::PrepareLocalResponse(RequestState &state)
{
    RETURN_IF_NOT_OK(ValidateRequest(state.request));
    RETURN_RUNTIME_ERROR_IF_NULL(getProc_);
    InitializeResponse(state);
    return EncodeLocalHits(state);
}

uint64_t WorkerQueryAndGetImpl::RecordPhase(RequestState &state)
{
    const uint64_t nowUs = GetSteadyTimeUs();
    const uint64_t elapsedUs = nowUs - state.lastCheckpointUs;
    state.lastCheckpointUs = nowUs;
    return elapsedUs;
}

void WorkerQueryAndGetImpl::InitializeResponse(RequestState &state) const
{
    state.response.Clear();
    state.payloads.clear();
    state.misses.clear();
    state.addedShmRefs.clear();
    state.tcpPayloadSize = 0;
    state.misses.reserve(state.request.object_keys_size());
    if (state.request.data_request().has_shm()) {
        state.addedShmRefs.reserve(state.request.object_keys_size());
    }
    state.response.mutable_results()->Reserve(state.request.object_keys_size());
    for (int i = 0; i < state.request.object_keys_size(); ++i) {
        state.response.add_results();
    }
}

void WorkerQueryAndGetImpl::RollbackShmRefs(const RequestState &state) const
{
    if (!state.request.data_request().has_shm() || state.addedShmRefs.empty()) {
        return;
    }
    const auto clientId = ClientKey::Intern(state.request.data_request().shm().client_id());
    for (const auto &shmId : state.addedShmRefs) {
        LOG_IF_ERROR(memoryRefTable_->RemoveShmUnit(clientId, shmId),
                     "Rollback undelivered QueryAndGet SHM reference");
    }
}

void WorkerQueryAndGetImpl::CollectStats(RequestState &state) const
{
    for (const auto &result : state.response.results()) {
        if (result.has_data_result()) {
            ++state.stats.inlineHits;
            state.stats.dataSize += result.location().object_size();
        } else {
            ++state.stats.misses;
        }
    }
}

void WorkerQueryAndGetImpl::LogCompletion(const RequestState &state, const Status &rc, uint64_t totalUs) const
{
    const auto &stats = state.stats;
    const auto config = GetServerLatencyTraceConfig();
    SLOW_LOG_IF_OR_VLOG(
        INFO, config.processSlowerThanUs > 0 && totalUs >= config.processSlowerThanUs, 1,
        FormatString("QueryAndGet done, keyCount: %d, inlineHits: %zu, misses: %zu, transport: %s, "
                     "preprocess: %.3fms, localRead: %.3fms, metadata: %.3fms, delivery: %.3fms, "
                     "total: %.3fms, status: %s",
                     state.request.object_keys_size(), stats.inlineHits, stats.misses,
                     GetTransportName(state.request),
                     static_cast<double>(stats.preprocessUs) / MICROSECONDS_PER_MILLISECOND,
                     static_cast<double>(stats.localReadUs) / MICROSECONDS_PER_MILLISECOND,
                     static_cast<double>(stats.metadataUs) / MICROSECONDS_PER_MILLISECOND,
                     static_cast<double>(stats.deliveryUs) / MICROSECONDS_PER_MILLISECOND,
                     static_cast<double>(totalUs) / MICROSECONDS_PER_MILLISECOND, rc.ToString()));
}

const char *WorkerQueryAndGetImpl::GetTransportName(const QueryAndGetReqPb &request) const
{
    if (!request.has_data_request()) {
        return "NONE";
    }
    if (request.data_request().has_shm()) {
        return "SHM";
    }
    if (request.data_request().has_ub()) {
        return "UB";
    }
    if (request.data_request().has_tcp()) {
        return "TCP";
    }
    return "UNKNOWN";
}

Status WorkerQueryAndGetImpl::ValidateRequest(const QueryAndGetReqPb &request) const
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsBatchSizeUnderLimit(request.object_keys_size()), K_INVALID,
                             "QueryAndGet batch size is invalid");
    CHECK_FAIL_RETURN_STATUS(request.object_keys_size() > 0, K_INVALID, "QueryAndGet object keys are empty");
    for (const auto &objectKey : request.object_keys()) {
        CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "QueryAndGet object key is empty");
    }
    if (!request.has_data_request()) {
        return Status::OK();
    }
    const auto &dataRequest = request.data_request();
    CHECK_FAIL_RETURN_STATUS(dataRequest.has_tcp() || dataRequest.has_ub() || dataRequest.has_shm(), K_INVALID,
                             "QueryAndGet data transport is not set");
    if (dataRequest.has_shm()) {
        RETURN_RUNTIME_ERROR_IF_NULL(memoryRefTable_);
        CHECK_FAIL_RETURN_STATUS(!dataRequest.shm().client_id().empty(), K_INVALID,
                                 "QueryAndGet SHM client ID is empty");
        const auto clientId = ClientKey::Intern(dataRequest.shm().client_id());
        CHECK_FAIL_RETURN_STATUS(worker::ClientManager::Instance().ClientShmEnabled(clientId), K_NOT_SUPPORTED,
                                 "QueryAndGet SHM session is unavailable");
    }
    if (!dataRequest.has_ub()) {
        return Status::OK();
    }
    const auto &ub = dataRequest.ub();
    CHECK_FAIL_RETURN_STATUS(ub.buffer_size() > 0 && !ub.urma_instance_id().empty(), K_INVALID,
                             "QueryAndGet UB buffer is invalid");
    CHECK_FAIL_RETURN_STATUS(ub.buffer_infos_size() == request.object_keys_size(), K_INVALID,
                             "QueryAndGet UB buffer count does not match object key count");
    const auto &remote = ub.buffer_infos(0);
    HostPort remoteAddress(remote.request_address().host(), remote.request_address().port());
    const std::string connectionId = remote.client_id().empty() ? remoteAddress.ToString() : remote.client_id();
    RETURN_IF_NOT_OK(CheckTransportConnectionStable(connectionId, ub.urma_instance_id()));
    return Status::OK();
}

Status WorkerQueryAndGetImpl::EncodeLocalHits(RequestState &state)
{
    const auto &request = state.request;
    uint64_t shmBytes = 0;
    for (int i = 0; i < request.object_keys_size(); ++i) {
        const auto &objectKey = request.object_keys(i);
        if (!request.has_data_request()) {
            state.misses.emplace_back(objectKey);
            continue;
        }
        std::unique_ptr<GetObjEntryParams> params;
        RETURN_IF_NOT_OK(getProc_->TryAcquireLocalObject(objectKey, params));
        if (params == nullptr) {
            state.misses.emplace_back(objectKey);
            continue;
        }
        const size_t payloadCount = state.payloads.size();
        const uint64_t tcpPayloadSize = state.tcpPayloadSize;
        bool encoded = false;
        Status rc = EncodeLocalHit(state, static_cast<size_t>(i), *params, encoded, shmBytes);
        if (rc.IsError() || !encoded) {
            state.payloads.resize(payloadCount);
            state.tcpPayloadSize = tcpPayloadSize;
            auto *result = state.response.mutable_results(i);
            result->clear_location();
            result->clear_data_result();
            state.misses.emplace_back(objectKey);
            VLOG_IF(1, rc.IsError()) << "[ObjectKey " << objectKey
                                     << "] QueryAndGet inline data fallback: " << rc.ToString();
        }
    }
    if (shmBytes > 0) {
        METRIC_ADD(metrics::KvMetricId::WORKER_TO_CLIENT_GET_SHM_TOTAL_BYTES, shmBytes);
    }
    return Status::OK();
}

Status WorkerQueryAndGetImpl::EncodeLocalHit(RequestState &state, size_t index,
                                             const GetObjEntryParams &params, bool &encoded, uint64_t &shmBytes)
{
    INJECT_POINT("worker.QueryAndGet.EncodeLocalHitFailure");
    const auto &request = state.request;
    auto &result = *state.response.mutable_results(static_cast<int>(index));
    if (request.data_request().has_tcp()) {
        RETURN_IF_NOT_OK(EncodeTcp(params, *result.mutable_data_result(), state, encoded));
    } else if (request.data_request().has_ub()) {
        RETURN_IF_NOT_OK(EncodeUb(request.data_request().ub(), index, params, result, encoded));
        if (encoded) {
            result.mutable_data_result();
        }
    } else {
        EncodeShm(request.data_request().shm(), params, *result.mutable_data_result(), state, shmBytes);
        encoded = true;
    }
    if (encoded) {
        auto *location = result.mutable_location();
        location->set_object_key(request.object_keys(static_cast<int>(index)));
        location->add_object_locations(localAddress_.ToString());
        location->set_object_size(params.dataSize);
    }
    return Status::OK();
}

Status WorkerQueryAndGetImpl::EncodeTcp(const GetObjEntryParams &params, QueryAndGetDataResultPb &result,
                                        RequestState &state, bool &encoded) const
{
    encoded = false;
    if (state.tcpPayloadSize > QUERY_AND_GET_MAX_TCP_PAYLOAD_SIZE
        || params.dataSize > QUERY_AND_GET_MAX_TCP_PAYLOAD_SIZE - state.tcpPayloadSize) {
        return Status::OK();
    }
    ShmGuard shmGuard(params.shmUnit, params.dataSize, params.metaSize);
    if (WorkerOcServiceCrudCommonApi::ShmEnable()) {
        RETURN_IF_NOT_OK(shmGuard.TryRLatch());
    }
    const size_t firstIndex = state.payloads.size();
    RETURN_IF_NOT_OK(shmGuard.TransferTo(state.payloads, 0, params.dataSize));
    for (size_t i = firstIndex; i < state.payloads.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(i <= std::numeric_limits<uint32_t>::max(), K_RUNTIME_ERROR,
                                 "QueryAndGet TCP payload index exceeds protocol limits");
        result.add_payload_indexes(static_cast<uint32_t>(i));
    }
    state.tcpPayloadSize += params.dataSize;
    METRIC_ADD(metrics::KvMetricId::WORKER_TO_CLIENT_GET_TCP_TOTAL_BYTES, params.dataSize);
    encoded = true;
    INJECT_POINT_NO_RETURN("worker.QueryAndGet.EncodeTcp");
    return Status::OK();
}

Status WorkerQueryAndGetImpl::EncodeUb(const QueryAndGetUbDataReqPb &request, size_t index,
                                       const GetObjEntryParams &params, QueryAndGetResultPb &result,
                                       bool &encoded) const
{
    encoded = false;
    if (params.dataSize > request.buffer_size()) {
        return Status::OK();
    }
    const auto &remote = request.buffer_infos(static_cast<int>(index));
    ShmGuard shmGuard(params.shmUnit, params.dataSize, params.metaSize);
    if (WorkerOcServiceCrudCommonApi::ShmEnable()) {
        RETURN_IF_NOT_OK(shmGuard.TryRLatch());
    }
    const uint64_t base = reinterpret_cast<uint64_t>(params.shmUnit->GetPointer());
    uint64_t segmentAddress = 0;
    uint64_t segmentSize = 0;
    GetSegmentInfoFromShmUnit(params.shmUnit, base, segmentAddress, segmentSize);
    const uint8_t srcChipId = NumaIdToChipId(params.shmUnit->GetNumaId());
    const uint8_t dstChipId = remote.has_chip_id() ? static_cast<uint8_t>(remote.chip_id()) : INVALID_CHIP_ID;
    std::vector<uint64_t> eventKeys;
    UrmaWriteFailure failure;
    auto lateCompletionContext =
        ubAdmission_ == nullptr
            ? std::nullopt
            : ubAdmission_->BuildLateCompletionContext(UbOperationKind::CLIENT_GET_WRITEBACK);
    Status rc = UrmaWritePayload(remote, segmentAddress, segmentSize, base, 0, params.dataSize, params.metaSize,
                                 srcChipId, dstChipId, true, eventKeys, nullptr, &failure,
                                 std::move(lateCompletionContext));
    if (rc.IsError()) {
        const auto &address = remote.request_address();
        const HostPort failedEndpoint(address.host(), address.port());
        ReportLocalUbOperationFailure(ubAdmission_.get(), localAddress_, failedEndpoint,
                                      UbOperationKind::CLIENT_GET_WRITEBACK, rc, failure.providerStatus,
                                      failure.cqeStatus);
        result.mutable_status()->set_error_code(rc.GetCode());
        result.mutable_status()->set_error_msg(rc.GetMsg());
        if (NeedDelayReleaseShmUnit(rc)) {
            LOG_EVERY_T(WARNING, DELAY_RELEASE_LOG_INTERVAL_SEC)
                << "[QUERY_AND_GET_DELAY_RELEASE_ADD] id=" << params.shmUnit->id
                << ", identity=" << params.shmUnit->GetIdentity() << ", bytes=" << params.shmUnit->size
                << ", delayMs=" << DEFAULT_SHM_DELAY_RELEASE_MS << ", reason=" << rc;
            DelayedReleaseShmManager::Instance().Add(
                params.shmUnit, std::chrono::milliseconds(DEFAULT_SHM_DELAY_RELEASE_MS));
        }
        return rc;
    }
    METRIC_ADD(metrics::KvMetricId::WORKER_TO_CLIENT_GET_URMA_TOTAL_BYTES, params.dataSize);
    encoded = true;
    INJECT_POINT_NO_RETURN("worker.QueryAndGet.EncodeUb");
    return Status::OK();
}

void WorkerQueryAndGetImpl::EncodeShm(const QueryAndGetShmDataReqPb &request, const GetObjEntryParams &params,
                                      QueryAndGetDataResultPb &result, RequestState &state, uint64_t &shmBytes) const
{
    const auto clientId = ClientKey::Intern(request.client_id());
    auto shmUnit = params.shmUnit;
    memoryRefTable_->AddShmUnit(clientId, shmUnit,
                                GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime());
    state.addedShmRefs.emplace_back(params.shmUnit->GetId());
    auto *info = result.mutable_shm_info();
    info->set_store_fd(params.shmUnit->GetFd());
    info->set_offset(static_cast<int64_t>(params.shmUnit->GetOffset()));
    info->set_data_size(static_cast<int64_t>(params.dataSize));
    info->set_metadata_size(static_cast<int64_t>(params.metaSize));
    info->set_mmap_size(static_cast<int64_t>(params.shmUnit->GetMmapSize()));
    info->set_shm_id(params.shmUnit->id.ToString());
    info->set_is_seal(params.isSealed);
    info->set_write_mode(static_cast<uint32_t>(params.objectMode.GetWriteMode()));
    info->set_consistency_type(static_cast<uint32_t>(params.objectMode.GetConsistencyType()));
    shmBytes += params.dataSize;
    INJECT_POINT_NO_RETURN("worker.QueryAndGet.EncodeShm");
}

Status WorkerQueryAndGetImpl::FillMissLocations(RequestState &state) const
{
    if (!state.misses.empty()) {
        INJECT_POINT_NO_RETURN("worker.QueryAndGet.QueryMissMetadata");
    }
    std::unordered_map<std::string, master::ObjectLocationInfoPb> locations;
    RETURN_IF_NOT_OK(getProc_->QueryObjectLocations(state.misses, locations));
    for (int i = 0; i < state.request.object_keys_size(); ++i) {
        auto *result = state.response.mutable_results(i);
        if (result->has_data_result()) {
            continue;
        }
        const auto &objectKey = state.request.object_keys(i);
        result->mutable_location()->set_object_key(objectKey);
        auto location = locations.find(objectKey);
        if (location != locations.end()) {
            FillLocation(location->second, *result->mutable_location());
        }
    }
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
