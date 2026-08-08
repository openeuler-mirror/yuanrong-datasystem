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

/** Description: Implements the client transport facade. */

#include "datasystem/client/transport/transport_layer.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>

#include "datasystem/client/transport/common/deadline_retry.h"
#include "datasystem/client/transport/data_plane/data_plane_executor.h"
#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/client/transport/metadata/object_metadata_client.h"
#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/object_read/replica_reader.h"
#include "datasystem/client/transport/rpc/exist_request_builder.h"
#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/transport_advisor.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/ub_failure_classifier.h"
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uri.h"

#include "butil/time.h"

namespace datasystem {
namespace client {
namespace {
constexpr uint32_t MAX_LOCAL_UB_PROBE_BACKOFF_LEVEL = 6;
// SHM-off fallback is a steady-state path (every write on a SHM-disabled same-host worker); throttle the
// diagnostic so it does not flood the log. Matches the read-path/DataPlaneExecutor convention.
constexpr int TRANSPORT_DIAG_LOG_RATE = 100;

uint64_t GetConfiguredUbInlineBufferSize()
{
    const char *value = std::getenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES");
    if (value == nullptr || *value == '\0') {
        return 0;
    }
    for (const char *cursor = value; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            LOG(WARNING) << "Ignore invalid DATASYSTEM_UB_GET_DATA_SIZE_BYTES: expected an unsigned integer";
            return 0;
        }
    }
    uint64_t size = 0;
    if (!Uri::StrToUint64(value, size)) {
        LOG(WARNING) << "Ignore invalid DATASYSTEM_UB_GET_DATA_SIZE_BYTES: value is out of range";
        return 0;
    }
    return size;
}

const char *TransportHintName(TransportHint hint)
{
    switch (hint) {
        case TransportHint::SHM_CANDIDATE:
            return "SHM";
        case TransportHint::UB_CANDIDATE:
            return "UB";
        case TransportHint::TCP_ONLY:
            return "TCP";
        default:
            return "UNKNOWN";
    }
}

// Write-path fallback order when a same-host SHM candidate cannot establish its fd-passing endpoint
// (K_NOT_SUPPORTED): try UB (if URMA is enabled) for an RDMA zero-copy write, then plain TCP. Read path
// stays SHM->TCP (handled in DataPlaneExecutor); writes prefer UB in between.
std::vector<TransportHint> CreateFallbackHints(TransportHint initial)
{
    std::vector<TransportHint> hints;
    if (initial != TransportHint::SHM_CANDIDATE) {
        return hints;
    }
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        hints.push_back(TransportHint::UB_CANDIDATE);
    }
#endif
    hints.push_back(TransportHint::TCP_ONLY);
    return hints;
}
}  // namespace

TransportLayer::TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                               uint64_t fastTransportMemSize, TransportLayerOptions options)
    : advisor_(std::make_shared<TransportAdvisor>()), releasePool_(std::move(options.releasePool))
{
    auto ubBufferProvider = CreateDefaultUbReceiveBufferProvider();
    manager_ = std::make_shared<DataPlaneManager>(std::move(signature), fastTransportMemSize,
                                                  std::move(options.channelConfig), ubBufferProvider,
                                                  options.enableClientDirectPipelineH2D, options.pipelineThreadNum,
                                                  releasePool_);
    auto retry = std::make_shared<DeadlineRetry>();
    auto metadata = std::make_shared<ObjectMetadataClient>(manager_, retry, advisor_, std::move(ubBufferProvider),
                                                           GetConfiguredUbInlineBufferSize());
    auto executor = std::make_shared<DataPlaneExecutor>(manager_, advisor_);
    auto healthFilter = options.readSourceFilter == nullptr ? std::make_shared<UbHealthFilter>()
                                                            : std::move(options.readSourceFilter);
    auto checkReadSource = [healthFilter](const HostPort &workerAddr) {
        return healthFilter->IsAvailable(workerAddr)
                   ? Status::OK()
                   : Status(K_URMA_READ_SOURCE_DENIED,
                            "Client UB read source denied: " + workerAddr.ToString());
    };
    auto reportReadOutcome = [healthFilter](const HostPort &workerAddr, const GetObjectRemoteRspPb &response) {
        if (response.has_provider_ub_failure_detail()) {
            (void)healthFilter->ReportProviderFailure(workerAddr, response.provider_ub_failure_detail());
        }
    };
    auto replicas = std::make_shared<ReplicaReader>(std::move(executor), std::move(retry), taskPool,
                                                    std::move(checkReadSource), std::move(reportReadOutcome));
    objectRead_ = std::make_unique<ObjectReadFlow>(std::move(metadata), std::move(replicas), std::move(taskPool));
}

TransportLayer::TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                               std::shared_ptr<TransportAdvisor> advisor)
    : TransportLayer(std::move(dataPlaneManager), std::move(advisor), std::chrono::seconds(1))
{
}

TransportLayer::TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                               std::shared_ptr<TransportAdvisor> advisor,
                               std::chrono::milliseconds localUbProbeBaseDelay)
    : manager_(std::move(dataPlaneManager)), advisor_(std::move(advisor))
{
    localUbProbeBaseDelay_ = std::max(localUbProbeBaseDelay, std::chrono::milliseconds(1));
}

Status TransportLayer::CheckLocalUbSenderAdmission(TransportHint hint) const
{
    CHECK_FAIL_RETURN_STATUS(!shutdownRequested_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "TransportLayer is shutting down");
    if (hint != TransportHint::UB_CANDIDATE || !localUbSenderUnavailable_.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    return Status(K_URMA_WORKER_UNAVAILABLE, "Client-local UB sender is unavailable");
}

Status TransportLayer::CheckLocalUbSenderAdmission() const
{
    return CheckLocalUbSenderAdmission(TransportHint::UB_CANDIDATE);
}

Status TransportLayer::RunClientLocalUbWrite(const HostPort &workerAddr, ObjectBufferInfo &bufferInfo,
                                             const std::function<Status()> &write)
{
    std::shared_lock<std::shared_mutex> admission;
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(TransportHint::UB_CANDIDATE, admission));
    bufferInfo.ubFailureReportRc = Status::OK();
    bufferInfo.ubProviderStatus.reset();
    bufferInfo.ubCqeStatus.reset();
    Status rc = write();
    const Status &failureRc = bufferInfo.ubFailureReportRc.IsError() ? bufferInfo.ubFailureReportRc : rc;
    (void)ReportLocalUbSenderFailure({ workerAddr, AccessTransportKind::UB, failureRc,
                                      bufferInfo.ubProviderStatus, bufferInfo.ubCqeStatus },
                                     admission);
    return rc;
}

Status TransportLayer::AcquireLocalUbSenderAdmission(TransportHint hint,
                                                     std::shared_lock<std::shared_mutex> &admission) const
{
    RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(hint));
    if (hint != TransportHint::UB_CANDIDATE) {
        return Status::OK();
    }
    admission = std::shared_lock<std::shared_mutex>(localUbSenderMutex_);
    CHECK_FAIL_RETURN_STATUS(!shutdownRequested_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "TransportLayer is shutting down");
    if (!localUbSenderUnavailable_.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    return Status(K_URMA_WORKER_UNAVAILABLE, "Client-local UB sender is unavailable");
}

bool TransportLayer::ReportLocalUbSenderFailure(const LocalUbSenderFailureView &failure,
                                                std::shared_lock<std::shared_mutex> &admission)
{
    auto releaseAdmission = [&admission]() {
        if (admission.owns_lock()) {
            admission.unlock();
        }
    };
    if (failure.kind != AccessTransportKind::UB || failure.status.IsOk()) {
        releaseAdmission();
        return false;
    }
    UbOpOutcome outcome(failure.workerAddr, UbOperationKind::CLIENT_PUT, failure.status);
    outcome.providerStatus = failure.providerStatus;
    outcome.cqeStatus = failure.cqeStatus;
    outcome.learnedFrom = "client_local_ub_write";
    if (UbFailureClassifier().Classify(outcome) != UbFailureClass::PORT_UNAVAILABLE_ERROR4) {
        releaseAdmission();
        return false;
    }
    // Quarantine the client-local UB sender: every subsequent UB Create/Set/MSet is fast-failed by
    // CheckLocalUbSenderAdmission until a recovery probe succeeds (TryRecoverLocalUbSender). Emit a
    // one-shot ERROR so operators see the circuit break without counting the per-request
    // "Create still failed" WARNING storm below (which is intentionally unthrottled to preserve
    // the failure rate signal during the outage).
    const bool wasUnavailable = localUbSenderUnavailable_.exchange(true, std::memory_order_acq_rel);
    releaseAdmission();
    {
        std::lock_guard<std::shared_mutex> lock(localUbSenderMutex_);
        localUbSenderUnavailable_.store(true, std::memory_order_release);
        localUbSenderFailure_ = failure.status;
        localUbProbeWorker_ = failure.workerAddr;
        ++localUbSenderGeneration_;
        localUbProbeBackoffLevel_ = 1;
        localUbProbeDeadline_ = std::chrono::steady_clock::now() + localUbProbeBaseDelay_;
    }
    if (!wasUnavailable) {
        // Only the first false->true transition trips the outage summary; concurrent failures that
        // also reach here while unavailable already fast-fail via the admission check above.
        LOG(ERROR) << "[LOCAL_UB_CIRCUIT_BREAK] Client-local UB sender quarantined, worker="
                   << failure.workerAddr.ToString() << ", status=" << failure.status.ToString()
                   << ", providerStatus=" << failure.providerStatus.value_or(0)
                   << ", cqeStatus=" << failure.cqeStatus.value_or(0)
                   << "; UB Create/Set/MSet will fast-fail K_URMA_WORKER_UNAVAILABLE until a recovery probe succeeds";
    }
    reconcileCv_.notify_one();
    return true;
}

std::optional<std::chrono::steady_clock::time_point> TransportLayer::GetLocalUbProbeDeadline() const
{
    std::shared_lock<std::shared_mutex> lock(localUbSenderMutex_);
    return localUbSenderFailure_.IsError() && localUbProbeWorker_.has_value()
               ? std::optional<std::chrono::steady_clock::time_point>{ localUbProbeDeadline_ }
               : std::nullopt;
}

void TransportLayer::TryRecoverLocalUbSender()
{
    std::optional<HostPort> workerAddr;
    uint64_t generation = 0;
    {
        std::shared_lock<std::shared_mutex> lock(localUbSenderMutex_);
        if (localUbSenderFailure_.IsOk() || !localUbProbeWorker_.has_value()
            || std::chrono::steady_clock::now() < localUbProbeDeadline_) {
            return;
        }
        workerAddr = localUbProbeWorker_;
        generation = localUbSenderGeneration_;
    }

    bool committed = false;
    Status probeRc = manager_->ProbeUbConnection(*workerAddr, [this, &workerAddr, generation, &committed] {
        std::lock_guard<std::shared_mutex> lock(localUbSenderMutex_);
        if (!shutdownRequested_.load(std::memory_order_acquire) && localUbSenderFailure_.IsError()
            && localUbProbeWorker_ == workerAddr && localUbSenderGeneration_ == generation) {
            localUbSenderFailure_ = Status::OK();
            localUbProbeWorker_.reset();
            localUbProbeBackoffLevel_ = 0;
            localUbSenderUnavailable_.store(false, std::memory_order_release);
            committed = true;
        }
    });
    if (committed) {
        LOG(INFO) << "Client-local UB sender recovered via probe to " << workerAddr->ToString();
        return;
    }
    if (shutdownRequested_.load(std::memory_order_acquire)) {
        return;
    }
    std::lock_guard<std::shared_mutex> lock(localUbSenderMutex_);
    if (localUbSenderFailure_.IsOk() || localUbProbeWorker_ != workerAddr || localUbSenderGeneration_ != generation) {
        return;
    }
    if (probeRc.IsOk()) {
        return;
    }
    localUbProbeBackoffLevel_ = std::min<uint32_t>(localUbProbeBackoffLevel_ + 1, MAX_LOCAL_UB_PROBE_BACKOFF_LEVEL);
    auto delay = localUbProbeBaseDelay_ * (1u << (localUbProbeBackoffLevel_ - 1));
    localUbProbeDeadline_ = std::chrono::steady_clock::now() + delay;
    LOG(WARNING) << "Client-local UB sender recovery probe failed for " << workerAddr->ToString() << ": " << probeRc;
}

TransportLayer::~TransportLayer()
{
    Shutdown();
}

Status TransportLayer::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_IF_NOT_OK(manager_->Init());
    std::lock_guard<bthread::Mutex> lock(reconcileMutex_);
    if (reconcileStarted_) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!reconcileStopping_, K_SHUTTING_DOWN, "TransportLayer is shutting down");
    try {
        reconcileThread_ = Thread(&TransportLayer::ReconcileLoop, this);
        reconcileStarted_ = true;
        reconcileThread_.set_name("transport-recon");
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("Start transport reconcile thread failed: ") + error.what());
    }
    return Status::OK();
}

Status TransportLayer::ResolveMetadata(const ObjectReadRequest &input,
                                       std::vector<ObjectMetadataItem> &metadata)
{
    RETURN_RUNTIME_ERROR_IF_NULL(objectRead_);
    return objectRead_->ResolveMetadata(input, metadata);
}

Status TransportLayer::AcquireDirectUbEndpointLease(
    const HostPort &workerAddr, std::unique_ptr<DataPlaneManager::DataPlaneLease> &lease)
{
    RETURN_IF_NOT_OK(manager_->AcquireDataPlaneLease(workerAddr, TransportHint::UB_CANDIDATE, lease));
    RETURN_RUNTIME_ERROR_IF_NULL(lease);
    RETURN_RUNTIME_ERROR_IF_NULL(lease->GetTransporter());
    CHECK_FAIL_RETURN_STATUS(lease->GetTransporter()->Kind() == AccessTransportKind::UB, K_NOT_SUPPORTED,
                             "Client direct pipeline H2D requires UB transport");
    return Status::OK();
}

Status TransportLayer::Get(const ObjectReadRequest &input, ObjectReadResult &output)
{
    RETURN_RUNTIME_ERROR_IF_NULL(objectRead_);
    VLOG(1) << "[TransportGet][TransportLayer] Start Get, key count: " << input.items.size()
            << ", remaining deadline us: " << ApiDeadline::Instance().ApiRemainingUs();
    Status status = objectRead_->Run(input, output);
    if (status.IsError()) {
        LOG(ERROR) << "[TransportGet][TransportLayer] Get failed, key count: " << input.items.size()
                   << ", status: " << status.ToString();
    } else {
        VLOG(1) << "[TransportGet][TransportLayer] Finish Get, key count: " << input.items.size()
                << ", transport: " << AccessTransportTracker::KindToName(output.actualKind);
    }
    return status;
}

Status TransportLayer::Exist(const HostPort &workerAddr, const TransportExistRequest &input,
                             TransportExistResult &output)
{
    ExistReqPb request;
    RETURN_IF_NOT_OK(BuildExistRequest(input, request));

    auto runExist = [&](ExistRspPb &rsp) -> Status {
        std::shared_ptr<WorkerRpcClient> rpcClient;
        RETURN_IF_NOT_OK(manager_->GetOrCreateRpcClient(workerAddr, rpcClient));
        return rpcClient->InvokeExist(input.subTimeoutMs, request, rsp);
    };

    ExistRspPb response;
    Status rc = runExist(response);
    if (IsRetryableRpcError(rc)) {
        LOG(WARNING) << "Rebuild RPC client for worker " << workerAddr.ToString() << " after Exist failed: " << rc;
        manager_->Teardown(workerAddr);
        rc = runExist(response);
        if (rc.IsError()) {
            LOG(WARNING) << "Exist still failed after rebuilding RPC client for worker " << workerAddr.ToString()
                         << ": " << rc;
            return rc;
        }
    } else if (IsNonRetryableRpcError(rc)) {
        LOG(WARNING) << "Tear down dead RPC peer for worker " << workerAddr.ToString()
                     << " after Exist failed without retry: " << rc;
        manager_->Teardown(workerAddr);
        return rc;
    } else if (rc.IsError()) {
        return rc;
    }

    if (!response.redirect_extra().empty()) {
        return Status(K_NOT_OWNER, "Exist keys redirected to new owners").WithExtra(response.redirect_extra());
    }

    if (static_cast<size_t>(response.exists_size()) != input.objectKeys.size()) {
        return Status(K_RUNTIME_ERROR, FormatString("Exist response size mismatch: expected %zu keys, got %d results",
                                                    input.objectKeys.size(), response.exists_size()));
    }
    output.exists.assign(response.exists().begin(), response.exists().end());
    return Status::OK();
}

Status TransportLayer::GetHashRing(const HostPort &workerAddr, uint64_t currentVersion, GetHashRingRspPb &response)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(manager_->GetOrCreateRpcClient(workerAddr, rpcClient));
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient);
    return rpcClient->InvokeGetHashRing(currentVersion, response);
}

Status TransportLayer::Create(const HostPort &workerAddr, const std::string &objectKey, uint64_t dataSize,
                              const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer)
{
    RETURN_IF_NOT_OK(ValidateCreateRequest(objectKey, dataSize, param));
    INJECT_POINT("TransportLayer.Create.beforeTransport");
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    TransportHint hint = advisor_->GetTransportHint(workerAddr);
    auto runCreate = [&](TransportHint h) -> Status {
        std::shared_lock<std::shared_mutex> admission;
        RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(h));
        std::shared_ptr<IDataTransporter> transporter;
        RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, h, transporter));
        RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(h, admission));
        Status r = transporter->Create(workerAddr, objectKey, dataSize, param, buffer);
        if (admission.owns_lock()) {
            admission.unlock();
        }
        return r;
    };
    Status rc = runCreate(hint);
    if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after Create failed: " << rc;
        manager_->Teardown(workerAddr);
        rc = runCreate(hint);
    }
    if (rc.GetCode() == K_NOT_SUPPORTED) {
        // SHM fd-passing endpoint unavailable on this worker; escalate UB then TCP for the write.
        for (const auto &fallbackHint : CreateFallbackHints(hint)) {
            LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
                << "Create SHM unavailable on worker " << workerAddr.ToString() << ", fall back to "
                << TransportHintName(fallbackHint);
            rc = runCreate(fallbackHint);
            if (rc.IsOk()) {
                return rc;
            }
        }
    }
    if (rc.IsError()) {
        LOG(WARNING) << "Create still failed for worker " << workerAddr.ToString() << ": " << rc;
    }
    return rc;
}

bool TransportLayer::RebuildPlaneOnSetFailure(const Status &rc, const HostPort &workerAddr)
{
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString() << " after Set failed: " << rc;
        manager_->ResetDataPlane(workerAddr);
        return true;
    }
    if (IsNonRetryableRpcError(rc)) {
        // Dead peer: tear down the stale connection but do not retry (the peer is gone).
        // The caller still releases the allocation and surfaces the original status.
        LOG(WARNING) << "Tear down dead RPC peer for worker " << workerAddr.ToString()
                     << " after Set failed without retry: " << rc;
        manager_->Teardown(workerAddr);
        return false;
    }
    if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after Set failed: " << rc;
        manager_->Teardown(workerAddr);
        return true;
    }
    return false;
}

namespace {
// Sampling interval for the routed Set triage log below: one line per N publish attempts so the Set
// hot path is not flooded; aggregate transport-kind/byte counters live in the metrics.
constexpr int ROUTED_SET_TRIAGE_LOG_RATE = 1000;
}  // namespace

void TransportLayer::LogSetResult(const HostPort &workerAddr, TransportHint hint, const Status &rc,
                                  std::chrono::steady_clock::time_point start)
{
    LOG_EVERY_N(INFO, ROUTED_SET_TRIAGE_LOG_RATE) << "[TransportSet] worker=" << workerAddr.ToString()
                            << " transport=" << TransportHintName(hint) << " rc=" << rc.GetCode()
                            << " latency_us=" << std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::steady_clock::now() - start).count();
}

Status TransportLayer::Set(ObjectBuffer &buffer, const TransportSetParam &param)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const HostPort workerAddr = ObjectBufferInternal::GetInfo(buffer).workerAddr;
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    const auto setStart = std::chrono::steady_clock::now();
    std::shared_lock<std::shared_mutex> admission;
    RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(hint));
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, admission));
    auto &mutableBufferInfo = ObjectBufferInternal::GetMutableInfo(buffer);
    mutableBufferInfo.ubFailureReportRc = Status::OK();
    mutableBufferInfo.ubProviderStatus.reset();
    mutableBufferInfo.ubCqeStatus.reset();
    Status rc = transporter->Set(buffer, param);
    return FinalizeSetPublish(workerAddr, buffer, param, hint, transporter, rc, admission, setStart);
}

Status TransportLayer::FinalizeSetPublish(const HostPort &workerAddr, ObjectBuffer &buffer,
                                          const TransportSetParam &param, TransportHint hint,
                                          std::shared_ptr<IDataTransporter> &transporter, const Status &publishRc,
                                          std::shared_lock<std::shared_mutex> &admission,
                                          std::chrono::steady_clock::time_point setStart)
{
    const auto &ubFailureReport = ObjectBufferInternal::GetInfo(buffer).ubFailureReportRc;
    const auto &bufferInfo = ObjectBufferInternal::GetInfo(buffer);
    // Routed SHM zero-copy buffers carry a send-side owner (ManagesWorkerReference) that releases the
    // worker reference on buffer destruction, so skip ScheduleRelease for them to avoid a double decrement.
    const bool ownerManagesRef = bufferInfo.receiveBufferOwner != nullptr
                                 && bufferInfo.receiveBufferOwner->ManagesWorkerReference();
    bool senderQuarantined = ReportLocalUbSenderFailure(
        { workerAddr, transporter->Kind(), ubFailureReport, bufferInfo.ubProviderStatus, bufferInfo.ubCqeStatus },
        admission);
    std::optional<Status> firstPublishRc;
    if (publishRc.GetCode() == K_RPC_UNAVAILABLE && !IsBrpcRequestDefinitelyNotSent(publishRc)) {
        firstPublishRc = publishRc;
    }
    // Skip ScheduleRelease when the buffer's owner manages the worker reference (routed SHM zero-copy);
    // otherwise schedule an async DecreaseReference (optionally forcing a fallback hint).
    auto releaseRef = [&](std::optional<TransportHint> releaseHint = std::nullopt) {
        if (!ownerManagesRef) {
            ScheduleRelease(workerAddr, ObjectBufferInternal::GetInfo(buffer).shmId, param.requestContext,
                            releaseHint);
        }
    };
    Status rc = publishRc;
    if (senderQuarantined) {
        releaseRef(TransportHint::TCP_ONLY);
        return rc.GetCode() == K_URMA_NEED_CONNECT ? ubFailureReport : rc;
    }
    if (!RebuildPlaneOnSetFailure(rc, workerAddr)) {
        releaseRef();
        return rc;
    }
    rc = RetrySet(workerAddr, buffer, param, hint);
    if (rc.IsError() && firstPublishRc.has_value()) {
        rc = *firstPublishRc;
    }
    releaseRef();
    LogSetResult(workerAddr, hint, rc, setStart);
    return rc;
}

Status TransportLayer::RetrySet(const HostPort &workerAddr, ObjectBuffer &buffer, const TransportSetParam &param,
                                TransportHint hint)
{
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    std::shared_lock<std::shared_mutex> admission;
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, admission));
    TransportSetParam retryParam = param;
    retryParam.isRetry = true;
    auto &mutableBufferInfo = ObjectBufferInternal::GetMutableInfo(buffer);
    mutableBufferInfo.ubFailureReportRc = Status::OK();
    mutableBufferInfo.ubProviderStatus.reset();
    mutableBufferInfo.ubCqeStatus.reset();
    Status rc = transporter->Set(buffer, retryParam);
    const auto &bufferInfo = ObjectBufferInternal::GetInfo(buffer);
    (void)ReportLocalUbSenderFailure(
        { workerAddr, transporter->Kind(), bufferInfo.ubFailureReportRc, bufferInfo.ubProviderStatus,
          bufferInfo.ubCqeStatus },
        admission);
    if (rc.IsError()) {
        LOG(WARNING) << "Set still failed after rebuilding transport for worker " << workerAddr.ToString() << ": "
                     << rc;
    }
    return rc;
}

Status TransportLayer::MCreate(const HostPort &workerAddr, const std::vector<std::string> &objectKeys,
                               const std::vector<uint64_t> &dataSizes, const TransportCreateParam &param,
                               std::vector<std::shared_ptr<ObjectBuffer>> &buffers)
{
    RETURN_IF_NOT_OK(ValidateMultiCreateRequest(objectKeys, dataSizes, param));
    INJECT_POINT("TransportLayer.MCreate.beforeTransport");
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    TransportHint hint = advisor_->GetTransportHint(workerAddr);
    auto runMCreate = [&](TransportHint h) -> Status {
        std::shared_lock<std::shared_mutex> admission;
        RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(h));
        std::shared_ptr<IDataTransporter> transporter;
        RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, h, transporter));
        RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(h, admission));
        Status r = transporter->MCreate(workerAddr, objectKeys, dataSizes, param, buffers);
        if (admission.owns_lock()) {
            admission.unlock();
        }
        return r;
    };
    Status rc = runMCreate(hint);
    if (IsNonRetryableRpcError(rc)) {
        // Dead peer: tear down without retrying (the peer is gone). MultiCreate is not replayed since it
        // has no idempotency marker and the worker may have allocated memory before the failure.
        LOG(WARNING) << "Tear down dead RPC peer for worker " << workerAddr.ToString()
                     << " after MCreate failed without retry: " << rc;
        manager_->Teardown(workerAddr);
        return rc;
    }
    if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        // Rebuild once and retry, consistent with the Create path. MultiCreate has no idempotency marker,
        // so a lost response may leave the worker holding partial allocations; those are reclaimed by the
        // expired-fds reconciler (same fallback Create relies on).
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after ambiguous MCreate failure, retrying once: " << rc;
        manager_->Teardown(workerAddr);
        rc = runMCreate(hint);
        if (rc.GetCode() != K_NOT_SUPPORTED) {
            return rc;
        }
    }
    if (rc.GetCode() == K_NOT_SUPPORTED) {
        for (const auto &fallbackHint : CreateFallbackHints(hint)) {
            LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
                << "MCreate SHM unavailable on worker " << workerAddr.ToString() << ", fall back to "
                << TransportHintName(fallbackHint);
            rc = runMCreate(fallbackHint);
            if (rc.IsOk()) {
                return rc;
            }
        }
    }
    return rc;
}

Status TransportLayer::MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers, const TransportSetParam &param,
                            TransportMSetResult &result)
{
    result.Clear();
    RETURN_IF_NOT_OK(ValidateMSetRequest(buffers, param));
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const HostPort workerAddr = ObjectBufferInternal::GetInfo(*buffers.front()).workerAddr;
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_lock<std::shared_mutex> admission;
    RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(hint));
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, admission));
    Status rc = transporter->MSet(buffers, param, result);
    const auto &ubFailureReport = result.ubFailureReportRc;
    bool senderQuarantined = ReportLocalUbSenderFailure(
        { workerAddr, transporter->Kind(), ubFailureReport, result.ubProviderStatus, result.ubCqeStatus }, admission);
    if (senderQuarantined) {
        ScheduleMSetReleases(buffers, param.requestContext, result, TransportHint::TCP_ONLY);
        return rc.GetCode() == K_URMA_NEED_CONNECT ? ubFailureReport : rc;
    }
    const bool retryUbWrite = rc.GetCode() == K_URMA_NEED_CONNECT;
    const bool retryUnsentPublish = IsRetryableRpcError(rc) && !result.publishAttempted;
    if (!retryUbWrite && !retryUnsentPublish) {
        if (IsRetryableRpcError(rc) || IsNonRetryableRpcError(rc)) {
            LOG(WARNING) << "Tear down RPC and data plane for worker " << workerAddr.ToString()
                         << " after ambiguous MSet failure without replay: " << rc;
            manager_->Teardown(workerAddr);
        }
        ScheduleMSetReleases(buffers, param.requestContext, result);
        return rc;
    }
    if (retryUbWrite) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString() << " after MSet failed: " << rc;
        manager_->ResetDataPlane(workerAddr);
    } else {
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after MSet failed before publish: " << rc;
        manager_->Teardown(workerAddr);
    }
    rc = RetryMSet(workerAddr, buffers, param, hint, result);
    std::optional<TransportHint> releaseHint;
    if (localUbSenderUnavailable_.load(std::memory_order_acquire)) {
        releaseHint = TransportHint::TCP_ONLY;
    }
    ScheduleMSetReleases(buffers, param.requestContext, result, releaseHint);
    return rc;
}

Status TransportLayer::RetryMSet(const HostPort &workerAddr, const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                 const TransportSetParam &param, TransportHint hint, TransportMSetResult &result)
{
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    std::shared_lock<std::shared_mutex> admission;
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, admission));
    result.Clear();
    Status rc = transporter->MSet(buffers, param, result);
    (void)ReportLocalUbSenderFailure(
        { workerAddr, transporter->Kind(), result.ubFailureReportRc, result.ubProviderStatus, result.ubCqeStatus },
        admission);
    if (rc.IsError()) {
        LOG(WARNING) << "MSet still failed after rebuilding transport for worker " << workerAddr.ToString() << ": "
                     << rc;
    }
    return rc;
}

Status TransportLayer::Release(ObjectBuffer &buffer, const TransportRequestContext &context)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const HostPort workerAddr = ObjectBufferInternal::GetInfo(buffer).workerAddr;
    const ShmKey shmId = ObjectBufferInternal::GetInfo(buffer).shmId;
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, advisor_->GetTransportHint(workerAddr), transporter));
    return InvokeReleaseWithRetry(workerAddr, shmId, context, transporter);
}

void TransportLayer::ScheduleRelease(const HostPort &workerAddr, const ShmKey &shmId,
                                     const TransportRequestContext &context, std::optional<TransportHint> transportHint)
{
    if (shmId.Empty()) {
        return;
    }
    if (releasePool_ == nullptr) {
        std::shared_ptr<IDataTransporter> transporter;
        Status rc = manager_->GetOrCreate(workerAddr, transportHint.value_or(advisor_->GetTransportHint(workerAddr)),
                                          transporter);
        if (rc.IsOk()) {
            rc = InvokeReleaseWithRetry(workerAddr, shmId, context, transporter);
        }
        LOG_IF_ERROR(rc, "Release routed Set allocation failed");
        return;
    }
    auto manager = manager_;
    auto advisor = advisor_;
    releasePool_->Execute([manager, advisor, workerAddr, shmId, context, transportHint]() {
        std::shared_ptr<IDataTransporter> transporter;
        Status rc = manager->GetOrCreate(workerAddr, transportHint.value_or(advisor->GetTransportHint(workerAddr)),
                                         transporter);
        if (rc.IsOk()) {
            rc = TransportLayer::InvokeReleaseWithRetryOnAliveTransporter(workerAddr, shmId, context, transporter,
                                                                          manager, advisor);
        }
        LOG_IF_ERROR(rc, "Async release of routed Set allocation failed");
    });
}

Status TransportLayer::InvokeReleaseWithRetry(const HostPort &workerAddr, const ShmKey &shmId,
                                              const TransportRequestContext &context,
                                              std::shared_ptr<IDataTransporter> &transporter)
{
    return InvokeReleaseWithRetryOnAliveTransporter(workerAddr, shmId, context, transporter, manager_, advisor_);
}

Status TransportLayer::InvokeReleaseWithRetryOnAliveTransporter(
    const HostPort &workerAddr, const ShmKey &shmId, const TransportRequestContext &context,
    std::shared_ptr<IDataTransporter> &transporter, const std::shared_ptr<DataPlaneManager> &manager,
    const std::shared_ptr<TransportAdvisor> &advisor)
{
    // Retry InvokeDecreaseReference up to 3 times with exponential backoff. On a persistent RPC
    // failure, rebuild the transporter once before the final retry so a torn-down connection does
    // not cause a permanent leak (worker-side shm ref would never be decremented).
    constexpr int kMaxAttempts = 3;
    constexpr int kBackoffMs[] = { 0, 100, 400 };
    Status rc;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        if (attempt == 0) {
            rc = transporter->Release(shmId, context);
            if (rc.IsOk() || rc.GetCode() == K_NOT_FOUND) {
                return rc;
            }
            if (IsNonRetryableRpcError(rc)) {
                manager->Teardown(workerAddr);
                return rc;
            }
            LOG(WARNING) << "InvokeDecreaseReference attempt " << (attempt + 1) << "/" << kMaxAttempts
                         << " failed for worker " << workerAddr.ToString() << ", shmId=" << shmId.ToString()
                         << ": " << rc.ToString();
            continue;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kBackoffMs[attempt]));
        // Re-fetch transporter if it died (e.g. after Teardown); otherwise reuse cached one.
        if (transporter == nullptr || !transporter->IsAlive()) {
            Status rebuildRc = manager->GetOrCreate(workerAddr, advisor->GetTransportHint(workerAddr), transporter);
            if (rebuildRc.IsError() && attempt == kMaxAttempts - 1) {
                return rebuildRc;
            }
            if (rebuildRc.IsError()) {
                continue;
            }
        }
        rc = transporter->Release(shmId, context);
        if (rc.IsOk() || rc.GetCode() == K_NOT_FOUND) {
            return rc;
        }
        if (IsNonRetryableRpcError(rc)) {
            manager->Teardown(workerAddr);
            return rc;
        }
        LOG(WARNING) << "InvokeDecreaseReference attempt " << (attempt + 1) << "/" << kMaxAttempts
                     << " failed for worker " << workerAddr.ToString() << ", shmId=" << shmId.ToString()
                     << ": " << rc.ToString();
    }
    return rc;
}

void TransportLayer::ScheduleMSetReleases(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                          const TransportRequestContext &context,
                                          const TransportMSetResult &result,
                                          std::optional<TransportHint> transportHint)
{
    if (result.workerAutoRelease && result.failedKeys.empty()) {
        return;
    }
    std::unordered_set<std::string> failedKeys;
    if (result.workerAutoRelease) {
        failedKeys.reserve(result.failedKeys.size());
        failedKeys.insert(result.failedKeys.begin(), result.failedKeys.end());
    }
    for (const auto &buffer : buffers) {
        const auto &info = ObjectBufferInternal::GetInfo(*buffer);
        // Owner-managed (routed SHM zero-copy) buffers release via their send-side owner on destruction.
        if (info.receiveBufferOwner != nullptr && info.receiveBufferOwner->ManagesWorkerReference()) {
            continue;
        }
        if (result.workerAutoRelease && failedKeys.find(info.objectKey) == failedKeys.end()) {
            continue;
        }
        ScheduleRelease(info.workerAddr, info.shmId, context, transportHint);
    }
}

Status TransportLayer::ApplyWorkerSnapshot(WorkerSnapshot snapshot)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    // Copy the same-host list before the snapshot is moved below. SetSameHostWorkers takes the
    // advisor's RWLock write lock; keep it out of the reconcileMutex_ critical section so the
    // reconcile thread (which touches entries_ under reconcileMutex_) never blocks on the advisor
    // write lock.
    std::vector<HostPort> sameHostAddrs = snapshot.sameHostAddrs;
    {
        std::lock_guard<bthread::Mutex> lock(reconcileMutex_);
        CHECK_FAIL_RETURN_STATUS(reconcileStarted_, K_NOT_READY, "Transport reconcile thread is not initialized");
        CHECK_FAIL_RETURN_STATUS(!reconcileStopping_, K_SHUTTING_DOWN, "TransportLayer is shutting down");
        // Publish the live-worker snapshot to the manager FIRST. The advisor's same-host set is
        // updated AFTER releasing reconcileMutex_ below; updating the advisor first would open a
        // window where GetTransportHint returns SHM_CANDIDATE for a worker the manager does not yet
        // know is live, so GetOrCreate returns K_NOT_FOUND and the release-retry path can leak a
        // shm ref. With manager-first, the advisor only marks as same-host workers the manager can
        // already hand out a transporter for.
        RETURN_IF_NOT_OK(manager_->UpdateWorkerSnapshot(snapshot));
        pendingSnapshot_ = std::move(snapshot);
        reconcileCv_.notify_one();
    }
    if (advisor_ != nullptr) {
        advisor_->SetSameHostWorkers(sameHostAddrs);
    }
    return Status::OK();
}

bool TransportLayer::WaitForSnapshotOrStop(std::unique_lock<bthread::Mutex> &lock)
{
    // bthread::ConditionVariable has no predicate overloads and its wait_until takes a CLOCK_REALTIME
    // timespec, not a chrono steady_clock time_point, so emulate master's wait_until(deadline, pred)
    // and wait(pred) by hand. A probe deadline expiring must EXIT this wait so ReconcileLoop can run
    // TryRecoverLocalUbSender and fire the recovery probe; therefore break after any wait returns
    // (deadline elapsed or notified), matching master's post-wait_until break.
    while (!reconcileStopping_ && !pendingSnapshot_.has_value()) {
        auto probeDeadline = GetLocalUbProbeDeadline();
        if (probeDeadline.has_value()) {
            const auto now = std::chrono::steady_clock::now();
            if (*probeDeadline > now) {
                const auto waitNs = std::chrono::duration_cast<std::chrono::nanoseconds>(*probeDeadline - now);
                (void)reconcileCv_.wait_until(lock, butil::nanoseconds_from_now(waitNs.count()));
            }
            break;
        }
        // No probe deadline: wait for a wake (stop / snapshot / new deadline set by a failure report),
        // then re-check at the top of the loop.
        reconcileCv_.wait(lock);
    }
    return !reconcileStopping_;
}

void TransportLayer::ReconcileLoop()
{
    bool keepRunning = true;
    while (keepRunning) {
        std::optional<WorkerSnapshot> snapshot;
        {
            std::unique_lock<bthread::Mutex> lock(reconcileMutex_);
            if (!WaitForSnapshotOrStop(lock)) {
                keepRunning = false;
                continue;
            }
            if (pendingSnapshot_.has_value()) {
                snapshot = std::move(pendingSnapshot_);
                pendingSnapshot_.reset();
            }
        }
        if (snapshot.has_value()) {
            manager_->ReconcileWithSnapshot(*snapshot);
        }
        TryRecoverLocalUbSender();
    }
}

void TransportLayer::Shutdown()
{
    std::lock_guard<bthread::Mutex> shutdownLock(shutdownMutex_);
    {
        std::lock_guard<std::shared_mutex> admission(localUbSenderMutex_);
        shutdownRequested_.store(true, std::memory_order_release);
        localUbSenderUnavailable_.store(true, std::memory_order_release);
    }
    Thread reconcileThread;
    {
        std::lock_guard<bthread::Mutex> lock(reconcileMutex_);
        reconcileStopping_ = true;
        pendingSnapshot_.reset();
        reconcileCv_.notify_all();
        if (reconcileStarted_) {
            reconcileThread = std::move(reconcileThread_);
            reconcileStarted_ = false;
        }
    }
    if (reconcileThread.joinable()) {
        reconcileThread.join();
    }
    // Drain pending DecreaseReference tasks before closing their endpoint connections.
    releasePool_.reset();
    objectRead_.reset();
    if (manager_ != nullptr) {
        manager_->Shutdown();
    }
}

}  // namespace client
}  // namespace datasystem
