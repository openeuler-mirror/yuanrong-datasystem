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

#include "datasystem/client/object_cache/transport/transport_layer.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <new>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>

#include "datasystem/client/object_cache/transport/common/deadline_retry.h"
#include "datasystem/client/object_cache/transport/data_plane/client_ub_probe_cooldown.h"
#include "datasystem/client/object_cache/transport/data_plane/data_plane_executor.h"
#include "datasystem/client/object_cache/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/object_cache/transport/data_plane/ub_transporter.h"
#include "datasystem/client/object_cache/transport/metadata/object_metadata_client.h"
#include "datasystem/client/object_cache/transport/object_buffer_internal.h"
#include "datasystem/client/object_cache/transport/object_read/replica_reader.h"
#include "datasystem/client/object_cache/transport/rpc/exist_request_builder.h"
#include "datasystem/client/object_cache/transport/rpc/mset_request_builder.h"
#include "datasystem/client/object_cache/transport/transport_advisor.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/object_cache/ub_failure_classifier.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/uuid_generator.h"

#include "butil/time.h"

namespace datasystem {
constexpr int FAILURE_LOG_RATE = 100;
namespace client {
namespace {
constexpr int32_t PROVIDER_UB_RECOVERY_PROBE_TIMEOUT_MS = 3'000;
// SHM-off fallback is a steady-state path (every write on a SHM-disabled same-host worker); throttle the
// diagnostic so it does not flood the log. Matches the read-path/DataPlaneExecutor convention.
constexpr int TRANSPORT_DIAG_LOG_RATE = 100;
constexpr int AMBIGUOUS_CREATE_CLEANUP_ATTEMPTS = 3;
constexpr int AMBIGUOUS_CREATE_CLEANUP_BACKOFF_MS[] = { 0, 100, 400 };
constexpr int AMBIGUOUS_CREATE_CLEANUP_RPC_TIMEOUT_MS = 500;
constexpr size_t AMBIGUOUS_CREATE_CLEANUP_THREAD_NUM = 4;
constexpr int64_t MCREATE_RESERVATION_RETRY_BACKOFF_MS = 1;

Status GenerateAllocationId(TransportCreateParam &param)
{
    try {
        param.allocationId.clear();
        param.allocationIds.clear();
        param.allocationId = GetStringUuid();
    } catch (const std::bad_alloc &error) {
        return Status(K_OUT_OF_MEMORY, error.what());
    }
    CHECK_FAIL_RETURN_STATUS(!param.allocationId.empty(), K_RUNTIME_ERROR, "Generate allocation ID failed");
    return Status::OK();
}

Status GenerateAllocationIds(size_t count, TransportCreateParam &param)
{
    param.allocationId.clear();
    param.allocationIds.clear();
    try {
        std::vector<std::string> allocationIds;
        allocationIds.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            auto allocationId = GetStringUuid();
            CHECK_FAIL_RETURN_STATUS(!allocationId.empty(), K_RUNTIME_ERROR, "Generate allocation ID failed");
            allocationIds.emplace_back(std::move(allocationId));
        }
        param.allocationIds = std::move(allocationIds);
    } catch (const std::bad_alloc &error) {
        return Status(K_OUT_OF_MEMORY, error.what());
    }
    return Status::OK();
}

void RecordAllocationIds(const TransportCreateParam &param, std::unordered_set<ShmKey> &shmIds)
{
    if (!param.allocationId.empty()) {
        shmIds.emplace(ShmKey::Intern(param.allocationId));
    }
    for (const auto &allocationId : param.allocationIds) {
        shmIds.emplace(ShmKey::Intern(allocationId));
    }
}

void ForgetAllocationIds(const TransportCreateParam &param, std::unordered_set<ShmKey> &shmIds)
{
    if (shmIds.empty()) {
        return;
    }
    if (!param.allocationId.empty()) {
        (void)shmIds.erase(ShmKey::Intern(param.allocationId));
    }
    for (const auto &allocationId : param.allocationIds) {
        (void)shmIds.erase(ShmKey::Intern(allocationId));
    }
}

bool IsAmbiguousCreateFailure(const Status &status)
{
    if (!IsRetryableRpcError(status) && !IsNonRetryableRpcError(status)) {
        return false;
    }
    return !IsBrpcRequestDefinitelyNotSent(status) && !IsBrpcServerApplicationError(status);
}

bool IsAllocationReplayConflict(const Status &status)
{
    return status.GetCode() == K_DUPLICATED || status.GetCode() == K_TRY_AGAIN;
}

Status ReleaseAllocationIdsOnce(const std::shared_ptr<DataPlaneManager> &manager, const HostPort &workerAddr,
                                const std::vector<ShmKey> &shmIds,
                                const TransportRequestContext &context)
{
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(manager->GetOrCreateRpcClient(workerAddr, rpcClient));
    return rpcClient->InvokeDecreaseReferences(context, shmIds);
}

void CleanupAmbiguousAllocations(const std::shared_ptr<DataPlaneManager> &manager, const HostPort &workerAddr,
                                 const std::vector<ShmKey> &shmIds,
                                 const TransportRequestContext &context)
{
    Status lastRc;
    for (int attempt = 0; attempt < AMBIGUOUS_CREATE_CLEANUP_ATTEMPTS; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(AMBIGUOUS_CREATE_CLEANUP_BACKOFF_MS[attempt]));
        ApiDeadlineGuard rpcDeadline(AMBIGUOUS_CREATE_CLEANUP_RPC_TIMEOUT_MS);
        lastRc = ReleaseAllocationIdsOnce(manager, workerAddr, shmIds, context);
    }
    if (lastRc.IsError()) {
        LOG(WARNING) << "Ambiguous Create allocation cleanup exhausted, worker=" << workerAddr.ToString()
                     << ", clientId=" << context.clientId << ", allocationCount=" << shmIds.size()
                     << ", status=" << lastRc;
    } else {
        VLOG(1) << "Ambiguous Create allocation cleanup RPCs completed, worker=" << workerAddr.ToString()
                << ", clientId=" << context.clientId << ", allocationCount=" << shmIds.size();
    }
}

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

}  // namespace

struct TransportLayer::LocalUbSenderState final : public UrmaLateCompletionObserver,
                                                  public std::enable_shared_from_this<LocalUbSenderState> {
    // Closing and the active count share one modification order so the last release cannot miss Shutdown's drain.
    static constexpr uint64_t IN_FLIGHT_CLOSING = 1ull << 63;
    static constexpr uint64_t IN_FLIGHT_COUNT_MASK = IN_FLIGHT_CLOSING - 1;
    bool IsShuttingDown() const
    {
        return (inFlightGate.load(std::memory_order_acquire) & IN_FLIGHT_CLOSING) != 0;
    }

    void CloseAdmission()
    {
        inFlightGate.fetch_or(IN_FLIGHT_CLOSING, std::memory_order_acq_rel);
    }

    bool TryAdmitOperation()
    {
        uint64_t current = inFlightGate.load(std::memory_order_acquire);
        while ((current & IN_FLIGHT_CLOSING) == 0) {
            if (inFlightGate.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    bool CompleteOperation()
    {
        return inFlightGate.fetch_sub(1, std::memory_order_acq_rel) == (IN_FLIGHT_CLOSING | 1);
    }

    uint64_t InFlightOperationCount() const
    {
        return inFlightGate.load(std::memory_order_acquire) & IN_FLIGHT_COUNT_MASK;
    }

    void RequestProbe(const HostPort &destination, ClientUbProbeScope scope, uint64_t generation = 0) noexcept
    {
        try {
            auto filter = healthFilter.lock();
            if (IsShuttingDown() ||
                (scope == ClientUbProbeScope::REMOTE_WORKER
                 && (filter == nullptr || !requestRemoteVerification))) {
                return;
            }
            const auto observedAtMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
            if (generation == 0 && filter != nullptr) {
                generation = filter->CaptureWriteTargetCompletionGeneration(destination);
            }
            if ((scope == ClientUbProbeScope::REMOTE_WORKER && generation == 0) ||
                !probeCooldown.TryAcquire(destination, scope, generation, observedAtMs) || !TryAdmitOperation()) {
                return;
            }
            LocalUbSenderOperation operation;
            operation.state = this;
            if (scope == ClientUbProbeScope::LOCAL_NODE) {
                INJECT_POINT_NO_RETURN("TransportLayer.ClientUbProbeCooldown.localAccepted");
                TriggerClientLocalUbPortHealthQuery();
                return;
            }
            if (filter->IsWriteTargetCompletionCurrent(destination, generation)) {
                INJECT_POINT_NO_RETURN("TransportLayer.ClientUbProbeCooldown.remoteAccepted");
                const bool requested = requestRemoteVerification(destination);
                if (!requested) {
                    LOG_FIRST_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
                        << "CLIENT_UB_PROBE action=request_not_accepted scope=remote_worker peer="
                        << destination.ToString() << " generation=" << generation
                        << " cooldown_ms=" << ClientUbProbeCooldown::COOLDOWN_MS;
                }
            }
        } catch (const std::exception &error) {
            LOG(ERROR) << "Failed to schedule UB port-health probe: " << error.what();
        } catch (...) {
            LOG(ERROR) << "Failed to schedule UB port-health probe: unknown exception";
        }
    }

    void ReconcileProbeDestinations(const std::unordered_set<HostPort> &destinations)
    {
        probeCooldown.Reconcile(destinations);
    }

    void OnLateUrmaCompletion(const UrmaLateCompletion &completion, uint64_t,
                              uint64_t peerToken) noexcept override
    {
        HostPort destination;
        if (completion.cqeStatus != URMA_REMOTE_ACK_TIMEOUT_STATUS
            && completion.cqeStatus != URMA_PORT_UNAVAILABLE_STATUS) {
            return;
        }
        if (destination.ParseString(completion.remoteAddress).IsError()) {
            return;
        }
        if (completion.cqeStatus == URMA_REMOTE_ACK_TIMEOUT_STATUS) {
            RequestProbe(destination, ClientUbProbeScope::REMOTE_WORKER, peerToken);
        } else {
            RequestProbe(destination, ClientUbProbeScope::LOCAL_NODE, peerToken);
        }
    }

    std::weak_ptr<bthread::Mutex> reconcileMutex;
    std::weak_ptr<bthread::ConditionVariable> reconcileCv;
    std::weak_ptr<UbHealthFilter> healthFilter;
    std::function<bool(const HostPort &)> requestRemoteVerification;
    ClientUbProbeCooldown probeCooldown;
    bthread::Mutex inFlightDrainMutex;
    bthread::ConditionVariable inFlightCv;
    std::atomic<uint64_t> inFlightGate{ 0 };
};

TransportLayer::LocalUbSenderOperation::~LocalUbSenderOperation()
{
    if (state == nullptr) {
        return;
    }
    if (state->CompleteOperation()) {
        std::lock_guard<bthread::Mutex> lock(state->inFlightDrainMutex);
        state->inFlightCv.notify_all();
    }
}

void TransportLayer::ConfigureUbHealthTriggers()
{
    localUbSenderState_->healthFilter = healthFilter_;
    std::weak_ptr<DataPlaneManager> weakManager(manager_);
    localUbSenderState_->requestRemoteVerification = [weakManager](const HostPort &peer) {
        auto manager = weakManager.lock();
        return manager != nullptr && manager->RequestUbPortHealthVerification(peer);
    };
    std::weak_ptr<LocalUbSenderState> weakState(localUbSenderState_);
    healthFilter_->SetRemotePortHealthVerificationTrigger([weakState](const HostPort &peer) {
        auto state = weakState.lock();
        if (state != nullptr) {
            state->RequestProbe(peer, ClientUbProbeScope::REMOTE_WORKER);
        }
    });
}

TransportLayer::TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                               uint64_t fastTransportMemSize, TransportLayerOptions options)
    : advisor_(std::make_shared<TransportAdvisor>()),
      releasePool_(std::move(options.releasePool)),
      ambiguousCreateCleanupPool_(
          std::make_shared<ThreadPool>(0, AMBIGUOUS_CREATE_CLEANUP_THREAD_NUM, "ambiguous-create-cleanup")),
      allowUbRuntimeFailure_(options.allowUbRuntimeFailure)
{
    localUbSenderState_ = std::make_shared<LocalUbSenderState>();
    localUbSenderState_->reconcileMutex = reconcileMutex_;
    localUbSenderState_->reconcileCv = reconcileCv_;
    healthFilter_ = options.readSourceFilter == nullptr ? std::make_shared<UbHealthFilter>()
                                                         : std::move(options.readSourceFilter);
    std::weak_ptr<UbHealthFilter> weakHealthFilter(healthFilter_);
    auto ubBufferProvider = CreateDefaultUbReceiveBufferProvider();
    auto supportsPortHealthVerification = [weakHealthFilter](const HostPort &peer) {
        auto filter = weakHealthFilter.lock();
        return filter != nullptr && filter->SupportsPortHealthVerification(peer);
    };
    manager_ = std::make_shared<DataPlaneManager>(std::move(signature), fastTransportMemSize,
                                                  std::move(options.channelConfig), ubBufferProvider,
                                                  options.enableClientDirectPipelineH2D, options.pipelineThreadNum,
                                                  releasePool_, options.initializeUbRuntime,
                                                  options.allowUbRuntimeFailure, options.hostMemoryPinManager,
                                                  std::move(options.ubHealthSummaryHook),
                                                  std::move(options.verifiedUbHealthSummaryHook),
                                                  [this] { NotifyReconcile(); },
                                                  std::move(supportsPortHealthVerification));
    ConfigureUbHealthTriggers();
    auto retry = std::make_shared<DeadlineRetry>(std::move(options.retryAdmissionCheck));
    auto ubFailureHandler = [this](const HostPort &provider, const ProviderUbFailureDetailPb &detail) {
        (void)ReportProviderUbFailure(provider, detail);
    };
    auto metadata = std::make_shared<ObjectMetadataClient>(manager_, retry, advisor_, std::move(ubBufferProvider),
                                                           GetConfiguredUbInlineBufferSize(),
                                                           std::move(options.metadataFailureHandler),
                                                           std::move(ubFailureHandler));
    auto executor = std::make_shared<DataPlaneExecutor>(manager_, advisor_, std::move(options.drainingFallbackHandler));
    auto reportReadOutcome = [this](const HostPort &workerAddr, const GetObjectRemoteRspPb &response) {
        if (response.has_provider_ub_failure_detail()) {
            (void)ReportProviderUbFailure(workerAddr, response.provider_ub_failure_detail());
        }
    };
    // A remote Worker's UB isolation must not gate reads: the Worker owns the UB writeback verdict and the read path
    // reacts to the response (TCP fallback or another replica). The client-local port admission still runs before
    // routing, in TransportLayer::Get and AcquireDirectUbEndpointLease.
    auto replicas = std::make_shared<ReplicaReader>(std::move(executor), std::move(retry), taskPool, nullptr,
                                                    std::move(reportReadOutcome));
    objectRead_ = std::make_unique<ObjectReadFlow>(std::move(metadata), std::move(replicas), std::move(taskPool));
    localPortHealthObserver_ = std::move(options.localPortHealthObserver);
}

TransportLayer::TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                               std::shared_ptr<TransportAdvisor> advisor)
    : TransportLayer(std::move(dataPlaneManager), std::move(advisor), nullptr)
{
}

TransportLayer::TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                               std::shared_ptr<TransportAdvisor> advisor,
                               std::shared_ptr<UbHealthFilter> readSourceFilter,
                               std::shared_ptr<ThreadPool> releasePool)
    : manager_(std::move(dataPlaneManager)),
      advisor_(std::move(advisor)),
      releasePool_(std::move(releasePool)),
      ambiguousCreateCleanupPool_(
          std::make_shared<ThreadPool>(0, AMBIGUOUS_CREATE_CLEANUP_THREAD_NUM, "ambiguous-create-cleanup")),
      healthFilter_(readSourceFilter == nullptr ? std::make_shared<UbHealthFilter>() : std::move(readSourceFilter))
{
    localUbSenderState_ = std::make_shared<LocalUbSenderState>();
    localUbSenderState_->reconcileMutex = reconcileMutex_;
    localUbSenderState_->reconcileCv = reconcileCv_;
    localUbSenderState_->healthFilter = healthFilter_;
}

bool TransportLayer::ReportProviderUbFailure(const HostPort &provider, const ProviderUbFailureDetailPb &detail)
{
    if (!IsClientUbFaultIsolationEnabled()) {
        return false;
    }
    if (IsClientUbWritebackAckTimeout(provider, detail)) {
        localUbSenderState_->RequestProbe(provider, ClientUbProbeScope::LOCAL_NODE);
    }
    if (healthFilter_ == nullptr) {
        return false;
    }
    const bool quarantined = healthFilter_->ReportProviderFailure(provider, detail);
    if (quarantined) {
        NotifyReconcile();
    }
    return quarantined;
}

void TransportLayer::ObserveUbHealthSummary(const UbHealthSummary &summary)
{
    if (manager_ != nullptr) {
        manager_->ObserveUbHealthSummary(summary);
    }
}

UbHealthSummaryApplyHook TransportLayer::GetUbHealthSummaryApplyHook() const
{
    std::weak_ptr<DataPlaneManager> weakManager(manager_);
    return [weakManager](const UbHealthSummary &summary) {
        auto manager = weakManager.lock();
        if (manager != nullptr) {
            manager->ObserveUbHealthSummary(summary);
        }
    };
}

Status TransportLayer::CheckUbReadSource(const HostPort &workerAddr, AccessTransportKind &deniedKind) const
{
    if (healthFilter_ != nullptr && healthFilter_->IsAvailable(workerAddr, WorkerAccessAction::CONTROL)) {
        return Status::OK();
    }
    // Report the denied medium to the caller-thread aggregation instead of writing the request-scoped
    // tracker here: a later replica may actually carry data over TCP, which must outrank this denial.
    deniedKind = AccessTransportKind::UB;
    return Status(K_URMA_READ_SOURCE_DENIED, "Client UB read source denied: " + workerAddr.ToString());
}

bool TransportLayer::ScheduleProviderRecoveryFromGlobalSummary(const HostPort &provider)
{
    if (!IsClientUbFaultIsolationEnabled() || localUbSenderState_->IsShuttingDown() || healthFilter_ == nullptr
        || !healthFilter_->SeedProviderRecoveryFromGlobalSummary(provider)) {
        return false;
    }
    NotifyReconcile();
    return true;
}

std::function<void(const HostPort &)> TransportLayer::MakeProviderRecoveryCallback() const
{
    std::weak_ptr<LocalUbSenderState> weakState(localUbSenderState_);
    return [weakState](const HostPort &provider) {
        auto state = weakState.lock();
        if (state == nullptr) {
            return;
        }
        auto filter = state->healthFilter.lock();
        auto mutex = state->reconcileMutex.lock();
        auto cv = state->reconcileCv.lock();
        if (filter == nullptr || mutex == nullptr || cv == nullptr) {
            return;
        }
        std::lock_guard<bthread::Mutex> lock(*mutex);
        if (!state->IsShuttingDown() && filter->SeedProviderRecoveryFromGlobalSummary(provider)) {
            cv->notify_one();
        }
    };
}

Status TransportLayer::CheckLocalUbSenderAdmission(TransportHint hint) const
{
    CHECK_FAIL_RETURN_STATUS(!localUbSenderState_->IsShuttingDown(), K_SHUTTING_DOWN,
                             "TransportLayer is shutting down");
    return hint == TransportHint::UB_CANDIDATE ? CheckClientLocalUbPortHealth() : Status::OK();
}

Status TransportLayer::CheckLocalUbSenderAdmission() const
{
    return CheckLocalUbSenderAdmission(TransportHint::UB_CANDIDATE);
}

Status TransportLayer::CheckLocalNodeAdmission() const
{
    return CheckClientLocalUbPortHealth();
}

void TransportLayer::ReportLocalPortHealthTrigger()
{
    TriggerClientLocalUbPortHealthQuery();
}

std::optional<UbPortHealthSummary> TransportLayer::GetLocalPortHealthSummary() const
{
    return localPortHealthMonitor_ == nullptr ? std::nullopt : localPortHealthMonitor_->GetSummary();
}

Status TransportLayer::RunClientLocalUbWrite(const HostPort &workerAddr, ObjectBufferInfo &bufferInfo,
                                             const std::function<Status()> &write)
{
    RETURN_IF_NOT_OK(CheckLocalNodeAdmission());
    LocalUbSenderOperation operation;
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(TransportHint::UB_CANDIDATE, operation));
    bufferInfo.ubFailureReportRc = Status::OK();
    bufferInfo.ubProviderStatus.reset();
    bufferInfo.ubCqeStatus.reset();
    PrepareLocalUbLateCompletion(bufferInfo, AccessTransportKind::UB, &workerAddr);
    Status rc = write();
    const Status &failureRc = bufferInfo.ubFailureReportRc.IsError() ? bufferInfo.ubFailureReportRc : rc;
    (void)ReportLocalUbSenderFailure({ workerAddr, AccessTransportKind::UB, failureRc,
                                      bufferInfo.ubProviderStatus, bufferInfo.ubCqeStatus });
    return rc;
}

Status TransportLayer::AcquireLocalUbSenderAdmission(TransportHint hint, LocalUbSenderOperation &operation) const
{
    // Callers may check before connection setup; repeat here to close the race before admitting the UB operation.
    RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(hint));
    if (hint != TransportHint::UB_CANDIDATE) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(localUbSenderState_->TryAdmitOperation(), K_SHUTTING_DOWN,
                             "TransportLayer is shutting down");
    operation.state = localUbSenderState_.get();
    return Status::OK();
}

void TransportLayer::PrepareLocalUbLateCompletion(ObjectBufferInfo &bufferInfo, AccessTransportKind kind,
                                                  const HostPort *explicitWorker) const
{
    // The context attributes an actual WR completion to its destination and generation. The completion only requests
    // verification; a current all-BAD query result remains the isolation authority.
    if (!IsClientUbFaultIsolationEnabled() || kind != AccessTransportKind::UB) {
        bufferInfo.ubLateCompletionContext.reset();
        return;
    }
    const HostPort &worker = explicitWorker == nullptr ? bufferInfo.workerAddr : *explicitWorker;
    const uint64_t peerToken =
        healthFilter_ == nullptr ? 0 : healthFilter_->CaptureWriteTargetCompletionGeneration(worker);
    bufferInfo.ubLateCompletionContext = UrmaLateCompletionContext{ localUbSenderState_, 0, peerToken, true };
}

bool TransportLayer::ReportWriteTargetUbFailure(const LocalUbSenderFailureView &failure)
{
    if (!IsClientUbFaultIsolationEnabled() || healthFilter_ == nullptr || failure.kind != AccessTransportKind::UB
        || failure.status.IsOk() || !failure.cqeStatus.has_value()
        || *failure.cqeStatus != URMA_REMOTE_ACK_TIMEOUT_STATUS) {
        return false;
    }
    // The WR completion observer owns CQE9 probing. The aggregate Set/MSet status is not a second observation.
    return !healthFilter_->IsWriteTargetAvailable(failure.workerAddr);
}

bool TransportLayer::ReportLocalUbSenderFailure(const LocalUbSenderFailureView &failure)
{
    if (!IsClientUbFaultIsolationEnabled() || failure.kind != AccessTransportKind::UB || failure.status.IsOk()) {
        return false;
    }
    UbOpOutcome outcome(failure.workerAddr, UbOperationKind::CLIENT_PUT, failure.status);
    outcome.providerStatus = failure.providerStatus;
    outcome.cqeStatus = failure.cqeStatus;
    outcome.learnedFrom = "client_local_ub_write";
    if (UbFailureClassifier().Classify(outcome) != UbFailureClass::PORT_UNAVAILABLE_ERROR4) {
        return false;
    }
    localUbSenderState_->RequestProbe(failure.workerAddr, ClientUbProbeScope::LOCAL_NODE);
    return true;
}

std::optional<std::chrono::steady_clock::time_point> TransportLayer::GetProviderUbProbeDeadline() const
{
    if (healthFilter_ == nullptr) {
        return std::nullopt;
    }
    auto deadlineMs = healthFilter_->NextProviderRecoveryDeadlineMs();
    if (!deadlineMs.has_value()) {
        return std::nullopt;
    }
    const uint64_t nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    const uint64_t delayMs = *deadlineMs > nowMs ? *deadlineMs - nowMs : 0;
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(delayMs);
}

std::optional<std::chrono::steady_clock::time_point> TransportLayer::GetWriteTargetUbProbeDeadline() const
{
    if (healthFilter_ == nullptr) {
        return std::nullopt;
    }
    auto deadlineMs = healthFilter_->NextWriteTargetRecoveryDeadlineMs();
    if (!deadlineMs.has_value()) {
        return std::nullopt;
    }
    const uint64_t nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    const uint64_t delayMs = *deadlineMs > nowMs ? *deadlineMs - nowMs : 0;
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(delayMs);
}

void TransportLayer::TryRecoverProviderUbSource()
{
    if (healthFilter_ == nullptr || localUbSenderState_->IsShuttingDown()) {
        return;
    }
    const uint64_t nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto candidate = healthFilter_->TryBeginProviderRecovery(nowMs);
    if (!candidate.has_value()) {
        return;
    }

    UbHealthSummary summary;
    Status probeRc = manager_->ProbeProviderUbRecovery(candidate->token.peer, candidate->expectedIncarnation,
                                                       PROVIDER_UB_RECOVERY_PROBE_TIMEOUT_MS, summary);
    std::optional<UbHealthSummary> responseSummary;
    if (!summary.worker.Empty() && !summary.incarnation.empty()) {
        responseSummary = summary;
    }
    const uint64_t completionMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    if (healthFilter_->CompleteProviderRecovery(*candidate, responseSummary, probeRc, completionMs)) {
        LOG(INFO) << "Client Provider UB source recovered via dedicated probe from "
                  << candidate->token.peer.ToString();
    } else if (probeRc.IsError()) {
        LOG_EVERY_N(WARNING, FAILURE_LOG_RATE) << "Client Provider UB source recovery probe failed for "
                                            << candidate->token.peer.ToString() << ": " << probeRc;
    } else {
        // A probe that succeeds while the port-health verifier owns the verdict produces no recovery and
        // no error, so this branch is otherwise silent even when the probe keeps firing.
        SLOW_LOG(INFO) << "Client Provider UB source probe deferred to port-health verification for "
            << candidate->token.peer.ToString();
    }
}

void TransportLayer::TryRecoverWriteTargetUbSource()
{
    if (healthFilter_ == nullptr || localUbSenderState_->IsShuttingDown()) {
        return;
    }
    const uint64_t nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto candidate = healthFilter_->TryBeginWriteTargetRecovery(nowMs);
    if (!candidate.has_value()) {
        return;
    }
    Status probeRc = manager_->ProbeUbWriteTarget(candidate->token.peer);
    const uint64_t completionMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    if (healthFilter_->CompleteWriteTargetRecovery(*candidate, probeRc, completionMs)) {
        LOG(INFO) << "Client UB write target recovered via directional probe, worker="
                  << candidate->token.peer.ToString();
    } else if (probeRc.IsError()) {
        LOG(WARNING) << "Client UB write target recovery probe failed, worker="
                     << candidate->token.peer.ToString() << ": " << probeRc;
    }
}

void TransportLayer::NotifyReconcile()
{
    // Synchronize the notification with the waiter's predicate-to-wait transition. The actual recovery
    // state lives under its domain lock; taking reconcileMutex_ here prevents a notification from being
    // lost after the waiter checked that state but before ConditionVariable::wait released this mutex.
    std::lock_guard<bthread::Mutex> lock(*reconcileMutex_);
    reconcileCv_->notify_one();
}

TransportLayer::~TransportLayer()
{
    Shutdown();
}

Status TransportLayer::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_IF_NOT_OK(manager_->Init());
    RETURN_IF_NOT_OK(ConfigureLocalPortHealth());
    std::lock_guard<bthread::Mutex> lock(*reconcileMutex_);
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

Status TransportLayer::ConfigureLocalPortHealth()
{
    if (!IsUrmaRuntimeConfigured()) {
        return Status::OK();
    }
    auto status = GetLocalUbPortHealthMonitor(localPortHealthMonitor_);
    if (status.IsError()) {
        if (!allowUbRuntimeFailure_) {
            return status;
        }
        SLOW_LOG(WARNING) << "Optional Client UB port-health monitor is unavailable; continue with SHM/TCP: "
            << status;
        return Status::OK();
    }
    if (!localPortHealthObserver_.expired()) {
        status = localPortHealthMonitor_->AddObserver(localPortHealthObserver_);
        if (status.IsError() && !allowUbRuntimeFailure_) {
            return status;
        }
        if (status.IsError()) {
            SLOW_LOG(WARNING) << "Optional Client UB port-health observer registration failed; continue with SHM/TCP: "
                << status;
        }
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
    RETURN_IF_NOT_OK(CheckLocalNodeAdmission());
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
    RETURN_IF_NOT_OK(CheckLocalNodeAdmission());
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

bool TransportLayer::IsSameHostWorker(const HostPort &workerAddr) const
{
    // No transport layer (local-only mode) or no topology yet: fall back to false so cross-host
    // callers route through the transport layer. The bound-worker SHM path is taken only when the
    // advisor explicitly classifies the worker as same-host.
    if (advisor_ == nullptr) {
        return false;
    }
    return advisor_->GetTransportHint(workerAddr) == TransportHint::SHM_CANDIDATE;
}

Status TransportLayer::Create(const HostPort &workerAddr, const std::string &objectKey, uint64_t dataSize,
                              TransportCreateParam param, std::shared_ptr<ObjectBuffer> &buffer)
{
    RETURN_IF_NOT_OK(CheckLocalNodeAdmission());
    RETURN_IF_NOT_OK(ValidateCreateRequest(objectKey, dataSize, param));
    INJECT_POINT("TransportLayer.Create.beforeTransport");
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const auto hint = advisor_->GetTransportHint(workerAddr);
    param.allocationId.clear();
    param.allocationIds.clear();
    if (hint != TransportHint::TCP_ONLY) {
        RETURN_IF_NOT_OK(GenerateAllocationId(param));
    }
    std::unordered_set<ShmKey> ambiguousShmIds;
    Status rc = TryCreate(workerAddr, objectKey, dataSize, param, hint, buffer, ambiguousShmIds);
    if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        const Status ambiguousRc = rc;
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after Create failed: " << rc;
        manager_->Teardown(workerAddr);
        rc = TryCreate(workerAddr, objectKey, dataSize, param, hint, buffer, ambiguousShmIds);
        if (IsAllocationReplayConflict(rc)) {
            rc = ambiguousRc;
        }
    }
    if (rc.GetCode() == K_NOT_SUPPORTED) {
        for (const auto &fallbackHint : advisor_->GetFallbackHints(hint)) {
            SLOW_LOG(WARNING) << "Create SHM unavailable on worker " << workerAddr.ToString() << ", fall back to "
                << TransportHintName(fallbackHint);
            rc = TryCreate(workerAddr, objectKey, dataSize, param, fallbackHint, buffer, ambiguousShmIds);
            if (rc.IsOk()) {
                ScheduleAmbiguousCreateCleanup(workerAddr, ambiguousShmIds, param.requestContext);
                return rc;
            }
        }
    }
    if (rc.IsError()) {
        // Throttle this terminal diagnostic like the SHM-unavailable fallback log above so a sustained UB
        // outage (e.g. client-local all-port isolation) does not emit one WARN per request.
        SLOW_LOG(WARNING) << "Create still failed for worker " << workerAddr.ToString() << ": " << rc;
        ScheduleAmbiguousCreateCleanup(workerAddr, ambiguousShmIds, param.requestContext);
    }
    return rc;
}

Status TransportLayer::TryCreate(const HostPort &workerAddr, const std::string &objectKey, uint64_t dataSize,
                                 const TransportCreateParam &param, TransportHint hint,
                                 std::shared_ptr<ObjectBuffer> &buffer,
                                 std::unordered_set<ShmKey> &ambiguousShmIds)
{
    LocalUbSenderOperation operation;
    RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(hint));
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, operation));
    Status rc = transporter->Create(workerAddr, objectKey, dataSize, param, buffer);
    if (IsAmbiguousCreateFailure(rc)) {
        try {
            RecordAllocationIds(param, ambiguousShmIds);
        } catch (const std::bad_alloc &error) {
            LOG(WARNING) << "Failed to track ambiguous Create allocation: " << error.what();
        }
    } else if (rc.IsOk() && hint != TransportHint::TCP_ONLY) {
        ForgetAllocationIds(param, ambiguousShmIds);
    }
    return rc;
}

bool TransportLayer::RebuildPlaneOnSetFailure(const Status &rc, const HostPort &workerAddr,
                                              const std::shared_ptr<IDataTransporter> &stale)
{
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString() << " after Set failed: " << rc;
        // Stale-guarded: a concurrent writer may already have rebuilt the plane after this request
        // failed, and dropping that instance would undo its recovery.
        manager_->ResetStaleUbDataPlane(workerAddr, stale);
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
bool IsUbWriteAllocation(const ObjectBuffer &buffer)
{
    return ObjectBufferInternal::GetInfo(buffer).ubUrmaDataInfo != nullptr;
}
}  // namespace

void TransportLayer::LogSetResult(const HostPort &workerAddr, TransportHint hint, const Status &rc,
                                  std::chrono::steady_clock::time_point start)
{
    LOG(INFO) << "[TransportSet] worker=" << workerAddr.ToString()
        << " transport=" << TransportHintName(hint) << " rc=" << rc.GetCode()
        << " latency_us=" << std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
}

Status TransportLayer::Set(ObjectBuffer &buffer, const TransportSetParam &param)
{
    TransportSetResult result;
    return Set(buffer, param, result);
}

Status TransportLayer::Set(ObjectBuffer &buffer, const TransportSetParam &param, TransportSetResult &result)
{
    result = TransportSetResult{};
    RETURN_IF_NOT_OK(CheckLocalNodeAdmission());
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const HostPort workerAddr = ObjectBufferInternal::GetInfo(buffer).workerAddr;
    TransportHint hint = advisor_->GetTransportHint(workerAddr);
    // Create may have fallen back from same-host SHM to UB; publish that allocation through UB too.
    if (hint == TransportHint::SHM_CANDIDATE && IsUbWriteAllocation(buffer)) {
        hint = TransportHint::UB_CANDIDATE;
    }
    const auto setStart = std::chrono::steady_clock::now();
    LocalUbSenderOperation operation;
    RETURN_IF_NOT_OK(WithRpcDiag(CheckLocalUbSenderAdmission(hint), "Set", workerAddr));
    std::shared_ptr<IDataTransporter> transporter;
    Status buildRc = manager_->GetOrCreate(workerAddr, hint, transporter);
    if (buildRc.IsError() && hint == TransportHint::UB_CANDIDATE) {
        // The UB data plane cannot be built right now (breaker cooling down, or the build failed):
        // degrade this write to TCP instead of failing it, matching the read path's UB->TCP fallback.
        // Each write re-asks the advisor, so a later write retries UB once the cooldown elapses.
        LOG_EVERY_N(WARNING, TRANSPORT_DIAG_LOG_RATE)
            << "UB data plane unavailable for worker " << workerAddr.ToString()
            << ", degrading this write to TCP: " << buildRc;
        hint = TransportHint::TCP_ONLY;
        buildRc = manager_->GetOrCreate(workerAddr, hint, transporter);
    }
    RETURN_IF_NOT_OK(WithRpcDiag(buildRc, "Set", workerAddr));
    RETURN_IF_NOT_OK(WithRpcDiag(AcquireLocalUbSenderAdmission(hint, operation), "Set", workerAddr));
    auto &mutableBufferInfo = ObjectBufferInternal::GetMutableInfo(buffer);
    mutableBufferInfo.ubFailureReportRc = Status::OK();
    mutableBufferInfo.ubProviderStatus.reset();
    mutableBufferInfo.ubCqeStatus.reset();
    PrepareLocalUbLateCompletion(mutableBufferInfo, transporter->Kind());
    Status rc = transporter->Set(buffer, param, &result);
    return WithRpcDiag(FinalizeSetPublish(workerAddr, buffer, param, hint, transporter, rc, result, setStart), "Set",
                       workerAddr);
}

Status TransportLayer::FinalizeSetPublish(const HostPort &workerAddr, ObjectBuffer &buffer,
                                          const TransportSetParam &param, TransportHint hint,
                                          std::shared_ptr<IDataTransporter> &transporter, const Status &publishRc,
                                          TransportSetResult &result,
                                          std::chrono::steady_clock::time_point setStart)
{
    const auto &ubFailureReport = ObjectBufferInternal::GetInfo(buffer).ubFailureReportRc;
    const auto &bufferInfo = ObjectBufferInternal::GetInfo(buffer);
    // Routed SHM zero-copy buffers carry a send-side owner (ManagesWorkerReference) that releases the
    // worker reference on buffer destruction, so skip ScheduleRelease for them to avoid a double decrement.
    const bool ownerManagesRef = bufferInfo.receiveBufferOwner != nullptr
                                 && bufferInfo.receiveBufferOwner->ManagesWorkerReference();
    const bool localPortFailure = ReportLocalUbSenderFailure(
        { workerAddr, transporter->Kind(), ubFailureReport, bufferInfo.ubProviderStatus, bufferInfo.ubCqeStatus });
    result.writeTargetQuarantined = ReportWriteTargetUbFailure(
        { workerAddr, transporter->Kind(), ubFailureReport, bufferInfo.ubProviderStatus, bufferInfo.ubCqeStatus });
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
    if (localPortFailure) {
        releaseRef(TransportHint::TCP_ONLY);
        return rc.GetCode() == K_URMA_NEED_CONNECT ? ubFailureReport : rc;
    }
    if (result.writeTargetQuarantined
        && (!result.publishAttempted || result.publishDefinitelyNotSent)) {
        releaseRef(TransportHint::TCP_ONLY);
        return rc;
    }
    // A URMA write failure that fell back to TCP still reports OK from Set, so the caller never sees
    // K_URMA_NEED_CONNECT and the breaker would stay OPEN until a payload the TCP limiter rejects.
    // Rebuild the UB plane here as well, with the same stale guard as the error path below.
    if (rc.IsOk() && ubFailureReport.GetCode() == K_URMA_NEED_CONNECT) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString()
                     << " after UB write fell back to TCP: " << ubFailureReport;
        manager_->ResetStaleUbDataPlane(workerAddr, transporter);
        INJECT_POINT_NO_RETURN("TransportLayer.RebuildUbAfterTcpFallback", [] {});
    }
    if (!RebuildPlaneOnSetFailure(rc, workerAddr, transporter)) {
        releaseRef();
        return rc;
    }
    rc = RetrySet(workerAddr, buffer, param, hint, result);
    if (rc.IsError() && firstPublishRc.has_value()) {
        rc = *firstPublishRc;
    }
    releaseRef();
    LogSetResult(workerAddr, hint, rc, setStart);
    return rc;
}

Status TransportLayer::RetrySet(const HostPort &workerAddr, ObjectBuffer &buffer, const TransportSetParam &param,
                                TransportHint hint, TransportSetResult &result)
{
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    LocalUbSenderOperation operation;
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, operation));
    TransportSetParam retryParam = param;
    retryParam.isRetry = true;
    auto &mutableBufferInfo = ObjectBufferInternal::GetMutableInfo(buffer);
    mutableBufferInfo.ubFailureReportRc = Status::OK();
    mutableBufferInfo.ubProviderStatus.reset();
    mutableBufferInfo.ubCqeStatus.reset();
    PrepareLocalUbLateCompletion(mutableBufferInfo, transporter->Kind());
    result = TransportSetResult{};
    Status rc = transporter->Set(buffer, retryParam, &result);
    const auto &bufferInfo = ObjectBufferInternal::GetInfo(buffer);
    (void)ReportLocalUbSenderFailure(
        { workerAddr, transporter->Kind(), bufferInfo.ubFailureReportRc, bufferInfo.ubProviderStatus,
          bufferInfo.ubCqeStatus });
    result.writeTargetQuarantined = ReportWriteTargetUbFailure(
        { workerAddr, transporter->Kind(), bufferInfo.ubFailureReportRc, bufferInfo.ubProviderStatus,
          bufferInfo.ubCqeStatus });
    if (rc.IsError()) {
        LOG(WARNING) << "Set still failed after rebuilding transport for worker " << workerAddr.ToString() << ": "
                     << rc;
    }
    return rc;
}

Status TransportLayer::MCreate(const HostPort &workerAddr, const std::vector<std::string> &objectKeys,
                               const std::vector<uint64_t> &dataSizes, TransportCreateParam param,
                               std::vector<std::shared_ptr<ObjectBuffer>> &buffers)
{
    RETURN_IF_NOT_OK(CheckLocalNodeAdmission());
    RETURN_IF_NOT_OK(ValidateMultiCreateRequest(objectKeys, dataSizes, param));
    INJECT_POINT("TransportLayer.MCreate.beforeTransport");
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const auto hint = advisor_->GetTransportHint(workerAddr);
    param.allocationId.clear();
    param.allocationIds.clear();
    if (hint != TransportHint::TCP_ONLY) {
        RETURN_IF_NOT_OK(GenerateAllocationIds(objectKeys.size(), param));
    }
    std::unordered_set<ShmKey> ambiguousShmIds;
    Status rc = TryMCreate(workerAddr, objectKeys, dataSizes, param, hint, buffers, ambiguousShmIds);
    if (rc.GetCode() == K_TRY_AGAIN) {
        int64_t backoffMs = MCREATE_RESERVATION_RETRY_BACKOFF_MS;
        RETURN_IF_NOT_OK(DeadlineRetry().Backoff(backoffMs));
        rc = TryMCreate(workerAddr, objectKeys, dataSizes, param, hint, buffers, ambiguousShmIds);
    }
    if (IsNonRetryableRpcError(rc)) {
        LOG(WARNING) << "Tear down dead RPC peer for worker " << workerAddr.ToString()
                     << " after MCreate failed without retry: " << rc;
        manager_->Teardown(workerAddr);
        ScheduleAmbiguousCreateCleanup(workerAddr, ambiguousShmIds, param.requestContext);
        return rc;
    }
    if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        const Status ambiguousRc = rc;
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after ambiguous MCreate failure, retrying once: " << rc;
        manager_->Teardown(workerAddr);
        rc = TryMCreate(workerAddr, objectKeys, dataSizes, param, hint, buffers, ambiguousShmIds);
        if (IsAllocationReplayConflict(rc)) {
            rc = ambiguousRc;
        }
        if (rc.GetCode() != K_NOT_SUPPORTED) {
            ScheduleAmbiguousCreateCleanup(workerAddr, ambiguousShmIds, param.requestContext);
            return rc;
        }
    }
    if (rc.GetCode() == K_NOT_SUPPORTED) {
        rc = TryMCreateFallbacks(workerAddr, objectKeys, dataSizes, param, hint, buffers, ambiguousShmIds);
    }
    ScheduleAmbiguousCreateCleanup(workerAddr, ambiguousShmIds, param.requestContext);
    return rc;
}

Status TransportLayer::TryMCreate(const HostPort &workerAddr, const std::vector<std::string> &objectKeys,
                                  const std::vector<uint64_t> &dataSizes, const TransportCreateParam &param,
                                  TransportHint hint, std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                  std::unordered_set<ShmKey> &ambiguousShmIds)
{
    LocalUbSenderOperation operation;
    RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(hint));
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, operation));
    Status rc = transporter->MCreate(workerAddr, objectKeys, dataSizes, param, buffers);
    if (IsAmbiguousCreateFailure(rc)) {
        try {
            RecordAllocationIds(param, ambiguousShmIds);
        } catch (const std::bad_alloc &error) {
            LOG(WARNING) << "Failed to track ambiguous MCreate allocations: " << error.what();
        }
    } else if (rc.IsOk() && hint != TransportHint::TCP_ONLY) {
        ForgetAllocationIds(param, ambiguousShmIds);
    }
    return rc;
}

Status TransportLayer::TryMCreateFallbacks(const HostPort &workerAddr,
                                           const std::vector<std::string> &objectKeys,
                                           const std::vector<uint64_t> &dataSizes,
                                           const TransportCreateParam &param, TransportHint hint,
                                           std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                           std::unordered_set<ShmKey> &ambiguousShmIds)
{
    Status rc(K_NOT_SUPPORTED, "No MCreate fallback transport is available");
    for (const auto &fallbackHint : advisor_->GetFallbackHints(hint)) {
        SLOW_LOG(WARNING) << "MCreate SHM unavailable on worker " << workerAddr.ToString() << ", fall back to "
            << TransportHintName(fallbackHint);
        rc = TryMCreate(workerAddr, objectKeys, dataSizes, param, fallbackHint, buffers, ambiguousShmIds);
        if (rc.IsOk()) {
            break;
        }
    }
    return rc;
}

Status TransportLayer::MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers, const TransportSetParam &param,
                            TransportMSetResult &result)
{
    result.Clear();
    RETURN_IF_NOT_OK(CheckLocalNodeAdmission());
    RETURN_IF_NOT_OK(ValidateMSetRequest(buffers, param));
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const HostPort workerAddr = ObjectBufferInternal::GetInfo(*buffers.front()).workerAddr;
    TransportHint hint = advisor_->GetTransportHint(workerAddr);
    if (hint == TransportHint::SHM_CANDIDATE
        && std::all_of(buffers.begin(), buffers.end(),
                       [](const auto &buffer) { return IsUbWriteAllocation(*buffer); })) {
        hint = TransportHint::UB_CANDIDATE;
    }
    LocalUbSenderOperation operation;
    RETURN_IF_NOT_OK(CheckLocalUbSenderAdmission(hint));
    std::shared_ptr<IDataTransporter> transporter;
    Status buildRc = manager_->GetOrCreate(workerAddr, hint, transporter);
    if (buildRc.IsError() && hint == TransportHint::UB_CANDIDATE) {
        // Same UB->TCP degradation as the single-buffer path: a UB plane that cannot be built right
        // now (cooldown, failed build) must not fail the batch.
        SLOW_LOG(WARNING) << "UB data plane unavailable for worker " << workerAddr.ToString()
            << ", degrading this MSet to TCP: " << buildRc;
        hint = TransportHint::TCP_ONLY;
        buildRc = manager_->GetOrCreate(workerAddr, hint, transporter);
    }
    RETURN_IF_NOT_OK(buildRc);
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, operation));
    for (const auto &buffer : buffers) {
        PrepareLocalUbLateCompletion(ObjectBufferInternal::GetMutableInfo(*buffer), transporter->Kind());
    }
    Status rc = transporter->MSet(buffers, param, result);
    const auto &ubFailureReport = result.ubFailureReportRc;
    const bool localPortFailure = ReportLocalUbSenderFailure(
        { workerAddr, transporter->Kind(), ubFailureReport, result.ubProviderStatus, result.ubCqeStatus });
    result.writeTargetQuarantined = ReportWriteTargetUbFailure(
        { workerAddr, transporter->Kind(), ubFailureReport, result.ubProviderStatus, result.ubCqeStatus });
    if (localPortFailure) {
        ScheduleMSetReleases(buffers, param.requestContext, result, TransportHint::TCP_ONLY);
        return rc.GetCode() == K_URMA_NEED_CONNECT ? ubFailureReport : rc;
    }
    if (result.writeTargetQuarantined
        && (!result.publishAttempted || result.publishDefinitelyNotSent)) {
        ScheduleMSetReleases(buffers, param.requestContext, result, TransportHint::TCP_ONLY);
        return rc;
    }
    // Mirrors FinalizeSetPublish: a UB write failure that fell back to TCP returns OK, so the
    // breaker needs this explicit, stale-guarded rebuild trigger.
    if (rc.IsOk() && ubFailureReport.GetCode() == K_URMA_NEED_CONNECT) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString()
                     << " after MSet UB write fell back to TCP: " << ubFailureReport;
        manager_->ResetStaleUbDataPlane(workerAddr, transporter);
        INJECT_POINT_NO_RETURN("TransportLayer.RebuildUbAfterTcpFallback", [] {});
    }
    return RetryOrReplayMSet(workerAddr, buffers, param, hint, result, rc, transporter);
}

Status TransportLayer::RetryOrReplayMSet(const HostPort &workerAddr,
                                         const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                         const TransportSetParam &param, TransportHint hint,
                                         TransportMSetResult &result, const Status &rc,
                                         const std::shared_ptr<IDataTransporter> &stale)
{
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
        // Stale-guarded: a concurrent writer may already have rebuilt the plane after this batch failed.
        manager_->ResetStaleUbDataPlane(workerAddr, stale);
    } else {
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after MSet failed before publish: " << rc;
        manager_->Teardown(workerAddr);
    }
    Status retryRc = RetryMSet(workerAddr, buffers, param, hint, result);
    ScheduleMSetReleases(buffers, param.requestContext, result);
    return retryRc;
}

Status TransportLayer::RetryMSet(const HostPort &workerAddr, const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                 const TransportSetParam &param, TransportHint hint, TransportMSetResult &result)
{
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    LocalUbSenderOperation operation;
    RETURN_IF_NOT_OK(AcquireLocalUbSenderAdmission(hint, operation));
    result.Clear();
    for (const auto &buffer : buffers) {
        PrepareLocalUbLateCompletion(ObjectBufferInternal::GetMutableInfo(*buffer), transporter->Kind());
    }
    Status rc = transporter->MSet(buffers, param, result);
    (void)ReportLocalUbSenderFailure(
        { workerAddr, transporter->Kind(), result.ubFailureReportRc, result.ubProviderStatus, result.ubCqeStatus });
    result.writeTargetQuarantined = ReportWriteTargetUbFailure(
        { workerAddr, transporter->Kind(), result.ubFailureReportRc, result.ubProviderStatus, result.ubCqeStatus });
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

void TransportLayer::ScheduleAmbiguousCreateCleanup(const HostPort &workerAddr,
                                                    const std::unordered_set<ShmKey> &shmIds,
                                                    const TransportRequestContext &context)
{
    if (shmIds.empty()) {
        return;
    }
    try {
        auto manager = manager_;
        std::vector<ShmKey> allocationIds(shmIds.begin(), shmIds.end());
        const auto allocationCount = allocationIds.size();
        const char *dropReason = nullptr;
        {
            // Serialize only pool admission with Shutdown; task execution never holds this lock.
            std::lock_guard<bthread::Mutex> shutdownLock(shutdownMutex_);
            if (ambiguousCreateCleanupPool_ == nullptr) {
                dropReason = "cleanup pool is unavailable";
            } else if (!ambiguousCreateCleanupPool_->ExecuteNoWait(
                [manager, workerAddr, allocationIds = std::move(allocationIds), context]() {
                    CleanupAmbiguousAllocations(manager, workerAddr, allocationIds, context);
                })) {
                dropReason = "cleanup pool is full";
            }
        }
        if (dropReason != nullptr) {
            METRIC_INC(metrics::KvMetricId::CLIENT_AMBIGUOUS_CREATE_CLEANUP_DROPPED_TOTAL);
            SLOW_LOG(WARNING)
                << "Drop ambiguous Create cleanup because " << dropReason << ", worker=" << workerAddr.ToString()
                << ", clientId=" << context.clientId << ", allocationCount=" << allocationCount
                << "; worker hard reclaim applies only when these allocations were marked reclaimable";
        }
    } catch (const std::exception &error) {
        METRIC_INC(metrics::KvMetricId::CLIENT_AMBIGUOUS_CREATE_CLEANUP_DROPPED_TOTAL);
        SLOW_LOG(WARNING)
            << "Drop ambiguous Create cleanup because scheduling failed, worker=" << workerAddr.ToString()
            << ", clientId=" << context.clientId << ", error=" << error.what()
            << "; worker hard reclaim applies only when these allocations were marked reclaimable";
    }
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
    // Copy the SHM candidate list before the snapshot is moved below. SetShmCandidateWorkers takes the
    // advisor's RWLock write lock; keep it out of the reconcileMutex_ critical section so the
    // reconcile thread (which touches entries_ under reconcileMutex_) never blocks on the advisor
    // write lock.
    std::vector<HostPort> shmCandidateAddrs = snapshot.shmCandidateAddrs;
    std::unordered_set<HostPort> probeDestinations;
    probeDestinations.reserve(snapshot.remoteTransportAddrs.size() + snapshot.workerIncarnations.size());
    probeDestinations.insert(snapshot.remoteTransportAddrs.begin(), snapshot.remoteTransportAddrs.end());
    for (const auto &[worker, incarnation] : snapshot.workerIncarnations) {
        (void)incarnation;
        probeDestinations.emplace(worker);
    }
    {
        std::lock_guard<bthread::Mutex> lock(*reconcileMutex_);
        CHECK_FAIL_RETURN_STATUS(reconcileStarted_, K_NOT_READY, "Transport reconcile thread is not initialized");
        CHECK_FAIL_RETURN_STATUS(!reconcileStopping_, K_SHUTTING_DOWN, "TransportLayer is shutting down");
        // Publish the live-worker snapshot to the manager FIRST. The advisor's same-host set is
        // updated AFTER releasing reconcileMutex_ below; updating the advisor first would open a
        // window where GetTransportHint returns SHM_CANDIDATE for a worker the manager does not yet
        // know is live, so GetOrCreate returns K_NOT_FOUND and the release-retry path can leak a
        // shm ref. With manager-first, the advisor only marks as same-host workers the manager can
        // already hand out a transporter for.
        RETURN_IF_NOT_OK(manager_->UpdateWorkerSnapshot(snapshot));
        localUbSenderState_->ReconcileProbeDestinations(probeDestinations);
        pendingSnapshot_ = std::move(snapshot);
        reconcileCv_->notify_one();
    }
    if (advisor_ != nullptr) {
        advisor_->SetShmCandidateWorkers(shmCandidateAddrs);
    }
    return Status::OK();
}

void TransportLayer::RecordRoutingRefresh(uint64_t ringVersion)
{
    if (manager_ != nullptr) {
        manager_->RecordRoutingRefresh(ringVersion);
    }
}

bool TransportLayer::WaitForSnapshotOrStop(std::unique_lock<bthread::Mutex> &lock)
{
    // bthread::ConditionVariable has no predicate overloads and its wait_until takes a CLOCK_REALTIME
    // timespec, not a chrono steady_clock time_point, so emulate master's wait_until(deadline, pred)
    // and wait(pred) by hand. A probe deadline expiring must exit this wait so ReconcileLoop can run
    // the recovery probe; therefore break after any wait returns (deadline elapsed or notified).
    while (!reconcileStopping_ && !pendingSnapshot_.has_value()) {
        auto probeDeadline = GetProviderUbProbeDeadline();
        auto writeTargetDeadline = GetWriteTargetUbProbeDeadline();
        if (!probeDeadline.has_value()
            || (writeTargetDeadline.has_value() && *writeTargetDeadline < *probeDeadline)) {
            probeDeadline = writeTargetDeadline;
        }
        auto portHealthDeadline = manager_->GetUbPortHealthQueryDeadline();
        if (!probeDeadline.has_value()
            || (portHealthDeadline.has_value() && *portHealthDeadline < *probeDeadline)) {
            probeDeadline = portHealthDeadline;
        }
        INJECT_POINT_NO_RETURN("TransportLayer.WaitForSnapshotOrStop.afterDeadlineCheck");
        if (probeDeadline.has_value()) {
            const auto now = std::chrono::steady_clock::now();
            if (*probeDeadline > now) {
                const auto waitNs = std::chrono::duration_cast<std::chrono::nanoseconds>(*probeDeadline - now);
                (void)reconcileCv_->wait_until(lock, butil::nanoseconds_from_now(waitNs.count()));
            }
            break;
        }
        // No probe deadline: wait for a wake (stop / snapshot / new deadline set by a failure report),
        // then re-check at the top of the loop.
        reconcileCv_->wait(lock);
    }
    return !reconcileStopping_;
}

void TransportLayer::ReconcileLoop()
{
    bool keepRunning = true;
    while (keepRunning) {
        std::optional<WorkerSnapshot> snapshot;
        {
            std::unique_lock<bthread::Mutex> lock(*reconcileMutex_);
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
        TryRecoverProviderUbSource();
        TryRecoverWriteTargetUbSource();
        manager_->RunDueUbPortHealthVerification();
    }
}

void TransportLayer::Shutdown()
{
    std::lock_guard<bthread::Mutex> shutdownLock(shutdownMutex_);
    localUbSenderState_->CloseAdmission();
    {
        std::unique_lock<bthread::Mutex> lock(localUbSenderState_->inFlightDrainMutex);
        while (localUbSenderState_->InFlightOperationCount() != 0) {
            localUbSenderState_->inFlightCv.wait(lock);
        }
    }
    Thread reconcileThread;
    {
        std::lock_guard<bthread::Mutex> lock(*reconcileMutex_);
        reconcileStopping_ = true;
        pendingSnapshot_.reset();
        reconcileCv_->notify_all();
        if (reconcileStarted_) {
            reconcileThread = std::move(reconcileThread_);
            reconcileStarted_ = false;
        }
    }
    if (reconcileThread.joinable()) {
        reconcileThread.join();
    }
    // Drain cleanup and regular DecreaseReference tasks before closing their endpoint connections.
    ambiguousCreateCleanupPool_.reset();
    releasePool_.reset();
    objectRead_.reset();
    // UrmaManager owns the context monitor and drains it before context teardown.
    if (manager_ != nullptr) {
        manager_->Shutdown();
    }
}

}  // namespace client
}  // namespace datasystem
