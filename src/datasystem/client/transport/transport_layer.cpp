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

#include <chrono>
#include <cstdlib>
#include <exception>
#include <thread>
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
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uri.h"

namespace datasystem {
namespace client {
namespace {
uint64_t GetConfiguredUbInlineBufferSize()
{
    const char *value = std::getenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES");
    if (value == nullptr || *value == '\0') {
        return 0;
    }
    for (const char *cursor = value; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            return 0;
        }
    }
    uint64_t size = 0;
    return Uri::StrToUint64(value, size) ? size : 0;
}
}  // namespace

TransportLayer::TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                               uint64_t fastTransportMemSize, BrpcChannelConfig channelConfig,
                               std::shared_ptr<ThreadPool> releasePool)
    : advisor_(std::make_shared<TransportAdvisor>()), releasePool_(std::move(releasePool))
{
    auto ubBufferProvider = CreateDefaultUbReceiveBufferProvider();
    manager_ = std::make_shared<DataPlaneManager>(std::move(signature), fastTransportMemSize,
                                                  std::move(channelConfig), ubBufferProvider);
    auto retry = std::make_shared<DeadlineRetry>();
    auto metadata = std::make_shared<ObjectMetadataClient>(manager_, retry, advisor_, std::move(ubBufferProvider),
                                                           GetConfiguredUbInlineBufferSize());
    auto executor = std::make_shared<DataPlaneExecutor>(manager_, advisor_);
    auto replicas = std::make_shared<ReplicaReader>(std::move(executor), std::move(retry), taskPool);
    objectRead_ = std::make_unique<ObjectReadFlow>(std::move(metadata), std::move(replicas), std::move(taskPool));
}

TransportLayer::TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                               std::shared_ptr<TransportAdvisor> advisor)
    : manager_(std::move(dataPlaneManager)), advisor_(std::move(advisor))
{
}

TransportLayer::~TransportLayer()
{
    Shutdown();
}

Status TransportLayer::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_IF_NOT_OK(manager_->Init());
    std::lock_guard<std::mutex> lock(reconcileMutex_);
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

Status TransportLayer::Get(const ObjectReadRequest &input, ObjectReadResult &output)
{
    RETURN_RUNTIME_ERROR_IF_NULL(objectRead_);
    return objectRead_->Run(input, output);
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
    if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        LOG(WARNING) << "Rebuild RPC client for worker " << workerAddr.ToString() << " after Exist failed: " << rc;
        manager_->Teardown(workerAddr);
        rc = runExist(response);
        if (rc.IsError()) {
            LOG(WARNING) << "Exist still failed after rebuilding RPC client for worker " << workerAddr.ToString()
                         << ": " << rc;
            return rc;
        }
    } else if (rc.IsError()) {
        return rc;
    }

    if (!response.redirect_extra().empty()) {
        return Status(K_NOT_OWNER, "Exist keys redirected to new owners").WithExtra(response.redirect_extra());
    }

    if (static_cast<size_t>(response.exists_size()) != input.objectKeys.size()) {
        return Status(K_RUNTIME_ERROR,
                      FormatString("Exist response size mismatch: expected %zu keys, got %d results",
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
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    Status rc = transporter->Create(workerAddr, objectKey, dataSize, param, buffer);
    if (rc.GetCode() != K_RPC_UNAVAILABLE) {
        return rc;
    }
    LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                 << " after Create failed: " << rc;
    manager_->Teardown(workerAddr);
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    rc = transporter->Create(workerAddr, objectKey, dataSize, param, buffer);
    if (rc.IsError()) {
        LOG(WARNING) << "Create still failed after rebuilding transport for worker " << workerAddr.ToString()
                     << ": " << rc;
    }
    return rc;
}

Status TransportLayer::Set(ObjectBuffer &buffer, const TransportSetParam &param)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const HostPort workerAddr = ObjectBufferInternal::GetInfo(buffer).workerAddr;
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    TransportSetParam retryParam = param;
    Status rc = transporter->Set(buffer, retryParam);
    if (rc.GetCode() == K_URMA_NEED_CONNECT) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString()
                     << " after Set failed: " << rc;
        manager_->ResetDataPlane(workerAddr);
    } else if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after Set failed: " << rc;
        manager_->Teardown(workerAddr);
    } else {
        const auto &info = ObjectBufferInternal::GetInfo(buffer);
        ScheduleRelease(workerAddr, info.shmId, param.requestContext);
        return rc;
    }
    Status rebuildRc = manager_->GetOrCreate(workerAddr, hint, transporter);
    if (rebuildRc.IsOk()) {
        retryParam.isRetry = true;
        rc = transporter->Set(buffer, retryParam);
        if (rc.IsError()) {
            LOG(WARNING) << "Set still failed after rebuilding transport for worker " << workerAddr.ToString()
                         << ": " << rc;
        }
    } else {
        rc = rebuildRc;
    }
    const auto &info = ObjectBufferInternal::GetInfo(buffer);
    ScheduleRelease(workerAddr, info.shmId, param.requestContext);
    return rc;
}

Status TransportLayer::MCreate(const HostPort &workerAddr, const std::vector<std::string> &objectKeys,
                               const std::vector<uint64_t> &dataSizes, const TransportCreateParam &param,
                               std::vector<std::shared_ptr<ObjectBuffer>> &buffers)
{
    RETURN_IF_NOT_OK(ValidateMultiCreateRequest(objectKeys, dataSizes, param));
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    Status rc = transporter->MCreate(workerAddr, objectKeys, dataSizes, param, buffers);
    if (rc.GetCode() == K_RPC_UNAVAILABLE) {
        // MultiCreate has no idempotency marker. The worker may have allocated memory even when the response is lost.
        LOG(WARNING) << "Tear down RPC and data plane for worker " << workerAddr.ToString()
                     << " after ambiguous MCreate failure without replay: " << rc;
        manager_->Teardown(workerAddr);
    }
    return rc;
}

Status TransportLayer::MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                            const TransportSetParam &param, TransportMSetResult &result)
{
    result.Clear();
    RETURN_IF_NOT_OK(ValidateMSetRequest(buffers, param));
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    RETURN_RUNTIME_ERROR_IF_NULL(advisor_);
    const HostPort workerAddr = ObjectBufferInternal::GetInfo(*buffers.front()).workerAddr;
    const TransportHint hint = advisor_->GetTransportHint(workerAddr);
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(manager_->GetOrCreate(workerAddr, hint, transporter));
    Status rc = transporter->MSet(buffers, param, result);
    const bool retryUbWrite = rc.GetCode() == K_URMA_NEED_CONNECT;
    const bool retryUnsentPublish = rc.GetCode() == K_RPC_UNAVAILABLE && !result.publishAttempted;
    if (!retryUbWrite && !retryUnsentPublish) {
        if (rc.GetCode() == K_RPC_UNAVAILABLE) {
            LOG(WARNING) << "Tear down RPC and data plane for worker " << workerAddr.ToString()
                         << " after ambiguous MSet failure without replay: " << rc;
            manager_->Teardown(workerAddr);
        }
        ScheduleReleases(buffers, param.requestContext);
        return rc;
    }
    if (retryUbWrite) {
        LOG(WARNING) << "Rebuild UB data plane for worker " << workerAddr.ToString()
                     << " after MSet failed: " << rc;
        manager_->ResetDataPlane(workerAddr);
    } else {
        LOG(WARNING) << "Rebuild RPC and data plane for worker " << workerAddr.ToString()
                     << " after MSet failed before publish: " << rc;
        manager_->Teardown(workerAddr);
    }
    Status rebuildRc = manager_->GetOrCreate(workerAddr, hint, transporter);
    if (rebuildRc.IsOk()) {
        result.Clear();
        rc = transporter->MSet(buffers, param, result);
        if (rc.IsError()) {
            LOG(WARNING) << "MSet still failed after rebuilding transport for worker " << workerAddr.ToString()
                         << ": " << rc;
        }
    } else {
        rc = rebuildRc;
    }
    ScheduleReleases(buffers, param.requestContext);
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
                                     const TransportRequestContext &context)
{
    if (shmId.Empty()) {
        return;
    }
    if (releasePool_ == nullptr) {
        std::shared_ptr<IDataTransporter> transporter;
        Status rc = manager_->GetOrCreate(workerAddr, advisor_->GetTransportHint(workerAddr), transporter);
        if (rc.IsOk()) {
            rc = InvokeReleaseWithRetry(workerAddr, shmId, context, transporter);
        }
        LOG_IF_ERROR(rc, "Release routed Set allocation failed");
        return;
    }
    auto manager = manager_;
    auto advisor = advisor_;
    releasePool_->Execute([manager, advisor, workerAddr, shmId, context]() {
        std::shared_ptr<IDataTransporter> transporter;
        Status rc = manager->GetOrCreate(workerAddr, advisor->GetTransportHint(workerAddr), transporter);
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
        LOG(WARNING) << "InvokeDecreaseReference attempt " << (attempt + 1) << "/" << kMaxAttempts
                     << " failed for worker " << workerAddr.ToString() << ", shmId=" << shmId.ToString()
                     << ": " << rc.ToString();
    }
    return rc;
}

void TransportLayer::ScheduleReleases(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                      const TransportRequestContext &context)
{
    for (const auto &buffer : buffers) {
        const auto &info = ObjectBufferInternal::GetInfo(*buffer);
        ScheduleRelease(info.workerAddr, info.shmId, context);
    }
}

Status TransportLayer::ApplyWorkerSnapshot(WorkerSnapshot snapshot)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    // Copy the same-host list before the snapshot is moved below. SetSameHostWorkers takes the
    // advisor's shared_mutex write lock; keep it out of the reconcileMutex_ critical section so the
    // reconcile thread (which touches entries_ under reconcileMutex_) never blocks on the advisor
    // write lock.
    std::vector<HostPort> sameHostAddrs = snapshot.sameHostAddrs;
    {
        std::lock_guard<std::mutex> lock(reconcileMutex_);
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

void TransportLayer::ReconcileLoop()
{
    bool keepRunning = true;
    while (keepRunning) {
        WorkerSnapshot snapshot;
        {
            std::unique_lock<std::mutex> lock(reconcileMutex_);
            reconcileCv_.wait(lock, [this] { return reconcileStopping_ || pendingSnapshot_.has_value(); });
            keepRunning = !reconcileStopping_;
            if (keepRunning) {
                snapshot = std::move(*pendingSnapshot_);
                pendingSnapshot_.reset();
            }
        }
        if (keepRunning) {
            manager_->ReconcileWithSnapshot(snapshot);
        }
    }
}

void TransportLayer::Shutdown()
{
    std::lock_guard<std::mutex> shutdownLock(shutdownMutex_);
    Thread reconcileThread;
    {
        std::lock_guard<std::mutex> lock(reconcileMutex_);
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
