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

#include "datasystem/utils/status.h"

#include <atomic>
#include <utility>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rdma/fast_transport_base.h"

#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#ifdef USE_RDMA
#include "datasystem/common/rdma/ucp_manager.h"
#endif
#ifdef USE_NPU
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#endif

namespace datasystem {
Status RegisterFastTransportMemory(void *segAddress, const uint64_t &segSize)
{
    (void)segAddress;
    (void)segSize;
#ifdef USE_URMA
    if (IsUrmaEnabled() && IsRegisterWholeArenaEnabled() && segAddress != nullptr) {
        LOG(INFO) << "Doing URMA memory registration of size " << segSize;
        RETURN_IF_NOT_OK(UrmaManager::Instance().RegisterSegment(reinterpret_cast<uint64_t>(segAddress), segSize));
    }
#endif

#ifdef USE_RDMA
    if (IsUcpEnabled() && IsRegisterWholeArenaEnabled() && segAddress != nullptr) {
        LOG(INFO) << "Doing UCP memory registration of size " << segSize;
        RETURN_IF_NOT_OK(UcpManager::Instance().RegisterSegment(reinterpret_cast<uint64_t>(segAddress), segSize));
    }
#endif
    return Status::OK();
}

Status RegisterHostMemory(void *segAddress, const uint64_t &segSize)
{
    (void)segAddress;
    (void)segSize;
#ifdef USE_NPU
    if (IsRemoteH2DEnabled() && FLAGS_urma_register_whole_arena && segAddress != nullptr) {
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().RegisterHostMemory(segAddress, segSize));
    }
#endif
    return Status::OK();
}

bool IsFastTransportEnabled()
{
    if (IsUrmaEnabled()) {
        return true;
    }

    if (IsUcpEnabled()) {
        return true;
    }

    return false;
}

bool IsRemoteH2DEnabled()
{
#ifdef USE_NPU
    return RemoteH2DManager::IsRemoteH2DEnabled();
#else
    return false;
#endif
}

namespace {
std::atomic<bool> &ClientUrmaRuntimeRequested()
{
    static std::atomic<bool> requested{ false };
    return requested;
}

std::atomic<bool> &ClientUrmaRuntimeReady()
{
    static std::atomic<bool> ready{ false };
    return ready;
}
}  // namespace

bool IsUrmaEnabled()
{
#ifdef USE_URMA
    return FLAGS_enable_urma || ClientUrmaRuntimeReady().load(std::memory_order_acquire);
#else
    return false;
#endif
}

void RequestClientUrmaRuntime()
{
    ClientUrmaRuntimeRequested().store(true, std::memory_order_release);
}

bool ShouldRequestClientUrmaRuntime(bool workerUbEnabled, bool clientMayAccessNonBoundWorker, bool endpointUsesUb)
{
    // Endpoint transport and process capability are deliberately separate: SHM can serve the bound worker while
    // UB is still required for another node. Conversely, client routing options must not activate UB when the
    // worker does not advertise it, because activation performs device discovery and memory registration.
    return workerUbEnabled && (clientMayAccessNonBoundWorker || endpointUsesUb);
}

bool ClientMayAccessNonBoundWorker(bool enableLocalCache, bool enableCrossNodeConnection)
{
    return !enableLocalCache || enableCrossNodeConnection;
}

bool IsUrmaRuntimeConfigured()
{
#ifdef USE_URMA
    return FLAGS_enable_urma || ClientUrmaRuntimeRequested().load(std::memory_order_acquire);
#else
    return false;
#endif
}

void PublishClientUrmaRuntimeReady()
{
    ClientUrmaRuntimeReady().store(true, std::memory_order_release);
}

bool IsUcpEnabled()
{
#ifdef USE_RDMA
    return FLAGS_enable_rdma;
#else
    return false;
#endif
}

bool IsRegisterWholeArenaEnabled()
{
    return FLAGS_urma_register_whole_arena;
}

bool IsUbNumaAffinityEnabled()
{
#ifdef USE_URMA
    return IsUrmaEnabled() && FLAGS_enable_ub_numa_affinity && IsRegisterWholeArenaEnabled();
#else
    return false;
#endif
}

bool ShouldBuildUbNumaRangeTable()
{
#ifdef USE_URMA
    // Initialization must use "configured/requested", not "ready": the range table is built from inside
    // UrmaManager::Init, before PublishClientUrmaRuntimeReady can run.
    return IsUrmaRuntimeConfigured() && FLAGS_enable_ub_numa_affinity && IsRegisterWholeArenaEnabled();
#else
    return false;
#endif
}

bool NeedRegisterWholeArena()
{
#ifdef USE_URMA
    if (IsUrmaEnabled() && IsRegisterWholeArenaEnabled()) {
        return true;
    }
#endif

#ifdef USE_RDMA
    if (IsUcpEnabled() && IsRegisterWholeArenaEnabled()) {
        return true;
    }
#endif
    return false;
}

Status WaitFastTransportEvent(std::vector<uint64_t> &keys, std::function<int64_t(void)> remainingTime,
                              std::function<Status(Status &)> errorHandler)
{
    return WaitFastTransportEventWithFailure(keys, std::move(remainingTime), std::move(errorHandler), nullptr);
}

Status WaitFastTransportEventWithFailure(std::vector<uint64_t> &keys, std::function<int64_t(void)> remainingTime,
                                         std::function<Status(Status &)> errorHandler, UrmaWriteFailure *failure)
{
    (void)keys;
    (void)remainingTime;
    (void)errorHandler;
    (void)failure;
#ifdef USE_URMA
    if (IsUrmaEnabled()) {
        Status firstError = Status::OK();
        UrmaSequentialWaitContext waitContext;
        for (auto key : keys) {
            // Wait for the event until timeout
            Status rc = UrmaManager::Instance().WaitToFinish(key, remainingTime(), failure, &waitContext);
            if (rc.IsError() && firstError.IsOk()) {
                firstError = errorHandler(rc);
            }
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(firstError, "Failed to wait for URMA event.");
    }
#endif

#ifdef USE_RDMA
    if (IsUcpEnabled()) {
        Status firstError = Status::OK();
        for (auto key : keys) {
            // Wait for the event until timeout
            Status rc = UcpManager::Instance().WaitToFinish(key, remainingTime());
            if (rc.IsError() && firstError.IsOk()) {
                firstError = errorHandler(rc);
            }
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(firstError, "Failed to wait for RDMA event.");
    }
#endif
    return Status::OK();
}
}  // namespace datasystem
