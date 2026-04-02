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
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/rdma/fast_transport_base.h"
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#ifdef USE_RDMA
#include "datasystem/common/rdma/ucp_manager.h"
#endif
#ifdef BUILD_HETERO
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#endif

namespace datasystem {

Status RegisterFastTransportMemory(void *segAddress, const uint64_t &segSize) {
    (void) segAddress;
    (void) segSize;
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

Status RegisterHostMemory(void *segAddress, const uint64_t &segSize) {
    (void) segAddress;
    (void) segSize;
#ifdef BUILD_HETERO
    if (IsRemoteH2DEnabled() && FLAGS_urma_register_whole_arena && segAddress != nullptr) {
            RETURN_IF_NOT_OK(RemoteH2DManager::Instance().RegisterHostMemory(segAddress, segSize));
        }
#endif
    return Status::OK();
}

 bool IsFastTransportEnabled() {
     if (IsUrmaEnabled()) {
         return true;
     }

     if (IsUcpEnabled()) {
         return true;
     }

     return false;
 }

 bool IsRemoteH2DEnabled() {
#ifdef BUILD_HETERO
     return !FLAGS_remote_h2d_device_ids.empty();
#else
     return false;
#endif
 }


bool IsUrmaEnabled() {
#ifdef USE_URMA
    return FLAGS_enable_urma;
#else
    return false;
#endif
}

bool IsUcpEnabled() {
#ifdef USE_RDMA
    return FLAGS_enable_rdma;
#else
    return false;
#endif
}

bool IsRegisterWholeArenaEnabled() {
    return FLAGS_urma_register_whole_arena;
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
    (void)keys;
    (void)remainingTime;
    (void)errorHandler;
#ifdef USE_URMA
    if (IsUrmaEnabled()) {
        for (auto key : keys) {
            // Wait for the event until timeout
            Status rc = UrmaManager::Instance().WaitToFinish(key, remainingTime());
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(errorHandler(rc), "Failed to wait for URMA event.");
        }
    }
#endif

#ifdef USE_RDMA
    if (IsUcpEnabled()) {
        for (auto key : keys) {
            // Wait for the event until timeout
            Status rc = UcpManager::Instance().WaitToFinish(key, remainingTime());
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(errorHandler(rc), "Failed to wait for RDMA event.");
        }
    }
#endif
    return Status::OK();
}

} // namespace datasystem