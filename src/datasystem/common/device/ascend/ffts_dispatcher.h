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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_FFTS_DISPATCHER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_FFTS_DISPATCHER_H

#include <vector>

#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/device/ascend/cann_types.h"

namespace datasystem {
namespace ffts {
using HcclRtSignal = void *;
using HcclRtNotify = void *;
using rtStream_t = void *;
#ifndef BUILD_HETERO
class FftsDispatcher {
public:
    FftsDispatcher(uint32_t, acl::AclDeviceManager *){};
    ~FftsDispatcher() = default;
    HcclResult Init()
    {
        return HCCL_E_NOT_SUPPORT;
    }
    HcclResult SetFftsCtx(size_t)
    {
        return HCCL_E_NOT_SUPPORT;
    }
    HcclResult CreateFftsCtxs(int)
    {
        return HCCL_E_NOT_SUPPORT;
    }
    HcclResult MemcpyAsync(void *, const void *, uint64_t, uint32_t *)
    {
        return HCCL_E_NOT_SUPPORT;
    }
    HcclResult AddTaskDependency(uint32_t, uint32_t)
    {
        return HCCL_E_NOT_SUPPORT;
    }
    HcclResult LaunchFftsTask(rtStream_t, uint16_t, int)
    {
        return HCCL_E_NOT_SUPPORT;
    }
    HcclResult ReuseCtx(size_t)
    {
        return HCCL_E_NOT_SUPPORT;
    }
};
#else
using HcclRtSignal = void *;
using HcclRtNotify = void *;
using rtStream_t = void *;

struct HcclFftsContextsInfo {
    bool completed = false;
    uint32_t refreshIndex = 0;  // Index for next rtFftsPlusComCtx_t in contexts
    uint32_t ctxNum = 0;
    std::vector<rtFftsPlusComCtx_t> contexts;
    HcclFftsContextsInfo()
    {
        // 100: number of contexts that can be stored by default, which can be dynamically expanded
        const size_t contextDefaultSize = 100;
        contexts.resize(contextDefaultSize);
    }
};

class FftsDispatcher {
public:
    FftsDispatcher(uint32_t devLogID, acl::AclDeviceManager *aclDeviceManager)
        : devLogID_(devLogID), aclDeviceManager_(aclDeviceManager)
    {
        const int contexCount = 10;
        fftsCtxs_.reserve(contexCount);
    }
    virtual ~FftsDispatcher();

    HcclResult Init();

    HcclResult AddTaskDependency(uint32_t predecessorId, uint32_t successorId);
    HcclResult SignalRecordCrossChip(HcclRtSignal signal, uint32_t *taskId, uint64_t notifyAddr);
    HcclResult SignalWaitCrossChip(HcclRtSignal signal, uint32_t *taskId, uint32_t notifyId);
    HcclResult MemcpyAsync(void *dst, const void *src, uint64_t size, uint32_t *taskId);
    HcclResult RdmaSend(uint32_t dbindex, uint64_t dbinfo, const struct send_wr &wr, uint32_t *taskId);
    HcclResult LaunchFftsTask(rtStream_t stm, uint16_t readyContextNum, int ctxIndex);

    HcclResult CreateFftsCtxs(int amount);
    HcclResult SetFftsCtx(size_t index);
    HcclResult ClearFftsCtx();
    HcclResult ReuseCtx(size_t index);

private:
    void EnsureFftsContextsSize();
    HcclResult InitFftsDescNotifyRecordRemote(HcclRtSignal signal, uint64_t notifyAddr);
    HcclResult InitFftsDescNotifyWait(HcclRtSignal signal, uint32_t notifyId);
    HcclResult InitFftsDescSdma(void *dst, const void *src, uint64_t cnt, uint32_t sdmaSqeHeader);
    HcclResult InitFftsDescMemcpy(void *dst, const void *src, uint64_t size);
    HcclResult InitFftsDescRdmaSend(uint32_t dbindex, uint64_t dbinfo);
    bool FftsCtxReady();
    HcclResult ConstructFftsSqe(rtFftsPlusSqe_t &fftsPlusSqe, uint16_t readyContextNum);
    HcclResult ConstructFftsTask(rtFftsPlusTaskInfo_t &task, rtFftsPlusSqe_t &fftsPlusSqe);
    HcclResult RdmaSendInternal(uint32_t dbindex, uint64_t dbinfo);
    HcclResult ConstructFftsWriteValueCtx(uint32_t dbindex, uint64_t dbinfo, rtFftsPlusWriteValueCtx_t *ctx);
    HcclResult ConstructFftsSdmaCtx(void *dst, const void *src, uint64_t cnt, uint32_t sdmaSqeHeader,
                                    rtFftsPlusSdmaCtx_t *ctx);
    HcclResult ConstructFFtsNotifyWaitCtx(HcclRtSignal &signal, rtFftsPlusNotifyCtx_t *ctx, uint32_t notifyId);
    HcclResult ConstructFftsNotifyRecordRemoteCtx(HcclRtSignal &signal, rtFftsPlusWriteValueCtx_t *ctx,
                                                  uint64_t notifyAddr);
    HcclResult ConstructFftsNotifyCtx(uint32_t notifyID, uint16_t contextType, rtFftsPlusNotifyCtx_t *ctx);
    bool IsInvalidRdmaParam(uint32_t dbindex, uint64_t dbinfo);
    HcclResult RtFftsPlusTaskLaunchWithFlag(rtFftsPlusTaskInfo_t *fftsPlusTaskInfo, rtStream_t stm, uint32_t flag);
    HcclResult RtGetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t &val);

    HcclFftsContextsInfo *fftsCtxsPtr_;
    std::vector<HcclFftsContextsInfo *> fftsCtxs_;
    int32_t devLogID_;
    int64_t chipId_;
    acl::AclDeviceManager *aclDeviceManager_;
};
#endif
}  // namespace ffts
}  // namespace datasystem
#endif
