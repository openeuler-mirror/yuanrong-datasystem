/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: ffts plus task dispatcher
 * Author: lilianlin
 * Create: 2023-05-01
 */

#ifndef P2P_DISPATCHER_FFTS_PUB_H
#define P2P_DISPATCHER_FFTS_PUB_H

#include "external/ra.h"
#include "hccl/hccl.h"
#include "npu/P2PStream.h"
#include "runtime/rt_ffts_plus_define.h"
#include "runtime/rt_stars_define.h"
#include "runtime/rt_ffts_plus.h"
#include "npu/ffts/ffts_common.h"

using HcclRtSignal = void *;
using HcclRtNotify = void *;

namespace p2p {
constexpr uint32_t CONTEXT_MAX_NUM = 128;

class DispatcherFFTS {
public:
    DispatcherFFTS(uint32_t devLogID) : devLogID(devLogID)
    {
        const int amount = 10;
        fftsCtxs.reserve(amount);
    };
    virtual ~DispatcherFFTS();

    HcclResult Init();

    HcclResult AddTaskDependency(uint32_t predecessorId, uint32_t successorId);
    HcclResult SignalRecordCrossChip(HcclRtSignal signal, uint32_t *taskId, uint64_t notifyAddr);
    HcclResult SignalWaitCrossChip(HcclRtSignal signal, uint32_t *taskId, uint32_t notifyId);
    HcclResult MemcpyAsync(void *dst, const void *src, uint64_t size, uint32_t *taskId);
    HcclResult RdmaSend(uint32_t dbindex, uint64_t dbinfo, const struct send_wr &wr, uint32_t *taskId);
    HcclResult LaunchFftsTask(rtStream_t stm, uint16_t readyContextNum, int ctxIndex);

    HcclResult CreateFftsCtxs(int amount);
    HcclResult SetFftsCtx(int index);
    HcclResult ClearFftsCtx();
    HcclResult ReuseCtx(int index);

private:
    void EnsureFftsContextsSize();
    HcclResult InitFftsDescNotifyRecordRemote(HcclRtSignal signal, uint64_t notifyAddr);
    HcclResult InitFftsDescNotifyWait(HcclRtSignal signal, uint32_t notifyId);
    HcclResult InitFftsDescSdma(void *dst, const void *src, uint64_t cnt, uint32_t sdmaSqeHeader);
    HcclResult InitFftsDescMemcpy(void *dst, const void *src, uint64_t size);
    HcclResult InitFftsDescRdmaSend(uint32_t dbindex, uint64_t dbinfo);
    bool FftsCtxReady();
    HcclResult PrintFFTSDebugDetails(rtFftsPlusSqe_t &fftsPlusSqe, rtFftsPlusTaskInfo_t &task);
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

    HcclFftsContextsInfo *fftsCtxsPtr;
    std::vector<HcclFftsContextsInfo *> fftsCtxs;
    std::vector<void *> argsHandleList;
    int32_t devLogID;
    int64_t chipId;
};
}  // namespace p2p
#endif  // P2P_DISPATCHER_FFTS_PUB_H
