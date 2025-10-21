/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: ffts plus task dispatcher
 * Author: lilianlin
 * Create: 2023-05-01
 */

#include "npu/ffts/dispatcher_ffts.h"
#include "runtime/rt_stars.h"
#include "runtime/kernel.h"
#include "runtime/dev.h"
#include "securec.h"
#include <algorithm>
#include <mutex>

namespace p2p {
HcclResult hrtFftsPlusTaskLaunchWithFlag(rtFftsPlusTaskInfo_t *fftsPlusTaskInfo, rtStream_t stm, uint32_t flag)
{
    CHK_PTR_NULL(fftsPlusTaskInfo);
    CHK_PTR_NULL(stm);
    uintptr_t input[2];  // When the fftsplus task is delivered, two parameters. 0: task info, 1: stream handle need to
                         // be entered.
    input[0] = reinterpret_cast<uintptr_t>(fftsPlusTaskInfo);
    input[1] = reinterpret_cast<uintptr_t>(stm);
    rtError_t ret = rtGeneralCtrl(input, 2, RT_GNL_CTRL_TYPE_FFTS_PLUS);
    if (ret != RT_ERROR_NONE) {
        std::cerr << "[hrtFftsPlusTaskLaunchWithFlag]rt ffts launch failed." << std::endl;
        return HCCL_E_RUNTIME;
    }
    return HCCL_SUCCESS;
}

HcclResult hrtGetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t &val)
{
    rtError_t ret = rtGetDeviceInfo(deviceId, moduleType, infoType, reinterpret_cast<int64_t *>(&val));
    if (ret != RT_ERROR_NONE) {
        std::cerr << "[hrtGetDeviceInfo]rt get device info failed." << std::endl;
        return HCCL_E_RUNTIME;
    }
    return HCCL_SUCCESS;
}

#define RT_INFO_TYPE_PHY_CHIP_ID 18
#define MAX_DEV_LOGIC_ID 7

HcclResult hrtGetRdmaDoorbellAddr(int32_t devLogID, int64_t chipID, uint32_t dbIndex, uint64_t &dbAddr)
{
    const uint64_t roceBaseAddr = 0x2000000000ULL;
    const uint64_t roceVfDbCfg0Reg = 0x230ULL;
    const uint64_t chipAddrOffset = 0x80000000000ULL;
    const uint64_t dieAddrOffset = 0x10000000000ULL;
    const uint32_t dbDieIdMask = 0x00ff0000;
    const uint32_t dbDieIdShift = 16;
    dbAddr = roceBaseAddr + roceVfDbCfg0Reg + chipAddrOffset * chipID
             + dieAddrOffset * ((dbIndex & dbDieIdMask) >> dbDieIdShift);
    return HCCL_SUCCESS;
}

DispatcherFFTS::~DispatcherFFTS()
{
}

HcclResult DispatcherFFTS::Init()
{
    CHK_RET(hrtGetDeviceInfo(devLogID, RT_MODULE_TYPE_SYSTEM, RT_INFO_TYPE_PHY_CHIP_ID, this->chipId));
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::SetFftsCtx(int index)
{
    if (index >= fftsCtxs.size()) {
        std::cerr << "Index exceeds context size." << std::endl;
        return HCCL_E_RUNTIME;
    }

    fftsCtxsPtr = fftsCtxs[index];
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::CreateFftsCtxs(int amount)
{
    if (fftsCtxs.size() > 0) {
        std::cerr << "Contexts not empty." << std::endl;
        return HCCL_E_RUNTIME;
    }

    for (int i = 0; i < amount; i++) {
        HcclFftsContextsInfo *ctx = new HcclFftsContextsInfo();
        fftsCtxs.push_back(ctx);
    }

    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::ClearFftsCtx()
{
    if (fftsCtxs.size() == 0) {
        std::cerr << "Contexts already empty." << std::endl;
        return HCCL_E_RUNTIME;
    }

    for (int i = 0; i < fftsCtxs.size(); i++) {
        delete fftsCtxs[i];
    }
    fftsCtxs.clear();

    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::ReuseCtx(int index)
{
    if (index > fftsCtxs.size()) {
        std::cerr << "Context does not exist." << std::endl;
        return HCCL_E_RUNTIME;
    }

    fftsCtxs[index]->completed = false;
    fftsCtxs[index]->refreshIndex = 0;
    fftsCtxs[index]->ctxNum = 0;

    return HCCL_SUCCESS;
}

// Dispatches all previously added commands as an FFTS task
HcclResult DispatcherFFTS::LaunchFftsTask(rtStream_t stm, uint16_t readyContextNum, int ctxIndex)
{
    HcclFftsContextsInfo *prevFftsCtxsPtr = fftsCtxsPtr;
    fftsCtxsPtr = fftsCtxs[ctxIndex];

    if (!fftsCtxsPtr->completed) {
        // Set amount of contexts ("tasks") to use for ffts = next context index
        fftsCtxsPtr->ctxNum = fftsCtxsPtr->refreshIndex;
        fftsCtxsPtr->completed = true;
    }

    if (fftsCtxsPtr->refreshIndex != fftsCtxsPtr->ctxNum) {
        HCCL_ERROR("ffts context num is invaild, expected:%u, actual:%u.", fftsCtxsPtr->ctxNum,
                   fftsCtxsPtr->refreshIndex);
        return HCCL_E_PARA;
    }

    if (fftsCtxsPtr->ctxNum == 0) {
        std::cout << "ffts context num is 0, will not submit this context." << std::endl;
        return HCCL_SUCCESS;
    }

    // Reset context counter
    fftsCtxsPtr->refreshIndex = 0;

    // Create FFTS+ sqe containing task type, amount of contexts, etc. info
    rtFftsPlusSqe_t fftsPlusSqe{};
    ConstructFftsSqe(fftsPlusSqe, readyContextNum);

    // Create FFTS+ task which can be launched
    rtFftsPlusTaskInfo_t task{};

    task.argsHandleInfoNum = 0;
    task.argsHandleInfoPtr = nullptr;
    ConstructFftsTask(task, fftsPlusSqe);

    // Launch FFTS+ task
    CHK_RET(hrtFftsPlusTaskLaunchWithFlag(&task, stm, 0));

    fftsCtxsPtr = prevFftsCtxsPtr;

    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::ConstructFftsSqe(rtFftsPlusSqe_t &fftsPlusSqe, uint16_t readyContextNum)
{
    fftsPlusSqe.fftsType = RT_FFTS_PLUS_TYPE;
    fftsPlusSqe.totalContextNum = fftsCtxsPtr->ctxNum;  // Amount of "tasks"
    fftsPlusSqe.readyContextNum = readyContextNum;
    fftsPlusSqe.preloadContextNum =
        (fftsPlusSqe.readyContextNum <= CONTEXT_MAX_NUM ? fftsPlusSqe.readyContextNum : CONTEXT_MAX_NUM);

    fftsPlusSqe.timeout = 0;
    fftsPlusSqe.subType =
        argsHandleList.empty()
            ? 0x5A
            : 0x5B;  // 0x5A: identifies the communication task and optimizes the FFTS+ scheduling performance.
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::ConstructFftsTask(rtFftsPlusTaskInfo_t &task, rtFftsPlusSqe_t &fftsPlusSqe)
{
    task.fftsPlusSqe = &fftsPlusSqe;
    task.descBuf = fftsCtxsPtr->contexts.data();
    task.descBufLen = sizeof(rtFftsPlusComCtx_t) * fftsCtxsPtr->ctxNum;
    task.descAddrType = 0;
    if (!argsHandleList.empty()) {
        task.argsHandleInfoNum = argsHandleList.size();
        task.argsHandleInfoPtr = argsHandleList.data();
        argsHandleList.clear();
    }
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::PrintFFTSDebugDetails(rtFftsPlusSqe_t &fftsPlusSqe, rtFftsPlusTaskInfo_t &task)
{
    std::cout << "totalContextNum " << fftsPlusSqe.totalContextNum << std::endl;
    printf("-------------------------------");
    printf("totalContextNum:0x%04x", fftsPlusSqe.totalContextNum);
    printf("readyContextNum:0x%04x", fftsPlusSqe.readyContextNum);
    printf("preloadContextNum:0x%04x", fftsPlusSqe.preloadContextNum);
    printf("descBuf:%p", task.descBuf);
    printf("descBufLen:%u", task.descBufLen);
    printf("descAddrType:%u", task.descAddrType);
    const uint32_t printLineNum = 32;
    const uint32_t printBytePreLine = sizeof(rtFftsPlusComCtx_t) / printLineNum;
    const uint32_t byte3Offset = 3;
    const uint32_t byte2Offset = 2;
    const uint32_t byte1Offset = 1;
    const uint32_t byte0Offset = 0;
    for (uint32_t i = 0; i < fftsCtxsPtr->ctxNum; i++) {
        printf("-------------------------------");
        printf("index:0x%02x", i);
    }
    printf("-------------------------------");

    return HCCL_SUCCESS;
}

// Check whether task has been launched already, should reset ffts context
bool DispatcherFFTS::FftsCtxReady()
{
    return fftsCtxsPtr->completed;
}

// Increase ctx size by 2x if size exceeded to ensure enough memory to add new ctx
void DispatcherFFTS::EnsureFftsContextsSize()
{
    if (fftsCtxsPtr->refreshIndex >= fftsCtxsPtr->contexts.size()) {
        const uint32_t kGrowthFactor = 2;
        fftsCtxsPtr->contexts.resize(
            fftsCtxsPtr->contexts.size()
            * kGrowthFactor);  // The context space is insufficient. Twice of the current context number is applied for.
    }
    return;
}

// Add remote notify record entry (context).
// we should just pass address as a parameter?
HcclResult DispatcherFFTS::InitFftsDescNotifyRecordRemote(HcclRtSignal signal, uint64_t notifyAddr)
{
    EnsureFftsContextsSize();
    rtFftsPlusComCtx_t &comCtx = fftsCtxsPtr->contexts[fftsCtxsPtr->refreshIndex];
    memset_s(&comCtx, sizeof(rtFftsPlusComCtx_t), 0, sizeof(rtFftsPlusComCtx_t));

    rtFftsPlusWriteValueCtx_t *ctx = reinterpret_cast<rtFftsPlusWriteValueCtx_t *>(&comCtx);

    ConstructFftsNotifyRecordRemoteCtx(signal, ctx, notifyAddr);

    fftsCtxsPtr->refreshIndex++;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::ConstructFftsNotifyRecordRemoteCtx(HcclRtSignal &signal, rtFftsPlusWriteValueCtx_t *ctx,
                                                              uint64_t notifyAddr)
{
    const uint64_t uint64_tHighMask = 0xffffffff00000000;
    const uint64_t uint64_tLowMask = 0x00000000ffffffff;
    const uint32_t shift = 32;
    ctx->contextType = RT_CTX_TYPE_WRITE_VALUE;
    ctx->threadDim = 1;
    ctx->awSize = 2;  // 2: write 4 bytes
    ctx->res11 = 4;   // 4: notify record identifier
    ctx->writeAddressBaseL = notifyAddr & uint64_tLowMask;
    ctx->writeAddressBaseH = (notifyAddr & uint64_tHighMask) >> shift;
    ctx->writeValue[0] = 1;  // index 1: byte0~3
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::InitFftsDescNotifyWait(HcclRtSignal signal, uint32_t notifyId)
{
    EnsureFftsContextsSize();
    rtFftsPlusComCtx_t &comCtx = fftsCtxsPtr->contexts[fftsCtxsPtr->refreshIndex];
    memset_s(&comCtx, sizeof(rtFftsPlusComCtx_t), 0, sizeof(rtFftsPlusComCtx_t));

    rtFftsPlusNotifyCtx_t *ctx = reinterpret_cast<rtFftsPlusNotifyCtx_t *>(&comCtx);

    ConstructFFtsNotifyWaitCtx(signal, ctx, notifyId);

    fftsCtxsPtr->refreshIndex++;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::ConstructFFtsNotifyWaitCtx(HcclRtSignal &signal, rtFftsPlusNotifyCtx_t *ctx,
                                                      uint32_t notifyId)
{
    return ConstructFftsNotifyCtx(notifyId, RT_CTX_TYPE_NOTIFY_WAIT, ctx);
}

HcclResult DispatcherFFTS::ConstructFftsNotifyCtx(uint32_t notifyID, uint16_t contextType, rtFftsPlusNotifyCtx_t *ctx)
{
    ctx->contextType = contextType;
    ctx->threadDim = 1;
    ctx->notifyIdBase = notifyID;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::InitFftsDescSdma(void *dst, const void *src, uint64_t cnt, uint32_t sdmaSqeHeader)
{
    EnsureFftsContextsSize();
    rtFftsPlusComCtx_t &comCtx = fftsCtxsPtr->contexts[fftsCtxsPtr->refreshIndex];
    memset_s(&comCtx, sizeof(rtFftsPlusComCtx_t), 0, sizeof(rtFftsPlusComCtx_t));

    rtFftsPlusSdmaCtx_t *ctx = reinterpret_cast<rtFftsPlusSdmaCtx_t *>(&comCtx);

    ConstructFftsSdmaCtx(dst, src, cnt, sdmaSqeHeader, ctx);

    fftsCtxsPtr->refreshIndex++;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::ConstructFftsSdmaCtx(void *dst, const void *src, uint64_t cnt, uint32_t sdmaSqeHeader,
                                                rtFftsPlusSdmaCtx_t *ctx)
{
    ctx->threadDim = 1;

    ctx->contextType = (cnt == 0) ? RT_CTX_TYPE_LABEL : RT_CTX_TYPE_SDMA;
    ctx->sdmaSqeHeader = sdmaSqeHeader;

    const uint64_t uint64_tHighMask = 0xffffffff00000000;
    const uint64_t uint64_tLowMask = 0x00000000ffffffff;
    const uint32_t shift = 32;

    ctx->sourceAddressBaseL = reinterpret_cast<uint64_t>(src) & uint64_tLowMask;
    ctx->sourceAddressBaseH = (reinterpret_cast<uint64_t>(src) & uint64_tHighMask) >> shift;

    ctx->destinationAddressBaseL = reinterpret_cast<uint64_t>(dst) & uint64_tLowMask;
    ctx->destinationAddressBaseH = (reinterpret_cast<uint64_t>(dst) & uint64_tHighMask) >> shift;

    ctx->nonTailDataLength = cnt;
    ctx->tailDataLength = cnt;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::InitFftsDescRdmaSend(uint32_t dbindex, uint64_t dbinfo)
{
    EnsureFftsContextsSize();
    rtFftsPlusComCtx_t &comCtx = fftsCtxsPtr->contexts[fftsCtxsPtr->refreshIndex];
    memset_s(&comCtx, sizeof(rtFftsPlusComCtx_t), 0, sizeof(rtFftsPlusComCtx_t));

    rtFftsPlusWriteValueCtx_t *ctx = reinterpret_cast<rtFftsPlusWriteValueCtx_t *>(&comCtx);

    ConstructFftsWriteValueCtx(dbindex, dbinfo, ctx);

    fftsCtxsPtr->refreshIndex++;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::ConstructFftsWriteValueCtx(uint32_t dbindex, uint64_t dbinfo, rtFftsPlusWriteValueCtx_t *ctx)
{
    const uint64_t uint64_tHighMask = 0xffffffff00000000;
    const uint64_t uint64_tLowMask = 0x00000000ffffffff;
    const uint32_t shift = 32;
    uint64_t dbAddr = 0;

    if (!IsInvalidRdmaParam(dbindex, dbinfo)) {
        CHK_RET(hrtGetRdmaDoorbellAddr(devLogID, chipId, dbindex, dbAddr));
    }

    ctx->contextType = IsInvalidRdmaParam(dbindex, dbinfo) ? RT_CTX_TYPE_LABEL : RT_CTX_TYPE_WRITE_VALUE;
    ctx->threadDim = 1;
    ctx->awSize = 3;  // 3: write 8 Bytes
    ctx->res11 = 2;   // 2: 标识rdma send
    ctx->writeAddressBaseL = dbAddr & uint64_tLowMask;
    ctx->writeAddressBaseH = (dbAddr & uint64_tHighMask) >> shift;
    ctx->writeValue[0] = dbinfo & uint64_tLowMask;              // index 0: byte0~3
    ctx->writeValue[1] = (dbinfo & uint64_tHighMask) >> shift;  // index 1: byte4~8
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::InitFftsDescMemcpy(void *dst, const void *src, uint64_t size)
{
    CHK_RET(InitFftsDescSdma(dst, src, size, SDMA_FP32_ATOMIC_MOVE_SQE));
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::AddTaskDependency(uint32_t predecessorId, uint32_t successorId)
{
    if (FftsCtxReady()) {
        std::cerr << "Launch has already been called for the current context" << std::endl;
        return HCCL_E_RUNTIME;
    }

    fftsCtxsPtr->contexts[predecessorId].successorList[fftsCtxsPtr->contexts[predecessorId].successorNum] = successorId;
    fftsCtxsPtr->contexts[predecessorId].successorNum++;
    fftsCtxsPtr->contexts[successorId].predCntInit++;
    fftsCtxsPtr->contexts[successorId].predCnt++;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::SignalRecordCrossChip(HcclRtSignal signal, uint32_t *taskId, uint64_t notifyAddr)
{
    if (FftsCtxReady()) {
        std::cerr << "Launch has already been called for the current context" << std::endl;
        return HCCL_E_RUNTIME;
    }

    CHK_RET(InitFftsDescNotifyRecordRemote(signal, notifyAddr));
    *taskId = fftsCtxsPtr->refreshIndex - 1;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::SignalWaitCrossChip(HcclRtSignal signal, uint32_t *taskId, uint32_t notifyId)
{
    if (FftsCtxReady()) {
        std::cerr << "Launch has already been called for the current context" << std::endl;
        return HCCL_E_RUNTIME;
    }

    CHK_RET(InitFftsDescNotifyWait(signal, notifyId));
    *taskId = fftsCtxsPtr->refreshIndex - 1;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::MemcpyAsync(void *dst, const void *src, uint64_t size, uint32_t *taskId)
{
    if (FftsCtxReady()) {
        std::cerr << "Launch has already been called for the current context" << std::endl;
        return HCCL_E_RUNTIME;
    }

    CHK_RET(InitFftsDescMemcpy(dst, src, size));
    *taskId = fftsCtxsPtr->refreshIndex - 1;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::RdmaSend(uint32_t dbindex, uint64_t dbinfo, const struct send_wr &wr, uint32_t *taskId)
{
    if (FftsCtxReady()) {
        std::cerr << "Launch has already been called for the current context" << std::endl;
        return HCCL_E_RUNTIME;
    }

    CHK_RET(RdmaSendInternal(dbindex, dbinfo));
    *taskId = fftsCtxsPtr->refreshIndex - 1;
    return HCCL_SUCCESS;
}

HcclResult DispatcherFFTS::RdmaSendInternal(uint32_t dbindex, uint64_t dbinfo)
{
    CHK_RET(InitFftsDescRdmaSend(dbindex, dbinfo));
    return HCCL_SUCCESS;
}

bool DispatcherFFTS::IsInvalidRdmaParam(uint32_t dbindex, uint64_t dbinfo)
{
    return dbindex == INVALID_UINT && dbinfo == INVALID_S64;
}

}  // namespace p2p