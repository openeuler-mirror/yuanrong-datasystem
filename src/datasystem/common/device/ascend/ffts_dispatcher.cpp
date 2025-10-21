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

#include "datasystem/common/device/ascend/ffts_dispatcher.h"

#include <iostream>
#include <cstring>
#include "securec.h"

#define CHK_RET(call)                                                                     \
    do {                                                                                  \
        HcclResult ret = (call);                                                          \
        if (ret != HCCL_SUCCESS) {                                                        \
            LOG(ERROR) << "hcclRet " << ret << " at " << __FUNCTION__ << ":" << __LINE__; \
            return ret;                                                                   \
        }                                                                                 \
    } while (0)

#define CHK_PTR_NULL(ptr)                                                                        \
    do {                                                                                         \
        if (ptr == nullptr) {                                                                    \
            LOG(ERROR) << "ptr [" << #ptr << "] is NULL at " << __FUNCTION__ << ":" << __LINE__; \
            return HCCL_E_PTR;                                                                   \
        }                                                                                        \
    } while (0)

#ifdef BUILD_HETERO
namespace datasystem {
namespace ffts {
constexpr uint32_t RT_INFO_TYPE_PHY_CHIP_ID = 18;
constexpr uint32_t CONTEXT_MAX_NUM = 128;
constexpr uint32_t SDMA_FP32_ATOMIC_MOVE_SQE = 0x1E70;
constexpr uint32_t INVALID_UINT = 0xFFFFFFFF;
constexpr uint64_t INVALID_S64 = 0xFFFFFFFFFFFFFFFF;
constexpr uint64_t UINT64_HIGH_MASK = 0xffffffff00000000;
constexpr uint64_t UINT64_LOW_MASK = 0x00000000ffffffff;

namespace {
HcclResult hrtGetRdmaDoorbellAddr(int32_t devLogID, int64_t chipID, uint32_t dbIndex, uint64_t &dbAddr)
{
    if (chipID < 0) {
        LOG(ERROR) << "chipID less than 0: " << chipID;
        return HCCL_E_RUNTIME;
    }
    (void)devLogID;
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
}  // namespace

FftsDispatcher::~FftsDispatcher()
{
}

HcclResult FftsDispatcher::Init()
{
    CHK_RET(RtGetDeviceInfo(devLogID_, RT_MODULE_TYPE_SYSTEM, RT_INFO_TYPE_PHY_CHIP_ID, this->chipId_));
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::SetFftsCtx(size_t index)
{
    if (index >= fftsCtxs_.size()) {
        LOG(ERROR) << "Index exceeds context size.";
        return HCCL_E_RUNTIME;
    }

    fftsCtxsPtr_ = fftsCtxs_[index];
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::CreateFftsCtxs(int amount)
{
    if (fftsCtxs_.size() > 0) {
        LOG(ERROR) << "Contexts not empty.";
        return HCCL_E_RUNTIME;
    }

    for (int i = 0; i < amount; i++) {
        HcclFftsContextsInfo *ctx = new HcclFftsContextsInfo();
        fftsCtxs_.push_back(ctx);
    }

    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::ClearFftsCtx()
{
    if (fftsCtxs_.size() == 0) {
        LOG(ERROR) << "Contexts already empty.";
        return HCCL_E_RUNTIME;
    }

    for (size_t i = 0; i < fftsCtxs_.size(); i++) {
        delete fftsCtxs_[i];
    }
    fftsCtxs_.clear();
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::ReuseCtx(size_t index)
{
    if (index >= fftsCtxs_.size()) {
        LOG(ERROR) << "Context does not exist.";
        return HCCL_E_RUNTIME;
    }

    fftsCtxs_[index]->completed = false;
    fftsCtxs_[index]->refreshIndex = 0;
    fftsCtxs_[index]->ctxNum = 0;

    return HCCL_SUCCESS;
}

// Dispatches all previously added commands as an FFTS task
HcclResult FftsDispatcher::LaunchFftsTask(rtStream_t stm, uint16_t readyContextNum, int ctxIndex)
{
    HcclFftsContextsInfo *prevFftsCtxsPtr = fftsCtxsPtr_;
    fftsCtxsPtr_ = fftsCtxs_[ctxIndex];

    if (!fftsCtxsPtr_->completed) {
        // Set amount of contexts ("tasks") to use for ffts = next context index
        fftsCtxsPtr_->ctxNum = fftsCtxsPtr_->refreshIndex;
        fftsCtxsPtr_->completed = true;
    }

    if (fftsCtxsPtr_->refreshIndex != fftsCtxsPtr_->ctxNum) {
        LOG(ERROR) << "ffts context num is invaild, expected:" << fftsCtxsPtr_->ctxNum
                   << ", actual:" << fftsCtxsPtr_->refreshIndex;
        return HCCL_E_PARA;
    }

    if (fftsCtxsPtr_->ctxNum == 0) {
        LOG(INFO) << "ffts context num is 0, will not submit this context.";
        return HCCL_SUCCESS;
    }

    // Reset context counter
    fftsCtxsPtr_->refreshIndex = 0;

    // Create FFTS+ sqe containing task type, amount of contexts, etc. info
    rtFftsPlusSqe_t fftsPlusSqe{};
    ConstructFftsSqe(fftsPlusSqe, readyContextNum);

    // Create FFTS+ task which can be launched
    rtFftsPlusTaskInfo_t task{};
    task.argsHandleInfoNum = 0;
    task.argsHandleInfoPtr = nullptr;
    ConstructFftsTask(task, fftsPlusSqe);

    // Launch FFTS+ task
    CHK_RET(RtFftsPlusTaskLaunchWithFlag(&task, stm, 0));
    fftsCtxsPtr_ = prevFftsCtxsPtr;

    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::ConstructFftsSqe(rtFftsPlusSqe_t &fftsPlusSqe, uint16_t readyContextNum)
{
    fftsPlusSqe.fftsType = RT_FFTS_PLUS_TYPE;
    fftsPlusSqe.totalContextNum = fftsCtxsPtr_->ctxNum;  // Amount of "tasks"
    fftsPlusSqe.readyContextNum = readyContextNum;
    fftsPlusSqe.preloadContextNum = fftsCtxsPtr_->ctxNum;
    fftsPlusSqe.preloadContextNum =
        (fftsPlusSqe.readyContextNum <= CONTEXT_MAX_NUM ? fftsPlusSqe.readyContextNum : CONTEXT_MAX_NUM);

    // Depends on device type. For 910B and C, 0 means no timeout. But usually this is positive
    fftsPlusSqe.timeout = 0;
    // Identifies the communication task and optimizes the FFTS+ scheduling performance. (The RTS requires that the
    // AIV/AIC task be 0x5B. Otherwise, the task is 0x5A.)
    // 0x5A: identifies the communication task and optimizes the FFTS+ scheduling performance.
    const uint8_t TASK_TYPE_AIV_AIC = 0x5B;
    const uint8_t TASK_TYPE_OTHER = 0x5A;
    fftsPlusSqe.subType = argsHandleList_.empty() ? TASK_TYPE_OTHER : TASK_TYPE_AIV_AIC;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::ConstructFftsTask(rtFftsPlusTaskInfo_t &task, rtFftsPlusSqe_t &fftsPlusSqe)
{
    task.fftsPlusSqe = &fftsPlusSqe;
    task.descBuf = fftsCtxsPtr_->contexts.data();
    task.descBufLen = sizeof(rtFftsPlusComCtx_t) * fftsCtxsPtr_->ctxNum;
    task.descAddrType = 0;
    if (!argsHandleList_.empty()) {
        task.argsHandleInfoNum = argsHandleList_.size();
        task.argsHandleInfoPtr = argsHandleList_.data();
        argsHandleList_.clear();
    }
    return HCCL_SUCCESS;
}

// Check whether task has been launched already, should reset ffts context
bool FftsDispatcher::FftsCtxReady()
{
    return fftsCtxsPtr_->completed;
}

// Increase ctx size by 2x if size exceeded to ensure enough memory to add new ctx
void FftsDispatcher::EnsureFftsContextsSize()
{
    if (fftsCtxsPtr_->refreshIndex >= fftsCtxsPtr_->contexts.size()) {
        // The context space is insufficient. Twice of the current context number is applied for.
        const size_t TWICE = 2;
        fftsCtxsPtr_->contexts.resize(fftsCtxsPtr_->contexts.size() * TWICE);
    }
    return;
}

// Add remote notify record entry (context).
HcclResult FftsDispatcher::InitFftsDescNotifyRecordRemote(HcclRtSignal signal, uint64_t notifyAddr)
{
    EnsureFftsContextsSize();
    rtFftsPlusComCtx_t &comCtx = fftsCtxsPtr_->contexts[fftsCtxsPtr_->refreshIndex];
    int ret = memset_s(&comCtx, sizeof(rtFftsPlusComCtx_t), 0, sizeof(rtFftsPlusComCtx_t));
    if (ret != EOK) {
        LOG(ERROR) << "memset_s filed.";
        return HCCL_E_RUNTIME;
    }

    rtFftsPlusWriteValueCtx_t *ctx = reinterpret_cast<rtFftsPlusWriteValueCtx_t *>(&comCtx);

    ConstructFftsNotifyRecordRemoteCtx(signal, ctx, notifyAddr);

    fftsCtxsPtr_->refreshIndex++;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::ConstructFftsNotifyRecordRemoteCtx(HcclRtSignal &signal, rtFftsPlusWriteValueCtx_t *ctx,
                                                              uint64_t notifyAddr)
{
    (void)signal;
    const uint32_t shift = 32;
    const uint8_t awSize = 2;      // 2: write 4 bytes
    const uint32_t resNotify = 4;  // 4: notify record identifier
    ctx->contextType = RT_CTX_TYPE_WRITE_VALUE;
    ctx->threadDim = 1;
    ctx->awSize = awSize;
    ctx->res11 = resNotify;
    ctx->writeAddressBaseL = notifyAddr & UINT64_LOW_MASK;
    ctx->writeAddressBaseH = (notifyAddr & UINT64_HIGH_MASK) >> shift;
    ctx->writeValue[0] = 1;  // index 1: byte0~3
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::InitFftsDescNotifyWait(HcclRtSignal signal, uint32_t notifyId)
{
    EnsureFftsContextsSize();
    rtFftsPlusComCtx_t &comCtx = fftsCtxsPtr_->contexts[fftsCtxsPtr_->refreshIndex];
    int ret = memset_s(&comCtx, sizeof(rtFftsPlusComCtx_t), 0, sizeof(rtFftsPlusComCtx_t));
    if (ret != EOK) {
        LOG(ERROR) << "memset_s filed.";
        return HCCL_E_RUNTIME;
    }

    rtFftsPlusNotifyCtx_t *ctx = reinterpret_cast<rtFftsPlusNotifyCtx_t *>(&comCtx);

    ConstructFFtsNotifyWaitCtx(signal, ctx, notifyId);

    fftsCtxsPtr_->refreshIndex++;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::ConstructFFtsNotifyWaitCtx(HcclRtSignal &signal, rtFftsPlusNotifyCtx_t *ctx,
                                                      uint32_t notifyId)
{
    (void)signal;
    return ConstructFftsNotifyCtx(notifyId, RT_CTX_TYPE_NOTIFY_WAIT, ctx);
}

HcclResult FftsDispatcher::ConstructFftsNotifyCtx(uint32_t notifyID, uint16_t contextType, rtFftsPlusNotifyCtx_t *ctx)
{
    ctx->contextType = contextType;
    ctx->threadDim = 1;
    ctx->notifyIdBase = notifyID;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::InitFftsDescSdma(void *dst, const void *src, uint64_t cnt, uint32_t sdmaSqeHeader)
{
    EnsureFftsContextsSize();
    rtFftsPlusComCtx_t &comCtx = fftsCtxsPtr_->contexts[fftsCtxsPtr_->refreshIndex];
    int ret = memset_s(&comCtx, sizeof(rtFftsPlusComCtx_t), 0, sizeof(rtFftsPlusComCtx_t));
    if (ret != EOK) {
        LOG(ERROR) << "memset_s filed.";
        return HCCL_E_RUNTIME;
    }

    rtFftsPlusSdmaCtx_t *ctx = reinterpret_cast<rtFftsPlusSdmaCtx_t *>(&comCtx);

    ConstructFftsSdmaCtx(dst, src, cnt, sdmaSqeHeader, ctx);

    fftsCtxsPtr_->refreshIndex++;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::ConstructFftsSdmaCtx(void *dst, const void *src, uint64_t cnt, uint32_t sdmaSqeHeader,
                                                rtFftsPlusSdmaCtx_t *ctx)
{
    ctx->threadDim = 1;

    ctx->contextType = (cnt == 0) ? RT_CTX_TYPE_LABEL : RT_CTX_TYPE_SDMA;
    ctx->sdmaSqeHeader = sdmaSqeHeader;
    const uint32_t shift = 32;

    ctx->sourceAddressBaseL = reinterpret_cast<uint64_t>(src) & UINT64_LOW_MASK;
    ctx->sourceAddressBaseH = (reinterpret_cast<uint64_t>(src) & UINT64_HIGH_MASK) >> shift;

    ctx->destinationAddressBaseL = reinterpret_cast<uint64_t>(dst) & UINT64_LOW_MASK;
    ctx->destinationAddressBaseH = (reinterpret_cast<uint64_t>(dst) & UINT64_HIGH_MASK) >> shift;

    ctx->nonTailDataLength = cnt;
    ctx->tailDataLength = cnt;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::InitFftsDescRdmaSend(uint32_t dbindex, uint64_t dbinfo)
{
    EnsureFftsContextsSize();
    rtFftsPlusComCtx_t &comCtx = fftsCtxsPtr_->contexts[fftsCtxsPtr_->refreshIndex];
    int ret = memset_s(&comCtx, sizeof(rtFftsPlusComCtx_t), 0, sizeof(rtFftsPlusComCtx_t));
    if (ret != EOK) {
        LOG(ERROR) << "memset_s filed.";
        return HCCL_E_RUNTIME;
    }

    rtFftsPlusWriteValueCtx_t *ctx = reinterpret_cast<rtFftsPlusWriteValueCtx_t *>(&comCtx);

    ConstructFftsWriteValueCtx(dbindex, dbinfo, ctx);

    fftsCtxsPtr_->refreshIndex++;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::ConstructFftsWriteValueCtx(uint32_t dbindex, uint64_t dbinfo, rtFftsPlusWriteValueCtx_t *ctx)
{
    const uint32_t shift = 32;
    const uint8_t awSize = 3;        // 3: write 8 Bytes
    const uint32_t resRdmaSend = 2;  // rdma send
    uint64_t dbAddr = 0;

    if (!IsInvalidRdmaParam(dbindex, dbinfo)) {
        CHK_RET(hrtGetRdmaDoorbellAddr(devLogID_, chipId_, dbindex, dbAddr));
    }

    ctx->contextType = IsInvalidRdmaParam(dbindex, dbinfo) ? RT_CTX_TYPE_LABEL : RT_CTX_TYPE_WRITE_VALUE;
    ctx->threadDim = 1;
    ctx->awSize = awSize;
    ctx->res11 = resRdmaSend;
    ctx->writeAddressBaseL = dbAddr & UINT64_LOW_MASK;
    ctx->writeAddressBaseH = (dbAddr & UINT64_HIGH_MASK) >> shift;
    ctx->writeValue[0] = dbinfo & UINT64_LOW_MASK;              // index 0: byte0~3
    ctx->writeValue[1] = (dbinfo & UINT64_HIGH_MASK) >> shift;  // index 1: byte4~8
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::InitFftsDescMemcpy(void *dst, const void *src, uint64_t size)
{
    CHK_RET(InitFftsDescSdma(dst, src, size, SDMA_FP32_ATOMIC_MOVE_SQE));
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::AddTaskDependency(uint32_t predecessorId, uint32_t successorId)
{
    if (FftsCtxReady()) {
        LOG(ERROR) << "Launch has already been called for the current context";
        return HCCL_E_RUNTIME;
    }

    if (predecessorId >= fftsCtxsPtr_->contexts.size() || successorId >= fftsCtxsPtr_->contexts.size()) {
        LOG(ERROR) << "predecessorId: " << predecessorId << " or successorId: " << successorId
                   << " larger than contexts size: " << fftsCtxsPtr_->contexts.size();
        return HCCL_E_RUNTIME;
    }

    fftsCtxsPtr_->contexts[predecessorId].successorList[fftsCtxsPtr_->contexts[predecessorId].successorNum] =
        successorId;
    fftsCtxsPtr_->contexts[predecessorId].successorNum++;
    fftsCtxsPtr_->contexts[successorId].predCntInit++;
    fftsCtxsPtr_->contexts[successorId].predCnt++;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::SignalRecordCrossChip(HcclRtSignal signal, uint32_t *taskId, uint64_t notifyAddr)
{
    if (FftsCtxReady()) {
        LOG(ERROR) << "Launch has already been called for the current context";
        return HCCL_E_RUNTIME;
    }

    CHK_RET(InitFftsDescNotifyRecordRemote(signal, notifyAddr));
    if (fftsCtxsPtr_->refreshIndex == 0) {
        LOG(ERROR) << "refreshIndex is 0";
        return HCCL_E_RUNTIME;
    }

    *taskId = fftsCtxsPtr_->refreshIndex - 1;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::SignalWaitCrossChip(HcclRtSignal signal, uint32_t *taskId, uint32_t notifyId)
{
    if (FftsCtxReady()) {
        LOG(ERROR) << "Launch has already been called for the current context";
        return HCCL_E_RUNTIME;
    }

    CHK_RET(InitFftsDescNotifyWait(signal, notifyId));
    if (fftsCtxsPtr_->refreshIndex == 0) {
        LOG(ERROR) << "refreshIndex is 0";
        return HCCL_E_RUNTIME;
    }

    *taskId = fftsCtxsPtr_->refreshIndex - 1;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::MemcpyAsync(void *dst, const void *src, uint64_t size, uint32_t *taskId)
{
    if (FftsCtxReady()) {
        LOG(ERROR) << "Launch has already been called for the current context";
        return HCCL_E_RUNTIME;
    }

    CHK_RET(InitFftsDescMemcpy(dst, src, size));
    if (fftsCtxsPtr_->refreshIndex == 0) {
        LOG(ERROR) << "refreshIndex is 0";
        return HCCL_E_RUNTIME;
    }

    *taskId = fftsCtxsPtr_->refreshIndex - 1;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::RdmaSend(uint32_t dbindex, uint64_t dbinfo, const struct send_wr &wr, uint32_t *taskId)
{
    (void)wr;
    if (FftsCtxReady()) {
        LOG(ERROR) << "Launch has already been called for the current context";
        return HCCL_E_RUNTIME;
    }

    CHK_RET(RdmaSendInternal(dbindex, dbinfo));
    if (fftsCtxsPtr_->refreshIndex == 0) {
        LOG(ERROR) << "refreshIndex is 0";
        return HCCL_E_RUNTIME;
    }
    *taskId = fftsCtxsPtr_->refreshIndex - 1;
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::RdmaSendInternal(uint32_t dbindex, uint64_t dbinfo)
{
    CHK_RET(InitFftsDescRdmaSend(dbindex, dbinfo));
    return HCCL_SUCCESS;
}

bool FftsDispatcher::IsInvalidRdmaParam(uint32_t dbindex, uint64_t dbinfo)
{
    return dbindex == INVALID_UINT && dbinfo == INVALID_S64;
}

HcclResult FftsDispatcher::RtFftsPlusTaskLaunchWithFlag(rtFftsPlusTaskInfo_t *fftsPlusTaskInfo, rtStream_t stm,
                                                        uint32_t flag)
{
    (void)flag;
    CHK_PTR_NULL(fftsPlusTaskInfo);
    CHK_PTR_NULL(stm);
    // When the fftsplus task is delivered, two parameters. 0: task info, 1: stream handle need to be entered.
    const size_t INPUT_SIZE = 2;
    uintptr_t input[INPUT_SIZE];
    input[0] = reinterpret_cast<uintptr_t>(fftsPlusTaskInfo);
    input[1] = reinterpret_cast<uintptr_t>(stm);
    auto ret = aclDeviceManager_->RtGeneralCtrl(input, INPUT_SIZE, RT_GNL_CTRL_TYPE_FFTS_PLUS);
    if (ret.IsError()) {
        LOG(ERROR) << "RtGeneralCtrl failed with " << ret.GetMsg();
        return HCCL_E_RUNTIME;
    }
    return HCCL_SUCCESS;
}

HcclResult FftsDispatcher::RtGetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t &val)
{
    auto ret = aclDeviceManager_->RtGetDeviceInfo(deviceId, moduleType, infoType, reinterpret_cast<int64_t *>(&val));
    if (ret.IsError()) {
        LOG(ERROR) << "RtGetDeviceInfo failed with " << ret.GetMsg();
        return HCCL_E_RUNTIME;
    }
    return HCCL_SUCCESS;
}
}  // namespace ffts
}  // namespace datasystem
#endif
