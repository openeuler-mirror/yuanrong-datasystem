/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: ffts plus task dispatcher
 * Author: limengjiao
 * Create: 2023-05-01
 */

#ifndef P2P_FFTS_COMMON_H
#define P2P_FFTS_COMMON_H

#include "npu/ffts/comm.h"
#include "runtime/rt_ffts_plus_define.h"
#include <cstdint>
#include <map>
#include <vector>
#include <memory>
#include <string>

namespace p2p {

constexpr int32_t NOTIFY_MAX_WAIT_TIME = 255 * 68;  // 非910B和910C场景notify wait最大等待时长，由硬件决定
constexpr int32_t NOTIFY_MAX_WAIT_TIME_V81 = 2147483647;  // 910B和910C场景notify wait最大等待时长，由软件实现
constexpr int32_t HCCL_EXEC_TIME_OUT_S =
    NOTIFY_MAX_WAIT_TIME;  // 910B和910C场景非HCCL默认的Notify wait超时时间设置为最大超时时间
constexpr int32_t HCCL_EXEC_TIME_OUT_S_V81 =
    NOTIFY_MAX_WAIT_TIME_V81;  // 910B和910C HCCL默认的Notify wait超时时间设置为最大超时时间

// V71: 910B, v81: 910c
// s32 HcclExecTimeout = (deviceType == DevType::DEV_TYPE_V81 || deviceType == DevType::DEV_TYPE_V71) ?\
//        HCCL_EXEC_TIME_OUT_S_V81 : HCCL_EXEC_TIME_OUT_S;

// ...|0000|1|1|1|1|0|0111|0001
constexpr uint32_t SDMA_FP32_ATOMIC_ADD_SQE = 0x1E71;
// 000|1|1|1|1|0|0111|0000
constexpr uint32_t SDMA_FP32_ATOMIC_MOVE_SQE = 0x1E70;

using FftsSdmaSqeHeader = union {
    uint32_t word;
    struct {
        uint32_t opcode : 4;    // 0:Non-atomic transfer 1:atomic add 2:atomic max
                                // 3:atomic min 4:atomic equal 5:memory set
                                // 6:L2Cache Preload 7:L2Cache Prewriteback
                                // 8:L2Cache invaild 9:L2Cache Flush
        uint32_t datatype : 4;  // 0:int8 1:int16 2:int32 6:fp16 normal 7:fp32
                                // 8:bf16 9:fp16 sat
        uint32_t ie2 : 1;       // Whether to report an interrupt after the operation
                                // is complete. HCCL is set to 0.
        uint32_t sssv : 1;      // Whether the substream ID corresponding to the
                                // source address is valid. HCCL is set to 1.
        uint32_t dssv : 1;      // Whether the substream ID corresponding to the
                                // destination address is valid. HCCL is set to 1.
        uint32_t sns : 1;       // Security attribute corresponding to the source
                                // address. The value can be 0 (secure) or 1
                                // (non-secure). HCCL is set to 1.
        uint32_t dns : 1;       // Security attribute corresponding to the destination
                                // address. The value can be 0 (secure) or 1
                                // (non-secure). HCCL is set to 1.
        uint32_t qos : 4;       // 0
        uint32_t sro : 1;       // 0
        uint32_t dro : 1;       // 0
        uint32_t partid : 8;    // 0
        uint32_t mpam : 1;      // 0
        uint32_t pmg : 2;       // 0
        uint32_t format : 1;    // 0
        uint32_t res6 : 1;      // 0
    } bit;
};

using HcclFftsContextsInfo = struct HcclFftsCtxsDef {
    bool completed = false;
    uint32_t refreshIndex = 0;  // Index for next rtFftsPlusComCtx_t in contexts
    uint32_t ctxNum = 0;
    std::vector<rtFftsPlusComCtx_t> contexts;
    HcclFftsCtxsDef()
    {
        contexts.resize(100);  // 100: number of contexts that can be stored by
                               // default, which can be dynamically expanded
    }
};

enum class ReduceType { INLINE_REDUCE = 0, TBE_REDUCE };

enum class CopyPattern { ZCOPY = 0, BCOPY };

using HcclOpMetaInfo = struct HcclOpMetaInfoDef {
    HcclCMDType opType = HcclCMDType::HCCL_CMD_INVALID;
    bool isRootRank = false;
    uint32_t rootRank;
    ReduceType reduceType = ReduceType::INLINE_REDUCE;
    CopyPattern copyPattern = CopyPattern::BCOPY;

    static HcclOpMetaInfoDef GetOneForAllReduce(ReduceType reduceType = ReduceType::INLINE_REDUCE)
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_ALLREDUCE;
        meta.reduceType = reduceType;
        return meta;
    }

    static HcclOpMetaInfoDef GetOneForAllGather()
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_ALLGATHER;
        return meta;
    }

    static HcclOpMetaInfoDef GetOneForBroadcast(bool isRootRank, uint32_t rootRank)
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_BROADCAST;
        meta.isRootRank = isRootRank;
        meta.rootRank = rootRank;
        return meta;
    }

    static HcclOpMetaInfoDef GetOneForReduceScatter()
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_REDUCE_SCATTER;
        return meta;
    }

    static HcclOpMetaInfoDef GetOneForAllToAllV(CopyPattern copyPattern)
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_ALLTOALLV;
        meta.copyPattern = copyPattern;
        return meta;
    }

    static HcclOpMetaInfoDef GetOneForAllToAllVC(CopyPattern copyPattern)
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_ALLTOALLVC;
        meta.copyPattern = copyPattern;
        return meta;
    }

    static HcclOpMetaInfoDef GetOneForSend()
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_SEND;
        return meta;
    }

    static HcclOpMetaInfoDef GetOneForRecieve()
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_RECEIVE;
        return meta;
    }

    static HcclOpMetaInfoDef GetOneForReduce(bool isRootRank, uint32_t rootRank)
    {
        HcclOpMetaInfoDef meta;
        meta.opType = HcclCMDType::HCCL_CMD_REDUCE;
        meta.isRootRank = isRootRank;
        meta.rootRank = rootRank;
        return meta;
    }

    std::string GetCacheKey() const
    {
        std::string isRootRankStr = isRootRank ? "1" : "0";
        return std::to_string(static_cast<int>(opType)) + isRootRankStr + std::to_string(static_cast<int>(reduceType))
               + std::to_string(rootRank);
    }
};
}  // namespace p2p
#endif  // P2P_FFTS_COMMON_H