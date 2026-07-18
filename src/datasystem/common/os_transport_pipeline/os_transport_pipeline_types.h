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

/**
 * Description: define pipeline h2d types
 */

#ifndef OS_XPRT_PIPLN_TYPES
#define OS_XPRT_PIPLN_TYPES

#include <algorithm>
#include <cstring>
#include <sstream>
#ifdef BUILD_PIPLN_H2D
#include <ub/umdk/urma/urma_api.h>
#endif

#define CHUNKTAG_REQID_START_INDEX 0
#define CHUNKTAG_REQID_LEN 10
#define CHUNKTAG_TYPE_START_INDEX 10
#define CHUNKTAG_TYPE_LEN 1
#define CHUNKTAG_ID_START_INDEX 11
#define CHUNKTAG_ID_LEN 4
#define CHUNKTAG_SIZE_START_INDEX 15
#define CHUNKTAG_SIZE_LEN 1
#define CHUNKTAG_RESERVE_INDEX 16
#define CHUNKTAG_RESERVE_LEN 48

enum PiplnDoneStep {
    PIPLN_DONE_NO_STEP = 0,
    PIPLN_DONE_ONE_STEP = 1,
    PIPLN_DONE_TWO_STEP = 2,
    PIPLN_DONE_THREE_STEP = 3
};

namespace OsXprtPipln {

enum TargetDeviceType : int32_t { CUDA = 0, MOCK = 99 };

struct DevShmInfo {
    TargetDeviceType devType;
    uint32_t devId;
    void *ptr;
    size_t size;
    // Optional opaque H2D stream handle. For CUDA, this is cudaStream_t cast to void *.
    // nullptr means RH2D should create an internal stream and wait for copy completion.
    void *h2dStream = nullptr;
};

struct ChunkTag {
    static inline constexpr int lastChunkTag = 0x1;
    static inline constexpr uint32_t chunkSize2MB = 2 * 1024 * 1024;
    uint64_t reqId : 10;
    uint64_t chunkType : 1;
    uint64_t chunkId : 4;
    uint64_t chunkSize : 1;
    uint64_t rsv : 48;  // temporary used for object total value size

    static inline uint64_t GetReservePart(ChunkTag tag)
    {
        return tag.rsv;
    }

    static inline void SetReservePart(ChunkTag &tag, uint64_t rsv)
    {
        tag.rsv = rsv;
    }

    static inline uint64_t GetRange(uint64_t tag, int start, int length)
    {
        return ((tag >> start) & (((uint64_t)1 << length) - 1));
    }

    static inline void SetRange(uint64_t &target, uint64_t tag, int start, int length)
    {
        target |= (tag & (((uint64_t)1 << length) - 1)) << start;
    }

    static inline ChunkTag FromUint64(uint64_t num)
    {
        ChunkTag tag{};
        tag.reqId = GetRange(num, CHUNKTAG_REQID_START_INDEX, CHUNKTAG_REQID_LEN);
        tag.chunkType = GetRange(num, CHUNKTAG_TYPE_START_INDEX, CHUNKTAG_TYPE_LEN);
        tag.chunkId = GetRange(num, CHUNKTAG_ID_START_INDEX, CHUNKTAG_ID_LEN);
        tag.chunkSize = GetRange(num, CHUNKTAG_SIZE_START_INDEX, CHUNKTAG_SIZE_LEN);
        tag.rsv = 0;
        return tag;
    }

    static inline uint64_t ToUint64(ChunkTag tag)
    {
        uint64_t ret = 0;
        SetRange(ret, tag.reqId, CHUNKTAG_REQID_START_INDEX, CHUNKTAG_REQID_LEN);
        SetRange(ret, tag.chunkType, CHUNKTAG_TYPE_START_INDEX, CHUNKTAG_TYPE_LEN);
        SetRange(ret, tag.chunkId, CHUNKTAG_ID_START_INDEX, CHUNKTAG_ID_LEN);
        SetRange(ret, tag.chunkSize, CHUNKTAG_SIZE_START_INDEX, CHUNKTAG_SIZE_LEN);
        return ret;
    }

    static inline bool IsLastChunk(ChunkTag tag)
    {
        return tag.chunkType == lastChunkTag;
    }

    static inline void SetIsLastChunk(ChunkTag &tag)
    {
        tag.chunkType = lastChunkTag;
    }

    static inline uint32_t DecodeChunkSize(ChunkTag tag)
    {
        (void)tag;
        return chunkSize2MB;
    }

    static inline uint32_t ResolveChunkSize(ChunkTag tag, uint64_t totalSize)
    {
        const uint32_t baseChunkSize = chunkSize2MB;
        if (!IsLastChunk(tag)) {
            return baseChunkSize;
        }
        const uint64_t offset = baseChunkSize * tag.chunkId;
        if (totalSize <= offset) {
            return baseChunkSize;
        }
        return static_cast<uint32_t>(std::min<uint64_t>(baseChunkSize, totalSize - offset));
    }

    static inline std::string DebugString(ChunkTag tag, uint32_t chunkSize = 0)
    {
        std::stringstream ss;
        ss << "isLast:" << (IsLastChunk(tag)) << " idx:" << tag.chunkId
           << " size:" << (chunkSize ? chunkSize : tag.chunkSize) << " reqId:" << tag.reqId << " rsv:" << tag.rsv;
        return ss.str();
    }
};

#ifdef BUILD_PIPLN_H2D
struct PiplnSndArgs {
    urma_jetty_t *jetty;
    urma_target_jetty_t *tjetty;
    uint64_t localAddr;
    uint64_t remoteAddr;
    urma_target_seg_t *localSeg;
    urma_target_seg_t *remoteSeg;
    uint64_t len;
    uint32_t serverKey;
    uint32_t clientKey;
};
#else
struct PiplnSndArgs {
    void *jetty;
    void *tjetty;
    uint64_t localAddr;
    uint64_t remoteAddr;
    void *localSeg;
    void *remoteSeg;
    uint64_t len;
    uint32_t serverKey;
    uint32_t clientKey;
};
#endif
}  // namespace OsXprtPipln

#define PIPLN_DEBUG_LOG_DATA_LENGTH 40
#define PIPLN_DEBUG_LOG_DATA_LEVEL 3
#define PIPLN_DEBUG_LOG_DATA(tag, key, reqId, shmUnit, dataOffset, size)                                    \
    if (FLAGS_v > PIPLN_DEBUG_LOG_DATA_LEVEL) {                                                             \
        std::string buf;                                                                                    \
        char *dataSrc = ((char *)(shmUnit)->GetPointer()) + (size_t)(dataOffset);                           \
        buf.resize(PIPLN_DEBUG_LOG_DATA_LENGTH + 1);                                                        \
        std::memcpy(&buf[0], dataSrc, std::min<size_t>(PIPLN_DEBUG_LOG_DATA_LENGTH, (size)));               \
        LOG(INFO) << PIPLN_LOG_PREFIX "[" << (tag) << "] " << (key) << "(reqId:" << (reqId) << ") pointer " \
                  << (shmUnit)->GetPointer() << " shmUnit " << (shmUnit).get() << " shmOffset "             \
                  << (shmUnit)->GetOffset() << " data " << buf;                                             \
    }

#define PIPLN_DEBUG_LOG_DATA_RAW(tag, key, reqId, dataSrc, size)                                            \
    if (FLAGS_v > PIPLN_DEBUG_LOG_DATA_LEVEL) {                                                             \
        std::string buf;                                                                                    \
        buf.resize(PIPLN_DEBUG_LOG_DATA_LENGTH + 1);                                                        \
        std::memcpy(&buf[0], (char *)(dataSrc), std::min<size_t>(PIPLN_DEBUG_LOG_DATA_LENGTH, (size)));     \
        LOG(INFO) << PIPLN_LOG_PREFIX "[" << (tag) << "] " << (key) << "(reqId:" << (reqId) << ") pointer " \
                  << (void *)(dataSrc) << " data " << buf;                                                  \
    }

#endif