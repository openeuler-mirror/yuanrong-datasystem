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
#define CHUNKTAG_TYPE_START_INDEX 0
#define CHUNKTAG_TYPE_LEN 2
#define CHUNKTAG_ID_START_INDEX 2
#define CHUNKTAG_ID_LEN 6
#define CHUNKTAG_SIZE_START_INDEX 8
#define CHUNKTAG_SIZE_LEN 24
#define CHUNKTAG_REQID_START_INDEX 32
#define CHUNKTAG_REQID_LEN 32

namespace OsXprtPipln {

enum TargetDeviceType : int32_t { CUDA = 0, MOCK = 99 };

struct DevShmInfo {
    TargetDeviceType devType;
    uint32_t devId;
    void *ptr;
    size_t size;
};

struct ChunkTag {
    static inline constexpr int lastChunkTag = 0x1;
    uint64_t chunkType : 2;
    uint64_t chunkId : 6;
    uint64_t chunkSize : 24;
    uint64_t reqId : 32;

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
        tag.chunkType = GetRange(num, CHUNKTAG_TYPE_START_INDEX, CHUNKTAG_TYPE_LEN);
        tag.chunkId = GetRange(num, CHUNKTAG_ID_START_INDEX, CHUNKTAG_ID_LEN);
        tag.chunkSize = GetRange(num, CHUNKTAG_SIZE_START_INDEX, CHUNKTAG_SIZE_LEN);
        tag.reqId = GetRange(num, CHUNKTAG_REQID_START_INDEX, CHUNKTAG_REQID_LEN);
        return tag;
    }

    static inline uint64_t ToUint64(ChunkTag tag)
    {
        uint64_t ret = 0;
        SetRange(ret, tag.chunkType, CHUNKTAG_TYPE_START_INDEX, CHUNKTAG_TYPE_LEN);
        SetRange(ret, tag.chunkId, CHUNKTAG_ID_START_INDEX, CHUNKTAG_ID_LEN);
        SetRange(ret, tag.chunkSize, CHUNKTAG_SIZE_START_INDEX, CHUNKTAG_SIZE_LEN);
        SetRange(ret, tag.reqId, CHUNKTAG_REQID_START_INDEX, CHUNKTAG_REQID_LEN);
        return ret;
    }

    static inline bool IsLastChunk(ChunkTag tag)
    {
        return (tag.chunkType & lastChunkTag) != 0;
    }
};

#ifdef BUILD_PIPLN_H2D
#include <ub/umdk/urma/urma_api.h>
struct PiplnSndArgs {
    urma_jfs_t *jfs;
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
    void *jfs;
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

#endif