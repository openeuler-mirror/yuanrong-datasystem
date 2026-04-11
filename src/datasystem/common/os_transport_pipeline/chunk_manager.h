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
 * Description: manage the splited segment chunks, use driver to implement device share memory IPC.
 */

#ifndef OS_XPRT_PIPLN_CHUNK_MANAGER
#define OS_XPRT_PIPLN_CHUNK_MANAGER

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <ub/umdk/urma/urma_api.h>

#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_types.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
namespace OsXprtPipln {

using datasystem::Status;
using datasystem::StatusCode;

class IpcDriver {
public:
    IpcDriver(const DevShmInfo &devInfo, const std::string &targetHandle, bool isClient)
        : devId(devInfo.devId),
          targetAddr(devInfo.ptr),
          targetSize(devInfo.size),
          targetHandle(targetHandle),
          targetType(devInfo.devType),
          isClient(isClient)
    {
    }
    virtual ~IpcDriver()
    {
    }
    virtual Status EncodeDriver() = 0;
    virtual Status DecodeDriver() = 0;
    virtual Status SubmitIO(void *srcData, size_t srcSize, size_t destOffset) = 0;
    virtual Status WaitIO() = 0;
    virtual Status Release() = 0;

    static Status GetDriver(const DevShmInfo &devInfo, const std::string &targetHandle, bool isClient,
                            std::shared_ptr<IpcDriver> &driver);

public:
    uint32_t devId;
    void *targetAddr;
    size_t targetSize;
    std::string targetHandle;
    TargetDeviceType targetType;
    bool isClient;

protected:
    static inline size_t ReservedHandleSize()
    {
        return sizeof(devId);
    };
    void *GetEncodeHandle(size_t handleSize);
    void *GetDecodeHandle(size_t handleSize);
#define DEFINE_ENCODE_HANDLE(type) type *handle = (type *)GetEncodeHandle(sizeof(type))
#define DEFINE_DECODE_HANDLE(type) type *handle = (type *)GetDecodeHandle(sizeof(type))
};

struct ReqInfo {
    int32_t receivedChunks = 0;
    int32_t failedChunkId = 0;
    void *syncHandle = nullptr;
    std::string key;
    std::shared_ptr<IpcDriver> driver;
    bool isCanceled = false;
};

class ChunkManager {
public:
    ChunkManager(bool isClient) : isClient_(isClient)
    {
    }
    ~ChunkManager()
    {
        ReleaseAll();
    }

    Status AddKey(const std::string &key, uint32_t reqId, const DevShmInfo &devInfo, const std::string &targetHandle);
    ReqInfo *GetReqInfo(uint32_t reqId);
    Status ReceiveRemoteChunk(ChunkTag tagId, void *srcData);
    Status ReceiveLocalChunk(uint32_t reqId, void *srcData, size_t srcSize);
    Status ReleaseAll();
    Status GetReqId(const std::string &key, uint32_t &reqId);
    static uint32_t GenerateReqId();
    int32_t KeyNum() const
    {
        return reqInfos.size();
    };

    static Status InitOsPiplnH2DEnv(urma_context_t *ctx, urma_jfc_t *jfc, urma_jfce_t *jfce, int threadNum);
    static void UnInitOsPiplnH2DEnv();
    Status StartReceiver(uint32_t reqId, uint64_t src, uint64_t size, urma_target_seg_t *targetSeg,
                         urma_jfr_t *targetJfr);
    Status StartSender(PiplnSndArgs &args);

    Status WaitAll();
    Status WaitPipelineDone();
    void Cancel(uint32_t ReqId);
    void CancelAll();

    // @return: is my event ?
    static int ReceiveEventHook(urma_cr_t *cr);

private:
    Status SubmitIO(ReqInfo &info, void *srcData, size_t srcSize, size_t destOffset);
    static std::atomic<uint32_t> reqId_;
    bool isClient_;
    static void *osPiplnH2DHandle_;
    std::unordered_map<uint32_t, ReqInfo> reqInfos;
    std::unordered_map<std::string, uint32_t> keyToReqIdMap;
    bool canceled = false;
};

}  // namespace OsXprtPipln

#endif