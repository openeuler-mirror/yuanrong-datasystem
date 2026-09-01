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
 * Description: BoundMode split out of ObjectClientImpl (lc=true data plane).
 */

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_BOUND_MODE_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_BOUND_MODE_H

#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#include "re2/re2.h"

#include "datasystem/client/mmap_manager/mmap_manager.h"
#include "datasystem/client/object_cache/client_memory_ref_table.h"
#include "datasystem/client/object_cache/object_client_impl.h"

namespace datasystem {
namespace object_cache {

// lc=true data-plane implementation split out of ObjectClientImpl.
// Dependencies are injected (no back-reference to the impl object) so unit tests can
// construct a BoundMode with mocks directly; member names match the former impl members
// so the migrated bodies keep their original access expressions.

constexpr double US_PER_MS = 1000.0;
void ComputeDataSizes(const std::vector<StringView> &vals, std::vector<uint64_t> &sizes, uint64_t &sum);

struct PipelineAsyncResource {
    std::future<Status> rpcFuture;
    std::promise<AsyncResult> promise;
    PiplnRh2dParam piplnRh2dParam;
};

class BoundMode {
public:
    // WorkerNode/SetRouteContext/SetFailureStage come from client_mode_types.h (namespace-scope).

    // Behavior dependencies on the host (ObjectClientImpl) that stay outside BoundMode.
    struct HostServices {
        std::function<std::shared_ptr<ObjectClientImpl>()> getSelf;
        std::function<Status()> isClientReady;
        std::function<Status()> checkConnection;
        std::function<Status()> checkConnWhileShmModify;
        std::function<bool(uint32_t)> isBufferAlive;
        std::function<Status(const std::shared_ptr<ObjectBufferInfo> &, const std::unordered_set<std::string> &,
                             bool)>
            publishRoutedBuffer;
        std::function<bool(const Status &, SetFailureStage, const HostPort &, std::vector<HostPort> &)>
            handleSetRouteFailure;
        std::function<void(const std::shared_ptr<IClientWorkerApi> &, const Status &)> handleDirectGetFailure;
        std::function<bool(const Status &)> isRoutingEvictionFailure;
        std::function<Status(HostPort &)> getCurrentWorkerHostPort;
        std::function<Status(const std::vector<std::string> &, const std::vector<uint64_t> &, const FullParam &,
                             const bool, std::vector<std::shared_ptr<Buffer>> &, std::vector<bool> &)>
            multiCreate;
        std::function<void(const std::vector<std::string> &, client::ObjectReadRequest &,
                           std::vector<Status> &, int64_t, bool)>
            buildTransportReadRequest;
        std::function<Status(const std::vector<std::shared_ptr<Buffer>> &, const MultiPublishRspPb &)>
            handleShmRefCountAfterMultiPublish;
        std::function<Status(const std::vector<std::string> &, const std::vector<Blob> &,
                             std::vector<std::shared_ptr<Buffer>> &, void *, std::vector<std::string> &)>
            runClientDirectPipelineRH2D;
        std::function<Status(std::promise<AsyncResult> &, PiplnRh2dParam &, GetRspPb &,
                             std::vector<std::shared_ptr<Buffer>> &)>
            postPipelineRH2D;
    };

    struct Deps {
        std::vector<std::shared_ptr<IClientWorkerApi>> &workerApi;
        std::unique_ptr<client::MmapManager> &mmapManager;
        ClientMemoryRefTable *memoryRefCount;
        TbbGlobalRefTable *globalRefCount;
        std::shared_timed_mutex *globalRefMutex;
        std::unique_ptr<client::TransportLayer> &transportLayer;
        std::shared_ptr<client::Routing> &routing;
        const std::shared_ptr<ThreadPool> &memoryCopyThreadPool;
        const std::shared_ptr<ThreadPool> &asyncReleasePool;
        const std::shared_ptr<ThreadPool> &asyncGetRPCPool;
        const std::shared_ptr<ThreadPool> &asyncPipelineRH2DPool;
        re2::RE2 &simpleIdRe;
        WorkerFailover *failover;
        std::shared_timed_mutex &shutdownMux;
        const std::atomic<WorkerNode> &currentNode;
        const int32_t &requestTimeoutMs;
        const std::string &tenantId;
        const SensitiveValue &token;
        const bool &enableLocalCache;
        const bool &enableClientDirectPipelineH2D;
        const int &parallismNum;
        std::function<Status(std::shared_ptr<IClientWorkerApi> &, std::unique_ptr<Raii> &)> getWorkerApi;
        std::function<Status(std::shared_ptr<IClientWorkerApi> &, std::unique_ptr<Raii> &, WorkerNode &)>
            getWorkerApiNode;
        HostServices host;
    };

    explicit BoundMode(const Deps &deps);
    ~BoundMode() = default;

    std::shared_ptr<ObjectBufferInfo> MakeUbPoolBufferInfo(const std::string &objectKey, uint64_t dataSize,
                                                           const FullParam &param, uint32_t version,
                                                           const ShmKey &shmId);
    Status CreateShmBuffer(const std::string &objectKey, uint64_t dataSize, const FullParam &param,
                           const std::shared_ptr<IClientWorkerApi> &workerApi, const LatencyTraceConfig &config,
                           bool traceEnabled, std::shared_ptr<Buffer> &newBuffer);
    Status ConstructMultiCreateParam(const std::vector<std::string> &objectKeyList,
                                     const std::vector<uint64_t> &dataSizeList,
                                     std::vector<std::shared_ptr<Buffer>> &bufferList,
                                     std::vector<MultiCreateParam> &multiCreateParamList, uint64_t &dataSizeSum);
    void BatchDecreaseRefCnt(const std::vector<std::pair<ShmKey, std::uint32_t>> &shmInfos);
    void DecreaseReferenceCnt(const ShmKey &shmId, bool isShm, uint32_t version = 0);
    Status DecreaseReferenceCntImpl(const ShmKey &shmId, bool isShm, uint32_t version);
    Status Seal(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                const std::unordered_set<std::string> &nestedObjectKeys, bool isShm);
    Status Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                   const std::unordered_set<std::string> &nestedObjectKeys, bool isShm);
    Status SendBufferViaUb(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data, uint64_t length,
                          bool traceEnabled);
    Status SendBufferViaUbFromPool(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data,
                                   uint64_t length, bool traceEnabled);
    Status InvalidateBuffer(const std::string &objectKey);
    Status TimedMmapLookupWithDeadline(const std::shared_ptr<ShmUnitInfo> &shmBuf, uint64_t size);
    Status TimedMemoryCopyWithDeadline(const std::shared_ptr<Buffer> &buffer, const uint8_t *data, uint64_t size,
                                       bool traceEnabled);
    Status ProcessShmPut(const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
                         const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond,
                         const std::shared_ptr<IClientWorkerApi> &workerApi, int existence,
                         SetFailureStage &failureStage, int32_t requestTimeoutMs);
    Status CheckLocalUbSenderAdmission(const std::shared_ptr<IClientWorkerApi> &workerApi) const;
    Status ProcessDirectSetWithoutTransport(const std::string &objectKey, const uint8_t *data, uint64_t size,
                                            const FullParam &param,
                                            const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond,
                                            int existence, const SetRouteContext &routeContext,
                                            SetFailureStage &failureStage, std::vector<HostPort> &excludedWorkers,
                                            int32_t requestTimeoutMs);
    Status CheckPipelineRH2DArgs(const std::vector<std::string> &objectKeys, const std::vector<Blob> &devBlob);
    Status CheckLocalPipelineRH2DArgs(std::shared_ptr<IClientWorkerApi> &workerApi);
    std::shared_future<AsyncResult> GetWithOsTransportPipeline(const std::vector<std::string> &objectKeys,
                                                               const std::vector<Blob> &devBlob,
                                                               std::vector<std::shared_ptr<Buffer>> &buffers,
                                                               void *h2dStream = nullptr);
    void BuildClientDirectRH2DReadRequest(const std::vector<std::string> &objectKeys,
                                          client::ObjectReadRequest &request,
                                          std::vector<Status> &itemStatuses, int64_t subTimeoutMs,
                                          bool queryL2Cache);
    Status RecoverWorkerAndRetryGet(const std::shared_ptr<IClientWorkerApi> &workerApi, GetParam &getParam,
                                    WorkerNode workerNode, const std::vector<std::string> &objectKeys,
                                    std::vector<std::shared_ptr<Buffer>> &buffers);
    Status GetFromLocalWorker(const std::vector<std::string> &objectKeys, int64_t subTimeoutMs,
                              std::vector<std::shared_ptr<Buffer>> &buffers, bool queryL2Cache, bool isRH2DSupported,
                              int32_t requestTimeoutMs);
    Status SetShmObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info, uint32_t version,
                              std::shared_ptr<Buffer> &buffer);
    Status MmapShmUnit(int64_t fd, uint64_t mmapSize, ptrdiff_t offset,
                       std::shared_ptr<client::IMmapTableEntry> &mmapEntry, uint8_t *&pointer);
    static std::shared_ptr<ObjectBufferInfo> MakeObjectBufferInfo(
        const std::string &objectKey, uint8_t *pointer, uint64_t size, uint64_t metaSize, const FullParam &param,
        bool isSeal, uint32_t version, const ShmKey &shmId = {},
        const std::shared_ptr<RpcMessage> &payloadPointer = nullptr,
        std::shared_ptr<client::IMmapTableEntry> mmapEntry = nullptr,
        std::shared_ptr<RemoteH2DHostInfoPb> remoteHostInfo = nullptr);
    Status GetBuffersFromWorker(std::shared_ptr<IClientWorkerApi> workerApi, GetParam &getParam,
                                std::vector<std::shared_ptr<Buffer>> &buffers);
    Status GetBuffersFromWorkerBatched(std::shared_ptr<IClientWorkerApi> workerApi, const GetParam &getParam,
                                       std::vector<std::shared_ptr<Buffer>> &buffers,
                                       const std::vector<ObjMetaInfo> &objMetas, uint64_t ubMaxGetSize,
                                       AccessTransportKind *requestTransportKind);
    Status GetOversizedBufferFromWorkerByChunks(std::shared_ptr<IClientWorkerApi> workerApi, const GetParam &getParam,
                                                size_t objectIndex, uint64_t objectSize, uint64_t ubMaxGetSize,
                                                std::shared_ptr<Buffer> &buffer,
                                                AccessTransportKind *requestTransportKind);
    Status GetOversizedBufferChunk(std::shared_ptr<IClientWorkerApi> workerApi, const GetParam &getParam,
                                   const std::string &objectKey, uint64_t offset, uint64_t chunkSize,
                                   std::shared_ptr<Buffer> &chunkBuffer, uint32_t &version,
                                   AccessTransportKind *requestTransportKind);
    Status CopyOversizedBufferChunk(const std::string &objectKey, uint64_t objectSize, uint64_t offset,
                                    const std::shared_ptr<Buffer> &chunkBuffer, std::shared_ptr<Buffer> &buffer,
                                    uint64_t &copiedSize);
    Status ProcessGetResponse(const std::vector<std::string> &objectKeys, const std::vector<ReadParam> &readParams,
                              GetRspPb &rsp, uint32_t version, std::vector<RpcMessage> &payloads,
                              std::vector<std::shared_ptr<Buffer>> &buffers, std::vector<std::string> &failedObjectKey,
                              const std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>>
                                  &ubBufferInfos = {});
    Status GetObjectBuffers(const std::vector<std::string> &objectsNeedToGet, const GetRspPb &rsp, uint32_t version,
                            const std::vector<ReadParam> &readParams, std::vector<RpcMessage> &payloads,
                            std::vector<std::shared_ptr<Buffer>> &buffers, std::vector<std::string> &failedObjectKey,
                            const std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>>
                                &ubBufferInfos = {});
    Status SetShmObjectBufferWithMetric(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info,
                                        uint32_t version, const std::vector<ReadParam> &readParams, size_t index,
                                        std::shared_ptr<Buffer> &bufferPtr);
    Status SetNoShmObjectBufferWithMetric(const std::string &objectKey, const GetRspPb::PayloadInfoPb &payloadInfo,
                                          uint32_t version, std::vector<RpcMessage> &payloads,
                                          const std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>>
                                              &ubBufferInfos,
                                          std::shared_ptr<Buffer> &bufferPtr);
    Status SetRemoteHostObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info, uint32_t version,
                                     std::shared_ptr<Buffer> &buffer);
    Status SetNonShmObjectBuffer(const std::string &objectKey, const GetRspPb::PayloadInfoPb &payloadInfo, int version,
                                 std::vector<RpcMessage> &payloads, std::shared_ptr<Buffer> &bufferPtr);
    Status SetOffsetReadObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info, uint32_t version,
                                     uint64_t offset, uint64_t size, std::shared_ptr<Buffer> &buffer);
    Status GIncreaseRef(const std::vector<std::string> &firstIncIds, std::vector<std::string> &failedObjectKeys,
                        const std::string &remoteClientId = "");
    std::string ConstructObjKeyWithTenantId(const std::string &objKey);
    void GIncreaseRefRollback(const std::vector<std::string> &rollbackObjectKeys,
                              std::map<std::string, GlobalRefInfo> &accessorTable);
    Status ReleaseGRefs(const std::string &remoteClientId);
    Status GDecreaseRef(const std::vector<std::string> &finishDecIds, std::vector<std::string> &failedObjectKeys,
                        const std::string &remoteClientId = "");
    void GDecreaseRefRollback(const std::vector<std::string> &rollbackObjectKeys,
                              std::map<std::string, GlobalRefInfo> &accessorTable);
    void RemoveZeroGlobalRefByRefTable(const std::vector<std::string> &checkIds,
                                       std::map<std::string, GlobalRefInfo> &accessorTable);
    void AddTbbLockForGlobalRefIds(const std::vector<std::string> &objectKeys,
                                   std::map<std::string, GlobalRefInfo> &accessorTable,
                                   std::unordered_map<std::string, std::string> &objTenantIdsToObj);
    Status MutiCreateParallel(const bool skipCheckExistence, const FullParam &param, const uint32_t &version,
                              std::vector<bool> &exists, std::vector<MultiCreateParam> &multiCreateParamList,
                              std::vector<std::shared_ptr<Buffer>> &bufferList);
    Status CreateBufferForMultiCreateParamAtIndex(size_t index, bool skipCheckExistence, const FullParam &param,
                                                  uint32_t version, const std::vector<bool> &exists,
                                                  std::vector<MultiCreateParam> &multiCreateParamList,
                                                  std::vector<std::shared_ptr<Buffer>> &bufferList);
    Status MemoryCopyParallel(bool isParallel, const std::vector<std::string> &keys,
                              const std::vector<StringView> &vals, const FullParam &createParam,
                              std::vector<std::shared_ptr<Buffer>> &bufferList,
                              std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfoList,
                              AccessTransportKind *requestTransportKind = nullptr);
    Status MemoryCopyParallelWithDeadline(bool isParallel, const std::vector<std::string> &keys,
                                          const std::vector<StringView> &vals, const FullParam &createParam,
                                          std::vector<std::shared_ptr<Buffer>> &bufferList,
                                          std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfoList,
                                          uint64_t dataSizeSum, AccessTransportKind *requestTransportKind);
    Status MSetCreateCopyAndPublish(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                                    const std::vector<std::string> &deduplicateKeys,
                                    const std::vector<StringView> &deduplicateVals, const MSetParam &param,
                                    const std::shared_ptr<IClientWorkerApi> &workerApi,
                                    std::vector<std::string> &outFailedKeys, PerfPoint &point);

private:
    std::vector<std::shared_ptr<IClientWorkerApi>> &workerApi_;
    std::unique_ptr<client::MmapManager> &mmapManager_;
    ClientMemoryRefTable *memoryRefCount_;
    TbbGlobalRefTable *globalRefCount_;
    std::shared_timed_mutex *globalRefMutex_;
    std::unique_ptr<client::TransportLayer> &transportLayer_;
    std::shared_ptr<client::Routing> &routing_;
    const std::shared_ptr<ThreadPool> &memoryCopyThreadPool_;
    const std::shared_ptr<ThreadPool> &asyncReleasePool_;
    const std::shared_ptr<ThreadPool> &asyncGetRPCPool_;
    const std::shared_ptr<ThreadPool> &asyncPipelineRH2DPool_;
    re2::RE2 &simpleIdRe_;
    WorkerFailover *failover_;
    std::shared_timed_mutex &shutdownMux_;
    const std::atomic<WorkerNode> &currentNode_;
    const int32_t &requestTimeoutMs_;
    const std::string &tenantId_;
    const SensitiveValue &token_;
    const bool &enableLocalCache_;
    const bool &enableClientDirectPipelineH2D_;
    const int &parallismNum_;
    std::function<Status(std::shared_ptr<IClientWorkerApi> &, std::unique_ptr<Raii> &)> getWorkerApi_;
    std::function<Status(std::shared_ptr<IClientWorkerApi> &, std::unique_ptr<Raii> &, WorkerNode &)>
        getWorkerApiNode_;
    HostServices host_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_BOUND_MODE_H
