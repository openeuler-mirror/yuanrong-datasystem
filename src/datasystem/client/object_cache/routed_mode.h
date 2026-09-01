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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_ROUTED_MODE_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_ROUTED_MODE_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/object_cache/bound_mode.h"
#include "datasystem/client/object_cache/object_client_impl.h"

namespace datasystem {
namespace object_cache {

constexpr size_t SET_ROUTE_MAX_ATTEMPTS = 3;
constexpr size_t STALE_LOCATION_REFRESH_ATTEMPTS = 5;

// lc=false data-plane implementation split out of ObjectClientImpl; dependencies are
// injected (same pattern as BoundMode) so unit tests can construct it with mocks.
class RoutedMode {
public:
    // WorkerNode/SetRouteContext/SetFailureStage/MSetRouteGroup come from client_mode_types.h.

    struct Deps {
        std::unique_ptr<client::TransportLayer> &transportLayer_;
        std::shared_ptr<client::Routing> &routing_;
        const int32_t &requestTimeoutMs_;
        const std::string &tenantId_;
        const client::DataPlacementPolicy &dataPlacementPolicy_;
        const std::shared_ptr<const SensitiveValue> &transportToken_;
        const std::shared_ptr<ThreadPool> &memoryCopyThreadPool_;
        const std::shared_ptr<ThreadPool> &asyncGetRPCPool_;
        const uint64_t &memcpyParallelThreshold_;
        const int &parallismNum_;
        const std::shared_ptr<ThreadPool> &setMemoryCopyThreadPool_;
        const uint64_t &setMemcpyParallelThreshold_;
        BoundMode *boundMode_;
        WorkerFailover *failover_;
    };

    struct HostServices {
        std::function<std::shared_ptr<ObjectClientImpl>()> getSelf;
        std::function<std::string()> getClientId;
        std::function<client::TransportRequestContext(const SetRouteContext &)> buildTransportRequestContext;
        std::function<Status(const HostPort &, SetRouteContext &)> buildSetRouteContext;
        std::function<std::vector<HostPort>(const std::vector<HostPort> &)> mergeWriteTargetExclusions;
        std::function<Status(const std::string &, const std::vector<HostPort> &, SetRouteContext &)> selectSetRoute;
        std::function<bool(StatusCode)> shouldRefreshRoutingAfterFailure;
        std::function<bool(const Status &, SetFailureStage, const HostPort &, std::vector<HostPort> &, const bool &)>
            handleSetRouteFailure;
    };

    explicit RoutedMode(const Deps &deps, const HostServices &host);

    ~RoutedMode() = default;

    void HandleDirectGetFailure(const std::shared_ptr<IClientWorkerApi> &workerApi, const Status &status);

    // Routed two-step Create/Publish (Component D). When local cache is off, allocate the buffer
    // on the hash-ring-selected worker via the transport layer and bridge the result to a legacy
    // Buffer; seal it via the transport layer on the worker pinned at Create time. The one-step
    // equivalents are ProcessTransportPut (Create+Set) below.
    Status CreateRoutedBuffer(const std::string &objectKey, uint64_t dataSize, const FullParam &param,
                              std::shared_ptr<Buffer> &buffer);

    // Routed two-step MultiCreate (lc=false batch). Allocates buffers on hash-ring-selected workers
    // via transportLayer_->MCreate and bridges each ObjectBufferInfo to a legacy Buffer at its
    // original key index. Mirrors CreateRoutedBuffer for the batch case.
    Status MultiCreateRouted(const std::vector<std::string> &objectKeyList,
                             const std::vector<uint64_t> &dataSizeList, const FullParam &param,
                             std::vector<std::shared_ptr<Buffer>> &bufferList, std::vector<bool> &exists);

    // Build the route context for one worker, call transportLayer_->MCreate, and bridge each
    // returned ObjectBuffer to a legacy Buffer at its original key index. Extracted from
    // MultiCreateRouted to keep each function within the codecheck size limit.
    Status ProcessRoutedMCreateGroup(const HostPort &worker, const std::vector<std::string> &keys,
                                     const std::vector<uint64_t> &sizes, const FullParam &param,
                                     const std::unordered_map<std::string, size_t> &keyIndex,
                                     std::vector<std::shared_ptr<Buffer>> &bufferList);
    Status ProcessTransportPut(const std::string &objectKey, const uint8_t *data, uint64_t size,
                               const FullParam &param, const std::unordered_set<std::string> &nestedObjectKeys,
                               uint32_t ttlSecond, int existence, const SetRouteContext &routeContext,
                               SetFailureStage &failureStage, client::TransportSetResult &transportResult,
                               int32_t requestTimeoutMs, bool isSeal = false);
    void BuildTransportReadRequest(const std::vector<std::string> &objectKeys, client::ObjectReadRequest &request,
                                   std::vector<Status> &itemStatuses, int64_t subTimeoutMs,
                                   bool queryL2Cache);
    Status BuildTransportGetResponse(
        client::ObjectReadItemResult &item, GetRspPb &response,
        std::unordered_map<std::string, std::shared_ptr<ObjectBufferInfo>> &ubBufferInfos, uint64_t &payloadSize);
    Status MaterializeTransportItem(const std::string &objectKey, client::ObjectReadItemResult &item,
                                    std::shared_ptr<Buffer> &buffer);
    Status ApplyTransportReadResult(const std::vector<std::string> &objectKeys,
                                    const client::ObjectReadRequest &request, client::ObjectReadResult &result,
                                    const Status &transportStatus, std::vector<std::shared_ptr<Buffer>> &buffers,
                                    std::vector<Status> &itemStatuses, AccessTransportKind &actualKind);
    Status FinishTransportRead(const std::vector<Status> &itemStatuses, AccessTransportKind actualKind,
                               const Status &transportStatus);
    Status ReadTransportRound(const std::vector<std::string> &objectKeys, bool traceEnabled, int64_t subTimeoutMs,
                              bool queryL2Cache, std::vector<std::shared_ptr<Buffer>> &buffers,
                              std::vector<Status> &itemStatuses, AccessTransportKind &actualKind,
                              Status &transportStatus);
    Status GetFromTransportLayer(const std::vector<std::string> &objectKeys,
                                 std::vector<std::shared_ptr<Buffer>> &buffers, bool traceEnabled,
                                 int64_t subTimeoutMs, bool queryL2Cache);
    Status CheckMultiSetInputParamValidationNtx(const std::vector<std::string> &keys,
                                                const std::vector<StringView> &vals,
                                                std::vector<std::string> &outFailedKeys,
                                                std::vector<std::string> &deduplicateKeys,
                                                std::vector<StringView> &deduplicateVals);
    Status MemoryCopyTransportMSetBuffers(const MSetRouteGroup &group,
                                          const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                          uint64_t dataSizeSum);
    Status BuildMSetRouteGroups(const std::vector<std::string> &keys, const std::vector<StringView> &values,
                                std::vector<MSetRouteGroup> &groups);
    Status ProcessTransportMSet(const MSetRouteGroup &group, const MSetParam &param,
                                const SetRouteContext &routeContext, client::TransportMSetResult &result,
                                SetFailureStage &failureStage, PerfPoint &point);
    Status BuildMSetRetryRouteGroups(const MSetRouteGroup &group, const std::vector<HostPort> &excludedWorkers,
                                     std::vector<MSetRouteGroup> &groups);
    Status ExecuteTransportMSetRetryGroups(const std::vector<MSetRouteGroup> &groups, const MSetParam &param,
                                           const std::vector<HostPort> &excludedWorkers, size_t attempt,
                                           std::vector<std::string> &outFailedKeys, PerfPoint &point);
    Status ExecuteTransportMSetGroupAttempt(const MSetRouteGroup &group, const MSetParam &param,
                                            std::vector<HostPort> excludedWorkers, size_t attempt,
                                            std::vector<std::string> &outFailedKeys, PerfPoint &point);
    Status ExecuteTransportMSetGroup(const MSetRouteGroup &group, const MSetParam &param,
                                     std::vector<std::string> &outFailedKeys, PerfPoint &point);
    Status MSetThroughTransport(const std::vector<std::string> &keys, const std::vector<StringView> &values,
                                const MSetParam &param, std::vector<std::string> &outFailedKeys, PerfPoint &point);
    Status RunExist(std::shared_ptr<client::Routing> routing, std::shared_ptr<IClientWorkerApi> &workerApi,
                    const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryL2Cache,
                    const bool isLocal, const SensitiveValue &token);

private:
    std::unique_ptr<client::TransportLayer> &transportLayer_;
    std::shared_ptr<client::Routing> &routing_;
    const int32_t &requestTimeoutMs_;
    const std::string &tenantId_;
    const client::DataPlacementPolicy &dataPlacementPolicy_;
    const std::shared_ptr<const SensitiveValue> &transportToken_;
    const std::shared_ptr<ThreadPool> &memoryCopyThreadPool_;
    const std::shared_ptr<ThreadPool> &asyncGetRPCPool_;
    const uint64_t &memcpyParallelThreshold_;
    const int &parallismNum_;
    const std::shared_ptr<ThreadPool> &setMemoryCopyThreadPool_;
    const uint64_t &setMemcpyParallelThreshold_;
    BoundMode *boundMode_;
    WorkerFailover *failover_;
    HostServices host_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_ROUTED_MODE_H
