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

/** Description: Defines Worker-side metadata-affine QueryAndGet processing. */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_QUERY_AND_GET_IMPL_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_QUERY_AND_GET_IMPL_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"

namespace datasystem {
namespace object_cache {

/** @brief Reads resident Worker objects inline and returns metadata-only locations for misses. */
class WorkerQueryAndGetImpl {
public:
    /**
     * @brief Construct the Worker QueryAndGet processor.
     * @param[in] getProc Existing local-read and metadata-location service.
     * @param[in] memoryRefTable Worker shared-memory reference table.
     * @param[in] akSkManager Worker authentication manager.
     * @param[in] localAddress Local Worker address.
     * @param[in] ubAdmission Worker UB admission state.
     */
    WorkerQueryAndGetImpl(std::shared_ptr<WorkerOcServiceGetImpl> getProc,
                          std::shared_ptr<SharedMemoryRefTable> memoryRefTable,
                          std::shared_ptr<AkSkManager> akSkManager, HostPort localAddress,
                          std::shared_ptr<PeerUbAdmission> ubAdmission);

    /**
     * @brief Destroy the Worker QueryAndGet processor.
     */
    ~WorkerQueryAndGetImpl() = default;

    /**
     * @brief Process one ordered QueryAndGet request.
     * @param[in] serverApi Server RPC reader and writer.
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryAndGet(
        std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> &serverApi);

private:
    struct QueryStats {
        size_t inlineHits = 0;
        size_t misses = 0;
        uint64_t dataSize = 0;
        uint64_t preprocessUs = 0;
        uint64_t localReadUs = 0;
        uint64_t metadataUs = 0;
        uint64_t deliveryUs = 0;
    };

    struct RequestState {
        QueryAndGetReqPb request;
        QueryAndGetRspPb response;
        std::vector<RpcMessage> payloads;
        std::vector<std::string> misses;
        std::vector<ShmKey> addedShmRefs;
        QueryStats stats;
        uint64_t startUs = 0;
        uint64_t lastCheckpointUs = 0;
        uint64_t tcpPayloadSize = 0;
        bool delivered = false;
    };

    Status ReadAndAuthenticate(
        const std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> &serverApi,
        QueryAndGetReqPb &request) const;
    Status ProcessAndDeliver(
        const std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> &serverApi,
        RequestState &state);
    Status BuildResponse(RequestState &state);
    Status DeliverResponse(
        const std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> &serverApi,
        RequestState &state) const;
    void InitializeResponse(RequestState &state) const;
    Status PrepareLocalResponse(RequestState &state);
    static uint64_t RecordPhase(RequestState &state);
    void RollbackShmRefs(const RequestState &state) const;
    void CollectStats(RequestState &state) const;
    void LogCompletion(const RequestState &state, const Status &rc, uint64_t totalUs) const;
    const char *GetTransportName(const QueryAndGetReqPb &request) const;
    Status ValidateRequest(const QueryAndGetReqPb &request) const;
    Status EncodeLocalHits(RequestState &state);
    Status EncodeLocalHit(RequestState &state, size_t index, const GetObjEntryParams &params, bool &encoded);
    Status EncodeTcp(const GetObjEntryParams &params, QueryAndGetDataResultPb &result,
                     RequestState &state, bool &encoded) const;
    Status EncodeUb(const QueryAndGetUbDataReqPb &request, size_t index, const GetObjEntryParams &params,
                    bool &encoded) const;
    void EncodeShm(const QueryAndGetShmDataReqPb &request, const GetObjEntryParams &params,
                   QueryAndGetDataResultPb &result, RequestState &state) const;
    Status FillMissLocations(RequestState &state) const;

    std::shared_ptr<WorkerOcServiceGetImpl> getProc_;
    std::shared_ptr<SharedMemoryRefTable> memoryRefTable_;
    std::shared_ptr<AkSkManager> akSkManager_;
    HostPort localAddress_;
    std::shared_ptr<PeerUbAdmission> ubAdmission_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_SERVICE_WORKER_QUERY_AND_GET_IMPL_H
