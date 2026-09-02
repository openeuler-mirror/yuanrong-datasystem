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

/** Description: Defines batched object metadata access used by transport operation flows. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_METADATA_OBJECT_METADATA_CLIENT_H
#define DATASYSTEM_CLIENT_TRANSPORT_METADATA_OBJECT_METADATA_CLIENT_H

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/client/object_cache/transport/common/deadline_retry.h"
#include "datasystem/client/object_cache/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/object_cache/transport/data_plane/ub_transporter.h"
#include "datasystem/client/object_cache/transport/transport_advisor.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"

namespace datasystem {
namespace client {
/** @brief Mutable metadata state for one object-read item. */
struct ObjectMetadataItem {
    std::string objectKey;
    Status status = Status(K_NOT_READY, "Object metadata is not resolved");
    master::ObjectLocationInfoPb location;
    std::optional<DataGetResult> inlineData;
};

/** @brief Non-owning metadata batch; pointed items must outlive the query. */
using ObjectMetadataBatch = std::vector<ObjectMetadataItem *>;

class ObjectMetadataClient {
public:
    ObjectMetadataClient(std::shared_ptr<DataPlaneManager> manager, std::shared_ptr<DeadlineRetry> retry,
                         std::shared_ptr<TransportAdvisor> advisor = nullptr,
                         std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider = nullptr,
                         uint64_t ubBufferSize = 0,
                         std::function<void(const HostPort &, const Status &)> metadataFailureHandler = {});
    virtual ~ObjectMetadataClient() = default;

    /**
     * @brief Query a metadata-owner Worker for ordered locations and optional inline data.
     * @param[in] address Initial metadata owner.
     * @param[in,out] items Ordered object metadata states belonging to address.
     * @param[in] readContext Read context required by the shared-memory transport.
     * @return K_OK after every item has an independent result; a group-wide error otherwise.
     */
    virtual Status QueryAndGet(const HostPort &address, const ObjectMetadataBatch &items,
                               std::shared_ptr<const TransportReadContext> readContext);

    /** @brief Query object locations without requesting inline object data. */
    virtual Status QueryMetadata(const HostPort &address, const ObjectMetadataBatch &items);

private:
    enum class InlineTransportMode : uint8_t { NONE = 0, TCP = 1, UB = 2, SHM = 3 };

    struct InlineRequestContext {
        InlineTransportMode mode = InlineTransportMode::NONE;
        std::unordered_map<ObjectMetadataItem *, UbReceiveBuffer> ubBuffers;
        std::string transportInstanceId;
        std::shared_ptr<ShmTransporter> shmTransporter;
        std::shared_ptr<ShmSession> shmSession;
        std::shared_ptr<const TransportReadContext> readContext;

        /** @brief Disable inline transfer and release prepared receive buffers. */
        void DisableInlineData()
        {
            mode = InlineTransportMode::NONE;
            ubBuffers.clear();
            transportInstanceId.clear();
            shmTransporter.reset();
            shmSession.reset();
            readContext.reset();
        }
    };

    Status Query(const HostPort &address, const ObjectMetadataBatch &items, bool enableInlineData,
                 std::shared_ptr<const TransportReadContext> readContext = nullptr);

    Status QueryWithRetry(const HostPort &address, const ObjectMetadataBatch &items,
                          QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads,
                          InlineRequestContext &context);
    Status BuildQueryRequest(const HostPort &address, const ObjectMetadataBatch &items,
                             InlineRequestContext &context, QueryAndGetReqPb &request) const;
    Status InvokeQueryAndGet(const HostPort &address, QueryAndGetReqPb &request,
                             QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads,
                             InlineRequestContext &context, bool &rpcDispatched);
    Status InvokeInlineQueryAndGet(const HostPort &address, QueryAndGetReqPb &request,
                                   QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads,
                                   InlineRequestContext &context, bool &invoked, bool &rpcDispatched);
    Status InvokeTcpQueryAndGet(const HostPort &address, QueryAndGetReqPb &request,
                                QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads,
                                bool &rpcDispatched);
    void SwitchInlineRequestToTcp(QueryAndGetReqPb &request, std::vector<RpcMessage> &payloads,
                                  InlineRequestContext &context) const;
    Status PrepareQueryRetry(const HostPort &address, const ObjectMetadataBatch &items, const Status &rc,
                             bool rpcDispatched, InlineRequestContext &context, int64_t &backoffMs);
    void DelayReleaseUbBuffers(InlineRequestContext &context, const Status &reason,
                               const std::string &reasonSource) const;
    bool HandleUbTransportStatus(ObjectMetadataItem &item, const QueryAndGetResultPb &result,
                                 InlineRequestContext &context) const;

    /**
     * @brief Select and initialize the inline transport for one metadata-owner request.
     * @param[in] address Metadata-owner address.
     * @param[in] items Ordered object metadata items.
     * @param[out] context Prepared inline-request context.
     * @return K_OK on success; the error code otherwise.
     */
    Status InitializeInlineRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                   std::shared_ptr<const TransportReadContext> readContext,
                                   InlineRequestContext &context) const;

    Status PrepareShmInlineRequest(const HostPort &address,
                                   std::shared_ptr<const TransportReadContext> readContext,
                                   InlineRequestContext &context) const;

    /** @brief Replace an unavailable SHM inline request with an available UB or TCP request. */
    Status PrepareShmInlineFallback(const HostPort &address, const ObjectMetadataBatch &items,
                                    InlineRequestContext &context) const;

    /**
     * @brief Prepare UB inline transfer when the endpoint and client configuration support it.
     * @param[in] address Metadata-owner address.
     * @param[in] items Ordered object metadata items.
     * @param[in,out] context Inline-request context.
     * @return K_OK on success or when UB is unavailable; the error code otherwise.
     */
    Status PrepareUbInlineRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                  InlineRequestContext &context) const;

    /**
     * @brief Allocate one UB receive buffer for each object metadata item.
     * @param[in] items Ordered object metadata items.
     * @param[in,out] context Inline-request context holding allocated buffers.
     * @return K_OK on success; the error code otherwise.
     */
    Status AllocateUbInlineBuffers(const ObjectMetadataBatch &items, InlineRequestContext &context) const;

    /**
     * @brief Encode the selected inline transport and receive buffers into a QueryAndGet request.
     * @param[in] items Ordered object metadata items.
     * @param[in] context Prepared inline-request context.
     * @param[in,out] request QueryAndGet request.
     * @return K_OK on success; the error code otherwise.
     */
    Status AddInlineDataRequest(const ObjectMetadataBatch &items, const InlineRequestContext &context,
                                QueryAndGetReqPb &request) const;

    /**
     * @brief Apply ordered metadata and inline-data results to a metadata batch.
     * @param[in,out] items Ordered object metadata items.
     * @param[in] response QueryAndGet response.
     * @param[in,out] payloads RPC payloads returned with the response.
     * @param[in,out] context Inline-request context.
     * @return K_OK on success; the error code otherwise.
     */
    Status ApplyResults(const ObjectMetadataBatch &items, const QueryAndGetRspPb &response,
                        std::vector<RpcMessage> &payloads, InlineRequestContext &context) const;

    /**
     * @brief Apply one QueryAndGet result to its object metadata item.
     * @param[in,out] item Object metadata item.
     * @param[in] result Per-object QueryAndGet result.
     * @param[in,out] payloads RPC payloads returned with the response.
     * @param[in,out] context Inline-request context.
     * @return K_OK on success; the error code otherwise.
     */
    Status ApplyResult(ObjectMetadataItem &item, const QueryAndGetResultPb &result,
                       std::vector<RpcMessage> &payloads, InlineRequestContext &context) const;

    /**
     * @brief Move TCP inline payloads into a data-read result.
     * @param[in] dataResult Inline-data payload indexes.
     * @param[in,out] payloads RPC payloads returned with the response.
     * @param[out] data Data-read result.
     * @return K_OK on success; the error code otherwise.
     */
    Status BuildTcpInlineData(const QueryAndGetDataResultPb &dataResult, uint64_t objectSize,
                              std::vector<RpcMessage> &payloads, DataGetResult &data) const;

    Status BuildShmInlineData(ObjectMetadataItem &item, const QueryAndGetShmInfoPb &shmInfo,
                              InlineRequestContext &context, DataGetResult &data) const;

    /**
     * @brief Move ownership of a prepared UB receive buffer into a data-read result.
     * @param[in,out] item Object metadata item owning the buffer association.
     * @param[in] location Object location and size metadata.
     * @param[in,out] context Inline-request context.
     * @param[out] data Data-read result.
     * @return K_OK on success; the error code otherwise.
     */
    Status BuildUbInlineData(ObjectMetadataItem &item, const master::ObjectLocationInfoPb &location,
                             InlineRequestContext &context, DataGetResult &data) const;

    std::shared_ptr<DataPlaneManager> manager_;
    std::shared_ptr<DeadlineRetry> retry_;
    std::shared_ptr<TransportAdvisor> advisor_;
    std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider_;
    uint64_t ubBufferSize_ = 0;
    std::function<void(const HostPort &, const Status &)> metadataFailureHandler_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_METADATA_OBJECT_METADATA_CLIENT_H
