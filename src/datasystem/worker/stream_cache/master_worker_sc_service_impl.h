/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_MASTER_WORKER_SC_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_STREAM_CACHE_MASTER_WORKER_SC_SERVICE_IMPL_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/stream_cache/consumer_meta.h"
#include "datasystem/protos/worker_stream.service.rpc.pb.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/stream_manager.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class MasterWorkerSCServiceImpl : public MasterWorkerSCService {
public:
    /**
     * @brief Construct MasterWorkerSCServiceImpl that is used to provide service for master.
     * @param[in] serverAddr The worker address.
     * @param[in] masterAddr The master address.
     * @param[in] clientSvc The pointer of client call worker service.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    MasterWorkerSCServiceImpl(HostPort serverAddr, HostPort masterAddr, ClientWorkerSCServiceImpl *clientSvc,
                              std::shared_ptr<AkSkManager> akSkManager);
    ~MasterWorkerSCServiceImpl() override = default;

    /**
     * @brief Init the service.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Synchronize all remote pub node for target stream to current worker node.
     * Invoked when first consumer occurs on current node.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status SyncPubNode(const SyncPubNodeReqPb &req, SyncPubNodeRspPb &rsp) override;

    /**
     * @brief Synchronize all remote consumer node for target stream to current worker node.
     * Invoked when first producer occurs on current node.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status SyncConsumerNode(const SyncConsumerNodeReqPb &req, SyncConsumerNodeRspPb &rsp) override;

    /**
     * @brief Clear all remote pub node for target stream on current worker node.
     * Invoked when last consumer disappears within current node.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ClearAllRemotePub(const ClearRemoteInfoReqPb &req, ClearRemoteInfoRspPb &rsp) override;

    /**
     * @brief Clear all remote consumer node for target stream on current worker node.
     * Invoked when last producer disappears within current node.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ClearAllRemoteConsumer(const ClearRemoteInfoReqPb &req, ClearRemoteInfoRspPb &rsp) override;

    /**
     * @brief Delete stream context broadcast to this worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status DelStreamContext(const DelStreamContextReqPb &req, DelStreamContextRspPb &rsp) override;

    /**
     * @brief Query meta data for all streams from worker.
     * @param[in, out] stream The server reader writer session.
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryMetadata(
        std::shared_ptr<ServerWriterReader<GetStreamMetadataRspPb, GetMetadataAllStreamReqPb>> stream) override;

    /**
     * @brief Query meta data from worker. This version simply populates the rsp for all the streams for to a master.
     * @param[in] req The metadata request to lookup
     * @param[out] rsp The metadata response populated with the results
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryMetadata(const GetMetadataAllStreamReqPb &req, GetMetadataAllStreamRspPb &rsp);

    /**
     * @brief Master notify worker to update Topo.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateTopoNotification(const UpdateTopoNotificationReq &req, UpdateTopoNotificationRsp &rsp) override;

    /**
     * @brief Get log prefix
     * @param[in] withAddress whether to log with address, the default is false.
     * @return Return log prefix
     */
    [[nodiscard]] std::string LogPrefix(bool withAddress = false) const;

private:
    /**
     * @brief Synchronize remote consumer to remoteWorkerManager_.
     * @param[in] streamManager Target stream
     * @param[in] remoteConsumerSet remote consumer set obtained in CreateProducer process.
     * @param[in] lastAckCursor The last ack cursor.
     * @return K_OK on success; the error code otherwise.
     */
    Status SyncRemoteConsumer(const std::shared_ptr<StreamManager> &streamManager,
                              const std::vector<ConsumerMeta> &remoteConsumerSet, uint64_t lastAckCursor);

    /**
     * @brief Master notify worker add remote consumer.
     * @param[in] streamManager The stream manager.
     * @param[in] consumerMeta The consumer metadata.
     * @return K_OK on success; the error code otherwise.
     */
    Status AddRemoteConsumer(const std::shared_ptr<StreamManager> &streamManager, const ConsumerMetaPb &consumerMeta);

    /**
     * @brief Master notify worker delete remote consumer.
     * @param[in] streamManager The stream manager.
     * @param[in] consumerMeta The consumer metadata.
     * @return K_OK on success; the error code otherwise.
     */
    Status DelRemoteConsumer(const std::shared_ptr<StreamManager> &streamManager, const ConsumerMetaPb &consumerMeta);

    HostPort localWorkerAddress_;
    HostPort masterAddress_;

    ClientWorkerSCServiceImpl *clientWorkerSCSvc_;
    std::shared_ptr<AkSkManager> akSkManager_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_STREAM_CACHE_MASTER_WORKER_SC_SERVICE_IMPL_H
