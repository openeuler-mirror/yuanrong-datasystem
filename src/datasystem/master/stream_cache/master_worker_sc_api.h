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

/**
 * Description: The interface of file cache worker descriptor.
 */

#ifndef DATASYSTEM_MASTER_STREAM_CACHE_MASTER_WORKER_SC_API_H
#define DATASYSTEM_MASTER_STREAM_CACHE_MASTER_WORKER_SC_API_H

#include <set>
#include <vector>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/worker_stream.stub.rpc.pb.h"
namespace datasystem {
namespace worker {
namespace stream_cache {
class MasterWorkerSCServiceImpl;
}  // namespace stream_cache
}  // namespace worker
namespace master {

enum class MasterWorkerSCApiType : int { MasterLocalWorkerSCApi = 0, MasterRemoteWorkerSCApi = 1 };

/**
 * @brief The MasterWorkerSCApi is an abstract class that defines the interface for interactions with the stream cache
 * worker service.
 */
class MasterWorkerSCApi {
public:
    /**
     * Default destructor
     */
    virtual ~MasterWorkerSCApi() = default;

    /**
     * @brief Initialize the MasterWorkerSCApi Object(include rpc channel).
     * @return Status of the call.
     */
    virtual Status Init() = 0;

    /**
     * @brief The type id of MasterWorkerSCApi Object.
     * @return The type id of MasterWorkerSCApi Object.
     */
    virtual MasterWorkerSCApiType TypeId() = 0;

    /**
     * @brief Broadcast delete-stream to all related node for a stream.
     * @param[in] streamName Target stream.
     * @param[in] forceDelete Force deletion.
     * @return Status of the call.
     */
    virtual Status DelStreamContextBroadcast(const std::string &streamName, bool forceDelete) = 0;

    /**
     * @brief Async broadcast delete-stream to all related node for a stream.
     * @param[in] streamName Target stream.
     * @param[in] forceDelete Force deletion.
     * @param[out] tagId The async RPC tag id.
     * @return Status of the call.
     */
    virtual Status DelStreamContextBroadcastAsyncWrite(const std::string &streamName, bool forceDelete,
                                                       int64_t &tagId) = 0;

    /**
     * @brief Read the async delete-stream broadcast response.
     * @param[in] tagId The async RPC tag id.
     * @param[in] flags The RPC receive flag option.
     * @return Status of the call.
     */
    virtual Status DelStreamContextBroadcastAsyncRead(int64_t tagId, RpcRecvFlags flags) = 0;

    /**
     * @brief Sync all pub node(except src node) for target stream to src node.
     * @param[in] streamName Target stream name.
     * @param[in] pubNodeSet All pub node set.
     * @param[in] isRecon Is this part of reconciliation process.
     * @return Status of the call.
     */
    virtual Status SyncPubNode(const std::string &streamName, const std::set<HostPort> &pubNodeSet, bool isRecon) = 0;

    /**
     * @brief Sync all consumer node(except consumer generated from src node) for target stream to src node.
     * @param[in] streamName Target stream name.
     * @param[in] consumerMetas All consumer metadata list.
     * @param[in] retainData Ask Producers to retain data if needed
     * @param[in] isRecon Is this part of reconciliation
     * @return Status of the call.
     */
    virtual Status SyncConsumerNode(const std::string &streamName, const std::vector<ConsumerMetaPb> &consumerMetas,
                                    const RetainDataState::State retainData, bool isRecon) = 0;

    /**
     * @brief Clear all remote pub node for target stream on src node.
     * @param[in] streamName Target stream name.
     * @return Status of the call.
     */
    virtual Status ClearAllRemotePub(const std::string &streamName) = 0;

    /**
     * @brief The stream rpc used to query metadata in worker.
     * @param[in/out] stream The stream rpc reader writer.
     * @return Status of the call.
     */
    virtual Status QueryMetadata(
        std::unique_ptr<ClientWriterReader<GetMetadataAllStreamReqPb, GetStreamMetadataRspPb>> &stream) = 0;

    /**
     * @brief Notify worker the topo update.
     * @param[in] req The request send to worker.
     * @return Status of the call.
     */
    virtual Status UpdateTopoNotification(UpdateTopoNotificationReq &req) = 0;

    /**
     * @brief A factory method to instantiate the correct derived version of the api. Remote masters will use an
     * rpc-based api, whereas local masters can be optimized for in-process pointer based api.
     * @param[in] hostPort The host port of the target master
     * @param[in] localHostPort The local worker rpc service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] service The local pointer to the master SC service implementation. If null, the created api must
     * default to the RPC-based version.
     * @return A base class pointer to the correct derived type of api.
     */
    static std::shared_ptr<MasterWorkerSCApi> CreateMasterWorkerSCApi(
        const HostPort &hostPort, const HostPort &localHostPort, const std::shared_ptr<AkSkManager> &akSkManager,
        worker::stream_cache::MasterWorkerSCServiceImpl *service);

protected:
    /**
     * @brief Constructor, Create a new MasterWorkerSCApi object from master to a particular worker.
     * @param[in] localMasterAddress The source master address (local)
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    MasterWorkerSCApi(HostPort localMasterAddress, std::shared_ptr<AkSkManager> akSkManager);

    /**
     * @brief Get log prefix
     * @return The log prefix
     */
    [[nodiscard]] virtual std::string LogPrefix() const = 0;

    /**
     * @brief Construct synchronize sub consumer nodes table protobuf.
     * @param[in] streamName Related stream name.
     * @param[in] consumerMetas The set of all sub consumer nodes information.
     * @param[in] src The source worker node of new producer.
     * @param[in] retainData Ask producer to retain data if needed.
     * @param[in] isRecon Is this part of reconciliation process.
     * @param[out] req Pointer to sync sub consumer nodes table protobuf.
     */
    static void ConstructSyncConsumerNodePb(const std::string &streamName,
                                            const std::vector<ConsumerMetaPb> &consumerMetas, const HostPort &src,
                                            const RetainDataState::State retainData, bool isRecon,
                                            SyncConsumerNodeReqPb &req) noexcept;

    /**
     * @brief Construct synchronize pub worker nodes table protobuf.
     * @param[in] streamName Related stream name.
     * @param[in] pubTable The set of all pub worker nodes address.
     * @param[in] src The source worker node of subscription.
     * @param[in] isRecon Is this part of reconciliation process.
     * @param[out] req Pointer to sync pub worker nodes table protobuf.
     */
    static void ConstructSyncPubNodePb(const std::string &streamName, const std::set<HostPort> &pubTable,
                                       const HostPort &src, bool isRecon, SyncPubNodeReqPb &req) noexcept;

    HostPort localMasterAddress_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
};

/**
 * @brief MasterRemoteWorkerSCApi is the derived remote version of the api for sending and receiving worker SC requests
 * where the worker is on a different host. This class will use an RPC mechanism for communication to the remote
 * location.
 * Callers will access this class naturally through base class polymorphism.
 * See the parent interface for function argument documentation.
 */
class MasterRemoteWorkerSCApi : public MasterWorkerSCApi {
public:
    /**
     * @brief Constructor, Create a new MasterWorkerSCApi object from master to a particular worker.
     * @param[in] workerAddress The target worker node address
     * @param[in] localAddress The source address of this host
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    explicit MasterRemoteWorkerSCApi(HostPort workerAddress, const HostPort &localAddress,
                                     std::shared_ptr<AkSkManager> akSkManager);
    ~MasterRemoteWorkerSCApi() override = default;
    Status Init() override;
    MasterWorkerSCApiType TypeId() override;
    Status DelStreamContextBroadcast(const std::string &streamName, bool forceDelete) override;
    Status DelStreamContextBroadcastAsyncWrite(const std::string &streamName, bool forceDelete,
                                               int64_t &tagId) override;
    Status DelStreamContextBroadcastAsyncRead(int64_t tagId, RpcRecvFlags flags) override;
    Status SyncPubNode(const std::string &streamName, const std::set<HostPort> &pubNodeSe, bool isRecon) override;
    Status SyncConsumerNode(const std::string &streamName, const std::vector<ConsumerMetaPb> &consumerMetas,
                            const RetainDataState::State retainData, bool isRecon) override;
    Status ClearAllRemotePub(const std::string &streamName) override;
    Status QueryMetadata(
        std::unique_ptr<ClientWriterReader<GetMetadataAllStreamReqPb, GetStreamMetadataRspPb>> &stream) override;
    Status UpdateTopoNotification(UpdateTopoNotificationReq &req) override;

    Status ClearAllRemotePubAsynWrite(const std::string &streamName, int64_t &tagId);
    Status ClearAllRemotePubAsynRead(int64_t tagId, RpcRecvFlags flags);

private:
    /**
     * @brief Get log prefix
     * @return The log prefix
     */
    [[nodiscard]] std::string LogPrefix() const override;

    HostPort workerAddress_;
    std::shared_ptr<MasterWorkerSCService_Stub> rpcSession_{ nullptr };  // Session to the worker rpc service.
};

/**
 * @brief MasterLocalWorkerSCApi is the derived local version of the api for sending and receiving worker SC requests
 * where the worker exists in the same process as the service. This class will directly reference the service through a
 * pointer and does not use any RPC mechanism for communication.
 * Callers will access this class naturally through base class polymorphism.
 * See the parent interface for function argument documentation.
 */
class MasterLocalWorkerSCApi : public MasterWorkerSCApi {
public:
    /**
     * @brief Constructor, Create a new MasterWorkerSCApi object from master to a particular worker.
     * @param[in] service The direct pointer to the service for the requests.
     * @param[in] localAddress The source address of this host
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    explicit MasterLocalWorkerSCApi(worker::stream_cache::MasterWorkerSCServiceImpl *service,
                                    const HostPort &localAddress, std::shared_ptr<AkSkManager> akSkManager);
    ~MasterLocalWorkerSCApi() override = default;
    Status Init() override;
    MasterWorkerSCApiType TypeId() override;
    Status DelStreamContextBroadcast(const std::string &streamName, bool forceDelete) override;
    Status DelStreamContextBroadcastAsyncWrite(const std::string &streamName, bool forceDelete,
                                               int64_t &tagId) override;
    Status DelStreamContextBroadcastAsyncRead(int64_t tagId, RpcRecvFlags flags) override;
    Status SyncPubNode(const std::string &streamName, const std::set<HostPort> &pubNodeSet, bool isRecon) override;
    Status SyncConsumerNode(const std::string &streamName, const std::vector<ConsumerMetaPb> &consumerMetas,
                            const RetainDataState::State retainData, bool isRecon) override;
    Status ClearAllRemotePub(const std::string &streamName) override;
    Status QueryMetadata(
        std::unique_ptr<ClientWriterReader<GetMetadataAllStreamReqPb, GetStreamMetadataRspPb>> &stream) override;
    Status QueryMetadata(const GetMetadataAllStreamReqPb &req, GetMetadataAllStreamRspPb &rsp);
    Status UpdateTopoNotification(UpdateTopoNotificationReq &req) override;

private:
    /**
     * @brief Get log prefix
     * @return The log prefix
     */
    [[nodiscard]] std::string LogPrefix() const override;
    worker::stream_cache::MasterWorkerSCServiceImpl *workerSC_{ nullptr };
};

/**
 * @brief Convert HostPortPb to string.
 * @param hostPb The HostPortPb object.
 * @return The string of the HostPortPb.
 */
inline std::string HostPb2Str(const HostPortPb &hostPb) noexcept
{
    HostPort addr(hostPb.host(), hostPb.port());
    return addr.ToString();
}
}  // namespace master
}  // namespace datasystem

#endif  // DATASYSTEM_MASTER_STREAM_CACHE_MASTER_WORKER_SC_API_H
