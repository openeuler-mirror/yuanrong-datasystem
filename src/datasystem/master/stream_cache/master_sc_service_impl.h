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
 * Description: Implement the stream cache services on the master.
 */
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_MASTER_SC_SERVICE_IMPL_H
#define DATASYSTEM_MASTER_STREAM_CACHE_MASTER_SC_SERVICE_IMPL_H

#include <memory>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/master/stream_cache/master_worker_sc_api.h"
#include "datasystem/master/stream_cache/rpc_session_manager.h"
#include "datasystem/master/stream_cache/stream_metadata.h"
#include "datasystem/protos/master_stream.service.rpc.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
class ReplicaManager;

namespace master {
class MasterSCServiceImpl : public MasterSCService {
public:
    MasterSCServiceImpl(const HostPort &masterAddress, std::shared_ptr<AkSkManager> akSkManager,
                        ReplicaManager *replicaManager);
    MasterSCServiceImpl() = default;
    ~MasterSCServiceImpl() override = default;

    /**
     * @brief Shutdown the sc metadata manager module.
     */
    static void Shutdown();

    /**
     * @brief Initialize master service.
     * @return Status of call.
     */
    Status Init() override;

    /**
     * @brief Create a producer, i.e., register a publisher to a stream. Similar to worker::CreateProducer.
     * This version is a wrapper that will redirect the call to the CreateProducerImpl() function.
     * @param[in] serverApi Used to read request from rpc client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateProducer(
        std::shared_ptr<ServerUnaryWriterReader<CreateProducerRspPb, CreateProducerReqPb>> serverApi) override;

    /**
     * @brief The real work of the CreateProducer is started in this function.
     * @param[in] serverApi Used to read request from rpc client and write response to client. nullptr can be passed
     * here if the caller is directly calling this function (not through rpc).
     * @param[in] req The create producer request details.
     * @param[out] rsp The create producer response.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateProducerImpl(
        const std::shared_ptr<ServerUnaryWriterReader<CreateProducerRspPb, CreateProducerReqPb>> &serverApi,
        const CreateProducerReqPb &req, CreateProducerRspPb &rsp);

    /**
     * @brief Close a producer, force flushing and page seal, unregister a publisher to a stream.
     * Similar to worker::CloseProducer. This is a wrapper version that will redirect the call to the
     * CloseProducerImpl() function.
     * @param[in] serverApi Used to read request from rpc client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducer(
        std::shared_ptr<ServerUnaryWriterReader<CloseProducerRspPb, CloseProducerReqPb>> serverApi) override;

    /**
     * @brief The real work of the CloseProducer is started in this function.
     * @param[in] serverApi Used to read request from rpc client and write response to client. nullptr can be passed
     * here if the caller is directly calling this function (not through rpc).
     * @param[in] req The close consumer request details
     * @param[out] rsp The close consumer response details
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducerImpl(
        const std::shared_ptr<ServerUnaryWriterReader<CloseProducerRspPb, CloseProducerReqPb>> &serverApi,
        const CloseProducerReqPb &req, CloseProducerRspPb &rsp);

    /**
     * @brief Subscribe to a stream, using a subscription name, i.e., register a consumer to a subscription.
     * Similar to worker::Subscribe. This is a wrapper version that will redirect the call to the SubscribeImpl()
     * function.
     * @param[in] serverApi Used to read request from rpc client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status Subscribe(std::shared_ptr<ServerUnaryWriterReader<SubscribeRspPb, SubscribeReqPb>> serverApi) override;

    /**
     * @brief The real work for the Subscribe is started in this function.
     * @param[in] serverApi Used to read request from rpc client and write response to client. nullptr can be passed
     * here if the caller is directly calling this function (not through rpc).
     * @param[in] req The subscribe request details
     * @param[out] rsp The subscribe response info
     * @return K_OK on success; the error code otherwise.
     */
    Status SubscribeImpl(const std::shared_ptr<ServerUnaryWriterReader<SubscribeRspPb, SubscribeReqPb>> &serverApi,
                         const SubscribeReqPb &req, SubscribeRspPb &rsp);

    /**
     * @brief Close a consumer, trigger subscription cursor change and unregister a subscribed consumer to a stream.
     * Similar to worker::CloseConsumer. This is a wrapper version that will redirect the call to the
     * CloseConsumerImpl() function.
     * @param[in] serverApi Used to read request from rpc client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseConsumer(
        std::shared_ptr<ServerUnaryWriterReader<CloseConsumerRspPb, CloseConsumerReqPb>> serverApi) override;

    /**
     * @brief The real work of the CloseConsumer is started in this function.
     * @param[in] serverApi Used to read request from rpc client and write response to client. nullptr can be passed
     * here if the caller is directly calling this function (not through rpc).
     * @param[in] req The close consumer request details.
     * @param[out] rsp The close consumer response.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseConsumerImpl(
        const std::shared_ptr<ServerUnaryWriterReader<CloseConsumerRspPb, CloseConsumerReqPb>> &serverApi,
        const CloseConsumerReqPb &req, CloseConsumerRspPb &rsp);

    /**
     * @brief Delete a stream.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status DeleteStream(const DeleteStreamReqPb &req, DeleteStreamRspPb &rsp) override;

    /**
     * @brief Query global producers for a stream.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryGlobalProducersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp) override;

    /**
     * @brief Query global consumers for a stream.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryGlobalConsumersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp) override;

    /**
     * @brief Check metadata when master starts
     * @return K_OK on success; the error code otherwise.
     */
    Status StartCheckMetadata();

    /**
     * @brief Setter function to assign the cluster manager back pointer.
     * @param[in] etcdCM The cluster manager pointer to assign
     */
    void SetClusterManager(EtcdClusterManager *etcdCM)
    {
        etcdCM_ = etcdCM;
    }

    /**
     * @brief Migrate stream metadata.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status MigrateSCMetadata(const MigrateSCMetadataReqPb &req, MigrateSCMetadataRspPb &rsp) override;

protected:
    /**
     * @brief Get the current db name.
     * @return std::string The db name.
     */
    std::string GetDbName();

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager
    std::unique_ptr<ThreadPool> threadPool_{ nullptr };
    ReplicaManager *replicaManager_;
};
}  // namespace master
}  // namespace datasystem

#endif  // DATASYSTEM_MASTER_STREAM_CACHE_MASTER_SC_SERVICE_IMPL_H
