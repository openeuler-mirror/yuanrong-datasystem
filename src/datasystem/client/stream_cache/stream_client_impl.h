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
 * Description: Implement stream cache client.
 */
#ifndef DATASYSTEM_CLIENT_STREAM_CACHE_STREAM_CLIENT_IMPL_H
#define DATASYSTEM_CLIENT_STREAM_CACHE_STREAM_CLIENT_IMPL_H

#include <memory>
#include <mutex>

#include <re2/re2.h>

#include "datasystem/client/client_state_manager.h"
#include "datasystem/client/listen_worker.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/utils/connection.h"

namespace datasystem {
namespace client {
namespace stream_cache {
class StreamClientImpl : public std::enable_shared_from_this<StreamClientImpl> {
public:
    explicit StreamClientImpl(const std::string &clientPublicKey = "", const SensitiveValue &clientPrivateKey = "",
                              const std::string &serverPublicKey = "", const std::string &accessKey = "",
                              const SensitiveValue &secretKey = "");

    explicit StreamClientImpl(const ConnectOptions &connectOptions);

    virtual ~StreamClientImpl();

    /**
     * @brief Shutdown a object client instance.
     * @param[out] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     * @param[in] isDestruct Since shutdown will also be called during client's destruction,
     * this parameter is used to avoid redundant log printing in the destruction scenario.
     * @return K_OK on success; the error code otherwise.
     */
    Status ShutDown(bool &needRollbackState, bool isDestruct = false);

    /**
     * @brief Create one Producer to send element.
     * @param[in] streamName The name of stream.
     * @param[out] outProducer The output Producer that user can use it to send element.
     * @param[in] producerConf The producer configure.
     * @return Status of the call.
     */
    Status CreateProducer(const std::string &streamName, std::shared_ptr<Producer> &outProducer,
                          const ProducerConf &producerConf = {});

    /**
     * @brief Create the relation of subscribe and generate one Consumer to receive elements.
     * @param[in] streamName The name of stream.
     * @param[in] config The config of subscription.
     * @param[out] outConsumer The output Consumer that user can use it to receive data elements.
     * @param[in] autoAck Toggles if autoAck is on or off
     * @return Status of the call.
     */
    Status Subscribe(const std::string &streamName, const struct SubscriptionConfig &config,
                     std::shared_ptr<Consumer> &outConsumer, bool autoAck);

    /**
     * @brief Delete one stream.
     * @param[in] streamName The name of stream.
     * @return Status of the call.
     */
    Status DeleteStream(const std::string &streamName);

    /**
     * @brief Query the number of global producers
     * @param[in] streamName The target stream
     * @param[out] gProducerNum The number of of global producers
     * @return Status of the call.
     */
    Status QueryGlobalProducersNum(const std::string &streamName, uint64_t &gProducerNum);

    /**
     * @brief Query the number of global consumers.
     * @param[in] streamName The target stream.
     * @param[out] gConsumerNum The number of of global consumers.
     * @return Status of the call.
     */
    Status QueryGlobalConsumersNum(const std::string &streamName, uint64_t &gConsumerNum);

    /**
     * @brief Initialize the Ds client connector.
     * @param[in] ip The worker ip address.
     * @param[in] port The worker port.
     * @param[in] reportWorkerLost Whether to report to the caller when worker had crashed or worker lost the client.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the input ip or port is invalid.
     */
    Status Init(const std::string &ip, const int &port, bool &needRollbackState, bool reportWorkerLost);

    /**
     * Fetch the lock id granted to this client.
     * @return 4 byte lock id
     */
    uint32_t GetLockId() const;

    /**
     * @brief Set producer and consumer inactive and clear.
     */
    void ClearProducerAndConsumer();

    /**
     * @brief Init/Shutdown complete handler.
     * @param[in] failed Init/Shutdown success or not.
     * @param[out] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     */
    void CompleteHandler(bool failed, bool needRollbackState)
    {
        clientStateManager_->CompleteHandler(failed, needRollbackState);
    }

    /**
     * @brief Creates the prefetch pool if it is not created yet.
     * @return Status of the call
     */
    Status CreatePrefetchPoolIfNotExist();

    /**
     * @brief Returns a pointer to the prefetch pool.
     * @return pointer to the prefetch pool
     */
    ThreadPool *GetPrefetchPool()
    {
        return prefetchThdPool_.get();
    }

    /**
     * @brief Check if worker was lost between heartbeats.
     * @return Status of the call
     */
    inline const Status CheckWorkerLost()
    {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            !workerWasLost_, StatusCode::K_SC_WORKER_WAS_LOST,
            FormatString("Client %s detected worker %s was lost", clientWorkerApi_->GetClientId(),
                         clientWorkerApi_->GetWorkHost()));
        return Status::OK();
    }

    /**
     * @brief Remove closed producer.
     * @param[in] producerId Producer Id.
     */
    void ClearProducer(const std::string &producerId);

    /**
     * @brief Remove closed consumer.
     * @param[in] consumerId Consumer Id.
     */
    void ClearConsumer(const std::string &consumerId);

private:
    /**
     * @brief Check the client is ready to execute any api
     * @return Status
     */
    inline Status IsClientReady();

    /**
     * @brief Check whether connect by unix domain socket.
     * @return Status
     */
    Status CheckConnectByUds();

    /**
     * @brief Clear all data and metadata for the producers and consumers for a resetting stream.
     * @param[in] resetProducers Pointer to the list of producers getting cleaned up
     * @param[in] resetConsumers Pointer to the list of consumers getting cleaned up
     */
    void CleanupProdsCons(std::vector<std::weak_ptr<Producer>> &resetProducers,
                          std::vector<std::weak_ptr<Consumer>> &resetConsumers);

    /**
     * @brief verify ProducerConfig.
     * @param[in] producerConf The producer config
     * @return Status of this call.
     */
    static Status VerifyProducerConfig(const ProducerConf &producerConf);

    std::mutex initMutex_;
    std::unique_ptr<Signature> signature_{ nullptr };
    std::shared_ptr<ClientWorkerApi> clientWorkerApi_;
    std::unique_ptr<MmapManager> mmapManager_;
    std::function<void()> callBack_;      // Fail callback handle, if worker disconnect this function would be call.
    std::shared_timed_mutex clearMutex_;  // Protect producers_ and consumers_.
    std::unordered_map<std::string, std::weak_ptr<Producer>>
        producers_;  // Ensure that the producer can be automatically destroyed. Key is producerId.
    std::unordered_map<std::string, std::weak_ptr<Consumer>>
        consumers_;  // Ensure that the consumer can be automatically destroyed. Key is consumerId.
    bool isInit_ = { false };
    bool reportWorkerLost_{ false };
    std::atomic<bool> workerWasLost_{ false };
    std::unique_ptr<ClientStateManager> clientStateManager_{ nullptr };
    std::unique_ptr<ThreadPool> prefetchThdPool_{ nullptr };

    // Listenworker needs to be placed at the bottom to ensure that it is destructed first.
    std::shared_ptr<ListenWorker> listenWorker_{ nullptr };

    RpcAuthKeys authKeys_;
    std::string tenantId_;
    int32_t requestTimeoutMs_ = RPC_TIMEOUT;
    int32_t connectTimeoutMs_ = RPC_TIMEOUT;
    // verify object key format.
    re2::RE2 idRe_{ "^[a-zA-Z0-9\\~\\.\\-\\/_!@#%\\^\\&\\*\\(\\)\\+\\=\\:;]*$" };
};
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem
#endif
