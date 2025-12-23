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
 * Description: Declare stream cache client.
 */
#ifndef DATASYSTEM_STREAM_CACHE_STREAM_CLIENT_H
#define DATASYSTEM_STREAM_CACHE_STREAM_CLIENT_H

#include <memory>

#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
namespace stream_cache {
class StreamClientImpl;
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem

namespace datasystem {
class __attribute((visibility("default"))) StreamClient {
public:
    /** brief Construct ObjectClient.
     * @param[in] connectOptions the connect options.
     */
    explicit StreamClient(ConnectOptions connectOptions);

    ~StreamClient();

    /**
     * @brief Shutdown the stream client.
     * @return K_OK on success; the error code otherwise.
     */
    Status ShutDown();

    /**
     * @brief Initialize the stream client.
     * @param[in] reportWorkerLost Report to the user that the worker was lost previously.
     * @return K_OK on success; the error code otherwise.
     */
    Status Init(bool reportWorkerLost = false);

    /**
     * @brief Create one Producer to send element.
     * @param[in] streamName The name of stream. The name should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256
     * @param[out] outProducer The output Producer that user can use it to send element.
     * @param[in] producerConf The producer configure.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_INVALID: invalid parameter.
     *         K_RUNTIME_ERROR: delete pub node in global scope fail on master process.
     *         K_RUNTIME_ERROR: fail to init mmap memory for producer.
     *         K_NOT_READY: the worker is not ready.
     *         K_IO_ERROR: can not open curve key from file.
     *         K_SC_STREAM_RESOURCE_ERROR: reserve memory failed.
     */
    Status CreateProducer(const std::string &streamName, std::shared_ptr<Producer> &outProducer,
                          ProducerConf producerConf = {});

    /**
     * @brief Create the relation of subscribe and generate one Consumer to receive elements.
     * @param[in] streamName The name of stream. The name should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256
     * @param[in] config The config of subscription.
     * @param[out] outConsumer The output Consumer that user can use it to receive data elements.
     * @param[in] autoAck Optional setting to toggle if automatic Acks should be enabled or not.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_RUNTIME_ERROR: add pub node in global scope fail on master process.
     *         K_NOT_READY: the worker is not ready.
     *         K_SC_STREAM_RESOURCE_ERROR: reserve memory failed.
     */
    Status Subscribe(const std::string &streamName, const struct SubscriptionConfig &config,
                     std::shared_ptr<Consumer> &outConsumer, bool autoAck = false);

    /**
     * @brief Delete one stream.
     * @param[in] streamName The name of stream. The name should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_NOT_READY: the worker is not ready.
     *         K_RUNTIME_ERROR: not allowed to delete stream when producer is running.
     *         K_RUNTIME_ERROR: not allowed to delete stream when consumer is running.
     *         K_RUNTIME_ERROR: not allowed to delete stream when remote producer is running.
     *         K_RUNTIME_ERROR: not allowed to delete stream when remote consumer is running.
     *         K_RUNTIME_ERROR: has pub node in global scope.
     *         K_RUNTIME_ERROR: has sub node in global scope.
     *         K_IO_ERROR: repeat deleting.
     *         K_KVSTORE_ERROR: can not delete the key.
     */
    Status DeleteStream(const std::string &streamName);

    /**
     * @brief Query the number of global producers.
     * @param[in] streamName The target stream. The name should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256
     * @param[out] gProducerNum The number of of global producers.
     * @return Status of the call.
     */
    Status QueryGlobalProducersNum(const std::string &streamName, uint64_t &gProducerNum);

    /**
     * @brief Query the number of global consumers.
     * @param[in] streamName The target stream. The name should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256
     * @param[out] gConsumerNum The number of of global consumers.
     * @return Status of the call.
     */
    Status QueryGlobalConsumersNum(const std::string &streamName, uint64_t &gConsumerNum);

    /**
     * @brief Update token for yr iam
     * @param[in] Token message for auth certification
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateToken(SensitiveValue token);

    /**
     * @brief Update aksk for yr iam
     * @param[in] acessKey message for auth certification
     * @param[in] secretKey message for auth certification
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateAkSk(const std::string accessKey, SensitiveValue secretKey);

private:
    //  for make_unique to access private/protected constructor.
    friend std::unique_ptr<client::stream_cache::StreamClientImpl>
    std::make_unique<client::stream_cache::StreamClientImpl>();
    friend std::unique_ptr<StreamClient> std::make_unique<StreamClient>();

    std::shared_ptr<client::stream_cache::StreamClientImpl> impl_;
    std::string ip_;
    int32_t port_;
    SensitiveValue token_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_STREAM_CACHE_STREAM_CLIENT_H
