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
 * Description: Define api of stream cache consumer.
 */
#ifndef DATASYSTEM_STREAM_CACHE_CONSUMER_H
#define DATASYSTEM_STREAM_CACHE_CONSUMER_H

#include <memory>
#include <vector>

#include "datasystem/stream/element.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
namespace stream_cache {
class StreamClientImpl;
class ConsumerImpl;
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem

namespace datasystem {
class __attribute((visibility("default"))) Consumer {
public:
    ~Consumer();
    /**
     * @brief Get expectNum elements form the subscription.
     * @param[in] expectNum The number of elements to be read.
     * @param[in] timeoutMs The timeout millisecond of elements to be Receive.
     * @param[out] outElements The received elements to be read.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_INVALID: invalid parameter.
     *         K_RPC_UNAVAILABLE: didn't receive any response from server.
     *         K_DUPLICATED: the consumer already had pending receive.
     *         K_SC_PRODUCER_NOT_FOUND: one or more producer in the stream are dead.
     *         K_SC_STREAM_IN_RESET_STATE: stream currently in reset state.
     *         K_SC_ALREADY_CLOSED: consumer is already closed/inactive.
     *         K_SC_STREAM_IN_USE: another thread is calling API from the same consumer at the same time.
     */
    Status Receive(uint32_t expectNum, uint32_t timeoutMs, std::vector<Element> &outElements);

    /**
     * @brief Get any number of elements already received from the subscription.
     * @param[in] timeoutMs The timeout millisecond of elements to be Receive.
     * @param[out] outElements The received elements to be read.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_INVALID: invalid parameter.
     *         K_RPC_UNAVAILABLE: didn't receive any response from server.
     *         K_DUPLICATED: the consumer already had pending receive.
     *         K_SC_PRODUCER_NOT_FOUND: one or more producer in the stream are dead.
     *         K_SC_STREAM_IN_RESET_STATE: stream currently in reset state.
     *         K_SC_ALREADY_CLOSED: consumer is already closed/inactive.
     *         K_SC_STREAM_IN_USE: another thread is calling API from the same consumer at the same time.
     */
    Status Receive(uint32_t timeoutMs, std::vector<Element> &outElements);

    /**
     * @brief Acknowledge elements that had been read by this consumer.
     * @param[in] elementId The element id that to be acknowledged.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_INVALID: invalid parameter.
     *         K_SC_STREAM_IN_RESET_STATE: stream currently in reset state.
     *         K_SC_ALREADY_CLOSED: consumer is already closed/inactive.
     *         K_SC_STREAM_IN_USE: another thread is calling API from the same consumer at the same time.
     */
    Status Ack(uint64_t elementId);

    /**
     * @brief Close the consumer, after close it will not allow Receive and Ack Elements.
     * Calling Close() on an already closed consumer will return K_OK.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_INVALID: invalid parameter.
     *         K_RUNTIME_ERROR: delete sub node in global scope fail on master process.
     *         K_SC_STREAM_IN_USE: another thread is calling API from the same consumer at the same time.
     */
    Status Close();

    /**
     * @brief Get the amount of received elements since this consumer construct, and the amount of elements
     * not processed.
     * @param[out] totalElements the amount of received elements since this consumer construct.
     * @param[out] notProcessedElements the amount of elements not processed.
     */
    void GetStatisticsMessage(uint64_t &totalElements, uint64_t &notProcessedElements);

private:
    explicit Consumer(std::unique_ptr<client::stream_cache::ConsumerImpl> impl);

    /**
     * @cond Friend does not show up in the documentation.
     */
    friend class client::stream_cache::StreamClientImpl;
    // @endcond

    // for make_shared to access private/protected constructor.
    friend std::shared_ptr<Consumer> std::make_shared<Consumer>();
    // for make_unique to access private/protected constructor.
    friend std::unique_ptr<Consumer> std::make_unique<Consumer>();

    std::unique_ptr<client::stream_cache::ConsumerImpl> impl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_STREAM_CACHE_CONSUMER_H
