/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Client c wrapper for stream cache client.
 */

#ifndef DATASYSTEM_STREAM_CLIENT_C_WRAPPER_H
#define DATASYSTEM_STREAM_CLIENT_C_WRAPPER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "datasystem/c_api/status_definition.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum SubType { STREAM, ROUND_ROBIN, KEY_PARTITIONS, UNKNOWN } SubType;

typedef struct StreamElement {
    uint8_t *ptr;
    uint64_t size;
    uint64_t id;
} StreamElement;

/**
 * @brief This wrapper provides a standard C api to invoke calls into the StreamClientImpl
 * It wraps various operations over the StreamClientImpl, denoted by the type StreamClient_p
 */
typedef void *StreamClient_p;  // The main type/handle for interacting with the stream cache
typedef void *Consumer_p;      // The handler for consumer
typedef void *Producer_p;      // The handler for producer

/**
 * @brief Creates and returns a handle for a stream cache client.  This lightweight call only creates the instance
 * of the connection handle.  It does not initialize an actual connection yet. See ConnectWorker() method for the
 * connection initialization.
 * @param[in] cWorkerHost A c string that contains the hostname for the worker
 * @param[in] workerPort The port number of the worker
 * @param[in] timeOut Timeout in milliseconds and the range is [0, INT32_MAX].
 * @param[in] clientPublicKey The rpc public key of client
 * @param[in] cClientPublicKeyLen The length of cClientPublicKey
 * @param[in] clientPrivateKey The rpc private key of client
 * @param[in] clientPrivateKeyLen The rpc private key length
 * @param[in] serverPublicKey The rpc public key of worker
 * @param[in] cServerPublicKeyLen The length of cServerPublicKey
 * @param[in] accessKey The access key for AK/SK authorize.
 * @param[in] cAccessKeyLen The length of cAccessKey
 * @param[in] secretKey The secret key for AK/SK authorize.
 * @param[in] secretKeyLen The secret key length.
 * @param[in] tenantId The tenant ID.
 * @param[in] cTenantIdLen The length of cTenantId
 * @param[in] enableCrossNodeConnection Indicates whether the client can connect to the standby node.
 * @return Return the pointer of StreamClient.
 */
StreamClient_p StreamCreateClient(const char *cWorkerHost, const int workerPort, const int timeOut,
                                  const char *clientPublicKey, size_t cClientPublicKeyLen, const char *clientPrivateKey,
                                  size_t clientPrivateKeyLen, const char *serverPublicKey, size_t cServerPublicKeyLen,
                                  const char *accessKey, size_t cAccessKeyLen, const char *secretKey,
                                  size_t secretKeyLen, const char *tenantId, size_t cTenantIdLen,
                                  const char *enableCrossNodeConnection);

/**
 * @brief Executes the initialization of the connection to the worker
 * @param[in] clientPtr The instance of the stream cache client to connect with
 * @param[in]  reportWorkerLost Report to the user that the worker was lost previously.
 * @return status of the call
 */
struct StatusC StreamConnectWorker(StreamClient_p clientPtr, bool reportWorkerLost);

/**
 * @brief Frees the stream cache client and releases all associated resources with this handle
 * @param[in] clientPtr The pointer of StreamClient.
 */
void StreamFreeClient(StreamClient_p clientPtr);

/**
 * @brief Subscribe to get a consumer.
 * @param[in] clientPtr The instance of the stream cache client to connect with
 * @param[in] streamName Stream Name
 * @param[in] streamNameLen streamName length
 * @param[in] subName Subscription name
 * @param[in] subNameLen subName length
 * @param[in] subType Subscription type
 * @param[in] autoAck Optional setting to toggle if automatic Acks should be enabled or not.
 * @param[in] cacheCapacity local subscription cache capacity
 * @param[in] cachePrefetchLWM cache prefetch percent. Enabled when value is greater than 0.
 * @param[out] consumer A pointer to a consumer pointer
 * @return status of the call
 */
struct StatusC StreamSubscribe(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                               const char *subName, size_t subNameLen, SubType subType, bool autoAck,
                               uint32_t cacheCapacity, uint16_t cachePrefetchLWM, Consumer_p *consumer);

/**
 * @brief Consumer receives elements of expected number.
 * @param[in] consumerPtr The pointer to the consumer
 * @param[in] expectNum The expected number of elements
 * @param[in] timeoutMs Timeout limit
 * @param[out] elements The received elements
 * @param[out] count The number of elements
 * @return status of the call
 */
struct StatusC StreamConsumerReceiveExpect(Consumer_p consumerPtr, uint32_t expectNum, uint32_t timeoutMs,
                                           StreamElement **elements, uint64_t *count);

/**
 * @brief Consumer receives elements
 * @param[in] consumerPtr The pointer to the consumer
 * @param[in] timeoutMs Timeout limit
 * @param[out] elements The received elements
 * @param[out] count The number of elements
 * @return status of the call
 */
struct StatusC StreamConsumerReceive(Consumer_p consumerPtr, uint32_t timeoutMs, StreamElement **elements,
                                     uint64_t *count);

/**
 * @brief Consumer acknowledges elements
 * @param[in] consumerPtr The pointer to the consumer
 * @param[in] elementId element ID
 * @return status of the call
 */
struct StatusC StreamConsumerAck(Consumer_p consumerPtr, uint64_t elementId);

/**
 * @brief Client creates producer
 * @param[in] clientPtr The instance of the stream cache client to connect with
 * @param[in] streamName Stream Name
 * @param[in] streamNameLen streamName length
 * @param[in] delayFlushTime delay flush time
 * @param[in] pageSize page size
 * @param[in] maxStreamSize stream size limit
 * @param[out] producer A pointer to a producer pointer
 * @return status of the call
 */
struct StatusC StreamCreateProducer(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                                    int64_t delayFlushTime, int64_t pageSize, uint64_t maxStreamSize, bool autoCleanup,
                                    Producer_p *producer);

/**
 * @brief Client creates producer
 * @param[in] clientPtr The instance of the stream cache client to connect with
 * @param[in] streamName Stream Name
 * @param[in] streamNameLen streamName length
 * @param[in] delayFlushTime delay flush time
 * @param[in] pageSize page size
 * @param[in] maxStreamSize stream size limit
 * @param[in] retainForNumConsumers num of consumers to retain data for
 * @param[in] encryptStream Enable stream sta encryption between workers
 * @param[in] reserveSize default reserve size to page size, must be a multiple of page size.
 * @param[out] producer A pointer to a producer pointer
 * @return status of the call
 */
struct StatusC StreamCreateProducerWithConfig(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                                              int64_t delayFlushTime, int64_t pageSize, uint64_t maxStreamSize,
                                              bool autoCleanup, uint64_t retainForNumConsumers, bool encryptStream,
                                              uint64_t reserveSize, Producer_p *producer);

/**
 * @brief Producer sends element
 * @param[in] producerPtr The pointer to the producer
 * @param[in] ptr The pointer to the element to send
 * @param[in] size The size of the element
 * @param[in] id The id of element which can created and increased by datasystem automatically
 * @return status of the call
 */
struct StatusC StreamProducerSend(Producer_p producerPtr, uint8_t *ptr, uint64_t size, uint64_t id);

/**
 * @brief QueryGlobalProducersNum
 * @param[in] clientPtr clientPtr The instance of the stream cache client to connect with
 * @param[in] streamName Stream Name
 * @param[in] streamNameLen streamName length
 * @param[out] gProducerNum Producers num
 * @return struct StatusC
 */
struct StatusC QueryGlobalProducersNum(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                                       uint64_t *gProducerNum);

/**
 * @brief QueryGlobalConsumersNum
 * @param[in] clientPtr clientPtr The instance of the stream cache client to connect with
 * @param[in] streamName Stream Name
 * @param[in] streamNameLen streamName length
 * @param[out] gConsumersNum Consumers num
 * @return struct StatusC
 */
struct StatusC QueryGlobalConsumersNum(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                                       uint64_t *gConsumersNum);

/**
 * @brief Delete one stream.
 * @param[in] clientPtr clientPtr The instance of the stream cache client to connect with
 * @param[in] streamName Stream Name
 * @param[in] streamNameLen streamName length
 * @return Status of the call
 */
struct StatusC DeleteStream(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen);

/**
 * @brief Close the producer, after close it will not allow Send new Elements, and it will trigger flush operations
 *  when the local buffer had not flushed elements.
 * @param[in] producerPtr The pointer to the producer
 * @return status of the call
 */
struct StatusC CloseProducer(Producer_p producerPtr);

/**
 * @brief Close the consumer, after close it will not allow Receive and Ack Elements.
 * @param[in] consumerPtr The pointer to the consumer
 * @return status of the call
 */
struct StatusC CloseConsumer(Consumer_p consumerPtr);

/**
 * @brief Get the amount of received elements since this consumer construct, and the amount of elements
 * not processed.
 * @param[in] consumerPtr The pointer to the consumer
 * @param[out] totalElements the amount of received elements since this consumer construct.
 * @param[out] notProcessedElements the amount of elements not processed.
 */
struct StatusC GetStatisticsMessage(Consumer_p consumerPtr, uint64_t *totalElements, uint64_t *notProcessedElements);
#ifdef __cplusplus
};
#endif
#endif  // DATASYSTEM_STREAM_CACHE_C_WRAPPER_H