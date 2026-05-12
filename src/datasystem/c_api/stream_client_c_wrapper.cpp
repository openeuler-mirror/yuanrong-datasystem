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

#include "datasystem/c_api/stream_client_c_wrapper.h"

#include <cstdint>
#include <cstdlib>

#include "datasystem/c_api/util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"
#include "datasystem/utils/connection.h"

StreamClient_p StreamCreateClient(const char *cWorkerHost, const int workerPort, const int timeOut, const char *token,
                                  size_t tokenLen, const char *clientPublicKey, size_t cClientPublicKeyLen,
                                  const char *clientPrivateKey, size_t clientPrivateKeyLen, const char *serverPublicKey,
                                  size_t cServerPublicKeyLen, const char *accessKey, size_t cAccessKeyLen,
                                  const char *secretKey, size_t secretKeyLen, const char *tenantId, size_t cTenantIdLen,
                                  const char *enableCrossNodeConnection)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetRequestTraceUUID();
    if (cWorkerHost == nullptr || strlen(cWorkerHost) == 0 || workerPort < 0) {
        LOG(ERROR) << "Host or Port have not been provided correctly";
        return nullptr;
    }
    std::string workerHost(cWorkerHost);
    struct datasystem::ConnectOptions opts = {
        .host = workerHost,
        .port = workerPort,
    };
    ConstructConnectOptions(timeOut, token, tokenLen, clientPublicKey, cClientPublicKeyLen, clientPrivateKey,
                            clientPrivateKeyLen, serverPublicKey, cServerPublicKeyLen, accessKey, cAccessKeyLen,
                            secretKey, secretKeyLen, tenantId, cTenantIdLen, enableCrossNodeConnection, opts);
    auto clientSharedPtr = std::make_shared<datasystem::StreamClient>(opts);
    auto clientUniquePtr = std::make_unique<std::shared_ptr<datasystem::StreamClient>>(std::move(clientSharedPtr));
    return reinterpret_cast<void *>(clientUniquePtr.release());
}

struct StatusC StreamConnectWorker(StreamClient_p clientPtr, bool reportWorkerLost)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetRequestTraceUUID();
    if (clientPtr == nullptr) {
        return StatusC{ datasystem::K_RUNTIME_ERROR, "The client has not been initialized." };
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    datasystem::Status rc = (*client)->Init(reportWorkerLost);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC StreamUpdateAkSk(StreamClient_p clientPtr, const char *cAccessKey, size_t cAccessKeyLen,
                                const char *cSecretKey, size_t cSecretKeyLen)
{
    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    std::string accessKey(cAccessKey, cAccessKeyLen);
    accessKey.assign(cAccessKey, cAccessKeyLen);
    datasystem::SensitiveValue secretKey(cSecretKey, cSecretKeyLen);
    datasystem::Status rc = (*client)->UpdateAkSk(accessKey, secretKey);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

void StreamFreeClient(StreamClient_p clientPtr)
{
    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    if (client != nullptr) {
        LOG_IF_ERROR((*client)->ShutDown(), "StreamClient shutdown failed");
        delete client;
        client = nullptr;
    }
}

struct StatusC StreamSubscribe(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                               const char *subName, size_t subNameLen, SubType subType, bool autoAck,
                               uint32_t cacheCapacity, uint16_t cachePrefetchLWM, Consumer_p *consumer)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(streamName, "streamName", errorMsg);
    CheckNullptr(subName, "subName", errorMsg);
    CheckNullptr(consumer, "consumer", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    std::string strStreamName(streamName, streamNameLen);
    std::string strSubName(subName, subNameLen);
    datasystem::SubscriptionType subscriptionType;
    switch (subType) {
        case SubType::STREAM:
            subscriptionType = SubscriptionType::STREAM;
            break;
        case SubType::ROUND_ROBIN:
            subscriptionType = SubscriptionType::ROUND_ROBIN;
            break;
        case SubType::KEY_PARTITIONS:
            subscriptionType = SubscriptionType::KEY_PARTITIONS;
            break;
        default:
            subscriptionType = SubscriptionType::UNKNOWN;
            break;
    }
    std::shared_ptr<datasystem::Consumer> outConsumer;
    datasystem::Status rc = (*client)->Subscribe(
        strStreamName, datasystem::SubscriptionConfig(strSubName, subscriptionType, cacheCapacity, cachePrefetchLWM),
        outConsumer, autoAck);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    std::unique_ptr<std::shared_ptr<datasystem::Consumer>> pOutConsumer =
        std::make_unique<std::shared_ptr<datasystem::Consumer>>(std::move(outConsumer));
    *consumer = reinterpret_cast<void *>(pOutConsumer.release());

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC StreamConsumerReceiveExpect(Consumer_p consumerPtr, uint32_t expectNum, uint32_t timeoutMs,
                                           struct StreamElement **elements, uint64_t *count)
{
    std::string errorMsg;
    CheckNullptr(consumerPtr, "consumer", errorMsg);
    CheckNullptr(count, "count", errorMsg);
    CheckNullptr(elements, "elements", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    std::vector<Element> eles;
    auto consumer = *reinterpret_cast<std::shared_ptr<datasystem::Consumer> *>(consumerPtr);
    datasystem::Status rc = consumer->Receive(expectNum, timeoutMs, eles);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    *count = eles.size();
    std::unique_ptr<StreamElement[]> pEles = std::make_unique<StreamElement[]>(eles.size());
    for (size_t i = 0; i < eles.size(); ++i) {
        pEles[i].id = eles[i].id;
        pEles[i].ptr = eles[i].ptr;
        pEles[i].size = eles[i].size;
    }
    *elements = pEles.release();

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC StreamConsumerReceive(Consumer_p consumerPtr, uint32_t timeoutMs, struct StreamElement **elements,
                                     uint64_t *count)
{
    std::string errorMsg;
    CheckNullptr(consumerPtr, "consumer", errorMsg);
    CheckNullptr(count, "count", errorMsg);
    CheckNullptr(elements, "elements", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    std::vector<Element> eles;
    auto consumer = *reinterpret_cast<std::shared_ptr<datasystem::Consumer> *>(consumerPtr);
    datasystem::Status rc = consumer->Receive(timeoutMs, eles);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    *count = eles.size();
    std::unique_ptr<StreamElement[]> pEles = std::make_unique<StreamElement[]>(eles.size());
    for (size_t i = 0; i < eles.size(); ++i) {
        pEles[i].id = eles[i].id;
        pEles[i].ptr = eles[i].ptr;
        pEles[i].size = eles[i].size;
    }
    *elements = pEles.release();

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC StreamConsumerAck(Consumer_p consumerPtr, uint64_t elementId)
{
    std::string errorMsg;
    CheckNullptr(consumerPtr, "consumer", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto consumer = *reinterpret_cast<std::shared_ptr<datasystem::Consumer> *>(consumerPtr);
    datasystem::Status rc = consumer->Ack(elementId);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC StreamCreateProducer(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                                    int64_t delayFlushTime, int64_t pageSize, uint64_t maxStreamSize, bool autoCleanup,
                                    Producer_p *producer)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(streamName, "streamName", errorMsg);
    CheckNullptr(producer, "producer", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    std::string strStreamName(streamName, streamNameLen);
    std::shared_ptr<datasystem::Producer> outProducer;
    datasystem::Status rc =
        (*client)->CreateProducer(strStreamName, outProducer, { delayFlushTime, pageSize, maxStreamSize, autoCleanup });
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    std::unique_ptr<std::shared_ptr<datasystem::Producer>> pOutProducer =
        std::make_unique<std::shared_ptr<datasystem::Producer>>(std::move(outProducer));
    *producer = reinterpret_cast<void *>(pOutProducer.release());

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC StreamCreateProducerWithConfig(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                                              int64_t delayFlushTime, int64_t pageSize, uint64_t maxStreamSize,
                                              bool autoCleanup, uint64_t retainForNumConsumers, bool encryptStream,
                                              uint64_t reserveSize, Producer_p *producer)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(streamName, "streamName", errorMsg);
    CheckNullptr(producer, "producer", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    std::string strStreamName(streamName, streamNameLen);
    std::shared_ptr<datasystem::Producer> outProducer;
    datasystem::Status rc = (*client)->CreateProducer(
        strStreamName, outProducer,
        { delayFlushTime, pageSize, maxStreamSize, autoCleanup, retainForNumConsumers, encryptStream, reserveSize });
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    std::unique_ptr<std::shared_ptr<datasystem::Producer>> pOutProducer =
        std::make_unique<std::shared_ptr<datasystem::Producer>>(std::move(outProducer));
    *producer = reinterpret_cast<void *>(pOutProducer.release());

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC StreamProducerSend(Producer_p producerPtr, uint8_t *ptr, uint64_t size, uint64_t id)
{
    std::string errorMsg;
    CheckNullptr(producerPtr, "producer", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto producer = *reinterpret_cast<std::shared_ptr<datasystem::Producer> *>(producerPtr);
    datasystem::Status rc = producer->Send({ ptr, size, id });
    if (rc.IsError()) {
        return ToStatusC(rc);
    }

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC QueryGlobalProducersNum(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                                       uint64_t *gProducerNum)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(streamName, "streamName", errorMsg);

    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    std::string strStreamName(streamName, streamNameLen);
    datasystem::Status rc = (*client)->QueryGlobalProducersNum(strStreamName, *gProducerNum);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC QueryGlobalConsumersNum(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen,
                                       uint64_t *gProducerNum)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(streamName, "streamName", errorMsg);

    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    std::string strStreamName(streamName, streamNameLen);
    datasystem::Status rc = (*client)->QueryGlobalConsumersNum(strStreamName, *gProducerNum);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC DeleteStream(StreamClient_p clientPtr, const char *streamName, size_t streamNameLen)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(streamName, "streamName", errorMsg);

    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto client = reinterpret_cast<std::shared_ptr<datasystem::StreamClient> *>(clientPtr);
    std::string strStreamName(streamName, streamNameLen);
    datasystem::Status rc = (*client)->DeleteStream(strStreamName);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }

    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC CloseProducer(Producer_p producerPtr)
{
    std::string errorMsg;
    CheckNullptr(producerPtr, "producer", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto producer = reinterpret_cast<std::shared_ptr<datasystem::Producer> *>(producerPtr);
    datasystem::Status rc = (*producer)->Close();
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    delete producer;
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC CloseConsumer(Consumer_p consumerPtr)
{
    std::string errorMsg;
    CheckNullptr(consumerPtr, "consumer", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto consumer = reinterpret_cast<std::shared_ptr<datasystem::Consumer> *>(consumerPtr);
    datasystem::Status rc = (*consumer)->Close();
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    delete consumer;
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC GetStatisticsMessage(Consumer_p consumerPtr, uint64_t *totalElements, uint64_t *notProcessedElements)
{
    std::string errorMsg;
    CheckNullptr(consumerPtr, "consumer", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }

    auto consumer = *reinterpret_cast<std::shared_ptr<datasystem::Consumer> *>(consumerPtr);
    consumer->GetStatisticsMessage(*totalElements, *notProcessedElements);

    return StatusC{ datasystem::K_OK, {} };
}