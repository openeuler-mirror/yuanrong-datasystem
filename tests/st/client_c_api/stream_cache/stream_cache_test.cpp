/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: test cases for c client api.
 */

#include <string>
#include <string.h>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream_client.h"

namespace datasystem {
namespace st {
class StreamCacheTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        int numWorkers = 2;
        opts.numWorkers = numWorkers;
        opts.numMasters = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=10000";
        opts.isStreamCacheCase = true;
    }

    void SetUp() override
    {
        ClusterTest::SetUp();
        HostPort srcWorkerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
        client0_ = CreateStreamCacheClient(srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "", "", ak_,
                                           sk_, "", "", "", "", "true");
        ASSERT_EQ(StreamConnectWorker(client0_, false).code, K_OK);
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, srcWorkerAddress));
        client1_ = CreateStreamCacheClient(srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "", "", ak_,
                                           sk_, "", "", "", "", "true");
        ASSERT_EQ(StreamConnectWorker(client1_, false).code, K_OK);
    }

    void TearDown() override
    {
        if (client0_ != nullptr) {
            StreamFreeClient(client0_);
        }
        if (client1_ != nullptr) {
            StreamFreeClient(client1_);
        }
    }

    StreamClient_p CreateStreamCacheClient(const std::string &workerHost, const int workerPort, const int timeOut,
                                           const std::string &token, const std::string &clientPublicKey,
                                           const std::string &clientPrivateKey, const std::string &serverPublicKey,
                                           const std::string &accessKey, const std::string &secretKey,
                                           const std::string &oAuthClientid, const std::string &oAuthClientSecret,
                                           const std::string &oAuthUrl, const std::string &tenantId,
                                           const std::string &enableCrossNodeConnection)
    {
        (void)oAuthClientid;
        (void)oAuthClientSecret;
        (void)oAuthUrl;
        (void)token;
        return StreamCreateClient(workerHost.c_str(), workerPort, timeOut,
                                  clientPublicKey.c_str(), clientPublicKey.length(), clientPrivateKey.c_str(),
                                  clientPrivateKey.length(), serverPublicKey.c_str(), serverPublicKey.length(),
                                  accessKey.c_str(), accessKey.length(), secretKey.c_str(), secretKey.length(),
                                  tenantId.c_str(), tenantId.length(), enableCrossNodeConnection.c_str());
    }

    void Subscribe(StreamClient_p clientPtr, const std::string &streamName, const std::string &subName,
                   Consumer_p *consumer)
    {
        auto rc = StreamSubscribe(clientPtr, streamName.c_str(), streamName.length(), subName.c_str(), subName.length(),
                                  SubType::STREAM, false, false, SubscriptionConfig::SC_CACHE_CAPACITY,
                                  SubscriptionConfig::SC_CACHE_LWM, consumer);
        ASSERT_EQ(rc.code, K_OK);
    }

    void CreateProducer(StreamClient_p clientPtr, const std::string &streamName, int64_t delayFlushTime,
                        int64_t pageSize, uint64_t maxStreamSize, bool autoCleanup, Producer_p *producer)
    {
        auto rc = StreamCreateProducer(clientPtr, streamName.c_str(), streamName.length(), delayFlushTime, pageSize,
                                       maxStreamSize, autoCleanup, producer);
        ASSERT_EQ(rc.code, K_OK);
    }

    void CreateProducerWithConfig(StreamClient_p clientPtr, const std::string &streamName, int64_t delayFlushTime,
                                  int64_t pageSize, uint64_t maxStreamSize, bool autoCleanup,
                                  uint64_t retainForNumConsumers, bool encryptStream, uint64_t reserveSize,
                                  Producer_p *producer)
    {
        auto rc = StreamCreateProducerWithConfig(clientPtr, streamName.c_str(), streamName.length(), delayFlushTime,
                                                 pageSize, maxStreamSize, autoCleanup, retainForNumConsumers,
                                                 encryptStream, reserveSize, producer);
        ASSERT_EQ(rc.code, K_OK);
    }

    void CreateElement(size_t elementSize, Element &element, std::string &writeElement)
    {
        writeElement = RandomData().GetRandomString(elementSize);
        element = Element(reinterpret_cast<uint8_t *>(&writeElement[0]), elementSize);
    }

    void CreateElements(size_t numEle, size_t elementSize, std::vector<Element> &elements,
                        std::vector<std::string> &writeElements)
    {
        elements.clear();
        elements.resize(numEle);
        writeElements.clear();
        writeElements.resize(numEle);
        for (size_t i = 0; i < numEle; ++i) {
            CreateElement(elementSize, elements[i], writeElements[i]);
        }
    }

    void SendElements(Producer_p producerPtr, std::vector<Element> &elements)
    {
        StatusC rc;
        for (auto &ele : elements) {
            rc = StreamProducerSend(producerPtr, ele.ptr, ele.size, ele.id);
            ASSERT_EQ(rc.code, K_OK);
        }
    }

    void ReceiveElements(Consumer_p consumerPtr, std::vector<std::string> &elements)
    {
        StreamElement *eles = nullptr;
        uint64_t count = 0;
        elements.clear();
        auto rc = StreamConsumerReceive(consumerPtr, timeout_, &eles, &count);
        ASSERT_EQ(rc.code, K_OK);
        elements.reserve(count);
        for (uint64_t i = 0; i < count; ++i) {
            elements.emplace_back(reinterpret_cast<char *>(eles[i].ptr), eles[i].size);
        }
        rc = StreamConsumerAck(consumerPtr, eles[count - 1].id);
        ASSERT_EQ(rc.code, K_OK);
        delete eles;
    }

    void ReceiveElementsExpected(Consumer_p consumerPtr, uint32_t numExpect, std::vector<std::string> &elements)
    {
        StreamElement *eles = nullptr;
        uint64_t count = 0;
        elements.clear();
        auto rc = StreamConsumerReceiveExpect(consumerPtr, numExpect, timeout_, &eles, &count);
        ASSERT_EQ(rc.code, K_OK);
        elements.reserve(count);
        for (uint64_t i = 0; i < count; ++i) {
            elements.emplace_back(reinterpret_cast<char *>(eles[i].ptr), eles[i].size);
        }
        rc = StreamConsumerAck(consumerPtr, eles[count - 1].id);
        ASSERT_EQ(rc.code, K_OK);
        delete eles;
    }

protected:
    std::string ak_ = "QTWAOYTTINDUT2QVKYUC";
    std::string sk_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    StreamClient_p client0_{ nullptr };
    StreamClient_p client1_{ nullptr };
    int64_t delayFlushTime_{ 5 };
    int64_t pageSize_{ 1024 * 1024ul };
    uint64_t maxStreamSize_{ 1024 * 1024 * 1024ul };
    uint32_t timeout_ = 100;
    uint64_t retainForNumConsumers = 0;
    bool autoCleanup = false;
    bool encryptStream = false;
};

TEST_F(StreamCacheTest, CreateProducerConsumer)
{
    Producer_p producer = nullptr;
    std::string streamName = "CreateProducerConsumer";
    CreateProducer(client0_, streamName, delayFlushTime_, pageSize_, maxStreamSize_, autoCleanup, &producer);
    std::string subName = "CreateProducerConsumerSub";
    Consumer_p consumer = nullptr;
    Subscribe(client0_, streamName, subName, &consumer);
}

TEST_F(StreamCacheTest, SendReceiveElements1)
{
    Producer_p producer = nullptr;
    std::string streamName = "SendReceiveElements1";
    CreateProducer(client0_, streamName, delayFlushTime_, pageSize_, maxStreamSize_, autoCleanup, &producer);
    std::string subName = "SendReceiveElements1Sub";
    Consumer_p consumer = nullptr;
    Subscribe(client0_, streamName, subName, &consumer);
    size_t numEle = 100;
    size_t eleSize = 1024;
    std::vector<Element> elements;
    std::vector<std::string> writeElements;
    CreateElements(numEle, eleSize, elements, writeElements);

    SendElements(producer, elements);
    auto rc = StreamProducerFlush(producer);
    ASSERT_EQ(rc.code, K_OK);
    std::vector<std::string> outElements;
    ReceiveElements(consumer, outElements);
    ASSERT_EQ(writeElements.size(), outElements.size());
    for (size_t i = 0; i < writeElements.size(); ++i) {
        ASSERT_EQ(writeElements[i], writeElements[i]);
    }
}

TEST_F(StreamCacheTest, SendReceiveElements2)
{
    Producer_p producer = nullptr;
    std::string streamName = "SendReceiveElements2";
    CreateProducer(client0_, streamName, delayFlushTime_, pageSize_, maxStreamSize_, autoCleanup, &producer);
    std::string subName = "SendReceiveElements2Sub";
    Consumer_p consumer = nullptr;
    Subscribe(client0_, streamName, subName, &consumer);
    size_t numEle = 100;
    size_t eleSize = 1024;
    std::vector<std::string> writeElements;
    for (size_t i = 0; i < numEle; ++i) {
        std::vector<Element> elements;
        std::vector<std::string> writeElement;
        CreateElements(1, eleSize, elements, writeElement);
        SendElements(producer, elements);
        auto rc = StreamProducerFlush(producer);
        ASSERT_EQ(rc.code, K_OK);
        writeElements.emplace_back(writeElement[0]);
    }
    std::vector<std::string> outElements;
    ReceiveElements(consumer, outElements);
    ASSERT_EQ(writeElements.size(), outElements.size());
    for (size_t i = 0; i < writeElements.size(); ++i) {
        ASSERT_EQ(writeElements[i], writeElements[i]);
    }
}

TEST_F(StreamCacheTest, SendReceiveElementsExpectFromRemote)
{
    Producer_p producer = nullptr;
    std::string streamName = "SendReceiveElementsFromRemote";
    CreateProducer(client0_, streamName, delayFlushTime_, pageSize_, maxStreamSize_, autoCleanup, &producer);
    std::string subName = "SendReceiveElementsFromRemoteSub";
    Consumer_p consumer = nullptr;
    // connect to remote worker
    Subscribe(client1_, streamName, subName, &consumer);
    size_t numEle = 50;
    size_t eleSize = 1024;
    std::vector<std::string> writeElements;
    for (size_t i = 0; i < numEle; ++i) {
        std::vector<Element> elements;
        std::vector<std::string> writeElement;
        CreateElements(1, eleSize, elements, writeElement);
        SendElements(producer, elements);
        auto rc = StreamProducerFlush(producer);
        ASSERT_EQ(rc.code, K_OK);
        writeElements.emplace_back(writeElement[0]);
    }
    std::vector<std::string> outElements;
    int sleepTime = 1;
    std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
    for (size_t i = 0; i < numEle; ++i) {
        std::vector<std::string> outElement;
        ReceiveElementsExpected(consumer, 1, outElement);
        outElements.emplace_back(outElement[0]);
    }
    for (size_t i = 0; i < numEle; ++i) {
        ASSERT_EQ(writeElements[i], writeElements[i]);
    }
    ReceiveElementsExpected(consumer, 1, outElements);
    ASSERT_EQ(outElements.size(), 0u);
}
}  // namespace st
}  // namespace datasystem