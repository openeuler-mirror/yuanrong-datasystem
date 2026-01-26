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

#include "datasystem/stream/producer.h"
#include <gtest/gtest.h>

#include "common.h"
#include "common/stream_cache/element_generator.h"
#include "common/stream_cache/stream_common.h"
#include "sc_client_common.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/stream_client.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/stream/consumer.h"

DS_DECLARE_uint32(page_size);
using namespace datasystem::client::stream_cache;

namespace datasystem {
namespace st {
constexpr int K_TWO = 2;
const uint64_t ELEMENTS_TOTAL_SIZE = 64 * 1024 * 1024;      // 64 MB
const uint64_t DEFAULT_MAX_STREAM_SIZE = 64 * 1024 * 1024;  // 64MB;

/**
 * @brief SendConfig.
 * @param[in] streamName Name of stream.
 * @param[in] producerName Name of producer.
 * @param[in] expectedNumOfConsumers Number of consumers to wait for this stream.
 * @param[in] numOfElements Number of elements to send.
 */
struct SendConfig {
    std::string streamName;
    std::string producerName;
    ProducerConf producerConf;
    size_t numOfElements;
};

/**
 * @brief RecvConfig.
 * @param[in] streamName Name of stream.
 * @param[in] subscriptionName Name of subscription (stream mode).
 * @param[in] numOfBatchElements Number of elements to receive.
 * @param[in] timeToWaitMs Time to wait in milli-seconds.
 * @param[in] ackInterval Number of elements received for ack.
 */
struct RecvConfig {
    std::string streamName;
    std::string subscriptionName;
    size_t numOfBatchElements;
    size_t timeToWaitMs;
    size_t ackInterval;
    bool autoAck;
};

class ProducerTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = NUM_WORKERS;
        opts.vLogLevel = 2;
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 1;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

    const int count = 30;

    /**
     * @brief Send streaming data.
     * @param[in] sendConfig Configuration for a producer.
     * @param[in] expectedNumOfConsumers Number of consumers to wait for this stream.
     * @param[in] elementsFut Elements to send for this stream.
     * @param[in] spClient The stream client to user for this send loop
     */
    Status SendStreamData(const SendConfig &sendConfig, uint64_t expectedNumOfConsumers,
                          std::shared_future<std::vector<std::string>> &elementsFut,
                          std::shared_ptr<StreamClient> spClient);

    /**
     * @brief Send streaming data with sleep in between for test purpose.
     * @param[in] sendConfig Configuration for a producer.
     * @param[in] expectedNumOfConsumers Number of consumers to wait for this stream.
     * @param[in] elementsFut Elements to send for this stream.
     * @param[in] spClient The stream client to user for this send loop
     */
    Status SendStreamDataSlow(const SendConfig &sendConfig1, const SendConfig &sendConfig2,
                              uint64_t expectedNumOfConsumers,
                              std::shared_future<std::vector<std::string>> &elementsFut1,
                              std::shared_future<std::vector<std::string>> &elementsFut2,
                              std::shared_ptr<StreamClient> spClient1, std::shared_ptr<StreamClient> spClient2);
    /*
     * @brief Receive streaming data.
     * @param[in] recvConfig Configuration for a receiver.
     * @param[in] numOfElements Total number of elements to receive.
     * @param[in] spClient The stream client to use for this receive loop
     * This does not work if there are multiple producers of the same stream and should be turned off in that case.
     */
    Status RecvStreamData(const RecvConfig &recvConfig, size_t numOfElements, std::shared_ptr<StreamClient> spClient);

    /*
     * @brief Receive streaming data with slowReceive set to true.
     * @param[in] recvConfig Configuration for a receiver.
     * @param[in] numOfElements Total number of elements to receive.
     * @param[in] spClient The stream client to use for this receive loop
     * This does not work if there are multiple producers of the same stream and should be turned off in that case.
     */
    Status RecvStreamDataWithSlowReceive(const RecvConfig &recvConfig, size_t numOfElements,
                                         std::shared_ptr<StreamClient> spClient);

    /**
     * @brief Creates a stream client at the given worker num
     * @param[in] workerNum The worker num to create the stream against
     * @param[out] spClient Shared pointer to the stream client
     * @return status
     */
    Status CreateClient(int workerNum, std::shared_ptr<StreamClient> &spClient)
    {
        InitStreamClient(workerNum, spClient);
        return Status::OK();
    }

    /**
     * @brief Creates a stream client at the given worker num and timeout
     * @param[in] workerNum The worker num to create the stream against
     * @param[in] timeout Timeout for RPC requests
     * @param[out] spClient Shared pointer to the stream client
     * @return status
     */
    Status CreateClient(int workerNum, int32_t timeout, std::shared_ptr<StreamClient> &spClient)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerNum, workerAddress));
        // Create a client with user defined timeout
        ConnectOptions connectOptions = { .host = workerAddress.Host(),
                                          .port = workerAddress.Port(),
                                          .connectTimeoutMs = timeout };
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        spClient = std::make_shared<StreamClient>(connectOptions);
        RETURN_IF_NOT_OK(spClient->Init());
        return Status::OK();
    }

    /**
     * @brief Creates a client worker api at the given worker num
     * @param[in] workerNum The worker num to create the api to
     * @param[out] spClient Shared pointer to the stream client
     * @return status
     */
    Status CreateClientWorkerApi(int workerNum, std::shared_ptr<ClientWorkerApi> &workerApi)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerNum, workerAddress));
        workerApi = std::make_shared<ClientWorkerApi>(workerAddress, RpcCredential(), "", signature_.get());
        RETURN_IF_NOT_OK(workerApi->Init(CLIENT_RPC_TIMEOUT, CLIENT_RPC_TIMEOUT));
        return Status::OK();
    }

    /**
     * @brief A bunch of the OOM testing all have the same setup/flow but different configs. This function captures
     * the common logic so that different tests can run the same stuff with different configs
     * @param[in] prodClient The client for the producer side
     * @param[in] conClient The client for the consumer side
     * @param[in] maxStreamSizeMB Max stream size used by stream
     */
    Status RunOOMTest(std::shared_ptr<StreamClient> prodClient, std::shared_ptr<StreamClient> conClient,
                      std::string streamName, uint64_t maxStreamSizeMB = 2);

protected:
    const int SLOW_CONSUME_WAIT = 5;  // seconds
    const int CLIENT_THREAD_POOL_SIZE = 2;
    const int NUM_WORKERS = 2;
    const int SLEEP_TIME = 10;
    const uint32_t RECV_WAIT_MILLI_SECONDS = 20;
    static constexpr int CLIENT_RPC_TIMEOUT = 4 * 60 * 1000;
    void InitTest()
    {
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;
        waitForGoCount_ = 1;  // assume single consumer.  if a testcase has more, then it need to update this
        interrupt_ = false;
    }

    void WaitForConsumers(std::shared_ptr<StreamClient> spClient, std::string streamName,
                          uint64_t expectedNumOfConsumers);
    Status SendHelper(const std::vector<std::string> &elements, const SendConfig &sendConfig,
                      std::shared_ptr<Producer> producer, std::atomic<size_t> &numOfRetries, unsigned long failInterval,
                      bool slowSend);

    uint64_t CheckProducerCount(std::shared_ptr<StreamClient> spClient, std::string streamName)
    {
        uint64_t totalProducerNum;
        spClient->QueryGlobalProducersNum(streamName, totalProducerNum);
        return totalProducerNum;
    }

    Status TryAndDeleteStream(std::shared_ptr<StreamClient> spClient, std::string streamName)
    {
        // if pending notifications retry delete
        Status rc = Status::OK();
        do {
            rc = spClient->DeleteStream(streamName);
            if (rc.IsError()) {
                sleep(K_TWO);
            }
        } while (rc.GetCode() == StatusCode::K_SC_STREAM_NOTIFICATION_PENDING);
        return rc;
    }

    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_;
    std::atomic<int> waitForGoCount_;
    std::atomic<bool> interrupt_;

    // These knobs tune how RecvStreamData and SendStreamData behave, since different testcases want to test
    // different scenarios. It's easier to store them here in the class with defaults rather than pass them around.
    bool slowConsume_ = false;      // Causes the receiver to pause at the start to allow sender to fill up and OOM
    bool slowReceive_ = false;      // Another way to throttle the receive side. It injects a sleep between recv's.
    bool validate_ = true;          // Inspects every record at the receiver. Cannot be used with multi producers
    bool waitForGo_ = false;        // used with waitForGoCount_ to sync producers
    bool checkNumOfFails_ = false;  // Provides a cap on retries and eventually fails if too many
    bool clientRetry_ = true;       // Toggles if the client should rerun the send if it got on OOM.
    bool autoAck_ = false;          // Toggles auto-ack mode
    uint16_t prefetchLWM_ = 0;      // Enable client prefetch if non-zero (0 to 100)
    uint32_t clientCacheSize_ = 0;  // client cache size. (0 means use the external default)
    uint64_t earlyExitCount_ = 0;  // Set to non-zero value if you want the receive to quit early once N elements recv'd
    int64_t sendTimeout_ = 1;      // timeout arg for send calls
    uint64_t eleSz_ = 1024;        // size of each element
    size_t ackInterval_ = 0;       // ack interval
    uint64_t elementsTotalSize_ = ELEMENTS_TOTAL_SIZE;  // total size of all elements. gets carved into elements
};

// Configure with a small amount
class BigShmTest : public ProducerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = NUM_WORKERS;
        opts.workerGflagParams = "-shared_memory_size_mb=512";
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    Status MultiTest_NStream(std::vector<std::shared_ptr<StreamClient>> &clients, int64_t pageSize,
                             uint64_t maxStreamSize, int numStreams, int numProds, int numSubs);
};

Status BigShmTest::MultiTest_NStream(std::vector<std::shared_ptr<StreamClient>> &clients, int64_t pageSize,
                                     uint64_t maxStreamSize, int numStreams, int numProds, int numSubs)
{
    ThreadPool preparePool(1);
    uint64_t eleSz = eleSz_;
    uint64_t numElements = elementsTotalSize_ / eleSz;
    waitForGoCount_ = numSubs;
    waitForGo_ = true;  // Ensure sync'd up before sending starts
    checkNumOfFails_ = true;

    LOG(INFO) << FormatString("Testing Size: %zu", eleSz);
    std::shared_future<std::vector<std::string>> elementsFut = preparePool.Submit([eleSz, numElements]() {
        ElementGenerator elementGenerator(eleSz + 1, std::min(eleSz, KB));
        auto elements = elementGenerator.GenElements("producer", numElements, 8ul);
        LOG(INFO) << "Element data generated. return from generator thread.";
        return elements;
    });

    // Wait for the data generation to complete before we launch producers and consumers
    LOG(INFO) << "Waiting for data generation";
    while (elementsFut.wait_for(std::chrono::seconds(1)) != std::future_status::ready)
        ;
    LOG(INFO) << "Data generation complete. kick off threads now";

    ThreadPool pool(numProds + numSubs);  // enough threads for the producers and consumers
    std::vector<std::string> streamNames;

    for (int i = 0; i < numStreams; ++i) {
        std::string newStream = "stream" + std::to_string(i);
        streamNames.push_back(newStream);
    }

    // kick off the producers first.
    std::unordered_map<std::string, uint64_t> expectedRecvCounts;
    std::vector<std::future<Status>> prodFutures;
    for (int i = 0; i < numProds; ++i) {
        // Round robin the client for each producer. This will spread producers over the list of workers.
        int clientIdx = i % clients.size();
        std::shared_ptr<StreamClient> prodClient = clients[i % clients.size()];
        LOG(INFO) << "Creating a producer/sender client thread for client/worker index: " << clientIdx;

        // Round robin the stream name as well for each producer
        std::string streamName = streamNames[i % numStreams];

        // What if multiple producers target the same stream?  Say stream 1 gets 10 records from producer1.
        // But producer2 is also sending to stream 1, so the number of data sent is 20 into that stream.
        // Identity the expected receive counts for each stream.
        auto iter = expectedRecvCounts.find(streamName);
        if (iter == expectedRecvCounts.end()) {
            // This stream not accounted for yet. Insert new key into the hash table with the expected initial count
            expectedRecvCounts[streamName] = numElements;
        } else {
            // Another producer targets this same stream.  bump the expected count.
            iter->second += numElements;
        }

        prodFutures.emplace_back(
            pool.Submit([this, &i, streamName, &elementsFut, numElements, prodClient, pageSize, maxStreamSize]() {
                ProducerConf prodCfg = {
                    .delayFlushTime = 20, .pageSize = pageSize, .maxStreamSize = maxStreamSize, .autoCleanup = true
                };
                prodCfg.reserveSize = maxStreamSize;  // reserve everything
                SendConfig sendCfg = { .streamName = streamName,
                                       .producerName = "producer",
                                       .producerConf = prodCfg,
                                       .numOfElements = numElements };
                return SendStreamData(sendCfg, 1, elementsFut, prodClient);
            }));
    }

    std::vector<std::future<Status>> subFutures;
    // instead of using i, create a custom counter for the receiver that is 1 off the value of i.
    // This results in staggering the clients.  so:
    // - producer on client0 pairs with consumer on client1
    // - producer on client1 pairs with consumer on client0
    // What if we want to have same-node client's but multiple workers?  No supported at this time for this
    // test function.
    int clientCounter = 1;
    for (int i = 0; i < numSubs; ++i) {
        uint64_t numStreamElements = 0;
        // Round robin the client for each consumer.
        int clientIdx = clientCounter % clients.size();
        std::shared_ptr<StreamClient> conClient = clients[clientIdx];
        LOG(INFO) << "Creating a receiver/consumer client thread for client/worker index: " << clientIdx;
        ++clientCounter;

        // Round robin the stream name as well for each consumer.
        std::string streamName = streamNames[i % numStreams];

        numStreamElements = expectedRecvCounts[streamName];

        size_t ackInterval;
        if (ackInterval_ == 0) {
            ackInterval = std::max<size_t>(400ul * KB / eleSz, 1ul);
        } else {
            ackInterval = ackInterval_;
        }
        subFutures.emplace_back(pool.Submit([this, &i, streamName, numStreamElements, ackInterval, conClient]() {
            RecvConfig rcvCfg = { .streamName = streamName,
                                  .subscriptionName = "subscription",
                                  .numOfBatchElements = 100,
                                  .timeToWaitMs = 20,
                                  .ackInterval = ackInterval,
                                  .autoAck = autoAck_ };
            return RecvStreamData(rcvCfg, numStreamElements, conClient);
        }));
    }

    // If any of the producers or subscribers got non-ok, they will assign return rc.
    // The last non-ok error collected will be the winner to return to the caller.
    Status returnRc = Status::OK();
    for (int i = 0; i < numSubs; ++i) {
        Status rc = subFutures[i].get();
        if (rc.IsError()) {
            returnRc = rc;
        }
    }

    for (int i = 0; i < numProds; ++i) {
        Status rc = prodFutures[i].get();
        if (rc.IsError()) {
            returnRc = rc;
        }
    }

    return returnRc;
}

Status CreateElement(size_t elementSize, Element &element, std::vector<uint8_t> &writeElement)
{
    writeElement = RandomData().RandomBytes(elementSize);
    element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    return Status::OK();
}

TEST_F(ProducerTest, LEVEL1_TestSendTriggerBackPressure)
{
    Timer timer;
    ThreadPool pool(2);

    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    // Producer sends in total 32 MB data to a stream of 4 MB capacity and 64 KB-sized pages.
    size_t numOfElements = 320000;
    auto producerFut = pool.Submit([this, numOfElements, spClient]() {
        size_t numOfRetries = 0;
        std::shared_ptr<Producer> producer;
        ProducerConf producerConf{ .delayFlushTime = 20, .pageSize = 64 * KB, .maxStreamSize = 4 * MB };
        uint64_t elementSize = 100;
        ElementGenerator elementGenerator(elementSize + 1, elementSize);
        uint64_t numOfConsumers = 0;
        while (numOfConsumers != 1) {
            spClient->QueryGlobalConsumersNum("SendTriggerBackPressure", numOfConsumers);
        }
        RETURN_IF_NOT_OK(spClient->CreateProducer("SendTriggerBackPressure", producer, producerConf));
        auto elements = elementGenerator.GenElements("producer", numOfElements, 8ul);
        Timer timer;
        for (size_t i = 0; i < numOfElements; i++) {
            // Resend if fail due to the async back-pressure mechanism.
            auto element = Element(reinterpret_cast<uint8_t *>(&elements[i].front()), elements[i].size());
            auto status = producer->Send(element);
            while (!status.IsOk()) {
                numOfRetries++;
                status = producer->Send(element);
            }
        }
        LOG(INFO) << FormatString("Producer's number of re-sending: %zu, sending time: %.6lf s", numOfRetries,
                                  timer.ElapsedSecond());
        return Status::OK();
    });

    // Receiver receive all the data.
    auto consumerFut = pool.Submit([this, numOfElements, spClient]() {
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(spClient->Subscribe("SendTriggerBackPressure", config, consumer));
        std::unordered_map<std::string, uint64_t> seqNoMap;
        Timer timer;
        size_t ackInterval = 4096;
        for (size_t i = 0; i < numOfElements;) {
            std::vector<Element> outElements;
            consumer->Receive(1, 20, outElements);
            i += outElements.size();
            for (auto &element : outElements) {
                ElementView elementView(std::string(reinterpret_cast<char *>(element.ptr), element.size));
                RETURN_IF_NOT_OK(elementView.VerifyFifo(seqNoMap));
                RETURN_IF_NOT_OK(elementView.VerifyIntegrity());
            }
            if (i % ackInterval == (ackInterval - 1)) {
                LOG(INFO) << FormatString("Ack id: %zu", outElements.back().id);
                RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
            }
        }
        LOG(INFO) << FormatString("Total Recv Time Elapsed: %.6lf s", timer.ElapsedSecond());
        return Status::OK();
    });

    ASSERT_EQ(consumerFut.get(), Status::OK());
    ASSERT_EQ(producerFut.get(), Status::OK());
    LOG(INFO) << FormatString("End To End Time Elapsed: %.6lf s", timer.ElapsedSecond());
}

Status ProducerTest::RecvStreamData(const RecvConfig &recvConfig, size_t numOfElements,
                                    std::shared_ptr<StreamClient> spClient)
{
    // Step 1: Subscribe.
    // On producer side, we ensure waiting for all the subscriptions before data sending.
    struct SubscriptionConfig cfg;
    cfg.subscriptionName = recvConfig.subscriptionName;
    cfg.subscriptionType = SubscriptionType::STREAM;
    if (prefetchLWM_) {
        cfg.cachePrefetchLWM = prefetchLWM_;
    }
    if (clientCacheSize_) {
        cfg.cacheCapacity = clientCacheSize_;
    }
    std::shared_ptr<Consumer> consumer;
    RETURN_IF_NOT_OK(spClient->Subscribe(recvConfig.streamName, cfg, consumer, recvConfig.autoAck));

    LOG(INFO) << "Stream consumer created";
    --waitForGoCount_;

    if (slowConsume_) {
        // purposely cause a delay so that the producer will fill up the memory and get an OOM.
        // Once this sleep is done, then it will start to drain and clear the OOM issues.
        LOG(INFO) << "Pausing the consumer to cause producer to build up data";
        std::this_thread::sleep_for(std::chrono::seconds(SLOW_CONSUME_WAIT));
    }

    // Step 2: Receive and verify elements.
    LOG(INFO) << "Starting receive loop. This consumer will expect to receive " << numOfElements << " elements";
    std::unordered_map<std::string, uint64_t> seqNoMap;
    Timer timer;
    size_t ackInterval = recvConfig.ackInterval;
    size_t toAckNum = 0;
    std::vector<Element> outElements;
    for (size_t i = 0; i < numOfElements && !interrupt_;) {
        RETURN_IF_NOT_OK(consumer->Receive(recvConfig.numOfBatchElements, RECV_WAIT_MILLI_SECONDS, outElements));
        i += outElements.size();
        // Don't log all the retries from timed out Receive calls.  Only log if we got something.
        if (outElements.size() != 0) {
            LOG(INFO) << "Consumer received. total elements read so far: " << i;
        }
        if (validate_) {
            for (auto &element : outElements) {
                ElementView elementView(std::string(reinterpret_cast<char *>(element.ptr), element.size));
                RETURN_IF_NOT_OK(elementView.VerifyFifo(seqNoMap));
                RETURN_IF_NOT_OK(elementView.VerifyIntegrity());
            }
        } else if (slowReceive_) {
            // Make the receiver a bit slower in between each call.
            // If validate is true, you get this for "free" because generally validating takes some work to compare
            // the data which makes the receive side run slow between each receive.
            const int recvSleepTime = 450;
            std::this_thread::sleep_for(std::chrono::microseconds(recvSleepTime));
        }
        toAckNum += outElements.size();
        if (!autoAck_ && toAckNum >= ackInterval) {
            LOG(INFO) << FormatString("Ack id: %zu", outElements.back().id);
            RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
            toAckNum = 0;
        }

        if (earlyExitCount_ != 0 && i > earlyExitCount_) {
            LOG(INFO) << "Consumer has reached its early exit row count of " << earlyExitCount_
                      << FormatString(". Total Recv Time Elapsed: %.6lf s", timer.ElapsedSecond());
            return Status::OK();
        }
    }
    if (interrupt_) {
        LOG(INFO) << "Consumer loop quits due to interrupt";
    }

    LOG(INFO) << FormatString("Total Recv Time Elapsed: %.6lf s", timer.ElapsedSecond());
    return Status::OK();
}

// For test purposes. Testing back pressure. Modified RecvStreamData where slowReceive_ is true.
// else if (slowReceive_) is removed to always delay between recv.
Status ProducerTest::RecvStreamDataWithSlowReceive(const RecvConfig &recvConfig, size_t numOfElements,
                                                   std::shared_ptr<StreamClient> spClient)
{
    struct SubscriptionConfig cfg;
    cfg.subscriptionName = recvConfig.subscriptionName;
    cfg.subscriptionType = SubscriptionType::STREAM;
    if (prefetchLWM_) {
        cfg.cachePrefetchLWM = prefetchLWM_;
    }
    if (clientCacheSize_) {
        cfg.cacheCapacity = clientCacheSize_;
    }
    std::shared_ptr<Consumer> consumer;
    RETURN_IF_NOT_OK(spClient->Subscribe(recvConfig.streamName, cfg, consumer, recvConfig.autoAck));

    LOG(INFO) << "Stream slow consumer created";
    --waitForGoCount_;

    if (slowConsume_) {
        LOG(INFO) << "Pausing the slow consumer to cause producer to build up data";
        std::this_thread::sleep_for(std::chrono::seconds(SLOW_CONSUME_WAIT));
    }

    LOG(INFO) << "Starting receive loop. This slow consumer will expect to receive " << numOfElements << " elements";
    std::unordered_map<std::string, uint64_t> seqNoMap;
    Timer timer;
    size_t ackInterval = recvConfig.ackInterval, toAckNum = 0;
    std::vector<Element> outElements;
    for (size_t i = 0; i < numOfElements && !interrupt_;) {
        RETURN_IF_NOT_OK(consumer->Receive(recvConfig.numOfBatchElements, RECV_WAIT_MILLI_SECONDS, outElements));
        i += outElements.size();
        LOG(INFO) << "Slow consumer received. total elements read so far: " << i;
        if (validate_) {
            for (auto &element : outElements) {
                ElementView elementView(std::string(reinterpret_cast<char *>(element.ptr), element.size));
                RETURN_IF_NOT_OK(elementView.VerifyFifo(seqNoMap));
                RETURN_IF_NOT_OK(elementView.VerifyIntegrity());
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));  // delay to slow recv
        toAckNum += outElements.size();
        if (!autoAck_ && toAckNum >= ackInterval) {
            LOG(INFO) << FormatString("Slow consumer Ack id: %zu", outElements.back().id);
            RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
            toAckNum = 0;
        }

        if (earlyExitCount_ != 0 && i > earlyExitCount_) {
            LOG(INFO) << "Slow consumer has reached its early exit row count of " << earlyExitCount_;
            return Status::OK();
        }
    }
    if (interrupt_) {
        LOG(INFO) << "Slow consumer loop quits due to interrupt";
    }

    LOG(INFO) << FormatString("Slow consumer Total Recv Time Elapsed: %.6lf s", timer.ElapsedSecond());
    return Status::OK();
}

void ProducerTest::WaitForConsumers(std::shared_ptr<StreamClient> spClient, std::string streamName,
                                    uint64_t expectedNumOfConsumers)
{
    uint64_t numOfConsumers = 0;
    while (true) {
        spClient->QueryGlobalConsumersNum(streamName, numOfConsumers);
        LOG(INFO) << "Current NumOf Consumers: " << numOfConsumers;
        if (numOfConsumers == expectedNumOfConsumers) {
            break;
        }
        auto sleepUseconds = 5000ul;
        usleep(sleepUseconds);
    }
}

// Helper function used in SendStreamDataSlow. Removes duplicate code and to
// keep function <= 50 lines
Status ProducerTest::SendHelper(const std::vector<std::string> &elements, const SendConfig &sendConfig,
                                std::shared_ptr<Producer> producer, std::atomic<size_t> &numOfRetries,
                                unsigned long failInterval, bool slowSend)
{
    for (size_t i = 0; i < sendConfig.numOfElements; ++i) {
        if (slowSend) {
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));  // add delay on send
        }
        // Resend if fail due to the async back-pressure mechanism.
        auto idx = i % elements.size();
        // Force cast due to element interface.
        auto element = Element((uint8_t *)(elements[idx].data()), elements[idx].size());
        auto status = producer->Send(element, sendTimeout_);
        if (status.IsError()) {
            LOG(INFO) << "Send element " << idx << " failed. Will do client retry? " << std::boolalpha << clientRetry_
                      << " " << status.ToString();
            if (!clientRetry_) {
                interrupt_ = true;  // break the receiver loop also and quit the test
                return status;
            }
        }
        while (!status.IsOk() && clientRetry_) {
            // Avoiding consumers being too slow to process
            usleep(failInterval);
            numOfRetries++;
            if (checkNumOfFails_) {
                size_t retryMaxTimes = 30u;
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(numOfRetries < retryMaxTimes, K_RUNTIME_ERROR, "too many retries");
            }
            status = producer->Send(element);
        }
    }
    return Status::OK();
}

Status ProducerTest::SendStreamData(const SendConfig &sendConfig, uint64_t expectedNumOfConsumers,
                                    std::shared_future<std::vector<std::string>> &elementsFut,
                                    std::shared_ptr<StreamClient> spClient)
{
    auto failInterval = 500000ul;
    // Step 1: Create a producer.
    std::shared_ptr<Producer> producer;
    RETURN_IF_NOT_OK(spClient->CreateProducer(sendConfig.streamName, producer, sendConfig.producerConf));

    // Step 2: Wait for consumers.
    WaitForConsumers(spClient, sendConfig.streamName, expectedNumOfConsumers);

    if (waitForGo_) {
        // block until the receiver side tells us to go. When each receiver is ready it decrements the waitForGo count.
        while (waitForGoCount_ > 0) {
            LOG(INFO) << "Waiting for consumer...";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    LOG(INFO) << "Starting send loop";

    // Step 3: Send elements and retry on failure.
    Timer timer;
    uint64_t numOfRetries = 0;
    const std::vector<std::string> &elements = elementsFut.get();
    for (size_t i = 0; i < sendConfig.numOfElements; i++) {
        // Resend if fail due to the async back-pressure mechanism.
        auto idx = i % elements.size();
        // Force cast due to element interface.
        auto element = Element((uint8_t *)(elements[idx].data()), elements[idx].size());
        auto status = producer->Send(element, sendTimeout_);
        if (status.IsError()) {
            LOG(INFO) << "Send element " << idx << " failed. Will do client retry? " << std::boolalpha << clientRetry_
                      << " " << status.ToString();
            if (!clientRetry_) {
                interrupt_ = true;  // break the receiver loop also and quit the test
                return status;
            }
        }
        while (!status.IsOk() && clientRetry_) {
            // Avoiding consumers being too slow to process
            usleep(failInterval);
            numOfRetries++;
            if (checkNumOfFails_) {
                size_t retryMaxTimes = 30u;
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(numOfRetries < retryMaxTimes, K_RUNTIME_ERROR, "too many retries");
            }
            status = producer->Send(element);
        }
    }
    LOG(INFO) << FormatString("Producer's number of re-sending: %zu/%zu, sending time: %.6lf s", numOfRetries,
                              sendConfig.numOfElements, timer.ElapsedSecond());

    LOG(INFO) << "Small sleep so that producing workers fully send all their data before closing.";
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // producer ptr will descope here and destructor call close (not the recommended say to close!)
    return Status::OK();
}

// For test purposes. Testing back pressure. 2 producers send. One producer sends slower than other.
Status ProducerTest::SendStreamDataSlow(const SendConfig &sendConfig1, const SendConfig &sendConfig2,
                                        uint64_t expectedNumOfConsumers,
                                        std::shared_future<std::vector<std::string>> &elementsFut1,
                                        std::shared_future<std::vector<std::string>> &elementsFut2,
                                        std::shared_ptr<StreamClient> spClient1,
                                        std::shared_ptr<StreamClient> spClient2)
{
    auto failInterval = 500000ul;
    // Step 1: Create a producer.
    std::shared_ptr<Producer> producer1, producer2;
    RETURN_IF_NOT_OK(spClient1->CreateProducer(sendConfig1.streamName, producer1, sendConfig1.producerConf));
    RETURN_IF_NOT_OK(spClient2->CreateProducer(sendConfig2.streamName, producer2, sendConfig2.producerConf));

    // Step 2: Wait for consumers.
    WaitForConsumers(spClient1, sendConfig1.streamName, expectedNumOfConsumers);
    WaitForConsumers(spClient2, sendConfig2.streamName, expectedNumOfConsumers);

    if (waitForGo_) {
        // block until the receiver side tells us to go. When each receiver is ready it decrements the waitForGo count.
        while (waitForGoCount_ > 0) {
            LOG(INFO) << "Waiting for consumer...";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    LOG(INFO) << "Starting send loop producers";

    // Step 3: Send elements and retry on failure.
    Timer timer;
    std::atomic<uint64_t> numOfRetries1{ 0 };
    std::atomic<uint64_t> numOfRetries2{ 0 };
    const std::vector<std::string> &elements1 = elementsFut1.get();
    const std::vector<std::string> &elements2 = elementsFut2.get();

    // Send data for producer1
    auto sendProducer1 = [&]() {
        SendHelper(elements1, sendConfig1, producer1, numOfRetries1, failInterval, false);
        LOG(INFO) << FormatString("Producer1's number of re-sending: %zu/%zu, sending time: %.6lf s", numOfRetries1,
                                  sendConfig1.numOfElements, timer.ElapsedSecond());
        return Status::OK();
    };

    // Slow send data for producer2
    auto sendProducer2 = [&]() {
        SendHelper(elements2, sendConfig2, producer2, numOfRetries2, failInterval, true);
        LOG(INFO) << FormatString("Producer2's number of re-sending: %zu/%zu, sending time: %.6lf s", numOfRetries2,
                                  sendConfig2.numOfElements, timer.ElapsedSecond());
        return Status::OK();
    };

    std::thread producer2Thread(sendProducer1);
    std::thread producer1Thread(sendProducer2);

    producer2Thread.join();
    producer1Thread.join();

    LOG(INFO) << "Producer Small sleep so that producing workers fully send all their data before closing.";
    // std::this_thread::sleep_for(std::chrono::seconds(5));
    // producer ptr will descope here and destructor call close (not the recommended say to close!)
    return Status::OK();
}

Status ProducerTest::RunOOMTest(std::shared_ptr<StreamClient> prodClient, std::shared_ptr<StreamClient> conClient,
                                std::string streamName, uint64_t maxStreamSizeMB)
{
    ThreadPool preparePool(1);
    uint64_t eleSz = 8192ul;  // this should be small element, not big element
    uint64_t numElements = elementsTotalSize_ / eleSz;
    slowConsume_ = true;
    waitForGo_ = true;

    LOG(INFO) << FormatString("Testing Size: %zu", eleSz);
    std::shared_future<std::vector<std::string>> elementsFut = preparePool.Submit([eleSz, numElements]() {
        ElementGenerator elementGenerator(eleSz + 1, eleSz);
        auto elements = elementGenerator.GenElements("producer", numElements, 8ul);
        LOG(INFO) << "Element data generated. return from generator thread.";
        return elements;
    });

    // Wait for the data generation to complete before we launch producers and consumers
    LOG(INFO) << "Waiting for data generation";
    while (elementsFut.wait_for(std::chrono::seconds(1)) != std::future_status::ready)
        ;
    LOG(INFO) << "Data generation complete. kick off threads now";

    ThreadPool pool(CLIENT_THREAD_POOL_SIZE);
    auto producerFut = pool.Submit([this, streamName, &elementsFut, numElements, prodClient, maxStreamSizeMB]() {
        ProducerConf prodCfg = { .delayFlushTime = 20, .pageSize = 1 * MB, .maxStreamSize = maxStreamSizeMB * MB };
        SendConfig sendCfg = {
            .streamName = streamName, .producerName = "producer", .producerConf = prodCfg, .numOfElements = numElements
        };
        return SendStreamData(sendCfg, 1, elementsFut, prodClient);
    });

    size_t ackInterval = std::max<size_t>(400ul * KB / eleSz, 1ul);
    auto consumerFut = pool.Submit([this, streamName, numElements, ackInterval, conClient]() {
        RecvConfig rcvCfg = { .streamName = streamName,
                              .subscriptionName = "subscription",
                              .numOfBatchElements = 100,
                              .timeToWaitMs = 20,
                              .ackInterval = ackInterval,
                              .autoAck = false };
        return RecvStreamData(rcvCfg, numElements, conClient);
    });

    // Collect the status after threads complete
    Status cStatus = consumerFut.get();
    Status pStatus = producerFut.get();

    // If either the producer or consumer run got an error, return their rc as the overall rc of the test run
    if (cStatus.IsError()) {
        return cStatus;
    } else if (pStatus.IsError()) {
        return pStatus;
    }

    return Status::OK();
}

TEST_F(ProducerTest, LEVEL1_TestVaryingEleSz)
{
    ThreadPool preparePool(1);
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());

    std::vector<uint64_t> eleSzs = { 100, 1 * KB, 4 * KB, 16 * KB };
    int i = 0;
    for (uint64_t eleSz : eleSzs) {
        LOG(INFO) << FormatString("Testing Size: %zu", eleSz);
        const int elementsTotalSize_ = 16 * MB;
        uint64_t numElements = elementsTotalSize_ / eleSz;
        std::shared_future<std::vector<std::string>> elementsFut = preparePool.Submit([eleSz, numElements]() {
            ElementGenerator elementGenerator(eleSz + 1, eleSz);
            auto elements = elementGenerator.GenElements("producer", numElements, 8ul);
            return elements;
        });

        ThreadPool pool(2);
        std::string streamName = FormatString("stream%d", i++);
        auto producerFut = pool.Submit([this, streamName, &elementsFut, numElements, spClient]() {
            ProducerConf prodCfg = { .delayFlushTime = 20, .pageSize = 1 * MB, .maxStreamSize = 4 * MB };
            SendConfig sendCfg = { .streamName = streamName,
                                   .producerName = "producer",
                                   .producerConf = prodCfg,
                                   .numOfElements = numElements };
            return SendStreamData(sendCfg, 1, elementsFut, spClient);
        });
        size_t ackInterval = std::max<size_t>(400ul * KB / eleSz, 1ul);
        auto consumerFut = pool.Submit([this, streamName, numElements, ackInterval, spClient]() {
            RecvConfig rcvCfg = { .streamName = streamName,
                                  .subscriptionName = "subscription",
                                  .numOfBatchElements = 100,
                                  .timeToWaitMs = 20,
                                  .ackInterval = ackInterval,
                                  .autoAck = false };
            return RecvStreamData(rcvCfg, numElements, spClient);
        });
        ASSERT_EQ(consumerFut.get(), Status::OK());
        ASSERT_EQ(producerFut.get(), Status::OK());
    }

    spClient = nullptr;
}

TEST_F(ProducerTest, TestNoConsumer)
{
    for (int j = 0; j < 5; j++) {
        std::shared_ptr<StreamClient> spClient;
        ASSERT_EQ(CreateClient(0, spClient), Status::OK());
        auto stream_name = "test_dfx_streamcache_node_scale_004";
        ProducerConf prodCfg = { .delayFlushTime = 5, .pageSize = 1 * MB, .maxStreamSize = 2 * MB };
        std::shared_ptr<Producer> producer;
        spClient->CreateProducer(stream_name, producer, prodCfg);
        for (int i = 0; i < 1000; i++) {
            std::string data = "test ";
            Element element(reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.length());
            ASSERT_EQ(producer->Send(element), Status::OK());
        }
    }
}

TEST_F(ProducerTest, TestInvalidSend)
{
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient->Subscribe("InvalidSend", config, consumer));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient->CreateProducer("InvalidSend", producer, defaultProducerConf_));

    std::string data = "Hello World";
    Element element1(reinterpret_cast<uint8_t *>(&data.front()), 0);

    // 1. Send element with 0 size
    const uint32_t timeoutMs = 1000;
    std::vector<Element> outElements;
    DS_ASSERT_NOT_OK(producer->Send(element1));
    DS_ASSERT_OK(consumer->Receive(1, timeoutMs, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
    outElements.clear();

    // 2. Send element with nullptr
    Element element2(nullptr, data.length());
    DS_ASSERT_NOT_OK(producer->Send(element2));
    DS_ASSERT_OK(consumer->Receive(1, timeoutMs, outElements));
    ASSERT_EQ(outElements.size(), size_t(0));
    outElements.clear();
}

TEST_F(ProducerTest, TestVaryingEleSzBigElement)
{
    ThreadPool preparePool(1);
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    checkNumOfFails_ = true;
    uint64_t eleSz = 64 * KB;
    const int elementsTotalSize_ = 32 * MB;
    uint64_t numElements = elementsTotalSize_ / eleSz;
    LOG(INFO) << FormatString("Testing Size: %zu", eleSz);
    std::shared_future<std::vector<std::string>> elementsFut = preparePool.Submit([eleSz, numElements]() {
        ElementGenerator elementGenerator(eleSz + 1, eleSz);
        auto elements = elementGenerator.GenElements("producer", numElements, 8ul);
        return elements;
    });

    ThreadPool pool(2);
    std::string streamName = "VaryingEleSzBigElement";
    auto producerFut = pool.Submit([this, streamName, &elementsFut, numElements, spClient]() {
        ProducerConf prodCfg = { .delayFlushTime = 20, .pageSize = 1 * MB, .maxStreamSize = 4 * MB };
        SendConfig sendCfg = {
            .streamName = streamName, .producerName = "producer", .producerConf = prodCfg, .numOfElements = numElements
        };
        return SendStreamData(sendCfg, 1, elementsFut, spClient);
    });

    //  To analyze big element memory issue.
    //  Another configuration: size_t ackInterval = std::max<size_t>(400ul * KB / eleSz, 1ul);
    size_t ackInterval = 1ul;
    auto consumerFut = pool.Submit([this, streamName, numElements, ackInterval, spClient]() {
        RecvConfig rcvCfg = { .streamName = streamName,
                              .subscriptionName = "subscription",
                              .numOfBatchElements = 100,
                              .timeToWaitMs = 20,
                              .ackInterval = ackInterval,
                              .autoAck = false };
        return RecvStreamData(rcvCfg, numElements, spClient);
    });
    ASSERT_EQ(producerFut.get(), Status::OK());
    ASSERT_EQ(consumerFut.get(), Status::OK());

    spClient = nullptr;
}

TEST_F(ProducerTest, TestBlockingUnBlockingShm)
{
    LOG(INFO) << "TestWorkerCrashStopRemotePush start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    ASSERT_EQ(CreateClient(1, client2), Status::OK());

    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.maxStreamSize = 10 * 1024 * 1024;
    DS_ASSERT_OK(client1->CreateProducer("BlockingUnBlockingShm", producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("BlockingUnBlockingShm", config, consumer));

    const size_t testSize = 4ul * 1024ul;
    // Keep sending until out of memory
    size_t sendCount = 0;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(testSize, element, writeElement);
    while (true) {
        Status rc = producer->Send(element);
        if (rc.IsOk()) {
            ++sendCount;
            usleep(10);
            continue;
        }
        ASSERT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
        break;
    }

    uint64_t elementId = 0;
    std::vector<Element> outElements;
    while (sendCount) {
        DS_ASSERT_OK(consumer->Receive(1, 1000, outElements));
        DS_ASSERT_OK(consumer->Ack(++elementId));
        --sendCount;
        outElements.clear();
    }
}

TEST_F(ProducerTest, TestBlockingUnBlockingShmOutofOrder)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "RemoteWorker.EnableStreamBlocking.sleep", "11*sleep(2000)"));
    LOG(INFO) << "TestWorkerCrashStopRemotePush start!";
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    ASSERT_EQ(CreateClient(1, client2), Status::OK());

    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.maxStreamSize = 10 * 1024 * 1024;
    DS_ASSERT_OK(client1->CreateProducer("ShmOutofOrder", producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("ShmOutofOrder", config, consumer));

    const size_t testSize = 1024 * 1024ul;
    // Keep sending until out of memory
    size_t sendCount = 1000;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(testSize, element, writeElement);
    ThreadPool pool(2);
    auto producerFut = pool.Submit([this, producer, element]() {
        size_t sendCount = 0;
        while (sendCount != 1000) {
            Status rc = producer->Send(element);
            if (rc.IsOk()) {
                ++sendCount;
                usleep(10);
                continue;
            }
            LOG(INFO) << rc.GetCode();
        }
    });
    sleep(1);
    LOG(INFO) << "sendCount " << sendCount;
    int count = 0;
    while (sendCount) {
        std::vector<Element> outElements;
        const int recvTimeout = 1000;
        DS_ASSERT_OK(consumer->Receive(1, recvTimeout, outElements));
        if (outElements.empty()) {
            LOG(INFO) << "empty........ " << count;
            continue;
        }
        DS_ASSERT_OK(consumer->Ack(outElements.back().id));
        count += outElements.size();
        --sendCount;
    }
    producerFut.get();
}

TEST_F(ProducerTest, TestUnblockingWithEarlyAck)
{
    LOG(INFO) << "TestUnblockingWithEarlyAck start!";
    std::shared_ptr<StreamClient> client1, client2;

    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    ASSERT_EQ(CreateClient(1, client2), Status::OK());

    std::shared_ptr<Producer> producer;
    const int DEFAULT_MAX_STREAM_SIZE = 5 * MB;
    ProducerConf conf;
    conf.maxStreamSize = DEFAULT_MAX_STREAM_SIZE;
    conf.pageSize = 1 * MB;
    DS_ASSERT_OK(client1->CreateProducer("EarlyAck", producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("EarlyAck", config, consumer));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "StreamManager.SendBlockProducerReq.delay", "1*sleep(5000)"));

    const size_t testSize = 500 * KB;
    // Keep sending until out of memory
    const size_t SEND_COUNT = 100;
    std::thread producerThrd([&producer]() {
        const int DEFAULT_SLEEP_TIME = 200;
        Element element;
        std::vector<uint8_t> writeElement;
        CreateElement(testSize, element, writeElement);
        for (size_t i = 0; i < SEND_COUNT; i++) {
            Status rc = producer->Send(element);
            int retryCount = 30;
            while (rc.GetCode() == K_OUT_OF_MEMORY && retryCount-- > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                rc = producer->Send(element);
            }
            DS_ASSERT_OK(rc);
        }
    });

    const int DEFAULT_WAIT_TIME = 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_WAIT_TIME));
    const int K_100 = 100;
    const int DEFAULT_RETRY_TIME = 30;
    Timer timer;
    std::vector<Element> outElements;
    int sendCount = SEND_COUNT;
    while (sendCount > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
        DS_ASSERT_OK(consumer->Receive(1, K_100, outElements));
        if (!outElements.empty()) {
            DS_ASSERT_OK(consumer->Ack(outElements.back().id));
            sendCount -= outElements.size();
        }
    }
    ASSERT_EQ(sendCount, 0);
    producerThrd.join();
}

TEST_F(ProducerTest, TestWriteLessThanWritePage)
{
    LOG(INFO) << "test WriteLessThanWritePage start!";
    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient->Subscribe("WriteLessThanWritePage", config, consumer));

    size_t testSize = 4ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient->CreateProducer("WriteLessThanWritePage", producer, defaultProducerConf_));
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    ASSERT_EQ(producer->Send(element), Status::OK());

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
    EXPECT_EQ(data, actualData);
    spClient = nullptr;
}

TEST_F(ProducerTest, TestWriteWithPages)
{
    LOG(INFO) << "test WriteWithPages start!";
    size_t testSize = 60ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    DS_ASSERT_OK(spClient->CreateProducer("WriteWithPages", producer, defaultProducerConf_));

    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient->Subscribe("WriteWithPages", config, consumer));

    std::string writeData;
    for (int i = 0; i < count; i++) {
        DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
        ASSERT_EQ(producer->Send(element), Status::OK());
        std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
        writeData += data;
    }

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(count, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), static_cast<size_t>(count));
    std::string actualData;
    for (int i = 0; i < count; i++) {
        std::string tmp(reinterpret_cast<char *>(outElements[i].ptr), outElements[i].size);
        actualData += tmp;
    }
    EXPECT_EQ(writeData, actualData);
    spClient = nullptr;
}

TEST_F(ProducerTest, TestWriteFlushAndCreatePage)
{
    LOG(INFO) << "test WriteBigElement start!";
    size_t testSize = 1000ul * 1024ul;
    Element element1, element2;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    DS_ASSERT_OK(spClient->CreateProducer("WriteFlushAndCreatePage", producer, defaultProducerConf_));

    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient->Subscribe("WriteFlushAndCreatePage", config, consumer));

    // Disable autoflush
    DS_ASSERT_OK(inject::Set("Client.ProducerImpl.SendWithNoAutoFlush", "1*call(-1)"));
    std::vector<uint8_t> writeElement;
    DS_ASSERT_OK(CreateElement(testSize, element1, writeElement));
    DS_ASSERT_OK(producer->Send(element1));
    // Page size 1024*1024 byte. Second write will overflow the page.
    // Send again for the flush and create page request together.
    DS_ASSERT_OK(CreateElement(testSize, element2, writeElement));
    DS_ASSERT_OK(producer->Send(element2));
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), (size_t)1);
    outElements.clear();
    ASSERT_EQ(consumer->Receive(1, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), (size_t)1);
    spClient.reset();
}

TEST_F(ProducerTest, TestWriteLocalBigElementSuccess)
{
    LOG(INFO) << "test WriteLocalBigElement start!";
    size_t testSize = 2ul * 1024ul * 1024ul;
    Element element;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    DS_ASSERT_OK(spClient->CreateProducer("WriteLocalBigElement", producer, defaultProducerConf_));

    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient->Subscribe("WriteLocalBigElement", config, consumer));

    std::vector<uint8_t> writeElement;
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    Status rc = producer->Send(element);
    LOG(INFO) << "Expected to get K_OK. Rc returned: " << rc.ToString();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    const int expectedNum = 1;
    const int timeoutMs = 5000;
    std::vector<Element> outElements;
    rc = consumer->Receive(expectedNum, timeoutMs, outElements);
    LOG(INFO) << "Expected to get K_OK. Rc returned: " << rc.ToString();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
    EXPECT_EQ(data, actualData);

    spClient.reset();
}

TEST_F(ProducerTest, TestReleaseBigElementMemoryInErrorCase)
{
    const size_t testSize = 2ul * 1024ul * 1024ul;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    DS_ASSERT_OK(spClient->CreateProducer("ReleaseBigEle", producer, defaultProducerConf_));
    Element element;
    std::vector<uint8_t> writeElement;
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    datasystem::inject::Set("ProducerImpl.ReleaseBigElementMemory", "1*return(K_RUNTIME_ERROR)");
    Status rc = producer->Send(element);
    LOG(INFO) << "Expected to get error. Rc returned: " << rc.ToString();
    ASSERT_NE(rc.GetCode(), StatusCode::K_OK);
}

TEST_F(ProducerTest, LEVEL1_TestBigElementBatchInsertRollback)
{
    // This testcase tests that the Big Element allocated at BatchInsert can be rollback correctly.
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    // Inject to the consumer side worker
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "InsertBigElement.Rollback", "100*return(K_TRY_AGAIN)"));
    const size_t testSize = 2ul * 1024ul * 1024ul;
    std::string streamName = "BigEleBatchInsertRollback";
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));
    const size_t SEND_COUNT = 1000;
    auto func = [&producer]() {
        const int DEFAULT_SLEEP_TIME = 300;
        Element element;
        std::vector<uint8_t> writeElement;
        CreateElement(testSize, element, writeElement);
        for (size_t i = 0; i < SEND_COUNT; i++) {
            Status rc = producer->Send(element);
            int retryCount = 30;
            while (rc.GetCode() == K_OUT_OF_MEMORY && retryCount-- > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                rc = producer->Send(element);
            }
            DS_ASSERT_OK(rc);
        }
    };
    std::thread producerThrd(func);
    const int DEFAULT_WAIT_TIME = 10'000;
    std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_WAIT_TIME));

    const int DEFAULT_RETRY_TIME = 20;
    Timer timer;
    std::vector<Element> outElements;
    int sendCount = SEND_COUNT;
    while (sendCount > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
        DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
        if (!outElements.empty()) {
            DS_ASSERT_OK(consumer->Ack(outElements.back().id));
            sendCount -= outElements.size();
        }
    }
    ASSERT_EQ(sendCount, 0);
    producerThrd.join();
}

TEST_F(ProducerTest, LEVEL1_TestEarlyProducerCloseWhileSendingData)
{
    size_t testSize = 2ul * 1024ul * 1024ul;
    Element element;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    DS_ASSERT_OK(client1->CreateProducer("EarlyProducerClose", producer, defaultProducerConf_));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "RemoteWorker.BatchAsyncFlushEntry.delay", "11*sleep(20000)"));

    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    DS_ASSERT_OK(client2->Subscribe("EarlyProducerClose", config, consumer));

    // Send data to producer
    std::vector<uint8_t> writeElement;
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    Status rc = producer->Send(element);
    DS_ASSERT_OK(producer->Close());

    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer("EarlyProducerClose", producer1, defaultProducerConf_));
    for (int i = 0; i < 100; ++i) {
        // Send data to producer
        std::vector<uint8_t> writeElement;
        DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
        Status rc = producer1->Send(element);
    }
    DS_ASSERT_OK(producer1->Close());

    sleep(SLEEP_TIME);
    // Get data and test for contents
    LOG(INFO) << "Expected to get K_OK. Rc returned: " << rc.ToString();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    const int expectedNum = 1;
    const int timeoutMs = 5000;
    std::vector<Element> outElements;
    rc = consumer->Receive(expectedNum, timeoutMs, outElements);
    LOG(INFO) << "Expected to get K_OK. Rc returned: " << rc.ToString();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
    EXPECT_EQ(data, actualData);

    client1.reset();
    client2.reset();
}

TEST_F(ProducerTest, DISABLED_TestConsumerCloseWhileDoingScanEval)
{
    // Testcase is disabled because ClearAllRemoteConsumer operations
    // no longer waits for FlushAllChanges wait post.
    // Create one producer and one consumer
    std::shared_ptr<StreamClient> spClient1;
    ASSERT_EQ(CreateClient(0, spClient1), Status::OK());
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(spClient1->CreateProducer("test1", producer, defaultProducerConf_));

    std::shared_ptr<StreamClient> spClient2;
    ASSERT_EQ(CreateClient(1, spClient2), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient2->Subscribe("test1", config, consumer));

    // Create a delay in waking up the WaitPost without sleep
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "StreamDataPool.ScanChangesAndEval.delaywakeup", "100000*call(0)"));
    // Producer produces data
    Element element;
    std::vector<uint8_t> writeElement;
    DS_ASSERT_OK(CreateElement(1024, element, writeElement));
    Status rc = producer->Send(element);
    LOG(INFO) << "Expected to get K_OK. Rc returned: " << rc.ToString();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    DS_ASSERT_NOT_OK(producer->Close());  // This will timeout
    // Consumer closes will fail because we are still waiting on wait post
    DS_ASSERT_OK(consumer->Close());
}

TEST_F(ProducerTest, TestWriteRemoteBigElementSuccess)
{
    LOG(INFO) << "test WriteRemoteBigElement start!";
    size_t testSize = 2ul * 1024ul * 1024ul;
    Element element;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    DS_ASSERT_OK(client1->CreateProducer("RemoteBigEleSuccess", producer, defaultProducerConf_));

    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    DS_ASSERT_OK(client2->Subscribe("RemoteBigEleSuccess", config, consumer));

    std::vector<uint8_t> writeElement;
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    Status rc = producer->Send(element);
    LOG(INFO) << "Expected to get K_OK. Rc returned: " << rc.ToString();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    const int expectedNum = 1;
    const int timeoutMs = 5000;
    std::vector<Element> outElements;
    rc = consumer->Receive(expectedNum, timeoutMs, outElements);
    LOG(INFO) << "Expected to get K_OK. Rc returned: " << rc.ToString();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
    EXPECT_EQ(data, actualData);

    client1.reset();
    client2.reset();
}

TEST_F(ProducerTest, TestWriteBigElementWhenMmapFailed)
{
    Element element;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> client;
    ASSERT_EQ(CreateClient(0, client), Status::OK());
    DS_ASSERT_OK(client->CreateProducer("MmapFailed", producer, defaultProducerConf_));

    DS_ASSERT_OK(inject::Set("IMmapTableEntry.mmap", "1*return(K_RUNTIME_ERROR)"));
    std::vector<uint8_t> writeElement;
    size_t testSize = 2ul * 1024ul * 1024ul;
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
    Status rc = producer->Send(element);
    LOG(INFO) << "Expected to get K_OK. Rc returned: " << rc.ToString();
    DS_ASSERT_NOT_OK(rc);
    client.reset();
}

TEST_F(ProducerTest, TestSendBigEleDuringLostHeartbeat)
{
    LOG(INFO) << "test WriteRemoteBigElement start!";
    size_t testSize = 2ul * 1024ul * 1024ul;
    Element element;
    Element element2;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    DS_ASSERT_OK(client1->CreateProducer("RemoteBigEleSuccess", producer, defaultProducerConf_));

    // Subscribe before send.
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    DS_ASSERT_OK(client2->Subscribe("RemoteBigEleSuccess", config, consumer));

    std::vector<uint8_t> writeElement;
    DS_ASSERT_OK(CreateElement(testSize, element, writeElement));

    DS_ASSERT_OK(datasystem::inject::Set("ProducerImpl.SendImpl.postInsertSuccess", "1*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(datasystem::inject::Set("ProducerConsumerWorkerApi.ReleaseBigElementMemory.preReleaseBigShmMemory",
                                         "1*sleep(3000)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "RemoteWorkerManager.SendElementsView.PostIncRefCount", "1*sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ShmUnit.FreeMemory", "call()"));
    DS_ASSERT_NOT_OK(producer->Send(element));

    const int expectedNum = 1;
    const int timeoutMs = 10000;
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(expectedNum, timeoutMs, outElements));
    ASSERT_EQ(outElements.size(), size_t(1));
    ASSERT_EQ(outElements[0].id, size_t(1));
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
    EXPECT_EQ(data, actualData);

    client1.reset();
    client2.reset();
}

TEST_F(ProducerTest, TestWriteMixedElements)
{
    LOG(INFO) << "test WriteMixedElements start!";
    size_t testSize = 60ul * 1024ul;
    size_t bigSize = 2ul * 1024ul * 1024ul;
    Element element;
    std::vector<uint8_t> writeElement;
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> spClient;
    ProducerConf prodConf = { .delayFlushTime = 0, .pageSize = 4 * MB, .maxStreamSize = TEST_STREAM_SIZE };
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    DS_ASSERT_OK(spClient->CreateProducer("MixedElements", producer, prodConf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient->Subscribe("MixedElements", config, consumer));
    std::string writeData;
    for (int i = 0; i < count; i++) {
        if (i % 10 != 0) {
            DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
            ASSERT_EQ(producer->Send(element), Status::OK());
            std::string data(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
            writeData += data;
        } else {
            DS_ASSERT_OK(CreateElement(bigSize, element, writeElement));
            ASSERT_EQ(producer->Send(element), Status::OK());
            std::string bigData(reinterpret_cast<char *>(writeElement.data()), writeElement.size());
            writeData += bigData;
        }
    }

    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(count, 0, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), static_cast<size_t>(count));
    std::string actualData;
    for (int i = 0; i < count; i++) {
        std::string tmp(reinterpret_cast<char *>(outElements[i].ptr), outElements[i].size);
        actualData += tmp;
    }
    EXPECT_EQ(writeData, actualData);
    spClient = nullptr;
}

TEST_F(ProducerTest, TestSendInvalidTimeout)
{
    std::shared_ptr<StreamClient> client;
    DS_ASSERT_OK(CreateClient(0, client));
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.pageSize = 40 * 1024;
    conf.maxStreamSize = 50 * 1024 * 1024;
    DS_ASSERT_OK(client->CreateProducer("SendInvalidTimeout", producer, conf));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client->Subscribe("SendInvalidTimeout", config, consumer));

    std::string data = "hello";
    Element element((uint8_t *)data.data(), data.size());
    ASSERT_EQ(producer->Send(element, 0).GetCode(), K_OK);
    ASSERT_EQ(producer->Send(element, -1).GetCode(), K_INVALID);

    const int timeout = 500;
    DS_ASSERT_OK(producer->Send(element, timeout));

    std::vector<Element> outElements;
    const int maxRecvNum = 2;
    DS_ASSERT_OK(consumer->Receive(maxRecvNum, timeout, outElements));
    ASSERT_EQ(outElements.size(), maxRecvNum);
    ASSERT_EQ(outElements[0].size, data.size());
    DS_ASSERT_OK(consumer->Ack(outElements[0].id));
}

TEST_F(ProducerTest, TestSendSmallElementBigElement)
{
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    std::shared_ptr<StreamClient> spClient1;
    DS_ASSERT_OK(CreateClient(1, spClient1));

    // Create a Producer
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.pageSize = 40 * 1024;
    conf.maxStreamSize = 50 * 1024 * 1024;
    DS_ASSERT_OK(spClient0->CreateProducer("SendSmallElementBigElement", producer, conf));

    // Create a Consumer on a different node
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient1->Subscribe("SendSmallElementBigElement", config, consumer));

    // Add delay to ScanEval thread so that it picks up both elements at the same time
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ExclusivePageQueue.ScanAndEval.wait", "sleep(500)"));

    // Send two elements: one small element and one big element
    const uint32_t eleSz1 = 37910;
    ElementGenerator elementGenerator1(eleSz1);
    auto strs1 = elementGenerator1.GenElements("producer1", 1, 1);
    DS_ASSERT_OK(producer->Send(Element((uint8_t *)strs1[0].data(), strs1[0].size()), 1000));

    const uint32_t eleSz2 = 185015;
    ElementGenerator elementGenerator2(eleSz2);
    auto strs2 = elementGenerator2.GenElements("producer2", 1, 1);
    DS_ASSERT_OK(producer->Send(Element((uint8_t *)strs2[0].data(), strs2[0].size()), 1000));

    sleep(1);

    // Receiver should get both correctly
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(2, 5000, outElements));
    ASSERT_EQ(outElements.size(), 2);
    ASSERT_EQ(outElements[0].size, strs1[0].size());
    ASSERT_EQ(outElements[1].size, strs2[0].size());
    DS_ASSERT_OK(consumer->Ack(2));

    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
}

TEST_F(ProducerTest, TestBigElementOOM)
{
    std::shared_ptr<StreamClient> spClient0;
    DS_ASSERT_OK(CreateClient(0, spClient0));

    // Create a Producer
    const int64_t pageSize = 16 * KB;
    const int numPages = 4;
    const int64_t streamSize = numPages * pageSize;
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.pageSize = pageSize;
    conf.maxStreamSize = streamSize;
    conf.retainForNumConsumers = 1;
    DS_ASSERT_OK(spClient0->CreateProducer("BigEleOOM", producer, conf));

    // Send three elements same as the page size. The new logic will convert them into BigElement
    RandomData rand;
    auto str = rand.GetRandomString(pageSize);
    for (int i = 0; i < numPages - 1; ++i) {
        DS_ASSERT_OK(producer->Send(Element((uint8_t *)str.data(), str.size())));
    }

    // Insert one more time should get OOM (because we have already created one data page and three big element pages)
    const int64_t timeoutMs = 5000;
    Status rc = producer->Send(Element((uint8_t *)str.data(), str.size()), timeoutMs);
    DS_ASSERT_TRUE(rc.GetCode(), K_OUT_OF_MEMORY);

    // Create a Consumer on the same node
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(spClient0->Subscribe("BigEleOOM", config, consumer));

    // Receive one, ack, and send again. Should be successful
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(RPC_TIMEOUT, outElements));
    DS_ASSERT_TRUE(outElements.size(), numPages - 1);
    consumer->Ack(outElements[0].id);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    DS_ASSERT_OK(producer->Send(Element((uint8_t *)str.data(), str.size())));

    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(spClient0->DeleteStream("BigEleOOM"));
}

TEST_F(ProducerTest, TestCreateProducerWithPage)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<Producer> producer2;
    std::shared_ptr<Producer> producer3;
    std::shared_ptr<Producer> producer4;
    std::shared_ptr<StreamClient> spClient;
    uint32_t baseSize = 4 * 1024;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    DS_ASSERT_OK(spClient->CreateProducer(
        "test_producer", producer, { .delayFlushTime = 0, .pageSize = baseSize, .maxStreamSize = TEST_STREAM_SIZE }));

    Status rc =
        spClient->CreateProducer("test_producer2", producer2,
                                 { .delayFlushTime = 0, .pageSize = 4294967295, .maxStreamSize = TEST_STREAM_SIZE });
    LOG_IF_ERROR(rc, "Expected failure for invalid page size");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);

    const uint64_t maxPageSize = 16 * MB;
    const uint64_t nextInvalidPageSize = maxPageSize + 4 * KB;
    DS_ASSERT_OK(
        spClient->CreateProducer("test_producer3", producer3,
                                 { .delayFlushTime = 0, .pageSize = maxPageSize, .maxStreamSize = TEST_STREAM_SIZE }));

    rc = spClient->CreateProducer(
        "test_producer4", producer4,
        { .delayFlushTime = 0, .pageSize = nextInvalidPageSize, .maxStreamSize = TEST_STREAM_SIZE });
    LOG_IF_ERROR(rc, "Expected failure for invalid page size");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);

    spClient = nullptr;
}

TEST_F(ProducerTest, TestCreateProducerWithoutPage)
{
    std::shared_ptr<Producer> producer;
    std::shared_ptr<StreamClient> spClient;
    ProducerConf conf;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    conf.delayFlushTime = 0;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    DS_ASSERT_OK(spClient->CreateProducer("test_default_producer", producer, conf));
    spClient = nullptr;
}

TEST_F(ProducerTest, TestCreateProducerReserveSize)
{
    // This testcase intends to test that invalid reserve size will lead to CreateProducer failure.
    ProducerConf conf;
    const int64_t DEFAULT_PAGE_SIZE = 8 * KB;
    const int64_t NOT_MULTIPLE_RESERVE_SIZE = 12 * KB;
    conf.pageSize = DEFAULT_PAGE_SIZE;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    std::shared_ptr<StreamClient> client;
    ASSERT_EQ(CreateClient(0, client), Status::OK());
    std::string streamName = "test_reserve_size";
    std::shared_ptr<Producer> producer;

    // Valid reserve size should be less than or equal to max stream size.
    ProducerConf exceedSizeConf(conf);
    exceedSizeConf.reserveSize = TEST_STREAM_SIZE + DEFAULT_PAGE_SIZE;
    DS_ASSERT_NOT_OK(client->CreateProducer(streamName, producer, exceedSizeConf));

    // Valid reserve size should be a multiple of page size.
    ProducerConf notMultipleConf(conf);
    notMultipleConf.reserveSize = NOT_MULTIPLE_RESERVE_SIZE;
    DS_ASSERT_NOT_OK(client->CreateProducer(streamName, producer, notMultipleConf));

    // 0 is an acceptable input for reserve size, the default reserve size will then be the page size.
    ProducerConf zeroConf(conf);
    notMultipleConf.reserveSize = 0;
    DS_ASSERT_OK(client->CreateProducer(streamName, producer, zeroConf));
}

TEST_F(ProducerTest, TestCreateMultiProducerWithPage)
{
    uint32_t baseSize = 4096;
    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    for (int i = 1; i < 11; i++) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(
            spClient->CreateProducer("test_multi_producer", producer,
                                     { .delayFlushTime = 0, .pageSize = baseSize, .maxStreamSize = TEST_STREAM_SIZE }));
    }
    spClient = nullptr;
}

TEST_F(ProducerTest, TestFlushPGIsFull)
{
    Timer timer;
    ThreadPool pool(2);

    std::shared_ptr<StreamClient> spClient;
    ASSERT_EQ(CreateClient(0, spClient), Status::OK());
    // Producer sends in total 64 MB data to a stream of 4 MB capacity and 64 KB-sized pages.
    size_t numOfElements = 100;
    auto producerFut = pool.Submit([this, numOfElements, spClient]() {
        size_t numOfRetries = 0;
        std::shared_ptr<Producer> producer;
        ProducerConf producerConf{ .delayFlushTime = -1, .pageSize = 64 * KB, .maxStreamSize = 4 * MB };
        ElementGenerator elementGenerator(3 * KB, 3 * KB);
        uint64_t numOfConsumers = 0;
        while (numOfConsumers != 1) {
            spClient->QueryGlobalConsumersNum("FlushPGIsFull", numOfConsumers);
        }
        RETURN_IF_NOT_OK(spClient->CreateProducer("FlushPGIsFull", producer, producerConf));
        auto elements = elementGenerator.GenElements("producer", numOfElements, 8ul);
        Timer timer;
        for (size_t i = 0; i < numOfElements; i++) {
            auto element = Element(reinterpret_cast<uint8_t *>(&elements[i].front()), elements[i].size());
            auto status = producer->Send(element);
        }
        LOG(INFO) << FormatString("Producer's number of re-sending: %zu, sending time: %.6lf s", numOfRetries,
                                  timer.ElapsedSecond());
        return Status::OK();
    });

    // Receiver receive all the data.
    auto consumerFut = pool.Submit([this, numOfElements, spClient]() {
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(spClient->Subscribe("FlushPGIsFull", config, consumer));
        std::unordered_map<std::string, uint64_t> seqNoMap;
        Timer timer;
        size_t ackInterval = 4096;
        for (size_t i = 0; i < numOfElements;) {
            std::vector<Element> outElements;
            consumer->Receive(1, 20, outElements);
            i += outElements.size();
            for (auto &element : outElements) {
                ElementView elementView(std::string(reinterpret_cast<char *>(element.ptr), element.size));
                RETURN_IF_NOT_OK(elementView.VerifyFifo(seqNoMap));
                RETURN_IF_NOT_OK(elementView.VerifyIntegrity());
            }
            if (i % ackInterval == (ackInterval - 1)) {
                LOG(INFO) << FormatString("Ack id: %zu", outElements.back().id);
                RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
            }
        }
        LOG(INFO) << FormatString("Total Recv Time Elapsed: %.6lf s", timer.ElapsedSecond());
        return Status::OK();
    });

    ASSERT_EQ(consumerFut.get(), Status::OK());
    ASSERT_EQ(producerFut.get(), Status::OK());
    LOG(INFO) << FormatString("End To End Time Elapsed: %.6lf s", timer.ElapsedSecond());
}

// Test a "local" OOM where the consumer is on the same node as the producer, but does not consume right away causing
// build up of data and OOM conditions. Client retry will eventually run leading to overall sending success.
// Send timeout of 0.
TEST_F(ProducerTest, LEVEL2_TestOOM1)
{
    Status rc;
    std::shared_ptr<StreamClient> spClient;
    std::string streamName = "TestOOM1";

    rc = CreateClient(0, spClient);
    LOG_IF_ERROR(rc, "Creating client failed.");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = RunOOMTest(spClient, spClient, streamName);
    LOG_IF_ERROR(rc, "Running OOM test gave error.");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    spClient = nullptr;
}

// Test a "local" OOM where the consumer is on the same node as the producer, but does not consume right away causing
// build up of data and OOM conditions. Use a send-side timeout such that OOM is only returned after timing out,
// and disable client retry so that the OOM timeout/failure will terminate the test run.
TEST_F(ProducerTest, TestOOM2)
{
    Status rc;
    clientRetry_ = false;
    sendTimeout_ = 3000;  // 3 second timeout
    std::shared_ptr<StreamClient> spClient;
    std::string streamName = "TestOOM2";

    rc = CreateClient(0, spClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = RunOOMTest(spClient, spClient, streamName);
    LOG_IF_ERROR(rc, "Running OOM test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OUT_OF_MEMORY);  // expected fail

    spClient = nullptr;
}

// Test a "local" OOM where the consumer is on the same node as the producer, but does not consume right away causing
// build up of data and OOM conditions. Use a send-side timeout that is large enough such that a blocked send will
// eventually complete successfully (before the timeout) once the consumer drains some data to free up memory.
TEST_F(ProducerTest, TestOOM3)
{
    Status rc;
    clientRetry_ = false;
    sendTimeout_ = 10000;  // 10 second timeout to give it lots of wait time on send blocking
    elementsTotalSize_ = 32 * 1024 * 1024;
    std::shared_ptr<StreamClient> spClient;
    std::string streamName = "TestOOM3";

    rc = CreateClient(0, spClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = RunOOMTest(spClient, spClient, streamName);
    LOG_IF_ERROR(rc, "Running OOM test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success

    spClient = nullptr;
}

// Tests LEVEL1_TestOOM4 and TestOOM5:
// Test a remote OOM which then leads to local OOM.
// Producer is on worker0 and consumer is on worker1.
// Flushes naturally free the pages locally on worker0 since there is no local consumer.
// However, if the remote node is returning OOMs when the pages are attempted to send from worker0 to worker1, then the
// local free of the page is delayed until the remote sends can reduce their ref count. This leads to local OOM and
// send waiting.

// no timeout on the send, use client-side retry when OOM's are returned
TEST_F(ProducerTest, LEVEL2_TestOOM4)
{
    Status rc;
    std::shared_ptr<StreamClient> prodClient;
    std::shared_ptr<StreamClient> conClient;
    std::string streamName = "TestOOM4";

    rc = CreateClient(0, prodClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = CreateClient(1, conClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = RunOOMTest(prodClient, conClient, streamName);
    LOG_IF_ERROR(rc, "Running OOM test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success

    prodClient = nullptr;
    conClient = nullptr;
}

// disable client retry and use a timeout on the send calls that is large enough to support the work
TEST_F(ProducerTest, TestOOM5)
{
    Status rc;
    clientRetry_ = false;
    sendTimeout_ = 10000;  // 10 second timeout to give it lots of wait time on send blocking
    std::shared_ptr<StreamClient> prodClient;
    std::shared_ptr<StreamClient> conClient;
    std::string streamName = "TestOOM5";

    rc = CreateClient(0, prodClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = CreateClient(1, conClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = RunOOMTest(prodClient, conClient, streamName);
    LOG_IF_ERROR(rc, "Running OOM test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success

    prodClient = nullptr;
    conClient = nullptr;
}

// ensure multiple producers will block invalid configs
TEST_F(ProducerTest, TestMultiProdCfg)
{
    Status rc;
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;

    rc = CreateClient(0, client1);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = CreateClient(1, client2);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    std::shared_ptr<Producer> producer1;
    std::shared_ptr<Producer> producer2;
    ProducerConf producerConf1{ .delayFlushTime = 20, .pageSize = 64 * KB, .maxStreamSize = 2 * MB };
    ProducerConf producerConf2{ .delayFlushTime = 20, .pageSize = 32 * KB, .maxStreamSize = 2 * MB };
    DS_ASSERT_OK(client1->CreateProducer("MultiProdCfg", producer1, producerConf1));

    // Same worker, different page size
    rc = client1->CreateProducer("MultiProdCfg", producer2, producerConf2);
    LOG_IF_ERROR(rc, "Expected failure for invalid page size of existing stream");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);

    // remote worker, different page size
    rc = client2->CreateProducer("MultiProdCfg", producer2, producerConf2);
    LOG_IF_ERROR(rc, "Expected failure for invalid page size of existing stream");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);

    // Same worker, turn on auto delete
    ProducerConf producerConf3 = producerConf1;
    producerConf3.autoCleanup = true;
    rc = client1->CreateProducer("MultiProdCfg", producer2, producerConf3);
    LOG_IF_ERROR(rc, "Expected failure for invalid auto cleanup of existing stream");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);

    client1 = nullptr;
    client2 = nullptr;
}

TEST_F(ProducerTest, TestMultiProdClose)
{
    const int numClients = 2;
    const int numStreams = 2;
    std::shared_ptr<StreamClient> producerClients[numClients];
    std::shared_ptr<StreamClient> consumerClients[numClients];
    std::vector<std::shared_ptr<Producer>> producers;
    Status rc;
    ProducerConf producerConf{ .delayFlushTime = 20, .pageSize = 64 * KB, .maxStreamSize = 4 * MB };

    // 2 clients for producers
    ASSERT_EQ(CreateClient(0, producerClients[0]), Status::OK());
    ASSERT_EQ(CreateClient(1, producerClients[1]), Status::OK());

    // Create 8 producers, 4 on the stream named "test0 and 4 on "test1"
    // 2 producers form each client. Layout:
    //
    //  stream  client   producer
    // -------  ------   --------
    // "test0"      p0          0
    // "test0"      p1          1
    // "test0"      p0          2
    // "test0"      p1          3
    // "test1"      p0          4
    // "test1"      p1          5
    // "test1"      p0          6
    // "test1"      p1          7

    std::vector<std::string> streamNames;
    for (int i = 0; i < numStreams; ++i) {
        streamNames.push_back("test" + std::to_string(i));
    }

    const int numProds = 4;
    for (int j = 0; j < numStreams; ++j) {
        for (int i = 0; i < numProds; ++i) {
            std::shared_ptr<Producer> newProducer;
            rc = producerClients[i % numClients]->CreateProducer(streamNames[j], newProducer, producerConf);
            LOG_IF_ERROR(rc, "Creating producer failed. ");
            ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
            producers.push_back(std::move(newProducer));
        }
    }

    const int numCons = 4;
    std::vector<SubscriptionConfig> subCfgs;
    for (int i = 0; i < numCons; ++i) {
        SubscriptionConfig consumerConf("sub" + std::to_string(i), SubscriptionType::STREAM);
        subCfgs.push_back(consumerConf);
    }

    // With different clients, create subscribers/consumers.
    //
    //  stream  client   consumer
    // -------  ------   --------
    // "test0"      c0          0
    // "test1"      c0          1
    // "test0"      c1          2
    // "test1"      c1          3

    std::vector<std::shared_ptr<Consumer>> consumers;
    ASSERT_EQ(CreateClient(0, consumerClients[0]), Status::OK());
    ASSERT_EQ(CreateClient(1, consumerClients[1]), Status::OK());

    std::shared_ptr<Consumer> newConsumer;
    int cIdx = 0;
    rc = consumerClients[0]->Subscribe(streamNames[0], subCfgs[cIdx++], newConsumer);
    LOG_IF_ERROR(rc, "(subscribe): Creating consumer failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    consumers.push_back(std::move(newConsumer));

    rc = consumerClients[0]->Subscribe(streamNames[1], subCfgs[cIdx++], newConsumer);
    LOG_IF_ERROR(rc, "(subscribe): Creating consumer failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    consumers.push_back(std::move(newConsumer));

    rc = consumerClients[1]->Subscribe(streamNames[0], subCfgs[cIdx++], newConsumer);
    LOG_IF_ERROR(rc, "(subscribe): Creating consumer failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    consumers.push_back(std::move(newConsumer));

    rc = consumerClients[1]->Subscribe(streamNames[1], subCfgs[cIdx], newConsumer);
    LOG_IF_ERROR(rc, "(subscribe): Creating consumer failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    consumers.push_back(std::move(newConsumer));

    // Do a simple receive from each consumer.  nothing sent so we expect no results here.
    LOG(INFO) << "Loop over the 4 consumers and do a receive.  Expect each to give 0 records.";
    for (auto consumer : consumers) {
        std::vector<Element> outElements;
        consumer->Receive(1, RECV_WAIT_MILLI_SECONDS, outElements);
        LOG(INFO) << "Used a consumer to call receive. Got " << outElements.size() << " elements returned";
    }

    // Instead of individually closing each consumer and producer, do a client reset to drive a forced
    // shutdown of the producers in clients 1 and 2.
    LOG(INFO) << "Client reset of client 0 start";
    producerClients[0].reset();
    LOG(INFO) << "Client reset of client 0 done.";

    LOG(INFO) << "Client reset of client 1 start";
    producerClients[1].reset();
    LOG(INFO) << "Client reset of client 1 done";

    // Repeat the receive attempts. This time, since the client disconnect drove a close with force mode
    // true, these consumers will not get any data as the producers are already closed.
    // The Receive() calls might get triggered after subscriber receiving producer close notification. Hence
    // no one will wake them up from the pending state other than the timer.
    LOG(INFO) << "Loop over the 4 consumers and do a receive.  Expect them all to fail.";
    for (auto consumer : consumers) {
        Status rc;
        std::vector<Element> outElements;
        rc = consumer->Receive(1, RECV_WAIT_MILLI_SECONDS, outElements);
        LOG_IF_ERROR(rc, "Calling receive failed ");
        ASSERT_EQ(outElements.size(), (size_t)0);
    }
}

TEST_F(ProducerTest, TestMultiProdClose2)
{
    std::shared_ptr<StreamClient> pClient;
    std::shared_ptr<StreamClient> cClient;
    std::vector<std::shared_ptr<Producer>> producers;
    std::shared_ptr<Consumer> consumer;
    Status rc;
    ProducerConf producerConf{ .delayFlushTime = 20, .pageSize = 64 * KB, .maxStreamSize = 4 * MB };

    ASSERT_EQ(CreateClient(0, pClient), Status::OK());
    std::string streamName("MultiProdClose2");

    // Create 4 producers
    const int numProds = 4;
    LOG(INFO) << "Creating 4 producers";
    for (int i = 0; i < numProds; ++i) {
        std::shared_ptr<Producer> newProducer;
        rc = pClient->CreateProducer(streamName, newProducer, producerConf);
        LOG_IF_ERROR(rc, "Creating producer failed. ");
        ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
        producers.push_back(std::move(newProducer));
    }

    // Create a consumer on the remote worker
    LOG(INFO) << "Creating consumer";
    ASSERT_EQ(CreateClient(1, cClient), Status::OK());
    SubscriptionConfig consumerConf("sub", SubscriptionType::STREAM);
    rc = cClient->Subscribe(streamName, consumerConf, consumer);
    LOG_IF_ERROR(rc, "Creating producer failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    // Inject a failure on the master worker dealing with close to test the retry handling of the
    // client disconnect force close
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.PubDecreaseNode.afterSendNotification",
                                           "1*return(K_RPC_UNAVAILABLE)"));

    LOG(INFO) << "Client reset start";
    pClient.reset();
    LOG(INFO) << "Client reset 0 done.";
}

// The generic MultiTest_NStream test driver function can provide different configurations.
// This run will test 8 producer, single consumer, over a 2 worker setup.
TEST_F(BigShmTest, MultiTest_NStream1)
{
    Status rc;
    sendTimeout_ = 10000;  // 10 second timeout to give it lots of wait time on send blocking
    std::shared_ptr<StreamClient> newClient;
    std::vector<std::shared_ptr<StreamClient>> clients;
    earlyExitCount_ = 327680;
    slowReceive_ = true;
    validate_ = false;

    rc = CreateClient(0, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    rc = CreateClient(1, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    int64_t pageSize = 1048576;  // 1 mb
    uint64_t maxStreamSize = 16 * MB;
    int numStreams = 1;
    int numProds = 8;
    int numSubs = 1;
    rc = MultiTest_NStream(clients, pageSize, maxStreamSize, numStreams, numProds, numSubs);
    LOG_IF_ERROR(rc, "Running multi test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success

    clients.clear();  // frees the client ptr's

    // The sleep at the end just ensures the final testcase kill happens after things are cleaned up and
    // makes it easier to read the logs if needed.
    LOG(INFO) << "sleep at the end of testcase";
    const int SLEEP = 5;
    std::this_thread::sleep_for(std::chrono::seconds(SLEEP));
    LOG(INFO) << "sleep at the end of testcase done";
}

// The generic MultiTest_NStream test driver function can provide different configurations.
// This run will test 8 producer, single consumer, over a 2 worker setup. This one uses autoAck
// feature, so manual acks are not sent and instead each receive will drive acks.
TEST_F(BigShmTest, LEVEL1_MultiTest_NStream2)
{
    Status rc;
    sendTimeout_ = 10000;  // 10 second timeout to give it lots of wait time on send blocking
    std::shared_ptr<StreamClient> newClient;
    std::vector<std::shared_ptr<StreamClient>> clients;
    autoAck_ = true;  // configure for auto ack mode
    validate_ = false;

    rc = CreateClient(0, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    rc = CreateClient(1, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    int64_t pageSize = 1048576;  // 1 mb
    uint64_t maxStreamSize = 16 * MB;
    int numStreams = 1;
    int numProds = 8;
    int numSubs = 1;
    rc = MultiTest_NStream(clients, pageSize, maxStreamSize, numStreams, numProds, numSubs);
    LOG_IF_ERROR(rc, "Running multi test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success
    clients.clear();                            // frees the client ptr's
}

// The generic MultiTest_NStream test driver function can provide different configurations.
// This run will test 4 producer, single consumer, over a 2 worker setup. This one does not
// use auto ack feature, and it is configured to ack frequently to test the smart ack
// logic
TEST_F(BigShmTest, MultiTest_NStream3)
{
    // README
    // The pageSz has been increased from 4 * KB to reduce run time during CI
    // To run the intended load locally, edit the value.
    Status rc;
    sendTimeout_ = 10000;  // 10 second timeout to give it lots of wait time on send blocking
    std::shared_ptr<StreamClient> newClient;
    std::vector<std::shared_ptr<StreamClient>> clients;
    ackInterval_ = 1;  // ack after every element!
    validate_ = false;
    eleSz_ = 8 * KB;

    rc = CreateClient(0, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    rc = CreateClient(1, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    int64_t pageSize = 1 * MB;
    uint64_t maxStreamSize = 32 * MB;
    int numStreams = 1;
    int numProds = 4;
    int numSubs = 1;
    rc = MultiTest_NStream(clients, pageSize, maxStreamSize, numStreams, numProds, numSubs);
    LOG_IF_ERROR(rc, "Running multi test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success
    clients.clear();                            // frees the client ptr's
}

// README
// MultiTest_NStream4 and 5 is consolidated into one testcase. Only difference is slowReceive_.
// The elementsTotalSize_ is decreased from 256 * MB to reduce run time during CI
// To run the intended load locally, edit the value.
// Toggle the flag inside the testacse to switch between the NStream4 and NStream5
// 1 producer, 1 consumer (remote worker) scenario.
// NStream4 - Consumer is slow in between each receive to ensure worker has data. Dumps a perf log at the end
// for perf analysis of logs
// NStream5 - Consumer is not slow in between each receive. This is not the winning scenario for prefetching,
// but we canuse this to test prefetching in a case where it does not aid performance to assess overhead.
TEST_F(BigShmTest, LEVEL1_MultiTest_NStream4)
{
    Status rc;
    sendTimeout_ = 10000;  // 10 second timeout to give it lots of wait time on send blocking
    std::shared_ptr<StreamClient> newClient;
    std::vector<std::shared_ptr<StreamClient>> clients;
    slowConsume_ = true;
    // Set True for NStream4 setting or False for NStream5 setting
    slowReceive_ = true;
    validate_ = false;
    clientCacheSize_ = 4096;  // override default for the cache size
    prefetchLWM_ = 50;        // cache threshold for prefetching
    elementsTotalSize_ = 64 * MB;
    eleSz_ = 128;  // small elements.  dft was 1K

    rc = CreateClient(0, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    rc = CreateClient(1, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    int64_t pageSize = 1048576;  // 1 mb
    uint64_t maxStreamSize = 32 * MB;
    int numStreams = 1;
    int numProds = 1;
    int numSubs = 1;
    rc = MultiTest_NStream(clients, pageSize, maxStreamSize, numStreams, numProds, numSubs);
    LOG_IF_ERROR(rc, "Running multi test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success
    clients.clear();                            // frees the client ptr's

    PerfManager *perfManager = PerfManager::Instance();
    perfManager->PrintPerfLog();
}

// 3 producers, 1 consumer, 1 worker.
TEST_F(BigShmTest, LEVEL2_MultiTest_NStream6)
{
    // README
    // The eleSz_ has been increased from 48 to reduce run time during CI
    // To run the intended load locally, edit the value.
    Status rc;
    sendTimeout_ = 10000;  // 10 second timeout to give it lots of wait time on send blocking
    std::shared_ptr<StreamClient> newClient;
    std::vector<std::shared_ptr<StreamClient>> clients;
    autoAck_ = true;  // configure for auto ack mode
    validate_ = false;
    eleSz_ = 128;

    rc = CreateClient(0, newClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    clients.push_back(std::move(newClient));

    int64_t pageSize = 1048576;  // 1 mb
    uint64_t maxStreamSize = 16 * MB;
    int numStreams = 1;
    int numProds = 3;
    int numSubs = 1;
    rc = MultiTest_NStream(clients, pageSize, maxStreamSize, numStreams, numProds, numSubs);
    LOG_IF_ERROR(rc, "Running multi test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success
    clients.clear();                            // frees the client ptr's

    PerfManager *perfManager = PerfManager::Instance();
    perfManager->PrintPerfLog();
}

class ProducerLocalMemTest : public ProducerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        // This will cause local cache to send OOMs
        opts.workerGflagParams = " -sc_local_cache_memory_size_mb=10 -v=1";
        opts.numWorkers = NUM_WORKERS;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

TEST_F(ProducerLocalMemTest, DISABLED_TestLocalCacheOOM1)
{
    Status rc;
    std::shared_ptr<StreamClient> prodClient;
    std::shared_ptr<StreamClient> conClient;
    std::string streamName = "TestLocalCacheOOM1";
    rc = CreateClient(0, prodClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);

    rc = CreateClient(0, conClient);
    LOG_IF_ERROR(rc, "Creating client failed. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);
    const int K_10 = 10;
    rc = RunOOMTest(prodClient, conClient, streamName, K_10);
    LOG_IF_ERROR(rc, "Running OOM test gave error. ");
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OK);  // expected success

    prodClient = nullptr;
    conClient = nullptr;
}

/*
Create 2 producers, 1 consumer. Testing Back pressure with modified RunOOMTest with a second producer.
One producer sends slower than other. Use future object to assert status is OK. Each producer sends
half of numElements so no TimeOut for test purposes.
*/
TEST_F(ProducerTest, LEVEL1_TestDifferentSendSpeed)
{
    std::shared_ptr<StreamClient> prodClient1, prodClient2, conClient;
    DS_ASSERT_OK(CreateClient(0, prodClient1));
    DS_ASSERT_OK(CreateClient(0, prodClient2));
    DS_ASSERT_OK(CreateClient(1, conClient));

    uint64_t maxStreamSizeMB = 2;
    ThreadPool preparePool(CLIENT_THREAD_POOL_SIZE);
    uint64_t eleSz = 8192ul;  // this should be small element, not big element
    uint64_t numElements = elementsTotalSize_ / eleSz / 2;
    uint64_t quarterNumElements = numElements / 4;
    uint64_t halfNumElements = numElements / 2;
    slowConsume_ = waitForGo_ = true;

    LOG(INFO) << FormatString("Testing Size: %zu", eleSz);
    std::shared_future<std::vector<std::string>> elementsFut1 = preparePool.Submit([eleSz, quarterNumElements]() {
        ElementGenerator elementGenerator(eleSz + 1, eleSz);
        auto elements1 = elementGenerator.GenElements("producer1", quarterNumElements, 8ul);
        return elements1;
    });
    std::shared_future<std::vector<std::string>> elementsFut2 = preparePool.Submit([eleSz, quarterNumElements]() {
        ElementGenerator elementGenerator(eleSz + 1, eleSz);
        auto elements2 = elementGenerator.GenElements("producer2", quarterNumElements, 8ul);
        return elements2;
    });

    // Wait for the data generation to complete before we launch producers and consumers
    while (elementsFut1.wait_for(std::chrono::seconds(1)) != std::future_status::ready)
        ;
    while (elementsFut2.wait_for(std::chrono::seconds(1)) != std::future_status::ready)
        ;
    LOG(INFO) << "Data generation complete. kick off threads now";

    ThreadPool pool(CLIENT_THREAD_POOL_SIZE);
    std::string streamName = "TestDifferentSendSpeed";
    ProducerConf prodCfg = { .delayFlushTime = 20, .pageSize = 1 * MB, .maxStreamSize = maxStreamSizeMB * MB };
    auto producerFut = pool.Submit(
        [this, streamName, prodCfg, &elementsFut1, &elementsFut2, quarterNumElements, prodClient1, prodClient2]() {
            SendConfig sendCfg1 = { .streamName = streamName,
                                    .producerName = "producer1",
                                    .producerConf = prodCfg,
                                    .numOfElements = quarterNumElements };
            SendConfig sendCfg2 = { .streamName = streamName,
                                    .producerName = "producer2",
                                    .producerConf = prodCfg,
                                    .numOfElements = quarterNumElements };
            return SendStreamDataSlow(sendCfg1, sendCfg2, 1, elementsFut1, elementsFut2, prodClient1, prodClient2);
        });

    size_t ackInterval = std::max<size_t>(400ul * KB / eleSz, 1ul);
    auto consumerFut = pool.Submit([this, streamName, halfNumElements, ackInterval, conClient]() {
        RecvConfig rcvCfg = { .streamName = streamName,
                              .subscriptionName = "subscription",
                              .numOfBatchElements = 100,
                              .timeToWaitMs = 20,
                              .ackInterval = ackInterval,
                              .autoAck = false };
        return RecvStreamData(rcvCfg, halfNumElements, conClient);
    });

    // assert status after threads complete
    ASSERT_EQ(producerFut.get(), Status::OK());
    ASSERT_EQ(consumerFut.get(), Status::OK());
}

/*
Create 2 consumers 1 producer. Testing Back pressure with modified RunOOMTest with a second consumer.
One Consumer receives slower than other. Use future object to assert status is OK.
*/
TEST_F(ProducerTest, LEVEL1_TestDifferentReceiveSpeed)
{
    std::shared_ptr<StreamClient> prodClient, conClient1, conClient2;
    DS_ASSERT_OK(CreateClient(0, prodClient));
    DS_ASSERT_OK(CreateClient(1, conClient1));
    DS_ASSERT_OK(CreateClient(1, conClient2));

    uint64_t maxStreamSizeMB = 2;
    ThreadPool preparePool(1);
    uint64_t eleSz = 8192ul;  // this should be small element, not big element
    uint64_t numElements = elementsTotalSize_ / eleSz / 2;
    slowConsume_ = waitForGo_ = true;

    LOG(INFO) << FormatString("Testing Size: %zu", eleSz);
    std::shared_future<std::vector<std::string>> elementsFut = preparePool.Submit([eleSz, numElements]() {
        ElementGenerator elementGenerator(eleSz + 1, eleSz);
        auto elements = elementGenerator.GenElements("producer", numElements, 8ul);
        return elements;
    });

    // Wait for the data generation to complete before we launch producers and consumers
    while (elementsFut.wait_for(std::chrono::seconds(1)) != std::future_status::ready)
        ;
    LOG(INFO) << "Data generation complete. kick off threads now";

    const int POOL_SIZE = 3;
    int numOfConsumer = 2;
    ThreadPool pool(POOL_SIZE);
    std::string streamName = "DifferentRecvSpeed";
    auto producerFut =
        pool.Submit([this, streamName, numOfConsumer, &elementsFut, numElements, prodClient, maxStreamSizeMB]() {
            ProducerConf prodCfg = { .delayFlushTime = 20, .pageSize = 1 * MB, .maxStreamSize = maxStreamSizeMB * MB };
            SendConfig sendCfg = { .streamName = streamName,
                                   .producerName = "producer",
                                   .producerConf = prodCfg,
                                   .numOfElements = numElements };
            return SendStreamData(sendCfg, numOfConsumer, elementsFut, prodClient);
        });

    size_t ackInterval = std::max<size_t>(400ul * KB / eleSz, 1ul);
    auto consumerFut1 = pool.Submit([this, streamName, numElements, ackInterval, conClient1]() {
        RecvConfig rcvCfg = { .streamName = streamName,
                              .subscriptionName = "sub1",
                              .numOfBatchElements = 100,
                              .timeToWaitMs = 20,
                              .ackInterval = ackInterval,
                              .autoAck = false };
        return RecvStreamData(rcvCfg, numElements, conClient1);
    });

    auto consumerFut2 = pool.Submit([this, streamName, numElements, ackInterval, conClient2]() {
        RecvConfig rcvCfg = { .streamName = streamName,
                              .subscriptionName = "sub2",
                              .numOfBatchElements = 100,
                              .timeToWaitMs = 20,
                              .ackInterval = ackInterval,
                              .autoAck = false };
        return RecvStreamDataWithSlowReceive(rcvCfg, numElements, conClient2);
    });

    // assert status after threads complete
    ASSERT_EQ(producerFut.get(), Status::OK());
    ASSERT_EQ(consumerFut1.get(), Status::OK());
    ASSERT_EQ(consumerFut2.get(), Status::OK());
}

TEST_F(ProducerTest, TestReCreateProducerDiscardData)
{
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    std::string streamName = "DiscardData";

    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    ProducerConf conf;
    conf.maxStreamSize = 10 * 1024 * 1024;
    const int NUM_ITER = 5;
    const int DEFAULT_WAIT_TIME = 5000;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.UsageMonitor.CheckOverUsedForStream.MockError",
                                           "return(K_OUT_OF_MEMORY)"));
    for (int iteration = 0; iteration < NUM_ITER; iteration++) {
        LOG(INFO) << "Iteration number " << iteration;
        DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));

        const size_t testSize = 500 * KB;
        Element element;
        std::vector<uint8_t> writeElement;
        DS_ASSERT_OK(CreateElement(testSize, element, writeElement));
        DS_ASSERT_OK(producer->Send(element));
        DS_ASSERT_OK(producer->Close());
    }
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "worker.UsageMonitor.CheckOverUsedForStream.MockError"));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(NUM_ITER, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), (size_t)NUM_ITER);
    DS_ASSERT_OK(consumer->Ack(outElements.back().id));
}

TEST_F(ProducerTest, TestConsumerFutexWake)
{
    FLAGS_v = SC_DEBUG_LOG_LEVEL;
    std::shared_ptr<StreamClient> client;
    ASSERT_EQ(CreateClient(0, client), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client->Subscribe("ConsumerFutexWake", config, consumer));
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const int pageSize = 4 * KB;
    conf.pageSize = pageSize;
    conf.maxStreamSize = DEFAULT_MAX_STREAM_SIZE;
    DS_ASSERT_OK(client->CreateProducer("ConsumerFutexWake", producer, conf));
    const int eleSize = 3 * KB;
    std::string a(eleSize, 'a');
    size_t numElementRecv = 0;
    size_t numElementSend = 0;
    ThreadPool pool(2);
    auto consFut = pool.Submit([&consumer, &numElementRecv]() {
        std::vector<Element> out;
        RETURN_IF_NOT_OK(consumer->Receive(RPC_TIMEOUT, out));
        numElementRecv += out.size();
        datasystem::inject::Set("StreamDataPage.WaitOnFutexForever", "1*call()");
        RETURN_IF_NOT_OK(consumer->Receive(RPC_TIMEOUT, out));
        numElementRecv += out.size();
        return Status::OK();
    });
    auto prodFut = pool.Submit([&a, &producer, &numElementSend]() {
        Element ele1(reinterpret_cast<uint8_t *>(a.data()), a.size());
        // Send one element
        RETURN_IF_NOT_OK(producer->Send(ele1));
        numElementSend++;
        std::this_thread::sleep_for(std::chrono::seconds(5));
        RETURN_IF_NOT_OK(producer->Send(ele1));
        numElementSend++;
        return Status::OK();
    });
    auto rc1 = consFut.get();
    auto rc2 = prodFut.get();
    DS_ASSERT_OK(rc1);
    DS_ASSERT_OK(rc2);
    ASSERT_EQ(numElementSend, numElementRecv);
    producer->Close();
    consumer->Close();
}

TEST_F(ProducerTest, TestConsumerTimingHole)
{
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    const size_t SEND_COUNT = 100;
    const size_t testSize = 500 * KB;
    ProducerConf conf;
    conf.maxStreamSize = 10 * 1024 * 1024;
    DS_ASSERT_OK(client1->CreateProducer("ConsumerTimingHole", producer, conf));
    std::thread producerThrd([&client1, &producer]() {
        const int DEFAULT_SLEEP_TIME = 300;
        Element element;
        std::vector<uint8_t> writeElement;
        uint64_t numOfConsumers = 0;
        while (numOfConsumers != 1) {
            client1->QueryGlobalConsumersNum("ConsumerTimingHole", numOfConsumers);
            std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
        }
        CreateElement(testSize, element, writeElement);
        for (size_t i = 0; i < SEND_COUNT; i++) {
            Status rc = producer->Send(element);
            int retryCount = 30;
            while (rc.GetCode() == K_OUT_OF_MEMORY && retryCount-- > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                rc = producer->Send(element);
            }
            DS_ASSERT_OK(rc);
        }
    });

    // Inject sleep to extend the consumer timing hole
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "ClientWorkerSC.Subscribe.TimingHole", "1*sleep(1000)"));

    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("ConsumerTimingHole", config, consumer));

    const int DEFAULT_WAIT_TIME = 1000;
    const int DEFAULT_RETRY_TIME = 10;
    Timer timer;
    std::vector<Element> outElements;
    int sendCount = SEND_COUNT;
    while (sendCount > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
        DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
        if (!outElements.empty()) {
            DS_ASSERT_OK(consumer->Ack(outElements.back().id));
            sendCount -= outElements.size();
        }
    }
    ASSERT_EQ(sendCount, 0);
    producerThrd.join();
}

TEST_F(ProducerTest, TestBlockedCreateRequestTimingHole)
{
    // 1 Producer -> 1 Consumer Same Node.
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 100 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    std::string streamName = "BlockedCreateRequestTimingHole";
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));

    // Only 1 element per page.
    const size_t elementSize = 900 * KB;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(elementSize, element, writeElement);

    // Normal Send.
    DS_ASSERT_OK(producer->Send(element));

    // Timer for the BlockedCreateRequest remain active, we do not cancel it.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "do.not.cancel.timer", "1*return()"));
    DS_ASSERT_OK(producer->Send(element, 10));  // Timer with less than 10ms
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "do.not.cancel.timer"));

    // The timer above will attempt to process the BlockedCreateRequest created below, the timer should do no ops.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetBlockedCreateRequest.sleep", "sleep(2000)"));
    DS_ASSERT_OK(producer->Send(element));  // No timer
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetBlockedCreateRequest.sleep"));

    // Clean up
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client1->DeleteStream(streamName));
}

TEST_F(ProducerTest, LEVEL2_TestCreateProducerLongTimeout1)
{
    // Request should not timeout if client timeout is set to 10mins and master takes more time

    // set timeout to 10 mins
    std::shared_ptr<StreamClient> client1;
    const int32_t timeoutMs = 1000 * 60 * 10;
    ASSERT_EQ(CreateClient(0, timeoutMs, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 100 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;

    // Make master wait for 1 min and it should not timeout
    // We actually dont know who is the master so inject in both
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "SCMetadataManager.CreateProducer.wait",
                                           "1*sleep(60000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "SCMetadataManager.CreateProducer.wait",
                                           "1*sleep(60000)"));

    // This request should not timeout as client timeout is 10 mins.
    DS_ASSERT_OK(client1->CreateProducer("ProducerLongTimeout1", producer, conf));
    DS_ASSERT_OK(producer->Close());
}

TEST_F(ProducerTest, LEVEL1_TestCreateProducerLongTimeout2)
{
    // Request should timeout if client timeout is set to 15s and master takes more

    // Set timeout to default
    std::shared_ptr<StreamClient> client1;
    const int timeoutMs = 15000;
    ASSERT_EQ(CreateClient(0, timeoutMs, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 100 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;

    // Make master wait for 1 min and whole CreateProducer() request should timeout
    // We actually dont know who is the master so inject in both
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "SCMetadataManager.CreateProducer.wait",
                                           "1*sleep(20000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "SCMetadataManager.CreateProducer.wait",
                                           "1*sleep(20000)"));

    // This request should fail as timeout is 15secs and master takes more than that
    DS_ASSERT_NOT_OK(client1->CreateProducer("ProducerLongTimeout2", producer, conf));
}

TEST_F(ProducerTest, LEVEL2_TestCreateProducerLongTimeout3)
{
    // MasterWorkerSCServiceImpl::SyncConsumerNode takes long time

    // set client timeout to 10 mins
    std::shared_ptr<StreamClient> client1;
    const int32_t timeoutMs = 1000 * 60 * 10;
    ASSERT_EQ(CreateClient(0, timeoutMs, client1), Status::OK());

    // Create a consumer so that we can get SyncConsumerNode
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::shared_ptr<Consumer> newConsumer;
    SubscriptionConfig consumerConf("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("ProducerLongTimeout3", consumerConf, newConsumer));

    // Make worker wait for 1 min in SyncConsumerNode() and CreateProducer request should not timeout
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0,
                                           "MasterWorkerSCServiceImpl.SyncConsumerNode.sleep", "1*sleep(60000)"));

    // Check request should not timeout as client timeout is 10 mins.
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.maxStreamSize = 67108864;
    DS_ASSERT_OK(client1->CreateProducer("ProducerLongTimeout3", producer, conf));
}

TEST_F(ProducerTest, DISABLED_TestMultiLocalProducerCreateClose)
{
    // This test case tests multi local producers
    // They will generate single master call
    // They all can be created and closed without an error
    const int num_producer = 10;
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());

    // Create a producer config
    auto streamName = "MultiLocalProdCreateClose";
    ProducerConf conf;
    const uint64_t maxStreamSize = 100 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;

    // Create 10 producers on same worker for same stream
    std::vector<std::shared_ptr<Producer>> producerList;
    for (int i = 0; i < num_producer; i++) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
        producerList.emplace_back(producer);
    }

    // Master should get only one request and count should be 1
    ASSERT_EQ(CheckProducerCount(client1, streamName), 1);

    DS_ASSERT_OK(producerList[0]->Close());
    // Count should not change
    ASSERT_EQ(CheckProducerCount(client1, streamName), 1);

    // Close remaining 9 producers on same worker for same stream
    for (int i = 1; i < num_producer; i++) {
        DS_ASSERT_OK(producerList[i]->Close());
    }
    ASSERT_EQ(CheckProducerCount(client1, streamName), 0);
    DS_ASSERT_OK(TryAndDeleteStream(client1, streamName));
}

TEST_F(ProducerTest, TestMultiLocalProducerSendReceive)
{
    // This test case tests multi local producers
    // They will generate single master call
    // All of them can send data to a consumer
    const int num_producer = 10;
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());

    // Create a producer config
    auto streamName = "MultiLocalProdSendReceive";
    ProducerConf conf;
    const uint64_t maxStreamSize = 2 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;

    // Create 10 producers on worker1 for same stream
    std::vector<std::shared_ptr<Producer>> producerList;
    for (int i = 0; i < num_producer; i++) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
        producerList.emplace_back(producer);
    }

    // Create consumer on worker2 for the stream
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    // Master should get only one request and count should be 1
    ASSERT_EQ(CheckProducerCount(client1, streamName), 1);

    // Send and Receive data from all producers
    // Only 1 element per page.
    const size_t elementSize = KB;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(elementSize, element, writeElement);

    // Normal Send.
    for (int i = 0; i < num_producer; i++) {
        DS_ASSERT_OK(producerList[i]->Send(element));
    }

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(num_producer, RECV_WAIT_MILLI_SECONDS, outElements));
    ASSERT_EQ(outElements.size(), num_producer);
    std::string actualData(reinterpret_cast<const char *>(outElements[0].ptr), outElements[0].size);
    std::string data(reinterpret_cast<const char *>(writeElement.data()), writeElement.size());
    EXPECT_EQ(data, actualData);

    // Close remaining 9 producers on same worker for same stream
    for (int i = 0; i < num_producer; i++) {
        DS_ASSERT_OK(producerList[i]->Close());
    }
    ASSERT_EQ(CheckProducerCount(client1, streamName), 0);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(TryAndDeleteStream(client1, streamName));
}

TEST_F(ProducerTest, TestDuplicatedBlockedCreateRequest)
{
    // 1 Producer -> 1 Consumer Same Node.
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 100 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    std::string streamName = "DupBlockedCreateReq";
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));

    // Only 1 element per page.
    const size_t elementSize = 900 * KB;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(elementSize, element, writeElement);

    // Normal Send.
    DS_ASSERT_OK(producer->Send(element));

    // ZMQ timeout before the BlockedCreateRequest is processed normally or timer expried but not yet remove the
    // BlockedCreateRequest.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UnblockCreators.sleep", "sleep(10000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetBlockedCreateRequest.sleep", "sleep(10000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ClientWorkerSCServiceImpl.HandleBlockedCreateTimeout.sleep",
                                           "sleep(10000)"));
    DS_ASSERT_OK(inject::Set("ProducerConsumerWorkerApi.CreateWritePage.adjustRpcTimeoutMs", "call(10)"));
    DS_ASSERT_NOT_OK(producer->Send(element, 10));                  // Timer with less than 10ms
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetBlockedCreateRequest.sleep"));
    DS_ASSERT_OK(inject::Set("ProducerConsumerWorkerApi.CreateWritePage.adjustRpcTimeoutMs", "call(60000)"));

    // Add new BlockedCreateRequest to unordered map, there should be a one exist already for the same producer.
    DS_ASSERT_OK(producer->Send(element, 10));  // Timer with less than 10ms
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "ClientWorkerSCServiceImpl.HandleBlockedCreateTimeout.sleep"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "UnblockCreators.sleep"));

    // Clean up
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client1->DeleteStream(streamName));
}

TEST_F(ProducerTest, TestDuplicatedBlockedCreateRequestOutOfOrder)
{
    // 1 Producer -> 1 Consumer Same Node.
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 100 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    std::string streamName = "DupBlockReqOutOfOrder";
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));

    // Only 1 element per page.
    const size_t elementSize = 900 * KB;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(elementSize, element, writeElement);

    // Normal Send.
    DS_ASSERT_OK(producer->Send(element));

    // Worker Thread A received CreateShmPage request A, but stuck before getting StreamManager lock.
    // Client rpc timeout.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UnblockCreators.sleep", "sleep(10000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamManager.AddBlockCreateRequest.sleep", "sleep(5000)"));
    DS_ASSERT_OK(inject::Set("ProducerConsumerWorkerApi.CreateWritePage.adjustRpcTimeoutMs", "call(10)"));
    DS_ASSERT_NOT_OK(producer->Send(element, 10));                  // Timer with less than 10ms
    DS_ASSERT_OK(inject::Set("ProducerConsumerWorkerApi.CreateWritePage.adjustRpcTimeoutMs", "call(60000)"));

    // Worker Thread B received CreateSgmPage request B, created and added a new BlockedCreateRequest into blockedList.
    // Worker Thread B stuck at getting the BlockedCreateRequest B out from the blockedList.
    // Worker Thread A try to add a new BlockedCreateRequest A but find BlockedCreateRequest B.
    // Worker Thread A: since BlockedCreateRequest B has request pb timestamp later than BlockedCreateRequest A's
    //     request pb timestamp, Worker Thread A do not add BlockedCreateRequest A into blockedList.
    // Worker Thread B continue to process BlockedCreateRequest B and return success to client.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetBlockedCreateRequest.sleep", "sleep(6000)"));
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetBlockedCreateRequest.sleep"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "UnblockCreators.sleep"));

    // Clean up
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client1->DeleteStream(streamName));
}

class ProducerNoKeysTest : public ProducerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        ProducerTest::SetClusterSetupOptions(opts);
        // Set these to not generate signature.
        // Requests to create shm page by the same producer should have non-zero unique timestamp.
        accessKey_ = "";
        secretKey_ = "";
        opts.systemAccessKey = accessKey_;
        opts.systemSecretKey = secretKey_;
    }
};

TEST_F(ProducerNoKeysTest, TestDuplicatedBlockedCreateRequestNoSignature)
{
    // 1 Producer -> 1 Consumer Same Node.
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 100 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    std::string streamName = "DupBlockReqNoSig";
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamName, config, consumer));

    // Only 1 element per page.
    const size_t elementSize = 900 * KB;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(elementSize, element, writeElement);

    // Normal Send.
    DS_ASSERT_OK(producer->Send(element));

    // ZMQ timeout before the BlockedCreateRequest is processed normally or timer expried but not yet remove the
    // BlockedCreateRequest.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UnblockCreators.sleep", "sleep(10000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetBlockedCreateRequest.sleep", "sleep(10000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ClientWorkerSCServiceImpl.HandleBlockedCreateTimeout.sleep",
                                           "sleep(10000)"));
    DS_ASSERT_OK(inject::Set("ProducerConsumerWorkerApi.CreateWritePage.adjustRpcTimeoutMs", "call(10)"));
    DS_ASSERT_NOT_OK(producer->Send(element, 10));                  // Timer with less than 10ms
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetBlockedCreateRequest.sleep"));
    DS_ASSERT_OK(inject::Set("ProducerConsumerWorkerApi.CreateWritePage.adjustRpcTimeoutMs", "call(60000)"));

    // Add new BlockedCreateRequest to unordered map, there should be a one exist already for the same producer.
    DS_ASSERT_OK(producer->Send(element, 10));  // Timer with less than 10ms
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "ClientWorkerSCServiceImpl.HandleBlockedCreateTimeout.sleep"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "UnblockCreators.sleep"));

    // Clean up
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client1->DeleteStream(streamName));
}

TEST_F(ProducerTest, TestScanAndEvalRecycledPage)
{
    // 1 Producer -> 1 Remote Consumer
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    ASSERT_EQ(CreateClient(1, client2), Status::OK());

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("ScanAndEvalRecycledPage", config, consumer));

    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 128 * KB;
    const uint64_t pageSize = 28 * KB;  // up to 4 pages allowed in the stream.
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    DS_ASSERT_OK(client1->CreateProducer("ScanAndEvalRecycledPage", producer, conf));

    const size_t elementSize = 20 * KB;
    const size_t numElementPerPage = 1;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(elementSize, element, writeElement);

    // What do we want to solve here?
    // 1. Producer send 2 elements, so 2 pages (A and B).
    // 2. Page A is acked.
    // 3. ScanAndEval thread locate the last page (B).
    // 4. ScanAndEval thread sleep.
    // 5. Page B is acked.
    // 6. Producer send 2 elements, reused acked pages (A and B).
    // 7. ScanAndEval thread finish sleep, try to receive elements from page (B).
    // 8. The begCursor is updated while ScanAndEval thread holding page (B)
    //    causing coredump because the slot value is garbage.
    //
    // What do we do to make to make the testcase passed?
    // In step 6 above, do not reused page B since page B is holded by the ScanAndEval thread, instead,
    // create a new page.
    DS_ASSERT_OK(producer->Send(element));
    sleep(1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UpdateLastAckCursorUnlocked.sleep", "sleep(1000)"));
    DS_ASSERT_OK(producer->Send(element));
    sleep(1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamDataPage::Receive.sleep", "sleep(15000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AppendFreePagesImplNotLocked", "sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamDataPage::Receive.fake.BIG_ELEMENT", "call()"));
    const int TWO_SEC = 2;
    sleep(TWO_SEC);
    DS_ASSERT_OK(producer->Send(element));
    const int TEN_SEC = 10;
    sleep(TEN_SEC);
    DS_ASSERT_OK(producer->Send(element));

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "StreamDataPage::Receive.sleep"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "UpdateLastAckCursorUnlocked.sleep"));

    // Receive all 4 elements.
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(numElementPerPage, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), numElementPerPage);
    outElements.clear();

    DS_ASSERT_OK(consumer->Receive(numElementPerPage, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), numElementPerPage);
    outElements.clear();

    DS_ASSERT_OK(consumer->Receive(numElementPerPage, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), numElementPerPage);
    outElements.clear();

    DS_ASSERT_OK(consumer->Receive(numElementPerPage, RPC_TIMEOUT, outElements));
    ASSERT_EQ(outElements.size(), numElementPerPage);
    outElements.clear();

    // Clean up
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(client1->DeleteStream("ScanAndEvalRecycledPage"));

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "StreamDataPage::Receive.fake.BIG_ELEMENT"));
}

TEST_F(ProducerTest, TestProducerDiscardPrivateBuffer)
{
    std::shared_ptr<StreamClient> client1;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    std::shared_ptr<Producer> producer;
    const size_t SEND_COUNT = 10;
    const size_t testSize = 100 * KB;
    ProducerConf conf;
    conf.maxStreamSize = 10 * 1024 * 1024;
    DS_ASSERT_OK(client1->CreateProducer("DiscardPrivateBuffer", producer, conf));
    std::thread producerThrd([&client1, &producer]() {
        const int DEFAULT_SLEEP_TIME = 300;
        Element element;
        std::vector<uint8_t> writeElement;
        uint64_t numOfConsumers = 0;
        while (numOfConsumers != 1) {
            client1->QueryGlobalConsumersNum("DiscardPrivateBuffer", numOfConsumers);
            std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
        }
        CreateElement(testSize, element, writeElement);
        for (size_t i = 0; i < SEND_COUNT; i++) {
            Status rc = producer->Send(element);
            int retryCount = 30;
            while (rc.GetCode() == K_OUT_OF_MEMORY && retryCount-- > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                rc = producer->Send(element);
            }
            DS_ASSERT_OK(rc);
        }
        DS_ASSERT_OK(producer->Close());
    });

    // Inject sleep to extend the consumer timing hole
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.UsageMonitor.CheckOverUsedForStream.MockError",
                                           "100*return(K_OUT_OF_MEMORY)"));
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("DiscardPrivateBuffer", config, consumer));

    const int DEFAULT_WAIT_TIME = 1000;
    const int DEFAULT_RETRY_TIME = 10;
    Timer timer;
    std::vector<Element> outElements;
    int sendCount = SEND_COUNT;
    while (sendCount > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
        DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
        if (!outElements.empty()) {
            DS_ASSERT_OK(consumer->Ack(outElements.back().id));
            sendCount -= outElements.size();
        }
    }
    ASSERT_EQ(sendCount, 0);
    producerThrd.join();
}

TEST_F(ProducerTest, TestStaleCursorEarlyReclaim)
{
    // This testcase tests that the early reclaim of shm happens with old last append cursor
    // can trigger data loss.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, client), Status::OK());
        clients.push_back(client);
    }
    std::string streamName = "StaleCursorEarlyReclaim";
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[1]->Subscribe(streamName, config, consumer));
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 10 * MB;
    conf.maxStreamSize = maxStreamSize;
    const size_t testPageSize = 8 * KB;
    conf.pageSize = testPageSize;
    DS_ASSERT_OK(clients[0]->CreateProducer(streamName, producer, conf));
    const size_t testSize = 5 * KB;
    Element element;
    std::vector<uint8_t> writeElement;
    CreateElement(testSize, element, writeElement);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamManager.RemoteAck.delay", "1*sleep(2000)"));
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamDataPool.SendElementsToRemote.wait", "1*sleep(2000)"));
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(producer->Close());
    const int DEFAULT_WAIT_TIME = 10000;
    std::vector<Element> outElements;
    const size_t expectedNum = 2;
    DS_ASSERT_OK(consumer->Receive(expectedNum, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), expectedNum);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients[0]->DeleteStream(streamName));
}

TEST_F(ProducerTest, TestCloseProducerEarlyReclaim)
{
    // Test that close producer triggers early reclaim when consumer was also local.
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::string streamNameBase = "CloseProducerEarlyReclaim";
    // There is only 64MB shm, so only 6 streams can be created on this node.
    ProducerConf conf;
    const uint64_t pageSize = 10 * MB;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.pageSize = pageSize;
    const int streamNum = 6;

    std::vector<std::shared_ptr<Consumer>> consumers(streamNum);
    std::vector<std::shared_ptr<Producer>> producers(streamNum);
    for (int i = 0; i < streamNum; i++) {
        std::string streamName = "CloseProducerEarlyReclaim" + std::to_string(i);
        SubscriptionConfig config("sub", SubscriptionType::STREAM);
        DS_ASSERT_OK(client1->Subscribe(streamName, config, consumers[i]));

        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client1->CreateProducer(streamName, producers[i], conf));
    }
    // Now the 7th stream should fail to create.
    std::shared_ptr<Producer> producer;
    DS_ASSERT_NOT_OK(client1->CreateProducer(streamNameBase, producer, conf));
    // But if one of the stream got their producer and consumer all closed,
    // the 7th stream can be created.
    // Close producer comes last.
    DS_ASSERT_OK(consumers[0]->Close());
    DS_ASSERT_OK(producers[0]->Close());
    DS_ASSERT_OK(client1->CreateProducer(streamNameBase, producer, conf));
}

TEST_F(ProducerTest, TestCloseConsumerEarlyReclaim)
{
    // Test that close consumer triggers early reclaim.
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::string streamNameBase = "CloseConsumerEarlyReclaim";
    // There is only 64MB shm, so only 6 streams can be created on this node.
    ProducerConf conf;
    const uint64_t pageSize = 10 * MB;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.pageSize = pageSize;
    const int streamNum = 6;

    std::vector<std::shared_ptr<Consumer>> consumers(streamNum);
    std::vector<std::shared_ptr<Producer>> producers(streamNum);
    for (int i = 0; i < streamNum; i++) {
        std::string streamName = "CloseProducerEarlyReclaim" + std::to_string(i);
        SubscriptionConfig config("sub", SubscriptionType::STREAM);
        DS_ASSERT_OK(client1->Subscribe(streamName, config, consumers[i]));

        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client1->CreateProducer(streamName, producers[i], conf));
    }
    // Now the 7th stream should fail to create.
    std::shared_ptr<Producer> producer;
    DS_ASSERT_NOT_OK(client1->CreateProducer(streamNameBase, producer, conf));
    // But if one of the stream got their producer and consumer all closed,
    // the 7th stream can be created.
    // Close consumer comes last.
    DS_ASSERT_OK(producers[0]->Close());
    DS_ASSERT_OK(consumers[0]->Close());
    DS_ASSERT_OK(client1->CreateProducer(streamNameBase, producer, conf));
}

TEST_F(ProducerTest, TestEarlyReclaimDeadlock)
{
    // Test that with incorrect code order, deadlock can be triggered with reclaimMutex_.
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(0, client1), Status::OK());
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::string streamNameBase = "CloseConsumerEarlyReclaim";
    // There is only 64MB shm, so only 6 streams can be created on this node.
    ProducerConf conf;
    const uint64_t pageSize = 10 * MB;
    conf.maxStreamSize = TEST_STREAM_SIZE;
    conf.pageSize = pageSize;
    const int streamNum = 6;

    std::vector<std::shared_ptr<Consumer>> consumers(streamNum);
    std::vector<std::shared_ptr<Producer>> producers(streamNum);
    for (int i = 0; i < streamNum; i++) {
        std::string streamName = "CloseProducerEarlyReclaim" + std::to_string(i);
        SubscriptionConfig config("sub", SubscriptionType::STREAM);
        DS_ASSERT_OK(client1->Subscribe(streamName, config, consumers[i]));

        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client1->CreateProducer(streamName, producers[i], conf));
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client1->Subscribe(streamNameBase, config, consumer));
    // Now the 7th stream should fail to create.
    std::shared_ptr<Producer> producer;
    DS_ASSERT_NOT_OK(client1->CreateProducer(streamNameBase, producer, conf));
    // Now test that CloseConsumer would not deadlock.
    DS_ASSERT_OK(consumer->Close());
}

TEST_F(ProducerTest, LEVEL1_TestCreateProducerTimeout1)
{
    // This testcase tests the case that if the CreateProducer requests take too long on master,
    // master will check the timeout and return early before actual timeout.
    // Sleep is injected so that by the time the thread pool picks up the request, it already timed out.
    // Consumer is on same node as the producers, so SyncConsumerNode and UpdateTopoNotification are not sent.
    const int timeoutMs = 10000;
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, timeoutMs, client), Status::OK());
        clients.push_back(client);
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "master.CreateProducer", "1*sleep(8000)"));
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[0]->Subscribe("CreateProducerTimeout1", config, consumer));
    ThreadPool producerPool(NUM_WORKERS);
    auto producerFunc([&clients](uint32_t index) {
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        if (clients[index]->CreateProducer("CreateProducerTimeout1", producer, conf).IsError()) {
            return std::shared_ptr<Producer>();
        }
        return producer;
    });
    Timer timer;
    auto ptr = producerFunc(0);
    ASSERT_EQ(ptr, nullptr);
    auto timeCost = timer.ElapsedMilliSecond();
    LOG(INFO) << "Elapsed time: " << timeCost;
    // sleep a bit so the CreateProducer request actually goes through on master after the timeout.
    const int DEFAULT_WAIT_TIME = 3;
    sleep(DEFAULT_WAIT_TIME);
    auto producer = producerFunc(0);
    ASSERT_NE(producer, nullptr);
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients[0]->DeleteStream("CreateProducerTimeout1"));
}

TEST_F(ProducerTest, LEVEL1_TestCreateProducerTimeout2)
{
    // This testcase tests the case that if rollback fails with timeout, the producer count is still handled.
    // Injection is to simulate SyncConsumerNode fail with timeout, and to trigger rollback logic.
    // And also that the rollback ClearAllRemoteConsumer fail with timeout.
    // This is to make sure the producer count is still decremented.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, client), Status::OK());
        clients.push_back(client);
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "master.PubIncreaseNodeImpl.beforeSendNotification",
                                               "1*return(K_RPC_UNAVAILABLE)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "MasterWorkerSCServiceImpl.ClearAllRemoteConsumer.sleep",
                                               "1*sleep(40000)"));
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[1]->Subscribe("CreateProducerTimeout2", config, consumer));
    ThreadPool producerPool(NUM_WORKERS);
    auto producerFunc([&clients](uint32_t index) {
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        if (clients[index]->CreateProducer("CreateProducerTimeout2", producer, conf).IsError()) {
            return std::shared_ptr<Producer>();
        }
        return producer;
    });
    producerPool.Execute([&producerFunc, i = 0]() { ASSERT_EQ(nullptr, producerFunc(i)); });
    // sleep a bit so the CreateProducer RPC request is sent.
    sleep(1);
    const int producerCount = 3;
    std::vector<std::future<std::shared_ptr<Producer>>> prodFutures;
    for (int i = 0; i < producerCount; i++) {
        prodFutures.push_back(producerPool.Submit([&producerFunc, i = 0]() { return producerFunc(i); }));
    }
    for (auto &fut : prodFutures) {
        auto producer = fut.get();
        ASSERT_NE(producer, nullptr);
        DS_ASSERT_OK(producer->Close());
    }
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients[0]->DeleteStream("CreateProducerTimeout2"));
}

TEST_F(ProducerTest, LEVEL1_TestCreateProducerTimeout3)
{
    // This testcase tests the case that CreateProducer can send UpdateTopoNotification through local bypass instead of
    // actual RPC. In that case it can be blocked by some locks and go beyond scTimeoutDuration. Then worker->master
    // CreateProducer will timeout, and that will release the create lock on worker, so other CreateProducer of the same
    // stream from the same worker can go through. Since the related change would allow parallel CreateProducer on
    // master, now it can for example get OK because it is not the first producer from the worker, but the first
    // producer request is still running and can fail. Injection is to simulate UpdateTopoNotification takes too long.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        const int timeout = 30000;
        ASSERT_EQ(CreateClient(i, timeout, client), Status::OK());
        clients.push_back(client);
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "master.PubIncreaseNodeImpl.beforeSendNotification",
                                               "1*sleep(25000)"));
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[1]->Subscribe("CreateProducerTimeout3", config, consumer));
    ThreadPool producerPool(NUM_WORKERS);
    auto producerFunc([&clients](uint32_t index, std::shared_ptr<Producer> &producer) {
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        return clients[index]->CreateProducer("CreateProducerTimeout3", producer, conf);
    });
    producerPool.Execute([&producerFunc, i = 0]() {
        std::shared_ptr<Producer> producer;
        ASSERT_EQ(producerFunc(i, producer).GetCode(), K_RPC_UNAVAILABLE);
    });
    // sleep a bit so the first CreateProducer RPC request is sent.
    sleep(1);
    std::shared_ptr<Producer> producer;
    Status rc = producerFunc(0, producer);
    ASSERT_EQ(rc.GetCode(), K_TRY_AGAIN);
    // Wait for the first CreateProducer request to rollback on master, and then the new requests should work.
    const int DEFAULT_WAIT_TIME = 5;
    sleep(DEFAULT_WAIT_TIME);
    DS_ASSERT_OK(producerFunc(0, producer));
    DS_ASSERT_OK(producer->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients[0]->DeleteStream("CreateProducerTimeout3"));
}

TEST_F(ProducerTest, LEVEL1_TestParallelCreateCloseProducer)
{
    // This testcase tests that CreateProducer is blocked by worker level create lock
    // when last producer on the worker is getting closed on master.
    // This is so that the conflict between CreateProducer and CloseProducer is mitigated.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i <= 1; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, client), Status::OK());
        clients.push_back(client);
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, i, "master.PubDecreaseNode.beforeSendNotification", "1*sleep(7000)"));
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[1]->Subscribe("ParallelCreateCloseProducer", config, consumer));
    ThreadPool producerPool(1);
    auto producerFunc([&clients](uint32_t index, std::shared_ptr<Producer> &producer) {
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        return clients[index]->CreateProducer("ParallelCreateCloseProducer", producer, conf);
    });
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(producerFunc(0, producer));
    producerPool.Execute([&producer]() { DS_ASSERT_OK(producer->Close()); });
    // sleep a bit so the CloseProducer RPC request is sent.
    sleep(1);
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(producerFunc(0, producer2));
    DS_ASSERT_OK(producer2->Close());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients[0]->DeleteStream("ParallelCreateCloseProducer"));
}

TEST_F(ProducerTest, TestLocalClearAllRemoteConsumerParallelSubscribe)
{
    // This testcase tests that the local ClearAllRemoteConsumer after CloseProducer
    // gets triggered after a remote Subscribe is done.
    // This creates a timing hole where ClearAllRemoteConsumer is called after new remote consumer is added,
    // and before scan is done.
    // But functionally it should be unaffected because ClearAllRemoteConsumer does not do flush nor RemoveStreamObject.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, client), Status::OK());
        clients.push_back(client);
        // Delay the local ClearAllRemoteConsumer after CloseProducer.
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "StreamManager.CloseProducer.timing", "1*sleep(2000)"));
    }
    // Delay scan.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamDataPool.SendElementsToRemote.wait", "1*sleep(4000)"));
    ThreadPool producerPool(NUM_WORKERS);
    auto producerFunc([&clients](uint32_t index, std::shared_ptr<Producer> &producer) {
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        conf.retainForNumConsumers = 1;
        return clients[index]->CreateProducer("ConsumerParallelSubscribe", producer, conf);
    });
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(producerFunc(0, producer));
    auto fut = producerPool.Submit([&producer]() {
        std::string data = "H";
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        RETURN_IF_NOT_OK(producer->Send(element));
        return producer->Close();
    });
    // sleep a bit so the CloseProducer RPC request is sent.
    sleep(1);
    const int DEFAULT_WAIT_TIME = 5000;
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[1]->Subscribe("ConsumerParallelSubscribe", config, consumer));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
    ASSERT_EQ(outElements.size(), size_t(1));
    DS_ASSERT_OK(fut.get());
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients[0]->DeleteStream("ConsumerParallelSubscribe"));
}

TEST_F(ProducerTest, SendReturnOOMTest)
{
    std::shared_ptr<StreamClient> client;
    ASSERT_EQ(CreateClient(0, client), Status::OK());

    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const uint64_t maxStreamSize = 10 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    conf.retainForNumConsumers = 1;
    DS_ASSERT_OK(client->CreateProducer("SendReturnOOM", producer, conf));

    Element element;
    std::vector<uint8_t> writeElement;
    const uint elementSize = 900 * KB;
    CreateElement(elementSize, element, writeElement);

    DS_ASSERT_OK(producer->Send(element));

    // We first sleep 5 seconds to let the timeout expired in the first loop, producer then request a new page.
    // After getting the new page, we return K_TRY_AGAIN in writePage_->Insert(...) in the second loop to simulate
    // timeout on getting the page lock on the new page.
    DS_ASSERT_OK(datasystem::inject::Set("producer_insert", "1*sleep(5000)->1*return(K_TRY_AGAIN)"));
    const uint timeout = 1000;
    ASSERT_EQ(producer->Send(element, timeout).GetCode(), K_OUT_OF_MEMORY);
}

TEST_F(ProducerTest, TestParallelProducerUse)
{
    std::shared_ptr<StreamClient> client;
    ASSERT_EQ(CreateClient(0, client), Status::OK());

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client->CreateProducer("ParallelProducerUse", producer));

    DS_ASSERT_OK(datasystem::inject::Set("CheckAndSetInUse.success.sleep", "sleep(5000)"));

    std::string data = "Hello";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // Create a producer thread that Send() last at least 5 seconds.
    ThreadPool pool(1);
    auto producerSendFunc([&producer, &element]() { return producer->Send(element); });
    std::future<Status> fut = pool.Submit([&producerSendFunc]() { return producerSendFunc(); });

    sleep(1);

    // Parallel call from the same producer should fail.
    StatusCode expectedCode = K_SC_STREAM_IN_USE;
    ASSERT_EQ(producer->Send(element).GetCode(), expectedCode);
    ASSERT_EQ(producer->Close().GetCode(), expectedCode);

    DS_ASSERT_OK(fut.get());

    DS_ASSERT_OK(datasystem::inject::Clear("CheckAndSetInUse.success.sleep"));

    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(producer->Close());
}

TEST_F(ProducerTest, EXCLUSIVE_TestParallelLocalCreateCloseProducer)
{
    // In this testcase, we will create multiple local producers in parallel
    const int num_producers = 10;
    std::shared_ptr<StreamClient> client;
    ASSERT_EQ(CreateClient(0, client), Status::OK());

    ThreadPool producerPool(num_producers);
    auto producerCreateFunc([&client]() {
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        if (client->CreateProducer("ParallelLocalCreateCloseProd", producer, conf).IsError()) {
            return std::shared_ptr<Producer>();
        }
        return producer;
    });

    // Create multiple local producers in the same node in parallel
    std::vector<std::future<std::shared_ptr<Producer>>> prodFutures;
    for (int i = 0; i < num_producers; i++) {
        prodFutures.push_back(producerPool.Submit([&producerCreateFunc]() { return producerCreateFunc(); }));
    }

    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("ParallelLocalCreateCloseProd", config, consumer));

    std::vector<std::shared_ptr<Producer>> producers;
    for (auto &fut : prodFutures) {
        auto producer = fut.get();
        ASSERT_NE(producer, nullptr);
        producers.push_back(producer);
    }

    // Master should get only one request and count should be 1
    ASSERT_EQ(CheckProducerCount(client, "ParallelLocalCreateCloseProd"), 1);

    // Close multiple local producers in the same node in parallel
    std::vector<std::future<Status>> prodCloseFutures;
    for (int i = 0; i < num_producers; i++) {
        prodCloseFutures.push_back(producerPool.Submit([&producers, i]() { return producers[i]->Close(); }));
    }
    for (auto &fut : prodCloseFutures) {
        DS_ASSERT_OK(fut.get());
    }

    // Master should get only one request and count should be 0
    ASSERT_EQ(CheckProducerCount(client, "ParallelLocalCreateCloseProd"), 0);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(TryAndDeleteStream(client, "ParallelLocalCreateCloseProd"));
}

TEST_F(ProducerTest, EXCLUSIVE_TestParallelLocalCreateCloseProducerRollBack)
{
    // In this testcase, we will create multiple local producers in parallel
    const int num_producers = 10;
    std::shared_ptr<StreamClient> client;
    ASSERT_EQ(CreateClient(0, client), Status::OK());
    std::string streamName = "ParallelLocalCreateCloseProdRollBack";
    ThreadPool producerPool(num_producers);
    auto producerCreateFunc([&client, streamName]() {
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        if (client->CreateProducer(streamName, producer, conf).IsError()) {
            return std::shared_ptr<Producer>();
        }
        return producer;
    });

    // Make first CreateProducer Call fail
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.CreateProducer.beforeSendToMaster",
                                           "1*return(K_RUNTIME_ERROR)"));

    // Create multiple local producers in the same node in parallel
    std::vector<std::future<std::shared_ptr<Producer>>> prodFutures;
    for (int i = 0; i < num_producers; i++) {
        prodFutures.push_back(producerPool.Submit([&producerCreateFunc]() { return producerCreateFunc(); }));
    }

    std::shared_ptr<StreamClient> client2;
    ASSERT_EQ(CreateClient(1, client2), Status::OK());
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));

    std::vector<std::shared_ptr<Producer>> producers;
    bool gotFailedProducer = false;
    for (auto &fut : prodFutures) {
        auto producer = fut.get();
        if (gotFailedProducer) {
            // Only one producer should be failed
            ASSERT_NE(producer, nullptr);
        }
        if (producer == nullptr) {
            // mark first producer fail
            gotFailedProducer = true;
            continue;
        }
        producers.push_back(producer);
    }

    // Master should get only one request and count should be 1
    ASSERT_EQ(CheckProducerCount(client, streamName), 1);

    // Close multiple local producers in the same node in parallel
    std::vector<std::future<Status>> prodCloseFutures;
    for (auto &producer : producers) {
        prodCloseFutures.push_back(producerPool.Submit([&producer]() { return producer->Close(); }));
    }
    for (auto &fut : prodCloseFutures) {
        DS_ASSERT_OK(fut.get());
    }

    // Master should get only one request and count should be 0
    ASSERT_EQ(CheckProducerCount(client, streamName), 0);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(TryAndDeleteStream(client, streamName));
}

class LargeScaleProducerTest : public ProducerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        ProducerTest::SetClusterSetupOptions(opts);
        opts.numWorkers = NUM_WORKERS;
        // Enable stream data verification for testing purposes
        opts.workerGflagParams += " -enable_stream_data_verification=true";
    }

    void SetUp() override
    {
        ProducerTest::SetUp();
    }

    void TearDown() override
    {
        ProducerTest::TearDown();
    }

protected:
    const int NUM_WORKERS = 10;
};

TEST_F(LargeScaleProducerTest, EXCLUSIVE_TestParallelCreateProducer1)
{
    // This testcase tests that CreateProducer can be handled in parallel on master.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, client), Status::OK());
        clients.push_back(client);
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, i, "MasterWorkerSCServiceImpl.SyncConsumerNode.sleep", "sleep(5000)"));
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[0]->Subscribe("ParallelCreate1", config, consumer));
    ThreadPool producerPool(NUM_WORKERS);
    auto producerFunc([&clients](uint32_t index) {
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        if (clients[index]->CreateProducer("ParallelCreate1", producer, conf).IsError()) {
            return std::shared_ptr<Producer>();
        }
        return producer;
    });
    Timer timer;
    // Multiple producers per node, to also test out the worker level create lock.
    std::vector<std::future<std::shared_ptr<Producer>>> prodFutures;
    for (int i = 0; i < NUM_WORKERS; i++) {
        prodFutures.push_back(producerPool.Submit([&producerFunc, i]() { return producerFunc(i); }));
    }
    std::vector<std::shared_ptr<Producer>> producers;
    for (auto &fut : prodFutures) {
        auto producer = fut.get();
        ASSERT_NE(producer, nullptr);
        producers.push_back(producer);
    }
    auto timeCost = timer.ElapsedMilliSecond();
    LOG(INFO) << "Elapsed time for Create Producer: " << timeCost;
    // A 5-second is injected to the SyncConsumerNode request, so the requests should take more than 5 seconds.
    const uint64_t minExpectedTime = 5000;
    // While they should run in parallel, so the total elapsed time should not be too off from 5 seconds.
    const uint64_t maxExpectedTime = minExpectedTime + 500;
    ASSERT_TRUE(timeCost >= minExpectedTime && timeCost <= maxExpectedTime);
    for (auto &producer : producers) {
        DS_ASSERT_OK(producer->Close());
    }
    Timer timer1;
    DS_ASSERT_OK(consumer->Close());
    timeCost = timer1.ElapsedMilliSecond();
    LOG(INFO) << "Elapsed time for Close Consumer: " << timeCost;

    DS_ASSERT_OK(clients.back()->DeleteStream("ParallelCreate1"));
}

TEST_F(LargeScaleProducerTest, EXCLUSIVE_TestParallelCreateProducer2)
{
    // This testcase tests that CreateProducer can be handled in parallel on master.
    // In this case, create multiple producers per node, to also test out the worker level create lock.
    // Also test stream data verification logic, to make sure that producer number is still handled correctly.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, client), Status::OK());
        clients.push_back(client);
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[0]->Subscribe("ParallelCreate2", config, consumer));
    ThreadPool producerPool(NUM_WORKERS);
    auto producerFunc([&clients](uint32_t index) {
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        if (clients[index]->CreateProducer("ParallelCreate2", producer, conf).IsError()) {
            return std::shared_ptr<Producer>();
        }
        return producer;
    });
    const int producerCount = 4;
    Timer timer;
    std::vector<std::future<std::shared_ptr<Producer>>> prodFutures;
    for (int i = 0; i < NUM_WORKERS; i++) {
        for (int j = 0; j < producerCount; j++) {
            prodFutures.push_back(producerPool.Submit([&producerFunc, i]() { return producerFunc(i); }));
        }
    }
    std::vector<std::shared_ptr<Producer>> producers;
    for (auto &fut : prodFutures) {
        auto producer = fut.get();
        ASSERT_NE(producer, nullptr);
        std::string data = "H";
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_OK(producer->Send(element));
        DS_ASSERT_OK(producer->Close());
    }
    const int totalNum = producerCount * NUM_WORKERS;
    const int DEFAULT_WAIT_TIME = 5000;
    std::vector<Element> outElements;
    ASSERT_EQ(consumer->Receive(totalNum, DEFAULT_WAIT_TIME, outElements), Status::OK());
    ASSERT_EQ(outElements.size(), size_t(totalNum));
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients.back()->DeleteStream("ParallelCreate2"));
}

TEST_F(LargeScaleProducerTest, EXCLUSIVE_TestParallelCreateProducer3)
{
    // This testcase tests that Subscribe happens in between of CreateProducer requests.
    // Injection is done at master level, so 5s will be spent even if there is no consumer for SyncConsumerNode.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, client), Status::OK());
        clients.push_back(client);
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, i, "master.PubIncreaseNodeImpl.beforeSendNotification", "sleep(5000)"));
    }
    ThreadPool producerPool(NUM_WORKERS);
    auto producerFunc([&clients](uint32_t index) {
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        const uint64_t maxStreamSize = 10 * MB;
        conf.maxStreamSize = maxStreamSize;
        conf.pageSize = 1 * MB;
        if (clients[index]->CreateProducer("ParallelCreate3", producer, conf).IsError()) {
            return std::shared_ptr<Producer>();
        }
        return producer;
    });
    Timer timer;
    // Multiple producers per node, to also test out the worker level create lock.
    std::vector<std::future<std::shared_ptr<Producer>>> prodFutures;
    const int halfWorkers = NUM_WORKERS / 2;
    for (int i = 0; i < halfWorkers; i++) {
        prodFutures.push_back(producerPool.Submit([&producerFunc, i]() { return producerFunc(i); }));
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[0]->Subscribe("ParallelCreate3", config, consumer));
    for (int i = halfWorkers; i < NUM_WORKERS; i++) {
        prodFutures.push_back(producerPool.Submit([&producerFunc, i]() { return producerFunc(i); }));
    }
    std::vector<std::shared_ptr<Producer>> producers;
    for (auto &fut : prodFutures) {
        auto producer = fut.get();
        ASSERT_NE(producer, nullptr);
        producers.push_back(producer);
    }
    auto timeCost = timer.ElapsedMilliSecond();
    LOG(INFO) << "Elapsed time: " << timeCost;
    // A 5-second is injected to the SyncConsumerNode request, so the requests should take more than 5 seconds.
    // But the Subscribe request will hold the xlock and accessor, so in total it should take more than 10 seconds.
    const uint64_t minExpectedTime = 10000;
    // While they should run in parallel, so the total elapsed time should not be too off from 5 seconds.
    const uint64_t maxExpectedTime = minExpectedTime + 500;
    ASSERT_TRUE(timeCost >= minExpectedTime && timeCost <= maxExpectedTime);
    for (auto &producer : producers) {
        DS_ASSERT_OK(producer->Close());
    }
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients.back()->DeleteStream("ParallelCreate3"));
}

TEST_F(LargeScaleProducerTest, DISABLED_TestParallelCloseProducer)
{
    // This testcase tests that CloseProducer can be handled in parallel on master.
    std::vector<std::shared_ptr<StreamClient>> clients;
    for (int i = 0; i < NUM_WORKERS; i++) {
        std::shared_ptr<StreamClient> client;
        ASSERT_EQ(CreateClient(i, client), Status::OK());
        clients.push_back(client);
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, i, "master.PubDecreaseNode.beforeSendNotification", "sleep(5000)"));
    }
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(clients[0]->Subscribe("ParallelClose", config, consumer));
    ThreadPool producerPool(NUM_WORKERS);
    const int producerCount = 4;
    std::vector<std::shared_ptr<Producer>> producers;
    for (int i = 0; i < NUM_WORKERS; i++) {
        for (int j = 0; j < producerCount; j++) {
            std::shared_ptr<Producer> producer;
            ProducerConf conf;
            const uint64_t maxStreamSize = 10 * MB;
            conf.maxStreamSize = maxStreamSize;
            conf.pageSize = 1 * MB;
            DS_ASSERT_OK(clients[i]->CreateProducer("ParallelClose", producer, conf));
            producers.push_back(producer);
        }
    }
    Timer timer;
    std::vector<std::future<Status>> prodFutures;
    for (auto &producer : producers) {
        prodFutures.push_back(producerPool.Submit([producer]() { return producer->Close(); }));
    }
    for (auto &fut : prodFutures) {
        DS_ASSERT_OK(fut.get());
    }
    auto timeCost = timer.ElapsedMilliSecond();
    LOG(INFO) << "Elapsed time: " << timeCost;
    // A 5-second is injected to the PubDecreaseNode, so the requests should take more than 5 seconds.
    const uint64_t minExpectedTime = 5000;
    // While they should run in parallel, so the total elapsed time should not be too off from 5 seconds.
    const uint64_t maxExpectedTime = minExpectedTime + 500;
    ASSERT_TRUE(timeCost >= minExpectedTime && timeCost <= maxExpectedTime);
    DS_ASSERT_OK(consumer->Close());
    DS_ASSERT_OK(clients.back()->DeleteStream("ParallelClose"));
}

TEST_F(ProducerTest, UpdateLocalPubLastDataPageFailed)
{
    // 2 producer -> 1 remote consumer.
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(CreateClient(0, client1));
    DS_ASSERT_OK(CreateClient(1, client2));
    std::string streamName = "UpdateLocalPubLastDataPgFail";
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer, true));

    std::shared_ptr<Producer> producer1;
    std::shared_ptr<Producer> producer2;
    ProducerConf conf;
    const uint maxStreamSize = 10 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer1, conf));
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer2, conf));

    // Each page is enough to hold only 1 element, so we are creating a new page for every element.
    const uint testSize1 = 600 * KB;
    Element element1;
    std::vector<uint8_t> writeElement1;
    DS_ASSERT_OK(CreateElement(testSize1, element1, writeElement1));
    for (uint i = 0; i < K_TWO; ++i) {
        DS_ASSERT_OK(producer1->Send(element1, RPC_TIMEOUT));
        DS_ASSERT_OK(producer2->Send(element1, RPC_TIMEOUT));
    }

    // Producer 1 and Producer 2 cursor's ShmView is at page 4.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UpdateLocalPubLastDataPage.skip", "return(K_OK)"));
    DS_ASSERT_OK(producer1->Send(element1, RPC_TIMEOUT));
    // Producer 2 cursor's ShmView remain at page 4.

    const uint SLEEP_TIME_SEC = 5;
    sleep(SLEEP_TIME_SEC);  // Page 4 is recycled during sleep.

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "UpdateLocalPubLastDataPage.skip"));
    const uint testSize2 = 300 * KB;
    Element element2;
    std::vector<uint8_t> writeElement2;
    DS_ASSERT_OK(CreateElement(testSize2, element2, writeElement2));

    // Since page 4 is recycled, Producer 2 should create new page from worker.
    DS_ASSERT_OK(producer2->Send(element2, RPC_TIMEOUT));
}

TEST_F(ProducerTest, TestProducerCloseAndNewProducerCreate)
{
    // 2 producer -> 1 remote consumer.
    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(CreateClient(0, client1));
    DS_ASSERT_OK(CreateClient(1, client2));
    std::string streamName = "ProducerCloseAndNewProducerCreate";
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer, true));

    std::shared_ptr<Producer> producer1;
    std::shared_ptr<Producer> producer2;
    ProducerConf conf;
    const uint maxStreamSize = 10 * MB;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = 1 * MB;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer1, conf));

    // Each page is enough to hold only 1 element, so we are creating a new page for every element.
    const uint testSize1 = 600 * KB;
    Element element1;
    std::vector<uint8_t> writeElement1;
    DS_ASSERT_OK(CreateElement(testSize1, element1, writeElement1));
    for (uint i = 0; i < K_TWO; ++i) {
        writeElement1[0] = '1' + i;
        DS_ASSERT_OK(producer1->Send(element1, RPC_TIMEOUT));
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamDataPool::ObjectPartition::RemoveStreamObject.sleep",
                                           "1*sleep(2000)"));
    DS_ASSERT_OK(producer1->Close());
    sleep(1);
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer2, conf));

    const uint testSize2 = 300 * KB;
    Element element2;
    std::vector<uint8_t> writeElement2;
    DS_ASSERT_OK(CreateElement(testSize2, element2, writeElement2));

    for (uint i = 0; i < K_TWO; ++i) {
        writeElement1[0] = '3' + i;
        DS_ASSERT_OK(producer2->Send(element1, RPC_TIMEOUT));
    }

    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(K_TWO + K_TWO, RPC_TIMEOUT, outElements));
    for (auto &ele : outElements) {
        LOG(INFO) << ele.ptr[0];
    }
    ASSERT_EQ(outElements.size(), K_TWO + K_TWO);
    // The 1st element of producer2 is inserted in page2, which worker believe is the reserved page,
    // but this page actually freed before returning CreateProducerRsp for creating producer2.
    // Therefore, the above LOG(INFO) output is: 1, 2, 4 and missing 3.
}

TEST_F(ProducerTest, TestCreateProducerWhenAllocPage)
{
    std::shared_ptr<StreamClient> client;
    std::string streamName = "stream001";

    DS_ASSERT_OK(CreateClient(0, client));

    const uint maxStreamSize = 10 * MB;
    const uint pageSize = 64 * KB;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client->CreateProducer(streamName, producer, conf));

    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client->Subscribe(streamName, config, consumer));

    const size_t sizeElement = 10 * KB;
    std::string writeElement = RandomData().GetRandomString(sizeElement);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    const int threadNum = 3;
    const size_t numElements = 1000;
    ThreadPool pool(threadNum);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.AddCursor.afterLockCursorMutex", "sleep(10)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.UpdateLocalCursorLastDataPage.beforeLockCursorMutex",
                                           "sleep(10)"));
    std::vector<std::future<Status>> futs;
    futs.push_back(pool.Submit([&producer, element] {
        const int DEFAULT_SLEEP_TIME = 300;
        int retryLimit = 30;

        for (size_t i = 0; i < numElements; i++) {
            auto rc = producer->Send(element);
            if (rc.IsError()) {
                while (rc.GetCode() == K_OUT_OF_MEMORY && retryLimit-- > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                    rc = producer->Send(element);
                }
            }
            RETURN_IF_NOT_OK(rc);
        }

        return Status::OK();
    }));

    futs.push_back(pool.Submit([&consumer] {
        Timer timer;
        size_t remaining = numElements;
        int round = 0;
        const int PER_RECEIVE_NUM = 1;
        const int DEFAULT_WAIT_TIME = 1000;
        const int DEFAULT_RETRY_TIME = 30;
        while (remaining > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
            std::vector<Element> outElements;
            RETURN_IF_NOT_OK(consumer->Receive(PER_RECEIVE_NUM, DEFAULT_WAIT_TIME, outElements));
            LOG(INFO) << "remaining num : " << remaining << ", receive num : " << outElements.size() << " ;" << round++;
            if (!outElements.empty()) {
                remaining -= outElements.size();
                RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
            }
        }
        CHECK_FAIL_RETURN_STATUS(remaining == 0, K_RUNTIME_ERROR, "failed to receive all data");
        return Status::OK();
    }));

    futs.push_back(pool.Submit([&client, &conf, &streamName] {
        const int createCount = 10;
        for (int i = 0; i < createCount; i++) {
            std::shared_ptr<Producer> producer;
            RETURN_IF_NOT_OK(client->CreateProducer(streamName, producer, conf));
        }
        return Status::OK();
    }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
}

TEST_F(ProducerTest, TestSendWithZeroTimeoutNotWait)
{
    std::shared_ptr<StreamClient> client;
    std::string streamName = "stream001";
    DS_ASSERT_OK(CreateClient(0, client));

    const uint maxStreamSize = 10 * MB;
    const uint pageSize = 64 * KB;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client->CreateProducer(streamName, producer, conf));
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client->Subscribe(streamName, config, consumer));

    // Each element occupies a single page.
    const size_t sizeElement = 50 * KB;
    std::string writeElement = RandomData().GetRandomString(sizeElement);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    // using the reserve page.
    DS_ASSERT_OK(producer->Send(element));

    cluster_->SetInjectAction(WORKER, 0, "worker.Allocator.AllocateMemory", "return(K_OUT_OF_MEMORY)");
    Timer timer;
    int maxSendElapsed = 100;
    ASSERT_EQ(producer->Send(element, 0).GetCode(), K_OUT_OF_MEMORY);
    auto elapsed = timer.ElapsedMilliSecondAndReset();
    LOG(INFO) << "elapsed:" << elapsed;
    ASSERT_LT(elapsed, maxSendElapsed);
}

TEST_F(ProducerTest, DISABLED_TestSendWithZeroTimeoutParallel)
{
    std::shared_ptr<StreamClient> client;
    std::string streamName = "stream001";

    DS_ASSERT_OK(CreateClient(0, client));

    const uint maxStreamSize = 10 * MB;
    const uint pageSize = 64 * KB;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;

    std::vector<std::shared_ptr<Producer>> producers;
    int producerCount = 5;
    for (int i = 0; i < producerCount; i++) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client->CreateProducer(streamName, producer, conf));
        producers.emplace_back(std::move(producer));
    }

    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client->Subscribe(streamName, config, consumer));

    const size_t testDataSize = pageSize * 100;
    const size_t sizeElement = 512;
    const size_t numElements = testDataSize / sizeElement / producers.size();
    std::string writeElement = RandomData().GetRandomString(sizeElement);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());

    ThreadPool pool(producers.size() + 1);
    std::vector<std::future<Status>> futs;
    std::atomic_bool failed{ false };
    for (auto &producer : producers) {
        futs.push_back(pool.Submit([producer, element, numElements, &failed] {
            for (size_t i = 0; i < numElements; i++) {
                if (failed) {
                    return Status::OK();
                }
                // send with 0 timeout.
                Status rc = producer->Send(element, 0);
                if (rc.IsError()) {
                    failed = true;
                    LOG(ERROR) << "Send failed with:" << rc.ToString();
                    return rc;
                }
            }
            return Status::OK();
        }));
    }
    const size_t recvNumElements = numElements * producers.size();
    futs.push_back(pool.Submit([&consumer, recvNumElements, &failed] {
        Timer timer;
        size_t remaining = recvNumElements;
        int round = 0;
        const int PER_RECEIVE_NUM = 1;
        const int DEFAULT_WAIT_TIME = 1000;
        const int DEFAULT_RETRY_TIME = 30;
        while (remaining > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
            if (failed) {
                return Status::OK();
            }
            std::vector<Element> outElements;
            Status rc = consumer->Receive(PER_RECEIVE_NUM, DEFAULT_WAIT_TIME, outElements);
            if (rc.IsError()) {
                failed = true;
                LOG(ERROR) << "Receive failed with:" << rc.ToString();
                return rc;
            }
            LOG(INFO) << "remaining num : " << remaining << ", receive num : " << outElements.size() << " ;" << round++;
            if (!outElements.empty()) {
                remaining -= outElements.size();
                RETURN_IF_NOT_OK(consumer->Ack(outElements.back().id));
            }
        }
        CHECK_FAIL_RETURN_STATUS(remaining == 0, K_RUNTIME_ERROR, "failed to receive all data");
        return Status::OK();
    }));

    for (auto &fut : futs) {
        DS_ASSERT_OK(fut.get());
    }
    ASSERT_TRUE(!failed);
}
}  // namespace st
}  // namespace datasystem
