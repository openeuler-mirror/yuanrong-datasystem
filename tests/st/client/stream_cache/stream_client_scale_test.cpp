/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Stream cache client scale tests.
 */

#include <unistd.h>
#include <chrono>
#include <csignal>
#include <memory>
#include <string>
#include <tuple>

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include "common.h"
#include "common_distributed_ext.h"
#include "common/stream_cache/stream_common.h"
#include "client/stream_cache/sc_client_common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
constexpr int K_TWO = 2;
constexpr int K_TEN = 10;
constexpr int K_THIRTY = 30;
constexpr int SCALE_UP_WAIT_TIME = 3;
constexpr int SCALE_DOWN_WAIT_TIME = 3;
constexpr int NODE_DEAD_TIMEOUT = 8;
const std::string HOST_IP = "127.0.0.1";
class StreamClientScaleTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = FormatString(" -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -log_monitor=true",
                                              nodeTimeoutS_, nodeDeadTimeoutS_);
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitStreamClient(0, w1Client_);
        InitStreamClient(1, w2Client_);
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE;

        InitTestEtcdInstance();
    }

    void TearDown() override
    {
        w1Client_.reset();
        w2Client_.reset();
        ExternalClusterTest::TearDown();
    }

    void CheckCount(std::shared_ptr<StreamClient> client, const std::string &streamName, int producerCount,
                    int consumerCount)
    {
        uint64_t result = 0;
        if (producerCount >= 0) {
            DS_ASSERT_OK(client->QueryGlobalProducersNum(streamName, result));
            EXPECT_EQ(result, static_cast<uint64_t>(producerCount));
            result = 0;
        }
        if (consumerCount >= 0) {
            DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamName, result));
            EXPECT_EQ(result, static_cast<uint64_t>(consumerCount));
            result = 0;
        }
    }

    Status AddNode()
    {
        const int newWorkerIdx = workerNum_++;
        HostPort workerAddr(HOST_IP, GetFreePort());
        HostPort masterAddr;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(0, masterAddr));
        RETURN_IF_NOT_OK(cluster_->AddNode(masterAddr, workerAddr.ToString(), GetFreePort()));
        RETURN_IF_NOT_OK(cluster_->WaitNodeReady(WORKER, newWorkerIdx));
        return Status::OK();
    }

    void VoluntaryScaleDownInject(int workerIdx)
    {
        std::string checkFilePath = FLAGS_log_dir.c_str();
        std::string client = "client";
        checkFilePath = checkFilePath.substr(0, checkFilePath.length() - client.length()) + "/worker"
                        + std::to_string(workerIdx) + "/log/worker-status";
        std::ofstream ofs(checkFilePath);
        if (!ofs.is_open()) {
            LOG(ERROR) << "Can not open worker status file in " << checkFilePath
                       << ", voluntary scale in will not start, errno: " << errno;
        } else {
            ofs << "voluntary scale in\n";
        }
        ofs.close();
        kill(cluster_->GetWorkerPid(workerIdx), SIGTERM);
    }

    void InitTestEtcdInstance()
    {
        if (db_ != nullptr) {
            return;
        }
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        FLAGS_etcd_address = etcdAddress;
        db_ = std::make_unique<EtcdStore>(etcdAddress);
        DS_ASSERT_OK(db_->Init());
        (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
        (void)db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE));
    }

    bool CheckScaleDownFinished(const std::string &workerAddr)
    {
        std::string value;
        auto status = db_->Get(ETCD_CLUSTER_TABLE, workerAddr, value);
        if (status.GetCode() == K_NOT_FOUND) {
            return true;
        }
        return false;
    }

    void WaitForVoluntaryDownFinished(int workerIndex, int timeoutS = 50)
    {
        HostPort workerAddr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddr));
        Timer timer;
        while (timer.ElapsedSecond() < timeoutS) {
            if (CheckScaleDownFinished(workerAddr.ToString()) && !cluster_->CheckWorkerProcess(workerIndex)) {
                LOG(INFO) << "scale down finish time: " << timer.ElapsedSecond()
                          << " worker: " << workerAddr.ToString();
                return;
            }
            auto interval = 100;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        ASSERT_TRUE(false) << "Voluntary scaling down is not completed: " << workerAddr.ToString();
    }

    /**
     * @brief Creates streamNum producers and consumers on w1Client, placing them into streams
     * @param[in] streams The map of stream names to pairs of prod/cons
     * @param[in] streamNum The number of streams to create
     * @param[in] sameWorker Whether to create consumers on w2Client instead
     */
    void CreateNProducerAndConsumer(std::map<std::string, std::pair<
        std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> &streams,
        int streamNum, std::string streamName,
        bool sameWorker = true)
    {
        for (int i = 0; i < streamNum; ++i) {
            std::string strmName = streamName + std::to_string(i);
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(w1Client_->CreateProducer(strmName, producer, defaultProducerConf_));
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config("sub" + std::to_string(i), SubscriptionType::STREAM);
            if (sameWorker) {
                DS_ASSERT_OK(w1Client_->Subscribe(strmName, config, consumer));
            } else {
                DS_ASSERT_OK(w2Client_->Subscribe(strmName, config, consumer));
            }
            streams.emplace(strmName, std::make_pair(producer, consumer));
            CheckCount(w1Client_, strmName, 1, 1);
        }
    }

    void WaitAllNodesJoinIntoHashRing(int num, uint64_t timeoutSec = 60, std::string azName = "")
    {
        int S2Ms = 1000;
        WaitHashRingChange(
            [&](const HashRingPb &hashRing) {
                if (hashRing.workers_size() != num || hashRing.add_node_info_size() != 0
                    || hashRing.del_node_info_size() != 0) {
                    return false;
                }
                for (auto &worker : hashRing.workers()) {
                    if (worker.second.state() != WorkerPb::ACTIVE) {
                        return false;
                    }
                }
                return true;
            },
            timeoutSec * S2Ms, azName);
        sleep(WORKER_RECEIVE_DELAY);
    }

    template <typename F>
    void WaitHashRingChange(F &&f, uint64_t timeoutMs = 30'000, std::string azName = "")
    {
        if (!db_) {
            InitTestEtcdInstance();
        }
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool flag = false;
        HashRingPb ring;
        while (std::chrono::steady_clock::now() < timeOut) {
            std::string hashRingStr;
            auto trueRingTable = azName.empty() ? ETCD_RING_PREFIX : '/' + azName + ETCD_RING_PREFIX;
            DS_ASSERT_OK(db_->Get(trueRingTable, "", hashRingStr));
            ASSERT_TRUE(ring.ParseFromString(hashRingStr));
            if (f(ring)) {
                flag = true;
                break;
            }
            const int interval = 100;  // 100ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        LOG(INFO) << "Check " << (flag ? "success" : "failed")
                  << ", Ring info:" << worker::HashRingToJsonString(ring);
        ASSERT_TRUE(flag);
    }

    void TestSendRecv(std::shared_ptr<Producer> &producer, std::shared_ptr<Consumer> &consumer)
    {
        // Produce element
        std::string data = "Hello World";
        Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
        DS_ASSERT_OK(producer->Send(element));

        // Read the element to make sure other requests can go through
        std::vector<Element> outElements;
        const int DEFAULT_WAIT_TIME = 5000;
        DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
        ASSERT_EQ(outElements.size(), size_t(1));
        DS_ASSERT_OK(consumer->Ack(1));
    }

    void SendHelper(std::shared_ptr<Producer> producer)
    {
        const int DEFAULT_SLEEP_TIME = 300;
        std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_SIZE);
        Element element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
        for (size_t i = 0; i < SEND_COUNT; i++) {
            Status rc = producer->Send(element);
            int retryCount = 30;
            while (rc.GetCode() == K_OUT_OF_MEMORY && retryCount-- > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_TIME));
                rc = producer->Send(element);
            }
            DS_ASSERT_OK(rc);
        }
    }

    void ReceiveHelper(std::shared_ptr<Consumer> consumer)
    {
        const int DEFAULT_RETRY_TIME = 100;
        Timer timer;
        std::vector<Element> outElements;
        int sendCount = SEND_COUNT;
        const int DEFAULT_WAIT_TIME = 1000;
        while (sendCount > 0 && timer.ElapsedSecond() < DEFAULT_RETRY_TIME) {
            DS_ASSERT_OK(consumer->Receive(1, DEFAULT_WAIT_TIME, outElements));
            if (!outElements.empty()) {
                DS_ASSERT_OK(consumer->Ack(outElements.back().id));
                sendCount -= outElements.size();
            }
        }
    }

    /**
     * @brief Adds 10 streams with producer/consumer in each one, test it,
     * and then ensures that all streams are functional by creating remote producers
     * and consumers
     * @param[in] streams The map of stream names to pairs of prod/cons
     * @param[in] remoteClient The remote client to use for producer
     */
    void PostScaleTest(
        std::map<std::string, std::pair<
        std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> &streams,
        std::shared_ptr<StreamClient> &remoteClient)
    {
        const int K_TEN = 10;
        int streamNum = streams.size();
        // Add 10 streams and producers/consumers after scale up
        for (int i = 0; i < K_TEN; ++i) {
            std::string streamName = "stream" + std::to_string(i + streamNum);
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
            std::shared_ptr<Consumer> consumer;
            SubscriptionConfig config("sub" + std::to_string(i + streamNum), SubscriptionType::STREAM);
            DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));
            streams.emplace(streamName, std::make_pair(producer, consumer));
            CheckCount(w1Client_, streamName, 1, 1);
        }

        // Make sure later requests get redirected and handled correctly
        for (auto &stream : streams) {
            const auto &streamName = stream.first;
            LOG(INFO) << "handle stream: " << streamName;

            // Add new remote producer and consumer after scale up/down
            std::shared_ptr<Producer> producer2;
            DS_ASSERT_OK(remoteClient->CreateProducer(streamName, producer2, defaultProducerConf_));

            std::shared_ptr<Consumer> consumer2;
            SubscriptionConfig config("remote_sub_" + streamName, SubscriptionType::STREAM);
            DS_ASSERT_OK(remoteClient->Subscribe(streamName, config, consumer2));
            if (w1Client_ == remoteClient) {
                // if both producers are created by same client/worker
                // then the producer count in master will be 1
                CheckCount(remoteClient, streamName, 1, K_TWO);
            } else {
                CheckCount(remoteClient, streamName, K_TWO, K_TWO);
            }
            auto &producer1 = stream.second.first;
            auto &consumer1 = stream.second.second;

            // Test Produce element
            TestSendRecv(producer1, consumer1);
            TestSendRecv(producer1, consumer2);
            TestSendRecv(producer2, consumer1);
            TestSendRecv(producer2, consumer2);

            // Close producer, consumer, and then delete the stream
            DS_ASSERT_OK(producer1->Close());
            DS_ASSERT_OK(consumer1->Close());
            DS_ASSERT_OK(producer2->Close());
            DS_ASSERT_OK(consumer2->Close());
            CheckCount(remoteClient, streamName, 0, 0);
            // We cant guarantee that Close consumer notification
            // would be finished before delete stream call
            // If notification is still slow DeleteStream will return
            // Stream is still in use
            Status rc = remoteClient->DeleteStream(streamName);
            if (rc.GetCode() != K_SC_STREAM_NOTIFICATION_PENDING) {
                DS_ASSERT_OK(rc);
            }
        }
    }

protected:

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

    int workerNum_ = 2;
    HostPort w1Addr_;
    HostPort w2Addr_;

    std::shared_ptr<StreamClient> w1Client_ = nullptr;
    std::shared_ptr<StreamClient> w2Client_ = nullptr;
    ProducerConf defaultProducerConf_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<EtcdStore> db_;
    int nodeTimeoutS_ = 3;
    int nodeDeadTimeoutS_ = 5;
    const size_t SEND_COUNT = 100000;
    const size_t TEST_SIZE = 1 * KB;
};

TEST_F(StreamClientScaleTest, TestConsumerCanRecvEleAfterScaleDown)
{
    // If the stream metadata is hashed to worker2, we can hit the target scenario. Therefore the use case needs to be
    // executed 10 times
    int streamNum = 10;
    int consumerRecvTimeoutMs = 10'000;

    AddNode();  // Now we have 3 nodes.
    std::vector<std::tuple<std::shared_ptr<Consumer>, std::shared_ptr<Producer>, std::shared_ptr<Producer>>> cache;
    cache.resize(streamNum);
    // Contruct element.
    const size_t sizeElement = 1 * KB;
    std::string writeElement = RandomData().GetRandomString(sizeElement);
    Element element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    // Init client in worker3.
    HostPort w3Addr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(2, w3Addr));  // 2 is the index of node.
    std::shared_ptr<StreamClient> w3Client;
    InitStreamClient(2, w3Client);   // 2 is the index of node.
    // Create producer/consumer -> send/recv one element -> close producer
    for (int i = 0; i < streamNum; ++i) {
        auto &consumer = std::get<0>(cache[i]);
        auto &producer1 = std::get<1>(cache[i]);
        auto streamName = "stream" + std::to_string(i);
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));
        DS_ASSERT_OK(w3Client->CreateProducer(streamName, producer1, defaultProducerConf_));
        DS_ASSERT_OK(producer1->Send(element));
        DS_ASSERT_OK(producer1->Close());
        std::vector<Element> eles;
        DS_ASSERT_OK(consumer->Receive(1, consumerRecvTimeoutMs, eles));
        ASSERT_EQ(eles.size(), 1);
    }
    // Scale down worker2
    w2Client_.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(nodeDeadTimeoutS_ + 1);
    // Consuemr can also recv ele success.
    for (int i = 0; i < streamNum; ++i) {
        auto &consumer = std::get<0>(cache[i]);
        auto &producer2 = std::get<2>(cache[i]);  // The index of producer2 is 2.
        auto streamName = "stream" + std::to_string(i);
        DS_ASSERT_OK(w3Client->CreateProducer(streamName, producer2, defaultProducerConf_));
        DS_ASSERT_OK(producer2->Send(element));
        DS_ASSERT_OK(producer2->Close());
        std::vector<Element> eles;
        DS_ASSERT_OK(consumer->Receive(1, consumerRecvTimeoutMs, eles));
        ASSERT_EQ(eles.size(), 1);
    }
}

TEST_F(StreamClientScaleTest, TestAutoDeleteStreamAfterScaleDown)
{
    // If the stream metadata is hashed to worker2, we can hit the target scenario. Therefore the use case needs to be
    // executed 10 times
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "SCNotifyWorkerManager.DeleteStreams", "return(K_RPC_UNAVAILABLE)"));
    int streamNum = 10;

    AddNode();  // Now we have 3 nodes.
    std::vector<std::tuple<std::shared_ptr<Consumer>, std::shared_ptr<Producer>, std::shared_ptr<Producer>>> cache;
    cache.resize(streamNum);

    std::shared_ptr<StreamClient> w3Client;
    InitStreamClient(2, w3Client); // index is 2
    // Create producer/consumer -> send/recv one element -> close producer
    defaultProducerConf_.autoCleanup = true;
    for (int i = 0; i < streamNum; ++i) {
        auto &consumer = std::get<0>(cache[i]);
        auto &producer1 = std::get<1>(cache[i]);
        auto streamName = "stream" + std::to_string(i);
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));
        DS_ASSERT_OK(w3Client->CreateProducer(streamName, producer1, defaultProducerConf_));
        DS_ASSERT_OK(producer1->Close());
        DS_ASSERT_OK(consumer->Close());
    }
    // Scale down worker2
    w2Client_.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(nodeDeadTimeoutS_ + 1);
    // Modify the stream configuration to confirm that the stream has been deleted
    defaultProducerConf_.maxStreamSize += 1;
    // Consuemr can also recv ele success.
    for (int i = 0; i < streamNum; ++i) {
        auto &producer2 = std::get<2>(cache[i]);  // The index of producer2 is 2.
        auto streamName = "stream" + std::to_string(i);
        DS_ASSERT_OK(w3Client->CreateProducer(streamName, producer2, defaultProducerConf_));
    }
}

TEST_F(StreamClientScaleTest, TestSimpleScaleUp)
{
    LOG(INFO) << "TestSimpleScaleUp start!";
    // Test the scale up and metadata migrate logic
    // In this case 7 streams will get metadata migrated at scale up,
    // including stream2, stream3, stream6, stream8, stream9, stream10, stream11

    // Initialize producers and consumers
    const int streamNum = 16;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testSimpleScaleUp";
    CreateNProducerAndConsumer(streams, streamNum, streamName);

    // Add new worker node to trigger scale up and metadata migration
    DS_ASSERT_OK(AddNode());

    // Make sure later requests get redirected and handled correctly
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        // Add new remote consumer after scale up
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
        DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, consumer));
        CheckCount(w2Client_, streamName, 1, K_TWO);

        auto &producer = stream.second.first;
        // Produce element
        TestSendRecv(producer, consumer);
        DS_ASSERT_OK(stream.second.first->Close());
        DS_ASSERT_OK(stream.second.second->Close());
        DS_ASSERT_OK(consumer->Close());
        CheckCount(w2Client_, streamName, 0, 0);
        DS_ASSERT_OK(w2Client_->DeleteStream(streamName));
    }
    LOG(INFO) << "TestSimpleScaleUp finish!";
}

TEST_F(StreamClientScaleTest, LEVEL1_TestScaleUpCrashWorker1)
{
    // Test the scale up and metadata migrate logic by closing one of the workers
    // Initialize producers and consumers
    const int streamNum = 16;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testScaleUpCrashWorker1";
    CreateNProducerAndConsumer(streams, streamNum, streamName);

    // Add new worker node to trigger scale up and metadata migration
    const int newWorkerIdx = workerNum_;
    DS_ASSERT_OK(AddNode());
    // Wait for scale up and migration done
    sleep(SCALE_UP_WAIT_TIME);
    std::shared_ptr<StreamClient> w3Client;
    InitStreamClient(newWorkerIdx, w3Client);

    // Kill one of the workers (instead of voluntary shutdown) to make sure that scale up is handled well
    std::set<std::string> STREAMS_ON_WORKER2 = { "stream2", "stream3",  "stream6", "stream8",
                                                 "stream9", "stream10", "stream11" };
    // close consumers and producers before kill so RPC_UNAVAILABLE is avoided at shutdown
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        if (STREAMS_ON_WORKER2.find(streamName) == STREAMS_ON_WORKER2.end()) {
            DS_ASSERT_OK(streams[streamName].first->Close());
            DS_ASSERT_OK(streams[streamName].second->Close());
        }
    }
    sleep(K_TWO);
    // Delete streams in seperate loop after to avoid running into pending notification failure
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        if (STREAMS_ON_WORKER2.find(streamName) == STREAMS_ON_WORKER2.end()) {
            DS_ASSERT_OK(w1Client_->DeleteStream(streamName));
        }
    }
    DS_ASSERT_OK(static_cast<ExternalCluster *>(cluster_.get())->KillWorker(1));

    // Make sure requests for migrated streams are still handled correctly after worker1 crashes
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        if (STREAMS_ON_WORKER2.find(streamName) == STREAMS_ON_WORKER2.end()) {
            continue;
        }
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w3Client->CreateProducer(streamName, producer, defaultProducerConf_));
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
        DS_ASSERT_OK(w3Client->Subscribe(streamName, config, consumer));
        CheckCount(w3Client, streamName, K_TWO, K_TWO);
        DS_ASSERT_OK(stream.second.first->Close());
        DS_ASSERT_OK(stream.second.second->Close());
        DS_ASSERT_OK(producer->Close());
        DS_ASSERT_OK(consumer->Close());
        CheckCount(w3Client, streamName, 0, 0);
    }
    sleep(K_TWO);
    // Delete streams in seperate loop after to avoid running into pending notification failure
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        if (STREAMS_ON_WORKER2.find(streamName) != STREAMS_ON_WORKER2.end()) {
            DS_ASSERT_OK(w3Client->DeleteStream(streamName));
        }
    }
}

TEST_F(StreamClientScaleTest, LEVEL2_TestScaleUpCrashWorker2)
{
    LOG(INFO) << "TestScaleUpCrashWorker2 start!";
    // Test the scale up and metadata migrate logic after the new node crash and restarts
    // Essentially the purpose is to make sure it can recover itself from rocksdb
    // Initialize producers and consumers
    const int streamNum = 5;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testScaleUpCraskWorker2";
    CreateNProducerAndConsumer(streams, streamNum, streamName);

    // Add new worker node to trigger scale up and metadata migration
    const int newWorkerIdx = workerNum_;
    DS_ASSERT_OK(AddNode());
    // Wait for scale up and migration done
    sleep(SCALE_UP_WAIT_TIME);
    // Use kill so it is not voluntary scale down
    DS_ASSERT_OK(static_cast<ExternalCluster *>(cluster_.get())->KillWorker(newWorkerIdx));
    // Wait for the process kill so it does not timeout after 60s on GcovFlush
    sleep(1);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, newWorkerIdx, {}));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, newWorkerIdx));
    std::shared_ptr<StreamClient> w3Client;
    InitStreamClient(newWorkerIdx, w3Client);
    // Make sure requests are still handled correctly after worker2 crash and restart
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w3Client->CreateProducer(streamName, producer, defaultProducerConf_));
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
        DS_ASSERT_OK(w3Client->Subscribe(streamName, config, consumer));
        CheckCount(w3Client, streamName, K_TWO, K_TWO);
        DS_ASSERT_OK(stream.second.first->Close());
        DS_ASSERT_OK(stream.second.second->Close());
        CheckCount(w3Client, streamName, 1, 1);
        DS_ASSERT_OK(producer->Close());
        DS_ASSERT_OK(consumer->Close());
        CheckCount(w3Client, streamName, 0, 0);
    }
    sleep(K_TWO);
    // Delete streams in seperate loop after to avoid running into pending notification failure
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        DS_ASSERT_OK(w3Client->DeleteStream(streamName));
    }
    // Shutdown the new node to avoid problem caused by kill with signal 9
    (void)cluster_->ShutdownNode(WORKER, newWorkerIdx);
    LOG(INFO) << "TestScaleUpCrashWorker2 finish!";
}

TEST_F(StreamClientScaleTest, LEVEL2_TestVoluntaryScaleDown)
{
    LOG(INFO) << "TestVoluntaryScaleDown start!";
    // Test the voluntary scale down and the related metadata migrate logic
    // In this case after worker2 shuts down, worker1 should be able to take over all the streams
    // Test will take around a minute due to the 16 streams being created

    // Initialize 16 producers and consumers
    const int streamNum = 16;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testVoluntaryScaleDown";
    CreateNProducerAndConsumer(streams, streamNum, streamName);

    // Shutdown worker2 to trigger voluntary scale down and metadata migration
    w2Client_.reset();
    VoluntaryScaleDownInject(1);
    // Wait for voluntary scale down to finish
    sleep(SCALE_DOWN_WAIT_TIME);

    PostScaleTest(streams, w1Client_);
    LOG(INFO) << "TestVoluntaryScaleDown finish!";
}

TEST_F(StreamClientScaleTest, LEVEL1_TestScaleDownAutoDeleteStream1)
{
    std::string streamName = "testScaleDownAutoDelStream";
    defaultProducerConf_.autoCleanup = true;
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, consumer));

    // Shutdown worker 1
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(K_TWO);

    // Close producer to invoke auto delete
    DS_ASSERT_OK(producer->Close());
    sleep(K_TWO);
    // Try to create a producer with different configs to test if stream was deleted, and can be recreated
    std::shared_ptr<Producer> producer1;
    defaultProducerConf_.autoCleanup = false;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer1, defaultProducerConf_));
}

TEST_F(StreamClientScaleTest, LEVEL2_TestScaleDownWhileRetainingData)
{
    int streamNum = 10;
    std::string streamNameBase = "testScaleDownAutoDelStream";
    defaultProducerConf_.autoCleanup = false;
    defaultProducerConf_.retainForNumConsumers = 1;

    for (int i = 0; i < streamNum; i++) {
        auto streamName = streamNameBase + std::to_string(i);
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
        DS_ASSERT_OK(producer->Close());
    }

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RunKeepAliveTask", "return(K_RPC_UNAVAILABLE)"));

    WaitAllNodesJoinIntoHashRing(1);

    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->StartWorkerAndWaitReady({0}));

    WaitAllNodesJoinIntoHashRing(2);  // 2 workers online

    for (int i = 0; i < streamNum; i++) {
        auto streamName = streamNameBase + std::to_string(i);
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, consumer));
    }
}

TEST_F(StreamClientScaleTest, DISABLED_LEVEL1_TestScaleDownDeleteStream)
{
    LOG(INFO) << "TestScaleDownDeleteStream start!";
    // Test that the related node info is kept for DeleteStream cleanup purposes
    // First shutdown worker2
    w2Client_.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(nodeDeadTimeoutS_ + 1);

    // Initialize producers and consumers
    // Less streams to have the testcase take shorter time
    const int streamNum = 3;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testScaleDownDelStream";
    CreateNProducerAndConsumer(streams, streamNum, streamName);
    for (auto &stream : streams) {
        DS_ASSERT_OK(stream.second.first->Close());
        DS_ASSERT_OK(stream.second.second->Close());
        CheckCount(w1Client_, stream.first, 0, 0);
    }

    // And then restart worker2 and scale down worker1, so all metadata gets migrated to worker2
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, {}));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    sleep(SCALE_UP_WAIT_TIME);

    InitStreamClient(1, w2Client_);
    VoluntaryScaleDownInject(0);
    sleep(SCALE_DOWN_WAIT_TIME);

    // Nodes that got shutdown will be detected and ignored so delete stream should be OK
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        CheckCount(w2Client_, streamName, 0, 0);
        DS_ASSERT_OK(w2Client_->DeleteStream(streamName));
    }
    LOG(INFO) << "TestScaleDownDeleteStream finish!";
}

TEST_F(StreamClientScaleTest, LEVEL1_TestScaleUpAndDown)
{
    LOG(INFO) << "LEVEL1_TestScaleUpAndDown start!";
    // Test a mixture of scale up and down operations
    // Initialize producers and consumers
    const int streamNum = 16;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testScaleUpAndDown";
    CreateNProducerAndConsumer(streams, streamNum, streamName);

    // A mixture of operations with enough time in between:
    // Add new worker node to trigger scale up and metadata migration
    // Scale down and Restart worker1
    DS_ASSERT_OK(AddNode());
    WaitAllNodesJoinIntoHashRing(3); // 3 workers online
    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    w1Client_.reset();
    w2Client_.reset();
    for (auto &stream : streams) {
        stream.second = { nullptr, nullptr };
    }
    WaitAllNodesJoinIntoHashRing(1); // 1 workers online
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, {}));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, {}));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    WaitAllNodesJoinIntoHashRing(3); // 3 workers online

    InitStreamClient(0, w1Client_);
    InitStreamClient(1, w2Client_);
    // Make sure all requests can still be handled correctly
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        LOG(INFO) << "Processing stream: " << streamName;
        // Producers and consumers related to worker1 got cleaned up at scale down
        CheckCount(w1Client_, streamName, 0, 0);
        // Add new producer and consumer
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
        DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));
        CheckCount(w1Client_, streamName, 1, 1);
        DS_ASSERT_OK(producer->Close());
        DS_ASSERT_OK(consumer->Close());
        CheckCount(w1Client_, streamName, 0, 0);
        DS_ASSERT_OK(w1Client_->DeleteStream(streamName));
    }
    LOG(INFO) << "LEVEL1_TestScaleUpAndDown finish!";
}


TEST_F(StreamClientScaleTest, DISABLED_LEVEL1_TestScaleDownProducerCount)
{
    LOG(INFO) << "LEVEL1_TestScaleDownProducerCount start!";
    // A simplified version of StreamClientScaleTest.TestScaleUpAndDown to monitor the producer count after migration.
    // Initialize producers and consumers
    const int streamNum = 16;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "TestScaleDownProducerCount";
    CreateNProducerAndConsumer(streams, streamNum, streamName);

    // A mixture of operations with enough time in between:
    // Add new worker node to trigger scale up and metadata migration
    // Scale down and Restart worker1
    DS_ASSERT_OK(AddNode());
    WaitAllNodesJoinIntoHashRing(3);  // 3 workers online
    VoluntaryScaleDownInject(0);
    w1Client_.reset();
    for (auto &stream : streams) {
        stream.second = { nullptr, nullptr };
    }
    WaitAllNodesJoinIntoHashRing(2);  // 2 workers online

    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        LOG(INFO) << "Processing stream: " << streamName;
        // Producers and consumers related to worker1 got cleaned up at scale down
        CheckCount(w2Client_, streamName, 0, 0);
    }
}

TEST_F(StreamClientScaleTest, DISABLED_LEVEL1_TestLargeScaleUp)
{
    LOG(INFO) << "LEVEL1_TestLargeScaleUp start!";
    // Test the scale up and metadata migrate logic, with opening new streams and
    // testing sending data across and on different nodes
    // Test will take around a minute due to the 100 streams being created

    // Initialize 90 producers and consumers
    const int streamNum = 90;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testLargeScaleUp";
    CreateNProducerAndConsumer(streams, streamNum, streamName);

    // Add new worker node to trigger scale up and metadata migration
    DS_ASSERT_OK(AddNode());

    PostScaleTest(streams, w2Client_);
    LOG(INFO) << "LEVEL1_TestLargeScaleUp finish!";
}

TEST_F(StreamClientScaleTest, LEVEL2_TestUnlimitedAutodelete1)
{
    // Test that node lost etcd event will clear metadata, so auto-delete is not retried when related node is lost.
    std::string streamName = "TestUnlimitedAutodelete1";
    ProducerConf oldConf;
    oldConf.autoCleanup = true;
    oldConf.pageSize = 4 * MB;  //  page size is 4 MB
    ProducerConf newConf;
    newConf.pageSize = 3 * MB;  // page size is 3 MB

    std::vector<std::string> streamVec;
    std::vector<std::shared_ptr<Producer>> producerVec;
    const int streamNum = 10;
    for (int i = 0; i < streamNum; i++) {
        auto tmpStreamName = streamName + std::to_string(i);

        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w1Client_->CreateProducer(tmpStreamName, producer, oldConf));
        producerVec.emplace_back(producer);
        streamVec.emplace_back(tmpStreamName);
    }

    // Close all producers, but do not handle auto-delete yet.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "master.ProcessDeleteStreams", "1*sleep(10000)"));
    for (auto &producer : producerVec) {
        producer->Close();
    }

    // Shutdown worker 0
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    const int WAIT_FOR_DELETION = 15;
    sleep(WAIT_FOR_DELETION);

    for (auto streamName : streamVec) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w2Client_->CreateProducer(streamName, producer, newConf));
    }
}

TEST_F(StreamClientScaleTest, LEVEL2_TestUnlimitedAutodelete2)
{
    // Test that even if metadata is not cleared, auto-delete will not be retried indefinitely when node is found lost.
    std::string streamName = "TestUnlimitedAutodelete2";
    ProducerConf oldConf;
    oldConf.autoCleanup = true;
    oldConf.pageSize = 4 * MB;  //  page size is 4 MB
    ProducerConf newConf;
    newConf.pageSize = 3 * MB;  // page size is 3 MB

    std::vector<std::string> streamVec;
    std::vector<std::shared_ptr<Producer>> producerVec;
    const int streamNum = 10;
    for (int i = 0; i < streamNum; i++) {
        auto tmpStreamName = streamName + std::to_string(i);

        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w1Client_->CreateProducer(tmpStreamName, producer, oldConf));
        producerVec.emplace_back(producer);
        streamVec.emplace_back(tmpStreamName);
    }

    // Close all producers, but do not handle auto-delete yet.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "master.ProcessDeleteStreams", "1*sleep(10000)"));
    for (auto &producer : producerVec) {
        producer->Close();
    }

    // Shutdown worker 0, also inject to simulate the scenario that metadata is not cleared.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "SCMetadataManager.SkipClearEmptyMeta", "call()"));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    const int WAIT_FOR_DELETION = 15;
    sleep(WAIT_FOR_DELETION);

    for (auto streamName : streamVec) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(w2Client_->CreateProducer(streamName, producer, newConf));
    }
}

TEST_F(StreamClientScaleTest, LEVEL1_ScaleWhenSyncConsumerNode)
{
    int streamNum = 10;
    std::vector<std::string> streams;
    std::vector<std::shared_ptr<Consumer>> consumers;
    for (int i = 0; i < streamNum; i++) {
        std::string streamName = RandomData().GetRandomString(10);  // stream name len is 10
        streams.emplace_back(streamName);
        SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, consumer));
        consumers.emplace_back(consumer);
    }
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "MasterWorkerSCServiceImpl.UpdateTopoNotification.begin", "sleep(15000)"));

    const int threadNum = 20;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futs;
    auto createProducer = [this](std::string stream) {
        std::shared_ptr<Producer> producer;
        w1Client_->CreateProducer(stream, producer, defaultProducerConf_);
    };
    auto closeConsumer = [](std::shared_ptr<Consumer> consumer) {
        sleep(1);
        DS_ASSERT_OK(consumer->Close());
    };
    for (const auto &stream : streams) {
        futs.emplace_back(threadPool.Submit(createProducer, stream));
    }
    for (const auto &consumer : consumers) {
        futs.emplace_back(threadPool.Submit(closeConsumer, consumer));
    }
    sleep(1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "SCMetadataManager.GetMetasMatch.timeout", "call(5)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "SCMetadataManager.GetMetasMatch.timeout", "call(5)"));
    AddNode();
    WaitAllNodesJoinIntoHashRing(3, 10); // wait 10s for worker 3 join
    for (const auto &fut : futs) {
        fut.wait();
    }
}

TEST_F(StreamClientScaleTest, LEVEL1_ContinuousRedirection)
{
    std::vector<std::shared_ptr<Consumer>> consumers;
    DS_ASSERT_OK(AddNode());
    int worker3Idx = 2;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker3Idx, "SCMetadataManager.Subscribe.wait", "2*sleep(3000)"));
    const int threadNum = 2;
    ThreadPool threadPool(threadNum);
    auto fut1 = threadPool.Submit([this, &consumers] () {
        int streamNum = 16;
        for (int i = 0; i < streamNum; i++) {
            std::string streamName = RandomData().GetRandomString(10);  // stream name len is 10
            SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
            std::shared_ptr<Consumer> consumer;
            DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));
            consumers.emplace_back(consumer);
        }
    });

    auto fut2 = threadPool.Submit([this, worker3Idx]() {
        VoluntaryScaleDownInject(worker3Idx);
        WaitAllNodesJoinIntoHashRing(2, 20);  // wait 20s for w3 scale down
    });

    fut1.wait();
    fut2.wait();

    for (const auto &consumer : consumers) {
        DS_ASSERT_OK(consumer->Close());
    }
}

class StreamClientPassiveScaleTest : public StreamClientScaleTest, public CommonDistributedExt {
public:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        // Start 3 workers
        opts.numWorkers = ++workerNum_;
        opts.enableDistributedMaster = "true";
        opts.vLogLevel = 1;
        // Set up node_dead_timeout_s and auto_del_dead_node flags, so that a new meta owner master can be reselected
        // for passive scale down purposes
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=3 -node_dead_timeout_s=%d -auto_del_dead_node=true -shared_memory_size_mb=10240",
            NODE_DEAD_TIMEOUT);
        SCClientCommon::SetClusterSetupOptions(opts);
    }
};

TEST_F(StreamClientPassiveScaleTest, DISABLED_TestRestartDuringEtcdCrash)
{
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->ShutdownEtcds());

    std::vector<uint8_t> writeElement = RandomData().RandomBytes(TEST_SIZE);
    Element element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    int receiveMaxWaitTimeMs = 10'000;
    std::vector<Element> outElements;

    auto streamNamePerRestart = "streamPerRestart";
    std::shared_ptr<Consumer> consumerPerRestart;
    SubscriptionConfig configPerRestart("subPerRestart", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamNamePerRestart, configPerRestart, consumerPerRestart));
    std::shared_ptr<Producer> producerPerRestart;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamNamePerRestart, producerPerRestart, defaultProducerConf_));

    DS_ASSERT_OK(producerPerRestart->Send(element));
    DS_ASSERT_OK(consumerPerRestart->Receive(1, receiveMaxWaitTimeMs, outElements));

    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 1 }));
    std::shared_ptr<StreamClient> w22Client;
    InitStreamClient(1, w22Client);

    auto streamNamePostRestart = "streamPostRestart";
    std::shared_ptr<Consumer> consumerPostRestart;
    SubscriptionConfig configPostRestart("subPostRestart", SubscriptionType::STREAM);
    DS_ASSERT_OK(w22Client->Subscribe(streamNamePostRestart, configPostRestart, consumerPostRestart));
    std::shared_ptr<Producer> producerPostRestart;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamNamePostRestart, producerPostRestart, defaultProducerConf_));

    DS_ASSERT_OK(producerPostRestart->Send(element));
    DS_ASSERT_OK(consumerPostRestart->Receive(1, receiveMaxWaitTimeMs, outElements));

    DS_ASSERT_OK(externalCluster->SetInjectAction(WORKER, 1, "WorkerOCServiceImpl.Reconciliation.SkipWait", "call()"));
    DS_ASSERT_OK(externalCluster->StartEtcdCluster());

    int waitReconciliationSec = 5;
    sleep(waitReconciliationSec);

    DS_ASSERT_OK(producerPerRestart->Send(element));
    DS_ASSERT_NOT_OK(consumerPerRestart->Receive(1, receiveMaxWaitTimeMs, outElements));

    DS_ASSERT_OK(producerPostRestart->Send(element));
    DS_ASSERT_OK(consumerPostRestart->Receive(1, receiveMaxWaitTimeMs, outElements));
}

TEST_F(StreamClientPassiveScaleTest, DISABLED_LEVEL1_TestPassiveScaleDown)
{
    LOG(INFO) << "LEVEL1_TestPassiveScaleDown start!";
    // Test passive scale down, that is other worker can take over and recover the metadata upon node crash

    // Use the worker3 for crash purpose
    const int worker3Index = 2;
    // Initialize 40 streams with producers and consumers, total 50
    const int streamNum = 40;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testPassiveScaleDown";
    // Create Producer on worker1 and Consumer on worker2
    CreateNProducerAndConsumer(streams, streamNum, streamName, false);

    // Kill worker3, and sleep to trigger passive scale down logic
    DS_ASSERT_OK(static_cast<ExternalCluster *>(cluster_.get())->KillWorker(worker3Index));
    sleep(NODE_DEAD_TIMEOUT + 1);

    // First verify that the existing producer->consumer can still go through
    // This does not involve master logic
    for (auto &stream : streams) {
        CheckCount(w1Client_, stream.first, 1, 1);
        TestSendRecv(stream.second.first, stream.second.second);
    }

    // Add a new worker4 for new consumer
    const int worker4Index = workerNum_;
    DS_ASSERT_OK(AddNode());
    std::shared_ptr<StreamClient> w4Client;
    InitStreamClient(worker4Index, w4Client);

    PostScaleTest(streams, w2Client_);

    LOG(INFO) << "LEVEL1_TestPassiveScaleDown finish!";
}

TEST_F(StreamClientPassiveScaleTest, LEVEL1_TestRestartPassiveScaleDown)
{
    LOG(INFO) << "TestRestartPassiveScaleDown start!";
    // Test when the passive scale down node gets restarted
    // It starts with passive scale down, and then a mixture of reconciliation and scale up metadata migration
    // In this case K_DUPLICATED is ignored at metadata migration, so it will not retry indefinitely

    // Use the worker3 for crash purpose
    const int worker3Index = 2;
    // Initialize producers and consumers
    const int streamNum = 16;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testRestartPassiveScaleDown";
    // Create Producer on worker1 and Consumer on worker2
    CreateNProducerAndConsumer(streams, streamNum, streamName, false);

    // Kill worker3, and sleep to trigger passive scale down logic
    DS_ASSERT_OK(static_cast<ExternalCluster *>(cluster_.get())->KillWorker(worker3Index));
    sleep(NODE_DEAD_TIMEOUT + 1);
    // Then restart worker3 to trigger scale up metadata migration logic
    DS_ASSERT_OK(cluster_->StartNode(WORKER, worker3Index, {}));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, worker3Index));
    std::shared_ptr<StreamClient> w3Client;
    InitStreamClient(worker3Index, w3Client);

    // Make sure requests can still be handled correctly for the new Consumer
    for (auto &stream : streams) {
        const auto &streamName = stream.first;
        auto &producer = stream.second.first;
        LOG(INFO) << "handle stream: " << streamName;
        // Add new consumer on worker3
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
        DS_ASSERT_OK(w3Client->Subscribe(streamName, config, consumer));
        CheckCount(w3Client, streamName, 1, K_TWO);

        // Make sure producer -> consumer can go through for the new consumer on worker4
        // This involves master related topo change logic
        TestSendRecv(producer, consumer);

        DS_ASSERT_OK(stream.second.first->Close());
        DS_ASSERT_OK(stream.second.second->Close());
        DS_ASSERT_OK(consumer->Close());
        CheckCount(w3Client, streamName, 0, 0);
        DS_ASSERT_OK(w3Client->DeleteStream(streamName));
    }
    // Shutdown the worker3 to avoid problem caused by kill with signal 9
    (void)cluster_->ShutdownNode(WORKER, worker3Index);
    LOG(INFO) << "TestRestartPassiveScaleDown finish!";
}

class StreamClientVoluntaryScaleDownTest : public StreamClientScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        // Start 3 workers
        opts.numWorkers = ++workerNum_;
        opts.enableDistributedMaster = "true";
        // Set up node_dead_timeout_s and auto_del_dead_node flags, so that a new meta owner master can be reselected
        // for passive scale down purposes
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=3 -node_dead_timeout_s=%d -auto_del_dead_node=true -shared_memory_size_mb=10240"
            " -log_monitor=true -log_monitor_interval_ms=1000 "
            "-sc_metrics_log_interval_s=1",
            NODE_DEAD_TIMEOUT);
        opts.skipWorkerPreShutdown = false;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

    void SetUp() override
    {
        StreamClientScaleTest::SetUp();
        defaultProducerConf_.maxStreamSize = TEST_STREAM_SIZE << 1;
        InitTestEtcdInstance();
    }

    void GetHashOnWorker(int workerIndex, int64_t &hash)
    {
        hash = -1;
        ASSERT_NE(db_, nullptr) << "The etcd store instance is not initialized";
        HostPort workerAddr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddr));
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ring.ParseFromString(value);
        auto tokens = ring.workers().at(workerAddr.ToString()).hash_tokens();
        ASSERT_GT(tokens.size(), 1) << "A node should have multiple tokens";
        hash = tokens[0] != 0 ? tokens[0] - 1 : tokens[1] - 1;
    }

    void InitClientsHelper()
    {
        InitStreamClient(0, w1Client_);
        InitStreamClient(1, w2Client_);
    }
};

// If the node to be scaled in has data to be sent by the Producer, the node exits only after the data is sent.
TEST_F(StreamClientVoluntaryScaleDownTest, LEVEL1_TestVoluntaryScaleDownWithUnfinishedTask1)
{
    DS_ASSERT_OK(inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)"));
    // The producer is on worker3 and the consumer is on worker1.
    const int worker3Index = 2;
    std::shared_ptr<StreamClient> w3Client;
    InitStreamClient(worker3Index, w3Client);

    std::string streamName = "testVolScaleDownUnfinishedTask1";
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(w3Client->CreateProducer(streamName, producer, defaultProducerConf_));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));
    CheckCount(w1Client_, streamName, 1, 1);

    // This injection will result in the producer being slower to push data to the remote worker.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker3Index, "ExclusivePageQueue.ScanAndEval", "sleep(3000)"));

    std::string eleContent = "hello";
    Element element(reinterpret_cast<uint8_t *>(&eleContent.front()), eleContent.size(), ULONG_MAX);
    int totalEleNum = 3;
    ThreadPool threadPool(1);
    auto producerFuture = threadPool.Submit([&] {
        for (int i = 0; i < totalEleNum; i++) {
            RETURN_IF_NOT_OK(producer->Send(element));
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return Status::OK();
    });
    // Shutdown worker3 to trigger voluntary scale down.
    VoluntaryScaleDownInject(worker3Index);
    DS_ASSERT_OK(producerFuture.get());
    producer.reset();
    w3Client.reset();
    // Wait for voluntary scale down to finish
    WaitForVoluntaryDownFinished(worker3Index);
    // Consumer can receive all of the data.
    std::vector<Element> outElements;
    uint32_t timeOutMs = 2'000;
    DS_ASSERT_OK(consumer->Receive(totalEleNum, timeOutMs, outElements));
    ASSERT_EQ(outElements.size(), totalEleNum);
    for (auto ele : outElements) {
        std::string actualData(reinterpret_cast<char *>(ele.ptr), ele.size);
        ASSERT_EQ(actualData, eleContent);
    }
}

TEST_F(StreamClientVoluntaryScaleDownTest, TestVoluntaryScaleDownWithTasksShouldBeDiscarded)
{
    std::string streamName = "testVoluntaryScaleDown";
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));

    std::shared_ptr<Producer> producer;
    ProducerConf pConf;
    pConf.autoCleanup = true;
    DS_ASSERT_OK(w2Client_->CreateProducer(streamName, producer, pConf));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "BufferPool.BatchAsyncFlush", "return(K_OK)"));

    std::string eleContent = "hello";
    Element element(reinterpret_cast<uint8_t *>(&eleContent.front()), eleContent.size());
    DS_ASSERT_OK(producer->Send(element));

    sleep(1);  // wait scan ele success

    consumer->Close();
    producer->Close();

    DS_ASSERT_OK(w2Client_->ShutDown());

    VoluntaryScaleDownInject(1);

    // Wait for voluntary scale down to finish
    WaitForVoluntaryDownFinished(1);

    w1Client_.reset();
    w2Client_.reset();
}

TEST_F(StreamClientVoluntaryScaleDownTest, LEVEL1_TestScaleDownAutoDeleteStream2)
{
    InitClientsHelper();
    // Test that with voluntary shutdown, pending auto-delete can be handled after migration.
    std::string streamName = "testScaleDownAutoDelStream2";
    // Inject so that auto delete cannot finish on worker1.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "master.ProcessDeleteStreams", "sleep(10000)"));
    defaultProducerConf_.autoCleanup = true;
    const int streamNum = 10;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    CreateNProducerAndConsumer(streams, streamNum, streamName);
    // Close producers and consumers so auto delete can be triggered.
    for (auto &stream : streams) {
        DS_ASSERT_OK(stream.second.first->Close());
        DS_ASSERT_OK(stream.second.second->Close());
        CheckCount(w1Client_, stream.first, 0, 0);
    }
    streams.clear();
    // Voluntarily scale down worker 1, the pending auto delete should be initiated from the new meta owner master.
    VoluntaryScaleDownInject(1);
    sleep(SCALE_DOWN_WAIT_TIME);
    // Try to create a producer with different configs to test if stream was deleted, and can be recreated.
    defaultProducerConf_.autoCleanup = false;
    CreateNProducerAndConsumer(streams, streamNum, streamName);
}

TEST_F(StreamClientVoluntaryScaleDownTest, LEVEL2_TestScaleDownNotifications1)
{
    InitClientsHelper();
    // Test that with voluntary shutdown, add pub and add sub notifications can be readded.
    // And also the stop data retention notification is also involved.
    std::string streamName = "testScaleDownNotifications1";
    defaultProducerConf_.retainForNumConsumers = 1;
    // Inject so that notifications all become async on worker3, and async notifications are not handled.
    const int worker3Index = 2;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, worker3Index, "SCNotifyWorkerManager.ForceAsyncNotification", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker3Index, "master.ProcessAsyncNotify", "sleep(10000)"));
    const int streamNum = 10;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    CreateNProducerAndConsumer(streams, streamNum, streamName, false);
    // Voluntarily scale down worker3, metadata will get migrated and notification/reconciliation logic should be triggered.
    VoluntaryScaleDownInject(worker3Index);
    sleep(SCALE_DOWN_WAIT_TIME);
    for (auto &stream : streams) {
        CheckCount(w1Client_, stream.first, 1, 1);
        TestSendRecv(stream.second.first, stream.second.second);
    }
}

TEST_F(StreamClientVoluntaryScaleDownTest, LEVEL2_TestScaleDownNotifications2)
{
    InitClientsHelper();
    // Test that with voluntary shutdown, del pub and del sub notifications can be readded.
    std::string streamName = "testScaleDownNotifications2";
    const int streamNum = 6;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    CreateNProducerAndConsumer(streams, streamNum, streamName, false);
    // Inject so that notifications all become async on worker3, and async notifications are not handled.
    const int worker3Index = 2;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, worker3Index, "SCNotifyWorkerManager.ForceAsyncNotification", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker3Index, "master.ProcessAsyncNotify", "sleep(10000)"));
    for (auto &stream : streams) {
        // Close the remote consumer, but the notifications on worker3 are not handled.
        stream.second.second->Close();
    }
    // Voluntarily scale down worker3,
    // metadata will get migrated and notification/reconciliation logic should be triggered.
    VoluntaryScaleDownInject(worker3Index);
    sleep(SCALE_DOWN_WAIT_TIME);
    // Force worker2 to fail to accept any elements with RPC_UNAVAILABLE.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "PushElementsCursors.begin", "return(K_RPC_UNAVAILABLE)"));
    for (auto &stream : streams) {
        auto &streamName = stream.first;
        auto &producer = stream.second.first;
        auto &consumer = stream.second.second;
        SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
        DS_ASSERT_OK(w1Client_->Subscribe(streamName, config, consumer));
        // Send data of amound more than max stream size, to make sure the procedure is fine.
        std::thread producerThrd([this, &producer]() { SendHelper(producer); });
        std::thread consumerThrd([this, &consumer]() { ReceiveHelper(consumer); });
        producerThrd.join();
        consumerThrd.join();
    }
}

TEST_F(StreamClientVoluntaryScaleDownTest, TestScaleDownNotifications3)
{
    InitClientsHelper();
    // Test that with voluntary shutdown, life time consumer count is correctly maintained,
    // so stop retain notification is correctly generated by new consumer.
    std::string streamName = "testScaleDownNotifications1";
    defaultProducerConf_.retainForNumConsumers = 2;
    const int worker3Index = 2;
    const int streamNum = 10;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    CreateNProducerAndConsumer(streams, streamNum, streamName, false);
    for (auto &stream : streams) {
        DS_ASSERT_OK(stream.second.second->Close());
    }
    // Voluntarily scale down worker3,
    // metadata will get migrated and notification/reconciliation logic should be triggered.
    VoluntaryScaleDownInject(worker3Index);
    sleep(SCALE_DOWN_WAIT_TIME);
    for (auto &stream : streams) {
        auto &streamName = stream.first;
        SubscriptionConfig config("sub_" + streamName, SubscriptionType::STREAM);
        DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, stream.second.second));
        CheckCount(w2Client_, stream.first, 1, 1);
        TestSendRecv(stream.second.first, stream.second.second);
    }
}

TEST_F(StreamClientVoluntaryScaleDownTest, LEVEL1_ScaleDownWhenMetaResidue)
{
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "SCMetadataManager.CreateStreamMetadata",
                                  "1*call(stream_residue_1)->1*call(stream_residue_2)->1*call(stream_residue_3)"));
    const int streamNum = 16;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testScaleDownWhenMetaResidue";
    CreateNProducerAndConsumer(streams, streamNum, streamName);

    w1Client_->ShutDown();
    VoluntaryScaleDownInject(0);
    int timeoutS = 20;
    WaitForVoluntaryDownFinished(0, timeoutS);
}

TEST_F(StreamClientPassiveScaleTest, LEVEL2_TestSyncConsumerNode)
{
    // Test that during the reconciliation from passive scale down,
    // the SyncConsumerNode would skip the duplicates
    // Use the worker3 for crash purpose
    const int worker3Index = 2;
    // Initialize producers and consumers
    const int streamNum = 10;
    struct PlaceHolder {
        PlaceHolder(std::shared_ptr<Producer> producer, std::shared_ptr<Consumer> consumer,
                    std::shared_ptr<std::thread> producerThrd, std::shared_ptr<std::thread> consumerThrd)
            : producer_(producer), consumer_(consumer), producerThrd_(producerThrd), consumerThrd_(consumerThrd)
        {
        }
        std::shared_ptr<Producer> producer_;
        std::shared_ptr<Consumer> consumer_;
        std::shared_ptr<std::thread> producerThrd_;
        std::shared_ptr<std::thread> consumerThrd_;
    };
    std::map<std::string, PlaceHolder> streams;

    // Create Producer on worker1 and Consumer on worker2
    for (int i = 0; i < streamNum; ++i) {
        std::string streamName = "testSyncConNode" + std::to_string(i);
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        const int TEST_PAGE_SIZE = 16 * KB;
        const int TEST_STREAM_MAX_SIZE = 5 * MB;
        conf.pageSize = TEST_PAGE_SIZE;
        conf.maxStreamSize = TEST_STREAM_MAX_SIZE;
        DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub" + std::to_string(i), SubscriptionType::STREAM);
        DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, consumer));
        auto producerThrd = std::make_shared<std::thread>([this, producer]() { SendHelper(producer); });
        auto consumerThrd = std::make_shared<std::thread>([this, consumer]() { ReceiveHelper(consumer); });
        streams.emplace(streamName, PlaceHolder(producer, consumer, producerThrd, consumerThrd));
        CheckCount(w1Client_, streamName, 1, 1);
    }

    // Kill worker3, and sleep to trigger passive scale down logic
    DS_ASSERT_OK(static_cast<ExternalCluster *>(cluster_.get())->KillWorker(worker3Index));
    sleep(NODE_DEAD_TIMEOUT + 1);

    // Make sure requests can still be handled correctly between producer and consumer
    for (auto &stream : streams) {
        stream.second.producerThrd_->join();
        stream.second.consumerThrd_->join();
    }
}

TEST_F(StreamClientPassiveScaleTest, LEVEL1_TestClearAsyncNotifyTask)
{
    ObtainHashTokens();
    std::string streamName;
    int masterIndex = 2;
    int timeoutSec = 10;
    Timer timer;
    while (timer.ElapsedSecond() < timeoutSec) {
        std::string tmpStreamName = "stream-" + GetStringUuid();
        WorkerEntry masterEntry;
        GetMetaLocationById(tmpStreamName, { 0, 1, 2 }, masterEntry);
        if (masterEntry.index == masterIndex) {
            streamName = tmpStreamName;
            break;
        }
    }
    ASSERT_TRUE(!streamName.empty());
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    const int TEST_PAGE_SIZE = 16 * KB;
    const int TEST_STREAM_MAX_SIZE = 5 * MB;
    conf.pageSize = TEST_PAGE_SIZE;
    conf.maxStreamSize = TEST_STREAM_MAX_SIZE;
    DS_ASSERT_OK(w1Client_->CreateProducer(streamName, producer, defaultProducerConf_));

    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub", SubscriptionType::STREAM);
    DS_ASSERT_OK(w2Client_->Subscribe(streamName, config, consumer));

    // Kill worker1,
    DS_ASSERT_OK(static_cast<ExternalCluster *>(cluster_.get())->KillWorker(0));
    DS_ASSERT_OK(consumer->Close());
    sleep(NODE_DEAD_TIMEOUT + 1);
    // sleep to trigger passive scale down logic
    DS_ASSERT_OK(w2Client_->DeleteStream(streamName));
}

class DataVerificationStreamClientScaleTest : public StreamClientScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        StreamClientScaleTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_stream_data_verification=true ";
    }

    void CreateStreams(uint numOfStream, uint producerPerStream, uint consumerPerStream,
        std::vector<std::pair<std::vector<std::shared_ptr<Producer>>,
        std::vector<std::shared_ptr<Consumer>>>> &streams, std::string streamName)
    {
        streams.resize(numOfStream);
        CreateNProducerAndMConsumerForEachStream(producerPerStream, consumerPerStream, streams,
                                                 streamName);
    }

    void CreateNProducerAndMConsumerForEachStream(uint producerPerStream, uint consumerPerStream,
        std::vector<std::pair<std::vector<std::shared_ptr<Producer>>,
        std::vector<std::shared_ptr<Consumer>>>> &streams,
        std::string streamName)
    {
        for (uint i = 0; i < streams.size(); ++i) {
            std::string streamName_ = streamName + std::to_string(i);
            auto &producers = streams.at(i).first;
            for (uint j = 0; j < producerPerStream; ++j) {
                std::shared_ptr<Producer> producer;
                DS_ASSERT_OK(w1Client_->CreateProducer(streamName_, producer, defaultProducerConf_));
                producers.emplace_back(producer);
            }
            auto &consumers = streams.at(i).second;
            uint consumersSize = consumers.size();
            for (uint j = 0; j < consumerPerStream; ++j) {
                std::shared_ptr<Consumer> consumer;
                SubscriptionConfig config("sub" + std::to_string(consumersSize + j), SubscriptionType::STREAM);
                DS_ASSERT_OK(w1Client_->Subscribe(streamName_, config, consumer));
                consumers.emplace_back(consumer);
            }
        }
    }

    void TestSendRecv(uint numOfElementPerProducer, uint numOfElementPerConsumer,
        std::vector<std::pair<std::vector<std::shared_ptr<Producer>>,
                              std::vector<std::shared_ptr<Consumer>>>> &streams)
    {
        for (auto &stream : streams) {
            auto &producers = stream.first;
            for (uint i = 0; i < producers.size(); ++i) {
                auto &producer = producers.at(i);
                if (producer) {
                    std::string data = "producer" + std::to_string(i + 1);
                    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
                    for (uint j = 0; j < numOfElementPerProducer; ++j) {
                        DS_ASSERT_OK(producer->Send(element));
                    }
                }
            }
            if (numOfElementPerConsumer == 0) {
                continue;
            }
            for (auto &consumer : stream.second) {
                if (consumer) {
                    std::vector<Element> outElements;
                    DS_ASSERT_OK(consumer->Receive(numOfElementPerConsumer, RPC_TIMEOUT, outElements));
                    ASSERT_EQ(outElements.size(), numOfElementPerConsumer);
                    outElements.clear();
                }
            }
        }
    }

    void CloseProducer(std::vector<uint> producerIndex,
        std::vector<std::pair<std::vector<std::shared_ptr<Producer>>,
                              std::vector<std::shared_ptr<Consumer>>>> &streams)
    {
        for (auto &stream : streams) {
            for (auto &idx : producerIndex) {
                if (stream.first.at(idx)) {
                    DS_ASSERT_OK(stream.first.at(idx)->Close());
                    stream.first.at(idx).reset();
                }
            }
        }
    }

    void CloseConsumers(std::vector<uint> consumerIndex,
        std::vector<std::pair<std::vector<std::shared_ptr<Producer>>,
                              std::vector<std::shared_ptr<Consumer>>>> &streams)
    {
        for (auto &stream : streams) {
            for (auto &idx : consumerIndex) {
                if (stream.second.at(idx)) {
                    DS_ASSERT_OK(stream.second.at(idx)->Close());
                    stream.second.at(idx).reset();
                }
            }
        }
    }

    void DeleteStreams(std::vector<std::pair<std::vector<std::shared_ptr<Producer>>,
                       std::vector<std::shared_ptr<Consumer>>>> &streams,
                       std::string streamName)
    {
        for (uint i = 0; i < streams.size(); ++i) {
            auto &stream = streams.at(i);
            for (auto &producer : stream.first) {
                if (producer) {
                    DS_ASSERT_OK(producer->Close());
                }
            }
            for (auto &consumer : stream.second) {
                if (consumer) {
                    DS_ASSERT_OK(consumer->Close());
                }
            }
            std::string streamName_ = streamName + std::to_string(i);
            DS_ASSERT_OK(w1Client_->DeleteStream(streamName_));
        }
    }
};

TEST_F(DataVerificationStreamClientScaleTest, TestVoluntaryScaleDown)
{
    LOG(INFO) << "TestVoluntaryScaleDown start!";

    const uint numOfStream = 16;
    uint producerPerStream = 3;
    const uint consumerPerStream = 1;
    std::vector<std::pair<std::vector<std::shared_ptr<Producer>>, std::vector<std::shared_ptr<Consumer>>>> streams;
    std::string streamName = "VoluntaryScaleDown";
    // Create 3 Producer and 1 Consumer for each stream.
    CreateStreams(numOfStream, producerPerStream, consumerPerStream, streams, streamName);

    // Normal Send and Recv
    const uint numOfElementPerProducer = 10;
    uint numOfElementPerConsumer = numOfElementPerProducer * producerPerStream;
    datasystem::inject::Set("VerifyProducerNo", "return()");
    TestSendRecv(numOfElementPerProducer, numOfElementPerConsumer, streams);

    // Close 1st and 3rd producer.
    std::vector<uint> producerIndex = {0, 2};
    CloseProducer(producerIndex, streams);
    producerPerStream -= producerIndex.size();

    // Shutdown worker2 to trigger voluntary scale down and metadata migration
    w2Client_.reset();
    VoluntaryScaleDownInject(1);
    // Wait for voluntary scale down to finish
    sleep(SCALE_DOWN_WAIT_TIME);

    // Create 3 more producer per stream
    uint newProducerPerStream = 3;
    CreateNProducerAndMConsumerForEachStream(newProducerPerStream, 0, streams, streamName);
    producerPerStream += newProducerPerStream;

    // Normal Send and Recv again
    numOfElementPerConsumer = numOfElementPerProducer * producerPerStream;
    TestSendRecv(numOfElementPerProducer, numOfElementPerConsumer, streams);
    datasystem::inject::Clear("VerifyProducerNo");

    // Cleanup
    DeleteStreams(streams, streamName);

    LOG(INFO) << "TestVoluntaryScaleDown finish!";
}

TEST_F(DataVerificationStreamClientScaleTest, TestVoluntaryScaleUp)
{
    LOG(INFO) << "TestVoluntaryScaleUp start!";

    const uint numOfStream = 16;
    uint producerPerStream = 3;
    const uint consumerPerStream = 1;
    std::vector<std::pair<std::vector<std::shared_ptr<Producer>>, std::vector<std::shared_ptr<Consumer>>>> streams;
    std::string streamName = "VoluntaryScaleUp";
    // Create 3 Producer and 1 Consumer for each stream.
    CreateStreams(numOfStream, producerPerStream, consumerPerStream, streams, streamName);

    // Normal Send and Recv
    const uint numOfElementPerProducer = 10;
    uint numOfElementPerConsumer = numOfElementPerProducer * producerPerStream;
    datasystem::inject::Set("VerifyProducerNo", "return()");
    TestSendRecv(numOfElementPerProducer, numOfElementPerConsumer, streams);

    // Close 1st and 3rd producer.
    std::vector<uint> producerIndex = {0, 2};
    CloseProducer(producerIndex, streams);
    producerPerStream -= producerIndex.size();

    // Add new worker node to trigger scale up and metadata migration
    DS_ASSERT_OK(AddNode());

    // Create 3 more producer per stream
    uint newProducerPerStream = 3;
    CreateNProducerAndMConsumerForEachStream(newProducerPerStream, 0, streams, streamName);
    producerPerStream += newProducerPerStream;

    // Normal Send and Recv again
    numOfElementPerConsumer = numOfElementPerProducer * producerPerStream;
    TestSendRecv(numOfElementPerProducer, numOfElementPerConsumer, streams);
    datasystem::inject::Clear("VerifyProducerNo");

    // Cleanup
    DeleteStreams(streams, streamName);

    LOG(INFO) << "TestVoluntaryScaleUp finish!";
}

class DataVerificationStreamClientPassiveScaleTest : public DataVerificationStreamClientScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        // Start 3 workers
        opts.numWorkers = ++workerNum_;
        opts.enableDistributedMaster = "true";
        opts.vLogLevel = 1;
        // Set up node_dead_timeout_s and auto_del_dead_node flags, so that a new meta owner master can be reselected
        // for passive scale down purposes
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=3 -node_dead_timeout_s=%d -auto_del_dead_node=true -shared_memory_size_mb=10240 "
            "-enable_stream_data_verification=true",
            NODE_DEAD_TIMEOUT);
        SCClientCommon::SetClusterSetupOptions(opts);
    }
};

TEST_F(DataVerificationStreamClientPassiveScaleTest, TestPassiveScaleDown)
{
    LOG(INFO) << "TestPassiveScaleDown start!";

    const uint worker3Index = 2;
    const uint numOfStream = 10;
    uint producerPerStream = 10;
    const uint consumerPerStream = 1;
    uint numOfNotReceiveElementPerConsumer = 0;
    std::vector<std::pair<std::vector<std::shared_ptr<Producer>>, std::vector<std::shared_ptr<Consumer>>>> streams;
    std::string streamName = "PassiveScaleDown";
    // Create 10 Producer and 1 Consumer for each stream.
    CreateStreams(numOfStream, producerPerStream, consumerPerStream, streams, streamName);

    // Normal Send and Recv.
    const uint numOfElementPerProducer = 10;
    uint numOfElementPerConsumer = 0;
    TestSendRecv(numOfElementPerProducer, numOfElementPerConsumer, streams);
    numOfNotReceiveElementPerConsumer += numOfElementPerProducer * producerPerStream;

    // Close 5 producers.
    std::vector<uint> producerIndex = {0, 6, 7, 8, 9};
    CloseProducer(producerIndex, streams);
    producerPerStream -= producerIndex.size();

    // Kill worker 3.
    DS_ASSERT_OK(static_cast<ExternalCluster *>(cluster_.get())->KillWorker(worker3Index));
    sleep(NODE_DEAD_TIMEOUT + 1);

    // Create 3 producers per stream.
    uint newProducerPerStream = 3;
    CreateNProducerAndMConsumerForEachStream(newProducerPerStream, 0, streams, streamName);
    producerPerStream += newProducerPerStream;

    // Normal Send and Recv.
    // 10 * 10 + 10 * (5 + 3) = 180
    datasystem::inject::Set("VerifyProducerNo", "return()");
    numOfNotReceiveElementPerConsumer += numOfElementPerProducer * producerPerStream;
    TestSendRecv(numOfElementPerProducer, numOfNotReceiveElementPerConsumer, streams);
    datasystem::inject::Clear("VerifyProducerNo");

    // Cleanup.
    DeleteStreams(streams, streamName);

    LOG(INFO) << "TestPassiveScaleDown finish!";
}

TEST_F(DataVerificationStreamClientPassiveScaleTest, TestRestartPassiveScaleDown)
{
    LOG(INFO) << "TestRestartPassiveScaleDown start!";

    const uint worker3Index = 2;
    const uint numOfStream = 10;
    uint producerPerStream = 10;
    const uint consumerPerStream = 1;
    uint numOfNotReceiveElementPerConsumer = 0;
    std::vector<std::pair<std::vector<std::shared_ptr<Producer>>, std::vector<std::shared_ptr<Consumer>>>> streams;
    std::string streamName = "RestartPassiveScaleDown";
    // Create 10 Producer and 1 Consumer for each stream.
    CreateStreams(numOfStream, producerPerStream, consumerPerStream, streams, streamName);

    // Normal Send and Recv.
    const uint numOfElementPerProducer = 10;
    uint numOfElementPerConsumer = 0;
    TestSendRecv(numOfElementPerProducer, numOfElementPerConsumer, streams);
    numOfNotReceiveElementPerConsumer += numOfElementPerProducer * producerPerStream;

    // Close 5 producers.
    std::vector<uint> producerIndex = {0, 6, 7, 8, 9};
    CloseProducer(producerIndex, streams);
    producerPerStream -= producerIndex.size();

    // Kill worker3, and sleep to trigger passive scale down logic.
    DS_ASSERT_OK(static_cast<ExternalCluster *>(cluster_.get())->KillWorker(worker3Index));
    sleep(NODE_DEAD_TIMEOUT + 1);
    // Then restart worker3 to trigger scale up metadata migration logic.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, worker3Index, {}));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, worker3Index));
    std::shared_ptr<StreamClient> w3Client;
    InitStreamClient(worker3Index, w3Client);

    // Create 3 producers per stream
    uint newProducerPerStream = 3;
    CreateNProducerAndMConsumerForEachStream(newProducerPerStream, 0, streams, streamName);
    producerPerStream += newProducerPerStream;

    // Normal Send and Recv.
    // 10 * 10 + 100 * (5 + 3) = 180
    datasystem::inject::Set("VerifyProducerNo", "return()");
    numOfNotReceiveElementPerConsumer += numOfElementPerProducer * producerPerStream;
    TestSendRecv(numOfElementPerProducer, numOfNotReceiveElementPerConsumer, streams);
    datasystem::inject::Clear("VerifyProducerNo");

    // Cleanup.
    DeleteStreams(streams, streamName);

    LOG(INFO) << "TestRestartPassiveScaleDown finish!";
}

class StreamClientScaleDfxTest : public StreamClientScaleTest {
};

TEST_F(StreamClientScaleDfxTest, LEVEL2_ScaleUpWhenMetaResidue)
{
    const int streamNum = 16;
    std::map<std::string, std::pair<std::shared_ptr<Producer>, std::shared_ptr<Consumer>>> streams;
    std::string streamName = "testScaleUpWhenMetaResidue";
    CreateNProducerAndConsumer(streams, streamNum, streamName, false);
    datasystem::inject::Set("StreamClient.ShutDown.skip", "return()");
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "BatchMigrateMetadata.finish", "1*sleep(2000)"));

    VoluntaryScaleDownInject(1);
    sleep(1); // wait hash ring change
    kill(cluster_->GetWorkerPid(1), SIGKILL);
    w2Client_.reset();
    sleep(6);  // wait 6s for worker passive reduction

    DS_ASSERT_OK(AddNode());
    WaitAllNodesJoinIntoHashRing(2, 10);  // wait 10s for 2 workers online
}

}  // namespace st
}  // namespace datasystem
