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

#include <unistd.h>
#include "datasystem/common/util/format.h"
#include "datasystem/stream_client.h"

#include <chrono>
#include <csignal>
#include <cstdint>
#include <sys/wait.h>
#include <thread>

#include "common.h"
#include "datasystem/utils/status.h"
#include "sc_client_common.h"
#include "common/stream_cache/element_generator.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/common/inject/inject_point.h"

using namespace datasystem::client::stream_cache;
namespace datasystem {
namespace st {
class ChildPidWrapper {
public:
    ChildPidWrapper(pid_t pid) : pid_(pid)
    {
    }
    ~ChildPidWrapper()
    {
        int status;
        waitpid(pid_, &status, 0);
    }

    ChildPidWrapper(const ChildPidWrapper &) = delete;
    ChildPidWrapper &operator=(const ChildPidWrapper &) = delete;

private:
    pid_t pid_;
};

template <class Func>
std::unique_ptr<ChildPidWrapper> RunInChildProcess(Func &&func)
{
    pid_t pid = fork();
    if (pid == 0) {
        func();
        return nullptr;
    }
    return std::make_unique<ChildPidWrapper>(pid);
}

constexpr int K_TWENTY = 20;
class ClientCrashTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.masterIdx = 0;
        opts.numRpcThreads = 0;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-client_dead_timeout_s=2 -v=2 -log_monitor=true";
        SCClientCommon::SetClusterSetupOptions(opts);
    }

protected:
    Status InitClient(int index, std::shared_ptr<StreamClient> &client)
    {
        InitStreamClient(index, client);
        return Status::OK();
    }

    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

class ClientSC1 {
public:
    explicit ClientSC1(std::string streamName) : streamName_(std::move(streamName))
    {
    }
    ~ClientSC1() = default;

    Status InitTestClient(const std::string &ip, const int &port, int timeout = 60000);

    Status CreateProducer(std::shared_ptr<Producer> &producer);

    Status CreateProducer(std::shared_ptr<Producer> &producer, ProducerConf conf);

    Status CreateProducer(std::shared_ptr<Producer> &producer, int64_t delayFlushTime);

    Status Subscribe(const std::string &subName, std::shared_ptr<Consumer> &consumer);

    Status QueryTotalProducerNum(uint64_t &totalProducerNum);

    Status QueryTotalConsumerNum(uint64_t &totalConsumerNum);

    Status Shutdown()
    {
        return client_->ShutDown();
    }

private:
    std::string streamName_;
    std::unique_ptr<StreamClient> client_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

Status ClientSC1::InitTestClient(const std::string &ip, const int &port, int timeout)
{
    ConnectOptions connectOptions;
    connectOptions.host = ip;
    connectOptions.port = port;
    connectOptions.connectTimeoutMs = timeout;
    connectOptions.SetAkSkAuth(accessKey_, secretKey_, "");
    client_ = std::make_unique<StreamClient>(connectOptions);
    return client_->Init();
}

Status ClientSC1::CreateProducer(std::shared_ptr<Producer> &producer)
{
    const uint64_t maxStreamSize = 20 * 1024 * 1024;  // The max size of stream page is 20M
    const int64_t pageSize = 4 * 1024;                // The size of page is 4096 bytes
    ProducerConf conf;
    conf.delayFlushTime = -1;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    return client_->CreateProducer(streamName_, producer, conf);
}

Status ClientSC1::CreateProducer(std::shared_ptr<Producer> &producer, ProducerConf conf)
{
    return client_->CreateProducer(streamName_, producer, conf);
}

Status ClientSC1::CreateProducer(std::shared_ptr<Producer> &producer, int64_t delayFlushTime)
{
    const uint64_t maxStreamSize = 20 * 1024 * 1024;  // The max size of stream page is 20M
    const int64_t pageSize = 4 * 1024;                // The size of page is 4096 bytes
    ProducerConf conf;
    conf.delayFlushTime = delayFlushTime;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    return client_->CreateProducer(streamName_, producer, conf);
}

Status ClientSC1::Subscribe(const std::string &subName, std::shared_ptr<Consumer> &consumer)
{
    SubscriptionConfig config(subName, SubscriptionType::STREAM);
    return client_->Subscribe(streamName_, config, consumer);
}

Status ClientSC1::QueryTotalProducerNum(uint64_t &totalProducerNum)
{
    return client_->QueryGlobalProducersNum(streamName_, totalProducerNum);
}

Status ClientSC1::QueryTotalConsumerNum(uint64_t &totalConsumerNum)
{
    return client_->QueryGlobalConsumersNum(streamName_, totalConsumerNum);
}

void WaitForTotalProducerNum(ClientSC1 &client, uint64_t expected)
{
    const uint64_t timeoutMs = 10'000;
    const uint64_t intervalMs = 100;
    Timer timer;
    uint64_t totalProducerNum = 0;
    Status rc = Status::OK();
    while (timer.ElapsedMilliSecond() < timeoutMs) {
        rc = client.QueryTotalProducerNum(totalProducerNum);
        if (rc.IsOk() && totalProducerNum == expected) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    DS_ASSERT_OK(rc);
    ASSERT_EQ(totalProducerNum, expected);
}

TEST_F(ClientCrashTest, DISABLED_TestProdClientCloseWhileReceiveSameHost)
{
    FLAGS_v = SC_INTERNAL_LOG_LEVEL;
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    // start client 1
    LOG(INFO) << "start create client 1";
    auto client1 = std::make_unique<ClientSC1>("SameHostProdClientClose");
    DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(producer1));

    // start client 2
    LOG(INFO) << "start create client 2";
    auto client2 = std::make_unique<ClientSC1>("SameHostProdClientClose");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(client2->Subscribe("subName1", consumer1));

    // Wait till consumer finds the producer.
    uint64_t producerCount = 0;
    while (producerCount == 0) {
        DS_ASSERT_OK(client2->QueryTotalProducerNum(producerCount));
    }

    ThreadPool threadPool(1);

    auto res = threadPool.Submit([consumer1]() {
        // Create a pending receive call.
        std::vector<Element> outElements;
        return consumer1->Receive(1, 20 * 1000, outElements);
    });

    // wait a safer period to let the other thread start.
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // client 1 abnormal close
    DS_ASSERT_OK(client1->Shutdown());
    auto rc = res.get();
    ASSERT_EQ(rc.GetCode(), K_SC_PRODUCER_NOT_FOUND);
}

TEST_F(ClientCrashTest, DISABLED_TestProdClientCloseWhileReceiveDiffHost)
{
    ThreadPool threadPool(1);

    threadPool.Submit([this]() {
        // start client 2
        HostPort workerAddress;
        LOG(INFO) << "start create client 2";
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddress));
        auto client2 = std::make_unique<ClientSC1>("DiffHostProdClientClose");
        DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Consumer> consumer1;
        DS_ASSERT_OK(client2->Subscribe("subName1", consumer1));

        // Wait till consumer finds the producer.
        uint64_t producerCount = 0;
        while (producerCount == 0) {
            DS_ASSERT_OK(client2->QueryTotalProducerNum(producerCount));
        }

        // Create a pending receive call.
        std::vector<Element> outElements;
        DS_ASSERT_OK(consumer1->Receive(1, 16 * 1000, outElements));
        ASSERT_EQ(outElements.size(), (size_t)1);
        Status rc = consumer1->Receive(1, 16 * 1000, outElements);
        DS_ASSERT_NOT_OK(rc);
        ASSERT_EQ(rc.GetCode(), K_SC_PRODUCER_NOT_FOUND);
    });

    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    // start client 1
    LOG(INFO) << "start create client 1";
    auto client1 = std::make_unique<ClientSC1>("DiffHostProdClientClose");
    DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer1;
    DS_ASSERT_OK(client1->CreateProducer(producer1));

    // Wait till consumer finds the producer.
    uint64_t consumerCount = 0;
    while (consumerCount == 0) {
        DS_ASSERT_OK(client1->QueryTotalConsumerNum(consumerCount));
    }

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(producer1->Send(element));

    // wait a safer period to let the other thread call the second Receive.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    DS_ASSERT_OK(client1->Shutdown());
}

TEST_F(ClientCrashTest, TestClientCrashWhenCloseProducer)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddress));
    auto client = std::make_unique<ClientSC1>("Client1CrashWhenCloseProducer");
    DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port()));

    std::vector<std::shared_ptr<Producer>> producerList;

    const int producerCount = 50;
    for (int i = 0; i < producerCount; i++) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client->CreateProducer(producer));
        producerList.emplace_back(producer);
    }

    std::thread t1([&producerList] {
        for (auto producer : producerList) {
            Status rc = producer->Close();
            if (rc.IsError()) {
                LOG(ERROR) << "Close failed:" << rc.GetMsg();
            }
        }
    });

    client = nullptr;
    t1.join();

    // Producers are implicitly closed, calling close again will be a no-op and return OK
    for (auto producer : producerList) {
        DS_ASSERT_OK(producer->Close());
    }

    // connect to worker again.
    auto client2 = std::make_unique<ClientSC1>("Client2CrashWhenCloseProducer");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
}

TEST_F(ClientCrashTest, LEVEL2_TestClientCrashWhenCreateProducer)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    DS_ASSERT_OK(cluster_->SetInjectAction(
        ClusterNodeType::WORKER, 0, "ClientWorkerSCServiceImpl.CloseProducerImplForceClose.sleep", "1*sleep(1000)"));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    auto pid = fork();
    // Client 1, CreateProducer, then crash
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("testCrashWhenCreateProd");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    // Client 2, CreateProducer, then sleep before adding to local
    auto client2 = std::make_unique<ClientSC1>("testCrashWhenCreateProd");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    DS_ASSERT_OK(cluster_->SetInjectAction(
        ClusterNodeType::WORKER, 0, "ClientWorkerSCServiceImpl.CreateProducerImpl.WaitBeforeAdd", "1*sleep(2000)"));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));

    // Wait for cleanup to finish. its set to 2secs above
    const int sleepTime = 3;
    sleep(sleepTime);

    int status;
    waitpid(pid, &status, 0);
    // When we close producer created by client2 it should not error out
    DS_ASSERT_OK(producer2->Close());
}

TEST_F(ClientCrashTest, TestClientCrashWhenCloseConsumer)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    auto client = std::make_unique<ClientSC1>("CrashWhenCloseProd");
    DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port()));

    std::vector<std::shared_ptr<Consumer>> consumerList;

    const int consumerCount = 50;
    for (int i = 0; i < consumerCount; i++) {
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(client->Subscribe("sub-" + std::to_string(i), consumer));
        consumerList.emplace_back(consumer);
    }

    std::thread t1([&consumerList] {
        for (auto consumer : consumerList) {
            Status rc = consumer->Close();
            if (rc.IsError()) {
                LOG(ERROR) << "Close failed:" << rc.GetMsg();
            }
        }
    });

    client = nullptr;
    t1.join();

    // Consumers are implicitly closed, calling close again will be a no-op and return OK
    for (auto consumer : consumerList) {
        DS_ASSERT_OK(consumer->Close());
    }

    // connect to worker again.
    auto client2 = std::make_unique<ClientSC1>("CrashWhenCloseProd");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
}

TEST_F(ClientCrashTest, TestProducerCrash1)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("ProducerCrash1");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    auto client2 = std::make_unique<ClientSC1>("ProducerCrash1");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer2));

    int status;
    waitpid(pid, &status, 0);

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    datasystem::inject::Clear("producer_obtained_lock");
    // Consumer is always lock free and can read
    std::vector<Element> out;
    const uint32_t timeoutMs = 100;
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 0);
    // Worker should be able to clean up the lock
    const int64_t TWO_MINUTES = 120'000;
    DS_ASSERT_OK(producer2->Send(element, TWO_MINUTES));
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 1);
    DS_ASSERT_OK(consumer2->Close());
}

TEST_F(ClientCrashTest, TestDownLevelProducerCrash1)
{
    DS_ASSERT_OK(datasystem::inject::Set("ClientBaseImpl.force_downlevel_client", "call()"));
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("DownLevelProducerCrash1");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    auto client2 = std::make_unique<ClientSC1>("DownLevelProducerCrash1");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer2));

    int status;
    waitpid(pid, &status, 0);

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    datasystem::inject::Clear("producer_obtained_lock");
    // Consumer is always lock free and can read
    std::vector<Element> out;
    const uint32_t timeoutMs = 100;
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 0);
    // Worker should be able to clean up the lock
    const int64_t TWO_MINUTES = 120'000;
    DS_ASSERT_OK(producer2->Send(element, TWO_MINUTES));
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 1);
    DS_ASSERT_OK(consumer2->Close());
}

TEST_F(ClientCrashTest, TestProducerCrash2)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("ProducerCrash2");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Fake a crash point within producer after it holds the lock
        // and update the slot count
        datasystem::inject::Set("producer_update_pending_slot_count_holding_lock", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    auto client2 = std::make_unique<ClientSC1>("ProducerCrash2");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer2));
    int status;
    waitpid(pid, &status, 0);

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    datasystem::inject::Clear("producer_update_pending_slot_count_holding_lock");
    // Consumer is always lock free and can read
    std::vector<Element> out;
    const uint32_t timeoutMs = 100;
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 0);
    // Worker should be able to clean up the lock
    const int64_t TWO_MINUTES = 120'000;
    DS_ASSERT_OK(producer2->Send(element, TWO_MINUTES));
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 1);
    DS_ASSERT_OK(consumer2->Close());
}

TEST_F(ClientCrashTest, TestProducerCrash3)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("ProducerCrash3");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Fake a crash point within producer after it holds the lock
        // and update the free space
        datasystem::inject::Set("producer_update_free_space", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    auto client2 = std::make_unique<ClientSC1>("ProducerCrash3");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer2));

    int status;
    waitpid(pid, &status, 0);

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    datasystem::inject::Clear("producer_update_free_space");
    // Consumer is always lock free and can read
    std::vector<Element> out;
    const uint32_t timeoutMs = 100;
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 0);
    // Worker should be able to clean up the lock
    const int64_t TWO_MINUTES = 120'000;
    DS_ASSERT_OK(producer2->Send(element, TWO_MINUTES));
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 1);
    DS_ASSERT_OK(consumer2->Close());
}

TEST_F(ClientCrashTest, DISABLED_TestProducerCrash4)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("ProducerCrash4");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Insert two rows first.
        // Fake a crash point within producer after it holds the lock
        // and update the free space
        DS_ASSERT_OK(producer1->Send(element));
        DS_ASSERT_OK(producer1->Send(element));
        datasystem::inject::Set("producer_update_slot_directory", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    auto client2 = std::make_unique<ClientSC1>("ProducerCrash4");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer2));

    int status;
    waitpid(pid, &status, 0);

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    datasystem::inject::Clear("producer_update_slot_directory");
    // Consumer is always lock free and can read
    std::vector<Element> out;
    const uint32_t timeoutMs = 100;
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    const int32_t expected = 2;
    DS_ASSERT_TRUE(out.size(), expected);
    // Worker should be able to clean up the lock
    const int64_t TWO_MINUTES = 120'000;
    DS_ASSERT_OK(producer2->Send(element, TWO_MINUTES));
    DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
    DS_ASSERT_TRUE(out.size(), 1);
    DS_ASSERT_OK(consumer2->Close());
}

TEST_F(ClientCrashTest, TestProducerCrash5)
{
    int replace = 1;
    (void)setenv("DATASYSTEM_LOG_ASYNC_ENABLE", "false", replace);
    
    HostPort workerAddress1;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress1));
    HostPort workerAddress2;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddress2));

    // Create a subscriber on worker2 so that there is a Worker1 to Worker2 flush
    auto client2 = std::make_unique<ClientSC1>("streamCrash5");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress2.Host(), workerAddress2.Port()));
    std::shared_ptr<Consumer> consumer2;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer2));
    // Create producer on worker1 and crash it after slot count is updated
    // This should just discard all data from the producer and remote worker should get ntg
    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "ClientManager.IsClientLost.heartbeatThreshold", "call(1)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StreamDataPool.SendElementsToRemote.wait", "pause()"));
    auto pid = fork();
    if (pid == 0) {
        // make scan eval thread wait for client timeout and clearAllRemoteConsumer
        auto client1 = std::make_unique<ClientSC1>("streamCrash5");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress1.Host(), workerAddress1.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Fake a crash point within producer after leaving the lock
        DS_ASSERT_OK(producer1->Send(element));
        datasystem::inject::Set("producer_update_pending_slot_count_without_lock", "1*abort()");
        DS_ASSERT_OK(producer1->Send(element));
        _exit(0);
    } else {
        ASSERT_TRUE(pid > 0);
        // Wait for producer to end
        int status;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        ASSERT_TRUE(WIFSIGNALED(status));
        ASSERT_EQ(WTERMSIG(status), SIGABRT);

        // Wait for worker to observe the crashed producer client and clear the producer metadata.
        WaitForTotalProducerNum(*client2, 0);
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "StreamDataPool.SendElementsToRemote.wait"));
        // Remote consumer should not get any data as producer crashed
        std::vector<Element> out;
        const uint32_t timeoutMs = 1000;
        DS_ASSERT_OK(consumer2->Receive(timeoutMs, out));
        DS_ASSERT_TRUE(out.size(), 0);
        DS_ASSERT_OK(consumer2->Close());
    }
}

TEST_F(ClientCrashTest, DISABLED_TestProducerCrash6)
{
    // Constructed based on TestProducerCrash1, while added more producer send
    // to trigger more of Ack related logic
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    const uint32_t timeoutMs = 10000;
    const size_t elementSize = 2000;
    const int numEle = 20;
    auto writeElement = RandomData().RandomBytes(elementSize);
    Element element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("ProducerCrash6");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Send a few pages of elements to populate ackChain
        for (int i = 0; i < numEle; i++) {
            DS_ASSERT_OK(producer1->Send(element));
        }
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    auto client2 = std::make_unique<ClientSC1>("ProducerCrash6");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(numEle, timeoutMs, outElements));
    DS_ASSERT_TRUE(outElements.size(), numEle);
    DS_ASSERT_OK(consumer->Ack(outElements.back().id));

    int status;
    waitpid(pid, &status, 0);

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    datasystem::inject::Clear("producer_obtained_lock");
    // Worker should be able to clean up the lock
    const int64_t ONE_MINUTE = 60'000;
    DS_ASSERT_OK(producer2->Send(element, ONE_MINUTE));
    DS_ASSERT_OK(consumer->Receive(timeoutMs, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    DS_ASSERT_OK(consumer->Ack(outElements.back().id));
    DS_ASSERT_OK(consumer->Close());
}

TEST_F(ClientCrashTest, TestProducerCrash7)
{
    // Constructed based on TestProducerCrash6, while included big elements
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    const uint32_t timeoutMs = 10000;
    const size_t elementSize = 1500;
    const size_t bigElementSize = 1024 * 10;
    const int numRound = 19;
    const int numEle = 59;
    auto writeElement = RandomData().RandomBytes(elementSize);
    Element element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    auto writeBigElement = RandomData().RandomBytes(bigElementSize);
    Element bigElement = Element(reinterpret_cast<uint8_t *>(writeBigElement.data()), writeBigElement.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("ProducerCrash7");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        // Send mixture of elements and big elements
        for (int i = 0; i < numRound; i++) {
            DS_ASSERT_OK(producer1->Send(element));
            DS_ASSERT_OK(producer1->Send(bigElement));
            DS_ASSERT_OK(producer1->Send(element));
        }
        DS_ASSERT_OK(producer1->Send(element));
        DS_ASSERT_OK(producer1->Send(bigElement));
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    auto client2 = std::make_unique<ClientSC1>("ProducerCrash7");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer));
    std::vector<Element> outElements;
    DS_ASSERT_OK(consumer->Receive(numEle, timeoutMs, outElements));
    DS_ASSERT_TRUE(outElements.size(), numEle);
    DS_ASSERT_OK(consumer->Ack(outElements.back().id));

    int status;
    waitpid(pid, &status, 0);

    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    datasystem::inject::Clear("producer_obtained_lock");
    // Worker should be able to clean up the lock
    const int64_t ONE_MINUTE = 60'000;
    DS_ASSERT_OK(producer2->Send(element, ONE_MINUTE));
    DS_ASSERT_OK(consumer->Receive(timeoutMs, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    DS_ASSERT_OK(consumer->Ack(outElements.back().id));
    DS_ASSERT_OK(consumer->Close());
}

TEST_F(ClientCrashTest, DISABLED_TestProducerCrash8)
{
    // README
    // numElem reduced from 30000 for CI runtime purposes
    // This testcase intends to construct the producer crash problem involving 2 clients
    // And the first client to recover is not the page lock holder.
    HostPort workerAddress1;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress1));
    HostPort workerAddress2;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddress2));

    const size_t elementSize = 180;
    const int numEle = 10000;
    auto writeElement = RandomData().RandomBytes(elementSize);
    Element element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    auto client1Pid = fork();
    if (client1Pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("ProducerCrash8");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress1.Host(), workerAddress1.Port()));
        auto client2 = std::make_unique<ClientSC1>("ProducerCrash8");
        DS_ASSERT_OK(client2->InitTestClient(workerAddress1.Host(), workerAddress1.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        for (int i = 0; i < numEle; i++) {
            DS_ASSERT_OK(producer1->Send(element));
        }
        std::shared_ptr<Producer> producer2;
        DS_ASSERT_OK(client2->CreateProducer(producer2));
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(producer2->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(client1Pid > 0);
    auto client3 = std::make_unique<ClientSC1>("ProducerCrash8");
    DS_ASSERT_OK(client3->InitTestClient(workerAddress2.Host(), workerAddress2.Port()));
    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client3->Subscribe("sub", consumer));

    int status;
    waitpid(client1Pid, &status, 0);

    const uint64_t clientDeadTimeoutSec = 15;
    sleep(clientDeadTimeoutSec + 1);
    DS_ASSERT_OK(consumer->Close());
    auto client4 = std::make_unique<ClientSC1>("StreamNameTest");
    DS_ASSERT_OK(client4->InitTestClient(workerAddress1.Host(), workerAddress1.Port()));
    std::shared_ptr<Producer> producer3;
    DS_ASSERT_OK(client4->CreateProducer(producer3));
}

TEST_F(ClientCrashTest, TestProducerCrashFixPage)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    // Create 2 producers for the same stream
    // Set element size and page size almost same
    // This means every Send() triggers a CreateShmPage
    // Once these conditions are there
    // Let one of the producer die at the lock
    // Now other will send to the same and get stuck

    // Page size is 4KB so one element should fit one page
    const size_t elementSize = 4000;
    auto writeElement = RandomData().RandomBytes(elementSize);
    Element element = Element(reinterpret_cast<uint8_t *>(writeElement.data()), writeElement.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("CrashFixPage");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer1;
        DS_ASSERT_OK(client1->CreateProducer(producer1));
        DS_ASSERT_OK(producer1->Send(element));
        DS_ASSERT_OK(producer1->Send(element));
        // Fake a crash point within producer while holding shared lock to read last page
        datasystem::inject::Set("producer_crash_getview", "1*abort()");
        DS_ASSERT_NOT_OK(producer1->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);
    auto client2 = std::make_unique<ClientSC1>("CrashFixPage");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer2;
    DS_ASSERT_OK(client2->CreateProducer(producer2));
    int status;
    waitpid(pid, &status, 0);
    // Other producer should not be stuck at create shm page
    DS_ASSERT_OK(producer2->Send(element));
}

TEST_F(ClientCrashTest, DISABLED_TestConsumerCrash1)
{
    // Test that consumer crash with ref count will not block further release and ack
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    const size_t maxStreamSize = 2 * 1024 * 1024;
    const size_t pageSize = 1024 * 1024;
    const size_t dataSize = 200 * 1024;
    const size_t numEle = 20;
    std::string data = RandomData().GetRandomString(dataSize);
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    auto pid = fork();
    if (pid == 0) {
        auto client2 = std::make_unique<ClientSC1>("ConsumerCrash1");
        DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        uint64_t producerCount = 0;
        while (producerCount == 0) {
            DS_ASSERT_OK(client2->QueryTotalProducerNum(producerCount));
        }
        std::shared_ptr<Consumer> consumer2;
        DS_ASSERT_OK(client2->Subscribe("sub", consumer2));
        const uint32_t timeoutMs = 5000;
        std::vector<Element> outElements;
        // Guarantee that the page is fetched, so the ref count is incremented with Worker EyeCatcher V0.
        DS_ASSERT_OK(consumer2->Receive(1, timeoutMs, outElements));
        ASSERT_EQ(outElements.size(), 1);

        std::abort();
    }
    ASSERT_TRUE(pid > 0);
    auto client1 = std::make_unique<ClientSC1>("ConsumerCrash1");
    DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    std::shared_ptr<Producer> producer1;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    conf.retainForNumConsumers = 1;
    DS_ASSERT_OK(client1->CreateProducer(producer1, conf));
    DS_ASSERT_OK(producer1->Send(element));

    int status;
    waitpid(pid, &status, 0);

    const uint64_t clientDeadTimeoutSec = 15;
    sleep(clientDeadTimeoutSec + 1);
    std::shared_ptr<Consumer> consumer1;
    DS_ASSERT_OK(client1->Subscribe("sub", consumer1));
    const uint32_t timeoutMs = 5000;
    std::vector<Element> outElements;
    // Continuously send until about 2 times max stream size,
    // to verify that pages are acked and released.
    for (size_t i = 0; i < numEle; i++) {
        DS_ASSERT_OK(producer1->Send(element, timeoutMs));
        DS_ASSERT_OK(consumer1->Receive(1, timeoutMs, outElements));
        ASSERT_EQ(outElements.size(), 1);
        DS_ASSERT_OK(consumer1->Ack(outElements.back().id));
    }
}

TEST_F(ClientCrashTest, AckPointLastRow)
{
    HostPort workerAddress, workerAddress2;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddress2));

    int timeOut = 1000;
    // Send enough elements so that the starting cursor picks up from previous consumer's
    // last ack point
    // 4096 + a few extra elements to get lastrecvcursor to be equal
    // to lastAck cursor
    int elementCount = 4100;
    std::string data = "A";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    std::vector<Element> outElements;

    auto client1 = std::make_unique<ClientSC1>("testAckPointLastRow");
    DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
    auto client2 = std::make_unique<ClientSC1>("testAckPointLastRow");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress2.Host(), workerAddress2.Port()));

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(producer));

    std::shared_ptr<Consumer> consumer1, consumer2;
    DS_ASSERT_OK(client2->Subscribe("sub", consumer1));

    // producer sends data
    for (int i = 0; i < elementCount; i++) {
        producer->Send(element);
    }

    // consume and ack all data so that cursor is on last row
    for (int i = 0; i < elementCount; i++) {
        DS_ASSERT_OK(consumer1->Receive(1, timeOut, outElements));
        ASSERT_EQ(outElements.size(), 1);
        DS_ASSERT_OK(consumer1->Ack(i + 1));
    }

    DS_ASSERT_OK(consumer1->Close());
    SubscriptionConfig config2("sub2", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe("sub2", consumer2));

    // Assert that it can still send, even with LastRecvCursor = AckCursor
    DS_ASSERT_OK(producer->Send(element));
    DS_ASSERT_OK(consumer2->Receive(1, timeOut, outElements));
    DS_ASSERT_TRUE(outElements.size(), 1);
    DS_ASSERT_OK(consumer2->Close());
}

TEST_F(ClientCrashTest, ClientResetTest)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    auto client = std::make_unique<ClientSC1>("testClientReset");
    DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port()));

    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client->CreateProducer(producer));

    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client->Subscribe("sub", consumer));

    client.reset();

    int elementCount = 100;
    int dataSize = 1024;
    std::string data(dataSize, 'a');
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // producer sends data
    for (int i = 0; i < elementCount; i++) {
        producer->Send(element);
    }

    int timeout = 3000;
    for (int i = 0; i < elementCount; i++) {
        std::vector<Element> outElements;
        DS_ASSERT_OK(consumer->Receive(1, timeout, outElements));
        ASSERT_EQ(outElements.size(), 1);
        DS_ASSERT_OK(consumer->Ack(i + 1));
    }

    producer.reset();
    consumer.reset();
}

TEST_F(ClientCrashTest, ClientShutdownWhenOOMTest)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    const int timeout = 10000;
    auto client = std::make_unique<ClientSC1>("testClientShutdownWhenOOM");
    DS_ASSERT_OK(client->InitTestClient(workerAddress.Host(), workerAddress.Port(), timeout));

    std::shared_ptr<Producer> producer;
    const uint64_t maxStreamSize = 20 * 1024 * 1024;  // The max size of stream page is 10M
    const int64_t pageSize = 4 * 1024;                // The size of page is 4096 bytes
    ProducerConf conf;
    conf.delayFlushTime = -1;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    conf.autoCleanup = true;
    DS_ASSERT_OK(client->CreateProducer(producer, conf));

    std::shared_ptr<Consumer> consumer;
    DS_ASSERT_OK(client->Subscribe("sub", consumer));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.CheckHadEnoughMem", "return(K_OUT_OF_MEMORY)"));
    DS_ASSERT_OK(inject::Set("client.CreateWritePage", "call()"));

    Timer timer;
    std::thread t([&client, &timer] {
        const int delay = 3000;
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        LOG_IF_ERROR(client->Shutdown(), "shutdown");
        timer.Reset();
    });
    int elementCount = 100;
    int dataSize = 1024;
    std::string data(dataSize, 'a');
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());

    // producer sends data
    for (int i = 0; i < elementCount; i++) {
        Status rc = producer->Send(element, timeout);
        if (rc.IsError()) {
            LOG(INFO) << rc.ToString();
            break;
        }
    }

    // waiting worker call HandleBlockedCreateTimeout after timeout.
    while (timer.ElapsedMicroSecond() <= timeout) {
        const int interval = 100;
        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }

    producer.reset();
    consumer.reset();
    t.join();
}

TEST_F(ClientCrashTest, DISABLED_TestForceCloseLocalProducersSameWorker)
{
    // We have two clients on same worker
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    const uint64_t maxStreamSize = 1024 * 1024;
    const int64_t pageSize = 4 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    std::string streamName = "testForceCloseProdSameWorker";

    // client1 have 5 producers for same stream
    std::vector<std::shared_ptr<Producer>> producers;
    const int num_producers = 5;
    for (int i = 0; i < num_producers; i++) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
        producers.emplace_back(producer);
    }

    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "ClientManager.Init.heartbeatInterval", "call(500)"));
    auto pid = fork();
    if (pid == 0) {
        // client2 have 5 producers for same stream
        std::shared_ptr<StreamClient> client2;
        DS_ASSERT_OK(InitClient(0, client2));
        std::vector<std::shared_ptr<Producer>> producers2;
        for (int i = 0; i < num_producers; i++) {
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(client2->CreateProducer(streamName, producer, conf));
            producers2.emplace_back(producer);
        }
        // client2 crashes after producer creation
        std::abort();
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(pid > 0);

    int status;
    waitpid(pid, &status, 0);

    // Wait for cleanup to finish. its set to 2secs above
    const int sleepTime = 3;
    sleep(sleepTime);

    // Other client will close all its producers
    for (auto &producer : producers) {
        DS_ASSERT_OK(producer->Close());
    }
    // Master metadata should be cleared and
    // We should be able to delete the stream
    DS_ASSERT_OK(client1->DeleteStream(streamName));
}

TEST_F(ClientCrashTest, DISABLED_TestForceCloseLocalProducersDifferentWorker)
{
    // We have two clients on same worker
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    const uint64_t maxStreamSize = 1024 * 1024;
    const int64_t pageSize = 4 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    std::string streamName = "testForceCloseProdDiffWorker";

    // client1 have 5 producers for same stream
    std::vector<std::shared_ptr<Producer>> producers;
    const int num_producers = 5;
    for (int i = 0; i < num_producers; i++) {
        std::shared_ptr<Producer> producer;
        DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
        producers.emplace_back(producer);
    }

    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "ClientManager.Init.heartbeatInterval", "call(500)"));
    auto pid = fork();
    if (pid == 0) {
        // client2 have 5 producers for same stream in a different worker
        std::shared_ptr<StreamClient> client2;
        DS_ASSERT_OK(InitClient(1, client2));
        std::vector<std::shared_ptr<Producer>> producers2;
        for (int i = 0; i < num_producers; i++) {
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(client2->CreateProducer(streamName, producer, conf));
            producers2.emplace_back(producer);
        }
        // client2 crashes after producer creation
        std::abort();
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(pid > 0);

    int status;
    waitpid(pid, &status, 0);

    // Wait for cleanup to finish. its set to 2secs above
    const int sleepTime = 3;
    sleep(sleepTime);

    // Other client will close all its producers
    for (auto &producer : producers) {
        DS_ASSERT_OK(producer->Close());
    }
    // Master metadata should be cleared and
    // We should be able to delete the stream
    DS_ASSERT_OK(client1->DeleteStream(streamName));
}

TEST_F(ClientCrashTest, LEVEL2_TestForceCloseDeadlock)
{
    // Test that with large amount of streams, force close can generate logical deadlock
    // if too many const_accessor are held at the same time.
    const int streamNum = 1000;
    const uint64_t maxStreamSize = 1024 * 1024;
    const int64_t pageSize = 4 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    conf.autoCleanup = true;
    auto pid = fork();
    if (pid == 0) {
        std::shared_ptr<StreamClient> client;
        DS_ASSERT_OK(InitClient(0, client));
        std::vector<std::shared_ptr<Producer>> producers;
        for (int i = 0; i < streamNum; i++) {
            std::string streamName = "Stream_" + std::to_string(i);
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(client->CreateProducer(streamName, producer, conf));
            producers.emplace_back(producer);
        }
        std::abort();
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(pid > 0);

    int status;
    waitpid(pid, &status, 0);

    // Wait for MasterWorkerSCService thread pool to be occupied.
    const int sleepTime = 20;
    sleep(sleepTime);
    std::shared_ptr<StreamClient> client1;
    DS_ASSERT_OK(InitClient(0, client1));
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(1, client2));
    std::string streamName = "Stream_" + RandomData().GetRandomString(10);
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client2->Subscribe(streamName, config, consumer));
    std::shared_ptr<Producer> producer;
    DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
}

TEST_F(ClientCrashTest, TestForceEarlyReturn)
{
    // Test that force close skips sending CloseProducer request to master
    // from some streams after previous manual delete fails.
    const int streamNum = 2;
    const uint64_t maxStreamSize = 1024 * 1024;
    const int64_t pageSize = 4 * 1024;
    ProducerConf conf;
    conf.maxStreamSize = maxStreamSize;
    conf.pageSize = pageSize;
    conf.autoCleanup = true;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "ClientManager.Init.heartbeatInterval", "call(500)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "CloseProducer.TimeoutInMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    pid_t pid = fork();
    if (pid == 0) {
        std::shared_ptr<StreamClient> client1;
        DS_ASSERT_OK(InitClient(0, client1));
        std::vector<std::shared_ptr<Producer>> producers;
        for (int i = 0; i < streamNum; i++) {
            std::string streamName = "Stream_" + std::to_string(i);
            std::shared_ptr<Producer> producer;
            DS_ASSERT_OK(client1->CreateProducer(streamName, producer, conf));
            producers.emplace_back(producer);
        }
        // construct the case where manual CloseProducer is successful on master, but timeout on worker.
        DS_ASSERT_NOT_OK(producers[0]->Close());
        std::abort();
    };

    ASSERT_TRUE(pid > 0);

    int status;
    waitpid(pid, &status, 0);

    // Wait for force close
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "ClientManager.IsClientLost.heartbeatThreshold",
                                           "call(1)"));
    sleep(streamNum);
}

TEST_F(ClientCrashTest, TestConsuemrBadFnCallCrashWithLock)
{
    size_t maxPageCount = 2;
    size_t pageSize = 1024 * 1024;
    int streamCount = 2;
    int oomTimeout = 3;

    const size_t elementSize = 10240;  // 10k.
    size_t nums = pageSize * maxPageCount * 2 / elementSize - 10;

    std::string streamNameCrash = "CrashConsumer";
    auto wrapper = RunInChildProcess([&] {
        std::shared_ptr<StreamClient> client;
        DS_ASSERT_OK(InitClient(1, client));
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        DS_ASSERT_OK(client->Subscribe(streamNameCrash, config, consumer, true));

        size_t recvNum = 0;
        while (recvNum < nums) {
            std::vector<Element> outElements;
            const int recvTimeout = 1000;
            ASSERT_EQ(consumer->Receive(1, recvTimeout, outElements), Status::OK());
            if (outElements.empty()) {
                continue;
            }
            if (recvNum == 0) {
                DS_ASSERT_OK(inject::Set("SharedMemViewImpl.GetView", "1*call()"));
            }
            recvNum += outElements.size();
            LOG(INFO) << "stream:" << streamNameCrash << ", SubProcess Recv num:" << recvNum;
        }
        std::abort();
    });
    if (wrapper == nullptr) {
        return;
    }

    std::shared_ptr<StreamClient> client1;
    std::shared_ptr<StreamClient> client2;
    DS_ASSERT_OK(InitClient(0, client1));
    DS_ASSERT_OK(InitClient(1, client2));
    std::string data(elementSize, 'a');
    Element element((uint8_t *)data.data(), data.size());

    ProducerConf conf;
    conf.pageSize = pageSize;
    conf.maxStreamSize = pageSize * maxPageCount;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    std::vector<StreamClient *> clients = { client1.get(), client2.get() };
    std::vector<std::shared_ptr<Consumer>> consumers;
    std::vector<std::shared_ptr<Producer>> producers;
    for (int index = 0; index < streamCount; index++) {
        std::string streamName = "TestStream-" + std::to_string(index);
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer;
        ASSERT_EQ(clients[index % clients.size()]->CreateProducer(streamName, producer, conf), Status::OK());
        ASSERT_EQ(clients[(index + 1) % clients.size()]->Subscribe(streamName, config, consumer, true), Status::OK());
        consumers.emplace_back(std::move(consumer));
        producers.emplace_back(std::move(producer));
    }

    DS_ASSERT_OK(inject::Set("SharedMemViewImpl.SetView", "1*call()"));
    ASSERT_EQ(producers[0]->Send(element).GetCode(), K_RUNTIME_ERROR);
    // Send.
    for (int index = 0; index < streamCount; index++) {
        for (size_t i = 0; i < nums; i++) {
            Status rc = producers[index]->Send(element);
            Timer timer;
            const int maxTimeout = 10;
            while (rc.GetCode() == K_OUT_OF_MEMORY && timer.ElapsedSecond() < maxTimeout) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                rc = producers[index]->Send(element);
            }
            DS_ASSERT_OK(rc);
            LOG(INFO) << FormatString("Stream index %zu, send count: %zu", streamCount, i);
        }
    }

    std::thread t([=] {
        ProducerConf conf;
        conf.pageSize = pageSize;
        conf.maxStreamSize = pageSize * maxPageCount;
        conf.autoCleanup = true;
        std::shared_ptr<Producer> producer;
        while (true) {
            size_t gConsumerNum = 0;
            DS_ASSERT_OK(client1->QueryGlobalConsumersNum(streamNameCrash, gConsumerNum));
            if (gConsumerNum > 0) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_EQ(client1->CreateProducer(streamNameCrash, producer, conf), Status::OK());
        for (size_t i = 0; i < nums; i++) {
            Status rc = producer->Send(element);
            Timer timer;
            const int maxTimeout = 10;
            while (rc.GetCode() == K_OUT_OF_MEMORY && timer.ElapsedSecond() < maxTimeout) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                rc = producer->Send(element);
            }
            DS_ASSERT_OK(rc);
            LOG(INFO) << FormatString("stream: %s,Stream index %zu,: send count: %zu", streamNameCrash, streamCount, i);
        }
    });

    sleep(oomTimeout);

    for (int index = 0; index < streamCount; index++) {
        size_t recvNum = 0;
        while (recvNum < nums) {
            std::vector<Element> outElements;
            const int recvTimeout = 1000;
            ASSERT_EQ(consumers[index]->Receive(nums, recvTimeout, outElements), Status::OK());
            recvNum += outElements.size();
            LOG(INFO) << "Recv num:" << recvNum;
        }
        ASSERT_EQ(recvNum, nums);
    }
    t.join();
}

class ClientCrashWithLockTest : public ClientCrashTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.masterIdx = 0;
        opts.numRpcThreads = 0;
        opts.numEtcd = 1;
        opts.workerGflagParams = FormatString("-client_dead_timeout_s=%zu -v=2 -log_monitor=true", clientDeadTimeout);
        SCClientCommon::SetClusterSetupOptions(opts);
    }

protected:
    int clientDeadTimeout = 5;
};

TEST_F(ClientCrashWithLockTest, TestProducerCrashWithPageMemViewLock)
{
    size_t maxPageCount = 4;
    size_t pageSize = 1024 * 1024;

    const size_t elementSize = 10240;  // 10k.
    std::string data(elementSize, 'a');
    Element element((uint8_t *)data.data(), data.size());

    std::string streamNameCrash = "CrashConsumer";
    auto wrapper = RunInChildProcess([&] {
        std::shared_ptr<StreamClient> client;
        DS_ASSERT_OK(InitClient(0, client));
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        conf.pageSize = pageSize;
        conf.maxStreamSize = pageSize * maxPageCount;
        while (true) {
            size_t gConsumerNum = 0;
            DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamNameCrash, gConsumerNum));
            if (gConsumerNum > 0) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_EQ(client->CreateProducer(streamNameCrash, producer, conf), Status::OK());
        DS_ASSERT_OK(inject::Set("client.Producer.beforeCheckNewPage", "2*off->1*call()"));
        // send untill crash.
        size_t sendCount = 0;
        while (true) {
            Status rc = producer->Send(element);
            if (rc.GetCode() == K_OUT_OF_MEMORY) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            DS_ASSERT_OK(rc);
            sendCount++;
            LOG(INFO) << "stream:" << streamNameCrash << ", SubProcess send num:" << sendCount;
        }
        std::abort();
    });
    if (wrapper == nullptr) {
        return;
    }

    std::shared_ptr<StreamClient> client;
    DS_ASSERT_OK(InitClient(0, client));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client->Subscribe(streamNameCrash, config, consumer, true));

    int delayBeforeRecvMs = 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(delayBeforeRecvMs));
    int testTimeMs = 5000;
    size_t recvNum = 0;
    Timer timer;
    while (timer.ElapsedMilliSecond() < testTimeMs) {
        std::vector<Element> outElements;
        const int recvTimeout = 1000;
        ASSERT_EQ(consumer->Receive(1, recvTimeout, outElements), Status::OK());
        if (outElements.empty()) {
            continue;
        }
        recvNum += outElements.size();
        LOG(INFO) << "stream:" << streamNameCrash << ", Recv num:" << recvNum;
    }
}

TEST_F(ClientCrashWithLockTest, LEVEL1_TestProducerCrashWithCursorMemViewLock)
{
    size_t maxPageCount = 5;
    size_t pageSize = 1024 * 1024;

    const size_t elementSize = 10240;  // 10k.
    std::string data(elementSize, 'a');
    Element element((uint8_t *)data.data(), data.size());

    std::string streamNameCrash = "CrashConsumer";
    auto wrapper = RunInChildProcess([&] {
        std::shared_ptr<StreamClient> client;
        DS_ASSERT_OK(InitClient(0, client));
        std::shared_ptr<Producer> producer;
        ProducerConf conf;
        conf.pageSize = pageSize;
        conf.maxStreamSize = pageSize * maxPageCount;
        while (true) {
            size_t gConsumerNum = 0;
            DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamNameCrash, gConsumerNum));
            if (gConsumerNum > 0) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_EQ(client->CreateProducer(streamNameCrash, producer, conf), Status::OK());
        DS_ASSERT_OK(inject::Set("client.Producer.beforeCheckCursor", "1*call()"));
        DS_ASSERT_OK(producer->Send(element));
        std::abort();
    });
    if (wrapper == nullptr) {
        return;
    }
    int delayBeforeSendMs = 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(delayBeforeSendMs));

    std::shared_ptr<StreamClient> client;
    DS_ASSERT_OK(InitClient(0, client));
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    DS_ASSERT_OK(client->Subscribe(streamNameCrash, config, consumer, true));

    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.pageSize = pageSize;
    conf.maxStreamSize = pageSize * maxPageCount;
    ASSERT_EQ(client->CreateProducer(streamNameCrash, producer, conf), Status::OK());
    // send again.
    size_t sendCount = 0;
    int testTimeMs = 5000;
    Timer timer;
    while (timer.ElapsedMilliSecond() < testTimeMs) {
        Status rc = producer->Send(element);
        if (rc.GetCode() == K_OUT_OF_MEMORY) {
            LOG(INFO) << "send finish with:" << rc.ToString();
            break;
        }
        DS_ASSERT_OK(rc);
        sendCount++;
        LOG(INFO) << "stream:" << streamNameCrash << ", Main Process send num:" << sendCount;
    }
}

TEST_F(ClientCrashWithLockTest, DISABLED_TestConsuemrCrashWithPageMemViewLock)
{
    // consumer crash with MemView in Page, worker and producer will block.
    size_t maxPageCount = 2;
    size_t pageSize = 1024 * 1024;

    const size_t elementSize = 10240;  // 10k.
    const int testTimeMs = 5000;

    std::string streamNameCrash = "CrashConsumer";
    auto wrapper = RunInChildProcess([&] {
        std::shared_ptr<StreamClient> client;
        DS_ASSERT_OK(InitClient(0, client));
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        DS_ASSERT_OK(client->Subscribe(streamNameCrash, config, consumer, true));

        DS_ASSERT_OK(inject::Set("SharedMemViewImpl.GetView", "abort"));
        size_t recvNum = 0;
        Timer timer;
        // recv until crash
        while (timer.ElapsedMilliSecond() < testTimeMs) {
            std::vector<Element> outElements;
            const int recvTimeout = 1000;
            ASSERT_EQ(consumer->Receive(1, recvTimeout, outElements), Status::OK());
            if (outElements.empty()) {
                continue;
            }
            recvNum += outElements.size();
            LOG(INFO) << "stream:" << streamNameCrash << ", SubProcess Recv num:" << recvNum;
        }
        std::abort();
    });
    if (wrapper == nullptr) {
        return;
    }

    std::string data(elementSize, 'a');
    Element element((uint8_t *)data.data(), data.size());

    std::shared_ptr<StreamClient> client;
    DS_ASSERT_OK(InitClient(0, client));
    std::shared_ptr<Producer> producer;
    ProducerConf conf;
    conf.pageSize = pageSize;
    conf.maxStreamSize = pageSize * maxPageCount;
    while (true) {
        size_t gConsumerNum = 0;
        DS_ASSERT_OK(client->QueryGlobalConsumersNum(streamNameCrash, gConsumerNum));
        if (gConsumerNum > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(client->CreateProducer(streamNameCrash, producer, conf), Status::OK());
    size_t sendCount = 0;
    Timer timer;
    while (timer.ElapsedMilliSecond() < testTimeMs) {
        Status rc = producer->Send(element);
        if (rc.GetCode() == K_OUT_OF_MEMORY) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        DS_ASSERT_OK(rc);
        sendCount++;
        LOG(INFO) << "stream:" << streamNameCrash << ", SubProcess send num:" << sendCount;
    }
}

class ClientLockVersionTest : public ClientCrashTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.masterIdx = 0;
        opts.numRpcThreads = 0;
        opts.numEtcd = 1;
        opts.workerGflagParams = FormatString("-client_dead_timeout_s=%zu -v=2 -log_monitor=true", clientDeadTimeout);
        SCClientCommon::SetClusterSetupOptions(opts);
    }

protected:
    int clientDeadTimeout = 5;

    Status StreamSendRecvTest(bool newProducer, bool newConsumer, bool newWorker)
    {
        const size_t maxPageCount = 3;
        const size_t pageSize = 100 * 1024;
        const size_t testElementCount = 1000;
        const size_t producerCount = 2;
        const size_t elementSize = 10240;  // 10k.

        std::string testStreamName = "SendRecvTestConsumer";
        auto wrapper = RunInChildProcess([&] {
            if (!newProducer) {
                LOG_IF_ERROR(inject::Set("MemView.Lock.OldVersion", "return"), "inject set failed");
            }
            std::shared_ptr<StreamClient> client;
            DS_ASSERT_OK(InitClient(0, client));
            ProducerConf conf;
            conf.pageSize = pageSize;
            conf.maxStreamSize = pageSize * maxPageCount;
            while (true) {
                size_t gConsumerNum = 0;
                DS_ASSERT_OK(client->QueryGlobalConsumersNum(testStreamName, gConsumerNum));
                if (gConsumerNum > 0) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            std::vector<std::shared_ptr<Producer>> producers;
            for (size_t n = 0; n < producerCount; n++) {
                std::shared_ptr<Producer> producer;
                ASSERT_EQ(client->CreateProducer(testStreamName, producer, conf), Status::OK());
                producers.emplace_back(std::move(producer));
            }

            std::vector<std::thread> threads;
            for (size_t n = 0; n < producerCount; n++) {
                threads.emplace_back([n, &producers, &testStreamName] {
                    size_t sendCount = 0;
                    while (sendCount < testElementCount) {
                        char ch = sendCount % INT8_MAX;
                        std::string data(elementSize, ch);
                        Element element((uint8_t *)data.data(), data.size());

                        Status rc = producers[n]->Send(element);
                        if (rc.GetCode() == K_OUT_OF_MEMORY) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                            continue;
                        }
                        DS_ASSERT_OK(rc);
                        sendCount++;
                        LOG(INFO) << "stream:" << testStreamName << ", SubProcess send num:" << sendCount;
                    }
                });
            }

            for (auto &t : threads) {
                t.join();
            }
            std::abort();
        });
        if (wrapper == nullptr) {
            return Status(K_INVALID, "invalid");
        }

        if (!newWorker) {
            RETURN_IF_NOT_OK(cluster_->SetInjectAction(WORKER, 0, "MemView.Lock.OldVersion", "return"));
        }

        if (!newConsumer) {
            RETURN_IF_NOT_OK(inject::Set("MemView.Lock.OldVersion", "return"));
        }

        std::shared_ptr<StreamClient> client;
        RETURN_IF_NOT_OK(InitClient(0, client));
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        RETURN_IF_NOT_OK(client->Subscribe(testStreamName, config, consumer, true));

        int testTimeOutMs = 20000;
        size_t expectRecvElementCount = testElementCount * producerCount;
        size_t recvNum = 0;
        Timer timer;
        while (timer.ElapsedMilliSecond() < testTimeOutMs && recvNum < expectRecvElementCount) {
            std::vector<Element> outElements;
            const int recvTimeout = 1000;
            RETURN_IF_NOT_OK(consumer->Receive(1, recvTimeout, outElements));
            if (outElements.empty()) {
                continue;
            }
            std::string expectData(elementSize, static_cast<char>(outElements[0].ptr[0]));
            std::string recvData((char *)(outElements[0].ptr), outElements[0].size);
            if (recvData != expectData) {
                const int printCount = 100;
                LOG(ERROR) << "expectData:" << expectData.substr(0, printCount)
                           << ", recvData:" << recvData.substr(0, printCount);
                return Status(K_RUNTIME_ERROR, "invalid data");
            }
            recvNum += outElements.size();
            LOG(INFO) << "stream:" << testStreamName << ", Recv num:" << recvNum;
        }

        if (recvNum != expectRecvElementCount) {
            return Status(K_RUNTIME_ERROR,
                          FormatString("Recv count %zu, expect count %zu", recvNum, expectRecvElementCount));
        }
        return Status::OK();
    }
};

TEST_F(ClientLockVersionTest, SendRecvTest1)
{
    DS_ASSERT_OK(StreamSendRecvTest(false, false, true));
}

TEST_F(ClientLockVersionTest, SendRecvTest2)
{
    DS_ASSERT_OK(StreamSendRecvTest(true, false, true));
}

TEST_F(ClientLockVersionTest, LEVEL2_SendRecvTest3)
{
    DS_ASSERT_OK(StreamSendRecvTest(false, true, true));
}

TEST_F(ClientLockVersionTest, SendRecvTest4)
{
    DS_ASSERT_OK(StreamSendRecvTest(true, true, false));
}

TEST_F(ClientLockVersionTest, SendRecvTest5)
{
    DS_ASSERT_OK(StreamSendRecvTest(true, false, false));
}

TEST_F(ClientLockVersionTest, SendRecvTest6)
{
    DS_ASSERT_OK(StreamSendRecvTest(false, true, false));
}

class ClientCrashShortTimeoutTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.masterIdx = 0;
        opts.numRpcThreads = 0;
        opts.numEtcd = 1;
        opts.workerGflagParams = FormatString("-client_dead_timeout_s=%zu -v=2", clientDeadTimeoutSec);
        SCClientCommon::SetClusterSetupOptions(opts);
    }

protected:
    const uint64_t clientDeadTimeoutSec = 15;
};

TEST_F(ClientCrashShortTimeoutTest, DISABLED_LEVEL1_TestResourceClearDuration)
{
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));

    std::string data = "Hello World";
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size());
    auto pid = fork();
    if (pid == 0) {
        auto client1 = std::make_unique<ClientSC1>("testResourceClear");
        DS_ASSERT_OK(client1->InitTestClient(workerAddress.Host(), workerAddress.Port()));
        std::shared_ptr<Producer> producer;
        std::shared_ptr<Consumer> consumer;
        DS_ASSERT_OK(client1->CreateProducer(producer));
        DS_ASSERT_OK(client1->Subscribe("sub1", consumer));
        // Fake a crash point within producer after it holds the lock
        datasystem::inject::Set("producer_obtained_lock", "1*abort()");
        DS_ASSERT_NOT_OK(producer->Send(element));
        _exit(0);
    }
    ASSERT_TRUE(pid > 0);

    int status;
    waitpid(pid, &status, 0);

    auto client2 = std::make_unique<ClientSC1>("testResourceClear");
    DS_ASSERT_OK(client2->InitTestClient(workerAddress.Host(), workerAddress.Port()));

    uint64_t totalConsumerNum;
    uint64_t totalProducerNum;
    DS_ASSERT_OK(client2->QueryTotalConsumerNum(totalConsumerNum));
    DS_ASSERT_OK(client2->QueryTotalProducerNum(totalProducerNum));
    ASSERT_EQ(totalConsumerNum, 1ul);
    ASSERT_EQ(totalProducerNum, 1ul);

    sleep(clientDeadTimeoutSec + 1);
    DS_ASSERT_OK(client2->QueryTotalConsumerNum(totalConsumerNum));
    DS_ASSERT_OK(client2->QueryTotalProducerNum(totalProducerNum));
    ASSERT_EQ(totalConsumerNum, 0ul);
    ASSERT_EQ(totalProducerNum, 0ul);
}
}  // namespace st
}  // namespace datasystem
