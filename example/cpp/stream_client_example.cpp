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
 * Description: The stream client example.
 */
#include "datasystem/datasystem.h"

#include <cstdlib>
#include <iostream>

using namespace datasystem;

static std::string DEFAULT_IP = "127.0.0.1";
static constexpr int DEFAULT_PORT = 9088;
static constexpr int PARAMETERS_NUM = 3;

static int CreateProducerAndConsumer(std::shared_ptr<StreamClient> &client, std::shared_ptr<Producer> &producer,
                                     std::shared_ptr<Consumer> &consumer)
{
    // Create the producer on the stream
    std::string streamName("example1");
    const uint64_t testStreamSize = 64 * 1024 * 1024;  // 64M
    ProducerConf producerConf;
    producerConf.maxStreamSize = testStreamSize;
    Status status = client->CreateProducer(streamName, producer, producerConf);
    if (status.IsError()) {
        std::cerr << "Failed to create producer : " << status.ToString() << std::endl;
        return -1;
    }
    std::cout << "Create producer successfully." << std::endl;

    // Create one subscription with one stream
    std::string subName("sub1");
    SubscriptionConfig config(subName, SubscriptionType::STREAM);
    status = client->Subscribe(streamName, config, consumer);
    if (status.IsError()) {
        std::cerr << "Failed to create subscription with one consumer : " << status.ToString() << std::endl;
        return -1;
    }
    std::cout << "Create consumer successfully." << std::endl;
    return 0;
}

static int WriteAndFlush(Producer *producer, std::string &data)
{
    // Write and flush one element
    Element element(reinterpret_cast<uint8_t *>(&data.front()), data.size(), ULONG_MAX);
    Status status = producer->Send(element);
    if (status.IsError()) {
        std::cerr << "Failed to Send one element : " << status.ToString() << std::endl;
        return -1;
    }
    std::cout << "Write one element successfully." << std::endl;
    return 0;
}

static int RecvAndVerify(Consumer *consumer, const std::string &data)
{
    // Read one element
    std::vector<Element> outElements;
    Status status = consumer->Receive(1, 0, outElements);
    if (status.IsError()) {
        std::cerr << "Failed to Receive one element : " << status.ToString() << std::endl;
        return -1;
    }

    if (outElements.size() != 1) {
        std::cerr << "Should receive one element but receive " << outElements.size() << std::endl;
        return -1;
    }
    if (outElements[0].id != 1) {
        std::cerr << "The element id should be 1 but is  " << outElements[0].id << std::endl;
        return -1;
    }
    std::string actualData(reinterpret_cast<char *>(outElements[0].ptr), outElements[0].size);
    if (data != actualData) {
        std::cerr << "The element verify failed expect: " << data << ", got:" << actualData << std::endl;
        return -1;
    }
    status = consumer->Ack(outElements[0].id);
    if (status.IsError()) {
        std::cerr << "Failed to ack one element : " << status.ToString() << std::endl;
        return -1;
    }
    std::cout << "Verify element successfully." << std::endl;
    return 0;
}

int RunExample(const std::string &ip, const int32_t port, const std::string &clientPublicKey,
               const std::string &clientPrivateKey, const std::string &serverPublicKey)
{
    ConnectOptions connectOpts{ .host = ip,
                                .port = port,
                                .connectTimeoutMs = 60 * 1000,
                                .clientPublicKey = clientPublicKey,
                                .clientPrivateKey = clientPrivateKey,
                                .serverPublicKey = serverPublicKey };
    connectOpts.enableExclusiveConnection = false;
    auto client = std::make_shared<StreamClient>(connectOpts);
    Status status = client->Init();
    if (status.IsError()) {
        std::cerr << "Failed to init stream client : " << status.ToString() << std::endl;
        return -1;
    }

    std::shared_ptr<Producer> producer;
    std::shared_ptr<Consumer> consumer;
    if (CreateProducerAndConsumer(client, producer, consumer)) {
        return -1;
    }
    std::string data = "Hello World";
    if (WriteAndFlush(producer.get(), data)) {
        return -1;
    }
    return RecvAndVerify(consumer.get(), data);
}

int main(int argc, char *argv[])
{
    const int authParametersNum = 6;
    std::string ip = DEFAULT_IP;
    int port = DEFAULT_PORT;
    int index = 0;
    std::string clientPublicKey;
    std::string clientPrivateKey;
    std::string serverPublicKey;
    if (argc == 1) {
        ip = DEFAULT_IP;
        port = DEFAULT_PORT;
    } else if (argc == PARAMETERS_NUM) {
        ip = argv[++index];
        port = atoi(argv[++index]);
    } else if (argc == authParametersNum) {
        ip = argv[++index];
        port = atoi(argv[++index]);
        clientPublicKey = argv[++index];
        clientPrivateKey = argv[++index];
        serverPublicKey = argv[++index];
    } else {
        std::cerr << "Invalid input parameters.";
    }

    // example call:
    // ./stream_example 127.0.0.1 18482 <client public key> <client private key> <worker public key>
    return RunExample(ip, port, clientPublicKey, clientPrivateKey, serverPublicKey);
}
