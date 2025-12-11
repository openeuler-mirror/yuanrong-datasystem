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
 * Description: The object client example.
 */
#include "datasystem/datasystem.h"

#include <iostream>

using datasystem::Buffer;
using datasystem::ConnectOptions;
using datasystem::CreateParam;
using datasystem::ObjectClient;
using datasystem::Optional;
using datasystem::Status;
static std::shared_ptr<ObjectClient> client_;
static std::shared_ptr<Buffer> buffer_;

static std::string DEFAULT_IP = "127.0.0.1";
static constexpr int DEFAULT_PORT = 9088;
static constexpr int PARAMETERS_NUM = 5;
static constexpr int64_t SHM_SIZE = 600 * 1024;

static void Write(int64_t size, bool isSeal)
{
    std::cout << "Writing data to a buffer" << std::endl;
    std::cout << std::boolalpha << "Immutable data: " << isSeal << std::endl;
    std::string objectKey = "123456789";
    std::string data(size, 'x');
    std::vector<std::string> failedIds;
    client_->GIncreaseRef({ objectKey }, failedIds);
    Status status = client_->Create(objectKey, size, CreateParam{}, buffer_);
    std::cout << "Create: " << status.ToString() << std::endl;
    buffer_->WLatch();
    buffer_->MemoryCopy((void *)data.data(), size);
    if (isSeal) {
        buffer_->Seal();
    } else {
        buffer_->Publish();
    }
    buffer_->UnWLatch();
    std::cout << "Write succeeds." << std::endl;
}

static void Read(int64_t size, bool isSeal)
{
    Write(size, isSeal);
    std::cout << "Reading data from a buffer" << std::endl;
    std::string objectKey = "123456789";
    std::vector<std::string> objKeys = { objectKey };
    const int64_t timeoutMs = 60000;
    std::vector<Optional<Buffer>> buffers;
    Status status = client_->Get(objKeys, timeoutMs, buffers);
    if (!status.IsOk()) {
        std::cerr << "Read Get Fail: " << status.ToString() << std::endl;
        return;
    }
    std::cout << "Get: " << status.ToString() << std::endl;
    auto &buf = buffers[0];
    buf->RLatch();
    auto data = std::string(reinterpret_cast<const char *>(buf->ImmutableData()), buf->GetSize());
    buf->UnRLatch();
    std::cout << "Read succeeds." << std::endl;
}

static void Modify(int64_t size)
{
    Write(size, false);
    std::cout << "Modifying data in the buffer" << std::endl;
    std::string objectKey = "123456789";
    std::vector<std::string> objKeys = { objectKey };
    const int64_t timeoutMs = 60000;
    std::vector<Optional<Buffer>> buffers;
    Status status = client_->Get(objKeys, timeoutMs, buffers);
    if (!status.IsOk()) {
        std::cerr << "Modify Get Fail: " << status.ToString() << std::endl;
        return;
    }
    std::cout << "Get: " << status.ToString() << std::endl;
    auto &buf = buffers[0];
    std::string newData = "test";
    auto newDataSize = newData.size();
    buf->WLatch();
    buf->MemoryCopy((void *)newData.data(), newDataSize);
    buf->Publish();
    buf->UnWLatch();

    // read the data after modification
    buf->RLatch();
    auto data = std::string(reinterpret_cast<const char *>(buf->ImmutableData()), buf->GetSize());
    buf->UnRLatch();
    std::cout << "Modify succeeds !!!" << std::endl;
}

static void Delete(int64_t size, bool isSeal)
{
    Write(size, isSeal);
    std::cout << "Deleting...." << std::endl;
    std::string objectKey = "123456789";
    std::vector<std::string> objKeys = { objectKey };
    std::vector<std::string> failedIds;
    Status status = client_->GDecreaseRef(objKeys, failedIds);
    std::cout << "GDecreaseRef: " << status.ToString() << std::endl;
}

void Start(int64_t size, bool isSeal)
{
    Write(size, isSeal);
    Read(size, isSeal);
    Modify(size);
    Delete(size, isSeal);
}

int InitClient(const ConnectOptions &connectOpts)
{
    // Get the object client
    std::string workerAddr = connectOpts.host + ':' + std::to_string(connectOpts.port);
    std::cout << "Get the object client workerAddr:" << workerAddr << std::endl;
    client_ = std::make_shared<ObjectClient>(connectOpts);
    Status status = client_->Init();
    if (status.IsError()) {
        std::cerr << "Failed to init object client : " << status.ToString() << std::endl;
        return -1;
    }

    return 0;
}

void CloseClient()
{
    if (client_) {
        client_->ShutDown();
        client_.reset();
    }
}

int main(int argc, char *argv[])
{
    const int authParametersNum = 8;
    std::string ip;
    int port = 0;
    int index = 0;
    int size;
    bool isSeal;
    std::string clientPublicKey, clientPrivateKey, serverPublicKey;

    if (argc == 1) {
        ip = DEFAULT_IP;
        port = DEFAULT_PORT;
        size = SHM_SIZE;
        isSeal = false;
    } else if (argc == PARAMETERS_NUM) {
        ip = argv[++index];
        port = atoi(argv[++index]);
        size = atoi(argv[++index]);
        isSeal = (std::string(argv[++index]) == "true");
    } else if (argc == authParametersNum) {
        // example call:
        // ./object_example 127.0.0.1 18482 true false <client public key> <client private key> <worker public key>
        ip = argv[++index];
        port = atoi(argv[++index]);
        size = atoi(argv[++index]);
        isSeal = (std::string(argv[++index]) == "true");
        clientPublicKey = argv[++index];
        clientPrivateKey = argv[++index];
        serverPublicKey = argv[++index];
    } else {
        std::cerr << "Invalid input parameters.";
        return -1;
    }

    ConnectOptions connectOpts{ .host = ip,
                                .port = port,
                                .connectTimeoutMs = 60 * 1000,
                                .requestTimeoutMs = 0,
                                .clientPublicKey = clientPublicKey,
                                .clientPrivateKey = clientPrivateKey,
                                .serverPublicKey = serverPublicKey };
    connectOpts.enableExclusiveConnection = false;
    if (InitClient(connectOpts) != 0) {
        return -1;
    }

    Start(size, isSeal);
    CloseClient();
    return 0;
}
