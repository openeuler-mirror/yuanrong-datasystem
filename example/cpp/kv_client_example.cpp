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
 * Description: The kv client example.
 */
#include "datasystem/datasystem.h"

#include <iostream>

using datasystem::ConnectOptions;
using datasystem::Context;
using datasystem::Optional;
using datasystem::ReadOnlyBuffer;
using datasystem::KVClient;
using datasystem::Status;

static std::shared_ptr<KVClient> client_;

static std::string DEFAULT_IP = "127.0.0.1";
static constexpr int DEFAULT_PORT = 9088;
static constexpr int PARAMETERS_NUM = 3;
static constexpr int SUCCESS = 0;
static constexpr int FAILED = -1;

static int Write()
{
    (void)Context::SetTraceId("write");
    std::string objectKey = "key1";
    std::string val = "test1";
    datasystem::SetParam opt;
    opt.writeMode = datasystem::WriteMode::NONE_L2_CACHE;
    Status status = client_->Set(objectKey, val, opt);
    if (status.IsError()) {
        std::cerr << "Set Fail: " << status.ToString() << std::endl;
        return FAILED;
    }
    std::cout << "KV client set succeeds." << std::endl;
    return SUCCESS;
}

static int Read()
{
    (void)Context::SetTraceId("read");
    std::string objectKey = "key1";
    std::string correctVal = "test1";
    std::string val;
    Status status = client_->Get(objectKey, val);
    if (status.IsError()) {
        std::cerr << "Get string value failed, detail: " << status.ToString() << std::endl;
        return FAILED;
    }
    if (correctVal == val) {
        std::cout << "KV client get string value succeeds." << std::endl;
    } else {
        std::cerr << "Get string value failed, expect value: " << correctVal << ", but get val: " << val << std::endl;
        return FAILED;
    }

    Optional<ReadOnlyBuffer> buffer;
    status = client_->Get(objectKey, buffer);
    auto str = std::string(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    if (status.IsError()) {
        std::cerr << "Get buffer value failed, detail: " << status.ToString() << std::endl;
        return FAILED;
    }
    if (correctVal != str) {
        std::cerr << "Get string value failed, expect value: " << correctVal << ", but get val: " << str << std::endl;
        return FAILED;
    }
    std::cout << "KV client get succeeds." << std::endl;
    return SUCCESS;
}

static int Delete()
{
    (void)Context::SetTraceId("delete");
    std::string objectKey = "key1";
    Status status = client_->Del(objectKey);
    if (status.IsError()) {
        std::cerr << "Delete failed, detail: " << status.ToString() << std::endl;
        return FAILED;
    }
    std::cout << "KV client delete succeeds." << std::endl;
    return SUCCESS;
}

static int TestSetValue()
{
    std::string val = "test1";
    datasystem::SetParam opt;
    uint32_t ttl = 5;
    opt.writeMode = datasystem::WriteMode::NONE_L2_CACHE;
    opt.ttlSecond = ttl;
    std::string key = client_->Set(val, opt);
    if (key.empty()) {
        std::cerr << "The key from set value api is empty." << std::endl;
        return FAILED;
    }
    std::cout << "KV client set value api succeeds." << std::endl;
    return SUCCESS;
}

int Start()
{
    int ret1 = Write();
    int ret2 = Read();
    int ret3 = Delete();
    int ret4 = TestSetValue();
    return ret1 | ret2 | ret3 | ret4;
}

int main(int argc, char *argv[])
{
    const int authParametersNum = 6;
    std::string ip;
    int port = 0;
    int index = 0;
    std::string clientPublicKey, clientPrivateKey, serverPublicKey;

    if (argc == 1) {
        ip = DEFAULT_IP;
        port = DEFAULT_PORT;
    } else if (argc == PARAMETERS_NUM) {
        ip = argv[++index];
        port = atoi(argv[++index]);
    } else if (argc == authParametersNum) {
        // example call:
        // ./kv_example 127.0.0.1 18482 <client public key> <client private key> <worker public key>
        ip = argv[++index];
        port = atoi(argv[++index]);
        clientPublicKey = argv[++index];
        clientPrivateKey = argv[++index];
        serverPublicKey = argv[++index];
    } else {
        std::cerr << "Invalid input parameters.";
        return FAILED;
    }

    ConnectOptions connectOpts{ .host = ip,
                                .port = port,
                                .connectTimeoutMs = 3 * 1000,
                                .requestTimeoutMs = 0,
                                .clientPublicKey = clientPublicKey,
                                .clientPrivateKey = clientPrivateKey,
                                .serverPublicKey = serverPublicKey };
    connectOpts.enableExclusiveConnection = false;
    client_ = std::make_shared<KVClient>(connectOpts);
    (void)Context::SetTraceId("init");
    Status status = client_->Init();
    if (status.IsError()) {
        std::cerr << "Failed to init kv client, detail: " << status.ToString() << std::endl;
        return FAILED;
    }

    if (Start() == FAILED) {
        std::cerr << "The kv client example run failed." << std::endl;
        return FAILED;
    }

    client_->ShutDown();
    client_.reset();

    return SUCCESS;
}
