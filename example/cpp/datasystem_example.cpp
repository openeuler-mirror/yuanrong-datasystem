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
 * Description: The ds client example.
 */
#include "datasystem/datasystem.h"

#include <iostream>
#include <memory>

using datasystem::ConnectOptions;
using datasystem::Context;
using datasystem::DsClient;
using datasystem::KVClient;
using datasystem::ObjectClient;
using datasystem::Status;
using datasystem::CreateParam;
using datasystem::Buffer;
using datasystem::Optional;

static std::string DEFAULT_IP = "127.0.0.1";
static constexpr int DEFAULT_PORT = 9088;
static constexpr int PARAMETERS_NUM = 3;
static constexpr int SUCCESS = 0;
static constexpr int FAILED = -1;

static std::shared_ptr<DsClient> dsClient_;
static std::shared_ptr<Buffer> buffer_;

static int Write(const std::shared_ptr<ObjectClient> &client, const std::string &writeId, std::string data, bool isSeal)
{
    std::cout << "Writing data to a buffer" << std::endl;
    std::cout << std::boolalpha << "Immutable data: " << isSeal << std::endl;
    std::vector<std::string> failedIds;
    Status status = client->GIncreaseRef({ writeId }, failedIds);
    if (status.IsError()) {
        std::cerr << "GIncrease object failed, detail: " << status.ToString() << std::endl;
        return FAILED;
    }

    status = client->Create(writeId, data.size(), CreateParam{}, buffer_);
    std::cout << "Create: " << status.ToString() << std::endl;
    if (status.IsError()) {
        return FAILED;
    }

    buffer_->WLatch();
    buffer_->MemoryCopy((void *)data.data(), data.size());
    if (isSeal) {
        status = buffer_->Seal();
    } else {
        status = buffer_->Publish();
    }
    buffer_->UnWLatch();
    if (status.IsError()) {
        std::cerr << "Publish or Seal object failed, detail: " << status.ToString() << std::endl;
        return FAILED;
    }
    std::cout << "Write succeeds." << std::endl;
    return SUCCESS;
}

static int Read(const std::shared_ptr<ObjectClient> &client, const std::string &verifyId, const std::string &verifyData)
{
    std::cout << "Reading data from a buffer" << std::endl;
    std::vector<std::string> objKeys = { verifyId };
    const int64_t timeoutMs = 60'000;
    std::vector<Optional<Buffer>> buffers;
    Status status = client->Get(objKeys, timeoutMs, buffers);
    if (!status.IsOk()) {
        std::cerr << "Read Get Fail: " << status.ToString() << std::endl;
        return FAILED;
    }
    std::cout << "Get: " << status.ToString() << std::endl;
    auto &buf = buffers[0];
    buf->RLatch();
    auto data = std::string(reinterpret_cast<const char *>(buf->ImmutableData()), buf->GetSize());
    buf->UnRLatch();
    if (data != verifyData) {
        std::cerr << "The obtained value \"" << data << "\" is different from the verified value \"" << verifyData
                  << "\"." << std::endl;
        return FAILED;
    }
    std::cout << "Read succeeds." << std::endl;
    return SUCCESS;
}

static int Modify(const std::shared_ptr<ObjectClient> &client, const std::string &writeId,
                  const std::string &updateValue)
{
    std::cout << "Modifying data in the buffer" << std::endl;
    std::vector<std::string> objKeys = { writeId };
    const int64_t timeoutMs = 60'000;
    std::vector<Optional<Buffer>> buffers;
    Status status = client->Get(objKeys, timeoutMs, buffers);
    if (!status.IsOk()) {
        std::cerr << "Modify Get Fail: " << status.ToString() << std::endl;
        return FAILED;
    }
    std::cout << "Get: " << status.ToString() << std::endl;
    auto &buf = buffers[0];
    auto newDataSize = updateValue.size();
    buf->WLatch();
    // If the length of the modified buffer is less than the length of the original buffer, the following characters
    // will remain.
    buf->MemoryCopy((void *)updateValue.data(), newDataSize);
    buf->Publish();
    buf->UnWLatch();
    std::cout << "Modify succeeds !!!" << std::endl;
    return SUCCESS;
}

int RunObjectTest(const std::shared_ptr<ObjectClient> &client)
{
    std::cout << "Run object client test." << std::endl;
    std::string writeObjectKey = "writeKey";
    std::string writeData("Hello object client");
    std::string modifyData("Hello modify object");

    if (Write(client, writeObjectKey, writeData, false) != SUCCESS) {
        return FAILED;
    }

    if (Read(client, writeObjectKey, writeData) != SUCCESS) {
        return FAILED;
    }

    if (Modify(client, writeObjectKey, modifyData) != SUCCESS) {
        return FAILED;
    }

    if (Read(client, writeObjectKey, modifyData) != SUCCESS) {
        return FAILED;
    }
    return SUCCESS;
}

int RunKVTest(const std::shared_ptr<KVClient> &kvClient)
{
    std::cout << "Run kv client test." << std::endl;
    std::string key = "testKey";
    std::string value = "Hello kv client";
    std::string value2 = "Hello modify";
    Status status = kvClient->Set(key, value);
    std::cout << "Set status: " << status.ToString();
    if (status.IsError()) {
        return FAILED;
    }

    std::string getValue;
    status = kvClient->Get(key, getValue);
    std::cout << "Get status: " << status.ToString();
    if (status.IsError()) {
        return FAILED;
    }

    status = kvClient->Set(key, value2);
    std::cout << "Modify Set status: " << status.ToString();
    if (status.IsError()) {
        return FAILED;
    }

    status = kvClient->Get(key, getValue);
    std::cout << "Get status: " << status.ToString();
    if (status.IsError()) {
        return FAILED;
    }

    status = kvClient->Del(key);
    std::cout << "Del status: " << status.ToString();
    if (status.IsError()) {
        return FAILED;
    }
    return SUCCESS;
}

int main(int argc, char *argv[])
{
    std::string ip;
    int port;
    int index = 0;

    if (argc == 1) {
        ip = DEFAULT_IP;
        port = DEFAULT_PORT;
    } else if (argc == PARAMETERS_NUM) {
        ip = argv[++index];
        port = std::atoi(argv[++index]);
    } else {
        std::cerr << "Invalid input parameters " << argv << std::endl;
        return FAILED;
    }

    ConnectOptions connectOptions{ .host = ip, .port = port, .connectTimeoutMs = 3 * 1000 };
    connectOptions.enableExclusiveConnection = false;
    dsClient_ = std::make_shared<DsClient>(connectOptions);
    (void)Context::SetTraceId("init");
    Status status = dsClient_->Init();
    if (status.IsError()) {
        std::cerr << "Failed to init ds client, detail: " << status.ToString() << std::endl;
        return FAILED;
    }

    std::shared_ptr<ObjectClient> objectClient = dsClient_->Object();
    if (RunObjectTest(objectClient) == FAILED) {
        std::cerr << "Run object client test failed." << std::endl;
        return FAILED;
    }

    std::shared_ptr<KVClient> kvClient = dsClient_->KV();
    if (RunKVTest(kvClient) == FAILED) {
        std::cerr << "Run kv client test failed." << std::endl;
        return FAILED;
    }

    std::cout << "Run ds client test success." << std::endl;
    return SUCCESS;
}