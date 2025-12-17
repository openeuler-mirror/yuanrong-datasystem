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
 * Description: The hetero client example.
 */
#include "datasystem/datasystem.h"

#include <iostream>

#include <acl/acl.h>

using datasystem::HeteroClient;
using datasystem::Status;
using datasystem::Context;
using datasystem::DeviceBlobList;
using datasystem::Blob;
using datasystem::ConnectOptions;

static std::shared_ptr<HeteroClient> client_;

static std::string DEFAULT_IP = "127.0.0.1";
static constexpr int DEFAULT_PORT = 9088;
static constexpr int PARAMETERS_NUM = 3;
static constexpr int SUCCESS = 0;
static constexpr int FAILED = -1;
static constexpr int DEVICE_IDX = 0;
static constexpr int SIZE = 10;

static bool Write()
{
    (void)Context::SetTraceId("write");
    std::string key = "key1";
    std::string data(SIZE, 'x');
    Blob blob;
    blob.size = SIZE;
    auto aclRc = aclrtMalloc(&blob.pointer, blob.size, aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
    if (aclRc != ACL_SUCCESS) {
        return false;
    }
    aclRc = aclrtMemcpy(blob.pointer, blob.size, data.data(), blob.size, ACL_MEMCPY_HOST_TO_DEVICE);
    if (aclRc != ACL_SUCCESS) {
        return false;
    }
    DeviceBlobList devSetBlobList;
    devSetBlobList.deviceIdx = DEVICE_IDX;
    devSetBlobList.blobs = { blob };
    std::vector<std::string> failedIdList;
    auto setRc = client_->DevMSet({ key }, { devSetBlobList }, failedIdList);
    if (setRc.IsError()) {
        std::cerr << "DevMSet failed: " << setRc.ToString() << std::endl;
        return false;
    }
    std::cout << "DevMSet succeeds." << std::endl;
    return true;
}

static int Read()
{
    (void)Context::SetTraceId("read");
    std::string key = "key1";
    Blob blob;
    blob.size = SIZE;
    auto aclRc = aclrtMalloc(&blob.pointer, blob.size, aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
    if (aclRc != ACL_SUCCESS) {
        return false;
    }
    DeviceBlobList devGetBlobList;
    devGetBlobList.deviceIdx = DEVICE_IDX;
    devGetBlobList.blobs = { blob };
    std::vector<std::string> failedIdList;
    int subTimeoutMs = 30000;
    std::vector<DeviceBlobList> devGetBlobLists = { devGetBlobList };
    auto getRc = client_->DevMGet({ key }, devGetBlobLists, failedIdList, subTimeoutMs);
    if (getRc.IsError() || !failedIdList.empty()) {
        std::cerr << "DevMGet failed: " << getRc.ToString() << std::endl;
        return false;
    }
    std::cout << "DevMGet succeeds." << std::endl;
    return true;
}

static bool InitAcl()
{
    aclError ret = aclInit(nullptr);
    if (ret != ACL_SUCCESS) {
        return false;
    }
    ret = aclrtSetDevice(0);
    if (ret != ACL_SUCCESS) {
        return false;
    }
    return true;
}

static bool Start()
{
    return InitAcl() && Write() && Read();
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
    client_ = std::make_shared<HeteroClient>(connectOpts);
    (void)Context::SetTraceId("init");
    Status status = client_->Init();
    if (status.IsError()) {
        std::cerr << "Failed to init hetero client, detail: " << status.ToString() << std::endl;
        return FAILED;
    }

    if (!Start()) {
        std::cerr << "The hetero client example run failed." << std::endl;
        return FAILED;
    }

    client_->ShutDown();
    client_.reset();

    (void)aclFinalize();
    return SUCCESS;
}
