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
 * Description: The GPU cross-host hetero client example.
 *
 * Usage:
 *   Machine A (writer): ./hetero_client_gpu_example --write <worker_ip> <worker_port> [device_idx]
 *   Machine B (reader): ./hetero_client_gpu_example --read  <worker_ip> <worker_port> [device_idx]
 *
 * Both machines must connect to workers in the same datasystem cluster.
 * Machine A writes GPU device memory via DevMSet, Machine B reads it via DevMGet.
 * When running both writer and reader on the same machine, specify different device_idx
 * values (e.g., 0 for writer, 1 for reader) to avoid NCCL conflicts on the same GPU.
 */
#include "datasystem/datasystem.h"

#include <iostream>

#include <cuda_runtime.h>

using datasystem::HeteroClient;
using datasystem::Status;
using datasystem::Context;
using datasystem::DeviceBlobList;
using datasystem::Blob;
using datasystem::ConnectOptions;

static std::shared_ptr<HeteroClient> client_;

static std::string DEFAULT_IP = "127.0.0.1";
static constexpr int DEFAULT_PORT = 9088;
static constexpr int MIN_PARAMETERS_NUM = 4;
static constexpr int SUCCESS = 0;
static constexpr int FAILED = -1;
static constexpr int DEFAULT_DEVICE_IDX = 0;
static int g_deviceIdx = DEFAULT_DEVICE_IDX;
static constexpr int SIZE = 10;
static constexpr int NUM_BLOBS = 3;
static constexpr int NUM_KEYS = 2;
static constexpr int SUB_TIMEOUT_MS = 30000;
static constexpr char FILL_CHAR = 'G';

static bool Write()
{
    (void)Context::SetTraceId("write");
    std::vector<std::string> keys;
    std::vector<DeviceBlobList> blobLists;

    for (int k = 0; k < NUM_KEYS; k++) {
        std::string key = "key" + std::to_string(k);
        keys.push_back(key);
        std::string data(SIZE, static_cast<char>(FILL_CHAR + k));

        DeviceBlobList devSetBlobList;
        devSetBlobList.deviceIdx = g_deviceIdx;

        for (int b = 0; b < NUM_BLOBS; b++) {
            Blob blob;
            blob.size = SIZE;
            auto cudaRc = cudaMalloc(&blob.pointer, blob.size);
            if (cudaRc != cudaSuccess) {
                return false;
            }
            cudaRc = cudaMemcpy(blob.pointer, data.data(), blob.size, cudaMemcpyHostToDevice);
            if (cudaRc != cudaSuccess) {
                return false;
            }
            devSetBlobList.blobs.push_back(blob);
        }
        blobLists.push_back(devSetBlobList);
    }

    std::vector<std::string> failedIdList;
    auto setRc = client_->DevMSet(keys, blobLists, failedIdList);
    if (setRc.IsError() || !failedIdList.empty()) {
        std::cerr << "DevMSet failed: " << setRc.ToString() << std::endl;
        return false;
    }
    std::cout << "DevMSet succeeds." << std::endl;
    return true;
}

static bool Read()
{
    (void)Context::SetTraceId("read");
    std::vector<std::string> keys;
    std::vector<DeviceBlobList> devGetBlobLists;

    for (int k = 0; k < NUM_KEYS; k++) {
        std::string key = "key" + std::to_string(k);
        keys.push_back(key);

        DeviceBlobList devGetBlobList;
        devGetBlobList.deviceIdx = g_deviceIdx;

        for (int b = 0; b < NUM_BLOBS; b++) {
            Blob blob;
            blob.size = SIZE;
            auto cudaRc = cudaMalloc(&blob.pointer, blob.size);
            if (cudaRc != cudaSuccess) {
                return false;
            }
            devGetBlobList.blobs.push_back(blob);
        }
        devGetBlobLists.push_back(devGetBlobList);
    }

    std::vector<std::string> failedIdList;
    auto getRc = client_->DevMGet(keys, devGetBlobLists, failedIdList, SUB_TIMEOUT_MS);
    if (getRc.IsError() || !failedIdList.empty()) {
        std::cerr << "DevMGet failed: " << getRc.ToString() << std::endl;
        return false;
    }
    std::cout << "DevMGet succeeds." << std::endl;

    // Verify data content
    bool allCorrect = true;
    for (int k = 0; k < NUM_KEYS; k++) {
        std::string expected(SIZE, static_cast<char>(FILL_CHAR + k));
        for (int b = 0; b < NUM_BLOBS; b++) {
            std::string result(SIZE, '\0');
            auto cudaRc = cudaMemcpy(result.data(), devGetBlobLists[k].blobs[b].pointer, SIZE,
                                     cudaMemcpyDeviceToHost);
            if (cudaRc != cudaSuccess) {
                allCorrect = false;
                continue;
            }
            if (result != expected) {
                std::cerr << "Data verification failed for key" << k << " blob" << b << std::endl;
                allCorrect = false;
            }
            cudaFree(devGetBlobLists[k].blobs[b].pointer);
        }
    }

    if (allCorrect) {
        std::cout << "Data verification succeeds." << std::endl;
    }
    return allCorrect;
}

static bool InitCuda()
{
    cudaError_t ret = cudaSetDevice(g_deviceIdx);
    if (ret != cudaSuccess) {
        std::cerr << "Failed to set GPU device " << g_deviceIdx << std::endl;
        return false;
    }
    std::cout << "Using GPU device: " << g_deviceIdx << std::endl;
    return true;
}

int main(int argc, char *argv[])
{
    const int authParametersNum = 7;
    const int authWithDeviceNum = 8;
    const int baseWithDeviceNum = 5;
    std::string mode;
    std::string ip;
    int port = 0;
    int index = 0;
    std::string clientPublicKey, clientPrivateKey, serverPublicKey;

    if (argc == MIN_PARAMETERS_NUM || argc == baseWithDeviceNum) {
        mode = argv[++index];
        ip = argv[++index];
        port = atoi(argv[++index]);
        if (argc == baseWithDeviceNum) {
            g_deviceIdx = atoi(argv[++index]);
        }
    } else if (argc == authParametersNum || argc == authWithDeviceNum) {
        mode = argv[++index];
        ip = argv[++index];
        port = atoi(argv[++index]);
        clientPublicKey = argv[++index];
        clientPrivateKey = argv[++index];
        serverPublicKey = argv[++index];
        if (argc == authWithDeviceNum) {
            g_deviceIdx = atoi(argv[++index]);
        }
    } else {
        std::cerr << "Invalid input parameters." << std::endl;
        std::cerr << "Usage: " << argv[0] <<
                    " --write/--read <ip> <port> [device_idx] [pubKey privKey srvKey [device_idx]]" << std::endl;
        return FAILED;
    }

    if (mode != "--write" && mode != "--read") {
        std::cerr << "Invalid mode. Use --write or --read." << std::endl;
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

    if (!InitCuda()) {
        std::cerr << "Failed to init CUDA device." << std::endl;
        return FAILED;
    }

    bool ok = false;
    if (mode == "--write") {
        ok = Write();
        if (ok) {
            std::cout << "Data written. Press Enter to release data and exit..." << std::endl;
            std::cin.get();
        }
    } else {
        ok = Read();
    }

    if (!ok) {
        std::cerr << "The GPU hetero client example run failed." << std::endl;
    }

    client_->ShutDown();
    client_.reset();

    return ok ? SUCCESS : FAILED;
}
