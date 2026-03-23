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
#include <string>

#include "p2p.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <chrono>
#include <sys/mman.h>

#include <acl/acl.h>
#include <acl/acl_prof.h>
#include <hccl/hccl.h>
#include <runtime/stream.h>

#include <cstring>
#include <cstdio>
#include <iomanip>
#include "test-tools/measurementSeries.h"
#include "test-tools/test-error.h"
#include "test-tools/fifo.h"
#include "test-tools/tools.h"
#include "securec.h"

#include <arpa/inet.h>

// HCCL can be used using #define USE_HCCL
constexpr size_t BLOCK_SIZE_MULT = 2;
constexpr size_t NUM_MS_IN_SEC = 1000;
constexpr int FRAC_2_PERC = 100;
constexpr int KEEP_0_DECIMAL_PACES = 0;
constexpr int KEEP_2_DECIMAL_PACES = 2;
constexpr int OUTPUT_WIDTH_10 = 10;
constexpr int OUTPUT_WIDTH_7 = 7;
constexpr int SHIFT_BYTES_TO_KB = 10;
constexpr P2pLink LINK_TYPE = P2P_LINK_ROCE;

int createServerSocket(int serverPort)
{
    // 1. Define server port (hardcoded)
    int server_port = 8085;

    // 2. Create a socket
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        std::cerr << "Error creating socket" << std::endl;
        return -1;
    }

    // 3. Set up server address structure
    sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;  // Listen on all interfaces
    server_addr.sin_port = htons(serverPort);

    // 4. Bind the socket
    if (bind(server_socket, (sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Bind failed" << std::endl;
        return -1;
    }

    // 5. Listen for connections
    const int len = 3;
    if (listen(server_socket, len) < 0) {
        std::cerr << "Listen failed" << std::endl;
        return -1;
    }

    std::cout << "Server listening on port " << serverPort << std::endl;

    // 6. Accept a connection
    sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    int client_socket = accept(server_socket, (sockaddr *)&client_addr, &client_addr_len);
    if (client_socket < 0) {
        std::cerr << "Accept failed" << std::endl;
        return -1;
    }
    return client_socket;
}

bool Verify(float *a, int size)
{
    for (int i = 0; i < size; i++) {
        if (a[i] != i) {
            std::cerr << std::fixed << "Verify failed at index " << i << ": " << a[i] << "!=" << i << std::endl;
            return false;
        }
    }
    return true;
}

// Pseudo barrier to synchronize both processes at least somewhat
void pseudoServerBarrier(int serverSocket)
{
    uint64_t barSendData = 1;
    uint64_t barRecvData;
    send(serverSocket, &barSendData, sizeof(barSendData), 0);
    recv(serverSocket, &barRecvData, sizeof(barRecvData), 0);
}

int SendDeviceLogic(int serverSocket, uint32_t sendDeviceId, size_t bufferSizeBytes, uint32_t recvRank,
                    uint32_t sendRank, int nSamples, int batchSize, int nDevices)
{
    NPU_ERROR(aclInit(nullptr));

    // Send device setup
    NPU_ERROR(aclrtSetDevice(sendDeviceId));
    aclrtContext sendDeviceContext;
    NPU_ERROR(aclrtCreateContext(&sendDeviceContext, sendDeviceId));
    NPU_ERROR(aclrtSetCurrentContext(sendDeviceContext));
    aclrtStream stream;
    NPU_ERROR(rtStreamCreateWithFlags(&stream, 0, RT_STREAM_FAST_SYNC));

    // Create send buffer on device
    float *hostBuff;
    NPU_ERROR(aclrtMallocHost((void **)&hostBuff, bufferSizeBytes));
    memset_s(hostBuff, bufferSizeBytes, 0, bufferSizeBytes);
    for (int i = 0; i < int(bufferSizeBytes / sizeof(float)); i += 1) {
        hostBuff[i] = static_cast<float>(i);
    }

    void **dSendBuffs = (void **)malloc(batchSize * sizeof(void *));
    for (int i = 0; i < batchSize; i++) {
        NPU_ERROR(aclrtMalloc((void **)&dSendBuffs[i], bufferSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST));
        NPU_ERROR(aclrtMemcpy(dSendBuffs[i], bufferSizeBytes, hostBuff, bufferSizeBytes, ACL_MEMCPY_HOST_TO_DEVICE));
    }

#ifdef USE_HCCL
    HcclRootInfo rootInfo;
    recv(serverSocket, &rootInfo, sizeof(HcclRootInfo), 0);
    pseudoServerBarrier(serverSocket);
    HcclComm hcclComm;
    MEASURE_TIME(NPU_ERROR(HcclCommInitRootInfo(nDevices, &rootInfo, sendRank, &hcclComm)), "Sender Init RootInfo");
#else
    HcclRootInfo rootInfo;
    recv(serverSocket, &rootInfo, sizeof(HcclRootInfo), 0);

    pseudoServerBarrier(serverSocket);
    P2PComm p2pComm;
    MEASURE_TIME(NPU_ERROR(P2PCommInitRootInfo(
                     &rootInfo, P2P_SENDER, LINK_TYPE, &p2pComm)),
                 "Sender P2PCommInitRootInfo");
#endif

    // Warmup
    for (int i = 0; i < batchSize; i++) {
#ifdef USE_HCCL
        NPU_ERROR(
            HcclSend(dSendBuffs[i], bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, recvRank, hcclComm, stream));
#else
        NPU_ERROR(P2PSend(dSendBuffs[i], bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, p2pComm, stream));
#endif
    }

    NPU_ERROR(aclrtSynchronizeStream(stream));

    // Send
    for (size_t transferSizeBytes = 128 * 1024; transferSizeBytes <= bufferSizeBytes;
         transferSizeBytes *= BLOCK_SIZE_MULT) {
        MeasurementSeries time;
        NPU_ERROR(aclrtSynchronizeStream(stream));

        pseudoServerBarrier(serverSocket);
        for (int sample = 0; sample < nSamples; sample++) {
            auto t1 = std::chrono::high_resolution_clock::now();

            for (int i = 0; i < batchSize; i++) {
#ifdef USE_HCCL
                NPU_ERROR(HcclSend(dSendBuffs[i], transferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, recvRank,
                                   hcclComm, stream));
#else
                NPU_ERROR(
                    P2PSend(dSendBuffs[i], transferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, p2pComm, stream));
#endif
            }

            NPU_ERROR(aclrtSynchronizeStream(stream));
            auto t2 = std::chrono::high_resolution_clock::now();
            const std::chrono::duration<double, std::milli> ms = t2 - t1;
            time.Add(ms.count() / (NUM_MS_IN_SEC));
        }

        double bw = (batchSize * transferSizeBytes) / time.Value();
        std::cout << std::fixed  //
                  << "Send Device: " << sendDeviceId << "   " << std::setw(OUTPUT_WIDTH_10)
                  << std::setprecision(KEEP_0_DECIMAL_PACES) << (transferSizeBytes >> SHIFT_BYTES_TO_KB) << "kB  "
                  << std::setprecision(KEEP_2_DECIMAL_PACES) << std::setw(OUTPUT_WIDTH_7)
                  << time.Value() * NUM_MS_IN_SEC << "ms " << std::setprecision(KEEP_2_DECIMAL_PACES)
                  << std::setw(OUTPUT_WIDTH_7) << bw * 1e-9 << "GB/s   " << time.Spread() * FRAC_2_PERC << "%\n";
    }

    NPU_ERROR(aclrtSynchronizeStream(stream));

    close(serverSocket);
    NPU_ERROR(aclFinalize());
    return 0;
}

int Benchmark()
{
    int batchSize = 512;
    int nSamples = 100; // ra_send_wr fails when using 1000 samples
    uint32_t sendDeviceId = 2;
    uint32_t nDevices = 2;
    size_t bufferSizeBytes = static_cast<size_t>(4) * 1024 * 1024;
    unsigned int modeRw = 0666;

    int server_port = 8085;
    int send_socket = createServerSocket(server_port);

    SendDeviceLogic(send_socket, sendDeviceId, bufferSizeBytes, 0, 1, nSamples, batchSize, nDevices);
    return 0;
}

// Note: for now send syncstream returns before receiver has received all data, so best to measure at receiver

auto main() -> int
{
    return Benchmark();
}
